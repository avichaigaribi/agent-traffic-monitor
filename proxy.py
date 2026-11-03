#!/usr/bin/env python3
"""proxy.py: agent-traffic proxy + live dashboard for Lab 4.

A reverse proxy that prints every request and response, forwards them unchanged,
and also serves a live browser dashboard of the traffic. Standard library only.

    python3 proxy.py                       # proxy on :8041, dashboard on :8042
    ANTHROPIC_BASE_URL=http://localhost:8041 claude
    # then open http://localhost:8042

The proxy changes nothing: headers go through untouched and the reply is
streamed back byte for byte. Capture is best-effort and can never break the
request path. History is kept in memory only (most recent N exchanges).

    python3 proxy.py --port 9000 --dashboard-port 9001 --max-exchanges 500
    python3 proxy.py --upstream https://chatgpt.com/backend-api/codex
"""

from __future__ import annotations

import argparse
import codecs
import http.client
import json
import queue
import sys
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

# Headers that describe one hop of the connection, not the message. They are
# never forwarded. `accept-encoding` is dropped too, so the API answers in plain
# text and what we print is readable.
SKIP = {
    "host", "connection", "keep-alive", "transfer-encoding", "te", "trailers",
    "upgrade", "proxy-authorization", "proxy-authenticate", "accept-encoding",
    "content-length",
}

UPSTREAM = "https://api.anthropic.com"


def stamp() -> str:
    return datetime.now().strftime("%H:%M:%S")


def now_ms() -> int:
    return int(time.time() * 1000)


def mask(name: str, value: str) -> str:
    return value[:16] + "..." if name.lower() in ("authorization", "x-api-key") else value


# --------------------------------------------------------------------------
# Capture hub: in-memory history + live fan-out to dashboard subscribers.
# Every public method swallows its own errors: capture must never break proxying.
# --------------------------------------------------------------------------

def _safe(fn):
    def wrap(*a, **k):
        try:
            return fn(*a, **k)
        except Exception as e:  # noqa: BLE001
            try:
                sys.stderr.write(f"[capture error] {e}\n")
            except Exception:  # noqa: BLE001
                pass
            return None
    return wrap


class Hub:
    def __init__(self, cap: int = 500) -> None:
        self.cap = max(1, cap)
        self.lock = threading.Lock()
        self.items: list[dict] = []
        self.by_id: dict[int, dict] = {}
        self.subs: set[queue.Queue] = set()
        self.seq = 0

    def _push(self, ev: dict) -> None:  # caller holds the lock
        data = json.dumps(ev, ensure_ascii=False)
        for q in list(self.subs):
            try:
                q.put_nowait(data)
            except queue.Full:
                self.subs.discard(q)  # slow client: dropped, it will reconnect + resync

    @staticmethod
    def _public(ex: dict) -> dict:
        d = {k: v for k, v in ex.items() if k != "parts"}
        d["resp"] = "".join(ex["parts"])
        return d

    @_safe
    def start(self, method: str, path: str, headers, body: bytes) -> int | None:
        with self.lock:
            self.seq += 1
            ex = {
                "id": self.seq, "seq": self.seq, "t0": now_ms(), "method": method, "path": path,
                "reqHeaders": {k: mask(k, v) for k, v in headers.items()},
                "body": body.decode("utf-8", "replace"),
                "status": None, "respHeaders": {}, "tHead": None, "tEnd": None,
                "parts": [], "error": None, "done": False,
            }
            self.items.append(ex)
            self.by_id[ex["id"]] = ex
            while len(self.items) > self.cap:
                old = self.items.pop(0)
                self.by_id.pop(old["id"], None)
            self._push({"type": "start", "ex": self._public(ex)})
            return ex["id"]

    @_safe
    def head(self, eid, status: int, headers: list) -> None:
        with self.lock:
            ex = self.by_id.get(eid)
            if ex:
                ex["status"] = status
                ex["respHeaders"] = {k: v for k, v in headers}
                ex["tHead"] = now_ms()
                self._push({"type": "head", "id": eid, "status": status,
                            "headers": ex["respHeaders"], "t": ex["tHead"]})

    @_safe
    def chunk(self, eid, text: str) -> None:
        if not text:
            return
        with self.lock:
            ex = self.by_id.get(eid)
            if ex:
                ex["parts"].append(text)
                self._push({"type": "chunk", "id": eid, "text": text})

    @_safe
    def done(self, eid) -> None:
        with self.lock:
            ex = self.by_id.get(eid)
            if ex and not ex["done"]:
                ex["done"] = True
                ex["tEnd"] = now_ms()
                self._push({"type": "done", "id": eid, "t": ex["tEnd"]})

    @_safe
    def error(self, eid, message: str, status: int | None = None) -> None:
        with self.lock:
            ex = self.by_id.get(eid)
            if ex:
                ex["error"] = message
                if status is not None:
                    ex["status"] = status
                ex["done"] = True
                ex["tEnd"] = now_ms()
                self._push({"type": "error", "id": eid, "message": message, "status": status, "t": ex["tEnd"]})

    def subscribe(self) -> tuple[queue.Queue, str]:
        q: queue.Queue = queue.Queue(maxsize=20000)
        with self.lock:
            snap = json.dumps({"type": "snapshot", "exchanges": [self._public(e) for e in self.items]},
                              ensure_ascii=False)
            self.subs.add(q)
        return q, snap

    def unsubscribe(self, q: queue.Queue) -> None:
        with self.lock:
            self.subs.discard(q)

    def history(self) -> str:
        with self.lock:
            return json.dumps({"exchanges": [self._public(e) for e in self.items]}, ensure_ascii=False)


HUB = Hub()


# --------------------------------------------------------------------------
# The proxy
# --------------------------------------------------------------------------

class Tap(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass  # we print our own lines

    def do_POST(self) -> None:
        self.relay()

    def do_GET(self) -> None:
        self.relay()

    def do_HEAD(self) -> None:
        self.relay()

    def relay(self) -> None:
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""

        # 1. Print what the agent sent (and record it for the dashboard).
        eid = HUB.start(self.command, self.path, self.headers, body)
        print(f"\n{'=' * 78}\n[{stamp()}] >>> {self.command} {self.path}", flush=True)
        for name, value in self.headers.items():
            print(f"    {name}: {mask(name, value)}")
        if body:
            try:
                print(json.dumps(json.loads(body), indent=2, ensure_ascii=False))
            except ValueError:
                print(body.decode("utf-8", "replace"))

        # 2. Forward it, headers intact.
        target = urlsplit(UPSTREAM)
        conn_cls = http.client.HTTPSConnection if target.scheme == "https" else http.client.HTTPConnection
        # Connect with a short timeout (a lost SYN must not stall the agent for
        # minutes); the long timeout is for reading a slow stream.
        upstream = conn_cls(target.netloc, timeout=10)
        try:
            upstream.connect()
        except OSError as e:
            print(f"\n[{stamp()}] !!! cannot reach {UPSTREAM}: {e}", flush=True)
            HUB.error(eid, f"proxy: cannot reach {UPSTREAM}: {e}", 502)
            self.send_error(502, f"proxy: cannot reach {UPSTREAM}: {e}")
            self.close_connection = True
            return
        upstream.sock.settimeout(600)
        headers = {k: v for k, v in self.headers.items() if k.lower() not in SKIP}
        try:
            upstream.request(self.command, target.path.rstrip("/") + self.path, body=body or None, headers=headers)
            reply = upstream.getresponse()
        except Exception as e:  # noqa: BLE001
            print(f"\n[{stamp()}] !!! upstream failure: {e}", flush=True)
            HUB.error(eid, f"proxy: upstream failure: {e}", 502)
            self.send_error(502, f"proxy: upstream failure: {e}")
            self.close_connection = True
            return

        # 3. Stream the reply back as it arrives, printing it on the way.
        print(f"\n[{stamp()}] <<< {reply.status} {reply.reason}", flush=True)
        HUB.head(eid, reply.status, reply.getheaders())
        self.send_response(reply.status, reply.reason)
        for name, value in reply.getheaders():
            if name.lower() not in SKIP and name.lower() != "content-encoding":
                self.send_header(name, value)
        self.send_header("Connection", "close")
        self.end_headers()
        if self.command == "HEAD":
            HUB.done(eid)
            self.close_connection = True
            return
        decoder = codecs.getincrementaldecoder("utf-8")("replace")
        try:
            while chunk := reply.read1(4096):  # read1: pass on each chunk as it lands, don't wait for 4096 bytes
                self.wfile.write(chunk)
                self.wfile.flush()
                sys.stdout.write(chunk.decode("utf-8", "replace"))
                sys.stdout.flush()
                HUB.chunk(eid, decoder.decode(chunk))
            HUB.chunk(eid, decoder.decode(b"", final=True))
            HUB.done(eid)
        except Exception as e:  # noqa: BLE001
            HUB.error(eid, f"stream interrupted: {e}")
            raise
        finally:
            self.close_connection = True
        print(f"\n[{stamp()}] --- done", flush=True)


# --------------------------------------------------------------------------
# Dashboard server: static page, SSE live feed, JSON history.
# --------------------------------------------------------------------------

class Dash(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass

    def _send(self, code: int, ctype: str, data: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)
        self.close_connection = True

    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        try:
            if path in ("/", "/index.html"):
                self._send(200, "text/html; charset=utf-8", PAGE.encode())
            elif path == "/api/history":
                self._send(200, "application/json; charset=utf-8", HUB.history().encode())
            elif path == "/events":
                self.events()
            else:
                self._send(404, "text/plain", b"not found")
        except (BrokenPipeError, ConnectionResetError, OSError):
            self.close_connection = True

    def events(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        q, snap = HUB.subscribe()
        try:
            self.wfile.write(f"data: {snap}\n\n".encode())
            self.wfile.flush()
            while True:
                try:
                    data = q.get(timeout=15)
                    out = f"data: {data}\n\n"
                except queue.Empty:
                    if q not in HUB.subs:
                        break  # we were dropped for being slow: close so the client resyncs
                    out = ": keepalive\n\n"
                self.wfile.write(out.encode())
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            HUB.unsubscribe(q)
            self.close_connection = True


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Agent Traffic Dashboard</title>
<style>
:root{--bg:#f6f7f9;--panel:#fff;--fg:#1c2128;--dim:#6a737d;--bd:#d8dde3;--sel:#e6f0ff;--code:#f0f2f5;
--sys:#8250df;--tools:#0a8f8f;--user:#1f6feb;--asst:#1a7f37;--call:#d4650a;--res:#9a6700;--new:#fff8c5;--err:#cf222e;--errbg:#ffebe9;--ok:#1a7f37;
--k:#0550ae;--s:#0a3069;--n:#953800;--b:#8250df}
@media(prefers-color-scheme:dark){:root{--bg:#0d1117;--panel:#161b22;--fg:#e6edf3;--dim:#8b949e;--bd:#30363d;--sel:#16304f;--code:#0d1117;
--sys:#bc8cff;--tools:#39c5c5;--user:#58a6ff;--asst:#56d364;--call:#ffa657;--res:#e3b341;--new:#3d3200;--err:#ff7b72;--errbg:#3c1618;--ok:#56d364;
--k:#79c0ff;--s:#a5d6ff;--n:#ffa657;--b:#d2a8ff}}
*{box-sizing:border-box}
html,body{margin:0;height:100%;background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,sans-serif;overflow-x:hidden}
pre,code,.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
#app{display:flex;flex-direction:column;height:100vh}
header{padding:8px 12px;background:var(--panel);border-bottom:1px solid var(--bd);display:flex;flex-wrap:wrap;gap:8px 16px;align-items:center}
header h1{font-size:15px;margin:0}
.stat b{font-size:15px}.stat{color:var(--dim)}
#bar{padding:6px 12px;border-bottom:1px solid var(--bd);background:var(--panel);display:flex;flex-wrap:wrap;gap:6px 10px;align-items:center}
input,select,button{font:inherit;color:var(--fg);background:var(--panel);border:1px solid var(--bd);border-radius:6px;padding:3px 8px}
button{cursor:pointer}button:hover{background:var(--sel)}
.chip{padding:1px 8px;border-radius:12px;font-size:12px}.chip.on{background:var(--call);color:#fff;border-color:var(--call)}
#conn{font-size:12px;padding:2px 8px;border-radius:10px;border:1px solid var(--bd)}
#conn.ok{color:var(--ok)}#conn.bad{color:var(--err);border-color:var(--err)}
#main{flex:1;display:grid;grid-template-columns:minmax(280px,380px) minmax(0,1fr);min-height:0}
#list{overflow-y:auto;border-right:1px solid var(--bd);background:var(--panel)}
#detail{overflow-y:auto;padding:12px;min-width:0}
.row{padding:5px 10px;border-bottom:1px solid var(--bd);cursor:pointer;display:flex;flex-wrap:wrap;gap:2px 8px;align-items:baseline;border-left:4px solid transparent}
.row:hover{background:var(--code)}.row.sel{background:var(--sel)}
.row.err{border-left-color:var(--err);background:var(--errbg)}
.row .t{color:var(--dim);font-size:12px}.row .tl{flex-basis:100%;font-size:12px;color:var(--call);overflow-wrap:anywhere}
.badge{display:inline-block;font-size:11px;padding:0 6px;border-radius:9px;border:1px solid var(--bd);white-space:nowrap}
.badge.err{color:var(--err);border-color:var(--err);background:var(--errbg);font-weight:600}
.badge.ok{color:var(--ok);border-color:var(--ok)}.badge.pend{color:var(--res);border-color:var(--res)}
.badge.new{background:var(--res);color:#fff;border-color:var(--res)}
.badge.main{color:var(--user)}.badge.subagent{color:var(--sys)}.badge.side{color:var(--dim)}
.dim{color:var(--dim)}
section.sec{border:1px solid var(--bd);border-left:5px solid var(--bd);border-radius:6px;background:var(--panel);margin:0 0 10px}
section.sec>details>summary{font-weight:600;padding:6px 10px;cursor:pointer}
section.sec>details>.lzb,section.sec>details>.body{padding:6px 10px}
section.sys{border-left-color:var(--sys)}section.tools{border-left-color:var(--tools)}section.conv{border-left-color:var(--user)}
section.newm{border-left-color:var(--res);background:var(--new)}section.reply{border-left-color:var(--asst)}section.diff{border-left-color:var(--dim)}
section.errs{border-left-color:var(--err);background:var(--errbg)}
.msg{border:1px solid var(--bd);border-left:4px solid var(--user);border-radius:5px;margin:6px 0;padding:4px 8px;background:var(--panel)}
.msg.assistant{border-left-color:var(--asst)}.msg.new{background:var(--new);border-color:var(--res)}
.mh{font-weight:600;font-size:12px}
.tool{border:1px solid var(--bd);border-left:4px solid var(--call);border-radius:5px;margin:6px 0;padding:4px 8px}
.tool.res{border-left-color:var(--res)}.tool.res.iserr,.tres.iserr{border-left-color:var(--err);background:var(--errbg)}
.tres{border-left:4px solid var(--res);padding:2px 8px;margin-top:4px}.tres.none{color:var(--dim);font-style:italic}
.rh{font-size:12px;font-weight:600;color:var(--res)}
pre.tx,pre.cp{margin:2px 0;padding:6px 8px;background:var(--code);border-radius:5px;white-space:pre-wrap;overflow-wrap:anywhere;word-break:break-word;font-size:12.5px;max-width:100%}
.lnk{border:none;background:none;color:var(--user);padding:0 4px;text-decoration:underline;font-size:12px}
.jd{margin-left:0}.jd>summary{cursor:pointer;list-style:revert}.ji{margin-left:16px;border-left:1px dotted var(--bd);padding-left:8px}
.jbox{font-family:ui-monospace,Consolas,monospace;font-size:12.5px;overflow-wrap:anywhere}
.jk{color:var(--k)}.js{color:var(--s)}.jn{color:var(--n)}.jb{color:var(--b)}
.params{display:flex;flex-wrap:wrap;gap:4px}.params span{background:var(--code);border-radius:5px;padding:0 6px;font-size:12px}
.dl{font-family:ui-monospace,Consolas,monospace;font-size:12px;white-space:pre-wrap;overflow-wrap:anywhere}
.dl .a{color:var(--ok)}.dl .d{color:var(--err)}
.flash{outline:2px solid var(--call)}
.crit{border:1px solid var(--bd);border-left:4px solid var(--call);border-radius:4px;padding:6px 10px;margin-bottom:10px;font-size:13px}.crit div{margin:2px 0;overflow-wrap:anywhere}.crit .ce{color:var(--err);font-weight:600}
.errbanner{background:var(--errbg);border:1px solid var(--err);color:var(--err);padding:6px 10px;border-radius:6px;margin-bottom:10px;font-weight:600}
h2.dh{margin:0 0 8px;font-size:15px;overflow-wrap:anywhere}
@media(max-width:800px){#main{grid-template-columns:1fr;grid-template-rows:40% 60%}#list{border-right:none;border-bottom:1px solid var(--bd)}}
</style></head><body><div id="app">
<header><h1>Agent Traffic</h1><span id="conn" class="bad">connecting…</span>
<span class="stat">exchanges <b id="sTotal">0</b></span><span class="stat">errors <b id="sErr">0</b></span>
<span class="stat">tokens in/out <b id="sTok">0</b></span><span class="stat" id="sTools"></span></header>
<div id="bar">
<button id="pause">⏸ Pause</button><span id="pcount" class="dim"></span>
<input id="q" type="search" placeholder="Search all content…" size="22">
<select id="kind"><option value="all">all threads</option><option value="main">main</option><option value="subagent">subagent</option><option value="side">side call</option></select>
<label><input type="checkbox" id="onlyErr"> ⛔ errors only</label>
<span id="chips"></span></div>
<div id="main"><div id="list"></div><div id="detail"><p class="dim">Waiting for traffic… run the agent with ANTHROPIC_BASE_URL pointed at the proxy.</p></div></div></div>
<script>
const $=s=>document.querySelector(s);
const esc=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let X=new Map(),sel=null,userPicked=false,paused=false,pendingN=0,reg=[],tm={},threads=[];
const filt={tools:new Set(),errors:false,kind:'all',q:''};
let chipKey='',timer=null;

/* ---------- parsing ---------- */
function sysText(s){if(!s)return'';if(typeof s==='string')return s;return s.map(b=>b.text||JSON.stringify(b)).join('\n\n')}
function rtext(c){if(c==null)return'';if(typeof c==='string')return c;if(Array.isArray(c))return c.map(b=>b&&b.type==='text'?b.text:JSON.stringify(b)).join('\n');return JSON.stringify(c,null,2)}
function blocksOf(m){return typeof m.content==='string'?[{type:'text',text:m.content}]:(Array.isArray(m.content)?m.content:[])}
function parseSSE(text){
  const evs=[];let msg=null,err=null;const blocks=[];
  for(const part of text.split(/\r?\n\r?\n/)){
    let ev='',data='';for(const l of part.split(/\r?\n/)){if(l.startsWith('event:'))ev=l.slice(6).trim();else if(l.startsWith('data:'))data+=l.slice(5).trim()}
    if(!data)continue;let d;try{d=JSON.parse(data)}catch(e){continue}
    evs.push(d);const t=d.type||ev;
    if(t==='message_start'){msg=Object.assign({},d.message,{content:[]});msg.usage=Object.assign({},d.message.usage)}
    else if(t==='content_block_start'){blocks[d.index]=Object.assign({},d.content_block);if(blocks[d.index].type==='tool_use')blocks[d.index]._j=''}
    else if(t==='content_block_delta'){const b=blocks[d.index];if(!b)continue;const x=d.delta;
      if(x.type==='text_delta')b.text=(b.text||'')+x.text;else if(x.type==='thinking_delta')b.thinking=(b.thinking||'')+x.thinking;
      else if(x.type==='input_json_delta')b._j+=x.partial_json;}
    else if(t==='message_delta'){if(msg){if(d.delta&&d.delta.stop_reason)msg.stop_reason=d.delta.stop_reason;msg.usage=Object.assign(msg.usage||{},d.usage||{})}}
    else if(t==='error'){err=d.error||d}
  }
  if(msg){msg.content=blocks.filter(Boolean).map(b=>{if(b.type==='tool_use'){const j=b._j;delete b._j;if(j===''){b.input=b.input||{}}else{try{b.input=JSON.parse(j)}catch(e){b.input={};b._partial=j}}}return b})}
  return {message:msg,error:err,events:evs};
}
function derive(ex){
  if(ex._rb!==ex.body){ex._rb=ex.body;try{ex.req=JSON.parse(ex.body)}catch(e){ex.req=null}
    ex.isMsg=!!(ex.req&&Array.isArray(ex.req.messages)&&/\/messages(\?|$)/.test(ex.path));ex.msgs=ex.isMsg?ex.req.messages:[]}
  if(ex._rl!==ex.resp.length){ex._rl=ex.resp.length;ex.message=null;ex.rerr=null;ex.events=null;ex.json=null;
    const ct=(ex.respHeaders&&(ex.respHeaders['content-type']||ex.respHeaders['Content-Type']))||'';
    if(/event-stream/.test(ct)||/^\s*(event|data):/.test(ex.resp)){const p=parseSSE(ex.resp);ex.message=p.message;ex.rerr=p.error;ex.events=p.events;ex.sse=true}
    else{ex.sse=false;try{ex.json=JSON.parse(ex.resp)}catch(e){ex.json=null}
      if(ex.json){if(ex.json.type==='message')ex.message=ex.json;else if(ex.json.type==='error')ex.rerr=ex.json.error||ex.json}}
    ex._hay=(ex.body+'\n'+ex.resp).toLowerCase()}
  ex.isErr=!!(ex.error||(ex.status!=null&&ex.status>=400)||ex.rerr);
}
function analyze(){
  const all=[...X.values()].sort((a,b)=>a.seq-b.seq);all.forEach(derive);
  tm={};const tk=new Map();threads=[];
  for(const ex of all){
    if(ex.isMsg){const k=sysText(ex.req.system).slice(0,500)+'|'+JSON.stringify(ex.msgs[0]||'').slice(0,500);
      let th=tk.get(k);if(!th){th={key:k,items:[],hasTools:false};tk.set(k,th);threads.push(th)}
      th.items.push(ex);ex.thread=th;if(Array.isArray(ex.req.tools)&&ex.req.tools.length)th.hasTools=true;
      let ns=0;ex.msgs.forEach((m,i)=>{if(m.role==='assistant')ns=i+1});ex.newStart=Math.min(ns,Math.max(0,ex.msgs.length-1));
      for(const m of ex.msgs)if(m.role==='assistant')for(const b of blocksOf(m))if(b.type==='tool_use'&&!tm[b.id])tm[b.id]={id:b.id,name:b.name,input:b.input,calledIn:null};
    }
    if(ex.message&&Array.isArray(ex.message.content))for(const b of ex.message.content)if(b.type==='tool_use')tm[b.id]=Object.assign(tm[b.id]||{},{id:b.id,name:b.name,input:b.input,calledIn:ex.id});
    if(ex.isMsg)for(const m of ex.msgs)if(m.role==='user')for(const b of blocksOf(m))if(b.type==='tool_result'){const t=tm[b.tool_use_id];if(t&&t.resultIn==null){t.resultIn=ex.id;t.result=b;t.isError=!!b.is_error}}
  }
  for(const id in tm){const t=tm[id];t.status=!t.result?'pending':t.isError?'error':'ok'}
  const mainTh=threads.find(t=>t.hasTools);
  for(const th of threads){th.kind=!th.hasTools?'side':th===mainTh?'main':'subagent';th.items.forEach(e=>e.kind=th.kind)}
  let errs=0,tin=0,tout=0;const tc={};
  for(const ex of all){
    if(!ex.isMsg)ex.kind='raw';
    const names=new Set();let toolErr=false;
    if(ex.message&&Array.isArray(ex.message.content))for(const b of ex.message.content)if(b.type==='tool_use'){names.add(b.name);tc[b.name]=(tc[b.name]||0)+1}
    if(ex.isMsg)ex.msgs.slice(ex.newStart).forEach(m=>{if(m.role==='user')for(const b of blocksOf(m))if(b.type==='tool_result'){const t=tm[b.tool_use_id];if(t)names.add(t.name);if(b.is_error)toolErr=true}});
    ex.toolNames=[...names];ex.toolErr=toolErr;ex.anyErr=ex.isErr||toolErr;if(ex.anyErr)errs++;
    const u=ex.message&&ex.message.usage;if(u){tin+=(u.input_tokens||0)+(u.cache_read_input_tokens||0)+(u.cache_creation_input_tokens||0);tout+=u.output_tokens||0}
  }
  $('#sTotal').textContent=all.length;$('#sErr').textContent=errs;$('#sTok').textContent=tin.toLocaleString()+' / '+tout.toLocaleString();
  $('#sTools').textContent=Object.keys(tc).length?'tools: '+Object.entries(tc).sort((a,b)=>b[1]-a[1]).map(([k,v])=>k+'×'+v).join(' '):'';
  const key=Object.keys(tc).sort().join(',');
  if(key!==chipKey){chipKey=key;$('#chips').innerHTML=Object.keys(tc).sort().map(n=>`<button class="chip ${filt.tools.has(n)?'on':''}" data-tool="${esc(n)}">${esc(n)}</button>`).join('')}
  return all;
}

/* ---------- rendering helpers ---------- */
function lz(summary,render,open,cls){const i=reg.push(render)-1;return `<details class="${cls||''}" data-lz="${i}"${open?' open':''}><summary>${summary}</summary><div class="lzb"></div></details>`}
function hydrate(root){let again=true;while(again){again=false;root.querySelectorAll('details[data-lz][open]').forEach(d=>{if(!d.dataset.done){d.dataset.done=1;d.querySelector(':scope>.lzb').innerHTML=reg[d.dataset.lz]();again=true}})}}
document.addEventListener('toggle',e=>{const d=e.target;if(d.dataset&&d.dataset.lz!==undefined&&d.open&&!d.dataset.done)hydrate(d.parentNode)},true);
function ctext(s,cls){s=String(s==null?'':s);const n=s.length,lines=s.split('\n').length;
  if(n<=700&&lines<=10)return `<pre class="tx ${cls||''}">${esc(s)}</pre>`;
  const i=reg.push(s)-1;let pv=s.split('\n').slice(0,6).join('\n');if(pv.length>400)pv=pv.slice(0,400);
  return `<div class="ct"><pre class="tx ${cls||''}">${esc(pv)}…</pre><button class="lnk" data-more="${i}">show more (${n.toLocaleString()} chars, ${lines} lines)</button></div>`}
function jv(v,d){d=d||0;
  if(v===null||v===undefined)return '<span class="jn">null</span>';
  if(typeof v==='string')return (v.length>300||v.includes('\n'))?ctext(v):`<span class="js">${esc(JSON.stringify(v))}</span>`;
  if(typeof v==='number')return `<span class="jn">${v}</span>`;if(typeof v==='boolean')return `<span class="jb">${v}</span>`;
  const arr=Array.isArray(v),keys=Object.keys(v);if(!keys.length)return arr?'[]':'{}';
  return `<details class="jd"${d<1?' open':''}><summary>${arr?'[':'{'} <span class="dim">${keys.length} ${arr?'items':'keys'}</span></summary><div class="ji">${keys.map((k,i)=>`<div>${arr?'':`<span class="jk">${esc(JSON.stringify(k))}</span>: `}${jv(v[k],d+1)}${i<keys.length-1?',':''}</div>`).join('')}</div>${arr?']':'}'}</details>`}
function jsonBox(v){let s;try{s=JSON.stringify(v,null,2)}catch(e){s=String(v)}if(s===undefined)s='null';
  const body=()=>`<div class="jbox">${jv(v)}</div>`;const cp=`<button class="lnk" data-cp="${reg.push(s)-1}">copy</button>`;
  if(s.length>50000)return cp+lz(`large JSON (${s.length.toLocaleString()} chars) — click to render`,body,false);
  return cp+body()}
function stBadge(st){return st==='pending'?'<span class="badge pend">⏳ pending</span>':st==='error'?'<span class="badge err">⛔ error</span>':'<span class="badge ok">✔ ok</span>'}
function toolUnit(t,inReply){const st=t.status||'pending';
  const jump=t.resultIn!=null?`<button class="lnk" data-jump="${t.resultIn}" data-to="resblk:${esc(t.id)}">→ result in #${t.resultIn}</button>`:'';
  const res=t.result?`<div class="tres ${st==='error'?'iserr':''}"><div class="rh">↩ result</div>${ctext(rtext(t.result.content))}</div>`:'<div class="tres none">no result yet</div>';
  return `<div class="tool"><div class="th" ${inReply?'data-call':'data-hcall'}="${esc(t.id)}">🔧 <b>${esc(t.name)}</b> ${stBadge(st)} ${jump}</div>${t.input!==undefined?`<div>${t._partial?ctext(t._partial):jsonBox(t.input)}</div>`:''}${res}</div>`}
function resBlock(b){const t=tm[b.tool_use_id];const err=!!b.is_error;
  const jump=t&&t.calledIn!=null?`<button class="lnk" data-jump="${t.calledIn}" data-to="call:${esc(b.tool_use_id)}">↑ call in #${t.calledIn}</button>`:'';
  return `<div class="tool res ${err?'iserr':''}" data-resblk="${esc(b.tool_use_id)}"><div class="th">↩ <b>tool result</b> ${t?esc(t.name):''} ${err?'<span class="badge err">⛔ error</span>':'<span class="badge ok">✔ ok</span>'} ${jump}</div>${ctext(rtext(b.content))}</div>`}
function rBlock(b,inReply){
  switch(b.type){
    case 'text':return ctext(b.text);
    case 'thinking':return lz('💭 thinking',()=>ctext(b.thinking||''),false);
    case 'redacted_thinking':return '<div class="dim">💭 [redacted thinking]</div>';
    case 'tool_use':return toolUnit(tm[b.id]?Object.assign({},tm[b.id],{_partial:b._partial}):{id:b.id,name:b.name,input:b.input,status:'pending',_partial:b._partial},inReply);
    case 'tool_result':return resBlock(b);
    case 'image':{const s=b.source||{};return `<div class="dim">🖼 image (${esc(s.media_type||s.type||'')}, ${(s.data||'').length.toLocaleString()} b64 chars)</div>`}
    default:return jsonBox(b)}}
function rMsg(m,i,isNew){const role=m.role;
  return `<div class="msg ${esc(role)} ${isNew?'new':''}"><div class="mh">${role==='user'?'👤 user':'🤖 assistant'} <span class="dim">#${i}</span>${isNew?' <span class="badge new">NEW</span>':''}</div>${blocksOf(m).map(b=>rBlock(b,false)).join('')}</div>`}
function lineDiff(a,b){let A=a.split('\n'),B=b.split('\n'),p=0;while(p<A.length&&p<B.length&&A[p]===B[p])p++;
  let e=0;while(e<A.length-p&&e<B.length-p&&A[A.length-1-e]===B[B.length-1-e])e++;A=A.slice(p,A.length-e);B=B.slice(p,B.length-e);
  if(A.length*B.length>2.5e6)return A.map(s=>['-',s]).concat(B.map(s=>['+',s]));
  const n=A.length,m=B.length,dp=Array.from({length:n+1},()=>new Uint32Array(m+1));
  for(let i=n-1;i>=0;i--)for(let j=m-1;j>=0;j--)dp[i][j]=A[i]===B[j]?dp[i+1][j+1]+1:Math.max(dp[i+1][j],dp[i][j+1]);
  const out=[];let i=0,j=0;while(i<n&&j<m){if(A[i]===B[j]){i++;j++}else if(dp[i+1][j]>=dp[i][j+1])out.push(['-',A[i++]]);else out.push(['+',B[j++]])}
  while(i<n)out.push(['-',A[i++]]);while(j<m)out.push(['+',B[j++]]);return out}
function dlHtml(a,b){const d=lineDiff(a,b).slice(0,400);return `<div class="dl">${d.map(([t,s])=>`<div class="${t==='+'?'a':'d'}">${t} ${esc(s)}</div>`).join('')||'<span class="dim">(whitespace only)</span>'}</div>`}
const PSKIP=new Set(['messages','system','tools']);
function paramsOf(r){const o={};for(const k in r)if(!PSKIP.has(k))o[k]=r[k];return o}

/* ---------- detail ---------- */
function secBegin(cls,title,open,render){return `<section class="sec ${cls}">${lz(title,render,open)}</section>`}
function renderDiff(ex){
  const th=ex.thread;const idx=th.items.indexOf(ex);const prev=idx>0?th.items[idx-1]:null;
  if(!prev)return `<section class="sec diff"><div class="body"><b>Δ since previous request:</b> <span class="badge">first request in this ${esc(ex.kind)} thread</span></div></section>`;
  const cur=ex.msgs,pm=prev.msgs;let prefix=cur.length>=pm.length;
  if(prefix)for(let i=0;i<pm.length;i++)if(JSON.stringify(cur[i])!==JSON.stringify(pm[i])){prefix=false;break}
  const sA=sysText(prev.req.system),sB=sysText(ex.req.system),sCh=sA!==sB;
  const tA=JSON.stringify(prev.req.tools||[]),tB=JSON.stringify(ex.req.tools||[]),tCh=tA!==tB;
  const pa=paramsOf(prev.req),pb=paramsOf(ex.req),pk=[...new Set([...Object.keys(pa),...Object.keys(pb)])].filter(k=>JSON.stringify(pa[k])!==JSON.stringify(pb[k]));
  const badges=[prefix?`<span class="badge ${cur.length>pm.length?'new':''}">+${cur.length-pm.length} messages</span>`:'<span class="badge err">history rewritten</span>',
    `<span class="badge">system ${sCh?'CHANGED':'unchanged'}</span>`,`<span class="badge">tools ${tCh?'CHANGED':'unchanged'}</span>`,`<span class="badge">params ${pk.length?'changed: '+esc(pk.join(', ')):'unchanged'}</span>`].join(' ');
  const detail=()=>{let h='';
    if(prefix&&cur.length>pm.length)h+='<h4>Added messages</h4>'+cur.slice(pm.length).map((m,i)=>rMsg(m,pm.length+i,false)).join('');
    if(!prefix)h+=`<p>Previous request had ${pm.length} messages, this one has ${cur.length}; the history is not a continuation (e.g. compaction).</p>`;
    if(sCh)h+='<h4>System prompt diff</h4>'+dlHtml(sA,sB);
    if(tCh){const ma=Object.fromEntries((prev.req.tools||[]).map(t=>[t.name,JSON.stringify(t)])),mb=Object.fromEntries((ex.req.tools||[]).map(t=>[t.name,JSON.stringify(t)]));
      const add=Object.keys(mb).filter(k=>!(k in ma)),rem=Object.keys(ma).filter(k=>!(k in mb)),chg=Object.keys(mb).filter(k=>k in ma&&ma[k]!==mb[k]);
      h+=`<h4>Tool changes</h4><div class="dl"><div class="a">+ added: ${esc(add.join(', ')||'none')}</div><div class="d">- removed: ${esc(rem.join(', ')||'none')}</div><div>~ modified: ${esc(chg.join(', ')||'none')}</div></div>`}
    if(pk.length)h+='<h4>Parameter changes</h4><div class="dl">'+pk.map(k=>`${esc(k)}: <span class="d">${esc(JSON.stringify(pa[k]))}</span> → <span class="a">${esc(JSON.stringify(pb[k]))}</span>`).join('<br>')+'</div>';
    return h||'<span class="dim">nothing else changed</span>'};
  return `<section class="sec diff"><div class="body"><b>Δ since #${prev.id}:</b> ${badges}</div>${lz('expand changes',detail,false)}</section>`}
function renderReq(ex){
  if(!ex.isMsg){return `<section class="sec"><div class="body"><b>Raw request</b> <span class="dim">(not a Messages API request; showing raw)</span>${ex.req?jsonBox(ex.req):ctext(ex.body||'(empty body)')}</div></section>`}
  const r=ex.req;let h='';
  const p=paramsOf(r);
  h+=`<section class="sec"><div class="body"><b>Parameters</b> <div class="params">${Object.entries(p).map(([k,v])=>{const s=typeof v==='object'?JSON.stringify(v):String(v);return `<span><b>${esc(k)}</b>=${esc(s.length>80?s.slice(0,80)+'…':s)}</span>`}).join('')}</div>${lz('request headers',()=>jsonBox(ex.reqHeaders),false)}</div></section>`;
  h+=renderDiff(ex);
  const sys=sysText(r.system);
  h+=secBegin('sys',`⚙ System instructions <span class="dim">(${sys.length.toLocaleString()} chars, ${sys?sys.split('\n').length:0} lines)</span>`,false,()=>sys?`<pre class="tx">${esc(sys)}</pre>`:'<span class="dim">none</span>');
  const tools=r.tools||[];
  h+=secBegin('tools',`🧰 Tool definitions <span class="dim">(${tools.length})</span>`,false,()=>tools.map(t=>lz(`<b>${esc(t.name||'?')}</b> <span class="dim">${esc(String(t.description||'').split('\n')[0].slice(0,140))}</span>`,()=>(t.description?ctext(t.description):'')+'<div class="dim">input schema</div>'+jsonBox(t.input_schema||t.parameters||t),false)).join('')||'<span class="dim">none</span>');
  const ns=ex.newStart,prior=ex.msgs.slice(0,ns),nw=ex.msgs.slice(ns);
  const newest=ex.id===Math.max(...[...X.keys()]);
  h+=secBegin('conv',`💬 Conversation so far <span class="dim">(${prior.length} messages)</span>`,newest&&prior.length>0&&prior.length<=6,()=>prior.map((m,i)=>rMsg(m,i,false)).join('')||'<span class="dim">none</span>');
  h+=secBegin('newm',`✨ New message <span class="dim">(${nw.length})</span>`,true,()=>nw.map((m,i)=>rMsg(m,ns+i,true)).join(''));
  return h}
function renderReply(ex){
  let h='';
  if(ex.error)h+=`<div class="errbanner">⛔ ${esc(ex.error)}</div>`;
  if(ex.rerr)h+=`<div class="errbanner">⛔ API error: ${esc(typeof ex.rerr==='string'?ex.rerr:(ex.rerr.type||'')+' '+(ex.rerr.message||JSON.stringify(ex.rerr)))}</div>`;
  else if(ex.status!=null&&ex.status>=400&&!ex.error)h+=`<div class="errbanner">⛔ HTTP ${ex.status}</div>`;
  let body='';
  if(ex.message){const m=ex.message,u=m.usage||{};
    body+=(m.content||[]).map(b=>rBlock(b,true)).join('')||'<span class="dim">(no content yet)</span>';
    if(!ex.done)body+='<div class="badge pend">⏳ streaming…</div>';
    body+=`<div class="params" style="margin-top:6px"><span>stop: <b>${esc(m.stop_reason||'—')}</b></span><span>in: ${u.input_tokens??'—'}</span><span>out: ${u.output_tokens??'—'}</span><span>cache read: ${u.cache_read_input_tokens??0}</span><span>cache write: ${u.cache_creation_input_tokens??0}</span><span>TTFB: ${ex.tHead?ex.tHead-ex.t0+' ms':'—'}</span><span>total: ${ex.tEnd?ex.tEnd-ex.t0+' ms':'—'}</span></div>`}
  else if(ex.resp){body+=ex.json?jsonBox(ex.json):ctext(ex.resp)}
  else body+=ex.done?'<span class="dim">(empty response)</span>':'<span class="badge pend">⏳ waiting for response…</span>';
  if(ex.resp&&(ex.sse||ex.message))body+=lz(ex.sse?'raw event stream':'raw response',()=>`<pre class="tx">${esc(ex.resp)}</pre>`,false);
  return h+secBegin('reply',`🤖 Reply ${ex.done?'':'<span class="badge pend">in progress</span>'}`,true,()=>body)}
function keyArg(inp){if(!inp||typeof inp!=='object')return'';const k=['command','file_path','path','pattern','url','query','description','prompt'].find(k=>typeof inp[k]==='string')||Object.keys(inp).find(k=>typeof inp[k]==='string');if(!k)return'';const v=inp[k].replace(/\s+/g,' ');return v.length>70?v.slice(0,70)+'…':v}
function critical(ex){
  const L=[];
  if(ex.error)L.push(['ce','⛔ '+esc(ex.error)]);
  if(ex.rerr)L.push(['ce','⛔ API error: '+esc(typeof ex.rerr==='string'?ex.rerr:(ex.rerr.type||'')+' '+(ex.rerr.message||''))]);
  else if(ex.status!=null&&ex.status>=400&&!ex.error)L.push(['ce','⛔ HTTP '+ex.status]);
  if(ex.isMsg){const nw=ex.msgs.slice(ex.newStart);let res=0,bad=0;
    nw.forEach(m=>blocksOf(m).forEach(b=>{if(b&&b.type==='tool_result'){res++;if(b.is_error)bad++}}));
    if(bad)L.push(['ce','⛔ '+bad+' tool result'+(bad>1?'s':'')+' flagged is_error']);
    if(res)L.push(['','📥 returned '+res+' tool result'+(res>1?'s':'')+(bad?' ('+bad+' error)':'')]);}
  const m=ex.message;
  if(m){const uses=(m.content||[]).filter(b=>b&&b.type==='tool_use');
    uses.forEach(b=>L.push(['','🔧 called <b>'+esc(b.name||'?')+'</b>'+(keyArg(b.input)?' <span class="dim">'+esc(keyArg(b.input))+'</span>':'')]));
    if(!uses.length){const t=(m.content||[]).filter(b=>b&&b.type==='text').map(b=>b.text).join(' ').replace(/\s+/g,' ').trim();
      if(t)L.push(['','💬 '+esc(t.length>140?t.slice(0,140)+'…':t)])}
    if(ex.done&&m.stop_reason&&!uses.length&&m.stop_reason!=='end_turn')L.push(['','stop: '+esc(m.stop_reason)])}
  else if(!ex.done&&!ex.error)L.push(['','⏳ waiting for reply…']);
  if(!L.length)return'';
  return `<div class="crit"><b>Key points</b>${L.map(([c,t])=>`<div class="${c}">${t}</div>`).join('')}</div>`}
function renderHead(ex){
  const mdl=(ex.req&&ex.req.model)||'';
  return `<h2 class="dh">#${ex.id} ${esc(ex.method)} ${esc(ex.path)} ${ex.kind&&ex.kind!=='raw'?`<span class="badge ${ex.kind}">${ex.kind}</span>`:'<span class="badge">raw</span>'} ${ex.anyErr?'<span class="badge err">⛔ ERROR</span>':''}</h2>
  <div class="params" style="margin-bottom:10px"><span>${new Date(ex.t0).toLocaleTimeString()}</span>${mdl?`<span>${esc(mdl)}</span>`:''}<span>status ${ex.status??'…'}</span><span>${ex.tEnd?ex.tEnd-ex.t0+' ms':'in progress'}</span></div>
  ${critical(ex)}
  ${ex.toolErr?'<div class="errbanner">⛔ This request carries a tool result flagged is_error</div>':''}`}
function showDetail(full){
  const ex=X.get(sel);const d=$('#detail');
  if(!ex){d.innerHTML='<p class="dim">Select an exchange.</p>';return}
  if(full||!$('#dReply')){reg=[];d.innerHTML=`<div id="dHead"></div><div id="dReq"></div><div id="dReply"></div>`;$('#dHead').innerHTML=renderHead(ex);$('#dReq').innerHTML=renderReq(ex);hydrate($('#dReq'))}
  else $('#dHead').innerHTML=renderHead(ex);
  $('#dReply').innerHTML=renderReply(ex);hydrate($('#dReply'));
  if(full)d.scrollTop=0}

/* ---------- list ---------- */
function visible(ex){
  if(filt.errors&&!ex.anyErr)return false;
  if(filt.kind!=='all'&&ex.kind!==filt.kind)return false;
  if(filt.tools.size&&!ex.toolNames.some(n=>filt.tools.has(n)))return false;
  if(filt.q&&!ex._hay.includes(filt.q))return false;return true}
function renderList(all){
  const rows=all.filter(visible).reverse();
  $('#list').innerHTML=rows.map(ex=>{
    const u=ex.message&&ex.message.usage;const st=ex.status==null?(ex.error?'ERR':'…'):ex.status;
    return `<div class="row ${ex.id===sel?'sel':''} ${ex.anyErr?'err':''}" data-id="${ex.id}"><span class="mono">#${ex.id}</span><span class="t">${new Date(ex.t0).toLocaleTimeString()}</span>
    <span class="badge ${ex.kind}">${ex.kind==='side'?'side call':ex.kind}</span>${ex.anyErr?'<span class="badge err">⛔ error</span>':''}
    <span class="badge ${ex.done?(ex.anyErr?'err':'ok'):'pend'}">${ex.done?st:'⏳ '+st}</span>
    <span class="t">${ex.tEnd?(ex.tEnd-ex.t0)+' ms':''}</span>${u?`<span class="t tok" title="input (incl. cache) / output tokens">🪙 ${((u.input_tokens||0)+(u.cache_read_input_tokens||0)+(u.cache_creation_input_tokens||0))} in / ${(u.output_tokens||0)} out</span>`:''}
    <span class="t">${esc(String(ex.method)+' '+ex.path.split('?')[0])}</span>${ex.toolNames.length?`<span class="tl">🔧 ${esc(ex.toolNames.join(', '))}</span>`:''}</div>`}).join('')||'<p class="dim" style="padding:10px">No matching exchanges.</p>'}

/* ---------- update loop ---------- */
let lastSel=null;
function refresh(){
  timer=null;const all=analyze();
  const newest=all.length?all[all.length-1].id:null;
  if(!paused){
    if(!userPicked||!X.has(sel))sel=newest;
    renderList(all);
    if(sel!==lastSel){lastSel=sel;showDetail(true)}else showDetail(false)}
}
function schedule(){if(!timer)timer=setTimeout(refresh,80)}
function ingest(ev){
  if(ev.type==='snapshot'){X=new Map(ev.exchanges.map(e=>[e.id,e]));lastSel=null;pendingN=0;updPause();schedule();return}
  if(ev.type==='start'){X.set(ev.ex.id,ev.ex);if(paused){pendingN++;updPause()}}
  else{const ex=X.get(ev.id);if(!ex)return;
    if(ev.type==='head'){ex.status=ev.status;ex.respHeaders=ev.headers;ex.tHead=ev.t}
    else if(ev.type==='chunk')ex.resp+=ev.text;
    else if(ev.type==='done'){ex.done=true;ex.tEnd=ev.t}
    else if(ev.type==='error'){ex.error=ev.message;if(ev.status!=null)ex.status=ev.status;ex.done=true;ex.tEnd=ev.t}}
  schedule()}
function updPause(){$('#pause').textContent=paused?'▶ Resume':'⏸ Pause';$('#pcount').textContent=paused&&pendingN?`${pendingN} new while paused`:paused?'paused':''}
function connect(){
  const es=new EventSource('/events');
  es.onopen=()=>{$('#conn').textContent='● connected';$('#conn').className='ok'};
  es.onmessage=e=>{try{ingest(JSON.parse(e.data))}catch(err){console.error(err)}};
  es.onerror=()=>{$('#conn').textContent='↻ reconnecting…';$('#conn').className='bad'}}

/* ---------- events ---------- */
function openAncestors(el){for(let p=el.parentElement;p;p=p.parentElement)if(p.tagName==='DETAILS'&&!p.open){p.open=true}}
function jumpTo(id,to){
  userPicked=true;sel=id;lastSel=id;renderList(analyze());showDetail(true);
  const [kind,tid]=to.split(/:(.*)/s);const attr=kind==='call'?'data-call':'data-resblk';
  hydrate($('#detail'));
  let el=$('#detail').querySelector(`[${attr}="${CSS.escape(tid)}"]`);
  if(el){openAncestors(el);hydrate($('#detail'));el.scrollIntoView({block:'center'});el.classList.add('flash');setTimeout(()=>el.classList.remove('flash'),1500)}}
document.addEventListener('click',e=>{
  const t=e.target;
  if(t.dataset.more!==undefined){const pre=t.previousElementSibling;if(t.dataset.open){pre.textContent=pre.dataset.pv;delete t.dataset.open;t.textContent=t.dataset.lbl}else{pre.dataset.pv=pre.textContent;t.dataset.lbl=t.textContent;pre.textContent=reg[t.dataset.more];t.dataset.open=1;t.textContent='show less'}return}
  if(t.dataset.cp!==undefined){navigator.clipboard&&navigator.clipboard.writeText(reg[t.dataset.cp]).then(()=>{t.textContent='copied';setTimeout(()=>t.textContent='copy',1000)});return}
  if(t.dataset.jump!==undefined){jumpTo(+t.dataset.jump,t.dataset.to);return}
  if(t.dataset.tool!==undefined){const n=t.dataset.tool;filt.tools.has(n)?filt.tools.delete(n):filt.tools.add(n);t.classList.toggle('on');renderList(analyze());return}
  const row=t.closest('.row');if(row){userPicked=true;sel=+row.dataset.id;lastSel=sel;renderList(analyze());showDetail(true)}});
$('#pause').onclick=()=>{paused=!paused;if(!paused)pendingN=0;updPause();if(!paused)refresh()};
$('#q').oninput=e=>{filt.q=e.target.value.toLowerCase();renderList(analyze())};
$('#kind').onchange=e=>{filt.kind=e.target.value;renderList(analyze())};
$('#onlyErr').onchange=e=>{filt.errors=e.target.checked;renderList(analyze())};
connect();
</script></body></html>
"""


def main() -> None:
    global UPSTREAM, HUB
    parser = argparse.ArgumentParser(description="Print and forward an agent's API traffic, with a live dashboard.")
    parser.add_argument("--port", type=int, default=8041)
    parser.add_argument("--dashboard-port", type=int, default=8042, help="port for the browser dashboard")
    parser.add_argument("--max-exchanges", type=int, default=500, help="exchanges kept in memory (oldest dropped)")
    parser.add_argument("--upstream", default=UPSTREAM, help="where to forward (default: the Anthropic API)")
    args = parser.parse_args()
    UPSTREAM = args.upstream.rstrip("/")
    HUB = Hub(args.max_exchanges)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Tap)
    server.daemon_threads = True
    try:
        dash = ThreadingHTTPServer(("127.0.0.1", args.dashboard_port), Dash)
        dash.daemon_threads = True
        threading.Thread(target=dash.serve_forever, daemon=True).start()
        print(f"dashboard on http://localhost:{args.dashboard_port}")
    except OSError as e:
        print(f"warning: dashboard disabled, cannot bind port {args.dashboard_port}: {e}")
    print(f"proxy listening on http://localhost:{args.port}  ->  {UPSTREAM}")
    print(f"run the agent with:  ANTHROPIC_BASE_URL=http://localhost:{args.port} claude")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
