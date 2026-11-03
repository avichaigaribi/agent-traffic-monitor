# SPEC: Agent Traffic Dashboard

Status: **DRAFT v0.2**. All 10 decisions in section 9 are resolved. Section 4 and the requirements still need a pass to reflect them before implementation starts.

## 1. Purpose

A browser dashboard that shows, live, the API traffic between an agent harness (Claude Code) and the model. The existing `proxy.py` already sits between them and prints every request and response to the terminal. The dashboard turns that raw stream into a readable, structured view so a person can follow what the agent is doing at a glance, even when messages pile up quickly.

Non-goals (v1):
- Modifying, blocking, or replaying traffic. The proxy stays a passive tap.
- Persisting history across proxy restarts (see Open Decisions).
- Multi-user access or authentication; this is a local developer tool.

## 2. Users and Usage

- **User:** a developer inspecting what their agent sends and receives.
- **Flow:**
  1. Start the proxy (`python3 proxy.py`).
  2. Run the agent with `ANTHROPIC_BASE_URL=http://localhost:8041 claude`.
  3. Open the dashboard URL in a browser. It updates live with no manual refresh.

## 3. Functional Requirements

### 3.1 Live feed
- FR-1: Every request the harness sends and every reply appears as it happens.
- FR-2: Entries are ordered **newest first**.
- FR-3: Each request/reply pair is one **exchange** (a card/row) showing: timestamp, sequence number, HTTP method and path, model, status code, duration, and token usage when available.
- FR-4: A reply that is still streaming is shown as in progress and fills in incrementally. When it ends, the exchange is marked complete.
- FR-5: The feed shows a connection indicator (connected, reconnecting) and a pause/resume control so a user can read without the list shifting. Entries that arrive while paused are counted and shown on resume.

### 3.2 Request breakdown
Each request is split into clearly separated sections, each with its own header and a collapse toggle:
- FR-6: **System instructions**, shown in full (never truncated), collapsible.
- FR-7: **Tool definitions**: a list with each tool's name and description; expanding a tool shows its JSON schema.
- FR-8: **Conversation so far**: prior messages in order, labeled by role (user / assistant), with content blocks (text, tool_use, tool_result, thinking, images) each rendered distinctly.
- FR-9: **New message**: the last message(s) added since the previous request, visually highlighted as the new content of this turn.
- FR-10: Request parameters (model, max_tokens, temperature, stream, etc.) in a compact summary.

### 3.3 Tool calls and results
- FR-11: Each `tool_use` block is paired with its `tool_result` by `tool_use_id`. The result arrives in a later request, so the pairing is resolved as that request is received.
- FR-12: A tool call is displayed as one unit: tool name, input arguments (formatted JSON), and the result beside or directly under it.
- FR-13: A tool call with no result yet is marked **pending**. Results flagged `is_error` are marked as errors.
- FR-14: From a tool call in a reply, the user can jump to its result in the later request, and the reverse.

### 3.4 Reply
- FR-15: The reply shows the model's text, any thinking blocks (collapsed by default), and the tool calls it requests.
- FR-16: Reply metadata: stop reason, input/output/cache token counts, and latency (time to first byte and total).
- FR-17: Streamed replies (SSE) are reassembled into a final message from the event stream; the raw event stream is available behind a toggle.

### 3.5 Insight features
- FR-18: **Diff since previous request**: for each exchange, show what changed relative to the previous request in the same conversation (messages added, system prompt or tool list changes, parameter changes). Unchanged sections are summarized as "unchanged".
- FR-19: **Filter by tool**: select one or more tool names to show only exchanges that call or return results for those tools.
- FR-20: **Error highlighting**: non-2xx responses, stream errors, proxy upstream failures (502), and error tool results are marked with a strong visual treatment (color plus icon, never color alone) and can be filtered to "errors only".
- FR-21: **Text search** across all captured content.
- FR-22: Header summary bar: total exchanges, error count, total tokens, and tools used with counts.

### 3.6 Formatting and readability
- FR-23: Long text (system prompt, tool results, messages) is **collapsible**. It shows a short preview with a "show more" control and a character/line count. Defaults: system prompt and tool definitions collapsed to a one-line header, newest exchange expanded.
- FR-24: JSON is pretty-printed, syntax highlighted, with collapsible nodes and a copy button.
- FR-25: Sections within a request are visually distinct (headers, left border color, or background per section type) and the same color/icon scheme is used consistently for system, tools, user, assistant, tool call, and tool result.
- FR-26: Monospace rendering for code and tool output; whitespace and newlines preserved.
- FR-27: Secrets are masked in the display the same way the proxy does today (`authorization`, `x-api-key` truncated).
- FR-28: Light and dark theme following the OS setting. Usable at laptop width; large payloads must not cause horizontal page scroll.

## 4. Architecture (proposed)

```
 claude (harness) --> proxy.py :8041 --> api.anthropic.com
                         |
                         | capture events
                         v
                  dashboard server --(live push)--> browser UI
```

- The proxy is extended (or wrapped) to emit structured **capture events** in addition to its terminal output: `request_started`, `response_headers`, `response_chunk`, `response_complete`, `error`.
- A small server exposes (a) the dashboard static page, (b) a live push channel to the browser, and (c) a JSON endpoint returning history so a freshly opened page can catch up.
- The browser holds exchanges in memory, reassembles streams, pairs tool calls with results, and computes diffs.
- Constraint carried over from the proxy: the forwarded traffic must remain **byte-for-byte unchanged**, and capture must never slow or break the agent's request path. If the dashboard is closed or fails, the proxy keeps working.

## 5. Data Model (sketch)

```
Exchange {
  id, seq, startedAt, durationMs, status
  request:  { method, path, headers (masked), params, system, tools[], messages[] }
  response: { status, headers, message{ content[], stop_reason, usage }, rawStream?, error? }
  conversationId   // inferred, for diffing and grouping
  newMessageIndex  // first message not present in the previous request
}

ToolCall {
  toolUseId, name, input
  calledIn:  exchangeId   // reply that requested it
  resultIn:  exchangeId?  // later request carrying the tool_result
  result?, isError?, status: pending | ok | error
}
```

## 6. Non-Functional Requirements

- NFR-1: Latency from proxy event to on-screen update under 250 ms on localhost.
- NFR-2: Remains responsive with at least 500 exchanges and individual payloads of several hundred KB (e.g. a 200 KB request). Large sections render lazily.
- NFR-3: Binds to localhost only. No data leaves the machine.
- NFR-4: Runs with minimal setup, matching the proxy's "nothing to install" spirit where practical.
- NFR-5: Captured traffic contains prompts, file contents, and tool output, so it is treated as sensitive. Nothing is written to disk unless the user opts in.
- NFR-6: The repository includes a `.gitignore` that ignores `__pycache__/` (and other Python bytecode) so `git status` stays clean.

## 7. Out of Scope for v1

- Editing and replaying requests.
- Support for non-Anthropic API formats (e.g. Codex). The proxy can forward them, but the parsed views assume the Messages API shape.
- Cost estimation in currency.
- Exporting or sharing sessions (candidate for v2).

## 8. Acceptance Criteria

1. With the proxy running and an agent session active, a newly sent request appears in the open dashboard within a second, without refresh.
2. The system prompt is viewable in full and is collapsed by default.
3. A tool call in one reply and its result in the next request appear together as a single unit; an unanswered call shows as pending.
4. A streamed reply renders as readable text and tool calls, not raw SSE, with a toggle to see the raw stream.
5. Selecting a tool in the filter hides exchanges that do not involve it.
6. A failed request (e.g. 4xx/5xx or an unreachable upstream) is unmistakable at a glance and can be isolated with the errors filter.
7. Opening the dashboard after traffic has begun shows the earlier exchanges.
8. Closing or crashing the dashboard does not interrupt the agent.

## 9. Open Decisions

| # | Topic | Status |
|---|-------|--------|
| 1 | Tech stack (backend + frontend) | **decided: Python stdlib backend (in `proxy.py`) + static HTML/JS page with a few small libraries loaded from a CDN, no build step**; the page needs internet on first load unless libraries are later vendored |
| 2 | Integration: extend `proxy.py` vs. separate process | **decided: extend `proxy.py`**; one process and one command serve both the proxy and the dashboard |
| 3 | Live transport to browser (SSE vs. WebSocket vs. polling) | **decided: Server-Sent Events**; plus a JSON history endpoint for catch-up on page load |
| 4 | History: in-memory only vs. persisted (file/SQLite) | **decided: in-memory only**, with a cap on retained exchanges (exact limit in #8); nothing written to disk |
| 5 | Conversation grouping (main agent vs. subagents / side requests) | **decided: auto-detect threads, show all in one feed**; each exchange is labeled main / subagent / side call, with a filter per kind; diffs compare within a thread |
| 6 | Diff granularity and presentation | **decided: compact change summary**; a badge row per exchange (e.g. "+2 messages, system unchanged, tools unchanged") expandable to the added messages, with a text diff only for sections that actually changed |
| 7 | Layout (list + detail pane vs. single expandable stream) | **decided: list + detail pane**; left is a compact newest-first list (one line per exchange: time, thread label, tools, status, error marker, and that exchange's own token usage as input and output tokens, e.g. `1,234 in / 56 out`, so per-task token cost is visible at a glance without opening the detail pane), right is the selected exchange in full |
| 8 | Handling of very large payloads and retention limits | **decided: cap by count, lazy render**; keep the most recent 500 exchanges by default (adjustable by flag), drop the oldest, and render large sections only when expanded |
| 9 | Redaction policy beyond auth headers | **decided: auth headers only** (`authorization`, `x-api-key`, as `proxy.py` does today); message bodies and tool output are shown in full |
| 10 | Non-Anthropic upstream support | **decided: Anthropic only, raw fallback**; Messages API traffic gets the parsed views, any other format still appears in the feed as raw formatted JSON/text with no tool pairing or diff |
