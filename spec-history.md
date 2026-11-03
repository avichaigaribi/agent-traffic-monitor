# SPEC.md History

Three commits have touched `SPEC.md`, oldest first.

## 1. `0d15cca` (2026-11-01): Initial SPEC.md draft from agent discussion

- **What changed:** Created the spec as **DRAFT v0.2**: purpose, users, functional requirements FR-1 to FR-28, a proposed architecture, a data model, NFR-1 to NFR-5, scope limits, acceptance criteria, and 10 resolved design decisions.
- **Reason:** Capture the agreed design for the traffic dashboard before building it. The initial build was done from this spec ("rebuilding from scratch").

---

## ▶ SWITCH POINT: rebuilding from scratch → editing the existing code

The next commit (`525502d`) is where we changed mode. The initial build works on live traffic, so from here on we **edit the existing `proxy.py`** instead of rebuilding it.

---

## 2. `525502d` (2026-11-03): Update SPEC.md: switch to editing mode, add per-exchange tokens and .gitignore

- **What changed:**
  - Decision 7 (layout): each list-row summary now includes that exchange's own input and output tokens.
  - New NFR-6: the repo must have a `.gitignore` that ignores `__pycache__/`.
- **Reason (from the commit message):** The initial build works, so we are switching to editing mode. The two features make per-task token cost visible at a glance, and clean up `git status`.

## 3. `713d4d0` (2026-11-03): Finalize SPEC.md as v1.0: add critical summary requirement (FR-29)

- **What changed:**
  - New FR-29: a concise critical summary at the top of each exchange's detail view. It shows errors first, then the tools actually called, the tool results returned, and the main action.
  - Status changed from DRAFT v0.2 to **FINAL v1.0**.
  - Removed "(proposed)" from Section 4.
  - The token format example now matches the code exactly: `🪙 <in> in / <out> out`.
- **Reason (from the commit message):** Extract signal from noise, and finalize the spec so it reflects the built system.

---

## Findings in the traffic

The traffic showed large interleaved requests which required careful conversation grouping (threads). We also noticed untracked cache files cluttering the environment, and realized that seeing total token cost wasn't enough; we needed per-task token visibility to track the agent's efficiency during specific tool calls. Finally, we added a critical summary to each exchange to quickly extract signal from noise.
