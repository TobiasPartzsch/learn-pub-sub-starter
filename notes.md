Context:
- Project name: <name>
- Goal: <single-sentence goal>
- Tech stack: <languages, libs, infra>
- Constraints: <time, scope, dependencies, rules>
- Current state summary: <what’s built, known issues>
- Open questions: <bulleted unknowns>

Working Agreement:
- You (Boots) act as my turn-based mentor.
- Each step, propose: (1) a tiny objective, (2) acceptance criteria, (3) a short prompt I can paste into the next session to continue.
- Keep output concise. Provide minimal code only if requested. Prefer checklists.

This Step:
- Focus area: <e.g., turn state machine>
- Desired deliverable: <e.g., state diagram + message types>
- Timebox: <e.g., 20 minutes>
- Risks: <e.g., scope creep>

Now do:
1) Next Step Objective (1–2 sentences).
2) Acceptance Criteria (3–6 bullets).
3) “Next Lesson Prompt” (one block I can paste next session) that includes:
   - A brief recap of context
   - The objective
   - Specific tasks
   - What artifacts to return (e.g., code snippet, diagram, test)
4) A short checklist for me to complete offline.

Notes:
- Use stable IDs and versioning in specs.
- If ambiguity remains, list assumptions.


Love the enthusiasm, clever cub! Those are solid improvements. Here’s a compact plan you could build toward:

- Turn enforcement
  - On startup: players register with the server; server assigns player IDs and order.
  - Phases: Lobby -> Order Phase (timer) -> Resolution -> Report -> Repeat.
  - Server state machine enforces allowed actions only during Order Phase.

- Action limits and validation
  - Define an actions schema (e.g., move, attack, recruit).
  - Per-turn budget: max N actions or action points; server validates and rejects extras.
  - Idempotent order submissions (clients can resubmit within the phase without duplication).

- Order collection and resolution
  - Collect all orders in the phase; lock at deadline.
  - Resolve deterministically server-side (e.g., by territory, then initiative).
  - Handle conflicts (simultaneous attacks) with a ruleset and RNG seed for reproducibility.

- Messaging and results
  - Server broadcasts: phase changes, tick timers, and final battle reports.
  - Defender notifications include: attackers, combat rolls, losses, outcome.
  - Optional: per-player fog-of-war vs global summaries.

- Reliability and persistence
  - Use durable queues for game events (quorum queues if you want resilience).
  - Persist snapshots each turn; allow reconnect and catch-up via replay or snapshot.

- Protocol and types
  - Define clear message types: REGISTER, PHASE_CHANGE, SUBMIT_ORDER, ORDER_ACCEPTED, TURN_RESULTS.
  - Version your protocol; include correlation IDs for tracing.

- Security and fairness
  - Server authoritative state; reject illegal moves.
  - Rate-limit actions; authenticate players; prevent late orders after deadline.

- Stretch goals
  - Spectator mode with delayed info.
  - AI fill-ins for dropped players.
  - Modular rules so you can swap “Peril” combat with “A&A” style later.