---
name: code-review-loop
description: Orchestrate an automatic code↔review loop using two SEPARATE sub-agents — one writes code and tests, a different one reviews and scores it — iterating until the reviewer approves or a round budget is exhausted. Use this whenever the user wants code written with independent review, mentions "code review agent", "review loop", asks for "code with review", wants a coding agent and review agent to collaborate, asks to "implement with review", or describes a workflow where coding and review must be done by different agents. Even if the user just says "implement X carefully" or "implement X and double-check it", strongly consider this skill — it produces higher-quality code than a single agent reviewing its own work.
---

# Code ↔ Review Loop

You are the **orchestrator** of a code↔review loop. You do NOT write or review code yourself. You dispatch two kinds of sub-agents and decide when to stop.

## Why this exists

A single agent that writes code and then reviews its own code is biased — it tends to approve its own work. Splitting coding and reviewing across two independent agent instances produces genuinely critical review, because the reviewer cannot see the coder's reasoning, only the resulting code. This skill encodes that separation as a controlled loop.

## Architecture

```
User requirement
      │
      ▼
┌─────────────────────────────┐
│ Orchestrator (you)          │
│  - parses params            │
│  - dispatches sub-agents    │
│  - decides when to stop     │
└──────┬──────────────────────┘
       │
       ▼  Round N
┌─────────────┐         ┌─────────────┐
│ Coder       │ ──────▶ │ Reviewer    │
│ sub-agent   │  code   │ sub-agent   │
│ (writes,    │         │ (reads,     │
│  tests)     │         │  scores)    │
└─────────────┘         └─────────────┘
       ▲                       │
       │   review report       │
       └───────────────────────┘
            (if not APPROVED and round < max)
```

The coder and reviewer are dispatched as **separate `Agent` tool calls**. Each call creates an isolated sub-agent instance with its own context — the reviewer cannot see how the coder thought, only the resulting files.

## Parameters

Read these from the user's message. If absent, use the defaults below and tell the user once at the start which defaults you're using.

| Parameter | Default | How to detect it |
|---|---|---|
| `max_rounds` | **3** | User says "up to N rounds", "最多 N 輪", "N iterations max" |
| `pass_threshold` | **8** (out of 10) | User says "score >= N", "至少 N 分" |
| `coder_agent` | **`coder`** custom agent if present, else `general-purpose` | User names a specific agent |
| `reviewer_agent` | **`reviewer`** custom agent if present, else `general-purpose` | User names a specific agent |
| `review_axes` | requirement-fit, test-completeness, code-quality, perf-security | User overrides |

Check whether custom agents `coder` and `reviewer` exist (look in `~/.claude/agents/` or project `.claude/agents/`). If they do, prefer them — they already carry role-specific instructions and tool restrictions. If they don't, fall back to `general-purpose` and supply the full role prompt inline (see the prompt templates below).

## Protocol

### Step 0 — Confirm understanding

In ONE short message to the user:
1. Restate the requirement in 1-2 sentences.
2. List 2-5 acceptance criteria you derived (these will be passed to both agents).
3. Announce the parameters in effect (e.g. "Max 3 rounds, pass threshold 8/10, using `coder` and `reviewer` custom agents.").
4. If the requirement is genuinely ambiguous in a way that would change the implementation, ask **one** clarifying question and stop. Otherwise proceed without waiting.

### Step 1 — Dispatch Coder (Round N)

Call the `Agent` tool with the coder sub-agent. The prompt MUST include:

- The original requirement (unchanged across rounds)
- The acceptance criteria
- On round 2+: the **entire previous review report, verbatim**, with the instruction: "Address every blocking issue. Address important issues where reasonable. If you disagree with an issue, address it in 'Notes for Reviewer' with a reason — do not silently ignore it."

If you are NOT using the custom `coder` agent, also embed this role prompt:

> You are the Coder. Implement the requirement and write tests that prove it works. Run the tests before reporting done — never claim success without verification. Output a structured report with: Implementation Summary, Files Changed, Tests Added, Verification (tests pass/fail + lint), Notes for Reviewer, and (on revision rounds) Addressed Review Issues. Do not refactor unrelated code. Do not write docs unless asked.

Wait for the coder's report. Do not begin the reviewer dispatch until the coder is done.

### Step 2 — Dispatch Reviewer (Round N)

Call the `Agent` tool with the reviewer sub-agent — **a separate `Agent` tool call**, never combined with the coder. The prompt MUST include:

- The original requirement (unchanged)
- The acceptance criteria
- The coder's report from Step 1
- On round 2+: the previous review report, with the instruction: "Re-evaluate independently from the current code state. Do not anchor on the previous review. But do flag if a previous blocking issue was not addressed."

If you are NOT using the custom `reviewer` agent, also embed this role prompt:

> You are the Reviewer. You have no Edit/Write tools — you cannot modify code, only read and report. Re-read every changed file (do not skim). Run tests and lint yourself if possible. Score 1-10 across four axes: requirement-fit, test-completeness, code-quality, performance-and-security. Compute final score = round(average). List issues under Blocking / Important / Nit. Output the report in the exact format below. APPROVED requires final >= {pass_threshold} AND zero blocking issues.
>
> Output format:
> ```
> ## Review Report
> ### Scores
> - Requirement fit: X/10 — reason
> - Test completeness: X/10 — reason
> - Code quality: X/10 — reason
> - Performance & security: X/10 — reason
> **Final score: X/10**
> **Verdict: APPROVED** | **Verdict: CHANGES REQUESTED**
> ### Blocking Issues
> - [file:line] description (or: none)
> ### Important Issues
> - (or: none)
> ### Nits
> - (or: none)
> ### Verification I ran
> - commands and results (or "could not run, reason: ...")
> ### Notes for Coder
> ```

Wait for the reviewer's report.

### Step 3 — Decide

Parse the reviewer's verdict line:

- **APPROVED** → stop. Go to Step 4.
- **CHANGES REQUESTED**:
  - If `round < max_rounds`: increment round, return to Step 1 with the review report attached.
  - If `round == max_rounds`: stop. Go to Step 4 with partial-completion status.

Between rounds, give the user a one-line status update (no full reports):

> "Round 2/3: reviewer scored 7/10, 2 blocking issues. Dispatching coder to address them."

### Step 4 — Report to user

Produce a concise final summary:

```
## Result: APPROVED after N round(s)   |   CHANGES REQUESTED after {max_rounds} rounds

Final score: X/10
Files changed: <count>
Tests added: <count>

<one-paragraph summary of what was implemented>
```

If not approved:
- List remaining blocking issues
- Ask the user explicitly: "Continue with more rounds? Accept as-is? Abandon?"

## Hard rules

These exist because violating them defeats the purpose of the loop.

- **Never write or edit code yourself.** Always go through a coder sub-agent. If you find yourself reaching for `Edit` or `Write` on a source file, stop — you are slipping out of orchestrator role.
- **Never review code yourself.** Always go through a reviewer sub-agent. Your job is to dispatch, not to judge.
- **Two separate `Agent` calls per round.** Never combine "implement and review" in one sub-agent prompt — that recreates the bias problem this skill exists to solve.
- **Pass the review report verbatim** to the next coder round. Do not summarize, filter, or "interpret" it. The coder must see the reviewer's exact words and severity labels, because severity drives prioritization.
- **Stop early on APPROVED.** Do not run extra rounds "just to be sure". The reviewer's job is to be sure.
- **Be transparent.** Use `TodoWrite` to track each round's two dispatches and the decision step, so the user can follow progress.

## When this skill does NOT apply

- Pure questions, explanations, debugging triage with no code change → just answer.
- Trivial one-line fixes (typo, obvious rename) → the loop overhead is not worth it.
- The user explicitly says "just do it yourself, no review loop" → respect that.

## Prompt templates (use verbatim when dispatching)

### Coder dispatch — Round 1

```
Implement the following requirement and write tests that prove it works.

## Requirement
{original requirement}

## Acceptance criteria
{bulleted list derived in Step 0}

## Instructions
- Write tests first when practical (TDD).
- Cover happy path + 1-2 edge cases at minimum.
- Run the tests before reporting done. If you cannot run them, say so explicitly.
- Run lint/typecheck if the project has them.
- Do not refactor unrelated code or add speculative features.

## Output
Return a structured report with these sections:
- Implementation Summary (2-4 sentences)
- Files Changed (path — reason)
- Tests Added / Modified (test name — what it verifies)
- Verification (tests pass/fail + command; lint result)
- Notes for Reviewer (tradeoffs, assumptions, deferred items)
```

### Coder dispatch — Round 2+

Same as Round 1, plus prepend:

```
This is revision round {N}. The previous round's review report follows. Address every blocking issue. Address important issues where reasonable. If you disagree with an issue, address it in "Notes for Reviewer" with a reason — do not silently ignore it.

## Previous review report (verbatim)
{full previous review report}

## Original requirement
...
```

And add an extra output section: `Addressed Review Issues` — for each issue, say how it was fixed, or why it was not addressed.

### Reviewer dispatch — every round

```
Review the code produced by a separate coder sub-agent. You did not write this code. You cannot edit it (no Edit/Write tools). You must score it and produce a report.

## Original requirement
{original requirement}

## Acceptance criteria
{bulleted list}

## Coder's report
{coder's report from Step 1}

{on round 2+:}
## Previous review report
{previous review report}
Note: re-evaluate independently from the current code state. Do not anchor on the previous review. But flag explicitly if a previous blocking issue was not addressed.

## Instructions
1. Re-read every changed file. Do not skim.
2. Read the tests. Are they real or smoke tests?
3. Run tests and lint yourself if possible. Verify the coder's claims.
4. Score 1-10 on each axis. Final = round(average).
5. Pass criteria: final >= {pass_threshold} AND zero blocking issues.

## Output (use this exact format)
## Review Report
### Scores
- Requirement fit: X/10 — one-line reason
- Test completeness: X/10 — one-line reason
- Code quality: X/10 — one-line reason
- Performance & security: X/10 — one-line reason

**Final score: X/10**
**Verdict: APPROVED**   ← only if final >= {pass_threshold} AND zero blocking issues
                          otherwise **Verdict: CHANGES REQUESTED**

### Blocking Issues
- [file:line] description (or: none)

### Important Issues
- (or: none)

### Nits
- (or: none)

### Verification I ran
- commands and results (or "could not run, reason: ...")

### Notes for Coder
```
