# Talk to your Data Slackbot — Project Context

## Project Overview

**Talk to your Data Slackbot** is a conversational AI agent that lets users ask data-related questions in Slack and receive verified, analyzed answers—or diagrams—directly in the channel.

**Why it exists:** It solves the gap between having data in a database and being able to answer ad-hoc questions in a team’s primary communication tool. Users get answers without writing SQL, switching tools, or manually checking data availability. The agent verifies that requested data exists, runs appropriate queries, interprets results (and can create figures when useful), then posts a clear, safe response back to Slack.

**For whom:** Teams that use Slack and need to query and understand their data quickly, with guardrails for accuracy and safety.

---

## System Scope

**In scope:**

- Receiving free-text, data-related questions from Slack.
- Checking data availability via a semantic layer before answering.
- Planning and executing queries, analysis, and optional visualizations.
- Formatting and posting responses (and figures) back to Slack.
- Input and output guardrails for safety and accuracy.
- Conversational context (memory) to support follow-up and clarification.

**Out of scope:**

- Direct database administration, schema changes, or ETL.
- Non-Slack interfaces (e.g. standalone web UI or API) unless explicitly added later.
- Real-time streaming or long-running batch jobs as first-class features.
- User authentication/authorization beyond what Slack and the hosting environment provide.

---

## Architecture Summary

The system is organized into four main subsystems plus **Memory** and an **External Environment** (database).

1. **Intake** — Receives the free-text query from Slack, parses and validates it, and applies guardrails. It consults the semantic layer to confirm that the requested data is available and can ask the user for clarification or inform them about availability before any heavy processing.

2. **Engine** — Plans how to fulfill the query (which datasets to use, what analysis or figures to produce), then executes: querying data, analyzing it, and optionally generating figures. It coordinates with the semantic layer for data access and sends results to the output subsystem.

3. **Semantic Layer** — Exposes a single **Data Orchestrator** that holds metadata about tables, entities, relationships, and join logic. It is the bridge between natural-language intent (from Intake and Engine) and the actual database, and is used for both availability checks and query execution.

4. **Output** — Takes the engine’s results, formats them for Slack (text and optional diagrams), applies output guardrails for safety and accuracy, then posts the final response to the user.

5. **Memory** — Stores conversational and session context. It is used primarily by the Intake subsystem to support clarification, follow-up questions, and consistent interpretation of the user’s intent.

6. **External Environment** — The **Database** is the only external system shown; the Engine (with Semantic Layer) is the component that talks to it.

Data flow: **Slack → Intake** (with Memory and Semantic Layer) **→ Engine** (with Semantic Layer) **→ Output → Slack**. The Engine talks to the **Database** when data is needed.

---

## Key Inputs and Outputs

| Role        | Description |
|------------|-------------|
| **Input**  | Free-text, data-related query from a user in Slack (e.g. a question about metrics, trends, or “show me X”). |
| **Output** | Formatted response posted back to Slack: interpreted findings, summary text, and optionally diagrams or figures. Responses are constrained by output guardrails. |

All other inputs (e.g. Slack events, credentials) and outputs (e.g. internal logs) are supporting; the main contract with the user is **query in → answer (and optional figure) out** in Slack.

---

## Design Rationale

- **Intake first, then Engine:** Separating “understand and validate” from “plan and execute” keeps the system from running expensive or irrelevant queries when the question is ambiguous or the data isn’t available. Guardrails and semantic-layer checks at intake reduce wasted work and improve safety.

- **Semantic Layer as single data interface:** A dedicated layer (Data Orchestrator) for tables, entities, relationships, and joins gives one place to define what exists and how to query it. That supports both “is this data available?” (for Intake) and “how do I get this data?” (for Engine), and keeps database details out of the rest of the agent.

- **Dedicated Output subsystem:** Formatting and output guardrails are isolated so that presentation and safety checks can evolve independently from analysis logic. Users get consistent, readable answers and figures in Slack without the Engine needing to know Slack’s formatting rules.

- **Memory for conversation:** Storing context (Memory) and tying it to Intake allows follow-up questions and clarification flows without re-explaining, and helps the agent give coherent, context-aware answers in a thread.

- **Guardrails at both ends:** Input guardrails block or redirect unsafe or unclear requests; output guardrails ensure that what is posted back is accurate and appropriate. Together they keep the agent safe and trustworthy in a shared Slack space.

This structure is intended to keep the agent maintainable, safe, and aligned with how users actually ask questions and consume answers in Slack.
