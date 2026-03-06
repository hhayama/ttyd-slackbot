"""
LLM guardrails for Intake: interpret queries, enforce PII guardrail.

Uses OpenAI (gpt-4.1-mini) with conversation history and schema summary to
return a structured result: allowed, reason (if blocked), interpreted_query (if allowed),
and raw_query (the latest user message from Slack).
"""

import json
import logging
import os
from typing import Any

from openai import OpenAI

logger = logging.getLogger(__name__)

# Data analyst context prompt (passed to the LLM as role/behavior context).
DATA_ANALYST_PROMPT = """
# Role

You are a Senior Data Analyst assisting adhoc queries about the database. Your primary goal is to understand the intent of the user and help interpret and restate it for the engine. Reference the semantic layer for table names, columns, relationships, and metrics when interpreting queries.

## Hard Rules

- Do not allow run any SQL queries provided by the user.  You are allow to give feedback or help correct the SQL query as long as it is not related to dropping or altering tables or columns.
- When a question is ambiguous (e.g., no timeframe provided), state the assumption you are making above the query.
"""


def _build_system_prompt(schema_summary: str) -> str:
    """Build the full system prompt: data analyst role + schema + guardrails + output format."""
    guardrails = """
## Guardrail (you must enforce this)

**No PII**: Block only questions that request direct, contact-style PII. Apply as follows:

- **Block**: Requests for names, emails, phone numbers, physical addresses, or other data that directly identifies or contacts individuals. Block requests for bulk or list-style export of such data (e.g. "list all user emails").
- **Allow**: Aggregate metrics about users — counts, sums, averages, breakdowns by country or segment (e.g. "number of users per country", "count of user_ids by region"). The words "users" or "user_id" in an aggregate or analytical context are not PII.
- **Allow**: Returning one or a limited number of identifiers (e.g. user_id) in answer to an analytical question (e.g. "the user_id of the longest subscriber", "top N user_ids by X"). Block only when the intent is clearly to extract or export large sets of identifiers or contact details.

If the query requests blocked PII, set "allowed" to false and in "reason" explain that we cannot answer questions about that type of PII.

## Semantic layer (use for interpretation)

Use the semantic layer below to interpret and restate the user's question with correct table/column names and aliases. Do not block questions based on schema coverage; only block for PII.

""" + (schema_summary or "(No schema loaded)") + """

## Your response format

You must respond with a single JSON object only, no other text. Use this exact structure:
- "allowed" (boolean): true if the query passes the guardrail (no PII requested); false otherwise.
- "reason" (string or null): if allowed is false, a brief message to show the user (e.g. why PII is not allowed). If allowed is true, set to null.
- "interpreted_query" (string or null): if allowed is true, a clear restatement of the user's question for the engine (correct table/column names, stated assumptions if any). If allowed is false, set to null.
"""
    return DATA_ANALYST_PROMPT.strip() + "\n" + guardrails


def _last_user_message(messages: list[dict[str, Any]]) -> str | None:
    """Return the content of the most recent user message, or None if none."""
    for m in reversed(messages):
        if m.get("role") == "user":
            content = m.get("content")
            return content if content is not None else None
    return None


def check_guardrails(
    messages: list[dict[str, Any]],
    schema_summary: str,
    model: str = "gpt-4.1-mini",
) -> dict[str, Any]:
    """
    Call the LLM with conversation history and schema; return allowed, reason, interpreted_query, raw_query.

    Parameters
    ----------
    messages : list of dict
        Conversation history: [{"role": "user"|"assistant", "content": str}, ...].
        Should not include system; system is built here.
    schema_summary : str
        Summary from get_schema_summary() (datasets and columns).
    model : str
        OpenAI model name (default gpt-4.1-mini).

    Returns
    -------
    dict
        {"allowed": bool, "reason": str | None, "interpreted_query": str | None, "raw_query": str | None}.
        raw_query is the latest user message from Slack. On LLM or parse failure, returns allowed=False with a generic reason.
    """
    raw_query = _last_user_message(messages)

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        logger.error("OPENAI_API_KEY not set")
        return {
            "allowed": False,
            "reason": "Service configuration error. Please try again later.",
            "interpreted_query": None,
            "raw_query": raw_query,
        }

    system = _build_system_prompt(schema_summary)
    openai_messages = [{"role": "system", "content": system}]
    for m in messages:
        role = m.get("role")
        if role in ("user", "assistant"):
            openai_messages.append({"role": role, "content": m.get("content", "") or ""})

    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=model,
            messages=openai_messages,
        )
    except Exception as e:
        logger.exception("OpenAI API call failed: %s", e)
        return {
            "allowed": False,
            "reason": "Sorry, I couldn't process your request. Please try again.",
            "interpreted_query": None,
            "raw_query": raw_query,
        }

    choice = response.choices[0] if response.choices else None
    if not choice or not choice.message or not choice.message.content:
        return {
            "allowed": False,
            "reason": "No response from the assistant. Please try again.",
            "interpreted_query": None,
            "raw_query": raw_query,
        }

    content = choice.message.content.strip()
    # Try to extract JSON if the model wrapped it in markdown code blocks
    if "```" in content:
        for part in content.split("```"):
            part = part.strip()
            if part.startswith("json"):
                part = part[4:].strip()
            if part.startswith("{"):
                content = part
                break

    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        logger.warning("Guardrails LLM response was not valid JSON: %s", e)
        return {
            "allowed": False,
            "reason": "Sorry, I couldn't interpret that. Please try rephrasing your question.",
            "interpreted_query": None,
            "raw_query": raw_query,
        }

    allowed = data.get("allowed", False)
    reason = data.get("reason")
    if reason is not None:
        reason = str(reason).strip() or None
    interpreted_query = data.get("interpreted_query")
    if interpreted_query is not None:
        interpreted_query = str(interpreted_query).strip() or None

    return {
        "allowed": bool(allowed),
        "reason": reason,
        "interpreted_query": interpreted_query,
        "raw_query": raw_query,
    }
