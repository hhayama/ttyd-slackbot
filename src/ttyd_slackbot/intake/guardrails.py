"""
Intake guardrails: regex-based PII blocklist and LLM query interpretation.

PII blocking is done via a regex blocklist on the latest user message (e.g. terms
like driver's license, name, dob, birthday, ssn, email). Messages matching the
blocklist are rejected before the LLM is called. The LLM (OpenAI gpt-4.1-mini) is
used only for query interpretation: it restates the question using the semantic
layer (table/column names) and returns allowed, reason, interpreted_query, and
raw_query (the latest user message from Slack).
"""

import json
import logging
import os
import re
from typing import Any

from openai import OpenAI

logger = logging.getLogger(__name__)

# Regex to block messages that explicitly request PII-related terms (case-insensitive).
# Matches: driver's license, name(s), dob, birthday(s), ssn(s), email(s).
PII_BLOCK_PATTERN = re.compile(
    r"(?:driver'?s?\s*license|\b(?:name|names|dob|birthday|birthdays|ssn|ssns|email|emails)\b)",
    re.IGNORECASE,
)
PII_BLOCK_REASON = (
    "We can't answer questions that request personal or contact information "
    "(e.g. name, email, SSN, DOB)."
)

# Data analyst context prompt (passed to the LLM as role/behavior context).
DATA_ANALYST_PROMPT = """
# Role

You are a Senior Data Analyst assisting adhoc queries about the database. Your primary goal is to understand the intent of the user and help interpret and restate it for the engine. Reference the semantic layer for table names, columns, relationships, and metrics when interpreting queries.

## Hard Rules

- Do not allow run any SQL queries provided by the user.  You are allow to give feedback or help correct the SQL query as long as it is not related to dropping or altering tables or columns.
- When a question is ambiguous (e.g., no timeframe provided), state the assumption you are making above the query.
"""


def _build_system_prompt(schema_summary: str) -> str:
    """Build the full system prompt: data analyst role + schema + output format."""
    prompt = """
## Semantic layer (use for interpretation)

Use the semantic layer below to interpret and restate the user's question with correct table/column names and aliases.

- Requests for the result as CSV or in export format (e.g. "can I get that as a csv?", "export as csv") are allowed. Set allowed to true and restate the query so the engine can return the data in the requested format (e.g. "Return the result of the previous query as a CSV file." or include "as CSV" in the interpreted_query).

""" + (schema_summary or "(No schema loaded)") + """

## Your response format

You must respond with a single JSON object only, no other text. Use this exact structure:
- "allowed" (boolean): true when you have successfully interpreted the query.
- "reason" (string or null): set to null when allowed is true.
- "interpreted_query" (string or null): when allowed is true, a clear restatement of the user's question for the engine (correct table/column names, stated assumptions if any).
"""
    return DATA_ANALYST_PROMPT.strip() + "\n" + prompt


def _last_user_message(messages: list[dict[str, Any]]) -> str | None:
    """Return the content of the most recent user message, or None if none."""
    for m in reversed(messages):
        if m.get("role") == "user":
            content = m.get("content")
            return content if content is not None else None
    return None


def _blocked_by_pii_regex(text: str) -> bool:
    """Return True if the text contains any blocked PII-related term."""
    if not text or not text.strip():
        return False
    return PII_BLOCK_PATTERN.search(text) is not None


def check_guardrails(
    messages: list[dict[str, Any]],
    schema_summary: str,
    model: str = "gpt-4.1-mini",
) -> dict[str, Any]:
    """
    Run regex PII check, then LLM interpretation; return allowed, reason, interpreted_query, raw_query.

    Messages containing certain PII-related terms (e.g. name, email, SSN, DOB,
    driver's license) are blocked by regex before the LLM is called. Otherwise
    the LLM interprets the query using the schema and returns a restatement.

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
        raw_query is the latest user message from Slack. On PII regex match or LLM/parse failure, returns allowed=False with a reason.
    """
    raw_query = _last_user_message(messages)

    if raw_query and _blocked_by_pii_regex(raw_query):
        return {
            "allowed": False,
            "reason": PII_BLOCK_REASON,
            "interpreted_query": None,
            "raw_query": raw_query,
        }

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
