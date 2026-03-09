"""
PII detection for output layer.

Blocks only direct contact-style PII: phone numbers and email addresses.
Uses regex only (no LLM); output is never sent for external PII checking.
Aggregate metrics and limited analytical use of identifiers (e.g. user_id)
are allowed, consistent with intake guardrails.
"""

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# Fixed message when PII is detected (do not send original content).
PII_BLOCK_MESSAGE = (
    "This response was withheld because it may contain personal information."
)

# User-friendly labels for each regex pattern (for the message when a pattern triggers).
_PII_PATTERN_LABELS: dict[str, str] = {
    "phone": "phone number",
    "email": "email address",
}


def format_pii_block_message(pattern: str | None = None) -> str:
    """Return the message to show when PII is detected; include which filter triggered if known."""
    base = "This response was withheld because it may contain personal information."
    if pattern and pattern in _PII_PATTERN_LABELS:
        return f"{base} (detected: {_PII_PATTERN_LABELS[pattern]})."
    return base

# Regex patterns for common PII (US-centric but catches many international formats).
# Require at least one separator so plain digit strings (revenue, counts, IDs) are not false positives.
_PHONE = re.compile(
    r"\+?\d{1,3}[-.\s]\(?\d{2,4}\)?[-.\s]?\d{2,4}[-.\s]?\d{2,4}\d*|\d{3}[-.\s]\d{3}[-.\s]\d{4}"
)
_EMAIL = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")


def _regex_contains_pii(text: str) -> bool:
    """Return True if text matches any PII pattern (phone or email)."""
    if not text or not isinstance(text, str):
        return False
    return bool(_PHONE.search(text) or _EMAIL.search(text))


def _which_regex_matched(text: str) -> str | None:
    """Return which PII pattern matched: 'phone', 'email', or None."""
    if not text or not isinstance(text, str):
        return None
    if _PHONE.search(text):
        return "phone"
    if _EMAIL.search(text):
        return "email"
    return None


def check_pii(
    text: str,
    messages: list[dict[str, Any]] | None = None,
    interpreted_query: str | None = None,
    use_llm: bool = True,
    model: str = "gpt-4.1-mini",
) -> dict[str, Any]:
    """
    Check if text contains PII using regex only (phone number and email).

    No output is sent to any LLM. Only direct contact-style PII is blocked:
    phone numbers and email addresses (except example.com/org/test.com).
    Aggregate metrics and limited analytical use of identifiers are allowed.

    Parameters
    ----------
    text : str
        Engine output to check (or formatted table string).
    messages : list of dict, optional
        Unused (kept for API compatibility). No LLM is called.
    interpreted_query : str, optional
        Unused (kept for API compatibility).
    use_llm : bool, optional
        Unused (kept for API compatibility). Output is never sent for LLM check.
    model : str, optional
        Unused (kept for API compatibility).

    Returns
    -------
    dict
        {"safe": bool, "output": str}. If safe is False, output is block message.
        If safe is True, output is the original text.
    """
    if not text or not isinstance(text, str):
        return {"safe": True, "output": text or ""}

    _regex_hit = _regex_contains_pii(text)
    _which = _which_regex_matched(text) if _regex_hit else None
    # Don't block on placeholder emails (e.g. schema examples: user@example.com)
    if _regex_hit and _which == "email":
        _m = _EMAIL.search(text)
        if _m and any(
            _m.group(0).lower().endswith(d)
            for d in ("@example.com", "@example.org", "@test.com")
        ):
            _regex_hit = False
    if _regex_hit:
        block_message = format_pii_block_message(_which)
        logger.info("Output PII check failed: regex detected PII (pattern=%s)", _which)
        return {"safe": False, "output": block_message}

    return {"safe": True, "output": text}


def _llm_pii_check(
    text: str,
    messages: list[dict[str, Any]],
    interpreted_query: str | None,
    api_key: str,
    model: str,
) -> bool:
    """Return False if LLM considers the text to contain PII."""
    from openai import OpenAI

    system = """You are an output guardrail for a data analyst bot. Decide if the given response text contains personally identifiable information (PII) that must be blocked.

Reply UNSAFE only when the response contains:
- Direct, contact-style PII: full names, email addresses, phone numbers, physical addresses, social security numbers, driver's license numbers, passport numbers, financial account numbers.
- Bulk or list-style export of such data (e.g. a list of all user emails) or large sets of identifiers clearly intended for extraction/export.

Reply SAFE when the response contains any of the following (these are not PII in this context):
- Aggregate metrics about users or entities: counts, sums, averages, breakdowns by country/region/segment (e.g. "number of users per country", "revenue by country", "distinct user_id counts", tables or charts showing such metrics).
- The words "user_id", "users", or column names in analytical or aggregate context (e.g. a table with columns like country, user_count, or revenue).
- One or a limited number of non-contact identifiers (e.g. user_id) in answer to an analytical question (e.g. "the user_id of the longest subscriber is 42", "top 5 user_ids by revenue"). Block only when the intent is clearly to extract or export large sets of identifiers or contact details.
- Responses that only describe what the user can ask about: lists of tables, column names, schema overview, example question types, or capabilities (e.g. "You can ask about users, sessions, revenue..."). These are not PII.

Reply with exactly one word: SAFE or UNSAFE."""

    user_parts = []
    if interpreted_query:
        user_parts.append(f"Interpreted user question: {interpreted_query}")
    user_parts.append("Conversation context (recent messages):")
    for m in messages[-6:]:  # last few messages
        role = m.get("role", "")
        content = (m.get("content") or "")[:500]
        user_parts.append(f"  {role}: {content}")
    user_parts.append("\nResponse text to check for PII:")
    user_parts.append(text[:8000])  # limit size

    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": "\n".join(user_parts)},
            ],
        )
        content = (response.choices[0].message.content or "").strip().upper()
        return "UNSAFE" not in content
    except Exception as e:
        logger.warning("LLM PII check failed, assuming safe: %s", e)
        return True
