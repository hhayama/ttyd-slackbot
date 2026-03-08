"""
PII detection for output layer.

Blocks only direct contact-style PII and bulk export of identifiers; aggregate
metrics and limited analytical use of identifiers (e.g. user_id) are allowed,
consistent with intake guardrails. Uses regex for phone, email, SSN, driver's
license. LLM check is currently commented out; only regex-based checks are active.
"""

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# Fixed message when PII is detected (do not send original content).
PII_BLOCK_MESSAGE = (
    "This response was withheld because it may contain personal information."
)

# Regex patterns for common PII (US-centric but catches many international formats).
_PHONE = re.compile(
    r"\+?\d{1,3}[-.\s]?\(?\d{2,4}\)?[-.\s]?\d{2,4}[-.\s]?\d{2,4}\d*|\d{3}[-.\s]\d{3}[-.\s]\d{4}"
)
_EMAIL = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
_SSN = re.compile(r"\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b")
# Driver's license: alphanumeric ID; matched segment must contain a digit (avoids "total revenue", "42,000").
_DRIVERS_LICENSE = re.compile(
    r"\b(?=[A-Z0-9]*\d)[A-Z0-9]{4,}[\s-]?[A-Z0-9]{2,}\b",
    re.IGNORECASE,
)


def _regex_contains_pii(text: str) -> bool:
    """Return True if text matches any PII pattern (phone, email, SSN, DL)."""
    if not text or not isinstance(text, str):
        return False
    return bool(
        _PHONE.search(text)
        or _EMAIL.search(text)
        or _SSN.search(text)
        or _DRIVERS_LICENSE.search(text)
    )


def _which_regex_matched(text: str) -> str | None:
    """Return which PII pattern matched: 'phone', 'email', 'ssn', 'drivers_license', or None."""
    if not text or not isinstance(text, str):
        return None
    if _PHONE.search(text):
        return "phone"
    if _EMAIL.search(text):
        return "email"
    if _SSN.search(text):
        return "ssn"
    if _DRIVERS_LICENSE.search(text):
        return "drivers_license"
    return None


def check_pii(
    text: str,
    messages: list[dict[str, Any]] | None = None,
    interpreted_query: str | None = None,
    use_llm: bool = True,
    model: str = "gpt-4.1-mini",
) -> dict[str, Any]:
    """
    Check if text contains PII; optionally use LLM with conversation context.

    Aggregate metrics (counts, sums, revenue by country, etc.) and limited
    analytical use of identifiers (e.g. returning one user_id for "longest
    subscriber") are considered safe. Only direct contact-style PII (names,
    emails, phones, addresses, SSN, etc.) and bulk export of identifiers
    are blocked.

    Parameters
    ----------
    text : str
        Engine output to check (or formatted table string).
    messages : list of dict, optional
        Thread conversation history [{"role": "user"|"assistant", "content": str}].
    interpreted_query : str, optional
        Intake-interpreted query for context.
    use_llm : bool, optional
        If True and OPENAI_API_KEY set, run LLM-based PII check with context.
        Default True.
    model : str, optional
        OpenAI model for LLM check. Default gpt-4.1-mini.

    Returns
    -------
    dict
        {"safe": bool, "output": str}. If safe is False, output is PII_BLOCK_MESSAGE.
        If safe is True, output is the original text (or redacted version in future).
    """
    if not text or not isinstance(text, str):
        return {"safe": True, "output": text or ""}

    # 1. Regex check first
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
        logger.info("Output PII check failed: regex detected PII")
        return {"safe": False, "output": PII_BLOCK_MESSAGE}

    # 2. Optional LLM check with intake context (LLM PII check disabled; regex-only.)
    # if use_llm:
    #     import os
    #
    #     api_key = os.environ.get("OPENAI_API_KEY")
    #     if api_key and messages is not None:
    #         llm_safe = _llm_pii_check(text, messages, interpreted_query, api_key, model)
    #         if not llm_safe:
    #             logger.info("Output PII check failed: LLM detected PII")
    #             return {"safe": False, "output": PII_BLOCK_MESSAGE}

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
