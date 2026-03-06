"""
PII detection for output layer.

Uses regex patterns for phone, email, SSN, driver's license, and optional LLM
with conversation context (same as intake guardrails) for context-aware checks.
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


def check_pii(
    text: str,
    messages: list[dict[str, Any]] | None = None,
    interpreted_query: str | None = None,
    use_llm: bool = True,
    model: str = "gpt-4.1-mini",
) -> dict[str, Any]:
    """
    Check if text contains PII; optionally use LLM with conversation context.

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
    if _regex_contains_pii(text):
        logger.info("Output PII check failed: regex detected PII")
        return {"safe": False, "output": PII_BLOCK_MESSAGE}

    # 2. Optional LLM check with intake context
    if use_llm:
        import os

        api_key = os.environ.get("OPENAI_API_KEY")
        if api_key and messages is not None:
            llm_safe = _llm_pii_check(text, messages, interpreted_query, api_key, model)
            if not llm_safe:
                logger.info("Output PII check failed: LLM detected PII")
                return {"safe": False, "output": PII_BLOCK_MESSAGE}

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

    system = """You are an output guardrail for a data analyst bot. Your only job is to decide if the given response text contains personally identifiable information (PII).

PII includes: full names, email addresses, phone numbers, physical addresses, social security numbers, driver's license numbers, passport numbers, financial account numbers, and similar identifiers.

Reply with exactly one word: SAFE or UNSAFE. If the response contains any PII, reply UNSAFE. Otherwise reply SAFE."""

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
