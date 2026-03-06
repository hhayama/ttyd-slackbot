"""
Prepare engine result for Slack: PII check and table formatting.

All engine output passes through this layer before being sent to Slack.
"""

import logging
from typing import Any

from ttyd_slackbot.engine import EngineResult
from ttyd_slackbot.output.format_table import format_table_for_slack
from ttyd_slackbot.output.pii_check import check_pii

logger = logging.getLogger(__name__)


def prepare_for_slack(
    engine_result: EngineResult,
    messages: list[dict[str, Any]] | None = None,
    interpreted_query: str | None = None,
    use_llm_pii: bool = True,
) -> str:
    """
    Run output guardrails and formatting on engine result; return string for Slack.

    - Text: PII check (with conversation context). If unsafe, return block message.
    - Table: format as markdown table, then PII check on formatted string.
    - Number / chart / error: convert to string, then PII check.

    Parameters
    ----------
    engine_result : EngineResult
        Structured result from engine (response_type + value).
    messages : list of dict, optional
        Thread conversation history for PII context (same as intake).
    interpreted_query : str, optional
        Intake-interpreted query for PII context.
    use_llm_pii : bool, optional
        Whether to use LLM for PII check in addition to regex. Default True.

    Returns
    -------
    str
        Safe, formatted string to send to Slack (or PII block message).
    """
    msg = messages or []
    text_to_check: str

    if engine_result.response_type == "table":
        text_to_check = format_table_for_slack(engine_result.value)
    elif engine_result.response_type in ("number", "chart", "error"):
        text_to_check = str(engine_result.value) if engine_result.value is not None else ""
    else:
        # text or unknown
        text_to_check = str(engine_result.value) if engine_result.value is not None else ""

    result = check_pii(
        text_to_check,
        messages=msg,
        interpreted_query=interpreted_query,
        use_llm=use_llm_pii,
    )
    return result["output"]
