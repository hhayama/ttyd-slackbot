"""
Prepare engine result for Slack: PII check and table formatting.

All engine output passes through this layer before being sent to Slack.
"""

import logging
import os
import tempfile
from typing import Any

from ttyd_slackbot.engine import EngineResult
from ttyd_slackbot.output.format_table import format_table_for_slack
from ttyd_slackbot.output.pii_check import PII_BLOCK_MESSAGE, check_pii

logger = logging.getLogger(__name__)

# Type alias for prepare result: (message text, optional file bytes, optional filename)
PrepareResult = tuple[str, bytes | None, str | None]

# Slack allows up to 1 GB per file; we cap at 20 MB for faster uploads and smaller payloads.
SLACK_CSV_FILE_SIZE_LIMIT_BYTES = min(1024**3, 20 * 1024 * 1024)

CSV_TRUNCATION_MESSAGE = (
    " The data has been truncated because it reached the upload size limit (20 MB)."
)


def prepare_for_slack(
    engine_result: EngineResult,
    messages: list[dict[str, Any]] | None = None,
    interpreted_query: str | None = None,
    use_llm_pii: bool = True,
) -> PrepareResult:
    """
    Run output guardrails and formatting on engine result; return payload for Slack.

    - Text: PII check (with conversation context). If unsafe, return block message.
    - Table: format as ASCII box-drawn table, then PII check on formatted string.
    - Number / error: convert to string, then PII check.
    - Chart: save chart to image bytes, PII check caption; return (caption, bytes, filename).
    - CSV file: PII check content, optionally truncate to size limit; return (message, bytes, filename).

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
    tuple of (str, bytes or None, str or None)
        (message text, optional file bytes for upload, optional filename).
        For chart: (caption, png_bytes, "chart.png"). Otherwise: (text, None, None).
    """
    msg = messages or []
    text_to_check: str
    file_bytes: bytes | None = None
    file_name: str | None = None

    if engine_result.response_type == "table":
        text_to_check = format_table_for_slack(engine_result.value)
    elif engine_result.response_type == "csv_file":
        file_bytes, file_name = engine_result.value
        limit = SLACK_CSV_FILE_SIZE_LIMIT_BYTES
        truncated = False
        if len(file_bytes) > limit:
            truncated = True
            last_newline = file_bytes.rfind(b"\n", 0, limit + 1)
            if last_newline != -1:
                file_bytes = file_bytes[: last_newline + 1]
            else:
                file_bytes = file_bytes[:limit]
        csv_text = file_bytes.decode("utf-8", errors="replace")
        pii_result = check_pii(
            csv_text,
            messages=msg,
            interpreted_query=interpreted_query,
            use_llm=use_llm_pii,
        )
        if not pii_result["safe"]:
            return (pii_result["output"], None, None)
        message = "Here's your CSV."
        if truncated:
            message += CSV_TRUNCATION_MESSAGE
        return (message, file_bytes, file_name)
    elif engine_result.response_type == "chart":
        caption = "Here's your chart."
        # Caption is fixed and cannot contain PII; skip LLM to avoid any context-driven false positive.
        pii_result = check_pii(
            caption,
            messages=[],
            interpreted_query=None,
            use_llm=False,
        )
        chart_value = engine_result.value
        if chart_value is not None and hasattr(chart_value, "save"):
            fd, path = tempfile.mkstemp(suffix=".png")
            try:
                os.close(fd)
                chart_value.save(path)
                with open(path, "rb") as f:
                    file_bytes = f.read()
                file_name = "chart.png"
                return (pii_result["output"], file_bytes, file_name)
            except Exception as e:
                logger.warning("Chart save failed, falling back to text: %s", e)
            finally:
                try:
                    os.unlink(path)
                except OSError:
                    pass
        else:
            # Engine may return a path string (e.g. PandasAI exports/charts/temp_chart_*.png).
            # Reading from path avoids PII-checking the path string (regex false-positives on UUIDs/digits).
            path_str = str(chart_value).strip() if chart_value is not None else ""
            if path_str and (path_str.endswith(".png") or path_str.endswith(".jpg") or ".png" in path_str or ".jpg" in path_str):
                if os.path.isfile(path_str):
                    try:
                        with open(path_str, "rb") as f:
                            file_bytes = f.read()
                        return (pii_result["output"], file_bytes, "chart.png")
                    except OSError as e:
                        logger.warning("Chart path read failed: %s", e)
            logger.warning("Chart value has no save() and not a readable path, returning string representation")
        text_to_check = str(chart_value) if chart_value is not None else ""
        result = check_pii(
            text_to_check,
            messages=msg,
            interpreted_query=interpreted_query,
            use_llm=use_llm_pii,
        )
        return (result["output"], None, None)
    elif engine_result.response_type in ("number", "error"):
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
    return (result["output"], None, None)
