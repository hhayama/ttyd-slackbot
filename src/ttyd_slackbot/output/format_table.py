"""
Format tabular engine output for Slack.

Renders DataFrames as markdown tables with truncation to respect Slack message limits.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Slack message limit is ~40k; leave headroom for block metadata and truncation note.
MAX_TABLE_CHARS = 35_000
DEFAULT_MAX_ROWS = 50


def format_table_for_slack(
    data: Any,
    max_rows: int = DEFAULT_MAX_ROWS,
    max_chars: int = MAX_TABLE_CHARS,
) -> str:
    """
    Format a DataFrame (or table-like) value as a readable markdown table for Slack.

    Parameters
    ----------
    data : DataFrame or any
        Tabular data; if not a DataFrame, converted via str() then returned as code block.
    max_rows : int, optional
        Maximum number of rows to include. Default 50.
    max_chars : int, optional
        Approximate maximum character count; truncate table or add note. Default 35000.

    Returns
    -------
    str
        Markdown table string (or code block for non-DataFrame), with truncation note if needed.
    """
    try:
        import pandas as pd
    except ImportError:
        return _code_block(str(data))

    if not isinstance(data, pd.DataFrame):
        return _code_block(str(data))

    df = data
    total_rows = len(df)
    if total_rows == 0:
        return "*(No rows)*"

    # Truncate rows
    show_df = df.head(max_rows)
    truncated = total_rows > max_rows

    # Build markdown table: header + separator + rows
    def _escape_cell(x: Any) -> str:
        s = str(x) if x is not None else ""
        # Avoid breaking table with pipe or newline
        s = s.replace("|", "\\|").replace("\n", " ")
        return s.strip()[:500]  # cap cell width

    headers = [_escape_cell(c) for c in df.columns]
    header_line = "| " + " | ".join(headers) + " |"
    sep_line = "| " + " | ".join("---" for _ in headers) + " |"
    lines = [header_line, sep_line]
    for _, row in show_df.iterrows():
        cells = [_escape_cell(row[c]) for c in df.columns]
        lines.append("| " + " | ".join(cells) + " |")

    out = "\n".join(lines)
    if truncated:
        out += f"\n\n*(Showing first {max_rows} of {total_rows} rows)*"

    if len(out) > max_chars:
        out = out[: max_chars - 80] + "\n\n*(Output truncated due to length)*"
    return out


def _code_block(s: str, max_chars: int = MAX_TABLE_CHARS) -> str:
    """Wrap string in a code block for Slack."""
    if len(s) > max_chars:
        s = s[: max_chars - 40] + "\n... (truncated)"
    return f"```\n{s}\n```"
