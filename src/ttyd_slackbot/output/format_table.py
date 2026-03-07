"""
Format tabular engine output for Slack.

Renders DataFrames as ASCII box-drawn tables with aligned columns and truncation
to respect Slack message limits.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Slack message limit is ~40k; leave headroom for block metadata and truncation note.
MAX_TABLE_CHARS = 35_000
DEFAULT_MAX_ROWS = 50
MAX_CELL_CHARS = 500


def format_table_for_slack(
    data: Any,
    max_rows: int = DEFAULT_MAX_ROWS,
    max_chars: int = MAX_TABLE_CHARS,
) -> str:
    """
    Format a DataFrame (or table-like) value as an ASCII box-drawn table for Slack.

    Numeric columns are right-aligned; other columns are left-aligned. Truncation
    respects max_rows and max_chars.

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
        ASCII box-drawn table in a code block (or code block for non-DataFrame), with truncation note if needed.
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

    show_df = df.head(max_rows)
    truncated = total_rows > max_rows

    def _escape_cell(x: Any) -> str:
        s = str(x) if x is not None else ""
        s = s.replace("|", " ").replace("\n", " ")
        return s.strip()[:MAX_CELL_CHARS]

    headers = [_escape_cell(str(c)) for c in df.columns]
    col_count = len(df.columns)

    # Column widths: max of header and each cell in column (over show_df)
    widths = [max(1, len(h)) for h in headers]
    for _, row in show_df.iterrows():
        for j, c in enumerate(df.columns):
            cell = _escape_cell(row[c])
            widths[j] = max(widths[j], len(cell), 1)

    # Right-align numeric columns
    try:
        numeric_cols = [pd.api.types.is_numeric_dtype(df[col]) for col in df.columns]
    except Exception:
        numeric_cols = [False] * col_count

    def _border() -> str:
        return "+" + "+".join("-" * w for w in widths) + "+"

    def _row(cells: list[str], right_align: list[bool]) -> str:
        parts = []
        for i, (cell, w, right) in enumerate(zip(cells, widths, right_align)):
            if right:
                parts.append(cell.rjust(w))
            else:
                parts.append(cell.ljust(w))
        return "|" + "|".join(parts) + "|"

    lines = [
        _border(),
        _row(headers, [False] * col_count),
        _border(),
    ]
    for _, row in show_df.iterrows():
        cells = [_escape_cell(row[c]) for c in df.columns]
        lines.append(_row(cells, numeric_cols))
    lines.append(_border())

    out = "\n".join(lines)
    if truncated:
        out += f"\n\n*(Showing first {max_rows} of {total_rows} rows)*"

    if len(out) > max_chars:
        out = out[: max_chars - 80] + "\n\n*(Output truncated due to length)*"
    return f"```\n{out}\n```"


def _code_block(s: str, max_chars: int = MAX_TABLE_CHARS) -> str:
    """Wrap string in a code block for Slack."""
    if len(s) > max_chars:
        s = s[: max_chars - 40] + "\n... (truncated)"
    return f"```\n{s}\n```"
