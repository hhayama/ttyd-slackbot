"""
Format tabular engine output for Slack.

Renders DataFrames as default text in a code block, with truncation at 20 rows
and a note when full data is attached as CSV.
"""

from typing import Any

DEFAULT_MAX_ROWS = 20


def format_table_for_slack(data: Any, max_rows: int = DEFAULT_MAX_ROWS) -> str:
    """
    Format a DataFrame (or table-like) value as default text in a code block for Slack.

    If the DataFrame has more than max_rows rows, only the first max_rows are shown
    and a note indicates full data is attached as CSV (handled by the prepare layer).

    Parameters
    ----------
    data : DataFrame or any
        Tabular data; if not a DataFrame, converted via str() then returned as code block.
    max_rows : int, optional
        Maximum number of rows to include in the code block. Default 20.

    Returns
    -------
    str
        Default string representation in a code block, with truncation note if needed.
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
    out = show_df.to_string()
    if total_rows > max_rows:
        out += f"\n\n*(Showing first {max_rows} of {total_rows} rows. Full data attached as CSV.)*"
    return f"```\n{out}\n```"


def _code_block(s: str) -> str:
    """Wrap string in a code block for Slack."""
    return f"```\n{s}\n```"
