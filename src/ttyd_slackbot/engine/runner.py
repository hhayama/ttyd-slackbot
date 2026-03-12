"""
Run natural-language queries against all datasets in datasets/<org> via PandasAI v3 Agent.

One Agent is created per Slack thread and reused so that follow-up questions use
agent.follow_up() and retain conversation memory. Expects DB_* or DATABASE_URL and
OPENAI_API_KEY (and load_dotenv() already called). Schema YAML placeholders like
${DB_HOST} are resolved from the environment before PandasAI loads them (PandasAI
does not resolve them when executing SQL).
"""

import logging
import os
import re
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, unquote

import yaml


# Description passed to PandasAI v3 Agent to enforce Postgres SQL assistant behavior.
AGENT_DESCRIPTION = """
# Role

You are an expert Postgres SQL assistant. Your primary goal is to generate correct, efficient, and production-ready Postgres SQL queries for the user.
Utilize the uploaded semantic layer files for table names, columns, relationships, metrics, and golden queries. SQL does not have styling guidelines thus to drive consistency of output,
refer to the SQL Styling Guidelines below.

## Hard Rules

- Do not use tables, columns, or metrics that are not explicitly defined in the semantic layer.
- Respect the exact names, descriptions, and relationships in the semantic layer.
- Use metrics and example queries when available instead of writing raw SQL from scratch.
- Use proper table aliases (`p` for payments, `s` for subscriptions, `u` for users, etc.) to keep queries readable.
- If the request involves a field, table, or metric that does not exist in the semantic layer, do not guess or invent anything. Respond with:  "This field/table is not available in the provided schema."
- When a question is ambiguous (e.g., no timeframe provided), state the assumption you are making above the query.
- Use Common Table Expressions (CTE)s with descriptive names instead of using subqueries.
- Include comments explaining the logic about what the query is doing.  If there are CTEs, also include specific comments about what the CTE is intended to do as this will help with understanding the query.
- When the user asks for data to be exported or returned as CSV (e.g. "export as csv", "give me this as a csv"), run the appropriate query to get the result, save the result DataFrame to a CSV file in the current working directory (e.g. with df.to_csv("exported_data.csv", index=False)), and respond with exactly: "CSV file saved as <filename>.csv" using the actual filename. CSV export is supported in this interface; do not refuse it.
- When the user asks to see the SQL query that generated the previous result (e.g. chart, CSV, table), you may provide that SQL; there is no policy forbidding it.
- The generated code must never return a raw Python dict. Return only one of: a string, a single DataFrame, a number, or a chart. If the answer has multiple parts (e.g. several metrics or breakdowns), format them as a single string (e.g. markdown or bullet list) or return one primary DataFrame and explain the rest in a string.
"""


# Regex for agent "CSV file saved as <filename>.csv" message.
_CSV_SAVED_PATTERN = re.compile(r"(?i)CSV file saved as\s+(.+\.csv)")

# When the user message indicates CSV export intent, we prepend this so the agent sees it in-turn.
_CSV_INSTRUCTION_PREFIX = (
    "The user wants the result as a CSV file. You must run the query, save the result DataFrame "
    "to a CSV file in the current working directory (e.g. df.to_csv('exported_data.csv', index=False)), "
    "and respond with exactly the line: CSV file saved as <filename>.csv (use the actual filename).\n\n"
)


@dataclass
class EngineResult:
    """Structured result from the engine for the output layer.

    Attributes
    ----------
    response_type : str
        One of "text", "table", "number", "chart", "error", "csv_file".
    value : str | Any
        Raw value: str for text/error, DataFrame for table, number, or chart object.
        For csv_file: tuple of (bytes, filename).
    """

    response_type: str
    value: Any

logger = logging.getLogger(__name__)

# Process-local cache: (channel_id, thread_ts) -> Agent instance
_agents_by_thread: dict[tuple[str, str], Any] = {}
_lock = threading.Lock()

# Last executed SQL per agent (keyed by id(agent) so one entry per thread).
# PandasAI may expose SQL via last_code_generated (Python code containing SQL) or other attributes.
_last_sql_by_agent: dict[int, str] = {}


def _get_datasets_dir_and_org() -> tuple[Path, str]:
    """Return (datasets_dir, org) using same convention as schema_loader."""
    base = os.environ.get("DATASETS_DIR")
    datasets_dir = Path(base).resolve() if base else Path.cwd() / "datasets"
    datasets_dir = datasets_dir.resolve()
    org = os.environ.get("SEMANTIC_LAYER_ORG", "ttyd")
    return datasets_dir, org


def _list_dataset_names(datasets_dir: Path, org: str) -> list[str]:
    """
    List dataset names under datasets_dir/org that have a schema.yaml.

    Returns
    -------
    list[str]
        Sorted list of directory names (e.g. ["payments", "sessions", "subscriptions", "users"]).
    """
    org_path = datasets_dir / org
    if not org_path.is_dir():
        return []
    names = []
    for path in sorted(org_path.iterdir()):
        if path.is_dir() and (path / "schema.yaml").exists():
            names.append(path.name)
    return names


def _resolve_placeholders(obj: Any) -> Any:
    """Recursively replace ${VAR} in strings with os.environ.get('VAR', '')."""
    if isinstance(obj, dict):
        return {k: _resolve_placeholders(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_placeholders(i) for i in obj]
    if isinstance(obj, str):
        return re.sub(
            r"\$\{(\w+)\}",
            lambda m: os.environ.get(m.group(1), ""),
            obj,
        )
    return obj


def _inject_connection_from_url(data: dict, url: str) -> None:
    """
    If the schema has a postgres connection with an empty password, fill it (and
    host/port/user/database) from DATABASE_URL. Mutates data in place.
    This allows Render (and others) to set only DATABASE_URL; otherwise
    ${DB_PASSWORD} resolves to '' and we get "no password supplied".
    """
    source = data.get("source") or {}
    if not isinstance(source, dict):
        return
    conn = source.get("connection")
    if not isinstance(conn, dict):
        return
    if source.get("type") != "postgres":
        return
    password = (conn.get("password") or "").strip()
    if password:
        return
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("postgres", "postgresql"):
            return
        host = (parsed.hostname or "").strip()
        port = parsed.port if parsed.port is not None else 5432
        database = (parsed.path or "").lstrip("/").split("/")[0] or ""
        user = (unquote(parsed.username or "")) or ""
        raw_password = parsed.password
        password = unquote(raw_password) if raw_password else ""
        if not all([host, database, user]):
            return
        conn["host"] = host
        conn["port"] = port
        conn["database"] = database
        conn["user"] = user
        conn["password"] = password
    except Exception:
        pass


def _build_resolved_schemas_dir(datasets_dir: Path, org: str, names: list[str]) -> Path:
    """
    Write resolved schema.yaml files (with ${VAR} replaced from env) to a temp dir.

    Structure: tempdir/datasets/<org>/<name>/schema.yaml. Returns the temp dir path
    (caller must chdir into it so PandasAI sees datasets/ and can pai.load(org/name)).
    """
    tmpdir = Path(tempfile.mkdtemp(prefix="ttyd_schemas_"))
    try:
        database_url = os.environ.get("DATABASE_URL", "").strip()
        for name in names:
            src = datasets_dir / org / name / "schema.yaml"
            with open(src, encoding="utf-8") as f:
                data = yaml.safe_load(f)
            if data is None:
                continue
            resolved = _resolve_placeholders(data)
            if database_url and isinstance(resolved, dict):
                _inject_connection_from_url(resolved, database_url)
            dest_dir = tmpdir / "datasets" / org / name
            dest_dir.mkdir(parents=True)
            with open(dest_dir / "schema.yaml", "w", encoding="utf-8") as f:
                yaml.dump(resolved, f, default_flow_style=False, allow_unicode=True)
        return tmpdir
    except Exception:
        import shutil

        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def create_agent(
    datasets_dir: Path | None = None,
    org: str | None = None,
) -> Any:
    """
    Load all datasets under datasets/<org>, create and return a PandasAI v3 Agent.

    Parameters
    ----------
    datasets_dir : Path or None
        Root datasets directory. Defaults to DATASETS_DIR env or cwd/datasets.
    org : str or None
        Organization name (default SEMANTIC_LAYER_ORG or "ttyd").

    Returns
    -------
    Agent
        PandasAI Agent backed by the loaded datasets.

    Raises
    ------
    ValueError
        If no datasets are found.
    """
    _dir, _org = _get_datasets_dir_and_org()
    datasets_dir = datasets_dir.resolve() if datasets_dir is not None else _dir
    org = org or _org

    names = _list_dataset_names(datasets_dir, org)
    if not names:
        raise ValueError(
            "No datasets found. Add schema.yaml files under datasets/<org>/<name>/."
        )

    import pandasai as pai
    from pandasai import Agent

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError(
            "OPENAI_API_KEY is required for the engine (PandasAI LLM). Set it in .env."
        )
    from pandasai_litellm.litellm import LiteLLM

    llm = LiteLLM(model="gpt-4.1-mini", api_key=api_key)
    pai.config.set({"llm": llm, "max_retries": 5})

    import shutil

    from pandasai.helpers.filemanager import DefaultFileManager

    resolved_root = _build_resolved_schemas_dir(datasets_dir, org, names)
    resolved_datasets = resolved_root / "datasets"
    # PandasAI's loader uses config.get().file_manager, which is created once with
    # base_path = find_project_root() + "/datasets". So we must temporarily point
    # file_manager at our resolved schemas dir so pai.load() reads resolved YAML.
    original_config = pai.config.get()
    custom_fm = DefaultFileManager()
    custom_fm.base_path = str(resolved_datasets)
    try:
        pai.config.update({"file_manager": custom_fm})
        paths = [f"{org}/{name}" for name in names]
        loaded = []
        for path in paths:
            try:
                obj = pai.load(path)
                loaded.append(obj)
            except Exception as e:
                logger.warning("pai.load(%r) failed: %s", path, e)
        if not loaded:
            raise ValueError("Could not load any dataset.")
        return Agent(loaded, description=AGENT_DESCRIPTION)
    finally:
        pai.config.update({"file_manager": original_config.file_manager})
        shutil.rmtree(resolved_root, ignore_errors=True)


def get_or_create_agent_for_thread(
    channel_id: str,
    thread_ts: str,
    datasets_dir: Path | None = None,
    org: str | None = None,
) -> Any:
    """
    Return the Agent for this Slack thread, creating and caching one if needed.

    Parameters
    ----------
    channel_id : str
        Slack channel ID.
    thread_ts : str
        Slack thread_ts (or event ts for top-level).
    datasets_dir : Path or None
        Passed to create_agent when creating a new agent.
    org : str or None
        Passed to create_agent when creating a new agent.

    Returns
    -------
    Agent
        The Agent instance for this thread (same instance on subsequent calls).
    """
    key = (channel_id, thread_ts)
    with _lock:
        if key not in _agents_by_thread:
            _agents_by_thread[key] = create_agent(datasets_dir=datasets_dir, org=org)
        return _agents_by_thread[key]


def _normalize_response(response: Any) -> EngineResult:
    """Map PandasAI response object to EngineResult (response_type + value)."""
    if response is None:
        return EngineResult(response_type="text", value="")
    value = getattr(response, "value", response)
    if value is None:
        return EngineResult(response_type="text", value="")
    type_name = type(response).__name__
    if "DataFrame" in type_name:
        return EngineResult(response_type="table", value=value)
    if "Chart" in type_name:
        return EngineResult(response_type="chart", value=value)
    if "Error" in type_name:
        return EngineResult(response_type="error", value=str(value))
    if "Number" in type_name:
        return EngineResult(response_type="number", value=value)
    # StringResponse or unknown: treat as text
    return EngineResult(response_type="text", value=str(value))


def _try_consume_agent_csv_file(engine_result: EngineResult) -> EngineResult:
    """
    If the result is text matching "CSV file saved as X.csv", read that file from cwd,
    delete it, and return a csv_file result. Otherwise return the result unchanged.
    """
    if engine_result.response_type != "text":
        return engine_result
    text = engine_result.value
    if not text or not isinstance(text, str):
        return engine_result
    match = _CSV_SAVED_PATTERN.search(text.strip())
    if not match:
        return engine_result
    filename = match.group(1).strip()
    if not filename or ".." in filename or os.path.sep in filename:
        return engine_result
    cwd = Path.cwd().resolve()
    path = (cwd / filename).resolve()
    if not path.is_file() or not path.is_relative_to(cwd):
        return engine_result
    try:
        content = path.read_bytes()
    except OSError:
        return engine_result
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass
    return EngineResult(response_type="csv_file", value=(content, filename))


def _user_wants_csv(query: str) -> bool:
    """True if the user message indicates they want the result as a CSV file."""
    if not query or not isinstance(query, str):
        return False
    q = query.strip().lower()
    return "csv" in q and (
        "as a csv" in q
        or "as csv" in q
        or "export" in q
        or "send" in q
        or "over as" in q
        or q.endswith("csv")
        or " in csv" in q
    )


def _user_wants_sql(query: str) -> bool:
    """True if the user is asking for the SQL query that generated the previous result."""
    if not query or not isinstance(query, str):
        return False
    q = query.strip().lower()
    if "sql" not in q:
        return False
    return (
        "query" in q
        or "generated" in q
        or "that ran" in q
        or "used" in q
        or "show" in q
        or "what" in q
        or "give me" in q
        or "that generated" in q
        or "which query" in q
        or "the query" in q
        or "return" in q
        or "get" in q
        or "for that" in q
    )


# Regex to extract SQL from generated Python. PandasAI v3 with SQL uses execute_sql_query("""...""").
_SQL_IN_TRIPLE_QUOTES = re.compile(
    r'''(?:execute_sql_query|execute_query|query|sql)\s*\(\s*["']{{3}}(.*?)["']{{3}}''',
    re.DOTALL | re.IGNORECASE,
)
_SQL_TRIPLE_DOUBLE = re.compile(r'"""([^"]*(?:SELECT|WITH|INSERT|UPDATE|DELETE|FROM)[^"]*)"""', re.DOTALL | re.IGNORECASE)
_SQL_TRIPLE_SINGLE = re.compile(r"'''([^']*(?:SELECT|WITH|INSERT|UPDATE|DELETE|FROM)[^']*)'''", re.DOTALL | re.IGNORECASE)


def _extract_sql_from_agent(agent: Any, response: Any = None) -> str | None:
    """
    Try to get the last executed SQL from the PandasAI agent or response.

    Tries agent.last_query, response.query, then parses generated code (from
    agent.last_generated_code or agent._state.last_code_generated) for
    triple-quoted SQL in execute_sql_query(...) or similar. Returns None if no SQL found.
    """
    # Direct attributes (pandasai-sql or agent may expose these)
    for src in (agent, response):
        if src is None:
            continue
        for attr in ("last_query", "query", "sql", "last_sql"):
            val = getattr(src, attr, None)
            if isinstance(val, str) and val.strip():
                return val.strip()
    # PandasAI v3 Agent stores code on _state; it also exposes last_generated_code property.
    code = None
    for attr in ("last_generated_code", "last_code_generated"):
        code = getattr(agent, attr, None)
        if isinstance(code, str) and code.strip():
            break
    if not code or not code.strip():
        state = getattr(agent, "_state", None)
        if state is not None:
            code = getattr(state, "last_code_generated", None)
    if not isinstance(code, str) or not code.strip():
        return None
    # Prefer match inside execute_query("""...""") or similar
    m = _SQL_IN_TRIPLE_QUOTES.search(code)
    if m:
        return m.group(1).strip()
    for pattern in (_SQL_TRIPLE_DOUBLE, _SQL_TRIPLE_SINGLE):
        m = pattern.search(code)
        if m:
            return m.group(1).strip()
    return None


def run_query(
    agent: Any,
    query: str,
    is_follow_up: bool = False,
) -> EngineResult:
    """
    Run a natural-language query with the given Agent.

    Use agent.chat(query) for the first question in a thread and agent.follow_up(query)
    for subsequent questions so that conversation memory is retained.
    When the user asks for the SQL that generated the previous result, returns stored SQL
    in a code block without calling the agent.

    Parameters
    ----------
    agent : Agent
        PandasAI Agent (e.g. from get_or_create_agent_for_thread).
    query : str
        Natural-language question.
    is_follow_up : bool, optional
        If True, use agent.follow_up(query); otherwise use agent.chat(query).
        Default False.

    Returns
    -------
    EngineResult
        Structured result with response_type ("text", "table", "number", "chart", "error", "csv_file") and value.
    """
    # Return stored SQL in a code block when user asks for it (no agent call).
    if _user_wants_sql(query):
        stored = _last_sql_by_agent.get(id(agent))
        if stored and stored.strip():
            text = "Here is the SQL that was run:\n\n```sql\n" + stored.strip() + "\n```"
            return EngineResult(response_type="text", value=text)
        return EngineResult(
            response_type="text",
            value="No previous query in this thread yet. Ask a data question first, then ask for the SQL.",
        )

    effective_query = query
    if _user_wants_csv(query):
        effective_query = _CSV_INSTRUCTION_PREFIX + query

    try:
        if is_follow_up:
            response = agent.follow_up(effective_query)
        else:
            response = agent.chat(effective_query)
        result = _normalize_response(response)
        result = _try_consume_agent_csv_file(result)
    except Exception as e:
        if type(e).__name__ == "InvalidOutputValueMismatch" or "invalid output type" in str(e).lower():
            return EngineResult(
                response_type="text",
                value="The answer couldn't be formatted. Try asking for one specific question or a single table or number.",
            )
        raise

    # Store last executed SQL for this agent so we can return it when the user asks.
    sql = _extract_sql_from_agent(agent, response)
    if sql:
        _last_sql_by_agent[id(agent)] = sql

    return result
