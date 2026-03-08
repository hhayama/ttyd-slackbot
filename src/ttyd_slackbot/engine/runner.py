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
"""


# Regex for agent "CSV file saved as <filename>.csv" message.
_CSV_SAVED_PATTERN = re.compile(r"(?i)CSV file saved as\s+(.+\.csv)")


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


def _build_resolved_schemas_dir(datasets_dir: Path, org: str, names: list[str]) -> Path:
    """
    Write resolved schema.yaml files (with ${VAR} replaced from env) to a temp dir.

    Structure: tempdir/datasets/<org>/<name>/schema.yaml. Returns the temp dir path
    (caller must chdir into it so PandasAI sees datasets/ and can pai.load(org/name)).
    """
    tmpdir = Path(tempfile.mkdtemp(prefix="ttyd_schemas_"))
    try:
        for name in names:
            src = datasets_dir / org / name / "schema.yaml"
            with open(src, encoding="utf-8") as f:
                data = yaml.safe_load(f)
            if data is None:
                continue
            resolved = _resolve_placeholders(data)
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
    pai.config.set({"llm": llm})

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


def run_query(
    agent: Any,
    query: str,
    is_follow_up: bool = False,
) -> EngineResult:
    """
    Run a natural-language query with the given Agent.

    Use agent.chat(query) for the first question in a thread and agent.follow_up(query)
    for subsequent questions so that conversation memory is retained.

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
        Structured result with response_type ("text", "table", "number", "chart", "error") and value.
    """
    if is_follow_up:
        response = agent.follow_up(query)
    else:
        response = agent.chat(query)
    result = _normalize_response(response)
    result = _try_consume_agent_csv_file(result)
    return result
