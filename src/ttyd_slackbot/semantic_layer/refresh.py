"""
Refresh PandasAI v3 semantic layer from Postgres.

Introspects the database, then calls pai.create() for each table.
Reports which tables were created vs already existed (skipped).
"""

import os
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, inspect

# PostgreSQL type name -> PandasAI semantic layer type
PANDASAI_TYPES = {
    "string": [
        "text",
        "varchar",
        "character varying",
        "char",
        "character",
        "uuid",
        "json",
        "jsonb",
        "xml",
        "name",
        "cidr",
        "inet",
        "macaddr",
    ],
    "integer": [
        "smallint",
        "int2",
        "integer",
        "int",
        "int4",
        "bigint",
        "int8",
        "serial",
        "serial4",
        "bigserial",
        "serial8",
    ],
    "float": ["real", "float4", "double precision", "float8", "numeric", "decimal"],
    "boolean": ["boolean", "bool"],
    "datetime": [
        "date",
        "time",
        "timetz",
        "time with time zone",
        "timestamp",
        "timestamp without time zone",
        "timestamptz",
        "timestamp with time zone",
        "interval",
    ],
}


def _pg_type_to_pandasai(pg_type: str) -> str:
    """Map PostgreSQL type name to PandasAI type (string, integer, float, boolean, datetime)."""
    if not pg_type:
        return "string"
    normalized = pg_type.lower().strip()
    for pai_type, pg_names in PANDASAI_TYPES.items():
        if any(n in normalized for n in pg_names):
            return pai_type
    return "string"


def _get_connection_params() -> dict[str, Any]:
    """Build DB connection params from env (DATABASE_URL or DB_*)."""
    url = os.environ.get("DATABASE_URL")
    if url:
        return {"url": url}
    host = os.environ.get("DB_HOST")
    port = os.environ.get("DB_PORT", "5432")
    database = os.environ.get("DB_NAME")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")
    if not all([host, database, user]):
        raise ValueError(
            "Set DATABASE_URL or DB_HOST, DB_NAME, DB_USER (and optionally DB_PORT, DB_PASSWORD)"
        )
    return {
        "host": host,
        "port": int(port),
        "database": database,
        "user": user,
        "password": password or "",
    }


def _build_sqlalchemy_url(params: dict[str, Any]) -> str:
    """Build a SQLAlchemy URL for introspection (real connection)."""
    if "url" in params:
        u = params["url"]
        return u if u.startswith("postgresql") else u.replace("postgres://", "postgresql://", 1)
    return (
        "postgresql://{user}:{password}@{host}:{port}/{database}".format(
            host=params["host"],
            port=params["port"],
            database=params["database"],
            user=params["user"],
            password=params["password"],
        )
    )


def _source_connection_placeholders() -> dict[str, str]:
    """Connection dict with env var placeholders for pai.create() YAML (no secrets)."""
    if os.environ.get("DATABASE_URL"):
        return {"connection_string": "${DATABASE_URL}"}
    return {
        "host": "${DB_HOST}",
        "port": "${DB_PORT}",
        "database": "${DB_NAME}",
        "user": "${DB_USER}",
        "password": "${DB_PASSWORD}",
    }


def _dataset_path_exists(org: str, table_name: str, datasets_dir: Path) -> bool:
    """Return True if datasets/<org>/<table_name> already exists (schema already created)."""
    path = datasets_dir / org / table_name
    return path.is_dir() and (path / "schema.yaml").exists()


def get_tables_and_columns(engine) -> list[tuple[str, list[dict[str, str]]]]:
    """
    Introspect public schema: list of (table_name, columns).

    columns: list of {"name": str, "type": str} (Postgres type).
    """
    inspector = inspect(engine)
    result = []
    for table_name in inspector.get_table_names(schema="public"):
        columns = []
        for col in inspector.get_columns(table_name, schema="public"):
            name = col["name"]
            type_ = col.get("type")
            pg_type = getattr(type_, "__visit_name__", None) or (type_.__class__.__name__ if type_ else "string")
            columns.append({"name": name, "type": str(pg_type)})
        result.append((table_name, columns))
    return result


def run_refresh(
    org: str | None = None,
    schema: str = "public",
    dry_run: bool = False,
    datasets_dir: Path | None = None,
) -> tuple[list[str], list[str]]:
    """
    Refresh semantic layer: for each table, create via pai.create() or skip if exists.

    Returns (created_tables, already_existed_tables).
    """
    import pandasai as pai

    org = org or os.environ.get("SEMANTIC_LAYER_ORG", "ttyd")
    datasets_dir = datasets_dir or Path.cwd() / "datasets"
    datasets_dir = datasets_dir.resolve()

    params = _get_connection_params()
    url = _build_sqlalchemy_url(params)
    engine = create_engine(url)

    if schema != "public":
        # inspector.get_table_names(schema=schema) used below
        pass

    inspector = inspect(engine)
    table_names = inspector.get_table_names(schema=schema)
    created = []
    already_existed = []

    for table_name in table_names:
        path_str = f"{org}/{table_name}"
        if _dataset_path_exists(org, table_name, datasets_dir):
            already_existed.append(table_name)
            continue

        if dry_run:
            created.append(table_name)
            continue

        columns_info = []
        for col in inspector.get_columns(table_name, schema=schema):
            name = col["name"]
            type_ = col.get("type")
            pg_type = getattr(type_, "__visit_name__", None) or (
                type_.__class__.__name__ if type_ else "string"
            )
            pai_type = _pg_type_to_pandasai(str(pg_type))
            columns_info.append(
                {"name": name, "type": pai_type, "description": f"Column {name}"}
            )

        source = {
            "type": "postgres",
            "connection": _source_connection_placeholders(),
            "table": table_name,
            "view": False,
        }
        if schema != "public":
            source["schema"] = schema

        try:
            pai.create(
                path=path_str,
                description=f"Table {table_name}",
                source=source,
                columns=columns_info,
            )
            created.append(table_name)
        except Exception as e:
            msg = str(e).lower()
            if "already exists" in msg or "exist" in msg:
                already_existed.append(table_name)
            else:
                raise

    return (created, already_existed)


def main() -> None:
    """CLI entry: load env, run refresh, print Tables created / Tables already existed."""
    import argparse

    from dotenv import load_dotenv

    load_dotenv()

    parser = argparse.ArgumentParser(description="Refresh PandasAI semantic layer from Postgres")
    parser.add_argument(
        "--org",
        default=os.environ.get("SEMANTIC_LAYER_ORG", "ttyd"),
        help="Organization name for path (default: SEMANTIC_LAYER_ORG or ttyd)",
    )
    parser.add_argument(
        "--schema",
        default="public",
        help="Postgres schema to introspect (default: public)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only list tables that would be created, do not call pai.create()",
    )
    parser.add_argument(
        "--datasets-dir",
        type=Path,
        default=None,
        help="Directory for datasets (default: cwd/datasets)",
    )
    args = parser.parse_args()

    try:
        created, already_existed = run_refresh(
            org=args.org,
            schema=args.schema,
            dry_run=args.dry_run,
            datasets_dir=args.datasets_dir,
        )
    except ValueError as e:
        print(f"Error: {e}", flush=True)
        raise SystemExit(1) from e

    print("Tables created:", ", ".join(created) if created else "(none)")
    print("Tables already existed (skipped):", ", ".join(already_existed) if already_existed else "(none)")


if __name__ == "__main__":
    main()
