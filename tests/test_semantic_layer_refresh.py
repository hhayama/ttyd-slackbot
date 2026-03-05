"""Tests for semantic layer refresh (type mapping, connection params, path check)."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from ttyd_slackbot.semantic_layer.refresh import (
    _build_sqlalchemy_url,
    _dataset_path_exists,
    _get_connection_params,
    _pg_type_to_pandasai,
    _source_connection_placeholders,
)


def test_pg_type_to_pandasai_string():
    """String-like Postgres types map to string."""
    for pg in ("text", "varchar", "character varying", "char", "uuid", "jsonb"):
        assert _pg_type_to_pandasai(pg) == "string"
    assert _pg_type_to_pandasai("unknown") == "string"
    assert _pg_type_to_pandasai("") == "string"


def test_pg_type_to_pandasai_integer():
    """Integer-like Postgres types map to integer."""
    for pg in ("int2", "int4", "int8", "smallint", "integer", "bigint", "serial", "bigserial"):
        assert _pg_type_to_pandasai(pg) == "integer"


def test_pg_type_to_pandasai_float():
    """Float-like Postgres types map to float."""
    for pg in ("real", "float4", "double precision", "float8", "numeric", "decimal"):
        assert _pg_type_to_pandasai(pg) == "float"


def test_pg_type_to_pandasai_boolean():
    """Boolean Postgres types map to boolean."""
    assert _pg_type_to_pandasai("boolean") == "boolean"
    assert _pg_type_to_pandasai("bool") == "boolean"


def test_pg_type_to_pandasai_datetime():
    """Date/time Postgres types map to datetime."""
    for pg in ("date", "time", "timestamp", "timestamptz", "timestamp with time zone"):
        assert _pg_type_to_pandasai(pg) == "datetime"


def test_get_connection_params_uses_database_url():
    """When DATABASE_URL is set, params contain url."""
    with patch.dict(os.environ, {"DATABASE_URL": "postgresql://u:p@h:5432/db"}, clear=False):
        params = _get_connection_params()
    assert params == {"url": "postgresql://u:p@h:5432/db"}


def test_get_connection_params_uses_db_vars():
    """When DB_* are set, params contain host, port, database, user, password."""
    with patch.dict(
        os.environ,
        {
            "DB_HOST": "localhost",
            "DB_PORT": "5433",
            "DB_NAME": "mydb",
            "DB_USER": "u",
            "DB_PASSWORD": "p",
        },
        clear=False,
    ):
        params = _get_connection_params()
    assert params["host"] == "localhost"
    assert params["port"] == 5433
    assert params["database"] == "mydb"
    assert params["user"] == "u"
    assert params["password"] == "p"


def test_get_connection_params_raises_when_missing():
    """Raises ValueError when neither DATABASE_URL nor required DB_* are set."""
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError, match="DATABASE_URL or DB_HOST"):
            _get_connection_params()


def test_build_sqlalchemy_url_from_url():
    """Builds URL from params with url key."""
    url = _build_sqlalchemy_url({"url": "postgres://u:p@h:5432/db"})
    assert "postgresql" in url
    assert "u" in url and "p" in url and "h" in url and "db" in url


def test_build_sqlalchemy_url_from_parts():
    """Builds URL from host, port, database, user, password."""
    url = _build_sqlalchemy_url(
        {
            "host": "h",
            "port": 5432,
            "database": "db",
            "user": "u",
            "password": "p",
        }
    )
    assert "postgresql://u:p@h:5432/db" == url


def test_source_connection_placeholders_with_db_vars():
    """Placeholders use env var names when DATABASE_URL not set."""
    with patch.dict(os.environ, {}, clear=True):
        os.environ.pop("DATABASE_URL", None)
    conn = _source_connection_placeholders()
    assert conn.get("host") == "${DB_HOST}"
    assert conn.get("user") == "${DB_USER}"


def test_source_connection_placeholders_with_database_url():
    """Placeholders use DATABASE_URL when set."""
    with patch.dict(os.environ, {"DATABASE_URL": "postgresql://x"}, clear=False):
        conn = _source_connection_placeholders()
    assert conn.get("connection_string") == "${DATABASE_URL}"


def test_dataset_path_exists_true(tmp_path):
    """Returns True when org/table dir exists with schema.yaml."""
    (tmp_path / "myorg" / "mytable").mkdir(parents=True)
    (tmp_path / "myorg" / "mytable" / "schema.yaml").write_text("x")
    assert _dataset_path_exists("myorg", "mytable", tmp_path) is True


def test_dataset_path_exists_false_no_dir(tmp_path):
    """Returns False when org/table dir does not exist."""
    assert _dataset_path_exists("myorg", "mytable", tmp_path) is False


def test_dataset_path_exists_false_no_yaml(tmp_path):
    """Returns False when dir exists but schema.yaml does not."""
    (tmp_path / "myorg" / "mytable").mkdir(parents=True)
    assert _dataset_path_exists("myorg", "mytable", tmp_path) is False
