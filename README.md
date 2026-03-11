# Talk to your Data Slackbot

A Slack bot that receives data-related questions and returns verified answers (and optional diagrams). Users ask in Slack; the bot checks data availability via a semantic layer, runs queries with PandasAI v3, and posts verified answers and optional diagrams, with input/output guardrails and per-thread conversation memory. See [PROJECT_CONTEXT.md](PROJECT_CONTEXT.md) for architecture and scope.

## Environment variables

Load from `.env` in the project root (see [.env.example](.env.example)).

| Variable | Required | Description |
|----------|----------|-------------|
| `SLACK_BOT_TOKEN` | Yes | Bot user OAuth token (starts with `xoxb-`). |
| `SLACK_APP_TOKEN` | Yes (Socket Mode) | App-level token (starts with `xapp-`) with `connections:write`. |
| `OPENAI_API_KEY` | Yes | Used by the engine (PandasAI LLM), guardrails, and PII checks. |
| `DATABASE_URL` or `DB_*` | For data queries | Either `DATABASE_URL` or `DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` (and optionally `DB_PORT`). Required to answer data questions and for semantic layer refresh. |
| `SEMANTIC_LAYER_ORG` | No | Organization name in semantic layer path (default: `ttyd`). |
| `DATASETS_DIR` | No | Root directory for datasets (default: `./datasets`). |
| `SLACK_DEBUG_QUERY_ERRORS` | No | Set to `1` to see step-level, sanitized error messages in Slack when a query fails (no keys/tokens). |

For Socket Mode (default): enable it in your Slack app settings and set both Slack tokens. No public URL is required.

## Run

```bash
poetry install
poetry run ttyd-slackbot
```

Or: `python -m ttyd_slackbot.main`

## Refresh semantic layer from the database

To populate or refresh the PandasAI v3 semantic layer from your Postgres database (one schema per table under `datasets/<org>/<table_name>/`), set the DB-related env vars (see [.env.example](.env.example): `DATABASE_URL` or `DB_HOST`, `DB_NAME`, `DB_USER`, etc.), then run:

```bash
poetry run ttyd-semantic-refresh
```

- **Tables created:** new tables get a schema and are listed at the end.
- **Tables already existed (skipped):** existing schema dirs are left unchanged and listed so you know what was skipped.

Options: `--org` (default: `SEMANTIC_LAYER_ORG` or `ttyd`), `--schema` (Postgres schema, default `public`), `--dry-run` (list tables only, no `pai.create()`), `--datasets-dir` (override output directory). Example: `poetry run ttyd-semantic-refresh --dry-run`

## Deploying on Render

You can run the bot as a **Background Worker** on [Render](https://render.com/) using the repo’s Docker setup.

1. **Connect the repo:** In the Render Dashboard, create a new **Background Worker**. Connect your GitHub repo (or use **Blueprint** and apply [render.yaml](render.yaml)).
2. **Use Docker:** Set the runtime to **Docker**. Render will build from the repo’s [Dockerfile](Dockerfile) (no extra build command).
3. **Set environment variables:** In the service’s **Environment** tab, add the following. Mark secret values as **Secret** so they are encrypted and hidden in the UI.
   - **Required:** `SLACK_BOT_TOKEN`, `SLACK_APP_TOKEN`, `OPENAI_API_KEY`
   - **For data queries:** `DATABASE_URL` (or `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`)
   - **Optional:** `SEMANTIC_LAYER_ORG` (default: `ttyd`), `DATASETS_DIR` (default: `./datasets` in the container)

Variable names and descriptions match [.env.example](.env.example). After saving, deploy; the worker runs `ttyd-slackbot` and stays up (Slack Socket Mode). To debug query failures (e.g. wrong credentials or setup), set `SLACK_DEBUG_QUERY_ERRORS=1` in the worker environment; the bot will post which step failed and a short sanitized error message in Slack (no keys or tokens are shown).

## Development

- **Tests:** From the repo root, run `pytest -q`. [pyproject.toml](pyproject.toml) sets `pythonpath = ["src"]` so tests discover the package.
- **Verify PandasAI load:** With DB vars (or `DATABASE_URL`) and `.env` set, run `poetry run python scripts/verify_pandasai_load.py` to confirm PandasAI v3 resolves `${VAR}` in schema YAML from the environment.
