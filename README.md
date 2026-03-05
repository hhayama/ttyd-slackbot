# Talk to your Data Slackbot

A Slack bot that receives data-related questions and returns verified answers (and optional diagrams). See [PROJECT_CONTEXT.md](PROJECT_CONTEXT.md) for architecture and scope.

## Environment variables

Load from `.env` in the project root (see [.env.example](.env.example)).

| Variable | Required | Description |
|----------|----------|-------------|
| `SLACK_BOT_TOKEN` | Yes | Bot user OAuth token (starts with `xoxb-`). |
| `SLACK_APP_TOKEN` | Yes (Socket Mode) | App-level token (starts with `xapp-`) with `connections:write`. |

For Socket Mode (default): enable it in your Slack app settings and set both tokens. No public URL is required.

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
