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
