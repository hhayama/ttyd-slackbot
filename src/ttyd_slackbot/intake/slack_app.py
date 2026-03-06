"""
Slack app for the Intake subsystem.

Uses Bolt with Socket Mode to receive message events. Applies LLM guardrails
(PII and schema availability), maintains per-thread conversation memory, and
replies with either a block reason or a success message that repeats the query.
"""

import logging
import os

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from ttyd_slackbot.intake.guardrails import check_guardrails
from ttyd_slackbot.intake.memory import append_message, get_messages
from ttyd_slackbot.intake.schema_loader import get_schema_summary

logger = logging.getLogger(__name__)

# Schema summary loaded once at first use and reused
_schema_summary: str | None = None


def _get_schema_summary() -> str:
    """Return schema summary for guardrails; load once and cache."""
    global _schema_summary
    if _schema_summary is None:
        _schema_summary = get_schema_summary()
    return _schema_summary


# Required env vars (loaded by caller via load_dotenv): SLACK_BOT_TOKEN, SLACK_APP_TOKEN
def _get_app() -> App:
    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        raise ValueError("SLACK_BOT_TOKEN is required")
    return App(token=token)


def _handle_message(event: dict, say, _context) -> None:
    """Handle incoming message events. Ignores bot messages; runs guardrails; sends reply."""
    if event.get("bot_id"):
        return
    text = event.get("text") or ""
    # Skip empty or irrelevant subtypes (e.g. channel_join) if needed
    subtype = event.get("subtype", "")
    if subtype in ("bot_message", "message_changed", "message_deleted"):
        return
    logger.info("Intake received message: %s", text[:200] + ("..." if len(text) > 200 else ""))

    channel_id = event["channel"]
    thread_ts = event.get("thread_ts") or event["ts"]
    append_message(channel_id, thread_ts, "user", text)
    messages = get_messages(channel_id, thread_ts)
    schema_summary = _get_schema_summary()
    result = check_guardrails(messages, schema_summary)

    if not result["allowed"]:
        reason = result["reason"] or "Your query could not be processed. Please try again."
        say(reason, thread_ts=thread_ts)
        append_message(channel_id, thread_ts, "assistant", reason)
        return

    interpreted = result["interpreted_query"] or text
    success_msg = f"There are no issues with your query. You asked: {interpreted}."
    say(success_msg, thread_ts=thread_ts)
    append_message(channel_id, thread_ts, "assistant", success_msg)


def run() -> None:
    """Start the Intake Slack app in Socket Mode. Blocks until shutdown."""
    app = _get_app()
    app.event("message")(_handle_message)

    app_token = os.environ.get("SLACK_APP_TOKEN")
    if not app_token:
        raise ValueError("SLACK_APP_TOKEN is required for Socket Mode")

    handler = SocketModeHandler(app, app_token)
    handler.start()
