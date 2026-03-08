"""
Slack app for the Intake subsystem.

Uses Bolt with Socket Mode to receive message events. Applies intake guardrails
(regex-based PII blocklist and LLM query interpretation), maintains per-thread
conversation memory, and replies with either a block reason or the output-layer
result only (engine result formatted for Slack).
"""

import logging
import os

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from ttyd_slackbot.engine import get_or_create_agent_for_thread, run_query
from ttyd_slackbot.intake.guardrails import check_guardrails
from ttyd_slackbot.intake.memory import append_message, get_messages
from ttyd_slackbot.intake.schema_loader import get_schema_summary
from ttyd_slackbot.output import prepare_for_slack

logger = logging.getLogger(__name__)

# Schema summary loaded once at first use and reused
_schema_summary: str | None = None


def _get_schema_summary() -> str:
    """Return schema summary for guardrails; load once and cache."""
    global _schema_summary
    if _schema_summary is None:
        _schema_summary = get_schema_summary()
    return _schema_summary


def _get_sender_display_name(event: dict, context) -> str:
    """
    Resolve Slack user ID to display name via users_info; fall back to 'there' on failure.
    """
    user_id = event.get("user")
    if not user_id:
        return "there"
    if context is None or not getattr(context, "client", None):
        return "there"
    try:
        response = context.client.users_info(user=user_id)
        user = response.get("user") if isinstance(response, dict) else None
        if not user:
            return "there"
        profile = user.get("profile") or {}
        name = profile.get("display_name") or user.get("real_name")
        return (name or "").strip() or "there"
    except Exception:
        return "there"


# Required env vars (loaded by caller via load_dotenv): SLACK_BOT_TOKEN, SLACK_APP_TOKEN
def _get_app() -> App:
    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        raise ValueError("SLACK_BOT_TOKEN is required")
    return App(token=token)


def _handle_message(event: dict, say, context) -> None:
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
    raw_query = result.get("raw_query") or text
    is_follow_up = any(m.get("role") == "assistant" for m in messages)
    if not is_follow_up:
        name = _get_sender_display_name(event, context)
        initial_message = (
            f"Hi! Thanks for your message {name}. I'm loading the data and am looking into it."
        )
        say(initial_message, thread_ts=thread_ts)
        append_message(channel_id, thread_ts, "assistant", initial_message)
    try:
        agent = get_or_create_agent_for_thread(channel_id, thread_ts)
        engine_result = run_query(agent, raw_query, is_follow_up=is_follow_up)
        text, file_bytes, file_name = prepare_for_slack(
            engine_result,
            messages=messages,
            interpreted_query=interpreted,
        )
        if file_bytes is not None and file_name is not None:
            context.client.files_upload_v2(
                channel=channel_id,
                content=file_bytes,
                filename=file_name,
                thread_ts=thread_ts,
            )
        say(text, thread_ts=thread_ts)
        append_message(channel_id, thread_ts, "assistant", text)
    except Exception as e:
        logger.exception("Engine failed for query %s: %s", raw_query[:100], e)
        fallback = "I couldn't run the query right now. Please try again later."
        say(fallback, thread_ts=thread_ts)
        append_message(channel_id, thread_ts, "assistant", fallback)


def run() -> None:
    """Start the Intake Slack app in Socket Mode. Blocks until shutdown."""
    app = _get_app()
    app.event("message")(_handle_message)

    app_token = os.environ.get("SLACK_APP_TOKEN")
    if not app_token:
        raise ValueError("SLACK_APP_TOKEN is required for Socket Mode")

    handler = SocketModeHandler(app, app_token)
    handler.start()
