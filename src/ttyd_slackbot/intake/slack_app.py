"""
Slack app for the Intake subsystem.

Uses Bolt with Socket Mode to receive message events. Applies intake guardrails
(regex-based PII blocklist and LLM query interpretation), maintains per-thread
conversation memory, and replies with either a block reason or the output-layer
result only (engine result formatted for Slack).
"""

import logging
import os
import re

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from ttyd_slackbot.engine import get_or_create_agent_for_thread, run_query
from ttyd_slackbot.intake.guardrails import check_guardrails
from ttyd_slackbot.intake.help_intent import is_help_intent
from ttyd_slackbot.intake.help_response import load_help_response
from ttyd_slackbot.intake.memory import append_message, get_messages
from ttyd_slackbot.intake.schema_loader import get_schema_summary
from ttyd_slackbot.output import prepare_for_slack

logger = logging.getLogger(__name__)

# Schema summary loaded once at first use and reused
_schema_summary: str | None = None
# Help response (what can be queried) loaded once at first use
_help_response: str | None = None


def _get_help_response() -> str:
    """Return help response for 'what can I ask' intent; load once and cache."""
    global _help_response
    if _help_response is None:
        _help_response = load_help_response()
    return _help_response


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
        # SlackResponse is dict-like but not isinstance(response, dict); use .get() when available
        getter = getattr(response, "get", None) if response is not None else None
        user = response.get("user") if getter else (response.get("user") if isinstance(response, dict) else None)
        if not user:
            return "there"
        profile = user.get("profile") or {}
        name = profile.get("display_name") or user.get("real_name")
        return (name or "").strip() or "there"
    except Exception as e:
        if "missing_scope" in str(e) and "users:read" in str(e):
            logger.info(
                "users_info failed due to missing_scope: add 'users:read' under Bot Token Scopes at api.slack.com/apps → your app → OAuth & Permissions, then reinstall the app."
            )
        return "there"


# Required env vars (loaded by caller via load_dotenv): SLACK_BOT_TOKEN, SLACK_APP_TOKEN
def _get_app() -> App:
    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        raise ValueError("SLACK_BOT_TOKEN is required")
    return App(token=token)


def _is_debug_query_errors() -> bool:
    """Return True if step-level error messages should be shown in Slack (for debugging)."""
    val = os.environ.get("SLACK_DEBUG_QUERY_ERRORS", "").strip().lower()
    return val in ("1", "true", "yes", "on", "enabled")


def _sanitize_error_message(e: Exception) -> str:
    """
    Return a safe one-line error string for Slack: exception type + redacted message.
    Never includes keys, tokens, or passwords.
    """
    msg = str(e).strip()
    # Flatten to one line
    msg = " ".join(msg.split())
    # Redact Slack tokens (xoxb-, xapp-, xoxp- and following segment)
    msg = re.sub(r"xox[bap]-[a-zA-Z0-9.-]+", "***", msg)
    # Redact API key prefixes (OpenAI, etc.)
    msg = re.sub(r"sk-[a-zA-Z0-9.-]+", "***", msg)
    msg = re.sub(r"sk_proj-[a-zA-Z0-9.-]+", "***", msg)
    # Redact password=value and api_key=value
    msg = re.sub(r"password=[^\s&]+", "password=***", msg, flags=re.IGNORECASE)
    msg = re.sub(r"api_key=[^\s&]+", "api_key=***", msg, flags=re.IGNORECASE)
    # Redact :password@ or :user:password@ in URLs (keep :***@)
    msg = re.sub(r":([^:@\s]{4,})@", ":***@", msg)
    # Redact long token-like segments (20+ alphanumeric) after sensitive keywords
    msg = re.sub(
        r"(token|key|secret|password)\s*[=:]\s*[a-zA-Z0-9_-]{20,}",
        r"\1=***",
        msg,
        flags=re.IGNORECASE,
    )
    # Truncate message part to avoid leakage at end
    max_msg = 120
    if len(msg) > max_msg:
        msg = msg[: max_msg - 3].rstrip() + "..."
    name = type(e).__name__
    if msg:
        return f"{name}: {msg}"
    return name


def _build_error_fallback(step_label: str, e: Exception) -> str:
    """Build fallback message for Slack; when debug on include sanitized reason."""
    if _is_debug_query_errors():
        reason = _sanitize_error_message(e)
        return f"Query failed while {step_label}. {reason}"
    return "I couldn't run the query right now. Please try again later."


def _post_fallback_and_append(
    channel_id: str,
    thread_ts: str,
    message_ts: str | None,
    context,
    say,
    fallback: str,
) -> None:
    """Update or post the fallback message to Slack and append to conversation memory."""
    if message_ts and getattr(context, "client", None):
        try:
            context.client.chat_update(
                channel=channel_id, ts=message_ts, text=fallback
            )
        except Exception as update_err:
            logger.warning("chat_update failed on error path: %s", update_err)
            say(fallback, thread_ts=thread_ts)
    else:
        say(fallback, thread_ts=thread_ts)
    append_message(channel_id, thread_ts, "assistant", fallback)


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

    if is_help_intent(text):
        help_content = _get_help_response()
        say(help_content, thread_ts=thread_ts)
        append_message(channel_id, thread_ts, "assistant", help_content)
        return

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
        placeholder = (
            f"Hi! Thanks for your message {name}. I'm loading the data and am looking into it."
        )
    else:
        placeholder = "Thinking..."
    post_response = say(placeholder, thread_ts=thread_ts)
    message_ts = post_response.get("ts") if post_response else None

    if _is_debug_query_errors():
        # Step-wise try/except so we can report which step failed (with sanitized reason).
        try:
            agent = get_or_create_agent_for_thread(channel_id, thread_ts)
        except Exception as e:
            logger.exception("Engine failed for query %s: %s", raw_query[:100], e)
            fallback = _build_error_fallback("creating the agent", e)
            _post_fallback_and_append(
                channel_id, thread_ts, message_ts, context, say, fallback
            )
            return
        try:
            engine_result = run_query(agent, raw_query, is_follow_up=is_follow_up)
        except Exception as e:
            logger.exception("Engine failed for query %s: %s", raw_query[:100], e)
            fallback = _build_error_fallback("running the query", e)
            _post_fallback_and_append(
                channel_id, thread_ts, message_ts, context, say, fallback
            )
            return
        try:
            text, file_bytes, file_name = prepare_for_slack(
                engine_result,
                messages=messages,
                interpreted_query=interpreted,
            )
        except Exception as e:
            logger.exception("Engine failed for query %s: %s", raw_query[:100], e)
            fallback = _build_error_fallback("formatting the response", e)
            _post_fallback_and_append(
                channel_id, thread_ts, message_ts, context, say, fallback
            )
            return
        try:
            if message_ts and getattr(context, "client", None):
                try:
                    context.client.chat_update(
                        channel=channel_id, ts=message_ts, text=text
                    )
                except Exception as update_err:
                    logger.warning(
                        "chat_update failed, posting new message: %s", update_err
                    )
                    say(text, thread_ts=thread_ts)
            else:
                say(text, thread_ts=thread_ts)
            if file_bytes is not None and file_name is not None:
                context.client.files_upload_v2(
                    channel=channel_id,
                    content=file_bytes,
                    filename=file_name,
                    thread_ts=thread_ts,
                )
            append_message(channel_id, thread_ts, "assistant", text)
        except Exception as e:
            logger.exception("Engine failed for query %s: %s", raw_query[:100], e)
            fallback = _build_error_fallback("sending the reply", e)
            _post_fallback_and_append(
                channel_id, thread_ts, message_ts, context, say, fallback
            )
            return
    else:
        # Single try/except; generic fallback (no step or error detail in Slack).
        try:
            agent = get_or_create_agent_for_thread(channel_id, thread_ts)
            engine_result = run_query(agent, raw_query, is_follow_up=is_follow_up)
            text, file_bytes, file_name = prepare_for_slack(
                engine_result,
                messages=messages,
                interpreted_query=interpreted,
            )
            if message_ts and getattr(context, "client", None):
                try:
                    context.client.chat_update(
                        channel=channel_id, ts=message_ts, text=text
                    )
                except Exception as update_err:
                    logger.warning(
                        "chat_update failed, posting new message: %s", update_err
                    )
                    say(text, thread_ts=thread_ts)
            else:
                say(text, thread_ts=thread_ts)
            if file_bytes is not None and file_name is not None:
                context.client.files_upload_v2(
                    channel=channel_id,
                    content=file_bytes,
                    filename=file_name,
                    thread_ts=thread_ts,
                )
            append_message(channel_id, thread_ts, "assistant", text)
        except Exception as e:
            logger.exception("Engine failed for query %s: %s", raw_query[:100], e)
            debug_val = os.environ.get("SLACK_DEBUG_QUERY_ERRORS", "")
            logger.warning(
                "Sending generic fallback. Set SLACK_DEBUG_QUERY_ERRORS=1 to see step and error detail in Slack. Current value: %r",
                debug_val if debug_val else "(unset)",
            )
            fallback = _build_error_fallback("", e)  # generic message when debug off
            _post_fallback_and_append(
                channel_id, thread_ts, message_ts, context, say, fallback
            )


def run() -> None:
    """Start the Intake Slack app in Socket Mode. Blocks until shutdown."""
    debug_val = os.environ.get("SLACK_DEBUG_QUERY_ERRORS", "").strip()
    if _is_debug_query_errors():
        logger.info(
            "SLACK_DEBUG_QUERY_ERRORS=%r -> step-level error messages will be shown in Slack",
            debug_val or "(empty)",
        )
    else:
        logger.info(
            "SLACK_DEBUG_QUERY_ERRORS=%r -> generic error message only (set to 1 or true for step-level detail)",
            debug_val if debug_val else "(unset)",
        )
    app = _get_app()
    app.event("message")(_handle_message)

    app_token = os.environ.get("SLACK_APP_TOKEN")
    if not app_token:
        raise ValueError("SLACK_APP_TOKEN is required for Socket Mode")

    handler = SocketModeHandler(app, app_token)
    handler.start()
