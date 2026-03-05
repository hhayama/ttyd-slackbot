"""
Slack app for the Intake subsystem.

Uses Bolt with Socket Mode to receive message events. For this phase,
only receives and logs raw message text; parsing and guardrails come later.
"""

import logging
import os

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

logger = logging.getLogger(__name__)

# Required env vars (loaded by caller via load_dotenv): SLACK_BOT_TOKEN, SLACK_APP_TOKEN
def _get_app() -> App:
    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        raise ValueError("SLACK_BOT_TOKEN is required")
    return App(token=token)


def _handle_message(event: dict, _context) -> None:
    """Handle incoming message events. Ignores bot messages; logs user text."""
    if event.get("bot_id"):
        return
    text = event.get("text") or ""
    # Skip empty or irrelevant subtypes (e.g. channel_join) if needed
    subtype = event.get("subtype", "")
    if subtype in ("bot_message", "message_changed", "message_deleted"):
        return
    logger.info("Intake received message: %s", text[:200] + ("..." if len(text) > 200 else ""))


def run() -> None:
    """Start the Intake Slack app in Socket Mode. Blocks until shutdown."""
    app = _get_app()
    app.event("message")(_handle_message)

    app_token = os.environ.get("SLACK_APP_TOKEN")
    if not app_token:
        raise ValueError("SLACK_APP_TOKEN is required for Socket Mode")

    handler = SocketModeHandler(app, app_token)
    handler.start()
