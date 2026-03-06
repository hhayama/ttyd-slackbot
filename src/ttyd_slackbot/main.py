"""
Entry point for Talk to your Data Slackbot.

Loads credentials from .env, then starts the Intake subsystem (Slack app in Socket Mode).
"""

import os

from dotenv import load_dotenv

from ttyd_slackbot.intake import run as run_intake


def main() -> None:
    load_dotenv()
    # Use non-GUI backend so Matplotlib (e.g. in PandasAI chart code) is safe on worker threads.
    os.environ.setdefault("MPLBACKEND", "Agg")
    run_intake()


if __name__ == "__main__":
    main()
