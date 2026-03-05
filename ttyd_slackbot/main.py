"""
Entry point for Talk to your Data Slackbot.

Loads credentials from .env, then starts the Intake subsystem (Slack app in Socket Mode).
"""

from dotenv import load_dotenv

from ttyd_slackbot.intake import run as run_intake


def main() -> None:
    load_dotenv()
    run_intake()


if __name__ == "__main__":
    main()
