"""Engine subsystem: run guardrail-passed queries via PandasAI v3 Agent."""

from ttyd_slackbot.engine.runner import (
    EngineResult,
    create_agent,
    get_or_create_agent_for_thread,
    run_query,
)

__all__ = ["EngineResult", "create_agent", "get_or_create_agent_for_thread", "run_query"]
