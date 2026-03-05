# This is a prompt for the Agent. 

You are helping me write a concise PROJECT_CONTEXT.md file for my AI agent project.

I will provide you with three things:

1. The **purpose of the agent** (what problem it solves and for whom).
2. A **short summary** of my design choices and how the system meets user needs.
3. The **architecture diagram** of it (systems, subsystems, components).

Using this information, generate a **clear and structured Markdown file** that gives overall context about the project.

The file should include:

- **Project Overview** – what this agent does and why it exists.
- **System Scope** – what’s in scope and what’s not.
- **Architecture Summary** – a short description of the system’s main subsystems and how they interact.
- **Key Inputs and Outputs** – what goes in, what comes out.
- **Design Rationale** – how the design supports user needs or constraints.

Do **not** include implementation details, code, or specific instructions about how to build it — the goal is only to capture *context* for development.

Format the output as a Markdown document suitable for saving as @PROJECT_CONTEXT.md:

Talk to you Data Slack Bot

Purpose: A Slack Bot that will take data related questions via Slack and will verify if the data are availble, make queries to data, interpret findings or create diagrams if necessary and post the output back to the users via Slack.

Short summary: The agent had 4 subsystems: intake, engine, semantic_layer, and output. The intake subsystem will parse and interpret the input and apply any guardrails to ensure accuracy and safety of the input. It will also ensure any data being asked for is available via checking the semantic_layer subsystem and will ask the user for clarification or inform the user of any data availability. The engine subsystem will handle the planning of which datasets to query and any reasoning to analyse data or creating figures to address the query.  The semantic_layer subsystem will contain information about tables that define entities, relationships, and join logic.  The output subsystem will handle any formatting of the output and guardrails for safety and accuracy