# Production image for ttyd-slackbot (Background Worker on Render).
# Python 3.11 matches pyproject.toml requires-python ">=3.10,<3.12".
FROM python:3.11-slim

RUN pip install --no-cache-dir poetry

WORKDIR /app

# Install dependencies and the app (requires src/ for the package).
COPY pyproject.toml poetry.lock ./
COPY src ./src
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

# Datasets (schema YAMLs) are read at runtime; default path is ./datasets.
COPY datasets ./datasets

# Matplotlib non-GUI backend (also set in main.py; safe for worker threads).
ENV MPLBACKEND=Agg

CMD ["ttyd-slackbot"]
