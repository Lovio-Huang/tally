FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
RUN useradd --system --uid 1001 app

# Dependencies first, from the lock file: every build gets the versions CI tested, and
# this layer is only rebuilt when the lock changes, not on every code change.
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY pyproject.toml README.md ./
COPY src src
COPY migrations migrations
RUN pip install --no-deps . && pip check

USER app
EXPOSE 8000
# Ready means the app answers and can reach the database (/health runs SELECT 1).
# `docker compose up --wait` waits for this.
HEALTHCHECK --interval=3s --timeout=3s --start-period=10s --retries=10 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2)"]
# At startup the app applies pending migrations if its database role may (the owner), under
# an advisory lock; connected as the runtime role (compose.yaml) it only checks none are pending.
CMD ["tally", "serve", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
