# Build context is ./backend so that /app is the import root and the package
# resolves as `scrobble_vault.*`, matching how it is run locally.
FROM python:3.14-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Dependencies first, so the layer is cached across source changes.
COPY scrobble_vault/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY scrobble_vault/ ./scrobble_vault/

# changes every release and would bust the layers above it, `dev` for local build
ARG APP_VERSION=dev
ENV APP_VERSION=$APP_VERSION

CMD ["python", "-m", "scrobble_vault.main"]
