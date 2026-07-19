# Build context is ./backend so that /app is the import root and the package
# resolves as `last_llm_service.*`, matching how it is run locally.
FROM python:3.14-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Dependencies first, so the layer is cached across source changes.
COPY last_llm_service/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY last_llm_service/ ./last_llm_service/

# One image, two roles: compose overrides the command for the api container
CMD ["python", "-m", "last_llm_service.main", "mcp", "--transport", "http"]
