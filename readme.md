# Last-analyser
A work in progress analyser that analysis your music listerning habits, also providing a RESTful API to interact with your local collection of listerning history.

## Goal
The app is envisioned to be ran in two seperate machines, to be eco-friendly.

### Scrobble vault
The scrobble vault is designed ran 24/7, on a low power usesage computer to sync scrobbles from last.fm to a local Postgres datbase.  
It will function as a seperate restful API that can be ran independantly.  

### Last LLM service
Designed as an OpenAI compatible service with a FastAPI wrapper, communicating with scrobble vault over HTTP.  
So it work with both local and  and remote LLM providers.  
The SvelteKit frontend can then call Last LLM service as the frontend.  
The same tools are also exposed over MCP, for if your cloud LLM provider doesn't provide OpenAI compatable endpoint or if it cost too much.
It has three run modes: `mcp` (MCP server), `api` (chat API for the frontend) and `chat` (terminal REPL for dev).

The chat UI lives in the frontend at `/`: replies stream in over SSE, tool calls show as activity chips, and conversations stay in your browser (localStorage).  
The browser never talks to the chat API directly, a frontend server route proxies it using `LAST_LLM_API_IPV4` (same meaning as `SCROBBLE_VAULT_IPV4`: the address as seen from the frontend's machine).

## Running the stack (Docker)
The whole stack is described in one `docker-compose.yaml`, and profiles select which role a machine runs.

1. Clone the repository and cd into the repository root
2. Copy the env template and fill it in (last.fm credentials, DB passwords): `cp .env.example .env`
3. Start the stack: `docker compose up -d`

By default `docker compose up` starts every profile listed in `COMPOSE_PROFILES` in your `.env`.  
Use a `--profile` flag to narrow the stack to a single role:

| Command | What runs |
| --- | --- |
| `docker compose up -d` | everything in `COMPOSE_PROFILES` |
| `docker compose --profile scrobble_vault up -d` | Postgres + scrobble vault only |
| `docker compose --profile frontend up -d` | frontend only, reaches the vault via `SCROBBLE_VAULT_IPV4` and the chat api via `LAST_LLM_API_IPV4` |
| `docker compose --profile mcp up -d` | MCP server only, reaches the vault via `SCROBBLE_VAULT_URL` |
| `docker compose --profile chat up -d` | MCP server + chat API + the frontend serving the chat UI |

Published ports bind to `127.0.0.1` by default. Set the `*_BIND_IP` variables in `.env` to a trusted interface (for example a VPN address) if another machine needs to reach a service.

## Development setup
Development uses the same compose file plus an override that bind-mounts the source and runs dev servers, so code edits apply without rebuilding images:

```bash
docker compose -f docker-compose.yaml -f docker-compose.dev.yaml up -d
```

What you get:
- Scrobble vault API on `http://localhost:8000`, the whole app restarts automatically when a `.py` file changes
- Last LLM service MCP on `http://localhost:8001/mcp` and chat API on `http://localhost:8002`, restarting on `.py` and `prompts/*.json` changes
- Vite dev server on `http://localhost:5173` with hot module reload for the frontend, chat UI at `/` and the summary at `/summary`
- The same Postgres data volume as the normal stack

To go back to the normal (production style) stack:

```bash
docker compose down && docker compose up -d
```

### Native development (no Docker for the app)
If you prefer running the code directly, only the DB needs Docker:

1. Start the database: `docker compose up -d db`
2. Create and activate a virtual environment: `python -m venv .venv && source .venv/bin/activate`
3. Install dependencies: `pip install -r backend/scrobble_vault/requirements.txt`
4. Start the vault from the backend directory: `cd backend && python -m scrobble_vault.main`
5. Start the frontend dev server: `cd front-end && npm install && npm run dev`
6. For the LLM service: `pip install -r backend/last_llm_service/requirements.txt`, then from the backend directory `python -m last_llm_service.main mcp|chat|api`
