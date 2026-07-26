# Last-analyser
An music analyser is designed to analysis your music listerning history locally, even with no internet access, either through Model Context protocal (MCP) or running the agent loop locally with your local or cloud based LLM model.  
Consisting of two parts, `scrobble_vault` and `last_llm_service` and a frontend to interact with them.

### Scrobble vault
The scrobble vault is designed ran 24/7, on a low power usage computer to sync scrobbles from last.fm to a local Postgres datbase.  
It will function as a seperate restful API that can be ran independantly.  
The vault's `/vibed-sql-runner` and `/semantic-search` endpoints used by `last_llm_services` starts by default, turn them off on the settings page if you only need the vault.  
Setting `LLM_ENDPOINTS_ENABLED='false'` in `.env` does the same thing, but you have to recreate the containers after so they pick it up.

### Last LLM service
Designed as an OpenAI API compatible service with a FastAPI wrapper, communicating with scrobble vault over HTTP.  
So it work with both local and  and remote LLM providers.  
The same tools are also exposed over MCP, for if you don't want to use the frontend in this app.  
It has three run modes: `mcp` (MCP server), `api` (chat API for the frontend) and `chat` (terminal REPL for dev).

The chat UI lives in the frontend at `/`: replies stream in over SSE, tool calls show as activity chips, and conversations stay in your browser (localStorage).  
The browser never talks to the chat API directly, a frontend server route proxies it using `LAST_LLM_API_IPV4` (same meaning as `SCROBBLE_VAULT_IPV4`: the address as seen from the frontend's machine).

## Running it (single machine setup)
1. Clone the repository and cd into it
2. `docker compose --profile full up -d`
3. Open `http://localhost:3000`, go to **Settings**, put in your Last.fm username and [API key](https://www.last.fm/api/account/create)

That is it, no files to edit. It starts syncing your scrobbles straight away.  
Only if you want the chat UI, add a model and API key on the Last LLM service card too. The MCP tools work without them.

## Settings
Everything you would normally change lives on the settings page: your Last.fm details, how often it syncs, and which model the chat uses.  
Changes apply straight away, nothing restarts, and they are kept between restarts. What you save there wins over anything in `.env`.

On one machine you never need a `.env` at all, and `.env.example` is there for the one case that does: splitting the stack across two machines, because nothing can guess the other machine's address. Changing ports or the database passwords goes there too, but most people will not.

## Running only part of it
Profiles pick which parts start:

| Command | What runs |
| --- | --- |
| `docker compose --profile full up -d` | everything |
| `docker compose --profile scrobble_vault up -d` | database + scrobble vault |
| `docker compose --profile frontend up -d` | frontend only |
| `docker compose --profile mcp up -d` | MCP server only |
| `docker compose --profile chat up -d` | MCP server + chat API + the chat UI |

## Across two machines (to save electricity)
This is what `.env` is for, and the only thing it is for. Everything binds to `127.0.0.1` by default so only that machine can reach it, and no default can guess where your other machine lives. Copy `.env.example` to `.env` on each machine and fill in its half.

Say the vault runs on a server and the UI on your laptop, over a VPN:

- on the server, `SCROBBLE_VAULT_BIND_IP` set to its VPN address so the laptop can reach it
- on the laptop, `SCROBBLE_VAULT_IPV4` set to that same address
- the same `ADMIN_API_TOKEN` on both, so the settings page is not left open on the VPN
- `COMPOSE_PROFILES` on each machine listing what that machine runs

After editing `.env`, run `docker compose up -d --force-recreate` so the containers pick it up.

## Development setup
Development uses the same compose file plus an override that bind-mounts the source and runs dev servers, so code edits apply without rebuilding images:

```bash
docker compose -f docker-compose.yaml -f docker-compose.dev.yaml --profile full up -d
```

What you get:
- Scrobble vault API on `http://localhost:8000`, the whole app restarts automatically when a `.py` file changes
- Last LLM service MCP on `http://localhost:8001/mcp` and chat API on `http://localhost:8002`, restarting on `.py` and `prompts/*.json` changes
- Vite dev server on `http://localhost:5173` with hot module reload for the frontend, chat UI at `/`, the summary at `/summary` and settings at `/settings`
- The same Postgres data volume as the normal stack

To go back to the normal (production style) stack:

```bash
docker compose down && docker compose --profile full up -d
```

### Native development (no Docker for the app)
If you prefer running the code directly, only the DB needs Docker.  
This is the one path that does need a `.env`: outside compose the code defaults to reaching the database at `db`, which only resolves inside the compose network. Copy `.env.example` to `.env` and set `POSTGRES_HOST='localhost'` plus the two database passwords to match your db container.

1. Start the database: `docker compose up -d db`
2. Create and activate a virtual environment: `python -m venv .venv && source .venv/bin/activate`
3. Install dependencies: `pip install -r backend/scrobble_vault/requirements.txt`
4. Start the vault from the backend directory: `cd backend && python -m scrobble_vault.main`
5. Start the frontend dev server: `cd front-end && npm install && npm run dev`
6. For the LLM service: `pip install -r backend/last_llm_service/requirements.txt`, then from the backend directory `python -m last_llm_service.main mcp|chat|api`
