<p align="center">
  <img src="docs/logo.svg" width="128" alt="Last-analyser">
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-8b5cf6.svg" alt="License: MIT"></a>
  <img src="https://img.shields.io/badge/Python-3.14-8b5cf6.svg" alt="Python 3.14">
  <img src="https://img.shields.io/badge/SvelteKit-2-8b5cf6.svg" alt="SvelteKit 2">
  <img src="https://img.shields.io/badge/Docker%20Compose-ready-8b5cf6.svg" alt="Docker Compose ready">
</p>

# Last-analyser

Analyses your music listening history locally, even with no internet access, either through the Model Context Protocol (MCP) or an agent loop running on your own machine, against a local or cloud LLM.
Two parts, `scrobble_vault` and `last_llm_service`, with a frontend for both.

## Scrobble vault

Meant to run 24/7 on a low power machine, syncing your last.fm scrobbles into a local Postgres database. It is a REST API you can run on its own.

Its `/vibed-sql-runner` and `/semantic-search` endpoints exist for `last_llm_service` and start by default. Turn them off on the settings page if you only want the vault, or set `LLM_ENDPOINTS_ENABLED='false'` in `.env` and recreate the containers.

## Last LLM service

An OpenAI API format compatible service behind FastAPI, talking to the vault over HTTP, so it works with local and remote providers alike. The same tools are exposed over MCP if you would rather not use the frontend. 

The frontend is the `/` page, letting you interact with the agent through a chatbot stream.

## Video DEMO

https://github.com/user-attachments/assets/4cb2ab93-7b33-429b-a255-fcd428333ea5

## Running it (single machine)

1. Clone the repository and cd into it
2. `docker compose up -d`
3. Open `http://localhost:3000`, go to **Settings**, put in your Last.fm username and [API key](https://www.last.fm/api/account/create)

No files to edit, it starts syncing straight away. For the chat UI, add a model and OpenAI API compatable API key on the Last LLM service card too. The MCP tools work without them.

## Settings

Everything you would normally change is on the settings page: your Last.fm details, how often it syncs, and which model the chat uses. Changes apply after you click save, what you save there wins over anything in `.env`.

On one machine you never need a `.env` ignore `.env.example`

## Running only part of it

Name the parts you want to run with Docker profile.

| Command                               | Starts                             | Needs reachable                                        |
| ------------------------------------- | ---------------------------------- | ------------------------------------------------------ |
| `docker compose up -d`                | everything                         | nothing, it is all here                                |
| `docker compose up -d scrobble_vault` | the vault and its database         | nothing                                                |
| `docker compose up -d front_end`      | the summary and chat pages         | `scrobble_vault`, and `last_llm_api` for the chat page |
| `docker compose up -d last_llm_mcp`   | the MCP server                     | `scrobble_vault`                                       |
| `docker compose up -d last_llm_api`   | the chat API the frontend talks to | `scrobble_vault`                                       |

Combine them like `docker compose up -d last_llm_mcp last_llm_api`.

## Across two machines (to save electricity)

Everything binds to `127.0.0.1` by default and no default can guess where your other machine lives, so this is the one thing `.env` is for. Copy `.env.example` to `.env` on each machine and fill in its half.

Say the vault runs on an always on server, and the UI and the model live on your laptop, over a VPN.

Server:

```bash
git clone https://github.com/bensonchow123/last-analyzer.git && cd last-analyzer
cp .env.example .env
# SCROBBLE_VAULT_BIND_IP  its VPN address, so the laptop can reach it
# ADMIN_API_TOKEN         so the settings page is not left open on the VPN
docker compose up -d scrobble_vault
```

Laptop:

```bash
git clone https://github.com/bensonchow123/last-analyzer.git && cd last-analyzer
cp .env.example .env
# SCROBBLE_VAULT_IPV4        the server's VPN address, how the frontend reaches it
# SCROBBLE_VAULT_URL_DOCKER  http://<that same address>:8000, how the LLM services reach it
# ADMIN_API_TOKEN            the same value as the server
docker compose up -d front_end last_llm_mcp last_llm_api
```

The laptop can be off most of the time. The summary page keeps working without it, only the chat needs it awake. After editing `.env`, run `docker compose up -d --force-recreate` so the containers pick it up.

## Prebuilt images

Every `v1.2.3` tag publishes images to the GitHub container registry, so a machine can pull instead of build:

| Image                                                 | Used by                                                        |
| ----------------------------------------------------- | -------------------------------------------------------------- |
| `ghcr.io/bensonchow123/last-analyzer/scrobble_vault`   | `scrobble_vault`                                               |
| `ghcr.io/bensonchow123/last-analyzer/last_llm_service` | `last_llm_mcp` and `last_llm_api`, they differ only by command |
| `ghcr.io/bensonchow123/last-analyzer/front_end`        | `front_end`                                                    |

`docker compose up -d` builds locally when the image is not already there, so a fresh clone works without touching the registry. To use the published ones, pull first, naming the services that box actually runs:

```bash
docker compose pull scrobble_vault && docker compose up -d scrobble_vault
```

Each tag publishes `1.2.3`, `1.2` and `latest`. Leaving `IMAGE_TAG` unset means `latest`, set it in `.env` to pin:

```bash
IMAGE_TAG='1.2'    # follows 1.2.x
IMAGE_TAG='1.2.3'  # never moves
```

It is per machine, so one box can sit on an older version while another moves ahead. What a container is running shows on the settings page, `dev` if it was built locally.

## Development setup

The same compose file plus an override that bind-mounts the source and runs dev servers, so edits apply without rebuilding:

```bash
docker compose -f docker-compose.yaml -f docker-compose.dev.yaml up -d
```

- Vault API on `http://localhost:8000`, the whole app restarts when a `.py` file changes
- LLM service MCP on `http://localhost:8001/mcp` and chat API on `http://localhost:8002`, restarting on `.py` and `prompts/*.json` changes
- Vite on `http://localhost:5173` with hot module reload, chat at `/`, summary at `/summary`, settings at `/settings`
- The same Postgres data volume as the normal stack

Back to the normal stack with `docker compose down && docker compose up -d`.

## Native development (no Docker for the app)

Only the DB needs Docker. This is the one path that does need a `.env`: outside compose the code defaults to reaching the database at `db`, which only resolves inside the compose network. Copy `.env.example` to `.env` and set `POSTGRES_HOST='localhost'` plus the two database passwords to match your db container.

1. Start the database: `docker compose up -d db`
2. Virtual environment: `python -m venv .venv && source .venv/bin/activate`
3. Dependencies: `pip install -r backend/scrobble_vault/requirements.txt`
4. The vault, from the backend directory: `cd backend && python -m scrobble_vault.main`
5. The frontend: `cd front-end && npm install && npm run dev`
6. The LLM service: `pip install -r backend/last_llm_service/requirements.txt`, then from the backend directory `python -m last_llm_service.main mcp|chat|api`
