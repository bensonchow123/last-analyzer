# Last-analyser
A work in progress analyser that analysis your music listerning habits, also providing a RESTful API to interact with your local collection of your music listerning habbit.

## Goal
The app is envisioned to be ran in two seperate machines, to be eco-friendly.

### Scrobble vault
The scrobble vault is designed ran 24/7, on a low power usesage computer to sync scrobbles from last.fm to a local Postgres datbase.  
It will function as a seperate restful API that can be ran independantly.  

#### Scrobble vault manual install (Linux only, use WSL2 if on Windows)
Currently the scrobble vault is completed, but the Docker automated setup is not complete.  
To start the scrobble vault:  
1. Clone the repository and cd into repository root
2. Create your Python virtual environment, and activate it `python -m venv .venv`
3. Install Docker and Docker compose
4. Start the postgres DB and run the initialization shell script, `docker compose up -d`
5. Install dependencies `pip install -r backend/scrobble_vault/requirements.txt`
6. Change directory to the backend directory `cd backend`
7. Start the Scrobble vault API `python -m scrobble_vault.main`

### Last LLM service
Current envisioned to be OpenAI API compatabile with a fastapi wrapper to communicate with scrobble vault.  
It will then support both local LLM models and remote LLM models.  
Which then a frontend made with sveltekit can be used to interact with Last LLM service, to analyse the data.  