# Last-analyser
A work in progress analyser that hopefully replaces spotify recommendation for me.

## Goal
The app is envisioned to be ran in two seperate machines, to be eco-friendly.

### Scrobble vault
The scrobble vault is designed ran 24/7, on a low power usesage computer to sync scrobbles from last.fm.
It will function as a seperate restful API that can be ran independantly.

### Last LLM service
Current envisioned to be OpenAI API compatabile with a fastapi wrapper to communicate with scrobble vault.
Which then a frontend made with sveltekit can be used to interact with Last LLM service, to analyse the data.