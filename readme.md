# Last-analyser
A work in progress analyser that hopefully replaces spotify recommendation for me.

## Goal
The app is envisioned to be ran in two seperate machines, to be eco-friendly.
### Scrobble vault
The scrobble vault is designed ran 24/7, on a low power usesage computer to sync scrobbles from last.fm

### Last_llm_service
Current envisioned to use llama.cpp with a fast api wrapper to communicate with scrobble vault.
Which then a frontend made with sveltekit can be used to interact with Last_llm_service.