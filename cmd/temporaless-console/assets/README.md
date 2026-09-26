# Embedded UI

`make build-console` builds `ui/` into `assets/dist/` (ignored by Git) and
then the console binary, which embeds it. A binary built without it still
serves the read-only API and a page that says how to build the UI.
