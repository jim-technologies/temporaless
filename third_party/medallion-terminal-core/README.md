# Vendored terminal-core contract

The protobuf files under `proto/` are copied byte for byte from the public
[`jim-technologies/medallion-terminal-core`](https://github.com/jim-technologies/medallion-terminal-core)
repository at tag `v0.6.0` (commit `5b86bb0ee5f2365a979310d5e00204fcfb3ef357`),
under its Apache-2.0 license (`LICENSE`).

They are the dashboard contract the optional read-only console
(`adapters/go/console`) serves so a generic dashboard template can render
executions. Only the console adapter uses them; the Temporaless core and its
SDKs do not.

To update: replace the three files from a newer tag, run `make generate`
(which regenerates `adapters/go/console/internal/terminalv1` and the
console's embedded descriptor sets), and record the tag here. Never edit the
files in place.
