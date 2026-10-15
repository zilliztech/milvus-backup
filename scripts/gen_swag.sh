#!/usr/bin/env bash

# The v1 wire types are plain Go structs in core/apiv1, so swag resolves them
# like any other package; no source rewriting is needed before or after the
# run. Run from the repository root.
set -e

swag init --generalInfo core/server/server.go
