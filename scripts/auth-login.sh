#!/usr/bin/env bash
# OAuth (user-to-machine) login for the Databricks CLI, run inside the dev container.
#
# The CLI waits for the browser redirect on 127.0.0.1:8020 *inside* the container, and a
# published Docker port can't reach a loopback-only listener. socat listens on
# 0.0.0.0:8021 and forwards to it; `make auth-login` publishes host port 8020 to
# container port 8021, so the browser's redirect to http://localhost:8020 reaches the CLI.
set -euo pipefail

host="${1:?usage: auth-login.sh <workspace-url>}"
profile="${DATABRICKS_CONFIG_PROFILE:?set DATABRICKS_CONFIG_PROFILE in .env}"

socat TCP-LISTEN:8021,fork,reuseaddr,bind=0.0.0.0 TCP:127.0.0.1:8020 &
trap 'kill $! 2>/dev/null' EXIT

# There is no browser in the container: BROWSER=none makes the CLI print the URL instead.
# The profile doesn't exist yet, so don't let the env var make the CLI try to load it.
env -u DATABRICKS_CONFIG_PROFILE BROWSER=none \
    databricks auth login --host "$host" --profile "$profile"

echo
echo "Logged in. Profile '$profile':"
databricks auth describe --profile "$profile"
