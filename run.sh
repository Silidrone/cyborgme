#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

# create venv on first run
if [ ! -d .venv ]; then
  echo "→ creating venv + installing deps (first run)…"
  python3 -m venv .venv
  ./.venv/bin/pip install -q --upgrade pip
  ./.venv/bin/pip install -q -r requirements.txt
fi

PORT="${PORT:-8777}"
echo "→ CyborgMe on http://localhost:${PORT}"
# --reload picks up code edits automatically; transcript + insight diagnostics print here
exec ./.venv/bin/uvicorn server:app --host 127.0.0.1 --port "${PORT}" --reload
