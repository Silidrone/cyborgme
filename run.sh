#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

PORT="${PORT:-8777}"
PIDFILE=".cyborgme.pid"
LOG="cyborgme.log"

# create venv on first run
if [ ! -d .venv ]; then
  echo "→ creating venv + installing deps (first run)…"
  python3 -m venv .venv
  ./.venv/bin/pip install -q --upgrade pip
  ./.venv/bin/pip install -q -r requirements.txt
fi

cmd="${1:-run}"

case "$cmd" in
  -d|--detach|detach|start)
    # already running?
    if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
      echo "→ already running (pid $(cat "$PIDFILE")) on http://localhost:${PORT}"
      exit 0
    fi
    # fully detached: survives terminal close; logs to $LOG; no --reload for stability
    setsid nohup ./.venv/bin/uvicorn server:app --host 127.0.0.1 --port "${PORT}" \
      > "$LOG" 2>&1 < /dev/null &
    echo $! > "$PIDFILE"
    sleep 2
    if kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
      echo "→ CyborgMe running detached (pid $(cat "$PIDFILE")) on http://localhost:${PORT}"
      echo "  logs:  tail -f $(pwd)/$LOG"
      echo "  stop:  ./run.sh stop"
    else
      echo "✗ failed to start — check $LOG"; tail -n 20 "$LOG"; exit 1
    fi
    ;;
  stop)
    if [ -f "$PIDFILE" ]; then
      kill "$(cat "$PIDFILE")" 2>/dev/null && echo "→ stopped (pid $(cat "$PIDFILE"))" || echo "→ not running"
      rm -f "$PIDFILE"
    else
      pkill -f "uvicorn server:app" && echo "→ stopped" || echo "→ not running"
    fi
    ;;
  status)
    if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
      echo "→ running (pid $(cat "$PIDFILE")) on http://localhost:${PORT}"
    else
      echo "→ not running"
    fi
    ;;
  run|*)
    echo "→ CyborgMe on http://localhost:${PORT}  (Ctrl+C to stop)"
    # --reload picks up code edits; transcript + insight diagnostics print here
    exec ./.venv/bin/uvicorn server:app --host 127.0.0.1 --port "${PORT}" --reload
    ;;
esac
