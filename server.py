"""
CyborgMe — real-time call assistant.

Captures your microphone ("ME") and system audio / call participants ("THEM"),
streams both to Deepgram for live transcription, shows everything in a localhost
web GUI, and uses Claude to surface quick facts/answers about topics as they come up.

Run:  ./run.sh   (or: uvicorn server:app --port 8777)
"""

import asyncio
import io
import json
import os
import subprocess
import time
import contextlib
import zipfile

import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
from anthropic import AsyncAnthropic

load_dotenv()
load_dotenv("/home/silidrone/silisoft/.env")  # also pull from the main project .env

# ----------------------------------------------------------------------------- config
DEEPGRAM_API_KEY = os.environ.get("DEEPGRAM_API_KEY", "").strip()
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "").strip()
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5").strip()
LANGUAGE = os.environ.get("LANGUAGE", "en").strip()
CONTEXT = os.environ.get("CONTEXT", "").strip()  # situational brief injected into Claude's prompts
SAMPLE_RATE = 16000
INSIGHT_DEBOUNCE = float(os.environ.get("INSIGHT_DEBOUNCE", "3.0"))  # seconds of quiet before auto-insight

claude = AsyncAnthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None


def detect_devices():
    """Return (mic_device, system_monitor_device), honoring env overrides."""
    mic = os.environ.get("MIC_DEVICE", "").strip()
    system = os.environ.get("SYSTEM_DEVICE", "").strip()
    try:
        if not mic:
            mic = subprocess.check_output(["pactl", "get-default-source"], text=True).strip()
        if not system:
            sink = subprocess.check_output(["pactl", "get-default-sink"], text=True).strip()
            system = sink + ".monitor"
    except Exception as e:
        print("device detect error:", e)
    return mic, system


# ----------------------------------------------------------------------------- shared state
class Hub:
    def __init__(self):
        self.clients: set[WebSocket] = set()
        self.transcript: list[dict] = []        # finalized lines: {speaker, text, ts}
        self.recent_insights: list[str] = []     # last few insight texts (for de-dup)
        self.ai_log: list[dict] = []              # all AI output: {kind, text, ts, question?}
        self.last_insight_idx = 0                 # index into transcript already summarized
        self.new_final = asyncio.Event()
        self.mode = os.environ.get("DEFAULT_MODE", "interactive").strip()  # "interactive" | "answers"
        # runtime-editable config (env provides the initial defaults)
        self.context = CONTEXT
        self.keyterms = [t.strip() for t in os.environ.get("KEYTERMS", "").split(",") if t.strip()]
        self.stream_tasks: list = []              # live Deepgram capture tasks (for reconnect)

    async def broadcast(self, obj: dict):
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_text(json.dumps(obj))
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)


hub = Hub()


# ----------------------------------------------------------------------------- deepgram stream
def _bool_env(name: str, default: str = "false") -> str:
    return "true" if os.environ.get(name, default).strip().lower() in ("1", "true", "yes") else "false"


def dg_url() -> str:
    from urllib.parse import quote
    params = {
        "model": os.environ.get("DEEPGRAM_MODEL", "nova-3").strip(),
        "language": LANGUAGE,
        "encoding": "linear16",
        "sample_rate": str(SAMPLE_RATE),
        "channels": "1",
        "interim_results": "true",        # instant live text
        "smart_format": "true",           # readable dates/times/numbers
        "punctuate": "true",
        "numerals": "true",               # "eight thousand" -> "8000"
        "endpointing": os.environ.get("DG_ENDPOINTING", "100").strip(),  # snappy but not choppy
        "utterance_end_ms": os.environ.get("DG_UTTERANCE_END_MS", "1000").strip(),
        "vad_events": "true",             # speech-start events
        "diarize": _bool_env("DG_DIARIZE"),  # speaker-change labels (multi-person calls)
    }
    q = "&".join(f"{k}={v}" for k, v in params.items())
    # keyterm prompting (nova-3): bias toward names/jargon likely on this call. Up to 100.
    for t in hub.keyterms[:100]:
        q += "&keyterm=" + quote(t)
    # find&replace: fix predictable mistranscriptions, e.g. REPLACE="fast api:FastAPI,fast a p i:FastAPI"
    for pair in [p.strip() for p in os.environ.get("REPLACE", "").split(",") if ":" in p]:
        q += "&replace=" + quote(pair)
    return f"wss://api.deepgram.com/v1/listen?{q}"


async def run_stream(speaker: str, device: str):
    """Continuously capture `device` via parec and transcribe with Deepgram."""
    while True:
        proc = None
        try:
            await hub.broadcast({"type": "status", "text": f"connecting {speaker} ({device})"})
            # auth via subprotocol avoids websockets header-API version churn
            async with websockets.connect(dg_url(), subprotocols=["token", DEEPGRAM_API_KEY],
                                          max_size=None, ping_interval=5) as ws:
                proc = await asyncio.create_subprocess_exec(
                    "parec", "-d", device,
                    "--format=s16le", f"--rate={SAMPLE_RATE}", "--channels=1",
                    "--latency-msec=30",
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
                )
                await hub.broadcast({"type": "status", "text": f"live: {speaker}"})

                async def pump_audio():
                    while True:
                        chunk = await proc.stdout.read(3200)  # ~100ms @ 16k mono s16
                        if not chunk:
                            break
                        await ws.send(chunk)

                async def pump_text():
                    async for msg in ws:
                        try:
                            data = json.loads(msg)
                        except Exception:
                            continue
                        if data.get("type") != "Results":
                            continue
                        alt = (data.get("channel", {}).get("alternatives") or [{}])[0]
                        text = (alt.get("transcript") or "").strip()
                        if not text:
                            continue
                        if data.get("is_final"):
                            line = {"speaker": speaker, "text": text, "ts": time.time()}
                            hub.transcript.append(line)
                            print(f"[final {speaker:4}] {text}", flush=True)
                            await hub.broadcast({"type": "final", **line})
                            hub.new_final.set()
                        else:
                            await hub.broadcast({"type": "interim", "speaker": speaker, "text": text})

                await asyncio.gather(pump_audio(), pump_text())
        except Exception as e:
            await hub.broadcast({"type": "status", "text": f"{speaker} stream error: {e} — retrying"})
        finally:
            if proc and proc.returncode is None:
                with contextlib.suppress(Exception):
                    proc.kill()
        await asyncio.sleep(2)  # backoff before reconnect


# ----------------------------------------------------------------------------- claude insight loop
# Mode "interactive": proactively drop facts/trivia/context about whatever is being discussed,
# reacting to EITHER speaker and taking the whole discussion into account.
FACTS_SYSTEM = (
    "You are a covert real-time research assistant helping the user during a live call. "
    "You receive a rolling transcript where [ME] is the user and [THEM] is the other party. "
    "Following the whole discussion (whoever is talking), surface NEW, genuinely useful little facts, "
    "trivia, definitions, or context about the topics, terms, companies, people, or numbers being discussed. "
    "Think 'whisper in the ear'. "
    "Hard rules: at most 3 bullet points, each <= 18 words, no preamble, no restating what was said. "
    "If nothing is worth adding right now, reply with exactly: SKIP"
)

# Mode "answers": stay silent UNLESS a question was just asked — either the other party [THEM] asked it,
# or the user [ME] explicitly repeated/asked it. Then give an answer the user can say.
AUTO_ANSWER_SYSTEM = (
    "You are a covert real-time interview assistant. A question was just asked in the conversation — "
    "either the interviewer [THEM] asked the user [ME], or [ME] repeated/asked a question out loud. "
    "Give [ME] a concise, senior-level answer they can say out loud: 2-5 short bullets or sentences, "
    "concrete and technically correct, no preamble. "
    "If no actual question was asked, reply with exactly: SKIP"
)

_QUESTION_STARTERS = (
    "what", "why", "how", "when", "where", "who", "which", "whose", "can you", "could you",
    "do you", "did you", "would you", "have you", "are you", "is there", "tell me", "explain",
    "describe", "walk me", "give me", "talk me", "let's say", "suppose", "imagine", "what if",
)


def is_question(text: str) -> bool:
    t = text.strip().lower()
    if not t:
        return False
    if "?" in t:
        return True
    return any(t.startswith(s) for s in _QUESTION_STARTERS)


def render_transcript(lines: list[dict], limit: int = 30) -> str:
    out = []
    for ln in lines[-limit:]:
        tag = "ME" if ln["speaker"] == "me" else "THEM"
        out.append(f"[{tag}] {ln['text']}")
    return "\n".join(out)


async def _generate(system: str, prompt: str, kind: str):
    try:
        resp = await claude.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=300,
            system=system + (f"\n\nSituational context:\n{hub.context}" if hub.context else ""),
            messages=[{"role": "user", "content": prompt}],
        )
        text = "".join(b.text for b in resp.content if getattr(b, "type", "") == "text").strip()
    except Exception as e:
        await hub.broadcast({"type": "status", "text": f"insight error: {e}"})
        return
    if not text or text.upper().startswith("SKIP"):
        print(f"[insight] {kind}: SKIP (nothing to add)", flush=True)
        return
    print(f"[insight] {kind}: emitted ({len(text)} chars)", flush=True)
    hub.recent_insights.append(text)
    hub.ai_log.append({"kind": kind, "text": text, "ts": time.time()})
    await hub.broadcast({"type": "insight", "kind": kind, "text": text, "ts": time.time()})


async def insight_loop():
    if not claude:
        await hub.broadcast({"type": "status", "text": "no ANTHROPIC_API_KEY — insights disabled"})
        return
    while True:
        await hub.new_final.wait()
        hub.new_final.clear()
        await asyncio.sleep(INSIGHT_DEBOUNCE)  # batch a burst of speech together
        hub.new_final.clear()

        if hub.last_insight_idx >= len(hub.transcript):
            continue
        new_lines = hub.transcript[hub.last_insight_idx:]
        hub.last_insight_idx = len(hub.transcript)
        context = render_transcript(hub.transcript, limit=30)
        print(f"[insight] mode={hub.mode} reacting to {len(new_lines)} new line(s)", flush=True)

        if hub.mode == "answers":
            # React only when a question was just asked — by THEM, or repeated by ME.
            new_text = " ".join(l["text"] for l in new_lines).strip()
            if not (is_question(new_text) or any(is_question(l["text"]) for l in new_lines)):
                continue
            prompt = (f"Conversation so far:\n{context}\n\n"
                      f"What was just said:\n\"{new_text}\"\n\nGive me an answer I can say.")
            await _generate(AUTO_ANSWER_SYSTEM, prompt, kind="answer")
        else:  # interactive
            new_part = render_transcript(new_lines, limit=30)
            avoid = ""
            if hub.recent_insights:
                avoid = "\n\nDo NOT repeat anything similar to these you already gave:\n- " + \
                        "\n- ".join(hub.recent_insights[-6:])
            prompt = (f"Recent conversation:\n{context}\n\n"
                      f"Newest lines to react to:\n{new_part}{avoid}")
            await _generate(FACTS_SYSTEM, prompt, kind="fact")


# ----------------------------------------------------------------------------- manual question
ASK_SYSTEM = (
    "You are a fast research assistant aiding the user mid-call. Use the conversation transcript "
    "as context. Answer the user's question concisely and concretely. Prefer bullets and short "
    "sentences. No fluff."
)


async def handle_ask(question: str, qid: str):
    if not claude:
        await hub.broadcast({"type": "answer_token", "id": qid, "text": "(no ANTHROPIC_API_KEY set)"})
        await hub.broadcast({"type": "answer_done", "id": qid})
        return
    context = render_transcript(hub.transcript, limit=50)
    prompt = f"Conversation so far:\n{context}\n\nMy question: {question}"
    await hub.broadcast({"type": "answer_start", "id": qid, "question": question})
    chunks = []
    try:
        async with claude.messages.stream(
            model=ANTHROPIC_MODEL,
            max_tokens=600,
            system=ASK_SYSTEM + (f"\n\nSituational context:\n{hub.context}" if hub.context else ""),
            messages=[{"role": "user", "content": prompt}],
        ) as stream:
            async for delta in stream.text_stream:
                chunks.append(delta)
                await hub.broadcast({"type": "answer_token", "id": qid, "text": delta})
    except Exception as e:
        await hub.broadcast({"type": "answer_token", "id": qid, "text": f"\n[error: {e}]"})
    hub.ai_log.append({"kind": "manual", "question": question, "text": "".join(chunks), "ts": time.time()})
    await hub.broadcast({"type": "answer_done", "id": qid})


# ----------------------------------------------------------------------------- web app
app = FastAPI()
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


@app.get("/")
async def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


def _fmt_ts(ts: float) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ts))


def _build_export() -> bytes:
    """Build a zip with 4 files: me, them, combined transcript, AI output."""
    me_lines, them_lines, combined = [], [], []
    for ln in hub.transcript:
        stamp = _fmt_ts(ln["ts"])
        if ln["speaker"] == "me":
            me_lines.append(f"[{stamp}] {ln['text']}")
        else:
            them_lines.append(f"[{stamp}] {ln['text']}")
        tag = "ME  " if ln["speaker"] == "me" else "THEM"
        combined.append(f"[{stamp}] {tag}: {ln['text']}")

    ai_lines = []
    for e in hub.ai_log:
        stamp = _fmt_ts(e["ts"])
        if e["kind"] == "manual":
            ai_lines.append(f"[{stamp}] Q: {e['question']}\n           A: {e['text']}\n")
        elif e["kind"] == "answer":
            ai_lines.append(f"[{stamp}] SUGGESTED ANSWER: {e['text']}\n")
        else:
            ai_lines.append(f"[{stamp}] INSIGHT: {e['text']}\n")

    files = {
        "me.txt": "\n".join(me_lines),
        "them.txt": "\n".join(them_lines),
        "transcript.txt": "\n".join(combined),
        "ai.txt": "\n".join(ai_lines),
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for name, content in files.items():
            z.writestr(name, content + "\n")
    return buf.getvalue()


@app.get("/export")
async def export():
    fname = "cyborgme-" + time.strftime("%Y%m%d-%H%M%S") + ".zip"
    return Response(
        content=_build_export(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    hub.clients.add(ws)
    await ws.send_text(json.dumps({"type": "status", "text": "connected"}))
    await ws.send_text(json.dumps({"type": "mode", "mode": hub.mode}))
    await ws.send_text(json.dumps(_config_msg()))
    try:
        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)
            if msg.get("type") == "ask" and msg.get("text", "").strip():
                qid = str(time.time())
                asyncio.create_task(handle_ask(msg["text"].strip(), qid))
            elif msg.get("type") == "mode" and msg.get("mode") in ("interactive", "answers"):
                hub.mode = msg["mode"]
                await hub.broadcast({"type": "mode", "mode": hub.mode})
            elif msg.get("type") == "config":
                await _apply_config(msg)
    except WebSocketDisconnect:
        pass
    finally:
        hub.clients.discard(ws)


def _config_msg() -> dict:
    return {"type": "config", "context": hub.context, "keyterms": ", ".join(hub.keyterms)}


async def _apply_config(msg: dict):
    """Update runtime context/keyterms from the UI; reconnect Deepgram if keyterms changed."""
    if "context" in msg:
        hub.context = (msg.get("context") or "").strip()
    kt_changed = False
    if "keyterms" in msg:
        new_kt = [t.strip() for t in (msg.get("keyterms") or "").split(",") if t.strip()]
        kt_changed = new_kt != hub.keyterms
        hub.keyterms = new_kt
    print(f"[config] context={len(hub.context)} chars, {len(hub.keyterms)} keyterms"
          f"{' (reconnecting streams)' if kt_changed else ''}", flush=True)
    await hub.broadcast(_config_msg())
    if kt_changed:
        asyncio.create_task(_restart_streams())


async def _start_streams():
    mic, system = detect_devices()
    print(f"MIC   (ME)   = {mic}\nSYSTEM(THEM) = {system}", flush=True)
    hub.stream_tasks = [
        asyncio.create_task(run_stream("me", mic)),
        asyncio.create_task(run_stream("them", system)),
    ]


async def _restart_streams():
    for t in hub.stream_tasks:
        t.cancel()
    await asyncio.sleep(0.3)  # let parec subprocesses die + sockets close
    await _start_streams()
    await hub.broadcast({"type": "status", "text": "reconnected with updated keyterms"})


@app.on_event("startup")
async def startup():
    if not DEEPGRAM_API_KEY:
        print("\n!!! DEEPGRAM_API_KEY is not set — transcription will not start.\n")
        return
    await _start_streams()
    asyncio.create_task(insight_loop())
