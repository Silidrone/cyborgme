# CyborgMe — real-time call assistant

Listens to **your mic (ME)** + **system audio / the other party (THEM)**, transcribes
both live with Deepgram, and uses Claude to surface quick facts/answers about whatever
you're discussing. Localhost web GUI.

## Setup
1. Add two keys to `/home/silidrone/silisoft/cyborgme/.env` (copy from `.env.example`)
   or to the main `/home/silidrone/silisoft/.env`:
   - `DEEPGRAM_API_KEY` — https://console.deepgram.com (free credits on signup)
   - `ANTHROPIC_API_KEY` — https://console.anthropic.com
2. `./run.sh`  (first run builds a venv + installs deps)
3. Open http://localhost:8777

## How it works
- **Left column** = live transcript, colour-coded ME vs THEM, interim text shown ghosted.
- **Right column** = AI assist: auto "insights" fire a few seconds after a burst of speech,
  plus answers to anything you type in the box at the bottom (uses the transcript as context).

## Tips
- **Use headphones.** On speakers, your mic also picks up the other party, so THEM gets
  duplicated into ME. Headphones keep the two channels clean.
- "THEM" = whatever plays through your default output (Zoom/Meet/Teams/browser audio).
- Tune `INSIGHT_DEBOUNCE` (lower = snappier, chattier; higher = calmer).
- For smarter but slower answers: `ANTHROPIC_MODEL=claude-sonnet-4-6`.
- Wrong device? Set `MIC_DEVICE` / `SYSTEM_DEVICE` in `.env` (list with `pactl list sources short`).
