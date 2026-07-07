# ClawSentry

Local-first **security & ops posture** dashboard for **OpenClaw**.

- **Grafana-ish at-a-glance UI**: overall health, gateway status, cron, channels, security audit
- **Changes + Timeline**: highlights “what changed” and recent events
- **Metadata-only activity feed** (privacy first): shows message *events* (inbound/outbound, channel, target, voice/file flags) but **never message contents**
- Designed to avoid whole-page horizontal scrolling; cards scroll internally

## URL
Deployed on the OpenClaw host and exposed over your tailnet.

- App: `http://127.0.0.1:3333`
- Tailnet: expose the app with `tailscale serve 3333` and use the HTTPS URL it prints

## Demo checklist (2 minutes)
- Open the page → confirm **Overall Health** bar is green
- Click **Update** tile → confirm version/availability loads
- Check **Cron Jobs** panel → confirm jobs list + last run status renders
- Scroll **Security Audit** findings → confirm it scrolls internally (no page-wide overflow)
- Watch **Timeline** update live (SSE)

## Requirements
- Linux host with systemd
- OpenClaw installed and configured on the same host
- `openclaw` CLI available in PATH

## Run locally (dev)
```bash
git clone git@github.com:ChrisJohnson89/ClawSentry.git clawsentry
cd clawsentry
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 server.py
# open http://127.0.0.1:3333
```

## Run as a service (prod)
Systemd unit currently lives at:
- `/etc/systemd/system/clawboard.service` *(name is legacy; you can rename it later)*

Typical commands:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now clawboard
sudo systemctl status clawboard
sudo journalctl -u clawboard -f
```

## Architecture (high level)
- Backend is a small Python server that **adapts the OpenClaw CLI** into cached JSON endpoints:
  - `/api/status` (OpenClaw status + cron status, cached)
  - `/api/cron` (cron list + recent run, cached)
  - `/api/host` (cheap CPU/mem/load for sparklines)
  - `/api/events` (SSE stream)
- Activity feed is derived by tailing OpenClaw session `.jsonl` logs and extracting **metadata only**.

## Repo
GitHub: `ChrisJohnson89/clawsentry`

If you cloned before the rename, update your remote:
```bash
git remote set-url origin git@github.com:ChrisJohnson89/clawsentry.git
```

## Safety / Privacy
- No message content is displayed or persisted by the dashboard.
- Do not commit secrets (tokens/keys) into this repo.

## If you're an agent (install/run on an OpenClaw host)
This is the no-surprises path to get ClawSentry running on the same box as OpenClaw.

1) Clone
```bash
cd ~/.openclaw/workspace
git clone git@github.com:ChrisJohnson89/ClawSentry.git clawsentry
```

2) Python deps
```bash
cd clawsentry
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3) Run it
```bash
source .venv/bin/activate
python3 server.py
# http://127.0.0.1:3333
```

4) (Optional) Make it a systemd service
- Existing unit on this host may still be named `clawboard.service`.
- If you create a new unit, point `WorkingDirectory=` at the repo folder and `ExecStart=` at `python3 server.py`.

5) (Optional) If GitHub repo was renamed
Set env so the “repo commits” panel tracks the correct slug:
```bash
export CLAWBOARD_GITHUB_REPO=clawsentry
```

## License
MIT.
