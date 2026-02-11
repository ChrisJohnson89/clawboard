# ClawSentry (formerly Clawboard)

Local-first **security & ops posture** dashboard for **OpenClaw**.

- **Grafana-ish at-a-glance UI**: overall health, gateway status, cron, channels, security audit
- **Changes + Timeline**: highlights “what changed” and recent events
- **Metadata-only activity feed** (privacy first): shows message *events* (inbound/outbound, channel, target, voice/file flags) but **never message contents**
- Designed to avoid whole-page horizontal scrolling; cards scroll internally

## Screenshot / URL
Deployed on the OpenClaw host and exposed over Tailnet.

- App: `http://127.0.0.1:3333`
- Tailnet (Serve): `https://ip-172-31-17-58.tail23fb1f.ts.net:8444/`

## Requirements
- Linux host with systemd
- OpenClaw installed and configured on the same host
- `openclaw` CLI available in PATH

## Run locally (dev)
```bash
cd clawboard
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 server.py
# open http://127.0.0.1:3333
```

## Run as a service (prod)
Systemd unit lives at:
- `/etc/systemd/system/clawboard.service`

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

## Naming
The repo may still be named `clawboard`, but the product name is **ClawSentry**.
If/when the GitHub repo is renamed to `clawsentry`, update your git remote:
```bash
git remote set-url origin git@github.com:ChrisJohnson89/clawsentry.git
```

## Safety / Privacy
- No message content is displayed or persisted by the dashboard.
- Do not commit secrets (tokens/keys) into this repo.

## License
TBD.
