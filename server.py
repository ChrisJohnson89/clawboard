"""Clawboard — a local-first dashboard for OpenClaw.

- Serves a small web UI.
- Provides /api/status and /api/events (SSE).

This is intentionally simple: no DB, no secrets on disk.
"""

from __future__ import annotations

import json
import os
import queue
import threading
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional

import requests
from flask import Flask, Response, jsonify, render_template

APP_TITLE = os.getenv("CLAWBOARD_TITLE", "Clawboard")

# Optional: OpenClaw Gateway API
GATEWAY_URL = os.getenv("OPENCLAW_GATEWAY_URL")  # e.g. http://127.0.0.1:18789
GATEWAY_TOKEN = os.getenv("OPENCLAW_GATEWAY_TOKEN")

# Local file-based signals (best effort)
OPENCLAW_DIR = os.path.expanduser(os.getenv("OPENCLAW_DIR", "~/.openclaw"))
RESTART_SENTINEL = os.path.join(OPENCLAW_DIR, "restart-sentinel.json")

app = Flask(__name__)


@dataclass
class Status:
    ts: float
    gateway_ok: bool
    gateway_url: Optional[str]
    gateway_latency_ms: Optional[int]
    gateway_version: Optional[str]
    cron_ok: Optional[bool]
    cron_error: Optional[str]
    last_restart: Optional[Dict[str, Any]]


_event_bus: "queue.Queue[dict]" = queue.Queue(maxsize=500)
_last_status: Optional[Status] = None


def _emit(evt: dict) -> None:
    """Emit an event to SSE listeners (best effort)."""
    evt = dict(evt)
    evt.setdefault("ts", time.time())
    try:
        _event_bus.put_nowait(evt)
    except queue.Full:
        # drop oldest by draining one
        try:
            _event_bus.get_nowait()
        except queue.Empty:
            pass
        try:
            _event_bus.put_nowait(evt)
        except queue.Full:
            pass


def _read_json(path: str) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _gateway_headers() -> Dict[str, str]:
    if not GATEWAY_TOKEN:
        return {}
    return {"Authorization": f"Bearer {GATEWAY_TOKEN}"}




def gateway_version() -> Optional[str]:
    if not GATEWAY_URL:
        return None
    url = GATEWAY_URL.rstrip("/") + "/rpc/version"
    try:
        r = requests.get(url, headers=_gateway_headers(), timeout=2)
        if r.status_code != 200:
            return None
        j = r.json()
        return j.get("version") or j.get("result", {}).get("version")
    except Exception:
        return None


def cron_status() -> tuple[Optional[bool], Optional[str]]:
    if not GATEWAY_URL:
        return (None, None)
    url = GATEWAY_URL.rstrip("/") + "/rpc/cron/status"
    try:
        r = requests.get(url, headers=_gateway_headers(), timeout=2)
        if r.status_code != 200:
            return (False, f"http {r.status_code}")
        j = r.json()
        ok = j.get("ok")
        return (bool(ok), None if ok else (j.get("error") or "cron not ok"))
    except Exception as e:
        return (False, str(e))


def gateway_probe() -> tuple[bool, Optional[int]]:
    """Try to hit the gateway health probe."""
    if not GATEWAY_URL:
        return (False, None)
    url = GATEWAY_URL.rstrip("/") + "/rpc/probe"
    start = time.time()
    try:
        r = requests.get(url, headers=_gateway_headers(), timeout=2)
        ok = r.status_code == 200
    except Exception:
        return (False, None)
    latency_ms = int((time.time() - start) * 1000)
    return (ok, latency_ms)


def compute_status() -> Status:
    global _last_status

    last_restart = _read_json(RESTART_SENTINEL)
    gw_ok, gw_latency = gateway_probe()
    gw_ver = gateway_version()
    cron_ok, cron_err = cron_status()

    st = Status(
        ts=time.time(),
        gateway_ok=gw_ok,
        gateway_url=GATEWAY_URL,
        gateway_latency_ms=gw_latency,
        gateway_version=gw_ver,
        cron_ok=cron_ok,
        cron_error=cron_err,
        last_restart=last_restart,
    )

    _last_status = st
    return st


def status_loop() -> None:
    prev_gateway_ok = None
    prev_restart_ts = None

    while True:
        st = compute_status()

        if prev_gateway_ok is None or st.gateway_ok != prev_gateway_ok:
            _emit({"type": "gateway", "ok": st.gateway_ok, "latency_ms": st.gateway_latency_ms})
            prev_gateway_ok = st.gateway_ok

        # restart sentinel change detection
        try:
            cur = st.last_restart or {}
            cur_ts = cur.get("ts")
            if cur_ts and cur_ts != prev_restart_ts:
                _emit({"type": "restart", "payload": cur})
                prev_restart_ts = cur_ts
        except Exception:
            pass

        time.sleep(2.0)


@app.get("/")
def index():
    return render_template("index.html", title=APP_TITLE)


@app.get("/api/status")
def api_status():
    st = _last_status or compute_status()
    return jsonify(asdict(st))


@app.get("/api/events")
def api_events():
    def gen():
        # initial hello
        yield "event: hello\n"
        yield f"data: {json.dumps({'title': APP_TITLE})}\n\n"

        while True:
            evt = _event_bus.get()
            etype = evt.get("type", "event")
            yield f"event: {etype}\n"
            yield f"data: {json.dumps(evt)}\n\n"

    return Response(gen(), mimetype="text/event-stream")


if __name__ == "__main__":
    threading.Thread(target=status_loop, daemon=True).start()
    port = int(os.getenv("PORT", "3333"))
    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)
