"""Clawboard — a local-first dashboard for OpenClaw.

Principles:
- Fast + lightweight by default.
- Prefer local sources.
- Use OpenClaw's own CLI to talk to the Gateway reliably.

Why CLI?
- The Gateway's HTTP surface is primarily the Control UI.
- The `openclaw` CLI already knows the right RPC/WebSocket details.

This app:
- Serves a small web UI.
- Provides /api/status and /api/events (SSE).
- Emits a few meaningful events when state changes.

No DB. No secrets committed.
"""

from __future__ import annotations

import json
import os
import queue
import subprocess
import threading
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

from flask import Flask, Response, jsonify, render_template

APP_TITLE = os.getenv("CLAWBOARD_TITLE", "Clawboard")

OPENCLAW_TIMEOUT_SEC = float(os.getenv("CLAWBOARD_OPENCLAW_TIMEOUT", "12"))
CRON_RUNS_LIMIT = int(os.getenv("CLAWBOARD_CRON_RUNS_LIMIT", "8"))

OPENCLAW_DIR = os.path.expanduser(os.getenv("OPENCLAW_DIR", "~/.openclaw"))
RESTART_SENTINEL = os.path.join(OPENCLAW_DIR, "restart-sentinel.json")

app = Flask(__name__)


@dataclass
class Status:
    ts: float

    openclaw_ok: bool
    openclaw_status: Optional[Dict[str, Any]]
    openclaw_error: Optional[str]

    cron_enabled: Optional[bool]
    cron_jobs: Optional[int]
    cron_next_wake_at_ms: Optional[int]

    last_restart: Optional[Dict[str, Any]]


_event_bus: "queue.Queue[dict]" = queue.Queue(maxsize=500)
_last_status: Optional[Status] = None
_last_good_openclaw_status: Optional[dict] = None
_last_good_at: Optional[float] = None
_last_openclaw_status_raw: Optional[dict] = None
_last_openclaw_status_at: Optional[float] = None


def _emit(evt: dict) -> None:
    evt = dict(evt)
    evt.setdefault("ts", time.time())
    try:
        _event_bus.put_nowait(evt)
    except queue.Full:
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


def _run_json(cmd: list[str], timeout: float = OPENCLAW_TIMEOUT_SEC) -> tuple[bool, Optional[dict], Optional[str]]:
    """Run a command expected to output JSON to stdout."""
    try:
        p = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
            text=True,
        )
    except Exception as e:
        return False, None, str(e)

    if p.returncode != 0:
        err = (p.stderr or p.stdout or "").strip()[:800]
        return False, None, f"exit {p.returncode}: {err}"

    try:
        return True, json.loads(p.stdout), None
    except Exception as e:
        return False, None, f"json parse: {e}"


def openclaw_status() -> tuple[bool, Optional[dict], Optional[str]]:
    global _last_openclaw_status_raw, _last_openclaw_status_at
    # cache for a few seconds so /api/activity doesn't block
    if _last_openclaw_status_raw is not None and _last_openclaw_status_at and (time.time() - _last_openclaw_status_at) < 5:
        return True, _last_openclaw_status_raw, None
    ok, j, err = _run_json(["openclaw", "status", "--json"], timeout=OPENCLAW_TIMEOUT_SEC)
    if ok and isinstance(j, dict):
        _last_openclaw_status_raw = j
        _last_openclaw_status_at = time.time()
    return ok, j, err




def openclaw_cron_list() -> Optional[dict]:
    ok, j, _err = _run_json(["openclaw", "cron", "list", "--json"], timeout=OPENCLAW_TIMEOUT_SEC)
    if not ok or not isinstance(j, dict):
        return None
    return j




def recent_sessions_activity(limit: int = 8) -> list[dict]:
    """Return recent session metadata (no message contents)."""
    try:
        j = _last_good_openclaw_status
        if not j or not isinstance(j, dict):
            return []
        recent = (((j.get("sessions") or {}).get("recent")) or [])
        out=[]
        for r in recent[:limit]:
            out.append({
                "kind": r.get("kind"),
                "key": r.get("key"),
                "updatedAt": r.get("updatedAt"),
                "agentId": r.get("agentId"),
                "totalTokens": r.get("totalTokens"),
                "model": r.get("model"),
                "flags": r.get("flags"),
            })
        return out
    except Exception:
        return []


def backfill_events() -> list[dict]:
    """Generate a short initial activity list (metadata only)."""
    ev=[]
    # Recent cron runs (last 2 per job)
    cl = openclaw_cron_list() or {}
    jobs = cl.get("jobs") or []
    for job in jobs:
        jid = job.get("id")
        if not jid:
            continue
        runs = openclaw_cron_runs(jid, limit=2) or {}
        for entry in (runs.get("entries") or []):
            ev.append({
                "type": "cron_run",
                "jobId": jid,
                "jobName": job.get("name"),
                "status": entry.get("status"),
                "summary": entry.get("summary"),
                "runAtMs": entry.get("runAtMs"),
                "durationMs": entry.get("durationMs"),
                "ts": (entry.get("ts") or time.time()),
            })
    # Recent sessions (metadata only)
    for r in recent_sessions_activity(limit=6):
        ev.append({
            "type": "session",
            "ts": time.time(),
            "session": r,
        })
    # Restart sentinel
    rs=_read_json(RESTART_SENTINEL)
    if rs:
        ev.append({"type":"restart","ts":time.time(),"payload":rs})

    # Sort newest-ish
    ev.sort(key=lambda x: x.get("runAtMs") or x.get("ts") or 0, reverse=True)
    return ev[:20]


def openclaw_cron_runs(job_id: str, limit: int = CRON_RUNS_LIMIT) -> Optional[dict]:
    ok, j, _err = _run_json(["openclaw", "cron", "runs", "--id", job_id, "--limit", str(limit)], timeout=OPENCLAW_TIMEOUT_SEC)
    if not ok or not isinstance(j, dict):
        return None
    return j


def openclaw_cron_status() -> tuple[Optional[bool], Optional[int], Optional[int]]:
    ok, j, _err = _run_json(["openclaw", "cron", "status", "--json"], timeout=OPENCLAW_TIMEOUT_SEC)
    if not ok or not isinstance(j, dict):
        return None, None, None
    return bool(j.get("enabled")), j.get("jobs"), j.get("nextWakeAtMs")


def compute_status() -> Status:
    global _last_status

    last_restart = _read_json(RESTART_SENTINEL)

    oc_ok, oc_status, oc_err = openclaw_status()
    cron_enabled, cron_jobs, cron_next = openclaw_cron_status()

    st = Status(
        ts=time.time(),
        openclaw_ok=oc_ok,
        openclaw_status=oc_status,
        openclaw_error=oc_err,
        cron_enabled=cron_enabled,
        cron_jobs=cron_jobs,
        cron_next_wake_at_ms=cron_next,
        last_restart=last_restart,
    )

    _last_status = st
    return st


def status_loop() -> None:
    prev_linked = None
    prev_restart_ts = None
    prev_cron_last_run: dict[str, int] = {}

    while True:
        st = compute_status()

        # Emit events on change
        try:
            linked = None
            if st.openclaw_status and isinstance(st.openclaw_status, dict):
                lc = st.openclaw_status.get("linkChannel")
                if isinstance(lc, dict):
                    linked = bool(lc.get("linked"))

            if linked is not None and linked != prev_linked:
                _emit({"type": "link", "linked": linked})
                prev_linked = linked
        except Exception:
            pass

        try:
            cur = st.last_restart or {}
            cur_ts = cur.get("ts")
            if cur_ts and cur_ts != prev_restart_ts:
                _emit({"type": "restart", "payload": cur})
                prev_restart_ts = cur_ts
        except Exception:
            pass

        time.sleep(3.0)


@app.get("/")
def index():
    return render_template("index.html", title=APP_TITLE)


@app.get("/api/status")
def api_status():
    st = _last_status or compute_status()
    return jsonify(asdict(st))


@app.get("/api/activity")
def api_activity():
    return jsonify({"events": backfill_events()})


@app.get("/api/events")
def api_events():
    def gen():
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
