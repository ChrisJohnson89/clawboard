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

import requests

APP_TITLE = os.getenv("CLAWBOARD_TITLE", "Clawboard")

OPENCLAW_TIMEOUT_SEC = float(os.getenv("CLAWBOARD_OPENCLAW_TIMEOUT", "12"))
CRON_RUNS_LIMIT = int(os.getenv("CLAWBOARD_CRON_RUNS_LIMIT", "8"))
STATUS_POLL_SEC = float(os.getenv("CLAWBOARD_STATUS_POLL_SEC", "30.0"))
TELEGRAM_TAIL_POLL_SEC = float(os.getenv("CLAWBOARD_TELEGRAM_TAIL_POLL_SEC", "3.0"))
CRON_STATUS_TTL_SEC = float(os.getenv("CLAWBOARD_CRON_STATUS_TTL_SEC", "10.0"))

OPENCLAW_DIR = os.path.expanduser(os.getenv("OPENCLAW_DIR", "~/.openclaw"))
RESTART_SENTINEL = os.path.join(OPENCLAW_DIR, "restart-sentinel.json")

# Public repos to show commit activity for (metadata only)
REPO_ACTIVITY = [
    ("ChrisJohnson89", "clawboard"),
    ("ChrisJohnson89", "Ferromon"),
    ("ChrisJohnson89", "openclaw-configs"),
]

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

    host_cpu_pct: Optional[float]
    host_mem_used_bytes: Optional[int]
    host_mem_total_bytes: Optional[int]
    host_load1: Optional[float]

    last_restart: Optional[Dict[str, Any]]


_event_bus: "queue.Queue[dict]" = queue.Queue(maxsize=500)
_clients_connected: int = 0
_last_client_at: float = 0.0
_client_lock = threading.Lock()

_last_status: Optional[Status] = None
_last_good_openclaw_status: Optional[dict] = None
_last_good_at: Optional[float] = None
_last_openclaw_status_raw: Optional[dict] = None
_last_openclaw_status_at: Optional[float] = None
_activity_cache: list[dict] = []
_activity_cache_at: Optional[float] = None

_cron_cache: Optional[dict] = None
_cron_cache_at: Optional[float] = None

_cron_status_cache: Optional[tuple[Optional[bool], Optional[int], Optional[int]]] = None
_cron_status_cache_at: Optional[float] = None

_update_last: Optional[dict] = None
_update_last_at: Optional[float] = None
_update_cache: Optional[dict] = None
_update_cache_at: Optional[float] = None


def _activity_push(evt: dict) -> None:
    """Append an event to the in-memory activity cache."""
    global _activity_cache, _activity_cache_at
    _activity_cache.insert(0, evt)
    _activity_cache = _activity_cache[:60]
    _activity_cache_at = time.time()


def _emit(evt: dict) -> None:
    evt = dict(evt)
    evt.setdefault("ts", time.time())

    # Keep a rolling cache so UI reloads don't lose the last few events.
    # NOTE: exclude high-frequency snapshots (e.g. status) to avoid flooding the Activity feed.
    try:
        if evt.get("type") != "status":
            _activity_push(evt)
    except Exception:
        pass

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


def host_health() -> tuple[Optional[float], Optional[int], Optional[int], Optional[float]]:
    """Return (cpu_pct, mem_used_bytes, mem_total_bytes, load1).

    Implemented without external deps (psutil), using /proc + os.getloadavg.
    """
    # CPU % by sampling /proc/stat deltas
    cpu_pct: Optional[float] = None
    try:
        def read_cpu():
            with open("/proc/stat", "r", encoding="utf-8") as f:
                line = f.readline()
            parts = line.split()
            if len(parts) < 5 or parts[0] != "cpu":
                return None
            vals = [int(x) for x in parts[1:]]
            total = sum(vals)
            idle = vals[3] + (vals[4] if len(vals) > 4 else 0)
            return total, idle

        a = read_cpu()
        time.sleep(0.10)
        b = read_cpu()
        if a and b:
            totald = b[0] - a[0]
            idled = b[1] - a[1]
            if totald > 0:
                cpu_pct = max(0.0, min(100.0, (1.0 - (idled / totald)) * 100.0))
    except Exception:
        cpu_pct = None

    # Memory from /proc/meminfo
    mem_used: Optional[int] = None
    mem_total: Optional[int] = None
    try:
        info: dict[str, int] = {}
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            for line in f:
                if ":" not in line:
                    continue
                k, rest = line.split(":", 1)
                parts = rest.strip().split()
                if not parts:
                    continue
                v = int(parts[0])
                # Values are kB
                info[k] = v * 1024
        mem_total = info.get("MemTotal")
        mem_avail = info.get("MemAvailable")
        if mem_total is not None and mem_avail is not None:
            mem_used = mem_total - mem_avail
    except Exception:
        mem_used, mem_total = None, None

    load1: Optional[float] = None
    try:
        load1 = float(os.getloadavg()[0])
    except Exception:
        load1 = None

    return cpu_pct, mem_used, mem_total, load1


def _safe_telegram_send_evt(args: dict) -> dict:
    """Redact content-bearing fields from message tool calls."""
    return {
        "type": "telegram_send",
        "channel": args.get("channel"),
        "action": args.get("action"),
        "target": args.get("target"),
        "asVoice": (bool(args.get("asVoice")) if args.get("asVoice") is not None else None),
        "hasPath": bool(args.get("path")),
        "filename": (os.path.basename(args.get("path")) if args.get("path") else None),
    }


def _safe_telegram_inbound_evt(text: str) -> Optional[dict]:
    """Extract Telegram inbound metadata from bridged header lines (no contents).

    In practice, inbound Telegram often appears inside a multi-line text blob that
    can include other lines ("System:" events, media attach notes, etc.).
    We scan all lines and match the first Telegram header we find.

    Example line:
      [Telegram Chris Johnson id:6907479327 +2m 2026-02-08 05:17 UTC] Test
    """
    try:
        import re

        for line in (text or "").splitlines():
            line = line.strip()
            if not line.startswith("[Telegram"):
                continue
            m = re.match(r"^\[Telegram\s+.*?\bid:(\-?\d+)\b.*?\]", line)
            if not m:
                continue
            peer_id = m.group(1)
            return {"type": "telegram_inbound", "peerId": peer_id}
        return None
    except Exception:
        return None


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



def cron_snapshot(ttl_sec: float = 60.0, force: bool = False) -> dict:
    """Return cron jobs + last run metadata (cached)."""
    global _cron_cache, _cron_cache_at

    if (not force) and _cron_cache is not None and _cron_cache_at and (time.time() - _cron_cache_at) < ttl_sec:
        return _cron_cache

    out: dict = {"ts": time.time(), "jobs": []}
    cl = openclaw_cron_list() or {}
    jobs = cl.get("jobs") or []

    for job in jobs:
        jid = job.get("id")
        if not jid:
            continue
        runs = openclaw_cron_runs(jid, limit=1) or {}
        last = None
        entries = runs.get("entries") or []
        if entries:
            e = entries[0] or {}
            last = {
                "status": e.get("status"),
                "summary": e.get("summary"),
                "runAtMs": e.get("runAtMs"),
                "durationMs": e.get("durationMs"),
                "ts": e.get("ts"),
            }
        out["jobs"].append({
            "id": jid,
            "name": job.get("name"),
            "enabled": bool(job.get("enabled", True)),
            "schedule": job.get("schedule"),
            "deliver": job.get("deliver"),
            "last": last,
        })

    # Sort: enabled first, then failing, then next-ish by last run time
    def sort_key(j):
        enabled = 1 if j.get('enabled') else 0
        last = j.get('last') or {}
        status = (last.get('status') or '').lower()
        failing = 1 if status in ('error','failed','fail') else 0
        last_ms = last.get('runAtMs') or 0
        return (-enabled, -failing, -last_ms)

    out["jobs"].sort(key=sort_key)

    _cron_cache = out
    _cron_cache_at = time.time()
    return out



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




def github_recent_commits(owner: str, repo: str, limit: int = 5) -> list[dict]:
    """Fetch recent commits from GitHub (public API). Metadata only."""
    url = f"https://api.github.com/repos/{owner}/{repo}/commits"
    try:
        r = requests.get(url, params={"per_page": str(limit)}, timeout=4)
        if r.status_code != 200:
            return []
        data = r.json()
        out=[]
        for c in data[:limit]:
            sha = (c.get("sha") or "")[:7]
            commit = (c.get("commit") or {})
            msg = (commit.get("message") or "").splitlines()[0][:120]
            author = ((commit.get("author") or {}).get("name"))
            date = ((commit.get("author") or {}).get("date"))
            out.append({"sha": sha, "message": msg, "author": author, "date": date, "url": c.get("html_url")})
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
    # Repo commits (metadata only)
    for owner, repo in REPO_ACTIVITY:
        for c in github_recent_commits(owner, repo, limit=3):
            ev.append({
                "type": "repo_commit",
                "repo": f"{owner}/{repo}",
                "sha": c.get("sha"),
                "message": c.get("message"),
                "author": c.get("author"),
                "date": c.get("date"),
                "url": c.get("url"),
                "ts": time.time(),
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
    global _cron_status_cache, _cron_status_cache_at
    if _cron_status_cache is not None and _cron_status_cache_at and (time.time() - _cron_status_cache_at) < CRON_STATUS_TTL_SEC:
        return _cron_status_cache
    ok, j, _err = _run_json(["openclaw", "cron", "status", "--json"], timeout=OPENCLAW_TIMEOUT_SEC)
    if not ok or not isinstance(j, dict):
        return None, None, None
    _cron_status_cache = (bool(j.get("enabled")), j.get("jobs"), j.get("nextWakeAtMs"))
    _cron_status_cache_at = time.time()
    return _cron_status_cache


def compute_status() -> Status:
    global _last_status

    last_restart = _read_json(RESTART_SENTINEL)

    oc_ok, oc_status, oc_err = openclaw_status()
    cron_enabled, cron_jobs, cron_next = openclaw_cron_status()

    cpu_pct, mem_used, mem_total, load1 = host_health()

    st = Status(
        ts=time.time(),
        openclaw_ok=oc_ok,
        openclaw_status=oc_status,
        openclaw_error=oc_err,
        cron_enabled=cron_enabled,
        cron_jobs=cron_jobs,
        cron_next_wake_at_ms=cron_next,
        host_cpu_pct=cpu_pct,
        host_mem_used_bytes=mem_used,
        host_mem_total_bytes=mem_total,
        host_load1=load1,
        last_restart=last_restart,
    )

    _last_status = st
    return st




def telegram_tail_loop() -> None:
    """Tail recent session .jsonl logs and emit Telegram metadata events.

    This is best-effort and intentionally content-blind:
    - Outbound: detects tool calls to `message` with channel=telegram.
    - Inbound: detects bridged header lines like "[Telegram ... id:123] ...".
    """
    sessions_dir = os.path.join(OPENCLAW_DIR, 'agents', 'main', 'sessions')
    offsets: dict[str, int] = {}

    def iter_recent_files():
        try:
            paths = [os.path.join(sessions_dir, f) for f in os.listdir(sessions_dir) if f.endswith('.jsonl')]
        except Exception:
            return []
        # Prefer most recently modified
        paths.sort(key=lambda p: os.stat(p).st_mtime if os.path.exists(p) else 0, reverse=True)
        return paths[:6]

    idle_cycles = 0
    while True:
        saw_activity = False
        try:
            for path in iter_recent_files():
                try:
                    st = os.stat(path)
                except Exception:
                    continue

                if path not in offsets:
                    # Start tailing from EOF (we rely on /api/activity backfill for history)
                    offsets[path] = st.st_size
                off = offsets.get(path, 0)
                # If file rotated/truncated
                if off > st.st_size:
                    off = 0

                if st.st_size <= off:
                    continue

                with open(path, 'r', encoding='utf-8', errors='replace') as f:
                    f.seek(off)
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        saw_activity = True
                        try:
                            obj = json.loads(line)
                        except Exception:
                            continue

                        if obj.get('type') != 'message':
                            continue
                        msg = obj.get('message') or {}
                        role = msg.get('role')

                        # outbound telegram sends via message tool
                        if role == 'assistant':
                            for c in (msg.get('content') or []):
                                if not isinstance(c, dict) or c.get('type') != 'toolCall':
                                    continue
                                if c.get('name') != 'message':
                                    continue
                                args = (c.get('arguments') or {})
                                if args.get('channel') != 'telegram':
                                    continue
                                evt = _safe_telegram_send_evt(args)
                                evt['sessionFile'] = os.path.basename(path)
                                evt['toolCallId'] = c.get('id')
                                _emit(evt)

                        # inbound bridged telegram headers (no content)
                        if role == 'user':
                            for c in (msg.get('content') or []):
                                if not isinstance(c, dict) or c.get('type') != 'text':
                                    continue
                                t = c.get('text') or ''
                                evt = _safe_telegram_inbound_evt(t)
                                if evt:
                                    evt['sessionFile'] = os.path.basename(path)
                                    evt['messageId'] = obj.get('id')
                                    _emit(evt)

                    offsets[path] = f.tell()
        except Exception:
            pass

        if saw_activity:
            idle_cycles = 0
        else:
            idle_cycles = min(idle_cycles + 1, 5)
        time.sleep(TELEGRAM_TAIL_POLL_SEC + (idle_cycles * 0.5))


def cron_loop() -> None:
    """Refresh cron snapshot in background (avoid expensive per-request work)."""
    while True:
        try:
            active = False
            with _client_lock:
                active = _clients_connected > 0 and (time.time() - _last_client_at) < 90

            if active:
                cron_snapshot(force=True)
                time.sleep(float(os.getenv("CLAWBOARD_CRON_POLL_SEC", "60.0")))
            else:
                # idle: refresh rarely
                time.sleep(float(os.getenv("CLAWBOARD_CRON_IDLE_POLL_SEC", "300.0")))
        except Exception:
            time.sleep(60.0)


def status_loop() -> None:
    prev_linked = None
    prev_restart_ts = None

    while True:
        # If nobody is watching, don't hammer the OpenClaw CLI.
        with _client_lock:
            active = _clients_connected > 0 and (time.time() - _last_client_at) < 90

        if not active:
            time.sleep(float(os.getenv("CLAWBOARD_STATUS_IDLE_POLL_SEC", "60.0")))
            continue

        st = compute_status()

        # Always emit status snapshots so clients don't need to poll /api/status.
        try:
            _emit({"type": "status", "status": asdict(st)})
        except Exception:
            pass

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

        time.sleep(STATUS_POLL_SEC)


@app.get("/")
def index():
    return render_template("index.html", title=APP_TITLE)


@app.get("/api/status")
def api_status():
    st = _last_status or compute_status()
    return jsonify(asdict(st))


@app.get("/api/activity")
def api_activity():
    global _activity_cache, _activity_cache_at
    if not _activity_cache:
        # Best-effort immediate backfill (may be slow on first request)
        try:
            _activity_cache = backfill_events()
            _activity_cache_at = time.time()
        except Exception:
            pass
    return jsonify({"events": _activity_cache, "ts": _activity_cache_at})


def openclaw_version() -> Optional[str]:
    try:
        p = subprocess.run(["openclaw", "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=8)
        if p.returncode != 0:
            return None
        v = (p.stdout or "").strip().splitlines()[0].strip()
        return v or None
    except Exception:
        return None


def update_status(ttl_sec: float = 600.0) -> dict:
    """Return update status: current vs latest (cached).

    This endpoint must be cheap: the UI may call it on page load/visibility changes.
    """
    global _update_cache, _update_cache_at

    if _update_cache is not None and _update_cache_at and (time.time() - _update_cache_at) < ttl_sec:
        return _update_cache

    # Use cached openclaw status when available.
    oc = _last_openclaw_status_raw
    if oc is None or (_last_openclaw_status_at is None) or (time.time() - _last_openclaw_status_at) > 30:
        ok, oc2, _err = openclaw_status()
        if ok and isinstance(oc2, dict):
            oc = oc2

    latest = None
    try:
        latest = (((oc or {}).get('update') or {}).get('registry') or {}).get('latestVersion')
    except Exception:
        latest = None

    current = openclaw_version()
    available = bool(latest and current and (latest != current))
    _update_cache = {"ts": time.time(), "current": current, "latest": latest, "available": available, "last": _update_last}
    _update_cache_at = time.time()
    return _update_cache


def run_update() -> dict:
    """Run OpenClaw self-update (requires sudo for global npm install)."""
    global _update_last, _update_cache, _update_cache_at

    started = time.time()
    out = {
        "ts": started,
        "status": "running",
        "steps": [],
    }
    _update_last = out
    # Invalidate cached update status
    _update_cache = None
    _update_cache_at = None

    def step(name: str, cmd: list[str], timeout: int = 900):
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=timeout)
        out['steps'].append({
            'name': name,
            'cmd': ' '.join(cmd),
            'exitCode': p.returncode,
            'output': (p.stdout or '')[-4000:],
        })
        return p.returncode == 0

    ok = step('npm_install_latest', ['sudo','npm','i','-g','openclaw@latest'], timeout=1200)
    if ok:
        # best-effort restart
        step('gateway_restart', ['openclaw','gateway','restart'], timeout=120)

    out['status'] = 'ok' if ok else 'error'
    out['finishedTs'] = time.time()
    return out


@app.get("/api/cron")
def api_cron():
    return jsonify(_cron_cache or cron_snapshot(ttl_sec=300.0, force=False))


@app.get("/api/update")
def api_update():
    return jsonify(update_status())


@app.post("/api/update/run")
def api_update_run():
    return jsonify(run_update())


@app.get("/api/host")
def api_host():
    cpu_pct, mem_used, mem_total, load1 = host_health()
    return jsonify({
        "ts": time.time(),
        "cpu_pct": cpu_pct,
        "mem_used_bytes": mem_used,
        "mem_total_bytes": mem_total,
        "load1": load1,
    })


@app.get("/api/events")
def api_events():
    def gen():
        global _clients_connected, _last_client_at
        with _client_lock:
            _clients_connected += 1
            _last_client_at = time.time()

        try:
            yield "event: hello\n"
            yield f"data: {json.dumps({'title': APP_TITLE})}\n\n"
            while True:
                with _client_lock:
                    _last_client_at = time.time()
                evt = _event_bus.get()
                etype = evt.get("type", "event")
                yield f"event: {etype}\n"
                yield f"data: {json.dumps(evt)}\n\n"
        except GeneratorExit:
            return
        finally:
            with _client_lock:
                _clients_connected = max(0, _clients_connected - 1)

    return Response(gen(), mimetype="text/event-stream")


if __name__ == "__main__":
    threading.Thread(target=status_loop, daemon=True).start()
    threading.Thread(target=cron_loop, daemon=True).start()
    threading.Thread(target=telegram_tail_loop, daemon=True).start()
    port = int(os.getenv("PORT", "3333"))
    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)
