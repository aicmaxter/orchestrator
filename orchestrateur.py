#!/usr/bin/env python
"""
orchestrateur.py — Multi-Agent Orchestrator local (LAB 3 / LAB 4).

Un seul service qui parle DEUX protocoles pour rester compatible avec tes agents :

  Protocole « multi-agent orchestrator » (clients type netsec-agent, Node) :
    GET  /health
    POST /api/v1/agents/enroll      Authorization: Bearer <cle d'enregistrement>
    GET  /api/v1/agents/me          Authorization: Bearer <jeton d'agent>
    POST /api/v1/agents/heartbeat   Authorization: Bearer <jeton d'agent>
    GET  /api/v1/agents/tasks       Authorization: Bearer <jeton d'agent>
    POST /api/v1/agents/tasks/{id}/result

  Protocole « reseau local d'agents » (clients type builder-agent, Python) :
    POST /register                  X-API-Key: <cle operateur>
    POST /delegate                  X-API-Key: <cle operateur>

  Console d'administration (navigateur) : GET /   (clé opérateur requise)
  Documentation d'API : GET /docs

Stockage : SQLite (data/orchestrateur.db). Journal : table events + stdout.
Aucune dépendance hors fastapi/uvicorn.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import sqlite3
import sys
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)
DB_PATH = DATA / "orchestrateur.db"
KEYS_PATH = ROOT / ".keys.json"
ALIASES_PATH = ROOT / "aliases.json"
CONSOLE_PATH = ROOT / "console.html"

SERVICE = "multi-agent-orchestrator"
VERSION = "1.0.0"
ONLINE_WINDOW_S = int(os.environ.get("ORCH_ONLINE_WINDOW", "90"))   # vu il y a moins de N s = ONLINE
LEASE_S = int(os.environ.get("ORCH_LEASE", "120"))                  # durée d'un bail de tâche
DELEGATE_TIMEOUT_S = int(os.environ.get("ORCH_DELEGATE_TIMEOUT", "150"))
HOST = os.environ.get("ORCH_HOST", "127.0.0.1")
PORT = int(os.environ.get("ORCH_PORT", "8000"))

# --------------------------------------------------------------------------- util
def iso(ts: float | None = None) -> str:
    return datetime.fromtimestamp(ts if ts else time.time(), timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


_lock = threading.RLock()
_conn: sqlite3.Connection | None = None


def conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
    return _conn


def q(sql: str, args: tuple = ()) -> list[sqlite3.Row]:
    with _lock:
        return conn().execute(sql, args).fetchall()


def x(sql: str, args: tuple = ()) -> None:
    with _lock:
        conn().execute(sql, args)
        conn().commit()


def one(sql: str, args: tuple = ()) -> sqlite3.Row | None:
    rows = q(sql, args)
    return rows[0] if rows else None


def sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def journal(kind: str, message: str, **data: Any) -> None:
    x("INSERT INTO events(at, kind, message, data) VALUES(?,?,?,?)",
      (time.time(), kind, message, json.dumps(data, ensure_ascii=False)))
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {kind:<22} {message}", flush=True)


def err(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}})


# --------------------------------------------------------------------------- schema
SCHEMA = """
CREATE TABLE IF NOT EXISTS agents (
  agent_id      TEXT PRIMARY KEY,
  name          TEXT UNIQUE NOT NULL,
  role          TEXT,
  runtime       TEXT,
  endpoint      TEXT,
  capabilities  TEXT NOT NULL DEFAULT '[]',
  token_hash    TEXT,
  status        TEXT NOT NULL DEFAULT 'OFFLINE',
  enrolled_at   REAL,
  last_seen     REAL,
  tasks_done    INTEGER NOT NULL DEFAULT 0,
  tasks_failed  INTEGER NOT NULL DEFAULT 0,
  protocol      TEXT NOT NULL DEFAULT 'v1'
);
CREATE TABLE IF NOT EXISTS tasks (
  task_id     TEXT PRIMARY KEY,
  capability  TEXT NOT NULL,
  instruction TEXT NOT NULL,
  action      TEXT,
  agent_id    TEXT,
  status      TEXT NOT NULL DEFAULT 'queued',
  result      TEXT,
  error       TEXT,
  origin      TEXT,
  created_at  REAL,
  leased_at   REAL,
  updated_at  REAL
);
CREATE TABLE IF NOT EXISTS events (
  id      INTEGER PRIMARY KEY AUTOINCREMENT,
  at      REAL,
  kind    TEXT,
  message TEXT,
  data    TEXT
);
CREATE TABLE IF NOT EXISTS settings (k TEXT PRIMARY KEY, v TEXT, updated_at REAL);
"""


def setting(k: str, default: str | None = None) -> str | None:
    row = one("SELECT v FROM settings WHERE k=?", (k,))
    return row["v"] if row else default


def set_setting(k: str, v: str) -> None:
    x("INSERT INTO settings(k,v,updated_at) VALUES(?,?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_at=excluded.updated_at", (k, v, time.time()))


ADMIN_KEY = ""
ENROLLMENT_KEY = ""


def init_keys() -> None:
    """Génère les clés au premier démarrage ; les réutilise ensuite (fichier .keys.json)."""
    global ADMIN_KEY, ENROLLMENT_KEY
    stored: dict[str, str] = {}
    if KEYS_PATH.exists():
        try:
            stored = json.loads(KEYS_PATH.read_text(encoding="utf-8"))
        except Exception:
            stored = {}
    ADMIN_KEY = os.environ.get("ORCH_ADMIN_KEY") or stored.get("admin_key") or setting("admin_key") or f"adm_{secrets.token_urlsafe(24)}"
    ENROLLMENT_KEY = stored.get("enrollment_key") or setting("enrollment_key") or f"enr_{secrets.token_urlsafe(24)}"
    set_setting("admin_key", ADMIN_KEY)
    set_setting("enrollment_key", ENROLLMENT_KEY)
    if setting("enrollment_key_created") is None:
        set_setting("enrollment_key_created", str(time.time()))
    KEYS_PATH.write_text(json.dumps({"admin_key": ADMIN_KEY, "enrollment_key": ENROLLMENT_KEY, "host": f"http://{HOST}:{PORT}"}, indent=2), encoding="utf-8")


def aliases() -> dict[str, list[str]]:
    if ALIASES_PATH.exists():
        try:
            return json.loads(ALIASES_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


# --------------------------------------------------------------------------- agents
def agent_public(row: sqlite3.Row, with_stats: bool = True) -> dict[str, Any]:
    d = {
        "agent_id": row["agent_id"],
        "name": row["name"],
        "role": row["role"],
        "runtime": row["runtime"],
        "endpoint": row["endpoint"],
        "capabilities": json.loads(row["capabilities"] or "[]"),
        "status": "ONLINE" if row["last_seen"] and (time.time() - row["last_seen"]) < ONLINE_WINDOW_S else "OFFLINE",
        "protocol": row["protocol"],
        "enrolled_at": iso(row["enrolled_at"]) if row["enrolled_at"] else None,
        "last_seen_at": iso(row["last_seen"]) if row["last_seen"] else None,
        "last_seen_age_s": int(time.time() - row["last_seen"]) if row["last_seen"] else None,
    }
    if with_stats:
        d["tasks_done"] = row["tasks_done"]
        d["tasks_failed"] = row["tasks_failed"]
        d["pending_tasks"] = one("SELECT COUNT(*) c FROM tasks WHERE agent_id=? AND status IN ('queued','leased')", (row["agent_id"],))["c"]
    return d


def agent_by_token(token: str | None) -> sqlite3.Row | None:
    if not token:
        return None
    return one("SELECT * FROM agents WHERE token_hash=?", (sha(token),))


def bearer(authorization: str | None) -> str | None:
    if not authorization:
        return None
    parts = authorization.split(None, 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return authorization.strip() or None


def online_agents() -> list[sqlite3.Row]:
    cut = time.time() - ONLINE_WINDOW_S
    return q("SELECT * FROM agents WHERE last_seen IS NOT NULL AND last_seen > ? ORDER BY last_seen DESC", (cut,))


def choose_agent(capability: str, want: str | None = None) -> tuple[sqlite3.Row | None, str]:
    """Routage : agent ONLINE qui déclare la capacité ; sinon alias configurés."""
    caps = {capability}
    for alias, targets in aliases().items():
        if capability == alias:
            caps.update(targets)
        elif alias in caps:
            caps.add(alias)
    live = online_agents()
    if want:
        for a in live:
            if want.lower() in (a["name"] or "").lower() or a["agent_id"] == want:
                provided = set(json.loads(a["capabilities"] or "[]"))
                if caps & provided:
                    return a, "cible explicite"
                return None, f"l'agent '{a['name']}' est ONLINE mais ne déclare pas la capacité '{capability}' ({sorted(provided)})"
        return None, f"aucun agent ONLINE nommé '{want}'"
    for a in live:
        if caps & set(json.loads(a["capabilities"] or "[]")):
            return a, "capacité déclarée" if capability in json.loads(a["capabilities"] or "[]") else "alias de capacité"
    return None, f"aucun agent ONLINE ne déclare la capacité '{capability}'"


def create_task(capability: str, instruction: str, action: str | None = None,
                want_agent: str | None = None, origin: str = "console") -> dict[str, Any]:
    row, reason = choose_agent(capability, want_agent)
    task_id = "tsk_" + uuid.uuid4().hex[:12]
    now = time.time()
    x("INSERT INTO tasks(task_id,capability,instruction,action,agent_id,status,origin,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
      (task_id, capability, instruction, action, row["agent_id"] if row else None,
       "queued" if row else "unrouted", origin, now, now))
    if row:
        journal("TASK_ROUTED", f"{task_id} → {row['name']} (capability={capability}, {reason})",
                task_id=task_id, agent=row["name"], capability=capability, origin=origin)
    else:
        journal("TASK_UNROUTED", f"{task_id} capability={capability} — {reason}", task_id=task_id, capability=capability)
    return get_task(task_id) | {"routing": reason}


def get_task(task_id: str) -> dict[str, Any]:
    r = one("SELECT * FROM tasks WHERE task_id=?", (task_id,))
    if not r:
        raise KeyError(task_id)
    return {
        "task_id": r["task_id"],
        "capability": r["capability"],
        "instruction": r["instruction"],
        "action": r["action"],
        "agent_id": r["agent_id"],
        "agent_name": (one("SELECT name FROM agents WHERE agent_id=?", (r["agent_id"],)) or {"name": None})["name"] if r["agent_id"] else None,
        "status": r["status"],
        "result": r["result"],
        "error": r["error"],
        "origin": r["origin"],
        "created_at": iso(r["created_at"]),
        "leased_at": iso(r["leased_at"]) if r["leased_at"] else None,
        "updated_at": iso(r["updated_at"]) if r["updated_at"] else None,
    }


# --------------------------------------------------------------------------- app
app = FastAPI(title="Multi-Agent Orchestrator", version=VERSION, docs_url="/docs", redoc_url=None)


@asynccontextmanager
async def lifespan(_: FastAPI):
    with _lock:
        conn().executescript(SCHEMA)
        conn().commit()
    init_keys()
    if not ALIASES_PATH.exists():
        ALIASES_PATH.write_text(json.dumps({
            "security_analysis": ["vulnerability_assessment", "cybersecurity"],
            "pentest": ["vulnerability_assessment"],
        }, indent=2), encoding="utf-8")
    journal("ORCH_START", f"{SERVICE} {VERSION} sur http://{HOST}:{PORT} — base {DB_PATH.name}")
    yield
    journal("ORCH_STOP", "arrêt du service")


app.router.lifespan_context = lifespan  # remplace l'ancien @app.on_event("startup")


def require_admin(key: str | None, request: Request | None = None) -> None:
    provided = key
    if not provided and request is not None:
        provided = request.headers.get("X-API-Key") or bearer(request.headers.get("Authorization")) or request.query_params.get("key")
    if not provided:
        raise HTTPException(status_code=401, detail="X-API-Key (clé opérateur) requise.")
    if not secrets.compare_digest(provided, ADMIN_KEY):
        raise HTTPException(status_code=401, detail="X-API-Key (clé opérateur) invalide.")


# ------------------------------------------------------------------ protocole public
@app.get("/health")
def health() -> dict[str, Any]:
    live = online_agents()
    return {
        "status": "ok",
        "service": SERVICE,
        "version": VERSION,
        "agents_online": len(live),
        "agents_total": one("SELECT COUNT(*) c FROM agents")["c"],
        "tasks": {
            "queued": one("SELECT COUNT(*) c FROM tasks WHERE status='queued'")["c"],
            "running": one("SELECT COUNT(*) c FROM tasks WHERE status='leased'")["c"],
            "completed": one("SELECT COUNT(*) c FROM tasks WHERE status='completed'")["c"],
            "failed": one("SELECT COUNT(*) c FROM tasks WHERE status IN ('failed','unrouted')")["c"],
        },
        "time": iso(),
    }


@app.post("/api/v1/agents/enroll")
async def enroll(authorization: str | None = Header(default=None), request: Request = None) -> Any:  # type: ignore[assignment]
    key = bearer(authorization)
    if not key:
        journal("ENROLL_REFUSED", "clé d'enregistrement absente")
        return err(401, "UNAUTHENTICATED", "Clé d'enregistrement requise.")
    if not secrets.compare_digest(key, ENROLLMENT_KEY):
        journal("ENROLL_REFUSED", "clé d'enregistrement invalide")
        return err(401, "UNAUTHENTICATED", "Clé d'enregistrement invalide.")
    try:
        body = await request.json()
    except Exception:
        body = {}
    name = (body.get("name") or body.get("agent_id") or "").strip()
    if not name:
        return err(422, "INVALID_PAYLOAD", "Champ 'name' requis.")
    caps = body.get("capabilities") or []
    if isinstance(caps, str):
        caps = [c.strip() for c in caps.split(",") if c.strip()]
    agent_id = (body.get("agent_id") or name).strip()
    token = "agt_" + secrets.token_urlsafe(32)
    now = time.time()

    existing = one("SELECT * FROM agents WHERE name=? OR agent_id=?", (name, agent_id))
    if existing:
        agent_id = existing["agent_id"]
        x("""UPDATE agents SET name=?, role=?, runtime=?, endpoint=?, capabilities=?, token_hash=?,
             status='ONLINE', enrolled_at=?, last_seen=?, protocol=? WHERE agent_id=?""",
          (name, body.get("role"), body.get("runtime"), body.get("endpoint"), json.dumps(caps),
           sha(token), existing["enrolled_at"] or now, now, "v1", agent_id))
        journal("ENROLL_REFRESH", f"{name} ré-enrôlé (jeton tourné)", agent=name, agent_id=agent_id, capabilities=caps)
    else:
        x("""INSERT INTO agents(agent_id,name,role,runtime,endpoint,capabilities,token_hash,status,enrolled_at,last_seen,protocol)
             VALUES(?,?,?,?,?,?,?,'ONLINE',?,?,'v1')""",
          (agent_id, name, body.get("role"), body.get("runtime"), body.get("endpoint"),
           json.dumps(caps), sha(token), now, now))
        journal("ENROLL_OK", f"{name} inscrit (role={body.get('role')}, {len(caps)} capacités)",
                agent=name, agent_id=agent_id, capabilities=caps)

    row = one("SELECT * FROM agents WHERE agent_id=?", (agent_id,))
    return JSONResponse(status_code=201, content={"data": {
        "agent_token": token,
        "token_type": "Bearer",
        "agent": agent_public(row),
    }}, headers={"X-Orchestrator": f"{SERVICE}/{VERSION}"})


@app.get("/api/v1/agents/me")
def me(authorization: str | None = Header(default=None)) -> Any:
    row = agent_by_token(bearer(authorization))
    if not row:
        return err(401, "UNAUTHENTICATED", "Jeton d'agent inconnu ou révoqué.")
    return {"data": agent_public(row)}


@app.post("/api/v1/agents/heartbeat")
async def heartbeat(authorization: str | None = Header(default=None), request: Request = None) -> Any:  # type: ignore[assignment]
    row = agent_by_token(bearer(authorization))
    if not row:
        return err(401, "UNAUTHENTICATED", "Jeton d'agent inconnu ou révoqué.")
    try:
        body = await request.json()
    except Exception:
        body = {}
    caps = None
    if body.get("capabilities"):
        caps = body["capabilities"]
        if isinstance(caps, str):
            caps = [c.strip() for c in caps.split(",") if c.strip()]
    now = time.time()
    if caps:
        x("UPDATE agents SET last_seen=?, status='ONLINE', capabilities=? WHERE agent_id=?", (now, json.dumps(caps), row["agent_id"]))
    else:
        x("UPDATE agents SET last_seen=?, status='ONLINE' WHERE agent_id=?", (now, row["agent_id"]))
    fresh = one("SELECT * FROM agents WHERE agent_id=?", (row["agent_id"],))
    pending = one("SELECT COUNT(*) c FROM tasks WHERE agent_id=? AND status IN ('queued','leased')", (row["agent_id"],))["c"]
    return {"status": "ONLINE", "data": agent_public(fresh), "pending_tasks": pending,
            "server_time": iso(now), **({"capabilities": json.loads(fresh["capabilities"] or "[]")} if caps else {})}


@app.get("/api/v1/agents/tasks")
def agent_tasks(authorization: str | None = Header(default=None)) -> Any:
    row = agent_by_token(bearer(authorization))
    if not row:
        return err(401, "UNAUTHENTICATED", "Jeton d'agent inconnu ou révoqué.")
    now = time.time()
    # livraison : tâches en attente pour cet agent, ou bail expiré (retry)
    rows = q("""SELECT * FROM tasks WHERE agent_id=? AND (status='queued' OR (status='leased' AND leased_at < ?))
                ORDER BY created_at LIMIT 5""", (row["agent_id"], now - LEASE_S))
    ids = [r["task_id"] for r in rows]
    for tid in ids:
        x("UPDATE tasks SET status='leased', leased_at=?, updated_at=? WHERE task_id=?", (now, now, tid))
        journal("TASK_DELIVERED", f"{tid} → {row['name']}", task_id=tid, agent=row["name"])
    tasks = []
    for r in q("SELECT * FROM tasks WHERE task_id IN (%s)" % ",".join("?" * len(ids)), tuple(ids)) if ids else []:
        t = get_task(r["task_id"])
        t["status"] = "running"
        tasks.append(t)
    return {"data": {"tasks": tasks, "count": len(tasks), "server_time": iso(now)}}


@app.post("/api/v1/agents/tasks/{task_id}/result")
async def agent_result(task_id: str, authorization: str | None = Header(default=None), request: Request = None) -> Any:  # type: ignore[assignment]
    row = agent_by_token(bearer(authorization))
    if not row:
        return err(401, "UNAUTHENTICATED", "Jeton d'agent inconnu ou révoqué.")
    task = one("SELECT * FROM tasks WHERE task_id=?", (task_id,))
    if not task:
        return err(404, "TASK_NOT_FOUND", f"Tâche inconnue : {task_id}")
    if task["agent_id"] and task["agent_id"] != row["agent_id"]:
        return err(403, "NOT_YOUR_TASK", "Cette tâche n'est pas assignée à cet agent.")
    try:
        body = await request.json()
    except Exception:
        body = {}
    status = (body.get("status") or "completed").lower()
    ok = status in ("completed", "success", "ok", "done")
    result = body.get("result")
    if result is not None and not isinstance(result, str):
        result = json.dumps(result, ensure_ascii=False)
    now = time.time()
    x("UPDATE tasks SET status=?, result=?, error=?, updated_at=? WHERE task_id=?",
      ("completed" if ok else "failed", result, None if ok else (body.get("error") or status), now, task_id))
    x("UPDATE agents SET tasks_done=tasks_done+?, tasks_failed=tasks_failed+?, last_seen=? WHERE agent_id=?",
      (1 if ok else 0, 0 if ok else 1, now, row["agent_id"]))
    journal("TASK_COMPLETED" if ok else "TASK_FAILED",
            f"{task_id} terminée par {row['name']} ({len(result or '')} car.)" if ok else f"{task_id} échouée par {row['name']}: {body.get('error') or status}",
            task_id=task_id, agent=row["name"])
    return {"data": get_task(task_id)}


# ------------------------------------------------------------------ protocole LAN (X-API-Key)
@app.post("/register")
async def lan_register(request: Request) -> Any:
    try:
        require_admin(request.headers.get("X-API-Key"), request)
    except HTTPException as e:
        journal("REGISTER_REFUSED", str(e.detail))
        return err(401, "UNAUTHORIZED", str(e.detail))
    body = await request.json()
    agent_id = (body.get("agent_id") or body.get("name") or "").strip()
    name = (body.get("name") or agent_id).strip()
    caps = body.get("capabilities") or []
    if isinstance(caps, str):
        caps = [c.strip() for c in caps if c.strip()]
    now = time.time()
    existing = one("SELECT * FROM agents WHERE agent_id=? OR name=?", (agent_id, name))
    if existing:
        agent_id = existing["agent_id"]
        x("UPDATE agents SET name=?, endpoint=?, capabilities=?, status='ONLINE', last_seen=?, protocol='lan' WHERE agent_id=?",
          (name, body.get("endpoint"), json.dumps(caps), now, agent_id))
    else:
        x("""INSERT INTO agents(agent_id,name,role,runtime,endpoint,capabilities,status,enrolled_at,last_seen,protocol)
             VALUES(?,?,?,?,?,?,'ONLINE',?,?,'lan')""",
          (agent_id, name, "généraliste", "python", body.get("endpoint"), json.dumps(caps), now, now))
    journal("LAN_REGISTER", f"{name} présenté au réseau (endpoint={body.get('endpoint')}, capacités={caps})", agent=name)
    return {"status": "registered", "agent": agent_public(one("SELECT * FROM agents WHERE agent_id=?", (agent_id,)))}


@app.post("/delegate")
async def lan_delegate(request: Request) -> Any:
    try:
        require_admin(request.headers.get("X-API-Key"), request)
    except HTTPException as e:
        journal("DELEGATE_REFUSED", str(e.detail))
        return err(401, "UNAUTHORIZED", str(e.detail))
    body = await request.json()
    capability = (body.get("capability") or "").strip()
    prompt = body.get("task") or body.get("instruction") or ""
    if not capability:
        return err(422, "INVALID_PAYLOAD", "Champ 'capability' requis.")
    task = create_task(capability, prompt, action=body.get("action"), origin="delegate")
    if task["status"] == "unrouted":
        # un agent LAN peut se présenter juste après : on laisse une courte fenêtre
        await asyncio.sleep(1.0)
        again = create_task(capability, prompt, action=body.get("action"), origin="delegate")
        task = again
    if task["status"] == "unrouted":
        return {"status": "refused", "task_id": task["task_id"], "capability": capability,
                "error": f"Aucun agent disponible pour la capacité « {capability} » — {task['routing']}"}
    deadline = time.time() + DELEGATE_TIMEOUT_S
    while time.time() < deadline:
        cur = get_task(task["task_id"])
        if cur["status"] in ("completed", "failed"):
            return {"status": cur["status"], "task_id": cur["task_id"], "capability": capability,
                    "agent_id": cur["agent_id"], "agent_name": cur["agent_name"],
                    "result": cur["result"], "error": cur["error"]}
        await asyncio.sleep(0.5)
    return {"status": "timeout", "task_id": task["task_id"], "capability": capability,
            "error": f"L'agent {task.get('agent_name')} n'a pas rendu de résultat en {DELEGATE_TIMEOUT_S}s "
                     f"(la tâche reste ouverte, elle sera relivrée au prochain cycle)."}


@app.get("/agents")
def lan_agents(request: Request) -> Any:
    try:
        require_admin(request.headers.get("X-API-Key"), request)
    except HTTPException as e:
        return err(401, "UNAUTHORIZED", str(e.detail))
    return {"agents": [agent_public(r) for r in q("SELECT * FROM agents ORDER BY enrolled_at")],
            "online": len(online_agents())}


# ------------------------------------------------------------------ administration
@app.get("/api/v1/agents")
def admin_agents(key: str | None = Query(default=None), x_api_key: str | None = Header(default=None)) -> Any:
    try:
        require_admin(x_api_key or key)
    except HTTPException as e:
        return err(401, "UNAUTHORIZED", str(e.detail))
    rows = q("SELECT * FROM agents ORDER BY enrolled_at")
    return {"data": {"agents": [agent_public(r) for r in rows], "online": len(online_agents())}}


@app.get("/api/v1/tasks")
def admin_tasks(limit: int = 100, key: str | None = Query(default=None), x_api_key: str | None = Header(default=None)) -> Any:
    try:
        require_admin(x_api_key or key)
    except HTTPException as e:
        return err(401, "UNAUTHORIZED", str(e.detail))
    rows = q("SELECT * FROM tasks ORDER BY created_at DESC LIMIT ?", (limit,))
    return {"data": {"tasks": [get_task(r["task_id"]) for r in rows], "count": len(rows)}}


@app.post("/api/v1/tasks")
async def admin_create_task(request: Request, x_api_key: str | None = Header(default=None)) -> Any:
    try:
        require_admin(x_api_key)
    except HTTPException as e:
        return err(401, "UNAUTHORIZED", str(e.detail))
    body = await request.json()
    capability = (body.get("capability") or "").strip()
    instruction = body.get("instruction") or body.get("task") or ""
    if not capability or not instruction:
        return err(422, "INVALID_PAYLOAD", "Champs 'capability' et 'instruction' requis.")
    task = create_task(capability, instruction, action=body.get("action"), want_agent=body.get("agent_name"), origin=body.get("origin") or "console")
    code = 201 if task["status"] == "queued" else 202
    return JSONResponse(status_code=code, content={"data": task})


@app.post("/api/v1/tasks/{task_id}/requeue")
def admin_requeue(task_id: str, x_api_key: str | None = Header(default=None)) -> Any:
    try:
        require_admin(x_api_key)
    except HTTPException as e:
        return err(401, "UNAUTHORIZED", str(e.detail))
    if not one("SELECT 1 FROM tasks WHERE task_id=?", (task_id,)):
        return err(404, "TASK_NOT_FOUND", f"Tâche inconnue : {task_id}")
    x("UPDATE tasks SET status='queued', leased_at=NULL, updated_at=? WHERE task_id=?", (time.time(), task_id))
    journal("TASK_REQUEUED", f"{task_id} remise en file", task_id=task_id)
    return {"data": get_task(task_id)}


@app.get("/api/v1/events")
def admin_events(limit: int = 200, key: str | None = Query(default=None), x_api_key: str | None = Header(default=None)) -> Any:
    try:
        require_admin(x_api_key or key)
    except HTTPException as e:
        return err(401, "UNAUTHORIZED", str(e.detail))
    rows = q("SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,))
    return {"data": {"events": [{"id": r["id"], "at": iso(r["at"]), "kind": r["kind"],
                                 "message": r["message"], "data": json.loads(r["data"] or "{}")} for r in rows]}}


@app.get("/api/v1/settings/enrollment-key")
def admin_enrollment_key(key: str | None = Query(default=None), x_api_key: str | None = Header(default=None)) -> Any:
    try:
        require_admin(x_api_key or key)
    except HTTPException as e:
        return err(401, "UNAUTHORIZED", str(e.detail))
    journal("KEY_READ", "clé d'enregistrement consultée")  # consultation journalisée
    used = one("SELECT COUNT(*) c FROM agents") ["c"]
    created = setting("enrollment_key_created")
    return {"data": {"enrollment_key": ENROLLMENT_KEY,
                     "created_at": iso(float(created)) if created else None,
                     "rotated_at": iso(float(setting("enrollment_key_rotated"))) if setting("enrollment_key_rotated") else None,
                     "agents_enrolled": used,
                     "usage": "Authorization: Bearer <clé> sur POST /api/v1/agents/enroll"}}


@app.post("/api/v1/settings/rotate-enrollment-key")
def admin_rotate(x_api_key: str | None = Header(default=None)) -> Any:
    global ENROLLMENT_KEY
    try:
        require_admin(x_api_key)
    except HTTPException as e:
        return err(401, "UNAUTHORIZED", str(e.detail))
    ENROLLMENT_KEY = "enr_" + secrets.token_urlsafe(24)
    set_setting("enrollment_key", ENROLLMENT_KEY)
    set_setting("enrollment_key_rotated", str(time.time()))
    keys = json.loads(KEYS_PATH.read_text(encoding="utf-8")) if KEYS_PATH.exists() else {}
    keys.update({"admin_key": ADMIN_KEY, "enrollment_key": ENROLLMENT_KEY, "host": f"http://{HOST}:{PORT}"})
    KEYS_PATH.write_text(json.dumps(keys, indent=2), encoding="utf-8")
    journal("KEY_ROTATED", "clé d'enregistrement tournée (les agents déjà inscrits gardent leur jeton)")
    return {"data": {"enrollment_key": ENROLLMENT_KEY, "rotated_at": iso()}}


@app.get("/api/v1/routing")
def admin_routing(key: str | None = Query(default=None), x_api_key: str | None = Header(default=None)) -> Any:
    try:
        require_admin(x_api_key or key)
    except HTTPException as e:
        return err(401, "UNAUTHORIZED", str(e.detail))
    cap_map: dict[str, list[str]] = {}
    for a in online_agents():
        for c in json.loads(a["capabilities"] or "[]"):
            cap_map.setdefault(c, []).append(a["name"])
    return {"data": {"capabilities": cap_map, "aliases": aliases()}}


@app.get("/keys.json")
def keys_file(request: Request) -> Any:
    """Confort local : permet à la console de pré-remplir la clé opérateur.
    Servi UNIQUEMENT aux clients loopback et jamais journalisé avec la valeur."""
    host = (request.client.host if request.client else "") or ""
    if host not in ("127.0.0.1", "::1", "localhost"):
        return err(403, "FORBIDDEN", "Réservé aux clients locaux.")
    if not KEYS_PATH.exists():
        return err(404, "NO_KEYS", "Aucune clé générée pour l'instant.")
    return JSONResponse(content=json.loads(KEYS_PATH.read_text(encoding="utf-8")))


@app.get("/", response_class=HTMLResponse)
def console() -> HTMLResponse:
    if CONSOLE_PATH.exists():
        return HTMLResponse(CONSOLE_PATH.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>Multi-Agent Orchestrator</h1><p>console.html manquant.</p>")


# ------------------------------------------------------------------------ entrée
def main() -> None:
    import uvicorn

    print(f"\n  {SERVICE} {VERSION} — http://{HOST}:{PORT}\n"
          f"  console  : http://{HOST}:{PORT}/  (clé opérateur dans {KEYS_PATH.name})\n"
          f"  API docs : http://{HOST}:{PORT}/docs\n"
          f"  base     : {DB_PATH}\n", flush=True)
    uvicorn.run(app, host=HOST, port=PORT, log_level=os.environ.get("ORCH_LOG", "warning"),
                timeout_keep_alive=int(os.environ.get("ORCH_KEEPALIVE", "120")))


if __name__ == "__main__":
    main()
