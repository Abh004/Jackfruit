"""
gateway/main.py — Consolidated Gateway with Monitoring Dashboard
================================================================
Responsibilities:
  - Serve the drawing frontend.
  - Sync new clients with the current RAFT state.
  - Forward strokes to the current leader with automatic failover.
  - Provide a dashboard endpoint for cluster health monitoring.
"""

import asyncio
import logging
import os
from typing import Optional

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

# CONFIG: Replicas share the same image but unique network aliases
REPLICA_URLS = [
    u.strip()
    for u in os.getenv(
        "REPLICA_URLS", "http://replica1:5000,http://replica2:5000,http://replica3:5000"
    ).split(",")
    if u.strip()
]

LEADER_POLL_INTERVAL = 1.0
STROKE_TIMEOUT = 2.0
LEADER_RETRY_DELAY = 0.2

logging.basicConfig(level=logging.INFO, format="[Gateway] %(levelname)s %(message)s")
log = logging.getLogger(__name__)


class GatewayState:
    def __init__(self):
        self.leader_url: Optional[str] = None
        self.clients: set[WebSocket] = set()


gw = GatewayState()
app = FastAPI(title="RAFT Gateway")


@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    """Serves the central drawing board interface."""
    try:
        with open("/usr/src/app/static/index.html", "r") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse(
            content="<h1>Frontend index.html not found</h1>", status_code=404
        )


@app.get("/health")
async def health():
    """Basic health check for the gateway itself."""
    return {
        "leader_url": gw.leader_url,
        "connected_clients": len(gw.clients),
        "replicas": REPLICA_URLS,
    }


@app.get("/dashboard")
async def get_dashboard():
    """
    BONUS CHALLENGE: Aggregates health data from all replicas to show
    current terms, roles, and log indices.
    """
    async with httpx.AsyncClient(timeout=0.5) as client:
        tasks = [client.get(f"{url}/health") for url in REPLICA_URLS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    dashboard_data = []
    for url, res in zip(REPLICA_URLS, results):
        if isinstance(res, Exception):
            dashboard_data.append(
                {"url": url, "status": "unreachable", "error": str(res)}
            )
        else:
            data = res.json()
            data["url"] = url
            data["status"] = "online"
            dashboard_data.append(data)

    return JSONResponse(content=dashboard_data)


async def _sync_new_client(websocket: WebSocket):
    """
    Requirement: Consistent canvas state after restarts.
    Fetches the full committed log from the leader for the new client.
    """
    if not gw.leader_url:
        await _discover_leader()

    if gw.leader_url:
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                # Calls the /canvas-state endpoint on the replica
                response = await client.get(f"{gw.leader_url}/canvas-state")
                if response.status_code == 200:
                    state = response.json()
                    await websocket.send_json(
                        {
                            "type": "init",
                            "log": state.get("log", []),
                            "commit_index": state.get("commit_index", -1),
                        }
                    )
                    log.info(f"Synced client with {len(state.get('log', []))} strokes.")
        except Exception as e:
            log.warning(f"Failed to sync client: {e}")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Handles real-time drawing propagation."""
    await websocket.accept()
    gw.clients.add(websocket)

    await _sync_new_client(websocket)

    try:
        while True:
            data = await websocket.receive_json()
            # Requirement: Forwarding with automatic failover
            committed_entry = await _forward_stroke_to_leader(data)
            if committed_entry:
                await _broadcast(committed_entry)
    except WebSocketDisconnect:
        log.info("Client disconnected")
    finally:
        gw.clients.discard(websocket)


async def _forward_stroke_to_leader(stroke: dict) -> Optional[dict]:
    """Retries submission to handle election windows and leader changes."""
    for attempt in range(10):
        if not gw.leader_url:
            await _discover_leader()
            if not gw.leader_url:
                await asyncio.sleep(LEADER_RETRY_DELAY)
                continue

        try:
            async with httpx.AsyncClient(timeout=STROKE_TIMEOUT) as client:
                response = await client.post(
                    f"{gw.leader_url}/stroke", json={"stroke": stroke}
                )

            if response.status_code == 200:
                return response.json().get("entry")

            if response.status_code == 403:  # Not the leader anymore
                gw.leader_url = None
                await _discover_leader()

            await asyncio.sleep(LEADER_RETRY_DELAY)
        except Exception:
            gw.leader_url = None
            await _discover_leader()
            await asyncio.sleep(LEADER_RETRY_DELAY)

    return None


async def _broadcast(entry: dict):
    """Propagates committed strokes to all active clients."""
    dead_clients = set()
    for client in gw.clients:
        try:
            await client.send_json({"type": "stroke", "entry": entry})
        except Exception:
            dead_clients.add(client)
    for client in dead_clients:
        gw.clients.discard(client)


async def _discover_leader():
    """Polls all replicas to find which one is currently the LEADER."""
    async with httpx.AsyncClient(timeout=0.5) as client:
        tasks = [client.get(f"{url}/health") for url in REPLICA_URLS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for url, result in zip(REPLICA_URLS, results):
        if not isinstance(result, Exception) and result.status_code == 200:
            data = result.json()
            if data.get("role") == "leader":
                if gw.leader_url != url:
                    log.info(f"Leader found: {url} (Term {data.get('term')})")
                    gw.leader_url = url
                return


async def _leader_poll_loop():
    """Background task to keep leader info fresh."""
    while True:
        await _discover_leader()
        await asyncio.sleep(LEADER_POLL_INTERVAL)


@app.on_event("startup")
async def startup():
    asyncio.create_task(_leader_poll_loop())
    log.info(f"Gateway live. Monitoring: {REPLICA_URLS}")
