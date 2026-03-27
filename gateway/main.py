"""
gateway/main.py  —  WebSocket Gateway
======================================
Responsibilities:
  - Accept browser WebSocket connections
  - Discover and track the current RAFT leader
  - Forward incoming strokes to the leader's /stroke endpoint
  - Broadcast committed strokes to ALL connected clients
  - Automatically re-route to a new leader during failover

Environment variables (set by docker-compose):
  REPLICA_URLS : comma-separated replica base URLs
                 e.g. http://replica1:5000,http://replica2:5000,http://replica3:5000
"""

import asyncio
import logging
import os
from typing import Optional

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

# CONFIG
REPLICA_URLS = [
    u.strip() for u in os.getenv("REPLICA_URLS", "").split(",") if u.strip()
]

LEADER_POLL_INTERVAL = 1.0  # seconds between leader-discovery polls
STROKE_TIMEOUT = 2.0  # seconds before a stroke request times out
LEADER_RETRY_DELAY = 0.2  # seconds between retries when leader is unknown

logging.basicConfig(level=logging.INFO, format="[Gateway] %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# STATE
class GatewayState:
    def __init__(self):
        self.leader_url: Optional[str] = None  # base URL of current leader replica
        self.clients: set[WebSocket] = set()  # all connected browser WebSockets


gw = GatewayState()

# FASTAPI APP
app = FastAPI(title="RAFT Gateway")


@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    return HTMLResponse(content=open("/usr/src/app/static/index.html").read())


# HEALTH
@app.get("/health")
async def health():
    return {
        "leader_url": gw.leader_url,
        "connected_clients": len(gw.clients),
        "replicas": REPLICA_URLS,
    }


# WEBSOCKET  — browser clients connect here
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    gw.clients.add(websocket)
    log.info(f"Client connected. Total clients: {len(gw.clients)}")

    try:
        while True:
            # Receive a stroke from this browser client
            data = await websocket.receive_json()

            # Forward to leader — retries until a leader is available
            committed_entry = await _forward_stroke_to_leader(data)

            if committed_entry:
                # Broadcast the committed stroke to ALL connected clients
                # so every canvas stays in sync
                await _broadcast(committed_entry)

    except WebSocketDisconnect:
        log.info("Client disconnected")
    except Exception as e:
        log.warning(f"WebSocket error: {e}")
    finally:
        gw.clients.discard(websocket)
        log.info(f"Client removed. Total clients: {len(gw.clients)}")


# STROKE FORWARDING
async def _forward_stroke_to_leader(stroke: dict) -> Optional[dict]:
    """
    Forward a stroke to the current leader's /stroke endpoint.
    If the leader is unknown or returns an error, discover the new leader
    and retry up to 5 times.
    """
    for attempt in range(5):
        if not gw.leader_url:
            log.info("Leader unknown — waiting for discovery...")
            await asyncio.sleep(LEADER_RETRY_DELAY)
            continue

        try:
            async with httpx.AsyncClient(timeout=STROKE_TIMEOUT) as client:
                response = await client.post(
                    f"{gw.leader_url}/stroke", json={"stroke": stroke}
                )

            if response.status_code == 200:
                log.info(f"Stroke committed via {gw.leader_url}")
                return response.json().get("entry")

            elif response.status_code == 403:
                # This replica is no longer the leader
                log.warning(
                    f"{gw.leader_url} rejected stroke (not leader) — rediscovering"
                )
                gw.leader_url = None
                await _discover_leader()

            else:
                log.warning(
                    f"Stroke failed with status {response.status_code} — retrying"
                )
                await asyncio.sleep(LEADER_RETRY_DELAY)

        except Exception as e:
            log.warning(
                f"Stroke request to {gw.leader_url} failed: {e} — rediscovering leader"
            )
            gw.leader_url = None
            await _discover_leader()

    log.error("Failed to commit stroke after 5 attempts")
    return None


# BROADCAST
async def _broadcast(entry: dict):
    """
    Send a committed log entry to every connected WebSocket client.
    Removes clients that have silently disconnected.
    """
    dead_clients = set()
    for client in gw.clients:
        try:
            await client.send_json({"type": "stroke", "entry": entry})
        except Exception:
            dead_clients.add(client)

    for client in dead_clients:
        gw.clients.discard(client)

    if dead_clients:
        log.info(f"Removed {len(dead_clients)} dead clients. Active: {len(gw.clients)}")


# LEADER DISCOVERY
async def _discover_leader():
    """
    Poll all replicas' /health endpoint to find who is currently the leader.
    Sets gw.leader_url when found.
    """
    async with httpx.AsyncClient(timeout=1.0) as client:
        tasks = [client.get(f"{url}/health") for url in REPLICA_URLS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for url, result in zip(REPLICA_URLS, results):
        if isinstance(result, Exception):
            log.warning(f"Health check failed for {url}: {result}")
            continue
        if result.status_code == 200:
            data = result.json()
            if data.get("role") == "leader":
                if gw.leader_url != url:
                    log.info(f" -> Leader discovered: {url} (term {data.get('term')})")
                    gw.leader_url = url
                return

    log.warning("No leader found in this poll — election may be in progress")


async def _leader_poll_loop():
    """
    Background task: continuously poll replicas to keep track of the leader.
    This handles automatic failover — if the leader changes, gw.leader_url updates.
    """
    log.info("Leader discovery loop started")
    while True:
        await _discover_leader()
        await asyncio.sleep(LEADER_POLL_INTERVAL)


# STARTUP
@app.on_event("startup")
async def startup():
    asyncio.create_task(_leader_poll_loop())
    log.info(f"Gateway started. Replicas: {REPLICA_URLS}")
