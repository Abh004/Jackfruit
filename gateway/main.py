"""
gateway/main.py  —  Robust WebSocket Gateway
============================================
Responsibilities:
  - Synchronize new clients with full canvas state upon connection.
  - Transparently handle leader failover during active drawing sessions.
  - Broadcast committed strokes to maintain real-time consistency.
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
    # Assumes static/index.html is provided as per project deliverables
    return HTMLResponse(content=open("/usr/src/app/static/index.html").read())


@app.get("/health")
async def health():
    return {
        "leader_url": gw.leader_url,
        "connected_clients": len(gw.clients),
        "replicas": REPLICA_URLS,
    }


async def _sync_new_client(websocket: WebSocket):
    """
    Fetches the current committed state from the RAFT leader
    and sends it to the newly connected client.
    """
    if not gw.leader_url:
        await _discover_leader()

    if gw.leader_url:
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                response = await client.get(f"{gw.leader_url}/canvas-state")
                if response.status_code == 200:
                    state = response.json()
                    # Send entire committed log to the new client
                    await websocket.send_json(
                        {
                            "type": "init",
                            "log": state.get("log", []),
                            "commit_index": state.get("commit_index", -1),
                        }
                    )
                    log.info(
                        f"Synchronized new client with {len(state.get('log', []))} strokes"
                    )
        except Exception as e:
            log.warning(f"Failed to sync new client: {e}")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    gw.clients.add(websocket)

    # Requirement: Consistent canvas state after restarts
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
    except Exception as e:
        log.warning(f"WebSocket error: {e}")
    finally:
        gw.clients.discard(websocket)


async def _forward_stroke_to_leader(stroke: dict) -> Optional[dict]:
    """
    Robust forwarding with up to 10 retries to handle election windows.
    """
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

            # If 403 (Not Leader), the replica stepped down; reset and rediscover
            if response.status_code == 403:
                log.warning(
                    f"Replica {gw.leader_url} is no longer leader. Rediscovering..."
                )
                gw.leader_url = None
                await _discover_leader()
            else:
                await asyncio.sleep(LEADER_RETRY_DELAY)

        except Exception as e:
            log.warning(f"Leader {gw.leader_url} unreachable: {e}. Rediscovering...")
            gw.leader_url = None
            await _discover_leader()

    log.error("Failed to commit stroke after multiple retries")
    return None


async def _broadcast(entry: dict):
    dead_clients = set()
    for client in gw.clients:
        try:
            await client.send_json({"type": "stroke", "entry": entry})
        except Exception:
            dead_clients.add(client)
    for client in dead_clients:
        gw.clients.discard(client)


async def _discover_leader():
    """
    Polls all replicas to identify the current RAFT leader.
    """
    async with httpx.AsyncClient(timeout=1.0) as client:
        tasks = [client.get(f"{url}/health") for url in REPLICA_URLS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for url, result in zip(REPLICA_URLS, results):
        if not isinstance(result, Exception) and result.status_code == 200:
            data = result.json()
            if data.get("role") == "leader":
                if gw.leader_url != url:
                    log.info(f"Leader discovered: {url} (Term {data.get('term')})")
                    gw.leader_url = url
                return


async def _leader_poll_loop():
    while True:
        await _discover_leader()
        await asyncio.sleep(LEADER_POLL_INTERVAL)


@app.on_event("startup")
async def startup():
    asyncio.create_task(_leader_poll_loop())
    log.info(f"Gateway started. Tracking replicas: {REPLICA_URLS}")
