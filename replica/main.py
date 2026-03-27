"""
replica/main.py  —  Robust Mini-RAFT Replica Node
================================================
Implements:
  - Persistent state (current_term, voted_for) saved to JSON
  - Full canvas state recovery via /canvas-state
  - Standard RAFT Role transitions and log replication [cite: 20, 21]
"""

import asyncio
import json
import logging
import os
import random
import time
from enum import Enum
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# CONFIG
REPLICA_ID = int(os.getenv("REPLICA_ID", "1"))
PORT = int(os.getenv("PORT", "5000"))
PEER_URLS = [p.strip() for p in os.getenv("PEERS", "").split(",") if p.strip()]
STATE_FILE = f"state_replica_{REPLICA_ID}.json"

HEARTBEAT_INTERVAL = 0.15
ELECTION_TIMEOUT_MIN = 0.5
ELECTION_TIMEOUT_MAX = 0.8

logging.basicConfig(
    level=logging.INFO, format=f"[Replica {REPLICA_ID}] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)


class Role(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftState:
    def __init__(self):
        # Persistent state
        self.current_term, self.voted_for = self.load_persistent_state()

        # Volatile state
        self.role: Role = Role.FOLLOWER
        self.leader_id: Optional[int] = None
        self.log: list[dict] = []
        self.commit_index: int = -1

        # Election timer
        self.last_heartbeat: float = time.time()
        self.election_timeout: float = self._new_timeout()

    def load_persistent_state(self):
        try:
            with open(STATE_FILE, "r") as f:
                data = json.load(f)
                return data.get("current_term", 0), data.get("voted_for")
        except (FileNotFoundError, json.JSONDecodeError):
            return 0, None

    def save_persistent_state(self):
        with open(STATE_FILE, "w") as f:
            json.dump(
                {"current_term": self.current_term, "voted_for": self.voted_for}, f
            )

    def _new_timeout(self) -> float:
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    def reset_election_timer(self):
        self.last_heartbeat = time.time()
        self.election_timeout = self._new_timeout()

    def last_log_index(self) -> int:
        return len(self.log) - 1

    def last_log_term(self) -> int:
        return self.log[-1]["term"] if self.log else 0


state = RaftState()


# SCHEMAS
class VoteRequest(BaseModel):
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


class VoteResponse(BaseModel):
    term: int
    vote_granted: bool


class AppendEntriesRequest(BaseModel):
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: list[dict]
    leader_commit: int


class AppendEntriesResponse(BaseModel):
    term: int
    success: bool
    match_index: int


class StrokeEntry(BaseModel):
    stroke: dict


app = FastAPI(title=f"RAFT Replica {REPLICA_ID}")


# NEW: State Recovery Endpoint
@app.get("/canvas-state")
async def get_canvas_state():
    """Returns the full committed log for consistent canvas state after restarts."""
    return {
        "log": [e for e in state.log if e["index"] <= state.commit_index],
        "commit_index": state.commit_index,
        "term": state.current_term,
    }


@app.get("/health")
async def health():
    return {
        "replica_id": REPLICA_ID,
        "role": state.role,
        "term": state.current_term,
        "commit_index": state.commit_index,
    }


@app.post("/request-vote", response_model=VoteResponse)
async def request_vote(req: VoteRequest):
    if req.term < state.current_term:
        return VoteResponse(term=state.current_term, vote_granted=False)

    if req.term > state.current_term:
        _step_down(req.term)

    already_voted = state.voted_for is not None and state.voted_for != req.candidate_id
    log_ok = req.last_log_term > state.last_log_term() or (
        req.last_log_term == state.last_log_term()
        and req.last_log_index >= state.last_log_index()
    )

    if already_voted or not log_ok:
        return VoteResponse(term=state.current_term, vote_granted=False)

    state.voted_for = req.candidate_id
    state.save_persistent_state()  # Persist vote
    state.reset_election_timer()
    return VoteResponse(term=state.current_term, vote_granted=True)


@app.post("/append-entries", response_model=AppendEntriesResponse)
async def append_entries(req: AppendEntriesRequest):
    if req.term < state.current_term:
        return AppendEntriesResponse(
            term=state.current_term, success=False, match_index=state.last_log_index()
        )

    if req.term > state.current_term or state.role != Role.FOLLOWER:
        _step_down(req.term)

    state.leader_id = req.leader_id
    state.reset_election_timer()

    # Consistency Check
    if req.prev_log_index >= 0:
        if (
            req.prev_log_index > state.last_log_index()
            or state.log[req.prev_log_index]["term"] != req.prev_log_term
        ):
            return AppendEntriesResponse(
                term=state.current_term,
                success=False,
                match_index=state.last_log_index(),
            )

    # Log Update
    for entry in req.entries:
        idx = entry["index"]
        if idx <= state.last_log_index():
            if state.log[idx]["term"] != entry["term"]:
                state.log = state.log[:idx]
            else:
                continue
        state.log.append(entry)

    if req.leader_commit > state.commit_index:
        state.commit_index = min(req.leader_commit, state.last_log_index())

    return AppendEntriesResponse(
        term=state.current_term, success=True, match_index=state.last_log_index()
    )


@app.post("/stroke")
async def receive_stroke(entry: StrokeEntry):
    if state.role != Role.LEADER:
        raise HTTPException(status_code=403, detail=f"Leader is {state.leader_id}")

    log_entry = {
        "index": len(state.log),
        "term": state.current_term,
        "stroke": entry.stroke,
    }
    state.log.append(log_entry)

    acks = await _replicate_entry(log_entry)
    if acks + 1 >= (len(PEER_URLS) + 1) // 2 + 1:
        state.commit_index = log_entry["index"]
        return {"committed": True, "entry": log_entry}
    raise HTTPException(status_code=500, detail="Quorum failed")


def _step_down(new_term: int):
    log.info(f"Stepping down to term {new_term}")
    state.current_term = new_term
    state.role = Role.FOLLOWER
    state.voted_for = None
    state.save_persistent_state()  # Persist state
    state.reset_election_timer()


async def _replicate_entry(entry: dict) -> int:
    acks = 0
    payload = AppendEntriesRequest(
        term=state.current_term,
        leader_id=REPLICA_ID,
        prev_log_index=entry["index"] - 1,
        prev_log_term=state.log[entry["index"] - 1]["term"]
        if entry["index"] > 0
        else 0,
        entries=[entry],
        leader_commit=state.commit_index,
    )
    async with httpx.AsyncClient(timeout=1.0) as client:
        tasks = [
            client.post(f"{peer}/append-entries", json=payload.model_dump())
            for peer in PEER_URLS
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for res in results:
        if (
            not isinstance(res, Exception)
            and res.status_code == 200
            and res.json().get("success")
        ):
            acks += 1
    return acks


async def _raft_loop():
    while True:
        await asyncio.sleep(0.05)
        if state.role == Role.LEADER:
            await _send_heartbeats()
            await asyncio.sleep(HEARTBEAT_INTERVAL - 0.05)
        elif state.role == Role.FOLLOWER:
            if time.time() - state.last_heartbeat >= state.election_timeout:
                await _start_election()


async def _send_heartbeats():
    payload = {
        "term": state.current_term,
        "leader_id": REPLICA_ID,
        "prev_log_index": state.last_log_index(),
        "prev_log_term": state.last_log_term(),
        "entries": [],
        "leader_commit": state.commit_index,
    }
    async with httpx.AsyncClient(timeout=0.5) as client:
        tasks = [
            client.post(f"{peer}/append-entries", json=payload) for peer in PEER_URLS
        ]
        await asyncio.gather(*tasks, return_exceptions=True)


async def _start_election():
    state.role = Role.CANDIDATE
    state.current_term += 1
    state.voted_for = REPLICA_ID
    state.save_persistent_state()
    state.reset_election_timer()
    log.info(f"Starting election term {state.current_term}")

    votes = 1
    req = {
        "term": state.current_term,
        "candidate_id": REPLICA_ID,
        "last_log_index": state.last_log_index(),
        "last_log_term": state.last_log_term(),
    }
    async with httpx.AsyncClient(timeout=0.5) as client:
        tasks = [client.post(f"{peer}/request-vote", json=req) for peer in PEER_URLS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for res in results:
        if (
            not isinstance(res, Exception)
            and res.status_code == 200
            and res.json().get("vote_granted")
        ):
            votes += 1

    if votes >= (len(PEER_URLS) + 1) // 2 + 1:
        state.role = Role.LEADER
        log.info(f"🏆 LEADER term {state.current_term}")
    else:
        _step_down(state.current_term)


@app.on_event("startup")
async def startup():
    asyncio.create_task(_raft_loop())
