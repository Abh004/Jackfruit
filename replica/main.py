"""
replica/main.py  —  Robust Mini-RAFT Replica Node
================================================
Implements:
  - Persistent state (current_term, voted_for) saved to JSON
  - Full canvas state recovery via /canvas-state
  - RAFT role transitions and log replication
  - Follower catch-up using nextIndex/matchIndex backtracking
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

        # Leader bookkeeping (peer_url -> next index / matched index)
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

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

    def init_leader_state(self):
        # On becoming leader, initialize all followers to one past last log index
        n = self.last_log_index() + 1
        self.next_index = {peer: n for peer in PEER_URLS}
        self.match_index = {peer: -1 for peer in PEER_URLS}


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


@app.get("/canvas-state")
async def get_canvas_state():
    """Returns committed entries for state recovery."""
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
        "last_log_index": state.last_log_index(),
        "leader_id": state.leader_id,
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
    state.save_persistent_state()
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

    # Consistency check
    if req.prev_log_index >= 0:
        if req.prev_log_index > state.last_log_index():
            return AppendEntriesResponse(
                term=state.current_term,
                success=False,
                match_index=state.last_log_index(),
            )
        if state.log[req.prev_log_index]["term"] != req.prev_log_term:
            return AppendEntriesResponse(
                term=state.current_term,
                success=False,
                match_index=req.prev_log_index - 1,
            )

    # Apply/merge entries
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

    ok = await _replicate_until_quorum()
    if ok:
        return {"committed": True, "entry": log_entry}
    raise HTTPException(status_code=500, detail="Quorum failed")


def _step_down(new_term: int):
    if new_term > state.current_term:
        log.info(f"Stepping down to term {new_term}")
        state.current_term = new_term
    state.role = Role.FOLLOWER
    state.voted_for = None
    state.save_persistent_state()
    state.reset_election_timer()
    state.next_index = {}
    state.match_index = {}


def _become_leader():
    state.role = Role.LEADER
    state.leader_id = REPLICA_ID
    state.init_leader_state()
    log.info(f"🏆 LEADER term {state.current_term}")


async def _send_append_to_peer(
    client: httpx.AsyncClient, peer: str, timeout: float = 0.6
) -> bool:
    """
    Try to advance one follower using nextIndex backtracking.
    Returns True if follower accepted append/heartbeat, else False.
    """
    if peer not in state.next_index:
        state.next_index[peer] = state.last_log_index() + 1
    if peer not in state.match_index:
        state.match_index[peer] = -1

    ni = max(0, state.next_index[peer])
    prev_idx = ni - 1
    prev_term = state.log[prev_idx]["term"] if prev_idx >= 0 else 0
    entries = state.log[ni:]  # send suffix from nextIndex

    payload = {
        "term": state.current_term,
        "leader_id": REPLICA_ID,
        "prev_log_index": prev_idx,
        "prev_log_term": prev_term,
        "entries": entries,
        "leader_commit": state.commit_index,
    }

    try:
        res = await client.post(f"{peer}/append-entries", json=payload, timeout=timeout)
    except Exception:
        return False

    if res.status_code != 200:
        return False

    data = res.json()
    peer_term = data.get("term", 0)
    if peer_term > state.current_term:
        _step_down(peer_term)
        return False

    if data.get("success"):
        # follower matched through prev_idx + len(entries)
        matched = prev_idx + len(entries)
        state.match_index[peer] = matched
        state.next_index[peer] = matched + 1
        return True

    # backtrack on failure
    hint = data.get("match_index")
    if isinstance(hint, int):
        state.next_index[peer] = max(0, hint + 1)
    else:
        state.next_index[peer] = max(0, ni - 1)
    return False


def _majority_count() -> int:
    # total nodes = leader + peers
    total = len(PEER_URLS) + 1
    return total // 2 + 1


def _advance_commit_index():
    """
    Leader commit rule:
    For N > commit_index, if a majority has matchIndex >= N and log[N].term == currentTerm,
    set commit_index = N.
    """
    if state.role != Role.LEADER or not state.log:
        return

    majority = _majority_count()
    # candidate indexes are leader's log indexes
    for n in range(state.last_log_index(), state.commit_index, -1):
        if state.log[n]["term"] != state.current_term:
            continue
        count = 1  # leader itself
        for peer in PEER_URLS:
            if state.match_index.get(peer, -1) >= n:
                count += 1
        if count >= majority:
            state.commit_index = n
            break


async def _replicate_until_quorum(max_rounds: int = 6) -> bool:
    """
    Replicate leader log to followers, allowing backtracking/catch-up rounds.
    """
    if state.role != Role.LEADER:
        return False

    majority = _majority_count()
    async with httpx.AsyncClient() as client:
        for _ in range(max_rounds):
            tasks = [_send_append_to_peer(client, peer) for peer in PEER_URLS]
            await asyncio.gather(*tasks, return_exceptions=True)

            _advance_commit_index()

            # Check if latest entry is committed
            if state.commit_index >= state.last_log_index():
                return True

            # Early quorum check for latest index
            latest = state.last_log_index()
            count = 1
            for peer in PEER_URLS:
                if state.match_index.get(peer, -1) >= latest:
                    count += 1
            if count >= majority:
                state.commit_index = latest
                return True

    return False


async def _raft_loop():
    while True:
        await asyncio.sleep(0.05)
        if state.role == Role.LEADER:
            await _send_heartbeats()
            await asyncio.sleep(max(HEARTBEAT_INTERVAL - 0.05, 0.01))
        elif state.role == Role.FOLLOWER:
            if time.time() - state.last_heartbeat >= state.election_timeout:
                await _start_election()
        elif state.role == Role.CANDIDATE:
            if time.time() - state.last_heartbeat >= state.election_timeout:
                await _start_election()


async def _send_heartbeats():
    if state.role != Role.LEADER:
        return
    async with httpx.AsyncClient() as client:
        tasks = [_send_append_to_peer(client, peer, timeout=0.5) for peer in PEER_URLS]
        await asyncio.gather(*tasks, return_exceptions=True)
    _advance_commit_index()


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
        if isinstance(res, Exception):
            continue
        if res.status_code != 200:
            continue

        body = res.json()
        peer_term = body.get("term", 0)
        if peer_term > state.current_term:
            _step_down(peer_term)
            return

        if body.get("vote_granted"):
            votes += 1

    if votes >= _majority_count():
        _become_leader()
        await _send_heartbeats()
    else:
        _step_down(state.current_term)


@app.on_event("startup")
async def startup():
    asyncio.create_task(_raft_loop())
