"""
replica/main.py  —  Mini-RAFT Replica Node
==========================================
Implements:
  - Follower / Candidate / Leader state machine
  - Leader election with randomized timeouts
  - Heartbeat sending (leader) and receiving (follower)
  - AppendEntries log replication
  - RequestVote RPC
  - /sync-log catch-up for restarted nodes
  - /health endpoint (required by Docker healthcheck)

Environment variables (set by docker-compose):
  REPLICA_ID   : unique integer ID  e.g. 1, 2, 3
  PORT         : port this server listens on  e.g. 5000
  PEERS        : comma-separated peer URLs  e.g. http://replica2:5000,http://replica3:5000
"""

import asyncio
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
REPLICA_ID   = int(os.getenv("REPLICA_ID", "1"))
PORT         = int(os.getenv("PORT", "5000"))
PEER_URLS    = [p.strip() for p in os.getenv("PEERS", "").split(",") if p.strip()]

HEARTBEAT_INTERVAL   = 0.15          # 150 ms  — leader sends heartbeats this often
ELECTION_TIMEOUT_MIN = 0.5           # 500 ms  \
ELECTION_TIMEOUT_MAX = 0.8           # 800 ms  /  follower waits random time in this range

logging.basicConfig(
    level=logging.INFO,
    format=f"[Replica {REPLICA_ID}] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

# STATE
class Role(str, Enum):
    FOLLOWER  = "follower"
    CANDIDATE = "candidate"
    LEADER    = "leader"

class RaftState:
    def __init__(self):
        # Persistent state
        self.current_term: int = 0
        self.voted_for: Optional[int] = None   # which candidate we voted for this term

        # Volatile state
        self.role: Role = Role.FOLLOWER
        self.leader_id: Optional[int] = None

        # Log — each entry: {"term": int, "index": int, "stroke": dict}
        self.log: list[dict] = []
        self.commit_index: int = -1             # index of last committed entry

        # Election timer
        self.last_heartbeat: float = time.time()
        self.election_timeout: float = self._new_timeout()

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

# PYDANTIC SCHEMAS
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
    prev_log_index: int          # index of entry immediately before new ones
    prev_log_term: int
    entries: list[dict]          # empty list = heartbeat
    leader_commit: int

class AppendEntriesResponse(BaseModel):
    term: int
    success: bool
    match_index: int             # follower's last matched index (for catch-up logic)

class SyncLogRequest(BaseModel):
    from_index: int              # follower wants entries from this index onward

class StrokeEntry(BaseModel):
    stroke: dict                 # raw drawing data from the gateway

# FASTAPI APP
app = FastAPI(title=f"RAFT Replica {REPLICA_ID}")

# HEALTH  (required by docker-compose healthcheck)
@app.get("/health")
async def health():
    return {
        "replica_id"   : REPLICA_ID,
        "role"         : state.role,
        "term"         : state.current_term,
        "leader_id"    : state.leader_id,
        "log_length"   : len(state.log),
        "commit_index" : state.commit_index,
    }

# STATUS  (handy for debugging)
@app.get("/status")
async def status():
    return {
        "replica_id"   : REPLICA_ID,
        "role"         : state.role,
        "term"         : state.current_term,
        "leader_id"    : state.leader_id,
        "log"          : state.log,
        "commit_index" : state.commit_index,
        "peers"        : PEER_URLS,
    }

# REQUEST VOTE RPC
@app.post("/request-vote", response_model=VoteResponse)
async def request_vote(req: VoteRequest):
    """
    A candidate asks us to vote for it.
    Rules:
      1. Reject if candidate's term < our term.
      2. If candidate's term > our term → step down to follower, update term.
      3. Grant vote if we haven't voted this term AND candidate's log is at least as up-to-date.
    """
    if req.term < state.current_term:
        log.info(f"Rejecting vote for {req.candidate_id}: stale term {req.term}")
        return VoteResponse(term=state.current_term, vote_granted=False)

    if req.term > state.current_term:
        _step_down(req.term)

    already_voted = (state.voted_for is not None and state.voted_for != req.candidate_id)
    log_ok = (
        req.last_log_term > state.last_log_term()
        or (req.last_log_term == state.last_log_term() and req.last_log_index >= state.last_log_index())
    )

    if already_voted or not log_ok:
        log.info(f"Rejecting vote for {req.candidate_id}: already_voted={already_voted} log_ok={log_ok}")
        return VoteResponse(term=state.current_term, vote_granted=False)

    state.voted_for = req.candidate_id
    state.reset_election_timer()
    log.info(f"Granted vote to candidate {req.candidate_id} for term {req.term}")
    return VoteResponse(term=state.current_term, vote_granted=True)

# APPEND ENTRIES RPC  (also used as heartbeat)
@app.post("/append-entries", response_model=AppendEntriesResponse)
async def append_entries(req: AppendEntriesRequest):
    """
    Receives log entries (or empty heartbeat) from the leader.
    Steps:
      1. Reject if leader's term is stale.
      2. Accept leader — reset election timer.
      3. Consistency check: our log must contain an entry at prev_log_index with matching term.
      4. Append new entries, overwriting conflicts.
      5. Advance commit_index if leader says so.
    """
    if req.term < state.current_term:
        return AppendEntriesResponse(
            term=state.current_term, success=False,
            match_index=state.last_log_index()
        )

    # Valid leader — step down if we were candidate/leader
    if req.term > state.current_term or state.role != Role.FOLLOWER:
        _step_down(req.term)

    state.leader_id = req.leader_id
    state.reset_election_timer()

    # ── Consistency check ──────────────────────────────────────────────
    if req.prev_log_index >= 0:
        if req.prev_log_index > state.last_log_index():
            # We're missing entries — tell leader where our log ends
            log.warning(f"Missing entries: our log ends at {state.last_log_index()}, leader wants prev={req.prev_log_index}")
            return AppendEntriesResponse(
                term=state.current_term, success=False,
                match_index=state.last_log_index()
            )
        if state.log[req.prev_log_index]["term"] != req.prev_log_term:
            # Conflicting entry — truncate from here
            state.log = state.log[:req.prev_log_index]
            log.warning(f"Term conflict at index {req.prev_log_index}, truncated log")
            return AppendEntriesResponse(
                term=state.current_term, success=False,
                match_index=state.last_log_index()
            )

    # ── Append new entries ─────────────────────────────────────────────
    for entry in req.entries:
        insert_index = entry["index"]
        if insert_index <= state.last_log_index():
            if state.log[insert_index]["term"] != entry["term"]:
                state.log = state.log[:insert_index]   # overwrite conflict
            else:
                continue                                # already have it
        state.log.append(entry)
        log.info(f"Appended log entry index={entry['index']} term={entry['term']}")

    # ── Advance commit index ───────────────────────────────────────────
    if req.leader_commit > state.commit_index:
        state.commit_index = min(req.leader_commit, state.last_log_index())
        log.info(f"Commit index advanced to {state.commit_index}")

    return AppendEntriesResponse(
        term=state.current_term, success=True,
        match_index=state.last_log_index()
    )

# SYNC LOG  (catch-up for restarted nodes)
@app.post("/sync-log")
async def sync_log(req: SyncLogRequest):
    """
    Called by a follower that has fallen behind.
    Returns all committed entries from req.from_index onward.
    Only the leader should respond meaningfully here.
    """
    if state.role != Role.LEADER:
        raise HTTPException(status_code=403, detail="Not the leader")

    missing = [
        e for e in state.log
        if e["index"] >= req.from_index and e["index"] <= state.commit_index
    ]
    log.info(f"Sync-log requested from index {req.from_index}: sending {len(missing)} entries")
    return {"entries": missing, "commit_index": state.commit_index}

# STROKE  (called by Gateway to submit a new stroke)
@app.post("/stroke")
async def receive_stroke(entry: StrokeEntry):
    """
    Only the leader accepts strokes from the Gateway.
    Steps:
      1. Append to local log.
      2. Replicate to followers (AppendEntries).
      3. Commit when majority ACK.
    Returns the committed log entry so the Gateway can broadcast it.
    """
    if state.role != Role.LEADER:
        raise HTTPException(
            status_code=403,
            detail=f"Not the leader. Current leader is replica {state.leader_id}"
        )

    new_index = len(state.log)
    log_entry = {
        "index"  : new_index,
        "term"   : state.current_term,
        "stroke" : entry.stroke,
    }
    state.log.append(log_entry)
    log.info(f"Leader appended stroke at index {new_index}")

    acks = await _replicate_entry(log_entry)
    majority = (len(PEER_URLS) + 1) // 2 + 1   # e.g. 2 out of 3

    if acks + 1 >= majority:   # +1 counts the leader itself
        state.commit_index = new_index
        log.info(f"Committed entry {new_index} with {acks+1} acks")
        return {"committed": True, "entry": log_entry}
    else:
        log.warning(f"Failed to commit entry {new_index}: only {acks+1} acks")
        raise HTTPException(status_code=500, detail="Failed to achieve majority")

# INTERNAL HELPERS
def _step_down(new_term: int):
    """Revert to follower state when we see a higher term."""
    log.info(f"Stepping down: term {state.current_term} → {new_term}")
    state.current_term = new_term
    state.role         = Role.FOLLOWER
    state.voted_for    = None
    state.leader_id    = None
    state.reset_election_timer()


async def _replicate_entry(entry: dict) -> int:
    """
    Send AppendEntries to all peers and count successful ACKs.
    Returns number of peers that responded with success=True.
    """
    acks = 0
    prev_index = entry["index"] - 1
    prev_term  = state.log[prev_index]["term"] if prev_index >= 0 else 0

    payload = AppendEntriesRequest(
        term            = state.current_term,
        leader_id       = REPLICA_ID,
        prev_log_index  = prev_index,
        prev_log_term   = prev_term,
        entries         = [entry],
        leader_commit   = state.commit_index,
    )

    async with httpx.AsyncClient(timeout=1.0) as client:
        tasks = [
            client.post(f"{peer}/append-entries", json=payload.model_dump())
            for peer in PEER_URLS
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for peer, result in zip(PEER_URLS, results):
        if isinstance(result, Exception):
            log.warning(f"Replication to {peer} failed: {result}")
            continue
        if result.status_code == 200:
            data = result.json()
            if data.get("success"):
                acks += 1
            else:
                # Peer is behind — trigger catch-up asynchronously
                asyncio.create_task(_catchup_peer(peer, data.get("match_index", -1) + 1))

    return acks


async def _catchup_peer(peer_url: str, from_index: int):
    """Push missing committed entries to a lagging follower."""
    missing = [e for e in state.log if e["index"] >= from_index and e["index"] <= state.commit_index]
    if not missing:
        return
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            await client.post(f"{peer_url}/sync-log", json={"from_index": from_index})
        log.info(f"Catch-up sent {len(missing)} entries to {peer_url} from index {from_index}")
    except Exception as e:
        log.warning(f"Catch-up to {peer_url} failed: {e}")


async def _send_heartbeats():
    """Leader sends empty AppendEntries to all peers every HEARTBEAT_INTERVAL."""
    payload = {
        "term"           : state.current_term,
        "leader_id"      : REPLICA_ID,
        "prev_log_index" : state.last_log_index(),
        "prev_log_term"  : state.last_log_term(),
        "entries"        : [],
        "leader_commit"  : state.commit_index,
    }
    async with httpx.AsyncClient(timeout=0.5) as client:
        tasks = [client.post(f"{peer}/append-entries", json=payload) for peer in PEER_URLS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for peer, result in zip(PEER_URLS, results):
        if isinstance(result, Exception):
            log.warning(f"Heartbeat to {peer} failed: {result}")
        elif result.status_code == 200:
            data = result.json()
            if data.get("term", 0) > state.current_term:
                _step_down(data["term"])   # higher term found — step down


async def _start_election():
    """Transition to candidate and request votes from all peers."""
    state.role          = Role.CANDIDATE
    state.current_term += 1
    state.voted_for     = REPLICA_ID     # vote for ourselves
    state.leader_id     = None
    state.reset_election_timer()

    log.info(f"Starting election for term {state.current_term}")

    vote_request = {
        "term"           : state.current_term,
        "candidate_id"   : REPLICA_ID,
        "last_log_index" : state.last_log_index(),
        "last_log_term"  : state.last_log_term(),
    }

    votes = 1   # count our own vote
    async with httpx.AsyncClient(timeout=0.5) as client:
        tasks = [client.post(f"{peer}/request-vote", json=vote_request) for peer in PEER_URLS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for peer, result in zip(PEER_URLS, results):
        if isinstance(result, Exception):
            log.warning(f"Vote request to {peer} failed: {result}")
            continue
        if result.status_code == 200:
            data = result.json()
            if data.get("term", 0) > state.current_term:
                _step_down(data["term"])
                return
            if data.get("vote_granted"):
                votes += 1
                log.info(f"Got vote from {peer} — total {votes}")

    majority = (len(PEER_URLS) + 1) // 2 + 1
    if state.role == Role.CANDIDATE and votes >= majority:
        state.role      = Role.LEADER
        state.leader_id = REPLICA_ID
        log.info(f"🏆 Became LEADER for term {state.current_term} with {votes} votes")
    else:
        log.info(f"Election failed ({votes} votes) — reverting to follower")
        _step_down(state.current_term)

# BACKGROUND TASK  — election timer + heartbeat loop
async def _raft_loop():
    """
    Main RAFT background loop.
    - If LEADER   → send heartbeats every 150 ms
    - If FOLLOWER → check if election timeout has elapsed → start election
    - If CANDIDATE → election is already in progress (handled in _start_election)
    """
    log.info(f"RAFT loop started. Role: {state.role}, Peers: {PEER_URLS}")
    while True:
        await asyncio.sleep(0.05)   # tick every 50 ms

        if state.role == Role.LEADER:
            await _send_heartbeats()
            await asyncio.sleep(HEARTBEAT_INTERVAL - 0.05)

        elif state.role == Role.FOLLOWER:
            elapsed = time.time() - state.last_heartbeat
            if elapsed >= state.election_timeout:
                log.info(f"Election timeout after {elapsed:.2f}s — starting election")
                await _start_election()


@app.on_event("startup")
async def startup():
    asyncio.create_task(_raft_loop())
    log.info(f"Replica {REPLICA_ID} started on port {PORT}")
