"""Raft node roles and the state every node tracks.

Straight off the Raft paper (Figure 2, Diego Ongaro & John Ousterhout,
"In Search of an Understandable Consensus Algorithm"). This is plain
data — implemented and tested. The RULES for when this state changes
are what you build across Day 2/3 inside node.py.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

from .log import LogEntry, RaftLog


class Role(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class NodeState:
    """Every field below is named exactly as in the Raft paper so you
    can cross-reference the paper while you implement the TODOs.

    Persistent state (paper calls this "persistent on all servers" —
    in a production system you'd fsync these to disk before replying
    to any RPC; we keep it in memory for this teaching build and call
    that out explicitly rather than pretending otherwise):
        current_term, voted_for, log

    Volatile state (all servers):
        commit_index, last_applied, leader_id

    Volatile state (leaders only, reinitialized after election):
        next_index, match_index
    """

    node_id: str
    role: Role = Role.FOLLOWER

    # --- persistent state ---
    current_term: int = 0
    voted_for: Optional[str] = None
    log: RaftLog = field(default_factory=RaftLog)

    # --- volatile state, all servers ---
    commit_index: int = 0
    last_applied: int = 0
    # Not in the paper's Figure 2 table, but every real Raft build tracks
    # this: who does THIS node currently believe is leader? Set whenever
    # we accept a real AppendEntries (that's rule 2 recognizing "a real
    # leader"), and cleared whenever we step down for a higher term without
    # yet knowing who the new leader is. It's what lets a follower answer
    # a misdirected client write with a useful leader_hint instead of a
    # bare "not me, good luck".
    leader_id: Optional[str] = None

    # --- volatile state, leaders only ---
    next_index: Dict[str, int] = field(default_factory=dict)
    match_index: Dict[str, int] = field(default_factory=dict)

    # guards all the fields above — every RPC handler and every
    # background timer thread touches this state, so every read/write
    # must happen under self.lock. This is the #1 place teaching
    # implementations get race conditions wrong.
    lock: threading.RLock = field(default_factory=threading.RLock)

    def snapshot(self) -> dict:
        """A lock-safe read of the fields useful for /health and logging."""
        with self.lock:
            return {
                "node_id": self.node_id,
                "role": self.role.value,
                "current_term": self.current_term,
                "voted_for": self.voted_for,
                "leader_id": self.leader_id,
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "log_length": len(self.log),
            }
