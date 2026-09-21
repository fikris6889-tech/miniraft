"""Wire format for the two Raft RPCs (RequestVote, AppendEntries) plus
a client-facing command RPC. Field names match the Raft paper's
Figure 2 so you can implement the handlers directly against the paper.

Fully implemented — serialization plumbing, not algorithm.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, List, Optional

from .log import LogEntry


@dataclass
class RequestVoteArgs:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "RequestVoteArgs":
        return RequestVoteArgs(**d)


@dataclass
class RequestVoteReply:
    term: int
    vote_granted: bool

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "RequestVoteReply":
        return RequestVoteReply(**d)


@dataclass
class AppendEntriesArgs:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[dict] = field(default_factory=list)  # LogEntry.to_dict()
    leader_commit: int = 0

    def to_dict(self) -> dict:
        return {
            "term": self.term,
            "leader_id": self.leader_id,
            "prev_log_index": self.prev_log_index,
            "prev_log_term": self.prev_log_term,
            "entries": self.entries,
            "leader_commit": self.leader_commit,
        }

    def entries_as_log_entries(self) -> List[LogEntry]:
        return [LogEntry.from_dict(e) for e in self.entries]

    @staticmethod
    def from_dict(d: dict) -> "AppendEntriesArgs":
        return AppendEntriesArgs(**d)


@dataclass
class AppendEntriesReply:
    term: int
    success: bool
    # Not in the base paper, but a well-known optimization (§5.3) that
    # lets a leader back off a conflicting follower's log faster than
    # one entry at a time. Optional TODO for the ambitious on Day 3.
    conflict_index: Optional[int] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "AppendEntriesReply":
        return AppendEntriesReply(**d)


@dataclass
class ClientCommandArgs:
    command: dict  # e.g. {"op": "set", "key": "x", "value": "1"}

    def to_dict(self) -> dict:
        return {"command": self.command}

    @staticmethod
    def from_dict(d: dict) -> "ClientCommandArgs":
        return ClientCommandArgs(command=d["command"])


@dataclass
class ClientCommandReply:
    ok: bool
    result: Any = None
    leader_hint: Optional[str] = None  # if we're not leader, point the client at who is
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "ClientCommandReply":
        return ClientCommandReply(**d)
