"""The replicated log. Fully implemented — this is a data structure,
not the consensus algorithm, so there's nothing to stub here.

Raft's log is 1-indexed in the paper. We keep index 0 as a sentinel
"no entry" slot so that last_index()/last_term() are well-defined
for an empty log (both return 0), which keeps the election/replication
logic you'll write on Day 2-3 free of special-casing.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional


@dataclass
class LogEntry:
    term: int
    index: int
    command: Any  # e.g. {"op": "set", "key": "x", "value": "1"}

    def to_dict(self) -> dict:
        return {"term": self.term, "index": self.index, "command": self.command}

    @staticmethod
    def from_dict(d: dict) -> "LogEntry":
        return LogEntry(term=d["term"], index=d["index"], command=d["command"])


_SENTINEL = LogEntry(term=0, index=0, command=None)


class RaftLog:
    def __init__(self) -> None:
        self._entries: List[LogEntry] = [_SENTINEL]

    def __len__(self) -> int:
        """Number of REAL entries (excludes the sentinel)."""
        return len(self._entries) - 1

    def append(self, term: int, command: Any) -> LogEntry:
        entry = LogEntry(term=term, index=self.last_index() + 1, command=command)
        self._entries.append(entry)
        return entry

    def get(self, index: int) -> Optional[LogEntry]:
        """1-indexed. Returns None if out of range."""
        if index <= 0 or index >= len(self._entries):
            return None
        return self._entries[index]

    def last_index(self) -> int:
        return self._entries[-1].index

    def last_term(self) -> int:
        return self._entries[-1].term

    def term_at(self, index: int) -> int:
        entry = self.get(index)
        return entry.term if entry else 0

    def entries_from(self, start_index: int) -> List[LogEntry]:
        """All entries with index >= start_index, in order."""
        return [e for e in self._entries[1:] if e.index >= start_index]

    def truncate_from(self, index: int) -> None:
        """Delete this entry and everything after it (used when a
        follower's log conflicts with the leader's — Raft paper
        §5.3). No-op if index is past the end."""
        self._entries = [e for e in self._entries if e.index < index]

    def append_entries(self, entries: List[LogEntry]) -> None:
        for e in entries:
            if e.index <= self.last_index():
                self.truncate_from(e.index)
            self._entries.append(e)
