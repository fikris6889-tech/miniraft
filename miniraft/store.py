"""The replicated state machine: a trivial in-memory key-value store.

Fully implemented. Raft's job is to get every node to apply the SAME
commands in the SAME order to something like this — the store itself
doesn't know or care that it's being replicated.
"""
from __future__ import annotations

import threading
from typing import Any, Dict, Optional


class KVStore:
    def __init__(self) -> None:
        self._data: Dict[str, Any] = {}
        self._lock = threading.Lock()

    def apply(self, command: dict) -> Any:
        """Apply one committed log entry's command to the state machine.
        command shape: {"op": "set"|"delete"|"get", "key": ..., "value": ...}
        This is called ONLY with committed entries, in log order — that
        ordering guarantee is what Raft exists to provide.
        """
        op = command.get("op")
        key = command.get("key")
        with self._lock:
            if op == "set":
                self._data[key] = command.get("value")
                return {"ok": True}
            if op == "delete":
                self._data.pop(key, None)
                return {"ok": True}
            if op == "get":
                return {"ok": True, "value": self._data.get(key)}
            raise ValueError(f"unknown op: {op!r}")

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            return self._data.get(key)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._data)
