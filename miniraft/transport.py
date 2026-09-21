"""HTTP transport for MiniRaft RPCs — stdlib only (http.server +
urllib), no third-party dependencies so the cluster runs anywhere
Python 3 runs.

Fully implemented and tested. This is infrastructure: it takes JSON
in over HTTP, dispatches to whatever RaftNode handler is registered,
and returns JSON. It does not know anything about Raft's rules.

Routes:
    POST /rpc/request_vote     -> node.handle_request_vote
    POST /rpc/append_entries   -> node.handle_append_entries
    POST /rpc/client_command   -> node.handle_client_command
    GET  /health                -> node.state.snapshot()
"""
from __future__ import annotations

import json
import logging
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional

logger = logging.getLogger("miniraft.transport")


class RPCHandler(BaseHTTPRequestHandler):
    # silence the default per-request stderr logging; use `logging` instead
    def log_message(self, format: str, *args) -> None:  # noqa: A002
        logger.debug("%s - %s", self.address_string(), format % args)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        return json.loads(raw or b"{}")

    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
        node = self.server.node  # type: ignore[attr-defined]
        if self.path == "/health":
            self._send_json(200, node.state.snapshot())
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        node = self.server.node  # type: ignore[attr-defined]
        try:
            payload = self._read_json()
            if self.path == "/rpc/request_vote":
                reply = node.handle_request_vote(payload)
            elif self.path == "/rpc/append_entries":
                reply = node.handle_append_entries(payload)
            elif self.path == "/rpc/client_command":
                reply = node.handle_client_command(payload)
            else:
                self._send_json(404, {"error": "not found"})
                return
            self._send_json(200, reply)
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("RPC handler error on %s", self.path)
            self._send_json(500, {"error": str(exc)})


class RPCServer:
    """Wraps a ThreadingHTTPServer bound to (host, port) and serves it
    on a background thread so node.start() returns immediately."""

    def __init__(self, host: str, port: int, node) -> None:
        self._httpd = ThreadingHTTPServer((host, port), RPCHandler)
        self._httpd.node = node  # type: ignore[attr-defined]
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        if self._thread:
            self._thread.join(timeout=2)


class RPCClient:
    """Fire an RPC at a peer and get back a parsed dict, or None on
    failure (peer down, timeout, etc). Raft handlers must treat a
    None reply as "no answer" and move on — never block a whole
    election/heartbeat round on one unreachable peer."""

    def __init__(self, timeout: float = 0.5) -> None:
        self.timeout = timeout

    def call(self, base_url: str, path: str, payload: dict) -> Optional[dict]:
        url = f"{base_url}{path}"
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url, data=data, headers={"Content-Type": "application/json"}, method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read())
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
            return None

    def health(self, base_url: str) -> Optional[dict]:
        req = urllib.request.Request(f"{base_url}/health", method="GET")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read())
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
            return None
