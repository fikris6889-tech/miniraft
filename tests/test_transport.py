"""Tests for the HTTP RPC transport layer, independent of any real
RaftNode — we use a minimal fake node so this test only exercises
transport.py, not node.py's (still-stubbed) algorithm."""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from miniraft.state import NodeState
from miniraft.transport import RPCClient, RPCServer


class FakeNode:
    """Just enough surface area for RPCHandler to dispatch to."""

    def __init__(self, node_id="fake"):
        self.state = NodeState(node_id=node_id)
        self.received = []

    def handle_request_vote(self, payload):
        self.received.append(("request_vote", payload))
        return {"term": 5, "vote_granted": True}

    def handle_append_entries(self, payload):
        self.received.append(("append_entries", payload))
        return {"term": 5, "success": True, "conflict_index": None}

    def handle_client_command(self, payload):
        self.received.append(("client_command", payload))
        return {"ok": True, "result": {"value": "42"}, "leader_hint": None, "error": None}


class TestTransport(unittest.TestCase):
    def setUp(self):
        self.node = FakeNode()
        self.port = 9500 + (os.getpid() % 400)  # avoid collisions across parallel test runs
        self.server = RPCServer("127.0.0.1", self.port, self.node)
        self.server.start()
        self.client = RPCClient(timeout=2.0)
        self.base_url = f"http://127.0.0.1:{self.port}"

    def tearDown(self):
        self.server.stop()

    def test_health_endpoint(self):
        reply = self.client.health(self.base_url)
        self.assertIsNotNone(reply)
        self.assertEqual(reply["node_id"], "fake")
        self.assertEqual(reply["role"], "follower")

    def test_request_vote_round_trip(self):
        reply = self.client.call(
            self.base_url,
            "/rpc/request_vote",
            {"term": 1, "candidate_id": "node2", "last_log_index": 0, "last_log_term": 0},
        )
        self.assertEqual(reply, {"term": 5, "vote_granted": True})
        self.assertEqual(self.node.received[0][0], "request_vote")

    def test_append_entries_round_trip(self):
        reply = self.client.call(
            self.base_url,
            "/rpc/append_entries",
            {
                "term": 1,
                "leader_id": "node1",
                "prev_log_index": 0,
                "prev_log_term": 0,
                "entries": [],
                "leader_commit": 0,
            },
        )
        self.assertEqual(reply["success"], True)

    def test_unknown_path_returns_404(self):
        reply = self.client.call(self.base_url, "/rpc/does_not_exist", {})
        # urllib raises HTTPError for 404, which RPCClient.call swallows -> None
        self.assertIsNone(reply)

    def test_unreachable_peer_returns_none_not_exception(self):
        # nothing listening on this port
        dead_url = "http://127.0.0.1:1"
        reply = self.client.call(dead_url, "/rpc/request_vote", {})
        self.assertIsNone(reply)


if __name__ == "__main__":
    unittest.main()
