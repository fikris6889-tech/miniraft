"""Day 4 behavioral tests: linearizable reads via ReadIndex.

Same split as every prior day: fast, deterministic direct-call unit
tests pin down the individual rules, then real multi-node HTTP
integration tests prove the emergent, whole-cluster behavior —
including the one that actually justifies this feature existing at
all: a partitioned, stale "leader" must never answer a read.

  * TestConfirmLeadershipWithMajority — direct calls to
    _confirm_leadership_with_majority: the single-node fast path, a
    real majority of reachable peers succeeding, and an unreachable
    majority correctly failing (fast, via a short patched timeout —
    no need to burn the real CLIENT_COMMAND_TIMEOUT to prove a
    negative).

  * TestLinearizableReadRules — direct calls to handle_client_command
    with a `get` op: non-leader replies with leader_hint exactly like
    a write does, and a single-node cluster answers a `get`
    synchronously without touching the log at all.

  * TestReadsIntegration — a real 3-node cluster over real HTTP:
    a `get` on the leader returns the last committed value, repeated
    reads never grow the log (the whole point of NOT routing them
    through _serve_write), a follower redirects a `get` with
    leader_hint just like it does for a write, and — the flagship
    test — a leader that's been cut off from the rest of the cluster
    refuses to answer a `get` instead of silently returning stale
    data, even though the value is sitting right there in its own
    local store.
"""
import itertools
import os
import sys
import time
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from miniraft.config import ClusterConfig
from miniraft.node import RaftNode, ELECTION_TIMEOUT_MAX
from miniraft.state import Role
from miniraft.transport import RPCClient

_next_base_port = itertools.count(9990 + (os.getpid() % 100) * 100, 10)


def _bare_node(node_id="node1", peer_ids=("node2", "node3")):
    ids = [node_id] + list(peer_ids)
    cluster = ClusterConfig.local_cluster(ids, base_port=next(_next_base_port))
    return RaftNode(node_id, cluster)


class TestConfirmLeadershipWithMajority(unittest.TestCase):
    """Direct, timer-free tests of _confirm_leadership_with_majority."""

    def test_single_node_cluster_confirms_instantly_with_no_network(self):
        node = _bare_node(node_id="solo", peer_ids=())
        with node.state.lock:
            node.state.role = Role.LEADER
            node.state.current_term = 1
        started = time.monotonic()
        confirmed = node._confirm_leadership_with_majority(term=1)
        elapsed = time.monotonic() - started
        self.assertTrue(confirmed)
        self.assertLess(elapsed, 0.1, "no peers to ask — must not wait on anything")

    def test_not_currently_leader_fails_immediately(self):
        node = _bare_node()
        with node.state.lock:
            node.state.role = Role.FOLLOWER
            node.state.current_term = 1
        self.assertFalse(node._confirm_leadership_with_majority(term=1))

    def test_stale_term_fails_immediately(self):
        node = _bare_node()
        with node.state.lock:
            node.state.role = Role.LEADER
            node.state.current_term = 5
        # Asking to confirm an OLD term we've since moved past.
        self.assertFalse(node._confirm_leadership_with_majority(term=3))

    def test_unreachable_peers_fail_to_confirm_within_a_bounded_time(self):
        # Peers point at real addresses nothing is listening on — every
        # probe returns None (connection refused). With a patched-down
        # CLIENT_COMMAND_TIMEOUT this proves the "can't reach a
        # majority" negative case fast instead of burning 5 real
        # seconds to prove it.
        node = _bare_node(node_id="node1", peer_ids=("node2", "node3"))
        with node.state.lock:
            node.state.role = Role.LEADER
            node.state.current_term = 1
        with mock.patch("miniraft.node.CLIENT_COMMAND_TIMEOUT", 0.3):
            started = time.monotonic()
            confirmed = node._confirm_leadership_with_majority(term=1)
            elapsed = time.monotonic() - started
        self.assertFalse(confirmed)
        self.assertLess(elapsed, 1.0, "should fail fast, bounded by the patched timeout")

    def test_real_majority_of_reachable_peers_confirms(self):
        # Boot two real peer nodes so the probes have someone real to
        # talk to and ack, without needing a full election first.
        base_port = 9800 + (os.getpid() % 100) * 3
        cluster = ClusterConfig.local_cluster(["node1", "node2", "node3"], base_port=base_port)
        node1 = RaftNode("node1", cluster)
        node2 = RaftNode("node2", cluster)
        node3 = RaftNode("node3", cluster)
        node2.start()
        node3.start()
        try:
            with node1.state.lock:
                node1.state.role = Role.LEADER
                node1.state.current_term = 1
            confirmed = node1._confirm_leadership_with_majority(term=1)
            self.assertTrue(confirmed)
            # The probe must NOT have touched either follower's log or
            # commit_index — it's supposed to be a pure no-op RPC.
            self.assertEqual(len(node2.state.log), 0)
            self.assertEqual(len(node3.state.log), 0)
            self.assertEqual(node2.state.commit_index, 0)
            self.assertEqual(node3.state.commit_index, 0)
        finally:
            node2.stop()
            node3.stop()


class TestLinearizableReadRules(unittest.TestCase):
    """Direct calls to handle_client_command with a `get` op."""

    def test_non_leader_get_replies_not_ok_with_leader_hint(self):
        node = _bare_node()
        with node.state.lock:
            node.state.role = Role.FOLLOWER
            node.state.leader_id = "node2"
        reply = node.handle_client_command({"command": {"op": "get", "key": "x"}})
        self.assertFalse(reply["ok"])
        self.assertEqual(reply["leader_hint"], "node2")

    def test_single_node_cluster_get_is_synchronous_and_does_not_touch_the_log(self):
        node = _bare_node(node_id="solo", peer_ids=())
        with node.state.lock:
            node.state.role = Role.LEADER
            node.state.current_term = 1
            node.state.leader_id = node.node_id
        write_reply = node.handle_client_command({"command": {"op": "set", "key": "x", "value": "42"}})
        self.assertTrue(write_reply["ok"])
        self.assertEqual(len(node.state.log), 1, "one write = one log entry")

        started = time.monotonic()
        read_reply = node.handle_client_command({"command": {"op": "get", "key": "x"}})
        elapsed = time.monotonic() - started
        self.assertTrue(read_reply["ok"])
        self.assertEqual(read_reply["result"], {"ok": True, "value": "42"})
        self.assertLess(elapsed, 0.5, "a 1-node cluster must answer a read synchronously")
        self.assertEqual(len(node.state.log), 1, "a `get` must NOT append a log entry")

    def test_get_of_missing_key_returns_none_not_an_error(self):
        node = _bare_node(node_id="solo", peer_ids=())
        with node.state.lock:
            node.state.role = Role.LEADER
            node.state.current_term = 1
            node.state.leader_id = node.node_id
        reply = node.handle_client_command({"command": {"op": "get", "key": "never-set"}})
        self.assertTrue(reply["ok"])
        self.assertEqual(reply["result"], {"ok": True, "value": None})


class TestReadsIntegration(unittest.TestCase):
    """Real 3-node cluster over real HTTP, real timers, real threads."""

    def setUp(self):
        base_port = 9700 + (os.getpid() % 300)
        self.cluster = ClusterConfig.local_cluster(["node1", "node2", "node3"], base_port=base_port)
        self.nodes = [RaftNode(nid, self.cluster) for nid in self.cluster.nodes]
        for n in self.nodes:
            n.start()
        self.client = RPCClient(timeout=6.0)

    def tearDown(self):
        for n in self.nodes:
            if not n._stopped:
                n.stop()

    def _wait_for_leader(self, nodes, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            snaps = [(n, n.state.snapshot()) for n in nodes]
            leaders = [(n, s) for n, s in snaps if s["role"] == Role.LEADER.value]
            if len(leaders) == 1:
                return leaders[0]
            time.sleep(0.05)
        return None

    def _wait_until(self, predicate, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(0.05)
        return predicate()

    def test_get_on_leader_returns_last_committed_value(self):
        found = self._wait_for_leader(self.nodes, ELECTION_TIMEOUT_MAX + 2.0)
        self.assertIsNotNone(found, "no leader emerged")
        _, leader_snap = found
        leader_addr = self.cluster.address_of(leader_snap["node_id"])

        write_reply = self.client.call(
            leader_addr.base_url, "/rpc/client_command", {"command": {"op": "set", "key": "x", "value": "hello"}}
        )
        self.assertTrue(write_reply["ok"], write_reply)

        read_reply = self.client.call(
            leader_addr.base_url, "/rpc/client_command", {"command": {"op": "get", "key": "x"}}
        )
        self.assertIsNotNone(read_reply)
        self.assertTrue(read_reply["ok"], read_reply)
        self.assertEqual(read_reply["result"], {"ok": True, "value": "hello"})

    def test_repeated_reads_never_grow_the_log(self):
        found = self._wait_for_leader(self.nodes, ELECTION_TIMEOUT_MAX + 2.0)
        self.assertIsNotNone(found)
        leader_node, leader_snap = found
        leader_addr = self.cluster.address_of(leader_snap["node_id"])

        write_reply = self.client.call(
            leader_addr.base_url, "/rpc/client_command", {"command": {"op": "set", "key": "x", "value": "1"}}
        )
        self.assertTrue(write_reply["ok"], write_reply)
        log_length_after_write = len(leader_node.state.log)

        for _ in range(10):
            read_reply = self.client.call(
                leader_addr.base_url, "/rpc/client_command", {"command": {"op": "get", "key": "x"}}
            )
            self.assertTrue(read_reply["ok"], read_reply)
            self.assertEqual(read_reply["result"], {"ok": True, "value": "1"})

        self.assertEqual(
            len(leader_node.state.log),
            log_length_after_write,
            "10 reads must not have appended a single log entry",
        )

    def test_follower_rejects_get_with_leader_hint(self):
        found = self._wait_for_leader(self.nodes, ELECTION_TIMEOUT_MAX + 2.0)
        self.assertIsNotNone(found)
        _, leader_snap = found
        follower = next(n for n in self.nodes if n.node_id != leader_snap["node_id"])
        follower_addr = self.cluster.address_of(follower.node_id)

        reply = self.client.call(
            follower_addr.base_url, "/rpc/client_command", {"command": {"op": "get", "key": "x"}}
        )
        self.assertIsNotNone(reply)
        self.assertFalse(reply["ok"])
        self.assertEqual(reply["leader_hint"], leader_snap["node_id"])

    def test_partitioned_leader_refuses_to_serve_a_stale_read(self):
        """The whole reason ReadIndex exists. Without it (a naive
        `return self.store.get(key)` straight off local state), this
        test would fail: the old leader would happily keep answering
        "x" = "1" forever, even after the rest of the cluster has moved
        on to "x" = "2" under a brand new leader."""
        found = self._wait_for_leader(self.nodes, ELECTION_TIMEOUT_MAX + 2.0)
        self.assertIsNotNone(found)
        old_leader, old_snap = found
        old_leader_addr = self.cluster.address_of(old_snap["node_id"])

        # Write "x"="1" while the cluster is fully connected, so the
        # old leader genuinely has real (soon-to-be-stale) data locally.
        write1 = self.client.call(
            old_leader_addr.base_url, "/rpc/client_command", {"command": {"op": "set", "key": "x", "value": "1"}}
        )
        self.assertTrue(write1["ok"], write1)
        self.assertEqual(old_leader.store.get("x"), "1")

        # Simulate a real network partition: the old leader can neither
        # receive RPCs (its HTTP server is stopped, exactly like
        # test_replication.py's leader-kill tests) nor send any (every
        # outgoing call is monkeypatched to behave like a timeout) —
        # a real partition cuts both directions, not just one.
        old_leader.stop()
        old_leader.rpc_client.call = lambda *a, **k: None

        survivors = [n for n in self.nodes if n is not old_leader]
        found2 = self._wait_for_leader(survivors, ELECTION_TIMEOUT_MAX + 3.0)
        self.assertIsNotNone(found2, "no new leader emerged on the majority side")
        new_leader, new_snap = found2
        self.assertNotEqual(new_snap["node_id"], old_snap["node_id"])

        # The new leader accepts a fresh write the old leader will
        # never see.
        new_leader_addr = self.cluster.address_of(new_snap["node_id"])
        write2 = self.client.call(
            new_leader_addr.base_url, "/rpc/client_command", {"command": {"op": "set", "key": "x", "value": "2"}}
        )
        self.assertTrue(write2["ok"], write2)

        # Now ask the OLD leader for a read — directly, in-process
        # (bypassing its now-dead HTTP server), the way you'd ask if
        # you were, say, a client that cached its address before the
        # partition happened. Its own state still says role=LEADER;
        # nothing has ever told it otherwise. A correct ReadIndex
        # implementation must refuse rather than answer from its
        # local (now-stale) store.
        with mock.patch("miniraft.node.CLIENT_COMMAND_TIMEOUT", 0.4):
            stale_read = old_leader.handle_client_command({"command": {"op": "get", "key": "x"}})

        self.assertFalse(
            stale_read["ok"],
            f"a partitioned leader must never answer a read, got: {stale_read}",
        )
        # Belt and suspenders: whatever it replied, it must not be the
        # stale value — proving this isn't just an incidentally-worded
        # error that happens to also leak the wrong data.
        self.assertNotEqual(stale_read.get("result"), {"ok": True, "value": "1"})
        self.assertEqual(
            old_leader.store.get("x"), "1", "the isolated node's own store is genuinely stale — that's the point"
        )


if __name__ == "__main__":
    unittest.main()
