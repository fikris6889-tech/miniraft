"""Day 3 behavioral tests: log replication.

Same split as Day 2's test_election.py, for the same reason — pin the
rules down with fast, deterministic direct-call unit tests, THEN prove
the emergent, whole-cluster behavior with real multi-node HTTP
integration tests:

  * TestAppendEntriesReplicationRules — direct calls to
    handle_append_entries exercising the real §5.3 consistency check,
    conflict_index back-off, and commit-index advancement that Day 2
    deliberately left as a TODO (every Day-2 args always used the
    trivially-true (0, 0, []) shape).

  * TestClientCommandLeaderRules / TestCommitIndexAdvancement — direct
    calls proving handle_client_command's leader-only write path and
    the §5.4.2 "only commit your OWN term's entries directly" subtlety
    that's the single easiest correctness rule to skip in a from-scratch
    Raft build (it looks unnecessary until a leader change happens mid
    replication).

  * TestReplicationIntegration — a real 3-node cluster over real HTTP:
    a client_command sent to the leader converges on every node's store,
    a follower redirects with leader_hint instead of silently failing,
    multiple commands apply in order everywhere, and — the big one —
    the cluster keeps accepting writes and keeps every committed value
    after the leader is killed mid-series.
"""
import itertools
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from miniraft.config import ClusterConfig
from miniraft.log import LogEntry
from miniraft.node import RaftNode, ELECTION_TIMEOUT_MAX
from miniraft.state import Role
from miniraft.transport import RPCClient

_next_base_port = itertools.count(9900 + (os.getpid() % 100) * 100, 10)


def _bare_node(node_id="node1", peer_ids=("node2", "node3")):
    """Same helper as test_election.py: a RaftNode with state wired up
    but no timers armed and no RPCs ever sent, for direct-call tests of
    the handler methods."""
    ids = [node_id] + list(peer_ids)
    cluster = ClusterConfig.local_cluster(ids, base_port=next(_next_base_port))
    return RaftNode(node_id, cluster)


def _append_args(term, prev_log_index, prev_log_term, entries=None, leader_commit=0, leader_id="node2"):
    return {
        "term": term,
        "leader_id": leader_id,
        "prev_log_index": prev_log_index,
        "prev_log_term": prev_log_term,
        "entries": entries or [],
        "leader_commit": leader_commit,
    }


class TestAppendEntriesReplicationRules(unittest.TestCase):
    """Direct, timer-free tests of handle_append_entries' Day-3 half:
    the real consistency check, conflict handling, and commit advance."""

    def test_accepts_entries_onto_an_empty_log(self):
        node = _bare_node()
        entry = LogEntry(term=1, index=1, command={"op": "set", "key": "a", "value": "1"}).to_dict()
        reply = node.handle_append_entries(_append_args(term=1, prev_log_index=0, prev_log_term=0, entries=[entry]))
        self.assertTrue(reply["success"])
        self.assertEqual(node.state.log.last_index(), 1)
        self.assertEqual(node.state.log.get(1).command["key"], "a")

    def test_rejects_when_prev_log_index_is_beyond_our_log_and_reports_conflict_index(self):
        node = _bare_node()
        # our log is empty (last_index=0); leader thinks we already have
        # 5 entries and tries to append starting from index 6.
        reply = node.handle_append_entries(_append_args(term=1, prev_log_index=5, prev_log_term=1))
        self.assertFalse(reply["success"])
        self.assertEqual(reply["conflict_index"], 1, "should point the leader at right after our last real entry")
        self.assertEqual(node.state.commit_index, 0, "a failed consistency check must never touch commit_index")

    def test_rejects_on_term_mismatch_and_walks_conflict_index_back_to_start_of_conflicting_term(self):
        node = _bare_node()
        # Build a log where indices 2, 3, 4 all belong to term 2, so a
        # leader whose OWN entry at index 4 is a different term should
        # be told to back off all the way to index 2, not just index 4.
        node.state.log.append(term=1, command={"op": "set", "key": "a", "value": "1"})
        node.state.log.append(term=2, command={"op": "set", "key": "b", "value": "2"})
        node.state.log.append(term=2, command={"op": "set", "key": "c", "value": "3"})
        node.state.log.append(term=2, command={"op": "set", "key": "d", "value": "4"})
        reply = node.handle_append_entries(_append_args(term=5, prev_log_index=4, prev_log_term=1))
        self.assertFalse(reply["success"])
        self.assertEqual(
            reply["conflict_index"], 2, "should skip the whole conflicting term 2 stretch in one round"
        )

    def test_conflicting_tail_is_truncated_and_replaced(self):
        node = _bare_node()
        node.state.log.append(term=1, command={"op": "set", "key": "a", "value": "1"})
        node.state.log.append(term=1, command={"op": "set", "key": "stale", "value": "old"})
        new_entry = LogEntry(term=2, index=2, command={"op": "set", "key": "b", "value": "new"}).to_dict()
        reply = node.handle_append_entries(
            _append_args(term=2, prev_log_index=1, prev_log_term=1, entries=[new_entry])
        )
        self.assertTrue(reply["success"])
        self.assertEqual(node.state.log.last_index(), 2)
        self.assertEqual(node.state.log.get(2).term, 2)
        self.assertEqual(node.state.log.get(2).command["key"], "b")

    def test_commit_index_advances_and_applies_to_the_store(self):
        node = _bare_node()
        entry = LogEntry(term=1, index=1, command={"op": "set", "key": "x", "value": "1"}).to_dict()
        reply = node.handle_append_entries(
            _append_args(term=1, prev_log_index=0, prev_log_term=0, entries=[entry], leader_commit=1)
        )
        self.assertTrue(reply["success"])
        self.assertEqual(node.state.commit_index, 1)
        self.assertEqual(node.state.last_applied, 1)
        self.assertEqual(node.store.get("x"), "1")

    def test_commit_index_is_capped_at_our_own_last_new_entry_not_the_leaders_claim(self):
        # Leader claims leader_commit=99, but we only actually received
        # one entry (index 1) this round — commit_index must not race
        # ahead of what our own log really has.
        node = _bare_node()
        entry = LogEntry(term=1, index=1, command={"op": "set", "key": "x", "value": "1"}).to_dict()
        node.handle_append_entries(
            _append_args(term=1, prev_log_index=0, prev_log_term=0, entries=[entry], leader_commit=99)
        )
        self.assertEqual(node.state.commit_index, 1)

    def test_a_failed_consistency_check_never_advances_commit_index_even_with_a_high_leader_commit(self):
        node = _bare_node()
        reply = node.handle_append_entries(_append_args(term=1, prev_log_index=5, prev_log_term=1, leader_commit=10))
        self.assertFalse(reply["success"])
        self.assertEqual(node.state.commit_index, 0)
        self.assertEqual(node.store.snapshot(), {})


class TestClientCommandLeaderRules(unittest.TestCase):
    """Direct calls to handle_client_command."""

    def test_non_leader_replies_not_ok_with_leader_hint(self):
        node = _bare_node()
        with node.state.lock:
            node.state.role = Role.FOLLOWER
            node.state.leader_id = "node2"
        reply = node.handle_client_command({"command": {"op": "set", "key": "x", "value": "1"}})
        self.assertFalse(reply["ok"])
        self.assertEqual(reply["leader_hint"], "node2")

    def test_single_node_cluster_commits_and_applies_immediately(self):
        # No peers at all: the leader's own log is already a majority
        # of one, so this must not block on any network round-trip.
        node = _bare_node(node_id="solo", peer_ids=())
        with node.state.lock:
            node.state.role = Role.LEADER
            node.state.current_term = 1
            node.state.leader_id = node.node_id
        started = time.monotonic()
        reply = node.handle_client_command({"command": {"op": "set", "key": "x", "value": "42"}})
        elapsed = time.monotonic() - started
        self.assertTrue(reply["ok"])
        self.assertEqual(reply["result"], {"ok": True})
        self.assertEqual(node.store.get("x"), "42")
        self.assertLess(elapsed, 0.5, "a 1-node cluster must commit synchronously, not wait out a timeout")


class TestCommitIndexAdvancement(unittest.TestCase):
    """Direct tests of _advance_commit_index_locked, including the
    §5.4.2 subtlety: a leader may not commit an entry from an OLDER
    term just because a majority now stores it — not until an entry
    from its OWN current term also reaches a majority alongside it."""

    def _leader_with_match_index(self, term, entries_terms, match_index):
        node = _bare_node(node_id="node1", peer_ids=("node2", "node3"))
        with node.state.lock:
            node.state.role = Role.LEADER
            node.state.current_term = term
            for t in entries_terms:
                node.state.log.append(term=t, command={"op": "set", "key": "k", "value": "v"})
            node.state.match_index.update(match_index)
        return node

    def test_majority_match_at_current_term_commits(self):
        # 3-node cluster, leader's own log already has 1 entry at the
        # current term (index 1). One peer has also replicated it.
        node = self._leader_with_match_index(term=1, entries_terms=[1], match_index={"node2": 1, "node3": 0})
        with node.state.lock:
            node._advance_commit_index_locked()
            self.assertEqual(node.state.commit_index, 1)

    def test_minority_match_does_not_commit(self):
        node = self._leader_with_match_index(term=1, entries_terms=[1], match_index={"node2": 0, "node3": 0})
        with node.state.lock:
            node._advance_commit_index_locked()
            self.assertEqual(node.state.commit_index, 0, "only the leader itself (1 of 3) has this entry")

    def test_old_term_entry_with_majority_does_not_commit_until_a_current_term_entry_joins_it(self):
        # index 1 is from term 1 (an earlier leader's entry). Both peers
        # already have it too — a full 3-of-3 majority by count — but
        # this leader is now on term 2 and hasn't replicated anything
        # of ITS OWN yet. §5.4.2: it must NOT be considered committed.
        node = self._leader_with_match_index(term=2, entries_terms=[1], match_index={"node2": 1, "node3": 1})
        with node.state.lock:
            node._advance_commit_index_locked()
            self.assertEqual(
                node.state.commit_index,
                0,
                "an old-term entry must not commit on match count alone, even with a unanimous majority",
            )

        # Leader now appends its OWN term-2 entry (index 2), but no peer
        # has replicated it yet — still must not commit index 2 (only
        # the leader itself has it), and index 1 still can't commit on
        # its own for the same §5.4.2 reason as above.
        with node.state.lock:
            node.state.log.append(term=2, command={"op": "set", "key": "k2", "value": "v2"})
            node._advance_commit_index_locked()
            self.assertEqual(node.state.commit_index, 0)

        # NOW a peer catches up on the term-2 entry too (match_index=2):
        # a majority (leader + that peer) has the CURRENT-term entry, so
        # it commits — and drags index 1 along with it, since committing
        # a later entry from the leader's log implies everything before
        # it is safe too.
        with node.state.lock:
            node.state.match_index["node2"] = 2
            node._advance_commit_index_locked()
            self.assertEqual(node.state.commit_index, 2)
        self.assertEqual(node.store.get("k"), "v")
        self.assertEqual(node.store.get("k2"), "v2")


class TestReplicationIntegration(unittest.TestCase):
    """Real 3-node cluster over real HTTP, real timers, real threads —
    the emergent behavior a from-scratch Raft build has to deliver:
    writes committed on the leader show up on every node, a follower
    redirects instead of silently eating a write, and the cluster keeps
    working (and keeps what it already committed) after a leader dies."""

    def setUp(self):
        base_port = 9950 + (os.getpid() % 300)
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

    def test_client_command_on_leader_converges_on_every_node(self):
        found = self._wait_for_leader(self.nodes, ELECTION_TIMEOUT_MAX + 2.0)
        self.assertIsNotNone(found, "no leader emerged")
        leader_node, leader_snap = found
        leader_addr = self.cluster.address_of(leader_snap["node_id"])

        reply = self.client.call(
            leader_addr.base_url, "/rpc/client_command", {"command": {"op": "set", "key": "x", "value": "hello"}}
        )
        self.assertIsNotNone(reply)
        self.assertTrue(reply["ok"], reply)
        self.assertEqual(reply["result"], {"ok": True})

        converged = self._wait_until(lambda: all(n.store.get("x") == "hello" for n in self.nodes), timeout=3.0)
        self.assertTrue(converged, [n.store.snapshot() for n in self.nodes])

    def test_follower_rejects_client_command_with_leader_hint(self):
        found = self._wait_for_leader(self.nodes, ELECTION_TIMEOUT_MAX + 2.0)
        self.assertIsNotNone(found)
        leader_node, leader_snap = found
        follower = next(n for n in self.nodes if n.node_id != leader_snap["node_id"])
        follower_addr = self.cluster.address_of(follower.node_id)

        reply = self.client.call(
            follower_addr.base_url, "/rpc/client_command", {"command": {"op": "set", "key": "x", "value": "1"}}
        )
        self.assertIsNotNone(reply)
        self.assertFalse(reply["ok"])
        self.assertEqual(reply["leader_hint"], leader_snap["node_id"])

    def test_multiple_commands_apply_in_order_on_every_node(self):
        found = self._wait_for_leader(self.nodes, ELECTION_TIMEOUT_MAX + 2.0)
        self.assertIsNotNone(found)
        _, leader_snap = found
        leader_addr = self.cluster.address_of(leader_snap["node_id"])

        commands = [
            {"op": "set", "key": "x", "value": "1"},
            {"op": "set", "key": "x", "value": "2"},
            {"op": "set", "key": "y", "value": "5"},
            {"op": "delete", "key": "x"},
        ]
        for command in commands:
            reply = self.client.call(leader_addr.base_url, "/rpc/client_command", {"command": command})
            self.assertIsNotNone(reply)
            self.assertTrue(reply["ok"], reply)

        converged = self._wait_until(
            lambda: all(n.store.snapshot() == {"y": "5"} for n in self.nodes), timeout=3.0
        )
        self.assertTrue(converged, [n.store.snapshot() for n in self.nodes])
        # every node's log should also have converged, not just the store
        log_lengths = {len(n.state.log) for n in self.nodes}
        self.assertEqual(log_lengths, {len(commands)})

    def test_cluster_survives_leader_death_and_keeps_accepting_writes(self):
        found = self._wait_for_leader(self.nodes, ELECTION_TIMEOUT_MAX + 2.0)
        self.assertIsNotNone(found)
        first_leader, first_snap = found
        first_leader_addr = self.cluster.address_of(first_snap["node_id"])

        reply = self.client.call(
            first_leader_addr.base_url, "/rpc/client_command", {"command": {"op": "set", "key": "before", "value": "1"}}
        )
        self.assertTrue(reply["ok"], reply)
        self.assertTrue(
            self._wait_until(lambda: all(n.store.get("before") == "1" for n in self.nodes), timeout=3.0)
        )

        first_leader.stop()
        survivors = [n for n in self.nodes if n is not first_leader]

        found2 = self._wait_for_leader(survivors, ELECTION_TIMEOUT_MAX + 3.0)
        self.assertIsNotNone(found2, "no new leader emerged among the survivors")
        new_leader, new_snap = found2
        self.assertNotEqual(new_snap["node_id"], first_snap["node_id"])
        new_leader_addr = self.cluster.address_of(new_snap["node_id"])

        reply2 = self.client.call(
            new_leader_addr.base_url, "/rpc/client_command", {"command": {"op": "set", "key": "after", "value": "2"}}
        )
        self.assertIsNotNone(reply2)
        self.assertTrue(reply2["ok"], reply2)
        # The leader itself has already applied "after" synchronously (that's
        # what made reply2["ok"] true), but a follower only learns the
        # leader's new commit_index on the NEXT AppendEntries round after
        # this one — the round that carried the entry itself necessarily
        # snapshotted leader_commit BEFORE this entry's own commit happened.
        # So a follower can lag up to one HEARTBEAT_INTERVAL behind the
        # leader's own store — don't assert on it synchronously.
        self.assertEqual(new_leader.store.get("after"), "2", "the leader's own apply must be synchronous")

        converged = self._wait_until(
            lambda: all(n.store.get("after") == "2" for n in survivors), timeout=2.0
        )
        self.assertTrue(converged, [n.store.snapshot() for n in survivors])
        for n in survivors:
            self.assertEqual(n.store.get("before"), "1", "data committed before the crash must survive it")


if __name__ == "__main__":
    unittest.main()
