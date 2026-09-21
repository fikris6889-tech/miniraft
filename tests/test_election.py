"""Day 2 behavioral tests: leader election.

Split into two groups on purpose:

  * Unit tests call handle_request_vote / handle_append_entries directly
    on a RaftNode (no HTTP, no timers, no threads) so the exact rules
    from Raft paper Figure 2 are pinned down deterministically — these
    replace Day 1's test_request_vote_stub_never_grants_a_vote and
    test_append_entries_stub_always_fails, which asserted the OPPOSITE
    of what should happen now that the algorithm is real.

  * Integration tests boot a real 3-node cluster over real HTTP (same
    pattern as test_boilerplate.py) and prove the emergent property
    Day 1's post promised: "a 3-node cluster picks a leader" and "the
    leader stays stable" (no heartbeats would mean constant re-election
    — see this project's Day 2 blog post for why that stability matters
    enough to be part of Day 2, not deferred whole-cloth to Day 3).
    These replace test_no_leader_emerges_without_election_logic.
"""
import itertools
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from miniraft.config import ClusterConfig
from miniraft.node import RaftNode, ELECTION_TIMEOUT_MAX
from miniraft.state import Role
from miniraft.transport import RPCClient

# RaftNode.__init__ eagerly binds a real socket for its RPCServer (even
# though .start() is what actually begins serving) — so every "bare"
# node used for direct-call unit tests below still needs its OWN unique
# port, or the second construction fails with "Address already in use".
_next_base_port = itertools.count(9700 + (os.getpid() % 100) * 100, 10)


def _bare_node(node_id="node1", peer_ids=("node2", "node3")):
    """A RaftNode with its state constructed but no timers armed and no
    RPCs ever sent — for pure unit tests of the RPC handler methods,
    called directly rather than over HTTP."""
    ids = [node_id] + list(peer_ids)
    cluster = ClusterConfig.local_cluster(ids, base_port=next(_next_base_port))
    return RaftNode(node_id, cluster)


class TestRequestVoteRules(unittest.TestCase):
    """Direct, timer-free tests of handle_request_vote's Figure 2 rules."""

    def test_grants_vote_to_candidate_with_fresh_state(self):
        node = _bare_node()
        reply = node.handle_request_vote(
            {"term": 1, "candidate_id": "node2", "last_log_index": 0, "last_log_term": 0}
        )
        self.assertTrue(reply["vote_granted"])
        self.assertEqual(reply["term"], 1)
        self.assertEqual(node.state.voted_for, "node2")
        self.assertEqual(node.state.current_term, 1)

    def test_rejects_vote_when_candidate_term_is_stale(self):
        node = _bare_node()
        with node.state.lock:
            node.state.current_term = 5
        reply = node.handle_request_vote(
            {"term": 3, "candidate_id": "node2", "last_log_index": 0, "last_log_term": 0}
        )
        self.assertFalse(reply["vote_granted"])
        self.assertEqual(reply["term"], 5, "reply must carry OUR term so the stale candidate learns it lost")

    def test_does_not_grant_a_second_vote_to_a_different_candidate_same_term(self):
        node = _bare_node()
        first = node.handle_request_vote(
            {"term": 1, "candidate_id": "node2", "last_log_index": 0, "last_log_term": 0}
        )
        second = node.handle_request_vote(
            {"term": 1, "candidate_id": "node3", "last_log_index": 0, "last_log_term": 0}
        )
        self.assertTrue(first["vote_granted"])
        self.assertFalse(second["vote_granted"], "a node must not double-vote within the same term")
        self.assertEqual(node.state.voted_for, "node2")

    def test_regranting_the_same_candidate_same_term_is_fine(self):
        # A duplicate/retried RequestVote from the SAME candidate in the
        # SAME term (e.g. our first reply got lost on the wire and the
        # candidate retried) must not be refused just because voted_for
        # is already set to them.
        node = _bare_node()
        node.handle_request_vote({"term": 1, "candidate_id": "node2", "last_log_index": 0, "last_log_term": 0})
        again = node.handle_request_vote(
            {"term": 1, "candidate_id": "node2", "last_log_index": 0, "last_log_term": 0}
        )
        self.assertTrue(again["vote_granted"])

    def test_higher_term_request_vote_steps_down_a_leader_before_evaluating(self):
        node = _bare_node()
        with node.state.lock:
            node.state.role = Role.LEADER
            node.state.current_term = 2
            node.state.voted_for = node.node_id
        reply = node.handle_request_vote(
            {"term": 5, "candidate_id": "node2", "last_log_index": 0, "last_log_term": 0}
        )
        self.assertTrue(reply["vote_granted"], "stepping down must happen BEFORE the vote is evaluated")
        self.assertEqual(node.state.role, Role.FOLLOWER)
        self.assertEqual(node.state.current_term, 5)

    def test_log_up_to_date_check_is_term_first_then_index(self):
        # Give our node a longer log but at an OLDER term than the
        # candidate's last entry. Rule: term wins first — a shorter log
        # with a newer last-log-term beats a longer log with an older one.
        node = _bare_node()
        node.state.log.append(term=1, command={"op": "set", "key": "a", "value": "1"})
        node.state.log.append(term=1, command={"op": "set", "key": "b", "value": "2"})
        # our last_log_term=1, last_log_index=2 (2 entries, both term 1)
        reply = node.handle_request_vote(
            {"term": 5, "candidate_id": "node2", "last_log_index": 1, "last_log_term": 2}
        )
        self.assertTrue(
            reply["vote_granted"],
            "candidate's last_log_term (2) > ours (1) must win even though their log is shorter",
        )

    def test_log_up_to_date_check_rejects_shorter_log_same_term(self):
        node = _bare_node()
        node.state.log.append(term=1, command={"op": "set", "key": "a", "value": "1"})
        node.state.log.append(term=1, command={"op": "set", "key": "b", "value": "2"})
        # same last_log_term (1) as ours, but candidate's log is shorter (index 1 < our 2)
        reply = node.handle_request_vote(
            {"term": 5, "candidate_id": "node2", "last_log_index": 1, "last_log_term": 1}
        )
        self.assertFalse(reply["vote_granted"], "same term but a shorter log is NOT at least as up-to-date")


class TestAppendEntriesRules(unittest.TestCase):
    """Direct, timer-free tests of handle_append_entries' Day-2 half
    (leader recognition / step-down); the log-consistency-check half is
    Day 3, so every args here uses the always-consistent (0, 0, [])
    shape that's all any Day-2 leader ever sends."""

    def _heartbeat(self, term, leader_id="node2"):
        return {
            "term": term,
            "leader_id": leader_id,
            "prev_log_index": 0,
            "prev_log_term": 0,
            "entries": [],
            "leader_commit": 0,
        }

    def test_rejects_stale_term_heartbeat(self):
        node = _bare_node()
        with node.state.lock:
            node.state.current_term = 5
        reply = node.handle_append_entries(self._heartbeat(term=3))
        self.assertFalse(reply["success"])
        self.assertEqual(reply["term"], 5)

    def test_accepts_heartbeat_at_current_term_and_stays_follower(self):
        node = _bare_node()
        reply = node.handle_append_entries(self._heartbeat(term=1))
        self.assertTrue(reply["success"])
        self.assertEqual(node.state.role, Role.FOLLOWER)
        self.assertEqual(node.state.current_term, 1)

    def test_candidate_steps_down_on_a_legitimate_leaders_heartbeat_same_term(self):
        # Raft rule: "if AppendEntries received from new leader: convert
        # to follower" applies even at an EQUAL term (some other node won
        # the election for the term we were also contesting).
        node = _bare_node()
        with node.state.lock:
            node.state.role = Role.CANDIDATE
            node.state.current_term = 4
            node.state.voted_for = node.node_id
        reply = node.handle_append_entries(self._heartbeat(term=4))
        self.assertTrue(reply["success"])
        self.assertEqual(node.state.role, Role.FOLLOWER)

    def test_leader_steps_down_and_cancels_heartbeat_timer_on_higher_term(self):
        node = _bare_node()
        with node.state.lock:
            node.state.role = Role.LEADER
            node.state.current_term = 2
        # give it a real (harmless, far-future) heartbeat timer to prove it gets cancelled
        import threading

        fake_timer = threading.Timer(999, lambda: None)
        node._heartbeat_timer = fake_timer
        reply = node.handle_append_entries(self._heartbeat(term=9))
        self.assertTrue(reply["success"])
        self.assertEqual(node.state.role, Role.FOLLOWER)
        self.assertEqual(node.state.current_term, 9)
        self.assertIsNone(node._heartbeat_timer)
        self.assertFalse(fake_timer.is_alive())


class TestElectionIntegration(unittest.TestCase):
    """Real 3-node cluster over real HTTP, real timers, real threads —
    the emergent behavior Day 1's post promised for Day 2."""

    def setUp(self):
        base_port = 9800 + (os.getpid() % 300)
        self.cluster = ClusterConfig.local_cluster(["node1", "node2", "node3"], base_port=base_port)
        self.nodes = [RaftNode(nid, self.cluster) for nid in self.cluster.nodes]
        for n in self.nodes:
            n.start()

    def tearDown(self):
        for n in self.nodes:
            n.stop()

    def _wait_for_leader(self, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            snaps = [n.state.snapshot() for n in self.nodes]
            leaders = [s for s in snaps if s["role"] == Role.LEADER.value]
            if len(leaders) == 1:
                return leaders[0], snaps
            time.sleep(0.05)
        return None, [n.state.snapshot() for n in self.nodes]

    def test_exactly_one_leader_emerges_within_the_election_timeout_window(self):
        # Give it the max possible single-round timeout plus a healthy
        # margin for HTTP round-trips on a loaded test machine.
        leader, snaps = self._wait_for_leader(timeout=ELECTION_TIMEOUT_MAX + 2.0)
        self.assertIsNotNone(leader, f"no leader emerged; final snapshots: {snaps}")
        followers = [s for s in snaps if s["role"] != Role.LEADER.value]
        self.assertEqual(len(followers), 2)
        for f in followers:
            self.assertEqual(f["role"], Role.FOLLOWER.value)
            # every node should have converged on the same term as the leader
            self.assertEqual(f["current_term"], leader["current_term"])
        self.assertGreaterEqual(leader["current_term"], 1)

    def test_leader_stays_stable_across_several_heartbeat_intervals(self):
        # This is the test that specifically proves out the Day-2 design
        # choice to implement minimal heartbeats now instead of leaving
        # send_heartbeats a total no-op until Day 3: without heartbeats,
        # every follower's election timer would eventually fire and a
        # NEW election would start every few seconds even with a
        # perfectly good leader already in place.
        leader, _ = self._wait_for_leader(timeout=ELECTION_TIMEOUT_MAX + 2.0)
        self.assertIsNotNone(leader)
        leader_id = leader["node_id"]
        leader_term = leader["current_term"]

        # Sit through several heartbeat intervals — comfortably past a
        # SINGLE election timeout, which is the failure mode we're
        # guarding against — and confirm nothing changed.
        time.sleep(ELECTION_TIMEOUT_MAX + 1.0)

        snaps = {n.node_id: n.state.snapshot() for n in self.nodes}
        self.assertEqual(snaps[leader_id]["role"], Role.LEADER.value)
        self.assertEqual(snaps[leader_id]["current_term"], leader_term, "leader must not have been re-elected")
        for nid, snap in snaps.items():
            if nid == leader_id:
                continue
            self.assertEqual(snap["role"], Role.FOLLOWER.value)
            self.assertEqual(snap["current_term"], leader_term)


if __name__ == "__main__":
    unittest.main()
