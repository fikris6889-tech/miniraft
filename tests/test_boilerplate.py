"""Wiring/lifecycle smoke tests: a real RaftNode (not a fake) using the
actual config/node/transport wiring, confirming the cluster boots and
answers RPCs safely.

Day 1's version of this file also asserted three THINGS-THAT-WERE-TRUE-
ONLY-BECAUSE-THE-ALGORITHM-WAS-STUBBED (moved to test_election.py on
Day 2). Day 2's version still had one: test_client_command_not_implemented_yet,
true only because handle_client_command was a Day-3 TODO. Day 3 makes
that one false on purpose too (that's the whole point of implementing
replication) — its real behavior now has its own dedicated tests in
tests/test_replication.py. What's left here is the wiring contract that
stays true across every day of this project: nodes boot as followers at
term 0, and every node answers real HTTP."""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from miniraft.config import ClusterConfig
from miniraft.node import RaftNode
from miniraft.state import Role
from miniraft.transport import RPCClient


class TestNodeBoilerplate(unittest.TestCase):
    def setUp(self):
        base_port = 9600 + (os.getpid() % 300)
        self.cluster = ClusterConfig.local_cluster(["node1", "node2", "node3"], base_port=base_port)
        self.nodes = [RaftNode(nid, self.cluster) for nid in self.cluster.nodes]
        for n in self.nodes:
            n.start()
        self.client = RPCClient(timeout=2.0)

    def tearDown(self):
        for n in self.nodes:
            n.stop()

    def test_every_node_starts_as_follower_term_zero(self):
        # Checked immediately after start(): the election timer's
        # minimum delay (ELECTION_TIMEOUT_MIN=1.5s) is comfortably
        # longer than the time it takes this assertion to run, so this
        # still pins down the boot state even though a real timer gets
        # armed inside start() now.
        for n in self.nodes:
            snap = n.state.snapshot()
            self.assertEqual(snap["role"], Role.FOLLOWER.value)
            self.assertEqual(snap["current_term"], 0)

    def test_all_nodes_reachable_over_http(self):
        for nid, addr in self.cluster.nodes.items():
            reply = self.client.health(addr.base_url)
            self.assertIsNotNone(reply, f"{nid} did not respond to /health")
            self.assertEqual(reply["node_id"], nid)


if __name__ == "__main__":
    unittest.main()
