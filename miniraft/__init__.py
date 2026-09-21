"""MiniRaft — a teaching implementation of the Raft consensus algorithm.

Day 1 shipped scaffolding only: networking, data structures, and the
state machine, fully implemented and tested, with the actual algorithm
left as TODO stubs. Day 2 implemented real LEADER ELECTION: randomized
election timeouts, the RequestVote rules from the Raft paper, and the
minimal heartbeat plumbing that keeps an elected leader from being
immediately un-elected by its own followers.

Day 3 implemented LOG REPLICATION: the real AppendEntries consistency
check (§5.3) with conflict_index back-off, commit-index advancement
with the §5.4.2 "only your own term commits directly" rule, and the
client-facing write path (handle_client_command) that blocks until a
command has genuinely reached a majority. A 3-node cluster accepted
real writes on its leader, converged every node's key-value store, and
kept everything it already committed (plus kept accepting new writes)
if you killed the leader mid-series.

Day 4 (today, FINAL) is the ReadIndex read path (§8, "Client
Interaction"): a `get` no longer has to be routed through the log the
way Day 3's write path does — it's answered from a leader's local
state once a lightweight quorum probe proves the leader hasn't been
silently partitioned away from the majority. Same safety Day 3 had
(no partitioned, stale leader can ever answer a read), without the
log-bloat of appending an entry for every read. MiniRaft's core
consensus algorithm — election, replication, and now safe reads — is
complete.

Cluster membership changes, snapshotting/log compaction, and
disk-persisted state remain explicitly out of scope for this teaching
build — real, important extensions, but each one is its own multi-day
project on top of what's here, not "final polish."
"""

__version__ = "1.0.0"
