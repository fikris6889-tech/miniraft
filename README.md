# MiniRaft — a teaching build of the Raft consensus algorithm

Expert-tier project for the Daily Coding Teaching Series. **Day 4 of 4 — COMPLETE.**

MiniRaft is a distributed key-value store where every node's copy of the data stays
consistent even when nodes crash, restart, or the network drops packets — using the
[Raft consensus algorithm](https://raft.github.io/) (Ongaro & Ousterhout, 2014).

## Status: Day 4 — COMPLETE

- Fully implemented & tested (Day 1): cluster config, the replicated log data
  structure (`log.py`), the key-value state machine (`store.py`), and the HTTP RPC
  transport (`transport.py`) — stdlib only, zero pip installs required.
- Fully implemented & tested (Day 2): randomized election timeouts, the real
  `RequestVote` rules (§5.2/§5.4), `handle_append_entries`' leader-recognition half,
  and the minimal heartbeat loop that keeps an elected leader from being immediately
  re-elected out of office by its own followers' timers.
- Fully implemented & tested (Day 3): the real §5.3 `AppendEntries` consistency
  check (does a follower's log actually agree with the leader up to
  `prev_log_index`?) with the `conflict_index` back-off optimization, conflict
  truncation, commit-index advancement respecting the §5.4.2 "only commit your OWN
  term's entries directly" rule, and `handle_client_command` — the leader-only write
  path that appends to the log, replicates to a majority, and only replies once the
  command is genuinely durable.
- Fully implemented & tested (Day 4 — new today): the **ReadIndex** technique
  (§8, "Client Interaction") — a `get` no longer has to be routed through the log
  the way a write is. A leader answers a read from its own local state once a
  lightweight quorum probe (`_confirm_leadership_with_majority`) proves, via a real
  round-trip to a majority of peers, that it hasn't been silently partitioned away
  from the cluster. Same safety guarantee Day 3's log-routed reads had — a
  partitioned, stale "leader" can never answer a read — without permanently bloating
  the log with an entry for every single read.
- A cluster accepts real writes on its leader (`POST /rpc/client_command` with
  `op: "set"`/`"delete"`) and real reads (`op: "get"`) on the same endpoint, every
  node's key-value store converges, a follower redirects either kind of request with
  a `leader_hint`, killing the leader mid-series loses nothing already committed and
  the survivors keep taking new writes AND reads under a new leader, and — Day 4's
  headline proof — a leader that gets cut off from the rest of the cluster refuses
  to answer a read instead of silently serving stale data forever. See the Day 4
  blog post for real captured multi-process output proving all of this, including a
  deliberately-broken "naive read" run that shows the exact bug this feature exists
  to prevent.
- **Explicitly out of scope** for this teaching build (not silently ignored — each
  one is its own multi-day project, not an extension of "final polish"): cluster
  membership changes (adding/removing nodes safely), snapshotting/log compaction,
  and disk-persisted state (a real deployment would `fsync` `current_term`,
  `voted_for`, and the log before ever replying to an RPC — MiniRaft keeps all of
  this in memory and says so plainly rather than pretending otherwise).

## Requirements

Python 3.8+. No third-party packages.

## Run it

```bash
# boot a local 3-node cluster (subprocesses on 127.0.0.1:9000-9002)
python3 cluster_run.py --nodes 3

# in another terminal — watch a leader get elected within a couple of seconds
curl http://127.0.0.1:9000/health

# write a command through the leader (swap 9000 for whichever port /health says
# role: leader) — it blocks until a majority has it, then replies
curl -X POST -d '{"command":{"op":"set","key":"x","value":"1"}}' \
     http://127.0.0.1:9000/rpc/client_command

# read it back — same endpoint, just a "get" op. Watch /health's log_length:
# repeated reads must NOT grow it, unlike the write above.
curl -X POST -d '{"command":{"op":"get","key":"x"}}' \
     http://127.0.0.1:9000/rpc/client_command

# confirm the write converged everywhere
curl http://127.0.0.1:9000/health   # commit_index / log_length should now be 1
curl http://127.0.0.1:9001/health
curl http://127.0.0.1:9002/health

# kill whichever node's /health says "leader" and re-run a write/read against
# whichever survivor becomes the new leader — the cluster keeps taking both
# and keeps everything it already committed.
```

There's also a scripted smoke test (`smoke_test_day4.py`) that does the boot →
write → repeated-read → kill-leader → write-again sequence above as real OS
subprocesses and asserts on the real captured output — the same output shown in
the Day 4 blog post.

## Test it

```bash
python3 -m unittest discover -s tests -v
```

60 tests, all green — the Day-1 data structures & transport, the Day-1/2 wiring
contract, `tests/test_election.py` (Day 2 leader election), `tests/test_replication.py`
(Day 3 log replication), and (new today) `tests/test_reads.py`: direct unit tests of
`_confirm_leadership_with_majority`'s quorum math (single-node fast path, real
peers acking, unreachable peers correctly failing) and the `get`-vs-`set` branch in
`handle_client_command` — plus real end-to-end integration tests over an actual
3-node HTTP cluster: a `get` returns the last committed value, ten reads in a row
never grow the log, a follower redirects a `get` with `leader_hint` exactly like it
does a write, and — the flagship test — a leader that's been cut off from its peers
refuses to answer a read instead of serving stale data, proven by deliberately
swapping in a naive (unsafe) read implementation first and watching that version
fail the exact same test.

## Project layout

```
miniraft/
  config.py     cluster membership (node_id -> host:port)      [done]
  state.py      NodeState: role, term, log, commit/apply index [done]
  log.py        RaftLog: append/get/truncate                   [done]
  store.py      KVStore: the replicated state machine          [done]
  rpc.py        RequestVote / AppendEntries / ClientCommand    [done]
  transport.py  HTTP server+client carrying the RPCs above     [done]
  node.py       RaftNode: election + replication + reads       [done — COMPLETE]
  main.py       process entrypoint
tests/
cluster_run.py     dev helper: boot N nodes locally
smoke_test_day4.py dev helper: scripted real-subprocess smoke test for Day 4
```

## GitHub repo

This project is live at: <https://github.com/fikris6889-tech/miniraft>

Part of the **Fikris Lab** portfolio of systems and algorithms projects: <https://github.com/fikris6889-tech/Fikris-lab>
