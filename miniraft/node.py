"""RaftNode — the piece you build across Day 2, Day 3, and Day 4.

Day 1 shipped the wiring: constructing a node, opening the HTTP
transport, and safe do-nothing replies to every RPC. Day 2 was LEADER
ELECTION: randomized timeouts, the RequestVote rules, and the minimal
heartbeat plumbing that keeps a winner from getting immediately
un-elected by its own followers' timers.

Day 3 was LOG REPLICATION — the part that turns "we elected a leader"
into "the cluster durably agrees on a sequence of client commands":

  - _serve_write (called by handle_client_command for set/delete): a
    leader accepts a write, appends it to its own log, pushes it out
    to followers, and only replies once a MAJORITY has it.
  - handle_append_entries: the real §5.3 consistency check (does this
    follower's log agree with the leader up to prev_log_index?),
    conflict truncation, and commit-index advancement — Day 2 only
    handled this RPC's heartbeat/leader-recognition half.
  - send_heartbeats / _replicate_to_peer: upgraded from "empty ping"
    to real replication — each peer gets exactly the entries it's
    missing (tracked via next_index), and a leader learns how far a
    peer has caught up via match_index.

Day 4 (this file, today) is POLISH — no new algorithm surface, but one
genuinely important correctness+performance upgrade the paper covers
in §8 ("Client Interaction") rather than Figure 2: a safe, FAST read
path.

  - _serve_linearizable_read / _confirm_leadership_with_majority: the
    ReadIndex technique. Day 3's handle_client_command already made a
    `get` safe by routing it through the same append-and-replicate
    path as a write — correct, but wasteful (every read permanently
    bloats the log) and it's worth understanding WHY that was even
    necessary in the first place before optimizing it away. Day 4
    keeps the safety and drops the log-bloat: no log entry, just a
    lightweight quorum check that this node is still really leader,
    then a local read.

Reference: Ongaro & Ousterhout, "In Search of an Understandable
Consensus Algorithm" (2014), Figure 2 has the full rule table; §5.3
covers log replication in detail, §5.4.2 covers the "only commit
entries from your own term directly" subtlety that
_advance_commit_index_locked has to respect, and §8 covers the client
interaction rules — including the read-safety hazard Day 4 fixes.
"""
from __future__ import annotations

import logging
import random
import threading
import time
from typing import Any, Dict, Optional

from .config import ClusterConfig
from .log import LogEntry
from .rpc import (
    AppendEntriesArgs,
    AppendEntriesReply,
    ClientCommandArgs,
    ClientCommandReply,
    RequestVoteArgs,
    RequestVoteReply,
)
from .state import NodeState, Role
from .store import KVStore
from .transport import RPCClient, RPCServer

logger = logging.getLogger("miniraft.node")

# Election timeouts are randomized in [ELECTION_TIMEOUT_MIN, MAX] seconds
# so that not every follower times out simultaneously and splits every
# vote forever. Raft's own paper uses 150-300ms; we widen the window
# because this build talks real HTTP (urllib) instead of raw sockets,
# and CI/sandboxed machines can have noisy scheduling latency.
ELECTION_TIMEOUT_MIN = 1.5
ELECTION_TIMEOUT_MAX = 3.0
HEARTBEAT_INTERVAL = 0.5  # leader sends AppendEntries (possibly carrying entries) this often
# How long handle_client_command will wait for an entry to reach a
# majority before giving up and telling the client to retry. Generous
# relative to HEARTBEAT_INTERVAL because a command triggers an
# immediate out-of-band replication round anyway (see send_heartbeats
# call at the bottom of handle_client_command) — this timeout mostly
# exists to bound how long a caller blocks if we lose leadership or a
# chunk of the cluster is down mid-request.
CLIENT_COMMAND_TIMEOUT = 5.0


class RaftNode:
    def __init__(self, node_id: str, cluster: ClusterConfig) -> None:
        self.node_id = node_id
        self.cluster = cluster
        self.peers: Dict[str, "config.NodeAddress"] = cluster.peers_of(node_id)  # type: ignore[name-defined]

        self.state = NodeState(node_id=node_id)
        self.store = KVStore()
        self.rpc_client = RPCClient()

        # Results of applying committed entries, keyed by log index, so a
        # handle_client_command call that's blocked waiting for ITS entry
        # to commit can pick up the store's return value once it does.
        # Only ever read/written under self.state.lock.
        self._results_by_index: Dict[int, Any] = {}

        addr = cluster.address_of(node_id)
        self._server = RPCServer(addr.host, addr.port, self)

        self._election_timer: Optional[threading.Timer] = None
        self._heartbeat_timer: Optional[threading.Timer] = None
        self._stopped = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        """Bring the node up: open the HTTP transport, then arm the
        election timer. A freshly started node is a FOLLOWER waiting
        for either a real leader's heartbeat or its own timeout,
        whichever comes first.
        """
        self._server.start()
        logger.info("node %s listening on %s", self.node_id, self.cluster.address_of(self.node_id).base_url)
        self.reset_election_timer()

    def stop(self) -> None:
        with self.state.lock:
            self._stopped = True
            election_timer, self._election_timer = self._election_timer, None
            heartbeat_timer, self._heartbeat_timer = self._heartbeat_timer, None
        if election_timer:
            election_timer.cancel()
        if heartbeat_timer:
            heartbeat_timer.cancel()
        self._server.stop()

    # ------------------------------------------------------------------
    # RPC handlers — the HTTP layer (transport.py) calls these with a
    # plain dict and expects a plain dict back.
    # ------------------------------------------------------------------
    def handle_request_vote(self, payload: dict) -> dict:
        """IMPLEMENTED (Day 2). Raft paper §5.2 + §5.4 (Figure 2,
        "RequestVote RPC").

        1. Reply false if args.term < currentTerm.
        2. If args.term > currentTerm: currentTerm = args.term, become
           FOLLOWER, votedFor = None (a stale term means you must step
           down BEFORE you evaluate anything else about this RPC).
        3. Grant the vote only if:
             - votedFor is None or votedFor == candidate_id, AND
             - the candidate's log is at least as up-to-date as ours:
               compare last_log_term first, then last_log_index.
        4. Granting a vote resets your election timer (you just
           promised to give this candidate a chance — don't turn
           around and start your own election immediately after).
        """
        args = RequestVoteArgs.from_dict(payload)
        with self.state.lock:
            if args.term < self.state.current_term:
                reply = RequestVoteReply(term=self.state.current_term, vote_granted=False)
                return reply.to_dict()

            if args.term > self.state.current_term:
                self._become_follower_for_term_locked(args.term)

            our_last_term = self.state.log.last_term()
            our_last_index = self.state.log.last_index()
            log_is_up_to_date = (args.last_log_term > our_last_term) or (
                args.last_log_term == our_last_term and args.last_log_index >= our_last_index
            )
            can_vote_for_this_candidate = self.state.voted_for in (None, args.candidate_id)
            grant = can_vote_for_this_candidate and log_is_up_to_date

            if grant:
                self.state.voted_for = args.candidate_id
            reply = RequestVoteReply(term=self.state.current_term, vote_granted=grant)

        if grant:
            self.reset_election_timer()
        return reply.to_dict()

    def handle_append_entries(self, payload: dict) -> dict:
        """IMPLEMENTED (Day 2 gave you rules 1-2; DAY 3 — today — adds
        the real consistency check, conflict handling, and commit
        advancement in rules 3-5).

        Raft paper §5.3 (Figure 2, "AppendEntries RPC"):

        1. Reply false if args.term < currentTerm.
        2. Otherwise: this is a real leader — become/stay FOLLOWER,
           reset your election timer (this IS the heartbeat), and
           remember who the leader is (so we can redirect a
           misdirected client write).
        3. Reply false if our log doesn't contain an entry at
           prev_log_index whose term matches prev_log_term (the
           consistency check that makes replication safe). When this
           fails, tell the leader where OUR log actually stands via
           `conflict_index` (§5.3's back-off optimization) instead of
           making it decrement next_index one entry at a time.
        4. If an existing entry conflicts with a new one (same index,
           different term), delete it and everything after it, then
           append the new entries. (RaftLog.append_entries already
           does this — it's Day 1 infrastructure, not new today.)
        5. If leader_commit > commit_index, advance commit_index to
           min(leader_commit, index of last new entry) and apply newly
           committed entries to the state machine (store.apply).

        Common bug: doing step 5 (and applying to the store) even when
        step 3 failed the consistency check — always check log
        consistency BEFORE touching commit_index. A close second: only
        backing next_index off by exactly one entry per round instead
        of using conflict_index — technically correct, but painfully
        slow to resync a follower that's fallen far behind.
        """
        args = AppendEntriesArgs.from_dict(payload)
        with self.state.lock:
            if args.term < self.state.current_term:
                reply = AppendEntriesReply(term=self.state.current_term, success=False)
                return reply.to_dict()

            # A real leader for our term (or a newer one) — recognize it.
            self._become_follower_for_term_locked(args.term)
            self.state.leader_id = args.leader_id

            if args.prev_log_index > 0:
                our_entry = self.state.log.get(args.prev_log_index)
                if our_entry is None:
                    # We don't even have an entry at that index yet — tell
                    # the leader to back off to right after our last one.
                    reply = AppendEntriesReply(
                        term=self.state.current_term,
                        success=False,
                        conflict_index=self.state.log.last_index() + 1,
                    )
                    self.reset_election_timer()
                    return reply.to_dict()
                if our_entry.term != args.prev_log_term:
                    # We have SOMETHING at that index, but from a
                    # different (necessarily earlier, uncommitted) term.
                    # Walk back to the first entry of that conflicting
                    # term so the leader can skip the whole bad stretch
                    # in one round instead of one entry at a time.
                    conflicting_term = our_entry.term
                    conflict_index = args.prev_log_index
                    while (
                        conflict_index > 1
                        and self.state.log.term_at(conflict_index - 1) == conflicting_term
                    ):
                        conflict_index -= 1
                    reply = AppendEntriesReply(
                        term=self.state.current_term,
                        success=False,
                        conflict_index=conflict_index,
                    )
                    self.reset_election_timer()
                    return reply.to_dict()

            new_entries = args.entries_as_log_entries()
            if new_entries:
                self.state.log.append_entries(new_entries)

            if args.leader_commit > self.state.commit_index:
                self.state.commit_index = min(args.leader_commit, self.state.log.last_index())
                self._apply_committed_locked()

            reply = AppendEntriesReply(term=self.state.current_term, success=True)

        self.reset_election_timer()
        return reply.to_dict()

    def handle_client_command(self, payload: dict) -> dict:
        """Single entrypoint for both writes and reads — DAY 3 built the
        write path, DAY 4 adds a dedicated (and much cheaper) read path
        alongside it, on the same RPC, distinguished by `command["op"]`.

        This split is itself worth noticing: `get` doesn't NEED to
        change any replicated state, so it doesn't need to go through
        the log at all once we have another way to prove it's safe —
        see _serve_linearizable_read's docstring for exactly what that
        means and why a naive local read would be unsafe.
        """
        args = ClientCommandArgs.from_dict(payload)
        if args.command.get("op") == "get":
            return self._serve_linearizable_read(args.command)
        return self._serve_write(args.command)

    def _serve_write(self, command: dict) -> dict:
        """IMPLEMENTED — DAY 3. The leader-only write path for `set`/
        `delete` (anything that's NOT a `get` — see handle_client_command).

        Only the LEADER accepts writes. If we're not leader, reply
        {ok: False, leader_hint: <who we think it is>} so the client
        (or a thin client library) can retry against the right node.

        If we ARE leader:
          1. Append the command to our own log at the current term.
          2. Kick an immediate replication round to every peer (this
             reuses send_heartbeats/_replicate_to_peer — a heartbeat
             IS an AppendEntries, just possibly with an empty entries
             list; when there's a real command waiting, it carries
             real entries instead).
          3. Wait until a majority of match_index values (including
             our own log, which is always "caught up" with itself)
             reach this entry's index, then commit_index advances and
             the entry gets applied to the store.
          4. Reply once OUR OWN apply of that entry has happened, with
             the store's return value — that's the point at which
             we've made Raft's core promise real: a client holding
             `ok: True` knows the command survived on a majority of
             nodes, not just on this one.

        This blocks the calling HTTP request thread until step 4 (or a
        timeout / loss of leadership) — that's a deliberate, honest
        trade for a teaching build: a client should not be told `ok:
        True` before the write is actually safe.
        """
        with self.state.lock:
            if self.state.role != Role.LEADER:
                reply = ClientCommandReply(
                    ok=False, error="not leader", leader_hint=self.state.leader_id
                )
                return reply.to_dict()

            entry = self.state.log.append(self.state.current_term, command)
            term = self.state.current_term
            target_index = entry.index
            # Handles the single-node-cluster case immediately (our own
            # log is already a majority of one) without waiting on any
            # network round — same idea as start_election's early-exit
            # for a 1-node cluster in maybe_become_leader().
            self._advance_commit_index_locked()

        # Push the new entry out now rather than waiting for the next
        # scheduled heartbeat tick (up to HEARTBEAT_INTERVAL seconds away).
        self.send_heartbeats()

        deadline = time.monotonic() + CLIENT_COMMAND_TIMEOUT
        while time.monotonic() < deadline:
            with self.state.lock:
                if self.state.role != Role.LEADER or self.state.current_term != term:
                    reply = ClientCommandReply(
                        ok=False,
                        error="lost leadership before commit",
                        leader_hint=self.state.leader_id,
                    )
                    return reply.to_dict()
                if self.state.last_applied >= target_index:
                    result = self._results_by_index.pop(target_index, None)
                    reply = ClientCommandReply(ok=True, result=result)
                    return reply.to_dict()
            time.sleep(0.02)

        reply = ClientCommandReply(ok=False, error="timed out waiting for majority replication")
        return reply.to_dict()

    # ------------------------------------------------------------------
    # Linearizable reads — DAY 4. The ReadIndex technique (Raft paper
    # §8): serve a `get` without appending anything to the log, while
    # staying exactly as safe as routing it through the log would be.
    # ------------------------------------------------------------------
    def _serve_linearizable_read(self, command: dict) -> dict:
        """Why this exists: `store.get(key)` read directly off whatever
        node currently BELIEVES it's leader is unsafe. A leader that's
        been silently network-partitioned away from the majority has
        no way to learn that on its own — nothing pushes a step-down to
        an isolated node, it only ever learns via an incoming RPC, and
        a partitioned node receives none. Meanwhile the OTHER side of
        the partition, which still has a majority, times out and elects
        a brand new leader that keeps taking writes. Until the network
        heals, you'd have TWO nodes both convinced they're leader — and
        a client unlucky enough to keep talking to the stale, isolated
        one would silently get old data forever, no error, nothing.
        Day 3's trick of routing `get` through the same append+majority
        path as a write dodges this entirely (an isolated leader can
        never get a majority to replicate anything, so the "read"
        just times out safely) — but it pays for that safety by
        permanently growing the log with an entry that never needed to
        exist. This function keeps the safety and drops the log-bloat:

          1. Snapshot commit_index right now as `read_index` — the
             point in the log we need to have LOCALLY applied before
             it's safe to answer.
          2. Confirm, via a real network round-trip to a majority of
             peers (_confirm_leadership_with_majority), that we are
             STILL leader for our current term at this exact moment.
             This is the step that makes an isolated leader safe: it
             can never complete this round-trip against a majority it
             can't reach, so it can never pass this check.
          3. Once confirmed, wait (locally, no more network) until
             last_applied has caught up to read_index — it may already
             have, or the apply from step 1's snapshot might still be
             a few milliseconds behind on a very busy leader.
          4. Only THEN read the store. `get` never mutates state (see
             KVStore.apply), so applying it directly here — instead of
             going through the log/_apply_committed_locked machinery —
             is safe: there's nothing to make durable or replicate.

        Common mistake this guards against: reading step 4 BEFORE step
        2 "because the data's already there anyway." It usually IS
        already there — which is exactly what makes this bug so easy
        to ship and so rare to catch in casual testing. It only shows
        up under a real partition, which is precisely why the project's
        test suite has to manufacture one on purpose (see
        test_reads.py's TestPartitionedLeaderCannotServeStaleReads)
        rather than relying on ever seeing it by accident.
        """
        with self.state.lock:
            if self.state.role != Role.LEADER:
                reply = ClientCommandReply(
                    ok=False, error="not leader", leader_hint=self.state.leader_id
                )
                return reply.to_dict()
            read_index = self.state.commit_index
            term = self.state.current_term

        if not self._confirm_leadership_with_majority(term):
            with self.state.lock:
                hint = self.state.leader_id
            reply = ClientCommandReply(
                ok=False,
                error="could not confirm leadership with a majority before this read",
                leader_hint=hint,
            )
            return reply.to_dict()

        deadline = time.monotonic() + CLIENT_COMMAND_TIMEOUT
        while time.monotonic() < deadline:
            with self.state.lock:
                if self.state.role != Role.LEADER or self.state.current_term != term:
                    reply = ClientCommandReply(
                        ok=False,
                        error="lost leadership waiting to serve this read",
                        leader_hint=self.state.leader_id,
                    )
                    return reply.to_dict()
                if self.state.last_applied >= read_index:
                    result = self.store.apply(command)
                    return ClientCommandReply(ok=True, result=result).to_dict()
            time.sleep(0.02)

        reply = ClientCommandReply(
            ok=False, error="timed out waiting for local state to catch up before read"
        )
        return reply.to_dict()

    def _confirm_leadership_with_majority(self, term: int) -> bool:
        """Fire a lightweight, side-effect-free AppendEntries "probe" at
        every peer and block until a majority (counting ourselves,
        exactly like _advance_commit_index_locked's majority math) have
        proven — within THIS round, not some earlier one — that they
        still recognize us as leader for `term`.

        The probe deliberately sends prev_log_index=0 and an empty
        entries list. prev_log_index=0 skips handle_append_entries'
        §5.3 consistency check entirely (it's only evaluated when
        prev_log_index > 0), so a probe can never spuriously fail the
        way a real replication round could, and an empty entries list
        plus leader_commit=0 means it can never truncate a follower's
        log or move its commit_index either. All this round can
        possibly tell us is the one thing we actually need: does this
        peer currently accept us as leader for `term`, or has it moved
        on to something newer? That's cheap on purpose — it runs on
        the hot path of every single read.

        A peer that's merely SLOW (but not actually gone) will still
        answer; only an unreachable or genuinely-ahead peer fails to
        ack. Returns False if we can't reach a majority in time, OR if
        we've stopped being leader for `term` by the time enough acks
        arrive — either way, the caller must refuse the read rather
        than risk serving stale data.
        """
        with self.state.lock:
            if self._stopped or self.state.role != Role.LEADER or self.state.current_term != term:
                return False
            peers = dict(self.peers)

        if not peers:
            # Single-node cluster: our own log is already a majority of
            # one, exactly like the write path's single-node fast case.
            return True

        majority = len(self.cluster.nodes) // 2 + 1
        acked = {self.node_id}
        acked_lock = threading.Lock()
        confirmed = threading.Event()

        def probe(peer_id: str, addr) -> None:
            args = AppendEntriesArgs(
                term=term,
                leader_id=self.node_id,
                prev_log_index=0,
                prev_log_term=0,
                entries=[],
                leader_commit=0,
            )
            reply_dict = self.rpc_client.call(addr.base_url, "/rpc/append_entries", args.to_dict())
            if reply_dict is None:
                return  # unreachable this round — no ack, not an error
            reply = AppendEntriesReply.from_dict(reply_dict)

            with self.state.lock:
                if reply.term > self.state.current_term:
                    # A higher term exists — someone else has already
                    # moved on. Step down for real, don't just fail
                    # this one read.
                    self._become_follower_for_term_locked(reply.term)
                    self.reset_election_timer()
                    return
                still_current = self.state.role == Role.LEADER and self.state.current_term == term

            if still_current and reply.term == term:
                with acked_lock:
                    acked.add(peer_id)
                    if len(acked) >= majority:
                        confirmed.set()

        threads = [
            threading.Thread(target=probe, args=(peer_id, addr), daemon=True)
            for peer_id, addr in peers.items()
        ]
        for t in threads:
            t.start()
        confirmed.wait(timeout=CLIENT_COMMAND_TIMEOUT)

        with self.state.lock:
            still_leader = self.state.role == Role.LEADER and self.state.current_term == term
        return confirmed.is_set() and still_leader

    # ------------------------------------------------------------------
    # Internal helper — stepping down is the same operation whether it's
    # triggered by a vote request, an append-entries heartbeat, or a
    # higher-term reply seen while we were a candidate/leader ourselves.
    # Callers must already hold self.state.lock.
    # ------------------------------------------------------------------
    def _become_follower_for_term_locked(self, term: int) -> None:
        was_leader = self.state.role == Role.LEADER
        self.state.role = Role.FOLLOWER
        self.state.current_term = term
        self.state.voted_for = None
        # We don't yet know who (if anyone) leads this new term — a
        # caller that DOES know (handle_append_entries) sets this right
        # back to something real immediately after calling us.
        self.state.leader_id = None
        heartbeat_timer, self._heartbeat_timer = self._heartbeat_timer, None
        if was_leader and heartbeat_timer:
            # Cancelling a Timer from inside a lock is fine (it never
            # blocks on the callback), but keep the .cancel() call itself
            # outside any lock re-entry concerns by doing it here, last.
            heartbeat_timer.cancel()

    # ------------------------------------------------------------------
    # Commit-index bookkeeping — DAY 3.
    # ------------------------------------------------------------------
    def _advance_commit_index_locked(self) -> None:
        """Raft paper §5.3/§5.4.2: a LEADER advances commit_index to the
        highest N such that a majority of match_index[*] (counting the
        leader's own log as always matching itself) are >= N, AND the
        entry at N belongs to the leader's CURRENT term.

        That second condition is the subtle, easy-to-skip part of the
        rule (§5.4.2): a leader may NOT conclude an entry from an
        OLDER term is committed just because a majority now stores it
        — an entry from a past term can still be silently overwritten
        by a future leader that doesn't have it, until an entry from
        the CURRENT term also reaches a majority alongside it. Skipping
        this check is a classic "looks fine in the happy path, breaks
        under a leader change mid-replication" bug.

        Caller must already hold self.state.lock, and this is a no-op
        if we're not currently the leader (a stale/late call after a
        step-down should never move commit_index).
        """
        if self.state.role != Role.LEADER:
            return
        majority = len(self.cluster.nodes) // 2 + 1
        match_values = [self.state.log.last_index()]  # the leader's own log
        for peer_id in self.peers:
            match_values.append(self.state.match_index.get(peer_id, 0))
        match_values.sort()
        # The majority-th highest value: with `majority` nodes (out of
        # len(cluster.nodes)) at or above it, N is safely committed.
        candidate_n = match_values[-majority]
        if candidate_n > self.state.commit_index and self.state.log.term_at(candidate_n) == self.state.current_term:
            self.state.commit_index = candidate_n
            self._apply_committed_locked()

    def _apply_committed_locked(self) -> None:
        """Apply every entry between last_applied and commit_index, in
        order, to the state machine — and stash each result so a
        handle_client_command call blocked on that index can pick it
        up. Caller must already hold self.state.lock."""
        while self.state.last_applied < self.state.commit_index:
            self.state.last_applied += 1
            entry = self.state.log.get(self.state.last_applied)
            result = self.store.apply(entry.command) if entry is not None else None
            self._results_by_index[self.state.last_applied] = result

    # ------------------------------------------------------------------
    # Election machinery — DAY 2.
    # ------------------------------------------------------------------
    def reset_election_timer(self) -> None:
        """Cancel any pending election timer and schedule a new one with
        a random timeout in [ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX]
        that calls self.start_election() when it fires. Called:
          - once, when the node starts (as a follower)
          - every time we grant a vote to a candidate
          - every time we accept a valid AppendEntries from the leader
            (this includes a failed consistency check on Day 3 — that's
            still proof a real leader for our term is alive, even
            though replication itself has to back off and retry)
        Skipping any of those call sites produces either spurious
        elections (annoying) or a follower that never notices a dead
        leader (much worse) — see Common Mistake #2 in Day 2's post.
        """
        with self.state.lock:
            if self._stopped:
                return
            if self._election_timer:
                self._election_timer.cancel()
            timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
            timer = threading.Timer(timeout, self._on_election_timeout)
            timer.daemon = True
            self._election_timer = timer
        timer.start()

    def _on_election_timeout(self) -> None:
        with self.state.lock:
            if self._stopped or self.state.role == Role.LEADER:
                return
        self.start_election()

    def start_election(self) -> None:
        """1. currentTerm += 1, become CANDIDATE, voted_for = self.node_id
        2. Reset your own election timer (in case this election also
           times out without a winner).
        3. Send RequestVote RPCs to every peer IN PARALLEL (don't do
           this sequentially — a single slow/dead peer would stall
           the whole election past the timeout).
        4. Count votes including your own. If you get a majority
           (> len(cluster)/2, integer division rounds down so use
           `>` not `>=`), call become_leader().
        5. If, while waiting, you see a reply with a higher term than
           yours, step down to FOLLOWER immediately and stop counting.
        """
        with self.state.lock:
            if self._stopped:
                return
            self.state.role = Role.CANDIDATE
            self.state.current_term += 1
            self.state.voted_for = self.node_id
            self.state.leader_id = None
            current_term = self.state.current_term
            last_log_index = self.state.log.last_index()
            last_log_term = self.state.log.last_term()
        logger.info("node %s starting election for term %d", self.node_id, current_term)
        self.reset_election_timer()

        peers = dict(self.peers)
        # `> len(cluster)/2` with integer semantics: for a 3-node cluster
        # that's 2, for 5 it's 3 — a strict majority either way.
        majority = len(self.cluster.nodes) // 2 + 1
        votes = {self.node_id}
        votes_lock = threading.Lock()
        decided = threading.Event()

        def maybe_become_leader() -> None:
            with votes_lock:
                if decided.is_set():
                    return
                if len(votes) >= majority:
                    decided.set()
                else:
                    return
            self.become_leader()

        # Handles the single-node-cluster case (no peers, our own vote
        # is already a majority) without waiting on any network I/O.
        maybe_become_leader()

        def request_vote_from(peer_id: str, addr) -> None:
            args = RequestVoteArgs(
                term=current_term,
                candidate_id=self.node_id,
                last_log_index=last_log_index,
                last_log_term=last_log_term,
            )
            reply_dict = self.rpc_client.call(addr.base_url, "/rpc/request_vote", args.to_dict())
            if reply_dict is None:
                return  # peer unreachable — treat as "no vote", not an error
            reply = RequestVoteReply.from_dict(reply_dict)

            with self.state.lock:
                if reply.term > self.state.current_term:
                    self._become_follower_for_term_locked(reply.term)
                    still_a_valid_candidate = False
                else:
                    still_a_valid_candidate = (
                        self.state.role == Role.CANDIDATE and self.state.current_term == current_term
                    )
            if reply.term > current_term:
                self.reset_election_timer()
                return
            if not still_a_valid_candidate or not reply.vote_granted:
                return

            with votes_lock:
                votes.add(peer_id)
            maybe_become_leader()

        for peer_id, addr in peers.items():
            threading.Thread(target=request_vote_from, args=(peer_id, addr), daemon=True).start()

    def become_leader(self) -> None:
        """- role = LEADER, leader_id = ourselves
        - Reinitialize next_index[peer] = self.state.log.last_index() + 1
          and match_index[peer] = 0 for every peer (Figure 2: "reinitialized
          after election").
        - Start sending heartbeats immediately (don't wait for the
          first HEARTBEAT_INTERVAL tick — a fresh leader that's silent
          for 500ms risks a peer's election timer firing first).
        """
        with self.state.lock:
            if self._stopped or self.state.role == Role.LEADER:
                return
            self.state.role = Role.LEADER
            self.state.leader_id = self.node_id
            last_index = self.state.log.last_index()
            for peer_id in self.peers:
                self.state.next_index[peer_id] = last_index + 1
                self.state.match_index[peer_id] = 0
            term = self.state.current_term
            election_timer, self._election_timer = self._election_timer, None
        if election_timer:
            election_timer.cancel()
        logger.info("node %s became LEADER for term %d", self.node_id, term)
        self.send_heartbeats()
        self._schedule_next_heartbeat()

    def _schedule_next_heartbeat(self) -> None:
        with self.state.lock:
            if self._stopped or self.state.role != Role.LEADER:
                return
            timer = threading.Timer(HEARTBEAT_INTERVAL, self._heartbeat_tick)
            timer.daemon = True
            self._heartbeat_timer = timer
        timer.start()

    def _heartbeat_tick(self) -> None:
        self.send_heartbeats()
        self._schedule_next_heartbeat()

    def send_heartbeats(self) -> None:
        """Fan out one AppendEntries round to every peer, in parallel.
        Despite the name (kept from Day 2 — this IS still what the
        heartbeat timer calls every HEARTBEAT_INTERVAL), each call now
        does real replication: whatever a peer's next_index says it's
        missing, it gets. When there's nothing new, that's an empty
        entries list — the same RPC the follower's timer treats as "the
        leader is alive," which is exactly the point.

        Also called directly (not just from the timer) by
        handle_client_command, so a fresh write goes out immediately
        instead of waiting up to HEARTBEAT_INTERVAL seconds for the
        next scheduled tick.
        """
        with self.state.lock:
            if self._stopped or self.state.role != Role.LEADER:
                return
            term = self.state.current_term
            peers = dict(self.peers)

        for peer_id, addr in peers.items():
            threading.Thread(
                target=self._replicate_to_peer, args=(peer_id, addr, term), daemon=True
            ).start()

    def _replicate_to_peer(self, peer_id: str, addr, term: int) -> None:
        """One replication round to one peer: send whatever entries
        `next_index[peer_id]` says it's missing, then on the reply
        either advance match_index/next_index (success) or back
        next_index off using the follower's conflict_index (failure) —
        DAY 3.

        Common bug to avoid: updating match_index/next_index using
        whatever the CURRENT next_index says, instead of the value
        this specific round actually sent — with concurrent rounds in
        flight (the heartbeat timer and an immediate client-triggered
        round can overlap), those can differ. This function captures
        prev_log_index/entries as local values up front and only ever
        uses those same local values to interpret the reply, so a
        match_index update always reflects what THIS round proved the
        peer has, not a guess based on state that may have moved on.
        """
        with self.state.lock:
            if self._stopped or self.state.current_term != term or self.state.role != Role.LEADER:
                return
            next_idx = self.state.next_index.get(peer_id, self.state.log.last_index() + 1)
            prev_log_index = next_idx - 1
            prev_log_term = self.state.log.term_at(prev_log_index)
            entries = [e.to_dict() for e in self.state.log.entries_from(next_idx)]
            leader_commit = self.state.commit_index

        args = AppendEntriesArgs(
            term=term,
            leader_id=self.node_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=leader_commit,
        )
        reply_dict = self.rpc_client.call(addr.base_url, "/rpc/append_entries", args.to_dict())
        if reply_dict is None:
            return  # peer unreachable this round — next heartbeat tick retries
        reply = AppendEntriesReply.from_dict(reply_dict)

        with self.state.lock:
            if self.state.current_term != term:
                return  # a lot happened since we sent this — ignore, stale round
            if reply.term > self.state.current_term:
                self._become_follower_for_term_locked(reply.term)
                self.reset_election_timer()
                return
            if self.state.role != Role.LEADER:
                return
            if reply.success:
                new_match = prev_log_index + len(entries)
                if new_match > self.state.match_index.get(peer_id, 0):
                    self.state.match_index[peer_id] = new_match
                    self.state.next_index[peer_id] = new_match + 1
                    self._advance_commit_index_locked()
            else:
                # Back next_index off — prefer the follower's own
                # conflict_index (§5.3 optimization: skip a whole bad
                # stretch in one round) over decrementing by one, but
                # never let a stale/out-of-order reply move next_index
                # BACK UP, only down.
                current_next = self.state.next_index.get(peer_id, 1)
                if reply.conflict_index is not None and reply.conflict_index > 0:
                    self.state.next_index[peer_id] = min(current_next, reply.conflict_index)
                else:
                    self.state.next_index[peer_id] = max(1, current_next - 1)
