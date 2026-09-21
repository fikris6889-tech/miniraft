import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from miniraft.log import RaftLog
from miniraft.store import KVStore


class TestRaftLog(unittest.TestCase):
    def test_empty_log(self):
        log = RaftLog()
        self.assertEqual(len(log), 0)
        self.assertEqual(log.last_index(), 0)
        self.assertEqual(log.last_term(), 0)
        self.assertIsNone(log.get(1))

    def test_append_and_get(self):
        log = RaftLog()
        e1 = log.append(term=1, command={"op": "set", "key": "a", "value": "1"})
        e2 = log.append(term=1, command={"op": "set", "key": "b", "value": "2"})
        self.assertEqual(e1.index, 1)
        self.assertEqual(e2.index, 2)
        self.assertEqual(len(log), 2)
        self.assertEqual(log.last_index(), 2)
        self.assertEqual(log.last_term(), 1)
        self.assertEqual(log.get(1).command["key"], "a")
        self.assertEqual(log.get(2).command["key"], "b")

    def test_term_at_out_of_range_is_zero(self):
        log = RaftLog()
        log.append(term=3, command={"op": "set", "key": "a", "value": "1"})
        self.assertEqual(log.term_at(99), 0)

    def test_truncate_from(self):
        log = RaftLog()
        log.append(term=1, command={"op": "set", "key": "a", "value": "1"})
        log.append(term=1, command={"op": "set", "key": "b", "value": "2"})
        log.append(term=2, command={"op": "set", "key": "c", "value": "3"})
        log.truncate_from(2)
        self.assertEqual(len(log), 1)
        self.assertEqual(log.last_index(), 1)

    def test_append_entries_conflict_overwrites_tail(self):
        log = RaftLog()
        log.append(term=1, command={"op": "set", "key": "a", "value": "1"})
        log.append(term=1, command={"op": "set", "key": "b", "value": "2"})
        from miniraft.log import LogEntry

        conflicting = [LogEntry(term=2, index=2, command={"op": "set", "key": "x", "value": "new"})]
        log.append_entries(conflicting)
        self.assertEqual(len(log), 2)
        self.assertEqual(log.get(2).term, 2)
        self.assertEqual(log.get(2).command["key"], "x")

    def test_entries_from(self):
        log = RaftLog()
        for i in range(5):
            log.append(term=1, command={"op": "set", "key": str(i), "value": i})
        entries = log.entries_from(3)
        self.assertEqual([e.index for e in entries], [3, 4, 5])


class TestKVStore(unittest.TestCase):
    def test_set_and_get(self):
        store = KVStore()
        store.apply({"op": "set", "key": "x", "value": "42"})
        self.assertEqual(store.get("x"), "42")

    def test_delete(self):
        store = KVStore()
        store.apply({"op": "set", "key": "x", "value": "42"})
        store.apply({"op": "delete", "key": "x"})
        self.assertIsNone(store.get("x"))

    def test_get_missing_key(self):
        store = KVStore()
        self.assertIsNone(store.get("missing"))

    def test_apply_get_op_returns_value(self):
        store = KVStore()
        store.apply({"op": "set", "key": "x", "value": "42"})
        result = store.apply({"op": "get", "key": "x"})
        self.assertEqual(result, {"ok": True, "value": "42"})

    def test_unknown_op_raises(self):
        store = KVStore()
        with self.assertRaises(ValueError):
            store.apply({"op": "frobnicate", "key": "x"})

    def test_snapshot(self):
        store = KVStore()
        store.apply({"op": "set", "key": "a", "value": 1})
        store.apply({"op": "set", "key": "b", "value": 2})
        self.assertEqual(store.snapshot(), {"a": 1, "b": 2})


if __name__ == "__main__":
    unittest.main()
