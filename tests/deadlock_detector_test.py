import unittest

from apps.broker.transactions.deadlock_detector import DeadlockDetector
from apps.broker.transactions.record import DbKey
from apps.broker.transactions.transaction import TxnId


class DeadlockDetectorTest(unittest.TestCase):
    def test_should_detect_deadlock_for_write_operations(self):
        """
        T1 -> T2 -> T3 -|
        ^---------------|
        """
        # given
        d = DeadlockDetector()
        t1 = TxnId(1)
        t2 = TxnId(2)
        t3 = TxnId(3)
        key1 = DbKey('key1')
        key2 = DbKey('key2')
        key3 = DbKey('key3')

        # T2 -> T3
        d.add_write(t3, key3)
        d.add_write(t2, key3)
        # T1 -> T2
        d.add_write(t2, key2)
        d.add_write(t1, key2)
        # T3 -> T1
        d.add_write(t1, key1)
        d.add_write(t3, key1)

        # when
        result = d.detect()

        # then
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], {t1, t2, t3})

    def test_should_detect_deadlock_for_conflicting_operations(self):
        """
        T1 -> T2 -> T3 -|
        ^---------------|
        """

        # given
        d = DeadlockDetector()
        t1 = TxnId(1)
        t2 = TxnId(2)
        t3 = TxnId(3)
        key1 = DbKey('key1')
        key2 = DbKey('key2')
        key3 = DbKey('key3')

        # T2 -> T3
        d.add_write(t3, key3)
        d.add_read(t2, key3)
        # T1 -> T2
        d.add_write(t2, key2)
        d.add_read(t1, key2)
        # T3 -> T1
        d.add_read(t1, key1)
        d.add_write(t3, key1)

        # when
        result = d.detect()

        # then
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], {t1, t2, t3})

    def test_should_not_detect_deadlock_for_read_operations(self):
        """
        T1 -> T2 -> T3 -|
        ^---------------|
        """

        # given
        d = DeadlockDetector()
        t1 = TxnId(1)
        t2 = TxnId(2)
        t3 = TxnId(3)
        key1 = DbKey('key1')
        key2 = DbKey('key2')
        key3 = DbKey('key3')

        # T2 -> T3
        d.add_read(t3, key3)
        d.add_read(t2, key3)
        # T1 -> T2
        d.add_read(t2, key2)
        d.add_read(t1, key2)
        # T3 -> T1
        d.add_read(t1, key1)
        d.add_read(t3, key1)

        # when
        result = d.detect()

        # then
        self.assertEqual(len(result), 0)

    def test_should_detect_all_deadlocks_at_once(self):
        """
         T1 --→  T2
              ↗  |
            /    ↓
         T4 ←-- T3

         T5 --→ T6
         ↑       |
         |       ↓
         T8 ←-- T7
        """
        # given
        d = DeadlockDetector()
        t1 = TxnId(1)
        t2 = TxnId(2)
        t3 = TxnId(3)
        t4 = TxnId(4)
        t5 = TxnId(5)
        t6 = TxnId(6)
        t7 = TxnId(7)
        t8 = TxnId(8)

        key1 = DbKey('key1')
        key2 = DbKey('key2')
        key3 = DbKey('key3')
        key4 = DbKey('key4')
        key5 = DbKey('key5')
        key6 = DbKey('key6')
        key7 = DbKey('key7')
        key8 = DbKey('key8')

        # T1 --> T2
        d.add_write(t2, key2)
        d.add_write(t1, key2)

        # T2 --> T3
        d.add_write(t3, key3)
        d.add_write(t2, key3)

        # T3 --> T4
        d.add_write(t4, key4)
        d.add_write(t3, key4)

        # T4 --> T2
        d.add_write(t2, key1)
        d.add_write(t4, key1)

        # T5 --> T6
        d.add_write(t6, key6)
        d.add_write(t5, key6)

        # T6 --> T7
        d.add_write(t7, key7)
        d.add_write(t6, key7)

        # T7 --> T8
        d.add_write(t8, key8)
        d.add_write(t7, key8)

        # T8 --> T5
        d.add_write(t5, key5)
        d.add_write(t8, key5)

        # when
        deadlocks = d.detect()

        # then
        self.assertEqual(len(deadlocks), 2)
        self.assertEqual(deadlocks[0], {t2, t3, t4})
        self.assertEqual(deadlocks[1], {t5, t6, t7, t8})

    def test_should_remove_transactions_and_deadlocks(self):
        """
         T1 --→  T2
              ↗  |
            /    ↓
         T4 ←-- T3

         T5 --→ T6
         ↑       |
         |       ↓
         T8 ←-- T7
        """
        # given
        d = DeadlockDetector()
        t1 = TxnId(1)
        t2 = TxnId(2)
        t3 = TxnId(3)
        t4 = TxnId(4)
        t5 = TxnId(5)
        t6 = TxnId(6)
        t7 = TxnId(7)
        t8 = TxnId(8)

        key1 = DbKey('key1')
        key2 = DbKey('key2')
        key3 = DbKey('key3')
        key4 = DbKey('key4')
        key5 = DbKey('key5')
        key6 = DbKey('key6')
        key7 = DbKey('key7')
        key8 = DbKey('key8')

        # T1 --> T2
        d.add_write(t2, key2)
        d.add_write(t1, key2)

        # T2 --> T3
        d.add_write(t3, key3)
        d.add_write(t2, key3)

        # T3 --> T4
        d.add_write(t4, key4)
        d.add_write(t3, key4)

        # T4 --> T2
        d.add_write(t2, key1)
        d.add_write(t4, key1)

        # T5 --> T6
        d.add_write(t6, key6)
        d.add_write(t5, key6)

        # T6 --> T7
        d.add_write(t7, key7)
        d.add_write(t6, key7)

        # T7 --> T8
        d.add_write(t8, key8)
        d.add_write(t7, key8)

        # T8 --> T5
        d.add_write(t5, key5)
        d.add_write(t8, key5)

        deadlocks = d.detect()

        self.assertEqual(len(deadlocks), 2)
        self.assertEqual(deadlocks[0], {t2, t3, t4})
        self.assertEqual(deadlocks[1], {t5, t6, t7, t8})

        # when
        d.remove_txn(t2, {key2, key3, key1})

        deadlocks = d.detect()

        self.assertEqual(len(deadlocks), 1)
        self.assertEqual(deadlocks[0], {t5, t6, t7, t8})

        # when
        d.remove_txn(t7, {key7, key8})

        deadlocks = d.detect()

        self.assertEqual(len(deadlocks), 0)

    def test_should_not_detect_deadlocks_for_single_transaction(self):
        # given
        d = DeadlockDetector()
        t1 = TxnId(1)

        d.add_read(t1, DbKey("key1"))
        d.add_write(t1, DbKey("key1"))

        # when
        deadlocks = d.detect()

        # then
        self.assertEqual(len(deadlocks), 0)
