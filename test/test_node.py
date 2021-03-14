import concurrent.futures
import unittest
import asyncio
import tempfile
import shutil
import datetime
import time
import psycopg2
import psycopg2.extensions
import db
import node
import logging


class NodeTests(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.INFO)

        try:

            # Dictionary to keep track of counts messages received
            self.message_events = {}

            # Create empty database cluster and database for coordinator and participant
            self.participant_dir = tempfile.mkdtemp(prefix="cs223_test_node")
            self.coordinator_dir = tempfile.mkdtemp(prefix="cs223_test_node")
            self.coordinator_db_server = db.DatabaseServer(self.coordinator_dir, port=22345)
            self.participant_db_server = db.DatabaseServer(self.participant_dir, port=22346)
            self.coordinator_db_server.make()
            self.coordinator_db_server.start()
            self.participant_db_server.make()
            self.participant_db_server.start()
            self.coordinator_log_db = self.coordinator_db_server.create_db_and_connect("coordinator_log")
            self.participant_log_db = self.participant_db_server.create_db_and_connect("participant_log")
            self.participant_data_db = self.participant_db_server.create_db_and_connect("participant_data")

            # Set up communications
            self.coordinator = node.TwoPhaseCommitCoordinator(self.coordinator_log_db,
                                                              "localhost", 2347,
                                                              [("localhost", 2348)])
            self.participant = node.TwoPhaseCommitParticipant(0,
                                                              self.participant_data_db, self.participant_log_db,
                                                              "localhost", 2348,
                                                              "localhost", 2347)

        except:
            self.tearDown()
            raise

    async def async_setup(self):
        await self.coordinator.setup()
        await self.participant.setup()
        await self.trackMessage(self.coordinator.server, "PREPARE")
        await self.trackMessage(self.coordinator.server, "DONE")
        await self.trackMessage(self.participant.server, "INSERT")
        await self.trackMessage(self.participant.server, "PREPARE")
        await self.trackMessage(self.participant.server, "COMMIT")
        await self.trackMessage(self.participant.server, "ABORT")
        await self.coordinator.start()
        await self.participant.start()

    def tearDown(self):
        for remote_call_obj in self.message_events:
            for kind in self.message_events[remote_call_obj]:
                self.untrackMessage(remote_call_obj, kind)
        self.participant_data_db.close()
        self.participant_log_db.close()
        self.coordinator_log_db.close()
        self.participant_db_server.stop()
        self.coordinator_db_server.stop()
        shutil.rmtree(self.participant_dir)
        shutil.rmtree(self.coordinator_dir)

    async def trackMessage(self, remote_call_obj, kind):
        """
        Wrap the message handler of the given RemoteCallServer to set an
        event when it is called. Useful to track whether messages are
        received or not.
        """
        rco_id = id(remote_call_obj)
        old_cb = remote_call_obj.handlers[kind]
        if rco_id not in self.message_events:
            self.message_events[rco_id] = {}
        self.message_events[rco_id][kind] = (old_cb, asyncio.Event())

        async def msg_wrapper(data):
            cb, event = self.message_events[rco_id][kind]
            retval = await old_cb(data)
            event.set()
            await asyncio.sleep(0)  # yield to waiting asserts
            return retval

        remote_call_obj.handlers[kind] = msg_wrapper

    def untrackMessage(self, remote_call_obj, kind):
        rco_id = id(remote_call_obj)
        if rco_id not in self.message_events or kind not in self.message_events[rco_id]:
            return
        old_cb, event = self.message_events[rco_id][kind]
        remote_call_obj.handlers[kind] = old_cb

    async def assertAwaitMessage(self, remote_call_obj, kind, timeout=10):
        rco_id = id(remote_call_obj)
        old_cb, event = self.message_events[rco_id][kind]
        try:
            await asyncio.wait_for(event.wait(), timeout)
        except concurrent.futures.TimeoutError:
            pass
        self.assertTrue(event.is_set())
        event.clear()

    def test_write_log(self):
        self.coordinator.setup()
        self.coordinator.transactions[1] = 'DONE'
        self.coordinator.transactions[2] = 'BEGUN'
        self.coordinator.transactions[3] = 'PREPARED'
        with self.subTest("initial write"):
            self.coordinator.write_log()
            log_cur = self.coordinator_log_db.cursor()
            log_cur.execute("select transaction_id, status from log")
            rows = log_cur.fetchall()
            self.assertIn((1, "DONE"), rows)
            self.assertIn((2, "BEGUN"), rows)
            self.assertIn((3, "PREPARED"), rows)
        with self.subTest("update"):
            self.coordinator.transactions[2] = 'PREPARED'
            self.coordinator.transactions[3] = 'DONE'
            self.coordinator.write_log()
            log_cur = self.coordinator_log_db.cursor()
            log_cur.execute("select transaction_id, status from log")
            rows = log_cur.fetchall()
            self.assertIn((1, "DONE"), rows)
            self.assertIn((2, "PREPARED"), rows)
            self.assertIn((3, "DONE"), rows)

    def test_regular_protocol_flow(self):
        """
        Three INSERTS are sent to one participant node. Ensure transaction COMMITs
        and final database contains three rows.
        """
        self.coordinator.n_participants = 1
        self.coordinator.batch_size = 3

        async def test():
            await self.async_setup()

            participant_log_db_cur = self.participant_log_db.cursor()

            # 1. SEND INSERTS --------------------
            # We register custom message wrappers to intercept the messages and make assertions
            t = datetime.datetime.fromtimestamp(time.time()).replace(microsecond=0)
            row_a = ("foobar123", 9999, t)
            row_b = ("asdfasfd", 12345, t)
            row_c = ("sensorid", 0, t)

            insert_a_task = asyncio.create_task(self.coordinator.insert(*row_a))
            await self.assertAwaitMessage(self.participant.server, "INSERT")
            trans_id = self.coordinator.current_trans_id
            self.assertEqual(self.participant.current_trans_id, trans_id)
            await insert_a_task

            insert_b_task = asyncio.create_task(self.coordinator.insert(*row_b))
            await self.assertAwaitMessage(self.participant.server, "INSERT")
            await insert_b_task

            # 2. PREPARE --------------------

            insert_c_task = asyncio.create_task(self.coordinator.insert(*row_c))

            await self.assertAwaitMessage(self.participant.server, "INSERT")
            await asyncio.sleep(0)  # yield to coordinator

            await self.assertAwaitMessage(self.participant.server, "PREPARE")
            self.assertEqual(self.participant.transactions[trans_id], "PREPARED")
            participant_log_db_cur.execute("select status from log where transaction_id = %s", (trans_id,))
            row = participant_log_db_cur.fetchone()
            self.assertEqual(row, ("PREPARED",))

            await self.assertAwaitMessage(self.coordinator.server, "PREPARE")

            # 3. SEND COMMITS --------------------
            # After the third transaction, because of our batch size of 3, a prepare was sent
            # (asserted above). We expect a commit back from the participant.

            await self.assertAwaitMessage(self.participant.server, "COMMIT")
            self.assertEqual(self.participant.transactions[trans_id], "COMMITTED")
            participant_log_db_cur.execute("select status from log where transaction_id = %s", (trans_id,))
            row = participant_log_db_cur.fetchone()
            self.assertEqual(row, ("COMMITTED",))

            await self.assertAwaitMessage(self.coordinator.server, "DONE")

            await insert_c_task

            # Check if final state of database ok
            participant_data_db_cur = self.participant_data_db.cursor()
            participant_data_db_cur.execute("select sensor_id, measurement, timestamp from data")
            rows = participant_data_db_cur.fetchall()
            self.assertIn(row_a, rows)
            self.assertIn(row_b, rows)
            self.assertIn(row_c, rows)

            await self.coordinator.stop()
            await self.participant.stop()

        asyncio.run(test())

    def test_dead_participant_flow(self):
        """
        An INSERT is sent to an alive node, but one dead participant node prevents
        the system from committing.
        Ensure transaction ABORTs and no data added to database.
        """
        self.coordinator.n_participants = 2
        self.coordinator.participant_hosts.append(("localhost", "99999"))  # some bogus unreachable participant #2
        self.coordinator.batch_size = 1
        self.coordinator.timeout = 1

        async def test():
            await self.async_setup()

            t = datetime.datetime.fromtimestamp(0).replace(microsecond=0)
            row = (0, 0, t)  # hash(0) + hash(timestamp=0) maps to node #0

            # INSERT sent
            success = await self.coordinator.insert(*row)
            self.assertEqual(success, True)
            # INSERT to be received by node #0
            await self.assertAwaitMessage(self.participant.server, "INSERT")
            # PREPARE to be received by node #0
            await self.assertAwaitMessage(self.participant.server, "PREPARE")
            trans_id = self.coordinator.current_trans_id
            self.assertEqual(trans_id, self.participant.current_trans_id)
            # PREPARED to be received by coordinator, but only from node #0
            await self.assertAwaitMessage(self.coordinator.server, "PREPARE")
            # Here, a timeout should occur while waiting for PREPARED from node #1
            # Then, coordinator should send ABORT; wait for participant to receive it here
            await self.assertAwaitMessage(self.participant.server, "ABORT")
            self.assertEqual(self.participant.transactions[trans_id], "ABORTED")
            # Coordinator to receive DONE from node #1, but not from node #2
            await self.assertAwaitMessage(self.coordinator.server, "DONE")

            # Check if final state of database ok
            participant_data_db_cur = self.participant_data_db.cursor()
            participant_data_db_cur.execute("select * from data")
            rows = participant_data_db_cur.fetchall()
            self.assertEqual(len(rows), 0)

            await self.coordinator.stop()
            await self.participant.stop()

        asyncio.run(test())

    def test_recovering_participant_flow(self):
        """
        (1) Coordinator sents INSERT to alive participant.
        (2) Participant promises to commit (sends PREPARED); then DIES.
        (3) Coordinator receives PREPARED, sends COMMIT.
            -> The participant does not see this COMMIT since it is dead.
        (4) Participant comes back up. Reads log, re-sends PREPARED and receives COMMIT.

        Ensure node recovers and data is stored.
        """
        self.coordinator.n_participants = 1
        self.coordinator.batch_size = 1
        self.coordinator.timeout = 1

        async def test():
            await self.async_setup()

            async def dropper(data):
                return
            commit_cb = self.participant.server.handlers["COMMIT"]
            self.participant.server.handlers["COMMIT"] = dropper

            t = datetime.datetime.fromtimestamp(0).replace(microsecond=0)
            row = ("hello world", 123, t)  # hash(0) + hash(timestamp=0) maps to node #0

            # INSERT sent
            success = await self.coordinator.insert(*row)
            #self.assertEqual(success, True)
            # INSERT and PREPARE to be received by node #0
            await self.assertAwaitMessage(self.participant.server, "INSERT")
            await self.assertAwaitMessage(self.participant.server, "PREPARE")
            trans_id = self.coordinator.current_trans_id
            self.assertEqual(trans_id, self.participant.current_trans_id)

            # After participant sends PREPARE, we KILL the participant
            await self.assertAwaitMessage(self.coordinator.server, "PREPARE")
            self.assertEqual(self.coordinator.transactions[trans_id], "COMMITTED")
            # Drop receipt of COMMIT message; kill participant immediately thereafter
            await self.participant.stop()
            await asyncio.sleep(2)  # Give coordinator time to send COMMIT and then time out

            # Bring participant back up
            self.participant.server.handlers["COMMIT"] = commit_cb
            await self.participant.start()
            # Ensure participant sends correct message for recovery
            await self.assertAwaitMessage(self.coordinator.server, "PREPARE")
            # Ensure coordinator re-sends its decision to commit
            await self.assertAwaitMessage(self.participant.server, "COMMIT")
            await self.assertAwaitMessage(self.coordinator.server, "DONE")

            # Check if final state of database ok
            participant_data_db_cur = self.participant_data_db.cursor()
            participant_data_db_cur.execute("select * from data")
            rows = participant_data_db_cur.fetchall()
            self.assertIn(row, rows)
            self.assertEqual(len(rows), 1)

            await self.coordinator.stop()
            await self.participant.stop()

        asyncio.run(test())

    def test_participant_setup(self):
        self.participant.setup()
        with self.subTest("data table initialized correctly"):
            data_cur = self.participant_data_db.cursor()
            data_cur.execute("select table_name, column_name, data_type "
                             "from information_schema.columns "
                             "where table_schema = 'public'")
            rows = data_cur.fetchall()
            self.assertIn(("data", "sensor_id", "character varying"), rows)
            self.assertIn(("data", "measurement", "integer"), rows)
            self.assertIn(("data", "timestamp", "timestamp without time zone"), rows)
        with self.subTest("log table initialized correctly"):
            log_cur = self.participant_log_db.cursor()
            log_cur.execute("select table_name, column_name, data_type "
                            "from information_schema.columns "
                            "where table_schema = 'public'")
            rows = log_cur.fetchall()
            self.assertIn(("log", "transaction_id", "integer"), rows)
            self.assertIn(("log", "status", "character varying"), rows)
