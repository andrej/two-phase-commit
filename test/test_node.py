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


class NodeTests(unittest.TestCase):

    def setUp(self):
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
            #await asyncio.sleep(0)  # yield to waiting asserts
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
        await asyncio.wait_for(event.wait(), timeout)
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
        self.coordinator.n_participants = 1
        self.coordinator.batch_size = 3

        self.coordinator.setup()
        self.participant.setup()

        async def test(kill_at=None):

            await self.trackMessage(self.coordinator.server, "PREPARE")
            await self.trackMessage(self.coordinator.server, "DONE")
            await self.trackMessage(self.participant.server, "BEGIN")
            await self.trackMessage(self.participant.server, "INSERT")
            await self.trackMessage(self.participant.server, "PREPARE")
            await self.trackMessage(self.participant.server, "COMMIT")
            await self.trackMessage(self.participant.server, "ABORT")

            await self.participant.start()
            await self.coordinator.start()
            await self.participant.coordinator.connect()

            participant_log_db_cur = self.participant_log_db.cursor()

            # 1. SEND INSERTS --------------------
            # We register custom message wrappers to intercept the messages and make assertions
            t = datetime.datetime.fromtimestamp(time.time()).replace(microsecond=0)
            row_a = ("foobar123", 9999, t)
            row_b = ("asdfasfd", 12345, t)
            row_c = ("sensorid", 0, t)

            insert_a_task = asyncio.create_task(self.coordinator.insert(*row_a))
            await self.assertAwaitMessage(self.participant.server, "BEGIN")
            trans_id = self.coordinator.current_trans_id
            self.assertEqual(self.participant.current_trans_id, trans_id)
            await self.assertAwaitMessage(self.participant.server, "INSERT")
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

    def test_err_protocol_flow_1(self):
        pass
        # assert prepare log written by coordinator

        # kill coordinator
        # bring coordinator back up

        # assert prepare message sent coordinator -> participant
        # assert prepare log written by participant
        # assert commit message sent participant -> coordinator
        # assert commit log written by coordinator
        # assert commit message sent coordinator -> participant
        # assert commit log written by participant
        # assert commit executed by participant
        # assert done message sent participant -> coordinator
        # assert data table state correct

    def test_err_protocol_flow_2(self):
        pass
        # assert prepare log written by coordinator
        # assert prepare message sent coordinator -> participant

        # kill participant before receiving prepare message

        # after timeout, assert abort message sent coordinator -> participant

        # assert abort log written by coordinator
        # assert commit message sent coordinator -> participant
        # assert commit log written by participant
        # assert commit executed by participant
        # assert done message sent participant -> coordinator
        # assert data table state correct

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
