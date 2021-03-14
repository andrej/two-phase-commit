import typing
import asyncio
import concurrent.futures
import time
import datetime
import logging
import psycopg2.errors
import psycopg2.extensions
import comm


class TwoPhaseCommitNode:
    """
    Common functionality to both coordinator and participant nodes;
    logging and database access.
    """

    def __init__(self, log_db_conn, own_hostname, own_port):
        self.server = comm.RemoteCallServer(own_hostname, own_port)
        """The remote call server that will handle the different messages received with callbacks."""

        self.transactions: typing.Dict[int, str] = {}
        """self.transactions maps transaction ID to last known state (according to logs)"""

        self.log_db_conn: psycopg2.extensions.connection = log_db_conn
        """self.db_conn is the database connection for this node used for logs"""

        self.log_db_cur: psycopg2.extensions.cursor = self.log_db_conn.cursor()

        self.logger = logging.getLogger()

    async def start(self):
        await self.server.start()

    async def stop(self):
        self.write_log()
        await self.server.stop()

    def initialize_log(self):
        self.log_db_cur.execute("create table if not exists log "
                                "(transaction_id int not null primary key, "
                                " status varchar(20) not null)")
        self.log_db_conn.commit()

    def write_log(self):
        """
        Force write the current state in self.transactions to the database.
        """
        with self.log_db_conn:
            # Remove all deleted ones
            self.log_db_cur.execute("select * from log")
            for trans_id, _ in self.log_db_cur.fetchall():
                if trans_id not in self.transactions:
                    self.log_db_cur.execute("delete from log where transaction_id = %s", (trans_id,))
            # Update to new values / Insert
            for trans_id, status in self.transactions.items():
                self.log_db_cur.execute("insert into log (transaction_id, status) values(%s, %s) "
                                        "on conflict (transaction_id) do update set status = %s",
                                        (trans_id, status, status))

    def read_log(self):
        """
        Read the current state from the database into self.transactions, for
        all non-completed transactions
        """
        self.transactions = {}  # reset
        self.log_db_cur.execute("select * from log")
        for trans_id, status in self.log_db_cur:
            self.transactions[trans_id] = status


class TwoPhaseCommitCoordinator(TwoPhaseCommitNode):

    def __init__(self, log_db_conn, own_hostname, own_port, participants, timeout=10):
        super().__init__(log_db_conn, own_hostname, own_port)
        self.logger.name = "Coordinator"
        self.participant_hosts = participants
        self.participants: typing.List[comm.RemoteCallClient] = []
        self.timeout = timeout
        self.current_trans_id = None
        self.insert_counter = 0
        self.batch_size = 3
        self.prepared_to_commit: typing.Dict[int, typing.List[typing.Optional[bool]]] = {}
        self.done: typing.Dict[int, typing.List[typing.Optional[bool]]] = {}
        self.everyone_prepared_event = None

    async def setup(self):
        for hostname, port in self.participant_hosts:
            participant = comm.RemoteCallClient(hostname, port)
            participant.timeout = self.timeout
            self.participants.append(participant)
        self.server.register_handler("PREPARE", self.recv_prepare)
        self.server.register_handler("DONE", self.recv_done)
        self.server.register_handler("INSERT", self.recv_insert)  # Client request
        self.everyone_prepared_event = asyncio.Event()
        self.initialize_log()

    async def start(self):
        await super().start()
        await self.recover()

    async def send_all(self, kind, data):
        """
        Send a message to all participants.
        """
        sends = []
        results = []
        for participant in self.participants:
            sends.append(asyncio.create_task(participant.send_timeout(kind, data)))
        for send in sends:
            results.append(await send)
        return results

    def set_done(self, trans_id, node_id):
        if trans_id not in self.done:
            self.done[trans_id] = [False] * len(self.participants)
        self.done[trans_id][node_id] = True

    def set_prepared(self, trans_id, node_id, prepared=True):
        if trans_id not in self.prepared_to_commit:
            self.prepared_to_commit[trans_id] = [None] * len(self.participants)
        self.prepared_to_commit[trans_id][node_id] = prepared
        if trans_id == self.current_trans_id and all(x is not None for x in self.prepared_to_commit[trans_id]):
            self.everyone_prepared_event.set()

    async def recv_insert(self, data):
        sensor_id = data["sensor_id"]
        measurement = data["measurement"]
        timestamp = datetime.datetime.fromtimestamp(data["timestamp"])
        self.logger.info(f"Received INSERT ({sensor_id}, {measurement}, {timestamp}) request from client.")
        await self.insert(sensor_id, measurement, timestamp)

    async def insert(self, sensor_id, measurement, timestamp: datetime.datetime):
        if self.insert_counter == 0:
            began = self.begin_transaction()
            if not began:
                return False
        node_i = (hash(sensor_id) + hash(timestamp.timestamp())) % len(self.participants)
        participant = self.participants[node_i]
        timestamp = time.mktime(timestamp.timetuple())
        inserted = await participant.send_timeout("INSERT", (self.current_trans_id, sensor_id, measurement, timestamp))
        if not inserted:
            self.logger.error("INSERT did not reach destination node or was not successful.")
            return False
        self.logger.info(f"Sent INSERT ({sensor_id}, {measurement}, {timestamp}) to participant {node_i}.")
        self.insert_counter += 1
        if self.insert_counter == self.batch_size:
            await self.complete_transaction()
        return True

    def begin_transaction(self):
        """
        A new transaction may only be begun if the previous one was completed!
        We wait for acknowledgements from all nodes that the BEGIN was successful,
        i.e. the previous transaction is done.
        """
        if not self.current_trans_id:
            self.current_trans_id = max(self.transactions.keys()) if self.transactions.keys() else 0
        if (self.current_trans_id in self.transactions
                and self.transactions[self.current_trans_id] not in ["PREPARED", "COMMITTED", "ABORTED"]):
            self.logger.error("May not BEGIN a new transaction; previous one has not completed yet.")
            return False
        self.current_trans_id += 1
        self.everyone_prepared_event.clear()
        return True

    async def complete_transaction(self):
        self.insert_counter = 0
        trans_id = self.current_trans_id
        # Send request-to-prepare messages
        # Write prepare to log (forced)
        self.transactions[trans_id] = "PREPARED"
        self.write_log()
        await self.prepare_transaction(trans_id)

    async def prepare_transaction(self, trans_id):
        assert self.transactions[trans_id] == "PREPARED"
        await self.send_all("PREPARE", trans_id)
        self.logger.info(f"Sent PREPARE {trans_id} to all participants.")
        try:
            await asyncio.wait_for(self.everyone_prepared_event.wait(), self.timeout)
            do_commit = all(self.prepared_to_commit[self.current_trans_id])
        except concurrent.futures.TimeoutError:
            do_commit = False
        if do_commit:
            self.logger.info(f"Every participant replied with PREPARED COMMIT.")
            self.transactions[trans_id] = "COMMITTED"
            self.write_log()
            await self.commit_transaction(trans_id)
        else:
            self.logger.info(f"At least one participant replied with PREPARED ABORT or timed out before replying with "
                             f"a PREPARED message.")
            self.transactions[trans_id] = "ABORTED"
            self.write_log()
            await self.abort_transaction(trans_id)

    async def recv_prepare(self, data):
        """
        Receive PREPARE from participant.
        (a) This is for our current transaction. Set appropriate flags.
        (b) This is for a past transaction. Simply reply with the decision
            to commit or abort without setting flags (we have already made the
            decision, participant just died and is asking what to do).
        """
        node_id, trans_id, action = data

        if trans_id not in self.transactions:
            # We only remove transactions from the table after all nodes have
            # acknowledged committing/aborting them; after this, they would not send a
            # PREPARE any more.
            self.logger.error("Illegal PREPARE message for unknown transaction received.")
            return False

        state = self.transactions[trans_id]

        if state == "COMMITTED":
            self.logger.info(f"Received PREPARED from participant {node_id} for transaction that has "
                             f"already committed previously.")
            await self.participants[node_id].send_timeout("COMMIT", trans_id)
            return

        elif state == "ABORTED":
            self.logger.info(f"Received PREPARED from participant {node_id} for transaction that has "
                             f"already been aborted previously.")
            await self.participants[node_id].send_timeout("ABORT", trans_id)
            return

        elif state == "PREPARED":
            self.set_prepared(trans_id, node_id, (action == "COMMIT"))
            self.logger.info(f"Received PREPARED {action} from participant {node_id}.")

        else:
            self.logger.error(f"Illegal PREPARE message received for transaction {trans_id} in state {state} from "
                              f"node {node_id}.")
            return

    async def commit_transaction(self, trans_id):
        assert self.transactions[trans_id] == "COMMITTED"
        self.logger.info(f"Sending COMMIT to all participants.")
        await self.send_all("COMMIT", trans_id)

    async def abort_transaction(self, trans_id):
        assert self.transactions[trans_id] == "ABORTED"
        self.logger.info(f"Sending ABORT to all participants.")
        await self.send_all("ABORT", trans_id)

    async def recv_done(self, data):
        node_id, trans_id = data
        if (trans_id not in self.transactions or
                self.transactions[trans_id] not in ["COMMITTED", "ABORTED"]):
            self.logger.error(f"Illegal DONE message received from node {node_id} for transaction {trans_id}.")
            return
        self.set_done(trans_id, node_id)
        self.logger.info(f"Received DONE from node {node_id}.")
        if all(self.done[trans_id]):
            self.logger.info(f"Everyone DONE. Removing transaction {trans_id}.")
            self.transactions[trans_id] = "DONE"
            # del self.transactions[trans_id]
            # FIXME do not delete transaction from table; we need it to keep increasing transaction_ids in begin_transaction
            del self.done[trans_id]
            if trans_id in self.prepared_to_commit:
                del self.prepared_to_commit[trans_id]
        return True

    async def recover(self):
        self.read_log()
        self.logger.info(f"Recovering. Read {len(self.transactions)} transactions from log.")
        tasks = []
        for trans_id, state in self.transactions.items():
            task = None
            if state == "PREPARED":
                task = asyncio.create_task(self.prepare_transaction(trans_id))
            elif state == "COMMITTED":
                task = asyncio.create_task(self.commit_transaction(trans_id))
            elif state == "ABORTED":
                task = asyncio.create_task(self.abort_transaction(trans_id))
            if task:
                tasks.append(task)
        for task in tasks:
            await task


class TwoPhaseCommitParticipant(TwoPhaseCommitNode):
    """
    A participant stores the actual data. For each transaction, we have the following
    states:

    BEGUN: Transaction is accepting new insert requests. There should only be
           one BEGUN transaction at a time (the current_trans_id).
    PREPARED: We have promised the coordinator to commit this transaction if
              he tells us to. The prepared transaction was written to disk.
              Do not know the decision yet.
    COMMITTED, ABORTED: We are done with this transaction (committed/aborted it),
                        or we crashed right before being done. Know the decision.
    """

    def __init__(self, node_id: int,
                 data_db_conn: psycopg2.extensions.connection, log_db_conn: psycopg2.extensions.connection,
                 own_hostname, own_port,
                 coordinator_hostname, coordinator_port,
                 timeout=10):
        super().__init__(log_db_conn, own_hostname, own_port)
        self.node_id = node_id
        self.logger.name = f"Participant#{self.node_id}"
        self.coordinator = comm.RemoteCallClient(coordinator_hostname, coordinator_port)
        self.timeout = timeout
        self.coordinator.timeout = self.timeout
        self.data_db_conn = data_db_conn
        self.data_db_conn.autocommit = True
        self.data_db_cur = self.data_db_conn.cursor()
        self.server.register_handler("INSERT", self.recv_insert)
        self.server.register_handler("PREPARE", self.recv_prepare)
        self.server.register_handler("COMMIT", self.recv_commit)
        self.server.register_handler("ABORT", self.recv_abort)
        self.current_trans_id = None

    async def setup(self):
        self.initialize_log()
        with self.data_db_conn:
            self.data_db_cur.execute("create table if not exists data"
                                     "(sensor_id varchar(255) not null primary key, "
                                     " measurement int not null, "
                                     " timestamp timestamp not null)")

    async def start(self):
        await super().start()
        await self.recover()

    async def stop(self):
        self.data_db_conn.rollback()  # for any transactions not yet commited through the protocol
        if self.coordinator:
            await self.coordinator.disconnect()
        await super().stop()

    async def begin_transaction(self, trans_id):
        """
        Begin a transaction. All following INSERTs will be associated with
        this new transaction.

        We need to cover the following cases:
        (a) This is already the current transaction; no need to begin anew.
        (b) The transaction to be begun is old (seen before).
            (i) The transaction is in a BEGUN state; we can still append to it.
            (ii) The transaction is PREPARED, COMMITTED or ABORTED; we must reject the
                 begin transaction, since we cannot append to it any more.
        (c) The transaction to be begun is new (not seen before).
            (i) The previous transaction was PREPARED, COMMITTED or ABORTED according to our logs;
                we can simply move on to the next one. (Regular protocol flow.)
            (ii) The previous transaction was BEGUN. Since, according to our logs, we have
                 not vouched to commit it (no PREPARED), we can simply abort it and start
                 the next one.
        """
        if self.current_trans_id == trans_id:
            # Already began this transaction.
            return True

        # Ensure this is a fresh transaction_id; if it belongs to a previous
        # transaction, it may not be completed.
        if trans_id in self.transactions:
            if self.transactions[trans_id] != "BEGUN":
                self.logger.error("Trying to append to transaction that is already completed or prepared.")
                return False

        # Complete or abort previous transaction.
        previous_id = self.current_trans_id
        if previous_id in self.transactions:
            if self.transactions[previous_id] == "BEGUN":
                # If the coordinator sends a request for a new transaction before
                # completing the previous one, the previous one will be aborted.
                self.logger.warning("Aborting previous transaction because a new one was begun.")
                self.do_abort(previous_id)
            # If the previous transaction is PREPARED, COMMITTED or ABORTED,
            # we can safely move on to the next transaction.

        # Update current transaction
        self.current_trans_id = trans_id
        self.transactions[trans_id] = "BEGUN"
        self.data_db_cur.execute("begin")
        self.logger.info(f"BEGAN new transaction {self.current_trans_id}.")
        return True

    async def recv_insert(self, data):
        """
        Insert new data; may start a new transaction. We reject INSERTS
        if the previous transaction was not successfully completed.
        """
        trans_id, sensor_id, measurement, timestamp = data

        if trans_id != self.current_trans_id:
            begun = await self.begin_transaction(trans_id)
            if not begun:
                # This INSERT refers to an old transaction which we already completed.
                # With a sane coordinator, this should not happen; it will only move to
                # the next transaction once the previous one is complete.
                return False

        timestamp = datetime.datetime.fromtimestamp(timestamp)
        self.data_db_cur.execute("insert into data (sensor_id, measurement, timestamp) "
                                 "values (%s, %s, %s)",
                                 (sensor_id, measurement, timestamp))
        self.logger.info(f"INSERT ({sensor_id}, {measurement}, {timestamp}) into database.")
        return True

    async def recv_prepare(self, trans_id):
        """
        Coordinator is ready to commit.
        (a) This is for our current transaction: We need to prepare actual commit work.
        (b) This is for a previous transaction (because coordinator died and forgot about it):
            We only need to inform coordinator about our decision.
        """

        if trans_id in self.transactions:
            # Case (b): This is a PREPARE request for a transaction we already completed previously
            status = self.transactions[trans_id]
            if status == "PREPARED":
                await self.coordinator.send_timeout("PREPARE", (self.node_id, trans_id, "COMMIT"))
                return True
            elif status == "ABORTED":
                await self.coordinator.send_timeout("PREPARE", (self.node_id, trans_id, "ABORT"))
                return True
            elif status == "COMMITTED":
                self.logger.error("Received invalid PREPARE message; coordinator already told us to commit.")
                return False
            assert status == "BEGUN"

        else:
            # This is a new trans_id; empty transaction, need to abort previous and create a new one
            begun = await self.begin_transaction(trans_id)
            assert begun

        # Case (a): Actual prepare work
        assert self.current_trans_id == trans_id
        assert self.transactions[trans_id] == "BEGUN"

        # `prepare transaction` stores transaction to disk.
        # After this, we can move on to other transactions, and still come
        # back to this and commit/rollback if needed.
        self.data_db_cur.execute("prepare transaction %s", (str(trans_id),))
        self.transactions[trans_id] = "PREPARED"
        self.write_log()

        await self.coordinator.send_timeout("PREPARE", (self.node_id, trans_id, "COMMIT"))
        self.logger.info(f"Sent PREPARE COMMIT {trans_id} to coordinator.")
        return True

    async def recv_commit(self, trans_id):
        status = self.transactions[trans_id]
        if status not in ["PREPARED", "COMMITTED"]:
            self.logger.error(f"Received illegal COMMIT; transaction {trans_id} has state {status}.")
            return False
        self.transactions[trans_id] = "COMMITTED"
        self.write_log()
        try:
            self.data_db_cur.execute("commit prepared %s", (str(trans_id),))
            self.logger.info(f"COMMITTED {trans_id} into database.")
        except psycopg2.errors.UndefinedObject:
            self.logger.warning(f"Received redundant COMMIT; have already committed this transaction {trans_id}.")
            # Already committed the transaction
            pass
        ack_done = await self.coordinator.send_timeout("DONE", (self.node_id, trans_id))
        self.logger.info(f"Sent DONE to coordinator.")
        #if ack_done:
        #    del self.transactions[trans_id]
        #    self.logger.info("DONE acknowledged. Removed transaction from log.")
        return True

    async def recv_abort(self, trans_id):
        if (trans_id not in self.transactions or
                self.transactions[trans_id] not in ["PREPARED", "ABORTED"]):
            self.logger.error(f"Received illegal ABORT for transaction {trans_id}.")
            return False
        self.do_abort(trans_id)
        self.write_log()
        ack_done = await self.coordinator.send_timeout("DONE", (self.node_id, trans_id))
        self.logger.info(f"Sent DONE to coordinator.")
        #if ack_done:
        #    del self.transactions[trans_id]
        #    self.logger.info("DONE acknowledged. Removed transaction from log.")
        return True

    def do_abort(self, trans_id):
        self.transactions[trans_id] = "ABORTED"
        self.write_log()
        if trans_id == self.current_trans_id:
            try:
                self.data_db_cur.execute("rollback prepared %s", (str(trans_id),))
                self.logger.info(f"ABORTED {trans_id} in database.")
            except psycopg2.errors.UndefinedObject:
                self.logger.warning(f"Received redundant ABORT for transaction {trans_id}.")
                # Already completed the transaction
                pass

    async def recover(self):
        """
        Recover from a possible previous crash by reading our logs and setting the state
        accordingly, then sending appropriate messages to coordinator.
        """
        self.read_log()
        awaitables = []
        self.logger.info(f"Recovering. {len(self.transactions)} transactions read from log.")
        for trans_id, status in self.transactions.items():
            # In case we decided to commit/abort, but we died before letting the
            # coordinator know, we should send the prepare message again. If the
            # message has reached the coordinator already, it will just be ignored.
            if status == "PREPARED":
                awaitables.append(asyncio.create_task(self.recv_prepare(trans_id)))
            elif status == "COMMITTED":
                awaitables.append(asyncio.create_task(self.recv_commit(trans_id)))
            elif status == "ABORTED":
                awaitables.append(asyncio.create_task(self.recv_abort(trans_id)))
        for awaitable in awaitables:
            await awaitable
