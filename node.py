import typing
import asyncio
import concurrent.futures
import time
import datetime
import logging
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
        await self.server.stop()

    def initialize_log(self):
        self.log_db_cur.execute("create table if not exists log "
                                "(transaction_id int not null primary key, "
                                " status varchar(20) not null)")
        self.log_db_conn.commit()

    def write_log(self):
        """
        Force write the current state in self.transactions to the database.
        Also deletes all completed transactions from the log, we do not need
        to keep track of them any more.
        """
        with self.log_db_conn:
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
        self.participants: typing.List[comm.RemoteCallClient] = []
        self.timeout = timeout
        for hostname, port in participants:
            participant = comm.RemoteCallClient(hostname, port)
            participant.timeout = self.timeout
            self.participants.append(participant)
        self.current_trans_id = None
        self.insert_counter = 0
        self.batch_size = 3
        self.prepared_to_commit: typing.Dict[int, typing.List[typing.Optional[bool]]] = {}
        self.done: typing.Dict[int, typing.List[typing.Optional[bool]]] = {}
        self.everyone_prepared_event = asyncio.Event()
        self.everyone_done_event = asyncio.Event()
        self.server.register_handler("PREPARE", self.recv_prepare)
        self.server.register_handler("DONE", self.recv_done)
        self.server.register_handler("INSERT", self.recv_insert)  # Client request

    def setup(self):
        self.initialize_log()

    async def start(self):
        await super().start()
        await self.recover()

    async def send_all(self, kind, data):
        """
        Send a message to all participants.
        """
        sends = []
        for participant in self.participants:
            sends.append(asyncio.create_task(participant.send_timeout(kind, data)))
        for send in sends:
            await send

    async def recv_insert(self, data):
        sensor_id = data["sensor_id"]
        measurement = data["measurement"]
        timestamp = datetime.datetime.fromtimestamp(data["timestamp"])
        self.logger.info(f"Received INSERT ({sensor_id}, {measurement}, {timestamp}) request from client.")
        await self.insert(sensor_id, measurement, timestamp)

    async def insert(self, sensor_id, measurement, timestamp: datetime.datetime):
        if self.insert_counter == 0:
            await self.begin_transaction()
        node_i = (hash(sensor_id) + hash(timestamp)) % len(self.participants)
        participant = self.participants[node_i]
        timestamp = time.mktime(timestamp.timetuple())
        await participant.send_timeout("INSERT", (self.current_trans_id, sensor_id, measurement, timestamp))
        self.logger.info(f"Sent INSERT ({sensor_id}, {measurement}, {timestamp}) to participant {node_i}.")
        if self.insert_counter == self.batch_size - 1:
            await self.complete_transaction()
        self.insert_counter += 1

    async def begin_transaction(self):
        if not self.current_trans_id:
            self.current_trans_id = max(self.transactions.keys()) if self.transactions.keys() else 0
        self.current_trans_id += 1
        self.everyone_prepared_event.clear()
        self.everyone_done_event.clear()
        self.prepared_to_commit[self.current_trans_id] = [None] * len(self.participants)
        self.done[self.current_trans_id] = [None] * len(self.participants)
        await self.send_all("BEGIN", self.current_trans_id)
        self.logger.info(f"Sent BEGIN to all participants.")

    async def recv_prepare(self, data):
        node_id, trans_id, action = data
        already_committed = (trans_id not in self.transactions or
                             self.transactions[trans_id] == "COMMITTED")
        already_aborted = (trans_id in self.transactions and
                           self.transactions[trans_id] == "ABORTED" or
                           not already_committed and trans_id != self.current_trans_id)
        if already_committed:
            self.logger.info(f"Received PREPARED from participant {node_id} for transaction that has "
                             f"already committed previously.")
            await self.participants[node_id].send_timeout("COMMIT", trans_id)
            return
        if already_aborted:
            self.logger.info(f"Received PREPARED from participant {node_id} for transaction that has "
                             f"already been aborted previously.")
            await self.participants[node_id].send_timeout("ABORT", trans_id)
            return
        self.prepared_to_commit[trans_id][node_id] = (action == "COMMIT")
        self.logger.info(f"Received PREPARED {action} from participant {node_id}.")
        if all(x is not None for x in self.prepared_to_commit):
            self.everyone_prepared_event.set()

    async def complete_transaction(self):
        self.insert_counter = 0
        transaction_id = self.current_trans_id
        # Send request-to-prepare messages
        # Write prepare to log (forced)
        self.transactions[transaction_id] = "PREPARED"
        self.write_log()
        await self.prepare_transaction(transaction_id)

    async def prepare_transaction(self, trans_id):
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

    async def commit_transaction(self, trans_id):
        await self.send_all("COMMIT", trans_id)
        self.logger.info(f"Sent COMMIT to all participants.")

    async def abort_transaction(self, trans_id):
        await self.send_all("ABORT", trans_id)
        self.logger.info(f"Sent ABORT to all participants.")

    async def recv_done(self, data):
        node_id, trans_id = data
        self.done[trans_id][node_id] = True
        self.logger.info(f"Received DONE from node {node_id}.")
        if all(self.done[trans_id]):
            self.logger.info(f"Everyone DONE. Removing transaction {trans_id}.")
            del self.transactions[trans_id]
            del self.done[trans_id]

    async def recover(self):
        self.read_log()
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
        self.server.register_handler("BEGIN", self.recv_begin)
        self.server.register_handler("INSERT", self.recv_insert)
        self.server.register_handler("PREPARE", self.recv_prepare)
        self.server.register_handler("COMMIT", self.recv_commit)
        self.server.register_handler("ABORT", self.recv_abort)
        self.current_trans_id = None

    def setup(self):
        self.initialize_log()
        with self.data_db_conn:
            self.data_db_cur.execute("create table if not exists data"
                                     "(sensor_id varchar(255) not null primary key, "
                                     " measurement int not null, "
                                     " timestamp timestamp not null)")

    async def start(self):
        await super().start()
        await self.recover()

    async def recv_begin(self, trans_id):
        self.current_trans_id = trans_id
        self.current_trans_id = self.current_trans_id
        self.transactions[trans_id] = "BEGUN"
        self.data_db_cur.execute("begin")
        self.logger.info(f"BEGAN new transaction {self.current_trans_id}.")
        return trans_id

    async def recv_insert(self, data):
        trans_id, sensor_id, measurement, timestamp = data
        if trans_id != self.current_trans_id:
            # Coordinator died before storing PREPARED, hence forgot about this
            # transaction and all inserts we've done; need to roll back.
            pass   # FIXME
        timestamp = datetime.datetime.fromtimestamp(timestamp)
        self.data_db_cur.execute("insert into data (sensor_id, measurement, timestamp) "
                                 "values (%s, %s, %s)",
                                 (sensor_id, measurement, timestamp))
        self.logger.info(f"INSERT ({sensor_id}, {measurement}, {timestamp}) into database.")

    async def recv_prepare(self, trans_id):
        do_commit = (trans_id in self.transactions and
                     self.transactions[trans_id] != "ABORTED")
        status = "COMMIT" if do_commit else "ABORT"
        if do_commit:
            self.transactions[trans_id] = "PREPARED"
            self.write_log()
        else:
            # Can just abort transaction
            await self.recv_abort(trans_id)
        await self.coordinator.send_timeout("PREPARE", (self.node_id, trans_id, status))
        self.logger.info(f"Sent PREPARE {status} {trans_id} to coordinator.")

    async def recv_commit(self, trans_id):
        self.transactions[trans_id] = "COMMITTED"
        self.write_log()
        self.data_db_conn.commit()
        self.logger.info(f"COMMITTED {trans_id} into database.")
        await self.coordinator.send_timeout("DONE", (self.node_id, trans_id))
        self.logger.info(f"Sent DONE to coordinator.")

    async def recv_abort(self, trans_id):
        self.transactions[trans_id] = "ABORTED"
        self.write_log()
        if trans_id == self.current_trans_id:
            self.data_db_conn.rollback()
            self.logger.info(f"ABORTED {trans_id} in database.")
        await self.coordinator.send_timeout("DONE", (self.node_id, trans_id))
        self.logger.info(f"Sent DONE to coordinator.")

    async def recover(self):
        """
        Recover from a possible previous crash by reading our logs and setting the state
        accordingly, then sending appropriate messages to coordinator.
        """
        self.read_log()
        awaitables = []
        self.logger.info(f"Recovering. {len(self.transactions)} transactions read from log.")
        for trans_id, status in self.transactions:
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
