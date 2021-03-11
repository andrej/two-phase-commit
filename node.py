import typing
import asyncio
import time
import datetime
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
        #with self.log_db_conn:  # Context manager to make this an atomic transaction
        #    self.log_db_cur.execute("delete from transaction_log")  # clear out everything
        #    for trans_id, status in self.transactions:
        #        self.log_db_cur.execute("insert into transaction_log (id, status) values(%s, %s)",
        #                                 (trans_id, status))
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

    def __init__(self, log_db_conn, own_hostname, own_port, participants):
        super().__init__(log_db_conn, own_hostname, own_port)
        self.participants: typing.List[comm.RemoteCallClient] = []
        for hostname, port in participants:
            self.participants.append(comm.RemoteCallClient(hostname, port))
        self.timeout = 10
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

    async def send_all(self, kind, data):
        """
        Send a message to all participants.
        """
        sends = []
        for participant in self.participants:
            sends.append(asyncio.create_task(participant.send(kind, data)))
        for send in sends:
            await asyncio.wait_for(send, self.timeout)

    async def connect_to_participants(self):
        client_tasks = []
        for participant in self.participants:
            client_task = asyncio.create_task(participant.connect())
            client_tasks.append(client_task)
        for client_task in client_tasks:
            await client_task
        await self.recover()

    async def recv_insert(self, data):
        sensor_id = data["sensor_id"]
        measurement = data["measurement"]
        timestamp = datetime.datetime.fromtimestamp(data["timestamp"])
        await self.insert(sensor_id, measurement, timestamp)

    async def insert(self, sensor_id, measurement, timestamp: datetime.datetime):
        if self.insert_counter == 0:
            await self.begin_transaction()
        node_i = (hash(sensor_id) + hash(timestamp)) % len(self.participants)
        participant = self.participants[node_i]
        timestamp = time.mktime(timestamp.timetuple())
        await participant.send("INSERT", (self.current_trans_id, sensor_id, measurement, timestamp))
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

    async def recv_prepare(self, data):
        node_id, trans_id, action = data
        if trans_id != self.current_trans_id:
            # We've moved on past this transaction but the node crashed at old one
            # TODO
            await self.abort_transaction(trans_id)
            return
        self.prepared_to_commit[trans_id][node_id] = (action == "COMMIT")
        if all(x is not None for x in self.prepared_to_commit):
            self.everyone_prepared_event.set()

    async def complete_transaction(self):
        transaction_id = self.current_trans_id
        # Send request-to-prepare messages
        # Write prepare to log (forced)
        self.transactions[transaction_id] = "PREPARED"
        self.write_log()
        await self.prepare_transaction(transaction_id)
        await asyncio.wait_for(self.everyone_done_event.wait(), self.timeout)
        if self.everyone_done_event.is_set() and transaction_id == self.current_trans_id:
            # Can safely delete from log since everyone is done
            del self.transactions[transaction_id]

    async def prepare_transaction(self, trans_id):
        await self.send_all("PREPARE", trans_id)
        await asyncio.wait_for(self.everyone_prepared_event.wait(), self.timeout)
        if self.everyone_prepared_event.is_set() and all(self.prepared_to_commit[self.current_trans_id]):
            self.transactions[trans_id] = "COMMITED"
            self.write_log()
            await self.commit_transaction(trans_id)
        else:
            self.transactions[trans_id] = "ABORTED"
            self.write_log()
            await self.abort_transaction(trans_id)

    async def commit_transaction(self, trans_id):
        await asyncio.wait_for(self.send_all("COMMIT", trans_id), self.timeout)

    async def abort_transaction(self, trans_id):
        await asyncio.wait_for(self.send_all("ABORT", trans_id), self.timeout)

    async def recv_done(self, data):
        node_id, trans_id = data
        self.done[trans_id][node_id] = True
        if all(self.done[trans_id]) and trans_id == self.current_trans_id:
            self.everyone_done_event.set()

    async def recover(self):
        self.read_log()
        tasks = []
        for trans_id, state in self.transactions.items():
            task = None
            if state == "PREPARED":
                task = asyncio.create_task(self.prepare_transaction(trans_id))
            elif state == "COMITTED":
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
                 coordinator_hostname, coordinator_port):
        super().__init__(log_db_conn, own_hostname, own_port)
        self.node_id = node_id
        self.coordinator = comm.RemoteCallClient(coordinator_hostname, coordinator_port)
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

    async def connect_to_coordinator(self):
        await self.coordinator.connect()

    async def recv_begin(self, trans_id):
        self.current_trans_id = trans_id
        self.current_trans_id = self.current_trans_id
        self.transactions[trans_id] = "BEGUN"
        self.data_db_cur.execute("begin")
        return trans_id

    async def recv_insert(self, data):
        trans_id, sensor_id, measurement, timestamp = data
        timestamp = datetime.datetime.fromtimestamp(timestamp)
        self.data_db_cur.execute("insert into data (sensor_id, measurement, timestamp) "
                                 "values (%s, %s, %s)",
                                 (sensor_id, measurement, timestamp))

    async def recv_prepare(self, trans_id):
        status = "COMMIT" if trans_id in self.transactions else "ABORT"
        if status == "COMMIT":
            self.transactions[trans_id] = "PREPARED"
            self.write_log()
        else:
            # Can just abort transaction
            await self.recv_abort(trans_id)
        await self.coordinator.send("PREPARE", (self.node_id, trans_id, status))

    async def recv_commit(self, trans_id):
        self.transactions[trans_id] = "COMMITTED"
        self.write_log()
        #self.data_db_cur.execute("commit prepared %s", str(trans_id))
        self.data_db_conn.commit()
        await self.coordinator.send("DONE", (self.node_id, trans_id))

    async def recv_abort(self, trans_id):
        self.transactions[trans_id] = "ABORTED"
        self.write_log()
        #self.data_db_cur.execute("rollback prepared %s", str(trans_id))
        self.data_db_conn.rollback()
        await self.coordinator.send("DONE", (self.node_id, trans_id))

    async def recover(self):
        """
        Recover from a possible previous crash by reading our logs and setting the state
        accordingly, then sending appropriate messages to coordinator.
        """
        self.read_log()
        awaitables = []
        for trans_id, status in self.transactions:
            # In case we decided to commit/abort, but we died before letting the
            # coordinator know, we should send the prepare message again. If the
            # message has reached the coordinator already, it will just be ignored.
            if status == "PREPARED":
                self.recv_prepare(trans_id)
            elif status == "COMMITTED":
                self.recv_commit(trans_id)
            elif status == "ABORTED":
                self.recv_abort(trans_id)
        for awaitable in awaitables:
            await awaitable
