import unittest
import tempfile
import shutil
import errno
import psycopg2
import psycopg2.extensions
import db


class DatabaseTestsBase(unittest.TestCase):
    """
    Base case for all database tests; contains setup of temporary directory
    and setup of a database server object.
    """

    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix="cs223_db_test")
        self.server: db.DatabaseServer = db.DatabaseServer(self.tempdir)

    def tearDown(self):
        try:
            shutil.rmtree(self.tempdir)
        except OSError as e:
            if e.errno != errno.ENOENT:  # directory does not exist
                raise


class DatabaseServerTestFundamentals(DatabaseTestsBase):

    def test_make_start_stop(self):
        ports = [None, 21345, 9879]
        with self.subTest("Test database creation"):
            self.server.make()
        for port in ports:
            # test starting
            with self.subTest("Test start"):
                self.server.port = port
                self.server.start()
            # ensure database empty
            with self.subTest("Connect to started server, ensure it is empty except for the default tables"):
                conn: psycopg2.extensions.connection = self.server.connect("postgres")
                try:
                    self.assertIsInstance(conn, psycopg2.extensions.connection)
                    cur: psycopg2.extensions.cursor = conn.cursor()
                    cur.execute("SELECT datname FROM pg_database")
                    results = cur.fetchall()
                    self.assertIn(("postgres",), results)
                    self.assertIn(("template0",), results)
                    self.assertIn(("template1",), results)
                finally:
                    conn.close()
            # test stopping
            with self.subTest("Test stop"):
                self.server.stop()


class DatabaseServerTests(DatabaseTestsBase):

    def setUp(self):
        super().setUp()
        self.server.port = 2345
        self.server.make()
        self.server.start()

    def tearDown(self):
        self.server.stop()
        super().tearDown()

    def test_create_cluster_db_and_connect(self):
        # Create database
        test_db_conn = self.server.create_db_and_connect("test_db")
        # Ensure database exists
        with self.subTest("database exists"):
            postgres_db_conn = self.server.connect("postgres")
            postgres_db_cur = postgres_db_conn.cursor()
            postgres_db_cur.execute("select datname from pg_database")
            rows = postgres_db_cur.fetchall()
            self.assertIn(("test_db",), rows)
        # Execute some query on database to test it
        with self.subTest("database connection usable"):
            test_db_cur = test_db_conn.cursor()
            with test_db_conn:
                test_db_cur.execute("create table foo (bar int primary key, baz varchar(20))")
                test_db_cur.execute("insert into foo (bar, baz) values (2, 'foobar')")
                test_db_cur.execute("insert into foo (bar, baz) values (1, 'qwertz')")
            with test_db_conn:
                test_db_cur.execute("select * from foo")
            rows = test_db_cur.fetchall()
            self.assertIn((2, 'foobar'), rows)
            self.assertIn((1, 'qwertz'), rows)

