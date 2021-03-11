"""
Convenience wrapper for database interactions; specifically, enables
programmatic setup of new empty database clusters, and starting a new local
PostgreSQL server.

Author: André Rösti
"""

import psycopg2
import psycopg2.sql
import subprocess


class DatabaseServerError(BaseException):
    pass


class DatabaseServer:

    pg_ctl_bin = "pg_ctl"
    """Path to the pg_ctl binary."""

    def __init__(self, data_dir=None, port=None):
        self.data_dir = data_dir
        self.port = port

    def make(self):
        """
        Initialize a new empty database in the given directory.
        """
        if not self.data_dir:
            raise NotADirectoryError()
        res = subprocess.call([self.pg_ctl_bin, "initdb", "-D", self.data_dir])
        if res != 0:
            raise DatabaseServerError("Non-zero exit flag from initidb: ".format(res))

    def start(self):
        args = []
        if self.port:
            args += ["env", "PGPORT={}".format(int(self.port))]
        args += [self.pg_ctl_bin, "start"]
        if self.data_dir:
            args += ["-D", self.data_dir]
        res = subprocess.call(args)
        if res != 0:
            raise DatabaseServerError("Non-zero exit flag from pg_ctl start: {}".format(res))

    def stop(self):
        args = []
        args += [self.pg_ctl_bin, "stop"]
        if self.data_dir:
            args += ["-D", self.data_dir]
        res = subprocess.call(args)
        if res != 0:
            raise DatabaseServerError("Non-zero exit flag from pg_ctl stop: {}".format(res))


    def connect(self, database):
        """
        Return a DB API 2.0 connection object to this server.
        """
        args = {"port": self.port,
                "dbname": database}
        arg_str = build_connect_arg_str(args)
        conn = psycopg2.connect(arg_str)
        return conn

    def create_db_and_connect(self, dbname):
        """
        Convenience method that combines all the following steps:
        1. Connect to "postgres" database
        2. Create new database with dbname
        3. Connect to new database
        4. return this connection
        :param dbname:
        :return:
        """
        db_conn = self.connect("postgres")
        db_conn.autocommit = True  # cannot run create database in transaction block
        db_cur = db_conn.cursor()
        db_cur.execute(psycopg2.sql.SQL("create database {}").format(psycopg2.sql.Identifier(dbname)))
        db_conn.close()
        db_conn = self.connect(dbname)
        return db_conn


def build_connect_arg_str(args):
    """
    Builds libpq connection string.
    See https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
    :return:
    """
    return " ".join("{}={}".format(k, str(v).replace("'", "\\'"))
                    for k, v in args.items()
                    if v)
