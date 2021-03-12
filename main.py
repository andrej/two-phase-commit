import argparse
import asyncio
import concurrent.futures
import tempfile
import shutil
import logging
import psycopg2
import node
import db
import util


def connect_or_create_db(arg, name):
    # If no command line argument is supplied, create the database
    server = None
    db_conn = None
    if not arg:
        port = util.find_free_port()
        server = db.DatabaseServer(tempfile.mkdtemp(), port)
        server.make()
        server.start()
        db_conn = server.create_db_and_connect(name)
    else:
        db_conn = psycopg2.connect(arg)
    return server, db_conn


async def main():

    logging.basicConfig(level=logging.ERROR)

    # Argument preprocessing
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--host", type=util.hostname_port_type, required=True,
                           help="Hostname and port where this agent will be listening for communication,"
                                "e.g. localhost:12345.")
    argparser.add_argument("--coordinator", type=util.hostname_port_type)
    argparser.add_argument("--node-id", type=int, help="Node ID for participants. MUST start at 0 and be "
                                                       "consecutive increasing numbers for all participants.")
    argparser.add_argument("--participant", type=util.hostname_port_type, action="append")
    argparser.add_argument("--data-db", type=str, help="Connection URI for data database; if none given, "
                                                       "a new database cluster will be started.")
    argparser.add_argument("--log-db", type=str, help="As for --data-db, but for log database.")
    argparser.add_argument("--timeout", type=int, default=10,
                           help="Nodes wait for replies from participants for this long.")
    argparser.add_argument("--batch-size", type=int, help="After N insert requests, transaction is committed.")
    argparser.add_argument("--verbose", action="store_true", default=False, help="If flag is present, more log mesages are printed.")
    args = argparser.parse_args()
    if args.coordinator and args.participant:
        print("Cannot specify both --coordinator and --participant. To start a "
              "coordinator node, only supply (possibly multiple) --particpant, "
              "to start a participant node, supply a single --coordinator.")
        return 1
    # If not coordinator is given, we implicitly assume that this should be the
    # coordinator node.
    is_coordinator = args.coordinator is None
    # Hostname and port of own communication server; we are reachable here
    own_hostname, own_port = args.host
    # Participant nodes must be consecutively numbered
    if not is_coordinator and args.node_id is None:
        print("All participant nodes must be supplied with a consecutive --node-id "
              "starting from zero.")
        return 1

    log_server, log_db = None, None
    data_server, data_db = None, None

    try:

        # Connect to databases
        log_server, log_db = connect_or_create_db(args.log_db, "log")
        if not is_coordinator:
            data_server, data_db = connect_or_create_db(args.data_db, "data")
        if log_server:
            print("Started log database cluster on port {}, using directory {}.".format(log_server.port,
                                                                                        log_server.data_dir))
        if data_server:
            print("Started data database cluster on port {}, using directory {}.".format(data_server.port,
                                                                                         data_server.data_dir))

        # Start communication servers
        if is_coordinator:
            the_node = node.TwoPhaseCommitCoordinator(log_db,
                                                      own_hostname,
                                                      own_port,
                                                      args.participant,
                                                      timeout=args.timeout)
            if args.batch_size:
                the_node.batch_size = args.batch_size
        else:
            coordinator_hostname, coordinator_port = args.coordinator
            the_node = node.TwoPhaseCommitParticipant(args.node_id,
                                                      data_db,
                                                      log_db,
                                                      own_hostname,
                                                      own_port,
                                                      coordinator_hostname,
                                                      coordinator_port,
                                                      timeout=args.timeout)
        the_node.logger.setLevel(logging.INFO)
        if not args.verbose:
            the_node.server.logger.setLevel(logging.ERROR)
            if is_coordinator:
                for participant in the_node.participants:
                    participant.logger.setLevel(logging.ERROR)
            else:
                the_node.coordinator.logger.setLevel(logging.ERROR)
        the_node.setup()
        try:
            await the_node.start()
            print("{} node listening on {}:{}.".format("Coordinator" if is_coordinator else "Participant",
                                                       own_hostname, own_port))

            try:
                while True:
                    await asyncio.sleep(0)
            except (KeyboardInterrupt, concurrent.futures.CancelledError):
                print("Killed.")

        finally:
            await the_node.stop()
            print("Shut down communication node.")

    finally:

        # Close database connections
        # If a database cluster/server was started, shut them down
        if log_db:
            log_db.close()
            print("Closed log database connection.")
        if data_db:
            data_db.close()
            print("Closed data database connection.")
        if log_server:
            log_server.stop()
            shutil.rmtree(log_server.data_dir)
            print("Stopped log database server and deleted cluster.")
        if data_server:
            data_server.stop()
            shutil.rmtree(data_server.data_dir)
            print("Stopped data database server and deleted cluster.")


if __name__ == "__main__":
    asyncio.run(main())
