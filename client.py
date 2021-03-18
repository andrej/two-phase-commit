import asyncio
import argparse
import comm
import util
import time
import sys
import psycopg2


async def send_execute_request(coordinator, node_id, query, args=tuple()):
    kind = "EXECUTE"
    data = {"node_id": node_id,
            "query": query,
            "args": args}
    return await coordinator.send(kind, data)


async def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--coordinator", type=util.hostname_port_type, required=True)
    argparser.add_argument("--demo", choices=demo_tables.keys())
    argparser.add_argument("--n-nodes", type=int)
    argparser.add_argument("--data-db")
    args = argparser.parse_args()
    coordinator_hostname, coordinator_port = args.coordinator

    is_demo = False
    if args.demo:
        if not args.n_nodes:
            print("For demo mode, the total number of participant nodes must be given with --n-nodes.")
        if not args.data_db:
            print("For demo mode, a source database must be given with --data-db.")
        if not args.n_nodes or not args.data_db:
            return 1
        is_demo = True
    else:
        if args.n_nodes or args.data_db:
            print("Arguments --n-nodes and --data-db may only be given in demo mode.")

    coordinator = comm.RemoteCallClient(coordinator_hostname, coordinator_port)
    await coordinator.connect()
    print("Connected to coordinator at {}:{}.".format(coordinator_hostname, coordinator_port))

    if not is_demo:
        await interactive_ui(coordinator)
    else:
        await demo_ui(coordinator, args.data_db, args.n_nodes, args.demo)


async def interactive_ui(coordinator):
    keep_going = True
    while keep_going:
        print("QUERY --------------------")
        sys.stdout.write("Execute query:  ")
        query = input()
        sys.stdout.write("On this node #: ")
        node_id = input()
        success = await send_execute_request(coordinator, node_id, query)
        if not success:
            print("Error: EXECUTE was not successful. (Server may be blocked at previous transaction.)")
        sys.stdout.write("Send another query request? (y/n) ")
        keep_going = (input().lower() == "y")


async def demo_ui(coordinator, data_db, n_nodes, table):
    columns = demo_tables[table]
    create_table_query = demo_create_tables[table]
    print("Initializing tables on all participant nodes.")
    for i in range(n_nodes):
        print(f"Sending CREATE TABLE {table} query to node {i}.")
        success = await send_execute_request(coordinator, i, create_table_query)
        if not success:
            print(f"Failed.")
            return

    source_db = psycopg2.connect(data_db)
    source_cur = source_db.cursor()
    source_cur.execute("select * from " + table)
    try:
        for row in source_cur:
            timestamp_i = columns.index("timestamp")
            row = list(row)
            row[timestamp_i] = row[timestamp_i].strftime('%Y-%m-%d %H:%M:%S')
            node_id = hash(row[0]) % n_nodes
            query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({', '.join(['%s']*len(columns))})"
            print(f"Sending INSERT {row[0]} request to node {node_id}.")
            success = await send_execute_request(coordinator, node_id, query, tuple(row))
            if not success:
                print("Failed.")
            print("Sleeping for one second.")
            time.sleep(1)
    except KeyboardInterrupt:
        print("Killed.")


demo_tables = {
    "thermometerobservation": ("id", "temperature", "timestamp", "sensor_id"),
    "wemoobservation": ("id", "currentmilliwatts", "ontodayseconds", "timestamp", "sensor_id"),
    "wifiapobservation": ("id", "clientid", "timestamp", "sensor_id")
}


demo_create_tables = {
    "thermometerobservation":
        """CREATE TABLE IF NOT EXISTS ThermometerObservation (
          id varchar(255) NOT NULL,
          temperature integer DEFAULT NULL,
          timeStamp timestamp NOT NULL,
          sensor_id varchar(255) DEFAULT NULL,
          PRIMARY KEY (id)
        )""",
    "wemoobservation":
        """CREATE TABLE IF NOT EXISTS wemoobservation (
          id varchar(255) NOT NULL,
          currentMilliWatts integer DEFAULT NULL,
          onTodaySeconds integer DEFAULT NULL,
          timeStamp timestamp NOT NULL,
          sensor_id varchar(255) DEFAULT NULL,
          PRIMARY KEY (id)
        )""",
    "wifiapobservation":
        """CREATE TABLE IF NOT EXISTS WiFiAPObservation (
          id varchar(255) NOT NULL,
          clientId varchar(255) DEFAULT NULL,
          timeStamp timestamp NOT NULL,
          sensor_id varchar(255) DEFAULT NULL,
          PRIMARY KEY (id)
        )"""
}


if __name__ == "__main__":
    asyncio.run(main())
