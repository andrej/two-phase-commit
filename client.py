import asyncio
import argparse
import comm
import util
import time
import sys


async def send_insert_request(coordinator, sensor_id, measurement, timestamp):
    kind = "INSERT"
    data = {"sensor_id": sensor_id,
            "measurement": measurement,
            "timestamp": timestamp}
    await coordinator.send(kind, data)


async def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--coordinator", type=util.hostname_port_type)
    args = argparser.parse_args()
    coordinator_hostname, coordinator_port = args.coordinator

    coordinator = None
    try:
        coordinator = comm.RemoteCallClient(coordinator_hostname, coordinator_port)
        await coordinator.connect()
        print("Connected to coordinator at {}:{}.".format(coordinator_hostname, coordinator_port))
        keep_going = True
        while keep_going:
            print("Send another insertion request? (y/n)")
            keep_going = (input().lower() == "y")
            if not keep_going:
                break
            sys.stdout.write("Sensor_ID: ")
            sensor_id = input()
            sys.stdout.write("Measurement: ")
            measurement = input()
            now = time.time()
            sys.stdout.write("Timestamp (default now: {}): ".format(now))
            timestamp = input() or now
            await send_insert_request(coordinator, sensor_id, measurement, timestamp)

    finally:
        if coordinator:
            await coordinator.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
