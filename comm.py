"""
Implements a very simple "remote procedure call" protocol. Servers respond to requests
formatted as JSON of the following format:

    {"kind": "callback_name",
     "data": arbitrary data or None}

The server then respons with JSON-serialized return value of the remote procedure call.

You can add new remote procedure calls using register_handler, then server requests by
spinning up a server using RemoteCallServer.start().

You can send requests to call these remote procedures using RemoteCallClient.send(),
after establishing a connection to a server using RemoteCallClient.connect().

Author: André Rösti
"""
import typing
import asyncio
import json
import logging


class RemoteCallUtils:
    """
    Helper utilities for RemoteCallServer and RemoteCallClient, mainly for
    logging and encoding/decoding (serializing/deserializing) messages.
    """

    def __init__(self, server_host, server_port):
        self.msg_separator = b"\0"
        self.encoding = "utf-8"
        self.server_host = server_host
        self.server_port = server_port
        self.logger: logging.Logger = logging.getLogger(f"{type(self).__name__}({self.server_host}:{self.server_port})")
        logging.basicConfig(level=logging.INFO)

    def encode(self, pyobj):
        return json.dumps(pyobj).encode(self.encoding) + self.msg_separator

    def decode(self, raw: bytes):
        if raw.endswith(self.msg_separator):
            raw = raw[:-len(self.msg_separator)]
        return json.loads(raw.decode(self.encoding))

    def encode_message(self, kind, data):
        return self.encode({"kind": kind, "data": data})

    def decode_message(self, raw):
        deserialized = self.decode(raw)
        if "kind" not in deserialized:
            raise KeyError("Request invalid: No 'kind' of remote callback specified in data.")
        if "data" not in deserialized:
            raise KeyError("Request invalid: No 'data' given.")
        kind = deserialized["kind"]
        data = deserialized["data"]
        return kind, data


class RemoteCallClient(RemoteCallUtils):
    """
    Client implementation; initialize using correct hostname/port, then
    "connect()", then send calls using "send()".
    """

    def __init__(self, server_host, server_port):
        super().__init__(server_host, server_port)
        self.reader: typing.Optional[asyncio.streams.StreamReader] = None
        self.writer: typing.Optional[asyncio.streams.StreamWriter] = None

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.server_host, self.server_port)

    async def disconnect(self):
        self.writer.write_eof()
        await self.writer.drain()
        self.writer.close()
        await self.writer.wait_closed()

    async def send(self, kind, data=None):
        if not self.writer:
            raise RuntimeError("Need to establish connection with server through connect() first.")
        self.logger.info(f"Sending remote call message to {self.server_host}:{self.server_port} of kind '{kind}'.")
        serialized = self.encode_message(kind, data)
        self.writer.write(serialized)
        await self.writer.drain()
        try:
            raw_ret = await self.reader.readuntil(self.msg_separator)
            self.logger.info(f"Server replied.")
            ret = self.decode(raw_ret)
            return ret
        except (asyncio.streams.IncompleteReadError, json.JSONDecodeError, ) as e:
            self.logger.error(f"Unexpected response from server.")
            self.logger.error(str(e))
            return None


class RemoteCallServer(RemoteCallUtils):
    """
    Serve remote procedure call requests sent by one or multiple RemoteCallClients.
    The procedure calls can be registered using "register_handler". They must be
    coroutines (async keyword). All coroutines exposed using "register_handler"
    receive one argument, "data" of the deserialized data sent with any remote
    requests.
    """

    def __init__(self, server_host, server_port):
        super().__init__(server_host, server_port)
        self.handlers = {}
        self.server: typing.Optional[asyncio.AbstractServer] = None

    def register_handler(self, kind: str, handler_cb: typing.Callable):
        if kind in self.handlers:
            raise KeyError(f"A handler for message kind {kind} already exists.")
        self.handlers[kind] = handler_cb

    def deregister_handler(self, kind: str):
        if kind not in self.handlers:
            raise KeyError(f"No handler for message kind {kind} registered.")
        del self.handlers[kind]

    async def start(self):
        self.server = await asyncio.start_server(self.handle_new_connection,
                                                 self.server_host,
                                                 self.server_port)

    async def stop(self):
        self.logger.info("Stopping server.")
        self.server.close()
        await self.server.wait_closed()

    async def loop(self):
        if not self.server:
            raise RuntimeError("Must start the server first using start().")
        async with self.server:
            await self.server.start_serving()

    async def handle_new_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        For each connection, we handle every request until EOF encountered.
        """
        while not reader.at_eof():
            await self.handle_request(reader, writer)

    async def handle_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            data_bytes: bytes = await reader.readuntil(self.msg_separator)
        except asyncio.streams.IncompleteReadError:
            return
        self.logger.info("Got request!")
        try:
            kind, data = self.decode_message(data_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError, KeyError) as e:
            self.logger.error("Got invalid request.")
            self.logger.error(str(e))
            return
        if kind not in self.handlers:
            self.logger.error(f"No handler registered for '{kind}'.")
            return
        return_value = await self.handlers[kind](data)
        self.logger.error(f"Callback for {kind} returned with exit value {return_value}.")
        writer.write(self.encode(return_value))
        await writer.drain()
