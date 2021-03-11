import unittest
import asyncio
import comm


class CommunicationTests(unittest.TestCase):

    async def async_set_up(self):
        # Start a server
        self.server = comm.RemoteCallServer("localhost", 12347)
        self.client = comm.RemoteCallClient("localhost", 12347)
        startup_server_task = asyncio.create_task(self.server.start())
        await startup_server_task
        startup_client_task = asyncio.create_task(self.client.connect())
        await startup_client_task
        self.server_task = asyncio.create_task(self.server.loop())

    async def async_tear_down(self):
        # Shut down server
        close_client_task = asyncio.create_task(self.client.disconnect())
        close_server_task = asyncio.create_task(self.server.stop())
        await close_client_task
        await self.server_task
        await close_server_task

    def async_wrap(self, test_case):
        async def test():
            await self.async_set_up()
            await test_case()
            await self.async_tear_down()
        asyncio.run(test())

    def test_send(self):
        self.async_wrap(self.async_test_send)

    async def async_test_send(self):
        async def func_a_cb(dat):
            return 42
        func_a_dat = "Hello, World."

        async def func_b_cb(dat):
            return dat
        func_b_dat = "{'looks like': 'Python'}"

        async def func_c_cb(dat):
            return len(dat["hi"]) + dat["foo"]
        func_c_dat = {"hi": "yo", "foo": 42}

        tests = [("func_a", func_a_cb, func_a_dat, 42),
                 ("func_b", func_b_cb, func_b_dat, func_b_dat),
                 ("func_c", func_c_cb, func_c_dat, len(func_c_dat["hi"]) + func_c_dat["foo"])]

        for kind, cb, data, ret in tests:
            with self.subTest(kind):
                n_calls = 0
                async def func_cb_wrapper(got_data):
                    nonlocal n_calls
                    n_calls += 1
                    self.assertEqual(data, got_data)
                    return await cb(got_data)
                self.server.register_handler(kind, func_cb_wrapper)
                ret_a = await self.client.send(kind, data)
                self.assertEqual(ret_a, ret)
                self.assertEqual(n_calls, 1)

    def test_send_bogus_message(self):
        self.async_wrap(self.async_test_send_bogus_message)

    async def async_test_send_bogus_message(self):
        inputs = ["asdf".encode(self.client.encoding),
                  "äé?`a".encode("cp1252"),
                  "{'invalid: json".encode(self.client.encoding),
                  "{'no':'kind or data'}".encode(self.client.encoding)]
        for inp in inputs:
            with self.subTest(inp):
                self.client.writer.write(inp + self.client.msg_separator)
                # First, confuse server with bogus messages
                await self.client.writer.drain()
                # Then, test that all the normal messages still work
                await self.async_test_send()
                # Reset everything
                await self.async_tear_down()
                await self.async_set_up()
