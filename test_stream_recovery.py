"""Offline reconnect checks using scripted sockets, never a provider request."""
import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK, InvalidStatus
from websockets.frames import Close
from websockets.http11 import Response

from stream_recovery import stream_with_reconnect

READY = {"type": "system", "event": "ready"}
QUOTE = {"type": "quote", "asset": "crypto", "symbol": "BTCUSD", "bid": "10", "ask": "11", "ts": 1}
TRADE = {"type": "trade", "asset": "crypto", "symbol": "BTCUSD", "price": "10.5", "size": "1", "ts": 2}


class Socket:
    def __init__(self, messages):
        self.messages = iter(messages)
        self.send = AsyncMock()

    async def recv(self):
        item = next(self.messages)
        if isinstance(item, BaseException):
            raise item
        return json.dumps(item)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


def upgrade_error(status, retry_after=None):
    headers = Headers()
    if retry_after is not None:
        headers["Retry-After"] = retry_after
    return InvalidStatus(Response(status, "Test response", headers))


class RecoveryChecks(unittest.IsolatedAsyncioTestCase):
    async def test_restart_preserves_messages_and_resubscribes(self):
        first = Socket([READY, QUOTE, ConnectionClosedError(Close(1012, "restart"), None)])
        second = Socket([READY, TRADE, QUOTE])
        with patch("stream_recovery.websockets.connect", side_effect=[first, second]) as connect, \
             patch("stream_recovery.asyncio.sleep", new_callable=AsyncMock) as sleep:
            result = await stream_with_reconnect("TEST_ONLY", max_messages=5)
        self.assertEqual(result["reconnect_attempts"], 1)
        self.assertEqual(len(result["events"]), 5)
        self.assertEqual(result["events"][1]["message"], QUOTE)
        self.assertEqual(connect.call_count, 2)
        sleep.assert_awaited_once_with(1)
        for socket in [first, second]:
            subscription = json.loads(socket.send.call_args.args[0])
            self.assertTrue(subscription["snapshot"])
            self.assertEqual(subscription["symbols"], ["BTCUSD"])

    async def test_503_honors_retry_after(self):
        with patch("stream_recovery.websockets.connect", side_effect=[upgrade_error(503, "2"), Socket([READY, QUOTE])]), \
             patch("stream_recovery.asyncio.sleep", new_callable=AsyncMock) as sleep:
            result = await stream_with_reconnect("TEST_ONLY", max_messages=2)
        self.assertEqual(result["reconnect_attempts"], 1)
        sleep.assert_awaited_once_with(2)

    async def test_network_retry_limit(self):
        with patch("stream_recovery.websockets.connect", side_effect=OSError("test-only disconnect")) as connect, \
             patch("stream_recovery.asyncio.sleep", new_callable=AsyncMock) as sleep:
            with self.assertRaisesRegex(RuntimeError, "reconnect limit"):
                await stream_with_reconnect("TEST_ONLY")
        self.assertEqual(connect.call_count, 4)
        self.assertEqual([c.args[0] for c in sleep.await_args_list], [1, 2, 4])

    async def test_auth_and_connection_limit_fail_without_retry(self):
        for status in [401, 403, 429]:
            with self.subTest(status=status), \
                 patch("stream_recovery.websockets.connect", side_effect=upgrade_error(status)) as connect, \
                 patch("stream_recovery.asyncio.sleep", new_callable=AsyncMock) as sleep:
                with self.assertRaises(InvalidStatus):
                    await stream_with_reconnect("TEST_ONLY")
                self.assertEqual(connect.call_count, 1)
                sleep.assert_not_awaited()

    async def test_subscription_error_fails_without_retry(self):
        with patch("stream_recovery.websockets.connect", return_value=Socket([READY, {"type": "error", "code": "INVALID_SYMBOL"}])) as connect:
            with self.assertRaisesRegex(RuntimeError, "INVALID_SYMBOL"):
                await stream_with_reconnect("TEST_ONLY")
        self.assertEqual(connect.call_count, 1)

    async def test_normal_close_and_idle_timeout_finish_capture(self):
        for ending in [ConnectionClosedOK(Close(1000, "done"), Close(1000, "done"), True), asyncio.TimeoutError()]:
            with patch("stream_recovery.websockets.connect", return_value=Socket([READY, QUOTE, ending])) as connect:
                result = await stream_with_reconnect("TEST_ONLY")
            self.assertEqual(len(result["events"]), 2)
            self.assertEqual(connect.call_count, 1)

    async def test_policy_close_is_not_retried(self):
        socket = Socket([READY, ConnectionClosedError(Close(1008, "policy"), None)])
        with patch("stream_recovery.websockets.connect", return_value=socket) as connect:
            with self.assertRaises(ConnectionClosedError):
                await stream_with_reconnect("TEST_ONLY")
        self.assertEqual(connect.call_count, 1)

    async def test_capture_budget_limits_retries(self):
        socket = Socket([READY, QUOTE, ConnectionClosedError(None, None)])
        with patch("stream_recovery.websockets.connect", return_value=socket) as connect, \
             patch("stream_recovery.asyncio.sleep", new_callable=AsyncMock) as sleep:
            result = await stream_with_reconnect("TEST_ONLY", seconds=.01)
        self.assertEqual(len(result["events"]), 2)
        self.assertEqual(connect.call_count, 1)
        self.assertLessEqual(sleep.await_args.args[0], .01)

    async def test_invalid_limits(self):
        with self.assertRaises(ValueError):
            await stream_with_reconnect("TEST_ONLY", seconds=0)


if __name__ == "__main__":
    unittest.main()
