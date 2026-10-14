"""Bounded WebSocket reconnection example; no replay of missed trades."""
import asyncio
import json
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote

import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatus

from market_monitor import utc_now


def retry_delay(header, fallback):
    """Honor Retry-After seconds or an HTTP date; refuse a long pause."""
    if header is None:
        return fallback
    try:
        delay = float(header)
    except ValueError:
        delay = (parsedate_to_datetime(header) - datetime.now(timezone.utc)).total_seconds()
    if delay > 60:
        raise RuntimeError("Server requested a long pause; try again later.")
    return max(0, delay)


async def stream_with_reconnect(api_key, seconds=15, max_messages=80, max_retries=3):
    """Keep collected frames across reconnects, with one shared capture window.

    Retry transient upgrade failures (502/503/504), network failures and
    selected disconnects. Invalid credentials, entitlements and subscription
    errors fail immediately. A resubscription asks for current snapshots,
    not a replay or guarantee of every trade missed while disconnected.
    """
    if seconds <= 0 or max_messages < 1 or max_retries < 0:
        raise ValueError("Use positive capture limits and nonnegative retries.")
    url = "wss://stream.tickerlayer.com/?apiKey=" + quote(api_key, safe="")
    events, deadline = [], None
    started, reconnects = time.perf_counter(), 0

    def result():
        return {"duration_seconds": round(time.perf_counter() - started, 2),
                "events": events, "reconnect_attempts": reconnects}

    def remaining(default):
        return default if deadline is None else max(.001, min(default, deadline - time.perf_counter()))

    for attempt in range(max_retries + 1):
        if deadline is not None and time.perf_counter() >= deadline:
            return result()
        reconnects = attempt
        delay = 2 ** attempt
        try:
            async with websockets.connect(url, open_timeout=remaining(15), close_timeout=5) as ws:
                ready = json.loads(await asyncio.wait_for(ws.recv(), timeout=remaining(8)))
                if ready.get("type") != "system" or ready.get("event") != "ready":
                    raise RuntimeError("Expected a ready frame before subscribing.")
                if deadline is None:
                    deadline = time.perf_counter() + seconds
                events.append({"received_at_utc": utc_now(), "message": ready})
                await ws.send(json.dumps({
                    "action": "subscribe", "channels": ["crypto.quotes", "crypto.trades"],
                    "symbols": ["BTCUSD"], "snapshot": True,
                }))
                while len(events) < max_messages and time.perf_counter() < deadline:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=remaining(seconds))
                    except asyncio.TimeoutError:
                        return result()  # An idle capture ending is not a connection failure.
                    message = json.loads(raw)
                    if message.get("type") == "error":
                        raise RuntimeError("Subscription failed: " + str(message.get("code", "unknown")))
                    events.append({"received_at_utc": utc_now(), "message": message})
                return result()
        except InvalidStatus as exc:
            if exc.response.status_code not in {502, 503, 504}:
                raise
            delay = retry_delay(exc.response.headers.get("Retry-After"), delay)
        except ConnectionClosed as exc:
            code = exc.rcvd.code if exc.rcvd else None
            if code in {1000, 1001}:
                return result()
            if code not in {None, 1006, 1011, 1012, 1013, 4008}:
                raise
        except (OSError, asyncio.TimeoutError):
            pass
        if attempt == max_retries:
            raise RuntimeError("WebSocket reconnect limit reached.") from None
        if deadline is not None:
            budget = deadline - time.perf_counter()
            if budget <= delay:
                if budget > 0:
                    await asyncio.sleep(budget)
                return result()
        await asyncio.sleep(delay)
    return result()
