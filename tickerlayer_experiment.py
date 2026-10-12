"""Reproducible TickerLayer REST, WebSocket, and MCP evaluation.

The API key is read only from TICKERLAYER_API_KEY and is never persisted.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
import websockets


REST_BASE = "https://api.tickerlayer.com"
WS_BASE = "wss://stream.tickerlayer.com/"
MCP_URL = "https://mcp.tickerlayer.com/mcp"
HERE = Path(__file__).resolve().parent
RESULTS_DIR = HERE / "results"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def trimmed(value: Any, limit: int = 1400) -> Any:
    """Keep evidence useful without saving large symbol directories."""
    if isinstance(value, dict):
        return {k: trimmed(v, limit) for k, v in value.items()}
    if isinstance(value, list):
        return [trimmed(v, limit) for v in value[:8]]
    if isinstance(value, str) and len(value) > limit:
        return value[:limit] + "..."
    return value


def timestamp_age_ms(payload: Any) -> float | None:
    if not isinstance(payload, dict):
        return None
    for field in ("timestamp", "ts", "last_timestamp"):
        value = payload.get(field)
        if isinstance(value, (int, float)):
            return round(time.time() * 1000 - value, 1)
    return None


def selected_headers(headers: requests.structures.CaseInsensitiveDict) -> dict[str, str]:
    allow = {
        "content-type",
        "cache-control",
        "etag",
        "x-ratelimit-limit",
        "x-ratelimit-remaining",
        "x-ratelimit-policy",
        "retry-after",
    }
    return {k.lower(): v for k, v in headers.items() if k.lower() in allow}


def body_metadata(body: Any) -> dict[str, Any]:
    if not isinstance(body, dict):
        return {}
    list_lengths = {key: len(value) for key, value in body.items() if isinstance(value, list)}
    return {"top_level_list_lengths": list_lengths} if list_lengths else {}


class RestRunner:
    def __init__(self, api_key: str) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {"x-api-key": api_key, "Accept": "application/json", "User-Agent": "independent-review/1.0"}
        )

    def get(self, label: str, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        started = time.perf_counter()
        response = self.session.get(REST_BASE + path, params=params, timeout=20)
        duration_ms = round((time.perf_counter() - started) * 1000, 1)
        try:
            body: Any = response.json()
        except ValueError:
            body = response.text
        return {
            "label": label,
            "method": "GET",
            "path": path,
            "status": response.status_code,
            "duration_ms": duration_ms,
            "headers": selected_headers(response.headers),
            "timestamp_age_ms": timestamp_age_ms(body),
            "body_metadata": body_metadata(body),
            "body": trimmed(body),
        }


def first_symbol(body: Any, fallback: str) -> str:
    if not isinstance(body, dict):
        return fallback
    rows = body.get("symbols") or body.get("data") or []
    if isinstance(rows, list) and rows:
        row = rows[0]
        if isinstance(row, str):
            return row
        if isinstance(row, dict) and isinstance(row.get("symbol"), str):
            return row["symbol"]
    return fallback


def quote_metrics(result: dict[str, Any]) -> dict[str, Any]:
    body = result.get("body")
    if not isinstance(body, dict):
        return {}
    bid, ask = body.get("bid"), body.get("ask")
    if isinstance(bid, (int, float)) and isinstance(ask, (int, float)):
        mid = (bid + ask) / 2
        return {
            "bid_le_ask": bid <= ask,
            "spread": round(ask - bid, 10),
            "spread_bps": round((ask - bid) / mid * 10_000, 4) if mid else None,
        }
    return {}


def run_rest(api_key: str) -> dict[str, Any]:
    runner = RestRunner(api_key)
    results: list[dict[str, Any]] = []

    discovery_specs = {
        "crypto": ("/crypto/symbols", "BTCUSD"),
        "forex": ("/forex/symbols", "EURUSD"),
        "stocks": ("/stocks/symbols", "US:AAPL"),
        "indices": ("/indices/symbols", "US500"),
        "etfs": ("/etfs/symbols", "US500ETF"),
        "commodities": ("/commodities/symbols", "XAUUSD"),
        "perpetuals": ("/perpetuals/symbols", "XAUUSDT"),
    }
    selected: dict[str, str] = {}

    for asset, (path, fallback) in discovery_specs.items():
        item = runner.get(f"discover_{asset}", path)
        results.append(item)
        selected[asset] = first_symbol(item["body"], fallback)

    # Prefer recognizable article examples when they are normally documented.
    selected.update({"crypto": "BTCUSD", "forex": "EURUSD", "stocks": "US:AAPL", "commodities": "XAUUSD"})

    for asset in ("crypto", "forex", "stocks", "indices", "etfs", "commodities", "perpetuals"):
        symbol = quote(selected[asset], safe=":")
        item = runner.get(f"quote_{asset}", f"/{asset}/quote/{symbol}")
        item["metrics"] = quote_metrics(item)
        results.append(item)

    results.append(runner.get("bond_us_10y", "/bond/snapshot/US:10Y"))
    results.append(runner.get("crypto_snapshot", "/crypto/snapshot/BTCUSD"))

    today = datetime.now(timezone.utc).date()
    start = today.replace(day=max(1, today.day - 6))
    history_path = f"/crypto/agg/BTCUSD/1/day/{start.isoformat()}/{today.isoformat()}"
    results.append(runner.get("crypto_daily_history", history_path, {"sort": "asc", "limit": 10}))

    results.append(runner.get("invalid_symbol", "/crypto/quote/NOTAREALSYMBOL"))

    successful_durations = [r["duration_ms"] for r in results if r["status"] == 200]
    return {
        "selected_symbols": selected,
        "requests": results,
        "summary": {
            "requests_total": len(results),
            "status_counts": {str(code): sum(r["status"] == code for r in results) for code in sorted({r["status"] for r in results})},
            "successful_median_ms": round(statistics.median(successful_durations), 1) if successful_durations else None,
            "successful_max_ms": max(successful_durations, default=None),
        },
    }


async def run_websocket(api_key: str, seconds: float = 12.0) -> dict[str, Any]:
    url = WS_BASE + "?apiKey=" + quote(api_key, safe="")
    started = time.perf_counter()
    events: list[dict[str, Any]] = []
    error: str | None = None
    close: dict[str, Any] = {}

    try:
        async with websockets.connect(url, open_timeout=15, close_timeout=5, ping_interval=None) as ws:
            ready_raw = await asyncio.wait_for(ws.recv(), timeout=8)
            ready = json.loads(ready_raw)
            events.append({"elapsed_ms": round((time.perf_counter() - started) * 1000, 1), "message": trimmed(ready)})

            await ws.send(
                json.dumps(
                    {
                        "action": "subscribe",
                        "channels": ["crypto.quotes", "crypto.trades"],
                        "symbols": ["BTCUSD"],
                        "snapshot": True,
                    }
                )
            )
            await ws.send(json.dumps({"action": "ping"}))

            deadline = time.perf_counter() + seconds
            while time.perf_counter() < deadline and len(events) < 15:
                timeout = max(0.1, min(2.0, deadline - time.perf_counter()))
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    continue
                message = json.loads(raw)
                events.append({"elapsed_ms": round((time.perf_counter() - started) * 1000, 1), "message": trimmed(message)})
                kinds = {(e["message"].get("type"), e["message"].get("event")) for e in events if isinstance(e["message"], dict)}
                has_data = any(k[0] in {"quote", "trade"} for k in kinds)
                has_subscribed = ("system", "subscribed") in kinds
                has_pong = ("system", "pong") in kinds
                if has_data and has_subscribed and has_pong:
                    break
            close = {"code": ws.close_code, "reason": ws.close_reason}
    except Exception as exc:  # Evidence should be written even on transport failure.
        error = f"{type(exc).__name__}: {exc}"

    messages = [e["message"] for e in events if isinstance(e.get("message"), dict)]
    return {
        "duration_ms": round((time.perf_counter() - started) * 1000, 1),
        "events": events,
        "event_types": sorted({str(m.get("type")) for m in messages}),
        "system_events": sorted({str(m.get("event")) for m in messages if m.get("type") == "system"}),
        "data_frames": sum(m.get("type") in {"quote", "trade"} for m in messages),
        "error": error,
        "close": close,
    }


def parse_mcp_response(response: requests.Response) -> Any:
    content_type = response.headers.get("content-type", "")
    if "text/event-stream" in content_type:
        data_lines = []
        for line in response.text.splitlines():
            if line.startswith("data:"):
                data_lines.append(line[5:].strip())
        if not data_lines:
            return {"raw": response.text[:2000]}
        parsed = [json.loads(line) for line in data_lines]
        return parsed[-1] if len(parsed) == 1 else parsed
    try:
        return response.json()
    except ValueError:
        return {"raw": response.text[:2000]}


def mcp_post(session: requests.Session, payload: dict[str, Any]) -> tuple[requests.Response, Any, float]:
    started = time.perf_counter()
    response = session.post(MCP_URL, json=payload, timeout=30)
    duration_ms = round((time.perf_counter() - started) * 1000, 1)
    return response, parse_mcp_response(response), duration_ms


def result_payload(parsed: Any) -> Any:
    if isinstance(parsed, dict):
        return parsed.get("result")
    if isinstance(parsed, list):
        for item in reversed(parsed):
            if isinstance(item, dict) and "result" in item:
                return item["result"]
    return None


def args_from_schema(schema: dict[str, Any]) -> dict[str, Any]:
    properties = schema.get("properties", {}) if isinstance(schema, dict) else {}
    required = schema.get("required", []) if isinstance(schema, dict) else []
    values: dict[str, Any] = {}
    candidates = {
        "asset": "crypto",
        "asset_class": "crypto",
        "type": "crypto",
        "symbol": "BTCUSD",
        "symbols": ["BTCUSD"],
    }
    for name in required:
        if name in candidates:
            values[name] = candidates[name]
        elif isinstance(properties.get(name), dict) and properties[name].get("enum"):
            values[name] = properties[name]["enum"][0]
    for name in ("asset", "asset_class", "type", "symbol"):
        if name in properties and name in candidates:
            values.setdefault(name, candidates[name])
    return values


def run_mcp(api_key: str) -> dict[str, Any]:
    session = requests.Session()
    session.headers.update(
        {
            "x-api-key": api_key,
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
            "User-Agent": "independent-review/1.0",
        }
    )
    out: dict[str, Any] = {"steps": [], "tool_names": [], "error": None}
    try:
        response, parsed, duration = mcp_post(
            session,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {},
                    "clientInfo": {"name": "independent-review", "version": "1.0"},
                },
            },
        )
        session_id = response.headers.get("mcp-session-id")
        if session_id:
            session.headers["mcp-session-id"] = session_id
        out["steps"].append({"name": "initialize", "status": response.status_code, "duration_ms": duration, "body": trimmed(parsed)})
        response.raise_for_status()

        # Notification may return 202 with an empty body.
        response, parsed, duration = mcp_post(
            session, {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}
        )
        out["steps"].append({"name": "initialized_notification", "status": response.status_code, "duration_ms": duration})

        response, parsed, duration = mcp_post(
            session, {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}
        )
        tools_result = result_payload(parsed) or {}
        tools = tools_result.get("tools", []) if isinstance(tools_result, dict) else []
        out["tool_names"] = [tool.get("name") for tool in tools if isinstance(tool, dict)]
        out["steps"].append(
            {
                "name": "tools_list",
                "status": response.status_code,
                "duration_ms": duration,
                "tool_count": len(tools),
                "tools": [
                    {
                        "name": tool.get("name"),
                        "description": tool.get("description"),
                        "inputSchema": tool.get("inputSchema"),
                    }
                    for tool in tools
                    if isinstance(tool, dict)
                ],
            }
        )
        response.raise_for_status()

        target = next((tool for tool in tools if tool.get("name") == "get_snapshot"), None)
        if target:
            arguments = args_from_schema(target.get("inputSchema") or {})
            response, parsed, duration = mcp_post(
                session,
                {
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "tools/call",
                    "params": {"name": "get_snapshot", "arguments": arguments},
                },
            )
            out["steps"].append(
                {
                    "name": "tools_call_get_snapshot",
                    "arguments": arguments,
                    "status": response.status_code,
                    "duration_ms": duration,
                    "body": trimmed(parsed),
                }
            )
            response.raise_for_status()
        else:
            out["error"] = "get_snapshot was not present in tools/list"
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


def build_summary(report: dict[str, Any]) -> dict[str, Any]:
    rest_requests = report.get("rest", {}).get("requests", [])
    ws = report.get("websocket", {})
    mcp = report.get("mcp", {})
    core_quote_labels = {
        "quote_crypto",
        "quote_forex",
        "quote_stocks",
        "quote_indices",
        "quote_etfs",
        "quote_commodities",
        "quote_perpetuals",
        "bond_us_10y",
    }
    quote_results = [r for r in rest_requests if r.get("label") in core_quote_labels]
    return {
        "rest_successes": sum(r.get("status") == 200 for r in rest_requests),
        "rest_total": len(rest_requests),
        "asset_successes": sum(r.get("status") == 200 for r in quote_results),
        "asset_checks": len(quote_results),
        "websocket_ready": "ready" in ws.get("system_events", []),
        "websocket_subscribed": "subscribed" in ws.get("system_events", []),
        "websocket_pong": "pong" in ws.get("system_events", []),
        "websocket_data_frames": ws.get("data_frames", 0),
        "mcp_tool_count": len(mcp.get("tool_names", [])),
        "mcp_get_snapshot_called": any(step.get("name") == "tools_call_get_snapshot" for step in mcp.get("steps", [])),
        "errors": [x for x in [ws.get("error"), mcp.get("error")] if x],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-websocket", action="store_true")
    parser.add_argument("--skip-mcp", action="store_true")
    args = parser.parse_args()

    api_key = os.environ.get("TICKERLAYER_API_KEY", "").strip()
    if not api_key:
        print("Set TICKERLAYER_API_KEY before running.", file=sys.stderr)
        return 2

    report: dict[str, Any] = {
        "experiment": "TickerLayer independent integration evaluation",
        "started_at_utc": utc_now(),
        "environment": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "requests": requests.__version__,
            "websockets": getattr(websockets, "__version__", "unknown"),
        },
        "rest": run_rest(api_key),
    }
    report["websocket"] = {"skipped": True} if args.skip_websocket else asyncio.run(run_websocket(api_key))
    report["mcp"] = {"skipped": True, "steps": [], "tool_names": []} if args.skip_mcp else run_mcp(api_key)
    report["finished_at_utc"] = utc_now()
    report["summary"] = build_summary(report)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = RESULTS_DIR / f"tickerlayer_experiment_{stamp}.json"
    text = json.dumps(json_safe(report), indent=2, ensure_ascii=False)
    path.write_text(text + "\n", encoding="utf-8")
    (RESULTS_DIR / "latest.json").write_text(text + "\n", encoding="utf-8")

    print(json.dumps(report["summary"], indent=2))
    print(f"Saved: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

