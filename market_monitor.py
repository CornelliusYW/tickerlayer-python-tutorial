"""Small TickerLayer tutorial monitor. API keys stay in this Python process.

Capture: python market_monitor.py --capture
View:    python market_monitor.py --serve
"""
from __future__ import annotations

import argparse
import asyncio
import html
import json
import os
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import requests
import websockets

from tickerlayer_experiment import run_mcp

HERE = Path(__file__).resolve().parent
BASE = "https://api.tickerlayer.com"
WATCHLIST = [
    ("crypto", "BTCUSD"), ("forex", "EURUSD"),
    ("stocks", "US:AAPL"), ("indices", "AR40"),
    ("etfs", "AAA"), ("commodities", "XAUUSD"),
    ("perpetuals", "AAOIUSDT"),
]


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def get_json(session, path, params=None):
    """Initial GET plus at most three retries on 503; fail other HTTP errors.

    A monthly 429 is not a temporary outage. This example fails it instead
    of repeatedly consuming requests. Retry-After, when supplied, is honored.
    """
    for attempt in range(4):
        response = session.get(BASE + path, params=params, timeout=20)
        if response.status_code != 503 or attempt == 3:
            response.raise_for_status()
            return response.json()
        delay = 2 ** attempt
        hint = response.headers.get("Retry-After")
        if hint:
            try:
                delay = max(0, float(hint))
            except ValueError:
                moment = parsedate_to_datetime(hint)
                delay = max(0, (moment - datetime.now(timezone.utc)).total_seconds())
        if delay > 60:
            raise RuntimeError("Server requested a long pause; try again later.")
        time.sleep(delay)


def quote_row(asset, payload):
    bid, ask = float(payload["bid"]), float(payload["ask"])
    return {
        "asset": asset, "symbol": payload["symbol"],
        "bid": bid, "ask": ask, "mid": (bid + ask) / 2,
        "spread": ask - bid,
        "event_time_utc": pd.to_datetime(payload["timestamp"], unit="ms", utc=True),
    }


def apply_event(state, message):
    """Keep trades separate from quotes and ignore older updates of each type."""
    kind = message.get("type")
    if kind not in {"quote", "trade"}:
        return
    key = (message["asset"], message["symbol"])
    bucket = state.setdefault(key, {})
    timestamp = message.get("ts", message.get("timestamp"))
    previous = bucket.get(kind)
    if previous and timestamp < previous["timestamp"]:
        return
    event = {"timestamp": timestamp}
    fields = ("bid", "ask") if kind == "quote" else ("price", "size")
    for field in fields:
        event[field] = float(message[field])
    bucket[kind] = event


async def stream_events(api_key, seconds=15):
    url = "wss://stream.tickerlayer.com/?apiKey=" + quote(api_key, safe="")
    events, state = [], {}
    started = time.perf_counter()
    async with websockets.connect(url, open_timeout=15, close_timeout=5) as ws:
        ready = json.loads(await asyncio.wait_for(ws.recv(), timeout=8))
        events.append({"received_at_utc": utc_now(), "message": ready})
        if ready.get("event") != "ready":
            raise RuntimeError("Expected a ready frame before subscribing.")
        await ws.send(json.dumps({
            "action": "subscribe", "channels": ["crypto.quotes", "crypto.trades"],
            "symbols": ["BTCUSD"], "snapshot": True,
        }))
        await ws.send(json.dumps({"action": "ping"}))
        deadline = time.perf_counter() + seconds
        while time.perf_counter() < deadline and len(events) < 80:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=min(2, max(.01, deadline-time.perf_counter())))
            except asyncio.TimeoutError:
                continue
            message = json.loads(raw)
            events.append({"received_at_utc": utc_now(), "message": message})
            apply_event(state, message)
    return {"duration_seconds": round(time.perf_counter()-started, 2), "events": events}


def capture():
    key = os.environ.get("TICKERLAYER_API_KEY", "").strip()
    if not key:
        raise SystemExit("Set TICKERLAYER_API_KEY before capturing.")
    report = {"started_at_utc": utc_now(), "quotes": [], "statuses": [], "errors": []}
    session = requests.Session()
    session.headers.update({"x-api-key": key, "Accept": "application/json"})
    for asset, symbol in WATCHLIST:
        try:
            payload = get_json(session, f"/{asset}/quote/{quote(symbol, safe=':')}")
            report["quotes"].append({"asset": asset, "payload": payload, "received_at_utc": utc_now()})
        except Exception as exc:
            report["errors"].append({"asset": asset, "symbol": symbol, "error": str(exc)})
    for field, path in (("bond", "/bond/snapshot/US:10Y"), ("snapshot", "/crypto/snapshot/BTCUSD")):
        try:
            report[field] = get_json(session, path)
        except Exception as exc:
            report["errors"].append({"field": field, "error": str(exc)})
    # Check session context for the two older quotes and the stock example.
    for asset, symbol in (("stocks", "US:AAPL"), ("indices", "AR40"), ("etfs", "AAA")):
        try:
            payload = get_json(session, "/markets/status", {"asset": asset, "symbol": symbol})
            report["statuses"].append({"asset": asset, "symbol": symbol, "payload": payload})
        except Exception as exc:
            report["errors"].append({"asset": asset, "symbol": symbol, "error": str(exc)})
    today = datetime.now(timezone.utc).date()
    start, end = today - timedelta(days=7), today - timedelta(days=1)
    try:
        report["history"] = get_json(session, f"/crypto/agg/BTCUSD/1/day/{start}/{end}", {"sort": "asc", "limit": 10})
    except Exception as exc:
        report["errors"].append({"field": "history", "error": str(exc)})
    try:
        report["websocket"] = asyncio.run(stream_events(key))
    except Exception as exc:
        report["errors"].append({"field": "websocket", "error": str(exc)})
    report["mcp"] = run_mcp(key)
    report["finished_at_utc"] = utc_now()
    folder = HERE / "fresh_results"
    folder.mkdir(exist_ok=True)
    path = folder / ("monitor_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + ".json")
    path.write_text(json.dumps(report, indent=2).replace(key, "[REDACTED]"), encoding="utf-8")
    print(f"Saved new capture: {path.name}")
    print(f"Quote rows: {len(report['quotes'])}; errors: {report['errors']}".replace(key, "[REDACTED]"))
    render(report)


def pretty(value):
    return html.escape(json.dumps(value, indent=2))


def render(report):
    """Render actual captured outputs, never simulated prices or provider UI."""
    out = HERE / "article_preview"
    out.mkdir(exist_ok=True)
    style = """body{font:16px system-ui,sans-serif;color:#172337;background:#f4f6fa;margin:0}
    main{max-width:1120px;margin:30px auto;padding:0 26px}h1{font-size:30px;margin:12px 0}
    h2{font-size:22px;margin:24px 0 12px}p{line-height:1.5}nav{display:flex;gap:22px;border-bottom:1px solid #ccd5df;padding:16px 0}
    a{color:#155d86;text-decoration:none}small,.muted{color:#526276}table{width:100%;border-collapse:collapse;background:white;font-size:14px}
    th,td{border:1px solid #d9dfe7;padding:11px 12px;text-align:left}th{background:#e8eef6;font-size:12px;text-transform:uppercase}
    td.num{text-align:right;font-variant-numeric:tabular-nums}pre{background:#fff;border:1px solid #d9dfe7;padding:18px;line-height:1.5;overflow:auto;font-size:14px}
    .label{color:#155d86;font-weight:650;font-size:13px;letter-spacing:.03em}.note{font-size:14px;color:#526276}.columns{display:grid;grid-template-columns:1fr 1fr;gap:24px}
    """
    def page(name, title, body):
        captured_at = pd.to_datetime(report["started_at_utc"], utc=True)
        content = f"""<!doctype html><html><head><meta charset="utf-8"><title>{title}</title><style>{style}</style></head>
        <body><main><div class="label">TICKERLAYER PYTHON TUTORIAL · ACTUAL API OUTPUT</div><h1>{title}</h1>
        <p class="muted">Captured {captured_at.strftime('%Y-%m-%d %H:%M:%S')} UTC on {captured_at.strftime('%B %d, %Y')}. This saved view does not auto-refresh.</p>
        <nav><a href="index.html">REST monitor</a><a href="history.html">Daily history</a><a href="stream.html">WebSocket frames</a><a href="mcp.html">MCP tool call</a></nav>
        {body}<p class="note">Tutorial output, not the TickerLayer dashboard. Indicative data for technical use, not investment advice.</p></main></body></html>"""
        (out / name).write_text(content, encoding="utf-8")
    rows = []
    for item in report["quotes"]:
        row = quote_row(item["asset"], item["payload"])
        age = (datetime.fromisoformat(item["received_at_utc"]) - row["event_time_utc"]).total_seconds()
        age_text = f"{age/3600:.1f} h" if age > 3600 else (f"ahead {-age:.2f} s" if age < 0 else f"{age:.2f} s")
        rows.append(f"<tr><td>{row['asset']}</td><td>{row['symbol']}</td><td class=num>{row['bid']:,.6f}</td><td class=num>{row['ask']:,.6f}</td><td class=num>{row['mid']:,.6f}</td><td>{row['event_time_utc'].strftime('%b %d %H:%M:%S')}</td><td>{age_text}</td></tr>")
    statuses = "; ".join(f"{i['symbol']}: {i['payload'].get('status','unknown')} ({i['payload'].get('phase','unknown')})" for i in report["statuses"])
    bond = report.get("bond", {})
    page("index.html", "A multi-asset market monitor", "<h2>REST quotes</h2><table><tr><th>Asset</th><th>Symbol</th><th>Bid</th><th>Ask</th><th>Mid reference</th><th>Event UTC</th><th>Age at capture</th></tr>" + "".join(rows) + f"</table><p><b>Daily bond observation:</b> US:10Y {bond.get('rate')}% · observation date {bond.get('date')} · change {bond.get('change_bps')} bps.</p><p class=note><b>Market-status response:</b> {html.escape(statuses)}. Calendar status is not a feed-health check.</p>")
    bars = report.get("history", {}).get("results", [])
    history_rows = "".join(f"<tr><td>{pd.to_datetime(b['t'],unit='ms',utc=True).strftime('%Y-%m-%d')}</td>" + "".join(f"<td class=num>{b[x]:,.2f}</td>" for x in ['o','h','l','c','v']) + "</tr>" for b in bars)
    page("history.html", "BTCUSD daily history", "<p>The query ends yesterday in UTC, excluding the current incomplete daily candle.</p><table><tr><th>UTC date</th><th>Open</th><th>High</th><th>Low</th><th>Close</th><th>Volume</th></tr>" + history_rows + "</table>")
    events = report.get("websocket", {}).get("events", [])
    table_rows = []
    for event in events[:8]:
        m = event["message"]
        detail = json.dumps({k:v for k,v in m.items() if k in {'bid','ask','price','size','snapshot','channels'}})
        table_rows.append(f"<tr><td>{event['received_at_utc'][11:23]}</td><td>{html.escape(m.get('event',m.get('type','')))}</td><td>{html.escape(m.get('symbol','—'))}</td><td>{html.escape(detail)}</td></tr>")
    quote_event = next((e["message"] for e in events if e["message"].get("type") == "quote"), {})
    page("stream.html", "Receiving BTCUSD over WebSocket", f"<p>{len(events)} frames captured during a bounded {report.get('websocket',{}).get('duration_seconds')} second connection. First eight shown below.</p><table><tr><th>Received UTC</th><th>Event</th><th>Symbol</th><th>Payload fields</th></tr>" + "".join(table_rows) + "</table><h2>An actual quote frame</h2><pre>" + pretty(quote_event) + "</pre>")
    mcp = report.get("mcp", {})
    call = next((s for s in mcp.get("steps",[]) if s.get("name") == "tools_call_get_snapshot"), {})
    result = call.get("body",{}).get("result",{})
    page("mcp.html", "Calling a typed MCP tool", f"<p>Discovered {len(mcp.get('tool_names',[]))} tools. This example calls get_snapshot, not every discovered tool.</p><div class=columns><section><h2>Tool arguments</h2><pre>{pretty(call.get('arguments',{}))}</pre><h2>Server endpoint</h2><pre>https://mcp.tickerlayer.com/mcp</pre><p>Streamable HTTP · authenticated server-side<br>HTTP status: {call.get('status')}</p></section><section><h2>Returned structuredContent</h2><pre>{pretty(result.get('structuredContent',{}))}</pre></section></div>")
    print(f"Preview saved in {out.name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", action="store_true")
    parser.add_argument("--serve", action="store_true")
    args = parser.parse_args()
    if args.capture:
        try:
            capture()
        except Exception as exc:
            key = os.environ.get("TICKERLAYER_API_KEY", "")
            message = str(exc).replace(key, "[REDACTED]") if key else str(exc)
            raise SystemExit(message) from None
    if args.serve:
        from functools import partial
        handler = partial(SimpleHTTPRequestHandler, directory=str(HERE / "article_preview"))
        print("Saved monitor view: http://127.0.0.1:8765", flush=True)
        ThreadingHTTPServer(("127.0.0.1", 8765), handler).serve_forever()
    if not args.capture and not args.serve:
        parser.print_help()


if __name__ == "__main__":
    main()
