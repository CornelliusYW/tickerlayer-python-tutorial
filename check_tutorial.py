"""Offline checks against only the freshly collected October 12 evidence."""
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import requests

from market_monitor import apply_event, get_json, quote_row, render

folder = Path(__file__).parent
path = folder / "fresh_results" / "monitor_20261012T145742Z.json"
report = json.loads(path.read_text(encoding="utf-8"))
assert report["started_at_utc"].startswith("2026-10-12")
assert not report["errors"]
assert report["mcp"]["error"] is None
assert len(report["mcp"]["tool_names"]) == 12
for item in report["quotes"]:
    row = quote_row(item["asset"], item["payload"])
    assert row["bid"] <= row["ask"]
    assert row["mid"] == (row["bid"] + row["ask"]) / 2
bars = pd.DataFrame(report["history"]["results"])
assert len(bars) == 7
assert pd.to_datetime(bars.t, unit="ms", utc=True).max().date().isoformat() == "2026-10-11"
state = {}
events = report["websocket"]["events"]
for event in events:
    apply_event(state, event["message"])
assert set(state[("crypto", "BTCUSD")]) == {"quote", "trade"}
latest_quote = state[("crypto", "BTCUSD")]["quote"].copy()
first_quote = next(e["message"] for e in events if e["message"].get("type") == "quote")
apply_event(state, first_quote)
assert state[("crypto", "BTCUSD")]["quote"] == latest_quote
# Stubbed responses exercise local retry logic, not provider outage recovery.
unavailable = Mock(status_code=503, headers={"Retry-After": "0"})
ok = Mock(status_code=200)
ok.json.return_value = {"ok": True}
session = Mock()
session.get.side_effect = [unavailable, ok]
with patch("market_monitor.time.sleep") as sleep:
    assert get_json(session, "/fake-local-check") == {"ok": True}
    sleep.assert_called_once_with(0)
bad = Mock(status_code=404)
bad.raise_for_status.side_effect = requests.HTTPError("not found")
session.get.side_effect = [bad]
try:
    get_json(session, "/fake-local-check")
except requests.HTTPError:
    pass
else:
    raise AssertionError("404 must fail without a retry")
render(report)
print("PASS: fresh evidence, normalization, history dates, stream reducer, retry branches")
print("Frames:", len(events), "Quotes:", sum(e['message'].get('type')=='quote' for e in events), "Trades:", sum(e['message'].get('type')=='trade' for e in events))
