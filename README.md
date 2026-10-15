# TickerLayer Python Tutorial

Code and saved outputs for the Non-Brand Data tutorial on accessing financial market data with TickerLayer. The tutorial is sponsored by TickerLayer, which provided reviewer access.

## Start here

Download this repository using **Code → Download ZIP**, then extract it. Keep the files and folders together.

Use Python 3.10 or newer and install the dependencies from the extracted folder:

```bash
python -m pip install -r requirements-article.txt
```

To explore the article's saved outputs without an API key, open `TickerLayer_Outputs.ipynb` and run its cells. The notebook reads `fresh_results/monitor_20261012T145742Z.json`; it does not make live requests.

If you need a notebook interface:

```bash
python -m pip install jupyterlab
python -m jupyterlab
```

## Request new data

Get your own API key from [TickerLayer](https://tickerlayer.com/docs/quickstart). Feed access and streaming availability depend on your account.

Set the key in your terminal. In PowerShell:

```powershell
$env:TICKERLAYER_API_KEY = "YOUR_API_KEY"
```

Or in macOS/Linux shells:

```bash
export TICKERLAYER_API_KEY="YOUR_API_KEY"
```

Then run:

```bash
python market_monitor.py --capture
```

The command requests multi-asset quotes, a bond yield, market status, Bitcoin daily bars, a short Bitcoin stream, and an MCP snapshot. It saves a new JSON file under `fresh_results` and a local HTML view under `article_preview`. Change `capture_path` in the notebook to inspect your new capture.

To follow the article's Python snippets, run them from this repository folder so their imports work.

## Files

- `market_monitor.py`: request helpers, watchlist, quote normalization, streaming, and the complete example.
- `tickerlayer_experiment.py`: the MCP helper imported by the example, plus the original integration-check runner.
- `TickerLayer_Outputs.ipynb`: notebook displaying the saved article outputs.
- `fresh_results/monitor_20261012T145742Z.json`: actual responses collected on October 12, 2026.
- `requirements-article.txt`: dependencies for the Python examples.
- `check_tutorial.py`: offline checks using the saved responses and stubbed HTTP responses.
- `stream_recovery.py`: bounded WebSocket reconnection, resubscription, and snapshot requests.
- `test_stream_recovery.py`: scripted, offline recovery checks; no provider requests.

Run the offline checks with `python check_tutorial.py`. They make no provider requests.

## Temporary errors and reconnects

`get_json` retries REST 503 responses up to three times, using 1, 2, and 4 second delays or a supplied `Retry-After`. Other HTTP errors are raised immediately.

The original `stream_events` function is a short, single-connection capture. For a bounded capture that retries transient disconnects:

```python
import asyncio
import os
from stream_recovery import stream_with_reconnect

key = os.environ["TICKERLAYER_API_KEY"]
capture = asyncio.run(stream_with_reconnect(key, seconds=15))
print("Reconnect attempts:", capture["reconnect_attempts"])
```

Set `TICKERLAYER_API_KEY` as shown above before running this example. In a notebook with an active event loop, use `capture = await stream_with_reconnect(key, seconds=15)` instead. The helper waits for `ready` on each connection, resubscribes with `snapshot=True`, and retains messages already collected. It permits up to three retries with backoff, honors `Retry-After` on temporary upgrade errors, and does not retry authentication, entitlement, connection-limit, or subscription errors. The 15-second capture window starts on the first successful connection; reconnection waits share that window. Connection setup and closing can add elapsed time.

Run `python -m unittest -v test_stream_recovery.py` for the scripted reconnection checks. They simulate disconnects and HTTP upgrade errors; they are not evidence of recovery during a real TickerLayer outage.

## Scope and safety

The saved results are historical examples, not current prices. The reviewer key had elevated access; these captures do not establish standard-plan limits, price accuracy, throughput, or long-term reliability. The original stream example stops at 80 messages or 15 seconds after connection. The optional reconnection helper requests current snapshots where available, not a replay of every trade missed during a disconnect. Neither example implements trade-gap recovery.

No API key is included. Keep credentials in your environment and do not commit them. New captures, local previews, and environment files are ignored by Git; only the specific article capture is tracked.

TickerLayer describes its data as derived and indicative. Review its [market data disclaimer](https://tickerlayer.com/market-data-disclaimer) and account terms for your intended use. This repository is a technical tutorial, not investment advice.
