"""Capture the current 60s RTDS TWAP at every 5m/15m boundary."""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from websockets.sync.client import connect

TOPICS = {"crypto_prices_twap_sixty": 60}
COINS = {"btc/usd", "eth/usd", "sol/usd", "xrp/usd"}
SUBSCRIBE = {
    "action": "subscribe",
    "subscriptions": [
        {"topic": topic, "type": "update"} for topic in TOPICS
    ],
}


def _parse(message: object) -> dict | None:
    if not isinstance(message, dict) or message.get("type") != "update":
        return None
    window_s = TOPICS.get(str(message.get("topic")))
    payload = message.get("payload")
    if window_s is None or not isinstance(payload, dict):
        return None
    symbol = str(payload.get("symbol", "")).lower()
    if symbol not in COINS or payload.get("window_s") != window_s:
        return None
    try:
        observation_ms = int(payload["timestamp"])
        observation_s = observation_ms // 1000
        exact = int(payload["full_accuracy_value"])
    except (KeyError, TypeError, ValueError):
        return None
    # Every 15m boundary is also a 5m boundary, so one 5m-aligned 60s stream
    # captures the opening anchor required by both live intervals.
    step = 300
    if observation_ms % 1000 != 0 or observation_s % step != 0 or exact <= 0:
        return None
    return {
        "symbol": symbol,
        "window_s": window_s,
        "observation_ts": observation_s,
        "full_accuracy_value": str(exact),
        "received_at": time.time(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True)
    parser.add_argument("--duration", type=float, default=1_200.0)
    parser.add_argument(
        "--url", default="wss://ws-live-data.polymarket.com"
    )
    args = parser.parse_args(argv)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + args.duration
    captured: set[tuple[str, int, int]] = set()
    while time.monotonic() < deadline:
        try:
            with connect(
                args.url,
                open_timeout=10,
                close_timeout=2,
                ping_interval=None,
            ) as websocket:
                websocket.send(json.dumps(SUBSCRIBE))
                last_ping = time.monotonic()
                while time.monotonic() < deadline:
                    if time.monotonic() - last_ping >= 5:
                        websocket.send("PING")
                        last_ping = time.monotonic()
                    try:
                        raw = websocket.recv(timeout=1)
                    except TimeoutError:
                        continue
                    try:
                        message = json.loads(raw)
                    except (TypeError, ValueError):
                        continue
                    for item in message if isinstance(message, list) else [message]:
                        row = _parse(item)
                        if row is None:
                            continue
                        key = (row["symbol"], row["window_s"], row["observation_ts"])
                        if key in captured:
                            continue
                        captured.add(key)
                        with out.open("a", encoding="utf-8") as handle:
                            handle.write(json.dumps(row, sort_keys=True) + "\n")
                        print(json.dumps(row, sort_keys=True), flush=True)
        except Exception as exc:  # noqa: BLE001 - bounded capture reconnect
            print(f"reconnect={type(exc).__name__}", flush=True)
            time.sleep(1)
    print(f"captured={len(captured)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
