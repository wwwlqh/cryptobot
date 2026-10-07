"""One-shot public RTDS connectivity and payload-contract preflight."""

from __future__ import annotations

import json
import os
import time

from websockets.sync.client import connect

from cryptobot_polymarket.price_to_beat import fetch_price_to_beat
from cryptobot_polymarket.twap_mirror import (
    DEFAULT_WS_URL,
    SUBSCRIBE_MESSAGE,
    parse_twap_message,
)

EXPECTED = {
    (coin, window_s)
    for coin in tuple(
        coin.strip().upper()
        for coin in os.environ.get("CRYPTOBOT_COINS", "BTC").split(",")
        if coin.strip()
    )
    for window_s in (60,)
}


def has_expected_series(
    accepted: set[tuple[str, int]],
    expected: set[tuple[str, int]] = EXPECTED,
) -> bool:
    """Accept extra public streams while requiring every configured stream."""
    return expected.issubset(accepted)


def main() -> None:
    accepted: set[tuple[str, int]] = set()
    deadline = time.monotonic() + 25.0
    with connect(
        os.environ.get("CRYPTOBOT_TWAP_RTDS_URL", DEFAULT_WS_URL),
        open_timeout=10.0,
        close_timeout=2.0,
        ping_interval=None,
    ) as websocket:
        websocket.send(json.dumps(SUBSCRIBE_MESSAGE))
        while time.monotonic() < deadline and not has_expected_series(accepted):
            try:
                raw = websocket.recv(timeout=2.0)
            except TimeoutError:
                websocket.send("PING")
                continue
            try:
                message = json.loads(raw)
            except (TypeError, ValueError):
                continue
            messages = message if isinstance(message, list) else [message]
            for item in messages:
                sample = parse_twap_message(item)
                if sample is None or time.time() - sample.observation_ts > 10.0:
                    continue
                accepted.add((sample.coin, sample.window_s))
                print(
                    f"accepted coin={sample.coin} window_s={sample.window_s} "
                    f"observation_ts={sample.observation_ts:.3f}"
                )
    if not accepted:
        raise SystemExit("no RTDS frames received")
    if not has_expected_series(accepted):
        raise SystemExit(
            f"missing valid RTDS TWAP series: {sorted(EXPECTED - accepted)}"
        )
    print(f"accepted_series={sorted(accepted)}")

    # RTDS publishes current samples but does not guarantee an observation at
    # every exact market boundary. Verify the same official server-rendered
    # Price To Beat payload the Polymarket UI uses before restarting live.
    now = int(time.time())
    coins = sorted({coin for coin, _window_s in EXPECTED})
    for coin in coins:
        for interval, duration_s in (("5m", 300), ("15m", 900)):
            boundary = now // duration_s * duration_s
            value = None
            for attempt in range(3):
                value = fetch_price_to_beat(
                    coin=coin,
                    interval=interval,
                    boundary_ts=boundary,
                    timeout=10.0,
                )
                if value is not None:
                    break
                if attempt < 2:
                    time.sleep(2.0)
            if value is None:
                raise SystemExit(
                    f"official Polymarket Price To Beat unavailable: "
                    f"coin={coin} interval={interval} boundary={boundary}"
                )
            print(
                f"accepted_price_to_beat coin={coin} interval={interval} "
                f"boundary={boundary} value={value}"
            )


if __name__ == "__main__":
    main()
