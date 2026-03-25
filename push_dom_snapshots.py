import argparse
import random
import time
from datetime import datetime, timezone

import requests


TWELVEDATA_API_KEY_DEFAULT = "ccd7a17988684453aaf59c7ae5373000"
DOM_UPDATE_URL_DEFAULT = "http://127.0.0.1:8000/dom/update"


def fetch_twelvedata_price(symbol: str, api_key: str) -> float | None:
    url = "https://api.twelvedata.com/price"
    params = {"symbol": symbol, "apikey": api_key}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        print(f"[price] error: {exc}")
        return None

    if payload.get("status") == "error":
        print(f"[price] api error: {payload.get('message', 'unknown error')}")
        return None

    try:
        return float(payload.get("price"))
    except (TypeError, ValueError):
        print("[price] invalid price payload")
        return None


def build_synthetic_book(last_price: float, levels: int, tick_size: float) -> dict:
    bids = []
    asks = []

    base_size = 900000
    for level in range(1, levels + 1):
        distance = tick_size * level
        bid_price = round(last_price - distance, 5)
        ask_price = round(last_price + distance, 5)

        bid_size = int(base_size + random.randint(0, 900000) + (levels - level) * 80000)
        ask_size = int(base_size + random.randint(0, 900000) + (levels - level) * 80000)

        bids.append({"price": bid_price, "size": bid_size})
        asks.append({"price": ask_price, "size": ask_size})

    return {"bids": bids, "asks": asks}


def push_snapshot(update_url: str, symbol: str, last_price: float, bids: list[dict], asks: list[dict]) -> bool:
    payload = {
        "symbol": symbol,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "last_price": last_price,
        "bids": bids,
        "asks": asks,
    }

    try:
        response = requests.post(update_url, json=payload, timeout=10)
        response.raise_for_status()
        print(f"[push] stored {symbol} with {len(bids) + len(asks)} levels")
        return True
    except Exception as exc:
        print(f"[push] error: {exc}")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Push synthetic DOM snapshots based on live Twelve Data price.")
    parser.add_argument("--symbol", default="GBP/USD", help="Twelve Data symbol, e.g. GBP/USD or AAPL")
    parser.add_argument("--api-key", default=TWELVEDATA_API_KEY_DEFAULT, help="Twelve Data API key")
    parser.add_argument("--update-url", default=DOM_UPDATE_URL_DEFAULT, help="DOM adapter update endpoint")
    parser.add_argument("--interval", type=int, default=15, help="Seconds between updates")
    parser.add_argument("--levels", type=int, default=12, help="Number of bid/ask levels to generate")
    parser.add_argument("--tick-size", type=float, default=0.00005, help="Price step between generated levels")
    args = parser.parse_args()

    print("Starting DOM snapshot pusher")
    print(f"Symbol: {args.symbol}")
    print(f"Update URL: {args.update_url}")
    print("Note: this generates synthetic DOM around a live Twelve Data price.")

    while True:
        price = fetch_twelvedata_price(args.symbol, args.api_key)
        if price is not None:
            book = build_synthetic_book(price, args.levels, args.tick_size)
            push_snapshot(args.update_url, args.symbol, price, book["bids"], book["asks"])
        time.sleep(max(args.interval, 1))


if __name__ == "__main__":
    main()
