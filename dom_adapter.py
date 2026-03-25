import json
import os
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


HOST = os.getenv("DOM_ADAPTER_HOST", "127.0.0.1")
PORT = int(os.getenv("DOM_ADAPTER_PORT", "8000"))
SNAPSHOT_DIR = Path(os.getenv("DOM_SNAPSHOT_DIR", Path(__file__).with_name("dom_snapshots")))
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

MEMORY_BOOKS: dict[str, dict] = {}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def symbol_to_filename(symbol: str) -> Path:
    safe_symbol = symbol.replace("/", "_").replace(":", "_").replace(" ", "_")
    return SNAPSHOT_DIR / f"{safe_symbol}.json"


def load_book(symbol: str) -> dict:
    if symbol in MEMORY_BOOKS:
        return MEMORY_BOOKS[symbol]

    file_path = symbol_to_filename(symbol)
    if file_path.exists():
        try:
            data = json.loads(file_path.read_text())
            if isinstance(data, dict):
                MEMORY_BOOKS[symbol] = data
                return data
        except Exception:
            pass

    return {"symbol": symbol, "bids": [], "asks": []}


def save_book(symbol: str, payload: dict) -> None:
    MEMORY_BOOKS[symbol] = payload
    symbol_to_filename(symbol).write_text(json.dumps(payload, indent=2))


class DomAdapterHandler(BaseHTTPRequestHandler):
    server_version = "DomAdapter/1.0"

    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)

        if parsed.path == "/health":
            self._send_json(200, {"status": "ok", "timestamp": utc_now_iso()})
            return

        if parsed.path == "/symbols":
            files = sorted(path.stem.replace("_", "/") for path in SNAPSHOT_DIR.glob("*.json"))
            memory_symbols = sorted(MEMORY_BOOKS.keys())
            symbols = sorted(set(files + memory_symbols))
            self._send_json(200, {"symbols": symbols})
            return

        if parsed.path != "/dom":
            self._send_json(404, {"error": "Not found"})
            return

        symbol = query.get("symbol", ["GBP/USD"])[0]
        book = load_book(symbol)
        payload = {
            "symbol": symbol,
            "timestamp": book.get("timestamp") or utc_now_iso(),
            "last_price": book.get("last_price"),
            "bids": book.get("bids", []),
            "asks": book.get("asks", []),
        }

        if not payload["bids"] and not payload["asks"]:
            payload["message"] = (
                "Nog geen DOM snapshot voor dit symbool. POST eerst order book data naar /dom/update "
                "of plaats een JSON-bestand in dom_snapshots/."
            )

        self._send_json(200, payload)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/dom/update":
            self._send_json(404, {"error": "Not found"})
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_length)
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except Exception as exc:
            self._send_json(400, {"error": f"Invalid JSON: {exc}"})
            return

        if not isinstance(payload, dict):
            self._send_json(400, {"error": "Payload must be a JSON object"})
            return

        symbol = str(payload.get("symbol", "")).strip() or "GBP/USD"
        bids = payload.get("bids", [])
        asks = payload.get("asks", [])
        if not isinstance(bids, list) or not isinstance(asks, list):
            self._send_json(400, {"error": "Payload must contain bids and asks arrays"})
            return

        normalized = {
            "symbol": symbol,
            "timestamp": payload.get("timestamp") or utc_now_iso(),
            "last_price": payload.get("last_price"),
            "bids": bids,
            "asks": asks,
        }
        save_book(symbol, normalized)
        self._send_json(200, {"status": "stored", "symbol": symbol, "levels": len(bids) + len(asks)})

    def log_message(self, format: str, *args) -> None:
        return


def main() -> None:
    print(f"DOM adapter listening on http://{HOST}:{PORT}")
    print(f"Snapshots directory: {SNAPSHOT_DIR}")
    server = ThreadingHTTPServer((HOST, PORT), DomAdapterHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nDOM adapter stopped.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
