from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
import importlib
from typing import Any, Dict, Optional, Tuple

try:
    oanda_api_module = importlib.import_module("oandapyV20")
    orders = importlib.import_module("oandapyV20.endpoints.orders")
    accounts = importlib.import_module("oandapyV20.endpoints.accounts")
except Exception as import_error:
    oanda_api_module = None
    orders = None
    accounts = None
    OANDA_IMPORT_ERROR = str(import_error)
else:
    OANDA_IMPORT_ERROR = ""


@dataclass
class OandaExecutionConfig:
    account_id: str
    access_token: str
    environment: str = "practice"
    risk_percent: float = 1.0
    fixed_units: int = 1000
    use_risk_sizing: bool = True


@dataclass
class OandaExecutionResult:
    success: bool
    message: str
    order_ticket: Optional[str] = None
    volume: Optional[float] = None
    symbol: str = ""


BUY = "Buy"
SELL = "Sell"


def oanda_is_available() -> Tuple[bool, str]:
    if oanda_api_module is None:
        return False, OANDA_IMPORT_ERROR or "oandapyV20 package is niet beschikbaar in deze Python-omgeving."
    return True, ""


def build_oanda_instrument(instrument_label: str) -> str:
    return str(instrument_label).replace("/", "_").replace(" ", "").upper()


def infer_price_decimals(instrument: str) -> int:
    if instrument.endswith("_JPY"):
        return 3
    if instrument.startswith("XAU_"):
        return 2
    return 5


def format_price(value: float, decimals: int) -> str:
    quantize_pattern = "1." + ("0" * decimals)
    return str(Decimal(str(value)).quantize(Decimal(quantize_pattern), rounding=ROUND_HALF_UP))


def get_client(config: OandaExecutionConfig):
    return oanda_api_module.API(access_token=config.access_token, environment=config.environment)


def get_account_summary(config: OandaExecutionConfig) -> Dict[str, Any]:
    client = get_client(config)
    request = accounts.AccountSummary(accountID=config.account_id)
    client.request(request)
    return request.response.get("account", {})


def calculate_order_units(signal: Dict[str, Any], config: OandaExecutionConfig) -> int:
    preferred_units = signal.get("preferred_fixed_units")
    if preferred_units is not None:
        return max(int(preferred_units), 1)

    if not config.use_risk_sizing:
        return max(int(config.fixed_units), 1)

    stop_loss = signal.get("stop_loss")
    entry_price = signal.get("price")
    if stop_loss is None or entry_price is None:
        return max(int(config.fixed_units), 1)

    stop_distance = abs(float(entry_price) - float(stop_loss))
    if stop_distance <= 0:
        return max(int(config.fixed_units), 1)

    try:
        account_summary = get_account_summary(config)
        nav = float(account_summary.get("NAV") or account_summary.get("balance") or 0.0)
    except Exception:
        nav = 0.0

    if nav <= 0:
        return max(int(config.fixed_units), 1)

    risk_amount = nav * (float(config.risk_percent) / 100.0)
    units = int(risk_amount / stop_distance)
    return max(units, 1)


def place_signal_order(signal: Dict[str, Any], instrument_label: str, config: OandaExecutionConfig, order_comment: str = "AI Dashboard") -> OandaExecutionResult:
    available, message = oanda_is_available()
    if not available:
        return OandaExecutionResult(False, f"OANDA connectie mislukt: {message}")

    instrument = build_oanda_instrument(instrument_label)
    direction = str(signal.get("signal", ""))
    stop_loss = signal.get("stop_loss")
    take_profit = signal.get("take_profit")

    if stop_loss is None or take_profit is None:
        return OandaExecutionResult(False, "Signaal mist SL of TP.", symbol=instrument)

    units = calculate_order_units(signal, config)
    signed_units = units if direction == BUY else -units if direction == SELL else 0
    if signed_units == 0:
        return OandaExecutionResult(False, f"Onbekende richting: {direction}", symbol=instrument)

    decimals = infer_price_decimals(instrument)
    order_payload = {
        "order": {
            "type": "MARKET",
            "instrument": instrument,
            "units": str(int(signed_units)),
            "timeInForce": "FOK",
            "positionFill": "DEFAULT",
            "takeProfitOnFill": {"price": format_price(float(take_profit), decimals)},
            "stopLossOnFill": {"price": format_price(float(stop_loss), decimals)},
            "clientExtensions": {
                "comment": str(order_comment)[:128],
                "tag": "ai-dashboard",
            },
        }
    }

    try:
        client = get_client(config)
        request = orders.OrderCreate(accountID=config.account_id, data=order_payload)
        client.request(request)
        response = request.response or {}
    except Exception as exc:
        return OandaExecutionResult(False, f"OANDA order afgewezen: {exc}", volume=abs(signed_units), symbol=instrument)

    fill_transaction = response.get("orderFillTransaction", {})
    cancel_transaction = response.get("orderCancelTransaction", {})

    if cancel_transaction:
        reason = cancel_transaction.get("reason") or cancel_transaction.get("type") or "onbekende fout"
        return OandaExecutionResult(False, f"OANDA order afgewezen: {reason}", volume=abs(signed_units), symbol=instrument)

    order_id = fill_transaction.get("id") or response.get("orderCreateTransaction", {}).get("id")
    return OandaExecutionResult(True, "order geplaatst", order_ticket=str(order_id) if order_id else None, volume=abs(signed_units), symbol=instrument)