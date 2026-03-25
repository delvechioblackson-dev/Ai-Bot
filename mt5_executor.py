from dataclasses import dataclass
import importlib
from typing import Any, Dict, List, Optional, Tuple

try:
    mt5 = importlib.import_module("MetaTrader5")
except Exception as import_error:
    mt5 = None
    MT5_IMPORT_ERROR = str(import_error)
else:
    MT5_IMPORT_ERROR = ""


@dataclass
class MT5ExecutionConfig:
    login: Optional[int]
    password: str
    server: str
    terminal_path: str = ""
    risk_percent: float = 1.0
    fixed_volume: float = 0.01
    use_risk_sizing: bool = True
    symbol_prefix: str = ""
    symbol_suffix: str = ""
    max_deviation: int = 20
    magic_number: int = 20260319


@dataclass
class MT5ExecutionResult:
    success: bool
    message: str
    order_ticket: Optional[int] = None
    volume: Optional[float] = None
    symbol: str = ""


BUY = "Buy"
SELL = "Sell"


def mt5_is_available() -> Tuple[bool, str]:
    if mt5 is None:
        return False, MT5_IMPORT_ERROR or "MetaTrader5 package is niet beschikbaar in deze Python-omgeving."
    return True, ""


def build_symbol_candidates(instrument_label: str, prefix: str = "", suffix: str = "") -> List[str]:
    normalized = str(instrument_label).replace("/", "").replace(" ", "").upper()
    candidates = [f"{prefix}{normalized}{suffix}"]
    if not prefix and not suffix:
        candidates.extend([
            normalized,
            f"{normalized}.a",
            f"{normalized}.r",
            f"{normalized}m",
            f"{normalized}.m",
            f"{normalized}_i",
        ])
    unique_candidates = []
    for candidate in candidates:
        if candidate not in unique_candidates:
            unique_candidates.append(candidate)
    return unique_candidates


def initialize_mt5(config: MT5ExecutionConfig) -> Tuple[bool, str]:
    available, message = mt5_is_available()
    if not available:
        return False, message

    init_kwargs: Dict[str, Any] = {}
    if config.terminal_path:
        init_kwargs["path"] = config.terminal_path

    initialized = mt5.initialize(**init_kwargs)
    if not initialized:
        return False, mt5.last_error()[1]

    if config.login and config.password and config.server:
        authorized = mt5.login(login=int(config.login), password=config.password, server=config.server)
        if not authorized:
            error_message = mt5.last_error()[1]
            mt5.shutdown()
            return False, error_message

    return True, "verbonden"


def shutdown_mt5() -> None:
    if mt5 is not None:
        try:
            mt5.shutdown()
        except Exception:
            pass


def resolve_symbol(instrument_label: str, config: MT5ExecutionConfig) -> Tuple[Optional[str], str]:
    for candidate in build_symbol_candidates(instrument_label, prefix=config.symbol_prefix, suffix=config.symbol_suffix):
        symbol_info = mt5.symbol_info(candidate)
        if symbol_info is None:
            continue
        if not symbol_info.visible and not mt5.symbol_select(candidate, True):
            continue
        return candidate, ""
    return None, f"Geen MT5-symbool gevonden voor {instrument_label}. Controleer prefix/suffix."


def calculate_order_volume(symbol: str, entry_price: float, stop_loss: float, config: MT5ExecutionConfig, signal: Optional[Dict[str, Any]] = None) -> float:
    preferred_volume = None
    if signal is not None:
        preferred_volume = signal.get("preferred_fixed_volume")

    symbol_info = mt5.symbol_info(symbol)
    account_info = mt5.account_info()
    if symbol_info is None or account_info is None:
        fallback_volume = preferred_volume if preferred_volume is not None else config.fixed_volume
        return round(float(fallback_volume), 2)

    if preferred_volume is not None:
        volume = float(preferred_volume)
    elif not config.use_risk_sizing:
        volume = float(config.fixed_volume)
    else:
        tick_size = float(symbol_info.trade_tick_size or 0)
        tick_value = float(symbol_info.trade_tick_value or 0)
        stop_distance = abs(float(entry_price) - float(stop_loss))

        if stop_distance <= 0 or tick_size <= 0 or tick_value <= 0:
            volume = float(config.fixed_volume)
        else:
            risk_amount = float(account_info.balance) * (float(config.risk_percent) / 100.0)
            loss_per_lot = (stop_distance / tick_size) * tick_value
            if loss_per_lot <= 0:
                volume = float(config.fixed_volume)
            else:
                volume = risk_amount / loss_per_lot

    volume_min = float(symbol_info.volume_min or 0.01)
    volume_max = float(symbol_info.volume_max or max(volume_min, 100.0))
    volume_step = float(symbol_info.volume_step or 0.01)
    if volume_step <= 0:
        volume_step = 0.01

    clamped = min(max(volume, volume_min), volume_max)
    stepped = round(clamped / volume_step) * volume_step
    decimals = max(0, len(str(volume_step).split(".")[-1].rstrip("0"))) if "." in str(volume_step) else 0
    return round(max(stepped, volume_min), decimals)


def place_signal_order(signal: Dict[str, Any], instrument_label: str, config: MT5ExecutionConfig, order_comment: str = "AI Dashboard") -> MT5ExecutionResult:
    connected, message = initialize_mt5(config)
    if not connected:
        return MT5ExecutionResult(False, f"MT5 connectie mislukt: {message}")

    try:
        symbol, symbol_error = resolve_symbol(instrument_label, config)
        if not symbol:
            return MT5ExecutionResult(False, symbol_error)

        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            return MT5ExecutionResult(False, f"Geen live tick ontvangen voor {symbol}.")

        direction = str(signal.get("signal", ""))
        if direction == BUY:
            order_type = mt5.ORDER_TYPE_BUY
            price = float(tick.ask)
        elif direction == SELL:
            order_type = mt5.ORDER_TYPE_SELL
            price = float(tick.bid)
        else:
            return MT5ExecutionResult(False, f"Onbekende richting: {direction}")

        stop_loss = signal.get("stop_loss")
        take_profit = signal.get("take_profit")
        if stop_loss is None or take_profit is None:
            return MT5ExecutionResult(False, "Signaal mist SL of TP.")

        volume = calculate_order_volume(symbol, price, float(stop_loss), config, signal=signal)
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(volume),
            "type": order_type,
            "price": price,
            "sl": float(stop_loss),
            "tp": float(take_profit),
            "deviation": int(config.max_deviation),
            "magic": int(config.magic_number),
            "comment": str(order_comment)[:31],
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }

        result = mt5.order_send(request)
        if result is None:
            return MT5ExecutionResult(False, f"MT5 order_send gaf geen resultaat terug: {mt5.last_error()[1]}", volume=volume, symbol=symbol)

        retcode = getattr(result, "retcode", None)
        if retcode != mt5.TRADE_RETCODE_DONE:
            return MT5ExecutionResult(False, f"MT5 order afgewezen ({retcode}): {getattr(result, 'comment', 'onbekende fout')}", volume=volume, symbol=symbol)

        return MT5ExecutionResult(True, "order geplaatst", order_ticket=getattr(result, "order", None) or getattr(result, "deal", None), volume=volume, symbol=symbol)
    finally:
        shutdown_mt5()
