from dataclasses import dataclass
from datetime import datetime, timezone
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urlparse

import pandas as pd
import requests

try:
    from ctrader_open_api import Auth, Client, EndPoints, Protobuf, TcpProtocol
    from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import ProtoHeartbeatEvent
    from ctrader_open_api.messages.OpenApiMessages_pb2 import (
        ProtoOAAccountAuthReq,
        ProtoOAApplicationAuthReq,
        ProtoOAErrorRes,
        ProtoOAExecutionEvent,
        ProtoOAGetAccountListByAccessTokenReq,
        ProtoOAGetPositionUnrealizedPnLReq,
        ProtoOAGetTrendbarsReq,
        ProtoOANewOrderReq,
        ProtoOASpotEvent,
        ProtoOAReconcileReq,
        ProtoOAOrderErrorEvent,
        ProtoOASubscribeSpotsReq,
        ProtoOASymbolsListReq,
        ProtoOATraderReq,
    )
    from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOAOrderType, ProtoOATradeSide, ProtoOATrendbarPeriod
    from twisted.internet import defer, reactor
except Exception as import_error:
    Auth = None
    Client = None
    EndPoints = None
    Protobuf = None
    TcpProtocol = None
    ProtoHeartbeatEvent = None
    ProtoOAAccountAuthReq = None
    ProtoOAApplicationAuthReq = None
    ProtoOAErrorRes = None
    ProtoOAExecutionEvent = None
    ProtoOAGetAccountListByAccessTokenReq = None
    ProtoOAGetPositionUnrealizedPnLReq = None
    ProtoOAGetTrendbarsReq = None
    ProtoOANewOrderReq = None
    ProtoOASpotEvent = None
    ProtoOAReconcileReq = None
    ProtoOAOrderErrorEvent = None
    ProtoOASubscribeSpotsReq = None
    ProtoOASymbolsListReq = None
    ProtoOATraderReq = None
    ProtoOAOrderType = None
    ProtoOATradeSide = None
    ProtoOATrendbarPeriod = None
    defer = None
    reactor = None
    CTRADER_IMPORT_ERROR = str(import_error)
else:
    CTRADER_IMPORT_ERROR = ""


BUY = "Buy"
SELL = "Sell"
_REACTOR_THREAD: Optional[threading.Thread] = None
_REACTOR_LOCK = threading.Lock()
_CTRADER_OPERATION_TIMEOUT_SECONDS = 25.0
_CTRADER_RETRY_ATTEMPTS = 2
_CTRADER_RETRY_SLEEP_SECONDS = 1.0


@dataclass
class CTraderExecutionConfig:
    client_id: str
    client_secret: str
    access_token: str
    ctid_trader_account_id: Optional[int]
    environment: str = "demo"
    refresh_token: str = ""
    redirect_uri: str = "http://localhost:8502"
    fixed_units: int = 1000
    use_risk_sizing: bool = False
    slippage_points: int = 20


@dataclass
class CTraderExecutionResult:
    success: bool
    message: str
    order_ticket: Optional[str] = None
    volume: Optional[float] = None
    symbol: str = ""
    updated_access_token: str = ""
    updated_refresh_token: str = ""


def ctrader_is_available() -> Tuple[bool, str]:
    if Client is None or reactor is None:
        return False, CTRADER_IMPORT_ERROR or "cTrader Open API package is niet beschikbaar in deze Python-omgeving."
    return True, ""


def get_ctrader_auth_uri(config: CTraderExecutionConfig, scope: str = "trading") -> str:
    client_id = str(config.client_id).strip()
    redirect_uri = str(config.redirect_uri).strip()
    if not client_id or not redirect_uri:
        return ""

    query_string = urlencode(
        {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": scope,
            "product": "web",
        }
    )
    return f"https://id.ctrader.com/my/settings/openapi/grantingaccess/?{query_string}"


def _extract_auth_code(raw_value: str) -> str:
    raw_value = str(raw_value).strip()
    if not raw_value:
        return ""
    if "code=" not in raw_value:
        return raw_value

    parsed = urlparse(raw_value)
    if parsed.query:
        query_code = parse_qs(parsed.query).get("code")
        if query_code:
            return str(query_code[0]).strip()

    if "code=" in raw_value:
        return raw_value.split("code=", 1)[1].split("&", 1)[0].strip()
    return raw_value


def exchange_auth_code(config: CTraderExecutionConfig, auth_code: str) -> Dict[str, Any]:
    auth_code = _extract_auth_code(auth_code)
    if not auth_code:
        return {"error": "Geen auth code opgegeven."}

    client_id = str(config.client_id).strip()
    client_secret = str(config.client_secret).strip()
    redirect_uri = str(config.redirect_uri).strip()
    if not client_id or not client_secret or not redirect_uri:
        return {"error": "Client ID, client secret en redirect URI zijn verplicht voor cTrader OAuth."}

    request_params = {
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "client_secret": client_secret,
    }

    try:
        response = requests.get(
            "https://openapi.ctrader.com/apps/token",
            params=request_params,
            timeout=20,
        )
        payload = response.json()
    except Exception as exc:
        return {"error": str(exc)}

    if payload.get("accessToken") or payload.get("access_token"):
        return payload

    try:
        fallback_response = requests.post(
            "https://openapi.ctrader.com/apps/token",
            params=request_params,
            timeout=20,
        )
        fallback_payload = fallback_response.json()
    except Exception:
        fallback_payload = {}

    if fallback_payload.get("accessToken") or fallback_payload.get("access_token"):
        return fallback_payload

    error_message = (
        payload.get("description")
        or payload.get("error")
        or payload.get("errorCode")
        or fallback_payload.get("description")
        or fallback_payload.get("error")
        or fallback_payload.get("errorCode")
        or f"Token exchange failed (HTTP {response.status_code})"
    )
    return {
        "error": error_message,
        "response": payload,
        "fallback_response": fallback_payload,
    }


def refresh_access_token(config: CTraderExecutionConfig) -> Dict[str, Any]:
    refresh_token = str(config.refresh_token).strip()
    if not refresh_token:
        return {"error": "Geen refresh token beschikbaar."}

    client_id = str(config.client_id).strip()
    client_secret = str(config.client_secret).strip()
    if not client_id or not client_secret:
        return {"error": "Client ID en client secret zijn verplicht om een cTrader token te verversen."}

    try:
        response = requests.get(
            "https://openapi.ctrader.com/apps/token",
            params={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=20,
        )
        return response.json()
    except Exception as exc:
        return {"error": str(exc)}


def calculate_order_units(signal: Dict[str, Any], config: CTraderExecutionConfig) -> int:
    preferred_units = signal.get("preferred_fixed_units")
    if preferred_units is not None:
        return max(int(preferred_units), 1)
    return max(int(config.fixed_units), 1)


def _decode_money_value(raw_value: Any, digits: Optional[int] = None) -> Optional[float]:
    if raw_value is None or raw_value == "":
        return None
    try:
        numeric_value = float(raw_value)
    except Exception:
        return None
    if digits is None:
        return numeric_value
    try:
        return numeric_value / (10 ** int(digits))
    except Exception:
        return numeric_value


def _timestamp_ms_to_iso(timestamp_ms: Any) -> str:
    try:
        timestamp_int = int(timestamp_ms)
    except Exception:
        return ""
    if timestamp_int <= 0:
        return ""
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp_int / 1000.0))
    except Exception:
        return str(timestamp_ms)


def _enum_name(enum_type: Any, value: Any) -> str:
    if enum_type is None or value is None:
        return str(value or "")
    try:
        return str(enum_type.Name(int(value)))
    except Exception:
        return str(value)


def _build_symbol_name_map(symbols_message: Any) -> Dict[int, str]:
    symbol_map: Dict[int, str] = {}
    for symbol in getattr(symbols_message, "symbol", []):
        symbol_id = getattr(symbol, "symbolId", None)
        symbol_name = getattr(symbol, "symbolName", "")
        if symbol_id is None:
            continue
        symbol_map[int(symbol_id)] = str(symbol_name or "").strip()
    return symbol_map


def _decode_price_value(raw_value: Any) -> Optional[float]:
    if raw_value is None or raw_value == "":
        return None
    try:
        return float(raw_value) / 100000.0
    except Exception:
        return None


def _trendbar_timestamp_to_datetime(timestamp_minutes: Any) -> Optional[datetime]:
    try:
        timestamp_int = int(timestamp_minutes)
    except Exception:
        return None
    if timestamp_int <= 0:
        return None
    try:
        return datetime.fromtimestamp(timestamp_int * 60, tz=timezone.utc)
    except Exception:
        return None


def _trendbar_period_value(label: str) -> Any:
    normalized = str(label or "").strip().upper()
    if ProtoOATrendbarPeriod is None or not normalized:
        return None
    try:
        return ProtoOATrendbarPeriod.Value(normalized)
    except Exception:
        return None


def _timeframe_seconds(label: str) -> Optional[int]:
    normalized = str(label or "").strip().upper()
    mapping = {
        "M1": 60,
        "M5": 5 * 60,
        "M15": 15 * 60,
        "M30": 30 * 60,
        "H1": 60 * 60,
        "H4": 4 * 60 * 60,
        "D1": 24 * 60 * 60,
    }
    return mapping.get(normalized)


def get_recent_trendbars(
    config: CTraderExecutionConfig,
    instrument_label: str,
    timeframe_label: str = "M1",
    count: int = 500,
    to_timestamp_ms: Optional[int] = None,
    from_timestamp_ms: Optional[int] = None,
) -> Tuple[bool, Any]:
    available, message = ctrader_is_available()
    if not available:
        return False, f"cTrader connectie mislukt: {message}"

    if not str(config.client_id).strip() or not str(config.client_secret).strip():
        return False, "cTrader client ID en client secret zijn verplicht."
    if not str(config.access_token).strip():
        return False, "cTrader access token ontbreekt."
    resolved_ok, resolved_payload = _resolve_authorized_account_id(config)
    if not resolved_ok:
        return False, f"cTrader account ID ontbreekt of is ongeldig: {resolved_payload}"
    account_id = int(resolved_payload.get("account_id") or config.ctid_trader_account_id)

    trendbar_period = _trendbar_period_value(timeframe_label)
    if trendbar_period is None:
        return False, f"Niet-ondersteunde cTrader timeframe: {timeframe_label}"

    host = EndPoints.PROTOBUF_LIVE_HOST if str(config.environment).strip().lower() == "live" else EndPoints.PROTOBUF_DEMO_HOST

    try:
        def workflow():
            client = Client(host, EndPoints.PROTOBUF_PORT, TcpProtocol)
            operation = defer.Deferred()
            state = {"finished": False}

            def stop_client() -> None:
                try:
                    client.stopService()
                except Exception:
                    pass

            def finish(value: Any = None, error: Any = None) -> None:
                if state["finished"]:
                    return
                state["finished"] = True
                stop_client()
                if error is not None:
                    if isinstance(error, Exception):
                        operation.errback(error)
                    else:
                        operation.errback(RuntimeError(str(error)))
                else:
                    operation.callback(value)

            def on_failure(failure):
                finish(error=_failure_message(failure))
                return failure

            def on_connected(_: Any) -> None:
                request = ProtoOAApplicationAuthReq()
                request.clientId = str(config.client_id).strip()
                request.clientSecret = str(config.client_secret).strip()
                deferred = client.send(request, responseTimeoutInSeconds=10)
                deferred.addCallback(on_application_auth)
                deferred.addErrback(on_failure)

            def on_disconnected(_: Any, reason: Any) -> None:
                finish(error=f"cTrader disconnected: {reason}")

            def on_message_received(_: Any, message: Any) -> None:
                if message.payloadType == ProtoHeartbeatEvent().payloadType:
                    return

            def on_application_auth(message: Any):
                extracted = Protobuf.extract(message)
                if isinstance(extracted, ProtoOAErrorRes):
                    raise RuntimeError(_format_ctrader_credential_error("application auth", extracted.description or extracted.errorCode, config))

                request = ProtoOAAccountAuthReq()
                request.ctidTraderAccountId = account_id
                request.accessToken = str(config.access_token).strip()
                deferred = client.send(request, responseTimeoutInSeconds=10)
                deferred.addCallback(on_account_auth)
                deferred.addErrback(on_failure)
                return deferred

            def on_account_auth(message: Any):
                extracted = Protobuf.extract(message)
                if isinstance(extracted, ProtoOAErrorRes):
                    raise RuntimeError(_format_ctrader_credential_error("account auth", extracted.description or extracted.errorCode, config))

                request = ProtoOASymbolsListReq()
                request.ctidTraderAccountId = account_id
                request.includeArchivedSymbols = False
                deferred = client.send(request, responseTimeoutInSeconds=15)
                deferred.addCallback(on_symbols_loaded)
                deferred.addErrback(on_failure)
                return deferred

            def on_symbols_loaded(message: Any):
                extracted = Protobuf.extract(message)
                if isinstance(extracted, ProtoOAErrorRes):
                    raise RuntimeError(f"cTrader symbols fout: {extracted.description or extracted.errorCode}")

                symbol_id, resolved_symbol = _find_symbol_id(extracted, instrument_label)
                if symbol_id is None:
                    raise RuntimeError(resolved_symbol)

                request = ProtoOAGetTrendbarsReq()
                request.ctidTraderAccountId = account_id
                request.symbolId = int(symbol_id)
                request.period = trendbar_period
                request.toTimestamp = int(to_timestamp_ms) if to_timestamp_ms is not None else int(time.time() * 1000)
                request.count = max(int(count), 10)
                timeframe_seconds = _timeframe_seconds(timeframe_label)
                if timeframe_seconds is None:
                    raise RuntimeError(f"Niet-ondersteunde cTrader timeframe voor history range: {timeframe_label}")
                request.fromTimestamp = (
                    int(from_timestamp_ms)
                    if from_timestamp_ms is not None
                    else int(request.toTimestamp - (request.count * timeframe_seconds * 1000))
                )
                deferred = client.send(request, responseTimeoutInSeconds=20)
                deferred.addCallback(lambda trendbars_message: on_trendbars_loaded(trendbars_message, resolved_symbol, symbol_id))
                deferred.addErrback(on_failure)
                return deferred

            def on_trendbars_loaded(message: Any, resolved_symbol: str, symbol_id: int):
                extracted = Protobuf.extract(message)
                if isinstance(extracted, ProtoOAErrorRes):
                    raise RuntimeError(f"cTrader trendbars fout: {extracted.description or extracted.errorCode}")

                records: List[Dict[str, Any]] = []
                for trendbar in getattr(extracted, "trendbar", []):
                    low = _decode_price_value(getattr(trendbar, "low", None))
                    delta_open = _decode_price_value(getattr(trendbar, "deltaOpen", None))
                    delta_close = _decode_price_value(getattr(trendbar, "deltaClose", None))
                    delta_high = _decode_price_value(getattr(trendbar, "deltaHigh", None))
                    bar_time = _trendbar_timestamp_to_datetime(getattr(trendbar, "utcTimestampInMinutes", None))
                    if low is None or delta_open is None or delta_close is None or delta_high is None or bar_time is None:
                        continue

                    open_price = low + delta_open
                    close_price = low + delta_close
                    high_price = low + delta_high
                    records.append(
                        {
                            "Datetime": pd.Timestamp(bar_time).tz_convert("Europe/Amsterdam").tz_localize(None),
                            "Open": float(round(open_price, 5)),
                            "High": float(round(high_price, 5)),
                            "Low": float(round(low, 5)),
                            "Close": float(round(close_price, 5)),
                            "Volume": float(getattr(trendbar, "volume", 0) or 0),
                        }
                    )

                finish(
                    value={
                        "instrument": resolved_symbol,
                        "symbol_id": int(symbol_id),
                        "timeframe": str(timeframe_label).upper(),
                        "bars": sorted(records, key=lambda item: item["Datetime"]),
                    }
                )
                return extracted

            client.setConnectedCallback(on_connected)
            client.setDisconnectedCallback(on_disconnected)
            client.setMessageReceivedCallback(on_message_received)
            client.startService()

            return operation

        payload = _run_ctrader_operation(workflow, config=config)
    except Exception as exc:
        return False, f"cTrader candles ophalen mislukt: {exc}"

    return True, payload


def get_spot_snapshot(config: CTraderExecutionConfig, instrument_label: str) -> Tuple[bool, Any]:
    available, message = ctrader_is_available()
    if not available:
        return False, f"cTrader connectie mislukt: {message}"

    if not str(config.client_id).strip() or not str(config.client_secret).strip():
        return False, "cTrader client ID en client secret zijn verplicht."
    if not str(config.access_token).strip():
        return False, "cTrader access token ontbreekt."
    resolved_ok, resolved_payload = _resolve_authorized_account_id(config)
    if not resolved_ok:
        return False, f"cTrader account ID ontbreekt of is ongeldig: {resolved_payload}"
    account_id = int(resolved_payload.get("account_id") or config.ctid_trader_account_id)

    host = EndPoints.PROTOBUF_LIVE_HOST if str(config.environment).strip().lower() == "live" else EndPoints.PROTOBUF_DEMO_HOST

    try:
        def workflow():
            client = Client(host, EndPoints.PROTOBUF_PORT, TcpProtocol)
            operation = defer.Deferred()
            state = {
                "finished": False,
                "symbol_id": None,
                "resolved_symbol": instrument_label,
            }

            def stop_client() -> None:
                try:
                    client.stopService()
                except Exception:
                    pass

            def finish(value: Any = None, error: Any = None) -> None:
                if state["finished"]:
                    return
                state["finished"] = True
                stop_client()
                if error is not None:
                    if isinstance(error, Exception):
                        operation.errback(error)
                    else:
                        operation.errback(RuntimeError(str(error)))
                else:
                    operation.callback(value)

            def on_failure(failure):
                finish(error=_failure_message(failure))
                return failure

            def on_connected(_: Any) -> None:
                request = ProtoOAApplicationAuthReq()
                request.clientId = str(config.client_id).strip()
                request.clientSecret = str(config.client_secret).strip()
                deferred = client.send(request, responseTimeoutInSeconds=10)
                deferred.addCallback(on_application_auth)
                deferred.addErrback(on_failure)

            def on_disconnected(_: Any, reason: Any) -> None:
                finish(error=f"cTrader disconnected: {reason}")

            def on_message_received(_: Any, message: Any) -> None:
                if message.payloadType == ProtoHeartbeatEvent().payloadType:
                    return
                extracted = Protobuf.extract(message)
                if isinstance(extracted, ProtoOASpotEvent):
                    event_symbol_id = getattr(extracted, "symbolId", None)
                    if state.get("symbol_id") is None or event_symbol_id != state.get("symbol_id"):
                        return
                    bid = _decode_price_value(getattr(extracted, "bid", None))
                    ask = _decode_price_value(getattr(extracted, "ask", None))
                    if bid is None and ask is None:
                        return
                    spread = None
                    if bid is not None and ask is not None:
                        spread = round((ask - bid) / 0.0001, 2)
                    finish(
                        value={
                            "instrument": state.get("resolved_symbol", instrument_label),
                            "symbol_id": int(event_symbol_id) if event_symbol_id is not None else None,
                            "bid": round(float(bid), 5) if bid is not None else None,
                            "ask": round(float(ask), 5) if ask is not None else None,
                            "spread_pips": spread,
                            "timestamp": _timestamp_ms_to_iso(getattr(extracted, "timestamp", None)),
                        }
                    )

            def on_application_auth(message: Any):
                extracted = Protobuf.extract(message)
                if isinstance(extracted, ProtoOAErrorRes):
                    raise RuntimeError(_format_ctrader_credential_error("application auth", extracted.description or extracted.errorCode, config))

                request = ProtoOAAccountAuthReq()
                request.ctidTraderAccountId = account_id
                request.accessToken = str(config.access_token).strip()
                deferred = client.send(request, responseTimeoutInSeconds=10)
                deferred.addCallback(on_account_auth)
                deferred.addErrback(on_failure)
                return deferred

            def on_account_auth(message: Any):
                extracted = Protobuf.extract(message)
                if isinstance(extracted, ProtoOAErrorRes):
                    raise RuntimeError(_format_ctrader_credential_error("account auth", extracted.description or extracted.errorCode, config))

                request = ProtoOASymbolsListReq()
                request.ctidTraderAccountId = account_id
                request.includeArchivedSymbols = False
                deferred = client.send(request, responseTimeoutInSeconds=15)
                deferred.addCallback(on_symbols_loaded)
                deferred.addErrback(on_failure)
                return deferred

            def on_symbols_loaded(message: Any):
                extracted = Protobuf.extract(message)
                if isinstance(extracted, ProtoOAErrorRes):
                    raise RuntimeError(f"cTrader symbols fout: {extracted.description or extracted.errorCode}")

                symbol_id, resolved_symbol = _find_symbol_id(extracted, instrument_label)
                if symbol_id is None:
                    raise RuntimeError(resolved_symbol)
                state["symbol_id"] = int(symbol_id)
                state["resolved_symbol"] = resolved_symbol

                request = ProtoOASubscribeSpotsReq()
                request.ctidTraderAccountId = account_id
                request.symbolId.append(int(symbol_id))
                request.subscribeToSpotTimestamp = True
                deferred = client.send(request, responseTimeoutInSeconds=10)
                deferred.addErrback(on_failure)
                return deferred

            client.setConnectedCallback(on_connected)
            client.setDisconnectedCallback(on_disconnected)
            client.setMessageReceivedCallback(on_message_received)
            client.startService()

            return operation

        payload = _run_ctrader_operation(workflow, config=config, timeout_seconds=25)
    except Exception as exc:
        return False, f"cTrader bid/ask ophalen mislukt: {exc}"

    return True, payload


def get_account_snapshot(config: CTraderExecutionConfig) -> Tuple[bool, Any]:
    available, message = ctrader_is_available()
    if not available:
        return False, f"cTrader connectie mislukt: {message}"

    if not str(config.client_id).strip() or not str(config.client_secret).strip():
        return False, "cTrader client ID en client secret zijn verplicht."
    if not str(config.access_token).strip():
        return False, "cTrader access token ontbreekt."
    resolved_ok, resolved_payload = _resolve_authorized_account_id(config)
    if not resolved_ok:
        return False, f"cTrader account ID ontbreekt of is ongeldig: {resolved_payload}"
    account_id = int(resolved_payload.get("account_id") or config.ctid_trader_account_id)

    host = EndPoints.PROTOBUF_LIVE_HOST if str(config.environment).strip().lower() == "live" else EndPoints.PROTOBUF_DEMO_HOST

    try:
        def workflow():
            client = Client(host, EndPoints.PROTOBUF_PORT, TcpProtocol)
            operation = defer.Deferred()
            state = {
                "finished": False,
                "symbol_map": {},
                "trader_payload": None,
                "reconcile_payload": None,
            }

            def stop_client() -> None:
                try:
                    client.stopService()
                except Exception:
                    pass

            def finish(value: Any = None, error: Any = None) -> None:
                if state["finished"]:
                    return
                state["finished"] = True
                stop_client()
                if error is not None:
                    if isinstance(error, Exception):
                        operation.errback(error)
                    else:
                        operation.errback(RuntimeError(str(error)))
                else:
                    operation.callback(value)

            def on_failure(failure):
                finish(error=_failure_message(failure))
                return failure

            def on_connected(_: Any) -> None:
                request = ProtoOAApplicationAuthReq()
                request.clientId = str(config.client_id).strip()
                request.clientSecret = str(config.client_secret).strip()
                deferred = client.send(request, responseTimeoutInSeconds=10)
                deferred.addCallback(on_application_auth)
                deferred.addErrback(on_failure)

            def on_disconnected(_: Any, reason: Any) -> None:
                finish(error=f"cTrader disconnected: {reason}")

            def on_message_received(_: Any, message: Any) -> None:
                if message.payloadType == ProtoHeartbeatEvent().payloadType:
                    return

            def on_application_auth(message: Any):
                extracted = Protobuf.extract(message)
                if isinstance(extracted, ProtoOAErrorRes):
                    raise RuntimeError(_format_ctrader_credential_error("application auth", extracted.description or extracted.errorCode, config))

                request = ProtoOAAccountAuthReq()
                request.ctidTraderAccountId = account_id
                request.accessToken = str(config.access_token).strip()
                deferred = client.send(request, responseTimeoutInSeconds=10)
                deferred.addCallback(on_account_auth)
                deferred.addErrback(on_failure)
                return deferred

            def on_account_auth(message: Any):
                extracted = Protobuf.extract(message)
                if isinstance(extracted, ProtoOAErrorRes):
                    raise RuntimeError(_format_ctrader_credential_error("account auth", extracted.description or extracted.errorCode, config))

                request = ProtoOASymbolsListReq()
                request.ctidTraderAccountId = account_id
                request.includeArchivedSymbols = False
                deferred = client.send(request, responseTimeoutInSeconds=15)
                deferred.addCallback(on_symbols_loaded)
                deferred.addErrback(on_failure)
                return deferred

            def on_symbols_loaded(message: Any):
                extracted = Protobuf.extract(message)
                if isinstance(extracted, ProtoOAErrorRes):
                    raise RuntimeError(f"cTrader symbols fout: {extracted.description or extracted.errorCode}")
                state["symbol_map"] = _build_symbol_name_map(extracted)

                request = ProtoOATraderReq()
                request.ctidTraderAccountId = account_id
                deferred = client.send(request, responseTimeoutInSeconds=10)
                deferred.addCallback(on_trader_loaded)
                deferred.addErrback(on_failure)
                return deferred

            def on_trader_loaded(message: Any):
                extracted = Protobuf.extract(message)
                if isinstance(extracted, ProtoOAErrorRes):
                    raise RuntimeError(f"cTrader trader fout: {extracted.description or extracted.errorCode}")
                state["trader_payload"] = getattr(extracted, "trader", None)

                request = ProtoOAReconcileReq()
                request.ctidTraderAccountId = account_id
                request.returnProtectionOrders = True
                deferred = client.send(request, responseTimeoutInSeconds=15)
                deferred.addCallback(on_reconcile_loaded)
                deferred.addErrback(on_failure)
                return deferred

            def on_reconcile_loaded(message: Any):
                extracted = Protobuf.extract(message)
                if isinstance(extracted, ProtoOAErrorRes):
                    raise RuntimeError(f"cTrader reconcile fout: {extracted.description or extracted.errorCode}")
                state["reconcile_payload"] = extracted

                request = ProtoOAGetPositionUnrealizedPnLReq()
                request.ctidTraderAccountId = account_id
                deferred = client.send(request, responseTimeoutInSeconds=10)
                deferred.addCallback(on_pnl_loaded)
                deferred.addErrback(on_failure)
                return deferred

            def on_pnl_loaded(message: Any):
                extracted = Protobuf.extract(message)
                if isinstance(extracted, ProtoOAErrorRes):
                    raise RuntimeError(f"cTrader PnL fout: {extracted.description or extracted.errorCode}")

                pnl_digits = getattr(extracted, "moneyDigits", None)
                pnl_map = {}
                for item in getattr(extracted, "positionUnrealizedPnL", []):
                    position_id = getattr(item, "positionId", None)
                    if position_id is None:
                        continue
                    pnl_map[int(position_id)] = {
                        "gross_unrealized_pnl": _decode_money_value(getattr(item, "grossUnrealizedPnL", None), pnl_digits),
                        "net_unrealized_pnl": _decode_money_value(getattr(item, "netUnrealizedPnL", None), pnl_digits),
                    }

                trader = state.get("trader_payload")
                reconcile_payload = state.get("reconcile_payload")
                symbol_map = state.get("symbol_map", {})

                balance = None
                equity = None
                margin = None
                free_margin = None
                account_currency = ""
                money_digits = getattr(trader, "moneyDigits", None) if trader is not None else None
                if trader is not None:
                    balance = _decode_money_value(getattr(trader, "balance", None), money_digits)
                    equity = _decode_money_value(getattr(trader, "equity", None), money_digits)
                    margin = _decode_money_value(getattr(trader, "margin", None), money_digits)
                    free_margin = _decode_money_value(getattr(trader, "freeMargin", None), money_digits)
                    account_currency = str(getattr(trader, "depositAssetName", "") or getattr(trader, "accountCurrency", "") or "").strip()

                positions = []
                for position in getattr(reconcile_payload, "position", []):
                    position_id = getattr(position, "positionId", None)
                    symbol_id = getattr(position, "symbolId", None)
                    position_pnl = pnl_map.get(int(position_id), {}) if position_id is not None else {}
                    positions.append(
                        {
                            "position_id": int(position_id) if position_id is not None else None,
                            "symbol": symbol_map.get(int(symbol_id), str(symbol_id or "")),
                            "side": _enum_name(ProtoOATradeSide, getattr(position, "tradeSide", None)),
                            "units": round(float(getattr(position, "volume", 0) or 0) / 100.0, 2),
                            "entry_price": float(getattr(position, "price", getattr(position, "entryPrice", 0.0)) or 0.0),
                            "stop_loss": float(getattr(position, "stopLoss", 0.0) or 0.0),
                            "take_profit": float(getattr(position, "takeProfit", 0.0) or 0.0),
                            "gross_unrealized_pnl": position_pnl.get("gross_unrealized_pnl"),
                            "net_unrealized_pnl": position_pnl.get("net_unrealized_pnl"),
                            "updated_at": _timestamp_ms_to_iso(getattr(position, "utcLastUpdateTimestamp", None)),
                            "comment": str(getattr(position, "comment", "") or "").strip(),
                        }
                    )

                pending_orders = []
                for order in getattr(reconcile_payload, "order", []):
                    order_id = getattr(order, "orderId", None)
                    symbol_id = getattr(order, "symbolId", None)
                    pending_orders.append(
                        {
                            "order_id": int(order_id) if order_id is not None else None,
                            "symbol": symbol_map.get(int(symbol_id), str(symbol_id or "")),
                            "order_type": _enum_name(ProtoOAOrderType, getattr(order, "orderType", None)),
                            "side": _enum_name(ProtoOATradeSide, getattr(order, "tradeSide", None)),
                            "units": round(float(getattr(order, "volume", 0) or 0) / 100.0, 2),
                            "limit_price": float(getattr(order, "limitPrice", 0.0) or 0.0),
                            "stop_price": float(getattr(order, "stopPrice", 0.0) or 0.0),
                            "stop_loss": float(getattr(order, "stopLoss", 0.0) or 0.0),
                            "take_profit": float(getattr(order, "takeProfit", 0.0) or 0.0),
                            "updated_at": _timestamp_ms_to_iso(getattr(order, "utcLastUpdateTimestamp", None)),
                            "comment": str(getattr(order, "comment", "") or "").strip(),
                        }
                    )

                finish(
                    value={
                        "summary": {
                            "account_id": account_id,
                            "environment": str(config.environment).strip().lower() or "demo",
                            "account_currency": account_currency,
                            "balance": balance,
                            "equity": equity,
                            "margin": margin,
                            "free_margin": free_margin,
                            "open_positions": len(positions),
                            "pending_orders": len(pending_orders),
                            "fetched_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                        },
                        "positions": positions,
                        "pending_orders": pending_orders,
                    }
                )
                return extracted

            client.setConnectedCallback(on_connected)
            client.setDisconnectedCallback(on_disconnected)
            client.setMessageReceivedCallback(on_message_received)
            client.startService()

            return operation

        snapshot = _run_ctrader_operation(workflow, config=config)
    except Exception as exc:
        return False, f"cTrader monitor ophalen mislukt: {exc}"

    return True, snapshot


def list_authorized_accounts(config: CTraderExecutionConfig) -> Tuple[bool, Any]:
    available, message = ctrader_is_available()
    if not available:
        return False, f"cTrader connectie mislukt: {message}"

    if not str(config.client_id).strip() or not str(config.client_secret).strip():
        return False, "cTrader client ID en client secret zijn verplicht."
    if not str(config.access_token).strip():
        return False, "cTrader access token ontbreekt."

    host = EndPoints.PROTOBUF_LIVE_HOST if str(config.environment).strip().lower() == "live" else EndPoints.PROTOBUF_DEMO_HOST

    try:
        def workflow():
            client = Client(host, EndPoints.PROTOBUF_PORT, TcpProtocol)
            operation = defer.Deferred()
            state = {"finished": False}

            def stop_client() -> None:
                try:
                    client.stopService()
                except Exception:
                    pass

            def finish(value: Any = None, error: Any = None) -> None:
                if state["finished"]:
                    return
                state["finished"] = True
                stop_client()
                if error is not None:
                    if isinstance(error, Exception):
                        operation.errback(error)
                    else:
                        operation.errback(RuntimeError(str(error)))
                else:
                    operation.callback(value)

            def on_failure(failure):
                finish(error=_failure_message(failure))
                return failure

            def on_connected(_: Any) -> None:
                request = ProtoOAApplicationAuthReq()
                request.clientId = str(config.client_id).strip()
                request.clientSecret = str(config.client_secret).strip()
                deferred = client.send(request, responseTimeoutInSeconds=10)
                deferred.addCallback(on_application_auth)
                deferred.addErrback(on_failure)

            def on_disconnected(_: Any, reason: Any) -> None:
                finish(error=f"cTrader disconnected: {reason}")

            def on_message_received(_: Any, message: Any) -> None:
                if message.payloadType == ProtoHeartbeatEvent().payloadType:
                    return

            def on_application_auth(message: Any):
                extracted = Protobuf.extract(message)
                if isinstance(extracted, ProtoOAErrorRes):
                    raise RuntimeError(_format_ctrader_credential_error("application auth", extracted.description or extracted.errorCode, config))

                request = ProtoOAGetAccountListByAccessTokenReq()
                request.accessToken = str(config.access_token).strip()
                deferred = client.send(request, responseTimeoutInSeconds=15)
                deferred.addCallback(on_account_list)
                deferred.addErrback(on_failure)
                return deferred

            def on_account_list(message: Any):
                extracted = Protobuf.extract(message)
                if isinstance(extracted, ProtoOAErrorRes):
                    raise RuntimeError(_format_ctrader_credential_error("account list", extracted.description or extracted.errorCode, config))

                accounts = []
                for account in getattr(extracted, "ctidTraderAccount", []):
                    account_id = getattr(account, "ctidTraderAccountId", None) or getattr(account, "accountId", None)
                    broker_name = (
                        getattr(account, "brokerTitle", "")
                        or getattr(account, "brokerName", "")
                        or getattr(account, "brokerAccountDisplayName", "")
                    )
                    trader_login = getattr(account, "traderLogin", "") or getattr(account, "accountNumber", "")
                    account_type = getattr(account, "accountType", "") or getattr(account, "traderAccountType", "")
                    is_live = getattr(account, "isLive", None)
                    is_real = getattr(account, "isReal", None)
                    live_value = getattr(account, "live", None)
                    live_flag = is_live if is_live is not None else is_real
                    if live_flag is None:
                        live_flag = live_value
                    accounts.append(
                        {
                            "account_id": int(account_id) if account_id is not None else None,
                            "broker_name": str(broker_name or "").strip(),
                            "trader_login": str(trader_login or "").strip(),
                            "account_type": str(account_type or "").strip(),
                            "environment": "live" if bool(live_flag) else "demo",
                        }
                    )

                finish(value=accounts)
                return extracted

            client.setConnectedCallback(on_connected)
            client.setDisconnectedCallback(on_disconnected)
            client.setMessageReceivedCallback(on_message_received)
            client.startService()
            return operation

        accounts = _run_ctrader_operation(workflow, config=config)
    except Exception as exc:
        return False, f"cTrader accountlijst ophalen mislukt: {exc}"

    return True, accounts


def _resolve_authorized_account_id(config: CTraderExecutionConfig) -> Tuple[bool, Any]:
    configured_value = str(config.ctid_trader_account_id or "").strip()
    if configured_value and configured_value.isdigit() and len(configured_value) >= 8:
        return True, {
            "account_id": int(configured_value),
            "trader_login": "",
            "environment": str(config.environment or "").strip().lower() or "demo",
        }

    ok, payload = list_authorized_accounts(config)
    if not ok:
        return False, payload

    accounts = payload if isinstance(payload, list) else []
    if not accounts:
        return False, "Geen geautoriseerde cTrader accounts gevonden voor deze token."

    target_environment = str(config.environment or "").strip().lower()
    environment_matches = [
        account for account in accounts
        if str(account.get("environment") or "").strip().lower() == target_environment
    ]
    candidates = environment_matches or accounts

    if configured_value:
        for account in candidates:
            account_id = str(account.get("account_id") or "").strip()
            trader_login = str(account.get("trader_login") or "").strip()
            if configured_value == account_id or configured_value == trader_login:
                config.ctid_trader_account_id = int(account_id)
                resolved_environment = str(account.get("environment") or "").strip().lower()
                if resolved_environment in {"demo", "live"}:
                    config.environment = resolved_environment
                return True, {
                    "account_id": int(account_id),
                    "trader_login": trader_login,
                    "environment": config.environment,
                }

    if len(candidates) == 1:
        account = candidates[0]
        account_id = int(account.get("account_id"))
        config.ctid_trader_account_id = account_id
        resolved_environment = str(account.get("environment") or "").strip().lower()
        if resolved_environment in {"demo", "live"}:
            config.environment = resolved_environment
        return True, {
            "account_id": account_id,
            "trader_login": str(account.get("trader_login") or "").strip(),
            "environment": config.environment,
        }

    return False, "Meerdere geautoriseerde cTrader accounts gevonden; kies het juiste cTID account ID in de app."


def _normalize_symbol_name(value: Any) -> str:
    return (
        str(value or "")
        .replace("/", "")
        .replace("_", "")
        .replace(" ", "")
        .replace(".", "")
        .upper()
    )


def _failure_message(failure: Any) -> str:
    try:
        return failure.getErrorMessage()
    except Exception:
        return str(failure)


def _is_token_related_error(error: Any) -> bool:
    message = str(error or "").strip().lower()
    if not message:
        return False
    token_markers = [
        "access denied",
        "invalid token",
        "invalid access token",
        "token expired",
        "token is expired",
        "not authorized",
        "unauthorized",
        "authorization",
        "oauth",
    ]
    return any(marker in message for marker in token_markers)


def _is_transient_connection_error(error: Any) -> bool:
    message = str(error or "").strip().lower()
    if not message:
        return False
    transient_markers = [
        "ctrader disconnected",
        "connectiondone",
        "connection was closed cleanly",
        "request time-out",
        "timed out",
        "timeout",
        "deferred",
        "no environment connection",
        "connection lost",
    ]
    return any(marker in message for marker in transient_markers)


def _refresh_access_token_in_place(config: CTraderExecutionConfig) -> Dict[str, Any]:
    payload = refresh_access_token(config)
    access_token = str(payload.get("accessToken") or payload.get("access_token") or "").strip()
    refresh_token = str(payload.get("refreshToken") or payload.get("refresh_token") or "").strip()
    if not access_token:
        error_message = payload.get("error") or payload.get("description") or payload.get("errorCode") or "Onbekende refresh-fout."
        raise RuntimeError(f"cTrader token refresh mislukt: {error_message}")

    config.access_token = access_token
    if refresh_token:
        config.refresh_token = refresh_token
    return payload


def _mask_present(value: Any) -> str:
    return "ingevuld" if str(value or "").strip() else "leeg"


def _format_ctrader_credential_error(stage: str, raw_error: Any, config: Optional[CTraderExecutionConfig] = None) -> str:
    message = str(raw_error or "onbekende fout").strip()
    normalized = message.lower()
    stage_label = str(stage or "cTrader").strip()

    details = []
    if config is not None:
        details.append(
            "status"
            f" client_id={_mask_present(getattr(config, 'client_id', ''))},"
            f" client_secret={_mask_present(getattr(config, 'client_secret', ''))},"
            f" access_token={_mask_present(getattr(config, 'access_token', ''))},"
            f" refresh_token={_mask_present(getattr(config, 'refresh_token', ''))},"
            f" account_id={str(getattr(config, 'ctid_trader_account_id', '') or '-').strip()},"
            f" environment={str(getattr(config, 'environment', '') or '-').strip()}"
        )

    suspect = ""
    if stage_label == "application auth":
        if "no environment connection" in normalized:
            suspect = "Verdachte oorzaak: cTrader demo/live environment niet bereikbaar of verkeerde environment-keuze; dit wijst meestal niet op je access token maar op de Open API verbinding of demo/live mismatch."
        elif any(marker in normalized for marker in ["access denied", "invalid", "unauthorized", "not authorized"]):
            suspect = "Verdachte credentials: cTrader client ID en/of client secret van je Open API app."
        else:
            suspect = "Verdachte credentials: eerst cTrader client ID/client secret controleren, daarna environment."
    elif stage_label in {"account auth", "account list"}:
        if "token" in normalized or any(marker in normalized for marker in ["access denied", "unauthorized", "not authorized"]):
            suspect = "Verdachte credential: cTrader access token (en mogelijk refresh token als refresh ook mislukt)."
        elif "account" in normalized:
            suspect = "Verdachte credential: cTrader account ID hoort niet bij deze token of bij deze environment."
        elif "no environment connection" in normalized:
            suspect = "Verdachte oorzaak: cTrader environment-verbinding faalt; controleer demo/live keuze en probeer opnieuw."
        else:
            suspect = "Verdachte velden: access token, account ID en demo/live environment."
    else:
        if "token" in normalized:
            suspect = "Verdachte credential: cTrader access token."
        elif "account" in normalized:
            suspect = "Verdachte credential: cTrader account ID."
        elif "no environment connection" in normalized:
            suspect = "Verdachte oorzaak: cTrader environment-verbinding is niet beschikbaar."

    parts = [f"cTrader {stage_label} fout: {message}"]
    if suspect:
        parts.append(suspect)
    if details:
        parts.append(f"({'; '.join(details)})")
    return " ".join(parts)


def _ensure_reactor_running() -> None:
    available, message = ctrader_is_available()
    if not available:
        raise RuntimeError(message)

    global _REACTOR_THREAD
    with _REACTOR_LOCK:
        if reactor.running:
            return
        if _REACTOR_THREAD is None or not _REACTOR_THREAD.is_alive():
            _REACTOR_THREAD = threading.Thread(
                target=reactor.run,
                kwargs={"installSignalHandlers": False},
                daemon=True,
                name="ctrader-reactor",
            )
            _REACTOR_THREAD.start()

    timeout_at = time.time() + 5.0
    while not reactor.running:
        if time.time() >= timeout_at:
            raise TimeoutError("Twisted reactor voor cTrader is niet gestart binnen de time-out.")
        time.sleep(0.05)


def _await_reactor_deferred(factory, timeout_seconds: float = 25.0):
    _ensure_reactor_running()
    completed = threading.Event()
    state: Dict[str, Any] = {}

    def resolve(value: Any = None, error: Any = None) -> None:
        if completed.is_set():
            return
        if error is not None:
            state["error"] = error
        else:
            state["value"] = value
        completed.set()

    def runner() -> None:
        try:
            deferred = factory()
            deferred.addCallbacks(lambda value: resolve(value=value), lambda failure: resolve(error=failure))
        except Exception as exc:
            resolve(error=exc)

    reactor.callFromThread(runner)
    if not completed.wait(timeout_seconds):
        raise TimeoutError("cTrader request time-out.")

    if "error" in state:
        error = state["error"]
        if isinstance(error, Exception):
            raise error
        raise RuntimeError(_failure_message(error))

    return state.get("value")


def _run_ctrader_operation(factory, config: Optional[CTraderExecutionConfig] = None, timeout_seconds: Optional[float] = None):
    last_error: Optional[Exception] = None
    timeout = float(timeout_seconds or _CTRADER_OPERATION_TIMEOUT_SECONDS)

    for attempt in range(1, _CTRADER_RETRY_ATTEMPTS + 1):
        try:
            return _await_reactor_deferred(factory, timeout_seconds=timeout)
        except Exception as exc:
            last_error = exc
            if config is not None and _is_token_related_error(exc):
                _refresh_access_token_in_place(config)
                continue
            if attempt < _CTRADER_RETRY_ATTEMPTS and _is_transient_connection_error(exc):
                time.sleep(_CTRADER_RETRY_SLEEP_SECONDS)
                continue
            raise

    if last_error is not None:
        raise last_error
    raise RuntimeError("cTrader operatie mislukt zonder foutmelding.")


def _find_symbol_id(symbols_message: Any, instrument_label: str) -> Tuple[Optional[int], str]:
    target = _normalize_symbol_name(instrument_label)
    exact_matches = []
    partial_matches = []
    for symbol in getattr(symbols_message, "symbol", []):
        symbol_name = _normalize_symbol_name(getattr(symbol, "symbolName", ""))
        if symbol_name == target:
            exact_matches.append(symbol)
        elif symbol_name.endswith(target) or symbol_name.startswith(target) or target in symbol_name:
            partial_matches.append(symbol)

    matches = exact_matches or partial_matches

    if not matches:
        return None, f"Geen cTrader symbool gevonden voor {instrument_label}."
    if len(matches) > 1:
        return None, f"Meerdere cTrader symbolen matchen met {instrument_label}; gebruik een account met uniek symboollabel."
    return int(matches[0].symbolId), _normalize_symbol_name(getattr(matches[0], "symbolName", instrument_label))


def _build_order_request(signal: Dict[str, Any], symbol_id: int, config: CTraderExecutionConfig, order_comment: str) -> Any:
    direction = str(signal.get("signal", ""))
    stop_loss = signal.get("stop_loss")
    take_profit = signal.get("take_profit")
    entry_price = signal.get("price")

    if stop_loss is None or take_profit is None or entry_price is None:
        raise ValueError("Signaal mist price, SL of TP voor cTrader orderplaatsing.")
    if direction not in {BUY, SELL}:
        raise ValueError(f"Onbekende richting voor cTrader: {direction}")

    request = ProtoOANewOrderReq()
    request.ctidTraderAccountId = int(config.ctid_trader_account_id)
    request.symbolId = int(symbol_id)
    request.orderType = ProtoOAOrderType.Value("MARKET")
    request.tradeSide = ProtoOATradeSide.Value("BUY" if direction == BUY else "SELL")
    request.volume = int(calculate_order_units(signal, config)) * 100
    request.relativeStopLoss = int(round(abs(float(entry_price) - float(stop_loss)) * 100000))
    request.relativeTakeProfit = int(round(abs(float(take_profit) - float(entry_price)) * 100000))
    request.comment = str(order_comment)[:512]
    request.label = "ai-dashboard"[:100]
    request.clientOrderId = f"ai-{int(time.time() * 1000)}"[:50]
    request.slippageInPoints = int(config.slippage_points)
    return request


def place_signal_order(
    signal: Dict[str, Any],
    instrument_label: str,
    config: CTraderExecutionConfig,
    order_comment: str = "AI Dashboard",
) -> CTraderExecutionResult:
    available, message = ctrader_is_available()
    if not available:
        return CTraderExecutionResult(False, f"cTrader connectie mislukt: {message}")

    if not str(config.client_id).strip() or not str(config.client_secret).strip():
        return CTraderExecutionResult(False, "cTrader client ID en client secret zijn verplicht.")
    if not str(config.access_token).strip():
        return CTraderExecutionResult(False, "cTrader access token ontbreekt.")
    resolved_ok, resolved_payload = _resolve_authorized_account_id(config)
    if not resolved_ok:
        return CTraderExecutionResult(False, f"cTrader account ID ontbreekt of is ongeldig: {resolved_payload}")

    host = EndPoints.PROTOBUF_LIVE_HOST if str(config.environment).strip().lower() == "live" else EndPoints.PROTOBUF_DEMO_HOST

    def _submit_order(active_access_token: str):
        def workflow():
            client = Client(host, EndPoints.PROTOBUF_PORT, TcpProtocol)
            operation = defer.Deferred()
            state = {"finished": False}

            def stop_client() -> None:
                try:
                    client.stopService()
                except Exception:
                    pass

            def finish(value: Any = None, error: Any = None) -> None:
                if state["finished"]:
                    return
                state["finished"] = True
                stop_client()
                if error is not None:
                    if isinstance(error, Exception):
                        operation.errback(error)
                    else:
                        operation.errback(RuntimeError(str(error)))
                else:
                    operation.callback(value)

            def on_failure(failure):
                finish(error=_failure_message(failure))
                return failure

            def on_connected(_: Any) -> None:
                request = ProtoOAApplicationAuthReq()
                request.clientId = str(config.client_id).strip()
                request.clientSecret = str(config.client_secret).strip()
                deferred = client.send(request, responseTimeoutInSeconds=10)
                deferred.addCallback(on_application_auth)
                deferred.addErrback(on_failure)

            def on_disconnected(_: Any, reason: Any) -> None:
                finish(error=f"cTrader disconnected: {reason}")

            def on_message_received(_: Any, message: Any) -> None:
                if message.payloadType == ProtoHeartbeatEvent().payloadType:
                    return

            def on_application_auth(message: Any):
                extracted = Protobuf.extract(message)
                if isinstance(extracted, ProtoOAErrorRes):
                    raise RuntimeError(_format_ctrader_credential_error("application auth", extracted.description or extracted.errorCode, config))

                request = ProtoOAAccountAuthReq()
                request.ctidTraderAccountId = int(config.ctid_trader_account_id)
                request.accessToken = str(active_access_token).strip()
                deferred = client.send(request, responseTimeoutInSeconds=10)
                deferred.addCallback(on_account_auth)
                deferred.addErrback(on_failure)
                return deferred

            def on_account_auth(message: Any):
                extracted = Protobuf.extract(message)
                if isinstance(extracted, ProtoOAErrorRes):
                    raise RuntimeError(_format_ctrader_credential_error("account auth", extracted.description or extracted.errorCode, config))

                request = ProtoOASymbolsListReq()
                request.ctidTraderAccountId = int(config.ctid_trader_account_id)
                request.includeArchivedSymbols = False
                deferred = client.send(request, responseTimeoutInSeconds=15)
                deferred.addCallback(on_symbols_loaded)
                deferred.addErrback(on_failure)
                return deferred

            def on_symbols_loaded(message: Any):
                extracted = Protobuf.extract(message)
                symbol_id, resolved_symbol = _find_symbol_id(extracted, instrument_label)
                if symbol_id is None:
                    raise RuntimeError(resolved_symbol)

                order_request = _build_order_request(signal, symbol_id, config, order_comment)
                deferred = client.send(order_request, responseTimeoutInSeconds=15)
                deferred.addCallback(lambda order_message: on_order_sent(order_message, resolved_symbol))
                deferred.addErrback(on_failure)
                return deferred

            def on_order_sent(message: Any, resolved_symbol: str):
                extracted = Protobuf.extract(message)

                if isinstance(extracted, ProtoOAErrorRes):
                    raise RuntimeError(f"cTrader order fout: {extracted.description or extracted.errorCode}")
                if isinstance(extracted, ProtoOAOrderErrorEvent):
                    raise RuntimeError(f"cTrader order afgewezen: {extracted.description or extracted.errorCode}")
                if not isinstance(extracted, ProtoOAExecutionEvent):
                    raise RuntimeError(f"Onverwachte cTrader orderrespons: {type(extracted).__name__}")

                order_id = getattr(getattr(extracted, "order", None), "orderId", None)
                if order_id is None:
                    order_id = getattr(getattr(extracted, "deal", None), "orderId", None)
                if order_id is None:
                    order_id = getattr(getattr(extracted, "position", None), "positionId", None)

                finish(value={
                    "order_ticket": str(order_id) if order_id is not None else None,
                    "symbol": resolved_symbol,
                })

                return extracted

            client.setConnectedCallback(on_connected)
            client.setDisconnectedCallback(on_disconnected)
            client.setMessageReceivedCallback(on_message_received)
            client.startService()

            return operation

        return _await_reactor_deferred(workflow)

    refreshed_access_token = ""
    refreshed_refresh_token = ""

    try:
        result = _submit_order(str(config.access_token).strip())
    except Exception as exc:
        error_text = str(exc)
        normalized_error = error_text.lower()
        should_try_refresh = bool(
            str(config.refresh_token).strip()
            and (
                "account auth" in normalized_error
                or "access token" in normalized_error
                or "token" in normalized_error
                or "unauthorized" in normalized_error
                or "invalid" in normalized_error
            )
        )
        if not should_try_refresh:
            return CTraderExecutionResult(False, f"cTrader order afgewezen: {exc}")

        refresh_payload = refresh_access_token(config)
        refreshed_access_token = str(refresh_payload.get("accessToken") or refresh_payload.get("access_token") or "").strip()
        refreshed_refresh_token = str(refresh_payload.get("refreshToken") or refresh_payload.get("refresh_token") or "").strip()
        if not refreshed_access_token:
            refresh_error = refresh_payload.get("description") or refresh_payload.get("error") or refresh_payload
            return CTraderExecutionResult(False, f"cTrader order afgewezen: {exc}. Token refresh mislukt: {refresh_error}")

        config.access_token = refreshed_access_token
        if refreshed_refresh_token:
            config.refresh_token = refreshed_refresh_token

        try:
            result = _submit_order(refreshed_access_token)
        except Exception as retry_exc:
            return CTraderExecutionResult(False, f"cTrader order afgewezen na token refresh: {retry_exc}")

    return CTraderExecutionResult(
        True,
        "order geplaatst" if not refreshed_access_token else "order geplaatst na token refresh",
        order_ticket=result.get("order_ticket") if isinstance(result, dict) else None,
        volume=calculate_order_units(signal, config),
        symbol=result.get("symbol", "") if isinstance(result, dict) else "",
        updated_access_token=refreshed_access_token,
        updated_refresh_token=refreshed_refresh_token,
    )