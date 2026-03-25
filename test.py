import streamlit as st
import pandas as pd
import numpy as np
import requests
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from streamlit_autorefresh import st_autorefresh
import time
import os

from mt5_executor import MT5ExecutionConfig, mt5_is_available, place_signal_order

TELEGRAM_TOKEN_DEFAULT = ""
TELEGRAM_CHAT_ID_DEFAULT = ""
TWELVEDATA_API_KEY_DEFAULT = ""
APP_TIMEZONE = "Europe/Amsterdam"
TWELVEDATA_MIN_FETCH_SECONDS = 60
ALERT_PRICE_TOLERANCE = 0.0002
ALERT_TIME_TOLERANCE_MINUTES = 3
ALERT_REPEAT_COOLDOWN_MINUTES = 30
ALERT_REPEAT_PRICE_TOLERANCE = 0.0005


def get_config_value(name, fallback=""):
    env_value = os.getenv(name)
    if env_value:
        return env_value

    try:
        secrets = getattr(st, "secrets", None)
        if secrets is not None and name in secrets:
            return str(secrets[name])
    except Exception:
        pass

    return fallback


def get_timeframe_strategy_settings(timeframe_label, high_win_rate_mode=False):
    settings = {
        'min_probability': 60,
        'reversal_rr': 2.2,
        'continuation_rr': 2.0,
        'require_high_zone_for_reversal': False,
        'require_trend_alignment_for_reversal': False,
        'require_retest_for_continuation': False,
        'continuation_body_atr_ratio': 0.35,
        'max_key_level_distance_pips': np.nan,
    }

    if timeframe_label == '15m' and high_win_rate_mode:
        settings.update({
            'min_probability': 72,
            'reversal_rr': 1.4,
            'continuation_rr': 1.3,
            'require_high_zone_for_reversal': True,
            'require_trend_alignment_for_reversal': True,
            'require_retest_for_continuation': True,
            'continuation_body_atr_ratio': 0.45,
            'max_key_level_distance_pips': 12,
        })

    return settings


def send_telegram_alert(message: str):
    """Stuur een simpele Telegram-alert (optioneel).

    Bronvolgorde voor configuratie:
    1. Waarden uit de Streamlit-UI (session_state)
    2. Environment-variabelen
    3. Eventueel Streamlit secrets
    """

    # 1) Waarden uit de Streamlit-UI (indien gezet)
    ui_token = st.session_state.get("TELEGRAM_TOKEN_UI")
    ui_chat_id = st.session_state.get("TELEGRAM_CHAT_ID_UI")

    # 2) Environment-variabelen / Streamlit secrets als fallback
    token = ui_token or get_config_value("TELEGRAM_TOKEN")
    chat_id = ui_chat_id or get_config_value("TELEGRAM_CHAT_ID")

    # 3) Vaste standaardwaarden uit deze app als laatste fallback
    if not token and TELEGRAM_TOKEN_DEFAULT:
        token = TELEGRAM_TOKEN_DEFAULT
    if not chat_id and TELEGRAM_CHAT_ID_DEFAULT:
        chat_id = TELEGRAM_CHAT_ID_DEFAULT

    if not token or not chat_id:
        st.warning("Telegram is niet geconfigureerd (token of chat ID mist). Geen alert verstuurd.")
        return

    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        # Iets ruimere timeout zodat tijdelijke netwerkvertraging geen directe fout geeft
        resp = requests.post(url, json={"chat_id": chat_id, "text": message}, timeout=15)
        if resp.status_code != 200:
            # Toon korte foutmelding zonder geheime gegevens
            try:
                info = resp.json()
                desc = info.get("description", "onbekende fout")
            except Exception:
                desc = resp.text[:200]
            st.warning(f"Telegram API-fout ({resp.status_code}): {desc}")
    except Exception as e:
        # Geen harde fout in de UI – maar nu wel een zichtbare waarschuwing
        st.warning(f"Kon Telegram-alert niet versturen: {e}")


def build_news_query(instrument_type, base_currency=None, target_currency=None, index_choice=None):
    """Maak een simpele zoekquery voor nieuws op basis van instrument."""
    if instrument_type == "Forex" and base_currency and target_currency:
        pair = f"{base_currency}{target_currency}"
        # Richt je vooral op valuta + algemene macro/FX termen
        return f"({base_currency} OR {target_currency} OR {pair}) AND (forex OR currency OR FX OR ECB OR FED OR central bank OR interest rates)"

    return "(forex OR currency OR FX) AND (market OR economy OR inflation OR interest rates)"


def fetch_news_articles(query, api_key, language="en", max_articles=8):
    """Haal nieuwsartikelen op via NewsData.io (alleen metadata: titel, beschrijving, url).

    Gebruikt de endpoint:
        https://newsdata.io/api/1/latest?apikey=...&q=...
    """
    if not api_key:
        return []

    url = "https://newsdata.io/api/1/latest"
    params = {
        "apikey": api_key,
        "q": query,
        "language": language,
        "page": 1,
    }

    try:
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()

        if not data or data.get("status") not in ("success", True):
            # NewsData.io returns status 'success' on success
            st.warning(f"News API fout: {data.get('message', 'onbekende fout') if isinstance(data, dict) else 'onbekend antwoord'}")
            return []

        articles = []
        for a in data.get("results", [])[:max_articles]:
            articles.append(
                {
                    "title": a.get("title", ""),
                    "description": a.get("description", ""),
                    "url": a.get("link") or a.get("source_url"),
                    "source": a.get("source_id", ""),
                    "publishedAt": a.get("pubDate", ""),
                }
            )
        return articles
    except Exception as e:
        st.warning(f"Kon nieuws niet ophalen: {e}")
        return []


def analyze_news_sentiment(articles):
    """Eenvoudige sentiment-score op basis van woorden in titel + beschrijving.

    Geeft een dict terug met 'score' (-1 t/m 1) en een label: Bullish / Bearish / Neutraal.
    Dit is bewust simpel gehouden als extra filter bovenop je technische signalen.
    """
    if not articles:
        return {"score": 0.0, "label": "Neutraal (geen nieuws)"}

    positive_words = [
        "rises",
        "rise",
        "surge",
        "rally",
        "record high",
        "growth",
        "strong",
        "bullish",
        "optimistic",
        "rebound",
        "recovery",
        "beats expectations",
    ]
    negative_words = [
        "falls",
        "fall",
        "plunge",
        "selloff",
        "recession",
        "weak",
        "bearish",
        "pessimistic",
        "crisis",
        "war",
        "inflation",
        "rate hike",
        "cuts forecast",
    ]

    total_score = 0
    for art in articles:
        text = f"{art.get('title', '')} {art.get('description', '')}".lower()
        score = 0
        for w in positive_words:
            if w in text:
                score += 1
        for w in negative_words:
            if w in text:
                score -= 1
        total_score += score

    avg = total_score / max(len(articles), 1)

    if avg > 0.5:
        label = "Bullish"
    elif avg < -0.5:
        label = "Bearish"
    else:
        label = "Neutraal"

    return {"score": float(avg), "label": label}


def filter_signals_by_news(signals, news_sentiment):
    """Ken elke trade een nieuws-gebonden slagingsscore toe en filter extreem
    tegengestelde trades weg.

    - Elke trade krijgt velden:
        - ``news_sentiment_score`` (numeriek)
        - ``news_sentiment_label`` (Bullish/Bearish/Neutraal)
        - ``news_success_score`` (0–100%, inschatting kans van slagen i.v.m. nieuws)
    - Bij sterk Bullish sentiment (score > 0.5): erg lage kans voor SELL (<30%)
        wordt weggefilterd.
    - Bij sterk Bearish sentiment (score < -0.5): erg lage kans voor BUY (<30%)
        wordt weggefilterd.
    """
    if not signals or not news_sentiment:
        return signals

    score = float(news_sentiment.get("score", 0.0))
    label = news_sentiment.get("label", "")

    def _estimate_success(direction, s):
        """Zet nieuwsscore om in simpele slagingskans per trade."""
        # Beperk de sterkte tot [-2, 2] om extreme waarden te dempen
        strength = min(abs(s), 2.0) / 2.0  # 0..1
        base = 0.5  # 50%

        if direction == "Buy":
            if s > 0:
                prob = base + 0.3 * strength  # max ~80%
            elif s < 0:
                prob = base - 0.3 * strength  # min ~20%
            else:
                prob = base
        elif direction == "Sell":
            if s < 0:
                prob = base + 0.3 * strength
            elif s > 0:
                prob = base - 0.3 * strength
            else:
                prob = base
        else:
            prob = base

        # Clamp tussen 0.1 en 0.9 zodat het nooit 0 of 100% is
        prob = max(0.1, min(0.9, prob))
        return prob * 100.0

    filtered = []
    for s in signals:
        sig = dict(s)  # kopie zodat we origineel niet muteren
        direction = str(sig.get("signal", "")).capitalize()

        sig["news_sentiment_score"] = score
        sig["news_sentiment_label"] = label
        sig["news_success_score"] = _estimate_success(direction, score)

        # Nieuws-filter logica alleen bij sterk sentiment én lage kans
        if score > 0.5 and direction == "Sell" and sig["news_success_score"] < 30:
            # Sterk positief nieuws → zeer lage kans voor short, overslaan
            continue
        if score < -0.5 and direction == "Buy" and sig["news_success_score"] < 30:
            # Sterk negatief nieuws → zeer lage kans voor long, overslaan
            continue

        filtered.append(sig)

    return filtered

def fetch_fx_history_twelve_data(from_currency, to_currency, freq, periods, api_key):
    """Haal echte intraday-forexdata op via Twelve Data.

    Valt terug naar een lege DataFrame als er iets misgaat.
    """
    if not api_key:
        return pd.DataFrame(), "missing_api_key"

    interval_map = {
        "1min": "1min",
        "5min": "5min",
        "15min": "15min",
        "30min": "30min",
    }
    interval = interval_map.get(freq)
    if not interval:
        return pd.DataFrame(), "unsupported_interval"

    symbol = f"{from_currency}/{to_currency}"
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": min(max(int(periods), 30), 5000),
        "timezone": APP_TIMEZONE,
        "format": "JSON",
        "apikey": api_key,
    }

    try:
        resp = requests.get(url, params=params, timeout=15)
        data = resp.json()

        if not isinstance(data, dict):
            return pd.DataFrame(), "invalid_response"

        if data.get("status") == "error" or data.get("code"):
            msg = data.get("message", "Unknown Twelve Data error")
            return pd.DataFrame(), msg

        values = data.get("values")
        if not values:
            return pd.DataFrame(), "no_values"

        records = []
        for candle in values:
            records.append({
                "Datetime": pd.to_datetime(candle.get("datetime")),
                "Open": float(candle.get("open", 0.0)),
                "High": float(candle.get("high", 0.0)),
                "Low": float(candle.get("low", 0.0)),
                "Close": float(candle.get("close", 0.0)),
                "Volume": float(candle.get("volume")) if candle.get("volume") not in (None, "") else np.nan,
            })

        if not records:
            return pd.DataFrame(), "no_records"

        df = pd.DataFrame(records).sort_values("Datetime").reset_index(drop=True)
        df = df.tail(periods)

        return df, None
    except Exception as e:
        return pd.DataFrame(), str(e)


def generate_historical_data(periods, freq, base_currency=None, target_currency=None, fx_api_key=None):
    """Return only live forex candles; never fall back to synthetic data."""
    if not (fx_api_key and base_currency and target_currency):
        st.session_state['latest_market_data_meta'] = {
            'source': 'missing',
            'error': 'missing_api_key',
            'age_seconds': np.nan,
            'fetched_at': None,
        }
        return pd.DataFrame()

    cache_key = f"{base_currency}_{target_currency}_{freq}_{periods}"
    cache_store = st.session_state.setdefault('twelvedata_cache', {})
    cached_entry = cache_store.get(cache_key)
    now_ts = time.time()

    if cached_entry:
        age_seconds = now_ts - cached_entry['fetched_at']
        if age_seconds < TWELVEDATA_MIN_FETCH_SECONDS:
            st.session_state['latest_market_data_meta'] = {
                'source': 'cache',
                'error': None,
                'age_seconds': age_seconds,
                'fetched_at': pd.to_datetime(cached_entry['fetched_at'], unit='s'),
            }
            return cached_entry['df'].copy()

    df, error_message = fetch_fx_history_twelve_data(base_currency, target_currency, freq, periods, fx_api_key)
    if not df.empty:
        cache_store[cache_key] = {
            'df': df.copy(),
            'fetched_at': now_ts,
        }
        st.session_state['latest_market_data_meta'] = {
            'source': 'live',
            'error': None,
            'age_seconds': 0.0,
            'fetched_at': pd.to_datetime(now_ts, unit='s'),
        }
        return df

    if cached_entry:
        age_seconds = now_ts - cached_entry['fetched_at']
        st.session_state['latest_market_data_meta'] = {
            'source': 'stale_cache',
            'error': error_message,
            'age_seconds': age_seconds,
            'fetched_at': pd.to_datetime(cached_entry['fetched_at'], unit='s'),
        }
        return cached_entry['df'].copy()

    st.session_state['latest_market_data_meta'] = {
        'source': 'error',
        'error': error_message,
        'age_seconds': np.nan,
        'fetched_at': None,
    }
    return pd.DataFrame()


def clamp(value, minimum, maximum):
    return max(minimum, min(maximum, value))


def safe_divide(numerator, denominator, default=0.0):
    if denominator in (0, None) or pd.isna(denominator):
        return default
    return numerator / denominator


def build_key_levels(df, pip_size=0.0001, lookback=120):
    if df is None or df.empty:
        return []

    recent = df.tail(min(len(df), lookback)).reset_index(drop=True)
    levels = []

    for index in range(2, len(recent) - 2):
        high_window = recent['High'].iloc[index - 2:index + 3]
        low_window = recent['Low'].iloc[index - 2:index + 3]

        if recent.iloc[index]['High'] >= high_window.max():
            levels.append({'price': float(recent.iloc[index]['High']), 'type': 'swing_high'})

        if recent.iloc[index]['Low'] <= low_window.min():
            levels.append({'price': float(recent.iloc[index]['Low']), 'type': 'swing_low'})

    round_step = pip_size * 50 if pip_size < 1 else 50.0
    latest_close = float(recent.iloc[-1]['Close'])
    anchor = round(latest_close / round_step) * round_step

    for offset in range(-4, 5):
        levels.append({'price': float(round(anchor + offset * round_step, 5)), 'type': 'round_number'})

    merged_levels = []
    for level in sorted(levels, key=lambda item: item['price']):
        if not merged_levels or abs(level['price'] - merged_levels[-1]['price']) > pip_size * 8:
            merged_levels.append(level)

    return merged_levels


def find_nearest_key_level(price, key_levels):
    if not key_levels:
        return None, np.nan

    nearest_level = min(key_levels, key=lambda item: abs(item['price'] - price))
    distance = abs(nearest_level['price'] - price)
    return nearest_level, distance


def calculate_signal_probability(df, index, direction, setup_name, zone, key_levels, pip_size, entry_price, stop_loss):
    row = df.iloc[index]
    zone_mid = (zone['top'] + zone['bottom']) / 2
    nearest_level, key_distance = find_nearest_key_level(entry_price, key_levels)
    key_distance_pips = key_distance / pip_size if pip_size else np.nan
    risk_pips = abs(entry_price - stop_loss) / pip_size if pip_size else np.nan
    atr_pips = row.get('ATR_14', np.nan) / pip_size if pip_size and pd.notna(row.get('ATR_14', np.nan)) else np.nan

    score = 48
    reasons = []

    if zone.get('strength') == 'High':
        score += 8
        reasons.append('sterke zone')
    else:
        score += 4
        reasons.append('middelsterke zone')

    trend_up = row['SMA_50'] >= row['SMA_200']
    trend_aligned = (direction == 'Buy' and trend_up) or (direction == 'Sell' and not trend_up)
    if trend_aligned:
        score += 10
        reasons.append('trend in lijn')
    else:
        score -= 8
        reasons.append('tegen trend')

    if pd.notna(key_distance_pips):
        if key_distance_pips <= 5:
            score += 12
            reasons.append('op keylevel')
        elif key_distance_pips <= 10:
            score += 7
            reasons.append('dicht bij keylevel')
        elif key_distance_pips <= 20:
            score += 2
        else:
            score -= 6
            reasons.append('ver van keylevel')

    if setup_name == 'Reversal':
        wick_size = row['Lower_Wick'] if direction == 'Buy' else row['Upper_Wick']
        wick_ratio = safe_divide(wick_size, max(row['Body_Size'], pip_size), default=0.0)

        if wick_ratio >= 1.5:
            score += 10
            reasons.append('sterke afwijzing')
        elif wick_ratio >= 1.0:
            score += 5
        else:
            score -= 6
            reasons.append('zwakke afwijzing')

        if (direction == 'Buy' and row['Close'] > zone_mid) or (direction == 'Sell' and row['Close'] < zone_mid):
            score += 8
            reasons.append('close bevestigt reversal')
        else:
            score -= 8
    else:
        body_ratio = safe_divide(row['Body_Size'], row['Candle_Range'], default=0.0)
        if body_ratio >= 0.55:
            score += 9
            reasons.append('sterke breakout candle')
        elif body_ratio >= 0.40:
            score += 4
        else:
            score -= 6
            reasons.append('zwakke breakout candle')

        if direction == 'Buy':
            breakout_pips = (row['Close'] - zone['top']) / pip_size
        else:
            breakout_pips = (zone['bottom'] - row['Close']) / pip_size

        if breakout_pips >= 4:
            score += 6
            reasons.append('duidelijke doorbraak')
        elif breakout_pips >= 2:
            score += 3
        else:
            score -= 4

    if pd.notna(risk_pips) and pd.notna(atr_pips) and atr_pips > 0:
        atr_risk_ratio = risk_pips / atr_pips
        if 0.6 <= atr_risk_ratio <= 1.8:
            score += 6
            reasons.append('risico binnen ATR')
        else:
            score -= 5

    volume_ratio = row.get('Volume_Ratio', np.nan)
    if pd.notna(volume_ratio) and volume_ratio > 0:
        if volume_ratio >= 1.4:
            score += 12
            volume_status = 'hoog volume'
            reasons.append('hoog volume bevestigt setup')
        elif volume_ratio >= 1.1:
            score += 7
            volume_status = 'boven gemiddeld volume'
        elif volume_ratio >= 0.85:
            score += 2
            volume_status = 'normaal volume'
        else:
            score -= 8
            volume_status = 'laag volume'
            reasons.append('lage volume-confirmatie')
    else:
        volume_status = 'volume niet beschikbaar in Twelve Data FX'

    probability = int(round(clamp(score, 20, 90)))

    return {
        'success_probability': probability,
        'nearest_key_level': nearest_level['price'] if nearest_level else np.nan,
        'key_level_type': nearest_level['type'] if nearest_level else '',
        'key_level_distance_pips': round(key_distance_pips, 1) if pd.notna(key_distance_pips) else np.nan,
        'volume_ratio': round(float(volume_ratio), 2) if pd.notna(volume_ratio) else np.nan,
        'volume_status': volume_status,
        'confidence_notes': ', '.join(reasons),
    }


def build_trade_signal(df, index, direction, setup_name, zone, timeframe_label, pip_size, key_levels, entry_price, stop_loss, take_profit, strategy_settings=None):
    strategy_settings = strategy_settings or get_timeframe_strategy_settings(timeframe_label)
    metadata = calculate_signal_probability(
        df,
        index,
        direction,
        setup_name,
        zone,
        key_levels,
        pip_size,
        entry_price,
        stop_loss,
    )

    max_key_distance = strategy_settings.get('max_key_level_distance_pips', np.nan)
    if pd.notna(max_key_distance) and pd.notna(metadata['key_level_distance_pips']):
        if metadata['key_level_distance_pips'] > max_key_distance:
            return None

    if metadata['success_probability'] < strategy_settings.get('min_probability', 60):
        return None

    return {
        'timestamp': df.iloc[index]['Datetime'],
        'signal': direction,
        'type': f"{timeframe_label.upper()} {zone['type']} {setup_name}",
        'setup': setup_name,
        'price': float(round(entry_price, 5)),
        'stop_loss': float(round(stop_loss, 5)),
        'take_profit': float(round(take_profit, 5)),
        'timeframe': timeframe_label,
        'zone_strength': zone.get('strength', ''),
        'risk_reward': f"{round(abs(take_profit - entry_price) / max(abs(entry_price - stop_loss), pip_size), 2)}:1",
        'success_probability': metadata['success_probability'],
        'nearest_key_level': metadata['nearest_key_level'],
        'key_level_type': metadata['key_level_type'],
        'key_level_distance_pips': metadata['key_level_distance_pips'],
        'volume_ratio': metadata['volume_ratio'],
        'volume_status': metadata['volume_status'],
        'confidence_notes': metadata['confidence_notes'],
    }


def generate_keylevel_signals(df, zones, timeframe_label="1m", pip_size=0.0001, key_levels=None, high_win_rate_mode=False):
    signals = []

    if df.empty or not zones:
        return signals

    active_key_levels = key_levels or build_key_levels(df, pip_size=pip_size)
    strategy_settings = get_timeframe_strategy_settings(timeframe_label, high_win_rate_mode=high_win_rate_mode)

    for index in range(1, len(df)):
        row = df.iloc[index]
        previous_row = df.iloc[index - 1]
        trend_up = row['SMA_50'] >= row['SMA_200']

        for zone in zones:
            zone_top = zone['top']
            zone_bottom = zone['bottom']
            zone_mid = (zone_top + zone_bottom) / 2
            zone_buffer = max(pip_size * 4, (zone_top - zone_bottom) * 0.15)
            bullish_retest_confirmed = (
                previous_row['Close'] > zone_top + zone_buffer
                and row['Low'] <= zone_top + zone_buffer
                and row['Close'] > zone_top
                and row['Close'] > row['Open']
                and safe_divide(row['Body_Size'], row['ATR_14'], default=0.0) >= 0.2
            )
            bearish_retest_confirmed = (
                previous_row['Close'] < zone_bottom - zone_buffer
                and row['High'] >= zone_bottom - zone_buffer
                and row['Close'] < zone_bottom
                and row['Close'] < row['Open']
                and safe_divide(row['Body_Size'], row['ATR_14'], default=0.0) >= 0.2
            )

            if zone['type'] == 'Supply':
                reversal_confirmed = (
                    row['High'] >= zone_bottom
                    and row['Close'] < zone_mid
                    and row['Close'] < row['Open']
                    and row['Upper_Wick'] > row['Body_Size'] * 1.2
                )
                continuation_confirmed = (
                    previous_row['Close'] <= zone_top
                    and row['Close'] > zone_top + zone_buffer
                    and row['Close'] > row['Open']
                    and row['Body_Size'] >= row['ATR_14'] * strategy_settings['continuation_body_atr_ratio']
                    and row['Low'] <= zone_top + zone_buffer
                )

                if strategy_settings['require_high_zone_for_reversal'] and zone.get('strength') != 'High':
                    reversal_confirmed = False
                if strategy_settings['require_trend_alignment_for_reversal'] and trend_up:
                    reversal_confirmed = False
                if strategy_settings['require_retest_for_continuation']:
                    continuation_confirmed = bullish_retest_confirmed

                if reversal_confirmed:
                    entry_price = float(row['Close'])
                    stop_loss = float(max(row['High'], zone_top) + pip_size * 5)
                    risk = stop_loss - entry_price
                    if risk > 0:
                        take_profit = entry_price - strategy_settings['reversal_rr'] * risk
                        stop_loss, take_profit = apply_pip_limits(entry_price, stop_loss, take_profit, 'Sell', pip_size)
                        signal = build_trade_signal(df, index, 'Sell', 'Reversal', zone, timeframe_label, pip_size, active_key_levels, entry_price, stop_loss, take_profit, strategy_settings=strategy_settings)
                        if signal:
                            signals.append(signal)

                if continuation_confirmed:
                    entry_price = float(row['Close'])
                    stop_loss = float(min(zone_top, row['Low']) - pip_size * 5)
                    risk = entry_price - stop_loss
                    if risk > 0:
                        take_profit = entry_price + strategy_settings['continuation_rr'] * risk
                        stop_loss, take_profit = apply_pip_limits(entry_price, stop_loss, take_profit, 'Buy', pip_size)
                        signal = build_trade_signal(df, index, 'Buy', 'Continuation', zone, timeframe_label, pip_size, active_key_levels, entry_price, stop_loss, take_profit, strategy_settings=strategy_settings)
                        if signal:
                            signals.append(signal)

            elif zone['type'] == 'Demand':
                reversal_confirmed = (
                    row['Low'] <= zone_top
                    and row['Close'] > zone_mid
                    and row['Close'] > row['Open']
                    and row['Lower_Wick'] > row['Body_Size'] * 1.2
                )
                continuation_confirmed = (
                    previous_row['Close'] >= zone_bottom
                    and row['Close'] < zone_bottom - zone_buffer
                    and row['Close'] < row['Open']
                    and row['Body_Size'] >= row['ATR_14'] * strategy_settings['continuation_body_atr_ratio']
                    and row['High'] >= zone_bottom - zone_buffer
                )

                if strategy_settings['require_high_zone_for_reversal'] and zone.get('strength') != 'High':
                    reversal_confirmed = False
                if strategy_settings['require_trend_alignment_for_reversal'] and not trend_up:
                    reversal_confirmed = False
                if strategy_settings['require_retest_for_continuation']:
                    continuation_confirmed = bearish_retest_confirmed

                if reversal_confirmed:
                    entry_price = float(row['Close'])
                    stop_loss = float(min(row['Low'], zone_bottom) - pip_size * 5)
                    risk = entry_price - stop_loss
                    if risk > 0:
                        take_profit = entry_price + strategy_settings['reversal_rr'] * risk
                        stop_loss, take_profit = apply_pip_limits(entry_price, stop_loss, take_profit, 'Buy', pip_size)
                        signal = build_trade_signal(df, index, 'Buy', 'Reversal', zone, timeframe_label, pip_size, active_key_levels, entry_price, stop_loss, take_profit, strategy_settings=strategy_settings)
                        if signal:
                            signals.append(signal)

                if continuation_confirmed:
                    entry_price = float(row['Close'])
                    stop_loss = float(max(zone_bottom, row['High']) + pip_size * 5)
                    risk = stop_loss - entry_price
                    if risk > 0:
                        take_profit = entry_price - strategy_settings['continuation_rr'] * risk
                        stop_loss, take_profit = apply_pip_limits(entry_price, stop_loss, take_profit, 'Sell', pip_size)
                        signal = build_trade_signal(df, index, 'Sell', 'Continuation', zone, timeframe_label, pip_size, active_key_levels, entry_price, stop_loss, take_profit, strategy_settings=strategy_settings)
                        if signal:
                            signals.append(signal)

    return signals

def apply_pip_limits(entry_price, stop_loss, take_profit, direction, pip_size, max_sl_pips=50, max_tp_pips=100):
    """Cap SL/TP distances to configured pip limits."""
    if stop_loss is None or take_profit is None or pip_size is None:
        return stop_loss, take_profit

    max_sl = max_sl_pips * pip_size
    max_tp = max_tp_pips * pip_size

    sl_dist = abs(stop_loss - entry_price)
    tp_dist = abs(take_profit - entry_price)

    if max_sl > 0 and sl_dist > max_sl:
        sl_dist = max_sl
        if direction == 'Buy':
            stop_loss = entry_price - sl_dist
        else:
            stop_loss = entry_price + sl_dist

    if max_tp > 0 and tp_dist > max_tp:
        tp_dist = max_tp
        if direction == 'Buy':
            take_profit = entry_price + tp_dist
        else:
            take_profit = entry_price - tp_dist

    return stop_loss, take_profit

# Identify Supply and Demand Zones
def identify_supply_demand_zones(df, lookback=50, threshold=0.002):
    """
    Identify supply (resistance) and demand (support) zones based on:
    - Price rejections (wicks)
    - Consolidation areas
    """
    zones = []
    
    for i in range(lookback, len(df) - 10):
        # Check for strong rejection candles (large wicks)
        body = abs(df.iloc[i]['Close'] - df.iloc[i]['Open'])
        upper_wick = df.iloc[i]['High'] - max(df.iloc[i]['Close'], df.iloc[i]['Open'])
        lower_wick = min(df.iloc[i]['Close'], df.iloc[i]['Open']) - df.iloc[i]['Low']
        total_range = df.iloc[i]['High'] - df.iloc[i]['Low']
        
        if total_range == 0:
            continue

        upper_wick_ratio = upper_wick / total_range
        lower_wick_ratio = lower_wick / total_range
            
        # SUPPLY ZONE (Resistance) - Strong rejection from top
        if upper_wick > body * 2 and upper_wick_ratio > 0.5:
            zone_top = df.iloc[i]['High']
            zone_bottom = df.iloc[i]['High'] - (total_range * 0.3)

            future_prices = df.iloc[i+1:i+11]['High']
            if len(future_prices) > 0 and future_prices.max() < zone_top * 1.001:
                zones.append({
                    'type': 'Supply',
                    'top': zone_top,
                    'bottom': zone_bottom,
                    'start_idx': i,
                    'strength': 'High' if upper_wick_ratio > 0.65 else 'Medium',
                    'touches': 1
                })
        
        # DEMAND ZONE (Support) - Strong rejection from bottom
        if lower_wick > body * 2 and lower_wick_ratio > 0.5:
            zone_bottom = df.iloc[i]['Low']
            zone_top = df.iloc[i]['Low'] + (total_range * 0.3)

            future_prices = df.iloc[i+1:i+11]['Low']
            if len(future_prices) > 0 and future_prices.min() > zone_bottom * 0.999:
                zones.append({
                    'type': 'Demand',
                    'top': zone_top,
                    'bottom': zone_bottom,
                    'start_idx': i,
                    'strength': 'High' if lower_wick_ratio > 0.65 else 'Medium',
                    'touches': 1
                })
    
    # Remove overlapping zones, keep the strongest
    filtered_zones = []
    for zone in zones:
        overlap = False
        for existing in filtered_zones:
            if zone['type'] == existing['type']:
                # Check for overlap
                if not (zone['top'] < existing['bottom'] or zone['bottom'] > existing['top']):
                    overlap = True
                    # Keep the one with higher strength
                    if zone['strength'] == 'High' and existing['strength'] == 'Medium':
                        filtered_zones.remove(existing)
                        overlap = False
                    break
        if not overlap:
            filtered_zones.append(zone)
    
    return filtered_zones

# Generate Supply/Demand Zone Signals
def generate_supply_demand_signals(df, zones, pip_size=0.0001, timeframe_label='1m', high_win_rate_mode=False):
    return generate_keylevel_signals(
        df,
        zones,
        timeframe_label=timeframe_label,
        pip_size=pip_size,
        high_win_rate_mode=high_win_rate_mode,
    )


def generate_m15_market_structure_signals(df_15m, zones, pip_size=0.0001, high_win_rate_mode=False):
    return generate_keylevel_signals(
        df_15m,
        zones,
        timeframe_label='15m',
        pip_size=pip_size,
        high_win_rate_mode=high_win_rate_mode,
    )


def generate_m5_market_structure_signals(df_5m, zones, pip_size=0.0001):
    return generate_keylevel_signals(df_5m, zones, timeframe_label='5m', pip_size=pip_size)


def generate_m30_market_structure_signals(df_30m, zones, pip_size=0.0001):
    return generate_keylevel_signals(df_30m, zones, timeframe_label='30m', pip_size=pip_size)

# Add lower highs and lower lows detection and technical indicators
def add_technical_indicators(df):
    df = df.copy()
    df['SMA_50'] = df['Close'].rolling(window=50, min_periods=1).mean()
    df['SMA_200'] = df['Close'].rolling(window=200, min_periods=1).mean()
    df['Lower_High'] = (df['High'].diff() < 0) & (df['High'].shift(-1) < df['High'])
    df['Lower_Low'] = (df['Low'].diff() < 0) & (df['Low'].shift(-1) < df['Low'])
    df['Higher_High'] = (df['High'].diff() > 0) & (df['High'].shift(-1) > df['High'])
    df['Higher_Low'] = (df['Low'].diff() > 0) & (df['Low'].shift(-1) > df['Low'])
    df['Candle_Range'] = df['High'] - df['Low']
    df['Body_Size'] = (df['Close'] - df['Open']).abs()
    df['Upper_Wick'] = df['High'] - df[['Open', 'Close']].max(axis=1)
    df['Lower_Wick'] = df[['Open', 'Close']].min(axis=1) - df['Low']
    previous_close = df['Close'].shift(1)
    true_range = pd.concat([
        df['High'] - df['Low'],
        (df['High'] - previous_close).abs(),
        (df['Low'] - previous_close).abs(),
    ], axis=1).max(axis=1)
    df['ATR_14'] = true_range.rolling(window=14, min_periods=1).mean()

    delta = df['Close'].diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    avg_gain = gains.rolling(window=14, min_periods=14).mean()
    avg_loss = losses.rolling(window=14, min_periods=14).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df['RSI_14'] = 100 - (100 / (1 + rs))
    df['RSI_14'] = df['RSI_14'].fillna(50)

    if 'Volume' in df.columns and df['Volume'].notna().any():
        df['Volume_SMA_20'] = df['Volume'].rolling(window=20, min_periods=1).mean()
        df['Volume_Ratio'] = df['Volume'] / df['Volume_SMA_20'].replace(0, np.nan)
    else:
        df['Volume'] = np.nan
        df['Volume_SMA_20'] = np.nan
        df['Volume_Ratio'] = np.nan

    typical_price = (df['High'] + df['Low'] + df['Close']) / 3
    df['Session_Average'] = typical_price.expanding().mean()
    return df

# Generate trading signals based on lower highs/lows and SMAs
def generate_sell_signals(df, pip_size=0.0001):
    signals = []
    for i in range(1, len(df)):
        entry_price = float(df.iloc[i]['Close'])
        body_ratio = safe_divide(df.iloc[i]['Body_Size'], df.iloc[i]['Candle_Range'], default=0.0)
        rsi_value = float(df.iloc[i].get('RSI_14', 50)) if pd.notna(df.iloc[i].get('RSI_14', np.nan)) else 50.0

        # SELL setup: lower highs/lows in a downtrend
        if df['Lower_High'][i] and df['Lower_Low'][i]:
            if df['SMA_50'][i] < df['SMA_200'][i] and df['Close'][i] < df['SMA_50'][i]:
                start_idx = max(0, i - 5)
                recent_high = float(df['High'].iloc[start_idx:i+1].max())
                stop_loss = max(recent_high, entry_price)
                risk = stop_loss - entry_price
                if risk <= 0:
                    risk = entry_price * 0.001
                take_profit = entry_price - 2 * risk

                probability_score = 55
                if rsi_value < 45:
                    probability_score += 8
                if body_ratio >= 0.5:
                    probability_score += 6
                if df.iloc[i]['Close'] < df.iloc[i]['Open']:
                    probability_score += 4
                success_probability = int(round(clamp(probability_score, 35, 85)))

                stop_loss, take_profit = apply_pip_limits(
                    entry_price, stop_loss, take_profit, 'Sell', pip_size
                )
                signals.append({
                    'timestamp': df.iloc[i]['Datetime'],
                    'signal': 'Sell',
                    'setup': 'Technical Pattern',
                    'type': 'Technical Pattern',
                    'price': entry_price,
                    'timeframe': '1m',
                    'stop_loss': float(round(stop_loss, 5)),
                    'take_profit': float(round(take_profit, 5)),
                    'risk_reward': f"{round(abs(take_profit - entry_price) / max(abs(entry_price - stop_loss), pip_size), 2)}:1",
                    'success_probability': success_probability,
                    'confidence_notes': 'trend continuation met lower highs/lows',
                })

        # BUY setup: higher highs/lows in an uptrend
        if df['Higher_High'][i] and df['Higher_Low'][i]:
            if df['SMA_50'][i] > df['SMA_200'][i] and df['Close'][i] > df['SMA_50'][i]:
                start_idx = max(0, i - 5)
                recent_low = float(df['Low'].iloc[start_idx:i+1].min())
                stop_loss = min(recent_low, entry_price)
                risk = entry_price - stop_loss
                if risk <= 0:
                    risk = entry_price * 0.001
                take_profit = entry_price + 2 * risk

                probability_score = 55
                if rsi_value > 55:
                    probability_score += 8
                if body_ratio >= 0.5:
                    probability_score += 6
                if df.iloc[i]['Close'] > df.iloc[i]['Open']:
                    probability_score += 4
                success_probability = int(round(clamp(probability_score, 35, 85)))

                stop_loss, take_profit = apply_pip_limits(
                    entry_price, stop_loss, take_profit, 'Buy', pip_size
                )
                signals.append({
                    'timestamp': df.iloc[i]['Datetime'],
                    'signal': 'Buy',
                    'setup': 'Technical Pattern',
                    'type': 'Technical Pattern',
                    'price': entry_price,
                    'timeframe': '1m',
                    'stop_loss': float(round(stop_loss, 5)),
                    'take_profit': float(round(take_profit, 5)),
                    'risk_reward': f"{round(abs(take_profit - entry_price) / max(abs(entry_price - stop_loss), pip_size), 2)}:1",
                    'success_probability': success_probability,
                    'confidence_notes': 'trend continuation met higher highs/lows',
                })
    return signals


def format_trade_level(value):
    if value is None or pd.isna(value):
        return "n/a"
    return f"{float(value):.5f}"


def format_probability(value):
    if value is None or pd.isna(value):
        return "n/a"
    return f"{int(round(float(value)))}%"


def format_timestamp(value):
    timestamp = pd.to_datetime(value, errors='coerce')
    if pd.isna(timestamp):
        return "n/a"
    return timestamp.strftime("%d-%m-%y | %H:%M:%S")


def get_signal_setup_label(signal):
    setup_value = str(signal.get('setup', '')).strip()
    signal_type = str(signal.get('type', '')).strip()

    if signal_type and signal_type.lower() == 'technical pattern':
        return 'Technical Pattern'

    if signal_type:
        type_parts = signal_type.split()
        if type_parts and type_parts[0].lower() in {'1m', '5m', '15m', '30m'}:
            signal_type = ' '.join(type_parts[1:]).strip()
        if signal_type:
            return signal_type

    return setup_value or 'Unknown'


def join_human_readable(items):
    cleaned_items = [str(item).strip() for item in items if str(item).strip()]
    if not cleaned_items:
        return ""
    if len(cleaned_items) == 1:
        return cleaned_items[0]
    if len(cleaned_items) == 2:
        return f"{cleaned_items[0]} en {cleaned_items[1]}"
    return f"{', '.join(cleaned_items[:-1])} en {cleaned_items[-1]}"


def combine_signal_types(signal_types):
    unique_types = list(dict.fromkeys(str(signal_type).strip() for signal_type in signal_types if str(signal_type).strip()))
    if not unique_types:
        return ""
    if len(unique_types) == 1:
        return unique_types[0]

    split_types = [signal_type.split() for signal_type in unique_types]
    prefix_length = 0
    for word_group in zip(*split_types):
        if len(set(word_group)) == 1:
            prefix_length += 1
        else:
            break

    if prefix_length == 0:
        return join_human_readable(unique_types)

    prefix = " ".join(split_types[0][:prefix_length])
    suffixes = [" ".join(words[prefix_length:]).strip() for words in split_types]
    if not all(suffixes):
        return join_human_readable(unique_types)

    return f"{prefix} {join_human_readable(suffixes)}"


def price_values_match(left_value, right_value, tolerance=ALERT_PRICE_TOLERANCE):
    if pd.isna(left_value) or pd.isna(right_value):
        return True
    return abs(float(left_value) - float(right_value)) <= tolerance


def signals_match_for_alert_group(reference_signal, candidate_signal, price_tolerance=ALERT_PRICE_TOLERANCE, time_tolerance_minutes=ALERT_TIME_TOLERANCE_MINUTES):
    reference_timestamp = pd.to_datetime(reference_signal.get('timestamp'), errors='coerce')
    candidate_timestamp = pd.to_datetime(candidate_signal.get('timestamp'), errors='coerce')
    if pd.isna(reference_timestamp) or pd.isna(candidate_timestamp):
        return False

    if abs(candidate_timestamp - reference_timestamp) > pd.Timedelta(minutes=time_tolerance_minutes):
        return False

    for field_name in ('timeframe', 'signal'):
        if str(reference_signal.get(field_name, '')) != str(candidate_signal.get(field_name, '')):
            return False

    for field_name in ('price', 'stop_loss', 'take_profit'):
        if not price_values_match(reference_signal.get(field_name, np.nan), candidate_signal.get(field_name, np.nan), tolerance=price_tolerance):
            return False

    return True


def build_alert_group_signature(signal, price_tolerance=ALERT_PRICE_TOLERANCE):
    def _bucket(value):
        if pd.isna(value):
            return 'na'
        return str(int(round(float(value) / price_tolerance)))

    return "|".join([
        str(signal.get('timeframe', '')),
        str(signal.get('signal', '')),
        _bucket(signal.get('price', np.nan)),
        _bucket(signal.get('stop_loss', np.nan)),
        _bucket(signal.get('take_profit', np.nan)),
    ])


def build_repeat_alert_signature(signal, instrument_label, price_tolerance=ALERT_REPEAT_PRICE_TOLERANCE):
    def _bucket(value):
        if pd.isna(value):
            return 'na'
        return str(int(round(float(value) / price_tolerance)))

    return "|".join([
        str(instrument_label),
        str(signal.get('timeframe', '')),
        str(signal.get('signal', '')),
        _bucket(signal.get('price', np.nan)),
        _bucket(signal.get('stop_loss', np.nan)),
        _bucket(signal.get('take_profit', np.nan)),
    ])


def prune_recent_alert_groups(recent_alert_groups, max_age_minutes=ALERT_REPEAT_COOLDOWN_MINUTES):
    if not recent_alert_groups:
        return {}

    cutoff_timestamp = pd.Timestamp.utcnow() - pd.Timedelta(minutes=max_age_minutes)
    pruned_groups = {}

    for signature, timestamp_value in recent_alert_groups.items():
        parsed_timestamp = pd.to_datetime(timestamp_value, errors='coerce', utc=True)
        if pd.isna(parsed_timestamp) or parsed_timestamp < cutoff_timestamp:
            continue
        pruned_groups[signature] = parsed_timestamp.isoformat()

    return pruned_groups


def cluster_signals_for_alerts(signal_df, price_tolerance=ALERT_PRICE_TOLERANCE, time_tolerance_minutes=ALERT_TIME_TOLERANCE_MINUTES):
    if signal_df is None or signal_df.empty:
        return []

    sorted_signal_df = signal_df.sort_values('timestamp').reset_index(drop=True)
    clusters = []

    for _, row in sorted_signal_df.iterrows():
        signal = row.to_dict()
        signal_timestamp = pd.to_datetime(signal.get('timestamp'), errors='coerce')
        assigned_cluster = None

        for cluster in clusters:
            if signals_match_for_alert_group(
                cluster['reference_signal'],
                signal,
                price_tolerance=price_tolerance,
                time_tolerance_minutes=time_tolerance_minutes,
            ):
                assigned_cluster = cluster
                break

        if assigned_cluster is None:
            clusters.append({
                'reference_signal': signal,
                'signals': [signal],
                'alert_ids': [signal.get('alert_id')],
                'latest_timestamp': signal_timestamp,
            })
            continue

        assigned_cluster['signals'].append(signal)
        assigned_cluster['alert_ids'].append(signal.get('alert_id'))
        if pd.notna(signal_timestamp) and (
            pd.isna(assigned_cluster['latest_timestamp']) or signal_timestamp > assigned_cluster['latest_timestamp']
        ):
            assigned_cluster['latest_timestamp'] = signal_timestamp

    return clusters


def sort_records_by_timestamp(records):
    return sorted(
        records,
        key=lambda record: (
            pd.to_datetime(record.get('timestamp'), errors='coerce'),
            record.get('success_probability', 0),
        ),
        reverse=True,
    )


def sort_dataframe_by_timestamp(df, timestamp_column='timestamp'):
    if df is None or df.empty or timestamp_column not in df.columns:
        return df

    sorted_df = df.copy()
    sorted_df[timestamp_column] = pd.to_datetime(sorted_df[timestamp_column], errors='coerce')
    sort_columns = [timestamp_column]
    ascending = [False]

    if 'success_probability' in sorted_df.columns:
        sort_columns.append('success_probability')
        ascending.append(False)

    return sorted_df.sort_values(by=sort_columns, ascending=ascending).reset_index(drop=True)


def get_timeframe_minutes(timeframe_label):
    mapping = {
        '1m': 1,
        '5m': 5,
        '15m': 15,
        '30m': 30,
    }
    return mapping.get(str(timeframe_label).lower(), 1)


def build_signal_alert_id(signal, instrument_label):
    timestamp = pd.to_datetime(signal.get('timestamp'), errors='coerce')
    timestamp_value = timestamp.isoformat() if pd.notna(timestamp) else 'na'
    timeframe = str(signal.get('timeframe', ''))
    direction = str(signal.get('signal', ''))
    signal_type = str(signal.get('type', ''))
    price = signal.get('price', np.nan)
    price_value = f"{float(price):.5f}" if pd.notna(price) else 'na'
    return "|".join([instrument_label, timeframe, direction, signal_type, price_value, timestamp_value])


def inject_dashboard_styles():
    st.markdown(
        """
        <style>
        :root {
            --accent-primary: #3d6f96;
            --accent-secondary: #c7ddee;
            --accent-soft: #eef5fb;
            --accent-warm: #f8f2e7;
            --surface-card: rgba(255, 255, 255, 0.78);
            --surface-card-strong: rgba(255, 255, 255, 0.92);
            --border-soft: rgba(61, 111, 150, 0.18);
            --text-main: #213547;
            --text-muted: #63788c;
        }

        .stApp {
            background: linear-gradient(180deg, #f5f9fd 0%, #fbfdff 35%, #f6f8fb 100%);
            color: var(--text-main);
        }

        .block-container {
            padding-top: 1.4rem;
        }

        section[data-testid="stSidebar"] {
            background: linear-gradient(180deg, #edf4fa 0%, #f8fbff 100%);
            border-right: 1px solid rgba(61, 111, 150, 0.12);
        }

        div[data-testid="stMetric"] {
            background: linear-gradient(180deg, var(--surface-card-strong) 0%, var(--surface-card) 100%);
            border: 1px solid var(--border-soft);
            border-radius: 16px;
            padding: 0.9rem 1rem;
            box-shadow: 0 8px 24px rgba(31, 53, 71, 0.05);
        }

        div[data-testid="stMetricLabel"] {
            color: var(--text-muted);
            font-weight: 600;
        }

        div[data-testid="stMetricValue"] {
            color: var(--text-main);
        }

        .dashboard-intro {
            background: linear-gradient(135deg, rgba(61, 111, 150, 0.10) 0%, rgba(248, 242, 231, 0.70) 100%);
            border: 1px solid rgba(61, 111, 150, 0.14);
            border-radius: 18px;
            padding: 1rem 1.15rem;
            margin: 0.4rem 0 1rem 0;
            box-shadow: 0 10px 30px rgba(31, 53, 71, 0.04);
        }

        .dashboard-intro-title {
            color: var(--accent-primary);
            font-size: 1rem;
            font-weight: 700;
            margin-bottom: 0.35rem;
        }

        .dashboard-intro-text {
            color: var(--text-main);
            margin: 0;
            line-height: 1.55;
        }

        .dashboard-intro-text strong {
            color: var(--accent-primary);
        }

        h2, h3 {
            color: #284b68;
        }

        .stTabs [data-baseweb="tab-list"] {
            gap: 0.45rem;
        }

        .stTabs [data-baseweb="tab"] {
            background: rgba(61, 111, 150, 0.07);
            border: 1px solid rgba(61, 111, 150, 0.10);
            border-radius: 12px 12px 0 0;
            color: var(--text-muted);
            padding: 0.55rem 0.95rem;
        }

        .stTabs [aria-selected="true"] {
            background: rgba(61, 111, 150, 0.16) !important;
            color: var(--accent-primary) !important;
            font-weight: 700;
        }

        div[data-testid="stDataFrame"],
        div[data-testid="stTable"] {
            background: var(--surface-card);
            border: 1px solid var(--border-soft);
            border-radius: 16px;
            overflow: visible;
            box-shadow: 0 8px 24px rgba(31, 53, 71, 0.04);
        }

        div[data-testid="stDataFrame"] > div,
        div[data-testid="stTable"] > div {
            overflow-x: auto !important;
            overflow-y: auto !important;
            border-radius: 16px;
        }

        div[data-testid="stDataFrame"] thead tr th,
        div[data-testid="stTable"] thead tr th {
            background: rgba(61, 111, 150, 0.08) !important;
            color: #355a78 !important;
        }

        div[data-testid="stExpander"] {
            background: rgba(255, 255, 255, 0.72);
            border: 1px solid var(--border-soft);
            border-radius: 14px;
        }

        div[data-testid="stAlert"] {
            border-radius: 14px;
            border-width: 1px;
        }

        .stCaption {
            color: var(--text-muted);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

# Streamlit app main function
def main():
    st.set_page_config(page_title="Supply/Demand Trading Dashboard", page_icon="📊", layout="wide")
    inject_dashboard_styles()
    st.title("📊 Advanced Supply/Demand Zone Trading Dashboard")
    
    st.markdown(
        """
        <div class="dashboard-intro">
            <div class="dashboard-intro-title">Overzicht dashboard</div>
            <p class="dashboard-intro-text">
                <strong>📈 Live Forex candles only</strong> ·
                <strong>🎯 Supply &amp; Demand zones</strong> ·
                <strong>🧭 Multi-timeframe signalanalyse</strong> ·
                <strong>📰 Nieuwsfilter</strong> ·
                <strong>🔔 Telegram alerts</strong>
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Sidebar settings
    st.sidebar.title("⚙️ Settings")

    # NIEUW: Primary Timeframe Selector
    st.sidebar.subheader("⏰ Primary Chart Timeframe")
    primary_timeframe_choice = st.sidebar.selectbox(
        "Select Primary Chart",
        ["M1 (1 minute)", "M5 (5 minutes)", "M15 (15 minutes)", "M30 (30 minutes)"],
        index=2,
        help="This timeframe will be shown in the main chart"
    )

    # Map naar config
    timeframe_config = {
        "M1 (1 minute)": {"freq": "1min", "periods": 1500, "label": "1m", "lookback": 50},
        "M5 (5 minutes)": {"freq": "5min", "periods": 500, "label": "5m", "lookback": 40},
        "M15 (15 minutes)": {"freq": "15min", "periods": 300, "label": "15m", "lookback": 30},
        "M30 (30 minutes)": {"freq": "30min", "periods": 200, "label": "30m", "lookback": 20},
    }

    tf_config = timeframe_config[primary_timeframe_choice]
    primary_freq = tf_config["freq"]
    primary_periods = tf_config["periods"]
    primary_label = tf_config["label"]
    zone_lookback = tf_config["lookback"]

    instrument_type = "Forex"
    base_currency = st.sidebar.selectbox("Base Currency", ["GBP", "EUR", "USD"], index=0)
    target_currency = st.sidebar.selectbox("Target Currency", ["USD", "EUR", "GBP"], index=0)
    index_choice = None

    auto_refresh_enabled = st.sidebar.checkbox("Auto refresh live data", value=True)
    refresh_seconds = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 15)
    if auto_refresh_enabled:
        st_autorefresh(interval=int(refresh_seconds * 1000), key="signals_live_refresh")
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("📊 Analysis Options")
    show_supply_demand = st.sidebar.checkbox("Show Supply/Demand Zones", value=True)
    show_technical = st.sidebar.checkbox("Show Technical Signals", value=False)
    show_signal_markers = st.sidebar.checkbox("Show signal markers on chart", value=False)
    show_trade_levels = st.sidebar.checkbox("Show TP/SL levels on chart", value=False)
    m15_high_win_rate_mode = st.sidebar.checkbox("M15 high win-rate mode", value=True)

    st.sidebar.markdown("---")
    st.sidebar.subheader("📰 Nieuws & Sentiment")
    show_news = st.sidebar.checkbox("Analyseer marktnieuws", value=True)
    default_news_key = get_config_value("NEWSDATA_API_KEY", get_config_value("NEWS_API_KEY", ""))
    news_api_key = st.sidebar.text_input(
        "NewsData.io API key (optioneel)", value=default_news_key, type="password"
    )
    news_language = st.sidebar.selectbox(
        "Nieuwstaal",
        ["en", "de", "fr", "es", "nl"],
        index=0,
    )

    st.sidebar.markdown("---")
    st.sidebar.subheader("📈 Echte Forex-data (Twelve Data)")
    default_fx_key = get_config_value("TWELVEDATA_API_KEY", TWELVEDATA_API_KEY_DEFAULT)
    fx_api_key_input = st.sidebar.text_input(
        "Twelve Data API key (verplicht voor live candles)",
        value="" if default_fx_key else "",
        type="password",
    )

    # Gebruik de environment key als fallback wanneer veld leeg is
    fx_api_key = fx_api_key_input or default_fx_key

    if default_fx_key:
        st.sidebar.caption("Twelve Data key geladen uit omgeving (wordt gebruikt, ook als dit veld leeg lijkt).")

    st.sidebar.markdown("---")
    st.sidebar.subheader("🧪 Test Zone Settings")
    enable_test_zone = st.sidebar.checkbox("Enable Signal Backtest", value=True)
    starting_balance = st.sidebar.number_input(
        "Starting Balance",
        min_value=1000.0,
        max_value=1000000.0,
        value=10000.0,
        step=1000.0,
    )
    pip_value_money = st.sidebar.number_input(
        "Value per Pip",
        min_value=0.1,
        max_value=100.0,
        value=1.0,
        step=0.1,
    )

    st.sidebar.markdown("---")
    st.sidebar.subheader("🔔 Alert Settings")
    default_tg_token = get_config_value("TELEGRAM_TOKEN", TELEGRAM_TOKEN_DEFAULT)
    default_tg_chat = get_config_value("TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID_DEFAULT)

    tg_token_input = st.sidebar.text_input(
        "Telegram bot token (optioneel)",
        value=default_tg_token,
        type="password",
    )
    tg_chat_input = st.sidebar.text_input(
        "Telegram chat ID (optioneel)",
        value=default_tg_chat,
    )

    # Sla UI-waarden op in session_state zodat send_telegram_alert ze kan gebruiken
    st.session_state["TELEGRAM_TOKEN_UI"] = tg_token_input
    st.session_state["TELEGRAM_CHAT_ID_UI"] = tg_chat_input

    if "ENABLE_TELEGRAM_ALERTS" not in st.session_state:
        st.session_state["ENABLE_TELEGRAM_ALERTS"] = True

    enable_alerts = st.sidebar.checkbox(
        "Enable Telegram alerts for new signals",
        value=st.session_state["ENABLE_TELEGRAM_ALERTS"],
    )
    st.session_state["ENABLE_TELEGRAM_ALERTS"] = enable_alerts
    if enable_alerts:
        st.sidebar.caption("Telegram-alerts staan standaard aan bij het openen van de app.")

    if st.sidebar.button("📨 Stuur testbericht naar Telegram"):
        send_telegram_alert("Test alert vanuit je Streamlit dashboard ✅")
        st.sidebar.success("Testbericht verstuurd (als token/chat ID kloppen).")

    st.sidebar.markdown("---")
    st.sidebar.subheader("🤖 MT5 Auto Execution")
    available_mt5, mt5_message = mt5_is_available()
    if "ENABLE_MT5_AUTO_EXECUTION" not in st.session_state:
        st.session_state["ENABLE_MT5_AUTO_EXECUTION"] = False

    enable_mt5_execution = st.sidebar.checkbox(
        "Enable MT5 auto execution",
        value=st.session_state["ENABLE_MT5_AUTO_EXECUTION"],
    )
    st.session_state["ENABLE_MT5_AUTO_EXECUTION"] = enable_mt5_execution

    default_mt5_login = get_config_value("MT5_LOGIN", "")
    default_mt5_password = get_config_value("MT5_PASSWORD", "")
    default_mt5_server = get_config_value("MT5_SERVER", "")
    default_mt5_path = get_config_value("MT5_PATH", "")
    default_mt5_prefix = get_config_value("MT5_SYMBOL_PREFIX", "")
    default_mt5_suffix = get_config_value("MT5_SYMBOL_SUFFIX", "")

    mt5_login_input = st.sidebar.text_input("MT5 login", value=default_mt5_login)
    mt5_password_input = st.sidebar.text_input("MT5 password", value=default_mt5_password, type="password")
    mt5_server_input = st.sidebar.text_input("MT5 server", value=default_mt5_server)
    mt5_path_input = st.sidebar.text_input("MT5 terminal path (optioneel)", value=default_mt5_path)
    mt5_symbol_prefix = st.sidebar.text_input("MT5 symbol prefix", value=default_mt5_prefix)
    mt5_symbol_suffix = st.sidebar.text_input("MT5 symbol suffix", value=default_mt5_suffix)
    mt5_use_risk_sizing = st.sidebar.checkbox("Gebruik risk-based lot size", value=True)
    mt5_risk_percent = st.sidebar.slider("MT5 risico per trade (%)", 0.1, 5.0, 1.0, 0.1)
    mt5_fixed_volume = st.sidebar.number_input("MT5 fixed lot size", min_value=0.01, max_value=50.0, value=0.01, step=0.01)

    if enable_mt5_execution:
        if available_mt5:
            st.sidebar.caption("MT5 auto execution staat aan. Orders worden als market orders met SL/TP verstuurd.")
        else:
            st.sidebar.warning(f"MT5 Python bridge niet beschikbaar: {mt5_message}")

    mt5_config = MT5ExecutionConfig(
        login=int(mt5_login_input) if str(mt5_login_input).strip().isdigit() else None,
        password=mt5_password_input,
        server=mt5_server_input,
        terminal_path=mt5_path_input,
        risk_percent=float(mt5_risk_percent),
        fixed_volume=float(mt5_fixed_volume),
        use_risk_sizing=mt5_use_risk_sizing,
        symbol_prefix=mt5_symbol_prefix,
        symbol_suffix=mt5_symbol_suffix,
    )
    mt5_execution_active = enable_mt5_execution and available_mt5

    # Single-pass render
    placeholder = st.empty()

    with placeholder.container():
        instrument_label = f"{base_currency}/{target_currency}"
        pip_size = 0.0001

        if not fx_api_key:
            st.error("Voer een Twelve Data API key in om alleen live forexdata te gebruiken.")
            return

        # Nieuws & sentiment ophalen voor dit instrument (als geactiveerd)
        news_articles = []
        news_sentiment = None
        if show_news and news_api_key:
            query = build_news_query(
                instrument_type,
                base_currency=base_currency,
                target_currency=target_currency,
                index_choice=index_choice,
            )
            news_articles = fetch_news_articles(query, news_api_key, language=news_language)
            news_sentiment = analyze_news_sentiment(news_articles)

        col1, col2, col3 = st.columns(3)

        # Generate PRIMARY timeframe historical data
        df_primary = generate_historical_data(
            primary_periods,
            primary_freq,
            base_currency=base_currency,
            target_currency=target_currency,
            fx_api_key=fx_api_key,
        )
        market_data_meta = st.session_state.get('latest_market_data_meta', {})
        if df_primary.empty:
            error_message = market_data_meta.get('error') or 'onbekende fout'
            st.warning(f"Geen candledata ontvangen van Twelve Data. Laatste fout: {error_message}")
            return

        # Add technical indicators
        df_primary = add_technical_indicators(df_primary)
        latest_price = float(df_primary.iloc[-1]['Close'])
        latest_open = float(df_primary.iloc[-1]['Open'])
        latest_range_pips = (float(df_primary.iloc[-1]['High']) - float(df_primary.iloc[-1]['Low'])) / pip_size
        latest_candle_time = pd.to_datetime(df_primary.iloc[-1]['Datetime'])

        col1.metric(label=f"💱 {instrument_label}", value=f"{latest_price:.5f}")
        col2.metric(
            label="🕯️ Laatste candle",
            value=f"{latest_range_pips:.1f} pips",
            delta=f"{'Bullish' if latest_price >= latest_open else 'Bearish'} close"
        )
        col3.metric(
            label="🕒 Laatste update candle (Amsterdam)",
            value=format_timestamp(latest_candle_time),
            delta=primary_label.upper(),
        )

        data_source = market_data_meta.get('source')
        cache_age = market_data_meta.get('age_seconds')
        fetched_at = market_data_meta.get('fetched_at')
        api_error = market_data_meta.get('error')

        if data_source == 'live' and fetched_at is not None:
            st.caption(
                f"Live candles opgehaald via Twelve Data om {format_timestamp(fetched_at)} Amsterdam-tijd."
            )
        elif data_source == 'cache' and fetched_at is not None:
            st.caption(
                f"UI refresht uit lokale cache om onder de free-plan limiet te blijven. Laatste API-call: {format_timestamp(fetched_at)} ({int(cache_age)}s geleden)."
            )
        elif data_source == 'stale_cache' and fetched_at is not None:
            st.warning(
                f"Twelve Data werd tijdelijk niet opnieuw aangeroepen ({api_error}). Laatste bruikbare candles uit cache van {format_timestamp(fetched_at)} worden getoond."
            )

        if df_primary['Volume'].notna().any():
            st.caption("Kansscore gebruikt live volume-participatie uit de providerfeed.")
        else:
            st.caption("Twelve Data levert voor dit FX-paar geen volume mee; kansscore gebruikt daarom keylevels, candle-structuur en trendconfluence.")

        if m15_high_win_rate_mode:
            st.caption("M15 high win-rate mode is actief: strengere filtering, retest-continuations en kortere TP-targets.")

        # Build HIGHER timeframes
        df_idx = df_primary.set_index('Datetime')

        def _resample_ohlcv(frame, rule):
            aggregation = {
                'Open': 'first',
                'High': 'max',
                'Low': 'min',
                'Close': 'last',
            }
            if 'Volume' in frame.columns:
                aggregation['Volume'] = 'sum'

            r = frame.resample(rule).agg(aggregation).dropna()
            r = r.reset_index()
            r = add_technical_indicators(r)
            return r

        # Dynamically create higher timeframes
        higher_tfs = []
        df_5m = None
        df_15m = None
        df_30m = None

        if primary_label == "1m":
            df_5m = _resample_ohlcv(df_idx, '5min')
            df_15m = _resample_ohlcv(df_idx, '15min')
            df_30m = _resample_ohlcv(df_idx, '30min')
            higher_tfs = [("5m", df_5m), ("15m", df_15m), ("30m", df_30m)]
            df_for_zones = df_15m  # Zones from 15m
        elif primary_label == "5m":
            df_15m = _resample_ohlcv(df_idx, '15min')
            df_30m = _resample_ohlcv(df_idx, '30min')
            higher_tfs = [("15m", df_15m), ("30m", df_30m)]
            df_for_zones = df_15m
        elif primary_label == "15m":
            df_30m = _resample_ohlcv(df_idx, '30min')
            higher_tfs = [("30m", df_30m)]
            df_for_zones = df_primary
        else:  # 30m
            df_for_zones = df_primary

        # Identify zones
        supply_demand_zones = []
        if show_supply_demand:
            supply_demand_zones = identify_supply_demand_zones(df_for_zones, lookback=zone_lookback)

        col3.metric(
            label="🎯 Gedetecteerde zones",
            value=len(supply_demand_zones),
            delta=primary_label.upper()
        )

        # Nieuwssentiment tonen (als beschikbaar)
        if show_news:
            if news_api_key and news_sentiment is not None:
                st.metric(
                    label="📰 Nieuwssentiment",
                    value=news_sentiment.get("label", "Neutraal"),
                    delta=f"Score: {news_sentiment.get('score', 0.0):.2f}",
                )
            elif not news_api_key:
                st.info("Voer je NewsData.io key in de sidebar in om nieuws te analyseren.")
        
        # Generate signals based on primary + higher timeframes
        all_signals = []
        m5_ms_signals = []
        m15_ms_signals = []
        m30_ms_signals = []

        # Primary timeframe signals
        if show_supply_demand and supply_demand_zones:
            sd_signals = generate_supply_demand_signals(
                df_primary,
                supply_demand_zones,
                pip_size=pip_size,
                timeframe_label=primary_label,
                high_win_rate_mode=m15_high_win_rate_mode,
            )
            all_signals.extend(sd_signals)

        # Higher timeframe signals
        for tf_label, tf_df in higher_tfs:
            if show_supply_demand and supply_demand_zones:
                if tf_label == "5m" and df_5m is not None:
                    ms_htf = generate_m5_market_structure_signals(
                        tf_df, supply_demand_zones, pip_size=pip_size
                    )
                elif tf_label == "15m" and df_15m is not None:
                    ms_htf = generate_m15_market_structure_signals(
                        tf_df,
                        supply_demand_zones,
                        pip_size=pip_size,
                        high_win_rate_mode=m15_high_win_rate_mode,
                    )
                elif tf_label == "30m" and df_30m is not None:
                    ms_htf = generate_m30_market_structure_signals(
                        tf_df, supply_demand_zones, pip_size=pip_size
                    )
                else:
                    ms_htf = []

                if tf_label == "5m":
                    m5_ms_signals = ms_htf
                elif tf_label == "15m":
                    m15_ms_signals = ms_htf
                elif tf_label == "30m":
                    m30_ms_signals = ms_htf

                all_signals.extend(ms_htf)

        if show_technical:
            technical_signals = generate_sell_signals(df_primary, pip_size=pip_size)
            for sig in technical_signals:
                sig['timeframe'] = primary_label
            all_signals.extend(technical_signals)

        all_signals = [sig for sig in all_signals if sig.get('timeframe') != '30m']

        if all_signals:
            all_signals = sort_records_by_timestamp(all_signals)

        # Filter alle signalen op basis van nieuws-sentiment (optioneel)
        if show_news and news_sentiment is not None and all_signals:
            before_n = len(all_signals)
            all_signals = filter_signals_by_news(all_signals, news_sentiment)
            after_n = len(all_signals)
            removed = before_n - after_n
            if removed > 0:
                st.info(
                    f"{removed} signalen gefilterd door nieuwssentiment: {news_sentiment.get('label', '')}"
                )

        # Alerts voor nieuwe signalen (per run, per instrument)
        if (enable_alerts or mt5_execution_active) and all_signals:
            signal_df_alert = pd.DataFrame(all_signals).copy()
            if 'timestamp' in signal_df_alert.columns:
                signal_df_alert['timestamp'] = pd.to_datetime(signal_df_alert['timestamp'])
            signal_df_alert['alert_id'] = signal_df_alert.apply(
                lambda row: build_signal_alert_id(row, instrument_label),
                axis=1,
            )

            # Unieke key per instrument, zodat elk valutapaar apart telt
            alert_key = f"last_alert_ts::{instrument_label}"
            sent_alerts_key = f"sent_alert_ids::{instrument_label}"
            recent_alert_groups_key = f"recent_alert_groups::{instrument_label}"
            mt5_executed_groups_key = f"mt5_executed_groups::{instrument_label}"
            sent_alert_ids = set(st.session_state.get(sent_alerts_key, []))
            recent_alert_groups = prune_recent_alert_groups(
                st.session_state.get(recent_alert_groups_key, {}),
                max_age_minutes=ALERT_REPEAT_COOLDOWN_MINUTES,
            )
            mt5_executed_groups = prune_recent_alert_groups(
                st.session_state.get(mt5_executed_groups_key, {}),
                max_age_minutes=ALERT_REPEAT_COOLDOWN_MINUTES,
            )

            if alert_key not in st.session_state:
                if 'timestamp' in signal_df_alert.columns and not signal_df_alert['timestamp'].empty:
                    latest_signal_ts = signal_df_alert['timestamp'].max()
                    bootstrap_minutes = max(get_timeframe_minutes(primary_label) * 2, 3)
                    bootstrap_ts = latest_signal_ts - pd.Timedelta(minutes=bootstrap_minutes)
                    st.session_state[alert_key] = bootstrap_ts
                    st.info("Telegram-alerts geactiveerd: recente signalen worden nu verstuurd, daarna alleen nieuwe signalen.")
                else:
                    st.session_state[alert_key] = pd.Timestamp.utcnow()

            last_ts = st.session_state[alert_key]
            new_mask = signal_df_alert['timestamp'] > last_ts
            unsent_mask = ~signal_df_alert['alert_id'].isin(sent_alert_ids)
            new_signals = signal_df_alert[new_mask & unsent_mask]

            if not new_signals.empty:
                sent_cluster_count = 0
                mt5_execution_count = 0
                mt5_errors = []
                clustered_signals = cluster_signals_for_alerts(new_signals)

                for cluster in clustered_signals:
                    representative_signal = cluster['reference_signal']
                    repeat_signature = build_repeat_alert_signature(representative_signal, instrument_label)
                    last_group_timestamp = pd.to_datetime(recent_alert_groups.get(repeat_signature), errors='coerce', utc=True)
                    latest_cluster_timestamp = cluster.get('latest_timestamp')
                    latest_cluster_timestamp = pd.to_datetime(latest_cluster_timestamp, errors='coerce', utc=True)

                    if (
                        pd.notna(last_group_timestamp)
                        and pd.notna(latest_cluster_timestamp)
                        and latest_cluster_timestamp - last_group_timestamp <= pd.Timedelta(minutes=ALERT_REPEAT_COOLDOWN_MINUTES)
                    ):
                        for alert_id in cluster['alert_ids']:
                            if alert_id:
                                sent_alert_ids.add(alert_id)
                        continue

                    tf = representative_signal.get('timeframe', primary_label)
                    direction = representative_signal.get('signal', '')
                    price = representative_signal.get('price', np.nan)
                    stop_loss = representative_signal.get('stop_loss', np.nan)
                    take_profit = representative_signal.get('take_profit', np.nan)
                    ts_str = format_timestamp(representative_signal.get('timestamp'))
                    combined_type = combine_signal_types([signal.get('type', '') for signal in cluster['signals']])

                    sl_text = f"SL {stop_loss:.5f}" if pd.notna(stop_loss) else "SL n/a"
                    tp_text = f"TP {take_profit:.5f}" if pd.notna(take_profit) else "TP n/a"
                    msg = (
                        f"{instrument_label} | {tf} {direction} @ {price:.5f} | "
                        f"{sl_text} | {tp_text} | {combined_type} | {ts_str}"
                    )

                    if enable_alerts:
                        send_telegram_alert(msg)
                        sent_cluster_count += 1

                    if mt5_execution_active:
                        last_mt5_timestamp = pd.to_datetime(mt5_executed_groups.get(repeat_signature), errors='coerce', utc=True)
                        if not (
                            pd.notna(last_mt5_timestamp)
                            and pd.notna(latest_cluster_timestamp)
                            and latest_cluster_timestamp - last_mt5_timestamp <= pd.Timedelta(minutes=ALERT_REPEAT_COOLDOWN_MINUTES)
                        ):
                            execution_result = place_signal_order(
                                representative_signal,
                                instrument_label,
                                mt5_config,
                                order_comment=f"AI {tf} {direction}",
                            )
                            if execution_result.success:
                                mt5_execution_count += 1
                                if pd.notna(latest_cluster_timestamp):
                                    mt5_executed_groups[repeat_signature] = latest_cluster_timestamp.isoformat()
                            else:
                                mt5_errors.append(execution_result.message)

                    for alert_id in cluster['alert_ids']:
                        if alert_id:
                            sent_alert_ids.add(alert_id)

                    if pd.notna(latest_cluster_timestamp):
                        recent_alert_groups[repeat_signature] = latest_cluster_timestamp.isoformat()

                # Toon ook een korte samenvatting in de UI
                if sent_cluster_count > 0:
                    st.success(f"🔔 {sent_cluster_count} nieuwe signalen verstuurd als alert.")
                if mt5_execution_count > 0:
                    st.success(f"🤖 {mt5_execution_count} MT5 order(s) geplaatst.")
                if mt5_errors:
                    st.warning(f"MT5 melding: {mt5_errors[0]}")

                st.session_state[alert_key] = new_signals['timestamp'].max()
                st.session_state[sent_alerts_key] = list(sent_alert_ids)
                st.session_state[recent_alert_groups_key] = recent_alert_groups
                st.session_state[mt5_executed_groups_key] = mt5_executed_groups

        # Nieuwssectie onder de signalen
        if show_news:
            st.subheader("📰 Laatste markt-nieuws voor dit instrument")
            if news_api_key and news_articles:
                for art in news_articles:
                    title = art.get("title", "(geen titel)")
                    desc = art.get("description") or ""
                    src = art.get("source") or ""
                    url = art.get("url") or ""
                    when = format_timestamp(art.get("publishedAt")) if art.get("publishedAt") else ""

                    st.markdown(
                        f"**{title}**  \n"
                        f"{desc}  \n"
                        f"Bron: {src} | {when}  \n"
                        f"[Open artikel]({url})"
                    )
            elif news_api_key and not news_articles:
                st.info("Geen relevante nieuwsartikelen gevonden voor dit instrument.")
            elif not news_api_key:
                st.info("Geen NewsData.io key opgegeven; nieuws wordt niet opgehaald.")
        
        # Display Supply/Demand Zones Summary
        if show_supply_demand and supply_demand_zones:
            st.subheader("🎯 Supply & Demand Zones")
            zone_col1, zone_col2 = st.columns(2)
            
            supply_zones = [z for z in supply_demand_zones if z['type'] == 'Supply']
            demand_zones = [z for z in supply_demand_zones if z['type'] == 'Demand']
            
            with zone_col1:
                st.markdown("### 🔴 SUPPLY ZONES (Resistance)")
                if supply_zones:
                    for idx, zone in enumerate(supply_zones[:3]):  # Show top 3
                        st.info(f"**Zone {idx+1}**: {zone['bottom']:.5f} - {zone['top']:.5f} | Strength: {zone['strength']}")
                else:
                    st.write("No supply zones detected")
            
            with zone_col2:
                st.markdown("### 🟢 DEMAND ZONES (Support)")
                if demand_zones:
                    for idx, zone in enumerate(demand_zones[:3]):  # Show top 3
                        st.success(f"**Zone {idx+1}**: {zone['bottom']:.5f} - {zone['top']:.5f} | Strength: {zone['strength']}")
                else:
                    st.write("No demand zones detected")
        
        # Display signals
        st.subheader("🎯 Trading Signals")
        if all_signals:
            signal_df = pd.DataFrame(all_signals)
            signal_df = sort_dataframe_by_timestamp(signal_df)

            display_columns = [
                'timestamp',
                'timeframe',
                'signal',
                'setup',
                'type',
                'price',
                'stop_loss',
                'take_profit',
                'success_probability',
                'key_level_type',
                'key_level_distance_pips',
                'volume_status',
                'risk_reward',
            ]
            
            if 'timeframe' not in signal_df.columns:
                signal_df['timeframe'] = '1m'

            top_setups = signal_df.head(8)
            st.markdown("### ⭐ Nieuwste signalen")

            best_levels = signal_df.head(3).reset_index(drop=True)
            st.markdown("### 📍 Instap-, SL- en TP-levels")
            level_columns = st.columns(min(3, len(best_levels)))

            for column, (_, best_signal) in zip(level_columns, best_levels.iterrows()):
                with column:
                    st.markdown(
                        (
                            f"**{best_signal.get('timeframe', '')} {best_signal.get('signal', '')}**  \n"
                            f"{best_signal.get('type', '')}"
                        )
                    )
                    st.success(f"Instap: {format_trade_level(best_signal.get('price'))}")
                    st.error(f"SL: {format_trade_level(best_signal.get('stop_loss'))}")
                    st.info(f"TP: {format_trade_level(best_signal.get('take_profit'))}")
                    st.caption(
                        f"Kans: {format_probability(best_signal.get('success_probability'))} | RR: {best_signal.get('risk_reward', 'n/a')}"
                    )

            display_df = top_setups[[col for col in display_columns if col in top_setups.columns]].copy()
            display_df = display_df.rename(columns={
                'price': 'entry_price',
                'stop_loss': 'sl',
                'take_profit': 'tp',
            })
            if 'timestamp' in display_df.columns:
                display_df['timestamp'] = display_df['timestamp'].apply(format_timestamp)
            if 'success_probability' in display_df.columns:
                display_df['success_probability'] = display_df['success_probability'].apply(format_probability)
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
            )

            visible_signal_timeframes = ['15m']
            signal_df = signal_df[signal_df['timeframe'].isin(visible_signal_timeframes)].reset_index(drop=True)

            unique_tfs = [
                timeframe
                for timeframe in visible_signal_timeframes
                if timeframe in set(signal_df['timeframe'].dropna().unique())
            ]
            if unique_tfs:
                tabs = st.tabs([f"{tf} Signals" for tf in unique_tfs])

                for tf, tab in zip(unique_tfs, tabs):
                    with tab:
                        tf_df = signal_df[signal_df['timeframe'] == tf]
                        if tf_df.empty:
                            st.info(f"No signals for {tf}.")
                        else:
                            summary_col1, summary_col2, summary_col3 = st.columns(3)
                            summary_col1.metric("Signals", len(tf_df))
                            summary_col2.metric("Gem. kans", f"{tf_df['success_probability'].mean():.0f}%")
                            summary_col3.metric("Buy / Sell", f"{(tf_df['signal'] == 'Buy').sum()} / {(tf_df['signal'] == 'Sell').sum()}")

                            tf_display_df = tf_df[[col for col in display_columns if col in tf_df.columns]].copy()
                            tf_display_df = tf_display_df.rename(columns={
                                'price': 'entry_price',
                                'stop_loss': 'sl',
                                'take_profit': 'tp',
                            })
                            if 'timestamp' in tf_display_df.columns:
                                tf_display_df['timestamp'] = tf_display_df['timestamp'].apply(format_timestamp)
                            if 'success_probability' in tf_display_df.columns:
                                tf_display_df['success_probability'] = tf_display_df['success_probability'].apply(format_probability)

                            st.dataframe(
                                tf_display_df,
                                use_container_width=True,
                                hide_index=True,
                            )

                            with st.expander(f"Details & motivatie voor {tf}"):
                                detail_columns = display_columns + [
                                    'zone_strength', 'nearest_key_level', 'confidence_notes'
                                ]
                                tf_detail_df = tf_df[[col for col in detail_columns if col in tf_df.columns]].copy()
                                tf_detail_df = tf_detail_df.rename(columns={
                                    'price': 'entry_price',
                                    'stop_loss': 'sl',
                                    'take_profit': 'tp',
                                })
                                if 'timestamp' in tf_detail_df.columns:
                                    tf_detail_df['timestamp'] = tf_detail_df['timestamp'].apply(format_timestamp)
                                if 'success_probability' in tf_detail_df.columns:
                                    tf_detail_df['success_probability'] = tf_detail_df['success_probability'].apply(format_probability)
                                st.dataframe(
                                    tf_detail_df,
                                    use_container_width=True,
                                    hide_index=True,
                                )
        else:
            st.info("No signals generated based on current data.")

        # Dedicated sections for higher-timeframe market structure / supply-demand signals
        any_ms = False
        if m15_ms_signals:
            any_ms = True
            st.subheader("📐 M15 Market Structure / Supply-Demand Signals")
            ms_df = sort_dataframe_by_timestamp(pd.DataFrame(m15_ms_signals))
            if 'timestamp' in ms_df.columns:
                ms_df['timestamp'] = ms_df['timestamp'].apply(format_timestamp)
            st.dataframe(ms_df, use_container_width=True, hide_index=True)

        if not any_ms and show_supply_demand and supply_demand_zones:
            st.info("No M15 market-structure signals for the current data.")

        # Backtest op echte candledata
        if enable_test_zone and all_signals:
            st.subheader("🧪 Backtest Signals on Live Candle History")

            signal_df_full = pd.DataFrame(all_signals).copy()
            if 'timestamp' in signal_df_full.columns:
                signal_df_full['timestamp'] = pd.to_datetime(signal_df_full['timestamp'])
                signal_df_full = signal_df_full.sort_values('timestamp').reset_index(drop=True)

            # Dynamic timeframe mapping
            timeframe_to_df = {primary_label: df_primary}
            for tf_label, tf_df in higher_tfs:
                timeframe_to_df[tf_label] = tf_df

            results = []
            equity = starting_balance

            for _, sig in signal_df_full.iterrows():
                direction = sig.get('signal')
                entry_price = sig.get('price')
                ts = sig.get('timestamp')
                sl = sig.get('stop_loss')
                tp = sig.get('take_profit')

                result = 'Open'
                exit_price = np.nan
                exit_time = pd.NaT
                pips = 0.0
                pnl = 0.0

                if pd.notna(entry_price) and pd.notna(ts) and pd.notna(sl) and pd.notna(tp):
                    timeframe_label = sig.get('timeframe', primary_label)
                    price_df = timeframe_to_df.get(timeframe_label, df_primary).sort_values('Datetime')

                    after_mask = price_df['Datetime'] >= ts
                    if after_mask.any():
                        idx_start = price_df.index[after_mask][0]

                        for j in range(idx_start + 1, len(price_df)):
                            bar = price_df.iloc[j]
                            bar_low = bar['Low']
                            bar_high = bar['High']
                            bar_time = bar['Datetime']

                            if direction == 'Buy':
                                sl_hit = bar_low <= sl
                                tp_hit = bar_high >= tp
                                if sl_hit:
                                    result = 'Loss'
                                    exit_price = sl
                                    exit_time = bar_time
                                    break
                                if tp_hit:
                                    result = 'Win'
                                    exit_price = tp
                                    exit_time = bar_time
                                    break
                            elif direction == 'Sell':
                                sl_hit = bar_high >= sl
                                tp_hit = bar_low <= tp
                                if sl_hit:
                                    result = 'Loss'
                                    exit_price = sl
                                    exit_time = bar_time
                                    break
                                if tp_hit:
                                    result = 'Win'
                                    exit_price = tp
                                    exit_time = bar_time
                                    break

                        if result in ('Win', 'Loss'):
                            move = (exit_price - entry_price) if direction == 'Buy' else (entry_price - exit_price)
                            pips = move / pip_size
                            pnl = pips * pip_value_money
                            equity += pnl

                results.append({
                    'timestamp': ts,
                    'signal': direction,
                    'setup': sig.get('setup', ''),
                    'setup_label': get_signal_setup_label(sig),
                    'type': sig.get('type'),
                    'timeframe': sig.get('timeframe', '1m'),
                    'price': entry_price,
                    'stop_loss': sl,
                    'take_profit': tp,
                    'exit_time': exit_time,
                    'exit_price': exit_price,
                    'result': result,
                    'pips': pips,
                    'pnl': pnl,
                    'equity_after': equity,
                })

            results_df = pd.DataFrame(results)
            results_df = results_df[results_df['timeframe'] != '30m'].reset_index(drop=True)
            results_df = sort_dataframe_by_timestamp(results_df)

            if not results_df.empty:
                wins = (results_df['result'] == 'Win').sum()
                losses = (results_df['result'] == 'Loss').sum()
                opens = (results_df['result'] == 'Open').sum()
                total_pips = results_df['pips'].sum()

                st.markdown(
                    f"**Wins:** {wins} | **Losses:** {losses} | **Open:** {opens} | **Total Pips:** {total_pips:.1f}"
                )
                st.markdown(
                    f"**Starting Balance:** {starting_balance:.2f} → **Final Balance:** {equity:.2f}"
                )

                closed_results_df = results_df[results_df['result'].isin(['Win', 'Loss'])].copy()
                if not closed_results_df.empty and 'setup_label' in closed_results_df.columns:
                    setup_stats_df = (
                        closed_results_df
                        .groupby('setup_label', dropna=False)
                        .agg(
                            trades=('result', 'count'),
                            wins=('result', lambda values: int((values == 'Win').sum())),
                            losses=('result', lambda values: int((values == 'Loss').sum())),
                            total_pips=('pips', 'sum'),
                        )
                        .reset_index()
                    )
                    setup_stats_df['win_rate'] = ((setup_stats_df['wins'] / setup_stats_df['trades']) * 100).round(1)
                    setup_stats_df['loss_rate'] = ((setup_stats_df['losses'] / setup_stats_df['trades']) * 100).round(1)
                    setup_stats_df = setup_stats_df.sort_values(
                        by=['win_rate', 'trades', 'setup_label'],
                        ascending=[False, False, True],
                    ).reset_index(drop=True)

                    st.markdown("### 📊 Win/Loss per setup")
                    st.dataframe(
                        setup_stats_df.rename(columns={'setup_label': 'setup'}),
                        use_container_width=True,
                        hide_index=True,
                    )

                def highlight_result(row):
                    if row['result'] == 'Win':
                        color = 'background-color: rgba(0, 150, 0, 0.6); color: white;'
                    elif row['result'] == 'Loss':
                        color = 'background-color: rgba(200, 0, 0, 0.7); color: white;'
                    else:
                        color = ''
                    return [color] * len(row)

                visible_result_timeframes = ['15m']
                results_df = results_df[results_df['timeframe'].isin(visible_result_timeframes)].reset_index(drop=True)
                unique_tfs = [
                    timeframe
                    for timeframe in visible_result_timeframes
                    if timeframe in set(results_df['timeframe'].dropna().unique())
                ]

                if unique_tfs:
                    tabs = st.tabs([f"{tf} Results" for tf in unique_tfs])

                    for tf, tab in zip(unique_tfs, tabs):
                        with tab:
                            tf_df = results_df[results_df['timeframe'] == tf]

                            if tf_df.empty:
                                st.info(f"No trades for {tf} timeframe.")
                            else:
                                wins_tf = (tf_df['result'] == 'Win').sum()
                                losses_tf = (tf_df['result'] == 'Loss').sum()
                                opens_tf = (tf_df['result'] == 'Open').sum()
                                total_pips_tf = tf_df['pips'].sum()

                                st.markdown(
                                    f"**{tf} Wins:** {wins_tf} | **Losses:** {losses_tf} | **Open:** {opens_tf} | **Total Pips:** {total_pips_tf:.1f}"
                                )

                                tf_display_results_df = tf_df.copy()
                                if 'timestamp' in tf_display_results_df.columns:
                                    tf_display_results_df['timestamp'] = tf_display_results_df['timestamp'].apply(format_timestamp)
                                if 'exit_time' in tf_display_results_df.columns:
                                    tf_display_results_df['exit_time'] = tf_display_results_df['exit_time'].apply(format_timestamp)

                                st.dataframe(
                                    tf_display_results_df.style.apply(highlight_result, axis=1),
                                    use_container_width=True,
                                )

        # Main chart
        st.subheader(f"📈 {instrument_label} - {primary_label.upper()} Chart with Multi-Timeframe Analysis")
        
        fig = make_subplots(
            rows=1, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            subplot_titles=('Price Action with Supply/Demand Zones',)
        )
        
        # Row 1: Candlesticks
        fig.add_trace(go.Candlestick(
            x=df_primary['Datetime'],
            open=df_primary['Open'],
            high=df_primary['High'],
            low=df_primary['Low'],
            close=df_primary['Close'],
            name="Price"
        ), row=1, col=1)
        
        # Add Supply/Demand Zones (15m zones projected across the full chart)
        if show_supply_demand and supply_demand_zones:
            for zone in supply_demand_zones:
                zone_start = df_primary['Datetime'].iloc[0]
                zone_end = df_primary['Datetime'].iloc[-1]

                color = 'rgba(255, 0, 0, 0.2)' if zone['type'] == 'Supply' else 'rgba(0, 255, 0, 0.2)'

                fig.add_shape(
                    type="rect",
                    x0=zone_start,
                    x1=zone_end,
                    y0=zone['bottom'],
                    y1=zone['top'],
                    fillcolor=color,
                    line=dict(color=color.replace('0.2', '0.5'), width=1),
                    layer='below',
                    row=1, col=1
                )
        
        # Add moving averages
        fig.add_trace(go.Scatter(
            x=df_primary['Datetime'],
            y=df_primary['SMA_50'],
            mode='lines',
            name='SMA-50',
            line=dict(color='blue', width=1)
        ), row=1, col=1)
        
        fig.add_trace(go.Scatter(
            x=df_primary['Datetime'],
            y=df_primary['Session_Average'],
            mode='lines',
            name='Session Average',
            line=dict(color='purple', width=1, dash='dash')
        ), row=1, col=1)
        
        # Add optional trade overlays to price chart
        if all_signals and (show_signal_markers or show_trade_levels):
            signal_df = pd.DataFrame(all_signals)
            buy_sigs = signal_df[signal_df['signal'] == 'Buy']
            sell_sigs = signal_df[signal_df['signal'] == 'Sell']
            
            if show_signal_markers and not buy_sigs.empty:
                fig.add_trace(go.Scatter(
                    x=buy_sigs['timestamp'],
                    y=buy_sigs['price'],
                    mode='markers',
                    name='Buy Signals',
                    marker=dict(color='lime', size=12, symbol='triangle-up'),
                    text=buy_sigs['type'],
                    hovertemplate='<b>%{text}</b><br>Price: %{y}<extra></extra>'
                ), row=1, col=1)
            
            if show_signal_markers and not sell_sigs.empty:
                fig.add_trace(go.Scatter(
                    x=sell_sigs['timestamp'],
                    y=sell_sigs['price'],
                    mode='markers',
                    name='Sell Signals',
                    marker=dict(color='red', size=12, symbol='triangle-down'),
                    text=sell_sigs['type'],
                    hovertemplate='<b>%{text}</b><br>Price: %{y}<extra></extra>'
                ), row=1, col=1)

            if show_trade_levels:
                for _, row_sig in signal_df.iterrows():
                    ts = row_sig['timestamp']
                    entry = row_sig['price']
                    tp = row_sig.get('take_profit')
                    sl = row_sig.get('stop_loss')
                    color = 'lime' if row_sig['signal'] == 'Buy' else 'red'

                    if tp is not None:
                        fig.add_trace(go.Scatter(
                            x=[ts, ts],
                            y=[entry, tp],
                            mode='lines',
                            line=dict(color=color, width=2, dash='dash'),
                            showlegend=False,
                            hoverinfo='skip'
                        ), row=1, col=1)

                    if sl is not None:
                        fig.add_trace(go.Scatter(
                            x=[ts, ts],
                            y=[entry, sl],
                            mode='lines',
                            line=dict(color='gray', width=2, dash='dot'),
                            showlegend=False,
                            hoverinfo='skip'
                        ), row=1, col=1)
        
        fig.update_layout(
            height=650,
            template="plotly_dark",
            showlegend=True,
            xaxis_rangeslider_visible=False
        )
        
        fig.update_xaxes(title_text="Time", row=1, col=1)
        fig.update_yaxes(title_text="Price", row=1, col=1, tickformat=".5f")
        
        st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()