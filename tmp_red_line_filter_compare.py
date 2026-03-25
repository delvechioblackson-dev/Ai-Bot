import json
import pandas as pd
import numpy as np
import signals

import os
INSTRUMENT = os.environ.get('INSTRUMENT', 'BTC/USD')
STARTING_BALANCE = 1000.0
PIP_VALUE = 1.0
PIP_SIZE = 1.0
TP_PIPS = 50
SL_PIPS = 25


def build_data_source():
    td_key = signals.get_config_value('TWELVEDATA_API_KEY', signals.TWELVEDATA_API_KEY_DEFAULT)
    if td_key:
        return 'TWELVEDATA', td_key
    return None, None


def fetch_data():

    source, td_key = build_data_source()
    df = None
    meta = {}
    if source:
        df = signals.generate_historical_data(
            1500,
            '1min',
            base_currency='BTC' if INSTRUMENT == 'BTC/USD' else None,
            target_currency='USD' if INSTRUMENT == 'BTC/USD' else None,
            fx_api_key=td_key,
            market_data_source=source,
            ctrader_config=None,
            symbol_label=INSTRUMENT,
        )
        if not df.empty:
            df = signals.add_technical_indicators(df)
            return df, {'source': source, 'bars': int(len(df)), 'instrument': INSTRUMENT}
        meta = getattr(signals.st, 'session_state', {}).get('latest_market_data_meta', {})

    # Fallback naar Yahoo Finance
    try:
        import yfinance as yf
        yf_symbol = None
        if INSTRUMENT == 'US30':
            yf_symbol = '^DJI'
        elif INSTRUMENT == 'US500':
            yf_symbol = '^GSPC'
        elif INSTRUMENT == 'BTC/USD':
            yf_symbol = 'BTC-USD'
        if yf_symbol:
            yf_period = '7d' if INSTRUMENT in ['US30', 'US500'] else '60d'
            df_yf = yf.download(yf_symbol, period=yf_period, interval='1m', progress=False)
            if not df_yf.empty:
                df_yf = df_yf.reset_index()
                # MultiIndex flattenen indien nodig
                if isinstance(df_yf.columns, pd.MultiIndex):
                    df_yf.columns = ['_'.join([str(l) for l in col if l]) for col in df_yf.columns.values]
                # Kolomnamen normaliseren
                col_map = {}
                if 'Datetime' not in df_yf.columns:
                    if 'index' in df_yf.columns:
                        col_map['index'] = 'Datetime'
                    elif 'Date' in df_yf.columns:
                        col_map['Date'] = 'Datetime'
                    elif 'Datetime' not in df_yf.columns and 'Timestamp' in df_yf.columns:
                        col_map['Timestamp'] = 'Datetime'

                # Speciaal voor indices: kolommen met suffix (zoals 'Close_^DJI')
                suffix = ''
                if INSTRUMENT == 'US30':
                    suffix = '_^DJI'
                elif INSTRUMENT == 'US500':
                    suffix = '_^GSPC'

                for c in ['Open','High','Low','Close','Volume']:
                    if c not in df_yf.columns:
                        # Yahoo gebruikt hoofdletters
                        if c.capitalize() in df_yf.columns:
                            col_map[c.capitalize()] = c
                        elif c.upper() in df_yf.columns:
                            col_map[c.upper()] = c
                        # MultiIndex flatten fallback
                        elif f'{c}_' in df_yf.columns:
                            col_map[f'{c}_'] = c
                        # Suffix mapping voor indices
                        elif f'{c}{suffix}' in df_yf.columns:
                            col_map[f'{c}{suffix}'] = c

                df_yf = df_yf.rename(columns=col_map)
                # Alleen relevante kolommen selecteren
                keep_cols = ['Datetime','Open','High','Low','Close','Volume']
                missing_cols = [col for col in keep_cols if col not in df_yf.columns]
                if missing_cols:
                    print(f"[DEBUG] Ontbrekende kolommen in Yahoo DataFrame: {missing_cols}")
                    print(f"[DEBUG] Alle kolommen: {list(df_yf.columns)}")
                df_yf = df_yf[[col for col in keep_cols if col in df_yf.columns]]
                if 'Datetime' in df_yf.columns:
                    df_yf['Datetime'] = pd.to_datetime(df_yf['Datetime'])
                df_yf = signals.add_technical_indicators(df_yf)
                return df_yf, {'source': 'YAHOO', 'bars': int(len(df_yf)), 'instrument': INSTRUMENT}
            else:
                return None, {'error': f'Geen data van Yahoo Finance voor {INSTRUMENT}'}
        else:
            return None, {'error': f'Geen Yahoo Finance symbool voor {INSTRUMENT}'}
    except Exception as e:
        return None, {'error': f'Yahoo Finance fout: {e}'}
    return None, {'error': f'Geen data: {meta.get("error")}'}


def passes_filter(df, index, mode):
    if mode == 'baseline':
        return True

    row = df.iloc[index]
    prev = df.iloc[index - 1]
    prev2 = df.iloc[index - 2] if index >= 2 else prev

    current_red = row.get('Red_Line_99', np.nan)
    prev_red = prev.get('Red_Line_99', np.nan)
    prev2_red = prev2.get('Red_Line_99', np.nan)
    if pd.isna(current_red) or pd.isna(prev_red):
        return False

    atr = row.get('ATR_14', np.nan)
    body_ratio = signals.safe_divide(row.get('Body_Size', 0.0), row.get('Candle_Range', 0.0), default=0.0)
    close_distance = abs(float(row['Close']) - float(current_red))
    volume_ratio = row.get('Volume_Ratio', np.nan)
    bullish = row['Close'] > row['Open']
    bearish = row['Close'] < row['Open']

    if mode == 'light':
        slope_ok = (bullish and current_red > prev_red) or (bearish and current_red < prev_red)
        body_ok = body_ratio >= 0.45
        distance_ok = pd.isna(atr) or close_distance >= (0.05 * float(atr))
        volume_ok = pd.isna(volume_ratio) or float(volume_ratio) >= 0.9
        return slope_ok and body_ok and distance_ok and volume_ok

    if mode == 'strict':
        slope_ok = (bullish and current_red > prev_red > prev2_red) or (bearish and current_red < prev_red < prev2_red)
        body_ok = body_ratio >= 0.6
        distance_ok = pd.notna(atr) and close_distance >= (0.15 * float(atr))
        volume_ok = pd.notna(volume_ratio) and float(volume_ratio) >= 1.1
        wick_ok = (bullish and row.get('Upper_Wick', 0.0) <= row.get('Body_Size', 0.0)) or (bearish and row.get('Lower_Wick', 0.0) <= row.get('Body_Size', 0.0))
        return slope_ok and body_ok and distance_ok and volume_ok and wick_ok

    return False


def generate_signals(df, mode):
    signal_rows = []
    take_profit_distance = TP_PIPS * PIP_SIZE
    stop_loss_distance = SL_PIPS * PIP_SIZE

    for index in range(1, len(df)):
        row = df.iloc[index]
        previous_row = df.iloc[index - 1]
        timestamp = signals.normalize_app_timestamp(row['Datetime'])
        if signals.is_within_no_trade_window(timestamp):
            continue

        current_red = row.get('Red_Line_99', np.nan)
        previous_red = previous_row.get('Red_Line_99', np.nan)
        if pd.isna(current_red) or pd.isna(previous_red):
            continue

        buy_cross = previous_row['Close'] <= previous_red and row['Close'] > current_red and row['Close'] > row['Open']
        sell_cross = previous_row['Close'] >= previous_red and row['Close'] < current_red and row['Close'] < row['Open']

        if not passes_filter(df, index, mode):
            continue

        if buy_cross:
            raw_entry = float(row['Close'])
            entry = signals.apply_entry_spread(raw_entry, 'Buy', PIP_SIZE)
            signal_rows.append({
                'timestamp': timestamp,
                'signal': 'Buy',
                'price': entry,
                'stop_loss': entry - stop_loss_distance,
                'take_profit': entry + take_profit_distance,
                'timeframe': '1m',
            })
        elif sell_cross:
            raw_entry = float(row['Close'])
            entry = signals.apply_entry_spread(raw_entry, 'Sell', PIP_SIZE)
            signal_rows.append({
                'timestamp': timestamp,
                'signal': 'Sell',
                'price': entry,
                'stop_loss': entry + stop_loss_distance,
                'take_profit': entry - take_profit_distance,
                'timeframe': '1m',
            })
    return signals.sort_records_by_timestamp(signal_rows) if signal_rows else []


def run_backtest(df, signals_list):
    equity = STARTING_BALANCE
    price_df = df.copy()
    price_df['Datetime'] = price_df['Datetime'].apply(signals.normalize_app_timestamp)
    price_df = price_df.sort_values('Datetime').reset_index(drop=True)
    results = []

    for sig in signals_list:
        direction = sig['signal']
        ts = signals.normalize_app_timestamp(sig['timestamp'])
        entry_price = sig['price']
        sl = sig['stop_loss']
        tp = sig['take_profit']
        result = 'Open'
        exit_price = np.nan

        after_mask = price_df['Datetime'] >= ts
        if after_mask.any():
            idx_start = price_df.index[after_mask][0]
            for j in range(idx_start + 1, len(price_df)):
                bar = price_df.iloc[j]
                if direction == 'Buy':
                    if bar['Low'] <= sl:
                        result = 'Loss'
                        exit_price = sl
                        break
                    if bar['High'] >= tp:
                        result = 'Win'
                        exit_price = tp
                        break
                else:
                    if bar['High'] >= sl:
                        result = 'Loss'
                        exit_price = sl
                        break
                    if bar['Low'] <= tp:
                        result = 'Win'
                        exit_price = tp
                        break

        pips = 0.0
        if result in ('Win', 'Loss'):
            move = (exit_price - entry_price) if direction == 'Buy' else (entry_price - exit_price)
            pips = move / PIP_SIZE
            equity += pips * PIP_VALUE
        results.append({'result': result, 'pips': pips})

    results_df = pd.DataFrame(results)
    closed = results_df[results_df['result'].isin(['Win', 'Loss'])].copy() if not results_df.empty else pd.DataFrame()
    wins = int((closed['result'] == 'Win').sum()) if not closed.empty else 0
    losses = int((closed['result'] == 'Loss').sum()) if not closed.empty else 0
    trades = int(len(closed))
    return {
        'signals': len(signals_list),
        'closed_trades': trades,
        'wins': wins,
        'losses': losses,
        'win_rate': round((wins / trades) * 100, 1) if trades else 0.0,
        'pips_total': round(float(closed['pips'].sum()), 1) if not closed.empty else 0.0,
        'final_balance': round(float(equity), 1),
    }


def main():
    df, meta = fetch_data()
    if df is None:
        print(json.dumps(meta, indent=2, ensure_ascii=False))
        return

    output = {'meta': {**meta, 'tp': TP_PIPS, 'sl': SL_PIPS}}
    for mode in ['baseline', 'light', 'strict']:
        sigs = generate_signals(df, mode)
        output[mode] = run_backtest(df, sigs)

    print(json.dumps(output, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    main()
