# Setup

Create a virtual environment and install dependencies (macOS):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run the Streamlit app locally:

```bash
streamlit run signals.py
```

Or with the project virtual environment:

```bash
./venv/bin/python -m streamlit run signals.py
```

Or use the launcher script:

```bash
./run_dashboard.sh
```

If you use VS Code, set the interpreter to the created environment to resolve imports.

# Secrets

Do not hardcode API keys in the repository.

- Local development: copy `.streamlit/secrets.toml.example` to `.streamlit/secrets.toml`
- Streamlit Community Cloud: paste the same keys into the app's **Secrets** settings

Required secret:

- `TWELVEDATA_API_KEY`

Optional secrets:

- `TELEGRAM_TOKEN`
- `TELEGRAM_CHAT_ID`
- `NEWSDATA_API_KEY`
- `OANDA_ACCOUNT_ID`
- `OANDA_ACCESS_TOKEN`
- `OANDA_ENVIRONMENT` (`practice` for demo, `live` for real money)
- `MT5_LOGIN`
- `MT5_PASSWORD`
- `MT5_SERVER`
- `MT5_PATH`

# Auto trading

The dashboard can auto-send market orders for new signals.

- On macOS, `OANDA` is the most practical route for demo/live auto trading.
- `MT5` support remains available, but native `MetaTrader5` Python integration usually works best on Windows.
- Always test with `OANDA_ENVIRONMENT=practice` or a broker demo account first.
- Configure broker credentials in environment variables, Streamlit secrets, or the sidebar.

# Deploy to Streamlit Community Cloud

1. Push this project to a GitHub repository
2. Go to Streamlit Community Cloud
3. Create a new app from your GitHub repo
4. Set the main file path to `signals.py`
5. Add your secrets in the Streamlit Cloud **Secrets** section
6. Deploy the app

After deployment, the app can be opened from your mobile phone using the public Streamlit URL.