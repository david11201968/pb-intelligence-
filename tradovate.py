"""
TRADOVATE INTEGRATION
======================
Real-time NQ/ES futures data via Tradovate API.
WebSocket streaming OHLCV + quotes.
Free demo account at tradovate.com
"""

import requests
import websocket
import json
import threading
import time
import pandas as pd
from datetime import datetime, timedelta
import pytz
import uuid

DEMO_URL  = "https://demo.tradovateapi.com/v1"
LIVE_URL  = "https://live.tradovateapi.com/v1"
MD_WS_URL = "wss://md.tradovateapi.com/v1/websocket"

# Tradovate demo client credentials (public developer keys)
DEMO_CID  = 8
DEMO_SEC  = "8a956cfa-5e5c-4c18-b8a4-ed7d8ab0b1eb"
APP_ID    = "PB Intelligence"
APP_VER   = "2.0"


class TradovateClient:
    """
    Full Tradovate API client.
    Handles auth, market data WebSocket, and historical bars.
    """

    def __init__(self, username: str, password: str, live: bool = False):
        self.username   = username
        self.password   = password
        self.base_url   = LIVE_URL if live else DEMO_URL
        self.live       = live
        self.token      = None
        self.token_exp  = None
        self.device_id  = str(uuid.uuid4())

        # Live data storage (updated by WebSocket)
        self.quotes: dict  = {}        # symbol -> latest quote
        self.bars: dict    = {}        # symbol -> pd.DataFrame of OHLCV
        self._ws           = None
        self._ws_thread    = None
        self._subscriptions: set = set()
        self._msg_id       = 0
        self._connected    = False

        self.tz = pytz.timezone("America/New_York")

    # ──────────────────────────────────────────
    # AUTH
    # ──────────────────────────────────────────

    def authenticate(self) -> bool:
        """Log in and get access token. Returns True on success."""
        payload = {
            "name":       self.username,
            "password":   self.password,
            "appId":      APP_ID,
            "appVersion": APP_VER,
            "cid":        DEMO_CID,
            "sec":        DEMO_SEC,
            "deviceId":   self.device_id,
        }
        try:
            r = requests.post(f"{self.base_url}/auth/accesstokenrequest",
                              json=payload, timeout=10)
            if r.status_code == 200:
                data = r.json()
                self.token     = data.get("accessToken")
                exp_sec        = data.get("expirationTime", 3600)
                self.token_exp = datetime.now() + timedelta(seconds=exp_sec)
                print(f"[Tradovate] Authenticated as {self.username}")
                return bool(self.token)
            else:
                print(f"[Tradovate] Auth failed: {r.status_code} {r.text}")
                return False
        except Exception as e:
            print(f"[Tradovate] Auth error: {e}")
            return False

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json"}

    def _refresh_if_needed(self):
        if self.token_exp and datetime.now() > self.token_exp - timedelta(minutes=5):
            self.authenticate()

    # ──────────────────────────────────────────
    # CONTRACT LOOKUP
    # ──────────────────────────────────────────

    def get_front_contract(self, root: str) -> dict | None:
        """Return the front-month contract for a root symbol (NQ, ES, etc.)."""
        self._refresh_if_needed()
        try:
            r = requests.get(f"{self.base_url}/contract/find",
                             params={"name": root},
                             headers=self._headers(), timeout=10)
            if r.status_code == 200:
                contracts = r.json()
                if isinstance(contracts, list) and contracts:
                    # Sort by expiry, take nearest
                    contracts.sort(key=lambda x: x.get("expirationDate", "9999"))
                    future = [c for c in contracts if c.get("expirationDate","") >= datetime.now().strftime("%Y-%m-%d")]
                    return future[0] if future else contracts[0]
                elif isinstance(contracts, dict):
                    return contracts
        except Exception as e:
            print(f"[Tradovate] Contract lookup error: {e}")
        return None

    def find_contract_id(self, root: str) -> int | None:
        c = self.get_front_contract(root)
        return c.get("id") if c else None

    # ──────────────────────────────────────────
    # HISTORICAL BARS (REST)
    # ──────────────────────────────────────────

    def get_bars(self, root: str, interval_type: str = "Minute",
                 interval: int = 5, count: int = 500) -> pd.DataFrame:
        """Fetch historical OHLCV bars. Returns DataFrame."""
        self._refresh_if_needed()
        contract_id = self.find_contract_id(root)
        if not contract_id:
            return pd.DataFrame()

        payload = {
            "symbol":         root,
            "contractId":     contract_id,
            "elementSize":    interval,
            "elementSizeUnit": interval_type,
            "withHistogram":  False,
        }
        try:
            r = requests.post(f"{self.base_url}/chart/getpricehistory",
                              json=payload, headers=self._headers(), timeout=15)
            if r.status_code == 200:
                data = r.json()
                bars = data.get("bars", [])
                if not bars:
                    return pd.DataFrame()
                df = pd.DataFrame(bars)
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
                df["timestamp"] = df["timestamp"].dt.tz_convert(self.tz)
                df = df.set_index("timestamp")
                df.columns = [c.lower() for c in df.columns]
                needed = [c for c in ["open","high","low","close","upVolume","downVolume"] if c in df.columns]
                df = df[needed].copy()
                if "upvolume" in df.columns and "downvolume" in df.columns:
                    df["volume"] = df["upvolume"] + df["downvolume"]
                elif "volume" not in df.columns:
                    df["volume"] = 0
                df = df[["open","high","low","close","volume"]].dropna()
                return df.tail(count)
        except Exception as e:
            print(f"[Tradovate] Bars error: {e}")
        return pd.DataFrame()

    # ──────────────────────────────────────────
    # WEBSOCKET (REAL-TIME)
    # ──────────────────────────────────────────

    def _next_id(self) -> int:
        self._msg_id += 1
        return self._msg_id

    def _ws_on_open(self, ws):
        print("[Tradovate WS] Connected")
        # Authenticate the WebSocket
        auth_msg = f"authorize\n{self._next_id()}\n\n{self.token}"
        ws.send(auth_msg)

    def _ws_on_message(self, ws, raw):
        """Handle incoming WebSocket messages."""
        try:
            if raw == "o":        # SockJS open frame
                return
            if raw.startswith("h"):  # heartbeat
                return
            if raw.startswith("a"):  # array of messages
                msgs = json.loads(raw[1:])
                for msg in msgs:
                    self._handle_ws_msg(msg)
        except Exception as e:
            pass  # Malformed frame — ignore

    def _handle_ws_msg(self, msg: dict):
        e = msg.get("e", "")
        d = msg.get("d", {})

        if e == "authorized":
            self._connected = True
            print("[Tradovate WS] Authorized — subscribing to market data")
            for sym in list(self._subscriptions):
                self._subscribe_quote(sym)

        elif e == "quote":
            sym = d.get("symbol", d.get("contractId", "?"))
            self.quotes[sym] = {
                "bid":       d.get("bidPrice"),
                "ask":       d.get("askPrice"),
                "last":      d.get("lastPrice"),
                "volume":    d.get("totalVolume"),
                "ts":        datetime.now(self.tz),
            }

        elif e == "chart":
            bars = d.get("bars", [])
            sym  = d.get("symbol", "NQ")
            if bars and sym in self.bars:
                for bar in bars:
                    ts = pd.Timestamp(bar["timestamp"], unit="ms", tz="UTC").tz_convert(self.tz)
                    self.bars[sym].loc[ts] = [
                        bar.get("open"), bar.get("high"),
                        bar.get("low"),  bar.get("close"),
                        bar.get("upVolume", 0) + bar.get("downVolume", 0),
                    ]

    def _subscribe_quote(self, symbol: str):
        """Subscribe to real-time quotes for a symbol."""
        if self._ws and self._connected:
            msg = f"md/subscribeQuote\n{self._next_id()}\n\n{json.dumps({'symbol': symbol})}"
            self._ws.send(msg)

    def _subscribe_chart(self, symbol: str, interval: int = 5):
        """Subscribe to real-time 5-min chart updates."""
        if self._ws and self._connected:
            msg = f"md/subscribeHistogram\n{self._next_id()}\n\n{json.dumps({'symbol': symbol})}"
            self._ws.send(msg)

    def _ws_on_error(self, ws, err):
        print(f"[Tradovate WS] Error: {err}")

    def _ws_on_close(self, ws, code, msg):
        self._connected = False
        print(f"[Tradovate WS] Closed: {code}")
        # Auto-reconnect after 5 seconds
        time.sleep(5)
        if self.token:
            self.start_streaming(list(self._subscriptions))

    def start_streaming(self, symbols: list[str]):
        """Start WebSocket streaming for given symbols."""
        self._subscriptions.update(symbols)

        def _run():
            self._ws = websocket.WebSocketApp(
                MD_WS_URL,
                on_open    = self._ws_on_open,
                on_message = self._ws_on_message,
                on_error   = self._ws_on_error,
                on_close   = self._ws_on_close,
            )
            self._ws.run_forever(ping_interval=20, ping_timeout=10)

        self._ws_thread = threading.Thread(target=_run, daemon=True)
        self._ws_thread.start()

    def get_live_quote(self, symbol: str) -> dict:
        """Get latest quote for symbol. Returns {} if not available."""
        return self.quotes.get(symbol, {})

    def get_live_price(self, symbol: str) -> float | None:
        q = self.get_live_quote(symbol)
        last = q.get("last")
        if last: return last
        # Fallback: mid of bid/ask
        bid, ask = q.get("bid"), q.get("ask")
        if bid and ask: return (bid + ask) / 2
        return None

    def stop(self):
        if self._ws:
            self._ws.close()


# ──────────────────────────────────────────────────────────────
# CONVENIENCE WRAPPER (used by app.py)
# ──────────────────────────────────────────────────────────────

_client: TradovateClient | None = None

def init_tradovate(username: str, password: str, live: bool = False) -> bool:
    """Initialize and authenticate Tradovate client. Returns True on success."""
    global _client
    _client = TradovateClient(username, password, live)
    ok = _client.authenticate()
    if ok:
        _client.start_streaming(["NQ", "ES"])
        # Pre-load historical bars
        for sym in ["NQ", "ES"]:
            bars = _client.get_bars(sym, "Minute", 5, 500)
            if not bars.empty:
                _client.bars[sym] = bars
                print(f"[Tradovate] Loaded {len(bars)} bars for {sym}")
    return ok

def get_nq_bars(n: int = 200) -> pd.DataFrame:
    if _client and "NQ" in _client.bars:
        return _client.bars["NQ"].tail(n)
    return pd.DataFrame()

def get_es_bars(n: int = 200) -> pd.DataFrame:
    if _client and "ES" in _client.bars:
        return _client.bars["ES"].tail(n)
    return pd.DataFrame()

def get_nq_price() -> float | None:
    if _client:
        return _client.get_live_price("NQ")
    return None

def get_es_price() -> float | None:
    if _client:
        return _client.get_live_price("ES")
    return None

def is_connected() -> bool:
    return _client is not None and _client._connected
