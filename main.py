import os
import re
import sqlite3
import threading
import time
import warnings
from urllib.parse import parse_qs, unquote, urlparse, quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import base64
import datetime
import hashlib
import json
import math
import random
import re

import requests
try:
    import feedparser  # type: ignore
except Exception:
    feedparser = None
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles


def _load_dotenv_if_present() -> None:
    try:
        base_dir = os.path.dirname(__file__)
        env_path = os.path.join(base_dir, ".env")
        if not os.path.exists(env_path):
            return
        raw_kv: Dict[str, str] = {}

        def _parse_line(raw: str) -> Optional[Tuple[str, str]]:
            line = (raw or "").strip()
            if not line:
                return None
            if line.startswith("#"):
                return None
            if "=" not in line:
                return None
            k, v = line.split("=", 1)
            k = (k or "").strip()
            v = (v or "").strip()
            if not k:
                return None
            if len(v) >= 2 and ((v[0] == '"' and v[-1] == '"') or (v[0] == "'" and v[-1] == "'")):
                v = v[1:-1]
            return k, v

        with open(env_path, "r", encoding="utf-8") as f:
            for raw in f:
                kv = _parse_line(raw)
                if not kv:
                    continue
                k, v = kv
                raw_kv[k] = v

        override = (raw_kv.get("DOTENV_OVERRIDE", "") or "").strip() in ("1", "true", "True", "yes", "YES")

        for k, v in raw_kv.items():
            if (not override) and k in os.environ and (os.environ.get(k) or "") != "":
                continue
            os.environ[k] = v
    except Exception:
        return


_load_dotenv_if_present()


APP_TITLE = "Gate 永续合约仪表板"

GATE_REST_FUTURES_USDT_BASE = "https://api.gateio.ws/api/v4/futures/usdt"
GATE_REST_SPOT_BASE = "https://api.gateio.ws/api/v4/spot"
BINANCE_REST_SPOT_BASE = "https://api.binance.com"

# 复用连接，减少每次请求的握手开销
HTTP = requests.Session()
HTTP_NO_PROXY = requests.Session()
HTTP_NO_PROXY.trust_env = False
_http_trust_env_raw = (os.getenv("HTTP_TRUST_ENV", "") or "").strip()
if _http_trust_env_raw:
    HTTP.trust_env = _http_trust_env_raw in ("1", "true", "True", "yes", "YES")
else:
    # 默认信任环境变量代理（PowerShell/Clash 常用 HTTP_PROXY/HTTPS_PROXY）
    # 如需关闭可设置 HTTP_TRUST_ENV=0
    HTTP.trust_env = True
    if (os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("ALL_PROXY") or os.getenv("NO_PROXY")):
        HTTP.trust_env = True

NEWS_DB_PATH = os.getenv("NEWS_DB_PATH", os.path.join(os.path.dirname(__file__), "news_sentinel.sqlite3"))
NEWS_HTTP_VERIFY = (os.getenv("NEWS_HTTP_VERIFY", "1") or "1").strip() in ("1", "true", "True", "yes", "YES")
NEWS_HTTP_USER_AGENT = (
    os.getenv(
        "NEWS_HTTP_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    )
    or ""
).strip()
COINDESK_FEED_URL = (
    os.getenv("COINDESK_FEED_URL", "https://www.coindesk.com/arc/outboundfeeds/rss/") or ""
).strip()
COINTELEGRAPH_FEED_URL = (os.getenv("COINTELEGRAPH_FEED_URL", "https://cointelegraph.com/rss") or "").strip()
THEBLOCK_FEED_URL = (os.getenv("THEBLOCK_FEED_URL", "https://www.theblock.co/rss.xml") or "").strip()

# If certificate verification is disabled (NEWS_HTTP_VERIFY=0), suppress noisy warnings.
if not NEWS_HTTP_VERIFY:
    try:
        import urllib3  # type: ignore

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except Exception:
        warnings.filterwarnings("ignore", message="Unverified HTTPS request")

TELEGRAM_API_BASE = os.getenv("TELEGRAM_API_BASE", "https://api.telegram.org").strip() or "https://api.telegram.org"
TELEGRAM_CONNECT_TIMEOUT = float(os.getenv("TELEGRAM_CONNECT_TIMEOUT", "10") or "10")
TELEGRAM_READ_TIMEOUT = float(os.getenv("TELEGRAM_READ_TIMEOUT", "20") or "20")

WHALE_ALERT_ENABLED = os.getenv("WHALE_ALERT_ENABLED", "0").strip() in ("1", "true", "True", "yes", "YES")
WHALE_ALERT_API_KEY = os.getenv("WHALE_ALERT_API_KEY", "").strip()
WHALES_ALERT_LOOP_ENABLED = os.getenv("WHALES_ALERT_LOOP_ENABLED", "0").strip() in ("1", "true", "True", "yes", "YES")
WHALES_ALERT_INTERVAL_SEC = int(float(os.getenv("WHALES_ALERT_INTERVAL_SEC", "30") or "30"))

NEWS_AUTO_PUSH_ENABLED = os.getenv("NEWS_AUTO_PUSH_ENABLED", "0").strip() in ("1", "true", "True", "yes", "YES")
NEWS_AUTO_PUSH_INTERVAL_SEC = int(float(os.getenv("NEWS_AUTO_PUSH_INTERVAL_SEC", "300") or "300"))
NEWS_AUTO_PUSH_WINDOW_SEC = int(float(os.getenv("NEWS_AUTO_PUSH_WINDOW_SEC", "300") or "300"))
NEWS_AUTO_PUSH_MAX_PER_FEED = int(float(os.getenv("NEWS_AUTO_PUSH_MAX_PER_FEED", "30") or "30"))
NEWS_AUTO_PUSH_ANALYZE_LIMIT = int(float(os.getenv("NEWS_AUTO_PUSH_ANALYZE_LIMIT", "30") or "30"))
NEWS_AUTO_PUSH_MAX_ITEMS_IN_MSG = int(float(os.getenv("NEWS_AUTO_PUSH_MAX_ITEMS_IN_MSG", "8") or "8"))

MACD_PREALERT_PUSH_ENABLED = os.getenv("MACD_PREALERT_PUSH_ENABLED", "0").strip() in ("1", "true", "True", "yes", "YES")
MACD_PREALERT_PUSH_INTERVAL_SEC = int(float(os.getenv("MACD_PREALERT_PUSH_INTERVAL_SEC", "1800") or "1800"))
MACD_PREALERT_PUSH_TOPN = int(float(os.getenv("MACD_PREALERT_PUSH_TOPN", "100") or "100"))
MACD_PREALERT_PUSH_MAX_ITEMS_IN_MSG = int(float(os.getenv("MACD_PREALERT_PUSH_MAX_ITEMS_IN_MSG", "20") or "20"))

MACD_MONITOR_PUSH_ENABLED = os.getenv("MACD_MONITOR_PUSH_ENABLED", "0").strip() in ("1", "true", "True", "yes", "YES")
MACD_MONITOR_PUSH_INTERVAL_SEC = int(float(os.getenv("MACD_MONITOR_PUSH_INTERVAL_SEC", "1800") or "1800"))
MACD_MONITOR_PUSH_TOPN = int(float(os.getenv("MACD_MONITOR_PUSH_TOPN", "100") or "100"))
MACD_MONITOR_PUSH_MAX_ITEMS_IN_MSG = int(float(os.getenv("MACD_MONITOR_PUSH_MAX_ITEMS_IN_MSG", "100") or "100"))

SIGNAL_DASHBOARD_ENABLED = os.getenv("SIGNAL_DASHBOARD_ENABLED", "1").strip() in ("1", "true", "True", "yes", "YES")
SIGNAL_DASHBOARD_TOPN = int(float(os.getenv("SIGNAL_DASHBOARD_TOPN", "100") or "100"))
SIGNAL_DASHBOARD_WATCHLIST = (os.getenv("SIGNAL_DASHBOARD_WATCHLIST", "") or "").strip()
SIGNAL_DASHBOARD_CACHE_TTL_SEC = int(float(os.getenv("SIGNAL_DASHBOARD_CACHE_TTL_SEC", "60") or "60"))

SIGNAL_PUSH_ENABLED = os.getenv("SIGNAL_PUSH_ENABLED", "0").strip() in ("1", "true", "True", "yes", "YES")
SIGNAL_PUSH_INTERVAL_SEC = int(float(os.getenv("SIGNAL_PUSH_INTERVAL_SEC", "300") or "300"))
SIGNAL_PUSH_SCORE_STRONG = float(os.getenv("SIGNAL_PUSH_SCORE_STRONG", "6") or "6")
SIGNAL_PUSH_COOLDOWN_SEC = int(float(os.getenv("SIGNAL_PUSH_COOLDOWN_SEC", "600") or "600"))
SIGNAL_PUSH_REPEAT_SAME_DIRECTION = os.getenv("SIGNAL_PUSH_REPEAT_SAME_DIRECTION", "0").strip() in ("1", "true", "True", "yes", "YES")
SIGNAL_PUSH_K_TF = (os.getenv("SIGNAL_PUSH_K_TF", "1h") or "1h").strip() or "1h"

TRI_SIGNAL_ENABLED = os.getenv("TRI_SIGNAL_ENABLED", "1").strip() in ("1", "true", "True", "yes", "YES")
TRI_SIGNAL_CONTRACTS = (
    os.getenv(
        "TRI_SIGNAL_CONTRACTS",
        "BTC_USDT,XAUT_USDT,XAGU_USDT,QQQX_USDT,SPYX_USDT,XBR_USDT,ETH_USDT,SOL_USDT,TSLAX_USDT,CRCLX_USDT,AAPLX_USDT,NVDAX_USDT,MSTRX_USDT,INTC_USDT,GOOGLX_USDT,TSM_USDT,ORCL_USDT,MSFT_USDT,XTI_USDT,NGU_USDT",
    )
    or ""
).strip()
TRI_SIGNAL_CACHE_TTL_SEC = int(float(os.getenv("TRI_SIGNAL_CACHE_TTL_SEC", "60") or "60"))
TRI_SIGNAL_MAX_WORKERS = int(float(os.getenv("TRI_SIGNAL_MAX_WORKERS", "3") or "3"))

MASTER_A_ENABLED = os.getenv("MASTER_A_ENABLED", "1").strip() in ("1", "true", "True", "yes", "YES")
MASTER_A_CONTRACTS = (os.getenv("MASTER_A_CONTRACTS", TRI_SIGNAL_CONTRACTS) or "").strip()
MASTER_A_CACHE_TTL_SEC = int(float(os.getenv("MASTER_A_CACHE_TTL_SEC", "60") or "60"))
MASTER_A_MAX_WORKERS = int(float(os.getenv("MASTER_A_MAX_WORKERS", "3") or "3"))

MASTER_B_ENABLED = os.getenv("MASTER_B_ENABLED", "1").strip() in ("1", "true", "True", "yes", "YES")
MASTER_B_CONTRACTS = (os.getenv("MASTER_B_CONTRACTS", MASTER_A_CONTRACTS) or "").strip()
MASTER_B_CACHE_TTL_SEC = int(float(os.getenv("MASTER_B_CACHE_TTL_SEC", "60") or "60"))
MASTER_B_MAX_WORKERS = int(float(os.getenv("MASTER_B_MAX_WORKERS", "3") or "3"))

TRI_SIGNAL_PUSH_ENABLED = os.getenv("TRI_SIGNAL_PUSH_ENABLED", "0").strip() in ("1", "true", "True", "yes", "YES")
TRI_SIGNAL_PUSH_INTERVAL_SEC = int(float(os.getenv("TRI_SIGNAL_PUSH_INTERVAL_SEC", "300") or "300"))
TRI_SIGNAL_PUSH_COOLDOWN_SEC = int(float(os.getenv("TRI_SIGNAL_PUSH_COOLDOWN_SEC", "3600") or "3600"))
TRI_SIGNAL_PUSH_ONLY_GRADE_A = os.getenv("TRI_SIGNAL_PUSH_ONLY_GRADE_A", "1").strip() in ("1", "true", "True", "yes", "YES")

MASTER_A_PUSH_ENABLED = os.getenv("MASTER_A_PUSH_ENABLED", "0").strip() in ("1", "true", "True", "yes", "YES")
MASTER_A_PUSH_INTERVAL_SEC = int(float(os.getenv("MASTER_A_PUSH_INTERVAL_SEC", "300") or "300"))
MASTER_A_PUSH_COOLDOWN_SEC = int(float(os.getenv("MASTER_A_PUSH_COOLDOWN_SEC", "1800") or "1800"))

MASTER_B_PUSH_ENABLED = os.getenv("MASTER_B_PUSH_ENABLED", "0").strip() in ("1", "true", "True", "yes", "YES")
MASTER_B_PUSH_INTERVAL_SEC = int(float(os.getenv("MASTER_B_PUSH_INTERVAL_SEC", "300") or "300"))
MASTER_B_PUSH_COOLDOWN_SEC = int(float(os.getenv("MASTER_B_PUSH_COOLDOWN_SEC", "1800") or "1800"))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-20240620").strip() or "claude-3-5-sonnet-20240620"

# 简单内存 TTL 缓存（避免短时间内重复拉取 Top50 + 100+ 次 REST）
_CACHE: Dict[str, Tuple[float, Any]] = {}
_CACHE_LOCK = threading.Lock()


def _cache_get(key: str, ttl: int) -> Any:
    now = time.time()
    with _CACHE_LOCK:
        item = _CACHE.get(key)
    if not item:
        return None
    ts, val = item
    if now - ts > ttl:
        return None
    return val



def _cache_set(key: str, val: Any) -> None:
    with _CACHE_LOCK:
        _CACHE[key] = (time.time(), val)


def _db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(NEWS_DB_PATH, check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA synchronous=NORMAL")
    except Exception:
        pass
    return conn


def _db_init() -> None:
    conn = _db_connect()
    try:
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except Exception:
            pass
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS move3m_alert_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER,
                exchange TEXT,
                symbol TEXT,
                pct_3m REAL,
                pct_24h REAL,
                quote_24h REAL,
                price REAL
            )
            """
        )
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_move3m_alert_log_created ON move3m_alert_log(created_at)")
        except Exception:
            pass
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_move3m_alert_log_symbol ON move3m_alert_log(symbol, created_at)")
        except Exception:
            pass

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS whale_watchlist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER,
                chain TEXT,
                address TEXT,
                label TEXT,
                tags TEXT
            )
            """
        )
        try:
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_whale_watchlist_chain_addr ON whale_watchlist(chain, address)")
        except Exception:
            pass
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_whale_watchlist_chain ON whale_watchlist(chain, created_at)")
        except Exception:
            pass

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS whale_alert_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER,
                enabled INTEGER,
                name TEXT,
                chain TEXT,
                min_usd REAL,
                direction TEXT,
                watchlist_only INTEGER
            )
            """
        )
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_whale_alert_rules_enabled ON whale_alert_rules(enabled, created_at)")
        except Exception:
            pass

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS whale_alert_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER,
                uniq TEXT,
                rule_id INTEGER,
                chain TEXT,
                direction TEXT,
                amount_usd REAL,
                asset TEXT,
                from_addr TEXT,
                to_addr TEXT,
                tx_hash TEXT,
                explorer_url TEXT,
                message TEXT,
                ok INTEGER,
                error TEXT
            )
            """
        )
        try:
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_whale_alert_history_uniq ON whale_alert_history(uniq)")
        except Exception:
            pass
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_whale_alert_history_created ON whale_alert_history(created_at)")
        except Exception:
            pass

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS news_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uniq TEXT NOT NULL UNIQUE,
                source TEXT,
                title TEXT,
                title_zh TEXT,
                link TEXT,
                published_at INTEGER,
                summary TEXT,
                summary_zh TEXT,
                tags TEXT,
                coins TEXT,
                sentiment TEXT,
                reason TEXT,
                strength REAL,
                created_at INTEGER,
                translated_at INTEGER
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS news_settings (
                k TEXT PRIMARY KEY,
                v TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS news_push_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER,
                uniq TEXT,
                level TEXT,
                title TEXT,
                link TEXT,
                message TEXT,
                ok INTEGER,
                error TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_push_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER,
                uniq TEXT,
                symbol TEXT,
                contract TEXT,
                level TEXT,
                score REAL,
                reasons TEXT,
                message TEXT,
                ok INTEGER,
                error TEXT
            )
            """
        )

        # 去重：同一信号桶只推一次
        try:
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_signal_push_history_uniq ON signal_push_history(uniq)")
        except Exception:
            pass

        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_signal_push_history_symbol ON signal_push_history(symbol, created_at)")
        except Exception:
            pass

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tri_signal_push_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER,
                uniq TEXT,
                contract TEXT,
                side TEXT,
                grade TEXT,
                high_prob INTEGER,
                reasons TEXT,
                entry REAL,
                sl REAL,
                tp REAL,
                atr REAL,
                message TEXT,
                ok INTEGER,
                error TEXT
            )
            """
        )
        try:
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_tri_signal_push_history_uniq ON tri_signal_push_history(uniq)")
        except Exception:
            pass
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tri_signal_push_history_contract ON tri_signal_push_history(contract, created_at)")
        except Exception:
            pass

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS master_a_push_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER,
                uniq TEXT,
                contract TEXT,
                side TEXT,
                reasons TEXT,
                entry REAL,
                sl REAL,
                tp1 REAL,
                tp2 REAL,
                atr REAL,
                message TEXT,
                ok INTEGER,
                error TEXT
            )
            """
        )
        try:
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_master_a_push_history_uniq ON master_a_push_history(uniq)")
        except Exception:
            pass
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_master_a_push_history_contract ON master_a_push_history(contract, created_at)")
        except Exception:
            pass

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS master_b_push_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER,
                uniq TEXT,
                contract TEXT,
                side TEXT,
                reasons TEXT,
                entry REAL,
                sl REAL,
                tp1 REAL,
                tp2 REAL,
                atr REAL,
                message TEXT,
                ok INTEGER,
                error TEXT
            )
            """
        )
        try:
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_master_b_push_history_uniq ON master_b_push_history(uniq)")
        except Exception:
            pass
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_master_b_push_history_contract ON master_b_push_history(contract, created_at)")
        except Exception:
            pass

        # 轻量迁移：push_history 缺字段时补齐
        ph_cols = [r[1] for r in cur.execute("PRAGMA table_info(news_push_history)").fetchall()]
        if "uniq" not in ph_cols:
            try:
                cur.execute("ALTER TABLE news_push_history ADD COLUMN uniq TEXT")
            except Exception:
                pass

        # 去重：同一条新闻只推一次（允许 uniq 为空）
        try:
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_news_push_history_uniq ON news_push_history(uniq)")
        except Exception:
            pass

        # 轻量迁移：老库缺字段时补齐（必须先补字段，再建索引）
        cols = [r[1] for r in cur.execute("PRAGMA table_info(news_items)").fetchall()]
        if "title_zh" not in cols:
            cur.execute("ALTER TABLE news_items ADD COLUMN title_zh TEXT")
        if "summary_zh" not in cols:
            cur.execute("ALTER TABLE news_items ADD COLUMN summary_zh TEXT")
        if "translated_at" not in cols:
            cur.execute("ALTER TABLE news_items ADD COLUMN translated_at INTEGER")
        if "coins" not in cols:
            cur.execute("ALTER TABLE news_items ADD COLUMN coins TEXT")
        if "reason" not in cols:
            cur.execute("ALTER TABLE news_items ADD COLUMN reason TEXT")

        # 索引：加速列表查询与去重
        cur.execute("CREATE INDEX IF NOT EXISTS idx_news_items_pub ON news_items(published_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_news_items_created ON news_items(created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_news_items_sentiment ON news_items(sentiment)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_news_items_coins ON news_items(coins)")
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_news_items_translated ON news_items(translated_at)")
        except Exception:
            pass
        conn.commit()
    finally:
        conn.close()


def _whale_addr_norm(addr: str) -> str:
    a = (addr or "").strip()
    return a.lower()


def _whale_chain_norm(chain: str) -> str:
    c = (chain or "ETH").strip().upper()
    if c not in ("ETH", "SOL", "BTC"):
        c = "ETH"
    return c


def _whale_direction_norm(direction: str) -> str:
    d = (direction or "all").strip().lower()
    if d in ("to_exchange", "from_exchange", "wallet", "unknown"):
        return d
    return "all"


def _get_gate_spot_last_usdt(symbol: str) -> Optional[float]:
    """获取 Gate 现货 USDT 最新价（免 Key）。仅用于 whales 模块的 USD 估算与过滤。"""
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    ck = f"gate_spot_last:{sym}"
    cached = _cache_get(ck, ttl=30)
    if cached is not None:
        try:
            return float(cached)
        except Exception:
            return None

    url = "https://api.gateio.ws/api/v4/spot/tickers"
    pair = f"{sym}_USDT"
    try:
        r = HTTP.get(url, params={"currency_pair": pair}, timeout=(8, 15))
        if r.status_code != 200:
            return None
        data = r.json()
        if isinstance(data, list) and data:
            last = data[0].get("last")
        elif isinstance(data, dict):
            last = data.get("last")
        else:
            last = None
        if last is None:
            return None
        px = float(last)
        if not math.isfinite(px) or px <= 0:
            return None
        _cache_set(ck, px)
        return px
    except Exception:
        return None


def _gate_spot_top_usdt_pairs(topn: int) -> Tuple[List[str], str]:
    try:
        n = max(5, min(1000, int(topn)))
    except Exception:
        n = 20

    ck = f"gate_spot:top_usdt_pairs:{n}"
    cached = _cache_get(ck, ttl=30)
    if cached is not None:
        try:
            arr = list(cached) if isinstance(cached, list) else []
            return [str(x) for x in arr if x], "ok"
        except Exception:
            pass

    url = f"{GATE_REST_SPOT_BASE}/tickers"
    stable = {
        "U",
        "USDT",
        "USDC",
        "DAI",
        "TUSD",
        "BUSD",
        "FDUSD",
        "USDP",
        "GUSD",
        "PAX",
        "USDJ",
        "USDD",
        "USDE",
        "PYUSD",
        "USD1",
    }

    lev_suffix = ("3L", "3S", "5L", "5S", "UP", "DOWN", "BULL", "BEAR")
    try:
        r = HTTP.get(url, timeout=(8, 18))
        if r.status_code != 200:
            return [], f"http {r.status_code}"
        data = r.json()
        if not isinstance(data, list):
            return [], "invalid_response"

        pairs: List[Tuple[str, float]] = []
        for it in data:
            if not isinstance(it, dict):
                continue
            cp = str(it.get("currency_pair") or "").strip().upper()
            if not cp or not cp.endswith("_USDT"):
                continue
            base = cp.split("_")[0] if "_" in cp else cp
            if base in stable:
                continue
            if base.endswith(lev_suffix):
                continue
            qv = it.get("quote_volume")
            try:
                qvf = float(qv) if qv is not None else 0.0
            except Exception:
                qvf = 0.0
            if not math.isfinite(qvf) or qvf <= 0:
                continue
            pairs.append((cp, qvf))

        pairs.sort(key=lambda x: x[1], reverse=True)
        out = [p for p, _ in pairs[:n]]
        _cache_set(ck, out)
        return out, "ok"
    except Exception as e:
        return [], str(e)


def _binance_spot_top_usdt_symbols(topn: int) -> Tuple[List[str], str]:
    try:
        n = max(5, min(1000, int(topn)))
    except Exception:
        n = 20

    ck = f"binance_spot:top_usdt_symbols:{n}"
    cached = _cache_get(ck, ttl=30)
    if cached is not None:
        try:
            arr = list(cached) if isinstance(cached, list) else []
            return [str(x) for x in arr if x], "ok"
        except Exception:
            pass

    stable = {
        "U",
        "USDT",
        "USDC",
        "DAI",
        "TUSD",
        "BUSD",
        "FDUSD",
        "USDP",
        "GUSD",
        "PAX",
        "USDJ",
        "USDD",
        "USDE",
        "PYUSD",
        "USD1",

    }
    lev_suffix = ("3L", "3S", "5L", "5S", "UP", "DOWN", "BULL", "BEAR")

    url = f"{BINANCE_REST_SPOT_BASE}/api/v3/ticker/24hr"
    try:
        r = HTTP.get(url, timeout=(8, 18))
        if r.status_code != 200:
            return [], f"http {r.status_code}"
        data = r.json()
        if not isinstance(data, list):
            return [], "invalid_response"

        pairs: List[Tuple[str, float]] = []
        for it in data:
            if not isinstance(it, dict):
                continue
            sym = str(it.get("symbol") or "").strip().upper()
            if not sym or not sym.endswith("USDT"):
                continue
            base = sym[: -4]
            if not base:
                continue
            if base in stable:
                continue
            if base.endswith(lev_suffix):
                continue
            qv = it.get("quoteVolume")
            try:
                qvf = float(qv) if qv is not None else 0.0
            except Exception:
                qvf = 0.0
            if not math.isfinite(qvf) or qvf <= 0:
                continue
            pairs.append((sym, qvf))

        pairs.sort(key=lambda x: x[1], reverse=True)
        out = [p for p, _ in pairs[:n]]
        _cache_set(ck, out)
        return out, "ok"
    except Exception as e:
        return [], str(e)


def _fetch_binance_spot_trades(symbol: str, limit: int = 200) -> Tuple[List[dict], str]:
    sym = (symbol or "").strip().upper()
    if not sym:
        return [], "missing_symbol"
    try:
        lim = max(10, min(1000, int(limit)))
    except Exception:
        lim = 200
    url = f"{BINANCE_REST_SPOT_BASE}/api/v3/trades"
    try:
        r = HTTP.get(url, params={"symbol": sym, "limit": lim}, timeout=(8, 18))
        if r.status_code != 200:
            return [], f"http {r.status_code}"
        data = r.json()
        if not isinstance(data, list):
            return [], "invalid_response"
        return data, "ok"
    except Exception as e:
        return [], str(e)


def _fetch_gate_spot_trades(currency_pair: str, limit: int = 50) -> Tuple[List[dict], str]:
    cp = (currency_pair or "").strip().upper()
    if not cp:
        return [], "missing_pair"
    try:
        lim = max(10, min(200, int(limit)))
    except Exception:
        lim = 50
    url = f"{GATE_REST_SPOT_BASE}/trades"
    try:
        r = HTTP.get(url, params={"currency_pair": cp, "limit": lim}, timeout=(8, 18))
        if r.status_code != 200:
            return [], f"http {r.status_code}"
        data = r.json()
        if not isinstance(data, list):
            return [], "invalid_response"
        return data, "ok"
    except Exception as e:
        return [], str(e)


def api_exchange_spot_large_trades(
    exchange: str = "binance",
    min_usd: float = 100_000,
    topn: int = 20,
    limit: int = 100,
    offset: int = 0,
) -> JSONResponse:
    try:
        min_usd = float(min_usd)
    except Exception:
        min_usd = 100_000.0
    min_usd = max(1_000.0, min(200_000_000.0, float(min_usd)))
    try:
        topn = max(5, min(200, int(topn)))
    except Exception:
        topn = 20
    try:
        limit = max(10, min(10000, int(limit)))
    except Exception:
        limit = 100
    try:
        offset = max(0, int(offset))
    except Exception:
        offset = 0

    ex_name = (exchange or "binance").strip().lower()
    if ex_name not in ("binance", "gate"):
        ex_name = "binance"

    stable = {
        "U",
        "USDT",
        "USDC",
        "DAI",
        "TUSD",
        "BUSD",
        "FDUSD",
        "USDP",
        "GUSD",
        "PAX",
        "USDJ",
        "USDD",
        "USDE",
        "PYUSD",
        "USDS",
        "SUSD",
        "LUSD",
        "FRAX",
        "USDX",
        "EURC",
        "USD1",
    }

    def _is_stable(sym: str) -> bool:
        s = (sym or "").strip().upper()
        if not s:
            return False
        if s in stable:
            return True
        # 启发式：常见稳定币多以 USD 结尾（例如：LUSD/SUSD/USDS），同时排除非稳定币的 BTC/ETH 等
        if s.endswith("USD") and len(s) <= 6:
            return True
        return False

    ck = f"ex:spot_large_trades:{ex_name}:{int(min_usd)}:{topn}:{limit}:{offset}"
    cached = _cache_get(ck, ttl=6)
    if cached is not None:
        return JSONResponse(cached)

    if ex_name == "gate":
        pairs, st_pairs = _gate_spot_top_usdt_pairs(topn)
    else:
        pairs, st_pairs = _binance_spot_top_usdt_symbols(topn)
    if not pairs:
        payload0 = {
            "ok": False,
            "items": [],
            "exchange": ex_name,
            "min_usd": min_usd,
            "topn": topn,
            "limit": limit,
            "offset": offset,
            "source": f"{ex_name}_spot",
            "source_status": f"tickers:{st_pairs}",
            "generated_at": int(time.time()),
        }
        return JSONResponse(payload0, status_code=502)

    if ex_name == "gate":
        max_workers = int(os.getenv("GATE_SPOT_TRADES_WORKERS", "6") or "6")
        max_workers = max(1, min(16, max_workers))
        per_pair = int(os.getenv("GATE_SPOT_TRADES_PER_PAIR", "200") or "200")
        per_pair = max(20, min(200, per_pair))
    else:
        max_workers = int(os.getenv("BINANCE_SPOT_TRADES_WORKERS", "10") or "10")
        max_workers = max(1, min(24, max_workers))
        per_pair = int(os.getenv("BINANCE_SPOT_TRADES_PER_PAIR", "1000") or "1000")
        per_pair = max(20, min(1000, per_pair))

    trades_all: List[dict] = []
    errs: List[str] = []
    seen: set = set()
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        if ex_name == "gate":
            futs = {ex.submit(_fetch_gate_spot_trades, cp, per_pair): cp for cp in pairs}
        else:
            futs = {ex.submit(_fetch_binance_spot_trades, sym, per_pair): sym for sym in pairs}
        for fut in as_completed(futs):
            cp = futs.get(fut) or ""
            try:
                rows, st = fut.result()
            except Exception as e:
                errs.append(f"{cp}:{str(e)}")
                continue
            if st != "ok":
                errs.append(f"{cp}:{st}")
                continue
            for t in rows:
                if not isinstance(t, dict):
                    continue
                if ex_name == "gate":
                    tid = str(t.get("id") or "").strip()
                    if tid:
                        uniq = f"gate:{cp}:{tid}"
                    else:
                        uniq = f"gate:{cp}:{t.get('create_time') or ''}:{t.get('price') or ''}:{t.get('amount') or ''}:{t.get('side') or ''}"
                    price = t.get("price")
                    amount = t.get("amount")
                    try:
                        p = float(price) if price is not None else 0.0
                        a = float(amount) if amount is not None else 0.0
                    except Exception:
                        continue
                    try:
                        ts = int(float(t.get("create_time") or 0))
                    except Exception:
                        ts = 0
                    side = str(t.get("side") or "").strip().lower() or "unknown"
                    pair_name = cp
                    asset = cp.split("_")[0] if "_" in cp else cp
                else:
                    tid = str(t.get("id") or "").strip()
                    if tid:
                        uniq = f"binance:{cp}:{tid}"
                    else:
                        uniq = f"binance:{cp}:{t.get('time') or ''}:{t.get('price') or ''}:{t.get('qty') or ''}"
                    price = t.get("price")
                    amount = t.get("qty")
                    try:
                        p = float(price) if price is not None else 0.0
                        a = float(amount) if amount is not None else 0.0
                    except Exception:
                        continue
                    try:
                        ts = int(float(t.get("time") or 0) / 1000.0)
                    except Exception:
                        ts = 0
                    is_buyer_maker = bool(t.get("isBuyerMaker"))
                    side = "sell" if is_buyer_maker else "buy"
                    base = cp[: -4] if cp.endswith("USDT") else cp
                    pair_name = f"{base}_USDT"
                    asset = base

                if _is_stable(asset):
                    continue

                if uniq in seen:
                    continue
                seen.add(uniq)
                if not math.isfinite(p) or not math.isfinite(a) or p <= 0 or a <= 0:
                    continue
                usd = p * a
                if usd < min_usd:
                    continue
                trades_all.append(
                    {
                        "id": uniq,
                        "ts": ts if ts > 0 else int(time.time()),
                        "exchange": ex_name,
                        "market": "spot",
                        "pair": pair_name,
                        "asset": asset,
                        "price": round(p, 10),
                        "amount": round(a, 10),
                        "amount_usd": round(float(usd), 2),
                        "side": side,
                        "trade_id": tid,
                        "source": f"{ex_name}_spot",
                    }
                )

    trades_all.sort(key=lambda x: float(x.get("amount_usd") or 0), reverse=True)
    page = trades_all[offset : offset + limit]
    payload = {
        "ok": True,
        "items": page,
        "exchange": ex_name,
        "min_usd": min_usd,
        "topn": topn,
        "limit": limit,
        "offset": offset,
        "source": f"{ex_name}_spot",
        "source_status": f"pairs:{st_pairs};errs:{len(errs)}",
        "errors": errs[:20],
        "generated_at": int(time.time()),
    }
    _cache_set(ck, payload)
    return JSONResponse(payload)


def api_exchange_spot_top_usdt_symbols(exchange: str = "binance", topn: int = 400) -> JSONResponse:
    ex_name = (exchange or "binance").strip().lower()
    if ex_name not in ("binance", "gate"):
        ex_name = "binance"
    try:
        topn_i = int(topn)
    except Exception:
        topn_i = 400
    topn_i = max(5, min(1000, topn_i))

    if ex_name == "gate":
        pairs, st = _gate_spot_top_usdt_pairs(topn_i)
        # gate pairs already like BTC_USDT
        symbols = [str(x).replace("_", "").upper() for x in pairs if x]
    else:
        symbols, st = _binance_spot_top_usdt_symbols(topn_i)

    return JSONResponse({"ok": True, "exchange": ex_name, "topn": topn_i, "symbols": symbols, "source_status": st})


def api_move3m_push(payload: Dict[str, Any]) -> JSONResponse:
    """3分钟异动 TG 推送（由前端触发，复用 Telegram 设置）。"""
    s = _news_settings()
    bot_token = (s.get("tg_bot_token") or "").strip()
    chat_id = (s.get("tg_chat_id") or "").strip()
    enabled_global = _setting_bool(s, "push_enabled", True)
    enabled_mod = _setting_bool(s, "push_move3m_enabled", True)
    if not enabled_global or not enabled_mod:
        return JSONResponse({"ok": True, "skipped": True, "error": "push_disabled"})
    if not bot_token or not chat_id:
        return JSONResponse({"ok": False, "error": "未配置 Telegram Bot Token 或 Chat ID"}, status_code=400)

    sym = str((payload or {}).get("symbol") or "").strip().upper()
    if not sym:
        return JSONResponse({"ok": False, "error": "missing symbol"}, status_code=400)
    try:
        pct3m = float((payload or {}).get("pct_3m") or 0.0)
    except Exception:
        pct3m = 0.0
    try:
        pct24h = float((payload or {}).get("pct_24h") or 0.0)
    except Exception:
        pct24h = 0.0
    try:
        price = float((payload or {}).get("price") or 0.0)
    except Exception:
        price = 0.0
    ex_name = str((payload or {}).get("exchange") or "binance").strip().lower() or "binance"

    # 去重/冷却：同一 symbol 每 120 秒最多推一次
    now_ts = int(time.time())
    bucket = int(now_ts / 120)
    uniq = f"move3m:{ex_name}:{sym}:{bucket}"
    if _cache_get(f"push:{uniq}", ttl=3600) is not None:
        return JSONResponse({"ok": True, "skipped": True, "error": "cooldown"})
    _cache_set(f"push:{uniq}", 1)

    ts_txt = datetime.datetime.fromtimestamp(now_ts).strftime("%Y-%m-%d %H:%M:%S")
    sign = "+" if pct3m >= 0 else ""
    msg = (
        f"【3分钟异动】{sym} ({ex_name})\n"
        f"时间：{ts_txt}\n"
        f"价格：{price:.10g}\n"
        f"3m涨跌：{sign}{pct3m:.2f}%\n"
        f"24h涨跌：{( '+' if pct24h >= 0 else '' )}{pct24h:.2f}%"
    )

    ok, err = _tg_send(bot_token=bot_token, chat_id=chat_id, text=msg)
    # 复用 push_history（写入 news_push_history，module=move3m 通过 level 区分）
    try:
        _push_history_add(
            uniq=uniq,
            level="move3m",
            title=f"{sym} {pct3m:+.2f}% (3m)",
            link=f"https://www.binance.com/en/trade/{sym}?type=spot" if ex_name == "binance" else "",
            message=msg,
            ok=ok,
            error=err,
        )
    except Exception:
        pass

    if not ok:
        return JSONResponse({"ok": False, "error": err or "send failed"}, status_code=502)
    return JSONResponse({"ok": True})


def api_move3m_log_list(limit: int = 100) -> JSONResponse:
    try:
        lim = int(limit)
    except Exception:
        lim = 100
    lim = max(1, min(100, lim))
    conn = _db_connect()
    try:
        rows = conn.execute(
            """
            SELECT id, created_at, exchange, symbol, pct_3m, pct_24h, quote_24h, price
            FROM move3m_alert_log
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (lim,),
        ).fetchall()
        items: List[Dict[str, Any]] = []
        for r in rows:
            try:
                items.append(
                    {
                        "id": int(r["id"]),
                        "ts": int(r["created_at"] or 0),
                        "exchange": str(r["exchange"] or ""),
                        "sym": str(r["symbol"] or ""),
                        "pct3m": float(r["pct_3m"] or 0.0),
                        "pct24h": float(r["pct_24h"] or 0.0),
                        "quote24h": float(r["quote_24h"] or 0.0),
                        "price": float(r["price"] or 0.0),
                    }
                )
            except Exception:
                continue
        return JSONResponse({"ok": True, "items": items, "limit": lim})
    finally:
        conn.close()


def api_move3m_log_add(payload: Dict[str, Any]) -> JSONResponse:
    try:
        ts = int((payload or {}).get("ts") or 0)
    except Exception:
        ts = 0
    if ts <= 0:
        ts = int(time.time())
    sym = str((payload or {}).get("sym") or (payload or {}).get("symbol") or "").strip().upper()
    if not sym:
        return JSONResponse({"ok": False, "error": "missing sym"}, status_code=400)
    ex_name = str((payload or {}).get("exchange") or "gate").strip().lower() or "gate"
    try:
        pct3m = float((payload or {}).get("pct3m") or (payload or {}).get("pct_3m") or 0.0)
    except Exception:
        pct3m = 0.0
    try:
        pct24h = float((payload or {}).get("pct24h") or (payload or {}).get("pct_24h") or 0.0)
    except Exception:
        pct24h = 0.0
    try:
        quote24h = float((payload or {}).get("quote24h") or (payload or {}).get("quote_24h") or 0.0)
    except Exception:
        quote24h = 0.0
    try:
        price = float((payload or {}).get("price") or 0.0)
    except Exception:
        price = 0.0

    conn = _db_connect()
    try:
        conn.execute(
            """
            INSERT INTO move3m_alert_log(created_at, exchange, symbol, pct_3m, pct_24h, quote_24h, price)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (ts, ex_name, sym, pct3m, pct24h, quote24h, price),
        )
        conn.commit()

        # 轻量清理：只保留最新 2000 条，避免无限增长
        try:
            conn.execute(
                """
                DELETE FROM move3m_alert_log
                WHERE id NOT IN (
                    SELECT id FROM move3m_alert_log ORDER BY created_at DESC, id DESC LIMIT 2000
                )
                """
            )
            conn.commit()
        except Exception:
            pass

        return JSONResponse({"ok": True})
    finally:
        conn.close()


def _eth_rpc_call(method: str, params: list) -> dict:
    url_raw = os.getenv("WHALES_ETH_RPC_URL", "https://cloudflare-eth.com").strip() or "https://cloudflare-eth.com"
    # 允许配置多个 RPC，逗号分隔：优先尝试前面的，失败自动切换
    urls = [u.strip() for u in str(url_raw).split(",") if u and str(u).strip()]
    if not urls:
        urls = ["https://cloudflare-eth.com"]

    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        max_retry = int(os.getenv("WHALES_ETH_RPC_RETRIES", "3") or "3")
    except Exception:
        max_retry = 3
    max_retry = max(0, min(10, max_retry))
    try:
        base_backoff = float(os.getenv("WHALES_ETH_RPC_BACKOFF_SEC", "0.6") or "0.6")
    except Exception:
        base_backoff = 0.6
    base_backoff = max(0.05, min(5.0, base_backoff))

    last_err: Optional[str] = None
    for url in urls:
        for attempt in range(max_retry + 1):
            try:
                r = HTTP_NO_PROXY.post(url, json=payload, timeout=(10, 25))
                if r.status_code != 200:
                    # 429/5xx 常见为限流或上游过载
                    if r.status_code in (429, 500, 502, 503, 504) and attempt < max_retry:
                        time.sleep(base_backoff * (2**attempt))
                        continue
                    raise RuntimeError(f"eth rpc http {r.status_code}")
                data = r.json()
                if not isinstance(data, dict):
                    raise RuntimeError("eth rpc invalid response")
                if data.get("error"):
                    err = data.get("error")
                    # Cloudflare/上游在压力大时常见：-32046 Cannot fulfill request
                    try:
                        code = err.get("code") if isinstance(err, dict) else None
                    except Exception:
                        code = None
                    msg = str(err)
                    if (code in (-32046, -32005, -32000) or "Cannot fulfill request" in msg) and attempt < max_retry:
                        time.sleep(base_backoff * (2**attempt))
                        continue
                    raise RuntimeError(msg)
                return data
            except Exception as e:
                last_err = f"{url}: {str(e)}"
                # 网络抖动/超时：重试
                if attempt < max_retry:
                    time.sleep(base_backoff * (2**attempt))
                    continue
                break
        # 本节点失败，切换下一个
        continue
    raise RuntimeError(last_err or "eth rpc failed")


def _eth_hex_to_int(x: Any) -> int:
    if x is None:
        return 0
    if isinstance(x, int):
        return x
    s = str(x)
    try:
        return int(s, 16) if s.startswith("0x") else int(s)
    except Exception:
        return 0


def _fetch_eth_rpc_transfers(min_usd: float, limit: int) -> Tuple[List[dict], str]:
    px = _get_gate_spot_last_usdt("ETH")
    if px is None:
        return [], "price_unavailable"

    try:
        # 总耗时预算：公共 RPC 扫块在高阈值（可能很难命中）时容易跑很久
        try:
            budget_sec = float(os.getenv("WHALES_ETH_SCAN_BUDGET_SEC", "12") or "12")
        except Exception:
            budget_sec = 12.0
        budget_sec = max(3.0, min(60.0, budget_sec))
        deadline = time.time() + budget_sec

        head = _eth_rpc_call("eth_blockNumber", [])
        bn_hex = head.get("result")
        head_n = _eth_hex_to_int(bn_hex)
        if head_n <= 0:
            return [], "invalid_head"

        want = max(1, min(400, int(limit)))
        items: List[dict] = []

        # 从最新区块往回扫，直到收集到足够多的大额 ETH 转账
        # 公共 RPC 容易限流，因此默认更保守；可用环境变量调大
        max_blocks = int(os.getenv("WHALES_ETH_SCAN_BLOCKS", "30") or "30")
        max_blocks = max(5, min(200, max_blocks))
        for i in range(max_blocks):
            if time.time() > deadline:
                return [], "timeout_budget_exceeded"
            n = head_n - i
            blk = _eth_rpc_call("eth_getBlockByNumber", [hex(n), True]).get("result")
            if not isinstance(blk, dict):
                continue
            ts = _eth_hex_to_int(blk.get("timestamp"))
            txs = blk.get("transactions")
            if not isinstance(txs, list):
                continue

            for tx in txs:
                if time.time() > deadline:
                    return [], "timeout_budget_exceeded"
                if not isinstance(tx, dict):
                    continue
                v = _eth_hex_to_int(tx.get("value"))
                if v <= 0:
                    continue
                amt = float(v) / 1e18
                usd = amt * float(px)
                if usd < float(min_usd):
                    continue
                tx_hash = str(tx.get("hash") or "").strip()
                from_addr = str(tx.get("from") or "").strip()
                to_addr = str(tx.get("to") or "").strip()
                if not tx_hash:
                    continue
                items.append(
                    {
                        "id": f"ETH:{tx_hash}",
                        "ts": int(ts) if ts > 0 else int(time.time()),
                        "chain": "ETH",
                        "asset": "ETH",
                        "amount": round(amt, 6),
                        "amount_usd": round(float(usd), 2),
                        "from": from_addr,
                        "to": to_addr,
                        "direction": "wallet",
                        "tags": {"fromLabel": "", "toLabel": "", "exchange": ""},
                        "tx_hash": tx_hash,
                        "explorer_url": f"https://etherscan.io/tx/{tx_hash}",
                        "source": "eth_rpc",
                    }
                )
                if len(items) >= want:
                    break
            if len(items) >= want:
                break

        items.sort(key=lambda x: int(x.get("ts") or 0), reverse=True)
        return items[:want], "ok"
    except Exception as e:
        return [], str(e)


def _blockscout_eth_base() -> str:
    # 可通过环境变量覆盖为其他链/自建 Blockscout
    return os.getenv("WHALES_ETH_BLOCKSCOUT_BASE", "https://eth.blockscout.com").strip() or "https://eth.blockscout.com"


def _fetch_eth_blockscout_transfers(min_usd: float, limit: int) -> Tuple[List[dict], str]:
    """使用 Blockscout v2 最近交易接口作为 ETH transfers 的降级数据源。

    说明：Blockscout v2 的字段在不同部署可能略有差异，因此这里尽量做容错解析。
    仅统计原生 ETH value 转账（value>0）。
    """
    px = _get_gate_spot_last_usdt("ETH")
    if px is None:
        return [], "price_unavailable"

    base = _blockscout_eth_base().rstrip("/")
    try:
        want = max(1, min(500, int(limit)))
    except Exception:
        want = 50

    # 多取一些以便过滤 min_usd 后仍能返回足够条数
    fetch_n = max(50, min(200, want * 3))
    url = f"{base}/api/v2/transactions"
    try:
        # 部分 Blockscout 部署对参数校验严格，会对未知/不支持的参数返回 422
        r = HTTP_NO_PROXY.get(url, params={"limit": fetch_n}, timeout=(10, 25))
        if r.status_code == 422:
            r = HTTP_NO_PROXY.get(url, timeout=(10, 25))
        if r.status_code != 200:
            return [], f"http {r.status_code}"
        data = r.json()
        items0 = data.get("items") if isinstance(data, dict) else None
        if not isinstance(items0, list):
            # 兼容直接返回 list 的实现
            items0 = data if isinstance(data, list) else None
        if not isinstance(items0, list):
            return [], "invalid_response"

        out: List[dict] = []
        for tx in items0:
            if not isinstance(tx, dict):
                continue
            # hash
            txh = str(tx.get("hash") or tx.get("tx_hash") or "").strip()
            if not txh:
                continue
            # from/to
            frm = tx.get("from")
            to = tx.get("to")
            from_addr = str(frm.get("hash") if isinstance(frm, dict) else frm or "").strip()
            to_addr = str(to.get("hash") if isinstance(to, dict) else to or "").strip()

            # timestamp
            ts = 0
            tsv = tx.get("timestamp") or tx.get("block_timestamp") or tx.get("timeStamp")
            if isinstance(tsv, (int, float)):
                ts = int(tsv)
            elif isinstance(tsv, str):
                # 尝试解析 "2024-..." 或者秒字符串
                try:
                    if tsv.isdigit():
                        ts = int(tsv)
                    else:
                        ts = int(datetime.datetime.fromisoformat(tsv.replace("Z", "+00:00")).timestamp())
                except Exception:
                    ts = 0

            # value (wei)
            val = tx.get("value")
            wei = 0
            try:
                if isinstance(val, int):
                    wei = int(val)
                elif isinstance(val, str):
                    if val.startswith("0x"):
                        wei = int(val, 16)
                    elif val.isdigit():
                        wei = int(val)
            except Exception:
                wei = 0
            if wei <= 0:
                continue
            amt = float(wei) / 1e18
            usd = amt * float(px)
            if usd < float(min_usd):
                continue

            out.append(
                {
                    "id": f"ETH:{txh}",
                    "ts": ts if ts > 0 else int(time.time()),
                    "chain": "ETH",
                    "asset": "ETH",
                    "amount": round(float(amt), 6),
                    "amount_usd": round(float(usd), 2),
                    "from": from_addr,
                    "to": to_addr,
                    "direction": "wallet",
                    "tags": {"fromLabel": "", "toLabel": "", "exchange": ""},
                    "tx_hash": txh,
                    "explorer_url": f"https://etherscan.io/tx/{txh}",
                    "source": "blockscout_recent",
                }
            )
            if len(out) >= want:
                break

        if not out:
            return [], "no_items_under_threshold"
        out.sort(key=lambda x: float(x.get("amount_usd") or 0), reverse=True)
        return out[:want], "ok"
    except Exception as e:
        return [], str(e)


def _fetch_eth_blockscout_txs(address: str, limit: int = 50) -> Tuple[List[dict], str]:
    addr = (address or "").strip()
    if not addr:
        return [], "missing_address"
    base = _blockscout_eth_base().rstrip("/")
    url = f"{base}/api"
    try:
        params = {
            "module": "account",
            "action": "txlist",
            "address": addr,
            "startblock": 0,
            "endblock": 99999999,
            "sort": "desc",
        }
        r = HTTP.get(url, params=params, timeout=(10, 25))
        if r.status_code != 200:
            return [], f"http {r.status_code}"
        data = r.json()
        if not isinstance(data, dict):
            return [], "invalid_response"
        status = str(data.get("status") or "")
        if status not in ("1", "0"):
            # 有些 blockscout 不用 status 字段
            pass
        res = data.get("result")
        if not isinstance(res, list):
            return [], "invalid_result"
        return res[: max(1, min(200, int(limit)))], "ok"
    except Exception as e:
        return [], str(e)


def _fetch_eth_balance_native(address: str) -> Tuple[Optional[float], str]:
    addr = (address or "").strip()
    if not addr:
        return None, "missing_address"
    try:
        data = _eth_rpc_call("eth_getBalance", [addr, "latest"])
        bal_hex = data.get("result")
        wei = _eth_hex_to_int(bal_hex)
        eth = float(wei) / 1e18
        return eth, "ok"
    except Exception as e:
        return None, str(e)


def _btc_is_testnet_address(address: str) -> bool:
    a = (address or "").strip()
    if not a:
        return False
    al = a.lower()
    # 常见 testnet 前缀：tb1(bech32), m/n(p2pkh), 2(p2sh)
    if al.startswith("tb1"):
        return True
    if al[0] in ("m", "n", "2"):
        return True
    return False


def _mempool_base(address: str = "") -> str:
    allow_testnet = os.getenv("WHALES_BTC_ALLOW_TESTNET", "0").strip() in ("1", "true", "True", "yes", "YES")
    if _btc_is_testnet_address(address) and allow_testnet:
        return os.getenv("WHALES_BTC_API_BASE_TESTNET", "https://mempool.space/testnet/api").strip() or "https://mempool.space/testnet/api"
    return os.getenv("WHALES_BTC_API_BASE", "https://mempool.space/api").strip() or "https://mempool.space/api"


def _fetch_btc_address_info(address: str) -> Tuple[dict, str]:
    addr = (address or "").strip()
    if not addr:
        return {}, "missing_address"
    if _btc_is_testnet_address(addr) and not (os.getenv("WHALES_BTC_ALLOW_TESTNET", "0").strip() in ("1", "true", "True", "yes", "YES")):
        return {}, "testnet_not_allowed"
    base = _mempool_base(addr).rstrip("/")
    addr_q = quote(addr, safe="")
    try:
        r = HTTP.get(f"{base}/address/{addr_q}", timeout=(10, 25))
        if r.status_code != 200:
            if r.status_code == 400:
                return {}, "invalid_btc_address"
            return {}, f"http {r.status_code}"
        data = r.json()
        return data if isinstance(data, dict) else {}, "ok"
    except Exception as e:
        return {}, str(e)


def _fetch_btc_address_txs(address: str, limit: int = 50) -> Tuple[List[dict], str]:
    addr = (address or "").strip()
    if not addr:
        return [], "missing_address"
    if _btc_is_testnet_address(addr) and not (os.getenv("WHALES_BTC_ALLOW_TESTNET", "0").strip() in ("1", "true", "True", "yes", "YES")):
        return [], "testnet_not_allowed"
    base = _mempool_base(addr).rstrip("/")
    addr_q = quote(addr, safe="")
    try:
        r = HTTP.get(f"{base}/address/{addr_q}/txs", timeout=(10, 25))
        if r.status_code != 200:
            if r.status_code == 400:
                return [], "invalid_btc_address"
            return [], f"http {r.status_code}"
        data = r.json()
        if not isinstance(data, list):
            return [], "invalid_response"
        return data[: max(1, min(200, int(limit)))], "ok"
    except Exception as e:
        return [], str(e)


def _btc_tx_value_delta_to_addr(tx: dict, addr: str) -> Tuple[float, float, float]:
    """返回 (delta_btc, in_btc, out_btc)；delta = in - out。"""
    a = (addr or "").strip()
    if not isinstance(tx, dict) or not a:
        return 0.0, 0.0, 0.0

    vin = tx.get("vin")
    vout = tx.get("vout")
    in_sat = 0
    out_sat = 0

    if isinstance(vout, list):
        for o in vout:
            if not isinstance(o, dict):
                continue
            if _btc_addr_from_scriptpubkey(o) == a:
                try:
                    in_sat += int(o.get("value") or 0)
                except Exception:
                    pass

    if isinstance(vin, list):
        for i in vin:
            if not isinstance(i, dict):
                continue
            prev = i.get("prevout")
            if isinstance(prev, dict) and _btc_addr_from_scriptpubkey(prev) == a:
                try:
                    out_sat += int(prev.get("value") or 0)
                except Exception:
                    pass

    in_btc = float(in_sat) / 1e8
    out_btc = float(out_sat) / 1e8
    return (in_btc - out_btc), in_btc, out_btc


def _eth_tx_value_delta_to_addr(tx: dict, addr: str) -> Tuple[float, float, float]:
    """返回 (delta_eth, in_eth, out_eth)；仅统计原生 ETH value。"""
    a = (addr or "").strip().lower()
    if not isinstance(tx, dict) or not a:
        return 0.0, 0.0, 0.0
    try:
        frm = str(tx.get("from") or "").strip().lower()
        to = str(tx.get("to") or "").strip().lower()
        val = tx.get("value")
        wei = int(val) if val is not None and str(val).isdigit() else 0
    except Exception:
        frm, to, wei = "", "", 0
    eth = float(wei) / 1e18
    in_eth = eth if to == a else 0.0
    out_eth = eth if frm == a else 0.0
    return (in_eth - out_eth), in_eth, out_eth


def _build_addr_series_24h(now: int) -> Dict[int, dict]:
    buckets: Dict[int, dict] = {}
    for k in range(24):
        ts0 = now - (23 - k) * 3600
        hour = int(ts0 // 3600) * 3600
        buckets[hour] = {"ts": hour, "in_usd": 0.0, "out_usd": 0.0, "net_usd": 0.0, "count": 0}
    return buckets


def api_whales_address_detail(
    chain: str,
    address: str,
    min_usd: float = 1_000_000,
    limit: int = 50,
) -> JSONResponse:
    try:
        limit_i = max(10, min(200, int(limit)))
    except Exception:
        limit_i = 50
    try:
        min_usd_f = float(min_usd)
    except Exception:
        min_usd_f = 1_000_000.0
    min_usd_f = max(10_000.0, min(200_000_000.0, min_usd_f))

    chain_u = _whale_chain_norm(chain)
    addr = (address or "").strip()
    if not addr:
        return JSONResponse({"ok": False, "error": "missing address"}, status_code=400)

    now = int(time.time())
    buckets = _build_addr_series_24h(now)
    big_moves: List[dict] = []
    recent: List[dict] = []

    if chain_u == "BTC":
        px = _get_gate_spot_last_usdt("BTC")
        if px is None:
            return JSONResponse({"ok": False, "error": "price_unavailable"}, status_code=502)
        info, st_info = _fetch_btc_address_info(addr)
        txs, st_txs = _fetch_btc_address_txs(addr, limit=limit_i)
        if not txs:
            return JSONResponse({"ok": False, "error": f"mempool_failed:{st_txs}"}, status_code=502)

        # 当前余额（BTC）
        bal_btc = None
        try:
            cs = info.get("chain_stats") if isinstance(info, dict) else None
            if isinstance(cs, dict):
                funded = int(cs.get("funded_txo_sum") or 0)
                spent = int(cs.get("spent_txo_sum") or 0)
                bal_btc = float(funded - spent) / 1e8
        except Exception:
            bal_btc = None

        for tx in txs:
            ts = 0
            try:
                status = tx.get("status") if isinstance(tx, dict) else None
                ts = int(status.get("block_time") or 0) if isinstance(status, dict) else 0
            except Exception:
                ts = 0
            delta_btc, in_btc, out_btc = _btc_tx_value_delta_to_addr(tx, addr)
            usd_abs = abs(delta_btc) * float(px)
            hour = int(ts // 3600) * 3600 if ts > 0 else None
            if hour is not None and hour in buckets:
                if delta_btc >= 0:
                    buckets[hour]["in_usd"] += float(in_btc) * float(px)
                else:
                    buckets[hour]["out_usd"] += float(out_btc) * float(px)
                buckets[hour]["count"] += 1

            txid = str(tx.get("txid") or "").strip() if isinstance(tx, dict) else ""
            recent.append(
                {
                    "ts": ts,
                    "tx_hash": txid,
                    "explorer_url": f"https://mempool.space/tx/{txid}" if txid else "",
                    "delta": round(delta_btc, 8),
                    "delta_usd": round(float(delta_btc) * float(px), 2),
                    "in": round(in_btc, 8),
                    "out": round(out_btc, 8),
                    "asset": "BTC",
                }
            )
            if usd_abs >= float(min_usd_f):
                big_moves.append(recent[-1])

        for h in buckets:
            buckets[h]["net_usd"] = float(buckets[h]["in_usd"]) - float(buckets[h]["out_usd"])

        payload = {
            "ok": True,
            "chain": chain_u,
            "address": addr,
            "generated_at": int(time.time()),
            "source": "mempool",
            "source_status": f"info:{st_info};txs:{st_txs}",
            "price_usd": float(px),
            "holdings": {
                "asset": "BTC",
                "balance": bal_btc,
                "balance_usd": round(float(bal_btc) * float(px), 2) if bal_btc is not None else None,
            },
            "series_24h": [
                {
                    "ts": int(b["ts"]),
                    "inflow_usd": round(float(b["in_usd"]), 2),
                    "outflow_usd": round(float(b["out_usd"]), 2),
                    "netflow_usd": round(float(b["net_usd"]), 2),
                    "tx_count": int(b["count"]),
                }
                for b in [buckets[k] for k in sorted(buckets.keys())]
            ],
            "recent_txs": recent[:limit_i],
            "big_moves": big_moves[:limit_i],
        }
        return JSONResponse(payload)

    if chain_u == "ETH":
        px = _get_gate_spot_last_usdt("ETH")
        if px is None:
            return JSONResponse({"ok": False, "error": "price_unavailable"}, status_code=502)

        bal_eth, st_bal = _fetch_eth_balance_native(addr)
        txs, st_txs = _fetch_eth_blockscout_txs(addr, limit=limit_i)
        # txlist 失败也不直接报错：至少返回余额 + 降级提示

        if txs:
            for tx in txs:
                try:
                    ts = int(tx.get("timeStamp") or 0)
                except Exception:
                    ts = 0
                delta_eth, in_eth, out_eth = _eth_tx_value_delta_to_addr(tx, addr)
                usd_abs = abs(delta_eth) * float(px)
                hour = int(ts // 3600) * 3600 if ts > 0 else None
                if hour is not None and hour in buckets:
                    if delta_eth >= 0:
                        buckets[hour]["in_usd"] += float(in_eth) * float(px)
                    else:
                        buckets[hour]["out_usd"] += float(out_eth) * float(px)
                    buckets[hour]["count"] += 1

                txh = str(tx.get("hash") or "").strip()
                recent.append(
                    {
                        "ts": ts,
                        "tx_hash": txh,
                        "explorer_url": f"https://etherscan.io/tx/{txh}" if txh else "",
                        "delta": round(delta_eth, 6),
                        "delta_usd": round(float(delta_eth) * float(px), 2),
                        "in": round(in_eth, 6),
                        "out": round(out_eth, 6),
                        "asset": "ETH",
                        "from": str(tx.get("from") or ""),
                        "to": str(tx.get("to") or ""),
                    }
                )
                if usd_abs >= float(min_usd_f):
                    big_moves.append(recent[-1])

        for h in buckets:
            buckets[h]["net_usd"] = float(buckets[h]["in_usd"]) - float(buckets[h]["out_usd"])

        payload = {
            "ok": True,
            "chain": chain_u,
            "address": addr,
            "generated_at": int(time.time()),
            "source": "eth_rpc+blockscout",
            "source_status": f"balance:{st_bal};txs:{st_txs}",
            "price_usd": float(px),
            "holdings": {
                "asset": "ETH",
                "balance": bal_eth,
                "balance_usd": round(float(bal_eth) * float(px), 2) if bal_eth is not None else None,
            },
            "series_24h": [
                {
                    "ts": int(b["ts"]),
                    "inflow_usd": round(float(b["in_usd"]), 2),
                    "outflow_usd": round(float(b["out_usd"]), 2),
                    "netflow_usd": round(float(b["net_usd"]), 2),
                    "tx_count": int(b["count"]),
                }
                for b in [buckets[k] for k in sorted(buckets.keys())]
            ],
            "recent_txs": recent[:limit_i],
            "big_moves": big_moves[:limit_i],
            "note": "ETH 交易列表来自 Blockscout（免费公共索引）；若失败将只展示余额。",
        }
        return JSONResponse(payload)

    return JSONResponse({"ok": False, "error": f"unsupported_chain:{chain_u}"}, status_code=400)


def _btc_addr_from_scriptpubkey(vout: dict) -> str:
    if not isinstance(vout, dict):
        return ""
    spk = vout.get("scriptpubkey")
    if isinstance(spk, str) and spk:
        # 某些接口直接给 scriptpubkey_address；此处兼容不同字段
        pass
    addr = vout.get("scriptpubkey_address")
    if isinstance(addr, str) and addr:
        return addr
    addrs = vout.get("scriptpubkey_addresses")
    if isinstance(addrs, list) and addrs:
        a0 = addrs[0]
        return str(a0) if a0 else ""
    return ""


def _fetch_btc_mempool_transfers(min_usd: float, limit: int) -> Tuple[List[dict], str]:
    px = _get_gate_spot_last_usdt("BTC")
    if px is None:
        return [], "price_unavailable"

    base = _mempool_base("").rstrip("/")
    want = max(1, min(400, int(limit)))
    ex_map = _whale_exchange_addr_map("BTC")
    try:
        blocks = HTTP.get(f"{base}/blocks", timeout=(8, 15)).json()
        if not isinstance(blocks, list) or not blocks:
            return [], "no_blocks"

        max_blocks = int(os.getenv("WHALES_BTC_SCAN_BLOCKS", "20") or "20")
        max_blocks = max(5, min(80, max_blocks))
        items: List[dict] = []
        for b in blocks[:max_blocks]:
            if not isinstance(b, dict):
                continue
            blk_id = b.get("id")
            ts = int(b.get("timestamp") or 0)
            if not blk_id:
                continue

            # 每个 block 先扫前 50 笔（2 页）；够用了
            for start in (0, 25):
                txs = HTTP.get(f"{base}/block/{blk_id}/txs/{start}", timeout=(10, 25)).json()
                if not isinstance(txs, list) or not txs:
                    continue
                for tx in txs:
                    if not isinstance(tx, dict):
                        continue
                    txid = str(tx.get("txid") or "").strip()
                    vout = tx.get("vout")
                    vin = tx.get("vin")
                    if not txid or not isinstance(vout, list):
                        continue

                    # 取最大的单输出作为“转入地址/金额”的近似（whale 监控够用）
                    max_v = 0
                    max_o: Optional[dict] = None
                    for o in vout:
                        if not isinstance(o, dict):
                            continue
                        try:
                            vv = int(o.get("value") or 0)
                        except Exception:
                            vv = 0
                        if vv > max_v:
                            max_v = vv
                            max_o = o
                    if max_v <= 0 or max_o is None:
                        continue

                    btc = float(max_v) / 1e8
                    usd = btc * float(px)
                    if usd < float(min_usd):
                        continue

                    to_addr = _btc_addr_from_scriptpubkey(max_o)
                    from_addr = ""
                    if isinstance(vin, list) and vin:
                        prev = vin[0].get("prevout") if isinstance(vin[0], dict) else None
                        if isinstance(prev, dict):
                            from_addr = _btc_addr_from_scriptpubkey(prev)

                    tags = {"fromLabel": "", "toLabel": "", "exchange": ""}
                    direction = "wallet"
                    try:
                        f0 = (from_addr or "").strip().lower()
                        t0 = (to_addr or "").strip().lower()
                        ex_from = ex_map.get(f0) if f0 else ""
                        ex_to = ex_map.get(t0) if t0 else ""
                        if ex_from:
                            tags["fromLabel"] = ex_from
                            tags["exchange"] = ex_from
                        if ex_to:
                            tags["toLabel"] = ex_to
                            tags["exchange"] = ex_to or tags.get("exchange") or ""
                        if ex_to and not ex_from:
                            direction = "to_exchange"
                        elif ex_from and not ex_to:
                            direction = "from_exchange"
                    except Exception:
                        pass

                    items.append(
                        {
                            "id": f"BTC:{txid}",
                            "ts": ts if ts > 0 else int(time.time()),
                            "chain": "BTC",
                            "asset": "BTC",
                            "amount": round(btc, 8),
                            "amount_usd": round(float(usd), 2),
                            "from": from_addr,
                            "to": to_addr,
                            "direction": direction,
                            "tags": tags,
                            "tx_hash": txid,
                            "explorer_url": f"https://mempool.space/tx/{txid}",
                            "source": "mempool",
                        }
                    )
                    if len(items) >= want:
                        break
                if len(items) >= want:
                    break
            if len(items) >= want:
                break

        items.sort(key=lambda x: int(x.get("ts") or 0), reverse=True)
        return items[:want], "ok"
    except Exception as e:
        return [], str(e)


def _whales_settings() -> dict:
    s = _settings_get("whales_settings", default={})
    return s if isinstance(s, dict) else {}


def _whale_watchlist_get(chain: Optional[str] = None) -> List[dict]:
    conn = _db_connect()
    try:
        if chain:
            cu = _whale_chain_norm(chain)
            rows = conn.execute(
                "SELECT id, created_at, chain, address, label, tags FROM whale_watchlist WHERE chain=? ORDER BY id DESC",
                (cu,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, created_at, chain, address, label, tags FROM whale_watchlist ORDER BY id DESC"
            ).fetchall()
        items: List[dict] = []
        for r in rows:
            d = dict(r)
            try:
                d["tags"] = json.loads(d.get("tags") or "{}")
            except Exception:
                d["tags"] = {}
            items.append(d)
        return items
    finally:
        conn.close()


def _whale_exchange_addr_map(chain: str) -> Dict[str, str]:
    """从 Watchlist 推导交易所地址标签映射：addr_lower -> exchange_name。

    说明：不依赖外部付费标签库；仅对 Watchlist 中你手动标注的交易所地址生效。
    - 优先使用 tags.exchange (string)
    - 否则从 label 中做简单关键字识别
    """
    cu = _whale_chain_norm(chain)
    ck = f"whales:ex_addr_map:{cu}"
    cached = _cache_get(ck, ttl=20)
    if isinstance(cached, dict):
        try:
            return {str(k): str(v) for k, v in cached.items() if k and v}
        except Exception:
            pass

    kw_map = {
        "binance": "Binance",
        "okx": "OKX",
        "coinbase": "Coinbase",
        "kraken": "Kraken",
        "huobi": "Huobi",
        "bybit": "Bybit",
        "gate": "Gate",
        "kucoin": "KuCoin",
        "bitfinex": "Bitfinex",
        "bitstamp": "Bitstamp",
    }

    out: Dict[str, str] = {}
    try:
        items = _whale_watchlist_get(cu)
    except Exception:
        items = []

    for it in items:
        try:
            addr = _whale_addr_norm(str(it.get("address") or ""))
        except Exception:
            addr = ""
        if not addr:
            continue
        tg = it.get("tags") if isinstance(it, dict) else None
        exchange = ""
        if isinstance(tg, dict):
            exv = tg.get("exchange")
            exchange = (str(exv).strip() if exv is not None else "")

        if not exchange:
            lb = (str(it.get("label") or "")).strip().lower()
            for k, name in kw_map.items():
                if k in lb:
                    exchange = name
                    break

        if exchange:
            out[addr.lower()] = exchange

    _cache_set(ck, out)
    return out


def _whale_watchlist_upsert(chain: str, address: str, label: str = "", tags: Optional[dict] = None) -> dict:
    cu = _whale_chain_norm(chain)
    addr = _whale_addr_norm(address)
    if not addr:
        raise ValueError("missing address")
    tg = tags if isinstance(tags, dict) else {}
    conn = _db_connect()
    try:
        now = int(time.time())
        conn.execute(
            """
            INSERT INTO whale_watchlist(created_at, chain, address, label, tags)
            VALUES(?,?,?,?,?)
            ON CONFLICT(chain, address) DO UPDATE SET
              label=excluded.label,
              tags=excluded.tags
            """,
            (now, cu, addr, (label or "").strip(), json.dumps(tg, ensure_ascii=False)),
        )
        conn.commit()
        r = conn.execute(
            "SELECT id, created_at, chain, address, label, tags FROM whale_watchlist WHERE chain=? AND address=? LIMIT 1",
            (cu, addr),
        ).fetchone()
        out = dict(r) if r else {"chain": cu, "address": addr, "label": label, "tags": tg}
        try:
            out["tags"] = json.loads(out.get("tags") or "{}")
        except Exception:
            out["tags"] = {}
        return out
    finally:
        conn.close()


def _whale_watchlist_delete(item_id: int) -> bool:
    conn = _db_connect()
    try:
        conn.execute("DELETE FROM whale_watchlist WHERE id=?", (int(item_id),))
        conn.commit()
        return True
    finally:
        conn.close()


def _whale_rules_list() -> List[dict]:
    conn = _db_connect()
    try:
        rows = conn.execute(
            "SELECT id, created_at, enabled, name, chain, min_usd, direction, watchlist_only FROM whale_alert_rules ORDER BY id DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _whale_rule_create(payload: dict) -> dict:
    name = (payload.get("name") or "").strip() if isinstance(payload, dict) else ""
    chain = _whale_chain_norm(payload.get("chain") if isinstance(payload, dict) else "ETH")
    direction = _whale_direction_norm(payload.get("direction") if isinstance(payload, dict) else "all")
    try:
        min_usd = float(payload.get("min_usd") if isinstance(payload, dict) else 1_000_000)
    except Exception:
        min_usd = 1_000_000.0
    min_usd = max(10_000.0, min(1_000_000_000.0, float(min_usd)))
    enabled = 1 if bool(payload.get("enabled", True)) else 0
    watchlist_only = 1 if bool(payload.get("watchlist_only", False)) else 0
    now = int(time.time())

    conn = _db_connect()
    try:
        conn.execute(
            """
            INSERT INTO whale_alert_rules(created_at, enabled, name, chain, min_usd, direction, watchlist_only)
            VALUES(?,?,?,?,?,?,?)
            """,
            (now, enabled, name, chain, float(min_usd), direction, watchlist_only),
        )
        conn.commit()
        r = conn.execute(
            "SELECT id, created_at, enabled, name, chain, min_usd, direction, watchlist_only FROM whale_alert_rules ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return dict(r) if r else {}
    finally:
        conn.close()


def _whale_rule_delete(rule_id: int) -> bool:
    conn = _db_connect()
    try:
        conn.execute("DELETE FROM whale_alert_rules WHERE id=?", (int(rule_id),))
        conn.commit()
        return True
    finally:
        conn.close()


def _whale_alert_history(limit: int = 200) -> List[dict]:
    limit = max(1, min(1000, int(limit)))
    conn = _db_connect()
    try:
        rows = conn.execute(
            """
            SELECT id, created_at, uniq, rule_id, chain, direction, amount_usd, asset, from_addr, to_addr, tx_hash, explorer_url, message, ok, error
            FROM whale_alert_history
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _whale_alert_history_add(
    uniq: str,
    rule_id: int,
    chain: str,
    direction: str,
    amount_usd: float,
    asset: str,
    from_addr: str,
    to_addr: str,
    tx_hash: str,
    explorer_url: str,
    message: str,
    ok: bool,
    error: str,
) -> None:
    conn = _db_connect()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO whale_alert_history(
              created_at, uniq, rule_id, chain, direction, amount_usd, asset, from_addr, to_addr, tx_hash, explorer_url, message, ok, error
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(time.time()),
                uniq,
                int(rule_id),
                _whale_chain_norm(chain),
                (direction or "").strip(),
                float(amount_usd or 0),
                (asset or "").strip(),
                _whale_addr_norm(from_addr),
                _whale_addr_norm(to_addr),
                (tx_hash or "").strip(),
                (explorer_url or "").strip(),
                (message or ""),
                1 if ok else 0,
                (error or ""),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _whale_alert_has_uniq(uniq: str) -> bool:
    conn = _db_connect()
    try:
        r = conn.execute("SELECT 1 FROM whale_alert_history WHERE uniq=? LIMIT 1", (uniq,)).fetchone()
        return bool(r)
    finally:
        conn.close()


def _whale_make_msg(rule: dict, tx: dict) -> str:
    now_ts = int(time.time())
    ts_txt = datetime.datetime.fromtimestamp(now_ts).strftime("%Y-%m-%d %H:%M")
    name = (rule.get("name") if isinstance(rule, dict) else "") or "鲸鱼告警"
    chain = tx.get("chain") or "—"
    asset = tx.get("asset") or "—"
    direction = tx.get("direction") or "unknown"
    usd = tx.get("amount_usd")
    try:
        usd_f = float(usd) if usd is not None else 0.0
    except Exception:
        usd_f = 0.0
    usd_txt = f"${usd_f:,.0f}"
    from_addr = (tx.get("from") or "—")
    to_addr = (tx.get("to") or "—")
    link = (tx.get("explorer_url") or "").strip()

    dir_cn = {
        "to_exchange": "转入交易所",
        "from_exchange": "转出交易所",
        "wallet": "钱包间",
        "unknown": "未知",
    }.get(str(direction), str(direction))

    header = f"<b>【鲸鱼动向】{name}</b>\n时间：{ts_txt}"
    lines = [
        header,
        f"链：{chain}｜资产：{asset}｜方向：{dir_cn}",
        f"金额：{usd_txt}",
        f"From：{from_addr}",
        f"To：{to_addr}",
    ]
    if link:
        lines.append(f"Tx：{link}")
    return "\n".join(lines)


def _fetch_whale_alert_transfers(chain: str, min_usd: float, limit: int) -> Tuple[List[dict], str]:
    if not WHALE_ALERT_ENABLED or not WHALE_ALERT_API_KEY:
        return [], "disabled"

    chain_u = _whale_chain_norm(chain)
    currency = {"ETH": "eth", "BTC": "btc", "SOL": "sol"}.get(chain_u, "eth")
    url = "https://api.whale-alert.io/v1/transactions"
    try:
        params = {
            "api_key": WHALE_ALERT_API_KEY,
            "currency": currency,
            "min_value": max(1.0, float(min_usd) / 1_000_000.0),
            "limit": max(1, min(100, int(limit))),
        }
        r = HTTP.get(url, params=params, timeout=(10, 20))
        if r.status_code != 200:
            return [], f"http {r.status_code}"
        data = {}
        try:
            data = r.json()
        except Exception:
            data = {}
        txs = data.get("transactions") if isinstance(data, dict) else None
        if not isinstance(txs, list):
            return [], "invalid response"

        items: List[dict] = []
        for t in txs:
            if not isinstance(t, dict):
                continue
            ts = t.get("timestamp") or t.get("time") or t.get("created_at")
            try:
                ts_i = int(ts)
            except Exception:
                ts_i = int(time.time())
            amount_usd = t.get("amount_usd")
            if amount_usd is None:
                amount_usd = t.get("amount_usd_value")
            try:
                usd_f = float(amount_usd) if amount_usd is not None else None
            except Exception:
                usd_f = None
            if usd_f is None or usd_f < float(min_usd):
                continue

            sym = (t.get("symbol") or t.get("transaction_type") or "").strip()
            asset = (t.get("symbol") or t.get("currency") or chain_u).upper()

            from_addr = ""
            to_addr = ""
            tags = {"fromLabel": "", "toLabel": "", "exchange": ""}
            try:
                f = t.get("from") if isinstance(t.get("from"), dict) else {}
                to = t.get("to") if isinstance(t.get("to"), dict) else {}
                from_addr = (f.get("address") or "")
                to_addr = (to.get("address") or "")
                if f.get("owner"):
                    tags["fromLabel"] = str(f.get("owner"))
                if to.get("owner"):
                    tags["toLabel"] = str(to.get("owner"))
                if f.get("owner_type") == "exchange":
                    tags["exchange"] = tags["fromLabel"] or "exchange"
                if to.get("owner_type") == "exchange":
                    tags["exchange"] = tags["toLabel"] or "exchange"
            except Exception:
                pass

            direction = "unknown"
            if tags.get("exchange"):
                if tags.get("toLabel") and (tags.get("toLabel") == tags.get("exchange")):
                    direction = "to_exchange"
                elif tags.get("fromLabel") and (tags.get("fromLabel") == tags.get("exchange")):
                    direction = "from_exchange"

            tx_hash = (t.get("hash") or t.get("tx_hash") or "").strip()
            explorer = ""
            if tx_hash:
                if chain_u == "ETH":
                    explorer = f"https://etherscan.io/tx/{tx_hash}"
                elif chain_u == "SOL":
                    explorer = f"https://solscan.io/tx/{tx_hash}"
                else:
                    explorer = f"https://mempool.space/tx/{tx_hash}"

            items.append(
                {
                    "id": f"{chain_u}:{tx_hash or (str(ts_i)+':'+str(len(items)))}",
                    "ts": ts_i,
                    "chain": chain_u,
                    "asset": asset,
                    "amount": t.get("amount") if t.get("amount") is not None else None,
                    "amount_usd": round(float(usd_f), 2),
                    "from": from_addr,
                    "to": to_addr,
                    "direction": direction,
                    "tags": tags,
                    "tx_hash": tx_hash,
                    "explorer_url": explorer,
                    "source": "whale_alert",
                    "raw_type": sym,
                }
            )

        items.sort(key=lambda x: int(x.get("ts") or 0), reverse=True)
        return items[:limit], "ok"
    except Exception as e:
        return [], str(e)


def _get_whale_transfers_auto(chain: str, min_usd: float, limit: int, offset: int) -> Tuple[List[dict], str, str]:
    chain_u = _whale_chain_norm(chain)
    if WHALE_ALERT_ENABLED and WHALE_ALERT_API_KEY:
        items, st = _fetch_whale_alert_transfers(chain_u, min_usd=min_usd, limit=limit + offset)
        if items:
            return items[offset : offset + limit], "whale_alert", st
        raise RuntimeError(f"whale_alert_failed:{st}")
    if chain_u == "ETH":
        items, st = _fetch_eth_rpc_transfers(min_usd=min_usd, limit=limit + offset)
        if items:
            return items[offset : offset + limit], "eth_rpc", st
        # 降级：Blockscout 最近交易
        items2, st2 = _fetch_eth_blockscout_transfers(min_usd=min_usd, limit=limit + offset)
        if not items2:
            raise RuntimeError(f"eth_rpc_failed:{st};blockscout_failed:{st2}")
        return items2[offset : offset + limit], "blockscout_recent", st2
    if chain_u == "BTC":
        items, st = _fetch_btc_mempool_transfers(min_usd=min_usd, limit=limit + offset)
        if not items:
            raise RuntimeError(f"mempool_failed:{st}")
        return items[offset : offset + limit], "mempool", st
    raise RuntimeError(f"unsupported_chain:{chain_u}")


def _whales_alert_loop() -> None:
    interval = max(10, min(3600, int(WHALES_ALERT_INTERVAL_SEC)))
    first = True
    while True:
        if first:
            time.sleep(interval)
            first = False
        try:
            s = _news_settings()
            bot_token = (s.get("tg_bot_token") or "").strip()
            chat_id = (s.get("tg_chat_id") or "").strip()
            enabled_all = _setting_bool(s, "push_enabled", True)
            enabled_mod = _setting_bool(s, "push_whales_enabled", True)
            if not (WHALES_ALERT_LOOP_ENABLED and enabled_all and enabled_mod and bot_token and chat_id):
                time.sleep(interval)
                continue

            rules = [r for r in _whale_rules_list() if int(r.get("enabled") or 0) == 1]
            if not rules:
                time.sleep(interval)
                continue

            watch = _whale_watchlist_get()
            watch_set = set((str(x.get("chain") or "").upper(), _whale_addr_norm(str(x.get("address") or ""))) for x in watch)

            for rule in rules:
                try:
                    chain = _whale_chain_norm(rule.get("chain") or "ETH")
                    direction = _whale_direction_norm(rule.get("direction") or "all")
                    min_usd = float(rule.get("min_usd") or 1_000_000.0)
                    watch_only = bool(int(rule.get("watchlist_only") or 0))
                    items, src, src_status = _get_whale_transfers_auto(chain, min_usd=min_usd, limit=100, offset=0)
                    for tx in items:
                        try:
                            tx_dir = str(tx.get("direction") or "unknown")
                            if direction != "all" and tx_dir != direction:
                                continue
                            from_a = _whale_addr_norm(str(tx.get("from") or ""))
                            to_a = _whale_addr_norm(str(tx.get("to") or ""))
                            if watch_only:
                                if (chain, from_a) not in watch_set and (chain, to_a) not in watch_set:
                                    continue

                            tx_hash = str(tx.get("tx_hash") or "").strip()
                            ts = int(tx.get("ts") or 0)
                            uniq = f"whale:{int(rule.get('id') or 0)}:{chain}:{tx_dir}:{tx_hash or ''}:{ts}:{int(float(tx.get('amount_usd') or 0))}"
                            if _whale_alert_has_uniq(uniq):
                                continue

                            msg = _whale_make_msg(rule, tx)
                            ok, err = _tg_send(bot_token=bot_token, chat_id=chat_id, text=msg, parse_mode="HTML")
                            if not ok:
                                err = (err or "send failed") + f" | src={src}:{src_status}"
                            _whale_alert_history_add(
                                uniq=uniq,
                                rule_id=int(rule.get("id") or 0),
                                chain=chain,
                                direction=tx_dir,
                                amount_usd=float(tx.get("amount_usd") or 0),
                                asset=str(tx.get("asset") or ""),
                                from_addr=str(tx.get("from") or ""),
                                to_addr=str(tx.get("to") or ""),
                                tx_hash=tx_hash,
                                explorer_url=str(tx.get("explorer_url") or ""),
                                message=msg,
                                ok=ok,
                                error=err,
                            )
                        except Exception:
                            continue
                except Exception:
                    continue
        except Exception:
            pass
        time.sleep(interval)


def _master_b_push_history_add(
    uniq: str,
    contract: str,
    side: str,
    reasons: List[str],
    entry: Optional[float],
    sl: Optional[float],
    tp1: Optional[float],
    tp2: Optional[float],
    atr: Optional[float],
    message: str,
    ok: bool,
    error: str,
) -> None:
    conn = _db_connect()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO master_b_push_history(created_at, uniq, contract, side, reasons, entry, sl, tp1, tp2, atr, message, ok, error)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(time.time()),
                uniq,
                contract,
                side,
                json.dumps(reasons, ensure_ascii=False),
                entry,
                sl,
                tp1,
                tp2,
                atr,
                message,
                1 if ok else 0,
                error or "",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _master_b_has_uniq(uniq: str) -> bool:
    uniq = (uniq or "").strip()
    if not uniq:
        return False
    conn = _db_connect()
    try:
        row = conn.execute("SELECT 1 FROM master_b_push_history WHERE uniq=? LIMIT 1", (uniq,)).fetchone()
        return bool(row)
    finally:
        conn.close()


def _master_b_last_push_ts(contract: str, side: str) -> Optional[int]:
    conn = _db_connect()
    try:
        row = conn.execute(
            """
            SELECT created_at FROM master_b_push_history
            WHERE contract=? AND side=?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (contract, side),
        ).fetchone()
        if not row:
            return None
        try:
            return int(row[0])
        except Exception:
            return None
    finally:
        conn.close()


def _signal_has_uniq(uniq: str) -> bool:
    uniq = (uniq or "").strip()
    if not uniq:
        return False
    conn = _db_connect()
    try:
        row = conn.execute(
            "SELECT 1 FROM signal_push_history WHERE uniq=? LIMIT 1",
            (uniq,),
        ).fetchone()
        return bool(row)
    finally:
        conn.close()


def _signal_last_push_ts(symbol: str) -> Optional[int]:
    conn = _db_connect()
    try:
        row = conn.execute(
            """
            SELECT created_at FROM signal_push_history
            WHERE symbol=?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (symbol,),
        ).fetchone()
        if not row:
            return None
        try:
            return int(row[0])
        except Exception:
            return None
    finally:
        conn.close()


def push_telegram_batch_recent(window_sec: int = 300, limit: int = 50, max_items_in_msg: int = 8) -> dict:
    """新闻多空哨兵：合并推送最近一段时间内的新闻信号。

    - window_sec：统计窗口（秒），只取 created_at >= now-window_sec 的新闻
    - strength 阈值从配置读取（默认 0.75），只推送 bullish/bearish 且 strength >= 阈值
    - 写入 news_push_history 作为去重与节流依据
    """
    s = _news_settings()
    enabled = _setting_bool(s, "push_enabled", True)
    threshold = s.get("push_threshold")
    try:
        threshold_f = float(threshold) if threshold is not None and threshold != "" else 0.75
    except Exception:
        threshold_f = 0.75
    bot_token = (s.get("tg_bot_token") or "").strip()
    chat_id = (s.get("tg_chat_id") or "").strip()

    if not enabled:
        return {"ok": True, "pushed": 0, "skipped": 0, "errors": []}
    if not bot_token or not chat_id:
        return {"ok": False, "pushed": 0, "skipped": 0, "errors": ["未配置 Telegram Bot Token 或 Chat ID"]}

    window_sec = max(60, min(3600, int(window_sec)))
    limit = max(1, min(200, int(limit)))
    max_items_in_msg = max(1, min(20, int(max_items_in_msg)))
    now_ts = int(time.time())
    since_ts = now_ts - window_sec

    conn = _db_connect()
    pushed = 0
    skipped = 0
    errors: List[str] = []
    try:
        rows = conn.execute(
            """
            SELECT uniq, title, link, coins, sentiment, strength, COALESCE(published_at, created_at) AS ts
            FROM news_items
            WHERE created_at >= ?
              AND sentiment IN ('bullish','bearish')
              AND strength IS NOT NULL
              AND strength >= ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (since_ts, threshold_f, limit),
        ).fetchall()

        candidates = []
        for r in rows:
            uniq = (r["uniq"] or "").strip()
            if not uniq:
                skipped += 1
                continue
            already = conn.execute("SELECT 1 FROM news_push_history WHERE uniq=? LIMIT 1", (uniq,)).fetchone()
            if already:
                skipped += 1
                continue
            candidates.append(r)

        if not candidates:
            return {"ok": True, "pushed": 0, "skipped": skipped, "errors": []}

        items = candidates[:max_items_in_msg]
        bull = sum(1 for r in items if (r["sentiment"] or "") == "bullish")
        bear = sum(1 for r in items if (r["sentiment"] or "") == "bearish")

        header = f"【新闻多空哨兵】近 {int(window_sec/60)} 分钟信号：利多 {bull} / 利空 {bear}（阈值 {threshold_f:.2f}）"
        lines: List[str] = [header]
        for r in items:
            sentiment = (r["sentiment"] or "").strip()
            sent_cn = "利多" if sentiment == "bullish" else "利空"
            strength = float(r["strength"])
            coins = (r["coins"] or "").strip() or "—"
            title = (r["title"] or "").strip()
            link = (r["link"] or "").strip()
            one = f"- {sent_cn} {strength:.2f} | {coins} | {title}"
            if link:
                one += f"\n  {link}"
            lines.append(one)

        msg = "\n".join(lines)
        if len(msg) > 3500:
            msg = msg[:3490] + "…"

        ok, err = _tg_send(bot_token=bot_token, chat_id=chat_id, text=msg)
        if not ok:
            errors.append(err or "send failed")

        for r in items:
            try:
                uniq = (r["uniq"] or "").strip()
                _push_history_add(
                    uniq=uniq,
                    level=(r["sentiment"] or "").strip(),
                    title=r["title"] or "",
                    link=r["link"] or "",
                    message=msg,
                    ok=ok,
                    error=err,
                )
                if ok:
                    pushed += 1
            except Exception as e:
                errors.append(str(e))

        return {"ok": ok, "pushed": pushed, "skipped": skipped, "errors": errors}
    except Exception as e:
        errors.append(str(e))
        return {"ok": False, "pushed": pushed, "skipped": skipped, "errors": errors}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _master_a_push_loop() -> None:
    interval = max(120, min(24 * 3600, int(MASTER_A_PUSH_INTERVAL_SEC)))
    first = True
    while True:
        if first:
            time.sleep(interval)
            first = False
        try:
            s = _news_settings()
            bot_token = (s.get("tg_bot_token") or "").strip()
            chat_id = (s.get("tg_chat_id") or "").strip()
            enabled_mod = _setting_bool(s, "push_master_a_enabled", True)
            if MASTER_A_PUSH_ENABLED and enabled_mod and bot_token and chat_id:
                global _MASTER_A_PUSH_LAST_RUN_TS, _MASTER_A_PUSH_LAST_PUSH, _MASTER_A_PUSH_LAST_ERROR
                _MASTER_A_PUSH_LAST_RUN_TS = int(time.time())
                _MASTER_A_PUSH_LAST_ERROR = ""
                _MASTER_A_PUSH_LAST_PUSH = push_tg_master_a(force=0)
        except Exception as e:
            try:
                _MASTER_A_PUSH_LAST_ERROR = str(e)
            except Exception:
                pass
        time.sleep(interval)


def push_tg_master_b(force: int = 0) -> dict:
    s = _news_settings()
    bot_token = (s.get("tg_bot_token") or "").strip()
    chat_id = (s.get("tg_chat_id") or "").strip()
    if not bot_token or not chat_id:
        return {"ok": False, "pushed": 0, "skipped": 0, "errors": ["未配置 Telegram Bot Token 或 Chat ID"]}

    data = _MASTER_B_ENGINE.matrix()
    items = data.get("items") if isinstance(data, dict) else None
    if not isinstance(items, list):
        return {"ok": False, "pushed": 0, "skipped": 0, "errors": ["invalid master_b matrix"]}

    now_ts = int(time.time())
    bucket = int(now_ts / 300)

    pushed = 0
    skipped = 0
    errors: List[str] = []

    # 只推：已触发（4h trigger）
    candidates: List[dict] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        trig = it.get("trigger") if isinstance(it.get("trigger"), dict) else {}
        st = str(trig.get("state") or "none")
        if st not in ("trigger_long", "trigger_short"):
            continue
        if not it.get("entry") or not it.get("sl") or not it.get("tp1") or not it.get("tp2"):
            continue
        candidates.append(it)

    if not candidates:
        return {"ok": True, "pushed": 0, "skipped": 0, "errors": []}

    ts_txt = datetime.datetime.fromtimestamp(now_ts).strftime("%Y-%m-%d %H:%M")
    header = f"<b>【策略B】触发信号</b>\n时间：{ts_txt}｜触发数：{len(candidates)}"
    lines: List[str] = [header]

    def _fmt(v: Any) -> str:
        try:
            if v is None:
                return "—"
            x = float(v)
            if abs(x) >= 1000:
                return f"{x:,.2f}"
            return f"{x:.6g}"
        except Exception:
            return "—"

    will_log: List[dict] = []
    for it in candidates[:20]:
        try:
            contract = str(it.get("contract") or "").strip()
            side = str(it.get("side") or "none")
            if side not in ("long", "short"):
                continue

            uniq = f"master_b:{contract}:{side}:{bucket}"
            if not force and _master_b_has_uniq(uniq):
                skipped += 1
                continue
            if not force:
                last_ts = _master_b_last_push_ts(contract, side)
                if last_ts is not None and (now_ts - int(last_ts)) < int(MASTER_B_PUSH_COOLDOWN_SEC):
                    skipped += 1
                    continue

            reasons = it.get("reasons") if isinstance(it.get("reasons"), list) else []
            reasons = [str(x) for x in reasons if x]
            rs_txt = " | ".join(reasons[:4])

            entry = it.get("entry")
            sl = it.get("sl")
            tp1 = it.get("tp1")
            tp2 = it.get("tp2")
            atr = it.get("atr_1d") or it.get("atr_4h")

            dir_txt = "做多" if side == "long" else "做空"
            line = (
                f"- {contract}\n"
                f"  [策略类型] 策略B\n"
                f"  [多空方向] {dir_txt}\n"
                f"  [共振理由] {rs_txt or '—'}\n"
                f"  [建议入场价] {_fmt(entry)}\n"
                f"  [止损价] {_fmt(sl)}\n"
                f"  [止盈价] TP1={_fmt(tp1)} TP2={_fmt(tp2)}"
            )
            if atr is not None:
                line += f"\n  ATR={_fmt(atr)}"
            lines.append(line)

            will_log.append(
                {
                    "uniq": uniq,
                    "contract": contract,
                    "side": side,
                    "reasons": reasons,
                    "entry": _safe_float(entry),
                    "sl": _safe_float(sl),
                    "tp1": _safe_float(tp1),
                    "tp2": _safe_float(tp2),
                    "atr": _safe_float(atr),
                }
            )
        except Exception:
            skipped += 1

    if not will_log:
        return {"ok": True, "pushed": 0, "skipped": skipped, "errors": []}

    msg = "\n".join(lines)
    if len(msg) > 3500:
        msg = msg[:3500] + "\n…(truncated)"

    ok, err = _tg_send(bot_token=bot_token, chat_id=chat_id, text=msg, parse_mode="HTML")
    for x in will_log:
        try:
            _master_b_push_history_add(
                uniq=str(x.get("uniq") or ""),
                contract=str(x.get("contract") or ""),
                side=str(x.get("side") or ""),
                reasons=x.get("reasons") if isinstance(x.get("reasons"), list) else [],
                entry=_safe_float(x.get("entry")),
                sl=_safe_float(x.get("sl")),
                tp1=_safe_float(x.get("tp1")),
                tp2=_safe_float(x.get("tp2")),
                atr=_safe_float(x.get("atr")),
                message=msg,
                ok=ok,
                error=err,
            )
        except Exception:
            pass

    if ok:
        pushed = len(will_log)
    else:
        errors.append(err or "send failed")

    return {"ok": ok, "pushed": pushed, "skipped": skipped, "errors": errors}


def _master_b_push_loop() -> None:
    interval = max(120, min(24 * 3600, int(MASTER_B_PUSH_INTERVAL_SEC)))
    first = True
    while True:
        if first:
            time.sleep(interval)
            first = False
        try:
            s = _news_settings()
            bot_token = (s.get("tg_bot_token") or "").strip()
            chat_id = (s.get("tg_chat_id") or "").strip()
            # 复用 telegram 页面里的模块开关（默认 true）
            enabled_mod = _setting_bool(s, "push_master_b_enabled", True)
            if MASTER_B_PUSH_ENABLED and enabled_mod and bot_token and chat_id:
                global _MASTER_B_PUSH_LAST_RUN_TS, _MASTER_B_PUSH_LAST_PUSH, _MASTER_B_PUSH_LAST_ERROR
                _MASTER_B_PUSH_LAST_RUN_TS = int(time.time())
                _MASTER_B_PUSH_LAST_ERROR = ""
                _MASTER_B_PUSH_LAST_PUSH = push_tg_master_b(force=0)
        except Exception as e:
            try:
                _MASTER_B_PUSH_LAST_ERROR = str(e)
            except Exception:
                pass
        time.sleep(interval)


def push_tg_master_a(force: int = 0) -> dict:
    s = _news_settings()
    bot_token = (s.get("tg_bot_token") or "").strip()
    chat_id = (s.get("tg_chat_id") or "").strip()
    if not bot_token or not chat_id:
        return {"ok": False, "pushed": 0, "skipped": 0, "errors": ["未配置 Telegram Bot Token 或 Chat ID"]}

    data = _MASTER_A_ENGINE.matrix()
    items = data.get("items") if isinstance(data, dict) else None
    if not isinstance(items, list):
        return {"ok": False, "pushed": 0, "skipped": 0, "errors": ["invalid master_a matrix"]}

    now_ts = int(time.time())
    bucket = int(now_ts / 300)

    pushed = 0
    skipped = 0
    errors: List[str] = []

    # 只推：已触发（15m breakout）
    candidates: List[dict] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        trig = it.get("trigger") if isinstance(it.get("trigger"), dict) else {}
        st = str(trig.get("state") or "none")
        if st not in ("trigger_long", "trigger_short"):
            continue
        if not it.get("entry") or not it.get("sl") or not it.get("tp1") or not it.get("tp2"):
            continue
        candidates.append(it)

    if not candidates:
        return {"ok": True, "pushed": 0, "skipped": 0, "errors": []}

    ts_txt = datetime.datetime.fromtimestamp(now_ts).strftime("%Y-%m-%d %H:%M")
    header = f"<b>【策略A】触发信号</b>\n时间：{ts_txt}｜触发数：{len(candidates)}"
    lines: List[str] = [header]

    def _fmt(v: Any) -> str:
        try:
            if v is None:
                return "—"
            x = float(v)
            if abs(x) >= 1000:
                return f"{x:,.2f}"
            return f"{x:.6g}"
        except Exception:
            return "—"

    will_log: List[dict] = []
    for it in candidates[:20]:
        try:
            contract = str(it.get("contract") or "").strip()
            side = str(it.get("side") or "none")
            if side not in ("long", "short"):
                continue

            uniq = f"master_a:{contract}:{side}:{bucket}"
            if not force and _master_a_has_uniq(uniq):
                skipped += 1
                continue
            if not force:
                last_ts = _master_a_last_push_ts(contract, side)
                if last_ts is not None and (now_ts - int(last_ts)) < int(MASTER_A_PUSH_COOLDOWN_SEC):
                    skipped += 1
                    continue

            reasons = it.get("reasons") if isinstance(it.get("reasons"), list) else []
            reasons = [str(x) for x in reasons if x]
            rs_txt = " | ".join(reasons[:4])

            entry = it.get("entry")
            sl = it.get("sl")
            tp1 = it.get("tp1")
            tp2 = it.get("tp2")
            atr = it.get("atr_1h")

            dir_txt = "做多" if side == "long" else "做空"
            line = (
                f"- {contract}\n"
                f"  [策略类型] 策略A\n"
                f"  [多空方向] {dir_txt}\n"
                f"  [共振理由] {rs_txt or '—'}\n"
                f"  [建议入场价] {_fmt(entry)}\n"
                f"  [止损价] {_fmt(sl)}\n"
                f"  [止盈价] TP1={_fmt(tp1)} TP2={_fmt(tp2)}"
            )
            lines.append(line)

            will_log.append(
                {
                    "uniq": uniq,
                    "contract": contract,
                    "side": side,
                    "reasons": reasons,
                    "entry": _safe_float(entry),
                    "sl": _safe_float(sl),
                    "tp1": _safe_float(tp1),
                    "tp2": _safe_float(tp2),
                    "atr": _safe_float(atr),
                }
            )
        except Exception:
            skipped += 1

    if not will_log:
        return {"ok": True, "pushed": 0, "skipped": skipped, "errors": []}

    msg = "\n".join(lines)
    if len(msg) > 3500:
        msg = msg[:3500] + "\n…(truncated)"

    ok, err = _tg_send(bot_token=bot_token, chat_id=chat_id, text=msg, parse_mode="HTML")
    for x in will_log:
        try:
            _master_a_push_history_add(
                uniq=str(x.get("uniq") or ""),
                contract=str(x.get("contract") or ""),
                side=str(x.get("side") or ""),
                reasons=x.get("reasons") if isinstance(x.get("reasons"), list) else [],
                entry=_safe_float(x.get("entry")),
                sl=_safe_float(x.get("sl")),
                tp1=_safe_float(x.get("tp1")),
                tp2=_safe_float(x.get("tp2")),
                atr=_safe_float(x.get("atr")),
                message=msg,
                ok=ok,
                error=err,
            )
        except Exception:
            pass

    if ok:
        pushed = len(will_log)
    else:
        errors.append(err or "send failed")

    return {"ok": ok, "pushed": pushed, "skipped": skipped, "errors": errors}


_NEWS_AUTO_THREAD: Optional[threading.Thread] = None
_NEWS_AUTO_THREAD_LOCK = threading.Lock()

_NEWS_AUTO_LAST_RUN_TS: Optional[int] = None
_NEWS_AUTO_LAST_REFRESH: Optional[dict] = None
_NEWS_AUTO_LAST_ANALYZE: Optional[dict] = None
_NEWS_AUTO_LAST_PUSH: Optional[dict] = None
_NEWS_AUTO_LAST_ERROR: str = ""

_MACD_PREALERT_THREAD: Optional[threading.Thread] = None
_MACD_PREALERT_THREAD_LOCK = threading.Lock()
_MACD_PREALERT_LAST_RUN_TS: Optional[int] = None
_MACD_PREALERT_LAST_PUSH: Optional[dict] = None
_MACD_PREALERT_LAST_ERROR: str = ""

_MACD_MONITOR_THREAD: Optional[threading.Thread] = None
_MACD_MONITOR_THREAD_LOCK = threading.Lock()
_MACD_MONITOR_LAST_RUN_TS: Optional[int] = None
_MACD_MONITOR_LAST_PUSH: Optional[dict] = None
_MACD_MONITOR_LAST_ERROR: str = ""

_MASTER_A_PUSH_THREAD: Optional[threading.Thread] = None
_MASTER_A_PUSH_THREAD_LOCK = threading.Lock()
_MASTER_A_PUSH_LAST_RUN_TS: Optional[int] = None
_MASTER_A_PUSH_LAST_PUSH: Optional[dict] = None
_MASTER_A_PUSH_LAST_ERROR: str = ""

_MASTER_B_PUSH_THREAD: Optional[threading.Thread] = None
_MASTER_B_PUSH_THREAD_LOCK = threading.Lock()
_MASTER_B_PUSH_LAST_RUN_TS: Optional[int] = None
_MASTER_B_PUSH_LAST_PUSH: Optional[dict] = None
_MASTER_B_PUSH_LAST_ERROR: str = ""


def _news_auto_loop() -> None:
    interval = max(60, min(3600, int(NEWS_AUTO_PUSH_INTERVAL_SEC)))
    first = True
    while True:
        if first:
            # 避免与 startup 的“重启即推送一次”并发，loop 首次先等待一个 interval
            time.sleep(interval)
            first = False
        try:
            s = _news_settings()
            enabled = _setting_bool(s, "push_enabled", True)
            enabled_mod = _setting_bool(s, "push_news_enabled", True)
            bot_token = (s.get("tg_bot_token") or "").strip()
            chat_id = (s.get("tg_chat_id") or "").strip()
            # 仅在启用推送且 TG 配置齐全时才跑后台抓取/分析/推送，避免无意义后台循环
            if enabled and enabled_mod and bot_token and chat_id:
                global _NEWS_AUTO_LAST_RUN_TS, _NEWS_AUTO_LAST_REFRESH, _NEWS_AUTO_LAST_ANALYZE, _NEWS_AUTO_LAST_PUSH, _NEWS_AUTO_LAST_ERROR
                _NEWS_AUTO_LAST_RUN_TS = int(time.time())
                _NEWS_AUTO_LAST_ERROR = ""

                _NEWS_AUTO_LAST_REFRESH = refresh_news(max_per_feed=NEWS_AUTO_PUSH_MAX_PER_FEED)
                _NEWS_AUTO_LAST_ANALYZE = analyze_pending_news(limit=NEWS_AUTO_PUSH_ANALYZE_LIMIT)
                _NEWS_AUTO_LAST_PUSH = push_telegram_batch_recent(
                    window_sec=NEWS_AUTO_PUSH_WINDOW_SEC,
                    limit=NEWS_AUTO_PUSH_ANALYZE_LIMIT,
                    max_items_in_msg=NEWS_AUTO_PUSH_MAX_ITEMS_IN_MSG,
                )
        except Exception as e:
            try:
                _NEWS_AUTO_LAST_ERROR = str(e)
            except Exception:
                pass
        time.sleep(interval)


def _macd_monitor_push_loop() -> None:
    interval = max(300, min(24 * 3600, int(MACD_MONITOR_PUSH_INTERVAL_SEC)))
    first = True
    while True:
        if first:
            # 避免与 startup 的“重启即推送一次”并发，loop 首次先等待一个 interval
            time.sleep(interval)
            first = False
        try:
            s = _news_settings()
            bot_token = (s.get("tg_bot_token") or "").strip()
            chat_id = (s.get("tg_chat_id") or "").strip()
            enabled_mod = _setting_bool(s, "push_macd_monitor_enabled", True)
            if MACD_MONITOR_PUSH_ENABLED and enabled_mod and bot_token and chat_id:
                global _MACD_MONITOR_LAST_RUN_TS, _MACD_MONITOR_LAST_PUSH, _MACD_MONITOR_LAST_ERROR
                _MACD_MONITOR_LAST_RUN_TS = int(time.time())
                _MACD_MONITOR_LAST_ERROR = ""
                out = push_tg_macd_monitor(
                    topn=MACD_MONITOR_PUSH_TOPN,
                    max_items_in_msg=MACD_MONITOR_PUSH_MAX_ITEMS_IN_MSG,
                )
                if isinstance(out, dict) and out.get("errors") == ["throttled"]:
                    pass
                else:
                    _MACD_MONITOR_LAST_PUSH = out
        except Exception as e:
            try:
                _MACD_MONITOR_LAST_ERROR = str(e)
            except Exception:
                pass
        time.sleep(interval)


def _macd_prealert_push_loop() -> None:
    interval = max(300, min(24 * 3600, int(MACD_PREALERT_PUSH_INTERVAL_SEC)))
    first = True
    while True:
        if first:
            # 避免与 startup 的“重启即推送一次”并发，loop 首次先等待一个 interval
            time.sleep(interval)
            first = False
        try:
            s = _news_settings()
            bot_token = (s.get("tg_bot_token") or "").strip()
            chat_id = (s.get("tg_chat_id") or "").strip()
            enabled_mod = _setting_bool(s, "push_macd_prealert_enabled", True)
            if MACD_PREALERT_PUSH_ENABLED and enabled_mod and bot_token and chat_id:
                global _MACD_PREALERT_LAST_RUN_TS, _MACD_PREALERT_LAST_PUSH, _MACD_PREALERT_LAST_ERROR
                _MACD_PREALERT_LAST_RUN_TS = int(time.time())
                _MACD_PREALERT_LAST_ERROR = ""
                out = push_tg_macd_prealerts(
                    topn=MACD_PREALERT_PUSH_TOPN,
                    max_items_in_msg=MACD_PREALERT_PUSH_MAX_ITEMS_IN_MSG,
                )
                if isinstance(out, dict) and out.get("errors") == ["throttled"]:
                    pass
                else:
                    _MACD_PREALERT_LAST_PUSH = out
        except Exception as e:
            try:
                _MACD_PREALERT_LAST_ERROR = str(e)
            except Exception:
                pass
        time.sleep(interval)


def push_tg_macd_prealerts(topn: int = 50, max_items_in_msg: int = 20, force: int = 0) -> dict:
    """MACD 预警推送：拉取 /api/macd_prealerts 的结果并合并推送。

    关键点：
    - 使用数据库查询上次推送时间实现全局节流（服务重启/多进程也有效）
    - 仅推送 1h timeframe 的“即将金叉/即将死叉”预警
    - Telegram 单条消息长度有限，超过会自动拆分多条
    """
    topn = max(10, min(200, int(topn)))
    max_items_in_msg = max(1, min(200, int(max_items_in_msg)))

    s = _news_settings()
    bot_token = (s.get("tg_bot_token") or "").strip()
    chat_id = (s.get("tg_chat_id") or "").strip()
    if not bot_token or not chat_id:
        return {"ok": False, "pushed": 0, "skipped": 0, "errors": ["未配置 Telegram Bot Token 或 Chat ID"]}

    # 全局节流：即便服务重启/多进程，也保证至少间隔 interval 才推送一次（force=1 可绕过）
    now_ts = int(time.time())
    conn = _db_connect()
    try:
        r = conn.execute(
            "SELECT created_at FROM news_push_history WHERE level='macd_prealert' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        last_ts = None
        try:
            last_ts = int(r["created_at"]) if r and r["created_at"] is not None else None
        except Exception:
            last_ts = None
        if (not int(force)) and last_ts is not None:
            interval = max(300, int(MACD_PREALERT_PUSH_INTERVAL_SEC))
            if (now_ts - last_ts) < (interval - 5):
                return {"ok": True, "pushed": 0, "skipped": 0, "errors": ["throttled"]}
    finally:
        conn.close()

    # 复用现有 macd_prealerts endpoint 的计算结果（only_warn=1 让后端过滤）
    resp = macd_prealerts(limit=topn, only_warn=1, warn_type="all", debug=0)
    payload = {}
    try:
        if isinstance(resp, JSONResponse):
            payload = json.loads((resp.body or b"{}").decode("utf-8", errors="ignore"))
        elif isinstance(resp, dict):
            payload = resp
    except Exception:
        payload = {}

    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list) or not items:
        return {"ok": True, "pushed": 0, "skipped": 0, "errors": []}

    # 仅推送 1h timeframe 的预警条目
    expanded = []
    for it in items:
        try:
            contract = (it.get("contract") or "").strip()
            symbol = (it.get("symbol") or "").strip() or contract
            rank = it.get("market_cap_rank")
            tf = "1h"
            st = it.get("status_1h")
            if st not in ("即将金叉", "即将死叉"):
                continue
            wt = it.get("latest_warn_type") or ""
            wts = it.get("latest_warn_time") or 0
            ratio = it.get("latest_ratio")
            uniq = f"macd_prealert:{contract}:{tf}:{wt}:{wts}"
            expanded.append(
                {
                    "uniq": uniq,
                    "contract": contract,
                    "symbol": symbol,
                    "rank": rank,
                    "tf": tf,
                    "status": st,
                    "warn_type": wt,
                    "warn_time": wts,
                    "ratio": ratio,
                }
            )
        except Exception:
            continue

    if not expanded:
        return {"ok": True, "pushed": 0, "skipped": 0, "errors": []}

    # 不去重：每次推送都是完整列表（受 Telegram 单条消息长度限制会自动拆分多条）
    expanded.sort(key=lambda z: int(z.get("warn_time") or 0), reverse=True)
    selected = expanded[:max_items_in_msg]

    golden = [z for z in selected if z["status"] == "即将金叉"]
    death = [z for z in selected if z["status"] == "即将死叉"]

    def _fmt_ts_local(ts: Any) -> str:
        try:
            _ts = int(ts or 0)
        except Exception:
            _ts = 0
        if _ts <= 0:
            return "—"
        # 兼容毫秒时间戳
        if _ts >= 10**12:
            try:
                _ts = int(_ts / 1000)
            except Exception:
                pass
        try:
            return datetime.datetime.fromtimestamp(_ts).strftime("%Y-%m-%d %H:%M")
        except Exception:
            return str(_ts)
    header = f"【MACD 预警 1h】🟢即将金叉 {len(golden)} / 🔴即将死叉 {len(death)}（TopN={topn}）"
    lines = [header]

    def _fmt_ratio(v: Any) -> str:
        if v is None:
            return "—"
        try:
            return f"{float(v):.4f}"
        except Exception:
            return str(v)

    if golden:
        lines.append("\n🟢⬆️ 即将金叉")
        for z in golden:
            rk = z["rank"] if z["rank"] is not None else "—"
            lines.append(
                f"- #{rk} {z['symbol']} | {_fmt_ts_local(z.get('warn_time'))} | ratio {_fmt_ratio(z.get('ratio'))}"
            )

    if death:
        lines.append("\n🔴⬇️ 即将死叉")
        for z in death:
            rk = z["rank"] if z["rank"] is not None else "—"
            lines.append(
                f"- #{rk} {z['symbol']} | {_fmt_ts_local(z.get('warn_time'))} | ratio {_fmt_ratio(z.get('ratio'))}"
            )

    # Telegram 单条消息限制（保守控制在 3500 以内）
    chunks: List[str] = []
    buf: List[str] = []
    cur_len = 0
    for ln in lines:
        add_len = len(ln) + (1 if buf else 0)
        if buf and (cur_len + add_len) > 3500:
            chunks.append("\n".join(buf))
            buf = [ln]
            cur_len = len(ln)
        else:
            if buf:
                cur_len += 1
            buf.append(ln)
            cur_len += len(ln)
    if buf:
        chunks.append("\n".join(buf))

    errors: List[str] = []
    ok_all = True
    sent_msgs = 0
    batch_ts = int(time.time())
    for idx, msg in enumerate(chunks, start=1):
        ok, err = _tg_send(bot_token=bot_token, chat_id=chat_id, text=msg)
        ok_all = ok_all and ok
        if not ok:
            errors.append(err or "send failed")
        if ok:
            sent_msgs += 1
        # 每条消息写一条 batch 记录，避免 uniq 冲突
        _push_history_add(
            uniq=f"macd_prealert_batch:{batch_ts}:{idx}",
            level="macd_prealert",
            title=f"MACD 预警 1h batch {idx}/{len(chunks)}",
            link="",
            message=msg,
            ok=ok,
            error=err,
        )

    return {"ok": ok_all, "pushed": sent_msgs, "skipped": 0, "errors": errors}


def push_tg_macd_monitor(topn: int = 50, max_items_in_msg: int = 30, force: int = 0) -> dict:
    """MACD 监控推送：推送最近发生的金叉/死叉事件（来自 /api/macd_signals）。

    - 同样使用数据库实现节流
    - 只推送 1h timeframe 的信号（避免 15m 过于频繁）
    - strength 为归一化后的柱子强度百分比（便于跨币种对比）
    """
    topn = max(10, min(200, int(topn)))
    max_items_in_msg = max(1, min(200, int(max_items_in_msg)))

    s = _news_settings()
    bot_token = (s.get("tg_bot_token") or "").strip()
    chat_id = (s.get("tg_chat_id") or "").strip()
    if not bot_token or not chat_id:
        return {"ok": False, "pushed": 0, "skipped": 0, "errors": ["未配置 Telegram Bot Token 或 Chat ID"]}

    now_ts = int(time.time())
    conn = _db_connect()
    try:
        r = conn.execute(
            "SELECT created_at FROM news_push_history WHERE level='macd_monitor' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        last_ts = None
        try:
            last_ts = int(r["created_at"]) if r and r["created_at"] is not None else None
        except Exception:
            last_ts = None
        if (not int(force)) and last_ts is not None:
            interval = max(300, int(MACD_MONITOR_PUSH_INTERVAL_SEC))
            if (now_ts - last_ts) < (interval - 5):
                return {"ok": True, "pushed": 0, "skipped": 0, "errors": ["throttled"]}
    finally:
        conn.close()

    resp = macd_signals(limit=topn, only_signal=1, timeframe="1h")
    payload = {}
    try:
        if isinstance(resp, JSONResponse):
            payload = json.loads((resp.body or b"{}").decode("utf-8", errors="ignore"))
        elif isinstance(resp, dict):
            payload = resp
    except Exception:
        payload = {}

    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list) or not items:
        return {"ok": True, "pushed": 0, "skipped": 0, "errors": []}

    rows: List[dict] = []
    for it in items:
        try:
            tf = (it.get("timeframe") or "").strip() or "—"
            if tf != "1h":
                continue
            st = (it.get("signal_type") or "").strip()  # golden/death
            if st not in ("golden", "death"):
                continue
            rows.append(it)
        except Exception:
            continue

    if not rows:
        return {"ok": True, "pushed": 0, "skipped": 0, "errors": []}

    rows.sort(key=lambda z: int(z.get("signal_time") or 0), reverse=True)
    selected = rows[:max_items_in_msg]

    def _fmt_strength(v: Any) -> str:
        if v is None:
            return "—"
        try:
            return f"{float(v):.4f}"
        except Exception:
            return str(v)

    def _fmt_ts_local(ts: Any) -> str:
        try:
            _ts = int(ts or 0)
        except Exception:
            _ts = 0
        if _ts <= 0:
            return "—"
        # 兼容毫秒时间戳
        if _ts >= 10**12:
            try:
                _ts = int(_ts / 1000)
            except Exception:
                pass
        try:
            return datetime.datetime.fromtimestamp(_ts).strftime("%Y-%m-%d %H:%M")
        except Exception:
            return str(_ts)

    golden = [z for z in selected if (z.get("signal_type") or "").strip() == "golden"]
    death = [z for z in selected if (z.get("signal_type") or "").strip() == "death"]

    header = f"【MACD 监控 1h】🟢金叉 {len(golden)} / 🔴死叉 {len(death)}（TopN={topn}）"
    lines: List[str] = [header]

    if golden:
        lines.append("\n🟢⬆️ 金叉")
        for z in golden:
            rk = z.get("market_cap_rank")
            rk = rk if rk is not None else "—"
            sym = (z.get("symbol") or "").strip() or (z.get("contract") or "").strip() or "—"
            ts = _fmt_ts_local(z.get("signal_time"))
            strength = _fmt_strength(z.get("signal_strength"))
            lines.append(f"- #{rk} {sym} | {ts} | strength {strength}")

    if death:
        lines.append("\n🔴⬇️ 死叉")
        for z in death:
            rk = z.get("market_cap_rank")
            rk = rk if rk is not None else "—"
            sym = (z.get("symbol") or "").strip() or (z.get("contract") or "").strip() or "—"
            ts = _fmt_ts_local(z.get("signal_time"))
            strength = _fmt_strength(z.get("signal_strength"))
            lines.append(f"- #{rk} {sym} | {ts} | strength {strength}")

    chunks: List[str] = []
    buf: List[str] = []
    cur_len = 0
    for ln in lines:
        add_len = len(ln) + (1 if buf else 0)
        if buf and (cur_len + add_len) > 3500:
            chunks.append("\n".join(buf))
            buf = [ln]
            cur_len = len(ln)
        else:
            if buf:
                cur_len += 1
            buf.append(ln)
            cur_len += len(ln)
    if buf:
        chunks.append("\n".join(buf))

    errors: List[str] = []
    ok_all = True
    sent_msgs = 0
    batch_ts = int(time.time())
    for idx, msg in enumerate(chunks, start=1):
        ok, err = _tg_send(bot_token=bot_token, chat_id=chat_id, text=msg)
        ok_all = ok_all and ok
        if not ok:
            errors.append(err or "send failed")
        if ok:
            sent_msgs += 1
        _push_history_add(
            uniq=f"macd_monitor_batch:{batch_ts}:{idx}",
            level="macd_monitor",
            title=f"MACD 监控信号 batch {idx}/{len(chunks)}",
            link="",
            message=msg,
            ok=ok,
            error=err,
        )

    return {"ok": ok_all, "pushed": sent_msgs, "skipped": 0, "errors": errors}


def _news_settings() -> dict:
    s = _settings_get("news_settings", default={})
    return s if isinstance(s, dict) else {}


def _setting_bool(settings: dict, key: str, default: bool = True) -> bool:
    if not isinstance(settings, dict):
        return bool(default)
    if key not in settings:
        return bool(default)
    v = settings.get(key)
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(int(v))
    if isinstance(v, str):
        vv = v.strip().lower()
        if vv in ("1", "true", "yes", "on"):
            return True
        if vv in ("0", "false", "no", "off"):
            return False
    return bool(default)


def _tg_send(bot_token: str, chat_id: str, text: str, parse_mode: Optional[str] = None) -> Tuple[bool, str]:
    bot_token = (bot_token or "").strip()
    chat_id = (chat_id or "").strip()
    if not bot_token or not chat_id:
        return False, "missing bot_token/chat_id"

    base = TELEGRAM_API_BASE.rstrip("/")
    url = f"{base}/bot{bot_token}/sendMessage"
    try:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        r = HTTP.post(
            url,
            json=payload,
            timeout=(TELEGRAM_CONNECT_TIMEOUT, TELEGRAM_READ_TIMEOUT),
        )
        if r.status_code != 200:
            return False, f"http {r.status_code}: {r.text[:300]}"
        data = {}
        try:
            data = r.json()
        except Exception:
            data = {}
        if isinstance(data, dict) and data.get("ok") is True:
            return True, ""
        return False, str(data)[:300]
    except Exception as e:
        return False, str(e)


def _push_history_add(
    uniq: str,
    level: str,
    title: str,
    link: str,
    message: str,
    ok: bool,
    error: str = "",
) -> None:
    conn = _db_connect()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO news_push_history(created_at, uniq, level, title, link, message, ok, error)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (int(time.time()), uniq or None, level, title, link, message, 1 if ok else 0, error),
        )
        conn.commit()
    finally:
        conn.close()


def push_telegram_for_news(limit: int = 50) -> dict:
    """根据设置把满足条件的新闻推送到 Telegram（带去重）。"""
    s = _news_settings()
    enabled = _setting_bool(s, "push_enabled", True)
    threshold = s.get("push_threshold")
    try:
        threshold_f = float(threshold) if threshold is not None and threshold != "" else 0.75
    except Exception:
        threshold_f = 0.75
    bot_token = (s.get("tg_bot_token") or "").strip()
    chat_id = (s.get("tg_chat_id") or "").strip()

    if not enabled:
        return {"ok": True, "pushed": 0, "skipped": 0, "errors": []}
    if not bot_token or not chat_id:
        return {"ok": False, "pushed": 0, "skipped": 0, "errors": ["未配置 Telegram Bot Token 或 Chat ID"]}

    limit = max(1, min(200, int(limit)))
    conn = _db_connect()
    pushed = 0
    skipped = 0
    errors: List[str] = []
    try:
        rows = conn.execute(
            """
            SELECT uniq, title, link, coins, sentiment, strength, reason
            FROM news_items
            WHERE sentiment IN ('bullish','bearish')
              AND strength IS NOT NULL
              AND strength >= ?
            ORDER BY COALESCE(published_at, created_at) DESC
            LIMIT ?
            """,
            (threshold_f, limit),
        ).fetchall()

        for r in rows:
            try:
                uniq = (r["uniq"] or "").strip()
                if not uniq:
                    skipped += 1
                    continue

                already = conn.execute(
                    "SELECT 1 FROM news_push_history WHERE uniq=? LIMIT 1",
                    (uniq,),
                ).fetchone()
                if already:
                    skipped += 1
                    continue

                title = r["title"] or ""
                link = r["link"] or ""
                coins = (r["coins"] or "").strip()
                sentiment = (r["sentiment"] or "").strip()
                strength = r["strength"]
                reason = (r["reason"] or "").strip()

                sent_cn = "利多" if sentiment == "bullish" else "利空"
                coins_cn = coins if coins else "—"
                msg = (
                    f"【新闻多空哨兵】{sent_cn} 强度 {float(strength):.2f}\n"
                    f"币种: {coins_cn}\n"
                    f"原因: {reason or '—'}\n"
                    f"{title}\n"
                    f"{link}"
                )

                ok, err = _tg_send(bot_token=bot_token, chat_id=chat_id, text=msg)
                _push_history_add(
                    uniq=uniq,
                    level=sentiment,
                    title=title,
                    link=link,
                    message=msg,
                    ok=ok,
                    error=err,
                )
                if ok:
                    pushed += 1
                else:
                    errors.append(err or "send failed")
            except Exception as e:
                errors.append(str(e))

        return {"ok": True, "pushed": pushed, "skipped": skipped, "errors": errors}
    finally:
        conn.close()


def _settings_get(key: str, default: Any = None) -> Any:
    conn = _db_connect()
    try:
        row = conn.execute("SELECT v FROM news_settings WHERE k=?", (key,)).fetchone()
        if not row:
            return default
        v = row["v"]
        if v is None:
            return default
        try:
            return json.loads(v)
        except Exception:
            return v
    finally:
        conn.close()


def _settings_set(key: str, value: Any) -> None:
    conn = _db_connect()
    try:
        if isinstance(value, (dict, list, bool, int, float)) or value is None:
            v = json.dumps(value, ensure_ascii=False)
        else:
            v = str(value)
        conn.execute(
            "INSERT INTO news_settings(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (key, v),
        )
        conn.commit()
    finally:
        conn.close()


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _rss_feeds_list() -> List[str]:
    feeds: List[str] = []
    if COINTELEGRAPH_FEED_URL:
        feeds.append(COINTELEGRAPH_FEED_URL)
    if COINDESK_FEED_URL:
        feeds.append(COINDESK_FEED_URL)
    if THEBLOCK_FEED_URL:
        feeds.append(THEBLOCK_FEED_URL)
    return feeds


def _entry_published_ts(entry: Any) -> Optional[int]:
    for k in ("published_parsed", "updated_parsed"):
        st = getattr(entry, k, None) or (entry.get(k) if isinstance(entry, dict) else None)
        if st:
            try:
                return int(time.mktime(st))
            except Exception:
                pass
    return None


def _entry_tags(entry: Any) -> str:
    tags = []
    tlist = getattr(entry, "tags", None) or (entry.get("tags") if isinstance(entry, dict) else None)
    if tlist and isinstance(tlist, list):
        for t in tlist:
            term = None
            if isinstance(t, dict):
                term = t.get("term") or t.get("label")
            else:
                term = getattr(t, "term", None) or getattr(t, "label", None)
            term = (term or "").strip()
            if term:
                tags.append(term)
    seen = set()
    out = []
    for t in tags:
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return ",".join(out)


def _news_uniq(source: str, link: str, title: str, published_at: Optional[int]) -> str:
    # 去重关键：link 必须稳定。Google News RSS 的 link 往往是跳转链接，且每次可能携带不同参数。
    # 这里做规范化：
    # - 去掉 hash
    # - 对 Google News 尝试提取真实 url 参数
    # - 统一去掉常见跟踪参数（utm_* 等）
    norm_link = _normalize_news_link(link)
    src = (source or "").strip().lower()
    base = f"{src}|{norm_link}|{(title or '').strip()}|{published_at or 0}"
    return hashlib.sha256(base.encode("utf-8", errors="ignore")).hexdigest()


def _normalize_news_link(link: str) -> str:
    link = (link or "").strip()
    if not link:
        return ""
    try:
        u = urlparse(link)
        host = (u.netloc or "").lower()
        qs = parse_qs(u.query or "")

        # Google News RSS: 可能存在 ?url=<real> 或 ?q=<real>
        if host.endswith("news.google.com"):
            cand = None
            if "url" in qs and qs["url"]:
                cand = qs["url"][0]
            elif "q" in qs and qs["q"]:
                cand = qs["q"][0]
            if cand:
                cand = unquote(cand)
                return _normalize_news_link(cand)

        # 清理常见跟踪参数
        drop_prefix = ("utm_",)
        drop_keys = {"ref", "ref_src", "source", "spm", "from"}
        kept = []
        for k in sorted(qs.keys()):
            kl = k.lower()
            if any(kl.startswith(p) for p in drop_prefix) or kl in drop_keys:
                continue
            for v in qs.get(k, [])[:1]:
                kept.append(f"{k}={v}")
        query = "&".join(kept)
        path = u.path or ""
        # 不保留 fragment
        return f"{u.scheme}://{u.netloc}{path}" + (f"?{query}" if query else "")
    except Exception:
        return link


_COIN_STOPWORDS = {
    "USD",
    "USDT",
    "USDC",
    "EUR",
    "ETF",
    "SEC",
    "FED",
    "CPI",
    "GDP",
    "CEO",
    "CFO",
    "ATH",
    "ATL",
    "DEX",
    "NFT",
    "L2",
    "TVL",
    "AI",
    "API",
    "IPO",
    "FBI",
    "DOJ",
}


_COIN_NAME_MAP = {
    "bitcoin": "BTC",
    "btc": "BTC",
    "ethereum": "ETH",
    "eth": "ETH",
    "solana": "SOL",
    "sol": "SOL",
    "ripple": "XRP",
    "xrp": "XRP",
    "dogecoin": "DOGE",
    "doge": "DOGE",
    "binance": "BNB",
    "bnb": "BNB",
    "cardano": "ADA",
    "ada": "ADA",
    "ton": "TON",
    "tron": "TRX",
    "trx": "TRX",
    "polkadot": "DOT",
    "dot": "DOT",
    "avalanche": "AVAX",
    "avax": "AVAX",
    "chainlink": "LINK",
    "link": "LINK",
    "litecoin": "LTC",
    "ltc": "LTC",
}


_COIN_CN_NAME_MAP = {
    "比特币": "BTC",
    "以太坊": "ETH",
    "索拉纳": "SOL",
    "狗狗币": "DOGE",
    "瑞波": "XRP",
    "瑞波币": "XRP",
    "币安币": "BNB",
    "艾达": "ADA",
    "波卡": "DOT",
    "雪崩": "AVAX",
    "链链接": "LINK",
    "莱特币": "LTC",
    "波场": "TRX",
    "特朗普": "TRUMP",
}


def extract_coins(title: str, summary: str, tags: str = "") -> str:
    text = f"{title}\n{summary}\n{tags}"
    if not text.strip():
        return ""

    found: List[str] = []

    # $BTC 形式
    for m in re.findall(r"\$([A-Z]{2,10})", text):
        sym = m.strip().upper()
        if sym and sym not in _COIN_STOPWORDS:
            found.append(sym)

    # 直接出现 BTC/ETH 形式（全大写单词）
    for m in re.findall(r"\b([A-Z]{2,10})\b", text):
        sym = m.strip().upper()
        if sym and sym not in _COIN_STOPWORDS:
            found.append(sym)

    # 英文币名映射
    lower = text.lower()
    for k, v in _COIN_NAME_MAP.items():
        if re.search(rf"\b{re.escape(k)}\b", lower):
            if v and v not in _COIN_STOPWORDS:
                found.append(v)

    # 常见中文币名映射
    for k, v in _COIN_CN_NAME_MAP.items():
        if k in text:
            if v and v not in _COIN_STOPWORDS:
                found.append(v)

    # 白名单符号大小写不敏感命中（避免全量英文单词误报）
    whitelist = set(_COIN_NAME_MAP.values()) | set(_COIN_CN_NAME_MAP.values())
    for sym in whitelist:
        if not sym or sym in _COIN_STOPWORDS:
            continue
        if re.search(rf"\b{re.escape(sym)}\b", text, flags=re.IGNORECASE):
            found.append(sym)

    # 去重保序
    out: List[str] = []
    seen = set()
    for s in found:
        if s in seen:
            continue
        seen.add(s)
        out.append(s)

    # 控制长度，避免极端噪声
    return ",".join(out[:10])


def _extract_first_json_object(text: str) -> Optional[dict]:
    if not text:
        return None
    s = text.strip()
    # 直接尝试
    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    # 找第一个 {...}
    start = s.find("{")
    if start < 0:
        return None
    depth = 0
    for i in range(start, len(s)):
        ch = s[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                frag = s[start : i + 1]
                try:
                    obj = json.loads(frag)
                    if isinstance(obj, dict):
                        return obj
                except Exception:
                    return None
    return None


def _normalize_sentiment(s: Any) -> Optional[str]:
    if s is None:
        return None
    t = str(s).strip().lower()
    if t in ("bullish", "bull", "long", "positive"):
        return "bullish"
    if t in ("bearish", "bear", "short", "negative"):
        return "bearish"
    if t in ("neutral", "none", "mixed"):
        return "neutral"
    return None


def _normalize_strength(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        x = float(v)
    except Exception:
        return None
    if x < 0:
        x = 0.0
    # 允许用户返回 0-100，自动归一到 0-1
    if x > 1.0 and x <= 100.0:
        x = x / 100.0
    if x > 1.0:
        x = 1.0
    return float(x)


def _normalize_reason(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        s = str(v).strip()
    except Exception:
        return None
    if not s:
        return None
    s = re.sub(r"\s+", " ", s)
    if len(s) > 120:
        s = s[:120].rstrip() + "…"
    return s


def _rule_sentiment(title: str, summary: str) -> Tuple[str, float, str]:
    text = f"{title}\n{summary}".lower()
    bull_words = [
        "surge",
        "soar",
        "rally",
        "breakout",
        "bull",
        "record high",
        "etf approval",
        "adoption",
        "partnership",
        "lists",
        "listing",
        "funding",
        "buy",
        "accumulate",
        "上涨",
        "拉升",
        "突破",
        "看涨",
        "利好",
        "增持",
        "上架",
    ]
    bear_words = [
        "dump",
        "plunge",
        "crash",
        "hack",
        "exploit",
        "lawsuit",
        "ban",
        "bear",
        "liquidation",
        "outflow",
        "sell",
        "down",
        "跌",
        "暴跌",
        "下跌",
        "看跌",
        "利空",
        "被盗",
        "漏洞",
        "监管",
        "起诉",
        "清算",
    ]

    score = 0
    hit_bull: List[str] = []
    hit_bear: List[str] = []
    for w in bull_words:
        if w in text:
            score += 1
            hit_bull.append(w)
    for w in bear_words:
        if w in text:
            score -= 1
            hit_bear.append(w)

    if score > 0:
        top = ",".join(hit_bull[:3])
        return "bullish", min(1.0, 0.35 + 0.10 * score), (f"关键词:{top}" if top else "规则:利多关键词")
    if score < 0:
        top = ",".join(hit_bear[:3])
        return "bearish", min(1.0, 0.35 + 0.10 * abs(score)), (f"关键词:{top}" if top else "规则:利空关键词")
    return "neutral", 0.30, "关键词不足/偏中性"


def _llm_prompt(title: str, summary: str, source: str, tags: str) -> str:
    return (
        "你是加密货币新闻情绪分析器。\n"
        "请根据新闻标题、摘要、来源、标签判断对对应币种/市场的影响倾向，并输出严格 JSON。\n"
        "只允许输出一个 JSON 对象，不要输出任何解释。\n\n"
        "输出 JSON Schema:\n"
        "{\n"
        '  "sentiment": "bullish|bearish|neutral",\n'
        '  "strength": 0.0-1.0,\n'
        '  "reason": "不超过60字的简要原因"\n'
        "}\n\n"
        f"标题: {title}\n"
        f"摘要: {summary}\n"
        f"来源: {source}\n"
        f"标签: {tags}\n"
    )


def _analyze_with_openai(prompt: str, timeout_sec: int = 25) -> Optional[dict]:
    if not OPENAI_API_KEY:
        return None
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    body = {
        "model": OPENAI_MODEL,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": "你只输出 JSON，不输出其他文本。"},
            {"role": "user", "content": prompt},
        ],
    }
    r = requests.post(url, headers=headers, json=body, timeout=timeout_sec)
    r.raise_for_status()
    data = r.json()
    content = (
        (((data or {}).get("choices") or [{}])[0].get("message") or {}).get("content")
        if isinstance(data, dict)
        else None
    )
    obj = _extract_first_json_object(content or "")
    if not obj:
        return None
    return obj


def _analyze_with_anthropic(prompt: str, timeout_sec: int = 25) -> Optional[dict]:
    if not ANTHROPIC_API_KEY:
        return None
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    body = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": 256,
        "temperature": 0,
        "messages": [{"role": "user", "content": prompt}],
    }
    r = requests.post(url, headers=headers, json=body, timeout=timeout_sec)
    r.raise_for_status()
    data = r.json()
    parts = (data or {}).get("content") if isinstance(data, dict) else None
    text = None
    if isinstance(parts, list) and parts:
        # 取第一个 text block
        for p in parts:
            if isinstance(p, dict) and p.get("type") == "text":
                text = p.get("text")
                break
    obj = _extract_first_json_object(text or "")
    if not obj:
        return None
    return obj


def analyze_news_item(title: str, summary: str, source: str, tags: str) -> Tuple[str, float, str, str]:
    prompt = _llm_prompt(title=title, summary=summary, source=source, tags=tags)
    provider = "rules"
    obj = None

    # 优先 OpenAI，其次 Anthropic，最后规则
    try:
        obj = _analyze_with_openai(prompt)
        if obj:
            provider = "openai"
    except Exception:
        obj = None
    if not obj:
        try:
            obj = _analyze_with_anthropic(prompt)
            if obj:
                provider = "anthropic"
        except Exception:
            obj = None

    if obj and isinstance(obj, dict):
        sent = _normalize_sentiment(obj.get("sentiment"))
        strength = _normalize_strength(obj.get("strength"))
        reason = _normalize_reason(obj.get("reason"))
        if sent and strength is not None:
            return sent, float(strength), (reason or ""), provider

    sent2, str2, reason2 = _rule_sentiment(title=title, summary=summary)
    return sent2, float(str2), reason2, provider


def _translate_prompt(title: str, summary: str) -> str:
    return (
        "你是专业翻译助手。请把下面的加密货币新闻标题和摘要翻译为简体中文。\n"
        "要求：保持专业术语准确；不要添加不存在的信息；输出严格 JSON；不要输出任何解释。\n\n"
        "输出 JSON Schema:\n"
        "{\n"
        '  "title_zh": "...",\n'
        '  "summary_zh": "..."\n'
        "}\n\n"
        f"title: {title}\n"
        f"summary: {summary}\n"
    )


def translate_to_zh(title: str, summary: str) -> Tuple[Optional[str], Optional[str], str]:
    prompt = _translate_prompt(title=title, summary=summary)
    provider = "none"
    obj = None
    try:
        obj = _analyze_with_openai(prompt)
        if obj:
            provider = "openai"
    except Exception:
        obj = None

    if not obj:
        try:
            obj = _analyze_with_anthropic(prompt)
            if obj:
                provider = "anthropic"
        except Exception:
            obj = None

    if not obj or not isinstance(obj, dict):
        return None, None, provider

    tzh = obj.get("title_zh")
    szh = obj.get("summary_zh")
    tzh = str(tzh).strip() if tzh is not None else None
    szh = str(szh).strip() if szh is not None else None
    if not tzh:
        tzh = None
    if not szh:
        szh = None
    return tzh, szh, provider


def translate_pending_news(limit: int = 20) -> dict:
    limit = max(1, min(200, int(limit)))
    if not OPENAI_API_KEY and not ANTHROPIC_API_KEY:
        return {"ok": False, "translated": 0, "errors": ["未配置 OPENAI_API_KEY 或 ANTHROPIC_API_KEY，无法自动翻译"]}

    conn = _db_connect()
    translated = 0
    errors: List[str] = []
    now_ts = int(time.time())
    try:
        rows = conn.execute(
            """
            SELECT id, title, summary
            FROM news_items
            WHERE (title_zh IS NULL OR title_zh = '')
            ORDER BY COALESCE(published_at, created_at) DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        for r in rows:
            try:
                rid = int(r["id"])
                title = r["title"] or ""
                summary = r["summary"] or ""
                tzh, szh, provider = translate_to_zh(title=title, summary=summary)
                if not tzh and not szh:
                    continue
                conn.execute(
                    "UPDATE news_items SET title_zh=?, summary_zh=?, translated_at=? WHERE id=?",
                    (tzh, szh, now_ts, rid),
                )
                translated += 1
            except Exception as e:
                errors.append(str(e))

        conn.commit()
        return {"ok": True, "translated": translated, "errors": errors}
    finally:
        conn.close()


def analyze_pending_news(limit: int = 20, force: int = 0) -> dict:
    limit = max(1, min(200, int(limit)))
    force = 1 if int(force or 0) else 0
    conn = _db_connect()
    analyzed = 0
    errors: List[str] = []
    try:
        if force:
            rows = conn.execute(
                """
                SELECT id, source, title, link, summary, tags
                FROM news_items
                ORDER BY COALESCE(published_at, created_at) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, source, title, link, summary, tags
                FROM news_items
                WHERE sentiment IS NULL OR sentiment = '' OR reason IS NULL OR reason = ''
                ORDER BY COALESCE(published_at, created_at) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        for r in rows:
            try:
                rid = int(r["id"])
                source = r["source"] or ""
                title = r["title"] or ""
                summary = r["summary"] or ""
                tags = r["tags"] or ""

                sent, strength, reason, provider = analyze_news_item(
                    title=title,
                    summary=summary,
                    source=source,
                    tags=tags,
                )
                conn.execute(
                    "UPDATE news_items SET sentiment=?, strength=?, reason=? WHERE id=?",
                    (sent, float(strength), (reason or ""), rid),
                )
                analyzed += 1
            except Exception as e:
                errors.append(str(e))

        conn.commit()
        return {"ok": True, "analyzed": analyzed, "errors": errors}
    finally:
        conn.close()


def refresh_news(max_per_feed: int = 30, timeout_sec: int = 12) -> dict:
    max_per_feed = max(1, min(200, int(max_per_feed)))
    timeout_sec = max(3, min(60, int(timeout_sec)))
    feeds = _rss_feeds_list()
    errors: List[str] = []
    if not feeds:
        return {"feeds": 0, "inserted": 0, "skipped": 0, "errors": ["未配置 CoinDesk / Cointelegraph / The Block 来源"]}

    inserted = 0
    skipped = 0
    now_ts = int(time.time())

    all_rows: List[Tuple[str, str, str, str, Optional[int], str, str, str, int]] = []

    def _fetch_one(feed_url: str) -> Tuple[List[Tuple[str, str, str, str, Optional[int], str, str, str, int]], Optional[str]]:
        try:
            # 更快失败：连接超时更短，读取超时按 timeout_sec
            r = HTTP.get(
                feed_url,
                timeout=(4, timeout_sec),
                headers={"User-Agent": NEWS_HTTP_USER_AGENT or "python-requests"},
                verify=NEWS_HTTP_VERIFY,
            )
            r.raise_for_status()
            parsed = feedparser.parse(r.text)

            source = None
            if parsed and getattr(parsed, "feed", None):
                source = (getattr(parsed.feed, "title", None) or "").strip() or None
            source = source or feed_url

            out_rows: List[Tuple[str, str, str, str, Optional[int], str, str, str, int]] = []
            entries = getattr(parsed, "entries", None) or []
            for entry in entries[:max_per_feed]:
                title = (getattr(entry, "title", None) or entry.get("title") or "").strip()
                link = (getattr(entry, "link", None) or entry.get("link") or "").strip()
                if not title and not link:
                    continue
                published_at = _entry_published_ts(entry)
                summary = (getattr(entry, "summary", None) or entry.get("summary") or "").strip()
                tags = _entry_tags(entry)
                coins = extract_coins(title=title, summary=summary, tags=tags)
                uniq = _news_uniq(source, link, title, published_at)
                out_rows.append((uniq, source, title, link, published_at, summary, tags, coins, now_ts))
            return out_rows, None
        except Exception as e:
            return [], f"{feed_url}: {e}"

    # 2) CoinDesk / Cointelegraph（补充来源，仅这两个）
    if feeds:
        if feedparser is None:
            errors.append("缺少依赖 feedparser，请先安装 requirements.txt 后再抓取 CoinDesk/Cointelegraph")
        else:
            max_workers = min(6, max(1, len(feeds)))
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futs = [ex.submit(_fetch_one, u) for u in feeds]
                for f in as_completed(futs):
                    rows, err = f.result()
                    if err:
                        errors.append(err)
                    if rows:
                        all_rows.extend(rows)

    conn = _db_connect()
    try:
        before = conn.total_changes
        # INSERT OR IGNORE 利用 uniq UNIQUE 约束去重，批量入库快很多
        conn.executemany(
            """
            INSERT OR IGNORE INTO news_items(uniq, source, title, link, published_at, summary, tags, coins, sentiment, strength, created_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)
            """,
            all_rows,
        )
        conn.commit()
        inserted = int(conn.total_changes - before)
        skipped = max(0, len(all_rows) - inserted)
    finally:
        conn.close()

    return {"feeds": len(feeds), "inserted": inserted, "skipped": skipped, "errors": errors}


def _mean_abs(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return sum(abs(x) for x in values) / float(len(values))


def detect_prealert(
    dif: List[float],
    dea: List[float],
    hist: List[float],
    lookback: int = 2,
    ratio_threshold: float = 0.75,
) -> Optional[dict]:
    # 返回预警：{"type": "pre_golden"|"pre_death", "distance": float, "ratio": float, "bar_dir": "up"|"down"}
    if len(dif) < 25 or len(dea) < 25:
        return None

    # 避免已发生交叉
    if dif[-2] <= dea[-2] and dif[-1] > dea[-1]:
        return None
    if dif[-2] >= dea[-2] and dif[-1] < dea[-1]:
        return None

    gap = dif[-1] - dea[-1]
    distance = abs(gap)

    gaps20 = [(dif[i] - dea[i]) for i in range(len(dif) - 20, len(dif))]
    base = _mean_abs(gaps20)
    if base is None or base == 0:
        return None
    ratio = distance / base
    if ratio > ratio_threshold:
        return None

    # 斜率
    dif_slope = dif[-1] - dif[-2]
    dea_slope = dea[-1] - dea[-2]

    # 连续收敛：最近 lookback 根 gap 的绝对值整体在变小（更稳，减少震荡误报）
    try:
        lb = max(2, int(lookback))
        if len(dif) >= lb + 1 and len(dea) >= lb + 1:
            gaps = [(dif[i] - dea[i]) for i in range(len(dif) - lb, len(dif))]
            abs_gaps = [abs(x) for x in gaps]
            # 要求多数步在收敛（允许一次小反复）
            improves = 0
            for i in range(1, len(abs_gaps)):
                if abs_gaps[i] <= abs_gaps[i - 1]:
                    improves += 1
            if improves < max(1, len(abs_gaps) - 2):
                return None
    except Exception:
        pass

    # 柱子改善：朝 0 方向靠近（放宽，允许没有 hist）
    hist_improving = None
    if hist and len(hist) >= 2:
        hist_improving = abs(hist[-1]) <= abs(hist[-2])

    bar_dir = None
    if hist and len(hist) >= 2:
        bar_dir = "up" if hist[-1] >= hist[-2] else "down"

    # 即将金叉：dif < dea 且 gap 在缩小（允许 DEA 有噪声，只看相对斜率）
    # gap = dif - dea，gap<0 时要向 0 走：dif_slope - dea_slope > 0
    if gap < 0 and (dif_slope - dea_slope) > 0:
        if hist_improving is False:
            return None
        return {
            "type": "pre_golden",
            "distance": float(distance),
            "ratio": float(ratio),
            "bar_dir": bar_dir,
        }

    # 即将死叉：dif > dea 且 gap 在缩小（向 0 走）：dif_slope - dea_slope < 0
    if gap > 0 and (dif_slope - dea_slope) < 0:
        if hist_improving is False:
            return None
        return {
            "type": "pre_death",
            "distance": float(distance),
            "ratio": float(ratio),
            "bar_dir": bar_dir,
        }

    return None

CONTRACTS_5 = [
    "TRUMP_USDT",
    "BTC_USDT",
    "ETH_USDT",
    "DOGE_USDT",
    "PEPE_USDT",
]

TIMEFRAMES = {
    "5m": "5m",
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}

MACD_TIMEFRAMES = {
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}


@dataclass
class Row:
    contract: str
    timeframe: str
    last_price: Optional[float]
    price_change_pct: Optional[float]
    oi_change_pct: Optional[float]
    score: Optional[float]
    market_signal: Optional[str]
    updated_at: int


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _pct_change(cur: Optional[float], prev: Optional[float]) -> Optional[float]:
    if cur is None or prev is None or prev == 0:
        return None
    return (cur - prev) / prev * 100.0


def _ema(values: List[float], span: int) -> List[float]:
    # 标准 EMA: alpha = 2/(span+1)
    if not values:
        return []
    alpha = 2.0 / (span + 1.0)
    out = [values[0]]
    for v in values[1:]:
        out.append(alpha * v + (1 - alpha) * out[-1])
    return out


def _sma(values: List[float], window: int) -> List[Optional[float]]:
    if not values or window <= 0:
        return []
    out: List[Optional[float]] = [None] * len(values)
    s = 0.0
    for i, v in enumerate(values):
        s += float(v)
        if i >= window:
            s -= float(values[i - window])
        if i >= window - 1:
            out[i] = s / float(window)
    return out


def _atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> List[Optional[float]]:
    n = min(len(highs), len(lows), len(closes))
    if n <= period:
        return []
    trs: List[float] = []
    for i in range(n):
        h = float(highs[i])
        l = float(lows[i])
        if i == 0:
            trs.append(h - l)
            continue
        pc = float(closes[i - 1])
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    out: List[Optional[float]] = [None] * n
    atr0 = sum(trs[1 : period + 1]) / float(period)
    out[period] = atr0
    prev = atr0
    for i in range(period + 1, n):
        prev = (prev * (period - 1) + trs[i]) / float(period)
        out[i] = prev
    return out


def _adx(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> List[Optional[float]]:
    n = min(len(highs), len(lows), len(closes))
    if n <= period + 2:
        return []
    tr: List[float] = [0.0] * n
    plus_dm: List[float] = [0.0] * n
    minus_dm: List[float] = [0.0] * n
    for i in range(1, n):
        up_move = float(highs[i]) - float(highs[i - 1])
        down_move = float(lows[i - 1]) - float(lows[i])
        plus_dm[i] = up_move if (up_move > down_move and up_move > 0) else 0.0
        minus_dm[i] = down_move if (down_move > up_move and down_move > 0) else 0.0
        h = float(highs[i])
        l = float(lows[i])
        pc = float(closes[i - 1])
        tr[i] = max(h - l, abs(h - pc), abs(l - pc))

    tr14: List[Optional[float]] = [None] * n
    pdm14: List[Optional[float]] = [None] * n
    mdm14: List[Optional[float]] = [None] * n
    tr_sum = sum(tr[1 : period + 1])
    pdm_sum = sum(plus_dm[1 : period + 1])
    mdm_sum = sum(minus_dm[1 : period + 1])
    tr14[period] = tr_sum
    pdm14[period] = pdm_sum
    mdm14[period] = mdm_sum
    for i in range(period + 1, n):
        tr_sum = tr_sum - (tr_sum / float(period)) + tr[i]
        pdm_sum = pdm_sum - (pdm_sum / float(period)) + plus_dm[i]
        mdm_sum = mdm_sum - (mdm_sum / float(period)) + minus_dm[i]
        tr14[i] = tr_sum
        pdm14[i] = pdm_sum
        mdm14[i] = mdm_sum

    pdi: List[Optional[float]] = [None] * n
    mdi: List[Optional[float]] = [None] * n
    dx: List[Optional[float]] = [None] * n
    for i in range(period, n):
        t = tr14[i]
        if t is None or t == 0:
            continue
        p = pdm14[i] or 0.0
        m = mdm14[i] or 0.0
        pdi[i] = 100.0 * (p / float(t))
        mdi[i] = 100.0 * (m / float(t))
        den = (pdi[i] or 0.0) + (mdi[i] or 0.0)
        if den == 0:
            continue
        dx[i] = 100.0 * abs((pdi[i] or 0.0) - (mdi[i] or 0.0)) / den

    out: List[Optional[float]] = [None] * n
    start = period * 2
    if start >= n:
        return out
    init_vals = [x for x in dx[period : start + 1] if isinstance(x, (int, float))]
    if len(init_vals) < period:
        return out
    adx0 = sum(init_vals[-period:]) / float(period)
    out[start] = adx0
    prev = adx0
    for i in range(start + 1, n):
        if dx[i] is None:
            continue
        prev = (prev * (period - 1) + float(dx[i])) / float(period)
        out[i] = prev
    return out


def _macd_hist(closes: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> List[Optional[float]]:
    n = len(closes)
    if n <= max(fast, slow) + signal + 2:
        return []
    ef = _ema(closes, fast)
    es = _ema(closes, slow)
    if not ef or not es or len(ef) != n or len(es) != n:
        return []
    macd_line: List[float] = []
    for i in range(n):
        macd_line.append(float(ef[i]) - float(es[i]))
    sig = _ema(macd_line, signal)
    if not sig or len(sig) != n:
        return []
    out: List[Optional[float]] = [None] * n
    for i in range(n):
        try:
            out[i] = float(macd_line[i]) - float(sig[i])
        except Exception:
            out[i] = None
    return out


def _macd(values: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[List[float], List[float], List[float]]:
    # DIF=EMA(fast)-EMA(slow), DEA=EMA(DIF,signal), HIST=2*(DIF-DEA)
    if len(values) < slow + signal:
        return [], [], []
    ema_fast = _ema(values, fast)
    ema_slow = _ema(values, slow)
    dif = [a - b for a, b in zip(ema_fast, ema_slow)]
    dea = _ema(dif, signal)
    hist = [(d - e) * 2.0 for d, e in zip(dif, dea)]
    return dif, dea, hist


# ==========================
# REST fallback
# ==========================

def _rest_should_retry_status(status_code: int) -> bool:
    try:
        sc = int(status_code)
    except Exception:
        return False
    if sc == 429:
        return True
    return 500 <= sc <= 599


def _rest_should_retry_exception(e: Exception) -> bool:
    try:
        import requests as _rq

        if isinstance(
            e,
            (
                _rq.exceptions.Timeout,
                _rq.exceptions.ConnectionError,
                _rq.exceptions.ChunkedEncodingError,
                _rq.exceptions.ContentDecodingError,
                _rq.exceptions.RequestException,
            ),
        ):
            return True
    except Exception:
        pass
    msg = str(e).lower()
    for kw in (
        "connection reset",
        "forcibly closed",
        "read timed out",
        "timed out",
        "temporarily unavailable",
        "remote end closed",
        "bad gateway",
        "service unavailable",
        "gateway timeout",
        "max retries exceeded",
    ):
        if kw in msg:
            return True
    return False


def _rest_backoff_sleep(i: int) -> None:
    try:
        base = 0.35 * (2**i)
        jitter = random.random() * 0.15
        time.sleep(min(6.0, base + jitter))
    except Exception:
        pass


def _rest_get_json(url: str, params: Optional[dict] = None, timeout: Any = 10) -> Any:
    last_err: Optional[Exception] = None
    last_status: Optional[int] = None
    last_body: str = ""
    for i in range(3):
        try:
            r = HTTP.get(url, params=params, timeout=timeout)
            last_status = getattr(r, "status_code", None)
            if last_status != 200:
                try:
                    last_body = (r.text or "")[:200]
                except Exception:
                    last_body = ""
                if last_status is not None and _rest_should_retry_status(int(last_status)):
                    raise RuntimeError(f"HTTP {last_status}: {last_body}")
                raise RuntimeError(f"HTTP {last_status}: {last_body}")
            try:
                return r.json()
            except Exception as je:
                last_err = je
                if _rest_should_retry_exception(je):
                    raise
                raise RuntimeError(f"JSON decode failed: {je}")
        except Exception as e:
            last_err = e
            retryable = _rest_should_retry_exception(e)
            if isinstance(e, RuntimeError) and last_status is not None and _rest_should_retry_status(int(last_status)):
                retryable = True
            if i >= 2 or (not retryable):
                break
            _rest_backoff_sleep(i)

    msg = str(last_err) if last_err is not None else "request failed"
    extra = ""
    if last_status is not None:
        extra = f" status={last_status}"
    if last_body:
        extra = f"{extra} body={last_body}"
    raise RuntimeError(f"REST GET failed:{extra} url={url} err={msg}")

def _rest_get(path: str, params: Optional[dict] = None) -> Any:
    url = f"{GATE_REST_FUTURES_USDT_BASE}{path}"
    return _rest_get_json(url, params=params, timeout=8)


def _rest_get_full_url(url: str, params: Optional[dict] = None, timeout: int = 10) -> Any:
    return _rest_get_json(url, params=params, timeout=timeout)


def get_tri_candles(contract: str, tf: str, limit: int) -> List[dict]:
    """获取三周期信号/策略模块用的K线数据（REST）。

    说明：
    - 这里的 tf 直接使用策略模块内部的时间框架（如 1h/4h/1d/1M）。
    - Gate REST 的月线使用 interval=30d 近似，因此 tf=="1M" 时会映射到 "30d"。
    - 返回结构为 Gate futures candlesticks 的 list[dict]，字段包含 t/o/h/l/c/v/sum。
    """
    interval = tf
    if tf == "1M":
        interval = "30d"
    ck = f"tri:candles:{contract}:{interval}:{int(limit)}:rest"
    cached = _cache_get(ck, ttl=max(10, int(TRI_SIGNAL_CACHE_TTL_SEC)))
    if cached is not None:
        return cached
    data = _rest_get("/candlesticks", params={"contract": contract, "interval": interval, "limit": int(limit)})
    out = data if isinstance(data, list) else []
    _cache_set(ck, out)
    return out


# ==========================
# Gate data helpers
# ==========================

def get_candles(contract: str, tf: str, limit: int = 2) -> List[dict]:
    """获取仪表板/异动检测用的K线数据（REST）。

    - tf 是页面选择的时间框架（15m/1h/4h/1d），会通过 TIMEFRAMES 映射到 Gate interval。
    - 返回值不做重排；上层计算会自行按时间戳 t 排序。
    """
    interval = TIMEFRAMES[tf]
    # 优先直接用 REST，MCP 的工具名不确定；后续可通过 tools/list 做映射
    data = _rest_get("/candlesticks", params={"contract": contract, "interval": interval, "limit": limit})
    # REST futures candlesticks: list[dict] with keys t,o,h,l,c,v,sum
    return data if isinstance(data, list) else []


def get_macd_candles(contract: str, tf: str, limit: int = 120) -> List[dict]:
    """获取 MACD 监控/预警用的K线数据（REST）。

    说明：
    - MACD 扫描会用相对更短的历史窗口（默认 120 根），避免请求过大。
    - tf 取 MACD_TIMEFRAMES 映射表（允许 all/15m/1h/1d 相关调用）。
    - 结果会缓存（避免高频刷新触发 Gate 429）。
    """
    # MACD 扫描用：只取最近 100-150 根，避免全量历史
    # 备注：Gate candlesticks interval 不包含 2d，这里用 1d 合成 2d，确保筛选后口径一致。
    ck = f"macd:candles:{contract}:{tf}:{limit}"
    cached = _cache_get(ck, ttl=180)
    if cached is not None:
        return cached

    if tf == "2d":
        # 2d = 两根 1d 合成一根 2d（O=第一根open, H/L=两根极值, C=第二根close, V=sum）
        raw_limit = max(20, min(800, int(limit) * 2 + 6))
        data = _rest_get("/candlesticks", params={"contract": contract, "interval": "1d", "limit": raw_limit})
        seq = [x for x in (data if isinstance(data, list) else []) if isinstance(x, dict)]
        seq.sort(key=lambda x: int(x.get("t") or 0))

        # 只做简单的 2-by-2 合成：保证每根 2d 都对应连续两根 1d
        if len(seq) % 2 == 1:
            seq = seq[1:]

        out: List[dict] = []
        for i in range(0, len(seq) - 1, 2):
            a = seq[i]
            b = seq[i + 1]
            try:
                o = _safe_float(a.get("o"))
                h1 = _safe_float(a.get("h"))
                l1 = _safe_float(a.get("l"))
                c1 = _safe_float(a.get("c"))
                o2 = _safe_float(b.get("o"))
                h2 = _safe_float(b.get("h"))
                l2 = _safe_float(b.get("l"))
                c2 = _safe_float(b.get("c"))
                if o is None or c2 is None:
                    continue

                hi = None
                lo = None
                for vv in (h1, h2):
                    if vv is None:
                        continue
                    hi = vv if hi is None else max(float(hi), float(vv))
                for vv in (l1, l2):
                    if vv is None:
                        continue
                    lo = vv if lo is None else min(float(lo), float(vv))
                if hi is None or lo is None:
                    continue

                v1 = _safe_float(a.get("v"))
                v2 = _safe_float(b.get("v"))
                sv1 = _safe_float(a.get("sum"))
                sv2 = _safe_float(b.get("sum"))

                out.append({
                    "t": int(b.get("t") or 0),
                    "o": float(o),
                    "h": float(hi),
                    "l": float(lo),
                    "c": float(c2),
                    "v": (float(v1 or 0.0) + float(v2 or 0.0)),
                    "sum": (float(sv1 or 0.0) + float(sv2 or 0.0)),
                })
            except Exception:
                continue

        # 只保留最后 limit 根 2d
        out = out[-int(limit):] if limit else out
    else:
        interval = MACD_TIMEFRAMES[tf]
        data = _rest_get("/candlesticks", params={"contract": contract, "interval": interval, "limit": limit})
        out = data if isinstance(data, list) else []

    _cache_set(ck, out)
    return out


def get_contract_stats(contract: str, tf: str, limit: int = 2) -> List[dict]:
    """获取 OI 等合约统计数据（REST）。

    用途：
    - 仪表板主表/市场异动的 OI 变化百分比计算
    - 多空综合雷达中 OI(tf)% 的计算

    注意：contract_stats 的时间戳字段可能是 t 或 time，上层会统一排序处理。
    """
    interval = TIMEFRAMES[tf]
    data = _rest_get("/contract_stats", params={"contract": contract, "interval": interval, "limit": limit})
    return data if isinstance(data, list) else []


def _pick_oi(stat: Dict[str, Any]) -> Optional[float]:
    """从 contract_stats 单条记录中提取 OI 字段。

    Gate 的不同接口/版本可能返回不同字段名，这里按候选字段依次尝试。
    返回 None 表示该条记录无法解析出 OI。
    """
    for k in (
        "open_interest",
        "open_interest_usd",
        "open_interest_size",
        "open_interest_qty",
        "oi",
    ):
        if k in stat:
            v = _safe_float(stat.get(k))
            if v is not None:
                return v
    return None


def get_all_futures_tickers() -> List[dict]:
    ck = "futures:tickers"
    cached = _cache_get(ck, ttl=20)
    if cached is not None:
        return cached
    data = _rest_get("/tickers")
    out = data if isinstance(data, list) else []
    _cache_set(ck, out)
    return out


def get_all_futures_contract_names() -> List[str]:
    ck = "futures:contracts"
    cached = _cache_get(ck, ttl=300)
    if cached is not None:
        return cached
    data = _rest_get("/contracts")
    out: List[str] = []
    if isinstance(data, list):
        for it in data:
            if isinstance(it, dict) and it.get("name"):
                out.append(str(it.get("name")))
    _cache_set(ck, out)
    return out


def _ticker_last_price_map() -> Dict[str, float]:
    tickers = get_all_futures_tickers()
    mp: Dict[str, float] = {}
    for t in tickers:
        if not isinstance(t, dict):
            continue
        c = t.get("contract")
        if not c:
            continue
        last = _safe_float(t.get("last"))
        if last is None:
            last = _safe_float(t.get("last_price"))
        if last is not None:
            mp[str(c)] = float(last)
    return mp


_STABLE_SYMBOLS = {
    "usdt",
    "usdc",
    "dai",
    "tusd",
    "busd",
    "fdusd",
    "usde",
    "usdp",
    "gusd",
    "usdd",
    "lusd",
}


def coingecko_top_marketcap(limit: int = 50) -> List[dict]:
    # CoinGecko 免费接口，无需 key；这里拿 Top 列表再过滤稳定币，最后截取 limit
    ck = f"cg:top:{limit}"
    cached = _cache_get(ck, ttl=600)
    if cached is not None:
        return cached
    if os.getenv("COINGECKO_DISABLE", "0").strip() in ("1", "true", "True", "yes", "YES"):
        contracts = top_contracts_by_quote_volume(max(1, int(limit)))
        out2: List[dict] = []
        rank = 1
        for c in contracts:
            if not isinstance(c, str) or not c.endswith("_USDT"):
                continue
            sym = c.replace("_USDT", "").lower().strip()
            if not sym or sym in _STABLE_SYMBOLS:
                continue
            out2.append({
                "symbol": sym,
                "market_cap_rank": rank,
                "market_cap": None,
            })
            rank += 1
            if len(out2) >= max(1, int(limit)):
                break
        _cache_set(ck, out2)
        return out2
    url = "https://api.coingecko.com/api/v3/coins/markets"
    per_page = 250 if int(limit) > 100 else 100
    try:
        data = _rest_get_full_url(
            url,
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": per_page,
                "page": 1,
                "sparkline": "false",
            },
            timeout=12,
        )
        out: List[dict] = []
        if isinstance(data, list):
            for it in data:
                if not isinstance(it, dict):
                    continue
                sym = str(it.get("symbol") or "").lower().strip()
                if not sym or sym in _STABLE_SYMBOLS:
                    continue
                out.append(it)
        out = out[: max(1, int(limit))]
        _cache_set(ck, out)
        return out
    except Exception:
        contracts = top_contracts_by_quote_volume(max(1, int(limit)))
        out2: List[dict] = []
        rank = 1
        for c in contracts:
            if not isinstance(c, str) or not c.endswith("_USDT"):
                continue
            sym = c.replace("_USDT", "").lower().strip()
            if not sym or sym in _STABLE_SYMBOLS:
                continue
            out2.append({
                "symbol": sym,
                "market_cap_rank": rank,
                "market_cap": None,
            })
            rank += 1
            if len(out2) >= max(1, int(limit)):
                break
        _cache_set(ck, out2)
        return out2


def detect_recent_cross(dif: List[float], dea: List[float], lookback: int = 3) -> Optional[Tuple[str, int]]:
    # 返回 (signal_type, idx) 其中 idx 是触发交叉的 candle index
    # signal_type: golden/death
    if len(dif) < 2 or len(dea) < 2:
        return None
    start = max(1, len(dif) - lookback - 1)
    for i in range(len(dif) - 1, start - 1, -1):
        prev_d, prev_e = dif[i - 1], dea[i - 1]
        cur_d, cur_e = dif[i], dea[i]
        if prev_d <= prev_e and cur_d > cur_e:
            return ("golden", i)
        if prev_d >= prev_e and cur_d < cur_e:
            return ("death", i)
    return None


def top_contracts_by_quote_volume(limit: int = 50) -> List[str]:
    # TopN 列表变化不需要秒级刷新，给更长一点 TTL
    ck = f"top_contracts:{limit}"
    cached = _cache_get(ck, ttl=60)
    if cached is not None:
        return cached

    tickers = get_all_futures_tickers()
    pairs: List[Tuple[str, float]] = []
    for t in tickers:
        if not isinstance(t, dict):
            continue
        c = t.get("contract")
        if not c:
            continue
        vol = (
            _safe_float(t.get("volume_24h_quote"))
            or _safe_float(t.get("volume_24h"))
            or _safe_float(t.get("volume_24h_usd"))
            or 0.0
        )
        pairs.append((c, float(vol)))
    pairs.sort(key=lambda x: x[1], reverse=True)
    out = [c for c, _ in pairs[: max(1, min(200, limit))]]
    _cache_set(ck, out)
    return out


def compute_row(contract: str, tf: str, lookback: int = 1) -> Row:
    """计算单个合约在指定时间框架下的仪表板行数据。

    核心输出：
    - last_price：该 tf 的最后一根收盘价（用于主表展示；不等同于实时 ticker last）
    - price_change_pct：按 lookback 根K线跨度的收盘价变化百分比
    - oi_change_pct：按 lookback 个 contract_stats 点跨度的 OI 变化百分比
    - score：异动强度分数（用于排序）：|ΔP| + 0.7 * |ΔOI|
    - market_signal：四象限市场信号（价格/持仓的符号组合）

    说明：
    - 取样点使用 "lookback + 1" 个数据，以便取到 last 与 prevN（倒数第 lookback+1 个点）。
    - REST 返回顺序可能不稳定，因此会按时间戳排序后取尾部点位。
    """
    lb = max(1, min(24, int(lookback or 1)))
    ck = f"row:{contract}:{tf}:lb{lb}"
    cached = _cache_get(ck, ttl=15)
    if cached is not None:
        try:
            return Row(**cached)
        except Exception:
            pass

    updated_at = int(time.time())

    candles = get_candles(contract, tf, limit=max(2, lb + 1))
    # 价格变化：按收盘价变化 (Close_last-Close_prev)/Close_prev * 100
    # 注意：REST 返回顺序可能变化，这里按时间戳 t 排序确保取到最后两根
    seq = [x for x in candles if isinstance(x, dict)]
    seq.sort(key=lambda x: int(x.get("t") or 0))

    prev_close = _safe_float(seq[-(lb + 1)].get("c")) if len(seq) >= (lb + 1) else None
    last_close = _safe_float(seq[-1].get("c")) if len(seq) >= 1 else None

    last_price = last_close
    if prev_close is None or prev_close == 0 or last_close is None:
        price_change_pct = None
    else:
        price_change_pct = (last_close - prev_close) / prev_close * 100.0

    stats = get_contract_stats(contract, tf, limit=max(2, lb + 1))
    stat_seq = [x for x in stats if isinstance(x, dict)]
    stat_seq.sort(key=lambda x: int(x.get("t") or x.get("time") or 0))
    prev_oi = _pick_oi(stat_seq[-(lb + 1)]) if len(stat_seq) >= (lb + 1) else None
    last_oi = _pick_oi(stat_seq[-1]) if len(stat_seq) >= 1 else None
    oi_change_pct = _pct_change(last_oi, prev_oi)

    score: Optional[float] = None
    try:
        if price_change_pct is not None and oi_change_pct is not None:
            # 强度分数：价格变化与 OI 变化的加权绝对值（便于排序，兼容不同时间框架）
            score = abs(float(price_change_pct)) + 0.7 * abs(float(oi_change_pct))
    except Exception:
        score = None

    market_signal = classify(price_change_pct, oi_change_pct)

    row = Row(
        contract=contract,
        timeframe=tf,
        last_price=last_price,
        price_change_pct=price_change_pct,
        oi_change_pct=oi_change_pct,
        score=score,
        market_signal=market_signal,
        updated_at=updated_at,
    )

    _cache_set(ck, row.__dict__)  # Cache the result
    return row


def classify(price_pct: Optional[float], oi_pct: Optional[float]) -> Optional[str]:
    """四象限分类：由价格变化%与OI变化%的符号组合得出“市场信号”。

    - 价格↑ + OI↑：多头强势进场（上涨伴随增仓）
    - 价格↑ + OI↓：多头获利了结（上涨但减仓）
    - 价格↓ + OI↑：空头强势进场（下跌伴随增仓）
    - 价格↓ + OI↓：空头获利了结（下跌但减仓）
    """
    if price_pct is None or oi_pct is None:
        return None
    if price_pct > 0 and oi_pct < 0:
        return "多头获利了结"
    if price_pct > 0 and oi_pct > 0:
        return "多头强势进场"
    if price_pct < 0 and oi_pct < 0:
        return "空头获利了结"
    if price_pct < 0 and oi_pct > 0:
        return "空头强势进场"
    return None


app = FastAPI(title=APP_TITLE)

app.get("/api/whales/address/detail")(api_whales_address_detail)
app.get("/api/exchange/spot/large_trades")(api_exchange_spot_large_trades)
app.get("/api/exchange/spot/top_usdt_symbols")(api_exchange_spot_top_usdt_symbols)
app.post("/api/move3m/push")(api_move3m_push)
app.get("/api/move3m/log")(api_move3m_log_list)
app.post("/api/move3m/log")(api_move3m_log_add)


def _rsi14(closes: List[float]) -> Optional[float]:
    try:
        if not closes or len(closes) < 15:
            return None
        gains = []
        losses = []
        for i in range(1, len(closes)):
            ch = closes[i] - closes[i - 1]
            gains.append(max(ch, 0.0))
            losses.append(max(-ch, 0.0))
        gains = gains[-14:]
        losses = losses[-14:]
        avg_gain = sum(gains) / 14.0
        avg_loss = sum(losses) / 14.0
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))
        return float(rsi)
    except Exception:
        return None


def _parse_watchlist(raw: str) -> List[str]:
    out: List[str] = []
    for x in (raw or "").split(","):
        s = (x or "").strip().upper()
        if not s:
            continue
        if s.endswith("_USDT"):
            out.append(s)
        else:
            out.append(f"{s}_USDT")
    # 去重保持顺序
    seen = set()
    dedup: List[str] = []
    for c in out:
        if c in seen:
            continue
        seen.add(c)
        dedup.append(c)
    return dedup


def _get_funding_rate(contract: str) -> Optional[float]:
    # Gate futures funding rate: GET /funding_rate?contract=BTC_USDT&limit=1
    ck = f"funding:{contract}"
    cached = _cache_get(ck, ttl=60)
    if cached is not None:
        return cached
    try:
        data = _rest_get("/funding_rate", params={"contract": contract, "limit": 1})
        fr = None
        if isinstance(data, list) and data:
            it = data[0]
            if isinstance(it, dict):
                fr = _safe_float(it.get("r"))
                if fr is None:
                    fr = _safe_float(it.get("funding_rate"))
        if fr is not None:
            _cache_set(ck, float(fr))
            return float(fr)
    except Exception:
        pass
    _cache_set(ck, None)
    return None


def _oi_changes_from_stats(contract: str) -> Dict[str, Optional[float]]:
    ck = f"oi:chg2:{contract}"
    cached = _cache_get(ck, ttl=120)
    if cached is not None:
        return cached
    out = {"oi_5m": None, "oi_15m": None, "oi_1h": None, "oi_1d": None}
    try:
        # 与仪表板 compute_row 同口径：按各 timeframe 取最近两根 contract_stats
        stats5 = get_contract_stats(contract, "5m", limit=2)
        seq5 = [x for x in stats5 if isinstance(x, dict)]
        seq5.sort(key=lambda x: int(x.get("t") or x.get("time") or 0))
        ois5 = [_pick_oi(x) for x in seq5]
        if len(ois5) >= 2 and ois5[-1] is not None and ois5[-2] is not None:
            out["oi_5m"] = _pct_change(ois5[-1], ois5[-2])
    except Exception:
        pass

    try:
        stats15 = get_contract_stats(contract, "15m", limit=2)
        seq15 = [x for x in stats15 if isinstance(x, dict)]
        seq15.sort(key=lambda x: int(x.get("t") or x.get("time") or 0))
        ois15 = [_pick_oi(x) for x in seq15]
        if len(ois15) >= 2 and ois15[-1] is not None and ois15[-2] is not None:
            out["oi_15m"] = _pct_change(ois15[-1], ois15[-2])
    except Exception:
        pass

    try:
        stats1h = get_contract_stats(contract, "1h", limit=2)
        seq1h = [x for x in stats1h if isinstance(x, dict)]
        seq1h.sort(key=lambda x: int(x.get("t") or x.get("time") or 0))
        ois1h = [_pick_oi(x) for x in seq1h]
        if len(ois1h) >= 2 and ois1h[-1] is not None and ois1h[-2] is not None:
            out["oi_1h"] = _pct_change(ois1h[-1], ois1h[-2])
    except Exception:
        pass

    try:
        # 1d：最近一根日线 vs 上一根
        stats1d = get_contract_stats(contract, "1d", limit=2)
        seq1d = [x for x in stats1d if isinstance(x, dict)]
        seq1d.sort(key=lambda x: int(x.get("t") or x.get("time") or 0))
        ois1d = [_pick_oi(x) for x in seq1d]
        if len(ois1d) >= 2 and ois1d[-1] is not None and ois1d[-2] is not None:
            out["oi_1d"] = _pct_change(ois1d[-1], ois1d[-2])
    except Exception:
        pass

    _cache_set(ck, out)
    return out


def _candle_change_pct(contract: str, tf: str) -> Optional[float]:
    try:
        candles = get_candles(contract, tf, limit=2)
        seq = [x for x in candles if isinstance(x, dict)]
        seq.sort(key=lambda x: int(x.get("t") or 0))
        if len(seq) < 2:
            return None
        prev_close = _safe_float(seq[-2].get("c"))
        last_close = _safe_float(seq[-1].get("c"))
        if prev_close is None or prev_close == 0 or last_close is None:
            return None
        return (last_close - prev_close) / prev_close * 100.0
    except Exception:
        return None


def _volume_ratio_tf_vs_24h(contract: str, tf: str, ticker: Optional[dict]) -> Optional[float]:
    try:
        # tf 量用 tf K线 v，24h 用 ticker 的 volume_24h_quote 近似
        vol24h = None
        if isinstance(ticker, dict):
            vol24h = _safe_float(ticker.get("volume_24h_quote")) or _safe_float(ticker.get("volume_24h"))
        if vol24h is None or vol24h <= 0:
            return None

        tf = (tf or "1h").strip()
        if tf not in TIMEFRAMES:
            tf = "1h"
        minutes_map = {"5m": 5, "15m": 15, "1h": 60, "4h": 240, "1d": 1440}
        m = minutes_map.get(tf, 60)
        tf_sec = int(m) * 60

        candles = get_candles(contract, tf, limit=2)
        seq = [x for x in candles if isinstance(x, dict)]
        seq.sort(key=lambda x: int(x.get("t") or 0))
        if not seq:
            return None

        now_ts = int(time.time())
        last = seq[-1]
        last_t = int(last.get("t") or 0)
        use = last
        if tf_sec > 0 and last_t > 0 and now_ts < (last_t + tf_sec) and len(seq) >= 2:
            use = seq[-2]

        vtf = _safe_float(use.get("v"))
        if vtf is None or vtf < 0:
            return None

        buckets = max(1.0, 1440.0 / float(m))
        base = float(vol24h) / buckets
        if base <= 0:
            return None
        return float(vtf) / base
    except Exception:
        return None


def _macd_status_and_rsi(contract: str, tf: str = "1h") -> Dict[str, Any]:
    out = {"macd": {"status": "—", "type": None, "ratio": None}, "rsi14": None}
    try:
        tf = (tf or "1h").strip()
        if tf not in MACD_TIMEFRAMES:
            tf = "1h"
        candles = get_macd_candles(contract, tf, limit=120)
        seq = [x for x in candles if isinstance(x, dict)]
        seq.sort(key=lambda x: int(x.get("t") or 0))
        closes = [float(x.get("c")) for x in seq if _safe_float(x.get("c")) is not None]
        if len(closes) < 50:
            return out
        out["rsi14"] = _rsi14(closes)
        dif, dea, hist = _macd(closes, 12, 26, 9)
        if not dif:
            return out
        cross = detect_recent_cross(dif, dea, lookback=3)
        if cross:
            st, _idx = cross
            out["macd"]["type"] = st
            out["macd"]["status"] = "金叉" if st == "golden" else "死叉"
            return out

        pre = detect_prealert(dif, dea, hist, lookback=2, ratio_threshold=0.75)
        if pre:
            tp = pre.get("type")
            out["macd"]["type"] = tp
            out["macd"]["ratio"] = pre.get("ratio")
            out["macd"]["status"] = "即将金叉" if tp == "pre_golden" else "即将死叉"
        return out
    except Exception:
        return out


def _signal_score(item: Dict[str, Any]) -> Tuple[float, List[str], str]:
    """多空综合雷达：对单个币的多维指标打分，输出综合分数与原因。

    返回：
    - score：[-10, +10] 的综合分数（会 clamp）
    - reasons：用于前端展示的主要加减分原因
    - level：strong_long/long/neutral/short/strong_short

    评分维度（大致）：
    - 资金费率 funding（极正/极负）
    - 价格(tf)% 与 OI(tf)% 共振（增仓趋势确认/减仓背离）
    - 成交量放大（vol_ratio）
    - MACD 事件（金叉/死叉/预警）
    - RSI(14) 超买超卖（在趋势确认时会降权，避免逆势加分）
    """
    score = 0.0
    reasons: List[str] = []

    trend_confirm = False

    k_tf = str(item.get("k_tf") or "1h").strip() or "1h"
    oi_tf = item.get("oi_change_tf")
    if oi_tf is None:
        oi_tf = item.get("oi_change_1h")
    if k_tf == "15m":
        oi_thr = 1.0
    elif k_tf == "1d":
        oi_thr = 5.0
    else:
        oi_thr = 2.0
    oi_confirm = False
    try:
        oi_confirm = isinstance(oi_tf, (int, float)) and abs(float(oi_tf)) >= float(oi_thr)
    except Exception:
        oi_confirm = False

    funding = item.get("funding")
    if isinstance(funding, (int, float)):
        if funding <= -0.0003:
            score += 2.0 if oi_confirm else 1.0
            reasons.append(f"资费率 {funding*100:.2f}%（极负）" + ("（无OI确认，降档）" if not oi_confirm else ""))
        elif funding <= -0.0001:
            score += 1.0
            reasons.append(f"资费率 {funding*100:.2f}%（负）")
        elif funding >= 0.0003:
            score -= 2.0 if oi_confirm else 1.0
            reasons.append(f"资费率 {funding*100:.2f}%（极正）" + ("（无OI确认，降档）" if not oi_confirm else ""))
        elif funding >= 0.0001:
            score -= 1.0
            reasons.append(f"资费率 {funding*100:.2f}%（正）")

    px_tf = item.get("pct_tf")
    # price + OI 共振：按 k_tf 采用不同阈值（短周期更敏感，长周期更稳）
    try:
        px_tf = item.get("pct_tf")
        oi_tf = item.get("oi_change_tf")
        if k_tf == "15m":
            oi_thr = 1.0
            px_thr = 0.3
        elif k_tf == "1d":
            oi_thr = 5.0
            px_thr = 1.5
        else:
            oi_thr = 2.0
            px_thr = 0.5

        oi_up = isinstance(oi_tf, (int, float)) and float(oi_tf) >= float(oi_thr)
        oi_dn = isinstance(oi_tf, (int, float)) and float(oi_tf) <= -float(oi_thr)
        px_up = isinstance(px_tf, (int, float)) and float(px_tf) >= float(px_thr)
        px_dn = isinstance(px_tf, (int, float)) and float(px_tf) <= -float(px_thr)
        if oi_up and px_up:
            score += 3.0
            trend_confirm = True
            reasons.append(f"OI↑({float(oi_tf):.2f}%) 价格↑({float(px_tf):.2f}%)")
        elif oi_up and px_dn:
            score -= 3.0
            trend_confirm = True
            reasons.append(f"OI↑({float(oi_tf):.2f}%) 价格↓({float(px_tf):.2f}%)")
        elif oi_dn and px_up:
            score += 1.5
            reasons.append(f"OI↓({float(oi_tf):.2f}%) 价格↑({float(px_tf):.2f}%)")
        elif oi_dn and px_dn:
            score -= 1.5
            reasons.append(f"OI↓({float(oi_tf):.2f}%) 价格↓({float(px_tf):.2f}%)")
    except Exception:
        pass

    vol_ratio = item.get("vol_ratio")
    if isinstance(vol_ratio, (int, float)):
        vr = float(vol_ratio)
        vscore = 0.0
        if vr >= 2.5:
            vscore = 2.5
        elif vr >= 1.5:
            vscore = 1.5

        if vscore != 0.0:
            px_for_vol = item.get("pct_tf")
            if px_for_vol is None:
                px_for_vol = item.get("pct_1h")
            try:
                if px_for_vol is not None and float(px_for_vol) > 0:
                    score += vscore
                    reasons.append(f"放量上涨 x{vr:.2f}")
                elif px_for_vol is not None and float(px_for_vol) < 0:
                    score -= vscore
                    reasons.append(f"放量下跌 x{vr:.2f}")
                else:
                    reasons.append(f"成交量放大 x{vr:.2f}")
            except Exception:
                reasons.append(f"成交量放大 x{vr:.2f}")

    macd = item.get("macd") if isinstance(item.get("macd"), dict) else {}
    macd_status = (macd.get("status") or "—")
    if macd_status == "金叉":
        score += 3.5
        trend_confirm = True
        reasons.append("MACD 金叉")
    elif macd_status == "即将金叉":
        score += 2
        reasons.append("MACD 即将金叉")
    elif macd_status == "死叉":
        score -= 3.5
        trend_confirm = True
        reasons.append("MACD 死叉")
    elif macd_status == "即将死叉":
        score -= 2
        reasons.append("MACD 即将死叉")

    rsi = item.get("rsi14")
    if isinstance(rsi, (int, float)):
        w = 0.5 if trend_confirm else 1.0
        if float(rsi) < 30:
            score += 2.0 * w
            reasons.append(f"RSI {float(rsi):.0f}（超卖）")
        elif float(rsi) < 40:
            score += 1.0 * w
            reasons.append(f"RSI {float(rsi):.0f}（偏低）")
        elif float(rsi) > 70:
            score -= 2.0 * w
            reasons.append(f"RSI {float(rsi):.0f}（超买）")
        elif float(rsi) > 60:
            score -= 1.0 * w
            reasons.append(f"RSI {float(rsi):.0f}（偏高）")

    # clamp
    if score > 10:
        score = 10.0
    if score < -10:
        score = -10.0

    if score >= 6:
        level = "strong_long"
    elif score >= 3:
        level = "long"
    elif score <= -6:
        level = "strong_short"
    elif score <= -3:
        level = "short"
    else:
        level = "neutral"
    return float(score), reasons, level


def build_signal_dashboard(
    mode: str = "top100",
    limit: int = 100,
    only_strong: int = 0,
    only_signal: int = 0,
    sort: str = "score",
    k_tf: str = "1h",
) -> dict:
    """构建“多空综合雷达”表格数据。

    参数：
    - mode：top100 或 watchlist
    - limit：TopN 数量（用于 top100 模式）
    - only_strong：只返回 strong_long/strong_short
    - only_signal：只返回非 neutral
    - sort：score/rank/symbol
    - k_tf：指标计算使用的K线时间框架（15m/1h/1d）

    说明：
    - 数据会缓存（key 中包含 watchlist 与过滤条件），避免频繁触发外部接口。
    - 该模块会综合 ticker / candles / contract_stats 等数据源。
    """
    if not SIGNAL_DASHBOARD_ENABLED:
        return {"items": [], "errors": ["disabled"]}
    mode = (mode or "top100").strip().lower()
    limit = max(10, min(200, int(limit)))
    only_strong = int(only_strong)
    only_signal = int(only_signal)
    sort = (sort or "score").strip().lower()

    k_tf = (k_tf or "1h").strip()
    if k_tf not in ("15m", "1h", "1d"):
        k_tf = "1h"

    ck = f"signal_dashboard:{mode}:{limit}:{only_strong}:{only_signal}:{sort}:{k_tf}:{SIGNAL_DASHBOARD_WATCHLIST}"
    cached = _cache_get(ck, ttl=max(5, int(SIGNAL_DASHBOARD_CACHE_TTL_SEC)))
    if cached is not None:
        return cached

    errors: List[str] = []
    items: List[dict] = []
    now_ts = int(time.time())

    contract_set = set(get_all_futures_contract_names())

    # 监控列表
    contracts: List[str] = []
    rank_map: Dict[str, Any] = {}
    if mode == "watchlist" and SIGNAL_DASHBOARD_WATCHLIST:
        contracts = [c for c in _parse_watchlist(SIGNAL_DASHBOARD_WATCHLIST) if c in contract_set]
    else:
        top = coingecko_top_marketcap(limit)
        for it in top:
            try:
                sym = str(it.get("symbol") or "").upper().strip()
                if not sym:
                    continue
                c = f"{sym}_USDT"
                if c not in contract_set:
                    continue
                contracts.append(c)
                rank_map[c] = it.get("market_cap_rank")
            except Exception:
                continue
        contracts = contracts[:limit]

    tickers = get_all_futures_tickers()
    ticker_map: Dict[str, dict] = {}
    for t in tickers:
        if isinstance(t, dict) and t.get("contract"):
            ticker_map[str(t.get("contract"))] = t

    def _one(contract: str) -> Optional[dict]:
        t = ticker_map.get(contract)
        last = None
        try:
            last = _safe_float((t or {}).get("last")) or _safe_float((t or {}).get("last_price"))
        except Exception:
            pass

        pct15m = _candle_change_pct(contract, "15m")
        pct1h = _candle_change_pct(contract, "1h")
        pct1d = _candle_change_pct(contract, "1d")
        pct_tf = _candle_change_pct(contract, k_tf)

        funding = _get_funding_rate(contract)
        oichg = _oi_changes_from_stats(contract)
        oi_tf = None
        try:
            if k_tf == "15m":
                oi_tf = oichg.get("oi_15m")
            elif k_tf == "1d":
                oi_tf = oichg.get("oi_1d")
            else:
                oi_tf = oichg.get("oi_1h")
        except Exception:
            oi_tf = None
        vol_ratio = _volume_ratio_tf_vs_24h(contract, k_tf, t)
        macd_rsi = _macd_status_and_rsi(contract, tf=k_tf)

        item = {
            "symbol": contract.replace("_USDT", ""),
            "contract": contract,
            "market_cap_rank": rank_map.get(contract),
            "price": last,
            "pct_15m": pct15m,
            "pct_1h": pct1h,
            "pct_1d": pct1d,
            "pct_tf": pct_tf,
            "funding": funding,
            "oi_change_5m": oichg.get("oi_5m"),
            "oi_change_15m": oichg.get("oi_15m"),
            "oi_change_1h": oichg.get("oi_1h"),
            "oi_change_1d": oichg.get("oi_1d"),
            "oi_change_tf": oi_tf,
            "vol_ratio": vol_ratio,
            "macd": macd_rsi.get("macd"),
            "rsi14": macd_rsi.get("rsi14"),
            "k_tf": k_tf,
            "updated_at": now_ts,
        }
        score, reasons, level = _signal_score(item)
        item["score"] = score
        item["level"] = level
        item["reasons"] = reasons
        if only_strong and level not in ("strong_long", "strong_short"):
            return None
        if (not only_strong) and only_signal and level == "neutral":
            return None
        return item

    max_workers = 6
    if len(contracts) <= 30:
        max_workers = 4
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_one, c): c for c in contracts}
        for f in as_completed(futs):
            c = futs[f]
            try:
                r = f.result()
                if r is not None:
                    items.append(r)
            except Exception as e:
                errors.append(f"{c}: {e}")

    if sort == "symbol":
        items.sort(key=lambda x: str(x.get("symbol") or ""))
    elif sort == "rank":
        items.sort(key=lambda x: int(x.get("market_cap_rank") or 10**9))
    else:
        items.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)

    payload = {"items": items, "errors": errors}
    _cache_set(ck, payload)
    return payload


@app.get("/api/signal_dashboard")
def api_signal_dashboard(
    mode: str = "top100",
    limit: int = 100,
    only_strong: int = 0,
    only_signal: int = 0,
    sort: str = "score",
    k_tf: str = "1h",
) -> JSONResponse:
    try:
        payload = build_signal_dashboard(
            mode=mode,
            limit=limit,
            only_strong=only_strong,
            only_signal=only_signal,
            sort=sort,
            k_tf=k_tf,
        )
        return JSONResponse(payload)
    except Exception as e:
        return JSONResponse({"items": [], "errors": [str(e)]}, status_code=200)


def _parse_contracts_csv(raw: str) -> List[str]:
    out: List[str] = []
    for x in (raw or "").split(","):
        s = (x or "").strip().upper()
        if not s:
            continue
        if "_" not in s:
            s = f"{s}_USDT"
        out.append(s)
    seen = set()
    dedup: List[str] = []
    for c in out:
        if c in seen:
            continue
        seen.add(c)
        dedup.append(c)
    return dedup


def _monthly_background(closes: List[float]) -> Dict[str, Any]:
    # MACD(12,26,9) needs at least slow + signal bars to be meaningful.
    # Using 30d as "monthly" approximation, many contracts don't have 40 bars (3.3y).
    if len(closes) < 35:
        return {"state": "—", "reason": ""}
    sma10 = _sma(closes, 10)
    dif, dea, hist = _macd(closes, 12, 26, 9)
    if not sma10 or not hist:
        return {"state": "—", "reason": ""}
    last_close = closes[-1]
    last_sma = sma10[-1]
    last_hist = hist[-1]
    if last_sma is None:
        return {"state": "—", "reason": ""}
    above = last_close > float(last_sma)
    macd_pos = float(last_hist) > 0
    if above and macd_pos:
        return {"state": "bull", "reason": "Close>SMA10 & MACD偏多"}
    if (not above) and (not macd_pos):
        return {"state": "bear", "reason": "Close<SMA10 & MACD偏空"}
    return {"state": "neutral", "reason": "SMA/MACD冲突(过渡)"}


def _daily_trend(highs: List[float], lows: List[float], closes: List[float]) -> Dict[str, Any]:
    if len(closes) < 60:
        return {"direction": "—", "strength": "—", "adx": None, "reason": ""}
    ema20 = _ema(closes, 20)
    ema50 = _ema(closes, 50)
    adx = _adx(highs, lows, closes, 14)
    if not ema20 or not ema50:
        return {"direction": "—", "strength": "—", "adx": None, "reason": ""}
    direction = "up" if ema20[-1] > ema50[-1] else "down"
    adx_last = None
    if adx:
        adx_last = adx[-1]
    strength = "weak"
    try:
        if adx_last is not None and float(adx_last) > 25:
            strength = "strong"
    except Exception:
        strength = "weak"
    return {
        "direction": direction,
        "strength": strength,
        "adx": float(adx_last) if isinstance(adx_last, (int, float)) else None,
        "reason": f"EMA20{'>' if direction == 'up' else '<'}EMA50, ADX={float(adx_last):.1f}" if isinstance(adx_last, (int, float)) else f"EMA20{'>' if direction == 'up' else '<'}EMA50",
    }


class TriSignalEngine:
    """三周期信号矩阵引擎。

    目标：
    - 用“月线背景 + 日线趋势 + 小时级执行”三段式结构，为每个合约输出方向/强弱/是否高胜率。
    - 该引擎主要用于“监控/提示”，而不是严格的交易回测系统。
    """
    def __init__(self, contracts: List[str]):
        self.contracts = contracts

    def _candles(self, contract: str, tf: str, limit: int) -> List[dict]:
        return get_tri_candles(contract=contract, tf=tf, limit=int(limit))

    def _series(self, candles: List[dict]) -> Tuple[List[int], List[float], List[float], List[float], List[float]]:
        # IMPORTANT: Keep OHLC arrays aligned (same indices belong to same candle).
        seq = [x for x in candles if isinstance(x, dict)]
        seq.sort(key=lambda x: int(x.get("t") or 0))
        ts: List[int] = []
        o: List[float] = []
        h: List[float] = []
        l: List[float] = []
        c: List[float] = []
        for x in seq:
            t = x.get("t")
            oo = _safe_float(x.get("o"))
            hh = _safe_float(x.get("h"))
            ll = _safe_float(x.get("l"))
            cc = _safe_float(x.get("c"))
            if t is None or oo is None or hh is None or ll is None or cc is None:
                continue
            try:
                ts.append(int(t))
                o.append(float(oo))
                h.append(float(hh))
                l.append(float(ll))
                c.append(float(cc))
            except Exception:
                continue
        return ts, o, h, l, c

    def _monthly_background(self, closes: List[float]) -> Dict[str, Any]:
        # MACD(12,26,9) needs at least slow + signal bars to be meaningful.
        # Using 30d as "monthly" approximation, many contracts don't have 40 bars (3.3y).
        if len(closes) < 35:
            return {"state": "—", "reason": ""}
        sma10 = _sma(closes, 10)
        dif, dea, hist = _macd(closes, 12, 26, 9)
        if not sma10 or not hist:
            return {"state": "—", "reason": ""}
        last_close = closes[-1]
        last_sma = sma10[-1]
        last_hist = hist[-1]
        if last_sma is None:
            return {"state": "—", "reason": ""}
        above = last_close > float(last_sma)
        macd_pos = float(last_hist) > 0
        if above and macd_pos:
            return {"state": "bull", "reason": "Close>SMA10 & MACD偏多"}
        if (not above) and (not macd_pos):
            return {"state": "bear", "reason": "Close<SMA10 & MACD偏空"}
        return {"state": "neutral", "reason": "SMA/MACD冲突(过渡)"}

    def _daily_trend(self, highs: List[float], lows: List[float], closes: List[float]) -> Dict[str, Any]:
        if len(closes) < 60:
            return {"direction": "—", "strength": "—", "adx": None, "reason": ""}
        ema20 = _ema(closes, 20)
        ema50 = _ema(closes, 50)
        adx = _adx(highs, lows, closes, 14)
        if not ema20 or not ema50:
            return {"direction": "—", "strength": "—", "adx": None, "reason": ""}
        direction = "up" if ema20[-1] > ema50[-1] else "down"
        adx_last = None
        if adx:
            adx_last = adx[-1]
        strength = "weak"
        try:
            if adx_last is not None and float(adx_last) > 25:
                strength = "strong"
        except Exception:
            strength = "weak"
        return {
            "direction": direction,
            "strength": strength,
            "adx": float(adx_last) if isinstance(adx_last, (int, float)) else None,
            "reason": f"EMA20{'>' if direction == 'up' else '<'}EMA50, ADX={float(adx_last):.1f}" if isinstance(adx_last, (int, float)) else f"EMA20{'>' if direction == 'up' else '<'}EMA50",
        }

    def _hourly_exec(self, highs: List[float], lows: List[float], closes: List[float]) -> Dict[str, Any]:
        if len(closes) < 220:
            return {"signal": "none", "reason": "数据不足", "setup": "none", "setup_reason": "数据不足", "rsi": None, "ema200": None, "entry": None, "sl": None, "tp": None, "atr": None}
        ema200 = _ema(closes, 200)
        # For the crossing event, only prev/last RSI are needed.
        rsi_last = _rsi14(closes)
        rsi_prev = _rsi14(closes[:-1]) if len(closes) >= 16 else None
        last_close = closes[-1]
        e200 = ema200[-1] if ema200 else None

        setup = "none"
        setup_reason = ""
        if rsi_last is not None and e200 is not None:
            try:
                if last_close > e200 and float(rsi_last) < 30.0:
                    setup = "setup_long"
                    setup_reason = "Close>EMA200 & RSI<30（等待上穿）"
                elif last_close < e200 and float(rsi_last) > 70.0:
                    setup = "setup_short"
                    setup_reason = "Close<EMA200 & RSI>70（等待下穿）"
            except Exception:
                setup = "none"
                setup_reason = ""

        signal = "none"
        reason = ""
        if rsi_prev is not None and rsi_last is not None and e200 is not None:
            if last_close > e200 and float(rsi_prev) < 30.0 and float(rsi_last) >= 30.0:
                signal = "long"
                reason = "Close>EMA200 & RSI上穿30"
            elif last_close < e200 and float(rsi_prev) > 70.0 and float(rsi_last) <= 70.0:
                signal = "short"
                reason = "Close<EMA200 & RSI下穿70"
            else:
                try:
                    cl = float(last_close)
                    e = float(e200)
                    rp = float(rsi_prev)
                    rl = float(rsi_last)
                    above = cl > e
                    below = cl < e
                    if above:
                        if rp < 30.0 and rl < 30.0:
                            reason = f"Close>EMA200 但 RSI未上穿30（prev={rp:.2f}, now={rl:.2f}）"
                        else:
                            reason = f"Close>EMA200 且 RSI未处于上穿区间（prev={rp:.2f}, now={rl:.2f}）"
                    elif below:
                        if rp > 70.0 and rl > 70.0:
                            reason = f"Close<EMA200 但 RSI未下穿70（prev={rp:.2f}, now={rl:.2f}）"
                        else:
                            reason = f"Close<EMA200 且 RSI未处于下穿区间（prev={rp:.2f}, now={rl:.2f}）"
                    else:
                        reason = f"Close≈EMA200（Close={cl:.6f}, EMA200={e:.6f}）"
                except Exception:
                    if not reason:
                        reason = "未触发（条件未满足）"
        elif not reason:
            if e200 is None:
                reason = "EMA200不足"
            elif rsi_last is None:
                reason = "RSI不足"
            elif rsi_prev is None:
                try:
                    rl = float(rsi_last) if isinstance(rsi_last, (int, float)) else None
                    reason = f"RSI历史不足（now={rl:.2f}）" if rl is not None else "RSI历史不足"
                except Exception:
                    reason = "RSI历史不足"
            else:
                reason = "未触发（条件未满足）"

        atr_series = _atr(highs, lows, closes, 14)
        atr_last = atr_series[-1] if atr_series else None
        entry = last_close
        sl = None
        tp = None
        if isinstance(atr_last, (int, float)) and atr_last > 0 and signal in ("long", "short"):
            if signal == "long":
                sl = entry - 1.5 * float(atr_last)
                tp = entry + 3.0 * float(atr_last)
            else:
                sl = entry + 1.5 * float(atr_last)
                tp = entry - 3.0 * float(atr_last)
        return {
            "signal": signal,
            "reason": reason,
            "setup": setup,
            "setup_reason": setup_reason,
            "rsi": float(rsi_last) if isinstance(rsi_last, (int, float)) else None,
            "ema200": float(e200) if isinstance(e200, (int, float)) else None,
            "entry": float(entry) if isinstance(entry, (int, float)) else None,
            "sl": float(sl) if isinstance(sl, (int, float)) else None,
            "tp": float(tp) if isinstance(tp, (int, float)) else None,
            "atr": float(atr_last) if isinstance(atr_last, (int, float)) else None,
        }

    def analyze_one(self, contract: str) -> Dict[str, Any]:
        """对单个合约执行三周期分析并返回结构化结果。"""
        now_ts = int(time.time())
        monthly = self._candles(contract, "1d", limit=160)
        daily = self._candles(contract, "4h", limit=260)
        hourly = self._candles(contract, "1h", limit=260)

        m_ts, _mo, _mh, _ml, m_c = self._series(monthly)
        d_ts, _do, d_h, d_l, d_c = self._series(daily)
        h_ts, _ho, h_h, h_l, h_c = self._series(hourly)

        m_bg = self._monthly_background(m_c)
        d_tr = self._daily_trend(d_h, d_l, d_c)
        h_ex = self._hourly_exec(h_h, h_l, h_c)

        hi_prob = False
        grade = "C"
        if h_ex.get("signal") in ("long", "short") and d_tr.get("direction") in ("up", "down"):
            want = "up" if h_ex.get("signal") == "long" else "down"
            if want == d_tr.get("direction"):
                hi_prob = True
                grade = "A" if d_tr.get("strength") == "strong" else "B"

        return {
            "contract": contract,
            "symbol": contract.replace("_USDT", ""),
            "updated_at": now_ts,
            "monthly": m_bg,
            "daily": d_tr,
            "hourly": h_ex,
            "high_prob": bool(hi_prob),
            "grade": grade,
            "last_price": float(h_c[-1]) if h_c else (float(d_c[-1]) if d_c else None),
            "ts": {"1d": (m_ts[-1] if m_ts else None), "4h": (d_ts[-1] if d_ts else None), "1h": (h_ts[-1] if h_ts else None)},
        }

    def matrix(self) -> dict:
        if not TRI_SIGNAL_ENABLED:
            return {"items": [], "errors": ["disabled"]}
        ck = f"tri_signal:matrix:{','.join(self.contracts)}"
        cached = _cache_get(ck, ttl=max(5, int(TRI_SIGNAL_CACHE_TTL_SEC)))
        if cached is not None:
            return cached
        errors: List[str] = []
        items: List[dict] = []

        contract_set = set(get_all_futures_contract_names())
        targets = [c for c in self.contracts if c in contract_set]

        max_workers = max(1, min(int(TRI_SIGNAL_MAX_WORKERS), max(1, len(targets))))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(self.analyze_one, c): c for c in targets}
            for f in as_completed(futs):
                c = futs[f]
                try:
                    items.append(f.result())
                except Exception as e:
                    errors.append(f"{c}: {e}")

        items.sort(key=lambda x: str(x.get("contract") or ""))
        payload = {"items": items, "errors": errors}
        _cache_set(ck, payload)
        return payload


class MasterBEngine:
    """量化策略 Master B（Voyage）引擎。

    结构：
    - 1D：环境过滤（趋势/ADX 等）决定只做多/只做空
    - 1D：预警（回调/反弹至 SMA10 0%~0.9% 区间）
    - 4H：触发（MACD 动能反转 + 吞没形态）
    - 风控：用 ATR 计算 SL/TP1/TP2
    """
    def __init__(self, contracts: List[str]):
        self.contracts = contracts

    def _candles(self, contract: str, tf: str, limit: int) -> List[dict]:
        return get_tri_candles(contract=contract, tf=tf, limit=int(limit))

    def _series(self, candles: List[dict]) -> Tuple[List[int], List[float], List[float], List[float], List[float]]:
        seq = [x for x in candles if isinstance(x, dict)]
        seq.sort(key=lambda x: int(x.get("t") or 0))
        ts: List[int] = []
        o: List[float] = []
        h: List[float] = []
        l: List[float] = []
        c: List[float] = []
        for x in seq:
            t = x.get("t")
            oo = _safe_float(x.get("o"))
            hh = _safe_float(x.get("h"))
            ll = _safe_float(x.get("l"))
            cc = _safe_float(x.get("c"))
            if t is None or oo is None or hh is None or ll is None or cc is None:
                continue
            try:
                ts.append(int(t))
                o.append(float(oo))
                h.append(float(hh))
                l.append(float(ll))
                c.append(float(cc))
            except Exception:
                continue
        return ts, o, h, l, c

    def _ttm_squeeze_on(self, highs: List[float], lows: List[float], closes: List[float]) -> Optional[bool]:
        if len(closes) < 50:
            return None
        bb_mid = _sma(closes, 20)
        if not bb_mid or bb_mid[-1] is None:
            return None
        try:
            win = closes[-20:]
            mean = float(bb_mid[-1])
            var = sum((float(x) - mean) ** 2 for x in win) / float(len(win))
            std = var ** 0.5
            bb_upper = mean + 2.0 * std
            bb_lower = mean - 2.0 * std
        except Exception:
            return None

        kc_mid = _ema(closes, 20)
        atr20 = _atr(highs, lows, closes, 20)
        if not kc_mid or not atr20 or kc_mid[-1] is None or atr20[-1] is None:
            return None
        try:
            m = float(kc_mid[-1])
            a = float(atr20[-1])
            kc_upper = m + 1.5 * a
            kc_lower = m - 1.5 * a
            bb_w = float(bb_upper) - float(bb_lower)
            kc_w = float(kc_upper) - float(kc_lower)
            return bool(bb_w < kc_w)
        except Exception:
            return None

    def _env_1d_voyage(self, highs_1d: List[float], lows_1d: List[float], closes_1d: List[float]) -> Dict[str, Any]:
        if len(closes_1d) < 60:
            return {"state": "none", "reason": "数据不足", "sma10": None, "sma30": None, "adx": None}
        sma10 = _sma(closes_1d, 10)
        sma30 = _sma(closes_1d, 30)
        adx14 = _adx(highs_1d, lows_1d, closes_1d, 14)
        if not sma10 or not sma30 or not adx14 or sma10[-1] is None or sma30[-1] is None or adx14[-1] is None:
            return {"state": "none", "reason": "指标不足", "sma10": None, "sma30": None, "adx": None}

        v10 = float(sma10[-1])
        v30 = float(sma30[-1])
        a = float(adx14[-1])
        if a <= 25.0:
            return {"state": "none", "reason": f"ADX≤25（{a:.1f}）", "sma10": v10, "sma30": v30, "adx": a}
        if v10 > v30:
            return {"state": "long_only", "reason": f"SMA10>SMA30 且 ADX>25（{a:.1f}）", "sma10": v10, "sma30": v30, "adx": a}
        if v10 < v30:
            return {"state": "short_only", "reason": f"SMA10<SMA30 且 ADX>25（{a:.1f}）", "sma10": v10, "sma30": v30, "adx": a}
        return {"state": "none", "reason": "SMA10=30", "sma10": v10, "sma30": v30, "adx": a}

    def _prealert_1d_voyage(self, closes_1d: List[float], side: str) -> Dict[str, Any]:
        if len(closes_1d) < 30:
            return {"state": "none", "reason": "数据不足", "price": None, "sma10": None, "dist": None}
        sma10 = _sma(closes_1d, 10)
        if not sma10 or sma10[-1] is None:
            return {"state": "none", "reason": "SMA10不足", "price": None, "sma10": None, "dist": None}
        price = float(closes_1d[-1])
        s10 = float(sma10[-1])
        if s10 <= 0:
            return {"state": "none", "reason": "SMA10异常", "price": price, "sma10": s10, "dist": None}
        if side == "long":
            # Pullback: price from above, stays slightly above SMA10 (0%~0.9%)
            dist = (price - s10) / s10
            if 0.0 <= dist <= 0.009:
                return {"state": "pre_long", "reason": "回调至 SMA10 上方 0%~0.9%", "price": price, "sma10": s10, "dist": dist}
            return {"state": "none", "reason": f"未回调到 SMA10 上方 0%~0.9%（当前{dist*100:.2f}%）", "price": price, "sma10": s10, "dist": dist}

        # side == short
        # Relief rally: price from below, stays slightly below SMA10 (0%~0.9%)
        dist = (s10 - price) / s10
        if 0.0 <= dist <= 0.009:
            return {"state": "pre_short", "reason": "反弹至 SMA10 下方 0%~0.9%", "price": price, "sma10": s10, "dist": dist}
        return {"state": "none", "reason": f"未反弹到 SMA10 下方 0%~0.9%（当前{dist*100:.2f}%）", "price": price, "sma10": s10, "dist": dist}

    def _trigger_4h_voyage(self, opens_4h: List[float], highs_4h: List[float], lows_4h: List[float], closes_4h: List[float], side: str) -> Dict[str, Any]:
        if len(closes_4h) < 60 or len(opens_4h) < 2:
            return {"state": "none", "reason": "数据不足"}

        hist = _macd_hist(closes_4h, 12, 26, 9)
        macd_ok = False
        try:
            if hist and len(hist) >= 2 and hist[-1] is not None and hist[-2] is not None:
                h0 = float(hist[-1])
                h1 = float(hist[-2])
                if side == "long" and h0 < 0 and abs(h0) < abs(h1):
                    macd_ok = True
                if side == "short" and h0 > 0 and abs(h0) < abs(h1):
                    macd_ok = True
        except Exception:
            macd_ok = False

        engulf_ok = False
        try:
            o_prev = float(opens_4h[-2])
            c_prev = float(closes_4h[-2])
            h_prev = float(highs_4h[-2])
            l_prev = float(lows_4h[-2])
            o_cur = float(opens_4h[-1])
            c_cur = float(closes_4h[-1])
            if side == "long":
                if c_prev < o_prev and c_cur > o_cur and c_cur > h_prev:
                    engulf_ok = True
            else:
                if c_prev > o_prev and c_cur < o_cur and c_cur < l_prev:
                    engulf_ok = True
        except Exception:
            engulf_ok = False

        if macd_ok and engulf_ok:
            return {"state": f"trigger_{side}", "reason": "4H MACD动能反转 + 吞没形态"}
        if macd_ok:
            return {"state": f"trigger_{side}", "reason": "4H MACD动能反转（柱状图缩短）"}
        if engulf_ok:
            return {"state": f"trigger_{side}", "reason": "4H 吞没形态"}
        return {"state": "none", "reason": "未出现MACD反转/吞没"}

    def _risk_voyage(self, entry: float, side: str, atr_1d: Optional[float], atr_4h: Optional[float]) -> Dict[str, Any]:
        if atr_1d is None or atr_4h is None:
            return {"sl": None, "tp1": None, "tp2": None, "atr_1d": atr_1d, "atr_4h": atr_4h}
        try:
            a1 = float(atr_1d)
            a4 = float(atr_4h)
        except Exception:
            return {"sl": None, "tp1": None, "tp2": None, "atr_1d": None, "atr_4h": None}
        if a1 <= 0 or a4 <= 0:
            return {"sl": None, "tp1": None, "tp2": None, "atr_1d": a1, "atr_4h": a4}
        d = max(2.0 * a1, 1.2 * a4)
        if side == "long":
            sl = entry - d
            r = entry - sl
            tp1 = entry + 2.0 * r
            tp2 = entry + 3.0 * r
        else:
            sl = entry + d
            r = sl - entry
            tp1 = entry - 2.0 * r
            tp2 = entry - 3.0 * r
        return {"sl": float(sl), "tp1": float(tp1), "tp2": float(tp2), "atr_1d": float(a1), "atr_4h": float(a4), "sl_dist": float(d)}

    def analyze_one(self, contract: str) -> Dict[str, Any]:
        now_ts = int(time.time())

        c1d = self._candles(contract, "1d", limit=360)
        c4h = self._candles(contract, "4h", limit=220)
        ts1d, _o1d, h1d, l1d, cl1d = self._series(c1d)
        ts4h, _o4h, h4h, l4h, cl4h = self._series(c4h)

        _ts4h2, o4h, _h4h2, _l4h2, _cl4h2 = self._series(c4h)

        env = self._env_1d_voyage(h1d, l1d, cl1d)
        side = "long" if env.get("state") == "long_only" else ("short" if env.get("state") == "short_only" else "none")

        pre = {"state": "none", "reason": "环境不足"}
        trig = {"state": "none", "reason": "预警未满足，未检测触发"}
        reasons: List[str] = []

        entry: Optional[float] = None
        sl: Optional[float] = None
        tp1: Optional[float] = None
        tp2: Optional[float] = None
        atr_1d_last: Optional[float] = None
        atr_4h_last: Optional[float] = None

        if side in ("long", "short"):
            reasons.append(str(env.get("reason") or ""))
            pre = self._prealert_1d_voyage(cl1d, side=side)
            reasons.append(str(pre.get("reason") or ""))
            if pre.get("state") in ("pre_long", "pre_short"):
                trig = self._trigger_4h_voyage(o4h, h4h, l4h, cl4h, side=side)
                reasons.append(str(trig.get("reason") or ""))
            else:
                trig = {"state": "none", "reason": "预警未满足，未检测触发"}
                reasons.append(str(trig.get("reason") or ""))

            if pre.get("state") in ("pre_long", "pre_short") and trig.get("state") in ("trigger_long", "trigger_short"):
                try:
                    entry = float(cl4h[-1]) if cl4h else None
                except Exception:
                    entry = None
                atrs1 = _atr(h1d, l1d, cl1d, 14)
                if atrs1 and atrs1[-1] is not None:
                    atr_1d_last = float(atrs1[-1])
                atrs4 = _atr(h4h, l4h, cl4h, 14)
                if atrs4 and atrs4[-1] is not None:
                    atr_4h_last = float(atrs4[-1])
                if entry is not None:
                    rk = self._risk_voyage(entry, side=side, atr_1d=atr_1d_last, atr_4h=atr_4h_last)
                    sl = rk.get("sl")
                    tp1 = rk.get("tp1")
                    tp2 = rk.get("tp2")

        return {
            "contract": contract,
            "updated_at": now_ts,
            "side": side,
            "env": env,
            "prealert": pre,
            "trigger": trig,
            "reasons": [x for x in reasons if x],
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "atr": atr_1d_last,
            "atr_1d": atr_1d_last,
            "atr_4h": atr_4h_last,
            "ts": {"1d": (ts1d[-1] if ts1d else None), "4h": (ts4h[-1] if ts4h else None)},
        }

    def matrix(self) -> dict:
        if not MASTER_B_ENABLED:
            return {"items": [], "errors": ["disabled"]}
        ck = f"master_b:matrix:{','.join(self.contracts)}"
        cached = _cache_get(ck, ttl=max(5, int(MASTER_B_CACHE_TTL_SEC)))
        if cached is not None:
            return cached

        errors: List[str] = []
        items: List[dict] = []

        contract_set = set(get_all_futures_contract_names())
        targets = [c for c in self.contracts if c in contract_set]

        max_workers = max(1, min(int(MASTER_B_MAX_WORKERS), max(1, len(targets))))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(self.analyze_one, c): c for c in targets}
            for f in as_completed(futs):
                c = futs[f]
                try:
                    items.append(f.result())
                except Exception as e:
                    errors.append(f"{c}: {e}")

        items.sort(key=lambda x: str(x.get("contract") or ""))
        payload = {"items": items, "errors": errors}
        _cache_set(ck, payload)
        return payload


class MasterAEngine:
    """量化策略 Master A 引擎。

    结构：
    - 1H：环境过滤（Close 相对 EMA200 的多空环境）
    - 1H：预警（TTM Squeeze ON + RSI 回钩）
    - 15M：触发（突破近 3 根的高/低点）
    - 风控：用 1H ATR 计算 SL/TP1/TP2
    """
    def __init__(self, contracts: List[str]):
        self.contracts = contracts

    def _candles(self, contract: str, tf: str, limit: int) -> List[dict]:
        # Reuse the same REST fetcher for Gate perpetual candles
        return get_tri_candles(contract=contract, tf=tf, limit=int(limit))

    def _series(self, candles: List[dict]) -> Tuple[List[int], List[float], List[float], List[float], List[float]]:
        # Keep consistent with SignalEngine
        seq = [x for x in candles if isinstance(x, dict)]
        seq.sort(key=lambda x: int(x.get("t") or 0))
        ts: List[int] = []
        o: List[float] = []
        h: List[float] = []
        l: List[float] = []
        c: List[float] = []
        for x in seq:
            t = x.get("t")
            oo = _safe_float(x.get("o"))
            hh = _safe_float(x.get("h"))
            ll = _safe_float(x.get("l"))
            cc = _safe_float(x.get("c"))
            if t is None or oo is None or hh is None or ll is None or cc is None:
                continue
            try:
                ts.append(int(t))
                o.append(float(oo))
                h.append(float(hh))
                l.append(float(ll))
                c.append(float(cc))
            except Exception:
                continue
        return ts, o, h, l, c

    def _ttm_squeeze_on(self, highs: List[float], lows: List[float], closes: List[float]) -> Optional[bool]:
        # Squeeze ON: BB_Width < KC_Width
        if len(closes) < 50:
            return None
        bb_mid = _sma(closes, 20)
        if not bb_mid or bb_mid[-1] is None:
            return None
        try:
            win = closes[-20:]
            mean = float(bb_mid[-1])
            var = sum((float(x) - mean) ** 2 for x in win) / float(len(win))
            std = var ** 0.5
            bb_upper = mean + 2.0 * std
            bb_lower = mean - 2.0 * std
        except Exception:
            return None

        kc_mid = _ema(closes, 20)
        atr20 = _atr(highs, lows, closes, 20)
        if not kc_mid or not atr20 or kc_mid[-1] is None or atr20[-1] is None:
            return None
        try:
            m = float(kc_mid[-1])
            a = float(atr20[-1])
            kc_upper = m + 1.5 * a
            kc_lower = m - 1.5 * a
            return bool(bb_upper < kc_upper and bb_lower > kc_lower)
        except Exception:
            return None

    def _prealert(self, highs: List[float], lows: List[float], closes: List[float], side: str) -> Dict[str, Any]:
        # side: long | short
        if len(closes) < 220:
            return {"state": "none", "reason": "数据不足", "squeeze": None, "rsi": None}

        squeeze = self._ttm_squeeze_on(highs, lows, closes)
        rsi_last = _rsi14(closes)
        rsi_prev = _rsi14(closes[:-1]) if len(closes) >= 16 else None
        if squeeze is None or rsi_last is None or rsi_prev is None:
            return {"state": "none", "reason": "指标不足", "squeeze": squeeze, "rsi": float(rsi_last) if isinstance(rsi_last, (int, float)) else None}

        try:
            if not squeeze:
                return {"state": "none", "reason": "未挤压", "squeeze": bool(squeeze), "rsi": float(rsi_last)}

            if side == "long":
                # oversold hook: RSI 处于超卖附近且开始回升
                if float(rsi_prev) < 40.0 and float(rsi_last) > float(rsi_prev) and float(rsi_last) >= 38.0:
                    return {"state": "pre_long", "reason": "Squeeze ON + RSI超卖回钩（prev<40 且回升且>=38）", "squeeze": True, "rsi": float(rsi_last)}
            else:
                # overbought hook: RSI 处于超买附近且开始回落
                if float(rsi_prev) > 60.0 and float(rsi_last) < float(rsi_prev) and float(rsi_last) <= 62.0:
                    return {"state": "pre_short", "reason": "Squeeze ON + RSI超买回钩（prev>60 且回落且<=62）", "squeeze": True, "rsi": float(rsi_last)}
        except Exception:
            pass

        return {"state": "none", "reason": "未满足回钩", "squeeze": bool(squeeze), "rsi": float(rsi_last) if isinstance(rsi_last, (int, float)) else None}

    def _env(self, closes_1h: List[float]) -> Dict[str, Any]:
        if len(closes_1h) < 220:
            return {"state": "none", "reason": "数据不足", "ema200": None}
        ema200 = _ema(closes_1h, 200)
        if not ema200 or ema200[-1] is None:
            return {"state": "none", "reason": "EMA不足", "ema200": None}
        last = closes_1h[-1]
        e200 = float(ema200[-1])
        if last > e200:
            return {"state": "long_only", "reason": "Close>EMA200（只找做多）", "ema200": e200}
        return {"state": "short_only", "reason": "Close<EMA200（只找做空）", "ema200": e200}

    def _trigger_15m(self, highs_15m: List[float], lows_15m: List[float], closes_15m: List[float], side: str) -> Dict[str, Any]:
        if len(closes_15m) < 20:
            return {"state": "none", "reason": "数据不足"}
        # breakout previous 3 closed candles
        try:
            last_close = float(closes_15m[-1])
            prev_high3 = max(float(x) for x in highs_15m[-4:-1])
            prev_low3 = min(float(x) for x in lows_15m[-4:-1])
            if side == "long" and last_close > prev_high3:
                return {"state": "trigger_long", "reason": "15M 突破近3根高点", "break": prev_high3}
            if side == "short" and last_close < prev_low3:
                return {"state": "trigger_short", "reason": "15M 跌破近3根低点", "break": prev_low3}
        except Exception:
            return {"state": "none", "reason": "计算失败"}
        return {"state": "none", "reason": "未突破"}

    def _risk(self, entry: float, side: str, atr_1h: Optional[float]) -> Dict[str, Any]:
        if atr_1h is None or not isinstance(atr_1h, (int, float)) or float(atr_1h) <= 0:
            return {"sl": None, "tp1": None, "tp2": None, "atr": None}
        a = float(atr_1h)
        if side == "long":
            sl = entry - 1.5 * a
            r = entry - sl
            tp1 = entry + 2.0 * r
            tp2 = entry + 3.0 * r
        else:
            sl = entry + 1.5 * a
            r = sl - entry
            tp1 = entry - 2.0 * r
            tp2 = entry - 3.0 * r
        return {"sl": float(sl), "tp1": float(tp1), "tp2": float(tp2), "atr": float(a)}

    def analyze_one(self, contract: str) -> Dict[str, Any]:
        now_ts = int(time.time())

        c1h = self._candles(contract, "1h", limit=320)
        c15 = self._candles(contract, "15m", limit=160)
        ts1h, _o1h, h1h, l1h, cl1h = self._series(c1h)
        ts15, _o15, h15, l15, cl15 = self._series(c15)

        env = self._env(cl1h)
        side = "long" if env.get("state") == "long_only" else ("short" if env.get("state") == "short_only" else "none")

        pre = {"state": "none", "reason": "环境不足", "squeeze": None, "rsi": None}
        trig = {"state": "none", "reason": "预警未满足，未检测触发"}
        reasons: List[str] = []

        entry: Optional[float] = None
        sl: Optional[float] = None
        tp1: Optional[float] = None
        tp2: Optional[float] = None
        atr_last: Optional[float] = None

        if side in ("long", "short"):
            reasons.append(str(env.get("reason") or ""))
            pre = self._prealert(h1h, l1h, cl1h, side=side)
            reasons.append(str(pre.get("reason") or ""))
            if pre.get("state") in ("pre_long", "pre_short"):
                trig = self._trigger_15m(h15, l15, cl15, side=side)
                reasons.append(str(trig.get("reason") or ""))
            else:
                trig = {"state": "none", "reason": "预警未满足，未检测触发"}
                reasons.append(str(trig.get("reason") or ""))

            if pre.get("state") in ("pre_long", "pre_short") and trig.get("state") in ("trigger_long", "trigger_short"):
                try:
                    entry = float(cl15[-1]) if cl15 else None
                except Exception:
                    entry = None

                atrs = _atr(h1h, l1h, cl1h, 14)
                if atrs and atrs[-1] is not None:
                    atr_last = float(atrs[-1])
                if entry is not None:
                    rk = self._risk(entry, side=side, atr_1h=atr_last)
                    sl = rk.get("sl")
                    tp1 = rk.get("tp1")
                    tp2 = rk.get("tp2")

        # Normalize reasons
        reasons = [x for x in reasons if x]
        if not reasons:
            reasons = ["—"]

        return {
            "contract": contract,
            "symbol": contract.replace("_USDT", ""),
            "updated_at": now_ts,
            "side": side,
            "env": env,
            "prealert": pre,
            "trigger": trig,
            "entry": float(entry) if isinstance(entry, (int, float)) else None,
            "sl": float(sl) if isinstance(sl, (int, float)) else None,
            "tp1": float(tp1) if isinstance(tp1, (int, float)) else None,
            "tp2": float(tp2) if isinstance(tp2, (int, float)) else None,
            "atr_1h": float(atr_last) if isinstance(atr_last, (int, float)) else None,
            "reasons": reasons,
            "ts": {"1h": (ts1h[-1] if ts1h else None), "15m": (ts15[-1] if ts15 else None)},
        }

    def matrix(self) -> dict:
        if not MASTER_A_ENABLED:
            return {"items": [], "errors": ["disabled"]}
        ck = f"master_a:matrix:{','.join(self.contracts)}"
        cached = _cache_get(ck, ttl=max(5, int(MASTER_A_CACHE_TTL_SEC)))
        if cached is not None:
            return cached

        errors: List[str] = []
        items: List[dict] = []

        contract_set = set(get_all_futures_contract_names())
        targets = [c for c in self.contracts if c in contract_set]

        max_workers = max(1, min(int(MASTER_A_MAX_WORKERS), max(1, len(targets))))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(self.analyze_one, c): c for c in targets}
            for f in as_completed(futs):
                c = futs[f]
                try:
                    items.append(f.result())
                except Exception as e:
                    errors.append(f"{c}: {e}")

        items.sort(key=lambda x: str(x.get("contract") or ""))
        payload = {"items": items, "errors": errors}
        _cache_set(ck, payload)
        return payload


_TRI_ENGINE = TriSignalEngine(_parse_contracts_csv(TRI_SIGNAL_CONTRACTS))

_MASTER_A_ENGINE = MasterAEngine(_parse_contracts_csv(MASTER_A_CONTRACTS))

_MASTER_B_ENGINE = MasterBEngine(_parse_contracts_csv(MASTER_B_CONTRACTS))


@app.get("/api/tri_signal/matrix")
def api_tri_signal_matrix() -> JSONResponse:
    try:
        return JSONResponse(_TRI_ENGINE.matrix())
    except Exception as e:
        return JSONResponse({"items": [], "errors": [str(e)]}, status_code=200)


@app.get("/api/tri_signal/candles")
def api_tri_signal_candles(contract: str = "BTC_USDT", tf: str = "1h", limit: int = 200) -> JSONResponse:
    try:
        contract = (contract or "BTC_USDT").strip().upper()
        tf = (tf or "1h").strip()
        if tf not in ("1h", "4h", "1d"):
            tf = "1h"
        limit = max(50, min(500, int(limit)))
        candles = _TRI_ENGINE._candles(contract, tf, limit=limit)
        ts, o, h, l, c = _TRI_ENGINE._series(candles)
        payload = {
            "contract": contract,
            "tf": tf,
            "items": [{"t": ts[i], "o": o[i], "h": h[i], "l": l[i], "c": c[i]} for i in range(min(len(ts), len(c)))],
        }
        return JSONResponse(payload)
    except Exception as e:
        return JSONResponse({"contract": contract, "tf": tf, "items": [], "errors": [str(e)]}, status_code=200)


@app.get("/api/master_a/matrix")
def api_master_a_matrix() -> JSONResponse:
    try:
        return JSONResponse(_MASTER_A_ENGINE.matrix())
    except Exception as e:
        return JSONResponse({"items": [], "errors": [str(e)]}, status_code=200)


@app.get("/api/master_a/candles")
def api_master_a_candles(contract: str = "BTC_USDT", tf: str = "1h", limit: int = 260) -> JSONResponse:
    try:
        contract = (contract or "BTC_USDT").strip().upper()
        tf = (tf or "1h").strip()
        if tf not in ("1h", "15m"):
            tf = "1h"
        limit = max(50, min(600, int(limit)))
        candles = _MASTER_A_ENGINE._candles(contract, tf, limit=limit)
        ts, o, h, l, c = _MASTER_A_ENGINE._series(candles)
        payload = {
            "contract": contract,
            "tf": tf,
            "items": [{"t": ts[i], "o": o[i], "h": h[i], "l": l[i], "c": c[i]} for i in range(min(len(ts), len(c)))],
        }
        return JSONResponse(payload)
    except Exception as e:
        return JSONResponse({"contract": contract, "tf": tf, "items": [], "errors": [str(e)]}, status_code=200)


@app.get("/api/master_b/matrix")
def api_master_b_matrix() -> JSONResponse:
    try:
        return JSONResponse(_MASTER_B_ENGINE.matrix())
    except Exception as e:
        return JSONResponse({"items": [], "errors": [str(e)]}, status_code=200)


@app.get("/api/master_b/candles")
def api_master_b_candles(contract: str = "BTC_USDT", tf: str = "4h", limit: int = 260) -> JSONResponse:
    try:
        contract = (contract or "BTC_USDT").strip().upper()
        tf = (tf or "4h").strip()
        if tf not in ("1d", "4h"):
            tf = "4h"
        limit = max(50, min(600, int(limit)))
        candles = _MASTER_B_ENGINE._candles(contract, tf, limit=limit)
        ts, o, h, l, c = _MASTER_B_ENGINE._series(candles)
        payload = {
            "contract": contract,
            "tf": tf,
            "items": [{"t": ts[i], "o": o[i], "h": h[i], "l": l[i], "c": c[i]} for i in range(min(len(ts), len(c)))],
        }
        return JSONResponse(payload)
    except Exception as e:
        return JSONResponse({"contract": contract, "tf": tf, "items": [], "errors": [str(e)]}, status_code=200)


@app.get("/api/master_a_push/now")
def api_master_a_push_now(force: int = 0) -> JSONResponse:
    try:
        out = push_tg_master_a(force=force)
        return JSONResponse(out)
    except Exception as e:
        return JSONResponse({"ok": False, "pushed": 0, "skipped": 0, "errors": [str(e)]}, status_code=200)


@app.get("/api/master_b_push/now")
def api_master_b_push_now(force: int = 0) -> JSONResponse:
    try:
        out = push_tg_master_b(force=force)
        return JSONResponse(out)
    except Exception as e:
        return JSONResponse({"ok": False, "pushed": 0, "skipped": 0, "errors": [str(e)]}, status_code=200)


_TRI_SIGNAL_PUSH_THREAD: Optional[threading.Thread] = None
_TRI_SIGNAL_PUSH_THREAD_LOCK = threading.Lock()
_TRI_SIGNAL_PUSH_LAST_RUN_TS: Optional[int] = None
_TRI_SIGNAL_PUSH_LAST_PUSH: Optional[dict] = None
_TRI_SIGNAL_PUSH_LAST_ERROR: str = ""


def _tri_signal_push_history_add(
    uniq: str,
    contract: str,
    side: str,
    grade: str,
    high_prob: bool,
    reasons: List[str],
    entry: Optional[float],
    sl: Optional[float],
    tp: Optional[float],
    atr: Optional[float],
    message: str,
    ok: bool,
    error: str,
) -> None:
    conn = _db_connect()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO tri_signal_push_history(created_at, uniq, contract, side, grade, high_prob, reasons, entry, sl, tp, atr, message, ok, error)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(time.time()),
                uniq,
                contract,
                side,
                grade,
                1 if high_prob else 0,
                json.dumps(reasons, ensure_ascii=False),
                entry,
                sl,
                tp,
                atr,
                message,
                1 if ok else 0,
                error or "",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _tri_signal_last_push_ts(contract: str, side: str) -> Optional[int]:
    conn = _db_connect()
    try:
        row = conn.execute(
            """
            SELECT created_at FROM tri_signal_push_history
            WHERE contract=? AND side=?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (contract, side),
        ).fetchone()
        if not row:
            return None
        try:
            return int(row[0])
        except Exception:
            return None
    finally:
        conn.close()


def _tri_signal_has_uniq(uniq: str) -> bool:
    conn = _db_connect()
    try:
        row = conn.execute("SELECT 1 FROM tri_signal_push_history WHERE uniq=? LIMIT 1", (uniq,)).fetchone()
        return bool(row)
    finally:
        conn.close()


def _master_a_push_history_add(
    uniq: str,
    contract: str,
    side: str,
    reasons: List[str],
    entry: Optional[float],
    sl: Optional[float],
    tp1: Optional[float],
    tp2: Optional[float],
    atr: Optional[float],
    message: str,
    ok: bool,
    error: str,
) -> None:
    conn = _db_connect()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO master_a_push_history(created_at, uniq, contract, side, reasons, entry, sl, tp1, tp2, atr, message, ok, error)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(time.time()),
                uniq,
                contract,
                side,
                json.dumps(reasons, ensure_ascii=False),
                entry,
                sl,
                tp1,
                tp2,
                atr,
                message,
                1 if ok else 0,
                error or "",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _master_a_has_uniq(uniq: str) -> bool:
    uniq = (uniq or "").strip()
    if not uniq:
        return False
    conn = _db_connect()
    try:
        row = conn.execute("SELECT 1 FROM master_a_push_history WHERE uniq=? LIMIT 1", (uniq,)).fetchone()
        return bool(row)
    finally:
        conn.close()


def _master_a_last_push_ts(contract: str, side: str) -> Optional[int]:
    conn = _db_connect()
    try:
        row = conn.execute(
            """
            SELECT created_at FROM master_a_push_history
            WHERE contract=? AND side=?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (contract, side),
        ).fetchone()
        if not row:
            return None
        try:
            return int(row[0])
        except Exception:
            return None
    finally:
        conn.close()


def push_tg_tri_signal(force: int = 0) -> dict:
    s = _news_settings()
    bot_token = (s.get("tg_bot_token") or "").strip()
    chat_id = (s.get("tg_chat_id") or "").strip()
    if not bot_token or not chat_id:
        return {"ok": False, "pushed": 0, "skipped": 0, "errors": ["未配置 Telegram Bot Token 或 Chat ID"]}

    data = _TRI_ENGINE.matrix()
    items = data.get("items") if isinstance(data, dict) else None
    if not isinstance(items, list):
        return {"ok": False, "pushed": 0, "skipped": 0, "errors": ["invalid tri matrix"]}

    now_ts = int(time.time())
    bucket = int(now_ts / max(60, int(TRI_SIGNAL_PUSH_INTERVAL_SEC)))

    pushed = 0
    skipped = 0
    errors: List[str] = []

    # 只推：1H 触发信号 + 高胜率(1D 同向)
    candidates: List[dict] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        hourly = it.get("hourly") if isinstance(it.get("hourly"), dict) else {}
        side = str(hourly.get("signal") or "none")
        if side not in ("long", "short"):
            continue
        if not bool(it.get("high_prob")):
            continue
        grade = str(it.get("grade") or "C")
        if TRI_SIGNAL_PUSH_ONLY_GRADE_A and grade != "A":
            continue
        candidates.append(it)

    if not candidates:
        return {"ok": True, "pushed": 0, "skipped": 0, "errors": []}

    # 合并成一条消息（避免刷屏）
    ts_txt = datetime.datetime.fromtimestamp(now_ts).strftime("%Y-%m-%d %H:%M")
    header = f"<b>三周期信号｜高胜率触发</b>\n时间：{ts_txt}｜合约数：{len(candidates)}"
    lines: List[str] = [header]

    def _fmt(v: Any) -> str:
        try:
            if v is None:
                return "—"
            x = float(v)
            if abs(x) >= 1000:
                return f"{x:,.2f}"
            return f"{x:.6g}"
        except Exception:
            return "—"

    will_log: List[dict] = []
    for it in candidates[:20]:
        try:
            contract = str(it.get("contract") or "").strip()
            hourly = it.get("hourly") if isinstance(it.get("hourly"), dict) else {}
            monthly = it.get("monthly") if isinstance(it.get("monthly"), dict) else {}
            daily = it.get("daily") if isinstance(it.get("daily"), dict) else {}

            side = str(hourly.get("signal") or "none")
            grade = str(it.get("grade") or "C")
            uniq = f"tri:{contract}:{side}:{grade}:{bucket}"
            if not force and _tri_signal_has_uniq(uniq):
                skipped += 1
                continue
            if not force:
                last_ts = _tri_signal_last_push_ts(contract, side)
                if last_ts is not None and (now_ts - int(last_ts)) < int(TRI_SIGNAL_PUSH_COOLDOWN_SEC):
                    skipped += 1
                    continue

            entry = hourly.get("entry")
            sl = hourly.get("sl")
            tp = hourly.get("tp")
            atr = hourly.get("atr")

            reason_1m = str(monthly.get("reason") or "").strip()
            reason_1d = str(daily.get("reason") or "").strip()
            reason_1h = str(hourly.get("reason") or "").strip()
            reasons = [x for x in [reason_1m, reason_1d, reason_1h] if x]
            rs_txt = " | ".join([f"{x}" for x in reasons[:3]])

            line = f"- {contract}  <b>{side.upper()}</b>  <b>Grade {grade}</b>\n  Entry:{_fmt(entry)} SL:{_fmt(sl)} TP:{_fmt(tp)} ATR:{_fmt(atr)}"
            if rs_txt:
                line += f"\n  {rs_txt}"
            lines.append(line)
            will_log.append({
                "uniq": uniq,
                "contract": contract,
                "side": side,
                "grade": grade,
                "high_prob": True,
                "reasons": reasons,
                "entry": _safe_float(entry),
                "sl": _safe_float(sl),
                "tp": _safe_float(tp),
                "atr": _safe_float(atr),
            })
        except Exception:
            skipped += 1

    if not will_log:
        return {"ok": True, "pushed": 0, "skipped": skipped, "errors": []}

    msg = "\n".join(lines)
    if len(msg) > 3500:
        msg = msg[:3500] + "\n…(truncated)"

    ok, err = _tg_send(bot_token=bot_token, chat_id=chat_id, text=msg, parse_mode="HTML")

    for x in will_log:
        try:
            _tri_signal_push_history_add(
                uniq=str(x.get("uniq") or ""),
                contract=str(x.get("contract") or ""),
                side=str(x.get("side") or ""),
                grade=str(x.get("grade") or ""),
                high_prob=bool(x.get("high_prob")),
                reasons=x.get("reasons") if isinstance(x.get("reasons"), list) else [],
                entry=_safe_float(x.get("entry")),
                sl=_safe_float(x.get("sl")),
                tp=_safe_float(x.get("tp")),
                atr=_safe_float(x.get("atr")),
                message=msg,
                ok=ok,
                error=err,
            )
        except Exception:
            pass

    if ok:
        pushed = len(will_log)
    else:
        errors.append(err or "send failed")

    return {"ok": ok, "pushed": pushed, "skipped": skipped, "errors": errors}


def _tri_signal_push_loop() -> None:
    interval = max(60, min(3600, int(TRI_SIGNAL_PUSH_INTERVAL_SEC)))
    while True:
        try:
            s = _news_settings()
            enabled_mod = _setting_bool(s, "push_tri_signal_enabled", True)
            if TRI_SIGNAL_PUSH_ENABLED and enabled_mod:
                global _TRI_SIGNAL_PUSH_LAST_RUN_TS, _TRI_SIGNAL_PUSH_LAST_PUSH, _TRI_SIGNAL_PUSH_LAST_ERROR
                _TRI_SIGNAL_PUSH_LAST_RUN_TS = int(time.time())
                _TRI_SIGNAL_PUSH_LAST_ERROR = ""
                _TRI_SIGNAL_PUSH_LAST_PUSH = push_tg_tri_signal(force=0)
        except Exception as e:
            try:
                _TRI_SIGNAL_PUSH_LAST_ERROR = str(e)
            except Exception:
                pass
        time.sleep(interval)


@app.get("/api/tri_signal_push/auto_status")
def api_tri_signal_push_auto_status() -> JSONResponse:
    alive = False
    name = None
    try:
        alive = bool(_TRI_SIGNAL_PUSH_THREAD is not None and _TRI_SIGNAL_PUSH_THREAD.is_alive())
        name = _TRI_SIGNAL_PUSH_THREAD.name if _TRI_SIGNAL_PUSH_THREAD is not None else None
    except Exception:
        alive = False
        name = None
    s = _news_settings()
    payload = {
        "enabled_env": bool(TRI_SIGNAL_PUSH_ENABLED),
        "interval_sec": int(TRI_SIGNAL_PUSH_INTERVAL_SEC),
        "cooldown_sec": int(TRI_SIGNAL_PUSH_COOLDOWN_SEC),
        "only_grade_a": bool(TRI_SIGNAL_PUSH_ONLY_GRADE_A),
        "thread_alive": alive,
        "thread_name": name,
        "enabled_mod": _setting_bool(s, "push_tri_signal_enabled", True),
        "has_bot_token": bool((s.get("tg_bot_token") or "").strip()),
        "has_chat_id": bool((s.get("tg_chat_id") or "").strip()),
        "last_run_ts": _TRI_SIGNAL_PUSH_LAST_RUN_TS,
        "last_error": _TRI_SIGNAL_PUSH_LAST_ERROR,
        "last_push": _TRI_SIGNAL_PUSH_LAST_PUSH,
    }
    return JSONResponse(payload)


@app.get("/api/telegram/push_history")
def api_telegram_push_history(limit: int = 100) -> JSONResponse:
    limit = max(1, min(100, int(limit)))
    conn = _db_connect()
    try:
        items: List[dict] = []

        # news
        rows1 = conn.execute(
            """
            SELECT created_at, uniq, level, title, link, message, ok, error
            FROM news_push_history
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        for r in rows1:
            d = dict(r)
            d["module"] = "news"
            items.append(d)

        # signal dashboard
        rows2 = conn.execute(
            """
            SELECT created_at, uniq, level, contract AS title, '' AS link, message, ok, error
            FROM signal_push_history
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        for r in rows2:
            d = dict(r)
            d["module"] = "signal"
            items.append(d)

        # tri signal
        rows3 = conn.execute(
            """
            SELECT created_at, uniq,
                   ('tri_' || grade || '_' || UPPER(side)) AS level,
                   contract AS title,
                   '' AS link,
                   message,
                   ok,
                   error
            FROM tri_signal_push_history
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        for r in rows3:
            d = dict(r)
            d["module"] = "tri_signal"
            items.append(d)

        # whales alert
        try:
            rows4 = conn.execute(
                """
                SELECT created_at, uniq,
                       ('whales_' || UPPER(direction)) AS level,
                       (chain || ' ' || asset || ' $' || printf('%.0f', COALESCE(amount_usd,0))) AS title,
                       explorer_url AS link,
                       message,
                       ok,
                       error
                FROM whale_alert_history
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            for r in rows4:
                d = dict(r)
                d["module"] = "whales"
                items.append(d)
        except Exception:
            pass

        items.sort(key=lambda x: int(x.get("created_at") or 0), reverse=True)
        items = items[:limit]
        return JSONResponse({"items": items})
    finally:
        conn.close()


_SIGNAL_PUSH_THREAD: Optional[threading.Thread] = None
_SIGNAL_PUSH_THREAD_LOCK = threading.Lock()
_SIGNAL_PUSH_LAST_RUN_TS: Optional[int] = None
_SIGNAL_PUSH_LAST_PUSH: Optional[dict] = None
_SIGNAL_PUSH_LAST_ERROR: str = ""


def _signal_push_history_add(uniq: str, symbol: str, contract: str, level: str, score: float, reasons: List[str], message: str, ok: bool, error: str) -> None:
    conn = _db_connect()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO signal_push_history(created_at, uniq, symbol, contract, level, score, reasons, message, ok, error)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(time.time()),
                uniq,
                symbol,
                contract,
                level,
                float(score),
                json.dumps(reasons, ensure_ascii=False),
                message,
                1 if ok else 0,
                error or "",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _signal_last_strong_level(symbol: str) -> Optional[str]:
    conn = _db_connect()
    try:
        row = conn.execute(
            """
            SELECT level FROM signal_push_history
            WHERE symbol=? AND level IN ('strong_long','strong_short')
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (symbol,),
        ).fetchone()
        if not row:
            return None
        return (row[0] or "").strip() or None
    finally:
        conn.close()


def push_tg_signal_strong(force: int = 0) -> dict:
    s = _news_settings()
    bot_token = (s.get("tg_bot_token") or "").strip()
    chat_id = (s.get("tg_chat_id") or "").strip()
    if not bot_token or not chat_id:
        return {"ok": False, "pushed": 0, "skipped": 0, "errors": ["未配置 Telegram Bot Token 或 Chat ID"]}

    # 拉取强信号
    k_tf = (SIGNAL_PUSH_K_TF or "1h").strip() or "1h"
    data_json = build_signal_dashboard(mode="top100", limit=int(SIGNAL_DASHBOARD_TOPN), only_strong=1, sort="score", k_tf=k_tf)
    items = data_json.get("items") if isinstance(data_json, dict) else None
    if not isinstance(items, list):
        return {"ok": False, "pushed": 0, "skipped": 0, "errors": ["invalid dashboard response"]}

    pushed = 0
    skipped = 0
    errors: List[str] = []
    now_ts = int(time.time())
    bucket = int(now_ts / max(60, int(SIGNAL_PUSH_INTERVAL_SEC)))

    def _fmt_pct(v: Any) -> str:
        try:
            return f"{float(v):+.2f}%" if v is not None else "—"
        except Exception:
            return "—"

    def _fmt_num(v: Any) -> str:
        try:
            if v is None:
                return "—"
            x = float(v)
            if abs(x) >= 1000:
                return f"{x:,.2f}"
            return f"{x:.6g}"
        except Exception:
            return str(v) if v is not None else "—"

    def _fmt_funding(v: Any) -> str:
        try:
            return f"{float(v) * 100:.4f}%" if v is not None else "—"
        except Exception:
            return "—"

    will_push: List[dict] = []
    for it in items[:50]:
        try:
            symbol = str(it.get("symbol") or "").upper().strip()
            contract = str(it.get("contract") or "").strip()
            level = str(it.get("level") or "").strip()
            score = float(it.get("score") or 0.0)
            if level not in ("strong_long", "strong_short"):
                continue
            if abs(score) < float(SIGNAL_PUSH_SCORE_STRONG):
                continue

            uniq = f"signal:{symbol}:{level}:{bucket}"
            if not force and _signal_has_uniq(uniq):
                skipped += 1
                continue

            prev = _signal_last_strong_level(symbol)
            if not force and (not SIGNAL_PUSH_REPEAT_SAME_DIRECTION) and prev == level:
                skipped += 1
                continue

            if not force:
                last_ts = _signal_last_push_ts(symbol)
                if last_ts is not None and (now_ts - int(last_ts)) < int(SIGNAL_PUSH_COOLDOWN_SEC):
                    skipped += 1
                    continue

            will_push.append({"it": it, "symbol": symbol, "contract": contract, "level": level, "score": score, "uniq": uniq})
        except Exception:
            skipped += 1

    if not will_push:
        return {"ok": True, "pushed": 0, "skipped": skipped, "errors": []}

    longs = [x for x in will_push if x.get("level") == "strong_long"]
    shorts = [x for x in will_push if x.get("level") == "strong_short"]
    longs.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
    shorts.sort(key=lambda x: float(x.get("score") or 0.0))

    ts_txt = datetime.datetime.fromtimestamp(now_ts).strftime("%Y-%m-%d %H:%M")
    header = f"<b>多空综合雷达｜强信号汇总</b>\n时间：{ts_txt}｜K线：{k_tf}\n强多：{len(longs)}｜强空：{len(shorts)}"

    def _line(x: dict) -> str:
        it0 = x.get("it") if isinstance(x.get("it"), dict) else {}
        contract0 = str(x.get("contract") or "")
        score0 = float(x.get("score") or 0.0)
        price0 = it0.get("price")
        pct_tf = it0.get("pct_tf")
        oi_tf = it0.get("oi_change_tf")
        funding0 = it0.get("funding")
        rs = it0.get("reasons") if isinstance(it0.get("reasons"), list) else []
        rs_txt = "，".join([str(r) for r in rs[:2] if r is not None])
        return (
            f"{contract0}  <b>{score0:+.2f}</b>｜价:{_fmt_num(price0)} ｜ 价Δ:{_fmt_pct(pct_tf)} ｜ OIΔ:{_fmt_pct(oi_tf)}｜"
            + (f"\n  {rs_txt}" if rs_txt else "")
        )

    parts: List[str] = [header]
    if longs:
        parts.append("\n<b>🟢 强多</b>")
        for x in longs[:25]:
            parts.append(_line(x))
    if shorts:
        parts.append("\n<b>🔴 强空</b>")
        for x in shorts[:25]:
            parts.append(_line(x))

    msg = "\n".join(parts)
    if len(msg) > 3500:
        msg = msg[:3500] + "\n…(truncated)"

    ok, err = _tg_send(bot_token=bot_token, chat_id=chat_id, text=msg, parse_mode="HTML")

    for x in will_push:
        it0 = x.get("it") if isinstance(x.get("it"), dict) else {}
        rs = it0.get("reasons") if isinstance(it0.get("reasons"), list) else []
        try:
            _signal_push_history_add(
                uniq=str(x.get("uniq") or ""),
                symbol=str(x.get("symbol") or ""),
                contract=str(x.get("contract") or ""),
                level=str(x.get("level") or ""),
                score=float(x.get("score") or 0.0),
                reasons=rs,
                message=msg,
                ok=ok,
                error=err,
            )
        except Exception:
            pass

    if ok:
        pushed = len(will_push)
    else:
        errors.append(err or "send failed")
    return {"ok": ok, "pushed": pushed, "skipped": skipped, "errors": errors}


def _signal_push_loop() -> None:
    interval = max(60, min(3600, int(SIGNAL_PUSH_INTERVAL_SEC)))
    while True:
        try:
            s = _news_settings()
            enabled_mod = _setting_bool(s, "push_signal_enabled", True)
            if SIGNAL_PUSH_ENABLED and enabled_mod:
                global _SIGNAL_PUSH_LAST_RUN_TS, _SIGNAL_PUSH_LAST_PUSH, _SIGNAL_PUSH_LAST_ERROR
                _SIGNAL_PUSH_LAST_RUN_TS = int(time.time())
                _SIGNAL_PUSH_LAST_ERROR = ""
                _SIGNAL_PUSH_LAST_PUSH = push_tg_signal_strong(force=0)
        except Exception as e:
            try:
                _SIGNAL_PUSH_LAST_ERROR = str(e)
            except Exception:
                pass
        time.sleep(interval)


@app.get("/api/signal_push/auto_status")
def api_signal_push_auto_status() -> JSONResponse:
    alive = False
    name = None
    try:
        alive = bool(_SIGNAL_PUSH_THREAD is not None and _SIGNAL_PUSH_THREAD.is_alive())
        name = _SIGNAL_PUSH_THREAD.name if _SIGNAL_PUSH_THREAD is not None else None
    except Exception:
        alive = False
        name = None
    s = _news_settings()
    payload = {
        "enabled_env": bool(SIGNAL_PUSH_ENABLED),
        "interval_sec": int(SIGNAL_PUSH_INTERVAL_SEC),
        "score_strong": float(SIGNAL_PUSH_SCORE_STRONG),
        "cooldown_sec": int(SIGNAL_PUSH_COOLDOWN_SEC),
        "repeat_same_direction": bool(SIGNAL_PUSH_REPEAT_SAME_DIRECTION),
        "k_tf": (SIGNAL_PUSH_K_TF or "1h").strip() or "1h",
        "thread_alive": alive,
        "thread_name": name,
        "has_bot_token": bool((s.get("tg_bot_token") or "").strip()),
        "has_chat_id": bool((s.get("tg_chat_id") or "").strip()),
        "last_run_ts": _SIGNAL_PUSH_LAST_RUN_TS,
        "last_error": _SIGNAL_PUSH_LAST_ERROR,
        "last_push": _SIGNAL_PUSH_LAST_PUSH,
    }
    return JSONResponse(payload)


@app.get("/api/signal_push/push_now")
def api_signal_push_now(force: int = 1) -> JSONResponse:
    s = _news_settings()
    if not _setting_bool(s, "push_signal_enabled", True):
        return JSONResponse({"ok": True, "pushed": 0, "skipped": 0, "errors": ["disabled_by_settings"]})
    out = push_tg_signal_strong(force=int(force))
    return JSONResponse(out)


@app.on_event("startup")
def _startup() -> None:
    _db_init()

    # 后台定时抓取/分析/合并推送（不依赖前端）
    if NEWS_AUTO_PUSH_ENABLED:
        global _NEWS_AUTO_THREAD
        with _NEWS_AUTO_THREAD_LOCK:
            if _NEWS_AUTO_THREAD is None or not _NEWS_AUTO_THREAD.is_alive():
                t = threading.Thread(target=_news_auto_loop, name="news_auto_push", daemon=True)
                _NEWS_AUTO_THREAD = t
                t.start()

    # 重启后立即推送一次（不阻塞启动）：新闻
    if NEWS_AUTO_PUSH_ENABLED:
        def _news_startup_push_once() -> None:
            try:
                s = _news_settings()
                enabled = _setting_bool(s, "push_enabled", True)
                enabled_mod = _setting_bool(s, "push_news_enabled", True)
                bot_token = (s.get("tg_bot_token") or "").strip()
                chat_id = (s.get("tg_chat_id") or "").strip()
                if enabled and enabled_mod and bot_token and chat_id:
                    global _NEWS_AUTO_LAST_RUN_TS, _NEWS_AUTO_LAST_REFRESH, _NEWS_AUTO_LAST_ANALYZE, _NEWS_AUTO_LAST_PUSH, _NEWS_AUTO_LAST_ERROR
                    _NEWS_AUTO_LAST_RUN_TS = int(time.time())
                    _NEWS_AUTO_LAST_ERROR = ""
                    _NEWS_AUTO_LAST_REFRESH = refresh_news(max_per_feed=NEWS_AUTO_PUSH_MAX_PER_FEED)
                    _NEWS_AUTO_LAST_ANALYZE = analyze_pending_news(limit=NEWS_AUTO_PUSH_ANALYZE_LIMIT)
                    _NEWS_AUTO_LAST_PUSH = push_telegram_batch_recent(
                        window_sec=NEWS_AUTO_PUSH_WINDOW_SEC,
                        limit=NEWS_AUTO_PUSH_ANALYZE_LIMIT,
                        max_items_in_msg=NEWS_AUTO_PUSH_MAX_ITEMS_IN_MSG,
                    )
            except Exception as e:
                try:
                    _NEWS_AUTO_LAST_ERROR = str(e)
                except Exception:
                    pass

        threading.Thread(target=_news_startup_push_once, name="news_startup_push_once", daemon=True).start()

    # MACD 预警：后台定时合并推送（默认每30分钟）
    if MACD_PREALERT_PUSH_ENABLED:
        global _MACD_PREALERT_THREAD
        with _MACD_PREALERT_THREAD_LOCK:
            if _MACD_PREALERT_THREAD is None or not _MACD_PREALERT_THREAD.is_alive():
                t2 = threading.Thread(target=_macd_prealert_push_loop, name="macd_prealert_push", daemon=True)
                _MACD_PREALERT_THREAD = t2
                t2.start()

        # 重启后立即推送一次（不阻塞启动）：MACD 预警（force=1 绕过节流）
        def _macd_prealert_startup_push_once() -> None:
            try:
                s = _news_settings()
                if not _setting_bool(s, "push_macd_prealert_enabled", True):
                    return
                global _MACD_PREALERT_LAST_RUN_TS, _MACD_PREALERT_LAST_PUSH, _MACD_PREALERT_LAST_ERROR
                _MACD_PREALERT_LAST_RUN_TS = int(time.time())
                _MACD_PREALERT_LAST_ERROR = ""
                _MACD_PREALERT_LAST_PUSH = push_tg_macd_prealerts(
                    topn=MACD_PREALERT_PUSH_TOPN,
                    max_items_in_msg=MACD_PREALERT_PUSH_MAX_ITEMS_IN_MSG,
                    force=1,
                )
            except Exception as e:
                try:
                    _MACD_PREALERT_LAST_ERROR = str(e)
                except Exception:
                    pass

        threading.Thread(target=_macd_prealert_startup_push_once, name="macd_prealert_startup_push_once", daemon=True).start()

    # MACD 监控：后台定时推送（与 MACD 监控页一致）
    if MACD_MONITOR_PUSH_ENABLED:
        global _MACD_MONITOR_THREAD
        with _MACD_MONITOR_THREAD_LOCK:
            if _MACD_MONITOR_THREAD is None or not _MACD_MONITOR_THREAD.is_alive():
                t3 = threading.Thread(target=_macd_monitor_push_loop, name="macd_monitor_push", daemon=True)
                _MACD_MONITOR_THREAD = t3
                t3.start()

        # 重启后立即推送一次（不阻塞启动）：MACD 监控（force=1 绕过节流）
        def _macd_monitor_startup_push_once() -> None:
            try:
                s = _news_settings()
                if not _setting_bool(s, "push_macd_monitor_enabled", True):
                    return
                global _MACD_MONITOR_LAST_RUN_TS, _MACD_MONITOR_LAST_PUSH, _MACD_MONITOR_LAST_ERROR
                _MACD_MONITOR_LAST_RUN_TS = int(time.time())
                _MACD_MONITOR_LAST_ERROR = ""
                _MACD_MONITOR_LAST_PUSH = push_tg_macd_monitor(
                    topn=MACD_MONITOR_PUSH_TOPN,
                    max_items_in_msg=MACD_MONITOR_PUSH_MAX_ITEMS_IN_MSG,
                    force=1,
                )
            except Exception as e:
                try:
                    _MACD_MONITOR_LAST_ERROR = str(e)
                except Exception:
                    pass

        threading.Thread(target=_macd_monitor_startup_push_once, name="macd_monitor_startup_push_once", daemon=True).start()

    # 综合信号：后台强信号推送
    if SIGNAL_PUSH_ENABLED:
        global _SIGNAL_PUSH_THREAD
        with _SIGNAL_PUSH_THREAD_LOCK:
            if _SIGNAL_PUSH_THREAD is None or not _SIGNAL_PUSH_THREAD.is_alive():
                t4 = threading.Thread(target=_signal_push_loop, name="signal_push", daemon=True)
                _SIGNAL_PUSH_THREAD = t4
                t4.start()

    # 三周期信号：后台推送（默认只推 Grade A）
    if TRI_SIGNAL_PUSH_ENABLED:
        global _TRI_SIGNAL_PUSH_THREAD
        with _TRI_SIGNAL_PUSH_THREAD_LOCK:
            if _TRI_SIGNAL_PUSH_THREAD is None or not _TRI_SIGNAL_PUSH_THREAD.is_alive():
                t5 = threading.Thread(target=_tri_signal_push_loop, name="tri_signal_push", daemon=True)
                _TRI_SIGNAL_PUSH_THREAD = t5
                t5.start()

    # Master Prompt：策略A/策略B 触发推送
    if MASTER_A_PUSH_ENABLED:
        global _MASTER_A_PUSH_THREAD
        with _MASTER_A_PUSH_THREAD_LOCK:
            if _MASTER_A_PUSH_THREAD is None or not _MASTER_A_PUSH_THREAD.is_alive():
                t6 = threading.Thread(target=_master_a_push_loop, name="master_a_push", daemon=True)
                _MASTER_A_PUSH_THREAD = t6
                t6.start()

    if MASTER_B_PUSH_ENABLED:
        global _MASTER_B_PUSH_THREAD
        with _MASTER_B_PUSH_THREAD_LOCK:
            if _MASTER_B_PUSH_THREAD is None or not _MASTER_B_PUSH_THREAD.is_alive():
                t7 = threading.Thread(target=_master_b_push_loop, name="master_b_push", daemon=True)
                _MASTER_B_PUSH_THREAD = t7
                t7.start()

    try:
        global _WHALES_ALERT_THREAD
        if "_WHALES_ALERT_THREAD" not in globals():
            _WHALES_ALERT_THREAD = None
        if WHALES_ALERT_LOOP_ENABLED and (_WHALES_ALERT_THREAD is None or not _WHALES_ALERT_THREAD.is_alive()):
            _WHALES_ALERT_THREAD = threading.Thread(target=_whales_alert_loop, name="whales_alert_loop", daemon=True)
            _WHALES_ALERT_THREAD.start()
    except Exception:
        pass

static_dir = os.path.join(os.path.dirname(__file__), "web")
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    with open(os.path.join(static_dir, "index.html"), "r", encoding="utf-8") as f:
        return f.read()


@app.get("/api/health")
def health() -> dict:
    return {
        "ok": True,
    }


@app.get("/api/whales/transfers")
def whales_transfers(
    chain: str = "ETH",
    min_usd: float = 1_000_000,
    limit: int = 50,
    offset: int = 0,
) -> JSONResponse:
    try:
        limit = max(10, min(500, int(limit)))
    except Exception:
        limit = 50
    try:
        offset = max(0, int(offset))
    except Exception:
        offset = 0
    try:
        min_usd = float(min_usd)
    except Exception:
        min_usd = 1_000_000.0
    min_usd = max(10_000.0, min(50_000_000.0, min_usd))

    chain_u = (chain or "ETH").upper()
    ck = f"whales:transfers:{chain_u}:{int(min_usd)}:{limit}:{offset}"
    cached = _cache_get(ck, ttl=10)
    if cached is not None:
        return JSONResponse(cached)

    try:
        # 过滤 from==to 后仍尽量返回足够条数：一次多取一些（最多 500），过滤后再按 offset/limit 切片
        try:
            fetch_limit = max(10, min(500, int(limit) + int(offset) + 100))
        except Exception:
            fetch_limit = 500
        items0, src, src_status = _get_whale_transfers_auto(chain_u, min_usd=min_usd, limit=fetch_limit, offset=0)

        def _na(v: Any) -> str:
            return str(v or "").strip().lower()

        filtered: List[dict] = []
        for it in (items0 or []):
            try:
                f = _na(it.get("from")) if isinstance(it, dict) else ""
                t = _na(it.get("to")) if isinstance(it, dict) else ""
                if f and t and f == t:
                    continue
            except Exception:
                pass
            if isinstance(it, dict):
                filtered.append(it)

        items = filtered[offset : offset + limit]
        payload = {
            "ok": True,
            "items": items,
            "chain": chain_u,
            "min_usd": min_usd,
            "limit": limit,
            "offset": offset,
            "source": src,
            "source_status": src_status,
            "generated_at": int(time.time()),
        }
        _cache_set(ck, payload)
        return JSONResponse(payload)
    except Exception as e:
        payload = {
            "ok": False,
            "items": [],
            "chain": chain_u,
            "min_usd": min_usd,
            "limit": limit,
            "offset": offset,
            "source": "real",
            "source_status": str(e),
            "generated_at": int(time.time()),
        }
        return JSONResponse(payload, status_code=502)


@app.get("/api/whales/watchlist")
def api_whales_watchlist(chain: str = "") -> JSONResponse:
    try:
        chain_u = _whale_chain_norm(chain) if chain else ""
        items = _whale_watchlist_get(chain_u or None)
        return JSONResponse({"ok": True, "items": items})
    except Exception as e:
        return JSONResponse({"ok": False, "items": [], "error": str(e)}, status_code=200)


@app.post("/api/whales/watchlist")
async def api_whales_watchlist_upsert(req: Request) -> JSONResponse:
    try:
        payload = await req.json()
    except Exception:
        payload = {}
    try:
        chain = payload.get("chain") if isinstance(payload, dict) else "ETH"
        address = payload.get("address") if isinstance(payload, dict) else ""
        label = payload.get("label") if isinstance(payload, dict) else ""
        tags = payload.get("tags") if isinstance(payload, dict) else {}
        out = _whale_watchlist_upsert(chain=str(chain or "ETH"), address=str(address or ""), label=str(label or ""), tags=tags if isinstance(tags, dict) else {})
        return JSONResponse({"ok": True, "item": out})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=200)


@app.delete("/api/whales/watchlist/{item_id}")
def api_whales_watchlist_delete(item_id: int) -> JSONResponse:
    try:
        _whale_watchlist_delete(int(item_id))
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=200)


@app.get("/api/whales/rules")
def api_whales_rules() -> JSONResponse:
    try:
        return JSONResponse({"ok": True, "items": _whale_rules_list()})
    except Exception as e:
        return JSONResponse({"ok": False, "items": [], "error": str(e)}, status_code=200)


@app.post("/api/whales/rules")
async def api_whales_rules_create(req: Request) -> JSONResponse:
    try:
        payload = await req.json()
    except Exception:
        payload = {}
    try:
        it = _whale_rule_create(payload if isinstance(payload, dict) else {})
        return JSONResponse({"ok": True, "item": it})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=200)


@app.delete("/api/whales/rules/{rule_id}")
def api_whales_rules_delete(rule_id: int) -> JSONResponse:
    try:
        _whale_rule_delete(int(rule_id))
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=200)


@app.get("/api/whales/alerts")
def api_whales_alerts(limit: int = 200) -> JSONResponse:
    try:
        return JSONResponse({"ok": True, "items": _whale_alert_history(limit=int(limit))})
    except Exception as e:
        return JSONResponse({"ok": False, "items": [], "error": str(e)}, status_code=200)


@app.get("/api/whales/push_now")
def api_whales_push_now(force: int = 0) -> JSONResponse:
    """手动触发一次鲸鱼告警检测并推送（不会启动新线程）。"""
    try:
        s = _news_settings()
        bot_token = (s.get("tg_bot_token") or "").strip()
        chat_id = (s.get("tg_chat_id") or "").strip()
        enabled_all = _setting_bool(s, "push_enabled", True)
        enabled_mod = _setting_bool(s, "push_whales_enabled", True)
        if not (enabled_all and enabled_mod):
            return JSONResponse({"ok": True, "pushed": 0, "skipped": 0, "errors": ["disabled_by_settings"]})
        if not bot_token or not chat_id:
            return JSONResponse({"ok": False, "pushed": 0, "skipped": 0, "errors": ["未配置 Telegram Bot Token 或 Chat ID"]})

        rules = [r for r in _whale_rules_list() if int(r.get("enabled") or 0) == 1]
        if not rules:
            return JSONResponse({"ok": True, "pushed": 0, "skipped": 0, "errors": ["no_rules"]})

        watch = _whale_watchlist_get()
        watch_set = set((str(x.get("chain") or "").upper(), _whale_addr_norm(str(x.get("address") or ""))) for x in watch)

        pushed = 0
        skipped = 0
        errors: List[str] = []

        for rule in rules:
            try:
                chain = _whale_chain_norm(rule.get("chain") or "ETH")
                direction = _whale_direction_norm(rule.get("direction") or "all")
                min_usd = float(rule.get("min_usd") or 1_000_000.0)
                watch_only = bool(int(rule.get("watchlist_only") or 0))
                items, src, src_status = _get_whale_transfers_auto(chain, min_usd=min_usd, limit=100, offset=0)
                for tx in items:
                    try:
                        tx_dir = str(tx.get("direction") or "unknown")
                        if direction != "all" and tx_dir != direction:
                            skipped += 1
                            continue
                        from_a = _whale_addr_norm(str(tx.get("from") or ""))
                        to_a = _whale_addr_norm(str(tx.get("to") or ""))
                        if watch_only:
                            if (chain, from_a) not in watch_set and (chain, to_a) not in watch_set:
                                skipped += 1
                                continue

                        tx_hash = str(tx.get("tx_hash") or "").strip()
                        ts = int(tx.get("ts") or 0)
                        uniq = f"whale:{int(rule.get('id') or 0)}:{chain}:{tx_dir}:{tx_hash or ''}:{ts}:{int(float(tx.get('amount_usd') or 0))}"
                        if (not int(force)) and _whale_alert_has_uniq(uniq):
                            skipped += 1
                            continue

                        msg = _whale_make_msg(rule, tx)
                        ok, err = _tg_send(bot_token=bot_token, chat_id=chat_id, text=msg, parse_mode="HTML")
                        if not ok:
                            err = (err or "send failed") + f" | src={src}:{src_status}"
                            errors.append(err)
                        _whale_alert_history_add(
                            uniq=uniq,
                            rule_id=int(rule.get("id") or 0),
                            chain=chain,
                            direction=tx_dir,
                            amount_usd=float(tx.get("amount_usd") or 0),
                            asset=str(tx.get("asset") or ""),
                            from_addr=str(tx.get("from") or ""),
                            to_addr=str(tx.get("to") or ""),
                            tx_hash=tx_hash,
                            explorer_url=str(tx.get("explorer_url") or ""),
                            message=msg,
                            ok=ok,
                            error=err,
                        )
                        if ok:
                            pushed += 1
                    except Exception:
                        skipped += 1
                        continue
            except Exception as e:
                errors.append(str(e))
                continue

        return JSONResponse({"ok": True, "pushed": pushed, "skipped": skipped, "errors": errors[:20]})
    except Exception as e:
        return JSONResponse({"ok": False, "pushed": 0, "skipped": 0, "errors": [str(e)]}, status_code=200)


@app.get("/api/whales/summary")
def whales_summary(chain: str = "ETH", min_usd: float = 1_000_000) -> JSONResponse:
    try:
        min_usd = float(min_usd)
    except Exception:
        min_usd = 1_000_000.0
    min_usd = max(10_000.0, min(50_000_000.0, min_usd))

    chain_u = (chain or "ETH").upper()
    ck = f"whales:summary:{chain_u}:{int(min_usd)}"
    cached = _cache_get(ck, ttl=15)
    if cached is not None:
        return JSONResponse(cached)

    now = int(time.time())
    try:
        items, src, src_status = _get_whale_transfers_auto(chain_u, min_usd=min_usd, limit=400, offset=0)
    except Exception as e:
        payload = {
            "ok": False,
            "chain": chain_u,
            "min_usd": min_usd,
            "generated_at": int(time.time()),
            "source": "real",
            "source_status": str(e),
            "kpi": {"inflow_usd_m": 0.0, "outflow_usd_m": 0.0, "netflow_usd_m": 0.0, "tx_count": 0},
            "series_24h": [],
        }
        return JSONResponse(payload, status_code=502)

    buckets: Dict[int, dict] = {}
    for k in range(24):
        ts0 = now - (23 - k) * 3600
        hour = int(ts0 // 3600) * 3600
        buckets[hour] = {"ts": hour, "in": 0.0, "out": 0.0, "count": 0}

    for tx in items:
        try:
            ts = int(tx.get("ts") or 0)
        except Exception:
            ts = 0
        if ts <= 0:
            continue
        hour = int(ts // 3600) * 3600
        if hour not in buckets:
            continue
        try:
            usd = float(tx.get("amount_usd") or 0.0)
        except Exception:
            usd = 0.0
        if usd <= 0:
            continue
        d = str(tx.get("direction") or "wallet")
        if d == "to_exchange":
            buckets[hour]["in"] += usd
        elif d == "from_exchange":
            buckets[hour]["out"] += usd
        buckets[hour]["count"] += 1

    series: List[dict] = []
    inflow = 0.0
    outflow = 0.0
    net = 0.0
    count = 0
    for hour in sorted(buckets.keys()):
        b = buckets[hour]
        v_in_m = float(b.get("in") or 0.0) / 1e6
        v_out_m = float(b.get("out") or 0.0) / 1e6
        v_net_m = v_out_m - v_in_m
        series.append(
            {
                "ts": int(hour),
                "inflow_usd_m": round(v_in_m, 3),
                "outflow_usd_m": round(v_out_m, 3),
                "netflow_usd_m": round(v_net_m, 3),
            }
        )
        inflow += v_in_m
        outflow += v_out_m
        net += v_net_m
        count += int(b.get("count") or 0)

    payload = {
        "ok": True,
        "chain": chain_u,
        "min_usd": min_usd,
        "generated_at": int(time.time()),
        "source": src,
        "source_status": src_status,
        "kpi": {
            "inflow_usd_m": round(inflow, 3),
            "outflow_usd_m": round(outflow, 3),
            "netflow_usd_m": round(net, 3),
            "tx_count": int(count),
        },
        "series_24h": series,
    }
    _cache_set(ck, payload)
    return JSONResponse(payload)


@app.get("/api/summary")
def summary(timeframe: str = "1h", lookback: int = 6) -> JSONResponse:
    """仪表板主表（固定5个合约）。

    - timeframe：15m/1h/4h/1d
    - lookback：用于 price_change_pct / oi_change_pct 的跨度（使用 lookback+1 个点取 last 与 prevN）

    返回：
    - items：Row.__dict__ 列表（包含 score/market_signal 等）
    - errors：失败合约列表
    """
    if timeframe not in TIMEFRAMES:
        return JSONResponse({"error": "invalid timeframe"}, status_code=400)

    lookback = max(1, min(24, int(lookback or 1)))

    ck = f"summary:{timeframe}:lb{lookback}"
    cached = _cache_get(ck, ttl=10)
    if cached is not None:
        return JSONResponse(cached)

    items: List[dict] = []
    errors: List[str] = []

    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(compute_row, c, timeframe, lookback): c for c in CONTRACTS_5}
        for f in as_completed(futs):
            c = futs[f]
            try:
                r = f.result()
                items.append(r.__dict__)
            except Exception as e:
                errors.append(f"{c}: {e}")

    # 默认按强度降序，便于主表直接体现“异动优先级”
    try:
        items.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
    except Exception:
        pass

    payload = {"items": items, "errors": errors}
    _cache_set(ck, payload)
    return JSONResponse(payload)


@app.get("/api/macd_preentries")
def macd_preentries(
    limit: int = 50,
    timeframe: str = "1h",
    allow_adx_20_25: int = 1,
) -> JSONResponse:
    """MACD 预警入场：基于 detect_prealert 的“即将金叉/即将死叉”。"""
    limit = max(10, min(120, int(limit)))
    timeframe = (timeframe or "1h").strip().lower()
    if timeframe not in ("15m", "1h", "4h", "1d"):
        timeframe = "1h"
    allow_adx_20_25 = 1 if str(allow_adx_20_25).strip() in ("1", "true", "True", "yes", "YES") else 0

    ck = f"macd_preentries:{limit}:{timeframe}:{allow_adx_20_25}"
    cached = _cache_get(ck, ttl=30)
    if cached is not None:
        return JSONResponse(cached)

    errors: List[str] = []
    items: List[dict] = []

    try:
        top = coingecko_top_marketcap(limit)
    except Exception as e:
        return JSONResponse({"error": f"CoinGecko 获取失败: {e}"}, status_code=200)

    try:
        contract_set = set(get_all_futures_contract_names())
    except Exception as e:
        return JSONResponse({"error": f"Gate 合约列表获取失败: {e}"}, status_code=200)

    last_price_map = _ticker_last_price_map()

    tf_sec_map = {
        "15m": 15 * 60,
        "1h": 60 * 60,
        "4h": 4 * 60 * 60,
        "1d": 24 * 60 * 60,
    }
    tf_sec = int(tf_sec_map.get(timeframe, 60 * 60))

    candidates: List[dict] = []
    for it in top:
        sym = str(it.get("symbol") or "").upper().strip()
        if not sym:
            continue
        contract = f"{sym}_USDT"
        if contract not in contract_set:
            continue
        candidates.append({
            "symbol": sym,
            "contract": contract,
            "market_cap_rank": it.get("market_cap_rank"),
            "market_cap": it.get("market_cap"),
        })

    def _analyze_one(cand: dict) -> Optional[dict]:
        contract = str(cand.get("contract") or "")
        symbol = str(cand.get("symbol") or "")
        last_px = last_price_map.get(contract)
        now_ts = int(time.time())

        try:
            candles = get_macd_candles(contract, timeframe, limit=260)
            seq = [x for x in candles if isinstance(x, dict)]
            seq.sort(key=lambda x: int(x.get("t") or 0))

            ts: List[int] = []
            h: List[float] = []
            l: List[float] = []
            c: List[float] = []
            v: List[float] = []
            for x in seq:
                tt = x.get("t")
                hh = _safe_float(x.get("h"))
                ll = _safe_float(x.get("l"))
                cc = _safe_float(x.get("c"))
                vv = _safe_float(x.get("v"))
                if tt is None or hh is None or ll is None or cc is None:
                    continue
                try:
                    ts.append(int(tt))
                    h.append(float(hh))
                    l.append(float(ll))
                    c.append(float(cc))
                    v.append(float(vv or 0.0))
                except Exception:
                    continue

            if len(c) < 120:
                return None

            dif, dea, hist = _macd(c, 12, 26, 9)
            if not dif or not dea:
                return None

            pre = detect_prealert(dif, dea, hist, lookback=2, ratio_threshold=0.75)
            if not pre:
                return None

            pre_type = str(pre.get("type") or "")
            side = "long" if pre_type == "pre_golden" else ("short" if pre_type == "pre_death" else "none")
            if side == "none":
                return None

            idx = len(c) - 1
            if idx < 0 or idx >= len(ts):
                return None

            vol_idx = idx
            try:
                # 若最后一根 tf K线未收盘，则量能确认改用前一根已收盘 K线
                if vol_idx == (len(ts) - 1) and vol_idx > 0:
                    last_open_ts = int(ts[vol_idx])
                    if now_ts < (last_open_ts + tf_sec):
                        vol_idx = vol_idx - 1
            except Exception:
                vol_idx = idx

            ema50 = _ema(c, 50)
            adx14 = _adx(h, l, c, 14)
            atr14 = _atr(h, l, c, 14)
            vol_sma20 = _sma(v, 20)

            e50 = ema50[idx] if ema50 and idx < len(ema50) else None
            adx_v = adx14[idx] if adx14 and idx < len(adx14) else None
            atr_v = atr14[idx] if atr14 and idx < len(atr14) else None
            vol_ma = vol_sma20[vol_idx] if vol_sma20 and vol_idx < len(vol_sma20) else None
            if e50 is None or adx_v is None or atr_v is None or vol_ma is None:
                return None

            adx_f = float(adx_v)
            if adx_f < 20.0:
                return None
            if (not allow_adx_20_25) and (adx_f < 25.0):
                return None

            entry = c[idx]
            if not isinstance(entry, (int, float)):
                return None

            try:
                vol_ratio = float(v[vol_idx]) / float(vol_ma) if float(vol_ma) > 0 else None
            except Exception:
                vol_ratio = None
            if vol_ratio is None or vol_ratio <= 1.3:
                return None

            try:
                if side == "long" and float(entry) <= float(e50):
                    return None
                if side == "short" and float(entry) >= float(e50):
                    return None
            except Exception:
                return None

            atr_f = float(atr_v) if isinstance(atr_v, (int, float)) else 0.0
            if atr_f <= 0:
                return None

            if side == "long":
                sl = float(entry) - 1.0 * atr_f
                tp1 = float(entry) + 2.0 * atr_f
            else:
                sl = float(entry) + 1.0 * atr_f
                tp1 = float(entry) - 2.0 * atr_f

            return {
                "symbol": symbol,
                "contract": contract,
                "timeframe": timeframe,
                "market_cap_rank": cand.get("market_cap_rank"),
                "current_price": last_px,
                "signal_type": pre_type,
                "signal_time": int(ts[idx]),
                "entry_price": float(entry),
                "ema50": float(e50),
                "adx14": float(adx_f),
                "vol": float(v[vol_idx]),
                "vol_sma20": float(vol_ma),
                "vol_ratio": float(vol_ratio),
                "atr14": float(atr_f),
                "sl": float(sl),
                "tp1": float(tp1),
                "side": side,
                "pre_ratio": (float(pre.get("ratio")) if pre.get("ratio") is not None else None),
                "pre_distance": (float(pre.get("distance")) if pre.get("distance") is not None else None),
                "pre_bar_dir": pre.get("bar_dir"),
                "updated_at": now_ts,
            }
        except Exception as e:
            errors.append(f"{contract}: {e}")
            return None

    max_workers = 6
    if limit <= 20:
        max_workers = 4
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_analyze_one, c) for c in candidates[:limit]]
        for f in as_completed(futs):
            try:
                r = f.result()
                if r:
                    items.append(r)
            except Exception as e:
                errors.append(str(e))

    try:
        items.sort(key=lambda x: int(x.get("signal_time") or 0), reverse=True)
    except Exception:
        pass

    payload = {"items": items, "errors": errors}
    _cache_set(ck, payload)
    return JSONResponse(payload)

@app.get("/api/macd_entries")
def macd_entries(
    limit: int = 50,
    timeframe: str = "1h",
    allow_adx_20_25: int = 1,
) -> JSONResponse:
    """MACD 监控入场（1H 为主）。

    入场条件（做多/做空）：
    - MACD 发生金叉/死叉（最近 lookback 根内的最近一次）
    - 叠加过滤：EMA50、ADX14、成交量均线（SMA20）确认
    - 禁止：ADX < 20
    - 可选：ADX 20-25 是否允许（allow_adx_20_25）

    输出：候选入场列表（包含各指标值与 ATR 风控参考）。
    """
    limit = max(10, min(120, int(limit)))
    timeframe = (timeframe or "1h").strip().lower()
    # 允许在页面对照更大周期趋势；默认仍为 1h
    if timeframe not in ("15m", "1h", "4h", "1d"):
        timeframe = "1h"
    allow_adx_20_25 = 1 if str(allow_adx_20_25).strip() in ("1", "true", "True", "yes", "YES") else 0

    ck = f"macd_entries:{limit}:{timeframe}:{allow_adx_20_25}"
    cached = _cache_get(ck, ttl=30)
    if cached is not None:
        return JSONResponse(cached)

    errors: List[str] = []
    items: List[dict] = []

    try:
        top = coingecko_top_marketcap(limit)
    except Exception as e:
        return JSONResponse({"error": f"CoinGecko 获取失败: {e}"}, status_code=200)

    try:
        contract_set = set(get_all_futures_contract_names())
    except Exception as e:
        return JSONResponse({"error": f"Gate 合约列表获取失败: {e}"}, status_code=200)

    last_price_map = _ticker_last_price_map()

    tf_sec_map = {
        "15m": 15 * 60,
        "1h": 60 * 60,
        "4h": 4 * 60 * 60,
        "1d": 24 * 60 * 60,
    }
    tf_sec = int(tf_sec_map.get(timeframe, 60 * 60))

    candidates: List[dict] = []
    for it in top:
        sym = str(it.get("symbol") or "").upper().strip()
        if not sym:
            continue
        contract = f"{sym}_USDT"
        if contract not in contract_set:
            continue
        candidates.append({
            "symbol": sym,
            "contract": contract,
            "market_cap_rank": it.get("market_cap_rank"),
            "market_cap": it.get("market_cap"),
        })

    def _analyze_one(cand: dict) -> Optional[dict]:
        contract = str(cand.get("contract") or "")
        symbol = str(cand.get("symbol") or "")
        last_px = last_price_map.get(contract)
        now_ts = int(time.time())

        try:
            # 需要足够长度：EMA50 / ADX14 / ATR14 / VOL SMA20
            candles = get_macd_candles(contract, timeframe, limit=260)
            seq = [x for x in candles if isinstance(x, dict)]
            seq.sort(key=lambda x: int(x.get("t") or 0))

            ts: List[int] = []
            h: List[float] = []
            l: List[float] = []
            c: List[float] = []
            v: List[float] = []
            for x in seq:
                tt = x.get("t")
                hh = _safe_float(x.get("h"))
                ll = _safe_float(x.get("l"))
                cc = _safe_float(x.get("c"))
                vv = _safe_float(x.get("v"))
                if tt is None or hh is None or ll is None or cc is None:
                    continue
                try:
                    ts.append(int(tt))
                    h.append(float(hh))
                    l.append(float(ll))
                    c.append(float(cc))
                    v.append(float(vv or 0.0))
                except Exception:
                    continue

            if len(c) < 120:
                return None

            dif, dea, hist = _macd(c, 12, 26, 9)
            if not dif or not dea:
                return None

            cross = detect_recent_cross(dif, dea, lookback=3)
            if not cross:
                return None
            signal_type, signal_idx = cross
            if signal_idx is None or signal_idx >= len(ts):
                return None

            vol_idx = int(signal_idx)
            try:
                # 若信号落在最后一根 tf K线且该 K线未收盘，则量能确认改用前一根已收盘 K线
                if vol_idx == (len(ts) - 1) and vol_idx > 0:
                    last_open_ts = int(ts[vol_idx])
                    if now_ts < (last_open_ts + tf_sec):
                        vol_idx = vol_idx - 1
            except Exception:
                vol_idx = int(signal_idx)

            # EMA50、ADX14、ATR14、VOL SMA20
            ema50 = _ema(c, 50)
            adx14 = _adx(h, l, c, 14)
            atr14 = _atr(h, l, c, 14)
            vol_sma20 = _sma(v, 20)

            e50 = ema50[signal_idx] if ema50 and signal_idx < len(ema50) else None
            adx_v = adx14[signal_idx] if adx14 and signal_idx < len(adx14) else None
            atr_v = atr14[signal_idx] if atr14 and signal_idx < len(atr14) else None
            vol_ma = vol_sma20[vol_idx] if vol_sma20 and vol_idx < len(vol_sma20) else None

            if e50 is None or adx_v is None or atr_v is None or vol_ma is None:
                return None

            try:
                adx_f = float(adx_v)
            except Exception:
                return None

            # ADX 禁止/可选开关
            if adx_f < 20.0:
                return None
            if (not allow_adx_20_25) and (adx_f < 25.0):
                return None

            entry = c[signal_idx]
            if not isinstance(entry, (int, float)):
                return None

            # 量能确认
            try:
                vol_ratio = float(v[vol_idx]) / float(vol_ma) if float(vol_ma) > 0 else None
            except Exception:
                vol_ratio = None
            if vol_ratio is None or vol_ratio <= 1.3:
                return None

            side = "long" if signal_type == "golden" else ("short" if signal_type == "death" else "none")
            if side == "none":
                return None

            # EMA50 过滤
            try:
                if side == "long" and float(entry) <= float(e50):
                    return None
                if side == "short" and float(entry) >= float(e50):
                    return None
            except Exception:
                return None

            atr_f = float(atr_v) if isinstance(atr_v, (int, float)) else 0.0
            if atr_f <= 0:
                return None

            # 风控参考：SL=1*ATR，TP1=2*ATR（仅输出参考，不做撮合/下单）
            if side == "long":
                sl = float(entry) - 1.0 * atr_f
                tp1 = float(entry) + 2.0 * atr_f
            else:
                sl = float(entry) + 1.0 * atr_f
                tp1 = float(entry) - 2.0 * atr_f

            return {
                "symbol": symbol,
                "contract": contract,
                "timeframe": timeframe,
                "market_cap_rank": cand.get("market_cap_rank"),
                "current_price": last_px,
                "signal_type": signal_type,
                "signal_time": int(ts[signal_idx]),
                "entry_price": float(entry),
                "ema50": float(e50),
                "adx14": float(adx_f),
                "vol": float(v[vol_idx]),
                "vol_sma20": float(vol_ma),
                "vol_ratio": float(vol_ratio),
                "atr14": float(atr_f),
                "sl": float(sl),
                "tp1": float(tp1),
                "side": side,
                "updated_at": now_ts,
            }
        except Exception as e:
            errors.append(f"{contract}: {e}")
            return None

    max_workers = 6
    if limit <= 20:
        max_workers = 4
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_analyze_one, c) for c in candidates[:limit]]
        for f in as_completed(futs):
            try:
                r = f.result()
                if r:
                    items.append(r)
            except Exception as e:
                errors.append(str(e))

    # 统一排序：最新信号优先
    try:
        items.sort(key=lambda x: int(x.get("signal_time") or 0), reverse=True)
    except Exception:
        pass

    payload = {"items": items, "errors": errors}
    _cache_set(ck, payload)
    return JSONResponse(payload)


@app.get("/api/macd_monitor/auto_status")
def api_macd_monitor_auto_status() -> JSONResponse:
    alive = False
    name = None
    try:
        alive = bool(_MACD_MONITOR_THREAD is not None and _MACD_MONITOR_THREAD.is_alive())
        name = _MACD_MONITOR_THREAD.name if _MACD_MONITOR_THREAD is not None else None
    except Exception:
        alive = False
        name = None

    s = _news_settings()
    payload = {
        "enabled_env": bool(MACD_MONITOR_PUSH_ENABLED),
        "interval_sec": int(MACD_MONITOR_PUSH_INTERVAL_SEC),
        "topn": int(MACD_MONITOR_PUSH_TOPN),
        "max_items_in_msg": int(MACD_MONITOR_PUSH_MAX_ITEMS_IN_MSG),
        "thread_alive": alive,
        "thread_name": name,
        "has_bot_token": bool((s.get("tg_bot_token") or "").strip()),
        "has_chat_id": bool((s.get("tg_chat_id") or "").strip()),
        "last_run_ts": _MACD_MONITOR_LAST_RUN_TS,
        "last_error": _MACD_MONITOR_LAST_ERROR,
        "last_push": _MACD_MONITOR_LAST_PUSH,
    }
    return JSONResponse(payload)


@app.get("/api/macd_monitor/push_now")
def api_macd_monitor_push_now(force: int = 1, topn: int = 0, max_items_in_msg: int = 0) -> JSONResponse:
    s = _news_settings()
    if not _setting_bool(s, "push_macd_monitor_enabled", True):
        return JSONResponse({"ok": True, "pushed": 0, "skipped": 0, "errors": ["disabled_by_settings"]})
    try:
        _topn = int(topn) if int(topn) > 0 else int(MACD_MONITOR_PUSH_TOPN)
    except Exception:
        _topn = int(MACD_MONITOR_PUSH_TOPN)
    try:
        _max = int(max_items_in_msg) if int(max_items_in_msg) > 0 else int(MACD_MONITOR_PUSH_MAX_ITEMS_IN_MSG)
    except Exception:
        _max = int(MACD_MONITOR_PUSH_MAX_ITEMS_IN_MSG)

    out = push_tg_macd_monitor(topn=_topn, max_items_in_msg=_max, force=int(force))
    return JSONResponse(out)


@app.get("/api/macd_prealert/push_now")
def api_macd_prealert_push_now(force: int = 1, topn: int = 0, max_items_in_msg: int = 0) -> JSONResponse:
    s = _news_settings()
    if not _setting_bool(s, "push_macd_prealert_enabled", True):
        return JSONResponse({"ok": True, "pushed": 0, "skipped": 0, "errors": ["disabled_by_settings"]})
    try:
        _topn = int(topn) if int(topn) > 0 else int(MACD_PREALERT_PUSH_TOPN)
    except Exception:
        _topn = int(MACD_PREALERT_PUSH_TOPN)
    try:
        _max = int(max_items_in_msg) if int(max_items_in_msg) > 0 else int(MACD_PREALERT_PUSH_MAX_ITEMS_IN_MSG)
    except Exception:
        _max = int(MACD_PREALERT_PUSH_MAX_ITEMS_IN_MSG)

    out = push_tg_macd_prealerts(topn=_topn, max_items_in_msg=_max, force=int(force))
    return JSONResponse(out)


@app.get("/api/news/refresh")
def api_news_refresh(max_per_feed: int = 30, analyze: int = 1, analyze_limit: int = 30) -> JSONResponse:
    out = refresh_news(max_per_feed=max_per_feed)
    if int(analyze) == 1:
        out["analyze"] = analyze_pending_news(limit=analyze_limit)
        out["push"] = push_telegram_for_news(limit=analyze_limit)
    return JSONResponse(out)


@app.get("/api/news/items")
def api_news_items(limit: int = 100, since_ts: int = 0) -> JSONResponse:
    limit = max(1, min(500, int(limit)))
    since_ts = _safe_int(since_ts) or 0
    conn = _db_connect()
    try:
        rows = conn.execute(
            """
            SELECT id, source, title, title_zh, link, published_at, summary, summary_zh, tags, coins, sentiment, strength, reason, created_at, translated_at
            FROM news_items
            WHERE published_at IS NULL OR published_at >= ?
            ORDER BY COALESCE(published_at, created_at) DESC
            LIMIT ?
            """,
            (since_ts, limit),
        ).fetchall()
        items = [dict(r) for r in rows]
        return JSONResponse({"items": items})
    finally:
        conn.close()


@app.post("/api/news/analyze")
def api_news_analyze(payload: Dict[str, Any]) -> JSONResponse:
    limit = 20
    force = 0
    if isinstance(payload, dict) and payload.get("limit") is not None:
        try:
            limit = int(payload.get("limit"))
        except Exception:
            limit = 20
    if isinstance(payload, dict) and payload.get("force") is not None:
        try:
            force = int(payload.get("force"))
        except Exception:
            force = 0
    out = analyze_pending_news(limit=limit, force=force)
    out["push"] = push_telegram_for_news(limit=limit)
    return JSONResponse(out)


@app.post("/api/news/push_test")
def api_news_push_test(payload: Dict[str, Any]) -> JSONResponse:
    s = _news_settings()
    bot_token = (s.get("tg_bot_token") or "").strip()
    chat_id = (s.get("tg_chat_id") or "").strip()
    text = "【新闻多空哨兵】Telegram 推送测试：如果你看到这条消息，说明配置成功。"
    if isinstance(payload, dict) and payload.get("text"):
        text = str(payload.get("text"))
    ok, err = _tg_send(bot_token=bot_token, chat_id=chat_id, text=text)
    _push_history_add(
        uniq=f"test:{int(time.time())}",
        level="test",
        title="Telegram Test",
        link="",
        message=text,
        ok=ok,
        error=err,
    )
    return JSONResponse({"ok": ok, "error": err})


@app.get("/api/news/push_test")
def api_news_push_test_get(text: str = "") -> JSONResponse:
    payload: Dict[str, Any] = {}
    if text:
        payload["text"] = text
    return api_news_push_test(payload)


@app.post("/api/news/translate")
def api_news_translate(payload: Dict[str, Any]) -> JSONResponse:
    limit = 20
    if isinstance(payload, dict) and payload.get("limit") is not None:
        try:
            limit = int(payload.get("limit"))
        except Exception:
            limit = 20
    return JSONResponse(translate_pending_news(limit=limit))


@app.post("/api/news/coins_backfill")
def api_news_coins_backfill(payload: Dict[str, Any]) -> JSONResponse:
    limit = 200
    if isinstance(payload, dict) and payload.get("limit") is not None:
        try:
            limit = int(payload.get("limit"))
        except Exception:
            limit = 200
    limit = max(1, min(2000, limit))

    conn = _db_connect()
    updated = 0
    errors: List[str] = []
    try:
        rows = conn.execute(
            """
            SELECT id, title, summary, tags
            FROM news_items
            WHERE coins IS NULL OR coins = ''
            ORDER BY COALESCE(published_at, created_at) DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        for r in rows:
            try:
                rid = int(r["id"])
                title = r["title"] or ""
                summary = r["summary"] or ""
                tags = r["tags"] or ""
                coins = extract_coins(title=title, summary=summary, tags=tags)
                if not coins:
                    continue
                conn.execute("UPDATE news_items SET coins=? WHERE id=?", (coins, rid))
                updated += 1
            except Exception as e:
                errors.append(str(e))

        conn.commit()
        return JSONResponse({"ok": True, "updated": updated, "errors": errors})
    finally:
        conn.close()


@app.get("/api/news/coins_backfill")
def api_news_coins_backfill_get(limit: int = 200) -> JSONResponse:
    return api_news_coins_backfill({"limit": limit})


@app.get("/api/news/settings")
def api_news_settings_get() -> JSONResponse:
    settings = _settings_get("news_settings", default={})
    if not isinstance(settings, dict):
        settings = {}
    return JSONResponse({"settings": settings})


@app.post("/api/news/settings")
def api_news_settings_set(payload: Dict[str, Any]) -> JSONResponse:
    settings = payload.get("settings") if isinstance(payload, dict) else None
    if not isinstance(settings, dict):
        return JSONResponse({"error": "invalid settings"}, status_code=400)
    _settings_set("news_settings", settings)
    return JSONResponse({"ok": True})


@app.get("/api/news/push_history")
def api_news_push_history(limit: int = 100) -> JSONResponse:
    limit = max(1, min(500, int(limit)))
    conn = _db_connect()
    try:
        rows = conn.execute(
            """
            SELECT id, created_at, uniq, level, title, link, message, ok, error
            FROM news_push_history
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        items = [dict(r) for r in rows]
        return JSONResponse({"items": items})
    finally:
        conn.close()


@app.get("/api/news/auto_status")
def api_news_auto_status() -> JSONResponse:
    s = _news_settings()
    th = None
    try:
        th = float(s.get("push_threshold")) if s.get("push_threshold") is not None else None
    except Exception:
        th = None

    alive = False
    name = None
    try:
        alive = bool(_NEWS_AUTO_THREAD is not None and _NEWS_AUTO_THREAD.is_alive())
        name = _NEWS_AUTO_THREAD.name if _NEWS_AUTO_THREAD is not None else None
    except Exception:
        alive = False
        name = None

    payload = {
        "enabled_env": bool(NEWS_AUTO_PUSH_ENABLED),
        "interval_sec": int(NEWS_AUTO_PUSH_INTERVAL_SEC),
        "window_sec": int(NEWS_AUTO_PUSH_WINDOW_SEC),
        "max_per_feed": int(NEWS_AUTO_PUSH_MAX_PER_FEED),
        "analyze_limit": int(NEWS_AUTO_PUSH_ANALYZE_LIMIT),
        "max_items_in_msg": int(NEWS_AUTO_PUSH_MAX_ITEMS_IN_MSG),
        "thread_alive": alive,
        "thread_name": name,
        "push_enabled": bool(s.get("push_enabled")),
        "push_threshold": th,
        "has_bot_token": bool((s.get("tg_bot_token") or "").strip()),
        "has_chat_id": bool((s.get("tg_chat_id") or "").strip()),
        "last_run_ts": _NEWS_AUTO_LAST_RUN_TS,
        "last_error": _NEWS_AUTO_LAST_ERROR,
        "last_refresh": _NEWS_AUTO_LAST_REFRESH,
        "last_analyze": _NEWS_AUTO_LAST_ANALYZE,
        "last_push": _NEWS_AUTO_LAST_PUSH,
    }
    return JSONResponse(payload)


@app.get("/api/macd_prealert/auto_status")
def api_macd_prealert_auto_status() -> JSONResponse:
    alive = False
    name = None
    try:
        alive = bool(_MACD_PREALERT_THREAD is not None and _MACD_PREALERT_THREAD.is_alive())
        name = _MACD_PREALERT_THREAD.name if _MACD_PREALERT_THREAD is not None else None
    except Exception:
        alive = False
        name = None

    s = _news_settings()
    payload = {
        "enabled_env": bool(MACD_PREALERT_PUSH_ENABLED),
        "interval_sec": int(MACD_PREALERT_PUSH_INTERVAL_SEC),
        "topn": int(MACD_PREALERT_PUSH_TOPN),
        "max_items_in_msg": int(MACD_PREALERT_PUSH_MAX_ITEMS_IN_MSG),
        "thread_alive": alive,
        "thread_name": name,
        "has_bot_token": bool((s.get("tg_bot_token") or "").strip()),
        "has_chat_id": bool((s.get("tg_chat_id") or "").strip()),
        "last_run_ts": _MACD_PREALERT_LAST_RUN_TS,
        "last_error": _MACD_PREALERT_LAST_ERROR,
        "last_push": _MACD_PREALERT_LAST_PUSH,
    }
    return JSONResponse(payload)


@app.get("/api/macd_prealerts")
def macd_prealerts(
    limit: int = 50,
    only_warn: int = 0,
    warn_type: str = "all",
    timeframe: str = "all",
    debug: int = 0,
) -> JSONResponse:
    limit = max(10, min(200, int(limit)))
    warn_type = (warn_type or "all").strip().lower()
    if warn_type not in ("all", "pre_golden", "pre_death"):
        warn_type = "all"

    timeframe = (timeframe or "all").strip().lower()
    if timeframe not in ("all", "15m", "1h", "4h", "1d", "2d"):
        timeframe = "all"

    tfs_scan = ("15m", "1h", "4h", "1d", "2d") if timeframe == "all" else (timeframe,)

    ck = f"macd_prealerts:{limit}:{only_warn}:{warn_type}:{timeframe}:{int(1 if debug else 0)}"
    if not debug:
        cached = _cache_get(ck, ttl=30)
        if cached is not None:
            return JSONResponse(cached)

    errors: List[str] = []
    items: List[dict] = []
    dbg = {
        "candidates": 0,
        "scanned": 0,
        "tf_scanned": {"15m": 0, "1h": 0, "4h": 0, "1d": 0, "2d": 0},
        "tf_prealert": {"15m": 0, "1h": 0, "4h": 0, "1d": 0, "2d": 0},
        "tf_insufficient": {"15m": 0, "1h": 0, "4h": 0, "1d": 0, "2d": 0},
        "tf_min_ratio": {"15m": None, "1h": None, "4h": None, "1d": None, "2d": None},
        "tf_min_abs_gap": {"15m": None, "1h": None, "4h": None, "1d": None, "2d": None},
        "tf_ratio_pass": {"15m": 0, "1h": 0, "4h": 0, "1d": 0, "2d": 0},
        "tf_slope_pass": {"15m": 0, "1h": 0, "4h": 0, "1d": 0, "2d": 0},
    }

    try:
        top = coingecko_top_marketcap(limit)
    except Exception as e:
        return JSONResponse({"error": f"CoinGecko 获取失败: {e}"}, status_code=200)

    try:
        contract_set = set(get_all_futures_contract_names())
    except Exception as e:
        return JSONResponse({"error": f"Gate 合约列表获取失败: {e}"}, status_code=200)

    last_price_map = _ticker_last_price_map()

    candidates: List[dict] = []
    for it in top:
        sym = str(it.get("symbol") or "").upper().strip()
        if not sym:
            continue
        contract = f"{sym}_USDT"
        if contract not in contract_set:
            continue
        candidates.append({
            "symbol": sym,
            "contract": contract,
            "market_cap_rank": it.get("market_cap_rank"),
            "market_cap": it.get("market_cap"),
        })

    dbg["candidates"] = len(candidates)

    def _analyze_one(cand: dict) -> Tuple[Optional[dict], Optional[dict]]:
        contract = cand["contract"]
        symbol = cand["symbol"]
        last_px = last_price_map.get(contract)
        now_ts = int(time.time())

        local_dbg = None
        if debug:
            local_dbg = {
                "scanned": 1,
                "tf_scanned": {"15m": 0, "1h": 0, "4h": 0, "1d": 0, "2d": 0},
                "tf_prealert": {"15m": 0, "1h": 0, "4h": 0, "1d": 0, "2d": 0},
                "tf_insufficient": {"15m": 0, "1h": 0, "4h": 0, "1d": 0, "2d": 0},
                "tf_min_ratio": {"15m": None, "1h": None, "4h": None, "1d": None, "2d": None},
                "tf_min_abs_gap": {"15m": None, "1h": None, "4h": None, "1d": None, "2d": None},
                "tf_ratio_pass": {"15m": 0, "1h": 0, "4h": 0, "1d": 0, "2d": 0},
                "tf_slope_pass": {"15m": 0, "1h": 0, "4h": 0, "1d": 0, "2d": 0},
            }

        # per timeframe status
        statuses: Dict[str, dict] = {}
        latest_warn = None

        for tf in tfs_scan:
            try:
                if debug:
                    local_dbg["tf_scanned"][tf] += 1
                candles = get_macd_candles(contract, tf, limit=240)
                seq = [x for x in candles if isinstance(x, dict)]
                seq.sort(key=lambda x: int(x.get("t") or 0))
                closes = [float(x.get("c")) for x in seq if _safe_float(x.get("c")) is not None]
                if len(closes) < 60:
                    if debug:
                        local_dbg["tf_insufficient"][tf] += 1
                    continue
                dif, dea, hist = _macd(closes, 12, 26, 9)
                if not dif:
                    continue

                if debug:
                    gap = float(dif[-1] - dea[-1])
                    abs_gap = abs(gap)
                    gaps20 = [(dif[i] - dea[i]) for i in range(len(dif) - 20, len(dif))]
                    base = _mean_abs(gaps20) or 0.0
                    ratio = (abs_gap / base) if base else None

                    # min stats
                    cur_min_gap = local_dbg["tf_min_abs_gap"][tf]
                    if cur_min_gap is None or abs_gap < float(cur_min_gap):
                        local_dbg["tf_min_abs_gap"][tf] = float(abs_gap)

                    if ratio is not None:
                        cur_min_ratio = local_dbg["tf_min_ratio"][tf]
                        if cur_min_ratio is None or ratio < float(cur_min_ratio):
                            local_dbg["tf_min_ratio"][tf] = float(ratio)
                        if ratio <= 0.9:
                            local_dbg["tf_ratio_pass"][tf] += 1

                    dif_slope = float(dif[-1] - dif[-2])
                    dea_slope = float(dea[-1] - dea[-2])
                    if (gap < 0 and (dif_slope - dea_slope) > 0) or (gap > 0 and (dif_slope - dea_slope) < 0):
                        local_dbg["tf_slope_pass"][tf] += 1
                pre = detect_prealert(dif, dea, hist, lookback=2, ratio_threshold=0.75)
                if not pre:
                    statuses[tf] = {"status": "—"}
                    continue

                if debug:
                    local_dbg["tf_prealert"][tf] += 1

                if warn_type != "all" and pre.get("type") != warn_type:
                    statuses[tf] = {"status": "—"}
                    continue

                t_last = int(seq[-1].get("t") or 0) if seq else 0
                statuses[tf] = {
                    "status": ("即将金叉" if pre["type"] == "pre_golden" else "即将死叉"),
                    "type": pre["type"],
                    "time": t_last,
                    "distance": pre.get("distance"),
                    "ratio": pre.get("ratio"),
                    "bar_dir": pre.get("bar_dir"),
                }
                if latest_warn is None or t_last > int(latest_warn.get("time") or 0):
                    latest_warn = {"time": t_last, **statuses[tf]}
            except Exception as e:
                errors.append(f"{contract} {tf}: {e}")

        # 只显示有预警
        has_warn = any(v.get("status") in ("即将金叉", "即将死叉") for v in statuses.values())
        if only_warn and not has_warn:
            return None, local_dbg

        out = {
            "symbol": symbol,
            "contract": contract,
            "market_cap_rank": cand.get("market_cap_rank"),
            "market_cap": cand.get("market_cap"),
            "current_price": last_px,
            # 为了保持前端渲染逻辑稳定，这里固定返回所有 status_xx 字段；
            # 但当 timeframe != all 时，只有被扫描的周期才会有真实值，其他周期保持“—”。
            "status_15m": statuses.get("15m", {}).get("status", "—"),
            "status_1h": statuses.get("1h", {}).get("status", "—"),
            "status_4h": statuses.get("4h", {}).get("status", "—"),
            "status_1d": statuses.get("1d", {}).get("status", "—"),
            "status_2d": statuses.get("2d", {}).get("status", "—"),
            "latest_warn_time": (latest_warn.get("time") if latest_warn else None),
            "latest_warn_type": (latest_warn.get("type") if latest_warn else None),
            "latest_distance": (latest_warn.get("distance") if latest_warn else None),
            "latest_ratio": (latest_warn.get("ratio") if latest_warn else None),
            "latest_bar_dir": (latest_warn.get("bar_dir") if latest_warn else None),
            "updated_at": now_ts,
        }
        return out, local_dbg

    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = [ex.submit(_analyze_one, c) for c in candidates[:limit]]
        for f in as_completed(futs):
            try:
                row, ld = f.result()
                if row:
                    items.append(row)
                if debug and ld:
                    dbg["scanned"] += int(ld.get("scanned") or 0)
                    for tf in ("15m", "1h", "4h", "1d", "2d"):
                        dbg["tf_scanned"][tf] += int((ld.get("tf_scanned") or {}).get(tf) or 0)
                        dbg["tf_prealert"][tf] += int((ld.get("tf_prealert") or {}).get(tf) or 0)
                        dbg["tf_insufficient"][tf] += int((ld.get("tf_insufficient") or {}).get(tf) or 0)

                        # min aggregation
                        rmin = (ld.get("tf_min_ratio") or {}).get(tf)
                        if rmin is not None:
                            cur = dbg["tf_min_ratio"][tf]
                            if cur is None or float(rmin) < float(cur):
                                dbg["tf_min_ratio"][tf] = float(rmin)

                        gmin = (ld.get("tf_min_abs_gap") or {}).get(tf)
                        if gmin is not None:
                            curg = dbg["tf_min_abs_gap"][tf]
                            if curg is None or float(gmin) < float(curg):
                                dbg["tf_min_abs_gap"][tf] = float(gmin)

                        dbg["tf_ratio_pass"][tf] += int((ld.get("tf_ratio_pass") or {}).get(tf) or 0)
                        dbg["tf_slope_pass"][tf] += int((ld.get("tf_slope_pass") or {}).get(tf) or 0)
            except Exception as e:
                errors.append(str(e))

    payload = {"items": items, "errors": errors}
    if debug:
        payload["debug"] = dbg
        return JSONResponse(payload)

    _cache_set(ck, payload)
    return JSONResponse(payload)


@app.get("/api/macd_prealert_detail")
def macd_prealert_detail(contract: str, tf: str = "1h", limit: int = 200) -> JSONResponse:
    tf = (tf or "1h").strip()
    if tf not in ("15m", "1h", "4h", "1d", "2d"):
        tf = "1h"
    limit = max(80, min(300, int(limit)))
    ck = f"macd_prealert_detail:{contract}:{tf}:{limit}"
    cached = _cache_get(ck, ttl=60)
    if cached is not None:
        return JSONResponse(cached)

    candles = get_macd_candles(contract, tf, limit=limit)
    seq = [x for x in candles if isinstance(x, dict)]
    seq.sort(key=lambda x: int(x.get("t") or 0))
    closes = [float(x.get("c")) for x in seq if _safe_float(x.get("c")) is not None]
    dif, dea, hist = _macd(closes, 12, 26, 9)

    # 对齐长度
    n = min(len(closes), len(dif), len(dea), len(hist), len(seq))
    out = {
        "contract": contract,
        "timeframe": tf,
        "t": [int(seq[i].get("t") or 0) for i in range(len(seq) - n, len(seq))],
        "close": closes[len(closes) - n :],
        "dif": dif[len(dif) - n :],
        "dea": dea[len(dea) - n :],
        "hist": hist[len(hist) - n :],
    }
    _cache_set(ck, out)
    return JSONResponse(out)


@app.get("/api/macd_signal_detail")
def macd_signal_detail(
    contract: str,
    tf: str = "1h",
    center_ts: int = 0,
    before: int = 80,
    after: int = 40,
    max_fetch: int = 320,
) -> JSONResponse:
    tf = (tf or "1h").strip()
    if tf not in ("15m", "1h", "4h", "1d", "2d"):
        tf = "1h"
    try:
        center_ts = int(center_ts or 0)
    except Exception:
        center_ts = 0
    before = max(30, min(220, int(before)))
    after = max(10, min(220, int(after)))
    max_fetch = max(120, min(800, int(max_fetch)))

    ck = f"macd_signal_detail:{contract}:{tf}:{center_ts}:{before}:{after}:{max_fetch}"
    cached = _cache_get(ck, ttl=60)
    if cached is not None:
        return JSONResponse(cached)

    fetch_limit = min(max_fetch, max(120, before + after + 60))
    candles = get_macd_candles(contract, tf, limit=fetch_limit)
    seq = [x for x in candles if isinstance(x, dict)]
    seq.sort(key=lambda x: int(x.get("t") or 0))

    if not seq:
        out_empty = {"contract": contract, "timeframe": tf, "t": [], "close": [], "dif": [], "dea": [], "hist": []}
        _cache_set(ck, out_empty)
        return JSONResponse(out_empty)

    # 找到最接近 center_ts 的 candle index（如果 center_ts 为空，则默认取最后一根）
    if center_ts > 0:
        best_i = len(seq) - 1
        best_d = None
        for i, it in enumerate(seq):
            ts = int(it.get("t") or 0)
            d = abs(ts - center_ts)
            if best_d is None or d < best_d:
                best_d = d
                best_i = i
        center_i = best_i
    else:
        center_i = len(seq) - 1

    start_i = max(0, center_i - before)
    end_i = min(len(seq), center_i + after + 1)
    win = seq[start_i:end_i]

    highs: List[float] = []
    lows: List[float] = []
    closes: List[float] = []
    vols: List[float] = []
    valid_win: List[dict] = []
    for it in win:
        c = _safe_float(it.get("c"))
        h = _safe_float(it.get("h"))
        l = _safe_float(it.get("l"))
        v = _safe_float(it.get("v"))
        if c is None or h is None or l is None:
            continue
        valid_win.append(it)
        highs.append(float(h))
        lows.append(float(l))
        closes.append(float(c))
        vols.append(float(v or 0.0))

    dif, dea, hist = _macd(closes, 12, 26, 9)
    ema50 = _ema(closes, 50) if closes else []
    adx14 = _adx(highs, lows, closes, 14) if closes else []
    vol_sma20 = _sma(vols, 20) if vols else []
    n = min(len(closes), len(dif), len(dea), len(hist), len(valid_win))
    out = {
        "contract": contract,
        "timeframe": tf,
        "center_ts": center_ts,
        "center_i": int(center_i - start_i),
        "t": [int(valid_win[i].get("t") or 0) for i in range(len(valid_win) - n, len(valid_win))],
        "close": closes[len(closes) - n :],
        "dif": dif[len(dif) - n :],
        "dea": dea[len(dea) - n :],
        "hist": hist[len(hist) - n :],
        # 额外指标：用于前端叠加画线（EMA50/ADX14/成交量SMA20）
        "ema50": ema50[len(ema50) - n :] if ema50 else [],
        "adx14": adx14[len(adx14) - n :] if adx14 else [],
        "vol": vols[len(vols) - n :] if vols else [],
        "vol_sma20": vol_sma20[len(vol_sma20) - n :] if vol_sma20 else [],
    }
    _cache_set(ck, out)
    return JSONResponse(out)


@app.get("/api/anomalies")
def anomalies(timeframe: str = "1h", top_n: int = 50, lookback: int = 1) -> JSONResponse:
    """市场异动检测（TopN 合约池）。

    逻辑：
    - 先取 TopN（按 24h 成交额）合约列表
    - 对每个合约调用 compute_row 计算 price_change_pct / oi_change_pct / score
    - 使用四象限 classify 分桶，并对每个桶按 score 降序排序

    参数：
    - timeframe：15m/1h/4h/1d
    - top_n：TopN 样本池（上限 200）
    - lookback：变化幅度的跨度（与主表保持一致时可传 6）
    """
    if timeframe not in TIMEFRAMES:
        return JSONResponse({"error": "invalid timeframe"}, status_code=400)
    top_n = max(10, min(200, int(top_n)))
    lookback = max(1, min(24, int(lookback or 1)))

    ck = f"anomalies:{timeframe}:{top_n}:lb{lookback}"
    cached = _cache_get(ck, ttl=20)
    if cached is not None:
        return JSONResponse(cached)

    errors: List[str] = []
    try:
        contracts = top_contracts_by_quote_volume(top_n)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=200)

    rows: List[Row] = []
    # 50 个合约 * (candles + contract_stats) 两个请求；用并发显著降低整体耗时
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {ex.submit(compute_row, c, timeframe, lookback): c for c in contracts}
        for f in as_completed(futs):
            c = futs[f]
            try:
                rows.append(f.result())
            except Exception as e:
                errors.append(f"{c}: {e}")

    buckets: Dict[str, List[dict]] = {
        "多头获利了结": [],
        "多头强势进场": [],
        "空头获利了结": [],
        "空头强势进场": [],
    }

    for r in rows:
        k = classify(r.price_change_pct, r.oi_change_pct)
        if k:
            buckets[k].append(r.__dict__)

    # 每个桶按强度分数降序，Top3 与详情一致
    try:
        for k in buckets.keys():
            buckets[k].sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
    except Exception:
        pass

    out = {
        "timeframe": timeframe,
        "top_n": top_n,
        "lookback": lookback,
        "counts": {k: len(v) for k, v in buckets.items()},
        "top3": {k: [x["contract"] for x in v[:3]] for k, v in buckets.items()},
        "details": buckets,
        "errors": errors,
    }

    _cache_set(ck, out)
    return JSONResponse(out)


@app.get("/api/macd_signals")
def macd_signals(limit: int = 50, only_signal: int = 0, timeframe: str = "all") -> JSONResponse:
    # 返回：市值前N（过滤稳定币）对应的 Gate USDT 永续合约，在 15m/1h/1d 的 MACD 金叉/死叉信号
    limit = max(10, min(200, int(limit)))
    timeframe = (timeframe or "all").strip().lower()
    if timeframe not in ("all", "15m", "1h", "4h", "1d", "2d"):
        timeframe = "all"
    ck = f"macd_signals:{limit}:{only_signal}:{timeframe}"
    cached = _cache_get(ck, ttl=60)
    if cached is not None:
        return JSONResponse(cached)

    errors: List[str] = []
    items: List[dict] = []

    try:
        top = coingecko_top_marketcap(limit)
    except Exception as e:
        return JSONResponse({"error": f"CoinGecko 获取失败: {e}"}, status_code=200)

    try:
        contract_set = set(get_all_futures_contract_names())
    except Exception as e:
        return JSONResponse({"error": f"Gate 合约列表获取失败: {e}"}, status_code=200)

    last_price_map = _ticker_last_price_map()

    # 组装候选合约
    candidates: List[dict] = []
    for it in top:
        sym = str(it.get("symbol") or "").upper().strip()
        if not sym:
            continue
        contract = f"{sym}_USDT"
        if contract not in contract_set:
            continue
        candidates.append({
            "symbol": sym,
            "contract": contract,
            "market_cap_rank": it.get("market_cap_rank"),
            "market_cap": it.get("market_cap"),
        })

    def _analyze_one(cand: dict) -> List[dict]:
        out_rows: List[dict] = []
        contract = cand["contract"]
        symbol = cand["symbol"]
        last_px = last_price_map.get(contract)
        now_ts = int(time.time())

        tfs = ("15m", "1h", "4h", "1d", "2d") if timeframe == "all" else (timeframe,)
        for tf in tfs:
            try:
                candles = get_macd_candles(contract, tf, limit=120)
                seq = [x for x in candles if isinstance(x, dict)]
                seq.sort(key=lambda x: int(x.get("t") or 0))
                closes = [float(x.get("c")) for x in seq if _safe_float(x.get("c")) is not None]
                if len(closes) < 50:
                    continue
                dif, dea, hist = _macd(closes, 12, 26, 9)
                if not dif:
                    continue
                cross = detect_recent_cross(dif, dea, lookback=3)
                signal_type = None
                signal_idx = None
                signal_time = None
                signal_price = None
                if cross:
                    signal_type, signal_idx = cross
                    # dif/dea/hist 与 closes 等长，seq 也按时间排好
                    if signal_idx is not None and signal_idx < len(seq):
                        signal_time = int(seq[signal_idx].get("t") or 0)
                        signal_price = _safe_float(seq[signal_idx].get("c"))

                macd_state = "多头" if dif[-1] > dea[-1] else "空头"
                # 信号强度（归一化，百分比）：abs(MACD柱子)/当前收盘价 * 100
                # 这样不同币之间更可比（至少量纲统一）
                last_close = closes[-1] if closes else None
                raw_strength = abs(hist[-1]) if hist else abs(dif[-1] - dea[-1])
                if last_close is None or last_close == 0:
                    strength = None
                else:
                    strength = float(raw_strength) / float(last_close) * 100.0

                row = {
                    "symbol": symbol,
                    "contract": contract,
                    "timeframe": tf,
                    "market_cap_rank": cand.get("market_cap_rank"),
                    "market_cap": cand.get("market_cap"),
                    "current_price": last_px,
                    "macd_state": macd_state,
                    "signal_type": signal_type,
                    "signal_time": signal_time,
                    "signal_price": signal_price,
                    "signal_strength": strength,
                    "updated_at": now_ts,
                }
                if only_signal and not signal_type:
                    continue
                out_rows.append(row)
            except Exception as e:
                errors.append(f"{contract} {tf}: {e}")
        return out_rows

    # 并发扫描：worker 过大容易触发 Gate REST 429
    max_workers = 6
    if limit <= 20:
        max_workers = 4
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_analyze_one, c) for c in candidates[:limit]]
        for f in as_completed(futs):
            try:
                items.extend(f.result() or [])
            except Exception as e:
                errors.append(str(e))

    payload = {"items": items, "errors": errors}
    _cache_set(ck, payload)
    return JSONResponse(payload)
