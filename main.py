import os
import time
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from typing import Any, Dict, List, Optional, Tuple
import base64
import datetime
import hashlib
import json
import math
import os
import re
import sqlite3
from urllib.parse import urlparse, parse_qs, unquote

import requests
try:
    import feedparser  # type: ignore
except Exception:
    feedparser = None
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles


def _load_dotenv_if_present() -> None:
    try:
        base_dir = os.path.dirname(__file__)
        env_path = os.path.join(base_dir, ".env")
        if not os.path.exists(env_path):
            return
        with open(env_path, "r", encoding="utf-8") as f:
            for raw in f:
                line = (raw or "").strip()
                if not line:
                    continue
                if line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = (k or "").strip()
                v = (v or "").strip()
                if not k:
                    continue
                if k in os.environ and (os.environ.get(k) or "") != "":
                    continue
                if len(v) >= 2 and ((v[0] == '"' and v[-1] == '"') or (v[0] == "'" and v[-1] == "'")):
                    v = v[1:-1]
                os.environ[k] = v
    except Exception:
        return


_load_dotenv_if_present()


APP_TITLE = "Gate 永续合约仪表板"

GATE_MCP_SERVER_URL = os.getenv("GATE_MCP_SERVER_URL", "https://api.gatemcp.ai/mcp")
GATE_MCP_TOKEN = os.getenv("GATE_MCP_TOKEN", "").strip()

GATE_REST_FUTURES_USDT_BASE = "https://api.gateio.ws/api/v4/futures/usdt"

# 复用连接，减少每次请求的握手开销
HTTP = requests.Session()
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
NEWS_RSS_FEEDS = os.getenv(
    "NEWS_RSS_FEEDS",
    "https://news.google.com/rss/search?q=cryptocurrency%20OR%20bitcoin%20OR%20ethereum%20when%3A1d&hl=en-US&gl=US&ceid=US:en;https://news.google.com/rss/search?q=%E5%8A%A0%E5%AF%86%E8%B4%A7%E5%B8%81%20OR%20%E6%AF%94%E7%89%B9%E5%B8%81%20OR%20%E4%BB%A5%E5%A4%AA%E5%9D%8A%20when%3A1d&hl=zh-CN&gl=CN&ceid=CN:zh-Hans;https://cointelegraph.com/rss;https://decrypt.co/feed;https://www.8btc.com/feed;https://www.odaily.news/rss;https://www.jinse.com/rss",
)

TELEGRAM_API_BASE = os.getenv("TELEGRAM_API_BASE", "https://api.telegram.org").strip() or "https://api.telegram.org"
TELEGRAM_CONNECT_TIMEOUT = float(os.getenv("TELEGRAM_CONNECT_TIMEOUT", "10") or "10")
TELEGRAM_READ_TIMEOUT = float(os.getenv("TELEGRAM_READ_TIMEOUT", "20") or "20")

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
    """合并推送：把最近 window_sec 内满足条件的新闻合成 1 条消息发送（仍按 uniq 去重落库）。"""
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
    finally:
        conn.close()


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
    """拉取 MACD 预警结果并合并推送 1 条 TG 消息；每个预警按 uniq 去重写入 push_history。"""
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
            SELECT uniq, title, link, coins, sentiment, strength
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

                sent_cn = "利多" if sentiment == "bullish" else "利空"
                coins_cn = coins if coins else "—"
                msg = (
                    f"【新闻多空哨兵】{sent_cn} 强度 {float(strength):.2f}\n"
                    f"币种: {coins_cn}\n"
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
    feeds = []
    for x in (NEWS_RSS_FEEDS or "").split(";"):
        u = (x or "").strip()
        if u:
            feeds.append(u)
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


def _rule_sentiment(title: str, summary: str) -> Tuple[str, float]:
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
    for w in bull_words:
        if w in text:
            score += 1
    for w in bear_words:
        if w in text:
            score -= 1

    if score > 0:
        return "bullish", min(1.0, 0.35 + 0.10 * score)
    if score < 0:
        return "bearish", min(1.0, 0.35 + 0.10 * abs(score))
    return "neutral", 0.30


def _llm_prompt(title: str, summary: str, source: str, tags: str) -> str:
    return (
        "你是加密货币新闻情绪分析器。\n"
        "请根据新闻标题、摘要、来源、标签判断对对应币种/市场的影响倾向，并输出严格 JSON。\n"
        "只允许输出一个 JSON 对象，不要输出任何解释。\n\n"
        "输出 JSON Schema:\n"
        "{\n"
        '  "sentiment": "bullish|bearish|neutral",\n'
        '  "strength": 0.0-1.0\n'
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


def analyze_news_item(title: str, summary: str, source: str, tags: str) -> Tuple[str, float, str]:
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
        if sent and strength is not None:
            return sent, float(strength), provider

    sent2, str2 = _rule_sentiment(title=title, summary=summary)
    return sent2, float(str2), provider


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
                WHERE sentiment IS NULL OR sentiment = ''
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

                sent, strength, provider = analyze_news_item(
                    title=title,
                    summary=summary,
                    source=source,
                    tags=tags,
                )
                conn.execute(
                    "UPDATE news_items SET sentiment=?, strength=? WHERE id=?",
                    (sent, float(strength), rid),
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
    if not feeds:
        return {"feeds": 0, "inserted": 0, "skipped": 0, "errors": ["NEWS_RSS_FEEDS 为空"]}

    if feedparser is None:
        return {
            "feeds": len(feeds),
            "inserted": 0,
            "skipped": 0,
            "errors": ["缺少依赖 feedparser，请先安装 requirements.txt 后再抓取 RSS"],
        }

    inserted = 0
    skipped = 0
    errors: List[str] = []
    now_ts = int(time.time())

    def _fetch_one(feed_url: str) -> Tuple[List[Tuple[str, str, str, str, Optional[int], str, str, str, int]], Optional[str]]:
        try:
            # 更快失败：连接超时更短，读取超时按 timeout_sec
            r = HTTP.get(feed_url, timeout=(4, timeout_sec))
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

    all_rows: List[Tuple[str, str, str, str, Optional[int], str, str, str, int]] = []
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
    "1d": "1d",
}


@dataclass
class Row:
    contract: str
    timeframe: str
    last_price: Optional[float]
    price_change_pct: Optional[float]
    oi_change_pct: Optional[float]
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

def _rest_get(path: str, params: Optional[dict] = None) -> Any:
    url = f"{GATE_REST_FUTURES_USDT_BASE}{path}"
    r = HTTP.get(url, params=params, timeout=8)
    if r.status_code != 200:
        raise RuntimeError(f"Gate REST HTTP {r.status_code}: {r.text[:200]}")
    return r.json()


def _rest_get_full_url(url: str, params: Optional[dict] = None, timeout: int = 10) -> Any:
    r = HTTP.get(url, params=params, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    return r.json()


# ==========================
# MCP client (HTTP)
# ==========================

def _mcp_headers() -> Dict[str, str]:
    hdrs = {"Content-Type": "application/json"}
    if GATE_MCP_TOKEN:
        hdrs["Authorization"] = f"Bearer {GATE_MCP_TOKEN}"
    return hdrs


def _mcp_post(payload: dict) -> dict:
    r = HTTP.post(GATE_MCP_SERVER_URL, json=payload, headers=_mcp_headers(), timeout=12)
    if r.status_code != 200:
        raise RuntimeError(f"MCP HTTP {r.status_code}: {r.text[:200]}")
    data = r.json()
    if "error" in data and data["error"]:
        raise RuntimeError(str(data["error"]))
    return data


def mcp_tools_list() -> List[dict]:
    data = _mcp_post({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    res = data.get("result") or {}
    tools = res.get("tools")
    return tools if isinstance(tools, list) else []


def mcp_tools_call(name: str, arguments: dict) -> Any:
    data = _mcp_post({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    })
    res = data.get("result") or {}
    # MCP 结果常见格式：content 为 list，其中包含 text/json
    if "content" in res:
        return res["content"]
    return res


def _content_to_json(content: Any) -> Any:
    # content 可能是: [{"type":"text","text":"..."}] 或 [{"type":"json","json":{...}}]
    if isinstance(content, list):
        for item in content:
            if isinstance(item, dict) and item.get("type") == "json":
                return item.get("json")
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                # 尝试解析 text 为 JSON
                txt = item.get("text")
                if isinstance(txt, str):
                    try:
                        return requests.utils.json.loads(txt)
                    except Exception:
                        return txt
    return content


# ==========================
# Gate data helpers
# ==========================

def get_candles(contract: str, tf: str, limit: int = 2) -> List[dict]:
    interval = TIMEFRAMES[tf]
    # 优先直接用 REST，MCP 的工具名不确定；后续可通过 tools/list 做映射
    data = _rest_get("/candlesticks", params={"contract": contract, "interval": interval, "limit": limit})
    # REST futures candlesticks: list[dict] with keys t,o,h,l,c,v,sum
    return data if isinstance(data, list) else []


def get_macd_candles(contract: str, tf: str, limit: int = 120) -> List[dict]:
    # MACD 扫描用：只取最近 100-150 根，避免全量历史
    interval = MACD_TIMEFRAMES[tf]
    ck = f"macd:candles:{contract}:{tf}:{limit}"
    cached = _cache_get(ck, ttl=180)
    if cached is not None:
        return cached
    data = _rest_get("/candlesticks", params={"contract": contract, "interval": interval, "limit": limit})
    out = data if isinstance(data, list) else []
    _cache_set(ck, out)
    return out


def get_contract_stats(contract: str, tf: str, limit: int = 2) -> List[dict]:
    interval = TIMEFRAMES[tf]
    data = _rest_get("/contract_stats", params={"contract": contract, "interval": interval, "limit": limit})
    return data if isinstance(data, list) else []


def _pick_oi(stat: Dict[str, Any]) -> Optional[float]:
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
    # CoinGecko 免费接口，无需 key；这里拿前 100 再过滤稳定币，最后截取 limit
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
    try:
        data = _rest_get_full_url(
            url,
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 100,
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


def compute_row(contract: str, tf: str) -> Row:
    ck = f"row:{contract}:{tf}"
    cached = _cache_get(ck, ttl=15)
    if cached is not None:
        try:
            return Row(**cached)
        except Exception:
            pass

    updated_at = int(time.time())

    candles = get_candles(contract, tf, limit=2)
    # 价格变化：按收盘价变化 (Close_last-Close_prev)/Close_prev * 100
    # 注意：REST 返回顺序可能变化，这里按时间戳 t 排序确保取到最后两根
    seq = [x for x in candles if isinstance(x, dict)]
    seq.sort(key=lambda x: int(x.get("t") or 0))

    prev_close = _safe_float(seq[-2].get("c")) if len(seq) >= 2 else None
    last_close = _safe_float(seq[-1].get("c")) if len(seq) >= 1 else None

    last_price = last_close
    if prev_close is None or prev_close == 0 or last_close is None:
        price_change_pct = None
    else:
        price_change_pct = (last_close - prev_close) / prev_close * 100.0

    stats = get_contract_stats(contract, tf, limit=2)
    stat_seq = [x for x in stats if isinstance(x, dict)]
    stat_seq.sort(key=lambda x: int(x.get("t") or x.get("time") or 0))
    prev_oi = _pick_oi(stat_seq[-2]) if len(stat_seq) >= 2 else None
    last_oi = _pick_oi(stat_seq[-1]) if len(stat_seq) >= 1 else None
    oi_change_pct = _pct_change(last_oi, prev_oi)

    market_signal = classify(price_change_pct, oi_change_pct)

    row = Row(
        contract=contract,
        timeframe=tf,
        last_price=last_price,
        price_change_pct=price_change_pct,
        oi_change_pct=oi_change_pct,
        market_signal=market_signal,
        updated_at=updated_at,
    )

    _cache_set(ck, row.__dict__)  # Cache the result
    return row


def classify(price_pct: Optional[float], oi_pct: Optional[float]) -> Optional[str]:
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
    # 返回 score, reasons, level
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
            score += 4.0 if oi_confirm else 2.0
            reasons.append(f"资费率 {funding*100:.2f}%（极负）" + ("（无OI确认，降档）" if not oi_confirm else ""))
        elif funding <= -0.0001:
            score += 2.0
            reasons.append(f"资费率 {funding*100:.2f}%（负）")
        elif funding >= 0.0003:
            score -= 4.0 if oi_confirm else 2.0
            reasons.append(f"资费率 {funding*100:.2f}%（极正）" + ("（无OI确认，降档）" if not oi_confirm else ""))
        elif funding >= 0.0001:
            score -= 2.0
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
            vscore = 2.0
        elif vr >= 1.5:
            vscore = 1.0

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
        score += 3.0
        trend_confirm = True
        reasons.append("MACD 金叉")
    elif macd_status == "即将金叉":
        score += 1.5
        reasons.append("MACD 即将金叉")
    elif macd_status == "死叉":
        score -= 3.0
        trend_confirm = True
        reasons.append("MACD 死叉")
    elif macd_status == "即将死叉":
        score -= 1.5
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
        "mcp_configured": bool(GATE_MCP_TOKEN),
        "mcp_server_url": GATE_MCP_SERVER_URL,
    }


@app.get("/api/tools")
def tools() -> JSONResponse:
    if not GATE_MCP_TOKEN:
        return JSONResponse({"error": "未设置 GATE_MCP_TOKEN"}, status_code=200)
    try:
        tools = mcp_tools_list()
        names = [t.get("name") for t in tools if isinstance(t, dict)]
        return JSONResponse({"items": names})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=200)


@app.get("/api/summary")
def summary(timeframe: str = "1h") -> JSONResponse:
    if timeframe not in TIMEFRAMES:
        return JSONResponse({"error": "invalid timeframe"}, status_code=400)

    ck = f"summary:{timeframe}"
    cached = _cache_get(ck, ttl=10)
    if cached is not None:
        return JSONResponse(cached)

    items: List[dict] = []
    errors: List[str] = []
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(compute_row, c, timeframe): c for c in CONTRACTS_5}
        for f in as_completed(futs):
            c = futs[f]
            try:
                r = f.result()
                items.append(r.__dict__)
            except Exception as e:
                errors.append(f"{c}: {e}")

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
            SELECT id, source, title, title_zh, link, published_at, summary, summary_zh, tags, coins, sentiment, strength, created_at, translated_at
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
def macd_prealerts(limit: int = 50, only_warn: int = 0, warn_type: str = "all", debug: int = 0) -> JSONResponse:
    limit = max(10, min(100, int(limit)))
    warn_type = (warn_type or "all").strip().lower()
    if warn_type not in ("all", "pre_golden", "pre_death"):
        warn_type = "all"

    ck = f"macd_prealerts:{limit}:{only_warn}:{warn_type}:{int(1 if debug else 0)}"
    if not debug:
        cached = _cache_get(ck, ttl=30)
        if cached is not None:
            return JSONResponse(cached)

    errors: List[str] = []
    items: List[dict] = []
    dbg = {
        "candidates": 0,
        "scanned": 0,
        "tf_scanned": {"15m": 0, "1h": 0, "1d": 0},
        "tf_prealert": {"15m": 0, "1h": 0, "1d": 0},
        "tf_insufficient": {"15m": 0, "1h": 0, "1d": 0},
        "tf_min_ratio": {"15m": None, "1h": None, "1d": None},
        "tf_min_abs_gap": {"15m": None, "1h": None, "1d": None},
        "tf_ratio_pass": {"15m": 0, "1h": 0, "1d": 0},
        "tf_slope_pass": {"15m": 0, "1h": 0, "1d": 0},
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
                "tf_scanned": {"15m": 0, "1h": 0, "1d": 0},
                "tf_prealert": {"15m": 0, "1h": 0, "1d": 0},
                "tf_insufficient": {"15m": 0, "1h": 0, "1d": 0},
                "tf_min_ratio": {"15m": None, "1h": None, "1d": None},
                "tf_min_abs_gap": {"15m": None, "1h": None, "1d": None},
                "tf_ratio_pass": {"15m": 0, "1h": 0, "1d": 0},
                "tf_slope_pass": {"15m": 0, "1h": 0, "1d": 0},
            }

        # per timeframe status
        statuses: Dict[str, dict] = {}
        latest_warn = None

        for tf in ("15m", "1h", "1d"):
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
            "status_15m": statuses.get("15m", {}).get("status", "—"),
            "status_1h": statuses.get("1h", {}).get("status", "—"),
            "status_1d": statuses.get("1d", {}).get("status", "—"),
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
                    for tf in ("15m", "1h", "1d"):
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
    if tf not in ("15m", "1h", "1d"):
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


@app.get("/api/anomalies")
def anomalies(timeframe: str = "1h", top_n: int = 50) -> JSONResponse:
    if timeframe not in TIMEFRAMES:
        return JSONResponse({"error": "invalid timeframe"}, status_code=400)
    top_n = max(10, min(200, int(top_n)))

    ck = f"anomalies:{timeframe}:{top_n}"
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
        futs = {ex.submit(compute_row, c, timeframe): c for c in contracts}
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

    out = {
        "timeframe": timeframe,
        "top_n": top_n,
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
    limit = max(10, min(100, int(limit)))
    timeframe = (timeframe or "all").strip().lower()
    if timeframe not in ("all", "15m", "1h", "1d"):
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

        tfs = ("15m", "1h", "1d") if timeframe == "all" else (timeframe,)
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
