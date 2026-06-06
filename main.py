import os
import re
import sqlite3
import threading
import time
import warnings
import heapq
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
import asyncio

import requests
import httpx
try:
    import pandas as pd
    import numpy as np
except Exception:
    pd = None
    np = None
try:
    import feedparser  # type: ignore
except Exception:
    feedparser = None
import xml.etree.ElementTree as ET
try:
    import yfinance as yf
except Exception:
    yf = None
from fastapi import FastAPI, Request

# Finnhub API 客户端（CUSIP → Ticker 映射）
try:
    import finnhub
    _finnhub_client = None
    def _get_finnhub_client():
        global _finnhub_client
        if _finnhub_client is not None:
            return _finnhub_client
        key = (os.getenv("FINNHUB_API_KEY") or "").strip()
        if key:
            _finnhub_client = finnhub.Client(api_key=key)
            return _finnhub_client
        return None
except Exception:
    finnhub = None
    def _get_finnhub_client(): return None
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi import Query
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

SEC_EDGAR_CACHE_TTL_SEC = int(float(os.getenv("SEC_EDGAR_CACHE_TTL_SEC", "21600") or "21600"))
SEC_EDGAR_USER_AGENT = (
    os.getenv(
        "SEC_EDGAR_USER_AGENT",
        "gate-mcp-dashboard-web/1.0 (contact: you@example.com)",
    )
    or "gate-mcp-dashboard-web/1.0 (contact: you@example.com)"
).strip()

SMARTMONEY_REFRESH_TOKEN = (os.getenv("SMARTMONEY_REFRESH_TOKEN") or "").strip()

UPSTASH_REDIS_REST_URL = (os.getenv("UPSTASH_REDIS_REST_URL") or "").strip().rstrip("/")
UPSTASH_REDIS_REST_TOKEN = (os.getenv("UPSTASH_REDIS_REST_TOKEN") or "").strip()
SMARTMONEY_SNAPSHOT_KEY = (os.getenv("SMARTMONEY_SNAPSHOT_KEY") or "smartmoney:institutions:snapshot:v1").strip()
SMARTMONEY_META_KEY = (os.getenv("SMARTMONEY_META_KEY") or "smartmoney:institutions:meta:v1").strip()
SEC_EDGAR_PER_INST_DELAY_SEC = float(os.getenv("SEC_EDGAR_PER_INST_DELAY_SEC", "0.15") or "0.15")

# 真实 13F（SEC EDGAR）机构配置（方案A：只依赖 CIK）
_SMARTMONEY_INSTITUTIONS_META: Dict[str, Dict[str, Any]] = {
     "brk": {"id": "berkshire-hathaway", "name": "Berkshire Hathaway", "cik": "0001067983"},
     "blk": {"id": "blk", "name": "BlackRock", "cik": "0002012383"},
     "vgi": {"id": "vgi", "name": "Vanguard", "cik": "0000102909"},
     "nvda": {"id": "nvda", "name": "NVIDIA", "cik": "0001045810"},
 } 

_SMARTMONEY_SNAPSHOT_LOCK = threading.Lock()
_SMARTMONEY_SNAPSHOT: Dict[str, Any] = {"ts": 0.0, "items": [], "last_error": ""}

_SMARTMONEY_META_LOCK = threading.Lock()
_SMARTMONEY_META: Dict[str, Any] = {"ts": 0.0, "items": []}

# 手动刷新任务状态跟踪
_SMARTMONEY_REFRESH_TASKS_LOCK = threading.Lock()
_SMARTMONEY_REFRESH_TASKS: Dict[str, Dict[str, Any]] = {}


def _upstash_cmd(args: List[Any]) -> Any:
    if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
        raise RuntimeError("upstash_not_configured")
    url = f"{UPSTASH_REDIS_REST_URL}/pipeline"
    headers = {
        "Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}",
        "Content-Type": "application/json",
    }
    # Upstash Redis REST 推荐使用 pipeline：POST /pipeline 发送命令数组，避免 URL 编码与长度限制
    payload = [[str(x) for x in (args or [])]]
    r = HTTP.post(url, headers=headers, json=payload, timeout=(10, 30))
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list) and data:
        return data[0]
    return data


def _upstash_pipeline(cmds: List[List[Any]]) -> Any:
    if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
        raise RuntimeError("upstash_not_configured")
    url = f"{UPSTASH_REDIS_REST_URL}/pipeline"
    headers = {
        "Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = [[str(x) for x in (c or [])] for c in (cmds or []) if isinstance(c, list) and c]
    if not payload:
        return []
    r = HTTP.post(url, headers=headers, json=payload, timeout=(10, 60))
    r.raise_for_status()
    return r.json()


def _upstash_get_snapshot() -> Optional[Dict[str, Any]]:
    try:
        resp = _upstash_cmd(["GET", SMARTMONEY_SNAPSHOT_KEY])
        res = resp.get("result") if isinstance(resp, dict) else None
        if not res:
            return None
        if isinstance(res, str):
            try:
                obj = json.loads(res)
                return obj if isinstance(obj, dict) else None
            except Exception:
                return None
        return None
    except Exception:
        return None


def _upstash_set_snapshot(obj: Dict[str, Any], ttl_sec: int = 0) -> bool:
    try:
        raw = json.dumps(obj, ensure_ascii=False)
        if ttl_sec and ttl_sec > 0:
            _upstash_cmd(["SET", SMARTMONEY_SNAPSHOT_KEY, raw, "EX", str(int(ttl_sec))])
        else:
            _upstash_cmd(["SET", SMARTMONEY_SNAPSHOT_KEY, raw])
        return True
    except Exception:
        return False


def _upstash_get_json(key: str) -> Optional[Any]:
    try:
        if not key:
            return None
        print(f"[DEBUG] _upstash_get_json: getting key={key}")
        resp = _upstash_cmd(["GET", key])
        print(f"[DEBUG] _upstash_get_json: resp={resp}")
        res = resp.get("result") if isinstance(resp, dict) else None
        print(f"[DEBUG] _upstash_get_json: res type={type(res)}, len={len(res) if isinstance(res, str) else 'N/A'}")
        if not res:
            return None
        if isinstance(res, str):
            try:
                return json.loads(res)
            except Exception:
                return None
        return res
    except Exception as e:
        print(f"[DEBUG] _upstash_get_json: error={e}")
        return None


def _upstash_set_json(key: str, obj: Any, ttl_sec: int = 0) -> bool:
    try:
        if not key:
            print(f"[DEBUG] _upstash_set_json: empty key")
            return False
        raw = json.dumps(obj, ensure_ascii=False)
        print(f"[DEBUG] _upstash_set_json: key={key}, raw_len={len(raw)}, ttl={ttl_sec}")
        if ttl_sec and ttl_sec > 0:
            _upstash_cmd(["SET", key, raw, "EX", str(int(ttl_sec))])
        else:
            _upstash_cmd(["SET", key, raw])
        print(f"[DEBUG] _upstash_set_json: success")
        return True
    except Exception as e:
        print(f"[DEBUG] _upstash_set_json: error={e}")
        return False


def _sm_snap_key_inst(iid: str) -> str:
    return f"{SMARTMONEY_SNAPSHOT_KEY}:inst:{(iid or '').strip().lower()}"


def _sm_snap_key_flows(sector: str, period: str) -> str:
    s = (sector or "all").strip() or "all"
    p = (period or "quarter").strip() or "quarter"
    return f"{SMARTMONEY_SNAPSHOT_KEY}:flows:{s}:{p}"


def _sm_snap_key_stock(cusip: str) -> str:
    c = re.sub(r"\s+", "", (cusip or "").strip().upper())
    return f"{SMARTMONEY_SNAPSHOT_KEY}:stock:{c}"


def _sm_norm_inst_id(name: str) -> str:
    s = str(name or "").strip().lower()
    if not s:
        return ""
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s[:32]


def _smartmoney_get_institutions_meta() -> List[Dict[str, Any]]:
    # 优先顺序：1.本地JSON文件 2.Upstash 3.内置默认
    items: List[Dict[str, Any]] = []
    
    # 1. 尝试从本地 JSON 文件加载
    try:
        json_path = os.path.join(os.path.dirname(__file__), "institutions_50.json")
        if os.path.exists(json_path):
            with open(json_path, "r", encoding="utf-8") as f:
                file_items = json.load(f)
            if isinstance(file_items, list) and file_items:
                items = [x for x in file_items if isinstance(x, dict) and x.get("cik")]
    except Exception:
        pass
    
    # 2. 如果本地文件没有，尝试 Upstash
    if not items:
        meta_from_upstash: Optional[Any] = None
        if UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN:
            meta_from_upstash = _upstash_get_json(SMARTMONEY_META_KEY)
        if isinstance(meta_from_upstash, dict) and isinstance(meta_from_upstash.get("items"), list):
            try:
                with _SMARTMONEY_META_LOCK:
                    _SMARTMONEY_META["ts"] = float(meta_from_upstash.get("ts") or 0.0)
                    _SMARTMONEY_META["items"] = list(meta_from_upstash.get("items") or [])
            except Exception:
                pass
        with _SMARTMONEY_META_LOCK:
            items0 = _SMARTMONEY_META.get("items")
        if isinstance(items0, list) and items0:
            items = [x for x in items0 if isinstance(x, dict)]
    
    # 3. 最后 fallback 到内置默认
    if not items:
        items = [x for x in _SMARTMONEY_INSTITUTIONS_META.values() if isinstance(x, dict)]

    out: List[Dict[str, Any]] = []
    used: Dict[str, int] = {}
    for it in items:
        nm = str(it.get("name") or "").strip()
        iid = str(it.get("id") or "").strip().lower()
        if not iid:
            iid = _sm_norm_inst_id(nm)
        if not iid:
            continue
        # 去重处理：同名/同 id 的极端情况
        k = iid
        if k in used:
            used[k] += 1
            k = f"{iid}-{used[iid]}"
        else:
            used[k] = 0
        o = dict(it)
        o["id"] = k
        o["name"] = nm
        o["cik"] = str(o.get("cik") or "").strip() or None
        out.append(o)
    return out


def _smartmoney_inst_map() -> Dict[str, Dict[str, Any]]:
    insts = _smartmoney_get_institutions_meta()
    m: Dict[str, Dict[str, Any]] = {}
    for it in insts:
        iid = str(it.get("id") or "").strip().lower()
        if not iid:
            continue
        m[iid] = it
    return m


def _smartmoney_save_institutions_meta(items: List[Dict[str, Any]]) -> bool:
    # 写入 Upstash 主数据（不设置 TTL，作为配置长期存在）
    if not (UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN):
        return False
    try:
        payload = {"ts": int(time.time()), "items": items if isinstance(items, list) else []}
        ok = _upstash_set_json(SMARTMONEY_META_KEY, payload, ttl_sec=0)
        if ok:
            try:
                with _SMARTMONEY_META_LOCK:
                    _SMARTMONEY_META["ts"] = float(payload.get("ts") or 0.0)
                    _SMARTMONEY_META["items"] = list(payload.get("items") or [])
            except Exception:
                pass
        return bool(ok)
    except Exception:
        return False


def _smartmoney_build_items() -> List[Dict[str, Any]]:
    items2: List[Dict[str, Any]] = []
    for inst in _smartmoney_get_institutions_meta():
        name = str(inst.get("name") or "")
        iid = str(inst.get("id") or "")
        if not str(iid or "").strip():
            iid = _sm_norm_inst_id(name)
        cn_name = str(inst.get("cn_name") or "")
        category = str(inst.get("category") or "")
        sub_tags = inst.get("sub_tags") if isinstance(inst.get("sub_tags"), list) else []
        importance_score = inst.get("importance_score")
        smart_money_weight = inst.get("smart_money_weight")
        cik = str(inst.get("cik") or "")
        aum: Optional[float] = None
        prev_aum: Optional[float] = None
        aum_change: Optional[float] = None
        ch_up = 0
        ch_dn = 0
        err = ""
        cur_acc = ""
        prev_acc = ""
        cur_form = ""
        prev_form = ""
        cur_report = ""
        prev_report = ""
        cur_filing = ""
        prev_filing = ""
        try:
            if not str(cik or "").strip():
                cur = {"ok": False, "error": "missing_cik"}
                prev = {"ok": False, "error": "missing_cik"}
            else:
                cur = _sec_get_13f_holdings_by_cik(cik, filing_index=0)
                prev = _sec_get_13f_holdings_by_cik(cik, filing_index=1)
            if not (isinstance(cur, dict) and cur.get("ok")):
                err = str(cur.get("error") or "sec_fetch_failed")
                cur_q = ""
                prev_q = ""
            else:
                aum = float(cur.get("total_value_usd") or 0.0)
                cur_q = str(cur.get("period_quarter") or "")
                prev_q = str(prev.get("period_quarter") or "") if isinstance(prev, dict) and prev.get("ok") else ""
                prev_aum = float(prev.get("total_value_usd") or 0.0) if isinstance(prev, dict) and prev.get("ok") else None

                cur_acc = str(cur.get("accession") or "")
                cur_form = str((cur.get("filing") or {}).get("form") or "") if isinstance(cur.get("filing"), dict) else ""
                cur_report = str(cur.get("report_date") or "")
                cur_filing = str(cur.get("filing_date") or "")
                if isinstance(prev, dict) and prev.get("ok"):
                    prev_acc = str(prev.get("accession") or "")
                    prev_form = str((prev.get("filing") or {}).get("form") or "") if isinstance(prev.get("filing"), dict) else ""
                    prev_report = str(prev.get("report_date") or "")
                    prev_filing = str(prev.get("filing_date") or "")

                if aum is not None and prev_aum is not None:
                    try:
                        aum_change = float(aum) - float(prev_aum)
                    except Exception:
                        aum_change = None

                hs = cur.get("holdings")
                hs_prev = prev.get("holdings") if isinstance(prev, dict) and prev.get("ok") else None
                cur_map: Dict[str, float] = {}
                prev_map: Dict[str, float] = {}
                if isinstance(hs, list):
                    for r in hs:
                        if isinstance(r, dict) and str(r.get("cusip") or "").strip():
                            cur_map[str(r.get("cusip") or "").strip()] = float(r.get("value_usd") or 0.0)
                if isinstance(hs_prev, list):
                    for r in hs_prev:
                        if isinstance(r, dict) and str(r.get("cusip") or "").strip():
                            prev_map[str(r.get("cusip") or "").strip()] = float(r.get("value_usd") or 0.0)
                for cusip2 in set(cur_map.keys()) | set(prev_map.keys()):
                    cur_val = float(cur_map.get(cusip2) or 0.0)
                    prev_val = float(prev_map.get(cusip2) or 0.0)
                    d = cur_val - prev_val
                    if d > 0:
                        ch_up += 1
                    elif d < 0:
                        ch_dn += 1
        except Exception as e:
            err = _sec_err_str(e)
            cur_q = ""
            prev_q = ""

        items2.append({
            "id": iid,
            "name": name,
            "cn_name": cn_name,
            "category": category,
            "sub_tags": sub_tags,
            "importance_score": importance_score,
            "smart_money_weight": smart_money_weight,
            "cik": cik or None,
            "aum_usd": float(aum) if aum is not None else None,
            "prev_aum_usd": float(prev_aum) if prev_aum is not None else None,
            "aum_change_usd": float(aum_change) if aum_change is not None else None,
            "cur_quarter": cur_q,
            "prev_quarter": prev_q,
            "cur_accession": cur_acc,
            "prev_accession": prev_acc,
            "cur_form": cur_form,
            "prev_form": prev_form,
            "cur_report_date": cur_report,
            "prev_report_date": prev_report,
            "cur_filing_date": cur_filing,
            "prev_filing_date": prev_filing,
            "changes": {"inc": int(ch_up), "dec": int(ch_dn)},
            "error": err,
        })
        try:
            if SEC_EDGAR_PER_INST_DELAY_SEC > 0:
                time.sleep(max(0.0, float(SEC_EDGAR_PER_INST_DELAY_SEC)))
        except Exception:
            pass
    items2.sort(key=lambda x: float(x.get("aum_usd") or 0), reverse=True)
    return items2


def _sec_headers() -> Dict[str, str]:
    return {
        "User-Agent": SEC_EDGAR_USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
        "Accept": "application/json, text/plain, */*",
    }


def _sec_get_json(url: str, cache_key: str, ttl: int) -> Any:
    cached = _cache_get(cache_key, ttl)
    if cached is not None:
        return cached
    r = HTTP.get(url, headers=_sec_headers(), timeout=(10, 30))
    r.raise_for_status()
    data = r.json()
    _cache_set(cache_key, data)
    return data


def _sec_get_text(url: str, cache_key: str, ttl: int) -> str:
    cached = _cache_get(cache_key, ttl)
    if cached is not None:
        return str(cached)
    r = HTTP.get(url, headers=_sec_headers(), timeout=(10, 30))
    r.raise_for_status()
    txt = r.text
    _cache_set(cache_key, txt)
    return txt


def _sec_http_status(e: Exception) -> Optional[int]:
    try:
        if isinstance(e, requests.HTTPError):
            resp = e.response
            if resp is not None:
                sc = getattr(resp, "status_code", None)
                return int(sc) if sc is not None else None
    except Exception:
        return None
    return None


def _sec_err_str(e: Exception) -> str:
    try:
        if isinstance(e, requests.HTTPError):
            resp = e.response
            if resp is not None:
                code = getattr(resp, "status_code", "")
                url = getattr(resp, "url", "")
                body = ""
                try:
                    body = (resp.text or "")[:300]
                except Exception:
                    body = ""
                body = re.sub(r"\s+", " ", body).strip()
                return f"http_{code} {url} {body}".strip()
        return f"{type(e).__name__}: {str(e)[:300]}".strip()
    except Exception:
        return "sec_exception"


def _sec_norm_cik(cik: str) -> str:
    s = re.sub(r"\D+", "", str(cik or "").strip())
    return s.zfill(10) if s else ""


def _sec_cik_no_leading(cik10: str) -> str:
    s = _sec_norm_cik(cik10)
    return str(int(s)) if s else ""


def _sec_quarter_label(date_str: str) -> str:
    s = str(date_str or "").strip()
    if not s:
        return ""
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
    if not m:
        return ""
    try:
        y = int(m.group(1))
        mon = int(m.group(2))
        q = ((mon - 1) // 3) + 1
        if q < 1 or q > 4:
            return ""
        return f"{y}Q{q}"
    except Exception:
        return ""


def _sec_find_recent_13f(submissions: Dict[str, Any], limit: int = 5) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    filings = (submissions or {}).get("filings") or {}
    recent = (filings.get("recent") if isinstance(filings, dict) else None) or {}
    forms = recent.get("form") or []
    accs = recent.get("accessionNumber") or []
    prim = recent.get("primaryDocument") or []
    rep_dates = recent.get("reportDate") or []
    filing_dates = recent.get("filingDate") or []
    if not isinstance(forms, list) or not isinstance(accs, list) or not isinstance(prim, list):
        return out
    n = min(len(forms), len(accs), len(prim))
    for i in range(n):
        f = str(forms[i] or "")
        if f not in ("13F-HR", "13F-HR/A"):
            continue
        out.append({
            "form": f,
            "accession": str(accs[i] or ""),
            "primary_doc": str(prim[i] or ""),
            "report_date": str(rep_dates[i] or "") if isinstance(rep_dates, list) and i < len(rep_dates) else "",
            "filing_date": str(filing_dates[i] or "") if isinstance(filing_dates, list) and i < len(filing_dates) else "",
        })
        if len(out) >= limit:
            break
    return out


def _sec_pick_infotable_file(index_json: Dict[str, Any]) -> Optional[str]:
    item = (index_json or {}).get("directory")
    if not isinstance(item, dict):
        return None
    files = item.get("item")
    if not isinstance(files, list):
        return None
    candidates: List[str] = []
    for it in files:
        if not isinstance(it, dict):
            continue
        name = str(it.get("name") or "")
        low = name.lower()
        if not name:
            continue
        # 跳过 13F 主表单/展示 XML（通常不包含 <infoTable>）
        # 例如：xslForm13F_X02/primary_doc.xml 或 primary_doc.xml
        if low.endswith("primary_doc.xml") or "primary_doc" in low:
            continue
        if low.startswith("xslform13f") or "/xslform13f" in low or "\\xslform13f" in low:
            continue
        if low.endswith(".xml") and ("infotable" in low or "informationtable" in low or "form13f" in low):
            candidates.append(name)
    if candidates:
        candidates.sort(key=lambda x: (0 if "infotable" in x.lower() else 1, len(x)))
        return candidates[0]
    for it in files:
        if not isinstance(it, dict):
            continue
        name = str(it.get("name") or "")
        low = name.lower()
        if not name:
            continue
        if low.endswith("primary_doc.xml") or "primary_doc" in low:
            continue
        if low.startswith("xslform13f") or "/xslform13f" in low or "\\xslform13f" in low:
            continue
        if low.endswith(".xml"):
            return name
    return None


def _sec_parse_13f_infotable_xml(xml_text: str) -> List[Dict[str, Any]]:
    txt = xml_text or ""
    if not txt.strip():
        return []
    try:
        root = ET.fromstring(txt)
    except Exception:
        return []

    def _strip_ns(tag: str) -> str:
        if not isinstance(tag, str):
            return ""
        return tag.split("}")[-1] if "}" in tag else tag

    out: List[Dict[str, Any]] = []
    for node in root.iter():
        if _strip_ns(node.tag) != "infoTable":
            continue

        def _get_text(child_name: str) -> str:
            for c in list(node):
                if _strip_ns(c.tag) == child_name:
                    return (c.text or "").strip()
            return ""

        issuer = _get_text("nameOfIssuer")
        title = _get_text("titleOfClass")
        cusip = re.sub(r"\s+", "", _get_text("cusip"))
        value_k = _get_text("value")

        sh_amt = ""
        sh_type = ""
        voting_sole = ""
        voting_shared = ""
        voting_none = ""
        for c in list(node):
            tn = _strip_ns(c.tag)
            if tn == "shrsOrPrnAmt":
                for cc in list(c):
                    nn = _strip_ns(cc.tag)
                    if nn == "sshPrnamt":
                        sh_amt = (cc.text or "").strip()
                    elif nn == "sshPrnamtType":
                        sh_type = (cc.text or "").strip()
            if tn == "votingAuthority":
                for cc in list(c):
                    nn = _strip_ns(cc.tag)
                    if nn == "Sole":
                        voting_sole = (cc.text or "").strip()
                    elif nn == "Shared":
                        voting_shared = (cc.text or "").strip()
                    elif nn == "None":
                        voting_none = (cc.text or "").strip()

        try:
            value_usd = float(re.sub(r"[^0-9\.]", "", value_k or "0") or 0.0) * 1000.0
        except Exception:
            value_usd = 0.0
        try:
            shares = float(re.sub(r"[^0-9\.]", "", sh_amt or "0") or 0.0)
        except Exception:
            shares = 0.0

        out.append({
            "cusip": cusip,
            "issuer": issuer,
            "title": title,
            "value_usd": value_usd,
            "shares": shares,
            "shares_type": sh_type,
            "voting": {"sole": voting_sole, "shared": voting_shared, "none": voting_none},
        })
    return out


def _sec_get_13f_holdings_by_cik(cik10: str, filing_index: int = 0) -> Dict[str, Any]:
    cik10n = _sec_norm_cik(cik10)
    if not cik10n:
        return {"ok": False, "error": "invalid_cik"}

    # 结果级缓存：避免在 stock/flows 中反复解析同一份 13F（submissions/index/xml 之外再缓存最终聚合结果）
    try:
        idxi = int(filing_index or 0)
    except Exception:
        idxi = 0
    ck = f"sec:13f:holdings:{cik10n}:{idxi}"
    cached = _cache_get(ck, SEC_EDGAR_CACHE_TTL_SEC)
    if isinstance(cached, dict) and cached.get("ok"):
        return cached
    try:
        sub_url = f"https://data.sec.gov/submissions/CIK{cik10n}.json"
        sub_key = f"sec:submissions:{cik10n}"
        try:
            subs = _sec_get_json(sub_url, sub_key, SEC_EDGAR_CACHE_TTL_SEC)
        except Exception as e:
            # 部分情况下 data.sec.gov 会返回 NoSuchKey/404，回退到 www.sec.gov
            if _sec_http_status(e) == 404:
                sub_url2 = f"https://www.sec.gov/submissions/CIK{cik10n}.json"
                subs = _sec_get_json(sub_url2, sub_key + ":www", SEC_EDGAR_CACHE_TTL_SEC)
            else:
                raise
        rec = _sec_find_recent_13f(subs, limit=5)
        if not rec:
            return {"ok": False, "error": "no_13f_found"}
        idx = max(0, min(int(filing_index or 0), len(rec) - 1))
        sel = rec[idx]
        accession = str(sel.get("accession") or "")
        if not accession:
            return {"ok": False, "error": "no_accession"}

        try:
            acc_prefix = accession.split("-")[0] if "-" in accession else accession
            acc_cik = _sec_norm_cik(acc_prefix)
        except Exception:
            acc_cik = ""
        # 注意：accession 前缀的 10 位数字在很多情况下是“提交代理/filing agent”的 CIK，
        # EDGAR Archives 路径通常仍应使用真正的 filer CIK（也就是请求参数 cik）。
        # 因此这里优先使用 cik10n，若对应路径 404 再回退尝试 acc_cik。
        file_cik10n = cik10n
        report_date = str(sel.get("report_date") or "")
        filing_date = str(sel.get("filing_date") or "")
        period_q = _sec_quarter_label(report_date) or _sec_quarter_label(filing_date)
        acc_nodash = accession.replace("-", "")
        def _load_index_for(cik10x: str) -> Dict[str, Any]:
            cnl = _sec_cik_no_leading(cik10x)
            url = f"https://data.sec.gov/Archives/edgar/data/{cnl}/{acc_nodash}/index.json"
            key = f"sec:index:{cnl}:{acc_nodash}"
            try:
                return _sec_get_json(url, key, SEC_EDGAR_CACHE_TTL_SEC)
            except Exception as e:
                if _sec_http_status(e) == 404:
                    url2 = f"https://www.sec.gov/Archives/edgar/data/{cnl}/{acc_nodash}/index.json"
                    return _sec_get_json(url2, key + ":www", SEC_EDGAR_CACHE_TTL_SEC)
                raise

        try:
            index_json = _load_index_for(file_cik10n)
        except Exception as e:
            # 如果用请求 cik 拼 archive 路径 404，则回退尝试 accession 前缀 CIK
            if _sec_http_status(e) == 404 and acc_cik and acc_cik != cik10n:
                file_cik10n = acc_cik
                index_json = _load_index_for(file_cik10n)
            else:
                raise
        info_file = _sec_pick_infotable_file(index_json)
        if not info_file:
            return {"ok": False, "error": "infotable_not_found", "accession": accession}
        cik_nolead = _sec_cik_no_leading(file_cik10n)
        xml_url = f"https://data.sec.gov/Archives/edgar/data/{cik_nolead}/{acc_nodash}/{info_file}"
        xml_key = f"sec:infotable:{cik_nolead}:{acc_nodash}:{info_file}"
        try:
            xml_txt = _sec_get_text(xml_url, xml_key, SEC_EDGAR_CACHE_TTL_SEC)
        except Exception as e:
            if _sec_http_status(e) == 404:
                xml_url2 = f"https://www.sec.gov/Archives/edgar/data/{cik_nolead}/{acc_nodash}/{info_file}"
                xml_txt = _sec_get_text(xml_url2, xml_key + ":www", SEC_EDGAR_CACHE_TTL_SEC)
            else:
                raise
        rows0 = _sec_parse_13f_infotable_xml(xml_txt)
        rows0 = [r for r in rows0 if isinstance(r, dict) and str(r.get("cusip") or "").strip()]

        # 如果信息表解析失败/选错文件，_sec_parse_13f_infotable_xml 会返回空数组。
        # 这种情况下把 total_value_usd 视为 0 会误导前端显示为“正常但为 0”。
        if not rows0:
            sample = ""
            try:
                sample = re.sub(r"\s+", " ", str(xml_txt or "")[:200]).strip()
            except Exception:
                sample = ""
            return {
                "ok": False,
                "error": "infotable_parse_failed",
                "cik": cik10n,
                "accession": accession,
                "info_file": info_file,
                "detail": sample,
            }

        # 13F informationTable 可能对同一 CUSIP 拆成多行（不同 class/type/voting 等）。这里按 CUSIP 聚合，避免前端重复。
        agg: Dict[str, Dict[str, Any]] = {}
        for r in rows0:
            cusip = str(r.get("cusip") or "").strip()
            if not cusip:
                continue
            issuer = str(r.get("issuer") or "").strip()
            title = str(r.get("title") or "").strip()
            sh_type = str(r.get("shares_type") or "").strip()
            value_usd = float(r.get("value_usd") or 0.0)
            shares = float(r.get("shares") or 0.0)
            voting = r.get("voting") if isinstance(r.get("voting"), dict) else {}
            v_sole = float(voting.get("sole") or 0.0) if isinstance(voting, dict) else 0.0
            v_shared = float(voting.get("shared") or 0.0) if isinstance(voting, dict) else 0.0
            v_none = float(voting.get("none") or 0.0) if isinstance(voting, dict) else 0.0

            a = agg.get(cusip)
            if not a:
                agg[cusip] = {
                    "cusip": cusip,
                    "issuer": issuer,
                    "title": title,
                    "shares_type": sh_type,
                    "value_usd": float(value_usd),
                    "shares": float(shares),
                    "voting": {"sole": float(v_sole), "shared": float(v_shared), "none": float(v_none)},
                }
                continue

            # 保留最先出现的 issuer/title/type（如果为空则补齐）
            if (not str(a.get("issuer") or "").strip()) and issuer:
                a["issuer"] = issuer
            if (not str(a.get("title") or "").strip()) and title:
                a["title"] = title
            if (not str(a.get("shares_type") or "").strip()) and sh_type:
                a["shares_type"] = sh_type

            a["value_usd"] = float(a.get("value_usd") or 0.0) + float(value_usd)
            a["shares"] = float(a.get("shares") or 0.0) + float(shares)
            av = a.get("voting") if isinstance(a.get("voting"), dict) else {"sole": 0.0, "shared": 0.0, "none": 0.0}
            a["voting"] = {
                "sole": float(av.get("sole") or 0.0) + float(v_sole),
                "shared": float(av.get("shared") or 0.0) + float(v_shared),
                "none": float(av.get("none") or 0.0) + float(v_none),
            }

        rows = list(agg.values())
        total = sum(float(r.get("value_usd") or 0.0) for r in rows)
        for r in rows:
            try:
                r["weight"] = (float(r.get("value_usd") or 0.0) / total) if total > 0 else 0.0
            except Exception:
                r["weight"] = 0.0
        rows.sort(key=lambda x: float(x.get("value_usd") or 0.0), reverse=True)
        resp_obj = {
            "ok": True,
            "cik": cik10n,
            "accession_cik": acc_cik,
            "file_cik": file_cik10n,
            "accession": accession,
            "filing": {"form": sel.get("form"), "primary_doc": sel.get("primary_doc")},
            "report_date": report_date,
            "filing_date": filing_date,
            "period_quarter": period_q,
            "total_value_usd": float(total),
            "holdings": rows,
        }
        _cache_set(ck, resp_obj)
        return resp_obj
    except Exception as e:
        err = _sec_err_str(e)
        # submissions 级别 404：通常代表 CIK 不存在/填错，给更清晰的错误码
        if "submissions/CIK" in err and "http_404" in err:
            return {"ok": False, "error": "submissions_not_found", "cik": cik10n, "detail": err}
        return {"ok": False, "error": err}


def api_sec_recent_13f(cik: str = "", limit: int = 25, nocache: int = 0) -> JSONResponse:
    cik10n = _sec_norm_cik(cik)
    if not cik10n:
        return JSONResponse({"ok": False, "error": "invalid_cik"}, status_code=422)
    try:
        lim = int(limit or 25)
    except Exception:
        lim = 25
    lim = max(1, min(lim, 200))
    try:
        sub_url = f"https://data.sec.gov/submissions/CIK{cik10n}.json"
        if int(nocache or 0) == 1:
            r = HTTP.get(sub_url, headers=_sec_headers(), timeout=(10, 30))
            r.raise_for_status()
            subs = r.json()
        else:
            subs = _sec_get_json(sub_url, f"sec:submissions:{cik10n}", SEC_EDGAR_CACHE_TTL_SEC)
        filings = (subs or {}).get("filings") or {}
        recent = (filings.get("recent") if isinstance(filings, dict) else None) or {}
        forms = recent.get("form") or []
        accs = recent.get("accessionNumber") or []
        prim = recent.get("primaryDocument") or []
        rep_dates = recent.get("reportDate") or []
        filing_dates = recent.get("filingDate") or []
        n = min(len(forms) if isinstance(forms, list) else 0, len(accs) if isinstance(accs, list) else 0)
        items: List[Dict[str, Any]] = []
        for i in range(n):
            f = str(forms[i] or "")
            if not f:
                continue
            if not (f.startswith("13F-")):
                continue
            items.append({
                "form": f,
                "accession": str(accs[i] or ""),
                "primary_doc": str(prim[i] or "") if isinstance(prim, list) and i < len(prim) else "",
                "report_date": str(rep_dates[i] or "") if isinstance(rep_dates, list) and i < len(rep_dates) else "",
                "filing_date": str(filing_dates[i] or "") if isinstance(filing_dates, list) and i < len(filing_dates) else "",
            })
            if len(items) >= lim:
                break
        return JSONResponse({"ok": True, "cik": cik10n, "items": items, "nocache": int(nocache or 0)})
    except Exception as e:
        return JSONResponse({"ok": False, "error": _sec_err_str(e)}, status_code=502)


def _smartmoney_all_holdings_flat() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for inst in _smartmoney_get_institutions_meta():
        iid = str(inst.get("id") or "")
        iname = str(inst.get("name") or "")
        cik = str(inst.get("cik") or "")
        try:
            cur = _sec_get_13f_holdings_by_cik(cik, filing_index=0)
            hs = cur.get("holdings") if isinstance(cur, dict) else None
        except Exception:
            hs = None
        if not isinstance(hs, list):
            continue
        for h in hs:
            if not isinstance(h, dict):
                continue
            r = dict(h)
            r["inst_id"] = iid
            r["inst_name"] = iname
            out.append(r)
    return out


def api_smartmoney_institutions(q: str = "") -> JSONResponse:
    qq = (q or "").strip().lower()

    # 优先读取 SQLite 持久化快照，其次 Upstash，最后内存快照
    db_data = _db_get_smartmoney_institutions()
    if isinstance(db_data, dict) and isinstance(db_data.get("items"), list) and db_data["items"]:
        # 写入内存缓存
        try:
            with _SMARTMONEY_SNAPSHOT_LOCK:
                _SMARTMONEY_SNAPSHOT["ts"] = float(db_data.get("ts") or 0.0)
                _SMARTMONEY_SNAPSHOT["items"] = list(db_data.get("items") or [])
                _SMARTMONEY_SNAPSHOT["last_error"] = ""
        except Exception:
            pass
        items = list(db_data["items"])
        snapshot_ts = float(db_data.get("ts") or 0.0)
        data_source = "db"
    else:
        # SQLite 无数据，尝试 Upstash
        snap_from_upstash: Optional[Dict[str, Any]] = None
        if UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN:
            snap_from_upstash = _upstash_get_snapshot()
        
        snap_from_upstash_used = False
        if isinstance(snap_from_upstash, dict) and isinstance(snap_from_upstash.get("items"), list):
            snap_from_upstash_used = True
            try:
                with _SMARTMONEY_SNAPSHOT_LOCK:
                    _SMARTMONEY_SNAPSHOT["ts"] = float(snap_from_upstash.get("ts") or 0.0)
                    _SMARTMONEY_SNAPSHOT["items"] = list(snap_from_upstash.get("items") or [])
                    _SMARTMONEY_SNAPSHOT["last_error"] = str(snap_from_upstash.get("last_error") or "")
            except Exception:
                pass

        with _SMARTMONEY_SNAPSHOT_LOCK:
            snap_items = _SMARTMONEY_SNAPSHOT.get("items")
            snap_ts = float(_SMARTMONEY_SNAPSHOT.get("ts") or 0.0)

        data_source = "live"
        snapshot_ts: Optional[float] = None
        stale_limit_sec = float(int(SEC_EDGAR_CACHE_TTL_SEC) * 6) if SEC_EDGAR_CACHE_TTL_SEC else 0.0
        
        if snap_from_upstash_used and isinstance(snap_items, list) and snap_items:
            items = list(snap_items)
            snapshot_ts = snap_ts if snap_ts else None
            data_source = "upstash"
        elif isinstance(snap_items, list) and snap_items and snap_ts > 0 and (not stale_limit_sec or (time.time() - snap_ts) < stale_limit_sec):
            items = list(snap_items)
            snapshot_ts = snap_ts if snap_ts else None
            data_source = "memory"
        else:
            items = _smartmoney_build_items()
            data_source = "live"
            snapshot_ts = None
          

    if qq:
        inst_map = _smartmoney_inst_map()
        items = [
            it
            for it in items
            if qq in str(it.get("name") or "").lower()
            or qq in str(it.get("id") or "").lower()
            or qq in str(inst_map.get(str(it.get("id") or "").lower(), {}).get("cik") or "").lower()
            or qq in str(inst_map.get(str(it.get("id") or "").lower(), {}).get("cn_name") or "").lower()
            or qq in str(inst_map.get(str(it.get("id") or "").lower(), {}).get("category") or "").lower()
        ]
    out = {"ok": True, "items": items, "data_source": data_source, "snapshot_ts": snapshot_ts}
    return JSONResponse(out, headers={"X-SM-Source": data_source, "X-SM-Snapshot-Ts": str(snapshot_ts or "")})


def api_smartmoney_institutions_meta() -> JSONResponse:
    meta_from_upstash: Optional[Any] = None
    meta_ts: Optional[float] = None
    data_source = "builtin"
    if UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN:
        meta_from_upstash = _upstash_get_json(SMARTMONEY_META_KEY)
    if isinstance(meta_from_upstash, dict) and isinstance(meta_from_upstash.get("items"), list):
        data_source = "upstash"
        try:
            meta_ts = float(meta_from_upstash.get("ts") or 0.0) or None
        except Exception:
            meta_ts = None
        return JSONResponse(
            {"ok": True, "items": list(meta_from_upstash.get("items") or []), "data_source": data_source, "ts": meta_ts},
            headers={"X-SM-Source": data_source, "X-SM-Snapshot-Ts": str(meta_ts or "")},
        )

    with _SMARTMONEY_META_LOCK:
        items0 = _SMARTMONEY_META.get("items")
        ts0 = _SMARTMONEY_META.get("ts")
    if isinstance(items0, list) and items0:
        data_source = "memory"
        try:
            meta_ts = float(ts0 or 0.0) or None
        except Exception:
            meta_ts = None
        return JSONResponse(
            {"ok": True, "items": list(items0), "data_source": data_source, "ts": meta_ts},
            headers={"X-SM-Source": data_source, "X-SM-Snapshot-Ts": str(meta_ts or "")},
        )

    return JSONResponse(
        {"ok": True, "items": [x for x in _SMARTMONEY_INSTITUTIONS_META.values() if isinstance(x, dict)], "data_source": data_source, "ts": None},
        headers={"X-SM-Source": data_source, "X-SM-Snapshot-Ts": ""},
    )


async def api_smartmoney_institutions_meta_import(request: Request) -> JSONResponse:
    if not SMARTMONEY_REFRESH_TOKEN:
        return JSONResponse({"ok": False, "error": "refresh_token_not_configured"}, status_code=500)
    xrt = request.headers.get("x-refresh-token")
    auth = request.headers.get("authorization")
    tok = ((xrt or "") or (auth or "")).strip()
    tok = tok.replace("Bearer ", "") if tok.lower().startswith("bearer ") else tok
    if tok != SMARTMONEY_REFRESH_TOKEN:
        return JSONResponse(
            {
                "ok": False,
                "error": "unauthorized",
                "debug": {
                    "has_x_refresh_token": bool((xrt or "").strip()),
                    "has_authorization": bool((auth or "").strip()),
                    "tok_len": len(tok or ""),
                },
            },
            status_code=401,
        )
    if not (UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN):
        return JSONResponse({"ok": False, "error": "upstash_not_configured"}, status_code=500)

    body: Any = None
    try:
        body = await request.json()
    except Exception:
        body = None
    if body is None:
        try:
            raw = await request.body()
            if isinstance(raw, (bytes, bytearray)) and raw:
                try:
                    s = raw.decode("utf-8-sig")
                except Exception:
                    s = raw.decode("utf-8", errors="ignore")
                s = (s or "").strip()
                if s:
                    body = json.loads(s)
        except Exception:
            body = None

    items_in: Any = None
    if isinstance(body, list):
        items_in = body
    elif isinstance(body, dict):
        items_in = body.get("items")
    if not isinstance(items_in, list):
        return JSONResponse({"ok": False, "error": "invalid_payload"}, status_code=400)

    cleaned: List[Dict[str, Any]] = []
    used: Dict[str, int] = {}
    missing_cik = 0
    for x in items_in:
        if not isinstance(x, dict):
            continue
        nm = str(x.get("name") or "").strip()
        iid = str(x.get("id") or "").strip().lower()
        if not iid:
            iid = _sm_norm_inst_id(nm)
        if not iid:
            continue
        cik = str(x.get("cik") or "").strip()
        if not cik:
            missing_cik += 1
        k = iid
        if k in used:
            used[k] += 1
            k = f"{iid}-{used[iid]}"
        else:
            used[k] = 0
        o = dict(x)
        o["id"] = k
        o["name"] = nm
        o["cik"] = cik or None
        cleaned.append(o)

    if not cleaned:
        return JSONResponse({"ok": False, "error": "empty_items"}, status_code=400)

    ok = _smartmoney_save_institutions_meta(cleaned)
    if not ok:
        return JSONResponse({"ok": False, "error": "save_failed"}, status_code=502)
    return JSONResponse({"ok": True, "count": len(cleaned), "missing_cik": missing_cik, "ts": int(time.time())})


def api_smartmoney_refresh(request: Request) -> JSONResponse:
    if not SMARTMONEY_REFRESH_TOKEN:
        return JSONResponse({"ok": False, "error": "refresh_token_not_configured"}, status_code=500)
    tok = (request.headers.get("x-refresh-token") or request.headers.get("authorization") or "").strip()
    tok = tok.replace("Bearer ", "") if tok.lower().startswith("bearer ") else tok
    if tok != SMARTMONEY_REFRESH_TOKEN:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)

    # 一次性刷新所有数据（batch_size=60覆盖全部机构）
    qp = getattr(request, "query_params", None)
    stage = ""
    cursor_raw = ""
    batch_size = 60
    target = "both"  # sqlite, upstash, or both
    try:
        stage = str((qp.get("stage") if qp is not None else "") or "").strip().lower()
        cursor_raw = str((qp.get("cursor") if qp is not None else "") or "").strip()
        batch_size = int((qp.get("batch_size") if qp is not None else 60) or 60)
        target = str((qp.get("target") if qp is not None else "both") or "both").strip().lower()
    except Exception:
        stage = stage or ""
        cursor_raw = cursor_raw or ""
        batch_size = 60
        target = "both"
    if stage not in ("", "inst", "flows"):
        return JSONResponse({"ok": False, "error": "invalid_stage"}, status_code=400)
    if target not in ("sqlite", "upstash", "both"):
        return JSONResponse({"ok": False, "error": "invalid_target"}, status_code=400)
    if batch_size <= 0:
        batch_size = 60
    if batch_size > 100:
        batch_size = 100
    try:
        cursor = int(cursor_raw or "0")
    except Exception:
        cursor = 0
    if cursor < 0:
        cursor = 0

    started = time.time()
    last_err = ""

    if not (UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN):
        return JSONResponse({"ok": False, "error": "upstash_not_configured"}, status_code=500)

    # TTL：与 institutions 列表快照一致
    ttl_sec = max(0, int(SEC_EDGAR_CACHE_TTL_SEC) * 6) if SEC_EDGAR_CACHE_TTL_SEC else 0

    flows_zbuy_key = "sm:flows:z:buys:all:quarter"
    flows_zsell_key = "sm:flows:z:sells:all:quarter"
    flows_issuer_key = "sm:flows:h:issuer:all:quarter"

    def _build_inst_detail(iid: str, inst: Dict[str, Any], cur: Dict[str, Any], prev: Dict[str, Any]) -> Dict[str, Any]:
        import heapq

        hs = cur.get("holdings") if isinstance(cur.get("holdings"), list) else []
        hs_prev = prev.get("holdings") if isinstance(prev, dict) and prev.get("ok") and isinstance(prev.get("holdings"), list) else []

        # prev cusip -> row (value_usd + issuer/sector)
        prev_map: Dict[str, Dict[str, Any]] = {}
        for r in hs_prev:
            if not isinstance(r, dict):
                continue
            cusip = str(r.get("cusip") or "").strip().upper()
            if not cusip:
                continue
            prev_map[cusip] = r

        cur_seen: set = set()

        # top holdings for output, keep only top N by value_usd
        topN = 200
        heap_hold: List[Tuple[float, Dict[str, Any]]] = []

        # top15 for summary
        heap_top15: List[Tuple[float, Dict[str, Any]]] = []

        # change category counters + top10 heaps by abs(delta)
        cnt_new = 0
        cnt_add = 0
        cnt_reduce = 0
        cnt_exit = 0

        heap_counter = [0]
        def _push_heap(hp: List[Tuple[float, int, Dict[str, Any]]], limit: int, score: float, row: Dict[str, Any]) -> None:
            if limit <= 0:
                return
            heap_counter[0] += 1
            if len(hp) < limit:
                heapq.heappush(hp, (score, heap_counter[0], row))
            else:
                if score > hp[0][0]:
                    heapq.heapreplace(hp, (score, heap_counter[0], row))

        heap_new: List[Tuple[float, int, Dict[str, Any]]] = []
        heap_add: List[Tuple[float, int, Dict[str, Any]]] = []
        heap_reduce: List[Tuple[float, int, Dict[str, Any]]] = []
        heap_exit: List[Tuple[float, int, Dict[str, Any]]] = []

        for h in hs:
            if not isinstance(h, dict):
                continue
            cusip = str(h.get("cusip") or "").strip().upper()
            if not cusip:
                continue
            cur_seen.add(cusip)

            prev_row = prev_map.get(cusip) or {}
            prev_val = float(prev_row.get("value_usd") or 0.0)
            cur_val = float(h.get("value_usd") or 0.0)
            delta = float(cur_val - prev_val)
            weight = float(h.get("weight") or 0.0)

            row = {
                "cusip": cusip,
                "issuer": h.get("issuer") or "",
                "sector": h.get("sector") or "",
                "value_usd": cur_val,
                "weight": weight,
                "qoq_value_change": delta,
            }

            # holdings output: keep by value_usd
            _push_heap(heap_hold, topN, cur_val, row)
            _push_heap(heap_top15, 15, cur_val, row)

            # changes
            if delta > 0:
                if prev_val <= 0:
                    cnt_new += 1
                    _push_heap(heap_new, 10, abs(delta), row)
                else:
                    cnt_add += 1
                    _push_heap(heap_add, 10, abs(delta), row)
            elif delta < 0:
                if cur_val <= 0:
                    # normally won't happen here (SEC cur has row only if still held)
                    pass
                else:
                    cnt_reduce += 1
                    _push_heap(heap_reduce, 10, abs(delta), row)

        # exited: in prev but not in cur
        for cusip, r in prev_map.items():
            if cusip in cur_seen:
                continue
            prev_val = float(r.get("value_usd") or 0.0)
            if prev_val <= 0:
                continue
            row = {
                "cusip": cusip,
                "issuer": r.get("issuer") or "",
                "sector": r.get("sector") or "",
                "value_usd": 0.0,
                "weight": 0.0,
                "qoq_value_change": -float(prev_val),
            }
            cnt_exit += 1
            _push_heap(heap_exit, 10, abs(prev_val), row)

        def _heap_to_top_rows(hp: List[Tuple[float, int, Dict[str, Any]]]) -> List[Dict[str, Any]]:
            rows = [x[2] for x in sorted(hp, key=lambda t: float(t[0] or 0.0), reverse=True)]
            out: List[Dict[str, Any]] = []
            for r in rows:
                out.append(
                    {
                        "issuer": r.get("issuer") or "",
                        "cusip": r.get("cusip") or "",
                        "delta_usd": float(r.get("qoq_value_change") or 0.0),
                        "value_usd": float(r.get("value_usd") or 0.0),
                        "weight": float(r.get("weight") or 0.0),
                    }
                )
            return out

        holdings_limited = [x[2] for x in sorted(heap_hold, key=lambda t: float(t[0] or 0.0), reverse=True)]
        top_holdings_rows = [x[2] for x in sorted(heap_top15, key=lambda t: float(t[0] or 0.0), reverse=True)]
        top_holdings = [
            {
                "issuer": h.get("issuer") or "",
                "cusip": h.get("cusip") or "",
                "value_usd": float(h.get("value_usd") or 0.0),
                "weight": float(h.get("weight") or 0.0),
            }
            for h in top_holdings_rows
        ]

        change_breakdown = {
            "counts": {
                "new": int(cnt_new),
                "add": int(cnt_add),
                "reduce": int(cnt_reduce),
                "exit": int(cnt_exit),
            },
            "top10": {
                "new": _heap_to_top_rows(heap_new),
                "add": _heap_to_top_rows(heap_add),
                "reduce": _heap_to_top_rows(heap_reduce),
                "exit": _heap_to_top_rows(heap_exit),
            },
        }

        recent_changes = {
            "new": change_breakdown["top10"]["new"],
            "add": change_breakdown["top10"]["add"],
            "reduce": change_breakdown["top10"]["reduce"],
            "exit": change_breakdown["top10"]["exit"],
        }

        return {
            "ok": True,
            "institution": {
                "id": iid,
                "name": inst.get("name") or "",
                "cik": inst.get("cik") or "",
                "aum_usd": float(cur.get("total_value_usd") or 0.0),
                "accession": cur.get("accession"),
            },
            "holdings": holdings_limited,
            "holdings_total": int(len(hs)) + int(cnt_exit),
            "top_holdings": top_holdings,
            "recent_changes": recent_changes,
            "change_breakdown": change_breakdown,
        }

    # 分批：inst
    if stage in ("", "inst", "flows"):
        insts = _smartmoney_get_institutions_meta()
        total = len(insts)
        if cursor >= total:
            return JSONResponse({
                "ok": True,
                "stage": stage if stage == "flows" else "inst",
                "done": True,
                "next_cursor": cursor,
                "total": total,
                "took_sec": round(time.time() - started, 3),
                "last_error": "",
            })

        # cursor=0 时刷新 institutions 列表快照（轻量）- 仅在 stage="" 或 stage="inst" 时执行
        if cursor == 0 and stage != "flows":
            try:
                items = _smartmoney_build_items()
                with _SMARTMONEY_SNAPSHOT_LOCK:
                    _SMARTMONEY_SNAPSHOT["ts"] = time.time()
                    _SMARTMONEY_SNAPSHOT["items"] = items if isinstance(items, list) else []
                    _SMARTMONEY_SNAPSHOT["last_error"] = ""
                snap_obj = {"ts": float(_SMARTMONEY_SNAPSHOT.get("ts") or 0.0), "items": items if isinstance(items, list) else [], "last_error": ""}
                # 根据 target 参数决定写入位置
                if target in ("upstash", "both"):
                    _upstash_set_snapshot(snap_obj, ttl_sec=ttl_sec)
                if target in ("sqlite", "both"):
                    _db_set_smartmoney_institutions(snap_obj, ttl_sec=ttl_sec)
            except Exception:
                pass

        # 重置 flows 聚合（写入 Redis 端，避免 Python 内存累加）- 仅在 cursor=0 时执行
        if cursor == 0:
            try:
                _upstash_pipeline(
                    [
                        ["DEL", flows_zbuy_key],
                        ["DEL", flows_zsell_key],
                        ["DEL", flows_issuer_key],
                    ]
                )
            except Exception:
                pass

        slice2 = insts[cursor : min(total, cursor + batch_size)]
        now_ts = int(time.time())
        cmds: List[List[Any]] = []
        wrote = 0
        for inst in slice2:
            iid = str(inst.get("id") or "").strip().lower()
            cik = str(inst.get("cik") or "")
            if not iid or not str(cik or "").strip():
                continue
            try:
                cur = _sec_get_13f_holdings_by_cik(cik, filing_index=0)
                prev = _sec_get_13f_holdings_by_cik(cik, filing_index=1)
                if not (isinstance(cur, dict) and cur.get("ok")):
                    continue
                obj = _build_inst_detail(iid, inst, cur, prev)
                o2 = dict(obj)
                o2["ts"] = now_ts
                cmds.append(["SET", _sm_snap_key_inst(iid), json.dumps(o2, ensure_ascii=False), "EX", str(int(ttl_sec))])
                wrote += 1

                # 全持仓精确 flows：直接基于 SEC 返回的 cur/prev 全量 holdings 做聚合
                hs_full = cur.get("holdings") if isinstance(cur.get("holdings"), list) else []
                hs_prev = prev.get("holdings") if isinstance(prev, dict) and prev.get("ok") and isinstance(prev.get("holdings"), list) else []
                prev_val_map: Dict[str, float] = {}
                # Debug: log flows calculation
                if stage == "flows" and cursor == 0:
                    print(f"[DEBUG] Flows calc for {iid}: cur_holdings={len(hs_full)}, prev_holdings={len(hs_prev)}, prev_ok={prev.get('ok') if isinstance(prev, dict) else False}")
                for r in hs_prev:
                    if not isinstance(r, dict):
                        continue
                    pc = str(r.get("cusip") or "").strip().upper()
                    if not pc:
                        continue
                    prev_val_map[pc] = float(r.get("value_usd") or 0.0)

                fcmds: List[List[Any]] = []
                f_batch = 500
                flow_count = 0
                for h in hs_full:
                    if not isinstance(h, dict):
                        continue
                    cusip = str(h.get("cusip") or "").strip().upper()
                    if not cusip:
                        continue
                    issuer = str(h.get("issuer") or "").strip()
                    cur_val = float(h.get("value_usd") or 0.0)
                    prev_val = float(prev_val_map.get(cusip) or 0.0)
                    delta = float(cur_val - prev_val)
                    if delta == 0:
                        continue
                    flow_count += 1
                    if issuer:
                        fcmds.append(["HSET", flows_issuer_key, cusip, issuer])
                    if delta > 0:
                        fcmds.append(["ZINCRBY", flows_zbuy_key, str(float(delta)), cusip])
                    else:
                        fcmds.append(["ZINCRBY", flows_zsell_key, str(float(abs(delta))), cusip])
                    if len(fcmds) >= f_batch:
                        _upstash_pipeline(fcmds)
                        fcmds = []

                if fcmds:
                    _upstash_pipeline(fcmds)
                # Debug: log flow count
                if stage == "flows" and cursor == 0:
                    print(f"[DEBUG] {iid} calculated {flow_count} flows")
            except Exception as e:
                last_err = (last_err + " | " if last_err else "") + _sec_err_str(e)

        try:
            if cmds:
                _upstash_pipeline(cmds)
        except Exception as e:
            last_err = (last_err + " | " if last_err else "") + "upstash_pipeline_failed: " + _sec_err_str(e)

        next_cursor = cursor + len(slice2)
        done = next_cursor >= total
        # 仅在 stage="" 或 stage="inst" 时返回，stage="flows" 时继续执行 flows 分支
        if stage != "flows":
            return JSONResponse({
                "ok": True,
                "stage": "inst",
                "done": bool(done),
                "next_cursor": int(next_cursor),
                "total": int(total),
                "batch_wrote": int(wrote),
                "took_sec": round(time.time() - started, 3),
                "last_error": str(last_err or ""),
            })

    # flows：从 Redis ZSET 读取 Top50 并写入快照（不做 Python 端全量聚合）
    insts = _smartmoney_get_institutions_meta()
    total = len(insts)
    next_cursor = total
    done = True
    flows_written = False

    # target=sqlite时，直接从SEC文件计算flows
    if target == "sqlite" and done:
        try:
            print(f"[DEBUG] Calculating flows from SEC files for SQLite, institutions count: {len(insts)}")

            # 定义heap辅助函数
            def _push_heap(hp: List[Tuple[float, Dict[str, Any]]], limit: int, score: float, row: Dict[str, Any]) -> None:
                if limit <= 0:
                    return
                if len(hp) < limit:
                    heapq.heappush(hp, (score, row))
                else:
                    if score > hp[0][0]:
                        heapq.heapreplace(hp, (score, row))

            # 使用字典按CUSIP聚合delta
            cusip_delta_map: Dict[str, float] = {}
            issuer_map: Dict[str, str] = {}

            for inst in insts:
                iid = str(inst.get("id") or "").strip().lower()
                cik = str(inst.get("cik") or "")
                if not iid or not str(cik or "").strip():
                    continue
                print(f"[DEBUG] Processing institution: {iid}, cik={cik}")
                try:
                    cur = _sec_get_13f_holdings_by_cik(cik, filing_index=0)
                    prev = _sec_get_13f_holdings_by_cik(cik, filing_index=1)
                    print(f"[DEBUG] {iid} cur ok={cur.get('ok') if isinstance(cur, dict) else False}, prev ok={prev.get('ok') if isinstance(prev, dict) else False}")
                    if not (isinstance(cur, dict) and cur.get("ok")):
                        continue

                    hs_full = cur.get("holdings") if isinstance(cur.get("holdings"), list) else []
                    hs_prev = prev.get("holdings") if isinstance(prev, dict) and prev.get("ok") and isinstance(prev.get("holdings"), list) else []
                    print(f"[DEBUG] {iid} cur_holdings={len(hs_full)}, prev_holdings={len(hs_prev)}")
                    prev_val_map: Dict[str, float] = {}
                    for r in hs_prev:
                        if not isinstance(r, dict):
                            continue
                        pc = str(r.get("cusip") or "").strip().upper()
                        if not pc:
                            continue
                        prev_val_map[pc] = float(r.get("value_usd") or 0.0)

                    flow_count = 0
                    for h in hs_full:
                        if not isinstance(h, dict):
                            continue
                        cusip = str(h.get("cusip") or "").strip().upper()
                        if not cusip:
                            continue
                        issuer = str(h.get("issuer") or "").strip()
                        if issuer:
                            issuer_map[cusip] = issuer
                        cur_val = float(h.get("value_usd") or 0.0)
                        prev_val = float(prev_val_map.get(cusip) or 0.0)
                        delta = float(cur_val - prev_val)
                        if delta == 0:
                            continue
                        flow_count += 1
                        # 按CUSIP聚合delta
                        cusip_delta_map[cusip] = cusip_delta_map.get(cusip, 0.0) + delta
                    print(f"[DEBUG] {iid} calculated {flow_count} flows")
                except Exception as e:
                    print(f"[DEBUG] Error calculating flows for {iid}: {e}")
                    continue

            # 使用heap聚合Top50 buys和Top50 sells
            buys_heap: List[Tuple[float, Dict[str, Any]]] = []
            sells_heap: List[Tuple[float, Dict[str, Any]]] = []

            for cusip, total_delta in cusip_delta_map.items():
                if total_delta == 0:
                    continue
                row = {"cusip": cusip, "issuer": issuer_map.get(cusip, ""), "sector": "", "flow_usd": float(total_delta)}
                if total_delta > 0:
                    _push_heap(buys_heap, 50, total_delta, row)
                else:
                    _push_heap(sells_heap, 50, abs(total_delta), row)

            # 从heap提取并排序（降序）
            top_buys_rows = [row for score, row in sorted(buys_heap, key=lambda x: x[0], reverse=True)]
            top_sells_rows = [row for score, row in sorted(sells_heap, key=lambda x: x[0], reverse=True)]

            # 填充issuer
            for row in top_buys_rows:
                cusip = row.get("cusip", "")
                if not row.get("issuer") and cusip in issuer_map:
                    row["issuer"] = issuer_map[cusip]
            for row in top_sells_rows:
                cusip = row.get("cusip", "")
                if not row.get("issuer") and cusip in issuer_map:
                    row["issuer"] = issuer_map[cusip]

            print(f"[DEBUG] Calculated flows: buys={len(top_buys_rows)}, sells={len(top_sells_rows)}")
            flows_snap = {
                "ok": True,
                "sector": "all",
                "period": "quarter",
                "top_buys": top_buys_rows,
                "top_sells": top_sells_rows,
                "ts": int(time.time()),
            }
            set_ok = _db_set_smartmoney_flows(flows_snap, ttl_sec=ttl_sec)
            print(f"[DEBUG] _db_set_smartmoney_flows result: {set_ok}")
            flows_written = True
        except Exception as e:
            last_err = _sec_err_str(e)
    elif done:
        # target=upstash时，从Redis ZSET读取
        try:
            print(f"[DEBUG] Reading flows from ZSETs: {flows_zbuy_key}, {flows_zsell_key}")
            rb = _upstash_pipeline([["ZREVRANGE", flows_zbuy_key, "0", "49", "WITHSCORES"]])
            rs = _upstash_pipeline([["ZREVRANGE", flows_zsell_key, "0", "49", "WITHSCORES"]])
            print(f"[DEBUG] ZREVRANGE buys: {rb}, sells: {rs}")
            braw = None
            sraw = None
            # Upstash returns [{'result': [...]}], extract the inner result
            if isinstance(rb, list) and rb and isinstance(rb[0], dict):
                braw = rb[0].get("result")
            if isinstance(rs, list) and rs and isinstance(rs[0], dict):
                sraw = rs[0].get("result")
            print(f"[DEBUG] Extracted braw: {braw}, sraw: {sraw}")

            def _pairs_to_rows(raw: Any) -> List[Dict[str, Any]]:
                out: List[Dict[str, Any]] = []
                if not isinstance(raw, list):
                    return out
                # 形如 [member, score, member, score, ...]
                cusips: List[str] = []
                for i in range(0, len(raw), 2):
                    if i + 1 >= len(raw):
                        break
                    cusip = str(raw[i] or "").strip().upper()
                    try:
                        score = float(raw[i + 1] or 0.0)
                    except Exception:
                        score = 0.0
                    if not cusip:
                        continue
                    cusips.append(cusip)
                    out.append({"cusip": cusip, "issuer": "", "sector": "", "flow_usd": float(score)})

                if cusips:
                    try:
                        hcmds = [["HGET", flows_issuer_key, c] for c in cusips]
                        hres = _upstash_pipeline(hcmds)
                        if isinstance(hres, list) and len(hres) == len(cusips):
                            for j in range(len(out)):
                                val = hres[j]
                                if isinstance(val, dict) and "result" in val:
                                    val = val["result"]
                                out[j]["issuer"] = str(val or "")
                    except Exception:
                        pass
                return out

            top_buys_rows = _pairs_to_rows(braw)
            top_sells_rows = _pairs_to_rows(sraw)
            print(f"[DEBUG] Flows rows: buys={len(top_buys_rows)}, sells={len(top_sells_rows)}")
            flows_snap = {
                "ok": True,
                "sector": "all",
                "period": "quarter",
                "top_buys": top_buys_rows,
                "top_sells": top_sells_rows,
                "ts": int(time.time()),
            }
            flows_key = _sm_snap_key_flows("all", "quarter")
            print(f"[DEBUG] Writing flows to key: {flows_key}")
            set_ok = _upstash_set_json(flows_key, flows_snap, ttl_sec=ttl_sec)
            print(f"[DEBUG] _upstash_set_json result: {set_ok}, ttl={ttl_sec}")
            flows_written = True
        except Exception as e:
            last_err = _sec_err_str(e)

    return JSONResponse({
        "ok": True,
        "stage": "flows",
        "done": bool(done),
        "next_cursor": int(next_cursor),
        "total": int(total),
        "flows_written": bool(flows_written),
        "took_sec": round(time.time() - started, 3),
        "last_error": str(last_err or ""),
    })


def api_smartmoney_refresh_status() -> JSONResponse:
    with _SMARTMONEY_SNAPSHOT_LOCK:
        snap_ts = float(_SMARTMONEY_SNAPSHOT.get("ts") or 0.0)
        last_error = str(_SMARTMONEY_SNAPSHOT.get("last_error") or "")

    upstash_ts = None
    if UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN:
        try:
            snap = _upstash_get_snapshot()
            if isinstance(snap, dict) and ("ts" in snap):
                upstash_ts = snap.get("ts")
        except Exception:
            upstash_ts = None

    return JSONResponse({
        "ok": True,
        "memory_snapshot_ts": snap_ts or None,
        "last_error": last_error,
        "upstash_configured": bool(UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN),
        "upstash_snapshot_ts": upstash_ts,
    })


def api_smartmoney_refresh_manual() -> JSONResponse:
    """手动刷新接口：同步拉取所有机构 SEC 数据并写入 Upstash（不轮询）"""
    import json as _json
    import heapq as _heapq

    insts = _smartmoney_get_institutions_meta()
    total = len(insts)
    ttl_sec = max(0, int(SEC_EDGAR_CACHE_TTL_SEC) * 6) if SEC_EDGAR_CACHE_TTL_SEC else 0
    now_ts = int(time.time())

    flows_zbuy_key = "sm:flows:z:buys:all:quarter"
    flows_zsell_key = "sm:flows:z:sells:all:quarter"
    flows_issuer_key = "sm:flows:h:issuer:all:quarter"

    all_cmds = []
    fcmds = []
    all_cmds.append(["DEL", flows_zbuy_key])
    all_cmds.append(["DEL", flows_zsell_key])
    all_cmds.append(["DEL", flows_issuer_key])

    # institutions 列表快照
    try:
        items = _smartmoney_build_items()
        with _SMARTMONEY_SNAPSHOT_LOCK:
            _SMARTMONEY_SNAPSHOT["ts"] = time.time()
            _SMARTMONEY_SNAPSHOT["items"] = items if isinstance(items, list) else []
            _SMARTMONEY_SNAPSHOT["last_error"] = ""
        snap_obj = {"ts": _SMARTMONEY_SNAPSHOT["ts"], "items": _SMARTMONEY_SNAPSHOT["items"], "last_error": ""}
        raw = _json.dumps(snap_obj, ensure_ascii=False)
        if ttl_sec > 0:
            all_cmds.append(["SET", SMARTMONEY_SNAPSHOT_KEY, raw, "EX", str(int(ttl_sec))])
        else:
            all_cmds.append(["SET", SMARTMONEY_SNAPSHOT_KEY, raw])
    except Exception:
        pass

    failed: List[str] = []
    for inst in insts:
        iid = str(inst.get("id") or "").strip().lower()
        cik = str(inst.get("cik") or "")
        if not iid or not str(cik or "").strip():
            continue
        try:
            cur = _sec_get_13f_holdings_by_cik(cik, filing_index=0)
            prev = _sec_get_13f_holdings_by_cik(cik, filing_index=1)
            if not (isinstance(cur, dict) and cur.get("ok")):
                failed.append(f"{iid}:sec_fetch_failed")
                continue

            hs = cur.get("holdings") if isinstance(cur.get("holdings"), list) else []
            hs_prev = prev.get("holdings") if isinstance(prev, dict) and prev.get("ok") and isinstance(prev.get("holdings"), list) else []
            prev_map = {}
            for r in hs_prev:
                if not isinstance(r, dict):
                    continue
                cusip = str(r.get("cusip") or "").strip().upper()
                if not cusip:
                    continue
                prev_map[cusip] = r

            cur_seen = set()
            topN = 200
            heap_hold = []
            heap_top15 = []
            cnt_new = 0
            cnt_add = 0
            cnt_reduce = 0
            cnt_exit = 0
            heap_new = []
            heap_add = []
            heap_reduce = []
            heap_exit = []

            heap_counter2 = [0]
            def _push_heap(hp, limit, score, row):
                if limit <= 0:
                    return
                heap_counter2[0] += 1
                if len(hp) < limit:
                    _heapq.heappush(hp, (score, heap_counter2[0], row))
                else:
                    if score > hp[0][0]:
                        _heapq.heapreplace(hp, (score, heap_counter2[0], row))

            for h in hs:
                if not isinstance(h, dict):
                    continue
                cusip = str(h.get("cusip") or "").strip().upper()
                if not cusip:
                    continue
                cur_seen.add(cusip)
                prev_row = prev_map.get(cusip) or {}
                prev_val = float(prev_row.get("value_usd") or 0.0)
                cur_val = float(h.get("value_usd") or 0.0)
                delta = float(cur_val - prev_val)
                weight = float(h.get("weight") or 0.0)
                row = {"cusip": cusip, "issuer": h.get("issuer") or "", "sector": h.get("sector") or "", "value_usd": cur_val, "weight": weight, "qoq_value_change": delta}
                _push_heap(heap_hold, topN, cur_val, row)
                _push_heap(heap_top15, 15, cur_val, row)
                if delta > 0:
                    if prev_val <= 0:
                        cnt_new += 1
                        _push_heap(heap_new, 10, abs(delta), row)
                    else:
                        cnt_add += 1
                        _push_heap(heap_add, 10, abs(delta), row)
                elif delta < 0:
                    if cur_val > 0:
                        cnt_reduce += 1
                        _push_heap(heap_reduce, 10, abs(delta), row)

            for cusip, r in prev_map.items():
                if cusip in cur_seen:
                    continue
                prev_val = float(r.get("value_usd") or 0.0)
                if prev_val <= 0:
                    continue
                cnt_exit += 1
                _push_heap(heap_exit, 10, abs(prev_val), {"cusip": cusip, "issuer": r.get("issuer") or "", "sector": r.get("sector") or "", "value_usd": 0.0, "weight": 0.0, "qoq_value_change": -prev_val})

            def _heap_to_top_rows(hp):
                rows = [x[2] for x in sorted(hp, key=lambda t: float(t[0] or 0.0), reverse=True)]
                out = []
                for r in rows:
                    out.append({"issuer": r.get("issuer") or "", "cusip": r.get("cusip") or "", "value_usd": float(r.get("value_usd") or 0.0), "weight": float(r.get("weight") or 0.0), "qoq_value_change": float(r.get("qoq_value_change") or 0.0)})
                return out

            holdings_limited = [x[2] for x in sorted(heap_hold, key=lambda t: float(t[0] or 0.0), reverse=True)]
            top_holdings_rows = [x[2] for x in sorted(heap_top15, key=lambda t: float(t[0] or 0.0), reverse=True)]
            top_holdings = [{"issuer": h.get("issuer") or "", "cusip": h.get("cusip") or "", "value_usd": float(h.get("value_usd") or 0.0), "weight": float(h.get("weight") or 0.0), "qoq_value_change": float(h.get("qoq_value_change") or 0.0)} for h in top_holdings_rows]

            change_breakdown = {"counts": {"new": int(cnt_new), "add": int(cnt_add), "reduce": int(cnt_reduce), "exit": int(cnt_exit)}, "top10": {"new": _heap_to_top_rows(heap_new), "add": _heap_to_top_rows(heap_add), "reduce": _heap_to_top_rows(heap_reduce), "exit": _heap_to_top_rows(heap_exit)}}
            recent_changes = {"new": change_breakdown["top10"]["new"], "add": change_breakdown["top10"]["add"], "reduce": change_breakdown["top10"]["reduce"], "exit": change_breakdown["top10"]["exit"]}

            detail_obj = {
                "ok": True,
                "institution": {"id": iid, "name": inst.get("name") or "", "cik": inst.get("cik") or "", "aum_usd": float(cur.get("total_value_usd") or 0.0), "accession": cur.get("accession")},
                "holdings": holdings_limited,
                "holdings_total": int(len(hs)) + int(cnt_exit),
                "top_holdings": top_holdings,
                "recent_changes": recent_changes,
                "change_breakdown": change_breakdown,
                "ts": now_ts,
            }
            detail_raw = _json.dumps(detail_obj, ensure_ascii=False)
            all_cmds.append(["SET", _sm_snap_key_inst(iid), detail_raw, "EX", str(int(ttl_sec))])

            hs_full = cur.get("holdings") if isinstance(cur.get("holdings"), list) else []
            hs_prev2 = prev.get("holdings") if isinstance(prev, dict) and prev.get("ok") and isinstance(prev.get("holdings"), list) else []
            prev_val_map2 = {}
            for r in hs_prev2:
                if not isinstance(r, dict):
                    continue
                c = str(r.get("cusip") or "").strip().upper()
                if not c:
                    continue
                prev_val_map2[c] = float(r.get("value_usd") or 0.0)
            seen_cusips = set()
            for h in hs_full:
                if not isinstance(h, dict):
                    continue
                cusip = str(h.get("cusip") or "").strip().upper()
                if not cusip or cusip in seen_cusips:
                    continue
                seen_cusips.add(cusip)
                cur_val2 = float(h.get("value_usd") or 0.0)
                prev_val2 = float(prev_val_map2.get(cusip) or 0.0)
                delta2 = cur_val2 - prev_val2
                # 跟踪个股持仓信息（用于生成 stock 快照）
                wt = float(h.get("weight") or 0.0)
                issuer = str(h.get("issuer") or "").strip()
                if cusip not in cusip_holders:
                    cusip_holders[cusip] = {"issuer": issuer, "holders": []}
                cusip_holders[cusip]["holders"].append({
                    "inst_id": iid,
                    "inst_name": iname,
                    "weight": wt,
                    "value_usd": cur_val2,
                    "qoq_value_change": delta2,
                })
                if delta2 == 0:
                    continue
                issuer = str(h.get("issuer") or "").strip()
                if issuer:
                    fcmds.append(["HSET", flows_issuer_key, cusip, issuer])
                if delta2 > 0:
                    fcmds.append(["ZINCRBY", flows_zbuy_key, str(float(delta2)), cusip])
                else:
                    fcmds.append(["ZINCRBY", flows_zsell_key, str(float(abs(delta2))), cusip])
        except Exception as e:
            failed.append(f"{iid}:{_sec_err_str(e)}")

    # 一次性 pipeline 写入
    if fcmds:
        fcmds.append(["EXPIRE", flows_zbuy_key, str(int(ttl_sec))])
        fcmds.append(["EXPIRE", flows_zsell_key, str(int(ttl_sec))])
        fcmds.append(["EXPIRE", flows_issuer_key, str(int(ttl_sec))])
        all_cmds.extend(fcmds)
    if all_cmds:
        try:
            _upstash_pipeline(all_cmds)
        except Exception:
            pass

    # flows 快照
    try:
        rb = _upstash_pipeline([["ZREVRANGE", flows_zbuy_key, "0", "49", "WITHSCORES"]])
        rs = _upstash_pipeline([["ZREVRANGE", flows_zsell_key, "0", "49", "WITHSCORES"]])
        # Upstash returns [{'result': [...]}], extract the inner result
        braw = rb[0].get("result") if isinstance(rb, list) and rb and isinstance(rb[0], dict) else None
        sraw = rs[0].get("result") if isinstance(rs, list) and rs and isinstance(rs[0], dict) else None

        def _pairs_to_rows(raw):
            out = []
            if not isinstance(raw, list):
                return out
            cusips = []
            for i in range(0, len(raw), 2):
                if i + 1 >= len(raw):
                    break
                cusip = str(raw[i] or "").strip().upper()
                try:
                    score = float(raw[i + 1] or 0.0)
                except Exception:
                    score = 0.0
                if not cusip:
                    continue
                cusips.append(cusip)
                out.append({"cusip": cusip, "issuer": "", "sector": "", "flow_usd": float(score)})
            if cusips:
                try:
                    hcmds = [["HGET", flows_issuer_key, c] for c in cusips]
                    hres = _upstash_pipeline(hcmds)
                    if isinstance(hres, list) and len(hres) == len(cusips):
                        for j in range(len(out)):
                            out[j]["issuer"] = str(hres[j] or "")
                except Exception:
                    pass
            return out

        top_buys_rows = _pairs_to_rows(braw)
        top_sells_rows = _pairs_to_rows(sraw)
        flows_snap = {"ok": True, "sector": "all", "period": "quarter", "top_buys": top_buys_rows, "top_sells": top_sells_rows, "ts": int(time.time())}
        _upstash_set_json(_sm_snap_key_flows("all", "quarter"), flows_snap, ttl_sec=ttl_sec)

    except Exception:
        pass

    return JSONResponse({
        "ok": True,
        "total": int(total),
        "failed": failed,
        "snapshot_ts": now_ts,
    })

def api_smartmoney_refresh_manual_status(task_id: str = Query(...)) -> JSONResponse:
    """查询手动刷新任务状态"""
    with _SMARTMONEY_REFRESH_TASKS_LOCK:
        task = _SMARTMONEY_REFRESH_TASKS.get(task_id)
    
    if not task:
        return JSONResponse({"ok": False, "error": "task_not_found"}, status_code=404)
    
    return JSONResponse({
        "ok": True,
        "status": task.get("status"),
        "stage": task.get("stage"),
        "progress": task.get("progress"),
        "total": task.get("total"),
        "started_at": task.get("started_at"),
        "error": task.get("error"),
    })


def api_smartmoney_institution_detail(
    inst_id: str = Query("", alias="id"),
    nocache: int = 0,
) -> JSONResponse:
    iid = (inst_id or "").strip().lower()
    inst = _smartmoney_inst_map().get(iid)
    if not inst:
        return JSONResponse({"ok": False, "error": "institution_not_found"}, status_code=404)
    if not str(inst.get("cik") or "").strip():
        return JSONResponse({"ok": False, "error": "missing_cik"}, status_code=422)
    ck = f"sm:inst_detail:{iid}"
    bypass_cache = False
    try:
        bypass_cache = int(nocache or 0) == 1
    except Exception:
        bypass_cache = False

    # 仅使用内存缓存，不使用 Upstash
    if not bypass_cache:
        cached = _cache_get(ck, SEC_EDGAR_CACHE_TTL_SEC)
        if isinstance(cached, dict) and cached.get("ok"):
            ts = cached.get("snapshot_ts") if isinstance(cached, dict) else None
            out = dict(cached)
            out.setdefault("data_source", "memory")
            out.setdefault("snapshot_ts", ts)
            return JSONResponse(out, headers={"X-SM-Source": "memory", "X-SM-Snapshot-Ts": str(out.get("snapshot_ts") or "")})
    cik = str(inst.get("cik") or "")
    cur = _sec_get_13f_holdings_by_cik(cik, filing_index=0)
    prev = _sec_get_13f_holdings_by_cik(cik, filing_index=1)
    if not (isinstance(cur, dict) and cur.get("ok")):
        err = "sec_fetch_failed"
        detail = None
        try:
            err = str(cur.get("error") or err) if isinstance(cur, dict) else err
            detail = cur.get("detail") if isinstance(cur, dict) else None
        except Exception:
            err = "sec_fetch_failed"
            detail = None
        out = {"ok": False, "error": err}
        if detail is not None:
            out["detail"] = detail
        if isinstance(cur, dict):
            for k in ("cik", "accession", "info_file", "file_cik", "accession_cik"):
                if k in cur and cur.get(k) is not None:
                    out[k] = cur.get(k)
        return JSONResponse(out, status_code=502)
    hs = cur.get("holdings") or []
    hs_prev = prev.get("holdings") if isinstance(prev, dict) and prev.get("ok") else []
    prev_map: Dict[str, Dict[str, Any]] = {}
    if isinstance(hs_prev, list):
        for r in hs_prev:
            if isinstance(r, dict) and str(r.get("cusip") or "").strip():
                prev_map[str(r.get("cusip") or "").strip()] = r

    holdings2: List[Dict[str, Any]] = []
    cur_seen: set = set()
    new_pos: List[Dict[str, Any]] = []
    add_pos: List[Dict[str, Any]] = []
    red_pos: List[Dict[str, Any]] = []
    for h in hs:
        if not isinstance(h, dict):
            continue
        cusip = str(h.get("cusip") or "").strip()
        issuer = str(h.get("issuer") or "").strip()
        value_usd = float(h.get("value_usd") or 0.0)
        weight = float(h.get("weight") or 0.0)
        shares = float(h.get("shares") or 0.0)
        prev_r = prev_map.get(cusip)
        prev_val = float(prev_r.get("value_usd") or 0.0) if isinstance(prev_r, dict) else 0.0
        cur_seen.add(cusip)
        qoq_value_change = value_usd - prev_val
        holdings2.append({
            "cusip": cusip,
            "issuer": issuer,
            "title": str(h.get("title") or ""),
            "weight": weight,
            "value_usd": value_usd,
            "shares": shares,
            "shares_type": str(h.get("shares_type") or ""),
            "qoq_value_change": qoq_value_change,
        })

        if prev_val <= 0 and value_usd > 0:
            new_pos.append({"cusip": cusip, "issuer": issuer, "value_usd": value_usd, "prev_value_usd": prev_val, "delta_usd": qoq_value_change})
        elif qoq_value_change > 0:
            add_pos.append({"cusip": cusip, "issuer": issuer, "value_usd": value_usd, "prev_value_usd": prev_val, "delta_usd": qoq_value_change})
        elif qoq_value_change < 0 and value_usd > 0 and prev_val > 0:
            red_pos.append({"cusip": cusip, "issuer": issuer, "value_usd": value_usd, "prev_value_usd": prev_val, "delta_usd": qoq_value_change})
    holdings2.sort(key=lambda x: float(x.get("value_usd") or 0.0), reverse=True)
    top10 = holdings2[:10]
    changes = sorted(holdings2, key=lambda x: abs(float(x.get("qoq_value_change") or 0.0)), reverse=True)[:10]

    exit_pos: List[Dict[str, Any]] = []
    for cusip, pr in prev_map.items():
        if cusip in cur_seen:
            continue
        try:
            prev_val2 = float((pr or {}).get("value_usd") or 0.0)
        except Exception:
            prev_val2 = 0.0
        if prev_val2 <= 0:
            continue
        issuer2 = str((pr or {}).get("issuer") or "").strip() if isinstance(pr, dict) else ""
        exit_pos.append({"cusip": str(cusip or "").strip(), "issuer": issuer2, "value_usd": 0.0, "prev_value_usd": prev_val2, "delta_usd": -prev_val2})

    def _top10(seq: List[Dict[str, Any]], key: str = "delta_usd", reverse: bool = True) -> List[Dict[str, Any]]:
        try:
            s2 = [x for x in (seq or []) if isinstance(x, dict)]
            s2.sort(key=lambda x: float(x.get(key) or 0.0), reverse=reverse)
            return s2[:10]
        except Exception:
            return (seq or [])[:10]

    change_breakdown = {
        "counts": {
            "new": int(len(new_pos)),
            "add": int(len(add_pos)),
            "reduce": int(len(red_pos)),
            "exit": int(len(exit_pos)),
        },
        "top10": {
            "new": _top10(new_pos, key="delta_usd", reverse=True),
            "add": _top10(add_pos, key="delta_usd", reverse=True),
            "reduce": _top10(red_pos, key="delta_usd", reverse=False),
            "exit": _top10(exit_pos, key="delta_usd", reverse=False),
        },
    }
    resp_obj = {
        "ok": True,
        "institution": {
            "id": inst.get("id"),
            "name": inst.get("name"),
            "cik": _sec_norm_cik(cik),
            "aum_usd": float(cur.get("total_value_usd") or 0.0),
            "accession": cur.get("accession"),
        },
        "holdings": holdings2,
        "top_holdings": top10,
        "recent_changes": changes,
        "change_breakdown": change_breakdown,
    }
    resp_obj["data_source"] = "live"
    resp_obj["snapshot_ts"] = None
    _cache_set(ck, resp_obj)
    return JSONResponse(resp_obj, headers={"X-SM-Source": "live", "X-SM-Snapshot-Ts": ""})


# YFinance helpers ------------------------------------------------------------

_YF_CUSIP_TICKER_CACHE: Dict[str, Tuple[float, Optional[str]]] = {}
_YF_CUSIP_TICKER_CACHE_TTL = 86400 * 7  # 7 days

def _yf_search_issuer(issuer: str) -> Optional[str]:
    """通过发行人名称（issuer）搜索 Yahoo Finance 找到对应 ticker。
    使用 yfinance.Search 进行搜索并缓存结果。
    缓存 key 为归一化的 issuer 名称，TTL = 7 天。
    """
    if yf is None:
        return None
    raw_key = (issuer or "").strip()
    key = re.sub(r"\s+", "", raw_key.upper())
    if not key:
        return None
    # check cache
    cached = _YF_CUSIP_TICKER_CACHE.get(key)
    if cached is not None:
        ts, val = cached
        if time.time() - ts < _YF_CUSIP_TICKER_CACHE_TTL:
            return val
    try:
        # 尝试直接查询 ticker（一些知名公司有标准代码）
        # 先用原始名称（带空格）查 known dict
        known_raw = raw_key.upper()
        known = {
            "APPLE INC": "AAPL",
            "APPLE": "AAPL",
            "MICROSOFT CORP": "MSFT",
            "MICROSOFT CORPORATION": "MSFT",
            "MICROSOFT": "MSFT",
            "ALPHABET INC": "GOOGL",
            "ALPHABET": "GOOGL",
            "GOOGLE": "GOOGL",
            "AMAZON COM INC": "AMZN",
            "AMAZON": "AMZN",
            "META PLATFORMS INC": "META",
            "META": "META",
            "BERKSHIRE HATHAWAY INC": "BRK.B",
            "BERKSHIRE HATHAWAY": "BRK.B",
            "NVIDIA CORPORATION": "NVDA",
            "NVIDIA": "NVDA",
            "TESLA INC": "TSLA",
            "TESLA": "TSLA",
            "JPMORGAN CHASE & CO": "JPM",
            "JPMORGAN CHASE": "JPM",
            "VISA INC": "V",
            "VISA": "V",
            "JOHNSON & JOHNSON": "JNJ",
            "WALMART INC": "WMT",
            "WALMART": "WMT",
            "PROCTER & GAMBLE": "PG",
            "PROCTER & GAMBLE CO": "PG",
            "UNITEDHEALTH GROUP INC": "UNH",
            "UNITEDHEALTH": "UNH",
            "BANK OF AMERICA CORP": "BAC",
            "BANK OF AMERICA": "BAC",
            "COCA COLA": "KO",
            "COCA-COLA": "KO",
            "COCA COLA CO": "KO",
            "WELLS FARGO & CO": "WFC",
            "WELLS FARGO": "WFC",
            "WALT DISNEY": "DIS",
            "WALT DISNEY CO": "DIS",
            "DISNEY": "DIS",
            "ADOBE INC": "ADBE",
            "ADOBE": "ADBE",
            "PFIZER INC": "PFE",
            "PFIZER": "PFE",
            "INTEL CORPORATION": "INTC",
            "INTEL": "INTC",
            "CISCO SYSTEMS INC": "CSCO",
            "CISCO": "CSCO",
            "NETFLIX INC": "NFLX",
            "NETFLIX": "NFLX",
            "CHEVRON CORPORATION": "CVX",
            "CHEVRON": "CVX",
            "EXXON MOBIL CORPORATION": "XOM",
            "EXXON MOBIL": "XOM",
            "HOME DEPOT INC": "HD",
            "HOME DEPOT": "HD",
            "NEOGENOMICS INC": "NEO",
            "NEOGENOMICS": "NEO",
            "AMERICAN TOWER CORP": "AMT",
            "AMERICAN TOWER": "AMT",
            "GENERAL ELECTRIC": "GE",
            "GENERAL ELECTRIC CO": "GE",
            "DIGITAL REALTY TRUST INC": "DLR",
            "DIGITAL REALTY": "DLR",
            "ASTRAZENECA PLC": "AZN",
            "ASTRAZENECA": "AZN",
            "MCDONALDS CORPORATION": "MCD",
            "MCDONALDS": "MCD",
        }
        if known_raw in known:
            ticker = known[known_raw]
            _YF_CUSIP_TICKER_CACHE[key] = (time.time(), ticker)
            return ticker

        # 利用 yfinance 的 Search 来查找
        search = yf.Search(query=issuer, max_results=3)
        quotes = search.quotes if hasattr(search, 'quotes') else []
        if isinstance(quotes, list) and quotes:
            for q in quotes[:3]:
                if isinstance(q, dict):
                    symbol = str(q.get('symbol') or '').strip()
                    if symbol:
                        _YF_CUSIP_TICKER_CACHE[key] = (time.time(), symbol)
                        return symbol
        # 如果 Search 没能返回结果，备选：尝试用 Ticker 直接查询
        # 把 issuername 中的不常见词去掉后尝试
        cleaned = re.sub(r'\b(INC|CORP|CORPORATION|&|CO|PLC|LTD|LIMITED|NV|SA|LP|LLC)\b', '', issuer, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        if cleaned and cleaned != issuer:
            search2 = yf.Search(query=cleaned, max_results=3)
            quotes2 = search2.quotes if hasattr(search2, 'quotes') else []
            if isinstance(quotes2, list) and quotes2:
                for q in quotes2[:3]:
                    if isinstance(q, dict):
                        symbol = str(q.get('symbol') or '').strip()
                        if symbol:
                            _YF_CUSIP_TICKER_CACHE[key] = (time.time(), symbol)
                            return symbol
    except Exception:
        pass
    _YF_CUSIP_TICKER_CACHE[key] = (time.time(), None)
    return None


def _yf_get_price(ticker: str) -> Optional[Dict[str, Any]]:
    """获取股票实时价格（支持盘后价）、涨跌幅等。
    
    返回: {"price": float, "change_pct": float, "currency": str, "market_state": str}
    或 None（查询失败时）。
    结果缓存 60 秒。
    """
    if yf is None or not ticker:
        return None
    ck = f"yf:price:{ticker}"
    cached = _cache_get(ck, ttl=60)
    if cached is not None:
        return cached
    try:
        tk = yf.Ticker(ticker)
        info = tk.info if hasattr(tk, 'info') else {}
        if not isinstance(info, dict):
            info = {}
        price = info.get('currentPrice') or info.get('regularMarketPrice') or info.get('previousClose')
        if price is None:
            # fallback: 尝试获取快速报价
            try:
                fast_info = tk.fast_info if hasattr(tk, 'fast_info') else None
                if fast_info is not None:
                    price = getattr(fast_info, 'last_price', None) or getattr(fast_info, 'regular_market_previous_close', None)
            except Exception:
                pass
        if price is None:
            return None
        try:
            price = float(price)
        except Exception:
            return None
        change_pct = info.get('regularMarketChangePercent') or info.get('regularMarketChange')
        try:
            change_pct = float(change_pct) if change_pct is not None else None
        except Exception:
            change_pct = None
        currency = str(info.get('currency') or 'USD')
        market_state = str(info.get('marketState') or 'REGULAR')
        result = {
            "price": float(price),
            "change_pct": float(change_pct) if change_pct is not None else None,
            "currency": currency,
            "market_state": market_state,
        }
        _cache_set(ck, result)
        return result
    except Exception:
        return None


def _cusip_to_ticker_via_finnhub(cusip: str) -> Optional[str]:
    """
    使用 Finnhub API 的 /search 接口将 CUSIP 映射为股票 Ticker。
    优先选择 type 为 "Common Stock" 且 exchange 为美国市场的结果。
    结果缓存 7 天。
    """
    cusip = (cusip or "").strip().upper()
    if not cusip:
        return None
    ck = f"finnhub:cusip2tk:{cusip}"
    cached = _cache_get(ck, ttl=86400 * 7)
    if isinstance(cached, str) and cached:
        return cached
    client = _get_finnhub_client()
    if client is None:
        return None
    try:
        result = client.symbol_lookup(cusip)
        if not isinstance(result, dict):
            return None
        items = result.get("result")
        if not isinstance(items, list) or not items:
            return None
        # 优先选择 Common Stock + 美国交易所
        best = None
        for item in items:
            if not isinstance(item, dict):
                continue
            sym = str(item.get("symbol") or "").strip().upper()
            if not sym:
                continue
            itype = str(item.get("type") or "")
            exchange = str(item.get("exchange") or "").upper()
            mic = str(item.get("mic") or "").upper()
            # 强匹配：Common Stock + US
            if itype == "Common Stock" and ("US" in exchange or exchange in ("XNYS", "XNAS", "XASE", "BATS", "ARCX")):
                best = sym
                break
            # 次匹配：Common Stock
            if itype == "Common Stock" and best is None:
                best = sym
            # 再次：只要 exchange 是美国
            if best is None and (exchange.startswith("US") or exchange in ("XNYS", "XNAS", "XASE", "BATS", "ARCX")):
                best = sym
        # 兜底：第一个结果
        if best is None:
            first = items[0]
            best = str((isinstance(first, dict) and first.get("symbol")) or "").strip().upper() or None
        if best:
            _cache_set(ck, best)
            return best
        return None
    except Exception:
        return None


def _yf_get_indicators(ticker: str) -> Optional[Dict[str, Any]]:
    """
    使用 yfinance 获取股票当前指标（实时价格 + 财务指标）。
    返回完整指标面板数据，缓存 120 秒。
    """
    if yf is None or not ticker:
        return None
    ticker = (ticker or "").strip().upper()
    ck = f"yf:indicators:{ticker}"
    cached = _cache_get(ck, ttl=120)
    if isinstance(cached, dict) and cached.get("ticker") == ticker:
        return cached
    try:
        tk = yf.Ticker(ticker)
        info = tk.info if hasattr(tk, "info") else {}
        if not isinstance(info, dict):
            info = {}

        # 价格字段
        current_price = info.get("currentPrice") or info.get("regularMarketPrice") or info.get("previousClose")
        try: current_price = float(current_price) if current_price is not None else None
        except Exception: current_price = None

        previous_close = info.get("previousClose") or info.get("regularMarketPreviousClose")
        try: previous_close = float(previous_close) if previous_close is not None else None
        except Exception: previous_close = None

        # 市值
        market_cap = info.get("marketCap")
        try: market_cap = float(market_cap) if market_cap is not None else None
        except Exception: market_cap = None

        # PE
        pe_trailing = info.get("trailingPE")
        try: pe_trailing = float(pe_trailing) if pe_trailing is not None else None
        except Exception: pe_trailing = None

        pe_forward = info.get("forwardPE")
        try: pe_forward = float(pe_forward) if pe_forward is not None else None
        except Exception: pe_forward = None

        # EPS
        eps_trailing = info.get("trailingEps")
        try: eps_trailing = float(eps_trailing) if eps_trailing is not None else None
        except Exception: eps_trailing = None

        # ROE
        roe = info.get("returnOnEquity")
        try: roe = float(roe) if roe is not None else None
        except Exception: roe = None

        # Beta
        beta = info.get("beta")
        try: beta = float(beta) if beta is not None else None
        except Exception: beta = None

        # 52周高低
        fifty_two_week_high = info.get("fiftyTwoWeekHigh")
        try: fifty_two_week_high = float(fifty_two_week_high) if fifty_two_week_high is not None else None
        except Exception: fifty_two_week_high = None

        fifty_two_week_low = info.get("fiftyTwoWeekLow")
        try: fifty_two_week_low = float(fifty_two_week_low) if fifty_two_week_low is not None else None
        except Exception: fifty_two_week_low = None

        # 成交量
        volume = info.get("volume") or info.get("regularMarketVolume")
        try: volume = int(volume) if volume is not None else None
        except Exception: volume = None

        average_volume = info.get("averageVolume") or info.get("averageDailyVolume10Day")
        try: average_volume = int(average_volume) if average_volume is not None else None
        except Exception: average_volume = None

        # 分析师目标价
        target_mean_price = info.get("targetMeanPrice")
        try: target_mean_price = float(target_mean_price) if target_mean_price is not None else None
        except Exception: target_mean_price = None

        # 分析师建议
        recommendation = info.get("recommendationKey") or info.get("recommendationMean")
        if recommendation is not None:
            try:
                if isinstance(recommendation, str):
                    recommendation = recommendation
                else:
                    recommendation = str(recommendation)
            except Exception:
                recommendation = None

        # 公司名
        company_name = (info.get("longName") or info.get("shortName") or ticker)

        # 货币
        currency = str(info.get("currency") or "USD")

        result = {
            "ticker": ticker,
            "company_name": str(company_name),
            "current_price": current_price,
            "previous_close": previous_close,
            "market_cap": market_cap,
            "pe_trailing": pe_trailing,
            "pe_forward": pe_forward,
            "eps_trailing": eps_trailing,
            "roe": roe,
            "beta": beta,
            "fifty_two_week_high": fifty_two_week_high,
            "fifty_two_week_low": fifty_two_week_low,
            "volume": volume,
            "average_volume": average_volume,
            "target_mean_price": target_mean_price,
            "recommendation": recommendation,
            "currency": currency,
            "last_updated": int(time.time()),
        }
        _cache_set(ck, result)
        return result
    except Exception as e:
        # 检查是否是速率限制错误
        err_type = type(e).__name__
        err_str = str(e)[:200]
        if "RateLimit" in err_type or "Too Many Requests" in err_str or "rate limited" in err_str.lower():
            return {
                "ticker": ticker,
                "error": "yfinance_rate_limited",
                "error_type": err_type,
                "error_message": "Yahoo Finance API 速率限制，请稍后重试",
                "last_updated": int(time.time()),
            }
        return None


def _resolve_cusip_to_indicators(cusip: str) -> Dict[str, Any]:
    """
    CUSIP → Ticker → 指标 的完整解析链：
    1) 本地映射表
    2) Finnhub API
    3) 13F issuer 推导
    然后调用 yfinance 获取当前指标。
    """
    indicators = None
    resolved_ticker = None
    resolved_method = ""

    # 策略1: 本地映射表
    from_map = _CUSIP_TICKER_MAP.get(cusip)
    if from_map:
        indicators = _yf_get_indicators(from_map)
        if indicators:
            resolved_ticker = from_map
            resolved_method = "local_map"

    # 策略2: Finnhub API
    if indicators is None or (indicators and indicators.get("error")):
        finn_ticker = _cusip_to_ticker_via_finnhub(cusip)
        if finn_ticker:
            indicators = _yf_get_indicators(finn_ticker)
            if indicators:
                resolved_ticker = finn_ticker
                resolved_method = "finnhub"

    # 策略3: 13F issuer → ticker 推导
    if indicators is None or (indicators and indicators.get("error")):
        # 尝试通过 13F 找到 issuer
        issuer = ""
        for inst in _smartmoney_get_institutions_meta():
            cik = str(inst.get("cik") or "")
            if not cik:
                continue
            try:
                cur = _sec_get_13f_holdings_by_cik(cik, filing_index=0)
                hs = cur.get("holdings") if isinstance(cur, dict) and cur.get("ok") else None
                if isinstance(hs, list):
                    for h in hs:
                        if isinstance(h, dict) and str(h.get("cusip") or "").strip().upper() == cusip:
                            issuer = str(h.get("issuer") or "").strip()
                            break
                if issuer:
                    break
            except Exception:
                continue
        if issuer:
            yf_ticker = _yf_search_issuer(issuer)
            if yf_ticker:
                indicators = _yf_get_indicators(yf_ticker)
                if indicators:
                    resolved_ticker = yf_ticker
                    resolved_method = "13f_issuer"

    return {
        "indicators": indicators,
        "resolved_ticker": resolved_ticker,
        "resolved_method": resolved_method,
    }

# ==================== Stock Holders 数据缓存 ====================
_SMARTMONEY_HOLDERS_SNAPSHOT: Dict[str, Any] = {}
_SMARTMONEY_HOLDERS_SNAPSHOT_LOCK = threading.Lock()
SMARTMONEY_HOLDERS_SNAPSHOT_KEY = (os.getenv("SMARTMONEY_HOLDERS_SNAPSHOT_KEY") or "smartmoney:holders:snapshot:v1").strip()

def _build_holders_data(cusip: str) -> Dict[str, Any]:
    """从 SEC EDGAR 实时构建 holders 数据"""
    # 加载机构中文名映射
    cn_name_map: Dict[str, str] = {}
    try:
        json_path = os.path.join(os.path.dirname(__file__), "institutions_50.json")
        if os.path.exists(json_path):
            with open(json_path, "r", encoding="utf-8") as f:
                file_items = json.load(f)
            if isinstance(file_items, list):
                for item in file_items:
                    if isinstance(item, dict):
                        name = str(item.get("name") or "").strip()
                        cn_name = str(item.get("cn_name") or "").strip()
                        if name and cn_name:
                            cn_name_map[name.lower()] = cn_name
    except Exception:
        pass

    holders: List[Dict[str, Any]] = []
    best_issuer = ""
    for inst in _smartmoney_get_institutions_meta():
        iid = str(inst.get("id") or "")
        iname = str(inst.get("name") or "")
        cik = str(inst.get("cik") or "")
        if not str(cik or "").strip():
            continue
        try:
            cur = _sec_get_13f_holdings_by_cik(cik, filing_index=0)
            prev = _sec_get_13f_holdings_by_cik(cik, filing_index=1)
        except Exception:
            continue
        hs = cur.get("holdings") if isinstance(cur, dict) and cur.get("ok") else None
        hs_prev = prev.get("holdings") if isinstance(prev, dict) and prev.get("ok") else None
        if not isinstance(hs, list):
            continue
        prev_map: Dict[str, Dict[str, Any]] = {}
        if isinstance(hs_prev, list):
            for r in hs_prev:
                if isinstance(r, dict) and str(r.get("cusip") or "").strip():
                    prev_map[str(r.get("cusip") or "").strip()] = r
        for h in hs:
            if not isinstance(h, dict):
                continue
            if str(h.get("cusip") or "").strip().upper() != cusip:
                continue
            if not best_issuer:
                best_issuer = str(h.get("issuer") or "").strip()
            val = float(h.get("value_usd") or 0.0)
            wt = float(h.get("weight") or 0.0)
            prev_val = float((prev_map.get(cusip) or {}).get("value_usd") or 0.0)
            holders.append({
                "inst_id": iid,
                "inst_name": iname,
                "cn_name": cn_name_map.get(iname.lower(), ""),
                "weight": wt,
                "value_usd": val,
                "qoq_value_change": val - prev_val,
            })
    holders.sort(
        key=lambda x: (float(x.get("weight") or 0.0), float(x.get("value_usd") or 0.0)),
        reverse=True,
    )
    return {
        "cusip": cusip,
        "issuer": best_issuer,
        "holders": holders,
        "ts": time.time(),
    }

def api_smartmoney_stock_holders(
    ticker: str = "",
    refresh: bool = Query(False, alias="refresh"),
) -> JSONResponse:
    """
    获取股票的机构持有（按占比）
    
    数据源优先级：
    1. 内存快照（本进程缓存）
    2. SQLite 数据库缓存（持久化，重启后恢复）
    3. SEC EDGAR 实时获取
    
    Args:
        ticker: 股票代码或 CUSIP
        refresh: 强制刷新，从 SEC 重新获取并更新缓存和数据库
    """
    cusip = re.sub(r"\s+", "", (ticker or "").strip().upper())
    if not cusip:
        return JSONResponse({"ok": False, "error": "missing_cusip"}, status_code=400)
    
    ttl_sec = max(0, int(SEC_EDGAR_CACHE_TTL_SEC) * 6) if SEC_EDGAR_CACHE_TTL_SEC else 21600
    
    # 强制刷新模式：直接获取最新数据
    if refresh:
        # 先清除 SEC 缓存，强制重新请求
        try:
            with _CACHE_LOCK:
                keys_to_del = [k for k in list(_CACHE.keys()) if k.startswith("sec:13f:holdings:") or k.startswith("sec:submissions:") or k.startswith("sec:index:")]
                for k in keys_to_del:
                    del _CACHE[k]
        except Exception:
            pass
        data = _build_holders_data(cusip)
        data["ok"] = True
        data["data_source"] = "live"
        # 写入内存缓存
        with _SMARTMONEY_HOLDERS_SNAPSHOT_LOCK:
            _SMARTMONEY_HOLDERS_SNAPSHOT[cusip] = data
        # 写入数据库（持久化）
        db_ok = False
        try:
            db_ok = _db_set_stock_holders(cusip, data, ttl_sec=ttl_sec)
        except Exception as e:
            data["db_error"] = str(e)
        data["db_saved"] = db_ok
        return JSONResponse(data, headers={"X-SM-Source": "live"})
    
    # 检查内存缓存
    stale_limit_sec = ttl_sec
    snap_ts: float = 0.0
    snap_items: Optional[List[Dict[str, Any]]] = None
    snap_issuer: str = ""
    try:
        with _SMARTMONEY_HOLDERS_SNAPSHOT_LOCK:
            snap = _SMARTMONEY_HOLDERS_SNAPSHOT.get(cusip)
            if isinstance(snap, dict):
                snap_ts = float(snap.get("ts") or 0.0)
                snap_items = snap.get("holders")
                snap_issuer = snap.get("issuer", "")
    except Exception:
        pass
    
    # 使用内存缓存数据（如果未过期）
    if isinstance(snap_items, list) and snap_items and (not stale_limit_sec or (time.time() - snap_ts) < stale_limit_sec):
        return JSONResponse({
            "ok": True,
            "cusip": cusip,
            "issuer": snap_issuer,
            "holders": snap_items,
            "data_source": "memory",
            "ts": snap_ts if snap_ts else None,
        }, headers={"X-SM-Source": "memory", "X-SM-Snapshot-Ts": str(snap_ts or "")})
    
    # 检查数据库缓存
    try:
        db_data = _db_get_stock_holders(cusip)
        if db_data and isinstance(db_data.get("holders"), list) and db_data["holders"]:
            # 写入内存缓存
            with _SMARTMONEY_HOLDERS_SNAPSHOT_LOCK:
                _SMARTMONEY_HOLDERS_SNAPSHOT[cusip] = db_data
            return JSONResponse({
                "ok": True,
                "cusip": cusip,
                "issuer": db_data.get("issuer", ""),
                "holders": db_data["holders"],
                "data_source": "db",
                "ts": db_data.get("ts"),
            }, headers={"X-SM-Source": "db"})
    except Exception:
        pass
    
    # 从 SEC 实时获取
    data = _build_holders_data(cusip)
    data["ok"] = True
    data["data_source"] = "live"
    
    # 写入内存缓存
    try:
        with _SMARTMONEY_HOLDERS_SNAPSHOT_LOCK:
            _SMARTMONEY_HOLDERS_SNAPSHOT[cusip] = data
    except Exception:
        pass
    
    # 写入数据库（持久化）
    try:
        _db_set_stock_holders(cusip, data, ttl_sec=ttl_sec)
    except Exception:
        pass
    
    return JSONResponse(data, headers={"X-SM-Source": "live"})


# ==================== SEC EDGAR 公司财报分析功能 ====================

def _sec_get_company_facts(cik: str, max_retries: int = 3, delay: float = 0.5) -> Optional[Dict[str, Any]]:
    """
    从 SEC EDGAR API 获取公司财务数据 (companyfacts.json)
    
    Args:
        cik: 公司 CIK (10位数字格式，如 0000320193)
        max_retries: 最大重试次数
        delay: 请求间隔延时（秒）
    
    Returns:
        companyfacts.json 解析后的字典，失败返回 None
    """
    cik_clean = (cik or "").strip()
    if not cik_clean:
        return None
    
    # 确保 CIK 是 10 位格式
    cik_padded = cik_clean.zfill(10)
    
    # 缓存 key
    cache_key = f"sec:companyfacts:{cik_padded}"
    ttl = 86400 * 7  # 7 天缓存
    
    # 检查缓存
    cached = _cache_get(cache_key, ttl)
    if isinstance(cached, dict):
        return cached
    
    # SEC EDGAR API URL
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json"
    
    headers = _sec_headers()
    
    # 重试机制
    for attempt in range(max_retries):
        try:
            # 延时避免请求过快
            if attempt > 0:
                time.sleep(delay * (2 ** attempt))  # 指数退避
            
            resp = HTTP.get(url, headers=headers, timeout=(10, 60))
            resp.raise_for_status()
            data = resp.json()
            
            # 存入缓存
            _cache_set(cache_key, data)
            return data
            
        except Exception as e:
            if attempt == max_retries - 1:
                break
            continue
    
    return None


def _sec_get_submissions(cik: str, max_retries: int = 3, delay: float = 0.5) -> Optional[Dict[str, Any]]:
    """
    从 SEC EDGAR API 获取公司提交的 filings (submissions.json)
    
    Args:
        cik: 公司 CIK (10位数字格式)
        max_retries: 最大重试次数
        delay: 请求间隔延时
    
    Returns:
        submissions.json 解析后的字典，失败返回 None
    """
    cik_clean = (cik or "").strip()
    if not cik_clean:
        return None
    
    cik_padded = cik_clean.zfill(10)
    cache_key = f"sec:submissions:{cik_padded}"
    ttl = 86400 * 1  # 1 天缓存
    
    cached = _cache_get(cache_key, ttl)
    if isinstance(cached, dict):
        return cached
    
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    headers = _sec_headers()
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                time.sleep(delay * (2 ** attempt))
            
            resp = HTTP.get(url, headers=headers, timeout=(10, 30))
            resp.raise_for_status()
            data = resp.json()
            
            _cache_set(cache_key, data)
            return data
            
        except Exception:
            if attempt == max_retries - 1:
                break
            continue
    
    return None


def _extract_financial_metrics(
    company_facts: Dict[str, Any],
    period_type: str = "annual",
    quarter_id: str = "",
    annual_id: str = "",
    eps_data: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    从 companyfacts.json 中提取关键财务指标
    
    Args:
        company_facts: SEC EDGAR companyfacts.json 数据
        period_type: "annual" (年报 10-K) 或 "quarterly" (季报 10-Q)
        quarter_id: 具体季度ID，如 "2026Q1"，用于筛选该季度的数据
        annual_id: 具体年报ID，如 "FY2025"，用于筛选该年度的数据
        eps_data: EPS数据结构，包含 basic_eps 和 diluted_eps
    
    提取的数据包括：
    - 盈利能力 (Income Statement)
    - 资产负债表 (Balance Sheet)
    - 现金流量表 (Cash Flow)
    - 其他重要指标
    """
    if not isinstance(company_facts, dict):
        return {"error": "invalid_data"}
    
    facts = company_facts.get("facts", {})
    if not isinstance(facts, dict):
        return {"error": "no_facts_data"}
    
    # 使用 us-gaap 或 ifrs-full  taxonomy
    gaap = facts.get("us-gaap", {})
    ifrs = facts.get("ifrs-full", {})
    
    # 合并数据（优先使用 us-gaap）
    data = {**ifrs, **gaap}
    
    # 根据 period_type 确定筛选条件
    # annual: 10-K 年报，取 fiscal period FY (全年)
    # quarterly: 10-Q 季报，取 fiscal period Q1/Q2/Q3/Q4
    is_annual = period_type == "annual"
    target_quarter = quarter_id  # 如 "2026Q1"
    
    def _filter_by_period(item: Dict[str, Any]) -> bool:
        """根据年报/季报筛选数据"""
        form = str(item.get("form", ""))
        fp = str(item.get("fp", "")).upper()
        fy = str(item.get("fy", ""))
        
        if is_annual:
            # 年报：10-K 或 fiscal period 为 FY
            return form == "10-K" or fp == "FY"
        else:
            # 季报：10-Q 且 fiscal period 为 Q1/Q2/Q3/Q4
            is_quarter = form == "10-Q" or fp in ["Q1", "Q2", "Q3", "Q4"]
            if not is_quarter:
                return False
            # 如果指定了 quarter_id，则只匹配该季度
            if target_quarter:
                item_quarter = f"{fy}{fp}"
                return item_quarter == target_quarter
            return True
    
    def _get_latest_value(concept: str, unit_preference: List[str] = None) -> Optional[float]:
        """获取某个财务概念的最新值（根据 period_type 筛选）"""
        concept_data = data.get(concept)
        if not isinstance(concept_data, dict):
            return None
        
        units = concept_data.get("units", {})
        if not isinstance(units, dict):
            return None
        
        # 确定单位优先级（默认 USD）
        unit_prefs = unit_preference or ["USD", "usd", "shares", "Shares"]
        unit_data = None
        for unit in unit_prefs:
            unit_data = units.get(unit)
            if isinstance(unit_data, list) and unit_data:
                break
        
        if not isinstance(unit_data, list) or not unit_data:
            return None
        
        # 按 filed 日期排序，并根据 period_type 筛选
        sorted_items = sorted(
            unit_data,
            key=lambda x: str(x.get("filed", "")),
            reverse=True
        )
        
        for item in sorted_items:
            if not _filter_by_period(item):
                continue
            val = item.get("val")
            if val is not None:
                return float(val)
        
        # 如果筛选后没有数据，回退到使用所有数据
        for item in sorted_items:
            val = item.get("val")
            if val is not None:
                return float(val)
        
        return None
    
    def _get_historical_values(concept: str, limit: int = 5, unit_preference: List[str] = None) -> List[Dict[str, Any]]:
        """获取某个财务概念的历史值列表（根据 period_type 筛选）"""
        concept_data = data.get(concept)
        if not isinstance(concept_data, dict):
            return []
        
        units = concept_data.get("units", {})
        if not isinstance(units, dict):
            return []
        
        # 确定单位优先级（默认 USD）
        unit_prefs = unit_preference or ["USD", "usd", "shares", "Shares"]
        unit_data = None
        for unit in unit_prefs:
            unit_data = units.get(unit)
            if isinstance(unit_data, list) and unit_data:
                break
        
        if not isinstance(unit_data, list):
            return []
        
        # 按 filed 日期排序，去重，并根据 period_type 筛选
        seen = set()
        results = []
        for item in sorted(unit_data, key=lambda x: str(x.get("filed", "")), reverse=True):
            filed = str(item.get("filed", ""))
            if filed in seen:
                continue
            
            # 年报/季报筛选
            if not _filter_by_period(item):
                continue
            
            seen.add(filed)
            
            val = item.get("val")
            if val is not None:
                results.append({
                    "filed": filed,
                    "fy": item.get("fy"),
                    "fp": item.get("fp"),
                    "val": float(val),
                    "form": item.get("form"),
                })
            
            if len(results) >= limit:
                break
        
        # 如果筛选后数据不足，补充非筛选数据
        if len(results) < limit:
            for item in sorted(unit_data, key=lambda x: str(x.get("filed", "")), reverse=True):
                filed = str(item.get("filed", ""))
                if filed in seen:
                    continue
                seen.add(filed)
                val = item.get("val")
                if val is not None:
                    results.append({
                        "filed": filed,
                        "fy": item.get("fy"),
                        "fp": item.get("fp"),
                        "val": float(val),
                        "form": item.get("form"),
                    })
                if len(results) >= limit:
                    break
        
        return results
    
    # ============== A. 盈利能力指标 ==============
    # 使用统一的季度计算逻辑获取收入指标（与EPS相同逻辑）
    
    # Revenue / Total Revenue
    revenue_data = _get_quarterly_financial_data(company_facts, [
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "Revenues",
        "SalesRevenueNet",
        "TotalRevenues"
    ])
    
    # Gross Profit
    gross_profit_data = _get_quarterly_financial_data(company_facts, [
        "GrossProfit"
    ])
    
    # Operating Income
    operating_income_data = _get_quarterly_financial_data(company_facts, [
        "OperatingIncomeLoss"
    ])
    
    # Net Income
    net_income_data = _get_quarterly_financial_data(company_facts, [
        "NetIncomeLoss",
        "ProfitLoss"
    ])
    
    # 根据当前选中的period获取对应的值（与EPS逻辑一致）
    def _get_value_from_quarterly_data(data: Dict, period_type: str, 
                                       quarter_id: str, annual_id: str) -> Optional[float]:
        """从季度数据中获取对应期间的值"""
        if not data or not data.get("quarters"):
            return None
        
        if period_type == "annual":
            # 年报模式：获取对应年份的数据
            if annual_id:
                target_fy = int(annual_id.replace("FY", ""))
                for a in data.get("annual_list", []):
                    if a.get("fy") == target_fy:
                        return a.get("value")
            else:
                # 未指定年份，取最新年报
                if data.get("latest_annual"):
                    return data["latest_annual"].get("value")
        else:
            # 季报模式：根据 quarter_id 找对应季度
            for q in data.get("quarters", []):
                if quarter_id:
                    if q.get("quarter") == quarter_id or q.get("quarter_label") == quarter_id:
                        return q.get("value")
                    if quarter_id.startswith("20") and q.get("quarter") == quarter_id:
                        return q.get("value")
                    if not quarter_id.startswith("20") and q.get("quarter_label") == quarter_id:
                        return q.get("value")
        return None
    
    # 获取当前选中期的指标值
    revenue = _get_value_from_quarterly_data(revenue_data, period_type, quarter_id, annual_id)
    gross_profit = _get_value_from_quarterly_data(gross_profit_data, period_type, quarter_id, annual_id)
    operating_income = _get_value_from_quarterly_data(operating_income_data, period_type, quarter_id, annual_id)
    net_income = _get_value_from_quarterly_data(net_income_data, period_type, quarter_id, annual_id)
    
    # 如果Gross Profit缺失但Revenue和Cost of Revenue有数据，尝试计算
    cost_of_revenue = None
    if gross_profit is None and revenue is not None:
        cost_of_revenue_data = _get_quarterly_financial_data(company_facts, [
            "CostOfRevenue",
            "CostOfGoodsAndServicesSold"
        ])
        cost_of_revenue = _get_value_from_quarterly_data(cost_of_revenue_data, period_type, quarter_id, annual_id)
        if cost_of_revenue is not None:
            gross_profit = revenue - cost_of_revenue
    
    # EPS - 从 eps_data 中获取当前选中期的值
    eps_basic = None
    eps_diluted = None
    
    if eps_data:
        is_annual_period = period_type == "annual"
        
        if is_annual_period:
            # 年报模式：根据 annual_id 获取对应年份的 EPS
            basic_info = eps_data.get("basic_eps", {})
            diluted_info = eps_data.get("diluted_eps", {})
            
            if annual_id:
                # annual_id 格式如 "FY2025"，提取年份
                target_fy = int(annual_id.replace("FY", ""))
                # 从 annual_list 中查找对应年份
                for a in basic_info.get("annual_list", []):
                    if a.get("fy") == target_fy:
                        eps_basic = a.get("value")
                        break
                for a in diluted_info.get("annual_list", []):
                    if a.get("fy") == target_fy:
                        eps_diluted = a.get("value")
                        break
            else:
                # 未指定年份，取最新年报
                if basic_info.get("latest_annual"):
                    eps_basic = basic_info["latest_annual"].get("value")
                if diluted_info.get("latest_annual"):
                    eps_diluted = diluted_info["latest_annual"].get("value")
        else:
            # 季报模式：根据 quarter_id 找对应季度
            basic_info = eps_data.get("basic_eps", {})
            diluted_info = eps_data.get("diluted_eps", {})
            
            # 在 quarters 列表中查找匹配的季度
            for q in basic_info.get("quarters", []):
                # 匹配 quarter_id (如 "2026Q1") 或 quarter_label (如 "26Q1")
                q_match = False
                if quarter_id:
                    # quarter_id 可能是 "2026Q1" 格式
                    if q.get("quarter") == quarter_id or q.get("quarter_label") == quarter_id:
                        q_match = True
                    # 也可能是 "26Q1" 格式
                    if quarter_id.startswith("20") and q.get("quarter") == quarter_id:
                        q_match = True
                    if not quarter_id.startswith("20") and q.get("quarter_label") == quarter_id:
                        q_match = True
                if q_match:
                    eps_basic = q.get("value")
                    break
            
            for q in diluted_info.get("quarters", []):
                q_match = False
                if quarter_id:
                    if q.get("quarter") == quarter_id or q.get("quarter_label") == quarter_id:
                        q_match = True
                    if quarter_id.startswith("20") and q.get("quarter") == quarter_id:
                        q_match = True
                    if not quarter_id.startswith("20") and q.get("quarter_label") == quarter_id:
                        q_match = True
                if q_match:
                    eps_diluted = q.get("value")
                    break
    
    # ============== B. 资产负债表指标 ==============
    # 使用基于 frame 的新逻辑获取资产负债表数据（时点数据）
    
    # Total Assets
    total_assets_data = _get_balance_sheet_data(company_facts, ["Assets"])
    
    # Total Liabilities
    total_liabilities_data = _get_balance_sheet_data(company_facts, ["Liabilities"])
    
    # Total Equity
    total_equity_data = _get_balance_sheet_data(company_facts, ["StockholdersEquity", "Equity"])
    
    # Current Assets
    current_assets_data = _get_balance_sheet_data(company_facts, ["AssetsCurrent"])
    
    # Current Liabilities
    current_liabilities_data = _get_balance_sheet_data(company_facts, ["LiabilitiesCurrent"])
    
    # Cash & Equivalents
    cash_data = _get_balance_sheet_data(company_facts, [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsAndRestrictedCash",
        "CashAndCashEquivalents"
    ])
    
    # Total Debt
    total_debt_data = _get_balance_sheet_data(company_facts, [
        "LongTermDebt",
        "LongTermDebtNoncurrent",
        "DebtNoncurrent"
    ])
    
    # 根据当前选中的 period 获取对应的值
    def _get_balance_sheet_value(data: Dict, period_type: str,
                                   quarter_id: str, annual_id: str) -> Optional[float]:
        """从资产负债表数据中获取当前选中期的值"""
        if not data:
            return None
        
        if period_type == "annual":
            # 年报模式
            if annual_id:
                target_fy = int(annual_id.replace("FY", ""))
                for a in data.get("annual_list", []):
                    if a.get("fy") == target_fy:
                        return a.get("value")
            else:
                if data.get("latest_annual"):
                    return data["latest_annual"].get("value")
        else:
            # 季报模式
            for p in data.get("periods", []):
                if quarter_id:
                    if p.get("quarter") == quarter_id or p.get("quarter_label") == quarter_id:
                        return p.get("value")
                    if quarter_id.startswith("20") and p.get("quarter") == quarter_id:
                        return p.get("value")
                    if not quarter_id.startswith("20") and p.get("quarter_label") == quarter_id:
                        return p.get("value")
        return None
    
    # 获取当前选中期的指标值
    total_assets = _get_balance_sheet_value(total_assets_data, period_type, quarter_id, annual_id)
    total_liabilities = _get_balance_sheet_value(total_liabilities_data, period_type, quarter_id, annual_id)
    total_equity = _get_balance_sheet_value(total_equity_data, period_type, quarter_id, annual_id)
    current_assets = _get_balance_sheet_value(current_assets_data, period_type, quarter_id, annual_id)
    current_liabilities = _get_balance_sheet_value(current_liabilities_data, period_type, quarter_id, annual_id)
    cash = _get_balance_sheet_value(cash_data, period_type, quarter_id, annual_id)
    total_debt = _get_balance_sheet_value(total_debt_data, period_type, quarter_id, annual_id)
    
    # ============== C. 现金流量表指标 ==============
    # 使用统一的季度计算逻辑获取现金流量指标（与EPS相同逻辑）
    
    # Operating Cash Flow
    operating_cash_flow_data = _get_quarterly_financial_data(company_facts, [
        "NetCashProvidedByUsedInOperatingActivities",
        "CashProvidedByUsedInOperatingActivities"
    ])
    
    # Capital Expenditures
    capex_data = _get_quarterly_financial_data(company_facts, [
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "CapitalExpendituresIncurredButNotYetPaid"
    ])
    
    # 获取当前选中期的指标值
    operating_cash_flow = _get_value_from_quarterly_data(
        operating_cash_flow_data, period_type, quarter_id, annual_id
    )
    capex = _get_value_from_quarterly_data(
        capex_data, period_type, quarter_id, annual_id
    )
    
    # Free Cash Flow = Operating CF - CapEx
    free_cash_flow = None
    if operating_cash_flow is not None and capex is not None:
        free_cash_flow = operating_cash_flow - abs(capex)
    
    # ============== D. 其他重要指标 ==============
    # Shares Outstanding (用于计算每股指标) - 使用与EPS相同的季度计算逻辑
    shares_outstanding_data = _get_quarterly_financial_data(
        company_facts,
        [
            "WeightedAverageNumberOfSharesOutstandingBasic",
            "CommonStockSharesOutstanding",
            "WeightedAverageNumberOfDilutedSharesOutstanding"
        ],
        unit_prefs=["shares", "Shares", "USD", "usd"]
    )
    
    shares_outstanding = _get_value_from_quarterly_data(
        shares_outstanding_data, period_type, quarter_id, annual_id
    )
    
    # 计算利润率
    gross_margin = None
    operating_margin = None
    net_margin = None
    
    if revenue and revenue > 0:
        if gross_profit is not None:
            gross_margin = gross_profit / revenue
        if operating_income is not None:
            operating_margin = operating_income / revenue
        if net_income is not None:
            net_margin = net_income / revenue
    
    # 计算财务健康指标
    current_ratio = None
    debt_to_equity = None
    
    if current_assets is not None and current_liabilities is not None and current_liabilities > 0:
        current_ratio = current_assets / current_liabilities
    
    if total_debt is not None and total_equity is not None and total_equity > 0:
        debt_to_equity = total_debt / total_equity
    
    # ROE, ROA
    roe = None
    roa = None
    
    if net_income is not None:
        if total_equity is not None and total_equity > 0:
            roe = net_income / total_equity
        if total_assets is not None and total_assets > 0:
            roa = net_income / total_assets
    
    # Book Value Per Share
    book_value_per_share = None
    if total_equity is not None and shares_outstanding is not None and shares_outstanding > 0:
        book_value_per_share = total_equity / shares_outstanding
    
    # 获取历史数据用于计算增长率（尝试多个备选概念）
    def _get_historical_with_fallback(concepts: List[str], limit: int = 10) -> List[Dict[str, Any]]:
        """尝试多个概念名称获取历史数据，返回第一个有数据的"""
        for concept in concepts:
            history = _get_historical_values(concept, limit=limit)
            if history and len(history) > 0:
                return history
        return []
    
    # 历史数据转换函数
    def _convert_to_history_format(data: Dict, is_annual: bool = False) -> List[Dict[str, Any]]:
        """将季度/年报数据转换为历史格式用于YoY计算"""
        history = []
        if is_annual:
            # 年报模式：使用 annual_list
            for a in data.get("annual_list", []):
                history.append({
                    "filed": a.get("end_date", ""),
                    "fy": a.get("fy"),
                    "fp": "FY",
                    "val": a.get("value"),
                    "form": a.get("form"),
                })
        else:
            # 季报模式：使用 quarters
            for q in data.get("quarters", []):
                history.append({
                    "filed": q.get("end_date", ""),
                    "fy": q.get("fy"),
                    "fp": q.get("fp"),
                    "val": q.get("value"),
                    "form": q.get("form"),
                })
        return history
    
    is_annual = period_type == "annual"
    revenue_history = _convert_to_history_format(revenue_data, is_annual=is_annual)
    net_income_history = _convert_to_history_format(net_income_data, is_annual=is_annual)
    
    # 计算 YoY 增长率
    def _calc_yoy_growth(
        history: List[Dict[str, Any]], 
        is_quarterly_mode: bool = False,
        target_quarter_id: str = "",
        target_annual_id: str = ""
    ) -> Optional[float]:
        """
        计算 YoY 增长率
        
        年报模式：按 fiscal year 去重，对比最近两年 (FY2024 vs FY2023)
        季报模式：对比上年同期 (2026Q1 vs 2025Q1)
        
        基于当前选中的 quarter_id 或 annual_id 来计算，而不是总是取最新记录
        """
        if len(history) < 2:
            return None
        
        if is_quarterly_mode:
            # 季报模式：找到当前选中的季度和去年同期
            # 从 quarter_id 解析 fy 和 fp
            current_fy = None
            current_fp = None
            
            if target_quarter_id:
                # quarter_id 格式如 "CY2025Q1" 或 "25Q1"
                if target_quarter_id.startswith("CY"):
                    # CY2025Q1 -> fy=2025, fp=Q1
                    match = re.match(r'CY(\d{4})Q([1-4])', target_quarter_id)
                    if match:
                        current_fy = int(match.group(1))
                        current_fp = f"Q{match.group(2)}"
                elif target_quarter_id.startswith("20"):
                    # 2025Q1 -> fy=2025, fp=Q1
                    match = re.match(r'(\d{4})Q([1-4])', target_quarter_id)
                    if match:
                        current_fy = int(match.group(1))
                        current_fp = f"Q{match.group(2)}"
                else:
                    # 25Q1 -> 需要找到对应的完整年份
                    # 从 history 中查找匹配的 quarter_label
                    for item in history:
                        if item.get("quarter_label") == target_quarter_id:
                            current_fy = item.get("fy")
                            current_fp = item.get("fp")
                            break
            
            # 如果没有找到当前选中季度，使用 filed 最新的作为回退
            if not current_fy or not current_fp:
                sorted_items = sorted(history, key=lambda x: str(x.get("filed", "")), reverse=True)
                current_item = sorted_items[0]
                current_fy = current_item.get("fy")
                current_fp = current_item.get("fp")
            
            if not current_fy or not current_fp:
                return None
            
            # 找当前选中期的值
            current_val = None
            for item in history:
                if item.get("fy") == current_fy and item.get("fp") == current_fp:
                    current_val = item.get("val")
                    break
            
            if current_val is None:
                return None
            
            # 找上年同期（相同 fiscal period，前一年）
            previous_fy = current_fy - 1
            previous_val = None
            for item in history:
                if item.get("fy") == previous_fy and item.get("fp") == current_fp:
                    previous_val = item.get("val")
                    break
            
            if previous_val is None or previous_val == 0:
                return None
            
            return (current_val - previous_val) / abs(previous_val)
        else:
            # 年报模式：基于选中的 annual_id 计算 YoY
            target_fy = None
            if target_annual_id and target_annual_id.startswith("FY"):
                target_fy = int(target_annual_id.replace("FY", ""))
            
            # 按 fiscal year 去重
            fy_map = {}
            for item in history:
                fy = item.get("fy")
                if fy and fy not in fy_map:
                    fy_map[fy] = item.get("val")
            
            if target_fy:
                # 计算选中年的 YoY
                current = fy_map.get(target_fy)
                previous = fy_map.get(target_fy - 1)
                
                if current is None or previous is None or previous == 0:
                    return None
                
                return (current - previous) / abs(previous)
            else:
                # 没有选中年报，对比最近两年
                fys = sorted(fy_map.keys(), reverse=True)
                if len(fys) < 2:
                    return None
                
                current = fy_map.get(fys[0])
                previous = fy_map.get(fys[1])
                
                if current is None or previous is None or previous == 0:
                    return None
                
                return (current - previous) / abs(previous)
    
    is_annual = period_type == "annual"
    revenue_yoy = _calc_yoy_growth(
        revenue_history, 
        is_quarterly_mode=not is_annual,
        target_quarter_id=quarter_id,
        target_annual_id=annual_id
    )
    net_income_yoy = _calc_yoy_growth(
        net_income_history, 
        is_quarterly_mode=not is_annual,
        target_quarter_id=quarter_id,
        target_annual_id=annual_id
    )
    
    return {
        "ok": True,
        "profitability": {
            "revenue": revenue,
            "gross_profit": gross_profit,
            "operating_income": operating_income,
            "net_income": net_income,
            "eps_basic": eps_basic,
            "eps_diluted": eps_diluted,
            "gross_margin": gross_margin,
            "operating_margin": operating_margin,
            "net_margin": net_margin,
        },
        "balance_sheet": {
            "total_assets": total_assets,
            "total_liabilities": total_liabilities,
            "total_equity": total_equity,
            "current_assets": current_assets,
            "current_liabilities": current_liabilities,
            "cash_and_equivalents": cash,
            "total_debt": total_debt,
            "current_ratio": current_ratio,
            "debt_to_equity": debt_to_equity,
        },
        "cash_flow": {
            "operating_cash_flow": operating_cash_flow,
            "capital_expenditures": capex,
            "free_cash_flow": free_cash_flow,
        },
        "other_metrics": {
            "roe": roe,
            "roa": roa,
            "shares_outstanding": shares_outstanding,
            "book_value_per_share": book_value_per_share,
            "revenue_yoy_growth": revenue_yoy,
            "net_income_yoy_growth": net_income_yoy,
        },
        "raw_data_available": {
            "revenue_count": len(revenue_history),
            "net_income_count": len(net_income_history),
        },
    }


def _get_quarterly_eps_direct(company_facts: Dict[str, Any], concept: str) -> List[Dict[str, Any]]:
    """
    优先根据10-Q/10-Q/A表单直接获取季度EPS数据
    按照用户指定的逻辑：
    1. 过滤有效记录（form in 10-Q, 10-Q/A, start/end/val not null）
    2. 计算期间长度，只保留80-101天的季度数据
    3. 转换为自然季度（CYYYYYQX）
    4. 按CIK+calendar_quarter分组去重
    5. 选择最终记录（filed最新，duration最接近91天）
    """
    from datetime import datetime
    from collections import defaultdict
    
    facts = company_facts.get("facts", {})
    if not isinstance(facts, dict):
        return []
    
    gaap = facts.get("us-gaap", {})
    ifrs = facts.get("ifrs-full", {})
    data = {**ifrs, **gaap}
    
    concept_data = data.get(concept)
    if not isinstance(concept_data, dict):
        return []
    
    units = concept_data.get("units", {})
    unit_data = units.get("USD/shares") or units.get("usd/shares") or units.get("USD") or units.get("usd")
    if not isinstance(unit_data, list):
        return []
    
    # STEP 1: 过滤有效记录
    valid_forms = {"10-Q", "10-Q/A"}
    valid_records = []
    
    for item in unit_data:
        form = str(item.get("form", ""))
        start = item.get("start", "")
        end = item.get("end", "")
        val = item.get("val")
        
        # 保留条件检查
        if form not in valid_forms:
            continue
        if not start or not end or val is None:
            continue
        
        # STEP 2: 计算期间长度
        try:
            start_dt = datetime.strptime(start, "%Y-%m-%d")
            end_dt = datetime.strptime(end, "%Y-%m-%d")
            duration_days = (end_dt - start_dt).days
        except:
            continue
        
        # STEP 3: 只保留季度EPS（80-101天）
        if not (80 <= duration_days <= 101):
            continue
        
        # STEP 4: 转换为自然季度
        try:
            month = end_dt.month
            year = end_dt.year
            if month <= 3:
                calendar_quarter = f"{year}Q1"
            elif month <= 6:
                calendar_quarter = f"{year}Q2"
            elif month <= 9:
                calendar_quarter = f"{year}Q3"
            else:
                calendar_quarter = f"{year}Q4"
        except:
            continue
        
        filed = str(item.get("filed", ""))
        
        valid_records.append({
            "value": float(val),
            "start": start,
            "end": end,
            "filed": filed,
            "duration_days": duration_days,
            "calendar_quarter": calendar_quarter,
            "form": form,
            "frame": item.get("frame", ""),
        })
    
    # STEP 5: 按calendar_quarter分组去重
    quarter_groups = defaultdict(list)
    for record in valid_records:
        quarter_groups[record["calendar_quarter"]].append(record)
    
    # STEP 6: 选择最终记录（排序：filed最新，duration最接近91天）
    def _duration_score(days):
        """计算duration与91天的接近程度，越小越接近"""
        return abs(days - 91)
    
    final_quarters = []
    for quarter, records in quarter_groups.items():
        if not records:
            continue
        
        # 排序优先级：1. filed最新 2. duration最接近91天
        records.sort(key=lambda x: (x["filed"], -_duration_score(x["duration_days"])), reverse=True)
        best_record = records[0]
        
        # 生成quarter_id
        year = quarter[:4]
        q = quarter[4:]
        q_id = f"CY{quarter}"
        quarter_label = f"{year[-2:]}{q}"
        
        # 从calendar_quarter解析年份和季度
        cal_year = int(quarter[:4])
        cal_q = quarter[4:]
        
        final_quarters.append({
            "quarter": q_id,
            "quarter_label": quarter_label,
            "form": best_record["form"],
            "value": best_record["value"],
            "end_date": best_record["end"],
            "start_date": best_record["start"],
            "frame": best_record["frame"],
            "calendar_quarter": quarter,
            "filed": best_record["filed"],
            "duration_days": best_record["duration_days"],
            "is_calculated": False,
            "is_direct_quarterly": True,  # 标记为直接从10-Q获取的季度数据
            "fy": cal_year,  # 财年使用日历年
            "fp": cal_q,     # 财季使用日历年季度
        })
    
    # 按end_date倒序排序
    final_quarters.sort(key=lambda x: x["end_date"], reverse=True)
    return final_quarters


def _get_balance_sheet_data(
    company_facts: Dict[str, Any], 
    concepts: List[str], 
    unit_prefs: List[str] = None
) -> Dict[str, Any]:
    """
    获取资产负债表数据（基于 frame 字段）
    
    逻辑：
    1. 根据 frame 字段识别周期（CY2024Q1I → 2024Q1）
    2. 正则匹配：^CY(\d{4})(Q([1-4]))?(I)?$
    3. 提取 year 和 quarter，转换为自然季度/年度
    4. 去重：以 (cik, period) 为 key
    5. 保留规则：优先 form=10-Q/10-K，再按 filed 最新
    """
    import re
    from collections import defaultdict
    
    facts = company_facts.get("facts", {})
    if not isinstance(facts, dict):
        return {"periods": [], "annual_list": [], "latest_annual": None}
    
    gaap = facts.get("us-gaap", {})
    ifrs = facts.get("ifrs-full", {})
    data = {**ifrs, **gaap}
    
    unit_prefs = unit_prefs or ["USD", "usd"]
    
    # 正则匹配 frame 字段
    frame_pattern = re.compile(r'^CY(\d{4})(Q([1-4]))?(I)?$')
    
    def _parse_frame(frame: str) -> tuple:
        """解析 frame 字段，返回 (year, quarter, is_instant)"""
        if not frame:
            return None, None, False
        match = frame_pattern.match(frame)
        if not match:
            return None, None, False
        year = match.group(1)
        quarter = match.group(3)
        is_instant = match.group(4) == 'I'
        return year, quarter, is_instant
    
    # 尝试所有概念，找到有数据的
    matched_unit_data = None
    
    for concept in concepts:
        concept_data = data.get(concept)
        if not isinstance(concept_data, dict):
            continue
        
        units = concept_data.get("units", {})
        unit_data = None
        for unit in unit_prefs:
            unit_data = units.get(unit)
            if isinstance(unit_data, list):
                break
        
        if unit_data:
            matched_unit_data = unit_data
            break
    
    if not matched_unit_data:
        return {"periods": [], "annual_list": [], "latest_annual": None}
    
    # 按 period 分组收集数据
    period_groups = defaultdict(list)
    
    for item in matched_unit_data:
        frame = item.get("frame", "")
        year, quarter, is_instant = _parse_frame(frame)
        
        if not year:
            continue
        
        # 构建 period 标识
        if quarter:
            period = f"{year}Q{quarter}"
            period_type = "quarterly"
            fp = f"Q{quarter}"
        else:
            period = year
            period_type = "annual"
            fp = "FY"
        
        form = str(item.get("form", ""))
        filed = str(item.get("filed", ""))
        end_date = item.get("end", "")
        val = item.get("val")
        
        if val is None:
            continue
        
        period_groups[period].append({
            "value": float(val),
            "form": form,
            "filed": filed,
            "end_date": end_date,
            "frame": frame,
            "period": period,
            "period_type": period_type,
            "fy": int(year),
            "fp": fp,
            "is_instant": is_instant,
        })
    
    # 去重：每个 period 只保留一条记录
    # 优先级：1. form (10-Q/10-K 优先) 2. filed 最新
    form_priority = {"10-Q": 3, "10-K": 3, "10-Q/A": 2, "10-K/A": 2}
    
    final_periods = []
    annual_list = []
    
    for period, records in period_groups.items():
        if not records:
            continue
        
        # 排序：form 优先级高 -> filed 最新
        records.sort(
            key=lambda x: (
                form_priority.get(x["form"], 0),
                x["filed"]
            ),
            reverse=True
        )
        best = records[0]
        
        # 构建返回格式
        period_info = {
            "period": best["period"],
            "period_type": best["period_type"],
            "form": best["form"],
            "value": best["value"],
            "end_date": best["end_date"],
            "frame": best["frame"],
            "filed": best["filed"],
            "fy": best["fy"],
            "fp": best["fp"],
            "is_instant": best["is_instant"],
        }
        
        # 区分季度和年报
        if best["period_type"] == "quarterly":
            quarter_label = f"{str(best['fy'])[-2:]}{best['fp']}"
            period_info["quarter"] = f"CY{best['period']}"
            period_info["quarter_label"] = quarter_label
            final_periods.append(period_info)
        else:
            period_info["annual_id"] = f"FY{best['fy']}"
            annual_list.append(period_info)
    
    # 排序：按 end_date 倒序
    final_periods.sort(key=lambda x: x["end_date"], reverse=True)
    annual_list.sort(key=lambda x: x["end_date"], reverse=True)
    
    latest_annual = annual_list[0] if annual_list else None
    
    return {
        "periods": final_periods[:8],  # 最近8个季度
        "annual_list": annual_list[:2],  # 最近2年年报
        "latest_annual": latest_annual,
    }


def _get_quarterly_financial_data(
    company_facts: Dict[str, Any], 
    concepts: List[str], 
    unit_prefs: List[str] = None
) -> Dict[str, Any]:
    """
    通用函数：获取季度财务数据（Revenue, GrossProfit, OperatingIncome, NetIncome等）
    使用与EPS相同的优先级逻辑：
    1. 优先从10-Q/10-Q/A直接获取（80-101天期间的记录）
    2. 用年报推导缺失季度
    
    Args:
        company_facts: SEC公司facts数据
        concepts: 概念名称列表（按优先级），如["Revenues", "TotalRevenues"]
        unit_prefs: 单位优先级列表，如["USD", "usd"]
    
    Returns:
        {"quarters": [...], "annual_list": [...], "latest_annual": {...}}
    """
    from datetime import datetime
    from collections import defaultdict
    
    facts = company_facts.get("facts", {})
    if not isinstance(facts, dict):
        return {"quarters": [], "annual_list": [], "latest_annual": None}
    
    gaap = facts.get("us-gaap", {})
    ifrs = facts.get("ifrs-full", {})
    data = {**ifrs, **gaap}
    
    unit_prefs = unit_prefs or ["USD", "usd"]
    
    def _get_concept_data(concept: str) -> tuple:
        """获取某个概念的所有原始数据"""
        concept_data = data.get(concept)
        if not isinstance(concept_data, dict):
            return None, None
        
        units = concept_data.get("units", {})
        unit_data = None
        for unit in unit_prefs:
            unit_data = units.get(unit)
            if isinstance(unit_data, list):
                break
        
        return concept_data, unit_data
    
    def _get_natural_quarter_from_date(end_date: str) -> tuple:
        """根据 end_date 返回 (年份, 季度)"""
        if not end_date or len(end_date) < 7:
            return None, None
        try:
            year = int(end_date[:4])
            month = int(end_date[5:7])
            if 1 <= month <= 3:
                return year, "Q1"
            elif 4 <= month <= 6:
                return year, "Q2"
            elif 7 <= month <= 9:
                return year, "Q3"
            elif 10 <= month <= 12:
                return year, "Q4"
        except:
            pass
        return None, None
    
    # 步骤1：尝试所有概念，找到有数据的
    all_annual_items = []
    all_quarter_items = []
    matched_concept = None
    matched_unit_data = None
    
    for concept in concepts:
        concept_data, unit_data = _get_concept_data(concept)
        if unit_data:
            matched_concept = concept
            matched_unit_data = unit_data
            
            # 分离年报和季报
            for item in unit_data:
                form = str(item.get("form", ""))
                fp = str(item.get("fp", "")).upper()
                fy = item.get("fy")
                val = item.get("val")
                
                if val is None or not fy:
                    continue
                
                record = {
                    "fy": fy,
                    "fp": fp,
                    "form": form,
                    "value": float(val),
                    "end_date": item.get("end", ""),
                    "start_date": item.get("start", ""),
                    "frame": item.get("frame", ""),
                    "filed": str(item.get("filed", "")),
                }
                
                if form == "10-K" or fp == "FY":
                    all_annual_items.append(record)
                elif form in ["10-Q", "10-Q/A"] and fp in ["Q1", "Q2", "Q3", "Q4"]:
                    all_quarter_items.append(record)
            
            break  # 找到第一个有数据的概念就停止
    
    if not matched_concept:
        return {"quarters": [], "annual_list": [], "latest_annual": None}
    
    # 步骤2：尝试从10-Q直接获取季度数据（80-101天）
    valid_forms = {"10-Q", "10-Q/A"}
    direct_quarters = []
    
    for item in matched_unit_data:
        form = str(item.get("form", ""))
        start = item.get("start", "")
        end = item.get("end", "")
        val = item.get("val")
        
        if form not in valid_forms or not start or not end or val is None:
            continue
        
        try:
            start_dt = datetime.strptime(start, "%Y-%m-%d")
            end_dt = datetime.strptime(end, "%Y-%m-%d")
            duration_days = (end_dt - start_dt).days
        except:
            continue
        
        if not (80 <= duration_days <= 101):
            continue
        
        # 转换为自然季度
        month = end_dt.month
        year = end_dt.year
        if month <= 3:
            calendar_quarter = f"{year}Q1"
        elif month <= 6:
            calendar_quarter = f"{year}Q2"
        elif month <= 9:
            calendar_quarter = f"{year}Q3"
        else:
            calendar_quarter = f"{year}Q4"
        
        direct_quarters.append({
            "value": float(val),
            "start": start,
            "end": end,
            "filed": str(item.get("filed", "")),
            "duration_days": duration_days,
            "calendar_quarter": calendar_quarter,
            "form": form,
            "frame": item.get("frame", ""),
        })
    
    # 按calendar_quarter分组去重，选择最佳记录
    if direct_quarters:
        quarter_groups = defaultdict(list)
        for record in direct_quarters:
            quarter_groups[record["calendar_quarter"]].append(record)
        
        def _duration_score(days):
            return abs(days - 91)
        
        final_direct_quarters = []
        for quarter, records in quarter_groups.items():
            if not records:
                continue
            records.sort(key=lambda x: (x["filed"], -_duration_score(x["duration_days"])), reverse=True)
            best = records[0]
            
            year = quarter[:4]
            q = quarter[4:]
            cal_year = int(year)
            
            final_direct_quarters.append({
                "quarter": f"CY{quarter}",
                "quarter_label": f"{year[-2:]}{q}",
                "form": best["form"],
                "value": best["value"],
                "end_date": best["end"],
                "start_date": best["start"],
                "frame": best["frame"],
                "calendar_quarter": quarter,
                "filed": best["filed"],
                "duration_days": best["duration_days"],
                "is_calculated": False,
                "is_direct_quarterly": True,
                "fy": cal_year,
                "fp": q,
            })
        
        direct_quarters = final_direct_quarters
    
    # 步骤3：获取年报列表
    annual_list = []
    seen_annual_fy = set()
    annual_items_sorted = sorted(all_annual_items, key=lambda x: x.get("end_date", ""), reverse=True)
    for item in annual_items_sorted:
        fy = item["fy"]
        if fy not in seen_annual_fy:
            seen_annual_fy.add(fy)
            annual_list.append(item)
        if len(annual_list) >= 2:
            break
    
    latest_annual = annual_list[0] if annual_list else None
    
    # 步骤4：用年报推导缺失季度
    year_data = defaultdict(lambda: {"quarters": {}, "annual": None})
    
    for q in direct_quarters:
        cq = q.get("calendar_quarter", "")
        if cq and len(cq) >= 6:
            year = int(cq[:4])
            quarter = cq[4:]
            year_data[year]["quarters"][quarter] = q
    
    for annual in annual_list:
        year, _ = _get_natural_quarter_from_date(annual["end_date"])
        if year:
            year_data[year]["annual"] = annual
    
    # 推导缺失季度
    quarters = []
    for year in sorted(year_data.keys(), reverse=True):
        data_year = year_data[year]
        annual = data_year["annual"]
        qs = data_year["quarters"]
        
        missing_q = None
        known_qs = []
        for q in ["Q1", "Q2", "Q3", "Q4"]:
            if q in qs:
                known_qs.append(qs[q])
            else:
                missing_q = q
        
        # 有年报且恰好缺失1个季度，推导
        if annual and missing_q and len(known_qs) == 3:
            derived_value = round(annual["value"] - sum(q["value"] for q in known_qs), 2)
            if missing_q == "Q1":
                end_date = f"{year}-03-31"
            elif missing_q == "Q2":
                end_date = f"{year}-06-30"
            elif missing_q == "Q3":
                end_date = f"{year}-09-30"
            else:
                end_date = f"{year}-12-31"
            
            known_qs.append({
                "quarter": f"CY{year}{missing_q}",
                "quarter_label": f"{str(year)[-2:]}{missing_q}",
                "value": derived_value,
                "end_date": end_date,
                "start_date": "",
                "fy": annual["fy"],
                "fp": missing_q,
                "form": annual["form"],
                "is_calculated": True,
                "is_derived": True,
            })
        
        for q in known_qs:
            quarters.append({
                "quarter": q["quarter"],
                "quarter_label": q["quarter_label"],
                "form": q.get("form", ""),
                "value": q["value"],
                "end_date": q["end_date"],
                "start_date": q.get("start_date", ""),
                "fy": q.get("fy", year),
                "fp": q.get("fp", ""),
                "is_calculated": q.get("is_calculated", False),
                "is_derived": q.get("is_derived", False),
            })
    
    quarters.sort(key=lambda x: x["end_date"], reverse=True)
    quarters = quarters[:8]
    
    return {
        "quarters": quarters,
        "annual_list": annual_list,
        "latest_annual": latest_annual,
        "concept": matched_concept,
    }


def _extract_eps_data(company_facts: Dict[str, Any]) -> Dict[str, Any]:
    """
    提取 EPS 数据（Basic 和 Diluted）
    返回最近年报和最近4个季度的结构化数据
    
    季度EPS获取优先级：
    1. 优先从10-Q/10-Q/A直接获取（80-101天期间的记录）
    2. 如果没有找到，则使用现有的累计值计算逻辑
    """
    facts = company_facts.get("facts", {})
    if not isinstance(facts, dict):
        return {}
    
    gaap = facts.get("us-gaap", {})
    ifrs = facts.get("ifrs-full", {})
    data = {**ifrs, **gaap}
    
    def _get_eps_values(concept: str) -> List[Dict[str, Any]]:
        """获取某个 EPS 概念的所有历史值"""
        concept_data = data.get(concept)
        if not isinstance(concept_data, dict):
            return []
        
        units = concept_data.get("units", {})
        
        # DEBUG: 查看可用的单位类型
        import logging
        logging.info(f"_get_eps_values for {concept}: available units = {list(units.keys())}")
        
        # EPS 单位通常是 USD/shares 或 USD
        unit_data = units.get("USD/shares") or units.get("usd/shares") or units.get("USD") or units.get("usd")
        if not isinstance(unit_data, list):
            logging.info(f"_get_eps_values for {concept}: unit_data not found or not a list")
            return []
        
        logging.info(f"_get_eps_values for {concept}: unit_data has {len(unit_data)} items")
        
        results = []
        seen = set()
        for item in unit_data:
            form = str(item.get("form", ""))
            fp = str(item.get("fp", "")).upper()
            fy = item.get("fy")
            end = item.get("end", "")
            start = item.get("start", "")
            val = item.get("val")
            frame = item.get("frame", "")  # 如 CY2026Q1
            filed = str(item.get("filed", ""))
            
            if val is None or not fy:
                continue
            
            # 生成唯一标识：fy + fp + end_date
            key = f"{fy}_{fp}_{end}"
            if key in seen:
                continue
            seen.add(key)
            
            results.append({
                "fy": fy,
                "fp": fp,
                "form": form,
                "value": float(val),
                "end_date": end,
                "start_date": start,
                "frame": frame,
                "filed": filed,
            })
        
        # 按 end_date 降序排序，获取最近季度
        results.sort(key=lambda x: x.get("end_date", ""), reverse=True)
        
        # DEBUG: 打印前15条数据查看
        logging.info(f"_get_eps_values for {concept}: got {len(results)} items after dedup")
        for i, r in enumerate(results[:15]):
            logging.info(f"  [{i}] fy={r['fy']}, fp={r['fp']}, end={r['end_date']}, frame={r['frame']}, form={r['form']}")
        
        return results
    
    def _is_single_quarter(record: Dict[str, Any]) -> bool:
        """通过日期差判断是否是单季度数据（75-105天）"""
        start = record.get("start_date", "")
        end = record.get("end_date", "")
        if not start or not end:
            return False
        try:
            from datetime import datetime
            start_dt = datetime.strptime(start, "%Y-%m-%d")
            end_dt = datetime.strptime(end, "%Y-%m-%d")
            days = (end_dt - start_dt).days
            return 75 <= days <= 105
        except:
            return False
    
    def _process_eps(concept: str) -> Dict[str, Any]:
        """处理单个 EPS 概念，返回结构化数据"""
        all_values = _get_eps_values(concept)
        if not all_values:
            return {}
        
        # 分离年报和季报
        # 年报：form=10-K 或 fp=FY
        annual_items = []
        quarter_items = []
        
        for x in all_values:
            if x["form"] == "10-K" or x["fp"] == "FY":
                annual_items.append(x)
            elif x["form"] == "10-Q" and x["fp"] in ["Q1", "Q2", "Q3", "Q4"]:
                # 接受所有 10-Q 数据（包括累计数据）
                quarter_items.append(x)
        
        # ========== 优先尝试新的直接季度EPS获取逻辑 ==========
        # 步骤1：先尝试从10-Q/10-Q/A直接获取季度EPS（80-101天期间的记录）
        direct_quarters = _get_quarterly_eps_direct(company_facts, concept)
        
        # 步骤2：获取年报数据（用于后续推导和返回）
        def _get_natural_quarter_from_date(end_date: str) -> tuple:
            """根据 end_date 返回 (年份, 季度)"""
            if not end_date or len(end_date) < 7:
                return None, None
            try:
                year = int(end_date[:4])
                month = int(end_date[5:7])
                if 1 <= month <= 3:
                    return year, "Q1"
                elif 4 <= month <= 6:
                    return year, "Q2"
                elif 7 <= month <= 9:
                    return year, "Q3"
                elif 10 <= month <= 12:
                    return year, "Q4"
            except:
                pass
            return None, None
        
        # 获取最近2年的年报
        annual_list = []
        seen_annual_fy = set()
        annual_items_sorted = sorted(annual_items, key=lambda x: x.get("end_date", ""), reverse=True)
        for item in annual_items_sorted:
            fy = item["fy"]
            if fy not in seen_annual_fy:
                seen_annual_fy.add(fy)
                annual_list.append({
                    "fy": fy,
                    "fp": "FY",
                    "form": item["form"],
                    "value": item["value"],
                    "end_date": item["end_date"],
                    "start_date": item.get("start_date", ""),
                    "frame": item.get("frame", ""),
                    "filed": item.get("filed", ""),
                })
            if len(annual_list) >= 2:
                break
        
        latest_annual = annual_list[0] if annual_list else None
        
        # 步骤3：尝试用年报推导缺失季度
        # 按日历年分组，关联年报和直接获取的季度
        from collections import defaultdict
        year_data = defaultdict(lambda: {"quarters": {}, "annual": None})
        
        # 将直接获取的季度按日历年分组
        for q in direct_quarters:
            cq = q.get("calendar_quarter", "")
            if cq and len(cq) >= 6:
                year = int(cq[:4])
                quarter = cq[4:]
                year_data[year]["quarters"][quarter] = q
        
        # 关联年报
        for annual in annual_list:
            year, _ = _get_natural_quarter_from_date(annual["end_date"])
            if year:
                year_data[year]["annual"] = annual
        
        # 推导缺失季度: 缺失季度 = 年报 - 其他3个季度之和
        quarters = []
        for year in sorted(year_data.keys(), reverse=True):
            data = year_data[year]
            annual = data["annual"]
            qs = data["quarters"]
            
            # 检查哪些季度缺失
            missing_q = None
            known_qs = []
            for q in ["Q1", "Q2", "Q3", "Q4"]:
                if q in qs:
                    known_qs.append(qs[q])
                else:
                    missing_q = q
            
            # 如果有年报且恰好缺失1个季度，推导缺失季度
            if annual and missing_q and len(known_qs) == 3:
                derived_value = round(annual["value"] - sum(q["value"] for q in known_qs), 2)
                # 确定缺失季度的end_date
                if missing_q == "Q1":
                    end_date = f"{year}-03-31"
                elif missing_q == "Q2":
                    end_date = f"{year}-06-30"
                elif missing_q == "Q3":
                    end_date = f"{year}-09-30"
                else:
                    end_date = f"{year}-12-31"
                
                known_qs.append({
                    "quarter": f"CY{year}{missing_q}",
                    "quarter_label": f"{str(year)[-2:]}{missing_q}",
                    "value": derived_value,
                    "end_date": end_date,
                    "start_date": "",
                    "fy": annual["fy"],
                    "fp": missing_q,
                    "form": annual["form"],
                    "is_calculated": True,
                    "is_derived": True,
                })
            
            # 添加所有可用季度（只有直接获取或成功推导的）
            for q in known_qs:
                quarters.append({
                    "quarter": q["quarter"],
                    "quarter_label": q["quarter_label"],
                    "form": q.get("form", ""),
                    "value": q["value"],
                    "end_date": q["end_date"],
                    "start_date": q.get("start_date", ""),
                    "fy": q.get("fy", year),
                    "fp": q.get("fp", ""),
                    "is_calculated": q.get("is_calculated", False),
                    "is_derived": q.get("is_derived", False),
                })
        
        # 按 end_date 倒序排序，取最近8个季度
        quarters.sort(key=lambda x: x["end_date"], reverse=True)
        quarters = quarters[:8]
        
        # 计算 TTM（最近4个季度总和）
        ttm = None
        if len(quarters) >= 4:
            ttm = sum(q["value"] for q in quarters[:4])
        
        return {
            "ttm": ttm,
            "latest_annual": latest_annual,
            "annual_list": annual_list,  # 最近2年的年报列表
            "quarters": quarters,
            "source": "calculated",  # 标记数据来源为计算值
        }
    
    # 尝试多个 EPS 概念名称
    eps_basic_concepts = ["EarningsPerShareBasic", "EarningsPerShareBasicAndDiluted"]
    eps_diluted_concepts = ["EarningsPerShareDiluted", "EarningsPerShareBasicAndDiluted"]
    
    # DEBUG: 收集所有原始 EPS 数据
    all_eps_raw_data = {}
    for concept in eps_basic_concepts + eps_diluted_concepts:
        raw = _get_eps_values(concept)
        if raw:
            all_eps_raw_data[concept] = raw[:10]  # 取前10条
    
    basic_eps = {}
    for concept in eps_basic_concepts:
        basic_eps = _process_eps(concept)
        if basic_eps:
            break
    
    diluted_eps = {}
    for concept in eps_diluted_concepts:
        diluted_eps = _process_eps(concept)
        if diluted_eps:
            break
    
    # 从 EPS 数据生成 available_quarters（确保与 EPS quarters 一致，取前8个）
    available_quarters = []
    eps_quarters = (basic_eps.get("quarters", []) or diluted_eps.get("quarters", []))[:8]
    for q in eps_quarters:
        available_quarters.append({
            "id": q["quarter"],
            "label": q["quarter_label"],
            "fy": q["fy"],
            "fp": q["fp"],
            "frame": q.get("frame", ""),
            "end_date": q["end_date"],
        })
    
    # 从 EPS 数据生成 available_annuals（年报按钮，最近2年）
    available_annuals = []
    eps_annuals = basic_eps.get("annual_list", []) or diluted_eps.get("annual_list", [])
    for a in eps_annuals[:2]:
        available_annuals.append({
            "id": f"FY{a['fy']}",
            "label": str(a["fy"]),
            "fy": a["fy"],
            "fp": "FY",
            "frame": a.get("frame", ""),
            "end_date": a["end_date"],
        })
    
    return {
        "basic_eps": basic_eps,
        "diluted_eps": diluted_eps,
        "available_quarters": available_quarters,
        "available_annuals": available_annuals,
        "_debug_all_eps_raw": all_eps_raw_data,
    }


def api_smartmoney_stock_financials(
    cik: str = Query("", alias="cik"),
    ticker: str = Query("", alias="ticker"),
    period_type: str = Query("annual", alias="period"),
    quarter_id: str = Query("", alias="quarter"),
    annual_id: str = Query("", alias="annual"),
) -> JSONResponse:
    """
    SEC EDGAR 公司财报分析接口
    
    通过 CIK 或 Ticker 获取公司的财务数据：
    - 盈利能力指标（收入、利润、利润率、EPS）
    - 资产负债表指标（资产、负债、权益、财务比率）
    - 现金流量表指标（经营现金流、自由现金流）
    - 其他重要指标（ROE、ROA、YoY增长率）
    
    Args:
        cik: 公司 CIK (如 0000320193)
        ticker: 股票代码 (如 AAPL)，会通过映射转换为 CIK
        period_type: 财报周期类型，"annual" (年报 10-K) 或 "quarterly" (季报 10-Q)
        quarter_id: 具体季度ID，如 "2026Q1"、"2025Q4"，用于获取该季度的数据
        annual_id: 具体年报ID，如 "FY2025"、"FY2024"，用于获取该年度的数据
    
    Returns:
        JSONResponse 包含结构化财务数据
    """
    # 优先使用 CIK，如果没有则尝试通过 ticker 转换
    target_cik = (cik or "").strip()
    
    if not target_cik and ticker:
        # 尝试通过 CUSIP 映射找到 CIK
        cusip = re.sub(r"\s+", "", (ticker or "").strip().upper())
        target_cik = _CUSIP_CIK_MAP.get(cusip, "")
    
    if not target_cik:
        return JSONResponse(
            {"ok": False, "error": "missing_cik_or_ticker"},
            status_code=400
        )
    
    # 验证 period_type 参数
    is_quarterly = period_type.lower() == "quarterly"
    
    try:
        # 获取公司财务数据
        company_facts = _sec_get_company_facts(target_cik)
        
        if company_facts is None:
            return JSONResponse({
                "ok": False,
                "error": "sec_api_error",
                "message": "无法从 SEC EDGAR API 获取财务数据，请稍后重试"
            }, status_code=503)
        
        # 先提取 EPS 数据（供后续财务指标提取使用）
        eps_data = _extract_eps_data(company_facts)
        
        # 提取关键财务指标（根据 period_type 和 quarter_id 筛选）
        financials = _extract_financial_metrics(
            company_facts, 
            period_type="quarterly" if is_quarterly else "annual",
            quarter_id=quarter_id if is_quarterly else "",
            annual_id=annual_id if not is_quarterly else "",
            eps_data=eps_data
        )
        
        if not financials.get("ok"):
            return JSONResponse({
                "ok": False,
                "error": financials.get("error", "data_extraction_failed"),
                "message": "无法解析财务数据，可能该公司暂无 XBRL 数据"
            }, status_code=404)
        
        # 获取可用季度列表（最近4个）- 无论年报季报模式都返回，供前端显示季度切换按钮
        facts = company_facts.get("facts", {})
        gaap = facts.get("us-gaap", {})
        ifrs = facts.get("ifrs-full", {})
        data = {**ifrs, **gaap}
        
        def _get_available_quarters(concept: str, limit: int = 4):
            """获取可用的季度列表"""
            concept_data = data.get(concept)
            if not isinstance(concept_data, dict):
                return []
            units = concept_data.get("units", {})
            # 支持多种单位
            unit_data = units.get("USD") or units.get("usd") or units.get("shares") or units.get("Shares")
            if not isinstance(unit_data, list):
                return []
            
            seen = set()
            results = []
            for item in sorted(unit_data, key=lambda x: x.get("end", ""), reverse=True):
                form = str(item.get("form", ""))
                fp = str(item.get("fp", "")).upper()
                fy = item.get("fy")
                frame = item.get("frame", "")
                end = item.get("end", "")
                # 只取季报数据
                if form != "10-Q" and fp not in ["Q1", "Q2", "Q3", "Q4"]:
                    continue
                if not fy or not fp:
                    continue
                
                # 优先使用 frame 作为季度ID，回退到 fy+fp
                if frame and frame.startswith("CY") and len(frame) >= 6:
                    quarter_key = frame  # 如 CY2026Q1
                    # 从 frame 解析季度标签: CY2026Q1 -> 26Q1
                    year = frame[2:6]  # 2026
                    q = frame[6:] if len(frame) > 6 else ""  # Q1
                    label = f"{year[-2:]}{q}"  # 26Q1
                else:
                    quarter_key = f"{fy}{fp}"  # 如 2026Q1
                    label = f"{str(fy)[-2:]}{fp}"  # 26Q1
                
                if quarter_key in seen:
                    continue
                seen.add(quarter_key)
                
                filed = str(item.get("filed", ""))
                results.append({
                    "id": quarter_key,  # 如 CY2026Q1 或 2026Q1
                    "fy": fy,
                    "fp": fp,
                    "frame": frame,
                    "filed": filed,
                    "end_date": end,
                    "label": label,  # 如 "26Q1"
                })
                if len(results) >= limit:
                    break
            return results
        
        # 使用 EPS 数据中的 available_quarters 和 available_annuals
        available_quarters = eps_data.get("available_quarters", [])
        available_annuals = eps_data.get("available_annuals", [])
        
        # 添加元数据
        result = {
            "ok": True,
            "cik": target_cik,
            "ticker": ticker if ticker else None,
            "period_type": "quarterly" if is_quarterly else "annual",
            "data_source": "sec_edgar",
            "fiscal_data_available": financials.get("raw_data_available"),
            "eps_data": eps_data,
            **{k: v for k, v in financials.items() if k != "ok" and k != "raw_data_available"},
        }
        
        # 季报模式添加可用季度列表
        if available_quarters:
            result["available_quarters"] = available_quarters
        
        # 年报模式添加可用年报列表
        if available_annuals:
            result["available_annuals"] = available_annuals
        
        return JSONResponse(result)
        
    except Exception as e:
        return JSONResponse({
            "ok": False,
            "error": "internal_error",
            "message": str(e)
        }, status_code=500)


# CUSIP 到 CIK 的映射表（用于 SEC EDGAR 财报查询）
_CUSIP_CIK_MAP: Dict[str, str] = {
    "037833100": "0000320193",  # AAPL
    "594918104": "0000789019",  # MSFT
    "023135106": "0001018724",  # AMZN
    "02079K305": "0001652044",  # GOOGL
    "02079K107": "0001652044",  # GOOG
    "30303M102": "0001326801",  # META
    "67066G104": "0001013484",  # NVDA
    "88160R101": "0001318605",  # TSLA
    "46625H100": "0000019617",  # JPM
    "92826C839": "0001403161",  # V
    "478160104": "0000200406",  # JNJ
    "931142103": "0000104169",  # WMT
    "742718109": "0000080424",  # PG
    "91324P102": "0000103310",  # UNH
    "060505104": "0000070858",  # BAC
    "191216100": "0000021344",  # KO
    "949746101": "0000072971",  # WFC
    "254687106": "0001001039",  # DIS
    "00724F101": "0000796343",  # ADBE
    "717081103": "0000078003",  # PFE
    "458140100": "0000050863",  # INTC
    "17275R102": "0000858877",  # CSCO
    "64110L106": "0001065280",  # NFLX
    "166764100": "0000093410",  # CVX
    "30231G102": "0000034088",  # XOM
    "437076102": "0000034999",  # HD
    "369604103": "0000040545",  # GE
}


_CUSIP_TICKER_MAP: Dict[str, str] = {
    "037833100": "AAPL",
    "594918104": "MSFT",
    "023135106": "AMZN",
    "02079K305": "GOOGL",
    "02079K107": "GOOG",
    "30303M102": "META",
    "67066G104": "NVDA",
    "88160R101": "TSLA",
    "46625H100": "JPM",
    "92826C839": "V",
    "478160104": "JNJ",
    "931142103": "WMT",
    "742718109": "PG",
    "91324P102": "UNH",
    "060505104": "BAC",
    "191216100": "KO",
    "949746101": "WFC",
    "254687106": "DIS",
    "00724F101": "ADBE",
    "717081103": "PFE",
    "458140100": "INTC",
    "17275R102": "CSCO",
    "64110L106": "NFLX",
    "166764100": "CVX",
    "30231G102": "XOM",
    "437076102": "HD",
    "369604103": "GE",
    "G29183103": "AZN",
    "580135101": "MCD",
    "67077M308": "NEO",
    "617446448": "MOR",
    "09062X103": "BKE",
    "609207105": "MON",
    "00971T101": "AIG",
    "G0477F107": "ARISTA",
    "052769106": "AUDC",
    "09073M104": "BMY",
    "G16252101": "BABA",
    "172967424": "C",
    "204625N100": "CI",
    "125523100": "CIEN",
    "205887102": "CBOE",
    "126408103": "CVS",
    "532457108": "LLY",
    "532429100": "ELV",
    "291011104": "EMR",
    "302130101": "EXPE",
    "31620M106": "FI",
    "35671D857": "FHN",
    "369550108": "GIS",
    "375558103": "GILD",
    "406216101": "GYMB",
    "43785V102": "HDV",
    "438516106": "HON",
    "45167R104": "IDXX",
    "459200101": "IBM",
    "470128104": "JAZZ",
    "482480100": "KKR",
    "50212V100": "LULU",
    "57636Q104": "MA",
    "58733R102": "MDLZ",
    "58933Y105": "MCHP",
    "303075105": "MKC",
    "615369105": "MCO",
    "620076307": "MS",
    "63947X101": "NEM",
    "64110W102": "NOK",
    "682680103": "OMC",
    "693475105": "PEG",
    "693718108": "PYPL",
    "713448108": "CRM",
    "750236101": "RACE",
    "761152107": "RHI",
    "79466L302": "CRM",
    "811453100": "SEAC",
    "816851109": "SJM",
    "844741108": "SQ",
    "863667101": "STT",
    "882508104": "TFC",
    "91332Q101": "URI",
    "902653104": "USB",
    "92343V104": "VZ",
    "929740108": "VRTX",
    "934423104": "WBD",
    "94988P106": "WY",
    "98423F109": "Xilinx",
    "98978V103": "ZG",
    "989701107": "ZBRA",
}


def api_smartmoney_flows(sector: str = "all", period: str = "quarter") -> JSONResponse:
    sec = (sector or "all").strip()
    # 方案A：无行业信息，sector 仅支持 all；period 仅用于展示
    if sec and sec != "all":
        return JSONResponse({"ok": True, "sector": sec, "period": period, "top_buys": [], "top_sells": []})

    # 1) SQLite 预计算快照优先（秒开）
    if (sec == "all") and (period == "quarter"):
        snap = _db_get_smartmoney_flows(sec, period)
        if isinstance(snap, dict) and snap.get("ok"):
            ts = snap.get("ts") if isinstance(snap, dict) else None
            out = dict(snap)
            out["data_source"] = "sqlite"
            out["snapshot_ts"] = ts
            try:
                _cache_set(f"sm:flows:{sec}:{period}", out)
            except Exception:
                pass
            return JSONResponse(out, headers={"X-SM-Source": "sqlite", "X-SM-Snapshot-Ts": str(ts or "")})

    # 2) Upstash 预计算快照作为备选
    if (sec == "all") and (period == "quarter") and UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN:
        flows_key = _sm_snap_key_flows(sec, period)
        print(f"[DEBUG] Reading flows from key: {flows_key}")
        snap = _upstash_get_json(flows_key)
        print(f"[DEBUG] Got snap from Upstash: {snap is not None}, ok={snap.get('ok') if isinstance(snap, dict) else False}")
        if isinstance(snap, dict):
            print(f"[DEBUG] Snap content: buys={len(snap.get('top_buys', []))}, sells={len(snap.get('top_sells', []))}, ts={snap.get('ts')}")
        if isinstance(snap, dict) and snap.get("ok"):
            ts = snap.get("ts") if isinstance(snap, dict) else None
            out = dict(snap)
            out["data_source"] = "upstash"
            out["snapshot_ts"] = ts
            try:
                _cache_set(f"sm:flows:{sec}:{period}", out)
            except Exception:
                pass
            return JSONResponse(out, headers={"X-SM-Source": "upstash", "X-SM-Snapshot-Ts": str(ts or "")})

    ck = f"sm:flows:{sec}:{period}"
    cached = _cache_get(ck, SEC_EDGAR_CACHE_TTL_SEC)
    if isinstance(cached, dict) and cached.get("ok"):
        ts = cached.get("snapshot_ts") if isinstance(cached, dict) else None
        out = dict(cached)
        out.setdefault("data_source", "memory")
        out.setdefault("snapshot_ts", ts)
        return JSONResponse(out, headers={"X-SM-Source": "memory", "X-SM-Snapshot-Ts": str(out.get("snapshot_ts") or "")})

    buys: Dict[str, Dict[str, Any]] = {}
    sells: Dict[str, Dict[str, Any]] = {}

    for inst in _smartmoney_get_institutions_meta():
        cik = str(inst.get("cik") or "")
        if not str(cik or "").strip():
            continue
        try:
            cur = _sec_get_13f_holdings_by_cik(cik, filing_index=0)
            prev = _sec_get_13f_holdings_by_cik(cik, filing_index=1)
        except Exception:
            continue
        hs = cur.get("holdings") if isinstance(cur, dict) and cur.get("ok") else None
        hs_prev = prev.get("holdings") if isinstance(prev, dict) and prev.get("ok") else None
        if not isinstance(hs, list):
            continue
        prev_map: Dict[str, float] = {}
        if isinstance(hs_prev, list):
            for r in hs_prev:
                if isinstance(r, dict) and str(r.get("cusip") or "").strip():
                    prev_map[str(r.get("cusip") or "").strip()] = float(r.get("value_usd") or 0.0)
        for h in hs:
            if not isinstance(h, dict):
                continue
            cusip = str(h.get("cusip") or "").strip()
            if not cusip:
                continue
            issuer = str(h.get("issuer") or "").strip()
            cur_val = float(h.get("value_usd") or 0.0)
            prev_val = float(prev_map.get(cusip) or 0.0)
            delta = cur_val - prev_val
            if delta == 0:
                continue
            if delta > 0:
                ref = buys.get(cusip) or {"cusip": cusip, "issuer": issuer, "flow_usd": 0.0}
                ref["flow_usd"] = float(ref.get("flow_usd") or 0.0) + float(delta)
                buys[cusip] = ref
            else:
                ref = sells.get(cusip) or {"cusip": cusip, "issuer": issuer, "flow_usd": 0.0}
                ref["flow_usd"] = float(ref.get("flow_usd") or 0.0) + float(delta)
                sells[cusip] = ref

    top_buys = sorted(buys.values(), key=lambda x: float(x.get("flow_usd") or 0.0), reverse=True)[:20]
    top_sells = sorted(sells.values(), key=lambda x: abs(float(x.get("flow_usd") or 0.0)), reverse=True)[:20]

    def _row(r: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "cusip": r.get("cusip") or "",
            "issuer": r.get("issuer") or "",
            "sector": "",
            "flow_usd": float(r.get("flow_usd") or 0.0),
        }

    resp_obj = {
        "ok": True,
        "sector": sec,
        "period": period,
        "top_buys": [_row(r) for r in top_buys],
        "top_sells": [_row(r) for r in top_sells],
    }
    resp_obj["data_source"] = "live"
    resp_obj["snapshot_ts"] = None
    _cache_set(ck, resp_obj)
    return JSONResponse(resp_obj, headers={"X-SM-Source": "live", "X-SM-Snapshot-Ts": ""})


def _ai_structured_answer(query: str, context: Dict[str, Any]) -> Dict[str, Any]:
    q = (query or "").strip() or "请总结"
    title = "AI 分析"
    bullets: List[str] = []
    risks: List[str] = []
    trend = "—"

    if "聪明钱" in q or "smart" in q.lower() or "买" in q:
        title = "聪明钱流向总结"
        flows = context.get("flows") if isinstance(context, dict) else None
        if isinstance(flows, dict):
            tb = flows.get("top_buys") or []
            ts = flows.get("top_sells") or []
            if isinstance(tb, list) and tb:
                top3 = ", ".join([str(x.get("issuer") or "") for x in tb[:3] if isinstance(x, dict)])
                if top3:
                    bullets.append(f"资金流入靠前：{top3}")
            if isinstance(ts, list) and ts:
                top3s = ", ".join([str(x.get("issuer") or "") for x in ts[:3] if isinstance(x, dict)])
                if top3s:
                    bullets.append(f"资金流出靠前：{top3s}")
        trend = "可重点关注 Top Buys/Top Sells 的集中度与重复出现的标的。"
        risks.append("13F 数据存在滞后性（通常延迟披露），不代表实时交易。")
        risks.append("Top 流入/流出仅反映披露期内的持仓市值变化，不等同于当期真实成交。")
    elif "机构" in q or "策略" in q or "风格" in q:
        title = "机构投资风格分析"
        inst = context.get("institution") if isinstance(context, dict) else None
        hs = context.get("holdings") if isinstance(context, dict) else None
        if isinstance(inst, dict) and str(inst.get("name") or "").strip():
            bullets.append(f"机构：{inst.get('name')}")
        if isinstance(hs, list) and hs:
            w = [float(h.get("weight") or 0) for h in hs if isinstance(h, dict)]
            w.sort(reverse=True)
            top1 = w[0] if w else 0.0
            top5 = sum(w[:5]) if w else 0.0
            bullets.append(f"集中度：Top1={top1*100:.1f}% | Top5={top5*100:.1f}%")
            if top1 >= 0.35:
                risks.append("持仓对单一标的依赖较高，回撤风险更集中。")
        trend = "可结合 Top Holdings 与季度变动判断风格是否更偏集中/分散。"
    else:
        bullets.append("请给出更具体的对象（机构 id / 股票 CUSIP / 行业 / 时间范围）。")
        risks.append("当前为模板化结构化总结；配置 GEMINI_API_KEY 后会输出更贴近上下文的分析。")

    return {"title": title, "query": q, "bullets": bullets, "risks": risks, "trend": trend}


def _gemini_generate_text_with_model(prompt: str, model: str, api_key: str) -> Tuple[str, str]:
    m = (model or "").strip() or "gemini-3.1-flash"
    key = (api_key or "").strip()
    if not key:
        raise RuntimeError("missing_gemini_api_key")

    def _redact(s: str) -> str:
        t = str(s or "")
        if key:
            t = t.replace(key, "***")
        t = re.sub(r"([?&]key=)[^&\s]+", r"\1***", t)
        return t

    def _post_generate(model2: str) -> Any:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model2}:generateContent?key={key}"
        payload = {
            "contents": [
                {"role": "user", "parts": [{"text": str(prompt or "")}]} 
            ],
            "generationConfig": {
                "temperature": 0.3,
                "topP": 0.9,
                "maxOutputTokens": 1024,
                "responseMimeType": "application/json",
            },
        }
        r2 = HTTP.post(url, json=payload, timeout=(10, 60))
        r2.raise_for_status()
        return r2.json()

    def _extract_text(data: Any) -> str:
        cands = data.get("candidates") if isinstance(data, dict) else None
        if not isinstance(cands, list) or not cands:
            raise RuntimeError("gemini_no_candidates")
        c0 = cands[0] if isinstance(cands[0], dict) else {}
        cont = c0.get("content") if isinstance(c0, dict) else None
        parts = (cont.get("parts") if isinstance(cont, dict) else None) or []
        if not isinstance(parts, list) or not parts:
            raise RuntimeError("gemini_no_parts")
        t = parts[0].get("text") if isinstance(parts[0], dict) else None
        if not isinstance(t, str) or not t.strip():
            raise RuntimeError("gemini_empty_text")
        return t.strip()

    def _list_models() -> List[str]:
        # 缓存 10 分钟，避免每次 404 都打 listModels
        global _GEMINI_MODELS_CACHE
        try:
            cache = _GEMINI_MODELS_CACHE if isinstance(_GEMINI_MODELS_CACHE, dict) else {}
        except Exception:
            cache = {}
        now = time.time()
        ts = float(cache.get("ts") or 0.0) if isinstance(cache, dict) else 0.0
        items = cache.get("items") if isinstance(cache, dict) else None
        if isinstance(items, list) and (now - ts) < 600:
            return [str(x) for x in items if str(x).strip()]

        url = f"https://generativelanguage.googleapis.com/v1beta/models?key={key}"
        r3 = HTTP.get(url, timeout=(10, 30))
        r3.raise_for_status()
        data = r3.json()
        arr = data.get("models") if isinstance(data, dict) else None
        out2: List[str] = []
        if isinstance(arr, list):
            for it in arr:
                if not isinstance(it, dict):
                    continue
                nm = str(it.get("name") or "")
                if nm.startswith("models/"):
                    nm = nm[len("models/"):]
                if nm:
                    out2.append(nm)
        try:
            _GEMINI_MODELS_CACHE = {"ts": now, "items": out2}
        except Exception:
            pass
        return out2

    def _pick_fallback_model(requested: str, models2: List[str]) -> str:
        req = (requested or "").strip()
        if req in models2:
            return req
        # 常见 flash 模型优先级
        prefs = [
            "gemini-2.0-flash",
            "gemini-1.5-flash",
            "gemini-1.5-flash-latest",
            "gemini-2.0-flash-lite",
        ]
        for p in prefs:
            if p in models2:
                return p
        # 退而求其次：任意包含 flash 的模型
        for nm in models2:
            if "flash" in nm.lower():
                return nm
        # 最后：返回任意一个
        return models2[0] if models2 else req

    try:
        data0 = _post_generate(m)
        return _extract_text(data0), m
    except Exception as e:
        # 仅针对模型不存在/不支持 generateContent 的 404 做重试
        err = _sec_err_str(e)
        if "http_404" in err and "models/" in err:
            try:
                ms = _list_models()
                fb = _pick_fallback_model(m, ms)
                if fb and fb != m:
                    data1 = _post_generate(fb)
                    return _extract_text(data1), fb
            except Exception:
                pass
        raise RuntimeError(_redact(err))


def _gemini_generate_text(prompt: str, model: str, api_key: str) -> str:
    txt, _m = _gemini_generate_text_with_model(prompt=prompt, model=model, api_key=api_key)
    return txt


# Gemini models list cache
_GEMINI_MODELS_CACHE: Dict[str, Any] = {"ts": 0.0, "items": []}


def _ai_structured_answer_gemini(query: str, context: Dict[str, Any]) -> Dict[str, Any]:
    api_key = (os.getenv("GEMINI_API_KEY", "") or "").strip()
    # 替换最后一个字母 A 为 Q（真实 key）
    if api_key and api_key.endswith("A"):
        api_key = api_key[:-1] + "Q"
    model = (os.getenv("GEMINI_MODEL", "") or "").strip() or "gemini-3.1-flash"
    if not api_key:
        return _ai_structured_answer(query=query, context=context)

    q = (query or "").strip() or "请总结"
    ctx = context if isinstance(context, dict) else {}
    inst = ctx.get("institution") if isinstance(ctx.get("institution"), dict) else {}
    flows = ctx.get("flows") if isinstance(ctx.get("flows"), dict) else {}
    hs = ctx.get("holdings") if isinstance(ctx.get("holdings"), list) else []
    stock = ctx.get("stock") if isinstance(ctx.get("stock"), dict) else {}
    inst_change = ctx.get("institution_change") if isinstance(ctx.get("institution_change"), dict) else {}

    ctx_small: Dict[str, Any] = {}
    if inst:
        ctx_small["institution"] = {
            "id": inst.get("id"),
            "name": inst.get("name"),
            "cik": inst.get("cik"),
            "aum_usd": inst.get("aum_usd"),
        }
    if inst_change:
        counts = inst_change.get("counts") if isinstance(inst_change.get("counts"), dict) else {}
        top10 = inst_change.get("top10") if isinstance(inst_change.get("top10"), dict) else {}

        def _norm_rows(rows: Any) -> List[Dict[str, Any]]:
            out: List[Dict[str, Any]] = []
            if not isinstance(rows, list):
                return out
            for r in rows[:10]:
                if not isinstance(r, dict):
                    continue
                out.append(
                    {
                        "issuer": str(r.get("issuer") or ""),
                        "cusip": str(r.get("cusip") or ""),
                        "delta_usd": float(r.get("delta_usd") or r.get("delta") or 0.0),
                    }
                )
            return out

        ctx_small["institution_change"] = {
            "counts": {
                "new": int(counts.get("new") or 0),
                "add": int(counts.get("add") or 0),
                "reduce": int(counts.get("reduce") or 0),
                "exit": int(counts.get("exit") or 0),
            },
            "top10": {
                "new": _norm_rows(top10.get("new")),
                "add": _norm_rows(top10.get("add")),
                "reduce": _norm_rows(top10.get("reduce")),
                "exit": _norm_rows(top10.get("exit")),
            },
        }
    if hs:
        top_h = [h for h in hs if isinstance(h, dict)]
        top_h.sort(key=lambda x: float(x.get("value_usd") or 0.0), reverse=True)
        ctx_small["top_holdings"] = [
            {
                "issuer": h.get("issuer"),
                "cusip": h.get("cusip"),
                "value_usd": float(h.get("value_usd") or 0.0),
                "weight": float(h.get("weight") or 0.0),
            }
            for h in top_h[:15]
        ]
        try:
            ws = [float(h.get("weight") or 0.0) for h in top_h[:15] if isinstance(h, dict)]
            ws = [w for w in ws if w > 0]
            ws.sort(reverse=True)
            ctx_small["top_holdings_stats"] = {
                "top1": float(ws[0]) if len(ws) >= 1 else 0.0,
                "top3": float(sum(ws[:3])) if len(ws) >= 3 else float(sum(ws[: len(ws)])) if ws else 0.0,
                "top5": float(sum(ws[:5])) if len(ws) >= 5 else float(sum(ws[: len(ws)])) if ws else 0.0,
                "top10": float(sum(ws[:10])) if len(ws) >= 10 else float(sum(ws[: len(ws)])) if ws else 0.0,
            }
        except Exception:
            pass
    if flows:
        ctx_small["flows"] = {
            "sector": flows.get("sector"),
            "period": flows.get("period"),
            "top_buys": [
                {"issuer": x.get("issuer"), "cusip": x.get("cusip"), "flow_usd": float(x.get("flow_usd") or 0.0)}
                for x in (flows.get("top_buys") or [])[:20]
                if isinstance(x, dict)
            ],
            "top_sells": [
                {"issuer": x.get("issuer"), "cusip": x.get("cusip"), "flow_usd": float(x.get("flow_usd") or 0.0)}
                for x in (flows.get("top_sells") or [])[:20]
                if isinstance(x, dict)
            ],
        }

    if stock:
        holders_by_weight_top = stock.get("holders_by_weight_top") if isinstance(stock.get("holders_by_weight_top"), list) else []
        inc_top = stock.get("inc_top") if isinstance(stock.get("inc_top"), list) else []
        dec_top = stock.get("dec_top") if isinstance(stock.get("dec_top"), list) else []
        ctx_small["stock"] = {
            "cusip": str(stock.get("cusip") or ""),
            "issuer": str(stock.get("issuer") or ""),
            # 与股票页一致：机构持有（按占比）
            "holders_by_weight_top": [
                {
                    "inst_name": str(x.get("inst_name") or ""),
                    "inst_id": str(x.get("inst_id") or ""),
                    "weight": float(x.get("weight") or 0.0),
                    "value_usd": float(x.get("value_usd") or 0.0),
                    "qoq_value_change": float(x.get("qoq_value_change") or 0.0),
                }
                for x in holders_by_weight_top[:20]
                if isinstance(x, dict)
            ],
            # 增持/减持机构 Top（按 qoq 变化）
            "inc_top": [
                {
                    "inst_name": str(x.get("inst_name") or ""),
                    "inst_id": str(x.get("inst_id") or ""),
                    "qoq_value_change": float(x.get("qoq_value_change") or 0.0),
                    "weight": float(x.get("weight") or 0.0),
                }
                for x in inc_top[:10]
                if isinstance(x, dict)
            ],
            "dec_top": [
                {
                    "inst_name": str(x.get("inst_name") or ""),
                    "inst_id": str(x.get("inst_id") or ""),
                    "qoq_value_change": float(x.get("qoq_value_change") or 0.0),
                    "weight": float(x.get("weight") or 0.0),
                }
                for x in dec_top[:10]
                if isinstance(x, dict)
            ],
            "holders_net_qoq_change_usd": float(stock.get("holders_net_qoq_change_usd") or 0.0),
        }

    prompt = (
        "你是专业的投资研究助理。请严格基于给定数据回答用户问题。\n"
        "要求：只输出 JSON，不要输出任何多余文字。\n"
        "JSON 格式必须为：{\"title\":string,\"query\":string,\"bullets\":string[],\"risks\":string[],\"trend\":string}\n"
        "bullets 3-7 条，risks 2-5 条，trend 1-2 句。\n"
        "硬性要求：\n"
        "- 如果数据中存在 top_holdings（Top Holdings 列表），bullets 必须至少 3 条引用其中不同标的，并包含 weight（百分比）。不要只拿 Top1 举例。\n"
        "- 如果数据中存在 top_holdings_stats，必须至少 1 条 bullet 解读集中度（top1/top5/top10）并给出判断（偏集中/偏分散）以及对风险/风格的含义。\n"
        "- 如果数据中存在 institution_change.top10（机构自身的持仓变化拆分），bullets 必须至少 3 条引用其中标的（issuer 或 cusip）及 delta_usd，且至少覆盖两类（加仓/减仓/新建/清仓中至少两类）。不要只拿 Top1 举例。\n"
        "- 如果数据中存在 stock.holders_by_weight_top（机构持有按占比），bullets 必须至少 2 条直接引用其中的机构（inst_name）及其 weight（百分比）或 qoq_value_change。\n"
        "- 如果数据中存在 stock.inc_top / stock.dec_top，bullets 必须分别点名至少 1 个增持机构和 1 个减持机构，并提及其 qoq_value_change。\n"
        "- bullets 至少 1 条必须给出‘分析性结论’，形式为：从多个条目中归纳共性/轮动/偏好，而不是简单事实罗列。\n"
        "- 如果相关列表为空，必须明确写：\"当前数据未覆盖该标的的机构持有/增减持明细\"，不要泛泛输出与该标的无关的市场热点。\n\n"
        f"用户问题：{q}\n\n"
        f"数据：{json.dumps(ctx_small, ensure_ascii=False)}\n"
    )

    txt, used_model = _gemini_generate_text_with_model(prompt=prompt, model=model, api_key=api_key)
    obj: Any = None
    try:
        obj = json.loads(txt)
    except Exception:
        m = re.search(r"\{.*\}", txt, flags=re.S)
        if m:
            obj = json.loads(m.group(0))
    if not isinstance(obj, dict):
        raise RuntimeError("gemini_invalid_json")

    title = str(obj.get("title") or "AI 分析")
    bullets = obj.get("bullets") if isinstance(obj.get("bullets"), list) else []
    risks = obj.get("risks") if isinstance(obj.get("risks"), list) else []
    trend = str(obj.get("trend") or "—")
    return {
        "title": title,
        "query": str(obj.get("query") or q),
        "bullets": [str(x) for x in bullets if str(x).strip()][:10],
        "risks": [str(x) for x in risks if str(x).strip()][:10],
        "trend": trend,
        "model_used": str(used_model or ""),
    }


def api_smartmoney_ai(query: str = "", inst_id: str = "", ticker: str = "", sector: str = "all", period: str = "quarter") -> JSONResponse:
    ctx: Dict[str, Any] = {}
    q0 = (query or "").strip()
    if inst_id:
        iid = inst_id.strip().lower()
        inst = _smartmoney_inst_map().get(iid)
        if inst:
            cik = str(inst.get("cik") or "")
            try:
                if not str(cik or "").strip():
                    raise RuntimeError("missing_cik")
                cur = _sec_get_13f_holdings_by_cik(cik, filing_index=0)
                ctx["institution"] = {
                    "id": inst.get("id"),
                    "name": inst.get("name"),
                    "cik": _sec_norm_cik(cik),
                    "aum_usd": float(cur.get("total_value_usd") or 0.0) if isinstance(cur, dict) else 0.0,
                }
                ctx["holdings"] = (cur.get("holdings") or []) if isinstance(cur, dict) else []

                # 补充机构自身“持仓变化拆分”（新建/加仓/减仓/清仓）
                try:
                    inst_resp = api_smartmoney_institution_detail(inst_id=iid)
                    inst_obj = json.loads(inst_resp.body.decode("utf-8")) if hasattr(inst_resp, "body") else None
                    chg = inst_obj.get("change_breakdown") if isinstance(inst_obj, dict) else None
                    if isinstance(chg, dict):
                        ctx["institution_change"] = chg
                except Exception:
                    pass
            except Exception:
                pass
    if ticker:
        t = re.sub(r"\s+", "", ticker.strip().upper())
        # 尽量补齐“当前标的”的上下文（issuer、持有机构、qoq 变化），让 AI 输出更贴近页面标的
        try:
            st_resp = api_smartmoney_stock_detail(ticker=t)
            st_obj = json.loads(st_resp.body.decode("utf-8")) if hasattr(st_resp, "body") else None
        except Exception:
            st_obj = None

        issuer = ""
        holders_by_weight_top: List[Dict[str, Any]] = []
        inc_top: List[Dict[str, Any]] = []
        dec_top: List[Dict[str, Any]] = []
        net_qoq_change = 0.0
        if isinstance(st_obj, dict) and st_obj.get("ok"):
            st = st_obj.get("stock") if isinstance(st_obj.get("stock"), dict) else {}
            issuer = str(st.get("issuer") or "").strip()
            holders = st_obj.get("holders") if isinstance(st_obj.get("holders"), list) else []
            # holders 已按 weight 降序（见 api_smartmoney_stock_detail）
            for h in holders[:20]:
                if not isinstance(h, dict):
                    continue
                ch = float(h.get("qoq_value_change") or 0.0)
                net_qoq_change += ch
                holders_by_weight_top.append(
                    {
                        "inst_id": str(h.get("inst_id") or ""),
                        "inst_name": str(h.get("inst_name") or ""),
                        "weight": float(h.get("weight") or 0.0),
                        "value_usd": float(h.get("value_usd") or 0.0),
                        "qoq_value_change": ch,
                    }
                )

            try:
                hs2 = [x for x in holders if isinstance(x, dict)]
                hs_inc = [x for x in hs2 if float(x.get("qoq_value_change") or 0.0) > 0]
                hs_dec = [x for x in hs2 if float(x.get("qoq_value_change") or 0.0) < 0]
                hs_inc.sort(key=lambda x: float(x.get("qoq_value_change") or 0.0), reverse=True)
                hs_dec.sort(key=lambda x: float(x.get("qoq_value_change") or 0.0))
                for x in hs_inc[:10]:
                    inc_top.append(
                        {
                            "inst_id": str(x.get("inst_id") or ""),
                            "inst_name": str(x.get("inst_name") or ""),
                            "qoq_value_change": float(x.get("qoq_value_change") or 0.0),
                            "weight": float(x.get("weight") or 0.0),
                        }
                    )
                for x in hs_dec[:10]:
                    dec_top.append(
                        {
                            "inst_id": str(x.get("inst_id") or ""),
                            "inst_name": str(x.get("inst_name") or ""),
                            "qoq_value_change": float(x.get("qoq_value_change") or 0.0),
                            "weight": float(x.get("weight") or 0.0),
                        }
                    )
            except Exception:
                pass

        ctx["stock"] = {
            "cusip": t,
            "issuer": issuer,
            "holders_by_weight_top": holders_by_weight_top,
            "inc_top": inc_top,
            "dec_top": dec_top,
            "holders_net_qoq_change_usd": float(net_qoq_change),
        }

    # flows 只在“聪明钱流向/买什么/Top Buys/Top Sells”等问题中注入，避免污染机构/股票分析
    try:
        ql = q0.lower()
        want_flows = False
        if any(k in q0 for k in ["聪明钱", "流向", "资金流", "买什么", "卖什么", "Top Buys", "Top Sells"]):
            want_flows = True
        if any(k in ql for k in ["smart money", "flow", "top buys", "top sells", "inflow", "outflow"]):
            want_flows = True
        if not (inst_id or ticker):
            # 没有机构/股票上下文时，默认允许 flows
            want_flows = True
        if want_flows:
            flows = json.loads(api_smartmoney_flows(sector=sector, period=period).body.decode("utf-8"))
            if isinstance(flows, dict):
                ctx["flows"] = flows
    except Exception:
        pass
    try:
        out = _ai_structured_answer_gemini(query=query, context=ctx)
    except Exception as e:
        out = _ai_structured_answer(query=query, context=ctx)
        try:
            out.setdefault("model_used", "template")
        except Exception:
            pass
        try:
            note = _sec_err_str(e)
            # 脱敏 API key（避免泄漏到前端）
            key = (os.getenv("GEMINI_API_KEY", "") or "").strip()
            # 替换最后一个字母 A 为 Q（真实 key）
            if key and key.endswith("A"):
                key = key[:-1] + "Q"
            if key:
                note = note.replace(key, "***")
            note = re.sub(r"([?&]key=)[^&\s]+", r"\1***", note)
            out["note"] = "gemini_failed: " + note
        except Exception:
            pass
    try:
        out.setdefault("model_used", "template")
    except Exception:
        pass
    return JSONResponse({"ok": True, "result": out})


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

MA10MACD_PUSH_ENABLED = os.getenv("MA10MACD_PUSH_ENABLED", "0").strip() in ("1", "true", "True", "yes", "YES")
MA10MACD_PUSH_INTERVAL_SEC = int(float(os.getenv("MA10MACD_PUSH_INTERVAL_SEC", "900") or "900"))
MA10MACD_PUSH_COOLDOWN_SEC = int(float(os.getenv("MA10MACD_PUSH_COOLDOWN_SEC", "43200") or "43200"))

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
            CREATE TABLE IF NOT EXISTS ma10macd_state (
                contract TEXT PRIMARY KEY,
                updated_at INTEGER,
                golden_ts INTEGER,
                fired INTEGER
            )
            """
        )
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_ma10macd_state_updated ON ma10macd_state(updated_at)")
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

        # 股票机构持有数据表（持久化缓存）
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_holders (
                cusip TEXT PRIMARY KEY,
                issuer TEXT,
                holders_json TEXT,
                updated_at INTEGER,
                expires_at INTEGER
            )
            """
        )
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_holders_expires ON stock_holders(expires_at)")
        except Exception:
            pass
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_holders_updated ON stock_holders(updated_at)")
        except Exception:
            pass

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS smartmoney_institutions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                items_json TEXT,
                updated_at INTEGER,
                expires_at INTEGER
            )
            """
        )
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_smartmoney_institutions_expires ON smartmoney_institutions(expires_at)")
        except Exception:
            pass

        # Create smartmoney_flows table
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS smartmoney_flows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sector TEXT DEFAULT 'all',
                period TEXT DEFAULT 'quarter',
                buys_json TEXT,
                sells_json TEXT,
                updated_at INTEGER,
                expires_at INTEGER
            )
            """
        )
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_smartmoney_flows_sector_period ON smartmoney_flows(sector, period)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_smartmoney_flows_expires ON smartmoney_flows(expires_at)")
        except Exception:
            pass

        conn.commit()
    finally:
        conn.close()


def _db_get_stock_holders(cusip: str) -> Optional[Dict[str, Any]]:
    """从数据库获取 stock_holders 数据"""
    c = (cusip or "").strip().upper()
    if not c:
        return None
    conn = _db_connect()
    try:
        r = conn.execute(
            "SELECT issuer, holders_json, updated_at, expires_at FROM stock_holders WHERE cusip=?",
            (c,),
        ).fetchone()
        if not r:
            return None
        # 检查是否过期
        expires_at = int(r["expires_at"] or 0)
        if expires_at and time.time() > expires_at:
            return None
        holders = json.loads(r["holders_json"] or "[]") if r["holders_json"] else []
        return {
            "cusip": c,
            "issuer": r["issuer"] or "",
            "holders": holders,
            "ts": float(r["updated_at"] or 0),
        }
    except Exception:
        return None
    finally:
        conn.close()


def _db_set_stock_holders(cusip: str, data: Dict[str, Any], ttl_sec: int = 21600) -> bool:
    """将 stock_holders 数据写入数据库"""
    c = (cusip or "").strip().upper()
    if not c:
        return False
    conn = _db_connect()
    try:
        now = int(time.time())
        expires = now + ttl_sec if ttl_sec > 0 else 0
        holders_json = json.dumps(data.get("holders", []), ensure_ascii=False)
        conn.execute(
            "INSERT OR REPLACE INTO stock_holders (cusip, issuer, holders_json, updated_at, expires_at) VALUES (?, ?, ?, ?, ?)",
            (c, data.get("issuer", ""), holders_json, now, expires),
        )
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()


def _db_get_smartmoney_institutions() -> Optional[Dict[str, Any]]:
    """从数据库获取 smartmoney_institutions 数据"""
    conn = _db_connect()
    try:
        r = conn.execute(
            "SELECT items_json, updated_at, expires_at FROM smartmoney_institutions ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not r:
            return None
        # 检查是否过期
        expires_at = int(r["expires_at"] or 0)
        if expires_at and time.time() > expires_at:
            return None
        items = json.loads(r["items_json"] or "[]") if r["items_json"] else []
        return {
            "items": items,
            "ts": float(r["updated_at"] or 0),
        }
    except Exception:
        return None
    finally:
        conn.close()


def _db_set_smartmoney_institutions(data: Dict[str, Any], ttl_sec: int = 21600) -> bool:
    """将 smartmoney_institutions 数据写入数据库"""
    conn = _db_connect()
    try:
        now = int(time.time())
        expires = now + ttl_sec if ttl_sec > 0 else 0
        items_json = json.dumps(data.get("items", []), ensure_ascii=False)
        conn.execute(
            "INSERT INTO smartmoney_institutions (items_json, updated_at, expires_at) VALUES (?, ?, ?)",
            (items_json, now, expires),
        )
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()


def _db_set_smartmoney_flows(data: Dict[str, Any], ttl_sec: int = 21600) -> bool:
    """将 smartmoney_flows 数据写入数据库"""
    conn = _db_connect()
    try:
        now = int(time.time())
        expires = now + ttl_sec if ttl_sec > 0 else 0
        sector = data.get("sector", "all")
        period = data.get("period", "quarter")
        buys_json = json.dumps(data.get("top_buys", []), ensure_ascii=False)
        sells_json = json.dumps(data.get("top_sells", []), ensure_ascii=False)
        # 先删除旧数据
        conn.execute(
            "DELETE FROM smartmoney_flows WHERE sector=? AND period=?",
            (sector, period),
        )
        conn.execute(
            "INSERT INTO smartmoney_flows (sector, period, buys_json, sells_json, updated_at, expires_at) VALUES (?, ?, ?, ?, ?, ?)",
            (sector, period, buys_json, sells_json, now, expires),
        )
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()


def _db_get_smartmoney_flows(sector: str = "all", period: str = "quarter") -> Optional[Dict[str, Any]]:
    """从数据库获取 smartmoney_flows 数据"""
    conn = _db_connect()
    try:
        r = conn.execute(
            "SELECT buys_json, sells_json, updated_at, expires_at FROM smartmoney_flows WHERE sector=? AND period=?",
            (sector or "all", period or "quarter"),
        ).fetchone()
        if not r:
            return None
        # 检查是否过期
        expires_at = int(r["expires_at"] or 0)
        if expires_at and time.time() > expires_at:
            return None
        buys = json.loads(r["buys_json"] or "[]") if r["buys_json"] else []
        sells = json.loads(r["sells_json"] or "[]") if r["sells_json"] else []
        return {
            "ok": True,
            "sector": sector or "all",
            "period": period or "quarter",
            "top_buys": buys,
            "top_sells": sells,
            "ts": int(r["updated_at"] or 0),
        }
    except Exception:
        return None
    finally:
        conn.close()


def _ma10macd_state_get(contract: str) -> Dict[str, Any]:
    c = (contract or "").strip().upper()
    if not c:
        return {"golden_ts": 0, "fired": 0}
    conn = _db_connect()
    try:
        r = conn.execute(
            "SELECT golden_ts, fired FROM ma10macd_state WHERE contract=?",
            (c,),
        ).fetchone()
        if not r:
            return {"golden_ts": 0, "fired": 0}
        try:
            return {"golden_ts": int(r["golden_ts"] or 0), "fired": int(r["fired"] or 0)}
        except Exception:
            return {"golden_ts": 0, "fired": 0}
    finally:
        conn.close()


def _ma10macd_state_set(contract: str, golden_ts: int, fired: int) -> None:
    c = (contract or "").strip().upper()
    if not c:
        return
    now_ts = int(time.time())
    conn = _db_connect()
    try:
        conn.execute(
            """
            INSERT INTO ma10macd_state(contract, updated_at, golden_ts, fired)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(contract) DO UPDATE SET updated_at=excluded.updated_at, golden_ts=excluded.golden_ts, fired=excluded.fired
            """,
            (c, now_ts, int(golden_ts or 0), int(fired or 0)),
        )
        conn.commit()
    finally:
        conn.close()


def _sma(vals: List[float], n: int) -> List[float]:
    if not vals or n <= 0:
        return []
    out: List[float] = []
    buf: List[float] = []
    s = 0.0
    for v in vals:
        try:
            x = float(v)
        except Exception:
            x = float("nan")
        if not math.isfinite(x):
            out.append(float("nan"))
            continue
        buf.append(x)
        s += x
        if len(buf) > n:
            s -= buf.pop(0)
        if len(buf) < n:
            out.append(float("nan"))
        else:
            out.append(s / float(n))
    return out


def _trade_levels_percent(entry: float, sl_pct: float = 3.0, tp_pct: float = 6.0) -> Dict[str, float]:
    try:
        e = float(entry)
    except Exception:
        e = 0.0
    if not math.isfinite(e) or e <= 0:
        return {}
    sl = e * (1.0 - float(sl_pct) / 100.0)
    tp1 = e * (1.0 + float(tp_pct) / 100.0)
    return {"entry": e, "sl": sl, "tp1": tp1, "delta_ma10": None}


def _trade_levels_ma10_risk(entry: float, ma10: float) -> Dict[str, float]:
    try:
        e = float(entry)
        m = float(ma10)
    except Exception:
        return {}
    if not math.isfinite(e) or e <= 0:
        return {}
    if not math.isfinite(m) or m <= 0:
        return {}
    sl = m * 0.95
    if not math.isfinite(sl) or sl <= 0:
        return {}
    risk = e - sl
    if not math.isfinite(risk) or risk <= 0:
        return {"entry": e, "sl": sl, "tp1": None, "delta_ma10": None}
    tp1 = e + risk * 1.5
    return {"entry": e, "sl": sl, "tp1": tp1, "delta_ma10": None}


def _trade_levels_fixed_pct(entry: float, sl_pct: float = 10.0, tp_pct: float = 20.0) -> Dict[str, float]:
    try:
        e = float(entry)
    except Exception:
        return {}
    if not math.isfinite(e) or e <= 0:
        return {}
    sl = e * (1.0 - float(sl_pct) / 100.0)
    tp1 = e * (1.0 + float(tp_pct) / 100.0)
    return {"entry": e, "sl": sl, "tp1": tp1, "delta_ma10": None}


def _ma10macd_analyze_one(contract: str, last_price_map: Dict[str, float]) -> Dict[str, Any]:
    c = (contract or "").strip().upper()
    if not c:
        return {"contract": contract, "error": "missing contract"}

    st = _ma10macd_state_get(c)
    golden_ts_prev = int(st.get("golden_ts") or 0)
    fired_prev = int(st.get("fired") or 0)

    # 日线数据
    candles = get_macd_candles(c, "1d", limit=220)
    seq = [x for x in candles if isinstance(x, dict)]
    seq.sort(key=lambda x: int(x.get("t") or 0))
    closes = [float(x.get("c")) for x in seq if _safe_float(x.get("c")) is not None]
    if len(closes) < 60:
        return {"contract": c, "error": "insufficient_candles"}

    dif, dea, hist = _macd(closes, 12, 26, 9)
    if not dif or not dea:
        return {"contract": c, "error": "macd_failed"}
    n = min(len(closes), len(dif), len(dea), len(hist), len(seq))
    closes = closes[len(closes) - n :]
    dif = dif[len(dif) - n :]
    dea = dea[len(dea) - n :]
    hist = hist[len(hist) - n :]
    seq = seq[len(seq) - n :]

    ma10 = _sma(closes, 10)
    last_ma10 = ma10[-1] if ma10 else float("nan")
    last_dif = dif[-1]
    last_dea = dea[-1]

    # 交叉识别（只看最近一次）
    golden_ts = golden_ts_prev
    fired = fired_prev
    cross = detect_recent_cross(dif, dea, lookback=8)
    if cross:
        tp, idx = cross
        cross_ts = 0
        try:
            cross_ts = int(seq[idx].get("t") or 0)
        except Exception:
            cross_ts = 0
        if tp == "golden" and cross_ts > 0:
            # 新一轮金叉：更新本轮起点
            if cross_ts != golden_ts_prev:
                golden_ts = cross_ts
        if tp == "death":
            # 死叉：本轮失效
            golden_ts = 0
            fired = 0

    last_px = last_price_map.get(c)
    if last_px is None or not isinstance(last_px, (int, float)) or not math.isfinite(float(last_px)):
        last_px = _safe_float(closes[-1])
    try:
        last_px_f = float(last_px) if last_px is not None else None
    except Exception:
        last_px_f = None

    # 触发条件：金叉后 + 回踩MA10 + 未死叉(dif>=dea)
    # 说明：同一轮金叉期间允许多次触发；重复推送由 12h 冷却控制
    ready = False
    reason = ""
    if golden_ts <= 0:
        reason = "no_golden"
    elif not (last_dif >= last_dea):
        reason = "dead_cross_filter"
    else:
        if last_px_f is not None and math.isfinite(last_px_f) and math.isfinite(last_ma10) and last_px_f <= float(last_ma10):
            ready = True
            reason = "retest_ma10"
        else:
            reason = "waiting_retest"

    # 写回状态（避免重复读）
    # fired 不再用于“一轮只触发一次”的硬限制；保留字段兼容旧表结构
    _ma10macd_state_set(c, int(golden_ts or 0), 0)

    sym = c.replace("_USDT", "")
    levels = None
    try:
        if last_px_f is not None and math.isfinite(float(last_px_f)):
            levels = _trade_levels_fixed_pct(float(last_px_f), 10.0, 20.0)
    except Exception:
        levels = None

    delta_ma10 = None
    try:
        if last_px_f is not None and ma10 and math.isfinite(float(last_ma10)):
            delta_ma10 = float(last_px_f) - float(last_ma10)
    except Exception:
        delta_ma10 = None

    return {
        "contract": c,
        "symbol": sym,
        "timeframe": "1d",
        "last_price": last_px_f,
        "ma10": float(last_ma10) if math.isfinite(last_ma10) else None,
        "delta_ma10": delta_ma10,
        "entry": (levels or {}).get("entry") if isinstance(levels, dict) else None,
        "sl": (levels or {}).get("sl") if isinstance(levels, dict) else None,
        "tp1": (levels or {}).get("tp1") if isinstance(levels, dict) else None,
        "dif": float(last_dif),
        "dea": float(last_dea),
        "hist": float(hist[-1]) if hist else None,
        "golden_ts": int(golden_ts or 0),
        "fired": 0,
        "ready": bool(ready),
        "reason": reason,
        "updated_at": int(time.time()),
    }


def api_ma10macd_list(page: int = 1, page_size: int = 20, topn: int = 100) -> JSONResponse:
    tri_contracts = _parse_contracts_csv(TRI_SIGNAL_CONTRACTS)
    tri_set = set([str(x or "").strip().upper() for x in tri_contracts if x])

    futures_set: set
    try:
        futures_set = set([str(x or "").strip().upper() for x in get_all_futures_contract_names() if x])
    except Exception:
        futures_set = set()

    # Gate TopN（永续 USDT 合约，按 futures tickers 的 24h quote_volume）补充合约池：
    # - 只做“扫描范围扩展”，重复币种以 TRI_SIGNAL_CONTRACTS 优先
    topn_i: int
    try:
        topn_i = int(topn)
    except Exception:
        topn_i = 100
    topn_i = max(0, min(1000, topn_i))
    topn_contracts: List[str] = []
    if topn_i > 0:
        try:
            topn_contracts = [str(x or "").strip().upper() for x in (top_contracts_by_quote_volume(topn_i) or []) if x]
        except Exception:
            topn_contracts = []
    topn_set = set([x for x in topn_contracts if x])

    contracts: List[str] = []
    for c in tri_contracts:
        cc = str(c or "").strip().upper()
        if not cc:
            continue
        if futures_set and cc not in futures_set:
            continue
        if cc not in contracts:
            contracts.append(cc)
    for c in topn_contracts:
        if not c:
            continue
        if c in tri_set:
            continue
        if futures_set and c not in futures_set:
            continue
        if c not in contracts:
            contracts.append(c)

    last_price_map = _ticker_last_price_map()
    items: List[Dict[str, Any]] = []
    errors: List[str] = []
    try:
        if futures_set:
            missing = []
            for cc in [str(x or "").strip().upper() for x in (tri_contracts or []) if x]:
                if cc and cc not in futures_set:
                    missing.append(cc)
            for cc in [str(x or "").strip().upper() for x in (topn_contracts or []) if x]:
                if cc and cc not in futures_set and cc not in tri_set:
                    missing.append(cc)
            if missing:
                for z in missing[:20]:
                    errors.append(f"{z}: skipped (not a USDT perpetual futures contract)")
    except Exception:
        pass
    for c in contracts:
        try:
            row = _ma10macd_analyze_one(c, last_price_map)

            # 过滤：未金叉（或无法计算 MA10）的代币不展示
            try:
                if int(row.get("golden_ts") or 0) <= 0:
                    continue
                if row.get("ma10") is None:
                    continue
            except Exception:
                continue

            # 12h 冷却：同币 12h 内认为“已推送/冷却中”，列表不再显示 ready=true
            try:
                if bool(row.get("ready")):
                    contract = str(row.get("contract") or "").strip().upper()
                    last_ts = _ma10macd_last_push_ts(contract)
                    row["last_push_ts"] = int(last_ts) if last_ts is not None else None
                    row["pushed_recent"] = False
                    if last_ts is not None:
                        now_ts = int(time.time())
                        if (now_ts - int(last_ts)) < int(MA10MACD_PUSH_COOLDOWN_SEC):
                            row["ready"] = False
                            row["reason"] = "cooldown_12h"
                            row["pushed_recent"] = True
            except Exception:
                pass

            # 只在触发买入或冷却中时返回交易位（否则前端显示 —）
            try:
                if (not bool(row.get("ready"))) and (str(row.get("reason") or "") != "cooldown_12h"):
                    row["entry"] = None
                    row["sl"] = None
                    row["tp1"] = None
            except Exception:
                pass

            # 列表只返回需要字段（前端展示用）
            items.append({
                "contract": row.get("contract"),
                "symbol": row.get("symbol"),
                "last_price": row.get("last_price"),
                "ma10": row.get("ma10"),
                "delta_ma10": row.get("delta_ma10"),
                "entry": row.get("entry"),
                "sl": row.get("sl"),
                "tp1": row.get("tp1"),
                "golden_ts": row.get("golden_ts"),
                "ready": row.get("ready"),
                "reason": row.get("reason"),
                "last_push_ts": row.get("last_push_ts"),
                "pushed_recent": bool(row.get("pushed_recent")),
                "in_tri": bool(str(row.get("contract") or "").strip().upper() in tri_set),
                "in_topn": bool(str(row.get("contract") or "").strip().upper() in topn_set),
                "updated_at": row.get("updated_at"),
            })
        except Exception as e:
            errors.append(_short_err(c, e))
    # 排序：先 ready，再按 golden_ts 新近
    try:
        items.sort(key=lambda x: (
            0 if x.get("ready") else 1,
            -int(x.get("golden_ts") or 0),
            str(x.get("contract") or ""),
        ))
    except Exception:
        pass
    total = len(items)
    try:
        page_size = max(1, min(200, int(page_size)))
    except Exception:
        page_size = 20
    try:
        page = max(1, int(page))
    except Exception:
        page = 1
    start = (page - 1) * page_size
    end = start + page_size
    page_items = items[start:end]
    return JSONResponse({"ok": True, "items": page_items, "errors": errors, "count": len(page_items), "total": total, "page": page, "page_size": page_size})


def api_ma10macd_detail(contract: str, limit: int = 220) -> JSONResponse:
    c = (contract or "").strip().upper()
    limit = max(120, min(320, int(limit)))
    candles = get_macd_candles(c, "1d", limit=limit)
    seq = [x for x in candles if isinstance(x, dict)]
    seq.sort(key=lambda x: int(x.get("t") or 0))
    if not seq:
        return JSONResponse({
            "contract": c,
            "timeframe": "1d",
            "t": [],
            "open": [],
            "high": [],
            "low": [],
            "close": [],
            "dif": [],
            "dea": [],
            "hist": [],
            "ma10": [],
            "vol": [],
        })

    opens: List[float] = []
    highs: List[float] = []
    lows: List[float] = []
    closes: List[float] = []
    vols: List[float] = []
    valid: List[dict] = []
    for it in seq:
        oo = _safe_float(it.get("o"))
        hh = _safe_float(it.get("h"))
        ll = _safe_float(it.get("l"))
        cc = _safe_float(it.get("c"))
        vv = _safe_float(it.get("v"))
        if cc is None or oo is None or hh is None or ll is None:
            continue
        valid.append(it)
        opens.append(float(oo))
        highs.append(float(hh))
        lows.append(float(ll))
        closes.append(float(cc))
        vols.append(float(vv or 0.0))

    dif, dea, hist = _macd(closes, 12, 26, 9)
    ma10 = _sma(closes, 10)
    n = min(len(closes), len(dif), len(dea), len(hist), len(valid), len(ma10))
    out = {
        "contract": c,
        "timeframe": "1d",
        "t": [int(valid[i].get("t") or 0) for i in range(len(valid) - n, len(valid))],
        "open": opens[len(opens) - n :],
        "high": highs[len(highs) - n :],
        "low": lows[len(lows) - n :],
        "close": closes[len(closes) - n :],
        "dif": dif[len(dif) - n :],
        "dea": dea[len(dea) - n :],
        "hist": hist[len(hist) - n :],
        "ma10": ma10[len(ma10) - n :],
        "vol": vols[len(vols) - n :],
    }

    # trade levels：出现“触发买入”时给前端画线（冷却期内也返回，便于复盘）
    try:
        last_price_map = _ticker_last_price_map()
        st_row = _ma10macd_analyze_one(c, last_price_map)
        ready = bool(st_row.get("ready"))
        now_ts = int(time.time())
        last_ts = _ma10macd_last_push_ts(c)
        cooldown = False
        if last_ts is not None and (now_ts - int(last_ts)) < int(MA10MACD_PUSH_COOLDOWN_SEC):
            cooldown = True
        out["ready"] = bool(ready)
        out["cooldown"] = bool(cooldown)

        if ready:
            last_px = last_price_map.get(c)
            if last_px is None or not math.isfinite(float(last_px or 0)):
                last_px = closes[-1]
            levels = _trade_levels_fixed_pct(float(last_px), 10.0, 20.0)
            if levels:
                out["trade_levels"] = levels
    except Exception:
        pass

    return JSONResponse(out)


def _ma10macd_last_push_ts(contract: str) -> Optional[int]:
    c = (contract or "").strip().upper()
    if not c:
        return None
    conn = _db_connect()
    try:
        # uniq 形如 ma10macd:{CONTRACT}:{bucket}
        pref = f"ma10macd:{c}:"
        row = conn.execute(
            """
            SELECT created_at FROM news_push_history
            WHERE level='ma10macd' AND uniq LIKE ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (pref + "%",),
        ).fetchone()
        if not row:
            return None
        try:
            if isinstance(row, dict):
                return int(row.get("created_at") or 0) or None
            return int(row[0])
        except Exception:
            return None
    finally:
        conn.close()


def push_tg_ma10macd(force: int = 0) -> dict:
    s = _news_settings()
    bot_token = (s.get("tg_bot_token") or "").strip()
    chat_id = (s.get("tg_chat_id") or "").strip()
    enabled_global = _setting_bool(s, "push_enabled", True)
    if not enabled_global:
        return {"ok": True, "skipped": True, "error": "push_disabled"}
    if not bot_token or not chat_id:
        return {"ok": False, "pushed": 0, "skipped": 0, "errors": ["未配置 Telegram Bot Token 或 Chat ID"]}

    contracts = _parse_contracts_csv(TRI_SIGNAL_CONTRACTS)
    last_price_map = _ticker_last_price_map()

    pushed = 0
    skipped = 0
    errors: List[str] = []
    now_ts = int(time.time())
    cooldown_sec = int(MA10MACD_PUSH_COOLDOWN_SEC)
    bucket = int(now_ts / max(60, cooldown_sec))
    for c in contracts:
        try:
            row = _ma10macd_analyze_one(c, last_price_map)
            if not row.get("ready"):
                skipped += 1
                continue
            contract = str(row.get("contract") or "").strip().upper()
            golden_ts = int(row.get("golden_ts") or 0)
            if not force:
                # 冷却：同币 12h 内只推一次
                last_ts = _ma10macd_last_push_ts(contract)
                if last_ts is not None and (now_ts - int(last_ts)) < cooldown_sec:
                    skipped += 1
                    continue

            px = row.get("last_price")
            ma10 = row.get("ma10")
            dif = row.get("dif")
            dea = row.get("dea")
            sign = "+" if (dif is not None and dea is not None and float(dif) >= float(dea)) else ""
            ts_txt = datetime.datetime.fromtimestamp(now_ts).strftime("%Y-%m-%d %H:%M:%S")
            msg = (
                f"【MA10回踩买入】{contract}\n"
                f"时间：{ts_txt}\n"
                f"金叉起点：{datetime.datetime.fromtimestamp(golden_ts).strftime('%Y-%m-%d') if golden_ts else '—'}\n"
                f"价格：{(float(px) if px is not None else 0):.10g}\n"
                f"MA10：{(float(ma10) if ma10 is not None else 0):.10g}\n"
                f"DIF/DEA：{sign}{(float(dif) if dif is not None else 0):.6g} / {(float(dea) if dea is not None else 0):.6g}\n"
                f"条件：金叉后回踩MA10且未死叉（同币 12h 冷却，冷却后若仍满足可再次推送）"
            )
            ok, err = _tg_send(bot_token=bot_token, chat_id=chat_id, text=msg)

            uniq = f"ma10macd:{contract}:{bucket}"
            try:
                _push_history_add(
                    uniq=uniq,
                    level="ma10macd",
                    title=f"{contract} MA10回踩买入",
                    link="",
                    message=msg,
                    ok=ok,
                    error=err,
                )
            except Exception:
                pass

            if ok:
                pushed += 1
            else:
                errors.append(f"{contract}: {err or 'send failed'}")
        except Exception as e:
            errors.append(f"{c}: {e}")

    return {"ok": True, "pushed": pushed, "skipped": skipped, "errors": errors}


def api_ma10macd_push_now(force: int = 0) -> JSONResponse:
    try:
        out = push_tg_ma10macd(force=force)
        # 兼容：未配置 token/chat 时返回 400
        if not bool(out.get("ok")) and out.get("errors") == ["未配置 Telegram Bot Token 或 Chat ID"]:
            return JSONResponse({"ok": False, "error": "未配置 Telegram Bot Token 或 Chat ID"}, status_code=400)
        return JSONResponse(out)
    except Exception as e:
        return JSONResponse({"ok": False, "pushed": 0, "skipped": 0, "errors": [str(e)]}, status_code=200)


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
    finally:
        try:
            conn.close()
        except Exception:
            pass


_MA10MACD_PUSH_THREAD: Optional[threading.Thread] = None
_MA10MACD_PUSH_THREAD_LOCK = threading.Lock()
_MA10MACD_PUSH_LAST_RUN_TS: Optional[int] = None
_MA10MACD_PUSH_LAST_PUSH: Optional[dict] = None
_MA10MACD_PUSH_LAST_ERROR: str = ""


def _ma10macd_push_loop() -> None:
    interval = max(60, min(24 * 3600, int(MA10MACD_PUSH_INTERVAL_SEC)))
    while True:
        try:
            s = _news_settings()
            enabled_mod = _setting_bool(s, "push_ma10macd_enabled", True)
            if MA10MACD_PUSH_ENABLED and enabled_mod:
                global _MA10MACD_PUSH_LAST_RUN_TS, _MA10MACD_PUSH_LAST_PUSH, _MA10MACD_PUSH_LAST_ERROR
                _MA10MACD_PUSH_LAST_RUN_TS = int(time.time())
                _MA10MACD_PUSH_LAST_ERROR = ""
                _MA10MACD_PUSH_LAST_PUSH = push_tg_ma10macd(force=0)
        except Exception as e:
            try:
                _MA10MACD_PUSH_LAST_ERROR = str(e)
            except Exception:
                pass
        time.sleep(interval)


def api_ma10macd_auto_status() -> JSONResponse:
    alive = False
    name = None
    try:
        alive = bool(_MA10MACD_PUSH_THREAD is not None and _MA10MACD_PUSH_THREAD.is_alive())
        name = _MA10MACD_PUSH_THREAD.name if _MA10MACD_PUSH_THREAD is not None else None
    except Exception:
        alive = False
        name = None
    s = _news_settings()
    payload = {
        "enabled_env": bool(MA10MACD_PUSH_ENABLED),
        "interval_sec": int(MA10MACD_PUSH_INTERVAL_SEC),
        "thread_alive": alive,
        "thread_name": name,
        "enabled_mod": _setting_bool(s, "push_ma10macd_enabled", True),
        "has_bot_token": bool((s.get("tg_bot_token") or "").strip()),
        "has_chat_id": bool((s.get("tg_chat_id") or "").strip()),
        "last_run_ts": _MA10MACD_PUSH_LAST_RUN_TS,
        "last_error": _MA10MACD_PUSH_LAST_ERROR,
        "last_push": _MA10MACD_PUSH_LAST_PUSH,
    }
    return JSONResponse(payload)


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


def _short_err(contract: str, e: Exception) -> str:
    c = (contract or "").strip().upper()
    msg = ""
    try:
        msg = str(e)
    except Exception:
        msg = repr(e)

    m = msg
    # REST GET failed: status=400 body=... url=... err=HTTP 400: {"label":"CONTRACT_NOT_FOUND"}
    try:
        if "REST GET failed" in m:
            status = None
            label = None
            try:
                import re
                ms = re.search(r"status=(\d+)", m)
                if ms:
                    status = ms.group(1)
                ml = re.search(r"\"label\"\s*:\s*\"([^\"]+)\"", m)
                if ml:
                    label = ml.group(1)
            except Exception:
                status = None
                label = None
            if label:
                return f"{c}: {label}" if c else f"{label}"
            if status:
                return f"{c}: REST status={status}" if c else f"REST status={status}"
            return f"{c}: REST failed" if c else "REST failed"
    except Exception:
        pass

    # 默认：截断过长错误，避免页面堆满
    try:
        if len(m) > 140:
            m = m[:140] + "…"
    except Exception:
        pass
    return f"{c}: {m}" if c else m


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
    out = [c for c, _ in pairs[: max(1, min(1000, limit))]]
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


@app.get("/api/healthz")
def api_healthz() -> JSONResponse:
    return JSONResponse(
        {
            "ok": True,
            "ts": int(time.time()),
            "app": str(APP_TITLE or ""),
        }
    )


@app.get("/api/config")
def api_config() -> JSONResponse:
    return JSONResponse(
        {
            "smartmoney_refresh_token": SMARTMONEY_REFRESH_TOKEN,
        }
    )


app.get("/api/whales/address/detail")(api_whales_address_detail)
app.get("/api/exchange/spot/large_trades")(api_exchange_spot_large_trades)
app.get("/api/exchange/spot/top_usdt_symbols")(api_exchange_spot_top_usdt_symbols)
app.post("/api/move3m/push")(api_move3m_push)
app.get("/api/move3m/log")(api_move3m_log_list)
app.post("/api/move3m/log")(api_move3m_log_add)
app.get("/api/ma10macd/list")(api_ma10macd_list)
app.get("/api/ma10macd/detail")(api_ma10macd_detail)
app.get("/api/ma10macd/push_now")(api_ma10macd_push_now)
app.get("/api/ma10macd/auto_status")(api_ma10macd_auto_status)
app.get("/api/sec/recent_13f")(api_sec_recent_13f)
app.get("/api/smartmoney/institutions")(api_smartmoney_institutions)
app.get("/api/smartmoney/institutions/meta")(api_smartmoney_institutions_meta)
app.post("/api/smartmoney/institutions/meta/import")(api_smartmoney_institutions_meta_import)
app.get("/api/smartmoney/institution")(api_smartmoney_institution_detail)
app.get("/api/smartmoney/stock/holders")(api_smartmoney_stock_holders)


# SEC EDGAR 公司财报分析接口
app.get("/api/smartmoney/stock/financials")(api_smartmoney_stock_financials)


app.get("/api/smartmoney/flows")(api_smartmoney_flows)
app.post("/api/smartmoney/refresh")(api_smartmoney_refresh)
app.get("/api/smartmoney/refresh/status")(api_smartmoney_refresh_status)
app.post("/api/smartmoney/refresh/manual")(api_smartmoney_refresh_manual)
app.get("/api/smartmoney/refresh/manual/status")(api_smartmoney_refresh_manual_status)
app.get("/api/smartmoney/ai")(api_smartmoney_ai)


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

    # MA10 回踩买入：后台定时推送（仅推“触发买入”，每轮金叉只触发一次）
    if MA10MACD_PUSH_ENABLED:
        global _MA10MACD_PUSH_THREAD
        with _MA10MACD_PUSH_THREAD_LOCK:
            if _MA10MACD_PUSH_THREAD is None or not _MA10MACD_PUSH_THREAD.is_alive():
                t_ma10 = threading.Thread(target=_ma10macd_push_loop, name="ma10macd_push", daemon=True)
                _MA10MACD_PUSH_THREAD = t_ma10
                t_ma10.start()

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
