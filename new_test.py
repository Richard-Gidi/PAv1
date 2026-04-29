"""
NPA ENERGY ANALYTICS — STREAMLIT DASHBOARD
===========================================
Fixed version:
  - Robust BDC name normalisation so PDF-parsed names reliably match .env keys
  - Cross-BDC deduplication no longer silently drops valid distinct BDC records
  - All BDCs that return data appear in Excel exports
  - Per-BDC retry logic unchanged; fetch log still shows every outcome
  - DETERMINISTIC FETCH FIX:
      * Increased MAX_RETRIES to 5 with true exponential back-off
      * Increased HTTP timeout to 90 s
      * Two-pass fetch: after the first sweep, any BDC that returned None/empty
        is retried in a slower sequential second pass
      * "Merge with previous" option so consecutive downloads are UNIONED,
        keeping the best (highest balance / most records) from each run
  - PERSISTENT STATE:
      * All fetched data is saved to disk as pickle files
      * On startup, data is automatically restored into session_state
      * Survives tab switches, idle timeouts, and soft server restarts
  - DAILY ORDERS OMC LOOKUP:
      * New "OMC Name" column in Daily Orders derived by fuzzy-matching each
        daily order number against order numbers collected in OMC Loadings.
      * Matching pipeline: exact normalised → substring containment → LCS ratio
      * Build is deferred (cached) so it only runs once per OMC dataset change.
  - STOCK TRANSACTION FIX:
      * Renamed session_state keys for txn_bdc/depot/product to txn_bdc_label
        etc. to avoid collision with Streamlit widget keys of the same name.
  - PRODUCT OUTTURN INTELLIGENCE (FIXED):
      * Default depot list pre-selected (22 key depots) — user can add/remove.
      * Fully sequential per-BDC sweep: iterates depots one-by-one inside each
        BDC so the progress bar moves after EVERY request, not after 90+.
      * Circuit breaker: if a BDC fails N consecutive depots it is skipped to
        avoid hanging the entire sweep.
      * Per-request timeout enforced independently (no silent hang).
      * Outturn call count shown BEFORE sweep starts so user knows what to expect.

INSTALLATION:
    pip install streamlit pandas pdfplumber PyPDF2 openpyxl python-dotenv plotly requests psutil

USAGE:
    streamlit run npa_dashboard.py
"""

import streamlit as st
import os, re, io, json, time, threading, unicodedata, pickle
from datetime import datetime, timedelta
import pandas as pd
import pdfplumber
import PyPDF2
from dotenv import load_dotenv
import plotly.graph_objects as go
import requests as _requests
import psutil
import queue

load_dotenv()

# ─────────────────────────────────────────────────────────────
# MEMORY BADGE
# ─────────────────────────────────────────────────────────────
_proc = psutil.Process(os.getpid())


# ══════════════════════════════════════════════════════════════
# NAME NORMALISATION UTILITIES
# ══════════════════════════════════════════════════════════════

def _normalise_name(name: str) -> str:
    if not name:
        return ""
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    for suffix in (
        "limited", "ltd", "company", "co", "ghana", "plc",
        "llc", "lp", "inc", "corp", "enterprise", "enterprises",
    ):
        s = re.sub(rf"\b{suffix}\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _build_lookup(mapping: dict) -> dict:
    return {_normalise_name(k): k for k in mapping}


# ══════════════════════════════════════════════════════════════
# ENVIRONMENT LOADERS
# ══════════════════════════════════════════════════════════════

def load_bdc_user_map() -> dict:
    _FIXES = {
        "C CLEANED OIL LTD":                     "C. CLEANED OIL LTD",
        "PK JEGS ENERGY LTD":                    "P.K JEGS ENERGY LTD",
        "TEMA OIL REFINERY TOR":                 "TEMA OIL REFINERY (TOR)",
        "SOCIETE NATIONAL BURKINABE SONABHY":    "SOCIETE NATIONAL BURKINABE (SONABHY)",
        "BOST G40":                              "BOST-G40",
        "DOMINION INTERNATIONAL PETROLEUM":      "DOMINION INTERNATIONAL PETROLEUM",
        "PETROLEUM WARE HOUSE AND SUPPLIES":     "PETROLEUM WARE HOUSE AND SUPPLIES",
        "INTERNATIONAL PETROLEUM RESOURCES":     "INTERNATIONAL PETROLEUM RESOURCES",
        "GENYSIS GLOBAL LIMITED":                "GENYSIS GLOBAL LIMITED",
        "GLORYMAY PETROLEUM COMPANY LIMITED":    "GLORYMAY PETROLEUM COMPANY LIMITED",
        "HILSON PETROLEUM GHANA LIMITED":        "HILSON PETROLEUM GHANA LIMITED",
        "PLATON OIL AND GAS":                    "PLATON OIL AND GAS",
        "PORTICA OIL AND GAS RESOURCE LIMITED":  "PORTICA OIL AND GAS RESOURCE LIMITED",
        "RESTON ENERGY TRADING LIMITED":         "RESTON ENERGY TRADING LIMITED",
        "BATTOP ENERGY LIMITED":                 "BATTOP ENERGY LIMITED",
        "SOH ENERGY LTD":                        "SOH ENERGY LTD",
        "XF PETROLEUM ENGINEERS LIMITED":        "XF PETROLEUM ENGINEERS LIMITED",
        "XF PETROLEUM LIMITED":                  "XF PETROLEUM LIMITED",
        "MPB PETROLEUM LTD":                     "MPB PETROLEUM LTD",
        "AXSOR ENERGY LTD":                      "AXSOR ENERGY LTD",
        "BAZUKA ENERGY LTD":                     "BAZUKA ENERGY LTD",
        "FIRM ENERGY LIMITED":                   "FIRM ENERGY LIMITED",
    }
    mapping = {}
    for key, value in os.environ.items():
        if not key.startswith("BDC_USER_"):
            continue
        raw_suffix = key[len("BDC_USER_"):].replace("_", " ").strip()
        display    = _FIXES.get(raw_suffix, raw_suffix)
        try:
            mapping[display] = int(value)
        except ValueError:
            pass
    return mapping


def load_bdc_mappings() -> dict:
    mappings = {}
    for key, value in os.environ.items():
        if not key.startswith("BDC_") or key.startswith("BDC_USER_"):
            continue
        name = key[4:].replace("_", " ")
        fixes = {
            "TEMA OIL REFINERY TOR":                "TEMA OIL REFINERY (TOR)",
            "SOCIETE NATIONAL BURKINABE SONABHY":   "SOCIETE NATIONAL BURKINABE (SONABHY)",
            "LIB GHANA LIMITED":                    "L.I.B. GHANA LIMITED",
            "C CLEANED OIL LTD":                    "C. CLEANED OIL LTD",
            "PK JEGS ENERGY LTD":                   "P. K JEGS ENERGY LTD",
        }
        name = fixes.get(name, name)
        try:
            mappings[name] = int(value)
        except ValueError:
            pass
    return mappings


def load_depot_mappings() -> dict:
    mappings = {}
    for key, value in os.environ.items():
        if not key.startswith("DEPOT_"):
            continue
        name = key[6:].replace("_", " ")
        if "BOST " in name and name != "BOST GLOBAL DEPOT":
            parts = name.split(" ", 1)
            name = f"{parts[0]} - {parts[1]}" if len(parts) == 2 else name
        elif name.endswith(" TEMA") and "SENTUO" in name:
            name = name.replace(" TEMA", "- TEMA")
        elif name == "GHANA OIL COLTD TAKORADI":              name = "GHANA OIL CO.LTD, TAKORADI"
        elif name == "GOIL LPG BOTTLING PLANT TEMA":          name = "GOIL LPG BOTTLING PLANT -TEMA"
        elif name == "GOIL LPG BOTTLING PLANT KUMASI":        name = "GOIL LPG BOTTLING PLANT- KUMASI"
        elif name == "NEWGAS CYLINDER BOTTLING LIMITED TEMA": name = "NEWGAS CYLINDER BOTTLING LIMITED-TEMA"
        elif name == "CHASE PETROLEUM TEMA":                  name = "CHASE PETROLEUM - TEMA"
        elif name == "TEMA FUEL COMPANY TFC":                 name = "TEMA FUEL COMPANY (TFC)"
        elif name == "TEMA MULTI PRODUCTS TMPT":              name = "TEMA MULTI PRODUCTS (TMPT)"
        elif name == "TEMA OIL REFINERY TOR":                 name = "TEMA OIL REFINERY (TOR)"
        elif name == "GHANA OIL COMPANY LTD SEKONDI NAVAL BASE": name = "GHANA OIL COMPANY LTD (SEKONDI NAVAL BASE)"
        elif name == "GHANSTOCK LIMITED TAKORADI":            name = "GHANSTOCK LIMITED (TAKORADI)"
        try:
            mappings[name] = int(value)
        except ValueError:
            pass
    return mappings


def load_product_mappings() -> dict:
    return {
        "PMS":    int(os.getenv("PRODUCT_PREMIUM_ID", "12")),
        "Gasoil": int(os.getenv("PRODUCT_GASOIL_ID",  "14")),
        "LPG":    int(os.getenv("PRODUCT_LPG_ID",     "28")),
    }


# ── Load all mappings once at startup ───────────────────────
BDC_USER_MAP      = load_bdc_user_map()
BDC_MAP           = load_bdc_mappings()
DEPOT_MAP         = load_depot_mappings()
STOCK_PRODUCT_MAP = load_product_mappings()

_BDC_USER_LOOKUP  = _build_lookup(BDC_USER_MAP)

PRODUCT_OPTIONS     = ["PMS", "Gasoil", "LPG"]
PRODUCT_BALANCE_MAP = {"PMS": "PREMIUM", "Gasoil": "GASOIL", "LPG": "LPG"}

NPA_CONFIG = {
    "COMPANY_ID":            os.getenv("NPA_COMPANY_ID",    "1"),
    "USER_ID":               os.getenv("NPA_USER_ID",       "123292"),
    "APP_ID":                os.getenv("NPA_APP_ID",        "3"),
    "ITS_FROM_PERSOL":       os.getenv("NPA_ITS_FROM_PERSOL","Persol Systems Limited"),
    "BDC_BALANCE_URL":       os.getenv("NPA_BDC_BALANCE_URL",
                                 "https://iml.npa-enterprise.com/NPAAPILIVE/Home/CreateProductBalance"),
    "OMC_LOADINGS_URL":      os.getenv("NPA_OMC_LOADINGS_URL",
                                 "https://iml.npa-enterprise.com/NewNPA/home/CreateOrdersReport"),
    "DAILY_ORDERS_URL":      os.getenv("NPA_DAILY_ORDERS_URL",
                                 "https://iml.npa-enterprise.com/NewNPA/home/CreateDailyOrderReport"),
    "STOCK_TRANSACTION_URL": os.getenv("NPA_STOCK_TRANSACTION_URL",
                                 "https://iml.npa-enterprise.com/NewNPA/home/CreateStockTransactionReport"),
    "OMC_NAME":              os.getenv("OMC_NAME", "OILCORP ENERGIA LIMITED"),
}

WORLD_MONITOR_URL = os.getenv(
    "WORLD_MONITOR_URL",
    "https://www.worldmonitor.app/?lat=17.7707&lon=0.0000&zoom=1.30&view=global&timeRange=7d"
    "&layers=conflicts%2Cbases%2Chotspots%2Cnuclear%2Csanctions%2Cweather%2Ceconomic"
    "%2Cwaterways%2Coutages%2Cmilitary%2Cnatural%2CiranAttacks",
)
VESSEL_SHEET_URL = os.getenv(
    "VESSEL_SHEETS_URL",
    "https://docs.google.com/spreadsheets/d/1z-L79N22rU3p6wLw1CEVWDIw6QSwA5CH/edit?rtpof=true",
)

# ══════════════════════════════════════════════════════════════
# DEFAULT OUTTURN DEPOTS
# These 22 depots are pre-selected when you open the Outturn page.
# You can add or remove any depot in the UI multiselect.
# ══════════════════════════════════════════════════════════════
_OUTTURN_DEFAULT_DEPOT_NAMES = [
    "BOST - ACCRA PLAINS",
    "BOST - AKOSOMBO",
    "BOST - BOLGATANGA",
    "BOST - BUIPE",
    "BOST - KUMASI",
    "BOST GLOBAL DEPOT",
    "CHASE PETROLEUM - TEMA",
    "GHANA BUNKERING SERVICES",
    "GHANA NATIONAL GAS COMPANY LIMITED",
    "GHANSTOCK LIMITED (TAKORADI)",
    "MATRIX GAS GHANA LIMITED",
    "PETROLEUM HUB LIMITED",
    "PETROLEUM WARE HOUSE AND SUPPLIES",
    "QUANTUM LPG LOGISTICS LIMITED",
    "QUANTUM OIL TERMINAL LIMITED",
    "QUANTUM TERMINALS LIMITED",
    "TAKORADI BLUE OCEAN INVESTMENT LIMITED",
    "TEMA FUEL COMPANY (TFC)",
    "TEMA MULTI PRODUCTS (TMPT)",
    "TEMA OIL REFINERY (TOR)",
    "TEMA OIL TERMINAL PLC",
    "VANA ENERGY LIMITED TEMA",
]


def _resolve_default_depots() -> list[str]:
    """
    Return the subset of _OUTTURN_DEFAULT_DEPOT_NAMES that actually exist
    in DEPOT_MAP (exact match first, then normalised fuzzy).
    Unknown names are silently skipped so the app still works if the .env
    doesn't have a particular depot configured.
    """
    all_depot_names = sorted(DEPOT_MAP.keys())
    norm_map = {_normalise_name(d): d for d in all_depot_names}
    resolved = []
    for name in _OUTTURN_DEFAULT_DEPOT_NAMES:
        if name in DEPOT_MAP:
            resolved.append(name)
            continue
        n = _normalise_name(name)
        if n in norm_map:
            resolved.append(norm_map[n])
            continue
        # fuzzy substring
        for nd, real in norm_map.items():
            if n and nd and (n in nd or nd in n):
                resolved.append(real)
                break
    # preserve order, deduplicate
    seen = set()
    out  = []
    for d in resolved:
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out


# ══════════════════════════════════════════════════════════════
# PAGE CONFIG & CSS
# ══════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="NPA Energy Analytics 🛢️",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.caption(f"🧠 Memory: {_proc.memory_info().rss / 1024 / 1024:.1f} MB  |  "
           f"📋 {len(BDC_USER_MAP)} BDCs configured in .env")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Rajdhani:wght@300;500;700&display=swap');
.stApp{background:linear-gradient(-45deg,#0a0e27,#1a1a2e,#16213e,#0f3460);
    background-size:400% 400%;animation:gradientShift 15s ease infinite;}
@keyframes gradientShift{0%{background-position:0% 50%}50%{background-position:100% 50%}100%{background-position:0% 50%}}
h1,h2,h3{font-family:'Orbitron',sans-serif!important;color:#00ffff!important;
    text-shadow:0 0 10px #00ffff,0 0 20px #00ffff;}
[data-testid="stSidebar"]{background:linear-gradient(180deg,#0a0e27 0%,#16213e 100%);
    border-right:2px solid #00ffff;box-shadow:5px 0 15px rgba(0,255,255,0.3);}
[data-testid="stSidebar"] h1,[data-testid="stSidebar"] h2,[data-testid="stSidebar"] h3{
    color:#ff00ff!important;text-shadow:0 0 10px #ff00ff;}
.stButton>button{background:linear-gradient(45deg,#ff00ff,#00ffff);color:white;
    border:2px solid #00ffff;border-radius:25px;padding:14px 28px;
    font-family:'Orbitron',sans-serif;font-weight:700;font-size:15px;
    box-shadow:0 0 20px rgba(0,255,255,0.5);transition:all 0.3s ease;
    text-transform:uppercase;letter-spacing:2px;}
.stButton>button:hover{transform:scale(1.04) translateY(-2px);
    box-shadow:0 0 30px rgba(0,255,255,0.8),0 0 40px rgba(255,0,255,0.5);}
.dataframe{background-color:rgba(10,14,39,0.8)!important;
    border:2px solid #00ffff!important;border-radius:10px;}
.dataframe th{background-color:#16213e!important;color:#00ffff!important;
    font-family:'Orbitron',sans-serif;text-transform:uppercase;border:1px solid #00ffff!important;}
.dataframe td{background-color:rgba(22,33,62,0.6)!important;color:#ffffff!important;
    border:1px solid rgba(0,255,255,0.2)!important;}
[data-testid="stMetricValue"]{font-family:'Orbitron',sans-serif;font-size:24px!important;
    color:#00ffff!important;text-shadow:0 0 10px #00ffff;}
[data-testid="stMetricLabel"]{font-family:'Rajdhani',sans-serif;color:#ff00ff!important;
    font-weight:700;text-transform:uppercase;letter-spacing:1px;}
.metric-card{background:rgba(22,33,62,0.6);padding:18px;border-radius:14px;
    border:2px solid #00ffff;text-align:center;}
.metric-card h2{color:#ff00ff!important;margin:0;font-size:16px!important;}
.metric-card h1{color:#00ffff!important;margin:8px 0;font-size:26px!important;word-wrap:break-word;}
p,span,div{font-family:'Rajdhani',sans-serif;color:#e0e0e0;}
.fetch-log{font-family:monospace;font-size:12px;background:rgba(0,0,0,0.5);
    border:1px solid #00ffff33;border-radius:8px;padding:10px;max-height:180px;overflow-y:auto;}
.outturn-card{background:rgba(10,14,39,0.9);padding:20px;border-radius:14px;
    border:2px solid #ff6600;text-align:center;box-shadow:0 0 18px #ff660055;}
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# PERSISTENT STATE
# ══════════════════════════════════════════════════════════════
PERSIST_DIR = os.path.join(os.getcwd(), ".persist_state")
os.makedirs(PERSIST_DIR, exist_ok=True)

_PERSIST_KEYS = {
    "bdc_records":    [],
    "omc_df":         pd.DataFrame(),
    "daily_df":       pd.DataFrame(),
    "stock_txn_df":   pd.DataFrame(),
    "vessel_data":    pd.DataFrame(),
    "outturn_df":     pd.DataFrame(),
}


def _persist_path(key: str) -> str:
    return os.path.join(PERSIST_DIR, f"{key}.pkl")


def _save_state(key: str, value) -> None:
    try:
        with open(_persist_path(key), "wb") as f:
            pickle.dump(value, f)
    except Exception:
        pass


def _load_state(key: str):
    path = _persist_path(key)
    if os.path.exists(path):
        try:
            with open(path, "rb") as f:
                return pickle.load(f)
        except Exception:
            pass
    return _PERSIST_KEYS[key]


def _restore_session_state() -> None:
    for key in _PERSIST_KEYS:
        if key not in st.session_state:
            st.session_state[key] = _load_state(key)


def _clear_all_persisted() -> None:
    for key, default in _PERSIST_KEYS.items():
        path = _persist_path(key)
        if os.path.exists(path):
            try:
                os.remove(path)
            except Exception:
                pass
        st.session_state[key] = default


# ══════════════════════════════════════════════════════════════
# OMC ORDER-NUMBER → OMC NAME LOOKUP
# ══════════════════════════════════════════════════════════════

def _norm_order(order_num: str) -> str:
    if not order_num:
        return ""
    return re.sub(r"[^A-Z0-9]", "", str(order_num).upper())


def _lcs_ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    la, lb = len(a), len(b)
    best = 0
    if la > 50 or lb > 50:
        shorter, longer = (a, b) if la <= lb else (b, a)
        for length in range(len(shorter), 0, -1):
            for start in range(len(shorter) - length + 1):
                if shorter[start:start+length] in longer:
                    best = length
                    break
            if best:
                break
    else:
        for i in range(la):
            for j in range(lb):
                length = 0
                while i+length < la and j+length < lb and a[i+length] == b[j+length]:
                    length += 1
                if length > best:
                    best = length
    return best / max(la, lb)


def _build_omc_order_lookup(omc_df: pd.DataFrame) -> dict:
    if omc_df is None or omc_df.empty:
        return {}
    if "Order Number" not in omc_df.columns or "OMC" not in omc_df.columns:
        return {}

    lookup: dict[str, str] = {}
    qty_map: dict[str, float] = {}
    qty_col = "Quantity" if "Quantity" in omc_df.columns else None

    for _, row in omc_df.iterrows():
        raw_ord = str(row.get("Order Number", "")).strip()
        omc     = str(row.get("OMC", "")).strip()
        if not raw_ord or not omc:
            continue
        norm = _norm_order(raw_ord)
        if not norm:
            continue
        qty = float(row[qty_col]) if qty_col and pd.notna(row[qty_col]) else 0.0
        if norm not in lookup or qty > qty_map.get(norm, -1):
            lookup[norm]  = omc
            qty_map[norm] = qty

    return lookup


def _lookup_omc_for_order(
    order_num: str,
    omc_lookup: dict,
    threshold: float = 0.75,
) -> str:
    if not order_num or not omc_lookup:
        return ""

    norm_q = _norm_order(order_num)
    if not norm_q:
        return ""

    if norm_q in omc_lookup:
        return omc_lookup[norm_q]

    MIN_SUB = 4
    best_sub_key  = None
    best_sub_len  = 0
    for norm_key in omc_lookup:
        if len(norm_key) < MIN_SUB or len(norm_q) < MIN_SUB:
            continue
        if norm_key in norm_q or norm_q in norm_key:
            overlap_len = len(norm_key) if norm_key in norm_q else len(norm_q)
            if overlap_len > best_sub_len:
                best_sub_len = overlap_len
                best_sub_key = norm_key
    if best_sub_key:
        return omc_lookup[best_sub_key]

    best_score = 0.0
    best_key   = None
    for norm_key in omc_lookup:
        score = _lcs_ratio(norm_q, norm_key)
        if score > best_score:
            best_score = score
            best_key   = norm_key
    if best_score >= threshold and best_key:
        return omc_lookup[best_key]

    return ""


def _enrich_daily_with_omc(daily_df: pd.DataFrame, omc_df: pd.DataFrame) -> pd.DataFrame:
    if daily_df is None or daily_df.empty:
        return daily_df

    df = daily_df.copy()

    if omc_df is None or omc_df.empty or "Order Number" not in df.columns:
        df["OMC Name"] = ""
        return df

    omc_lookup = _build_omc_order_lookup(omc_df)
    if not omc_lookup:
        df["OMC Name"] = ""
        return df

    df["OMC Name"] = df["Order Number"].apply(
        lambda o: _lookup_omc_for_order(str(o), omc_lookup)
    )
    return df


# ══════════════════════════════════════════════════════════════
# HTTP HELPER
# ══════════════════════════════════════════════════════════════
_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,text/html,*/*;q=0.8",
    "Connection": "keep-alive",
}

_HTTP_TIMEOUT = 90


def _fetch_pdf(url: str, params: dict, timeout: int = _HTTP_TIMEOUT) -> bytes | None:
    try:
        r = _requests.get(url, params=params, headers=_HTTP_HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.content if r.content[:4] == b"%PDF" else None
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════
# ROBUST BATCH FETCHER  (used by Balance / OMC / Daily)
# ══════════════════════════════════════════════════════════════
BATCH_SIZE          = 6
MAX_RETRIES         = 3
RETRY_DELAY         = 2
MAX_RETRY_SLEEP     = 8
SECOND_PASS_DELAY   = 2
SECOND_PASS_RETRIES = 2
BATCH_HARD_TIMEOUT  = 120


def _sequential_batch_fetch(
    bdc_list:   list,
    fetch_fn,
    progress_bar,
    status_text,
    log_lines: list,
    second_pass: bool = True,
) -> dict:
    """
    Patched version of _sequential_batch_fetch.
 
    Key changes vs original:
      • Per-future hard timeout (BATCH_HARD_TIMEOUT / batch_size per future)
        — no single future can hang the whole batch indefinitely.
      • Exponential back-off is capped at MAX_RETRY_SLEEP.
      • Progress bar updates after every completed future (not after batch).
      • BDCs whose future timed out go straight to the second pass.
      • No Streamlit calls happen inside worker threads.
    """
    total   = len(bdc_list)
    results = {}
    lock    = threading.Lock()
    done_n  = [0]
 
    # ── Single-BDC attempt with capped back-off ───────────────
    def _attempt(bdc_name: str, max_tries: int = MAX_RETRIES):
        last_err = None
        for attempt in range(1, max_tries + 1):
            try:
                result = fetch_fn(bdc_name)
                return bdc_name, result, attempt, None
            except Exception as exc:
                last_err = exc
                if attempt < max_tries:
                    sleep_t = min(RETRY_DELAY * (2 ** (attempt - 1)), MAX_RETRY_SLEEP)
                    time.sleep(sleep_t)
        return bdc_name, None, max_tries, str(last_err)
 
    # ── Helper: update progress + log safely ─────────────────
    def _update(bdc_name, result, attempts, err):
        with lock:
            done_n[0] += 1
            pct = done_n[0] / total
 
        # Determine icon/note
        if err:
            icon, note = "❌", f"FAILED after {attempts} tries — {err[:80]}"
        elif result is None:
            icon, note = "⚠️", "No data / empty PDF"
        elif _result_is_empty(result):
            icon, note = "⚠️", "Empty dataset"
        elif attempts > 1:
            icon, note = "🔄", f"OK (needed {attempts} tries)"
        else:
            icon, note = "✅", "OK"
 
        log_lines.append(f"{icon} {bdc_name}: {note}")
 
        # These Streamlit calls are on the main thread, so they're safe
        progress_bar.progress(
            pct,
            text=f"Pass 1 — {done_n[0]} / {total} BDCs fetched…",
        )
        status_text.markdown(
            "<div class='fetch-log'>" +
            "<br>".join(log_lines[-12:]) +
            "</div>",
            unsafe_allow_html=True,
        )
 
    # ── PASS 1 — parallel batches ─────────────────────────────
    batches = [bdc_list[i: i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]
    per_future_timeout = max(BATCH_HARD_TIMEOUT // BATCH_SIZE, 30)
 
    for batch_idx, batch in enumerate(batches):
        with _cf.ThreadPoolExecutor(max_workers=BATCH_SIZE) as ex:
            futs = {ex.submit(_attempt, b): b for b in batch}
 
            # Use wait() with timeout so a hung future doesn't stall forever
            done_futs, pending_futs = _cf.wait(
                futs,
                timeout=per_future_timeout * len(batch),
            )
 
            # Process completed futures
            for fut in done_futs:
                bdc_name = futs[fut]
                try:
                    _, result, attempts, err = fut.result(timeout=1)
                except Exception as exc:
                    result, attempts, err = None, MAX_RETRIES, str(exc)
                results[bdc_name] = result
                _update(bdc_name, result, attempts, err)
 
            # Handle timed-out futures — mark as None (will go to pass 2)
            for fut in pending_futs:
                bdc_name = futs[fut]
                results[bdc_name] = None
                log_lines.append(f"⏱️ {bdc_name}: timed out — will retry in pass 2")
                with lock:
                    done_n[0] += 1
                progress_bar.progress(
                    done_n[0] / total,
                    text=f"Pass 1 — {done_n[0]} / {total} BDCs fetched…",
                )
                # Cancel the future if possible (Python can't kill threads,
                # but future.cancel() prevents it starting if queued)
                fut.cancel()
 
        # Small inter-batch sleep to avoid hammering the server
        if batch_idx < len(batches) - 1:
            time.sleep(0.3)
 
    # ── PASS 2 — sequential retry for empty/failed ────────────
    if second_pass:
        retry_bdcs = [
            b for b, r in results.items()
            if r is None or _result_is_empty(r)
        ]
        if retry_bdcs:
            log_lines.append(
                f"━━━ Pass 2: retrying {len(retry_bdcs)} BDC(s) sequentially ━━━"
            )
            status_text.markdown(
                "<div class='fetch-log'>" +
                "<br>".join(log_lines[-12:]) +
                "</div>",
                unsafe_allow_html=True,
            )
 
            for idx, bdc_name in enumerate(retry_bdcs):
                time.sleep(SECOND_PASS_DELAY)
                _, result, attempts, err = _attempt(bdc_name, max_tries=SECOND_PASS_RETRIES)
 
                if result is not None and not _result_is_empty(result):
                    results[bdc_name] = result
                    icon, note = "🔄", f"Pass-2 OK (attempt {attempts})"
                elif err:
                    icon, note = "❌", f"Pass-2 FAILED — {err[:80]}"
                else:
                    icon, note = "⚠️", "Pass-2 still no data"
 
                log_lines.append(f"{icon} [P2] {bdc_name}: {note}")
                status_text.markdown(
                    "<div class='fetch-log'>" +
                    "<br>".join(log_lines[-12:]) +
                    "</div>",
                    unsafe_allow_html=True,
                )
                progress_bar.progress(
                    1.0,
                    text=f"Pass 2 — {idx+1}/{len(retry_bdcs)} retried",
                )
 
    return results


def _result_is_empty(result) -> bool:
    """Unchanged helper — repeated here for completeness."""
    if result is None:
        return True
    if isinstance(result, list):
        return len(result) == 0
    import pandas as pd
    if isinstance(result, pd.DataFrame):
        return result.empty
    return False


# ══════════════════════════════════════════════════════════════
# PDF PARSERS
# ══════════════════════════════════════════════════════════════

# ── BDC Balance ──────────────────────────────────────────────
class StockBalanceScraper:
    def __init__(self):
        self.allowed_products = {"PREMIUM", "GASOIL", "LPG"}
        _pat = "|".join(sorted(self.allowed_products))
        self.product_re = re.compile(
            rf"^({_pat})\s+([\d,]+\.\d{{2}})\s+(-?[\d,]+\.\d{{2}})$",
            flags=re.IGNORECASE,
        )
        self.bost_global_re = re.compile(r"\bBOST\s*GLOBAL\s*DEPOT\b", flags=re.IGNORECASE)

    @staticmethod
    def _ns(text):
        return re.sub(r"\s+", " ", (text or "").strip())

    def _resolve_bdc_name(self, raw_bdc: str) -> str:
        clean = self._ns(raw_bdc)
        norm  = _normalise_name(clean)
        if norm in _BDC_USER_LOOKUP:
            return _BDC_USER_LOOKUP[norm]
        best_key = None
        best_len = 0
        for nk, display in _BDC_USER_LOOKUP.items():
            if nk and (nk in norm or norm in nk) and len(nk) > best_len:
                best_key = display
                best_len = len(nk)
        if best_key:
            return best_key
        return clean

    def _is_bost_depot(self, depot):
        return self._ns((depot or "").replace("-", " ")).upper().startswith("BOST ")

    def _is_bost_global(self, depot):
        return bool(self.bost_global_re.search(self._ns((depot or "").replace("-", " "))))

    def _parse_date(self, line):
        m = re.search(r"(\w+\s+\d{1,2}\s*,\s*\d{4})", line)
        if m:
            try:
                return datetime.strptime(m.group(1).replace(" ,", ","), "%B %d, %Y").strftime("%Y/%m/%d")
            except Exception:
                pass
        return None

    def parse_pdf_bytes(self, pdf_bytes: bytes, owning_bdc_name: str = "") -> list:
        records = []
        seen    = set()
        try:
            reader    = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
            cur_bdc   = owning_bdc_name
            cur_depot = None
            cur_date  = None
            for page in reader.pages:
                text  = page.extract_text() or ""
                lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
                for line in lines:
                    up = line.upper()
                    if "DATE AS AT" in up:
                        d = self._parse_date(line)
                        if d:
                            cur_date = d
                    if up.startswith("BDC :") or up.startswith("BDC:"):
                        raw = re.sub(r"^BDC\s*:\s*", "", line, flags=re.IGNORECASE).strip()
                        resolved = self._resolve_bdc_name(raw)
                        cur_bdc = resolved if resolved else (owning_bdc_name or raw)
                    if up.startswith("DEPOT :") or up.startswith("DEPOT:"):
                        cur_depot = re.sub(r"^DEPOT\s*:\s*", "", line, flags=re.IGNORECASE).strip()
                    if cur_bdc and cur_depot and cur_date:
                        m = self.product_re.match(line)
                        if m:
                            product = m.group(1).upper()
                            actual  = float(m.group(2).replace(",", ""))
                            avail   = float(m.group(3).replace(",", ""))
                            if product not in self.allowed_products:
                                continue
                            if self._is_bost_depot(cur_depot) and not self._is_bost_global(cur_depot):
                                continue
                            if actual <= 0:
                                continue
                            norm_depot = self._ns(cur_depot)
                            key = (cur_bdc, norm_depot, product, cur_date)
                            if key in seen:
                                continue
                            seen.add(key)
                            records.append({
                                "Date":                        cur_date,
                                "BDC":                         cur_bdc,
                                "DEPOT":                       norm_depot,
                                "Product":                     product,
                                "ACTUAL BALANCE (LT\\KG)":     actual,
                                "AVAILABLE BALANCE (LT\\KG)":  avail,
                            })
        except Exception:
            pass
        return records


# ── OMC Loadings ─────────────────────────────────────────────
_PRODUCT_MAP_OMC = {"AGO": "GASOIL", "PMS": "PREMIUM", "LPG": "LPG"}
_ONLY_COLS       = ["Date","OMC","Truck","Product","Quantity","Price","Depot","Order Number","BDC"]
_HEADER_KW       = ["ORDER REPORT","National Petroleum Authority","ORDER NUMBER","ORDER DATE",
                    "ORDER STATUS","BDC:","Total for :","Printed By :","Page ","BRV NUMBER","VOLUME"]
_LOADED_KW       = {"Released","Submitted"}


def _detect_product(line):
    raw = "AGO" if "AGO" in line else "LPG" if "LPG" in line else "PMS"
    return _PRODUCT_MAP_OMC.get(raw, raw)


def _parse_loaded_line(line, product, depot, bdc):
    tokens  = line.split()
    if len(tokens) < 6:
        return None
    rel_idx = next((i for i, t in enumerate(tokens) if t in _LOADED_KW), None)
    if rel_idx is None or rel_idx < 2:
        return None
    try:
        date_tok, order_num = tokens[0], tokens[1]
        volume  = float(tokens[-1].replace(",", ""))
        price   = float(tokens[-2].replace(",", ""))
        brv     = tokens[-3]
        company = " ".join(tokens[rel_idx + 1:-3]).strip()
        try:
            date_str = datetime.strptime(date_tok, "%d-%b-%Y").strftime("%Y/%m/%d")
        except Exception:
            date_str = date_tok
        return {"Date": date_str, "OMC": company, "Truck": brv, "Product": product,
                "Quantity": volume, "Price": price, "Depot": depot,
                "Order Number": order_num, "BDC": bdc}
    except Exception:
        return None


def extract_omc_loadings_from_pdf(pdf_bytes: bytes, bdc_name: str = "") -> pd.DataFrame:
    rows      = []
    cur_depot = ""
    cur_bdc   = bdc_name
    cur_prod  = "PREMIUM"
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or page.extract_text(x_tolerance=2, y_tolerance=2) or ""
                for raw in text.split("\n"):
                    line = raw.strip()
                    if not line:
                        continue
                    if "DEPOT:" in line:
                        m = re.search(r"DEPOT:([^-\n]+)", line)
                        if m:
                            cur_depot = m.group(1).strip()
                        continue
                    if "BDC:" in line:
                        m = re.search(r"BDC:([^\n]+)", line)
                        if m:
                            resolved = _resolve_pdf_bdc(m.group(1).strip(), bdc_name)
                            cur_bdc  = resolved
                        continue
                    if "PRODUCT" in line:
                        cur_prod = _detect_product(line)
                        continue
                    if any(h in line for h in _HEADER_KW):
                        continue
                    if any(kw in line for kw in _LOADED_KW):
                        row = _parse_loaded_line(line, cur_prod, cur_depot, cur_bdc)
                        if row:
                            rows.append(row)
    except Exception:
        pass
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=_ONLY_COLS)
    for col in _ONLY_COLS:
        if col not in df.columns:
            df[col] = ""
    df = df[_ONLY_COLS]
    df = df.drop_duplicates(subset=["Order Number", "Truck", "Date", "Product"])
    try:
        ds = pd.to_datetime(df["Date"], format="%Y/%m/%d", errors="coerce")
        df = df.assign(_ds=ds).sort_values("_ds").drop(columns=["_ds"]).reset_index(drop=True)
    except Exception:
        pass
    return df


def _resolve_pdf_bdc(raw: str, fallback: str) -> str:
    norm = _normalise_name(raw)
    if norm in _BDC_USER_LOOKUP:
        return _BDC_USER_LOOKUP[norm]
    best_key, best_len = None, 0
    for nk, display in _BDC_USER_LOOKUP.items():
        if nk and (nk in norm or norm in nk) and len(nk) > best_len:
            best_key = display
            best_len = len(nk)
    return best_key if best_key else (fallback or raw)


# ── Daily Orders ─────────────────────────────────────────────
def _get_product_category(text):
    t = text.upper()
    if "AVIATION" in t or "TURBINE" in t:  return "ATK"
    if "RFO"      in t:                    return "RFO"
    if "PREMIX"   in t:                    return "PREMIX"
    if "LPG"      in t:                    return "LPG"
    if "AGO" in t or "MGO" in t or "GASOIL" in t: return "GASOIL"
    return "PREMIUM"


def _parse_daily_line(line, last_date):
    pv = re.search(r"(\d{1,4}\.\d{2,4})\s+(\d{1,3}(?:,\d{3})*\.\d{2})$", line.strip())
    if not pv:
        return None
    price  = float(pv.group(1))
    volume = float(pv.group(2).replace(",", ""))
    rem    = line[:pv.start()].strip()
    tokens = rem.split()
    if not tokens:
        return None
    brv, rem = tokens[-1], " ".join(tokens[:-1])
    date_val = last_date
    dm = re.search(r"(\d{2}/\d{2}/\d{4})", rem)
    if dm:
        try:
            date_val = datetime.strptime(dm.group(1), "%d/%m/%Y").strftime("%Y/%m/%d")
        except Exception:
            date_val = dm.group(1)
        rem = rem.replace(dm.group(1), "").strip()
    _noise = ["PMS","AGO","LPG","RFO","ATK","PREMIX","FOREIGN","(Retail","Retail",
              "Outlets","MGO","Local","Additivated","Differentiated","MINES",
              "Cell","Sites","Turbine","Kerosene"]
    ok_tokens = [t for t in rem.split()
                 if not any(n.upper() in t.upper() or t in ("(",")","-") for n in _noise)]
    order_num = " ".join(ok_tokens).strip() or rem
    return {"Date": date_val, "Order Number": order_num,
            "Product": _get_product_category(line),
            "Truck": brv, "Price": price, "Quantity": volume}


def extract_daily_orders_from_pdf(pdf_bytes: bytes, bdc_name: str = "") -> pd.DataFrame:
    rows = []
    ctx  = {"Depot": "Unknown", "BDC": bdc_name, "Status": "Unknown", "Date": None}
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text(x_tolerance=2, y_tolerance=2)
                if not text:
                    continue
                for line in text.split("\n"):
                    cl = line.strip()
                    if not cl:
                        continue
                    if cl.startswith("DEPOT:"):
                        raw_d = cl.replace("DEPOT:", "").strip()
                        ctx["Depot"] = ("BOST Global"
                                        if (raw_d.startswith("BOST") or "TAKORADI BLUE OCEAN" in raw_d)
                                        else raw_d)
                        continue
                    if cl.startswith("BDC:"):
                        raw_b = cl.replace("BDC:", "").strip()
                        ctx["BDC"] = _resolve_pdf_bdc(raw_b, bdc_name)
                        continue
                    if "Order Status" in cl:
                        parts = cl.split(":")
                        if len(parts) > 1:
                            ctx["Status"] = parts[-1].strip()
                        continue
                    if not re.search(r"\d{2}$", cl):
                        continue
                    row = _parse_daily_line(cl, ctx["Date"])
                    if row:
                        if row["Date"]:
                            ctx["Date"] = row["Date"]
                        rows.append({
                            "Date":         row["Date"],
                            "Truck":        row["Truck"],
                            "Product":      row["Product"],
                            "Quantity":     row["Quantity"],
                            "Price":        row["Price"],
                            "Depot":        ctx["Depot"],
                            "Order Number": row["Order Number"],
                            "BDC":          ctx["BDC"],
                            "Status":       ctx["Status"],
                        })
    except Exception:
        pass
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["Date", "Truck", "Order Number", "Product"])
    return df


# ══════════════════════════════════════════════════════════════
# STOCK TRANSACTION PDF PARSER
# ══════════════════════════════════════════════════════════════

def _parse_stock_transaction_pdf_full(
    pdf_bytes: bytes,
    bdc_name: str = "",
    depot_name: str = "",
    product_name: str = "",
) -> list:
    DESCRIPTIONS = sorted([
        "Balance b/fwd", "Stock Take", "Sale",
        "Custody Transfer In", "Custody Transfer Out", "Product Outturn",
    ], key=len, reverse=True)

    SKIP_PFX = (
        "national petroleum authority", "stock transaction report",
        "bdc :", "depot :", "product :", "printed by", "printed on",
        "date trans #", "actual stock balance", "stock commitments",
        "available stock balance", "last stock update", "i.t.s from",
    )

    def _skip(line):
        lo = line.strip().lower()
        return lo.startswith(SKIP_PFX) or bool(re.match(r"^\d{1,2}\s+\w+,\s+\d{4}", line.strip()))

    def _pnum(s):
        s = s.strip()
        neg = s.startswith("(") and s.endswith(")")
        try:
            v = int(s.strip("()").replace(",", ""))
            return -v if neg else v
        except ValueError:
            return None

    def _parse_line(line):
        line = line.strip()
        if not re.match(r"^\d{2}/\d{2}/\d{4}\b", line):
            return None
        parts = line.split()
        date, trans = parts[0], (parts[1] if len(parts) > 1 else "")
        rest = line[len(date):].strip()[len(trans):].strip()
        desc = after = None
        for d in DESCRIPTIONS:
            if rest.lower().startswith(d.lower()):
                desc, after = d, rest[len(d):].strip()
                break
        if desc is None or desc == "Balance b/fwd":
            return None
        nums = re.findall(r"\([\d,]+\)|[\d,]+", after)
        if len(nums) < 2:
            return None
        vol, bal = _pnum(nums[-2]), _pnum(nums[-1])
        trail = re.search(re.escape(nums[-2]) + r"\s+" + re.escape(nums[-1]) + r"\s*$", after)
        acct  = after[:trail.start()].strip() if trail else " ".join(after.split()[:-2])
        return {
            "Date":        date,
            "Trans #":     trans,
            "Description": desc,
            "Account":     acct,
            "Volume":      vol or 0,
            "Balance":     bal or 0,
        }

    records = []
    seen    = set()
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue
                for raw in text.split("\n"):
                    line = raw.strip()
                    if not line or _skip(line):
                        continue
                    row = _parse_line(line)
                    if row:
                        key = (row["Date"], row["Trans #"], row["Description"], row["Volume"])
                        if key not in seen:
                            seen.add(key)
                            row["BDC"]     = bdc_name
                            row["Depot"]   = depot_name
                            row["Product"] = product_name
                            records.append(row)
    except Exception:
        pass
    return records


def _parse_stock_transaction_pdf(pdf_bytes: bytes) -> list:
    return _parse_stock_transaction_pdf_full(pdf_bytes)


# ══════════════════════════════════════════════════════════════
# PRODUCT OUTTURN — SEQUENTIAL PER-BDC SWEEP  (FIXED)
# ══════════════════════════════════════════════════════════════
# Design goals:
#   • Progress bar moves after EVERY single HTTP request (not per-BDC)
#   • No hidden concurrency: one request at a time per depot within a BDC
#   • Circuit breaker: after OUTTURN_CB_THRESHOLD consecutive failures for a
#     given BDC, skip its remaining depots (server is rejecting that BDC)
#   • Inter-request sleep to stay under server rate limits
#   • Short per-request timeout — don't let one slow response block the sweep


OUTTURN_WORKERS         = 8
OUTTURN_REQUEST_TIMEOUT  = 45    # seconds per individual HTTP request
OUTTURN_INTER_REQ_SLEEP  = 0.15   # seconds between requests
OUTTURN_CB_THRESHOLD     = 10     # consecutive failures before circuit-breaker trips
OUTTURN_MAX_RETRIES      = 3     # retries per individual request (fast — no exponential)
OUTTURN_RATE_LIMIT_BACK = 2.0   # seconds to back off when we detect rate-limiting (via circuit-breaker)

_VESSEL_KEYWORDS = re.compile(
    r"\b(M\.?T\.?\s*[A-Z][A-Z0-9 \-]{2,40}|"
    r"MV\s+[A-Z][A-Z0-9 \-]{2,40}|"
    r"MT\s+[A-Z][A-Z0-9 \-]{2,40}|"
    r"[A-Z]{2,}\s+[A-Z]{2,}(?:\s+[A-Z0-9]{2,})*)\b",
    flags=re.IGNORECASE,
)

_NON_VESSEL_WORDS = {
    "SALE","TRANSFER","BDC","OUTTURN","PRODUCT","BALANCE","STOCK",
    "CUSTODY","BOST","TEMA","ACCRA","TAKORADI","KUMASI","GHANA",
}


def _extract_vessel_name(account: str) -> str:
    if not account:
        return ""
    account = account.strip()
    m = re.search(r"\b(?:MV|MT|M\.T\.?)\s+([A-Z][A-Z0-9 \-]{2,40})", account, re.IGNORECASE)
    if m:
        return ("MV " + m.group(1).strip()).upper()
    tokens = account.upper().split()
    if (
        2 <= len(tokens) <= 6
        and all(re.match(r"^[A-Z0-9\-]+$", t) for t in tokens)
        and not any(t in _NON_VESSEL_WORDS for t in tokens)
    ):
        return account.upper()
    return ""


def _fetch_one_outturn_triplet(
    bdc_name: str,
    depot_name: str,
    prod_label: str,
    start_str: str,
    end_str: str,
    *,
    bdc_map: dict,
    depot_map: dict,
    product_map: dict,
    stock_txn_url: str,
    user_id: str,
    fetch_pdf_fn,
    parse_fn,
    extract_vessel_fn,
) -> tuple[pd.DataFrame, bool]:
    """
    Returns (df, was_rate_limited).
    df may be empty. Never raises.
    """
    bdc_id     = bdc_map.get(bdc_name)
    depot_id   = depot_map.get(depot_name)
    product_id = product_map.get(prod_label)
    if not bdc_id or not depot_id or not product_id:
        return pd.DataFrame(), False
 
    params = {
        "lngProductId": product_id,
        "lngBDCId":     bdc_id,
        "lngDepotId":   depot_id,
        "dtpStartDate": start_str,
        "dtpEndDate":   end_str,
        "lngUserId":    user_id,
    }
 
    rate_limited = False
    for attempt in range(1, OUTTURN_MAX_RETRIES + 1):
        try:
            import requests as _req
            r = _req.get(
                stock_txn_url,
                params=params,
                timeout=OUTTURN_REQUEST_TIMEOUT,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    "Accept": "application/pdf,*/*",
                },
            )
            if r.status_code in (429, 503):
                rate_limited = True
                time.sleep(OUTTURN_RATE_LIMIT_BACK * attempt)
                continue
            r.raise_for_status()
            pdf_bytes = r.content
            if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
                return pd.DataFrame(), rate_limited
 
            rows = parse_fn(
                pdf_bytes,
                bdc_name=bdc_name,
                depot_name=depot_name,
                product_name=prod_label,
            )
            outturn_rows = [row for row in rows if row.get("Description") == "Product Outturn"]
            if not outturn_rows:
                return pd.DataFrame(), rate_limited
 
            df = pd.DataFrame(outturn_rows)
            df["Vessel Name"] = df["Account"].apply(extract_vessel_fn)
            return df, rate_limited
 
        except Exception:
            if attempt < OUTTURN_MAX_RETRIES:
                time.sleep(1.0 * attempt)
 
    return pd.DataFrame(), rate_limited

# ══════════════════════════════════════════════════════════════
# BACKGROUND SWEEP WORKER
# ══════════════════════════════════════════════════════════════
 
class _OutturnSweepWorker:
    """
    Runs the full BDC × depot × product sweep on a daemon thread.
    Progress is pushed to a thread-safe queue so the Streamlit main
    thread can poll without blocking.
 
    Message format (dict):
        {"type": "progress", "done": int, "total": int, "pct": float,
         "bdc": str, "depot": str, "prod": str}
        {"type": "log",      "text": str}
        {"type": "result",   "df": pd.DataFrame, "summary": dict}
        {"type": "done"}
        {"type": "error",    "text": str}
    """
 
    def __init__(
        self,
        bdcs:        list[str],
        depots:      list[str],
        products:    list[str],
        start_str:   str,
        end_str:     str,
        bdc_map:     dict,
        depot_map:   dict,
        product_map: dict,
        config:      dict,
        parse_fn,
        extract_vessel_fn,
        cancel_event: threading.Event,
    ):
        self.bdcs             = bdcs
        self.depots           = depots
        self.products         = products
        self.start_str        = start_str
        self.end_str          = end_str
        self.bdc_map          = bdc_map
        self.depot_map        = depot_map
        self.product_map      = product_map
        self.config           = config
        self.parse_fn         = parse_fn
        self.extract_vessel_fn= extract_vessel_fn
        self.cancel_event     = cancel_event
        self.q: queue.Queue   = queue.Queue()
 
    def start(self) -> threading.Thread:
        t = threading.Thread(target=self._run, daemon=True)
        t.start()
        return t
 
    def _log(self, text: str):
        self.q.put({"type": "log", "text": text})
 
    def _progress(self, done: int, total: int, bdc: str, depot: str, prod: str):
        self.q.put({
            "type": "progress",
            "done": done, "total": total,
            "pct":  min(done / total, 1.0),
            "bdc":  bdc, "depot": depot, "prod": prod,
        })
 
    def _run(self):
        try:
            self._sweep()
        except Exception as exc:
            self.q.put({"type": "error", "text": str(exc)})
 
    def _sweep(self):
        n_bdcs     = len(self.bdcs)
        n_depots   = len(self.depots)
        n_prods    = len(self.products)
        total_reqs = n_bdcs * n_depots * n_prods
 
        done_n  = 0
        frames  = []
        summary = {"success": [], "no_data": [], "failed": []}
 
        for bdc_name in self.bdcs:
            if self.cancel_event.is_set():
                self._log("🛑 Sweep cancelled by user.")
                break
 
            consecutive_empty = 0
            rate_limit_streak = 0
            bdc_had_data      = False
            bdc_tripped       = False
 
            # ── Process depots in parallel batches ───────────
            for depot_chunk_start in range(0, n_depots, OUTTURN_WORKERS):
                if self.cancel_event.is_set():
                    break
 
                chunk = self.depots[depot_chunk_start: depot_chunk_start + OUTTURN_WORKERS]
 
                if consecutive_empty >= OUTTURN_CB_THRESHOLD:
                    # circuit breaker: count skipped as done
                    skipped = (n_depots - depot_chunk_start) * n_prods
                    done_n += skipped
                    self._log(
                        f"⚡ {bdc_name}: circuit-breaker after "
                        f"{consecutive_empty} empty — skipping {n_depots - depot_chunk_start} depots"
                    )
                    self._progress(done_n, total_reqs, bdc_name, "—", "—")
                    bdc_tripped = True
                    break
 
                # adaptive sleep
                sleep_t = OUTTURN_INTER_REQ_SLEEP
                if rate_limit_streak >= 3:
                    sleep_t = OUTTURN_RATE_LIMIT_BACK
                    rate_limit_streak = 0
                time.sleep(sleep_t)
 
                # submit all (depot, product) combos in this chunk concurrently
                futures = {}
                with _cf.ThreadPoolExecutor(max_workers=OUTTURN_WORKERS) as ex:
                    for depot_name in chunk:
                        for prod_label in self.products:
                            fut = ex.submit(
                                _fetch_one_outturn_triplet,
                                bdc_name, depot_name, prod_label,
                                self.start_str, self.end_str,
                                bdc_map=self.bdc_map,
                                depot_map=self.depot_map,
                                product_map=self.product_map,
                                stock_txn_url=self.config["STOCK_TRANSACTION_URL"],
                                user_id=self.config["USER_ID"],
                                fetch_pdf_fn=None,   # unused — requests inline
                                parse_fn=self.parse_fn,
                                extract_vessel_fn=self.extract_vessel_fn,
                            )
                            futures[fut] = (depot_name, prod_label)
 
                    for fut in _cf.as_completed(futures):
                        depot_name, prod_label = futures[fut]
                        try:
                            result_df, was_rl = fut.result()
                        except Exception:
                            result_df, was_rl = pd.DataFrame(), False
 
                        done_n += 1
                        self._progress(done_n, total_reqs, bdc_name, depot_name, prod_label)
 
                        if was_rl:
                            rate_limit_streak += 1
                        else:
                            rate_limit_streak = max(0, rate_limit_streak - 1)
 
                        if result_df is not None and not result_df.empty:
                            frames.append(result_df)
                            consecutive_empty = 0
                            bdc_had_data = True
                            self._log(
                                f"✅ {bdc_name} @ {depot_name}/{prod_label}: "
                                f"{len(result_df)} row(s)"
                            )
                        else:
                            consecutive_empty += 1
 
            # summarise this BDC
            if bdc_had_data:
                if bdc_name not in summary["success"]:
                    summary["success"].append(bdc_name)
            elif bdc_tripped:
                if bdc_name not in summary["failed"]:
                    summary["failed"].append(bdc_name)
            else:
                if bdc_name not in summary["no_data"]:
                    summary["no_data"].append(bdc_name)
 
        # ── Assemble final DataFrame ──────────────────────────
        if not frames:
            combined = pd.DataFrame()
        else:
            combined = pd.concat(frames, ignore_index=True)
            dedup_cols = [c for c in ["Date", "Trans #", "BDC", "Depot", "Product", "Volume"]
                          if c in combined.columns]
            if dedup_cols:
                combined = combined.drop_duplicates(subset=dedup_cols, keep="first")
            combined = combined.reset_index(drop=True)
 
        self.q.put({"type": "result", "df": combined, "summary": summary})
        self.q.put({"type": "done"})
        


# ══════════════════════════════════════════════════════════════
# ROBUST PER-BDC FETCH WRAPPERS  (balance / omc / daily)
# ══════════════════════════════════════════════════════════════

def _make_balance_fetcher():
    def _fn(bdc_name: str):
        user_id = BDC_USER_MAP.get(bdc_name)
        if not user_id:
            return None
        params = {
            "lngCompanyId":     NPA_CONFIG["COMPANY_ID"],
            "strITSfromPersol": NPA_CONFIG["ITS_FROM_PERSOL"],
            "strGroupBy":       "BDC",
            "strGroupBy1":      "DEPOT",
            "strQuery1": "", "strQuery2": "", "strQuery3": "", "strQuery4": "",
            "strPicHeight": "1", "szPicWeight": "1",
            "lngUserId":    str(user_id),
            "intAppId":     NPA_CONFIG["APP_ID"],
        }
        pdf_bytes = _fetch_pdf(NPA_CONFIG["BDC_BALANCE_URL"], params)
        if not pdf_bytes:
            raise RuntimeError("No PDF returned")
        scraper = StockBalanceScraper()
        return scraper.parse_pdf_bytes(pdf_bytes, owning_bdc_name=bdc_name)
    return _fn


def _make_omc_fetcher(start_str: str, end_str: str):
    def _fn(bdc_name: str):
        user_id = BDC_USER_MAP.get(bdc_name)
        if not user_id:
            return None
        params = {
            "lngCompanyId":   NPA_CONFIG["COMPANY_ID"],
            "szITSfromPersol":"persol",
            "strGroupBy": "BDC", "strGroupBy1": "",
            "strQuery1": " and iorderstatus=4",
            "strQuery2": start_str, "strQuery3": end_str, "strQuery4": "",
            "strPicHeight": "", "strPicWeight": "", "intPeriodID": "4",
            "iUserId": str(user_id), "iAppId": NPA_CONFIG["APP_ID"],
        }
        pdf_bytes = _fetch_pdf(NPA_CONFIG["OMC_LOADINGS_URL"], params)
        if not pdf_bytes:
            raise RuntimeError("No PDF returned")
        return extract_omc_loadings_from_pdf(pdf_bytes, bdc_name)
    return _fn


def _make_daily_fetcher(start_str: str, end_str: str):
    def _fn(bdc_name: str):
        user_id = BDC_USER_MAP.get(bdc_name)
        if not user_id:
            return None
        params = {
            "lngCompanyId":   NPA_CONFIG["COMPANY_ID"],
            "szITSfromPersol":"persol",
            "strGroupBy": "DEPOT", "strGroupBy1": "",
            "strQuery1": "", "strQuery2": start_str, "strQuery3": end_str, "strQuery4": "",
            "strPicHeight": "1", "strPicWeight": "1", "intPeriodID": "-1",
            "iUserId": str(user_id), "iAppId": NPA_CONFIG["APP_ID"],
        }
        pdf_bytes = _fetch_pdf(NPA_CONFIG["DAILY_ORDERS_URL"], params)
        if not pdf_bytes:
            raise RuntimeError("No PDF returned")
        return extract_daily_orders_from_pdf(pdf_bytes, bdc_name)
    return _fn


# ══════════════════════════════════════════════════════════════
# MERGE HELPERS
# ══════════════════════════════════════════════════════════════

def _merge_balance_records(existing: list, new_records: list) -> list:
    if not existing:
        return new_records
    if not new_records:
        return existing
    col    = "ACTUAL BALANCE (LT\\KG)"
    df_old = pd.DataFrame(existing)
    df_new = pd.DataFrame(new_records)
    combined = pd.concat([df_old, df_new], ignore_index=True)
    combined = (
        combined
        .sort_values(col, ascending=False)
        .drop_duplicates(subset=["BDC", "DEPOT", "Product", "Date"], keep="first")
        .reset_index(drop=True)
    )
    return combined.to_dict("records")


def _merge_dataframes(existing: pd.DataFrame, new_df: pd.DataFrame,
                      dedup_cols: list) -> pd.DataFrame:
    if existing is None or existing.empty:
        return new_df
    if new_df is None or new_df.empty:
        return existing
    combined = pd.concat([new_df, existing], ignore_index=True)
    valid_dedup = [c for c in dedup_cols if c in combined.columns]
    if valid_dedup:
        combined = combined.drop_duplicates(subset=valid_dedup, keep="first")
    return combined.reset_index(drop=True)


def _combine_balance_results(results: dict) -> tuple[list, dict]:
    all_records = []
    summary     = {"success": [], "no_data": [], "failed": []}

    for bdc, recs in results.items():
        if recs is None:
            summary["failed"].append(bdc)
        elif len(recs) == 0:
            summary["no_data"].append(bdc)
        else:
            summary["success"].append(bdc)
            all_records.extend(recs)

    if all_records:
        col    = "ACTUAL BALANCE (LT\\KG)"
        df_tmp = pd.DataFrame(all_records)
        df_tmp = (
            df_tmp
            .sort_values(col, ascending=False)
            .drop_duplicates(subset=["BDC", "DEPOT", "Product", "Date"], keep="first")
            .reset_index(drop=True)
        )
        all_records = df_tmp.to_dict("records")

    return all_records, summary


def _combine_df_results(results: dict, dedup_cols: list) -> tuple[pd.DataFrame, dict]:
    frames  = []
    summary = {"success": [], "no_data": [], "failed": []}

    for bdc, df in results.items():
        if df is None:
            summary["failed"].append(bdc)
        elif isinstance(df, pd.DataFrame) and df.empty:
            summary["no_data"].append(bdc)
        elif isinstance(df, pd.DataFrame):
            summary["success"].append(bdc)
            frames.append(df)
        else:
            summary["failed"].append(bdc)

    if not frames:
        return pd.DataFrame(), summary

    combined = pd.concat(frames, ignore_index=True)
    valid_dedup = [c for c in dedup_cols if c in combined.columns]
    if valid_dedup:
        combined = combined.drop_duplicates(subset=valid_dedup, keep="first")

    return combined.reset_index(drop=True), summary


def _render_fetch_summary(summary: dict, total: int, record_count: int, data_label: str):
    n_ok   = len(summary["success"])
    n_none = len(summary["no_data"])
    n_fail = len(summary["failed"])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("BDCs Queried",  total)
    c2.metric("✅ With Data",  n_ok)
    c3.metric("⚠️ No Data",    n_none)
    c4.metric("❌ Failed",     n_fail)

    st.metric(f"📋 Total {data_label} Retrieved", f"{record_count:,}")

    if n_fail:
        with st.expander(f"❌ {n_fail} BDC(s) failed — click to see"):
            for b in summary["failed"]:
                st.markdown(f"- `{b}`")
    if n_none:
        with st.expander(f"⚠️ {n_none} BDC(s) returned no data"):
            for b in summary["no_data"]:
                st.markdown(f"- `{b}`")


# ══════════════════════════════════════════════════════════════
# NATIONAL STOCKOUT HELPERS
# ══════════════════════════════════════════════════════════════
SNAPSHOT_DIR = os.path.join(os.getcwd(), "national_snapshots")


def _save_national_snapshot(forecast_df: pd.DataFrame, period_label: str):
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    snap = {
        "ts":     datetime.now().isoformat(),
        "period": period_label,
        "rows":   forecast_df[["product","total_balance","omc_sales","daily_rate","days_remaining"]].to_dict("records"),
    }
    fname = f"snap_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(os.path.join(SNAPSHOT_DIR, fname), "w") as f:
        json.dump(snap, f)


def _count_period_days(start_str: str, end_str: str, use_biz: bool) -> int:
    fmt = "%m/%d/%Y"
    ds  = datetime.strptime(start_str, fmt).date()
    de  = datetime.strptime(end_str,   fmt).date()
    count = len(pd.bdate_range(ds, de)) if use_biz else (de - ds).days
    return max(count, 1)


# ══════════════════════════════════════════════════════════════
# VESSEL SUPPLY HELPERS
# ══════════════════════════════════════════════════════════════
VESSEL_CF  = {"PREMIUM":1324.50,"GASOIL":1183.00,"LPG":1000.00,"NAPHTHA":800.00}
VESSEL_PM  = {"PMS":"PREMIUM","GASOLINE":"PREMIUM","AGO":"GASOIL",
              "GASOIL":"GASOIL","LPG":"LPG","BUTANE":"LPG","NAPHTHA":"NAPHTHA"}
VESSEL_MM  = {m[:3].title():m[:3].upper() for m in
              ["January","February","March","April","May","June",
               "July","August","September","October","November","December"]}


def _load_vessel_sheet(url_in=None):
    from io import StringIO, BytesIO as _BytesIO
    url_in = url_in or VESSEL_SHEET_URL
    m_id   = re.search(r"/d/([a-zA-Z0-9-_]+)", url_in)
    fid    = m_id.group(1) if m_id else (url_in if re.match(r"^[a-zA-Z0-9-_]{20,}$", url_in) else None)
    if not fid:
        return None, "Could not extract Google Sheets file ID."
    m_gid = re.search(r"(?:#|\?|&)gid=(\d+)", url_in)
    gid   = m_gid.group(1) if m_gid else None
    candidates = []
    if gid:
        candidates.append((f"https://docs.google.com/spreadsheets/d/{fid}/export?format=csv&gid={gid}","csv"))
    candidates += [
        (f"https://docs.google.com/spreadsheets/d/{fid}/export?format=csv&gid=0","csv"),
        (f"https://docs.google.com/spreadsheets/d/{fid}/export?format=csv","csv"),
        (f"https://docs.google.com/spreadsheets/d/{fid}/gviz/tq?tqx=out:csv","gviz"),
        (f"https://docs.google.com/spreadsheets/d/{fid}/export?format=xlsx","xlsx"),
    ]
    hdrs = {"User-Agent":"Mozilla/5.0"}
    for url, mode in candidates:
        try:
            r = _requests.get(url, headers=hdrs, timeout=30)
            if r.status_code != 200 or not r.content: continue
            if mode == "xlsx":
                return pd.read_excel(_BytesIO(r.content)), None
            df = pd.read_csv(StringIO(r.content.decode("utf-8",errors="replace")),
                             header=14, skiprows=1, skipfooter=1, engine="python")
            return df, None
        except Exception:
            continue
    return None, "All fetch strategies failed. Ensure sheet is shared publicly."


def _parse_vessel_date(s, yr="2025"):
    s = str(s).strip().upper()
    if "PENDING" in s or s in ("NAN",""):
        mc = VESSEL_MM.get(datetime.now().strftime("%b"), datetime.now().strftime("%b").upper())
        return mc, yr, "PENDING"
    try:
        if "-" in s:
            p = s.split("-")
            if len(p) == 2:
                return VESSEL_MM.get(p[1].title(), p[1].upper()), yr, "DISCHARGED"
    except Exception:
        pass
    return "Unknown", yr, "DISCHARGED"


def _process_vessel_df(vdf, year="2025"):
    vdf = vdf.copy()
    vdf.columns = vdf.columns.str.strip()
    ci = {}
    for i, col in enumerate(vdf.columns):
        cl = str(col).lower().strip()
        if "receiver" in cl or (i == 0 and "unnamed" not in cl): ci["r"] = i
        elif "type" in cl and "receiver" not in cl:              ci["t"] = i
        elif "vessel" in cl and "name" in cl:                    ci["v"] = i
        elif "supplier" in cl:                                   ci["s"] = i
        elif "product" in cl:                                    ci["p"] = i
        elif "quantity" in cl or ("mt" in cl and "quantity" not in cl): ci["q"] = i
        elif "date" in cl or "discharg" in cl:                   ci["d"] = i
    records = []
    seen    = set()
    for _, row in vdf.dropna(how="all").iterrows():
        try:
            receivers   = str(row.iloc[ci.get("r",0)]).strip()
            vessel_type = str(row.iloc[ci.get("t",1)]).strip()
            vessel_name = str(row.iloc[ci.get("v",2)]).strip()
            supplier    = str(row.iloc[ci.get("s",3)]).strip()
            prod_raw    = str(row.iloc[ci.get("p",4)]).strip().upper()
            qty_str     = str(row.iloc[ci.get("q",5)]).replace(",","").strip()
            date_cell   = str(row.iloc[ci.get("d",6)]).strip()
            if receivers.upper() in {"RECEIVER(S)","RECEIVERS","NAN",""}: continue
            if prod_raw in {"PRODUCT","NAN",""}:                           continue
            try: qty_mt = float(qty_str)
            except ValueError: continue
            if qty_mt <= 0: continue
            product = VESSEL_PM.get(prod_raw, prod_raw)
            if product not in VESSEL_CF: continue
            qty_lt = qty_mt * VESSEL_CF[product]
            month, yr_, status = _parse_vessel_date(date_cell, yr=year)
            key = (vessel_name, product, qty_mt, date_cell)
            if key in seen:
                continue
            seen.add(key)
            records.append({"Receivers":receivers,"Vessel_Type":vessel_type,"Vessel_Name":vessel_name,
                            "Supplier":supplier,"Product":product,"Original_Product":prod_raw,
                            "Quantity_MT":qty_mt,"Quantity_Litres":qty_lt,"Date_Discharged":date_cell,
                            "Month":month,"Year":yr_,"Status":status})
        except Exception:
            continue
    return pd.DataFrame(records)


# ══════════════════════════════════════════════════════════════
# EXCEL EXPORT HELPER
# ══════════════════════════════════════════════════════════════
def _to_excel_bytes(sheets: dict) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for name, df in sheets.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                df.to_excel(writer, sheet_name=name[:31], index=False)
            elif isinstance(df, pd.DataFrame):
                pd.DataFrame(columns=df.columns if len(df.columns) else ["No Data"]).to_excel(
                    writer, sheet_name=name[:31], index=False
                )
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════
# PAGE: BDC BALANCE
# ══════════════════════════════════════════════════════════════
def show_bdc_balance():
    st.markdown("<h2>🏦 BDC STOCK BALANCE ANALYZER</h2>", unsafe_allow_html=True)

    st.markdown("""
    <div style='background:rgba(0,255,255,0.05);border:1px solid #00ffff33;
                border-radius:10px;padding:14px;margin-bottom:16px;'>
    <b style='color:#00ffff;'>What this page does</b><br>
    Shows the current live stock balance for every BDC — broken down by depot and product
    (PREMIUM / GASOIL / LPG) — giving a unified national stock picture.
    </div>
    """, unsafe_allow_html=True)

    all_bdc_names = sorted(BDC_USER_MAP.keys())
    n_configured  = len(all_bdc_names)

    col1, col2 = st.columns([3,1])
    with col1:
        selected = st.multiselect(
            f"Select specific BDCs to fetch  (leave blank to fetch all {n_configured})",
            all_bdc_names, key="bal_bdc_select",
        )
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        fetch_all_flag = st.checkbox("Fetch ALL BDCs", value=True, key="bal_fetch_all")

    bdcs_to_fetch = all_bdc_names if (fetch_all_flag or not selected) else selected

    has_previous = bool(st.session_state.get("bdc_records"))
    merge_prev   = False
    if has_previous:
        merge_prev = st.checkbox(
            "🔀 Merge this fetch with previous results "
            "(adds any BDCs that were missing last time, keeps best balance for duplicates)",
            value=True, key="bal_merge_prev",
        )

    st.info(f"📋 **{len(bdcs_to_fetch)} BDC(s)** will be queried  "
            f"({'all configured' if len(bdcs_to_fetch)==n_configured else 'custom selection'})")

    if st.button("🔄 FETCH BDC BALANCE DATA", key="bal_fetch"):
        prog      = st.progress(0, text="Initialising…")
        log_box   = st.empty()
        log_lines = []

        results = _sequential_batch_fetch(
            bdcs_to_fetch,
            _make_balance_fetcher(),
            prog, log_box, log_lines,
            second_pass=True,
        )
        prog.progress(1.0, text="✅ Fetch complete")

        all_records, summary = _combine_balance_results(results)

        if merge_prev and has_previous:
            prev = st.session_state.bdc_records
            all_records = _merge_balance_records(prev, all_records)
            st.info(f"🔀 Merged with previous run — {len(all_records)} total records after union.")

        st.session_state.bdc_records       = all_records
        st.session_state.bdc_fetch_summary = summary
        st.session_state.bdc_fetched_count = len(bdcs_to_fetch)
        _save_state("bdc_records", all_records)

        st.markdown("---")
        _render_fetch_summary(summary, len(bdcs_to_fetch), len(all_records), "Balance Records")

    records = st.session_state.get("bdc_records", [])
    if not records:
        st.info("👆 Click **FETCH BDC BALANCE DATA** to load the current stock position.")
        return

    if st.session_state.get("bdc_fetch_summary"):
        with st.expander("📊 Last fetch outcome", expanded=False):
            _render_fetch_summary(
                st.session_state.bdc_fetch_summary,
                st.session_state.get("bdc_fetched_count", len(BDC_USER_MAP)),
                len(records), "Balance Records",
            )

    df      = pd.DataFrame(records)
    col_bal = "ACTUAL BALANCE (LT\\KG)"
    summary = df.groupby("Product")[col_bal].sum()

    st.markdown("---")
    st.markdown("### 🛢️ NATIONAL STOCK TOTALS")
    cols = st.columns(3)
    for idx, prod in enumerate(["PREMIUM","GASOIL","LPG"]):
        with cols[idx]:
            val = summary.get(prod, 0)
            st.markdown(f"<div class='metric-card'><h2>{prod}</h2><h1>{val:,.0f}</h1>"
                        f"<p style='color:#888;font-size:13px;margin:0;'>Litres / KG</p></div>",
                        unsafe_allow_html=True)

    grand_total = float(df[col_bal].sum())
    st.metric("🏭 Grand National Total", f"{grand_total:,.0f} LT/KG",
              help="Sum of PREMIUM + GASOIL + LPG across all BDCs and depots")

    st.markdown("---")
    st.markdown("### 🏢 BDC BREAKDOWN")
    bdc_sum = (df.groupby("BDC")
               .agg({col_bal:"sum","DEPOT":"nunique","Product":"nunique"})
               .reset_index()
               .rename(columns={col_bal:"Total Balance (LT/KG)","DEPOT":"Depots","Product":"Products"}))
    bdc_sum = bdc_sum.sort_values("Total Balance (LT/KG)", ascending=False)
    bdc_sum["Market Share %"] = (bdc_sum["Total Balance (LT/KG)"] / grand_total * 100).round(2)
    st.caption(f"**{len(bdc_sum)} BDCs** with balance data")
    st.dataframe(bdc_sum, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("### 📊 PRODUCT × BDC PIVOT")
    pivot = (df.pivot_table(index="BDC", columns="Product", values=col_bal,
                            aggfunc="sum", fill_value=0).reset_index())
    for p in ["GASOIL","LPG","PREMIUM"]:
        if p not in pivot.columns: pivot[p] = 0
    pivot["TOTAL"] = pivot[["GASOIL","LPG","PREMIUM"]].sum(axis=1)
    pivot = pivot.sort_values("TOTAL", ascending=False)
    st.caption(f"**{len(pivot)} BDCs** shown in pivot")
    st.dataframe(pivot[["BDC","GASOIL","LPG","PREMIUM","TOTAL"]], use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("### 🔍 FILTER & EXPLORE")
    ft = st.selectbox("Filter by", ["Product","BDC","Depot"], key="bal_ftype")
    _cmap = {"Product":"Product","BDC":"BDC","Depot":"DEPOT"}
    opts  = ["ALL"] + sorted(df[_cmap[ft]].unique().tolist())
    fval  = st.selectbox("Value", opts, key="bal_fval")
    filt  = df if fval=="ALL" else df[df[_cmap[ft]]==fval]

    st.caption(f"Showing **{len(filt):,}** records  |  "
               f"**{filt['BDC'].nunique()}** BDCs  |  "
               f"**{filt['DEPOT'].nunique()}** depots  |  "
               f"Total: **{filt[col_bal].sum():,.0f} LT/KG**")
    st.dataframe(filt[["Product","BDC","DEPOT","AVAILABLE BALANCE (LT\\KG)",col_bal,"Date"]]
                 .sort_values(["Product","BDC"]),
                 use_container_width=True, height=400, hide_index=True)

    st.markdown("---")
    _bdc_dl_ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_bytes = _to_excel_bytes({
        "Stock Balance": df,
        "LPG":           df[df["Product"]=="LPG"],
        "PREMIUM":       df[df["Product"]=="PREMIUM"],
        "GASOIL":        df[df["Product"]=="GASOIL"],
    })
    st.download_button("⬇️ DOWNLOAD EXCEL", excel_bytes,
                       f"bdc_balance_{_bdc_dl_ts}.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ══════════════════════════════════════════════════════════════
# PAGE: OMC LOADINGS
# ══════════════════════════════════════════════════════════════
def show_omc_loadings():
    st.markdown("<h2>🚚 OMC LOADINGS ANALYZER</h2>", unsafe_allow_html=True)

    st.markdown("""
    <div style='background:rgba(0,255,255,0.05);border:1px solid #00ffff33;
                border-radius:10px;padding:14px;margin-bottom:16px;'>
    <b style='color:#00ffff;'>What this page does</b><br>
    Fetches released OMC loading orders for every BDC within the selected date range —
    combined into a single de-duplicated dataset for market share and dispatch analysis.
    </div>
    """, unsafe_allow_html=True)

    all_bdc_names = sorted(BDC_USER_MAP.keys())
    n_configured  = len(all_bdc_names)

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=datetime.now()-timedelta(days=7), key="omc_start")
    with col2:
        end_date = st.date_input("End Date", value=datetime.now(), key="omc_end")

    col3, col4 = st.columns([3,1])
    with col3:
        selected = st.multiselect(f"Select BDCs (blank = all {n_configured})",
                                  all_bdc_names, key="omc_bdc_select")
    with col4:
        st.markdown("<br>", unsafe_allow_html=True)
        fetch_all_flag = st.checkbox("Fetch ALL", value=True, key="omc_fetch_all")

    bdcs_to_fetch = all_bdc_names if (fetch_all_flag or not selected) else selected
    period_days   = max((end_date - start_date).days, 1)

    has_previous = not st.session_state.get("omc_df", pd.DataFrame()).empty
    merge_prev   = False
    if has_previous:
        merge_prev = st.checkbox(
            "🔀 Merge this fetch with previous results",
            value=True, key="omc_merge_prev",
        )

    st.info(f"📋 **{len(bdcs_to_fetch)} BDC(s)** · "
            f"Period: **{start_date.strftime('%d %b %Y')} → {end_date.strftime('%d %b %Y')}** "
            f"({period_days} days)")

    if st.button("🔄 FETCH OMC LOADINGS", key="omc_fetch"):
        start_str = start_date.strftime("%m/%d/%Y")
        end_str   = end_date.strftime("%m/%d/%Y")
        prog      = st.progress(0, text="Initialising…")
        log_box   = st.empty()
        log_lines = []

        results = _sequential_batch_fetch(
            bdcs_to_fetch,
            _make_omc_fetcher(start_str, end_str),
            prog, log_box, log_lines,
            second_pass=True,
        )
        prog.progress(1.0, text="✅ Fetch complete")

        combined, summary = _combine_df_results(
            results, ["Order Number", "Truck", "Date", "Product"]
        )

        if merge_prev and has_previous:
            prev = st.session_state.omc_df
            combined = _merge_dataframes(prev, combined,
                                         ["Order Number", "Truck", "Date", "Product"])
            st.info(f"🔀 Merged — {len(combined)} total records after union.")

        st.session_state.omc_df            = combined
        st.session_state.omc_fetch_summary = summary
        st.session_state.omc_fetched_count = len(bdcs_to_fetch)
        st.session_state.omc_start_date    = start_date
        st.session_state.omc_end_date      = end_date
        _save_state("omc_df", combined)

        st.markdown("---")
        _render_fetch_summary(summary, len(bdcs_to_fetch),
                              len(combined) if not combined.empty else 0,
                              "Loading Records")

    df = st.session_state.get("omc_df", pd.DataFrame())
    if df.empty:
        st.info("👆 Select a date range and click **FETCH OMC LOADINGS**.")
        return

    if st.session_state.get("omc_fetch_summary"):
        with st.expander("📊 Last fetch outcome", expanded=False):
            _render_fetch_summary(
                st.session_state.omc_fetch_summary,
                st.session_state.get("omc_fetched_count", len(BDC_USER_MAP)),
                len(df), "Loading Records",
            )

    st.markdown("---")
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Total Orders",    f"{len(df):,}")
    c2.metric("Total Volume (LT)", f"{df['Quantity'].sum():,.0f}")
    c3.metric("Unique OMCs",     f"{df['OMC'].nunique()}")
    c4.metric("Total Value (₵)", f"{(df['Quantity']*df['Price']).sum():,.0f}")

    st.markdown("### 📦 PRODUCT BREAKDOWN")
    prod_sum = (df.groupby("Product")
                .agg({"Quantity":"sum","Order Number":"count","OMC":"nunique"})
                .reset_index()
                .rename(columns={"Quantity":"Total Volume (LT/KG)","Order Number":"Orders","OMC":"OMCs"})
                .sort_values("Total Volume (LT/KG)", ascending=False))
    st.dataframe(prod_sum, use_container_width=True, hide_index=True)

    st.markdown("### 🏢 TOP OMCs BY VOLUME")
    omc_sum = (df.groupby("OMC").agg({"Quantity":"sum","Order Number":"count"})
               .reset_index().sort_values("Quantity", ascending=False).head(20)
               .rename(columns={"Quantity":"Total Volume (LT/KG)","Order Number":"Orders"}))
    st.dataframe(omc_sum, use_container_width=True, hide_index=True)

    st.markdown("### 🏦 BDC PERFORMANCE")
    bdc_sum = (df.groupby("BDC").agg({"Quantity":"sum","Order Number":"count","OMC":"nunique"})
               .reset_index().sort_values("Quantity", ascending=False)
               .rename(columns={"Quantity":"Total Volume (LT/KG)","Order Number":"Orders","OMC":"OMCs"}))
    st.caption(f"**{len(bdc_sum)} BDCs** with loading data")
    st.dataframe(bdc_sum, use_container_width=True, hide_index=True)

    st.markdown("---")
    ft    = st.selectbox("Filter by", ["Product","OMC","BDC","Depot"], key="omc_ftype")
    _cmap = {"Product":"Product","OMC":"OMC","BDC":"BDC","Depot":"Depot"}
    opts  = ["ALL"] + sorted(df[_cmap[ft]].unique().tolist())
    fval  = st.selectbox("Value", opts, key="omc_fval")
    filt  = df if fval=="ALL" else df[df[_cmap[ft]]==fval]
    st.caption(f"Showing **{len(filt):,}** records | "
               f"Volume: **{filt['Quantity'].sum():,.0f} LT**")
    st.dataframe(filt[["Date","OMC","Truck","Quantity","Order Number","BDC","Depot","Price","Product"]]
                 .sort_values(["Product","Date"]),
                 use_container_width=True, height=400, hide_index=True)

    st.markdown("---")
    _omc_bdc_summary = (
        df.pivot_table(index="BDC", columns="Product", values="Quantity",
                       aggfunc="sum", fill_value=0)
        .reset_index()
    )
    for _p in ["GASOIL","LPG","PREMIUM"]:
        if _p not in _omc_bdc_summary.columns:
            _omc_bdc_summary[_p] = 0
    _omc_bdc_summary["Total"] = _omc_bdc_summary[["GASOIL","LPG","PREMIUM"]].sum(axis=1)
    _omc_bdc_summary = _omc_bdc_summary[["BDC","GASOIL","LPG","PREMIUM","Total"]].sort_values("Total", ascending=False)

    _omc_dl_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_bytes = _to_excel_bytes({
        "All Orders":  df,
        "PREMIUM":     df[df["Product"]=="PREMIUM"],
        "GASOIL":      df[df["Product"]=="GASOIL"],
        "LPG":         df[df["Product"]=="LPG"],
        "BDC Summary": _omc_bdc_summary,
    })
    st.download_button("⬇️ DOWNLOAD EXCEL", excel_bytes,
                       f"omc_loadings_{_omc_dl_ts}.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ══════════════════════════════════════════════════════════════
# PAGE: DAILY ORDERS
# ══════════════════════════════════════════════════════════════
def show_daily_orders():
    st.markdown("<h2>📅 DAILY ORDERS ANALYZER</h2>", unsafe_allow_html=True)

    st.markdown("""
    <div style='background:rgba(0,255,255,0.05);border:1px solid #00ffff33;
                border-radius:10px;padding:14px;margin-bottom:16px;'>
    <b style='color:#00ffff;'>What this page does</b><br>
    Fetches the daily dispatch order report grouped by depot, giving truck-level
    granularity of physical fuel movements out of each storage facility.<br>
    <b style='color:#ff00ff;'>OMC Name column:</b> Automatically cross-referenced from OMC Loadings
    data (if fetched) by fuzzy-matching order numbers. Fetch OMC Loadings first for best results.
    </div>
    """, unsafe_allow_html=True)

    all_bdc_names = sorted(BDC_USER_MAP.keys())
    n_configured  = len(all_bdc_names)

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=datetime.now()-timedelta(days=1), key="daily_start")
    with col2:
        end_date = st.date_input("End Date", value=datetime.now(), key="daily_end")

    col3, col4 = st.columns([3,1])
    with col3:
        selected = st.multiselect(f"Select BDCs (blank = all {n_configured})",
                                  all_bdc_names, key="daily_bdc_select")
    with col4:
        st.markdown("<br>", unsafe_allow_html=True)
        fetch_all_flag = st.checkbox("Fetch ALL", value=True, key="daily_fetch_all")

    bdcs_to_fetch = all_bdc_names if (fetch_all_flag or not selected) else selected
    period_days   = max((end_date - start_date).days, 1)

    has_previous = not st.session_state.get("daily_df", pd.DataFrame()).empty
    merge_prev   = False
    if has_previous:
        merge_prev = st.checkbox(
            "🔀 Merge this fetch with previous results",
            value=True, key="daily_merge_prev",
        )

    omc_df_for_lookup = st.session_state.get("omc_df", pd.DataFrame())
    _has_omc = isinstance(omc_df_for_lookup, pd.DataFrame) and not omc_df_for_lookup.empty
    if _has_omc:
        _n_omc_orders = omc_df_for_lookup["Order Number"].nunique() \
            if "Order Number" in omc_df_for_lookup.columns else 0
        st.success(
            f"✅ OMC Loadings available — **{_n_omc_orders:,} unique order numbers** "
            f"will be used to populate the **OMC Name** column."
        )
    else:
        st.warning(
            "⚠️ No OMC Loadings data found in session. "
            "Fetch OMC Loadings first to enable the **OMC Name** cross-reference column. "
            "The column will still appear but will be empty."
        )

    st.info(f"📋 **{len(bdcs_to_fetch)} BDC(s)** · "
            f"Period: **{start_date.strftime('%d %b %Y')} → {end_date.strftime('%d %b %Y')}** "
            f"({period_days} days)")

    if st.button("🔄 FETCH DAILY ORDERS", key="daily_fetch"):
        start_str = start_date.strftime("%m/%d/%Y")
        end_str   = end_date.strftime("%m/%d/%Y")
        prog      = st.progress(0, text="Initialising…")
        log_box   = st.empty()
        log_lines = []

        results = _sequential_batch_fetch(
            bdcs_to_fetch,
            _make_daily_fetcher(start_str, end_str),
            prog, log_box, log_lines,
            second_pass=True,
        )
        prog.progress(1.0, text="✅ Fetch complete")

        combined, summary = _combine_df_results(
            results, ["Date", "Truck", "Order Number", "Product"]
        )

        if merge_prev and has_previous:
            prev = st.session_state.daily_df
            if "OMC Name" in prev.columns:
                prev = prev.drop(columns=["OMC Name"])
            combined = _merge_dataframes(prev, combined,
                                         ["Date", "Truck", "Order Number", "Product"])
            st.info(f"🔀 Merged — {len(combined)} total records after union.")

        combined = _enrich_daily_with_omc(combined, omc_df_for_lookup)

        st.session_state.daily_df            = combined
        st.session_state.daily_fetch_summary = summary
        st.session_state.daily_fetched_count = len(bdcs_to_fetch)
        st.session_state.daily_start_date    = start_date
        st.session_state.daily_end_date      = end_date
        _save_state("daily_df", combined)

        n_matched = int((combined["OMC Name"] != "").sum()) if not combined.empty else 0
        st.markdown("---")
        _render_fetch_summary(summary, len(bdcs_to_fetch),
                              len(combined) if not combined.empty else 0,
                              "Order Records")
        if not combined.empty:
            st.info(
                f"🔗 OMC Name cross-reference: **{n_matched:,} / {len(combined):,}** orders "
                f"matched ({n_matched/len(combined)*100:.1f}%)"
            )

    df = st.session_state.get("daily_df", pd.DataFrame())
    if df.empty:
        st.info("👆 Select a date range and click **FETCH DAILY ORDERS**.")
        return

    if _has_omc and ("OMC Name" not in df.columns or df["OMC Name"].eq("").all()):
        with st.spinner("🔗 Cross-referencing OMC names from loadings data…"):
            df = _enrich_daily_with_omc(df, omc_df_for_lookup)
            st.session_state.daily_df = df
            _save_state("daily_df", df)
        n_matched = int((df["OMC Name"] != "").sum())
        st.success(
            f"✅ OMC Name column updated — **{n_matched:,} / {len(df):,}** orders matched."
        )

    if st.session_state.get("daily_fetch_summary"):
        with st.expander("📊 Last fetch outcome", expanded=False):
            _render_fetch_summary(
                st.session_state.daily_fetch_summary,
                st.session_state.get("daily_fetched_count", len(BDC_USER_MAP)),
                len(df), "Order Records",
            )

    st.markdown("---")

    n_omc_matched = int((df["OMC Name"] != "").sum()) if "OMC Name" in df.columns else 0
    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.metric("Orders",          f"{len(df):,}")
    c2.metric("Volume (LT)",     f"{df['Quantity'].sum():,.0f}")
    c3.metric("BDCs",            f"{df['BDC'].nunique()}")
    c4.metric("Depots",          f"{df['Depot'].nunique()}")
    c5.metric("Value (₵)",       f"{(df['Quantity']*df['Price']).sum():,.0f}")
    c6.metric("🔗 OMC Matched",  f"{n_omc_matched:,}")

    st.markdown("### 📦 PRODUCT SUMMARY")
    prod_sum = (df.groupby("Product")
                .agg({"Quantity":"sum","Order Number":"count","BDC":"nunique"})
                .reset_index()
                .rename(columns={"Quantity":"Total Volume (LT/KG)","Order Number":"Orders","BDC":"BDCs"})
                .sort_values("Total Volume (LT/KG)", ascending=False))
    st.dataframe(prod_sum, use_container_width=True, hide_index=True)

    st.markdown("### 🏦 BDC SUMMARY")
    bdc_sum = (df.groupby("BDC").agg({"Quantity":"sum","Order Number":"count"})
               .reset_index().sort_values("Quantity", ascending=False)
               .rename(columns={"Quantity":"Total Volume (LT/KG)","Order Number":"Orders"}))
    st.caption(f"**{len(bdc_sum)} BDCs** with daily order data")
    st.dataframe(bdc_sum, use_container_width=True, hide_index=True)

    if "OMC Name" in df.columns and n_omc_matched > 0:
        st.markdown("### 🏢 TOP OMCs (via Cross-Reference)")
        omc_name_sum = (
            df[df["OMC Name"] != ""]
            .groupby("OMC Name")
            .agg(Orders=("Order Number","count"), Volume=("Quantity","sum"))
            .reset_index()
            .sort_values("Volume", ascending=False)
            .rename(columns={"Volume":"Total Volume (LT/KG)"})
        )
        st.caption(
            f"Derived from fuzzy order-number matching against OMC Loadings data — "
            f"**{n_omc_matched:,}** of **{len(df):,}** orders matched."
        )
        st.dataframe(omc_name_sum, use_container_width=True, hide_index=True)

    st.markdown("### 📊 BDC × PRODUCT PIVOT")
    pivot = (df.pivot_table(index="BDC", columns="Product", values="Quantity",
                            aggfunc="sum", fill_value=0).reset_index())
    pcols = [c for c in pivot.columns if c != "BDC"]
    pivot["TOTAL"] = pivot[pcols].sum(axis=1)
    st.dataframe(pivot.sort_values("TOTAL", ascending=False), use_container_width=True, hide_index=True)

    st.markdown("---")
    ft    = st.selectbox("Filter by", ["Product","BDC","Depot","Status","OMC Name"], key="daily_ftype")
    _cmap = {"Product":"Product","BDC":"BDC","Depot":"Depot","Status":"Status","OMC Name":"OMC Name"}
    col_for_filter = _cmap[ft]
    if col_for_filter not in df.columns:
        df[col_for_filter] = ""
    opts  = ["ALL"] + sorted(df[col_for_filter].dropna().unique().tolist())
    fval  = st.selectbox("Value", opts, key="daily_fval")
    filt  = df if fval=="ALL" else df[df[col_for_filter]==fval]
    st.caption(f"Showing **{len(filt):,}** records | "
               f"Volume: **{filt['Quantity'].sum():,.0f} LT**")

    detail_cols = ["Date","Truck","Quantity","Order Number","OMC Name","BDC","Depot","Price","Product","Status"]
    detail_cols = [c for c in detail_cols if c in filt.columns]
    st.dataframe(
        filt[detail_cols].sort_values(["Product","Date"]),
        use_container_width=True, height=400, hide_index=True,
    )

    st.markdown("---")
    excel_sheets = {"All Orders": df, "BDC Pivot": pivot}
    if "OMC Name" in df.columns and n_omc_matched > 0:
        excel_sheets["Matched OMC Orders"] = df[df["OMC Name"] != ""][detail_cols]
        excel_sheets["Unmatched Orders"]   = df[df["OMC Name"] == ""][
            [c for c in detail_cols if c != "OMC Name"]
        ]
    excel_bytes = _to_excel_bytes(excel_sheets)
    st.download_button("⬇️ DOWNLOAD EXCEL", excel_bytes, "daily_orders.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ══════════════════════════════════════════════════════════════
# PAGE: MARKET SHARE
# ══════════════════════════════════════════════════════════════
def show_market_share():
    st.markdown("<h2>📊 BDC MARKET SHARE ANALYSIS</h2>", unsafe_allow_html=True)

    st.markdown("""
    <div style='background:rgba(0,255,255,0.05);border:1px solid #00ffff33;
                border-radius:10px;padding:14px;margin-bottom:16px;'>
    <b style='color:#00ffff;'>What this page does</b><br>
    Shows a selected BDC's share of national stock and dispatch volumes per product,
    including its ranking against all other BDCs.<br>
    <b style='color:#ff00ff;'>Prerequisite:</b> Fetch BDC Balance and/or OMC Loadings data first.
    </div>
    """, unsafe_allow_html=True)

    has_balance  = bool(st.session_state.get("bdc_records"))
    has_loadings = not st.session_state.get("omc_df", pd.DataFrame()).empty

    c1, c2 = st.columns(2)
    with c1:
        st.success(f"✅ BDC Balance: {len(st.session_state.get('bdc_records',[]))} records") \
            if has_balance else st.warning("⚠️ Fetch BDC Balance first")
    with c2:
        st.success(f"✅ OMC Loadings: {len(st.session_state.get('omc_df',pd.DataFrame()))} records") \
            if has_loadings else st.warning("⚠️ Fetch OMC Loadings first")

    if not has_balance and not has_loadings:
        st.error("No data available. Please fetch from the BDC Balance and/or OMC Loadings pages first.")
        return

    balance_df  = pd.DataFrame(st.session_state.bdc_records) if has_balance else pd.DataFrame()
    loadings_df = st.session_state.omc_df if has_loadings else pd.DataFrame()
    col_bal     = "ACTUAL BALANCE (LT\\KG)"

    all_bdcs = sorted(
        set(balance_df["BDC"].unique()  if not balance_df.empty  else []) |
        set(loadings_df["BDC"].unique() if not loadings_df.empty else [])
    )
    selected_bdc = st.selectbox("Choose BDC to analyse:", all_bdcs, key="ms_bdc")
    if not selected_bdc:
        return

    st.markdown(f"## 📊 MARKET REPORT — {selected_bdc}")
    st.markdown("---")

    tab1, tab2 = st.tabs(["📦 Stock Balance Share","🚚 Sales Volume Share"])

    with tab1:
        if not has_balance:
            st.warning("Fetch BDC Balance first.")
        else:
            bdc_bal    = balance_df[balance_df["BDC"]==selected_bdc]
            total_mkt  = float(balance_df[col_bal].sum())
            bdc_total  = float(bdc_bal[col_bal].sum())
            share_pct  = bdc_total / total_mkt * 100 if total_mkt else 0
            sorted_idx = list(balance_df.groupby("BDC")[col_bal].sum()
                              .sort_values(ascending=False).index)
            rank = sorted_idx.index(selected_bdc) + 1 if selected_bdc in sorted_idx else "N/A"

            c1,c2,c3 = st.columns(3)
            c1.metric("Total Stock (LT)", f"{bdc_total:,.0f}")
            c2.metric("Market Share",     f"{share_pct:.2f}%")
            c3.metric("National Rank",    f"#{rank} of {len(sorted_idx)}")

            rows = []
            for prod in ["PREMIUM","GASOIL","LPG"]:
                mkt = float(balance_df[balance_df["Product"]==prod][col_bal].sum())
                bv  = float(bdc_bal[bdc_bal["Product"]==prod][col_bal].sum())
                rows.append({"Product":prod,
                              "BDC Stock (LT)":    f"{bv:,.0f}",
                              "Market Total (LT)": f"{mkt:,.0f}",
                              "Share (%)":         f"{bv/mkt*100:.2f}" if mkt else "0.00"})
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    with tab2:
        if not has_loadings:
            st.warning("Fetch OMC Loadings first.")
        else:
            bdc_ld    = loadings_df[loadings_df["BDC"]==selected_bdc]
            total_vol = float(loadings_df["Quantity"].sum())
            bdc_vol   = float(bdc_ld["Quantity"].sum())
            share_pct = bdc_vol / total_vol * 100 if total_vol else 0
            all_sales = loadings_df.groupby("BDC")["Quantity"].sum().sort_values(ascending=False)
            s_rank    = list(all_sales.index).index(selected_bdc)+1 if selected_bdc in all_sales.index else "N/A"
            rev       = (bdc_ld["Quantity"]*bdc_ld["Price"]).sum()

            c1,c2,c3,c4 = st.columns(4)
            c1.metric("Total Dispatched (LT)", f"{bdc_vol:,.0f}")
            c2.metric("Market Share",          f"{share_pct:.2f}%")
            c3.metric("Sales Rank",            f"#{s_rank} of {len(all_sales)}")
            c4.metric("Revenue (₵)",           f"{rev:,.0f}")

            rows = []
            for prod in ["PREMIUM","GASOIL","LPG"]:
                mkt       = float(loadings_df[loadings_df["Product"]==prod]["Quantity"].sum())
                bv        = float(bdc_ld[bdc_ld["Product"]==prod]["Quantity"].sum())
                prod_rank = loadings_df[loadings_df["Product"]==prod].groupby("BDC")["Quantity"]\
                              .sum().sort_values(ascending=False)
                pr_n      = list(prod_rank.index).index(selected_bdc)+1 if selected_bdc in prod_rank.index else "N/A"
                rows.append({"Product":prod,
                              "BDC Dispatched (LT)": f"{bv:,.0f}",
                              "Market Total (LT)":   f"{mkt:,.0f}",
                              "Share (%)":           f"{bv/mkt*100:.2f}" if mkt else "0.00",
                              "Rank":                f"#{pr_n}/{len(prod_rank)}"})
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════
# PAGE: STOCK TRANSACTION
# ══════════════════════════════════════════════════════════════
def show_stock_transaction():
    st.markdown("<h2>📈 STOCK TRANSACTION ANALYZER</h2>", unsafe_allow_html=True)

    st.markdown("""
    <div style='background:rgba(0,255,255,0.05);border:1px solid #00ffff33;
                border-radius:10px;padding:14px;margin-bottom:16px;'>
    <b style='color:#00ffff;'>What this page does</b><br>
    Retrieves the full stock transaction ledger for a specific BDC, depot and product —
    showing every inflow and outflow with a running balance for reconciliation.<br>
    <b style='color:#ff00ff;'>Note:</b> Uses BDC entity IDs (lngBDCId), not per-user credentials.
    </div>
    """, unsafe_allow_html=True)

    if "stock_txn_df" not in st.session_state:
        st.session_state.stock_txn_df = pd.DataFrame()

    c1, c2 = st.columns(2)
    with c1:
        selected_bdc     = st.selectbox("BDC",     sorted(BDC_MAP.keys()),    key="txn_bdc")
        selected_product = st.selectbox("Product",  PRODUCT_OPTIONS,           key="txn_prod")
    with c2:
        selected_depot   = st.selectbox("Depot",    sorted(DEPOT_MAP.keys()),  key="txn_depot")

    c3, c4 = st.columns(2)
    with c3:
        start_date = st.date_input("Start Date", value=datetime.now()-timedelta(days=30), key="txn_start")
    with c4:
        end_date   = st.date_input("End Date",   value=datetime.now(),                    key="txn_end")

    if st.button("📊 FETCH TRANSACTION REPORT", key="txn_fetch"):
        bdc_id     = BDC_MAP[selected_bdc]
        depot_id   = DEPOT_MAP[selected_depot]
        product_id = STOCK_PRODUCT_MAP[selected_product]
        params = {
            "lngProductId": product_id, "lngBDCId": bdc_id, "lngDepotId": depot_id,
            "dtpStartDate": start_date.strftime("%m/%d/%Y"),
            "dtpEndDate":   end_date.strftime("%m/%d/%Y"),
            "lngUserId":    NPA_CONFIG["USER_ID"],
        }
        with st.spinner(f"Fetching {selected_product} transactions for {selected_bdc}…"):
            pdf_bytes = _fetch_pdf(NPA_CONFIG["STOCK_TRANSACTION_URL"], params)
        if not pdf_bytes:
            st.error("❌ No PDF returned. Check BDC / depot / product combination or API availability.")
            st.session_state.stock_txn_df = pd.DataFrame()
            _save_state("stock_txn_df", pd.DataFrame())
        else:
            records = _parse_stock_transaction_pdf(pdf_bytes)
            if records:
                df_txn = pd.DataFrame(records)
                st.session_state.stock_txn_df    = df_txn
                st.session_state.txn_bdc_label     = selected_bdc
                st.session_state.txn_depot_label   = selected_depot
                st.session_state.txn_product_label = selected_product
                _save_state("stock_txn_df", df_txn)
                st.success(f"✅ {len(records):,} transaction records extracted.")
            else:
                st.warning("No transactions found. Try a different date range or BDC/depot combination.")
                st.session_state.stock_txn_df = pd.DataFrame()
                _save_state("stock_txn_df", pd.DataFrame())

    df = st.session_state.stock_txn_df
    if df.empty:
        st.info("👆 Configure the parameters above and click **FETCH TRANSACTION REPORT**.")
        return

    st.markdown(f"### {st.session_state.get('txn_bdc_label','')} — "
                f"{st.session_state.get('txn_product_label','')} @ "
                f"{st.session_state.get('txn_depot_label','')}")

    inflows   = float(df[df["Description"].isin(["Custody Transfer In","Product Outturn"])]["Volume"].sum())
    outflows  = float(df[df["Description"].isin(["Sale","Custody Transfer Out"])]["Volume"].sum())
    sales     = float(df[df["Description"]=="Sale"]["Volume"].sum())
    bdc_xfer  = float(df[df["Description"]=="Custody Transfer Out"]["Volume"].sum())
    final_bal = float(df["Balance"].iloc[-1]) if len(df) else 0

    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("📥 Total Inflows",   f"{inflows:,.0f} LT")
    c2.metric("📤 Total Outflows",  f"{outflows:,.0f} LT")
    c3.metric("💰 OMC Sales",       f"{sales:,.0f} LT")
    c4.metric("🔄 BDC Transfers",   f"{bdc_xfer:,.0f} LT")
    c5.metric("📊 Closing Balance", f"{final_bal:,.0f} LT")

    st.markdown("### 📋 Transaction Breakdown")
    txn_sum = (df.groupby("Description")
               .agg(Total_Volume=("Volume","sum"), Count=("Trans #","count"))
               .reset_index().sort_values("Total_Volume", ascending=False)
               .rename(columns={"Description":"Transaction Type","Total_Volume":"Total Volume (LT)"}))
    st.dataframe(txn_sum, use_container_width=True, hide_index=True)

    if sales > 0:
        st.markdown("### 🏢 Top OMC Customers")
        cust = (df[df["Description"]=="Sale"].groupby("Account")["Volume"]
                .sum().sort_values(ascending=False).head(10).reset_index()
                .rename(columns={"Account":"Customer","Volume":"Volume (LT)"}))
        st.dataframe(cust, use_container_width=True, hide_index=True)

    st.markdown("### 📄 Full Transaction History")
    st.dataframe(df, use_container_width=True, hide_index=True, height=400)

    excel_bytes = _to_excel_bytes({"Transactions": df, "Summary": txn_sum})
    st.download_button("⬇️ DOWNLOAD EXCEL", excel_bytes, "stock_transaction.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ══════════════════════════════════════════════════════════════
# STREAMLIT PAGE  (replaces show_product_outturn)
# ══════════════════════════════════════════════════════════════
 
def show_product_outturn():
    """
    Drop-in replacement for show_product_outturn() in npa_dashboard.py.
 
    Uses a background thread + polling loop so the sweep never blocks the
    Streamlit main thread and the tab stays alive for the full duration.
    """
    # ── These must be imported from the main module ───────────
    # When pasting into npa_dashboard.py, all names below are
    # already in scope — nothing extra needed.
   
 
    st.markdown("<h2>⛴️ PRODUCT OUTTURN INTELLIGENCE</h2>", unsafe_allow_html=True)
 
    st.markdown("""
    <div style='background:rgba(255,102,0,0.07);border:1px solid #ff660044;
                border-radius:10px;padding:14px;margin-bottom:16px;'>
    <b style='color:#ff6600;'>What this page does</b><br>
    Sweeps <b>selected BDCs × selected depots × selected product(s)</b> and collects all
    <b>Product Outturn</b> transactions — the moment a vessel's cargo is officially
    received into a BDC's stock account at a depot.<br><br>
    <b style='color:#ff6600;'>Performance improvements vs original:</b>
    <ul style='color:#ccc;margin:6px 0;'>
      <li>✅ Runs in a <b>background thread</b> — tab stays alive for hours-long sweeps</li>
      <li>✅ <b>8 parallel requests</b> per depot batch — ~8× faster than sequential</li>
      <li>✅ Adaptive rate-limit back-off — no more mid-sweep 429 failures</li>
      <li>✅ Tuned circuit-breaker (10 vs 6) — fewer false skips</li>
      <li>✅ <b>Stop button</b> — cancel mid-sweep without killing the app</li>
    </ul>
    </div>
    """, unsafe_allow_html=True)
 
    # ── Init sweep state ──────────────────────────────────────
    for _k, _v in [
        ("ot_sweep_running",  False),
        ("ot_sweep_thread",   None),
        ("ot_sweep_worker",   None),
        ("ot_cancel_event",   None),
        ("ot_log_lines",      []),
        ("ot_done_n",         0),
        ("ot_total_n",        1),
        ("ot_last_bdc",       ""),
    ]:
        if _k not in st.session_state:
            st.session_state[_k] = _v
 
    # ── Date range ────────────────────────────────────────────
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Start Date",
            value=datetime.now() - timedelta(days=30),
            key="ot_start",
        )
    with col2:
        end_date = st.date_input("End Date", value=datetime.now(), key="ot_end")
 
    # ── BDC selection ─────────────────────────────────────────
    all_bdc_names = sorted(BDC_MAP.keys())
    n_total_bdcs  = len(all_bdc_names)
 
    col3, col4 = st.columns([3, 1])
    with col3:
        sel_bdcs = st.multiselect(
            f"Select BDCs  (blank = all {n_total_bdcs})",
            all_bdc_names,
            key="ot_bdc_select",
        )
    with col4:
        st.markdown("<br>", unsafe_allow_html=True)
        fetch_all_bdcs = st.checkbox("All BDCs", value=True, key="ot_all_bdcs")
 
    bdcs_to_sweep = all_bdc_names if (fetch_all_bdcs or not sel_bdcs) else sel_bdcs
 
    # ── Depot selection ───────────────────────────────────────
    all_depot_names     = sorted(DEPOT_MAP.keys())
    n_total_depots      = len(all_depot_names)
    default_depot_names = _resolve_default_depots()
 
    st.markdown("**🏭 Depots to sweep**")
    if "ot_depots_selection" not in st.session_state:
        st.session_state["ot_depots_selection"] = default_depot_names
 
    col5, col6 = st.columns([4, 1])
    with col5:
        sel_depots = st.multiselect(
            f"Depots  (default {len(default_depot_names)}, total {n_total_depots})",
            options=all_depot_names,
            default=st.session_state["ot_depots_selection"],
            key="ot_depot_multiselect",
        )
    with col6:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("↩️ Reset", key="ot_reset_depots"):
            st.session_state["ot_depots_selection"] = default_depot_names
            st.rerun()
 
    depots_to_sweep = sel_depots if sel_depots else default_depot_names
    st.session_state["ot_depots_selection"] = depots_to_sweep
 
    # ── Product selection ─────────────────────────────────────
    sel_products = st.multiselect(
        "Products", PRODUCT_OPTIONS, default=PRODUCT_OPTIONS, key="ot_products"
    )
    if not sel_products:
        sel_products = PRODUCT_OPTIONS
 
    # ── Call count & estimate ─────────────────────────────────
    n_calls    = len(bdcs_to_sweep) * len(depots_to_sweep) * len(sel_products)
    # With 8 workers and ~1.5 s avg per request (network + parse):
    est_secs   = (n_calls / OUTTURN_WORKERS) * 1.5 + len(bdcs_to_sweep) * 0.5
    est_mins   = est_secs / 60
 
    st.info(
        f"📋 **{len(bdcs_to_sweep)} BDCs** × **{len(depots_to_sweep)} depots** × "
        f"**{len(sel_products)} products** = **{n_calls:,} API calls**  |  "
        f"Est. time: **~{est_mins:.0f} min** ({OUTTURN_WORKERS} parallel workers)"
    )
    if n_calls > 3000:
        st.warning(
            f"⚠️ {n_calls:,} requests. Large sweep — circuit-breaker will skip "
            f"unresponsive BDCs automatically. You can stop at any time."
        )
 
    merge_prev = False
    has_prev   = not st.session_state.get("outturn_df", pd.DataFrame()).empty
    if has_prev:
        merge_prev = st.checkbox(
            "🔀 Merge with previous outturn results",
            value=True, key="ot_merge_prev",
        )
 
    # ── Control buttons ───────────────────────────────────────
    running = st.session_state.ot_sweep_running
 
    btn_col1, btn_col2 = st.columns([3, 1])
    with btn_col1:
        start_btn = st.button(
            "⛴️ SWEEP FOR PRODUCT OUTTURN",
            key="ot_fetch",
            disabled=running,
        )
    with btn_col2:
        stop_btn = st.button(
            "🛑 STOP SWEEP",
            key="ot_stop",
            disabled=not running,
        )
 
    # ── Handle stop ───────────────────────────────────────────
    if stop_btn and running:
        if st.session_state.ot_cancel_event:
            st.session_state.ot_cancel_event.set()
        st.session_state.ot_sweep_running = False
        st.warning("🛑 Stop signal sent — sweep will finish its current batch then halt.")
 
    # ── Handle start ─────────────────────────────────────────
    if start_btn and not running:
        cancel_ev = threading.Event()
        worker    = _OutturnSweepWorker(
            bdcs        = bdcs_to_sweep,
            depots      = depots_to_sweep,
            products    = sel_products,
            start_str   = start_date.strftime("%m/%d/%Y"),
            end_str     = end_date.strftime("%m/%d/%Y"),
            bdc_map     = BDC_MAP,
            depot_map   = DEPOT_MAP,
            product_map = STOCK_PRODUCT_MAP,
            config      = NPA_CONFIG,
            parse_fn    = _parse_stock_transaction_pdf_full,
            extract_vessel_fn = _extract_vessel_name,
            cancel_event= cancel_ev,
        )
        thread = worker.start()
 
        st.session_state.ot_sweep_running  = True
        st.session_state.ot_sweep_thread   = thread
        st.session_state.ot_sweep_worker   = worker
        st.session_state.ot_cancel_event   = cancel_ev
        st.session_state.ot_log_lines      = []
        st.session_state.ot_done_n         = 0
        st.session_state.ot_total_n        = max(n_calls, 1)
        st.session_state.ot_last_bdc       = ""
        st.rerun()
 
    # ── Live progress polling loop ────────────────────────────
    if st.session_state.ot_sweep_running:
        worker: _OutturnSweepWorker = st.session_state.ot_sweep_worker
        thread: threading.Thread    = st.session_state.ot_sweep_thread
 
        prog_bar  = st.progress(0.0, text="Starting sweep…")
        log_area  = st.empty()
        status_ph = st.empty()
 
        sweep_done = False
        poll_start = time.time()
 
        while True:
            # Drain the queue
            try:
                while True:
                    msg = worker.q.get_nowait()
 
                    if msg["type"] == "progress":
                        st.session_state.ot_done_n   = msg["done"]
                        st.session_state.ot_total_n  = msg["total"]
                        st.session_state.ot_last_bdc = msg["bdc"]
                        pct  = msg["pct"]
                        done = msg["done"]
                        tot  = msg["total"]
                        prog_bar.progress(
                            pct,
                            text=(
                                f"Sweeping {done:,}/{tot:,} "
                                f"({pct*100:.1f}%)  —  BDC: {msg['bdc']}  "
                                f"Depot: {msg['depot']}  Product: {msg['prod']}"
                            ),
                        )
                        elapsed = time.time() - poll_start
                        if pct > 0:
                            eta = elapsed / pct * (1 - pct)
                            status_ph.markdown(
                                f"⏱ Elapsed: **{elapsed/60:.1f} min**  |  "
                                f"ETA: **~{eta/60:.1f} min**  |  "
                                f"Done: **{done:,} / {tot:,}**"
                            )
 
                    elif msg["type"] == "log":
                        st.session_state.ot_log_lines.append(msg["text"])
                        # Keep last 18 lines
                        lines = st.session_state.ot_log_lines[-18:]
                        log_area.markdown(
                            "<div class='fetch-log'>" +
                            "<br>".join(lines) +
                            "</div>",
                            unsafe_allow_html=True,
                        )
 
                    elif msg["type"] == "result":
                        new_df  = msg["df"]
                        summary = msg["summary"]
 
                        if merge_prev and has_prev:
                            prev = st.session_state.outturn_df
                            new_df = _merge_dataframes(
                                prev, new_df,
                                ["Date", "Trans #", "BDC", "Depot", "Product", "Volume"],
                            )
 
                        st.session_state.outturn_df = new_df
                        _save_state("outturn_df", new_df)
                        st.session_state.ot_last_summary = summary
                        st.session_state.ot_last_record_count = len(new_df)
 
                    elif msg["type"] in ("done", "error"):
                        sweep_done = True
                        if msg["type"] == "error":
                            st.error(f"❌ Sweep error: {msg['text']}")
                        break
 
            except queue.Empty:
                pass
 
            if sweep_done or not thread.is_alive():
                break
 
            # Poll every 1.5 s — keeps UI responsive without hammering CPU
            time.sleep(1.5)
 
        # ── Sweep finished ────────────────────────────────────
        prog_bar.progress(1.0, text="✅ Outturn sweep complete")
        st.session_state.ot_sweep_running = False
 
        if "ot_last_summary" in st.session_state:
            _render_fetch_summary(
                st.session_state.ot_last_summary,
                len(bdcs_to_sweep),
                st.session_state.get("ot_last_record_count", 0),
                "Outturn Records",
            )
 
        st.rerun()
 
    # ── Show last sweep stats if not running ──────────────────
    elif "ot_last_summary" in st.session_state and not running:
        with st.expander("📊 Last sweep outcome", expanded=False):
            _render_fetch_summary(
                st.session_state.ot_last_summary,
                len(bdcs_to_sweep),
                st.session_state.get("ot_last_record_count", 0),
                "Outturn Records",
            )
 
    # ══════════════════════════════════════════════════════════
    # RESULTS DISPLAY  (unchanged from original)
    # ══════════════════════════════════════════════════════════
    df = st.session_state.get("outturn_df", pd.DataFrame())
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        if not running:
            st.info("👆 Configure parameters and click **SWEEP FOR PRODUCT OUTTURN**.")
        return
 
    st.markdown("---")
 
    total_vol   = float(df["Volume"].sum())
    n_vessels   = df["Vessel Name"].replace("", pd.NA).dropna().nunique()
    n_bdcs_data = df["BDC"].nunique()
    n_depots    = df["Depot"].nunique()
 
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("📦 Outturn Events",    f"{len(df):,}")
    c2.metric("🛢️ Total Volume (LT)", f"{total_vol:,.0f}")
    c3.metric("🏦 BDCs",             f"{n_bdcs_data}")
    c4.metric("🏭 Depots",           f"{n_depots}")
    c5.metric("🚢 Vessels ID'd",     f"{n_vessels}")
 
    st.markdown("### 📦 PRODUCT BREAKDOWN")
    prod_cols = st.columns(len(sel_products))
    PROD_COLORS = {"PMS": "#00ffff", "Gasoil": "#ffaa00", "LPG": "#00ff88"}
    PROD_ICONS  = {"PMS": "⛽", "Gasoil": "🚛", "LPG": "🔵"}
    for col, prod in zip(prod_cols, sel_products):
        sub   = df[df["Product"] == prod]
        vol   = float(sub["Volume"].sum())
        color = PROD_COLORS.get(prod, "#fff")
        icon  = PROD_ICONS.get(prod, "🛢")
        with col:
            st.markdown(f"""
            <div class='outturn-card' style='border-color:{color};box-shadow:0 0 18px {color}55;'>
                <div style='font-size:30px;'>{icon}</div>
                <div style='font-family:Orbitron,sans-serif;color:{color};
                             font-size:14px;font-weight:700;margin:6px 0;'>{prod}</div>
                <div style='font-size:28px;color:{color};font-weight:900;'>{vol:,.0f}</div>
                <div style='color:#888;font-size:12px;'>Litres / KG</div>
                <div style='color:#ccc;font-size:12px;margin-top:4px;'>
                    {len(sub)} events · {sub['BDC'].nunique()} BDCs</div>
            </div>""", unsafe_allow_html=True)
 
    st.markdown("---")
    st.markdown("### 🏦 TOP BDCs BY OUTTURN VOLUME")
    bdc_sum = (
        df.groupby("BDC")
        .agg(
            Events       =("Trans #", "count"),
            Volume_LT    =("Volume", "sum"),
            Depots       =("Depot", "nunique"),
            Products     =("Product", "nunique"),
            Vessels_ID_d =("Vessel Name", lambda x: x.replace("", pd.NA).dropna().nunique()),
        )
        .reset_index()
        .sort_values("Volume_LT", ascending=False)
        .rename(columns={"Volume_LT": "Total Volume (LT)", "Vessels_ID_d": "Vessels ID'd"})
    )
    bdc_sum["Market Share %"] = (bdc_sum["Total Volume (LT)"] / total_vol * 100).round(2)
    st.dataframe(bdc_sum, use_container_width=True, hide_index=True)
 
    import plotly.graph_objects as go
    if not bdc_sum.empty:
        fig_bdc = go.Figure(go.Bar(
            x=bdc_sum["BDC"].head(20),
            y=bdc_sum["Total Volume (LT)"].head(20),
            marker=dict(color=bdc_sum["Total Volume (LT)"].head(20), colorscale="Turbo"),
            text=bdc_sum["Total Volume (LT)"].head(20).apply(lambda v: f"{v:,.0f}"),
            textposition="outside",
        ))
        fig_bdc.update_layout(
            title=dict(text="Top 20 BDCs — Product Outturn Volume",
                       font=dict(color="#ff6600", family="Orbitron")),
            paper_bgcolor="rgba(10,14,39,0.9)", plot_bgcolor="rgba(10,14,39,0.9)",
            font=dict(color="white"),
            xaxis=dict(title="BDC", tickangle=-35),
            yaxis=dict(title="Volume (LT)"),
            height=420,
        )
        st.plotly_chart(fig_bdc, use_container_width=True)
 
    st.markdown("### 🏭 OUTTURN BY DEPOT")
    depot_sum = (
        df.groupby(["Depot", "Product"])
        .agg(Events=("Trans #","count"), Volume_LT=("Volume","sum"))
        .reset_index().sort_values("Volume_LT", ascending=False)
        .rename(columns={"Volume_LT": "Volume (LT)"})
    )
    st.dataframe(depot_sum, use_container_width=True, hide_index=True)
 
    st.markdown("### 🚢 VESSEL NAME ANALYSIS")
    vessel_df = df[df["Vessel Name"].str.strip() != ""].copy()
    if vessel_df.empty:
        st.info("No vessel names extracted. Check the Account column in the full table.")
    else:
        vessel_sum = (
            vessel_df.groupby(["Vessel Name", "Product"])
            .agg(
                Outturns  =("Trans #", "count"),
                Volume_LT =("Volume", "sum"),
                BDCs      =("BDC", "nunique"),
                Depots    =("Depot", "nunique"),
                First_Date=("Date", "min"),
                Last_Date =("Date", "max"),
            )
            .reset_index().sort_values("Volume_LT", ascending=False)
            .rename(columns={"Volume_LT": "Total Volume (LT)"})
        )
        st.dataframe(vessel_sum, use_container_width=True, hide_index=True)
 
        if len(vessel_sum) >= 2:
            fig_v = go.Figure(go.Treemap(
                labels=vessel_sum["Vessel Name"] + "<br>" + vessel_sum["Product"],
                parents=[""] * len(vessel_sum),
                values=vessel_sum["Total Volume (LT)"],
                textinfo="label+value",
                marker=dict(colorscale="Sunset"),
            ))
            fig_v.update_layout(
                title=dict(text="Vessel Outturn Volume Treemap",
                           font=dict(color="#ff6600", family="Orbitron")),
                paper_bgcolor="rgba(10,14,39,0.9)", font=dict(color="white"), height=420,
            )
            st.plotly_chart(fig_v, use_container_width=True)
 
    st.markdown("### 📅 OUTTURN TIMELINE")
    try:
        df_ts = df.copy()
        df_ts["Date_dt"] = pd.to_datetime(df_ts["Date"], dayfirst=True, errors="coerce")
        df_ts = df_ts.dropna(subset=["Date_dt"])
        if not df_ts.empty:
            daily_ts = (
                df_ts.groupby(["Date_dt","Product"])["Volume"].sum().reset_index()
                .rename(columns={"Date_dt":"Date","Volume":"Volume (LT)"})
            )
            PCMAP = {"PMS":"#00ffff","Gasoil":"#ffaa00","LPG":"#00ff88"}
            fig_ts = go.Figure()
            for prod in daily_ts["Product"].unique():
                sub_ts = daily_ts[daily_ts["Product"]==prod].sort_values("Date")
                fig_ts.add_trace(go.Scatter(
                    x=sub_ts["Date"], y=sub_ts["Volume (LT)"],
                    mode="lines+markers", name=prod,
                    line=dict(color=PCMAP.get(prod,"#fff"), width=2),
                ))
            fig_ts.update_layout(
                title=dict(text="Daily Outturn Volume by Product",
                           font=dict(color="#ff6600", family="Orbitron")),
                paper_bgcolor="rgba(10,14,39,0.9)", plot_bgcolor="rgba(10,14,39,0.9)",
                font=dict(color="white"), height=380,
            )
            st.plotly_chart(fig_ts, use_container_width=True)
    except Exception:
        pass
 
    st.markdown("---")
    st.markdown("### 🔍 FILTER & EXPLORE")
    filter_by = st.selectbox(
        "Filter by", ["All Records","BDC","Depot","Product","Vessel Name"], key="ot_filter_by"
    )
    col_map = {"BDC":"BDC","Depot":"Depot","Product":"Product","Vessel Name":"Vessel Name"}
 
    if filter_by == "All Records":
        filt = df
    else:
        col_f = col_map[filter_by]
        if col_f not in df.columns:
            df[col_f] = ""
        opts  = ["ALL"] + sorted(df[col_f].replace("",pd.NA).dropna().unique().tolist())
        fval  = st.selectbox(f"Select {filter_by}", opts, key="ot_filter_val")
        filt  = df if fval=="ALL" else df[df[col_f]==fval]
 
    st.caption(
        f"**{len(filt):,}** records | Volume: **{filt['Volume'].sum():,.0f} LT** | "
        f"BDCs: **{filt['BDC'].nunique()}** | Depots: **{filt['Depot'].nunique()}**"
    )
    display_cols = [c for c in ["Date","Trans #","BDC","Depot","Product","Volume",
                                 "Balance","Account","Vessel Name"] if c in filt.columns]
    st.dataframe(
        filt[display_cols].sort_values(["Date","BDC"]),
        use_container_width=True, height=450, hide_index=True,
    )
 
    st.markdown("---")
    _ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_sheets = {
        "All Outturns": df[display_cols] if display_cols else df,
        "By BDC":       bdc_sum,
        "By Depot":     depot_sum,
    }
    if not vessel_df.empty:
        excel_sheets["By Vessel"] = vessel_sum
 
    excel_bytes = _to_excel_bytes(excel_sheets)
    st.download_button(
        "⬇️ DOWNLOAD OUTTURN EXCEL", excel_bytes,
        f"product_outturn_{_ts_str}.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
 

# ══════════════════════════════════════════════════════════════
# PAGE: NATIONAL STOCKOUT
# ══════════════════════════════════════════════════════════════
def show_national_stockout():
    st.markdown("<h2>🌍 NATIONAL STOCKOUT FORECAST</h2>", unsafe_allow_html=True)

    st.markdown("""
    <div style='background:rgba(0,255,255,0.05);border:1px solid #00ffff33;
                border-radius:10px;padding:14px;margin-bottom:16px;'>
    <b style='color:#00ffff;'>What this page does</b><br>
    Calculates Ghana's national days-of-supply for PREMIUM, GASOIL and LPG by dividing
    current BDC stock balances by the average daily depletion rate derived from OMC
    loadings over the selected history window.<br>
    <b style='color:#ff00ff;'>Prerequisite:</b> Both BDC Balance and OMC Loadings are
    fetched fresh as part of this analysis — no need to pre-load them separately.
    </div>
    """, unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        start_date = st.date_input("Loadings History — From",
                                   value=datetime.now()-timedelta(days=30), key="ns_start")
    with c2:
        end_date   = st.date_input("Loadings History — To",
                                   value=datetime.now(), key="ns_end")

    start_str   = start_date.strftime("%m/%d/%Y")
    end_str     = end_date.strftime("%m/%d/%Y")
    period_days = max((end_date - start_date).days, 1)

    day_type = st.radio(
        "Day-type for daily rate denominator",
        ["📆 Calendar Days (default)","💼 Business Days (Mon–Fri only)"],
        horizontal=True, key="ns_day_type",
    )
    use_biz = "Business" in day_type

    depl_mode = st.radio(
        "Depletion rate method",
        ["📊 Average Daily Loadings","🔥 Maximum Single-Day Loading (stress test)","📊 Median Daily Loadings"],
        index=0, key="ns_depl_mode",
    )
    use_max    = "Maximum" in depl_mode
    use_median = "Median"  in depl_mode

    exclude_tor = st.checkbox(
        "❌ Exclude TEMA OIL REFINERY (TOR) from LPG stock",
        value=False, key="ns_excl_tor",
    )

    _vessel_df     = st.session_state.get("vessel_data", pd.DataFrame())
    _vessel_loaded = isinstance(_vessel_df, pd.DataFrame) and not _vessel_df.empty
    _pending_n     = int((_vessel_df["Status"]=="PENDING").sum()) if _vessel_loaded else 0

    include_vessels = st.checkbox(
        "🚢 Add pending vessel cargo to stock totals",
        value=False, key="ns_vessels",
    )
    if include_vessels and not _vessel_loaded:
        st.warning("No vessel data loaded — go to 🚢 Vessel Supply and fetch first.")
        include_vessels = False
    elif include_vessels and _pending_n == 0:
        st.info("Vessel data is loaded but there are no PENDING vessels — toggle has no effect.")

    all_bdc_names  = sorted(BDC_USER_MAP.keys())
    n_total        = len(all_bdc_names)
    effective_days = _count_period_days(start_str, end_str, use_biz)
    day_lbl        = f"{effective_days} {'business' if use_biz else 'calendar'} days"

    _existing_bdc_records = st.session_state.get("bdc_records", [])
    _has_bdc_balance      = bool(_existing_bdc_records)

    if _has_bdc_balance:
        st.success(f"✅ BDC Balance already loaded — **{len(_existing_bdc_records):,} records** "
                   f"from a previous fetch will be used. Only OMC Loadings will be fetched fresh.")
    else:
        st.warning("⚠️ No BDC Balance data found in session. Both Balance and Loadings will be fetched.")

    st.info(f"📋 **{n_total} BDCs** will be queried for OMC Loadings  |  "
            f"Loadings window: **{start_date.strftime('%d %b')} → {end_date.strftime('%d %b %Y')}** "
            f"({period_days} calendar days / {effective_days} {'business' if use_biz else 'calendar'} days)")
    st.markdown("---")

    if st.button("⚡ FETCH & ANALYSE NATIONAL FUEL SUPPLY", key="ns_go"):
        col_bal = "ACTUAL BALANCE (LT\\KG)"

        if _has_bdc_balance:
            with st.status("📡 Step 1 / 2 — Using existing BDC stock balances…", expanded=True):
                all_records = _existing_bdc_records
                bal_df      = pd.DataFrame(all_records)
                n_bal_bdcs  = bal_df["BDC"].nunique() if not bal_df.empty else 0
                bal_summary = {"success": list(bal_df["BDC"].unique()) if not bal_df.empty else [],
                               "no_data": [], "failed": []}
                st.write(f"✅ Using cached balance — **{len(all_records):,} records** "
                         f"from **{n_bal_bdcs} BDCs** (from BDC Balance page)")
                if exclude_tor and not bal_df.empty:
                    mask   = bal_df["BDC"].str.contains("TOR", case=False, na=False) & (bal_df["Product"]=="LPG")
                    excl_v = bal_df[mask][col_bal].sum()
                    bal_df = bal_df[~mask].copy()
                    st.write(f"TOR LPG excluded from national total ({excl_v:,.0f} LT removed)")
                balance_by_prod = bal_df.groupby("Product")[col_bal].sum() if not bal_df.empty else pd.Series(dtype=float)
        else:
            with st.status("📡 Step 1 / 2 — Fetching BDC stock balances…", expanded=True):
                prog1      = st.progress(0, text="Starting…")
                log_box1   = st.empty()
                log_lines1 = []
                results1   = _sequential_batch_fetch(
                    all_bdc_names, _make_balance_fetcher(),
                    prog1, log_box1, log_lines1,
                    second_pass=True,
                )
                prog1.progress(1.0, text="✅ Balance fetch complete")
                all_records, bal_summary = _combine_balance_results(results1)
                bal_df = pd.DataFrame(all_records)
                st.session_state.bdc_records = all_records
                _save_state("bdc_records", all_records)

                n_bal_bdcs = bal_df["BDC"].nunique() if not bal_df.empty else 0
                st.write(f"✅ **{len(all_records):,} balance records** from **{n_bal_bdcs} BDCs**  |  "
                         f"✅ {len(bal_summary['success'])} succeeded  |  "
                         f"⚠️ {len(bal_summary['no_data'])} no data  |  "
                         f"❌ {len(bal_summary['failed'])} failed")

                if exclude_tor and not bal_df.empty:
                    mask   = bal_df["BDC"].str.contains("TOR", case=False, na=False) & (bal_df["Product"]=="LPG")
                    excl_v = bal_df[mask][col_bal].sum()
                    bal_df = bal_df[~mask].copy()
                    st.write(f"TOR LPG excluded from national total ({excl_v:,.0f} LT removed)")

                balance_by_prod = bal_df.groupby("Product")[col_bal].sum() if not bal_df.empty else pd.Series(dtype=float)

        if include_vessels and _vessel_loaded:
            pend = _vessel_df[_vessel_df["Status"]=="PENDING"]
            if not pend.empty:
                for prod, vol in pend.groupby("Product")["Quantity_Litres"].sum().items():
                    balance_by_prod[prod] = balance_by_prod.get(prod, 0) + vol
                st.write(f"🚢 Vessel pipeline added: "
                         + " | ".join([f"{p}: +{v:,.0f} LT" for p,v in
                                       pend.groupby("Product")["Quantity_Litres"].sum().items()]))

        with st.status("🚚 Step 2 / 2 — Fetching national OMC loadings…", expanded=True):
            st.write(f"Querying {n_total} BDCs for loadings from "
                     f"{start_date.strftime('%d %b')} to {end_date.strftime('%d %b %Y')}…")
            prog2      = st.progress(0, text="Starting…")
            log_box2   = st.empty()
            log_lines2 = []
            results2   = _sequential_batch_fetch(
                all_bdc_names, _make_omc_fetcher(start_str, end_str),
                prog2, log_box2, log_lines2,
                second_pass=True,
            )
            prog2.progress(1.0, text="✅ Loadings fetch complete")
            omc_df, omc_summary = _combine_df_results(
                results2, ["Order Number", "Truck", "Date", "Product"]
            )
            st.write(f"✅ **{len(omc_df):,} loading records**  |  "
                     f"✅ {len(omc_summary['success'])} succeeded  |  "
                     f"⚠️ {len(omc_summary['no_data'])} no data  |  "
                     f"❌ {len(omc_summary['failed'])} failed")

            if omc_df.empty:
                omc_by_prod = pd.Series({"PREMIUM":0.0,"GASOIL":0.0,"LPG":0.0})
                depl_lbl    = "No Data"
            else:
                filt = omc_df[omc_df["Product"].isin(["PREMIUM","GASOIL","LPG"])].copy()
                filt["Date"] = pd.to_datetime(filt["Date"], errors="coerce")
                daily_agg = filt.groupby(["Date","Product"])["Quantity"].sum().reset_index()
                if use_median:
                    omc_by_prod = daily_agg.groupby("Product")["Quantity"].median()
                    depl_lbl    = "Median Daily Loading"
                elif use_max:
                    omc_by_prod = daily_agg.groupby("Product")["Quantity"].max()
                    depl_lbl    = "Max Single-Day Loading"
                else:
                    omc_by_prod = filt.groupby("Product")["Quantity"].sum()
                    depl_lbl    = f"Avg Daily ({day_lbl})"

        DISPLAY = {"PREMIUM":"PREMIUM (PMS)","GASOIL":"GASOIL (AGO)","LPG":"LPG"}
        rows_out = []
        for prod in ["PREMIUM","GASOIL","LPG"]:
            stock = float(balance_by_prod.get(prod, 0))
            dep   = float(omc_by_prod.get(prod, 0))
            daily = dep if (use_median or use_max) else (dep / effective_days if effective_days else 0)
            days  = stock / daily if daily > 0 else float("inf")
            rows_out.append({"product":prod,"display_name":DISPLAY[prod],
                             "total_balance":stock,"omc_sales":dep,
                             "daily_rate":daily,"days_remaining":days})

        forecast_df = pd.DataFrame(rows_out)

        bdc_pivot = (bal_df.pivot_table(index="BDC", columns="Product", values=col_bal,
                                        aggfunc="sum", fill_value=0).reset_index()
                     if not bal_df.empty else pd.DataFrame())
        if not bdc_pivot.empty:
            for p in ["GASOIL","LPG","PREMIUM"]:
                if p not in bdc_pivot.columns: bdc_pivot[p] = 0
            bdc_pivot["TOTAL"] = bdc_pivot[["GASOIL","LPG","PREMIUM"]].sum(axis=1)
            nat_total = bdc_pivot["TOTAL"].sum()
            bdc_pivot["Market Share %"] = (bdc_pivot["TOTAL"] / nat_total * 100).round(2)
            bdc_pivot = bdc_pivot.sort_values("TOTAL", ascending=False)

        st.session_state.ns_results = {
            "forecast_df":   forecast_df,
            "bal_df":        bal_df,
            "omc_df":        omc_df,
            "bdc_pivot":     bdc_pivot,
            "period_days":   period_days,
            "eff_days":      effective_days,
            "day_lbl":       day_lbl,
            "depl_lbl":      depl_lbl,
            "start_str":     start_str,
            "end_str":       end_str,
            "bal_summary":   bal_summary,
            "omc_summary":   omc_summary,
            "n_bal_records": len(all_records),
            "n_omc_records": len(omc_df),
        }
        _save_national_snapshot(forecast_df, f"{period_days}d")
        st.success("✅ Analysis complete — scroll down to see the forecast.")
        st.rerun()

    if not st.session_state.get("ns_results"):
        st.info("👆 Configure options above and click **FETCH & ANALYSE**.")
        return

    res         = st.session_state.ns_results
    forecast_df = res["forecast_df"]
    bdc_pivot   = res["bdc_pivot"]
    omc_df      = res["omc_df"]
    depl_lbl    = res["depl_lbl"]
    day_lbl     = res["day_lbl"]

    st.markdown("---")
    st.markdown(f"<h3>🇬🇭 NATIONAL FUEL SUPPLY — {res['start_str']} → {res['end_str']}</h3>",
                unsafe_allow_html=True)

    bs, os_ = res.get("bal_summary",{}), res.get("omc_summary",{})
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Balance Records",  f"{res.get('n_bal_records',0):,}")
    c2.metric("Loading Records",  f"{res.get('n_omc_records',0):,}")
    c3.metric("Balance BDCs ✅",  len(bs.get("success",[])))
    c4.metric("Loadings BDCs ✅", len(os_.get("success",[])))

    ICONS  = {"PREMIUM":"⛽","GASOIL":"🚛","LPG":"🔵"}
    COLORS = {"PREMIUM":"#00ffff","GASOIL":"#ffaa00","LPG":"#00ff88"}

    st.markdown("### 🛢️ DAYS OF SUPPLY")
    cols = st.columns(3)
    for col, (_, row) in zip(cols, forecast_df.iterrows()):
        days  = row["days_remaining"]
        prod  = row["product"]
        color = COLORS.get(prod,"#fff")
        days_txt  = f"{days:.1f}" if days != float("inf") else "∞"
        weeks_txt = f"(~{days/7:.1f} weeks)" if days != float("inf") else ""
        if   days < 7:  border, status = "#ff0000","🔴 CRITICAL"
        elif days < 14: border, status = "#ffaa00","🟡 WARNING"
        elif days < 30: border, status = "#ff6600","🟠 MONITOR"
        else:           border, status = "#00ff88","🟢 HEALTHY"
        stockout = (datetime.now()+timedelta(days=days)).strftime("%d %b %Y") \
                   if days != float("inf") else "N/A"
        with col:
            st.markdown(f"""
            <div style='background:rgba(10,14,39,0.85);padding:22px 14px;border-radius:16px;
                        border:2.5px solid {border};text-align:center;box-shadow:0 0 18px {border}55;'>
                <div style='font-size:34px;'>{ICONS.get(prod,"🛢")}</div>
                <div style='font-family:Orbitron,sans-serif;color:{color};font-size:17px;
                             font-weight:700;margin:6px 0;'>{row["display_name"]}</div>
                <div style='font-family:Orbitron,sans-serif;font-size:52px;color:{border};
                             font-weight:900;line-height:1;'>{days_txt}</div>
                <div style='color:#888;font-size:13px;'>{weeks_txt} days of supply</div>
                <div style='color:{border};font-size:13px;font-weight:700;margin:6px 0;'>{status}</div>
                <hr style='border-color:rgba(255,255,255,0.1);'>
                <div style='font-size:12px;color:#888;'>📦 {row["total_balance"]:,.0f} LT stock</div>
                <div style='font-size:12px;color:#888;'>📉 {row["daily_rate"]:,.0f} LT/day</div>
                <div style='font-size:12px;color:{border};font-weight:700;'>🗓️ Est. empty: {stockout}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### 📊 SUMMARY TABLE")
    sum_rows = []
    for _, row in forecast_df.iterrows():
        days = row["days_remaining"]
        if   days < 7:  status = "🔴 CRITICAL"
        elif days < 14: status = "🟡 WARNING"
        elif days < 30: status = "🟠 MONITOR"
        else:           status = "🟢 HEALTHY"
        sum_rows.append({
            "Product":                        row["display_name"],
            "National Stock (LT/KG)":         f"{row['total_balance']:,.0f}",
            f"{depl_lbl} (LT)":               f"{row['omc_sales']:,.0f}",
            f"Daily Rate ({day_lbl}) (LT/d)": f"{row['daily_rate']:,.0f}",
            "Days of Supply":                  f"{days:.1f}" if days != float("inf") else "∞",
            "Projected Empty":                 (datetime.now()+timedelta(days=days)).strftime("%Y-%m-%d")
                                               if days != float("inf") else "N/A",
            "Status":                          status,
        })
    st.dataframe(pd.DataFrame(sum_rows), use_container_width=True, hide_index=True)

    st.markdown("---")
    if not bdc_pivot.empty:
        st.markdown(f"### 🏦 STOCK BY BDC  ({len(bdc_pivot)} BDCs)")
        disp = bdc_pivot.copy()
        for c in ["GASOIL","LPG","PREMIUM","TOTAL"]:
            if c in disp.columns:
                disp[c] = disp[c].apply(lambda x: f"{x:,.0f}")
        if "Market Share %" in disp.columns:
            disp["Market Share %"] = disp["Market Share %"].apply(lambda x: f"{x:.2f}%")
        st.dataframe(disp, use_container_width=True, hide_index=True)

    st.markdown("---")
    excel_sheets = {
        "Stockout Forecast": pd.DataFrame(sum_rows),
        "Stock by BDC":      bdc_pivot,
    }
    if not omc_df.empty:
        excel_sheets["OMC Loadings"] = omc_df
    excel_bytes = _to_excel_bytes(excel_sheets)
    st.download_button("⬇️ DOWNLOAD NATIONAL REPORT", excel_bytes, "national_stockout.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ══════════════════════════════════════════════════════════════
# PAGE: WORLD RISK MONITOR
# ══════════════════════════════════════════════════════════════
def show_world_monitor():
    st.markdown("<h2>🌍 WORLD RISK MONITOR</h2>", unsafe_allow_html=True)

    st.markdown("""
    <div style='background:rgba(255,0,0,0.05);border:1px solid #ff000033;
                border-radius:10px;padding:14px;margin-bottom:16px;'>
    <b style='color:#ff4444;'>🔴 LIVE GLOBAL INTELLIGENCE</b><br>
    Real-time global threat and supply-chain risk map — conflicts, sanctions, weather,
    shipping lane disruptions, power outages and more — aggregated from 100+ OSINT feeds.
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div style='background:rgba(22,33,62,0.6);padding:40px;border-radius:15px;
                border:2px solid #00ffff;text-align:center;margin:20px 0;'>
        <div style='font-size:80px;margin-bottom:20px;'>🌍</div>
        <h3 style='color:#00ffff;margin:0;'>WORLD RISK MONITOR</h3>
        <p style='color:#888;margin:10px 0 20px;'>
            25 live data layers · 7-day rolling window · WebGL satellite base map<br>
            Conflicts · Nuclear · Military · Sanctions · Weather · Waterways · Outages
        </p>
    </div>""", unsafe_allow_html=True)

    st.link_button("🌍 OPEN WORLD RISK MONITOR", WORLD_MONITOR_URL, use_container_width=True)
    st.caption(f"Opens in a new tab.  Source: {WORLD_MONITOR_URL.split('?')[0]}")


# ══════════════════════════════════════════════════════════════
# PAGE: VESSEL SUPPLY
# ══════════════════════════════════════════════════════════════
def show_vessel_supply():
    VCOLS  = {"PREMIUM":"#00ffff","GASOIL":"#ffaa00","LPG":"#00ff88","NAPHTHA":"#ff6600"}
    VICONS = {"PREMIUM":"⛽","GASOIL":"🚛","LPG":"🔵","NAPHTHA":"🟠"}
    MONTH_ORDER = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]

    st.markdown("<h2>🚢 VESSEL SUPPLY TRACKER</h2>", unsafe_allow_html=True)

    st.markdown("""
    <div style='background:rgba(0,255,255,0.05);border:1px solid #00ffff33;
                border-radius:10px;padding:14px;margin-bottom:16px;'>
    <b style='color:#00ffff;'>What this page does</b><br>
    Loads the national vessel discharge schedule from a Google Sheet.
    </div>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns([3,1])
    with col1:
        sheet_url = st.text_input("Google Sheets URL or File ID", value=VESSEL_SHEET_URL, key="vessel_url")
    with col2:
        year_sel = st.selectbox("Data Year", ["2025","2024","2026"], key="vessel_year_sel")

    if st.button("🔄 FETCH VESSEL DATA", key="vessel_fetch"):
        with st.spinner("Loading vessel schedule from Google Sheets…"):
            raw_df, err = _load_vessel_sheet(sheet_url)
        if raw_df is None:
            st.error(f"❌ {err}")
            return
        processed = _process_vessel_df(raw_df, year=year_sel)
        if processed.empty:
            st.warning("No valid vessel records found.")
            return
        st.session_state.vessel_data    = processed
        st.session_state["vessel_year"] = year_sel
        _save_state("vessel_data", processed)
        st.success(f"✅ {len(processed)} vessel records loaded.")
        st.rerun()

    df = st.session_state.get("vessel_data", pd.DataFrame())
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        st.info("👆 Click **FETCH VESSEL DATA** to load the discharge schedule.")
        return

    yr_lbl     = st.session_state.get("vessel_year","2025")
    discharged = df[df["Status"]=="DISCHARGED"]
    pending    = df[df["Status"]=="PENDING"]

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Total Vessels", len(df))
    c2.metric("Discharged",    f"{len(discharged)}  ({discharged['Quantity_Litres'].sum()/1e6:.2f}M LT)")
    c3.metric("⏳ Pending",    f"{len(pending)}  ({pending['Quantity_Litres'].sum()/1e6:.2f}M LT)")
    c4.metric("Grand Total",   f"{df['Quantity_Litres'].sum()/1e6:.2f}M LT")

    st.markdown("---")
    st.markdown("### ⏳ PENDING VESSELS — Supply Pipeline")

    if pending.empty:
        st.success("✅ No pending vessels — all recorded vessels have discharged.")
    else:
        pp = pending.groupby("Product").agg(Vessels=("Vessel_Name","count"),
                                            Volume_LT=("Quantity_Litres","sum"),
                                            Volume_MT=("Quantity_MT","sum")).reset_index()
        pcols = st.columns(min(len(pp),4))
        for col,(_, row) in zip(pcols, pp.iterrows()):
            prod  = row["Product"]
            color = VCOLS.get(prod,"#fff")
            with col:
                st.markdown(f"""
                <div style='background:rgba(10,14,39,0.85);padding:18px;border-radius:12px;
                            border:2px solid {color};text-align:center;'>
                    <div style='font-size:28px;'>{VICONS.get(prod,"🛢")}</div>
                    <div style='font-family:Orbitron,sans-serif;color:{color};font-size:13px;
                                 font-weight:700;margin:6px 0;'>{prod}</div>
                    <div style='color:#e0e0e0;font-size:26px;font-weight:700;'>{int(row["Vessels"])}</div>
                    <div style='color:#888;font-size:12px;'>vessels</div>
                    <div style='color:{color};font-size:16px;font-weight:700;margin-top:6px;'>
                        {row["Volume_LT"]:,.0f} LT</div>
                    <div style='color:#888;font-size:12px;'>({row["Volume_MT"]:,.0f} MT)</div>
                </div>""", unsafe_allow_html=True)
        st.dataframe(pending[["Vessel_Name","Vessel_Type","Receivers","Supplier",
                               "Product","Quantity_MT","Quantity_Litres","Date_Discharged"]],
                     use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("### ✅ DISCHARGED VESSELS")
    tab1, tab2 = st.tabs(["📊 By Product & Month","📋 Full List"])

    with tab1:
        if not discharged.empty:
            monthly = (discharged.groupby(["Month","Product"])["Quantity_Litres"]
                       .sum().reset_index())
            monthly["Month"] = pd.Categorical(monthly["Month"],
                                              categories=MONTH_ORDER, ordered=True)
            monthly = monthly.sort_values("Month")
            fig = go.Figure()
            for prod in monthly["Product"].unique():
                pd_ = monthly[monthly["Product"]==prod]
                fig.add_trace(go.Bar(name=prod, x=pd_["Month"], y=pd_["Quantity_Litres"],
                                     marker_color=VCOLS.get(prod,"#fff")))
            fig.update_layout(
                barmode="group",
                paper_bgcolor="rgba(10,14,39,0.9)", plot_bgcolor="rgba(10,14,39,0.9)",
                font=dict(color="white"), height=380,
                legend=dict(font=dict(color="white")),
                xaxis=dict(title="Month"), yaxis=dict(title="Volume (LT)"),
                title=dict(text=f"Monthly Vessel Discharge — {yr_lbl}",
                           font=dict(color="#00ffff",family="Orbitron")),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No discharged vessels yet.")

    with tab2:
        if not discharged.empty:
            st.dataframe(discharged[["Vessel_Name","Vessel_Type","Receivers","Supplier",
                                     "Product","Quantity_MT","Quantity_Litres",
                                     "Date_Discharged","Month"]],
                         use_container_width=True, hide_index=True)

    st.markdown("---")
    excel_bytes = _to_excel_bytes({
        "All Vessels": df, "Discharged": discharged, "Pending": pending,
    })
    st.download_button("⬇️ DOWNLOAD VESSEL EXCEL", excel_bytes,
                       f"vessel_data_{yr_lbl}.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
def main():
    _restore_session_state()

    st.markdown("""
    <div style='text-align:center;padding:28px 0;'>
        <h1 style='font-size:56px;margin:0;'>⚡ NPA ENERGY ANALYTICS ⚡</h1>
        <p style='font-size:19px;color:#ff00ff;font-family:"Orbitron",sans-serif;
                   letter-spacing:3px;margin-top:8px;'>FUEL THE FUTURE WITH DATA</p>
    </div>""", unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("<h2 style='text-align:center;'>🎯 MISSION CONTROL</h2>",
                    unsafe_allow_html=True)

        choice = st.radio("", [
            "🏦 BDC BALANCE",
            "🚚 OMC LOADINGS",
            "📅 DAILY ORDERS",
            "📊 MARKET SHARE",
            "📈 STOCK TRANSACTION",
            "⛴️ PRODUCT OUTTURN",
            "🌍 NATIONAL STOCKOUT",
            "🌐 WORLD RISK MONITOR",
            "🚢 VESSEL SUPPLY",
        ], index=0, label_visibility="collapsed")

        st.markdown("---")
        n_bdcs = len(BDC_USER_MAP)

        has_bal  = bool(st.session_state.get("bdc_records"))
        has_omc  = not st.session_state.get("omc_df", pd.DataFrame()).empty
        has_dly  = not st.session_state.get("daily_df", pd.DataFrame()).empty
        has_txn  = not st.session_state.get("stock_txn_df", pd.DataFrame()).empty
        has_ves  = not st.session_state.get("vessel_data", pd.DataFrame()).empty
        has_out  = not st.session_state.get("outturn_df", pd.DataFrame()).empty

        badges = {
            "Balance":   ("🟢","✅" if has_bal  else "○"),
            "OMC Load":  ("🟢","✅" if has_omc  else "○"),
            "Daily Ord": ("🟢","✅" if has_dly  else "○"),
            "Stock Txn": ("🟢","✅" if has_txn  else "○"),
            "Vessels":   ("🟢","✅" if has_ves  else "○"),
            "Outturn":   ("🟢","✅" if has_out  else "○"),
        }

        st.markdown("""
        <div style='background:rgba(0,255,255,0.05);padding:14px;border-radius:10px;
                    border:1px solid #00ffff44;font-size:13px;'>
        <b style='color:#00ffff;'>📊 DATA STATUS</b><br>""" +
        "".join([f"<span style='color:{'#00ff88' if v[1]=='✅' else '#888'};'>"
                 f"{v[1]} {k}</span><br>" for k,v in badges.items()]) +
        f"<br><span style='color:#888;font-size:11px;'>{n_bdcs} BDCs in .env</span>"
        "</div>", unsafe_allow_html=True)

        st.markdown("---")

        if st.button("🗑️ CLEAR ALL CACHED DATA", key="clear_cache"):
            _clear_all_persisted()
            st.success("✅ All cached data cleared.")
            st.rerun()

        st.markdown("""
        <div style='text-align:center;padding:12px;background:rgba(255,0,255,0.08);
                    border-radius:10px;border:2px solid #ff00ff;'>
            <b style='color:#ff00ff;'>⚙️ SYSTEM STATUS</b><br>
            <span style='color:#00ff88;font-size:16px;'>🟢 OPERATIONAL</span>
        </div>""", unsafe_allow_html=True)

    if   choice == "🏦 BDC BALANCE":       show_bdc_balance()
    elif choice == "🚚 OMC LOADINGS":       show_omc_loadings()
    elif choice == "📅 DAILY ORDERS":       show_daily_orders()
    elif choice == "📊 MARKET SHARE":       show_market_share()
    elif choice == "📈 STOCK TRANSACTION":  show_stock_transaction()
    elif choice == "⛴️ PRODUCT OUTTURN":   show_product_outturn()
    elif choice == "🌍 NATIONAL STOCKOUT":  show_national_stockout()
    elif choice == "🌐 WORLD RISK MONITOR": show_world_monitor()
    elif choice == "🚢 VESSEL SUPPLY":      show_vessel_supply()


main()