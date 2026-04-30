"""
NPA ENERGY ANALYTICS — SLIM DASHBOARD
=======================================
Restricted to:
  - BDC Balance page
  - Daily Orders (Loadings) page

Configured BDCs:
  1. Maranatha
  2. Reston
  3. Veritas
  4. Matrix
  5. Nenser
  6. International Petroleum
  7. Cirrus
  8. Chase
  9. Everstone

INSTALLATION:
    pip install streamlit pandas pdfplumber PyPDF2 openpyxl python-dotenv plotly requests psutil

USAGE:
    streamlit run npa_dashboard_slim.py
"""

import streamlit as st
import os, re, io, time, threading, unicodedata, pickle
from datetime import datetime, timedelta
import pandas as pd
import pdfplumber
import PyPDF2
from dotenv import load_dotenv
import requests as _requests
import psutil
import queue
import concurrent.futures as _cf

load_dotenv()

# ─────────────────────────────────────────────────────────────
# MEMORY BADGE
# ─────────────────────────────────────────────────────────────
_proc = psutil.Process(os.getpid())

# ══════════════════════════════════════════════════════════════
# TARGETED BDC LIST — only these 9 BDCs are active
# ══════════════════════════════════════════════════════════════
ACTIVE_BDC_KEYWORDS = [
    "maranatha",
    "reston",
    "veritas",
    "matrix",
    "nenser",
    "international petroleum",
    "cirrus",
    "chase",
    "everstone",
]

def _bdc_is_active(name: str) -> bool:
    n = name.lower()
    return any(kw in n for kw in ACTIVE_BDC_KEYWORDS)


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
        if not _bdc_is_active(display):
            continue
        try:
            mapping[display] = int(value)
        except ValueError:
            pass
    return mapping


def load_product_mappings() -> dict:
    return {
        "PMS":    int(os.getenv("PRODUCT_PREMIUM_ID", "12")),
        "Gasoil": int(os.getenv("PRODUCT_GASOIL_ID",  "14")),
        "LPG":    int(os.getenv("PRODUCT_LPG_ID",     "28")),
    }


# ── Load all mappings once at startup ───────────────────────
BDC_USER_MAP      = load_bdc_user_map()
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
    "DAILY_ORDERS_URL":      os.getenv("NPA_DAILY_ORDERS_URL",
                                 "https://iml.npa-enterprise.com/NewNPA/home/CreateDailyOrderReport"),
}


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
           f"📋 {len(BDC_USER_MAP)} BDCs active")

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
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# PERSISTENT STATE
# ══════════════════════════════════════════════════════════════
PERSIST_DIR = os.path.join(os.getcwd(), ".persist_state")
os.makedirs(PERSIST_DIR, exist_ok=True)

_PERSIST_KEYS = {
    "bdc_records": [],
    "daily_df":    pd.DataFrame(),
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

    _today = datetime.now().date()
    defaults = {
        "bal_bdc_select":   [],
        "bal_fetch_all":    True,
        "bal_merge_prev":   True,
        "bal_ftype":        "Product",
        "bal_fval":         "ALL",
        "daily_start":      _today - timedelta(days=1),
        "daily_end":        _today,
        "daily_bdc_select": [],
        "daily_fetch_all":  True,
        "daily_merge_prev": True,
        "daily_ftype":      "Product",
        "daily_fval":       "ALL",
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


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


def _fetch_pdf(url: str, params: dict, timeout: int = _HTTP_TIMEOUT) -> bytes:
    try:
        r = _requests.get(url, params=params, headers=_HTTP_HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.content if r.content[:4] == b"%PDF" else None
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════
# ROBUST BATCH FETCHER
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
    total   = len(bdc_list)
    results = {}
    lock    = threading.Lock()
    done_n  = [0]

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

    def _update(bdc_name, result, attempts, err):
        with lock:
            done_n[0] += 1
            pct = done_n[0] / total

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
        progress_bar.progress(pct, text=f"Pass 1 — {done_n[0]} / {total} BDCs fetched…")
        status_text.markdown(
            "<div class='fetch-log'>" + "<br>".join(log_lines[-12:]) + "</div>",
            unsafe_allow_html=True,
        )

    batches = [bdc_list[i: i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]
    per_future_timeout = max(BATCH_HARD_TIMEOUT // BATCH_SIZE, 30)

    for batch_idx, batch in enumerate(batches):
        with _cf.ThreadPoolExecutor(max_workers=BATCH_SIZE) as ex:
            futs = {ex.submit(_attempt, b): b for b in batch}
            done_futs, pending_futs = _cf.wait(futs, timeout=per_future_timeout * len(batch))

            for fut in done_futs:
                bdc_name = futs[fut]
                try:
                    _, result, attempts, err = fut.result(timeout=1)
                except Exception as exc:
                    result, attempts, err = None, MAX_RETRIES, str(exc)
                results[bdc_name] = result
                _update(bdc_name, result, attempts, err)

            for fut in pending_futs:
                bdc_name = futs[fut]
                results[bdc_name] = None
                log_lines.append(f"⏱️ {bdc_name}: timed out — will retry in pass 2")
                with lock:
                    done_n[0] += 1
                progress_bar.progress(done_n[0] / total, text=f"Pass 1 — {done_n[0]} / {total} BDCs fetched…")
                fut.cancel()

        if batch_idx < len(batches) - 1:
            time.sleep(0.3)

    if second_pass:
        retry_bdcs = [b for b, r in results.items() if r is None or _result_is_empty(r)]
        if retry_bdcs:
            log_lines.append(f"━━━ Pass 2: retrying {len(retry_bdcs)} BDC(s) sequentially ━━━")
            status_text.markdown(
                "<div class='fetch-log'>" + "<br>".join(log_lines[-12:]) + "</div>",
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
                    "<div class='fetch-log'>" + "<br>".join(log_lines[-12:]) + "</div>",
                    unsafe_allow_html=True,
                )
                progress_bar.progress(1.0, text=f"Pass 2 — {idx+1}/{len(retry_bdcs)} retried")

    return results


def _result_is_empty(result) -> bool:
    if result is None:
        return True
    if isinstance(result, list):
        return len(result) == 0
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
        return best_key if best_key else clean

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


# ── Daily Orders ─────────────────────────────────────────────
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


def _get_product_category(text):
    t = text.upper()
    if "AVIATION" in t or "TURBINE" in t:        return "ATK"
    if "RFO" in t:                               return "RFO"
    if "PREMIX" in t:                            return "PREMIX"
    if "LPG" in t:                               return "LPG"
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


def _merge_dataframes(existing: pd.DataFrame, new_df: pd.DataFrame, dedup_cols: list) -> pd.DataFrame:
    if existing is None or existing.empty:
        return new_df
    if new_df is None or new_df.empty:
        return existing
    combined = pd.concat([new_df, existing], ignore_index=True)
    valid_dedup = [c for c in dedup_cols if c in combined.columns]
    if valid_dedup:
        combined = combined.drop_duplicates(subset=valid_dedup, keep="first")
    return combined.reset_index(drop=True)


def _combine_balance_results(results: dict) -> tuple:
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


def _combine_df_results(results: dict, dedup_cols: list) -> tuple:
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
        with st.expander(f"❌ {n_fail} BDC(s) failed"):
            for b in summary["failed"]:
                st.markdown(f"- `{b}`")
    if n_none:
        with st.expander(f"⚠️ {n_none} BDC(s) returned no data"):
            for b in summary["no_data"]:
                st.markdown(f"- `{b}`")


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
# FETCH WRAPPERS
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
# PAGE: BDC BALANCE
# ══════════════════════════════════════════════════════════════
def show_bdc_balance():
    st.markdown("<h2>🏦 BDC STOCK BALANCE ANALYZER</h2>", unsafe_allow_html=True)

    st.markdown("""
    <div style='background:rgba(0,255,255,0.05);border:1px solid #00ffff33;
                border-radius:10px;padding:14px;margin-bottom:16px;'>
    <b style='color:#00ffff;'>What this page does</b><br>
    Shows the current live stock balance for each configured BDC — broken down by depot
    and product (PREMIUM / GASOIL / LPG).
    </div>
    """, unsafe_allow_html=True)

    all_bdc_names = sorted(BDC_USER_MAP.keys())
    n_configured  = len(all_bdc_names)

    if n_configured == 0:
        st.error(
            "No matching BDCs found in your .env file. "
            "Please add BDC_USER_ entries for: Maranatha, Reston, Veritas, Matrix, "
            "Nenser, International Petroleum, Cirrus, Chase, Everstone."
        )
        return

    st.info(
        f"📋 **{n_configured} active BDC(s)** configured: "
        + ", ".join(all_bdc_names)
    )

    col1, col2 = st.columns([3, 1])
    with col1:
        selected = st.multiselect(
            f"Select specific BDCs (leave blank to fetch all {n_configured})",
            all_bdc_names, key="bal_bdc_select",
        )
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        fetch_all_flag = st.checkbox(
            "Fetch ALL BDCs",
            value=st.session_state.get("bal_fetch_all", True),
            key="bal_fetch_all",
        )

    bdcs_to_fetch = all_bdc_names if (fetch_all_flag or not selected) else selected

    has_previous = bool(st.session_state.get("bdc_records"))
    merge_prev   = False
    if has_previous:
        merge_prev = st.checkbox(
            "🔀 Merge this fetch with previous results",
            value=st.session_state.get("bal_merge_prev", True),
            key="bal_merge_prev",
        )

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
                st.session_state.get("bdc_fetched_count", n_configured),
                len(records), "Balance Records",
            )

    df      = pd.DataFrame(records)
    col_bal = "ACTUAL BALANCE (LT\\KG)"
    summary_prod = df.groupby("Product")[col_bal].sum()

    st.markdown("---")
    st.markdown("### 🛢️ STOCK TOTALS")
    cols = st.columns(3)
    for idx, prod in enumerate(["PREMIUM", "GASOIL", "LPG"]):
        with cols[idx]:
            val = summary_prod.get(prod, 0)
            st.markdown(
                f"<div class='metric-card'><h2>{prod}</h2><h1>{val:,.0f}</h1>"
                f"<p style='color:#888;font-size:13px;margin:0;'>Litres / KG</p></div>",
                unsafe_allow_html=True,
            )

    grand_total = float(df[col_bal].sum())
    st.metric("🏭 Grand Total", f"{grand_total:,.0f} LT/KG")

    st.markdown("---")
    st.markdown("### 🏢 BDC BREAKDOWN")
    bdc_sum = (
        df.groupby("BDC")
        .agg({col_bal: "sum", "DEPOT": "nunique", "Product": "nunique"})
        .reset_index()
        .rename(columns={col_bal: "Total Balance (LT/KG)", "DEPOT": "Depots", "Product": "Products"})
    )
    bdc_sum = bdc_sum.sort_values("Total Balance (LT/KG)", ascending=False)
    bdc_sum["Market Share %"] = (bdc_sum["Total Balance (LT/KG)"] / grand_total * 100).round(2)
    st.dataframe(bdc_sum, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("### 📊 PRODUCT × BDC PIVOT")
    pivot = (
        df.pivot_table(index="BDC", columns="Product", values=col_bal,
                       aggfunc="sum", fill_value=0).reset_index()
    )
    for p in ["GASOIL", "LPG", "PREMIUM"]:
        if p not in pivot.columns:
            pivot[p] = 0
    pivot["TOTAL"] = pivot[["GASOIL", "LPG", "PREMIUM"]].sum(axis=1)
    pivot = pivot.sort_values("TOTAL", ascending=False)
    st.dataframe(pivot[["BDC", "GASOIL", "LPG", "PREMIUM", "TOTAL"]], use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("### 🔍 FILTER & EXPLORE")
    ft    = st.selectbox("Filter by", ["Product", "BDC", "Depot"], key="bal_ftype")
    _cmap = {"Product": "Product", "BDC": "BDC", "Depot": "DEPOT"}
    opts  = ["ALL"] + sorted(df[_cmap[ft]].unique().tolist())
    fval  = st.selectbox("Value", opts, key="bal_fval")
    filt  = df if fval == "ALL" else df[df[_cmap[ft]] == fval]

    st.caption(
        f"Showing **{len(filt):,}** records  |  "
        f"**{filt['BDC'].nunique()}** BDCs  |  "
        f"**{filt['DEPOT'].nunique()}** depots  |  "
        f"Total: **{filt[col_bal].sum():,.0f} LT/KG**"
    )
    st.dataframe(
        filt[["Product", "BDC", "DEPOT", "AVAILABLE BALANCE (LT\\KG)", col_bal, "Date"]]
        .sort_values(["Product", "BDC"]),
        use_container_width=True, height=400, hide_index=True,
    )

    st.markdown("---")
    _ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_bytes = _to_excel_bytes({
        "Stock Balance": df,
        "LPG":           df[df["Product"] == "LPG"],
        "PREMIUM":       df[df["Product"] == "PREMIUM"],
        "GASOIL":        df[df["Product"] == "GASOIL"],
    })
    st.download_button(
        "⬇️ DOWNLOAD EXCEL", excel_bytes,
        f"bdc_balance_{_ts}.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


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
    granularity of physical fuel movements out of each storage facility.
    </div>
    """, unsafe_allow_html=True)

    all_bdc_names = sorted(BDC_USER_MAP.keys())
    n_configured  = len(all_bdc_names)

    if n_configured == 0:
        st.error(
            "No matching BDCs found in your .env file. "
            "Please add BDC_USER_ entries for: Maranatha, Reston, Veritas, Matrix, "
            "Nenser, International Petroleum, Cirrus, Chase, Everstone."
        )
        return

    st.info(
        f"📋 **{n_configured} active BDC(s)** configured: "
        + ", ".join(all_bdc_names)
    )

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Start Date",
            value=st.session_state.get("daily_start", datetime.now() - timedelta(days=1)),
            key="daily_start",
        )
    with col2:
        end_date = st.date_input(
            "End Date",
            value=st.session_state.get("daily_end", datetime.now()),
            key="daily_end",
        )

    col3, col4 = st.columns([3, 1])
    with col3:
        selected = st.multiselect(
            f"Select BDCs (blank = all {n_configured})",
            all_bdc_names, key="daily_bdc_select",
        )
    with col4:
        st.markdown("<br>", unsafe_allow_html=True)
        fetch_all_flag = st.checkbox(
            "Fetch ALL",
            value=st.session_state.get("daily_fetch_all", True),
            key="daily_fetch_all",
        )

    bdcs_to_fetch = all_bdc_names if (fetch_all_flag or not selected) else selected
    period_days   = max((end_date - start_date).days, 1)

    has_previous = not st.session_state.get("daily_df", pd.DataFrame()).empty
    merge_prev   = False
    if has_previous:
        merge_prev = st.checkbox(
            "🔀 Merge this fetch with previous results",
            value=st.session_state.get("daily_merge_prev", True),
            key="daily_merge_prev",
        )

    st.info(
        f"📋 **{len(bdcs_to_fetch)} BDC(s)** · "
        f"Period: **{start_date.strftime('%d %b %Y')} → {end_date.strftime('%d %b %Y')}** "
        f"({period_days} days)"
    )

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
            prev     = st.session_state.daily_df
            combined = _merge_dataframes(prev, combined,
                                         ["Date", "Truck", "Order Number", "Product"])
            st.info(f"🔀 Merged — {len(combined)} total records after union.")

        st.session_state.daily_df            = combined
        st.session_state.daily_fetch_summary = summary
        st.session_state.daily_fetched_count = len(bdcs_to_fetch)
        _save_state("daily_df", combined)

        st.markdown("---")
        _render_fetch_summary(
            summary, len(bdcs_to_fetch),
            len(combined) if not combined.empty else 0,
            "Order Records",
        )

    df = st.session_state.get("daily_df", pd.DataFrame())
    if df.empty:
        st.info("👆 Select a date range and click **FETCH DAILY ORDERS**.")
        return

    if st.session_state.get("daily_fetch_summary"):
        with st.expander("📊 Last fetch outcome", expanded=False):
            _render_fetch_summary(
                st.session_state.daily_fetch_summary,
                st.session_state.get("daily_fetched_count", n_configured),
                len(df), "Order Records",
            )

    st.markdown("---")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Orders",        f"{len(df):,}")
    c2.metric("Volume (LT)",   f"{df['Quantity'].sum():,.0f}")
    c3.metric("BDCs",          f"{df['BDC'].nunique()}")
    c4.metric("Depots",        f"{df['Depot'].nunique()}")
    c5.metric("Value (₵)",     f"{(df['Quantity'] * df['Price']).sum():,.0f}")

    st.markdown("### 📦 PRODUCT SUMMARY")
    prod_sum = (
        df.groupby("Product")
        .agg({"Quantity": "sum", "Order Number": "count", "BDC": "nunique"})
        .reset_index()
        .rename(columns={"Quantity": "Total Volume (LT/KG)", "Order Number": "Orders", "BDC": "BDCs"})
        .sort_values("Total Volume (LT/KG)", ascending=False)
    )
    st.dataframe(prod_sum, use_container_width=True, hide_index=True)

    st.markdown("### 🏦 BDC SUMMARY")
    bdc_sum = (
        df.groupby("BDC")
        .agg({"Quantity": "sum", "Order Number": "count"})
        .reset_index()
        .sort_values("Quantity", ascending=False)
        .rename(columns={"Quantity": "Total Volume (LT/KG)", "Order Number": "Orders"})
    )
    st.caption(f"**{len(bdc_sum)} BDCs** with daily order data")
    st.dataframe(bdc_sum, use_container_width=True, hide_index=True)

    st.markdown("### 📊 BDC × PRODUCT PIVOT")
    pivot = (
        df.pivot_table(index="BDC", columns="Product", values="Quantity",
                       aggfunc="sum", fill_value=0).reset_index()
    )
    pcols = [c for c in pivot.columns if c != "BDC"]
    pivot["TOTAL"] = pivot[pcols].sum(axis=1)
    st.dataframe(pivot.sort_values("TOTAL", ascending=False), use_container_width=True, hide_index=True)

    st.markdown("---")
    ft    = st.selectbox("Filter by", ["Product", "BDC", "Depot", "Status"], key="daily_ftype")
    _cmap = {"Product": "Product", "BDC": "BDC", "Depot": "Depot", "Status": "Status"}
    col_for_filter = _cmap[ft]
    if col_for_filter not in df.columns:
        df[col_for_filter] = ""
    opts  = ["ALL"] + sorted(df[col_for_filter].dropna().unique().tolist())
    fval  = st.selectbox("Value", opts, key="daily_fval")
    filt  = df if fval == "ALL" else df[df[col_for_filter] == fval]
    st.caption(f"Showing **{len(filt):,}** records | Volume: **{filt['Quantity'].sum():,.0f} LT**")

    detail_cols = ["Date", "Truck", "Quantity", "Order Number", "BDC", "Depot", "Price", "Product", "Status"]
    detail_cols = [c for c in detail_cols if c in filt.columns]
    st.dataframe(
        filt[detail_cols].sort_values(["Product", "Date"]),
        use_container_width=True, height=400, hide_index=True,
    )

    st.markdown("---")
    _ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_bytes = _to_excel_bytes({"All Orders": df, "BDC Pivot": pivot})
    st.download_button(
        "⬇️ DOWNLOAD EXCEL", excel_bytes, f"daily_orders_{_ts}.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


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
            "📅 DAILY ORDERS",
        ], index=0, label_visibility="collapsed")

        st.markdown("---")

        # Active BDC list display
        st.markdown("""
        <div style='background:rgba(0,255,255,0.05);padding:14px;border-radius:10px;
                    border:1px solid #00ffff44;font-size:12px;'>
        <b style='color:#00ffff;'>🏦 ACTIVE BDCs</b><br>""" +
        "".join([f"<span style='color:#e0e0e0;'>• {b}</span><br>" for b in sorted(BDC_USER_MAP.keys())]) +
        "</div>", unsafe_allow_html=True)

        st.markdown("---")

        # Data status
        has_bal = bool(st.session_state.get("bdc_records"))
        has_dly = not st.session_state.get("daily_df", pd.DataFrame()).empty
        st.markdown(
            "<div style='background:rgba(0,255,255,0.05);padding:14px;border-radius:10px;"
            "border:1px solid #00ffff44;font-size:13px;'>"
            "<b style='color:#00ffff;'>📊 DATA STATUS</b><br>"
            f"<span style='color:{'#00ff88' if has_bal else '#888'};'>"
            f"{'✅' if has_bal else '○'} Balance</span><br>"
            f"<span style='color:{'#00ff88' if has_dly else '#888'};'>"
            f"{'✅' if has_dly else '○'} Daily Orders</span>"
            "</div>",
            unsafe_allow_html=True,
        )

        st.markdown("---")

        # Safe clear cache
        st.markdown(
            "<div style='background:rgba(255,0,0,0.08);padding:10px;border-radius:8px;"
            "border:1px solid #ff000044;'>"
            "<b style='color:#ff4444;font-size:12px;'>⚠️ DANGER ZONE</b>"
            "</div>",
            unsafe_allow_html=True,
        )
        confirm_clear = st.checkbox(
            "✔ Confirm: clear all cached data",
            value=False,
            key="confirm_clear_cache",
        )
        if st.button("🗑️ CLEAR CACHE", key="clear_cache_btn", disabled=not confirm_clear):
            _clear_all_persisted()
            st.session_state["confirm_clear_cache"] = False
            st.success("✅ All cached data cleared.")
            st.rerun()

        st.markdown("""
        <div style='text-align:center;padding:12px;background:rgba(255,0,255,0.08);
                    border-radius:10px;border:2px solid #ff00ff;'>
            <b style='color:#ff00ff;'>⚙️ SYSTEM STATUS</b><br>
            <span style='color:#00ff88;font-size:16px;'>🟢 OPERATIONAL</span>
        </div>""", unsafe_allow_html=True)

    if choice == "🏦 BDC BALANCE":
        show_bdc_balance()
    elif choice == "📅 DAILY ORDERS":
        show_daily_orders()


main()