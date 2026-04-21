"""
NPA ENERGY ANALYTICS — STREAMLIT DASHBOARD
===========================================
Fixed version:
  - Robust BDC name normalisation so PDF-parsed names reliably match .env keys
  - Cross-BDC deduplication no longer silently drops valid distinct BDC records
  - All BDCs that return data appear in Excel exports
  - Per-BDC retry logic unchanged; fetch log still shows every outcome

INSTALLATION:
    pip install streamlit pandas pdfplumber PyPDF2 openpyxl python-dotenv plotly requests psutil

USAGE:
    streamlit run npa_dashboard.py
"""

import streamlit as st
import os, re, io, json, time, threading, unicodedata
from datetime import datetime, timedelta
import pandas as pd
import pdfplumber
import PyPDF2
from dotenv import load_dotenv
import plotly.graph_objects as go
import requests as _requests
import psutil

load_dotenv()

# ─────────────────────────────────────────────────────────────
# MEMORY BADGE
# ─────────────────────────────────────────────────────────────
_proc = psutil.Process(os.getpid())


# ══════════════════════════════════════════════════════════════
# NAME NORMALISATION UTILITIES
# ══════════════════════════════════════════════════════════════

def _normalise_name(name: str) -> str:
    """
    Canonical key for fuzzy BDC name matching.
    Strips accents, lowercases, collapses whitespace, removes all punctuation
    and common legal suffixes so that small variations in spacing / punctuation
    between the .env key and the PDF text do not cause mismatches.
    """
    if not name:
        return ""
    # Unicode normalise + strip accents
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    # Replace punctuation / separators with spaces
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    # Remove very common noise suffixes
    for suffix in (
        "limited", "ltd", "company", "co", "ghana", "plc",
        "llc", "lp", "inc", "corp", "enterprise", "enterprises",
    ):
        s = re.sub(rf"\b{suffix}\b", " ", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _build_lookup(mapping: dict) -> dict:
    """Return {normalised_key: original_key} for fuzzy lookup."""
    return {_normalise_name(k): k for k in mapping}


# ══════════════════════════════════════════════════════════════
# ENVIRONMENT LOADERS
# ══════════════════════════════════════════════════════════════

def load_bdc_user_map() -> dict:
    """
    Build {display_name -> user_id} from BDC_USER_* env vars.
    The display name is derived directly from the env key (underscores → spaces,
    title-cased) with a small set of hand-coded fixes for known edge cases.
    """
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

# Pre-build normalised lookup tables for fuzzy matching
_BDC_USER_LOOKUP  = _build_lookup(BDC_USER_MAP)   # normalised → display name

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
</style>
""", unsafe_allow_html=True)


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


def _fetch_pdf(url: str, params: dict, timeout: int = 60) -> bytes | None:
    """Single HTTP GET that returns raw bytes only if a valid PDF is returned."""
    try:
        r = _requests.get(url, params=params, headers=_HTTP_HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.content if r.content[:4] == b"%PDF" else None
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════
# ROBUST BATCH FETCHER
# ══════════════════════════════════════════════════════════════
BATCH_SIZE  = 5
MAX_RETRIES = 3
RETRY_DELAY = 2


def _sequential_batch_fetch(
    bdc_list:   list,
    fetch_fn,
    progress_bar,
    status_text,
    log_lines: list,
) -> dict:
    import concurrent.futures as _cf

    total   = len(bdc_list)
    results = {}
    lock    = threading.Lock()
    done_n  = [0]

    def _attempt(bdc_name: str):
        last_err = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = fetch_fn(bdc_name)
                return bdc_name, result, attempt, None
            except Exception as exc:
                last_err = exc
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY * attempt)
        return bdc_name, None, MAX_RETRIES, str(last_err)

    batches = [bdc_list[i: i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]

    for batch_idx, batch in enumerate(batches):
        with _cf.ThreadPoolExecutor(max_workers=BATCH_SIZE) as ex:
            futs = {ex.submit(_attempt, b): b for b in batch}
            for fut in _cf.as_completed(futs):
                bdc_name, result, attempts, err = fut.result()
                results[bdc_name] = result

                with lock:
                    done_n[0] += 1
                    progress_bar.progress(
                        done_n[0] / total,
                        text=f"Fetched {done_n[0]} / {total} BDCs…",
                    )

                if err:
                    icon = "❌"
                    note = f"FAILED after {attempts} attempts — {err}"
                elif result is None:
                    icon = "⚠️"
                    note = "No data / empty PDF"
                elif attempts > 1:
                    icon = "🔄"
                    note = f"OK (needed {attempts} attempts)"
                else:
                    icon = "✅"
                    note = "OK"

                log_lines.append(f"{icon} {bdc_name}: {note}")
                status_text.markdown(
                    f"<div class='fetch-log'>{'<br>'.join(log_lines[-12:])}</div>",
                    unsafe_allow_html=True,
                )

        if batch_idx < len(batches) - 1:
            time.sleep(0.5)

    return results


# ══════════════════════════════════════════════════════════════
# PDF PARSERS
# ══════════════════════════════════════════════════════════════

# ── BDC Balance ──────────────────────────────────────────────
class StockBalanceScraper:
    """
    Parses the NPA BDC stock-balance PDF.

    KEY FIX: The BDC name stored against each record is now the *canonical
    display name from BDC_USER_MAP* (looked up via fuzzy normalisation) rather
    than the raw text scraped from the PDF.  This guarantees that every record
    that arrives from a BDC's own PDF is attributed to exactly the same name
    string used in the rest of the dashboard, so nothing disappears in joins or
    group-bys.

    Cross-BDC deduplication has been made much more conservative: we only drop
    a record if the SAME BDC name appears TWICE for the same depot+product+date
    (i.e. within a single BDC's own PDF pages).  We no longer drop records just
    because two different BDCs report stock at the same depot.
    """

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
        """
        Map the raw BDC name from the PDF to the canonical display name used in
        BDC_USER_MAP.  Falls back to the cleaned raw name if no match is found.
        """
        clean = self._ns(raw_bdc)
        norm  = _normalise_name(clean)
        # Exact normalised match
        if norm in _BDC_USER_LOOKUP:
            return _BDC_USER_LOOKUP[norm]
        # Partial / substring match — pick the longest key that is a substring
        best_key = None
        best_len = 0
        for nk, display in _BDC_USER_LOOKUP.items():
            if nk and (nk in norm or norm in nk) and len(nk) > best_len:
                best_key = display
                best_len = len(nk)
        if best_key:
            return best_key
        # No match — return the cleaned raw name so the record is still kept
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
        """
        Parse the PDF.  owning_bdc_name is the display name from BDC_USER_MAP
        that was used to request this PDF — we use it as the fallback / override
        when the PDF's own BDC label can't be resolved.
        """
        records = []
        # Per-BDC dedup only: same BDC+depot+product+date within this PDF
        seen    = set()
        try:
            reader   = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
            cur_bdc  = owning_bdc_name   # seed with requesting BDC name
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
                        # If we can't resolve, keep the owning BDC name so the
                        # record is still attributed to the correct entity
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
                            # Dedup KEY: within this BDC's own PDF only
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
                            # Resolve to canonical name; fall back to owning BDC name
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
    # Deduplicate within a single BDC's PDF on order + truck
    df = df.drop_duplicates(subset=["Order Number", "Truck", "Date", "Product"])
    try:
        ds = pd.to_datetime(df["Date"], format="%Y/%m/%d", errors="coerce")
        df = df.assign(_ds=ds).sort_values("_ds").drop(columns=["_ds"]).reset_index(drop=True)
    except Exception:
        pass
    return df


def _resolve_pdf_bdc(raw: str, fallback: str) -> str:
    """Resolve a raw BDC name from a PDF field to the canonical BDC_USER_MAP key."""
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
    # Deduplicate within a single BDC's daily PDF
    df = df.drop_duplicates(subset=["Date", "Truck", "Order Number", "Product"])
    return df


# ── Stock Transaction ────────────────────────────────────────
def _parse_stock_transaction_pdf(pdf_bytes: bytes) -> list:
    DESCRIPTIONS = sorted([
        "Balance b/fwd","Stock Take","Sale",
        "Custody Transfer In","Custody Transfer Out","Product Outturn",
    ], key=len, reverse=True)
    SKIP_PFX = (
        "national petroleum authority","stock transaction report",
        "bdc :","depot :","product :","printed by","printed on",
        "date trans #","actual stock balance","stock commitments",
        "available stock balance","last stock update","i.t.s from",
    )

    def _skip(line):
        lo = line.strip().lower()
        return lo.startswith(SKIP_PFX) or bool(re.match(r"^\d{1,2}\s+\w+,\s+\d{4}", line.strip()))

    def _pnum(s):
        s = s.strip()
        neg = s.startswith("(") and s.endswith(")")
        try:
            v = int(s.strip("()").replace(",",""))
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
        return {"Date": date, "Trans #": trans, "Description": desc,
                "Account": acct, "Volume": vol or 0, "Balance": bal or 0}

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
                            records.append(row)
    except Exception:
        pass
    return records


# ══════════════════════════════════════════════════════════════
# ROBUST PER-BDC FETCH WRAPPERS
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
        # Pass the canonical BDC name so the parser uses it as the attribution fallback
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


# ── Aggregate helpers ─────────────────────────────────────────

def _combine_balance_results(results: dict) -> tuple[list, dict]:
    """
    Combine per-BDC balance record lists.

    Deduplication policy (FIXED):
    ─────────────────────────────
    Each BDC fetches its OWN PDF, so records in different BDCs' PDFs are
    legitimately distinct even if they share a depot name (e.g. BOST Global
    appears as a depot for many BDCs).

    We ONLY deduplicate when the EXACT SAME (BDC, DEPOT, PRODUCT, DATE) tuple
    appears MORE THAN ONCE across all collected records — which would indicate
    that the same PDF was somehow fetched twice under different names.  In that
    case we keep the record with the higher actual balance.

    We do NOT drop a record just because another BDC also has stock at the same
    depot; that is correct and expected behaviour.
    """
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
        # Keep highest balance when the exact same (BDC, depot, product, date) appears twice
        df_tmp = (
            df_tmp
            .sort_values(col, ascending=False)
            .drop_duplicates(subset=["BDC", "DEPOT", "Product", "Date"], keep="first")
            .reset_index(drop=True)
        )
        all_records = df_tmp.to_dict("records")

    return all_records, summary


def _combine_df_results(results: dict, dedup_cols: list) -> tuple[pd.DataFrame, dict]:
    """
    Combine per-BDC DataFrames.

    Deduplication policy (FIXED):
    ─────────────────────────────
    Cross-BDC dedup is applied ONLY on the natural business key that uniquely
    identifies an order (Order Number + Truck + Date + Product).  We never drop
    rows just because the BDC column differs — an order should appear at most once
    in the combined dataset, but attributed to the BDC that was the source of the
    matching PDF.  When duplicates exist we keep the first occurrence (which will
    be from whichever BDC fetched it; the order content is identical either way).
    """
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

    # Cross-BDC dedup on the tightest natural key (exclude BDC from key)
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
# EXCEL EXPORT HELPER  (FIXED: preserves all BDCs)
# ══════════════════════════════════════════════════════════════
def _to_excel_bytes(sheets: dict) -> bytes:
    """
    Write multiple sheets to an Excel file.
    Each DataFrame is written as-is — no additional filtering or dedup is
    applied here so that every BDC that returned data appears in the output.
    """
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for name, df in sheets.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                df.to_excel(writer, sheet_name=name[:31], index=False)
            elif isinstance(df, pd.DataFrame):
                # Write empty placeholder so the sheet still exists
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
    st.info(f"📋 **{len(bdcs_to_fetch)} BDC(s)** will be queried  "
            f"({'all configured' if len(bdcs_to_fetch)==n_configured else 'custom selection'})")

    if st.button("🔄 FETCH BDC BALANCE DATA", key="bal_fetch"):
        prog      = st.progress(0, text="Initialising…")
        log_box   = st.empty()
        log_lines = []

        results  = _sequential_batch_fetch(
            bdcs_to_fetch,
            _make_balance_fetcher(),
            prog, log_box, log_lines,
        )
        prog.progress(1.0, text="✅ Fetch complete")

        all_records, summary = _combine_balance_results(results)
        st.session_state.bdc_records          = all_records
        st.session_state.bdc_fetch_summary    = summary
        st.session_state.bdc_fetched_count    = len(bdcs_to_fetch)

        st.markdown("---")
        _render_fetch_summary(summary, len(bdcs_to_fetch), len(all_records), "Balance Records")

    # ── Display ──────────────────────────────────────────────
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
    excel_bytes = _to_excel_bytes({
        "All Records":  df,
        "LPG":          df[df["Product"]=="LPG"],
        "PREMIUM":      df[df["Product"]=="PREMIUM"],
        "GASOIL":       df[df["Product"]=="GASOIL"],
        "BDC Summary":  bdc_sum,
        "BDC Pivot":    pivot,
    })
    st.download_button("⬇️ DOWNLOAD EXCEL", excel_bytes, "bdc_balance.xlsx",
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
        )
        prog.progress(1.0, text="✅ Fetch complete")

        combined, summary = _combine_df_results(
            results, ["Order Number", "Truck", "Date", "Product"]
        )
        st.session_state.omc_df            = combined
        st.session_state.omc_fetch_summary = summary
        st.session_state.omc_fetched_count = len(bdcs_to_fetch)
        st.session_state.omc_start_date    = start_date
        st.session_state.omc_end_date      = end_date

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
    pivot = (df.pivot_table(index="BDC", columns="Product", values="Quantity",
                            aggfunc="sum", fill_value=0).reset_index())
    for p in ["GASOIL","LPG","PREMIUM"]:
        if p not in pivot.columns: pivot[p] = 0
    pivot["TOTAL"] = pivot[["GASOIL","LPG","PREMIUM"]].sum(axis=1)

    excel_bytes = _to_excel_bytes({
        "All Orders":   df,
        "BDC Summary":  bdc_sum,
        "BDC Pivot":    pivot,
        **{p: df[df["Product"]==p] for p in ["PREMIUM","GASOIL","LPG"]},
    })
    st.download_button("⬇️ DOWNLOAD EXCEL", excel_bytes, "omc_loadings.xlsx",
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
    granularity of physical fuel movements out of each storage facility.
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
        )
        prog.progress(1.0, text="✅ Fetch complete")

        combined, summary = _combine_df_results(
            results, ["Date", "Truck", "Order Number", "Product"]
        )
        st.session_state.daily_df            = combined
        st.session_state.daily_fetch_summary = summary
        st.session_state.daily_fetched_count = len(bdcs_to_fetch)
        st.session_state.daily_start_date    = start_date
        st.session_state.daily_end_date      = end_date

        st.markdown("---")
        _render_fetch_summary(summary, len(bdcs_to_fetch),
                              len(combined) if not combined.empty else 0,
                              "Order Records")

    df = st.session_state.get("daily_df", pd.DataFrame())
    if df.empty:
        st.info("👆 Select a date range and click **FETCH DAILY ORDERS**.")
        return

    if st.session_state.get("daily_fetch_summary"):
        with st.expander("📊 Last fetch outcome", expanded=False):
            _render_fetch_summary(
                st.session_state.daily_fetch_summary,
                st.session_state.get("daily_fetched_count", len(BDC_USER_MAP)),
                len(df), "Order Records",
            )

    st.markdown("---")
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Orders",       f"{len(df):,}")
    c2.metric("Volume (LT)",  f"{df['Quantity'].sum():,.0f}")
    c3.metric("BDCs",         f"{df['BDC'].nunique()}")
    c4.metric("Depots",       f"{df['Depot'].nunique()}")
    c5.metric("Value (₵)",    f"{(df['Quantity']*df['Price']).sum():,.0f}")

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

    st.markdown("### 📊 BDC × PRODUCT PIVOT")
    pivot = (df.pivot_table(index="BDC", columns="Product", values="Quantity",
                            aggfunc="sum", fill_value=0).reset_index())
    pcols = [c for c in pivot.columns if c != "BDC"]
    pivot["TOTAL"] = pivot[pcols].sum(axis=1)
    st.dataframe(pivot.sort_values("TOTAL", ascending=False), use_container_width=True, hide_index=True)

    st.markdown("---")
    ft    = st.selectbox("Filter by", ["Product","BDC","Depot","Status"], key="daily_ftype")
    _cmap = {"Product":"Product","BDC":"BDC","Depot":"Depot","Status":"Status"}
    opts  = ["ALL"] + sorted(df[_cmap[ft]].dropna().unique().tolist())
    fval  = st.selectbox("Value", opts, key="daily_fval")
    filt  = df if fval=="ALL" else df[df[_cmap[ft]]==fval]
    st.caption(f"Showing **{len(filt):,}** records | "
               f"Volume: **{filt['Quantity'].sum():,.0f} LT**")
    st.dataframe(filt[["Date","Truck","Quantity","Order Number","BDC","Depot","Price","Product","Status"]]
                 .sort_values(["Product","Date"]),
                 use_container_width=True, height=400, hide_index=True)

    st.markdown("---")
    excel_bytes = _to_excel_bytes({"All Orders": df, "BDC Pivot": pivot})
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
                              "BDC Stock (LT)":     f"{bv:,.0f}",
                              "Market Total (LT)":  f"{mkt:,.0f}",
                              "Share (%)":          f"{bv/mkt*100:.2f}" if mkt else "0.00"})
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
        selected_bdc     = st.selectbox("BDC",     sorted(BDC_MAP.keys()),   key="txn_bdc")
        selected_product = st.selectbox("Product",  PRODUCT_OPTIONS,          key="txn_prod")
    with c2:
        selected_depot   = st.selectbox("Depot",    sorted(DEPOT_MAP.keys()), key="txn_depot")

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
        else:
            records = _parse_stock_transaction_pdf(pdf_bytes)
            if records:
                st.session_state.stock_txn_df = pd.DataFrame(records)
                st.session_state.txn_bdc      = selected_bdc
                st.session_state.txn_depot    = selected_depot
                st.session_state.txn_product  = selected_product
                st.success(f"✅ {len(records):,} transaction records extracted.")
            else:
                st.warning("No transactions found.  Try a different date range or BDC/depot combination.")
                st.session_state.stock_txn_df = pd.DataFrame()

    df = st.session_state.stock_txn_df
    if df.empty:
        st.info("👆 Configure the parameters above and click **FETCH TRANSACTION REPORT**.")
        return

    st.markdown(f"### {st.session_state.get('txn_bdc','')} — "
                f"{st.session_state.get('txn_product','')} @ "
                f"{st.session_state.get('txn_depot','')}")

    inflows  = float(df[df["Description"].isin(["Custody Transfer In","Product Outturn"])]["Volume"].sum())
    outflows = float(df[df["Description"].isin(["Sale","Custody Transfer Out"])]["Volume"].sum())
    sales    = float(df[df["Description"]=="Sale"]["Volume"].sum())
    bdc_xfer = float(df[df["Description"]=="Custody Transfer Out"]["Volume"].sum())
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
        help="Business Days gives a higher (more conservative) daily rate → fewer days of supply.",
    )
    use_biz = "Business" in day_type

    depl_mode = st.radio(
        "Depletion rate method",
        ["📊 Average Daily Loadings","🔥 Maximum Single-Day Loading (stress test)","📊 Median Daily Loadings"],
        index=0, key="ns_depl_mode",
        help="Average is the standard baseline.  Maximum is a worst-case stress test.",
    )
    use_max    = "Maximum" in depl_mode
    use_median = "Median"  in depl_mode

    exclude_tor = st.checkbox(
        "❌ Exclude TEMA OIL REFINERY (TOR) from LPG stock",
        value=False, key="ns_excl_tor",
        help="TOR LPG is often internal/strategic reserve and should not count toward "
             "commercial supply runway.",
    )

    _vessel_df     = st.session_state.get("vessel_data", pd.DataFrame())
    _vessel_loaded = isinstance(_vessel_df, pd.DataFrame) and not _vessel_df.empty
    _pending_n     = int((_vessel_df["Status"]=="PENDING").sum()) if _vessel_loaded else 0

    include_vessels = st.checkbox(
        "🚢 Add pending vessel cargo to stock totals",
        value=False, key="ns_vessels",
        help="Adds litres from every PENDING vessel in the Vessel Supply tracker to the "
             "BDC balance before computing days of supply.",
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

    st.info(f"📋 **{n_total} BDCs** will be queried for both Balance and OMC Loadings  |  "
            f"Loadings window: **{start_date.strftime('%d %b')} → {end_date.strftime('%d %b %Y')}** "
            f"({period_days} calendar days / {effective_days} {'business' if use_biz else 'calendar'} days)")
    st.markdown("---")

    if st.button("⚡ FETCH & ANALYSE NATIONAL FUEL SUPPLY", key="ns_go"):
        col_bal = "ACTUAL BALANCE (LT\\KG)"

        # ── Step 1: Balance ──────────────────────────────────
        with st.status("📡 Step 1 / 2 — Fetching BDC stock balances…", expanded=True):
            prog1      = st.progress(0, text="Starting…")
            log_box1   = st.empty()
            log_lines1 = []
            results1   = _sequential_batch_fetch(
                all_bdc_names, _make_balance_fetcher(),
                prog1, log_box1, log_lines1,
            )
            prog1.progress(1.0, text="✅ Balance fetch complete")
            all_records, bal_summary = _combine_balance_results(results1)
            bal_df = pd.DataFrame(all_records)

            n_bal_bdcs = bal_df["BDC"].nunique() if not bal_df.empty else 0
            st.write(f"✅ **{len(all_records):,} balance records** from **{n_bal_bdcs} BDCs**  |  "
                     f"✅ {len(bal_summary['success'])} succeeded  |  "
                     f"⚠️ {len(bal_summary['no_data'])} no data  |  "
                     f"❌ {len(bal_summary['failed'])} failed")

            if exclude_tor and not bal_df.empty:
                mask    = bal_df["BDC"].str.contains("TOR", case=False, na=False) & (bal_df["Product"]=="LPG")
                excl_v  = bal_df[mask][col_bal].sum()
                bal_df  = bal_df[~mask].copy()
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

        # ── Step 2: OMC Loadings ─────────────────────────────
        with st.status("🚚 Step 2 / 2 — Fetching national OMC loadings…", expanded=True):
            st.write(f"Querying {n_total} BDCs for loadings from "
                     f"{start_date.strftime('%d %b')} to {end_date.strftime('%d %b %Y')}…")
            prog2      = st.progress(0, text="Starting…")
            log_box2   = st.empty()
            log_lines2 = []
            results2   = _sequential_batch_fetch(
                all_bdc_names, _make_omc_fetcher(start_str, end_str),
                prog2, log_box2, log_lines2,
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

        # ── Build forecast ───────────────────────────────────
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
            "forecast_df":  forecast_df,
            "bal_df":       bal_df,
            "omc_df":       omc_df,
            "bdc_pivot":    bdc_pivot,
            "period_days":  period_days,
            "eff_days":     effective_days,
            "day_lbl":      day_lbl,
            "depl_lbl":     depl_lbl,
            "start_str":    start_str,
            "end_str":      end_str,
            "bal_summary":  bal_summary,
            "omc_summary":  omc_summary,
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
    Use for proactive upstream procurement decisions.
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
    Loads the national vessel discharge schedule from a Google Sheet — showing discharged
    cargo and pending vessels (at anchorage or en route), with quantities converted from
    MT to litres using standard NPA conversion factors.<br>
    <b style='color:#ff00ff;'>Integration:</b> Enable the vessel toggle on the National Stockout
    page to include pending cargo in the days-of-supply calculation.
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
            st.warning("No valid vessel records found. Check sheet format and sharing settings.")
            return
        st.session_state.vessel_data   = processed
        st.session_state["vessel_year"] = year_sel
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
    c1.metric("Total Vessels",  len(df))
    c2.metric("Discharged",     f"{len(discharged)}  ({discharged['Quantity_Litres'].sum()/1e6:.2f}M LT)")
    c3.metric("⏳ Pending",     f"{len(pending)}  ({pending['Quantity_Litres'].sum()/1e6:.2f}M LT)")
    c4.metric("Grand Total",    f"{df['Quantity_Litres'].sum()/1e6:.2f}M LT")

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

        badges = {
            "Balance":   ("🟢","✅" if has_bal  else "○"),
            "OMC Load":  ("🟢","✅" if has_omc  else "○"),
            "Daily Ord": ("🟢","✅" if has_dly  else "○"),
            "Stock Txn": ("🟢","✅" if has_txn  else "○"),
            "Vessels":   ("🟢","✅" if has_ves  else "○"),
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
    elif choice == "🌍 NATIONAL STOCKOUT":  show_national_stockout()
    elif choice == "🌐 WORLD RISK MONITOR": show_world_monitor()
    elif choice == "🚢 VESSEL SUPPLY":      show_vessel_supply()


main()