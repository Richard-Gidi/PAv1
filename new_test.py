"""
NPA ENERGY ANALYTICS - STREAMLIT DASHBOARD (REFACTORED)
========================================================
Per-BDC API calls: BDC Balance, OMC Loadings, Daily Orders now use each
BDC's own userId stored in .env under BDC_USER_* keys.

INSTALLATION:
    pip install streamlit pandas pdfplumber PyPDF2 openpyxl python-dotenv plotly requests streamlit-js-eval psutil

USAGE:
    streamlit run npa_dashboard.py
"""
import streamlit as st
import os, re, io, json, concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import pdfplumber
import PyPDF2
from dotenv import load_dotenv
import plotly.graph_objects as go
import requests as _requests
import psutil

load_dotenv()

# ── memory badge ────────────────────────────────────────────
_proc = psutil.Process(os.getpid())

# ══════════════════════════════════════════════════════════════
# ENV HELPERS
# ══════════════════════════════════════════════════════════════

def load_bdc_user_map() -> dict:
    """
    Returns {display_name: user_id_int} for every BDC_USER_* key in .env.
    These are the per-BDC userIds used when calling the new per-BDC endpoints.
    """
    mapping = {}
    name_fixes = {
        "C CLEANED OIL LTD": "C. CLEANED OIL LTD",
        "PK JEGS ENERGY LTD": "P.K JEGS ENERGY LTD",
        "TEMA OIL REFINERY TOR": "TEMA OIL REFINERY(TOR)",
        "SOCIETE NATIONAL BURKINABE SONABHY": "SOCIETE NATIONAL BURKINABE (SONABHY)",
        "BOST G40": "BOST-G40",
        "DOMINION INTERNATIONAL PETROLEUM": "DOMINION INTERNATIONAL PETR",
        "PETROLEUM WARE HOUSE AND SUPPLIES": "PETROLEUM WARE HOUSE AND S",
        "INTERNATIONAL PETROLEUM RESOURCES": "INTERNATIONAL PETROLEUM RES",
        "GHANA NATIONAL GAS COMPANY": "GHANA NATIONAL GAS COMPANY",
        "GENYSIS GLOBAL LIMITED": "Genysis Global Limited",
        "GLORYMAY PETROLEUM COMPANY LIMITED": "GLORYMAY PETROLEUM COMPAN",
        "HILSON PETROLEUM GHANA LIMITED": "HILSON PETROLEUM GHANA LIM",
        "CHRISVILLE ENERGY SOLUTIONS": "CHRISVILLE ENERGY SOLUTIONS",
        "PLATON OIL AND GAS": "Platon Oil and Gas",
        "PORTICA OIL AND GAS RESOURCE LIMITED": "Portica Oil and Gas Resource Lim",
        "RESTON ENERGY TRADING LIMITED": "Reston Energy Trading Limited",
        "BATTOP ENERGY LIMITED": "Battop Energy Limited",
    }
    for key, value in os.environ.items():
        if not key.startswith("BDC_USER_"):
            continue
        raw = key[9:].replace("_", " ").strip()
        display = name_fixes.get(raw, raw)
        try:
            mapping[display] = int(value)
        except ValueError:
            pass
    return mapping


def load_bdc_mappings() -> dict:
    """BDC name → lngBDCId (for Stock Transaction page — unchanged)."""
    mappings = {}
    for key, value in os.environ.items():
        if not key.startswith("BDC_") or key.startswith("BDC_USER_"):
            continue
        name = key[4:].replace("_", " ")
        if name == "TEMA OIL REFINERY TOR":
            name = "TEMA OIL REFINERY (TOR)"
        elif name == "SOCIETE NATIONAL BURKINABE SONABHY":
            name = "SOCIETE NATIONAL BURKINABE (SONABHY)"
        elif name == "LIB GHANA LIMITED":
            name = "L.I.B. GHANA LIMITED"
        elif name == "C CLEANED OIL LTD":
            name = "C. CLEANED OIL LTD"
        elif name == "PK JEGS ENERGY LTD":
            name = "P. K JEGS ENERGY LTD"
        mappings[name] = int(value)
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
        elif name == "GHANA OIL COLTD TAKORADI":
            name = "GHANA OIL CO.LTD, TAKORADI"
        elif name == "GOIL LPG BOTTLING PLANT TEMA":
            name = "GOIL LPG BOTTLING PLANT -TEMA"
        elif name == "GOIL LPG BOTTLING PLANT KUMASI":
            name = "GOIL LPG BOTTLING PLANT- KUMASI"
        elif name == "NEWGAS CYLINDER BOTTLING LIMITED TEMA":
            name = "NEWGAS CYLINDER BOTTLING LIMITED-TEMA"
        elif name == "CHASE PETROLEUM TEMA":
            name = "CHASE PETROLEUM - TEMA"
        elif name == "TEMA FUEL COMPANY TFC":
            name = "TEMA FUEL COMPANY (TFC)"
        elif name == "TEMA MULTI PRODUCTS TMPT":
            name = "TEMA MULTI PRODUCTS (TMPT)"
        elif name == "TEMA OIL REFINERY TOR":
            name = "TEMA OIL REFINERY (TOR)"
        elif name == "GHANA OIL COMPANY LTD SEKONDI NAVAL BASE":
            name = "GHANA OIL COMPANY LTD (SEKONDI NAVAL BASE)"
        elif name == "GHANSTOCK LIMITED TAKORADI":
            name = "GHANSTOCK LIMITED (TAKORADI)"
        mappings[name] = int(value)
    return mappings


def load_product_mappings() -> dict:
    return {
        "PMS":    int(os.getenv("PRODUCT_PREMIUM_ID", "12")),
        "Gasoil": int(os.getenv("PRODUCT_GASOIL_ID", "14")),
        "LPG":    int(os.getenv("PRODUCT_LPG_ID", "28")),
    }


# ── Load at startup ─────────────────────────────────────────
BDC_USER_MAP   = load_bdc_user_map()    # display_name → userId  (NEW per-BDC calls)
BDC_MAP        = load_bdc_mappings()    # display_name → lngBDCId (Stock Transaction)
DEPOT_MAP      = load_depot_mappings()
STOCK_PRODUCT_MAP = load_product_mappings()
PRODUCT_OPTIONS   = ["PMS", "Gasoil", "LPG"]
PRODUCT_BALANCE_MAP = {"PMS": "PREMIUM", "Gasoil": "GASOIL", "LPG": "LPG"}

NPA_CONFIG = {
    "COMPANY_ID":        os.getenv("NPA_COMPANY_ID", "1"),
    "USER_ID":           os.getenv("NPA_USER_ID", "123292"),
    "APP_ID":            os.getenv("NPA_APP_ID", "3"),
    "ITS_FROM_PERSOL":   os.getenv("NPA_ITS_FROM_PERSOL", "Persol Systems Limited"),
    "BDC_BALANCE_URL":   os.getenv("NPA_BDC_BALANCE_URL",
                            "https://iml.npa-enterprise.com/NPAAPILIVE/Home/CreateProductBalance"),
    "OMC_LOADINGS_URL":  os.getenv("NPA_OMC_LOADINGS_URL",
                            "https://iml.npa-enterprise.com/NewNPA/home/CreateOrdersReport"),
    "DAILY_ORDERS_URL":  os.getenv("NPA_DAILY_ORDERS_URL",
                            "https://iml.npa-enterprise.com/NewNPA/home/CreateDailyOrderReport"),
    "STOCK_TRANSACTION_URL": os.getenv("NPA_STOCK_TRANSACTION_URL",
                            "https://iml.npa-enterprise.com/NewNPA/home/CreateStockTransactionReport"),
    "OMC_NAME":          os.getenv("OMC_NAME", "OILCORP ENERGIA LIMITED"),
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
# HTTP HELPER
# ══════════════════════════════════════════════════════════════
_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,text/html,*/*;q=0.8",
}


def _fetch_pdf(url: str, params: dict, timeout: int = 45) -> bytes | None:
    try:
        r = _requests.get(url, params=params, headers=_HTTP_HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.content if r.content[:4] == b"%PDF" else None
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════
# PAGE CONFIG & CSS
# ══════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="NPA Energy Analytics 🛢️",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.caption(f"Memory: {_proc.memory_info().rss / 1024 / 1024:.1f} MB")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Rajdhani:wght@300;500;700&display=swap');
.stApp {
    background: linear-gradient(-45deg,#0a0e27,#1a1a2e,#16213e,#0f3460);
    background-size:400% 400%;
    animation:gradientShift 15s ease infinite;
}
@keyframes gradientShift{0%{background-position:0% 50%}50%{background-position:100% 50%}100%{background-position:0% 50%}}
h1,h2,h3{font-family:'Orbitron',sans-serif!important;color:#00ffff!important;
    text-shadow:0 0 10px #00ffff,0 0 20px #00ffff,0 0 30px #00ffff;}
[data-testid="stSidebar"]{background:linear-gradient(180deg,#0a0e27 0%,#16213e 100%);
    border-right:2px solid #00ffff;box-shadow:5px 0 15px rgba(0,255,255,0.3);}
[data-testid="stSidebar"] h1,[data-testid="stSidebar"] h2,[data-testid="stSidebar"] h3{
    color:#ff00ff!important;text-shadow:0 0 10px #ff00ff;}
.stButton>button{background:linear-gradient(45deg,#ff00ff,#00ffff);color:white;
    border:2px solid #00ffff;border-radius:25px;padding:15px 30px;
    font-family:'Orbitron',sans-serif;font-weight:700;font-size:16px;
    box-shadow:0 0 20px rgba(0,255,255,0.5);transition:all 0.3s ease;
    text-transform:uppercase;letter-spacing:2px;}
.stButton>button:hover{transform:scale(1.05) translateY(-3px);
    box-shadow:0 0 30px rgba(0,255,255,0.8),0 0 40px rgba(255,0,255,0.5);}
.dataframe{background-color:rgba(10,14,39,0.8)!important;border:2px solid #00ffff!important;
    border-radius:10px;box-shadow:0 0 20px rgba(0,255,255,0.3);}
.dataframe th{background-color:#16213e!important;color:#00ffff!important;
    font-family:'Orbitron',sans-serif;text-transform:uppercase;border:1px solid #00ffff!important;}
.dataframe td{background-color:rgba(22,33,62,0.6)!important;color:#ffffff!important;
    border:1px solid rgba(0,255,255,0.2)!important;}
[data-testid="stMetricValue"]{font-family:'Orbitron',sans-serif;font-size:26px!important;
    color:#00ffff!important;text-shadow:0 0 15px #00ffff;}
[data-testid="stMetricLabel"]{font-family:'Rajdhani',sans-serif;color:#ff00ff!important;
    font-weight:700;text-transform:uppercase;letter-spacing:2px;}
.metric-card{background:rgba(22,33,62,0.6);padding:20px;border-radius:15px;
    border:2px solid #00ffff;text-align:center;}
.metric-card h2{color:#ff00ff!important;margin:0;font-size:18px!important;}
.metric-card h1{color:#00ffff!important;margin:10px 0;font-size:28px!important;word-wrap:break-word;}
p,span,div{font-family:'Rajdhani',sans-serif;color:#e0e0e0;}
[data-testid="stFileUploader"]{border:2px dashed #00ffff;border-radius:15px;
    background:rgba(22,33,62,0.3);padding:20px;}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
# PDF PARSERS  (unchanged from original)
# ══════════════════════════════════════════════════════════════

# ── BDC Balance PDF parser ───────────────────────────────────
class StockBalanceScraper:
    def __init__(self):
        self.allowed_products = {"PREMIUM", "GASOIL", "LPG"}
        product_alt = "|".join(sorted(self.allowed_products))
        self.product_line_re = re.compile(
            rf"^({product_alt})\s+([\d,]+\.\d{{2}})\s+(-?[\d,]+\.\d{{2}})$",
            flags=re.IGNORECASE,
        )
        self.bost_global_re = re.compile(r"\bBOST\s*GLOBAL\s*DEPOT\b", flags=re.IGNORECASE)

    @staticmethod
    def _normalize_spaces(text):
        return re.sub(r"\s+", " ", (text or "").strip())

    def _normalize_bdc(self, bdc):
        clean = self._normalize_spaces(bdc)
        up = self._normalize_spaces(clean.upper().replace("-", " ").replace("_", " "))
        return "BOST" if up.startswith("BOST") else clean

    def _is_bost_labeled_depot(self, depot):
        d = self._normalize_spaces((depot or "").replace("-", " ")).upper()
        return d.startswith("BOST ")

    def _is_bost_global_depot(self, depot):
        d = self._normalize_spaces((depot or "").replace("-", " "))
        return bool(self.bost_global_re.search(d))

    def _parse_date_from_line(self, line):
        m = re.search(r"(\w+\s+\d{1,2}\s*,\s*\d{4})", line)
        if m:
            cleaned = m.group(1).replace(" ,", ",")
            try:
                return datetime.strptime(cleaned, "%B %d, %Y").strftime("%Y/%m/%d")
            except Exception:
                pass
        return None

    def _append_record(self, records, date, bdc, depot, product, actual, available):
        product = (product or "").upper()
        if product not in self.allowed_products:
            return
        if self._is_bost_labeled_depot(depot) and not self._is_bost_global_depot(depot):
            return
        if actual <= 0:
            return
        records.append({
            "Date": date,
            "BDC": self._normalize_bdc(bdc),
            "DEPOT": self._normalize_spaces(depot),
            "Product": product,
            "ACTUAL BALANCE (LT\\KG)": actual,
            "AVAILABLE BALANCE (LT\\KG)": available,
        })

    def parse_pdf_bytes(self, pdf_bytes: bytes) -> list:
        records = []
        try:
            reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
            cur_bdc = cur_depot = cur_date = None
            for page in reader.pages:
                text = page.extract_text() or ""
                lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
                for line in lines:
                    up = line.upper()
                    if "DATE AS AT" in up:
                        d = self._parse_date_from_line(line)
                        if d:
                            cur_date = d
                    if up.startswith("BDC :") or up.startswith("BDC:"):
                        cur_bdc = re.sub(r"^BDC\s*:\s*", "", line, flags=re.IGNORECASE).strip()
                    if up.startswith("DEPOT :") or up.startswith("DEPOT:"):
                        cur_depot = re.sub(r"^DEPOT\s*:\s*", "", line, flags=re.IGNORECASE).strip()
                    if cur_bdc and cur_depot and cur_date:
                        m = self.product_line_re.match(line)
                        if m:
                            self._append_record(
                                records, cur_date, cur_bdc, cur_depot,
                                m.group(1),
                                float(m.group(2).replace(",", "")),
                                float(m.group(3).replace(",", "")),
                            )
        except Exception as e:
            st.error(f"Balance PDF parse error: {e}")
        return records


# ── OMC Loadings PDF parser ──────────────────────────────────
_PRODUCT_MAP_OMC = {"AGO": "GASOIL", "PMS": "PREMIUM", "LPG": "LPG"}
_ONLY_COLS = ["Date", "OMC", "Truck", "Product", "Quantity", "Price", "Depot", "Order Number", "BDC"]
_HEADER_KEYWORDS = [
    "ORDER REPORT", "National Petroleum Authority", "ORDER NUMBER", "ORDER DATE",
    "ORDER STATUS", "BDC:", "Total for :", "Printed By :", "Page ", "BRV NUMBER", "VOLUME",
]
_LOADED_KEYWORDS = {"Released", "Submitted"}


def _looks_like_header(line):
    return any(h in line for h in _HEADER_KEYWORDS)


def _detect_product(line):
    raw = "AGO" if "AGO" in line else "LPG" if "LPG" in line else "PMS"
    return _PRODUCT_MAP_OMC.get(raw, raw)


def _parse_loaded_line(line, product, depot, bdc):
    tokens = line.split()
    if len(tokens) < 6:
        return None
    rel_idx = next((i for i, t in enumerate(tokens) if t in _LOADED_KEYWORDS), None)
    if rel_idx is None or rel_idx < 2:
        return None
    try:
        date_token, order_number = tokens[0], tokens[1]
        volume = float(tokens[-1].replace(",", ""))
        price  = float(tokens[-2].replace(",", ""))
        brv    = tokens[-3]
        company = " ".join(tokens[rel_idx + 1:-3]).strip()
        try:
            date_str = datetime.strptime(date_token, "%d-%b-%Y").strftime("%Y/%m/%d")
        except Exception:
            date_str = date_token
        return {"Date": date_str, "OMC": company, "Truck": brv, "Product": product,
                "Quantity": volume, "Price": price, "Depot": depot,
                "Order Number": order_number, "BDC": bdc}
    except Exception:
        return None


def extract_omc_loadings_from_pdf(pdf_bytes: bytes, bdc_name: str = "") -> pd.DataFrame:
    rows = []
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
                            cur_bdc = m.group(1).strip()
                        continue
                    if "PRODUCT" in line:
                        cur_prod = _detect_product(line)
                        continue
                    if _looks_like_header(line):
                        continue
                    if any(kw in line for kw in _LOADED_KEYWORDS):
                        row = _parse_loaded_line(line, cur_prod, cur_depot, cur_bdc)
                        if row:
                            rows.append(row)
    except Exception as e:
        st.error(f"OMC PDF parse error: {e}")
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=_ONLY_COLS)
    for col in _ONLY_COLS:
        if col not in df.columns:
            df[col] = ""
    df = df[_ONLY_COLS].drop_duplicates()
    try:
        ds = pd.to_datetime(df["Date"], format="%Y/%m/%d", errors="coerce")
        df = df.assign(_ds=ds).sort_values("_ds").drop(columns=["_ds"]).reset_index(drop=True)
    except Exception:
        pass
    return df


# ── Daily Orders PDF parser ──────────────────────────────────
def _get_product_category(text):
    t = text.upper()
    if "AVIATION" in t or "TURBINE" in t:
        return "ATK"
    if "RFO" in t:
        return "RFO"
    if "PREMIX" in t:
        return "PREMIX"
    if "LPG" in t:
        return "LPG"
    if "AGO" in t or "MGO" in t or "GASOIL" in t:
        return "GASOIL"
    return "PREMIUM"


def _parse_daily_line(line, last_date):
    pv = re.search(r"(\d{1,4}\.\d{2,4})\s+(\d{1,3}(?:,\d{3})*\.\d{2})$", line.strip())
    if not pv:
        return None
    price  = float(pv.group(1))
    volume = float(pv.group(2).replace(",", ""))
    remainder = line[: pv.start()].strip()
    tokens = remainder.split()
    if not tokens:
        return None
    brv = tokens[-1]
    remainder = " ".join(tokens[:-1])
    date_val = last_date
    dm = re.search(r"(\d{2}/\d{2}/\d{4})", remainder)
    if dm:
        try:
            date_val = datetime.strptime(dm.group(1), "%d/%m/%Y").strftime("%Y/%m/%d")
        except Exception:
            date_val = dm.group(1)
        remainder = remainder.replace(dm.group(1), "").strip()
    product_cat = _get_product_category(line)
    noise = ["PMS","AGO","LPG","RFO","ATK","PREMIX","FOREIGN","(Retail","Retail",
             "Outlets","MGO","Local","Additivated","Differentiated","MINES",
             "Cell","Sites","Turbine","Kerosene"]
    order_tokens = [t for t in remainder.split()
                    if not any(nw.upper() in t.upper() or t in ("(",")","-") for nw in noise)]
    order_number = " ".join(order_tokens).strip() or remainder
    return {"Date": date_val, "Order Number": order_number, "Product": product_cat,
            "Truck": brv, "Price": price, "Quantity": volume}


def extract_daily_orders_from_pdf(pdf_bytes: bytes, bdc_name: str = "") -> pd.DataFrame:
    all_rows = []
    ctx = {"Depot": "Unknown", "BDC": bdc_name, "Status": "Unknown", "Date": None}
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
                        ctx["Depot"] = "BOST Global" if (raw_d.startswith("BOST") or "TAKORADI BLUE OCEAN" in raw_d) else raw_d
                        continue
                    if cl.startswith("BDC:"):
                        ctx["BDC"] = cl.replace("BDC:", "").strip()
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
                        all_rows.append({
                            "Date": row["Date"], "Truck": row["Truck"],
                            "Product": row["Product"], "Quantity": row["Quantity"],
                            "Price": row["Price"], "Depot": ctx["Depot"],
                            "Order Number": row["Order Number"],
                            "BDC": ctx["BDC"], "Status": ctx["Status"],
                        })
    except Exception as e:
        st.error(f"Daily orders PDF parse error: {e}")
    if not all_rows:
        return pd.DataFrame()
    df = pd.DataFrame(all_rows)
    # Simplify BDC names to first 2 words
    def _simplify(name):
        if not name:
            return name
        return " ".join(name.split()[:2]).upper()
    df["BDC"] = df["BDC"].apply(_simplify)
    return df


# ── Stock Transaction PDF parser ─────────────────────────────
def _parse_stock_transaction_pdf(pdf_bytes: bytes) -> list:
    DESCRIPTIONS = sorted([
        "Balance b/fwd", "Stock Take", "Sale",
        "Custody Transfer In", "Custody Transfer Out", "Product Outturn",
    ], key=len, reverse=True)
    SKIP_PREFIXES = (
        "national petroleum authority", "stock transaction report",
        "bdc :", "depot :", "product :", "printed by", "printed on",
        "date trans #", "actual stock balance", "stock commitments",
        "available stock balance", "last stock update", "i.t.s from",
    )

    def _skip(line):
        lo = line.strip().lower()
        return lo.startswith(SKIP_PREFIXES) or bool(re.match(r"^\d{1,2}\s+\w+,\s+\d{4}", line.strip()))

    def _parse_num(s):
        s = s.strip()
        neg = s.startswith("(") and s.endswith(")")
        try:
            val = int(s.strip("()").replace(",", ""))
            return -val if neg else val
        except ValueError:
            return None

    def _parse_line(line):
        line = line.strip()
        if not re.match(r"^\d{2}/\d{2}/\d{4}\b", line):
            return None
        parts = line.split()
        date, trans = parts[0], (parts[1] if len(parts) > 1 else "")
        rest = line[len(date):].strip()[len(trans):].strip()
        description = after_desc = None
        for desc in DESCRIPTIONS:
            if rest.lower().startswith(desc.lower()):
                description = desc
                after_desc = rest[len(desc):].strip()
                break
        if description is None or description == "Balance b/fwd":
            return None
        nums = re.findall(r"\([\d,]+\)|[\d,]+", after_desc)
        if len(nums) < 2:
            return None
        volume  = _parse_num(nums[-2])
        balance = _parse_num(nums[-1])
        trail = re.search(re.escape(nums[-2]) + r"\s+" + re.escape(nums[-1]) + r"\s*$", after_desc)
        account = after_desc[:trail.start()].strip() if trail else " ".join(after_desc.split()[:-2])
        return {"Date": date, "Trans #": trans, "Description": description,
                "Account": account, "Volume": volume or 0, "Balance": balance or 0}

    records = []
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
                        records.append(row)
    except Exception as e:
        st.error(f"Stock transaction PDF parse error: {e}")
    return records


# ══════════════════════════════════════════════════════════════
# PER-BDC FETCH HELPERS  (the new logic)
# ══════════════════════════════════════════════════════════════

def _build_balance_params(user_id: int) -> dict:
    return {
        "lngCompanyId":     NPA_CONFIG["COMPANY_ID"],
        "strITSfromPersol": NPA_CONFIG["ITS_FROM_PERSOL"],
        "strGroupBy":       "BDC",
        "strGroupBy1":      "DEPOT",
        "strQuery1":        "",
        "strQuery2":        "",
        "strQuery3":        "",
        "strQuery4":        "",
        "strPicHeight":     "1",
        "szPicWeight":      "1",
        "lngUserId":        str(user_id),
        "intAppId":         NPA_CONFIG["APP_ID"],
    }


def _build_omc_params(user_id: int, start_str: str, end_str: str) -> dict:
    return {
        "lngCompanyId":   NPA_CONFIG["COMPANY_ID"],
        "szITSfromPersol":"persol",
        "strGroupBy":     "BDC",
        "strGroupBy1":    "",
        "strQuery1":      " and iorderstatus=4",
        "strQuery2":      start_str,
        "strQuery3":      end_str,
        "strQuery4":      "",
        "strPicHeight":   "",
        "strPicWeight":   "",
        "intPeriodID":    "4",
        "iUserId":        str(user_id),
        "iAppId":         NPA_CONFIG["APP_ID"],
    }


def _build_daily_params(user_id: int, start_str: str, end_str: str) -> dict:
    return {
        "lngCompanyId":   NPA_CONFIG["COMPANY_ID"],
        "szITSfromPersol":"persol",
        "strGroupBy":     "DEPOT",
        "strGroupBy1":    "",
        "strQuery1":      "",
        "strQuery2":      start_str,
        "strQuery3":      end_str,
        "strQuery4":      "",
        "strPicHeight":   "1",
        "strPicWeight":   "1",
        "intPeriodID":    "-1",
        "iUserId":        str(user_id),
        "iAppId":         NPA_CONFIG["APP_ID"],
    }


def fetch_all_bdc_balance(selected_bdcs: list, progress_cb=None) -> list:
    """Fetch BDC Balance for multiple BDCs in parallel. Returns combined records list."""
    scraper = StockBalanceScraper()
    all_records = []
    total = len(selected_bdcs)
    done = [0]

    def _fetch_one(bdc_name):
        user_id = BDC_USER_MAP.get(bdc_name)
        if not user_id:
            return bdc_name, [], "No userId"
        params = _build_balance_params(user_id)
        pdf_bytes = _fetch_pdf(NPA_CONFIG["BDC_BALANCE_URL"], params)
        if not pdf_bytes:
            return bdc_name, [], "No PDF returned"
        records = scraper.parse_pdf_bytes(pdf_bytes)
        return bdc_name, records, None

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(_fetch_one, b): b for b in selected_bdcs}
        for fut in concurrent.futures.as_completed(futures):
            done[0] += 1
            try:
                bdc_name, records, err = fut.result()
                all_records.extend(records)
            except Exception as e:
                pass
            if progress_cb:
                progress_cb(done[0], total)

    return all_records


def fetch_all_bdc_omc(selected_bdcs: list, start_str: str, end_str: str, progress_cb=None) -> pd.DataFrame:
    """Fetch OMC Loadings for multiple BDCs in parallel."""
    frames = []
    total = len(selected_bdcs)
    done = [0]

    def _fetch_one(bdc_name):
        user_id = BDC_USER_MAP.get(bdc_name)
        if not user_id:
            return pd.DataFrame()
        params = _build_omc_params(user_id, start_str, end_str)
        pdf_bytes = _fetch_pdf(NPA_CONFIG["OMC_LOADINGS_URL"], params)
        if not pdf_bytes:
            return pd.DataFrame()
        return extract_omc_loadings_from_pdf(pdf_bytes, bdc_name)

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(_fetch_one, b): b for b in selected_bdcs}
        for fut in concurrent.futures.as_completed(futures):
            done[0] += 1
            try:
                df = fut.result()
                if not df.empty:
                    frames.append(df)
            except Exception:
                pass
            if progress_cb:
                progress_cb(done[0], total)

    if not frames:
        return pd.DataFrame(columns=_ONLY_COLS)
    combined = pd.concat(frames, ignore_index=True)
    dedup_cols = [c for c in ["Date", "Order Number", "Truck", "Product", "Depot", "BDC"]
                  if c in combined.columns]
    return combined.drop_duplicates(subset=dedup_cols) if dedup_cols else combined.drop_duplicates()


def fetch_all_bdc_daily(selected_bdcs: list, start_str: str, end_str: str, progress_cb=None) -> pd.DataFrame:
    """Fetch Daily Orders for multiple BDCs in parallel."""
    frames = []
    total = len(selected_bdcs)
    done = [0]

    def _fetch_one(bdc_name):
        user_id = BDC_USER_MAP.get(bdc_name)
        if not user_id:
            return pd.DataFrame()
        params = _build_daily_params(user_id, start_str, end_str)
        pdf_bytes = _fetch_pdf(NPA_CONFIG["DAILY_ORDERS_URL"], params)
        if not pdf_bytes:
            return pd.DataFrame()
        return extract_daily_orders_from_pdf(pdf_bytes, bdc_name)

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(_fetch_one, b): b for b in selected_bdcs}
        for fut in concurrent.futures.as_completed(futures):
            done[0] += 1
            try:
                df = fut.result()
                if not df.empty:
                    frames.append(df)
            except Exception:
                pass
            if progress_cb:
                progress_cb(done[0], total)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).drop_duplicates()


# ══════════════════════════════════════════════════════════════
# NATIONAL STOCKOUT HELPERS  (from original, adapted)
# ══════════════════════════════════════════════════════════════
SNAPSHOT_DIR = os.path.join(os.getcwd(), "national_snapshots")


def _save_national_snapshot(forecast_df: pd.DataFrame, period_label: str):
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    snap = {
        "ts": datetime.now().isoformat(),
        "period": period_label,
        "rows": forecast_df[["product", "total_balance", "omc_sales", "daily_rate", "days_remaining"]].to_dict("records"),
    }
    with open(os.path.join(SNAPSHOT_DIR, f"snap_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"), "w") as f:
        json.dump(snap, f)


def _count_period_days(start_str: str, end_str: str, use_business_days: bool) -> int:
    fmt = "%m/%d/%Y"
    d_start = datetime.strptime(start_str, fmt).date()
    d_end   = datetime.strptime(end_str,   fmt).date()
    count = len(pd.bdate_range(d_start, d_end)) if use_business_days else (d_end - d_start).days
    return max(count, 1)


# ══════════════════════════════════════════════════════════════
# VESSEL SUPPLY HELPERS  (unchanged from original)
# ══════════════════════════════════════════════════════════════
VESSEL_CONVERSION_FACTORS = {"PREMIUM": 1324.50, "GASOIL": 1183.00, "LPG": 1000.00, "NAPHTHA": 800.00}
VESSEL_PRODUCT_MAPPING = {"PMS": "PREMIUM","GASOLINE": "PREMIUM","AGO": "GASOIL",
                          "GASOIL": "GASOIL","LPG": "LPG","BUTANE": "LPG","NAPHTHA": "NAPHTHA"}
VESSEL_MONTH_MAPPING = {m[:3].title(): m[:3].upper() for m in
    ["January","February","March","April","May","June",
     "July","August","September","October","November","December"]}


def _load_vessel_sheet(url_in=None):
    from io import StringIO, BytesIO
    url_in = url_in or VESSEL_SHEET_URL
    m_id  = re.search(r"/d/([a-zA-Z0-9-_]+)", url_in)
    file_id = m_id.group(1) if m_id else (url_in if re.match(r"^[a-zA-Z0-9-_]{20,}$", url_in) else None)
    if not file_id:
        return None, "Could not extract Google Sheets file ID."
    m_gid = re.search(r"(?:#|\?|&)gid=(\d+)", url_in)
    gid = m_gid.group(1) if m_gid else None
    candidates = []
    if gid:
        candidates.append((f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv&gid={gid}", "csv"))
    candidates += [
        (f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv&gid=0", "csv"),
        (f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv", "csv"),
        (f"https://docs.google.com/spreadsheets/d/{file_id}/gviz/tq?tqx=out:csv", "gviz"),
        (f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx", "xlsx"),
    ]
    hdrs = {"User-Agent": "Mozilla/5.0"}
    for url, mode in candidates:
        try:
            r = _requests.get(url, headers=hdrs, timeout=30)
            if r.status_code != 200 or not r.content:
                continue
            if mode == "xlsx":
                return pd.read_excel(BytesIO(r.content)), None
            df = pd.read_csv(StringIO(r.content.decode("utf-8", errors="replace")),
                             header=14, skiprows=1, skipfooter=1, engine="python")
            return df, None
        except Exception:
            continue
    return None, "All fetch strategies failed. Ensure sheet is shared publicly."


def _parse_vessel_date(date_str, default_year="2025"):
    date_str = str(date_str).strip().upper()
    if "PENDING" in date_str or date_str in ("NAN", ""):
        month_code = VESSEL_MONTH_MAPPING.get(datetime.now().strftime("%b"), datetime.now().strftime("%b").upper())
        return month_code, default_year, "PENDING"
    try:
        if "-" in date_str:
            parts = date_str.split("-")
            if len(parts) == 2:
                month = VESSEL_MONTH_MAPPING.get(parts[1].title(), parts[1].upper())
                return month, default_year, "DISCHARGED"
    except Exception:
        pass
    return "Unknown", default_year, "DISCHARGED"


def _process_vessel_df(vessel_df, year="2025"):
    vessel_df = vessel_df.copy()
    vessel_df.columns = vessel_df.columns.str.strip()
    col_idx = {}
    for i, col in enumerate(vessel_df.columns):
        cl = str(col).lower().strip()
        if "receiver" in cl or (i == 0 and "unnamed" not in cl):
            col_idx["receivers"] = i
        elif "type" in cl and "receiver" not in cl:
            col_idx["type"] = i
        elif "vessel" in cl and "name" in cl:
            col_idx["vessel_name"] = i
        elif "supplier" in cl:
            col_idx["supplier"] = i
        elif "product" in cl:
            col_idx["product"] = i
        elif "quantity" in cl or ("mt" in cl and "quantity" not in cl):
            col_idx["quantity"] = i
        elif "date" in cl or "discharg" in cl:
            col_idx["date"] = i
    records = []
    for _, row in vessel_df.dropna(how="all").iterrows():
        try:
            receivers   = str(row.iloc[col_idx.get("receivers", 0)]).strip()
            vessel_type = str(row.iloc[col_idx.get("type", 1)]).strip()
            vessel_name = str(row.iloc[col_idx.get("vessel_name", 2)]).strip()
            supplier    = str(row.iloc[col_idx.get("supplier", 3)]).strip()
            product_raw = str(row.iloc[col_idx.get("product", 4)]).strip().upper()
            qty_str     = str(row.iloc[col_idx.get("quantity", 5)]).replace(",", "").strip()
            date_cell   = str(row.iloc[col_idx.get("date", 6)]).strip()
            if receivers.upper() in {"RECEIVER(S)","RECEIVERS","NAN",""} or product_raw in {"PRODUCT","NAN",""}: continue
            try: qty_mt = float(qty_str)
            except ValueError: continue
            if qty_mt <= 0: continue
            product = VESSEL_PRODUCT_MAPPING.get(product_raw, product_raw)
            if product not in VESSEL_CONVERSION_FACTORS: continue
            qty_lt = qty_mt * VESSEL_CONVERSION_FACTORS[product]
            month, yr, status = _parse_vessel_date(date_cell, default_year=year)
            records.append({"Receivers": receivers,"Vessel_Type": vessel_type,"Vessel_Name": vessel_name,
                            "Supplier": supplier,"Product": product,"Original_Product": product_raw,
                            "Quantity_MT": qty_mt,"Quantity_Litres": qty_lt,"Date_Discharged": date_cell,
                            "Month": month,"Year": yr,"Status": status})
        except Exception:
            continue
    return pd.DataFrame(records)


# ══════════════════════════════════════════════════════════════
# EXCEL EXPORT HELPERS
# ══════════════════════════════════════════════════════════════

def _to_excel_bytes(sheets: dict) -> bytes:
    """Convert {sheet_name: df} to Excel bytes for download."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name[:31], index=False)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════
# PAGE: BDC BALANCE
# ══════════════════════════════════════════════════════════════
def show_bdc_balance():
    st.markdown("<h2>🏦 BDC STOCK BALANCE ANALYZER</h2>", unsafe_allow_html=True)
    st.info("Fetches the current stock balance for each BDC using their individual API credentials.")
    st.markdown("---")

    all_bdc_names = sorted(BDC_USER_MAP.keys())
    if not all_bdc_names:
        st.error("No BDC_USER_* entries found in .env. Please add them.")
        return

    col1, col2 = st.columns([3, 1])
    with col1:
        selected = st.multiselect(
            "Select BDCs to fetch (leave blank = fetch ALL)",
            all_bdc_names,
            key="bal_bdc_select",
            help="Fetching all may take 1–2 minutes depending on server speed.",
        )
    with col2:
        fetch_all_flag = st.checkbox("Fetch ALL BDCs", value=True, key="bal_fetch_all")

    bdcs_to_fetch = all_bdc_names if (fetch_all_flag or not selected) else selected

    if st.button("🔄 FETCH BDC BALANCE DATA", key="bal_fetch"):
        prog = st.progress(0, text="Starting…")

        def _cb(done, total):
            prog.progress(done / total, text=f"Fetched {done}/{total} BDCs…")

        with st.spinner("Fetching in parallel…"):
            records = fetch_all_bdc_balance(bdcs_to_fetch, progress_cb=_cb)
        prog.progress(1.0, text="Done!")

        st.session_state.bdc_records = records
        if records:
            st.success(f"✅ {len(records)} records from {len(bdcs_to_fetch)} BDCs.")
        else:
            st.warning("No records returned. Check credentials or API availability.")

    records = st.session_state.get("bdc_records", [])
    if not records:
        st.info("👆 Click FETCH to load data.")
        return

    df = pd.DataFrame(records)
    col_bal = "ACTUAL BALANCE (LT\\KG)"

    st.markdown("---")
    summary = df.groupby("Product")[col_bal].sum()
    cols = st.columns(3)
    for idx, prod in enumerate(["PREMIUM", "GASOIL", "LPG"]):
        with cols[idx]:
            val = summary.get(prod, 0)
            st.markdown(f"<div class='metric-card'><h2>{prod}</h2><h1>{val:,.0f}</h1>"
                        f"<p style='color:#888;font-size:14px;margin:0'>LT/KG</p></div>",
                        unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### 🏢 BDC BREAKDOWN")
    bdc_sum = (df.groupby("BDC")
               .agg({col_bal: "sum", "DEPOT": "nunique", "Product": "nunique"})
               .reset_index())
    bdc_sum.columns = ["BDC", "Total Balance (LT/KG)", "Depots", "Products"]
    bdc_sum = bdc_sum.sort_values("Total Balance (LT/KG)", ascending=False)
    st.dataframe(bdc_sum, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("### 📊 PRODUCT × BDC PIVOT")
    pivot = df.pivot_table(index="BDC", columns="Product", values=col_bal,
                           aggfunc="sum", fill_value=0).reset_index()
    for p in ["GASOIL", "LPG", "PREMIUM"]:
        if p not in pivot.columns:
            pivot[p] = 0
    pivot["TOTAL"] = pivot[["GASOIL", "LPG", "PREMIUM"]].sum(axis=1)
    pivot = pivot.sort_values("TOTAL", ascending=False)
    st.dataframe(pivot[["BDC", "GASOIL", "LPG", "PREMIUM", "TOTAL"]], use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("### 🔍 FILTER")
    ftype = st.selectbox("Filter by", ["Product", "BDC", "Depot"], key="bal_ftype")
    opts = ["ALL"] + sorted(df[{"Product": "Product", "BDC": "BDC", "Depot": "DEPOT"}[ftype]].unique().tolist())
    fval = st.selectbox("Value", opts, key="bal_fval")
    filtered = df if fval == "ALL" else df[df[{"Product": "Product", "BDC": "BDC", "Depot": "DEPOT"}[ftype]] == fval]
    st.dataframe(
        filtered[["Product", "BDC", "DEPOT", "AVAILABLE BALANCE (LT\\KG)", col_bal, "Date"]]
        .sort_values(["Product", "BDC"]),
        use_container_width=True, height=400, hide_index=True,
    )

    st.markdown("---")
    excel_bytes = _to_excel_bytes({
        "All": df,
        "LPG": df[df["Product"] == "LPG"],
        "PREMIUM": df[df["Product"] == "PREMIUM"],
        "GASOIL": df[df["Product"] == "GASOIL"],
        "BDC Summary": pivot,
    })
    st.download_button("⬇️ DOWNLOAD EXCEL", excel_bytes, "bdc_balance.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ══════════════════════════════════════════════════════════════
# PAGE: OMC LOADINGS
# ══════════════════════════════════════════════════════════════
def show_omc_loadings():
    st.markdown("<h2>🚚 OMC LOADINGS ANALYZER</h2>", unsafe_allow_html=True)
    st.info("Fetches released OMC orders for each BDC using their individual API credentials.")
    st.markdown("---")

    all_bdc_names = sorted(BDC_USER_MAP.keys())
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=7), key="omc_start")
    with col2:
        end_date = st.date_input("End Date", value=datetime.now(), key="omc_end")

    col3, col4 = st.columns([3, 1])
    with col3:
        selected = st.multiselect("Select BDCs (blank = ALL)", all_bdc_names, key="omc_bdc_select")
    with col4:
        fetch_all_flag = st.checkbox("Fetch ALL BDCs", value=True, key="omc_fetch_all")

    bdcs_to_fetch = all_bdc_names if (fetch_all_flag or not selected) else selected

    if st.button("🔄 FETCH OMC LOADINGS", key="omc_fetch"):
        start_str = start_date.strftime("%m/%d/%Y")
        end_str   = end_date.strftime("%m/%d/%Y")
        prog = st.progress(0, text="Starting…")

        def _cb(done, total):
            prog.progress(done / total, text=f"Fetched {done}/{total} BDCs…")

        with st.spinner("Fetching in parallel…"):
            df = fetch_all_bdc_omc(bdcs_to_fetch, start_str, end_str, progress_cb=_cb)
        prog.progress(1.0, text="Done!")

        st.session_state.omc_df = df
        st.session_state.omc_start_date = start_date
        st.session_state.omc_end_date   = end_date

        if not df.empty:
            st.success(f"✅ {len(df):,} loading records from {len(bdcs_to_fetch)} BDCs.")
        else:
            st.warning("No records returned.")

    df = st.session_state.get("omc_df", pd.DataFrame())
    if df.empty:
        st.info("👆 Select dates and click FETCH.")
        return

    st.markdown("---")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Orders", f"{len(df):,}")
    c2.metric("Total Volume (LT)", f"{df['Quantity'].sum():,.0f}")
    c3.metric("OMCs", f"{df['OMC'].nunique()}")
    c4.metric("Value (₵)", f"{(df['Quantity']*df['Price']).sum():,.0f}")

    st.markdown("### 📦 PRODUCT BREAKDOWN")
    prod_sum = df.groupby("Product").agg({"Quantity": "sum", "Order Number": "count", "OMC": "nunique"}).reset_index()
    prod_sum.columns = ["Product", "Total Volume (LT/KG)", "Orders", "OMCs"]
    st.dataframe(prod_sum.sort_values("Total Volume (LT/KG)", ascending=False), use_container_width=True, hide_index=True)

    st.markdown("### 🏢 TOP OMCs")
    omc_sum = (df.groupby("OMC").agg({"Quantity": "sum", "Order Number": "count"})
               .reset_index().sort_values("Quantity", ascending=False).head(20))
    omc_sum.columns = ["OMC", "Total Volume (LT/KG)", "Orders"]
    st.dataframe(omc_sum, use_container_width=True, hide_index=True)

    st.markdown("### 🏦 BDC PERFORMANCE")
    bdc_sum = (df.groupby("BDC").agg({"Quantity": "sum", "Order Number": "count", "OMC": "nunique"})
               .reset_index().sort_values("Quantity", ascending=False))
    bdc_sum.columns = ["BDC", "Total Volume (LT/KG)", "Orders", "OMCs"]
    st.dataframe(bdc_sum, use_container_width=True, hide_index=True)

    st.markdown("---")
    ftype = st.selectbox("Filter by", ["Product", "OMC", "BDC", "Depot"], key="omc_ftype")
    col_map = {"Product": "Product", "OMC": "OMC", "BDC": "BDC", "Depot": "Depot"}
    opts = ["ALL"] + sorted(df[col_map[ftype]].unique().tolist())
    fval = st.selectbox("Value", opts, key="omc_fval")
    filt = df if fval == "ALL" else df[df[col_map[ftype]] == fval]
    st.dataframe(filt[["Date","OMC","Truck","Quantity","Order Number","BDC","Depot","Price","Product"]]
                 .sort_values(["Product","Date"]), use_container_width=True, height=400, hide_index=True)

    st.markdown("---")
    pivot = df.pivot_table(index="BDC", columns="Product", values="Quantity", aggfunc="sum", fill_value=0).reset_index()
    for p in ["GASOIL","LPG","PREMIUM"]:
        if p not in pivot.columns: pivot[p] = 0
    pivot["TOTAL"] = pivot[["GASOIL","LPG","PREMIUM"]].sum(axis=1)
    excel_bytes = _to_excel_bytes({"All Orders": df, "BDC Summary": pivot, **{p: df[df["Product"]==p] for p in ["PREMIUM","GASOIL","LPG"]}})
    st.download_button("⬇️ DOWNLOAD EXCEL", excel_bytes, "omc_loadings.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ══════════════════════════════════════════════════════════════
# PAGE: DAILY ORDERS
# ══════════════════════════════════════════════════════════════
def show_daily_orders():
    st.markdown("<h2>📅 DAILY ORDERS ANALYZER</h2>", unsafe_allow_html=True)
    st.info("Fetches daily order data for each BDC using their individual API credentials.")
    st.markdown("---")

    all_bdc_names = sorted(BDC_USER_MAP.keys())
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=1), key="daily_start")
    with col2:
        end_date = st.date_input("End Date", value=datetime.now(), key="daily_end")

    col3, col4 = st.columns([3, 1])
    with col3:
        selected = st.multiselect("Select BDCs (blank = ALL)", all_bdc_names, key="daily_bdc_select")
    with col4:
        fetch_all_flag = st.checkbox("Fetch ALL BDCs", value=True, key="daily_fetch_all")

    bdcs_to_fetch = all_bdc_names if (fetch_all_flag or not selected) else selected

    if st.button("🔄 FETCH DAILY ORDERS", key="daily_fetch"):
        start_str = start_date.strftime("%m/%d/%Y")
        end_str   = end_date.strftime("%m/%d/%Y")
        prog = st.progress(0, text="Starting…")

        def _cb(done, total):
            prog.progress(done / total, text=f"Fetched {done}/{total} BDCs…")

        with st.spinner("Fetching in parallel…"):
            df = fetch_all_bdc_daily(bdcs_to_fetch, start_str, end_str, progress_cb=_cb)
        prog.progress(1.0, text="Done!")

        st.session_state.daily_df = df
        st.session_state.daily_start_date = start_date
        st.session_state.daily_end_date   = end_date

        if not df.empty:
            st.success(f"✅ {len(df):,} daily order records.")
        else:
            st.warning("No records returned.")

    df = st.session_state.get("daily_df", pd.DataFrame())
    if df.empty:
        st.info("👆 Select dates and click FETCH.")
        return

    st.markdown("---")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Orders", f"{len(df):,}")
    c2.metric("Volume (LT)", f"{df['Quantity'].sum():,.0f}")
    c3.metric("BDCs", f"{df['BDC'].nunique()}")
    c4.metric("Depots", f"{df['Depot'].nunique()}")
    c5.metric("Value (₵)", f"{(df['Quantity']*df['Price']).sum():,.0f}")

    st.markdown("### 📦 PRODUCT SUMMARY")
    prod_sum = df.groupby("Product").agg({"Quantity": "sum", "Order Number": "count", "BDC": "nunique"}).reset_index()
    prod_sum.columns = ["Product", "Total Volume (LT/KG)", "Orders", "BDCs"]
    st.dataframe(prod_sum.sort_values("Total Volume (LT/KG)", ascending=False), use_container_width=True, hide_index=True)

    st.markdown("### 🏦 BDC SUMMARY")
    bdc_sum = df.groupby("BDC").agg({"Quantity": "sum", "Order Number": "count"}).reset_index().sort_values("Quantity", ascending=False)
    bdc_sum.columns = ["BDC", "Total Volume (LT/KG)", "Orders"]
    st.dataframe(bdc_sum, use_container_width=True, hide_index=True)

    st.markdown("### 📊 BDC × PRODUCT PIVOT")
    pivot = df.pivot_table(index="BDC", columns="Product", values="Quantity", aggfunc="sum", fill_value=0).reset_index()
    pcols = [c for c in pivot.columns if c != "BDC"]
    pivot["TOTAL"] = pivot[pcols].sum(axis=1)
    st.dataframe(pivot.sort_values("TOTAL", ascending=False), use_container_width=True, hide_index=True)

    st.markdown("---")
    ftype = st.selectbox("Filter by", ["Product", "BDC", "Depot", "Status"], key="daily_ftype")
    col_map = {"Product": "Product", "BDC": "BDC", "Depot": "Depot", "Status": "Status"}
    opts = ["ALL"] + sorted(df[col_map[ftype]].dropna().unique().tolist())
    fval = st.selectbox("Value", opts, key="daily_fval")
    filt = df if fval == "ALL" else df[df[col_map[ftype]] == fval]
    st.dataframe(filt[["Date","Truck","Quantity","Order Number","BDC","Depot","Price","Product","Status"]]
                 .sort_values(["Product","Date"]), use_container_width=True, height=400, hide_index=True)

    st.markdown("---")
    excel_bytes = _to_excel_bytes({"All Orders": df, "BDC Pivot": pivot})
    st.download_button("⬇️ DOWNLOAD EXCEL", excel_bytes, "daily_orders.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ══════════════════════════════════════════════════════════════
# PAGE: MARKET SHARE
# ══════════════════════════════════════════════════════════════
def show_market_share():
    st.markdown("<h2>📊 BDC MARKET SHARE ANALYSIS</h2>", unsafe_allow_html=True)
    st.markdown("---")

    has_balance  = bool(st.session_state.get("bdc_records"))
    has_loadings = not st.session_state.get("omc_df", pd.DataFrame()).empty

    c1, c2 = st.columns(2)
    with c1:
        if has_balance:
            st.success(f"✅ BDC Balance: {len(st.session_state.bdc_records)} records")
        else:
            st.warning("⚠️ Fetch BDC Balance first")
    with c2:
        if has_loadings:
            st.success(f"✅ OMC Loadings: {len(st.session_state.omc_df)} records")
        else:
            st.warning("⚠️ Fetch OMC Loadings first")

    if not has_balance and not has_loadings:
        st.error("No data available. Fetch data from BDC Balance and/or OMC Loadings pages first.")
        return

    balance_df  = pd.DataFrame(st.session_state.bdc_records) if has_balance else pd.DataFrame()
    loadings_df = st.session_state.omc_df if has_loadings else pd.DataFrame()

    all_bdcs = sorted(
        set(balance_df["BDC"].unique() if not balance_df.empty else []) |
        set(loadings_df["BDC"].unique() if not loadings_df.empty else [])
    )
    selected_bdc = st.selectbox("Choose BDC:", all_bdcs, key="ms_bdc")
    if not selected_bdc:
        return

    col_bal = "ACTUAL BALANCE (LT\\KG)"
    st.markdown(f"## 📊 MARKET REPORT: {selected_bdc}")
    st.markdown("---")

    tab1, tab2 = st.tabs(["📦 Stock Balance", "🚚 Sales Volume"])

    with tab1:
        if not has_balance:
            st.warning("Fetch BDC Balance first.")
        else:
            bdc_bal   = balance_df[balance_df["BDC"] == selected_bdc]
            total_mkt = float(balance_df[col_bal].sum())
            bdc_total = float(bdc_bal[col_bal].sum())
            share_pct = bdc_total / total_mkt * 100 if total_mkt else 0
            rank = list(balance_df.groupby("BDC")[col_bal].sum().sort_values(ascending=False).index).index(selected_bdc) + 1 \
                   if selected_bdc in balance_df["BDC"].values else "N/A"

            c1, c2, c3 = st.columns(3)
            c1.metric("Total Stock (LT)", f"{bdc_total:,.0f}")
            c2.metric("Market Share", f"{share_pct:.2f}%")
            c3.metric("Rank", f"#{rank}")

            rows = []
            for prod in ["PREMIUM", "GASOIL", "LPG"]:
                mkt = float(balance_df[balance_df["Product"] == prod][col_bal].sum())
                bv  = float(bdc_bal[bdc_bal["Product"] == prod][col_bal].sum())
                rows.append({"Product": prod, "BDC Stock (LT)": f"{bv:,.0f}",
                              "Market Total (LT)": f"{mkt:,.0f}",
                              "Share (%)": f"{bv/mkt*100:.2f}" if mkt else "0.00"})
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    with tab2:
        if not has_loadings:
            st.warning("Fetch OMC Loadings first.")
        else:
            bdc_ld    = loadings_df[loadings_df["BDC"] == selected_bdc]
            total_vol = float(loadings_df["Quantity"].sum())
            bdc_vol   = float(bdc_ld["Quantity"].sum())
            share_pct = bdc_vol / total_vol * 100 if total_vol else 0
            all_bdc_sales = loadings_df.groupby("BDC")["Quantity"].sum().sort_values(ascending=False)
            sales_rank = list(all_bdc_sales.index).index(selected_bdc) + 1 if selected_bdc in all_bdc_sales.index else "N/A"

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Sales (LT)", f"{bdc_vol:,.0f}")
            c2.metric("Market Share", f"{share_pct:.2f}%")
            c3.metric("Sales Rank", f"#{sales_rank}")
            c4.metric("Revenue (₵)", f"{(bdc_ld['Quantity']*bdc_ld['Price']).sum():,.0f}")

            rows = []
            for prod in ["PREMIUM", "GASOIL", "LPG"]:
                mkt = float(loadings_df[loadings_df["Product"] == prod]["Quantity"].sum())
                bv  = float(bdc_ld[bdc_ld["Product"] == prod]["Quantity"].sum())
                prod_bdc_sales = loadings_df[loadings_df["Product"] == prod].groupby("BDC")["Quantity"].sum().sort_values(ascending=False)
                prod_rank = list(prod_bdc_sales.index).index(selected_bdc) + 1 if selected_bdc in prod_bdc_sales.index else "N/A"
                rows.append({"Product": prod, "BDC Sales (LT)": f"{bv:,.0f}",
                              "Market Total (LT)": f"{mkt:,.0f}",
                              "Share (%)": f"{bv/mkt*100:.2f}" if mkt else "0.00",
                              "Rank": f"#{prod_rank}/{len(prod_bdc_sales)}"})
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════
# PAGE: STOCK TRANSACTION
# ══════════════════════════════════════════════════════════════
def show_stock_transaction():
    st.markdown("<h2>📈 STOCK TRANSACTION ANALYZER</h2>", unsafe_allow_html=True)
    st.markdown("---")

    if "stock_txn_df" not in st.session_state:
        st.session_state.stock_txn_df = pd.DataFrame()

    c1, c2 = st.columns(2)
    with c1:
        selected_bdc     = st.selectbox("BDC", sorted(BDC_MAP.keys()), key="txn_bdc")
        selected_product = st.selectbox("Product", PRODUCT_OPTIONS, key="txn_prod")
    with c2:
        selected_depot = st.selectbox("Depot", sorted(DEPOT_MAP.keys()), key="txn_depot")

    c3, c4 = st.columns(2)
    with c3:
        start_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=30), key="txn_start")
    with c4:
        end_date = st.date_input("End Date", value=datetime.now(), key="txn_end")

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
        with st.spinner("Fetching transaction PDF…"):
            pdf_bytes = _fetch_pdf(NPA_CONFIG["STOCK_TRANSACTION_URL"], params)
        if not pdf_bytes:
            st.error("❌ No PDF returned. Check credentials or date range.")
            st.session_state.stock_txn_df = pd.DataFrame()
        else:
            records = _parse_stock_transaction_pdf(pdf_bytes)
            if records:
                st.session_state.stock_txn_df = pd.DataFrame(records)
                st.session_state.txn_bdc     = selected_bdc
                st.session_state.txn_depot   = selected_depot
                st.session_state.txn_product = selected_product
                st.success(f"✅ {len(records)} transactions extracted.")
            else:
                st.warning("No transactions found for this selection.")
                st.session_state.stock_txn_df = pd.DataFrame()

    df = st.session_state.stock_txn_df
    if df.empty:
        st.info("👆 Configure and click FETCH.")
        return

    st.markdown(f"### {st.session_state.get('txn_bdc','')} — {st.session_state.get('txn_product','')}")
    inflows  = float(df[df["Description"].isin(["Custody Transfer In","Product Outturn"])]["Volume"].sum())
    outflows = float(df[df["Description"].isin(["Sale","Custody Transfer Out"])]["Volume"].sum())
    sales    = float(df[df["Description"] == "Sale"]["Volume"].sum())
    bdc_xfer = float(df[df["Description"] == "Custody Transfer Out"]["Volume"].sum())
    final_bal = float(df["Balance"].iloc[-1]) if len(df) else 0

    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("📥 Inflows (LT)",      f"{inflows:,.0f}")
    c2.metric("📤 Outflows (LT)",     f"{outflows:,.0f}")
    c3.metric("💰 Sales to OMCs",     f"{sales:,.0f}")
    c4.metric("🔄 BDC Transfers",     f"{bdc_xfer:,.0f}")
    c5.metric("📊 Final Balance",     f"{final_bal:,.0f}")

    st.markdown("### 📋 Transaction Breakdown")
    txn_sum = (df.groupby("Description")
               .agg(Total_Volume=("Volume","sum"), Count=("Trans #","count"))
               .reset_index().sort_values("Total_Volume", ascending=False))
    st.dataframe(txn_sum, use_container_width=True, hide_index=True)

    if sales > 0:
        st.markdown("### 🏢 Top Customers")
        cust = (df[df["Description"]=="Sale"].groupby("Account")["Volume"]
                .sum().sort_values(ascending=False).head(10).reset_index())
        cust.columns = ["Customer", "Volume (LT)"]
        st.dataframe(cust, use_container_width=True, hide_index=True)

    st.markdown("### 📄 Full History")
    st.dataframe(df, use_container_width=True, hide_index=True, height=400)

    excel_bytes = _to_excel_bytes({"Transactions": df, "Summary": txn_sum})
    st.download_button("⬇️ DOWNLOAD EXCEL", excel_bytes, "stock_transaction.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ══════════════════════════════════════════════════════════════
# PAGE: NATIONAL STOCKOUT
# ══════════════════════════════════════════════════════════════
def show_national_stockout():
    st.markdown("<h2>🌍 NATIONAL STOCKOUT FORECAST</h2>", unsafe_allow_html=True)
    st.markdown("---")

    c1, c2 = st.columns(2)
    with c1:
        start_date = st.date_input("From", value=datetime.now() - timedelta(days=30), key="ns_start")
    with c2:
        end_date = st.date_input("To", value=datetime.now(), key="ns_end")

    start_str   = start_date.strftime("%m/%d/%Y")
    end_str     = end_date.strftime("%m/%d/%Y")
    period_days = max((end_date - start_date).days, 1)

    day_type = st.radio("Day Type for Daily Rate",
                        ["📆 Calendar Days","💼 Business Days (Mon–Fri)"],
                        horizontal=True, key="ns_day_type")
    use_biz = "Business" in day_type

    depletion_mode = st.radio(
        "Depletion Rate",
        ["📊 Average Daily","🔥 Maximum Daily (stress test)","📊 Median Daily"],
        index=0, key="ns_depl_mode",
    )
    use_max    = "Maximum" in depletion_mode
    use_median = "Median"  in depletion_mode

    exclude_tor = st.checkbox("❌ Exclude TOR from LPG calculation", value=False, key="ns_excl_tor")

    _vessel_loaded = st.session_state.get("vessel_data") is not None and not st.session_state.get("vessel_data", pd.DataFrame()).empty
    include_vessels = st.checkbox("🚢 Include pending vessels in stock", value=False, key="ns_vessels",
                                  help="Requires data from Vessel Supply page.")
    if include_vessels and not _vessel_loaded:
        st.warning("No vessel data loaded. Go to 🚢 Vessel Supply first.")
        include_vessels = False

    all_bdc_names = sorted(BDC_USER_MAP.keys())
    st.markdown("---")

    if st.button("⚡ FETCH & ANALYSE NATIONAL FUEL SUPPLY", key="ns_go"):
        col_bal = "ACTUAL BALANCE (LT\\KG)"
        effective_days = _count_period_days(start_str, end_str, use_biz)
        day_lbl = f"{effective_days} {'business' if use_biz else 'calendar'} days"

        # ── Step 1: BDC Balance ──
        with st.status("📡 Step 1/2 — Fetching national BDC balance…", expanded=True) as s1:
            prog1 = st.progress(0)
            def _cb1(d,t): prog1.progress(d/t)
            records = fetch_all_bdc_balance(all_bdc_names, progress_cb=_cb1)
            bal_df  = pd.DataFrame(records)

            if exclude_tor:
                mask = bal_df["BDC"].str.contains("TOR", case=False, na=False) & (bal_df["Product"]=="LPG")
                excl_vol = bal_df[mask][col_bal].sum()
                bal_df = bal_df[~mask].copy()
                st.write(f"TOR LPG excluded: {excl_vol:,.0f} LT removed")

            balance_by_prod = bal_df.groupby("Product")[col_bal].sum()

            if include_vessels and _vessel_loaded:
                pend = st.session_state.vessel_data[st.session_state.vessel_data["Status"]=="PENDING"]
                if not pend.empty:
                    for prod, vol in pend.groupby("Product")["Quantity_Litres"].sum().items():
                        balance_by_prod[prod] = balance_by_prod.get(prod, 0) + vol

            s1.update(label=f"✅ Balance done — {bal_df['BDC'].nunique()} BDCs", state="running")

        # ── Step 2: OMC Loadings ──
        with st.status("🚚 Step 2/2 — Fetching national OMC loadings…", expanded=True) as s2:
            prog2 = st.progress(0)
            def _cb2(d,t): prog2.progress(d/t)
            omc_df = fetch_all_bdc_omc(all_bdc_names, start_str, end_str, progress_cb=_cb2)

            if omc_df.empty:
                omc_by_prod   = pd.Series({"PREMIUM":0.0,"GASOIL":0.0,"LPG":0.0})
                depl_lbl = "No Data"
            else:
                filt = omc_df[omc_df["Product"].isin(["PREMIUM","GASOIL","LPG"])].copy()
                filt["Date"] = pd.to_datetime(filt["Date"], errors="coerce")
                daily_agg = filt.groupby(["Date","Product"])["Quantity"].sum().reset_index()
                if use_median:
                    omc_by_prod  = daily_agg.groupby("Product")["Quantity"].median()
                    depl_lbl = "Median Daily"
                elif use_max:
                    omc_by_prod  = daily_agg.groupby("Product")["Quantity"].max()
                    depl_lbl = "Max Daily"
                else:
                    omc_by_prod  = filt.groupby("Product")["Quantity"].sum()
                    depl_lbl = f"Avg Daily ({day_lbl})"

            s2.update(label=f"✅ OMC done — {len(omc_df):,} records", state="complete")

        # ── Build forecast ──
        DISPLAY = {"PREMIUM":"PREMIUM (PMS)","GASOIL":"GASOIL (AGO)","LPG":"LPG"}
        rows_out = []
        for prod in ["PREMIUM","GASOIL","LPG"]:
            stock = float(balance_by_prod.get(prod, 0))
            dep   = float(omc_by_prod.get(prod, 0))
            daily = dep if (use_median or use_max) else (dep / effective_days if effective_days else 0)
            days  = stock / daily if daily > 0 else float("inf")
            rows_out.append({"product": prod, "display_name": DISPLAY[prod],
                             "total_balance": stock, "omc_sales": dep,
                             "daily_rate": daily, "days_remaining": days})

        forecast_df = pd.DataFrame(rows_out)
        bdc_pivot = (bal_df.pivot_table(index="BDC", columns="Product", values=col_bal,
                                         aggfunc="sum", fill_value=0).reset_index())
        for p in ["GASOIL","LPG","PREMIUM"]:
            if p not in bdc_pivot.columns: bdc_pivot[p] = 0
        bdc_pivot["TOTAL"] = bdc_pivot[["GASOIL","LPG","PREMIUM"]].sum(axis=1)
        nat_total = bdc_pivot["TOTAL"].sum()
        bdc_pivot["Market Share %"] = (bdc_pivot["TOTAL"] / nat_total * 100).round(2)
        bdc_pivot = bdc_pivot.sort_values("TOTAL", ascending=False)

        st.session_state.ns_results = {
            "forecast_df": forecast_df, "bal_df": bal_df, "omc_df": omc_df,
            "bdc_pivot": bdc_pivot, "period_days": period_days,
            "effective_days": effective_days, "use_biz": use_biz,
            "day_lbl": day_lbl, "depl_lbl": depl_lbl,
            "start_str": start_str, "end_str": end_str,
        }
        _save_national_snapshot(forecast_df, f"{period_days}d")
        st.success("✅ Analysis complete! Scroll down.")
        st.rerun()

    if not st.session_state.get("ns_results"):
        st.info("👆 Configure options and click FETCH.")
        return

    # ── Display results ──
    res         = st.session_state.ns_results
    forecast_df = res["forecast_df"]
    bdc_pivot   = res["bdc_pivot"]
    omc_df      = res["omc_df"]
    depl_lbl    = res["depl_lbl"]
    day_lbl     = res["day_lbl"]

    st.markdown("---")
    st.markdown(f"<h3>🇬🇭 NATIONAL FUEL SUPPLY — {res['start_str']} → {res['end_str']}</h3>", unsafe_allow_html=True)
    st.caption(f"Balance: {res['bal_df']['BDC'].nunique()} BDCs | "
               f"OMC loadings: {len(omc_df):,} records | "
               f"Depletion: {depl_lbl} | Day type: {day_lbl}")

    ICONS  = {"PREMIUM":"⛽","GASOIL":"🚛","LPG":"🔵"}
    COLORS = {"PREMIUM":"#00ffff","GASOIL":"#ffaa00","LPG":"#00ff88"}

    cols = st.columns(3)
    for col, (_, row) in zip(cols, forecast_df.iterrows()):
        days  = row["days_remaining"]
        prod  = row["product"]
        color = COLORS.get(prod,"#fff")
        days_txt  = f"{days:.1f}" if days != float("inf") else "∞"
        weeks_txt = f"(~{days/7:.1f} wks)" if days != float("inf") else ""
        if days < 7:   border, status = "#ff0000","🔴 CRITICAL"
        elif days < 14: border, status = "#ffaa00","🟡 WARNING"
        elif days < 30: border, status = "#ff6600","🟠 MONITOR"
        else:           border, status = "#00ff88","🟢 HEALTHY"
        stockout = (datetime.now() + timedelta(days=days)).strftime("%d %b %Y") if days != float("inf") else "N/A"
        with col:
            st.markdown(f"""
            <div style='background:rgba(10,14,39,0.85);padding:22px 14px;border-radius:16px;
                        border:2.5px solid {border};text-align:center;box-shadow:0 0 18px {border}55;'>
                <div style='font-size:34px;'>{ICONS.get(prod,"🛢")}</div>
                <div style='font-family:Orbitron,sans-serif;color:{color};font-size:17px;font-weight:700;margin:6px 0;'>
                    {row["display_name"]}</div>
                <div style='font-family:Orbitron,sans-serif;font-size:52px;color:{border};font-weight:900;line-height:1;'>
                    {days_txt}</div>
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
        if days < 7:   status = "🔴 CRITICAL"
        elif days < 14: status = "🟡 WARNING"
        elif days < 30: status = "🟠 MONITOR"
        else:           status = "🟢 HEALTHY"
        sum_rows.append({
            "Product":                row["display_name"],
            "Stock (LT)":             f"{row['total_balance']:,.0f}",
            f"{depl_lbl} (LT)":       f"{row['omc_sales']:,.0f}",
            f"Daily Rate ({day_lbl})": f"{row['daily_rate']:,.0f}",
            "Days of Supply":          f"{days:.1f}" if days != float("inf") else "∞",
            "Est. Empty":             (datetime.now()+timedelta(days=days)).strftime("%Y-%m-%d") if days!=float("inf") else "N/A",
            "Status":                  status,
        })
    st.dataframe(pd.DataFrame(sum_rows), use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("### 🏦 STOCK BY BDC")
    disp = bdc_pivot.copy()
    for c in ["GASOIL","LPG","PREMIUM","TOTAL"]:
        disp[c] = disp[c].apply(lambda x: f"{x:,.0f}")
    disp["Market Share %"] = disp["Market Share %"].apply(lambda x: f"{x:.2f}%")
    st.dataframe(disp, use_container_width=True, hide_index=True)

    st.markdown("---")
    excel_bytes = _to_excel_bytes({
        "Stockout Forecast": pd.DataFrame(sum_rows),
        "Stock by BDC": bdc_pivot,
        **({} if omc_df.empty else {"OMC Loadings": omc_df}),
    })
    st.download_button("⬇️ DOWNLOAD NATIONAL REPORT", excel_bytes, "national_stockout.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ══════════════════════════════════════════════════════════════
# PAGE: WORLD RISK MONITOR
# ══════════════════════════════════════════════════════════════
def show_world_monitor():
    st.markdown("<h2>🌍 WORLD RISK MONITOR</h2>", unsafe_allow_html=True)
    st.info("🔴 LIVE GLOBAL INTELLIGENCE: Real-time conflicts, military bases, nuclear sites, sanctions, "
            "weather, economic indicators, waterways, power outages, natural disasters & Iran attacks. "
            "Fully interactive map — 7-day view.")
    st.caption("Source: " + WORLD_MONITOR_URL.split("?")[0])

    st.markdown("""
    <div style='background:rgba(22,33,62,0.6);padding:40px;border-radius:15px;
                border:2px solid #00ffff;text-align:center;margin:20px 0;'>
        <div style='font-size:80px;margin-bottom:20px;'>🌍</div>
        <h3 style='color:#00ffff;margin:0;'>WORLD RISK MONITOR</h3>
        <p style='color:#888;margin:10px 0 20px;'>
            Real-time global intelligence powered by AI &amp; 100+ OSINT feeds.<br>
            25 data layers: conflicts, nuclear, military, sanctions, weather,<br>
            infrastructure, satellites &amp; more.
        </p>
    </div>""", unsafe_allow_html=True)
    st.link_button("🌍 OPEN WORLD RISK MONITOR", WORLD_MONITOR_URL, use_container_width=True)
    st.caption("Opens in a new tab with pre-configured satellite view, layers & 7-day window.")


# ══════════════════════════════════════════════════════════════
# PAGE: VESSEL SUPPLY  (unchanged from original)
# ══════════════════════════════════════════════════════════════
def show_vessel_supply():
    VCOLS  = {"PREMIUM":"#00ffff","GASOIL":"#ffaa00","LPG":"#00ff88","NAPHTHA":"#ff6600"}
    VICONS = {"PREMIUM":"⛽","GASOIL":"🚛","LPG":"🔵","NAPHTHA":"🟠"}
    MONTH_ORDER = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]

    st.markdown("<h2>🚢 VESSEL SUPPLY TRACKER</h2>", unsafe_allow_html=True)
    st.markdown("---")

    col1, col2 = st.columns([3,1])
    with col1:
        sheet_url = st.text_input("Google Sheets URL", value=VESSEL_SHEET_URL, key="vessel_url")
    with col2:
        year_sel = st.selectbox("Data Year", ["2025","2024","2026"], key="vessel_year_sel")

    if st.button("🔄 FETCH VESSEL DATA", key="vessel_fetch"):
        with st.spinner("Loading from Google Sheets…"):
            raw_df, err = _load_vessel_sheet(sheet_url)
            if raw_df is None:
                st.error(err)
                return
            processed = _process_vessel_df(raw_df, year=year_sel)
            if processed.empty:
                st.warning("No valid records found.")
                return
            st.session_state.vessel_data = processed
            st.session_state["vessel_year"] = year_sel
            st.success(f"✅ {len(processed)} vessel records loaded.")
            st.rerun()

    if not st.session_state.get("vessel_data") is not None and not st.session_state.get("vessel_data", pd.DataFrame()).empty:
        df = st.session_state.get("vessel_data", pd.DataFrame())
        if df.empty:
            st.info("👆 Click FETCH VESSEL DATA.")
            return
    else:
        df = st.session_state.get("vessel_data", pd.DataFrame())
        if df is None or df.empty:
            st.info("👆 Click FETCH VESSEL DATA.")
            return

    yr_lbl     = st.session_state.get("vessel_year","2025")
    discharged = df[df["Status"]=="DISCHARGED"]
    pending    = df[df["Status"]=="PENDING"]

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Total Vessels", len(df))
    c2.metric("Discharged", f"{len(discharged)} ({discharged['Quantity_Litres'].sum()/1e6:.2f}M LT)")
    c3.metric("Pending", f"{len(pending)} ({pending['Quantity_Litres'].sum()/1e6:.2f}M LT)")
    c4.metric("Total Volume", f"{df['Quantity_Litres'].sum()/1e6:.2f}M LT")

    st.markdown("---")
    st.markdown("### ⏳ PENDING VESSELS")
    if pending.empty:
        st.success("No pending vessels — all recorded vessels discharged.")
    else:
        pend_prod = pending.groupby("Product").agg(Vessels=("Vessel_Name","count"),
                                                    Volume_LT=("Quantity_Litres","sum"),
                                                    Volume_MT=("Quantity_MT","sum")).reset_index()
        pcols = st.columns(min(len(pend_prod),4))
        for col,(_, row) in zip(pcols, pend_prod.iterrows()):
            with col:
                prod  = row["Product"]
                color = VCOLS.get(prod,"#fff")
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
            monthly["Month"] = pd.Categorical(monthly["Month"], categories=MONTH_ORDER, ordered=True)
            monthly = monthly.sort_values("Month")
            fig = go.Figure()
            for prod in monthly["Product"].unique():
                pd_ = monthly[monthly["Product"]==prod]
                fig.add_trace(go.Bar(name=prod, x=pd_["Month"], y=pd_["Quantity_Litres"],
                                     marker_color=VCOLS.get(prod,"#fff")))
            fig.update_layout(barmode="group", paper_bgcolor="rgba(10,14,39,0.9)",
                              plot_bgcolor="rgba(10,14,39,0.9)", font=dict(color="white"), height=380)
            st.plotly_chart(fig, use_container_width=True)
    with tab2:
        if not discharged.empty:
            st.dataframe(discharged[["Vessel_Name","Vessel_Type","Receivers","Supplier",
                                     "Product","Quantity_MT","Quantity_Litres","Date_Discharged","Month"]],
                         use_container_width=True, hide_index=True)

    st.markdown("---")
    all_sheets = {"All Vessels": df, "Discharged": discharged, "Pending": pending}
    excel_bytes = _to_excel_bytes(all_sheets)
    st.download_button("⬇️ DOWNLOAD VESSEL EXCEL", excel_bytes, f"vessel_data_{yr_lbl}.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
def main():
    st.markdown("""
    <div style='text-align:center;padding:30px 0;'>
        <h1 style='font-size:60px;margin:0;'>⚡ NPA ENERGY ANALYTICS ⚡</h1>
        <p style='font-size:20px;color:#ff00ff;font-family:"Orbitron",sans-serif;
                   letter-spacing:3px;margin-top:10px;'>FUEL THE FUTURE WITH DATA</p>
    </div>""", unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("<h2 style='text-align:center;'>🎯 MISSION CONTROL</h2>", unsafe_allow_html=True)
        choice = st.radio("SELECT YOUR DATA MISSION:", [
            "🏦 BDC BALANCE",
            "🚚 OMC LOADINGS",
            "📅 DAILY ORDERS",
            "📊 MARKET SHARE",
            "📈 STOCK TRANSACTION",
            "🌍 NATIONAL STOCKOUT",
            "🌍 WORLD RISK MONITOR",
            "🚢 VESSEL SUPPLY",
        ], index=0)

        st.markdown("---")
        n_bdcs = len(BDC_USER_MAP)
        st.markdown(f"""
        <div style='text-align:center;padding:15px;background:rgba(255,0,255,0.1);
                    border-radius:10px;border:2px solid #ff00ff;'>
            <h3>⚙️ SYSTEM STATUS</h3>
            <p style='color:#00ff88;font-size:18px;'>🟢 OPERATIONAL</p>
            <p style='color:#888;font-size:12px;'>{n_bdcs} BDCs configured</p>
        </div>""", unsafe_allow_html=True)

    if choice == "🏦 BDC BALANCE":
        show_bdc_balance()
    elif choice == "🚚 OMC LOADINGS":
        show_omc_loadings()
    elif choice == "📅 DAILY ORDERS":
        show_daily_orders()
    elif choice == "📊 MARKET SHARE":
        show_market_share()
    elif choice == "📈 STOCK TRANSACTION":
        show_stock_transaction()
    elif choice == "🌍 NATIONAL STOCKOUT":
        show_national_stockout()
    elif choice == "🌍 WORLD RISK MONITOR":
        show_world_monitor()
    elif choice == "🚢 VESSEL SUPPLY":
        show_vessel_supply()


main()