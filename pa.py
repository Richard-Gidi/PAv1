"""
NPA ENERGY ANALYTICS — Streamlit Dashboard
===========================================
Install:
    pip install streamlit pandas pdfplumber PyPDF2 openpyxl python-dotenv plotly requests streamlit-js-eval

Run:
    streamlit run npa_dashboard.py
"""

# ── Standard library ──────────────────────────────────────────────────────────
import os
import re
import io
import json
import shutil
import concurrent.futures
from datetime import datetime, timedelta
from math import ceil

# ── Third-party ───────────────────────────────────────────────────────────────
import requests
import pandas as pd
import pdfplumber
import PyPDF2
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

load_dotenv()


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default)


NPA_CONFIG = {
    "COMPANY_ID":       _env("NPA_COMPANY_ID", "1"),
    "USER_ID":          _env("NPA_USER_ID", "123292"),
    "APP_ID":           _env("NPA_APP_ID", "3"),
    "ITS_FROM_PERSOL":  _env("NPA_ITS_FROM_PERSOL", "Persol Systems Limited"),
    "BDC_BALANCE_URL":  _env("NPA_BDC_BALANCE_URL",
                              "https://iml.npa-enterprise.com/NPAAPILIVE/Home/CreateProductBalance"),
    "OMC_LOADINGS_URL": _env("NPA_OMC_LOADINGS_URL",
                              "https://iml.npa-enterprise.com/NewNPA/home/CreateOrdersReport"),
    "DAILY_ORDERS_URL": _env("NPA_DAILY_ORDERS_URL",
                              "https://iml.npa-enterprise.com/NewNPA/home/CreateDailyOrderReport"),
    "STOCK_TRANSACTION_URL": _env("NPA_STOCK_TRANSACTION_URL",
                                   "https://iml.npa-enterprise.com/NewNPA/home/CreateStockTransactionReport"),
    "OMC_NAME":         _env("OMC_NAME", "OILCORP ENERGIA LIMITED"),
}

WORLD_MONITOR_URL = _env(
    "WORLD_MONITOR_URL",
    "https://www.worldmonitor.app/?lat=20.0000&lon=0.0000&zoom=1.00&view=global"
    "&timeRange=7d&layers=conflicts%2Cbases%2Chotspots%2Cnuclear%2Csanctions"
    "%2Cweather%2Ceconomic%2Cwaterways%2Coutages%2Cmilitary%2Cnatural%2CiranAttacks",
)

SNAPSHOT_DIR = os.path.join(os.getcwd(), "national_snapshots")

# Product identifiers
PRODUCT_OPTIONS   = ["PMS", "Gasoil", "LPG"]
PRODUCT_BALANCE_MAP = {"PMS": "PREMIUM", "Gasoil": "GASOIL", "LPG": "LPG"}

# Colour / icon palettes used across pages
PRODUCT_COLORS = {"PREMIUM": "#00ffff", "GASOIL": "#ffaa00", "LPG": "#00ff88"}
PRODUCT_ICONS  = {"PREMIUM": "⛽",       "GASOIL": "🚛",      "LPG": "🔵"}
PRODUCT_LABELS = {"PREMIUM": "PREMIUM (PMS)", "GASOIL": "GASOIL (AGO)", "LPG": "LPG"}


# ── ID mappings from .env ─────────────────────────────────────────────────────

_BDC_NAME_FIXES = {
    "TEMA OIL REFINERY TOR":              "TEMA OIL REFINERY (TOR)",
    "SOCIETE NATIONAL BURKINABE SONABHY": "SOCIETE NATIONAL BURKINABE (SONABHY)",
    "LIB GHANA LIMITED":                  "L.I.B. GHANA LIMITED",
    "C CLEANED OIL LTD":                  "C. CLEANED OIL LTD",
    "PK JEGS ENERGY LTD":                 "P. K JEGS ENERGY LTD",
}

_DEPOT_NAME_FIXES = {
    "GHANA OIL COLTD TAKORADI":                 "GHANA OIL CO.LTD, TAKORADI",
    "GOIL LPG BOTTLING PLANT TEMA":             "GOIL LPG BOTTLING PLANT -TEMA",
    "GOIL LPG BOTTLING PLANT KUMASI":           "GOIL LPG BOTTLING PLANT- KUMASI",
    "NEWGAS CYLINDER BOTTLING LIMITED TEMA":    "NEWGAS CYLINDER BOTTLING LIMITED-TEMA",
    "CHASE PETROLEUM TEMA":                     "CHASE PETROLEUM - TEMA",
    "TEMA FUEL COMPANY TFC":                    "TEMA FUEL COMPANY (TFC)",
    "TEMA MULTI PRODUCTS TMPT":                 "TEMA MULTI PRODUCTS (TMPT)",
    "TEMA OIL REFINERY TOR":                    "TEMA OIL REFINERY (TOR)",
    "GHANA OIL COMPANY LTD SEKONDI NAVAL BASE": "GHANA OIL COMPANY LTD (SEKONDI NAVAL BASE)",
    "GHANSTOCK LIMITED TAKORADI":               "GHANSTOCK LIMITED (TAKORADI)",
}


def _apply_name_fixes(name: str, fixes: dict) -> str:
    return fixes.get(name, name)


def load_bdc_mappings() -> dict:
    out = {}
    for key, val in os.environ.items():
        if key.startswith("BDC_"):
            name = _apply_name_fixes(key[4:].replace("_", " "), _BDC_NAME_FIXES)
            out[name] = int(val)
    return out


def load_depot_mappings() -> dict:
    out = {}
    for key, val in os.environ.items():
        if not key.startswith("DEPOT_"):
            continue
        name = key[6:].replace("_", " ")
        if "BOST " in name and name != "BOST GLOBAL DEPOT":
            parts = name.split(" ", 1)
            name = f"{parts[0]} - {parts[1]}" if len(parts) == 2 else name
        elif name.endswith(" TEMA") and "SENTUO" in name:
            name = name.replace(" TEMA", "- TEMA")
        elif "BLUE_OCEAN_INVESTMENT_LTD_KOTOKA_AIRPORT_ATK" in key:
            name = "BLUE OCEAN INVESTMENT LTD-KOTOKA AIRPORT (ATK)"
        else:
            name = _apply_name_fixes(name, _DEPOT_NAME_FIXES)
        out[name] = int(val)
    return out


def load_product_mappings() -> dict:
    return {
        "PMS":    int(_env("PRODUCT_PREMIUM_ID", "12")),
        "Gasoil": int(_env("PRODUCT_GASOIL_ID",  "14")),
        "LPG":    int(_env("PRODUCT_LPG_ID",      "28")),
    }


BDC_MAP          = load_bdc_mappings()
DEPOT_MAP        = load_depot_mappings()
STOCK_PRODUCT_MAP = load_product_mappings()


# ═══════════════════════════════════════════════════════════════════════════════
# STREAMLIT PAGE CONFIG & CSS
# ═══════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="NPA Energy Analytics 🛢️",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900
    &family=Rajdhani:wght@300;500;700&display=swap');

/* Background */
.stApp {
    background: linear-gradient(-45deg,#0a0e27,#1a1a2e,#16213e,#0f3460);
    background-size:400% 400%;
    animation:gradientShift 15s ease infinite;
}
@keyframes gradientShift{0%{background-position:0% 50%}50%{background-position:100% 50%}100%{background-position:0% 50%}}

/* Headings */
h1,h2,h3{
    font-family:'Orbitron',sans-serif!important;
    color:#00ffff!important;
    animation:glow 2s ease-in-out infinite alternate;
}
@keyframes glow{
    from{text-shadow:0 0 5px #00ffff,0 0 10px #00ffff,0 0 15px #00ffff}
    to  {text-shadow:0 0 10px #00ffff,0 0 20px #00ffff,0 0 30px #00ffff,0 0 40px #0ff}
}

/* Sidebar */
[data-testid="stSidebar"]{
    background:linear-gradient(180deg,#0a0e27 0%,#16213e 100%);
    border-right:2px solid #00ffff;
    box-shadow:5px 0 15px rgba(0,255,255,0.3);
}
[data-testid="stSidebar"] h1,[data-testid="stSidebar"] h2,[data-testid="stSidebar"] h3{
    color:#ff00ff!important;
    text-shadow:0 0 10px #ff00ff;
}

/* Buttons */
.stButton>button{
    background:linear-gradient(45deg,#ff00ff,#00ffff);
    color:white;border:2px solid #00ffff;border-radius:25px;
    padding:15px 30px;font-family:'Orbitron',sans-serif;
    font-weight:700;font-size:18px;
    box-shadow:0 0 20px rgba(0,255,255,0.5);
    transition:all 0.3s ease;text-transform:uppercase;letter-spacing:2px;
}
.stButton>button:hover{
    transform:scale(1.05) translateY(-3px);
    box-shadow:0 0 30px rgba(0,255,255,0.8),0 0 40px rgba(255,0,255,0.5);
    background:linear-gradient(45deg,#00ffff,#ff00ff);
}

/* Tables */
.dataframe{background-color:rgba(10,14,39,0.8)!important;border:2px solid #00ffff!important;border-radius:10px;box-shadow:0 0 20px rgba(0,255,255,0.3)}
.dataframe th{background-color:#16213e!important;color:#00ffff!important;font-family:'Orbitron',sans-serif;text-transform:uppercase;border:1px solid #00ffff!important}
.dataframe td{background-color:rgba(22,33,62,0.6)!important;color:#ffffff!important;border:1px solid rgba(0,255,255,0.2)!important}

/* Metrics */
[data-testid="stMetricValue"]{font-family:'Orbitron',sans-serif;font-size:28px!important;color:#00ffff!important;text-shadow:0 0 15px #00ffff}
[data-testid="stMetricLabel"]{font-family:'Rajdhani',sans-serif;color:#ff00ff!important;font-weight:700;text-transform:uppercase;letter-spacing:2px}

/* Metric card */
.metric-card{background:rgba(22,33,62,0.6);padding:20px;border-radius:15px;border:2px solid #00ffff;text-align:center}
.metric-card h2{color:#ff00ff!important;margin:0;font-size:20px!important}
.metric-card h1{color:#00ffff!important;margin:10px 0;font-size:32px!important;word-wrap:break-word}

p,span,div{font-family:'Rajdhani',sans-serif;color:#e0e0e0}
[data-testid="stFileUploader"]{border:2px dashed #00ffff;border-radius:15px;background:rgba(22,33,62,0.3);padding:20px}
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# UI HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def metric_card(label: str, value: str, unit: str = "") -> str:
    unit_html = f"<p style='color:#888;font-size:14px;margin:0;'>{unit}</p>" if unit else ""
    return f"""
    <div class='metric-card'>
        <h2>{label}</h2>
        <h1>{value}</h1>
        {unit_html}
    </div>"""


def section(title: str):
    st.markdown(f"<h3>{title}</h3>", unsafe_allow_html=True)
    st.markdown("---")


def page_header(title: str, subtitle: str = ""):
    st.markdown(f"<h2>{title}</h2>", unsafe_allow_html=True)
    if subtitle:
        st.info(subtitle)
    st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════════
# HTTP / PDF HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,text/html,*/*;q=0.8",
}


def fetch_pdf_bytes(url: str, params: dict, timeout: int = 45) -> bytes | None:
    """GET `url` with `params`; return raw bytes only if the response is a PDF."""
    try:
        r = requests.get(url, params=params, headers=_HTTP_HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.content if r.content[:4] == b"%PDF" else None
    except Exception:
        return None


def fetch_pdf_or_error(url: str, params: dict, label: str = "PDF") -> bytes | None:
    """Like fetch_pdf_bytes but shows Streamlit error messages on failure."""
    try:
        r = requests.get(url, params=params, headers=_HTTP_HEADERS, timeout=30)
        r.raise_for_status()
        if r.content[:4] != b"%PDF":
            st.error(f"❌ Response is not a {label}.")
            return None
        st.success(f"✅ {label} received ({len(r.content):,} bytes)")
        return r.content
    except requests.exceptions.RequestException as exc:
        st.error(f"❌ Network Error: {exc}")
        return None
    except Exception as exc:
        import traceback
        st.error(f"❌ Error: {exc}")
        st.code(traceback.format_exc())
        return None


def excel_download_button(df_or_path, filename: str, sheets: dict | None = None, label: str = "⬇️ DOWNLOAD EXCEL"):
    """Render a Streamlit download button for an Excel file."""
    if isinstance(df_or_path, str):
        if not os.path.exists(df_or_path):
            return
        with open(df_or_path, "rb") as f:
            data = f.read()
    else:
        buf = io.BytesIO()
        if sheets:
            with pd.ExcelWriter(buf, engine="openpyxl") as w:
                for sheet, df in sheets.items():
                    df.to_excel(w, sheet_name=sheet, index=False)
        else:
            df_or_path.to_excel(buf, index=False, engine="openpyxl")
        data = buf.getvalue()
    st.download_button(
        label, data, filename,
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# BDC BALANCE — SCRAPER
# ═══════════════════════════════════════════════════════════════════════════════

class StockBalanceScraper:
    ALLOWED_PRODUCTS = {"PREMIUM", "GASOIL", "LPG"}

    def __init__(self):
        self.output_dir = os.path.join(os.getcwd(), "bdc_stock_dataset")
        os.makedirs(self.output_dir, exist_ok=True)
        prod_re = "|".join(sorted(self.ALLOWED_PRODUCTS))
        self._product_re = re.compile(
            rf"^({prod_re})\s+([\d,]+\.\d{{2}})\s+(-?[\d,]+\.\d{{2}})$",
            flags=re.IGNORECASE,
        )
        self._bost_global_re = re.compile(r"\bBOST\s*GLOBAL\s*DEPOT\b", flags=re.IGNORECASE)

    @staticmethod
    def _norm(text: str) -> str:
        return re.sub(r"\s+", " ", (text or "").strip())

    def _norm_bdc(self, bdc: str) -> str:
        clean = self._norm(bdc)
        up = re.sub(r"\s+", " ", clean.upper().replace("-", " ").replace("_", " "))
        return "BOST" if up.startswith("BOST") else clean

    def _is_bost_depot(self, depot: str) -> bool:
        d = re.sub(r"\s+", " ", self._norm(depot).replace("-", " "))
        return d.upper().startswith("BOST ")

    def _is_bost_global(self, depot: str) -> bool:
        return bool(self._bost_global_re.search(self._norm(depot)))

    @staticmethod
    def _parse_date(line: str) -> str | None:
        m = re.search(r"(\w+\s+\d{1,2}\s*,\s*\d{4})", line)
        if m:
            return datetime.strptime(
                m.group(1).replace(" ,", ","), "%B %d, %Y"
            ).strftime("%Y/%m/%d")
        return None

    def _append(self, records, date, bdc, depot, product, actual, available):
        product = (product or "").upper()
        if product not in self.ALLOWED_PRODUCTS:
            return
        if self._is_bost_depot(depot) and not self._is_bost_global(depot):
            return
        if actual <= 0:
            return
        records.append({
            "Date":                         date,
            "BDC":                          self._norm_bdc(bdc),
            "DEPOT":                        self._norm(depot),
            "Product":                      product,
            "ACTUAL BALANCE (LT\\KG)":      actual,
            "AVAILABLE BALANCE (LT\\KG)":   available,
        })

    def _process_lines(self, lines, records):
        date = bdc = depot = None
        for line in lines:
            up = line.upper()
            if "DATE AS AT" in up:
                date = self._parse_date(line) or date
            elif up.startswith(("BDC :", "BDC:")):
                bdc = re.sub(r"^BDC\s*:\s*", "", line, flags=re.IGNORECASE).strip()
            elif up.startswith(("DEPOT :", "DEPOT:")):
                depot = re.sub(r"^DEPOT\s*:\s*", "", line, flags=re.IGNORECASE).strip()
            elif bdc and depot and date:
                m = self._product_re.match(line)
                if m:
                    self._append(
                        records, date, bdc, depot, m.group(1),
                        float(m.group(2).replace(",", "")),
                        float(m.group(3).replace(",", "")),
                    )

    def parse_pdf_file(self, pdf_file) -> list:
        records = []
        try:
            for page in PyPDF2.PdfReader(pdf_file).pages:
                text = page.extract_text() or ""
                lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
                self._process_lines(lines, records)
        except Exception as e:
            st.error(f"Error parsing PDF: {e}")
        return records

    def save_to_excel(self, records, filename: str | None = None) -> str | None:
        if not records:
            return None
        filename = filename or f"stock_balance_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
        out_path = os.path.join(self.output_dir, os.path.basename(filename))
        df = pd.DataFrame(records).sort_values(
            ["Product", "BDC", "DEPOT", "Date"], ignore_index=True
        )
        with pd.ExcelWriter(out_path, engine="openpyxl") as w:
            df.to_excel(w, index=False, sheet_name="Stock Balance")
            for prod in ["LPG", "PREMIUM", "GASOIL"]:
                sub = df[df["Product"].str.upper() == prod]
                (sub if not sub.empty else pd.DataFrame(columns=df.columns)).to_excel(
                    w, index=False, sheet_name=prod
                )
        return out_path


# ── Fetch helper used by multiple pages ───────────────────────────────────────

def _bdc_balance_params() -> dict:
    cfg = NPA_CONFIG
    return {
        "lngCompanyId":    cfg["COMPANY_ID"],
        "strITSfromPersol": cfg["ITS_FROM_PERSOL"],
        "strGroupBy":      "BDC",
        "strGroupBy1":     "DEPOT",
        "strQuery1": "", "strQuery2": "", "strQuery3": "", "strQuery4": "",
        "strPicHeight": "1", "szPicWeight": "1",
        "lngUserId":       cfg["USER_ID"],
        "intAppId":        cfg["APP_ID"],
    }


def fetch_and_store_bdc_balance(spinner_label: str = "Fetching BDC Balance…") -> bool:
    """Fetch BDC balance, parse, store in session_state. Returns True on success."""
    with st.spinner(spinner_label):
        pdf = fetch_pdf_bytes(NPA_CONFIG["BDC_BALANCE_URL"], _bdc_balance_params())
        if not pdf:
            st.error("❌ Could not fetch BDC Balance PDF.")
            return False
        st.session_state.bdc_records = StockBalanceScraper().parse_pdf_file(io.BytesIO(pdf))
        return bool(st.session_state.bdc_records)


# ═══════════════════════════════════════════════════════════════════════════════
# OMC LOADINGS — PARSER
# ═══════════════════════════════════════════════════════════════════════════════

_PRODUCT_MAP     = {"AGO": "GASOIL", "PMS": "PREMIUM", "LPG": "LPG"}
_HEADER_KEYWORDS = [
    "ORDER REPORT", "National Petroleum Authority", "ORDER NUMBER", "ORDER DATE",
    "ORDER STATUS", "BDC:", "Total for :", "Printed By :", "Page ",
    "BRV NUMBER", "VOLUME",
]
_LOADED_KEYWORDS = {"Released", "Submitted"}
_OMC_COLUMNS     = ["Date", "OMC", "Truck", "Product", "Quantity", "Price",
                     "Depot", "Order Number", "BDC"]


def _is_header(line: str) -> bool:
    return any(h in line for h in _HEADER_KEYWORDS)


def _extract_depot(line: str) -> str | None:
    m = re.search(r"DEPOT:([^-\n]+)", line)
    return m.group(1).strip() if m else None


def _extract_bdc_name(line: str) -> str | None:
    m = re.search(r"BDC:([^\n]+)", line)
    return m.group(1).strip() if m else None


def _detect_product(line: str) -> str:
    if "AGO" in line:  return _PRODUCT_MAP["AGO"]
    if "LPG" in line:  return _PRODUCT_MAP["LPG"]
    return _PRODUCT_MAP["PMS"]


def _parse_loaded_line(line: str, product: str, depot: str, bdc: str) -> dict | None:
    tokens = line.split()
    if len(tokens) < 6:
        return None
    rel_idx = next((i for i, t in enumerate(tokens) if t in _LOADED_KEYWORDS), None)
    if rel_idx is None or rel_idx < 2:
        return None
    try:
        volume = float(tokens[-1].replace(",", ""))
        price  = float(tokens[-2].replace(",", ""))
        brv    = tokens[-3]
        company = " ".join(tokens[rel_idx + 1:-3]).strip()
        try:
            date_str = datetime.strptime(tokens[0], "%d-%b-%Y").strftime("%Y/%m/%d")
        except ValueError:
            date_str = tokens[0]
        return {
            "Date": date_str, "OMC": company, "Truck": brv,
            "Product": product, "Quantity": volume, "Price": price,
            "Depot": depot, "Order Number": tokens[1], "BDC": bdc,
        }
    except Exception:
        return None


def _parse_omc_text(lines: list[str]) -> pd.DataFrame:
    rows, depot, bdc, product = [], "", "", _PRODUCT_MAP["PMS"]
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        if "DEPOT:" in line:
            depot = _extract_depot(line) or depot
        elif "BDC:" in line:
            bdc = _extract_bdc_name(line) or bdc
        elif "PRODUCT" in line:
            product = _detect_product(line)
        elif _is_header(line):
            continue
        elif any(kw in line for kw in _LOADED_KEYWORDS):
            row = _parse_loaded_line(line, product, depot, bdc)
            if row:
                rows.append(row)
    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=_OMC_COLUMNS)
    for col in _OMC_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[_OMC_COLUMNS].drop_duplicates()
    try:
        df = df.assign(_ds=pd.to_datetime(df["Date"], format="%Y/%m/%d", errors="coerce")) \
               .sort_values("_ds").drop(columns=["_ds"]).reset_index(drop=True)
    except Exception:
        pass
    return df


def extract_npa_data_from_pdf(pdf_file) -> pd.DataFrame:
    try:
        with pdfplumber.open(pdf_file) as pdf:
            lines = []
            for page in pdf.pages:
                text = page.extract_text() or page.extract_text(x_tolerance=2, y_tolerance=2)
                if text:
                    lines.extend(text.split("\n"))
        return _parse_omc_text(lines)
    except Exception as e:
        st.error(f"Error parsing PDF: {e}")
        return pd.DataFrame(columns=_OMC_COLUMNS)


def save_omc_excel(df: pd.DataFrame, filename: str | None = None) -> str:
    out_dir = os.path.join(os.getcwd(), "omc_loadings")
    os.makedirs(out_dir, exist_ok=True)
    filename = filename or f"npa_orders_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    out_path = os.path.join(out_dir, filename)
    filtered = df[df["Product"].isin(["PREMIUM", "GASOIL", "LPG"])].copy()
    pivot = (
        filtered.pivot_table(index="BDC", columns="Product", values="Quantity",
                              aggfunc="sum", fill_value=0.0).reset_index()
        if not filtered.empty else pd.DataFrame(columns=["BDC", "GASOIL", "LPG", "PREMIUM"])
    )
    for p in ["GASOIL", "LPG", "PREMIUM"]:
        if p not in pivot.columns:
            pivot[p] = 0.0
    pivot["Total"] = pivot[["GASOIL", "LPG", "PREMIUM"]].sum(axis=1)
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="All Orders", index=False)
        for prod in ["PREMIUM", "GASOIL", "LPG"]:
            df[df["Product"] == prod].to_excel(w, sheet_name=prod, index=False)
        pivot.to_excel(w, sheet_name="BDC Summary", index=False)
    return out_path


# ═══════════════════════════════════════════════════════════════════════════════
# DAILY ORDERS — PARSER
# ═══════════════════════════════════════════════════════════════════════════════

def _clean_currency(s: str) -> float:
    try:
        return float((s or "").replace(",", "").strip())
    except ValueError:
        return 0.0


def _get_product_category(text: str) -> str:
    t = text.upper()
    if "AVIATION" in t or "TURBINE" in t: return "ATK"
    if "RFO"     in t:                    return "RFO"
    if "PREMIX"  in t:                    return "PREMIX"
    if "LPG"     in t:                    return "LPG"
    if "AGO" in t or "MGO" in t or "GASOIL" in t: return "GASOIL"
    return "PREMIUM"


def _parse_daily_line(line: str, last_known_date: str | None) -> dict | None:
    line = line.strip()
    pv = re.search(r"(\d{1,4}\.\d{2,4})\s+(\d{1,3}(?:,\d{3})*\.\d{2})$", line)
    if not pv:
        return None
    price  = _clean_currency(pv.group(1))
    volume = _clean_currency(pv.group(2))
    rem    = line[:pv.start()].strip()
    tokens = rem.split()
    if not tokens:
        return None
    brv, tokens = tokens[-1], tokens[:-1]
    rem = " ".join(tokens)
    date_val = last_known_date
    dm = re.search(r"(\d{2}/\d{2}/\d{4})", rem)
    if dm:
        try:
            date_val = datetime.strptime(dm.group(1), "%d/%m/%Y").strftime("%Y/%m/%d")
        except ValueError:
            date_val = dm.group(1)
        rem = rem.replace(dm.group(1), "").strip()
    _noise = {"PMS","AGO","LPG","RFO","ATK","PREMIX","FOREIGN","RETAIL","OUTLETS",
               "MGO","LOCAL","ADDITIVATED","DIFFERENTIATED","MINES","TURBINE","KEROSENE",
               "(",")","-","AVIATION"}
    order_num = " ".join(t for t in rem.split() if t.upper() not in _noise).strip() or rem
    return {
        "Date": date_val, "Order Number": order_num,
        "Product": _get_product_category(line),
        "Truck": brv, "Price": price, "Quantity": volume,
    }


def extract_daily_orders_from_pdf(pdf_file) -> pd.DataFrame:
    rows, ctx = [], {"Depot": "Unknown Depot", "BDC": "Unknown BDC",
                     "Status": "Unknown Status", "Date": None}
    try:
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text(x_tolerance=2, y_tolerance=2)
                if not text:
                    continue
                for line in text.split("\n"):
                    c = line.strip()
                    if not c:
                        continue
                    if c.startswith("DEPOT:"):
                        raw = c.replace("DEPOT:", "").strip()
                        ctx["Depot"] = "BOST Global" if (raw.startswith("BOST") or "TAKORADI BLUE OCEAN" in raw) else raw
                    elif c.startswith("BDC:"):
                        ctx["BDC"] = c.replace("BDC:", "").strip()
                    elif "Order Status" in c:
                        parts = c.split(":")
                        if len(parts) > 1:
                            ctx["Status"] = parts[-1].strip()
                    elif not re.search(r"\d{2}$", c):
                        continue
                    else:
                        rd = _parse_daily_line(c, ctx["Date"])
                        if rd:
                            ctx["Date"] = rd["Date"] or ctx["Date"]
                            rows.append({**rd, "Depot": ctx["Depot"],
                                         "BDC": ctx["BDC"], "Status": ctx["Status"]})
    except Exception as e:
        st.error(f"Error parsing PDF: {e}")
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if not df.empty:
        # Simplify BDC names to first two words
        df["BDC"] = df["BDC"].apply(lambda n: " ".join(n.split()[:2]).upper() if n else n)
    return df


def save_daily_orders_excel(df: pd.DataFrame, filename: str | None = None) -> str:
    out_dir = os.path.join(os.getcwd(), "daily_orders")
    os.makedirs(out_dir, exist_ok=True)
    filename = filename or f"daily_orders_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    out_path = os.path.join(out_dir, filename)
    pivot = (
        df.pivot_table(index="BDC", columns="Product", values="Quantity",
                        aggfunc="sum", fill_value=0).reset_index()
        if not df.empty else pd.DataFrame()
    )
    if not pivot.empty:
        pivot["Grand Total"] = pivot[[c for c in pivot.columns if c != "BDC"]].sum(axis=1)
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="All Orders", index=False)
        if not pivot.empty:
            pivot.to_excel(w, sheet_name="Summary by BDC", index=False)
    return out_path


# ═══════════════════════════════════════════════════════════════════════════════
# STOCK TRANSACTION — PARSER
# ═══════════════════════════════════════════════════════════════════════════════

_TX_DESCRIPTIONS = sorted([
    "Balance b/fwd", "Stock Take", "Sale",
    "Custody Transfer In", "Custody Transfer Out", "Product Outturn",
], key=len, reverse=True)

_TX_SKIP = (
    "national petroleum authority", "stock transaction report",
    "bdc :", "depot :", "product :", "printed by", "printed on",
    "date trans #", "actual stock balance", "stock commitments",
    "available stock balance", "last stock update", "i.t.s from",
)


def _parse_num(s: str) -> int | None:
    s = s.strip()
    neg = s.startswith("(") and s.endswith(")")
    try:
        val = int(s.strip("()").replace(",", ""))
        return -val if neg else val
    except ValueError:
        return None


def parse_stock_transaction_pdf(pdf_file) -> list:
    def _skip(line: str) -> bool:
        lo = line.strip().lower()
        return lo.startswith(_TX_SKIP) or bool(re.match(r"^\d{1,2}\s+\w+,\s+\d{4}", line.strip()))

    def _parse_line(line: str) -> dict | None:
        line = line.strip()
        if not re.match(r"^\d{2}/\d{2}/\d{4}\b", line):
            return None
        parts  = line.split()
        date   = parts[0]
        trans  = parts[1] if len(parts) > 1 else ""
        rest   = line[len(date):].strip()[len(trans):].strip()
        desc   = next((d for d in _TX_DESCRIPTIONS if rest.lower().startswith(d.lower())), None)
        if not desc or desc == "Balance b/fwd":
            return None
        after = rest[len(desc):].strip()
        nums  = re.findall(r"\([\d,]+\)|[\d,]+", after)
        if len(nums) < 2:
            return None
        volume  = _parse_num(nums[-2])
        balance = _parse_num(nums[-1])
        trail   = re.search(re.escape(nums[-2]) + r"\s+" + re.escape(nums[-1]) + r"\s*$", after)
        account = after[:trail.start()].strip() if trail else " ".join(after.split()[:-2])
        return {
            "Date": date, "Trans #": trans, "Description": desc,
            "Account": account,
            "Volume":  volume  if volume  is not None else 0,
            "Balance": balance if balance is not None else 0,
        }

    records = []
    try:
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue
                for raw in text.split("\n"):
                    ln = raw.strip()
                    if ln and not _skip(ln):
                        row = _parse_line(ln)
                        if row:
                            records.append(row)
    except Exception as e:
        st.error(f"Error parsing PDF: {e}")
    return records


# ═══════════════════════════════════════════════════════════════════════════════
# NATIONAL OMC LOADINGS — CHUNKED FETCH
# ═══════════════════════════════════════════════════════════════════════════════

def _omc_params(start_str: str, end_str: str) -> dict:
    cfg = NPA_CONFIG
    return {
        "lngCompanyId":  cfg["COMPANY_ID"],
        "szITSfromPersol": "persol",
        "strGroupBy":    "BDC",
        "strGroupBy1":   "",
        "strQuery1":     " and iorderstatus=4",
        "strQuery2":     start_str,
        "strQuery3":     end_str,
        "strQuery4":     "",
        "strPicHeight":  "", "strPicWeight": "",
        "intPeriodID":   "4",
        "iUserId":       cfg["USER_ID"],
        "iAppId":        cfg["APP_ID"],
    }


def fetch_national_omc_loadings(
    start_str: str,
    end_str: str,
    progress_cb=None,
) -> pd.DataFrame:
    """Fetch all-BDC OMC loadings in weekly chunks (parallel)."""
    fmt = "%m/%d/%Y"
    cursor  = datetime.strptime(start_str, fmt)
    d_end   = datetime.strptime(end_str,   fmt)
    windows = []
    while cursor <= d_end:
        chunk_end = min(cursor + timedelta(days=6), d_end)
        windows.append((cursor.strftime(fmt), chunk_end.strftime(fmt)))
        cursor = chunk_end + timedelta(days=1)

    def _fetch(w_start, w_end):
        pdf = fetch_pdf_bytes(NPA_CONFIG["OMC_LOADINGS_URL"], _omc_params(w_start, w_end), timeout=60)
        return extract_npa_data_from_pdf(io.BytesIO(pdf)) if pdf else pd.DataFrame()

    frames, completed = [], 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(_fetch, ws, we): (ws, we) for ws, we in windows}
        for future in concurrent.futures.as_completed(futures):
            completed += 1
            try:
                df = future.result()
                if not df.empty:
                    frames.append(df)
            except Exception:
                pass
            if progress_cb:
                progress_cb(completed, len(windows))

    return pd.concat(frames, ignore_index=True).drop_duplicates() if frames else pd.DataFrame()


def _count_days(start_str: str, end_str: str, business_days: bool) -> int:
    fmt = "%m/%d/%Y"
    d0  = datetime.strptime(start_str, fmt).date()
    d1  = datetime.strptime(end_str,   fmt).date()
    count = len(pd.bdate_range(d0, d1)) if business_days else (d1 - d0).days
    return max(count, 1)


# ═══════════════════════════════════════════════════════════════════════════════
# SNAPSHOT HISTORY
# ═══════════════════════════════════════════════════════════════════════════════

def save_national_snapshot(forecast_df: pd.DataFrame, period_label: str):
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    snap = {
        "ts":     datetime.now().isoformat(),
        "period": period_label,
        "rows":   forecast_df[
            ["product", "total_balance", "omc_sales", "daily_rate", "days_remaining"]
        ].to_dict("records"),
    }
    fname = f"snap_{datetime.now():%Y%m%d_%H%M%S}.json"
    with open(os.path.join(SNAPSHOT_DIR, fname), "w") as f:
        json.dump(snap, f)


def load_all_snapshots() -> pd.DataFrame:
    if not os.path.exists(SNAPSHOT_DIR):
        return pd.DataFrame()
    rows = []
    for fname in sorted(os.listdir(SNAPSHOT_DIR)):
        if not fname.endswith(".json"):
            continue
        try:
            with open(os.path.join(SNAPSHOT_DIR, fname)) as f:
                snap = json.load(f)
            ts = pd.to_datetime(snap["ts"])
            for r in snap["rows"]:
                rows.append({**r, "timestamp": ts, "period": snap.get("period", "")})
        except Exception:
            continue
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════════
# DEPOT MAP — COORDINATES
# ═══════════════════════════════════════════════════════════════════════════════

_DEPOT_COORDS = {
    "TEMA":        (5.6698, -0.0166),
    "TAKORADI":    (4.8845, -1.7554),
    "ACCRA":       (5.6037, -0.1870),
    "KUMASI":      (6.6885, -1.6244),
    "BUIPE":       (8.7853, -1.5420),
    "BOLGATANGA":  (10.7856, -0.8514),
    "AKOSOMBO":    (6.3000,  0.0500),
    "MAMI WATER":  (6.25,    0.10),
    "KOTOKA":      (5.6052, -0.1668),
    "SEKONDI":     (4.934,  -1.715),
}


def _guess_coords(depot_name: str) -> tuple | None:
    if not depot_name:
        return None
    n = depot_name.upper()
    if ("KOTOKA" in n or "AIRPORT" in n or "ATK" in n) and "BLUE OCEAN" in n:
        return _DEPOT_COORDS["KOTOKA"]
    if "BOLGATANGA" in n or "BOLGA" in n:
        return _DEPOT_COORDS["BOLGATANGA"]
    if "AKOSOMBO" in n:
        return _DEPOT_COORDS["AKOSOMBO"]
    if "MAMI" in n:
        return _DEPOT_COORDS["MAMI WATER"]
    if "SEKONDI" in n or "NAVAL" in n:
        return _DEPOT_COORDS["SEKONDI"]
    if "GHANSTOCK" in n or ("TAKORADI" in n and "SEKONDI" not in n):
        return _DEPOT_COORDS["TAKORADI"]
    for key in ["TEMA", "TAKORADI", "KUMASI", "BUIPE", "ACCRA"]:
        if key in n:
            return _DEPOT_COORDS[key]
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: BDC BALANCE
# ═══════════════════════════════════════════════════════════════════════════════

def show_bdc_balance():
    page_header("🏦 BDC STOCK BALANCE ANALYZER", "Click the button below to fetch BDC Balance data")
    st.session_state.setdefault("bdc_records", [])

    if st.button("🔄 FETCH BDC BALANCE DATA", use_container_width=True):
        pdf = fetch_pdf_or_error(NPA_CONFIG["BDC_BALANCE_URL"], _bdc_balance_params(), "BDC Balance PDF")
        if pdf:
            st.session_state.bdc_records = StockBalanceScraper().parse_pdf_file(io.BytesIO(pdf))
            if not st.session_state.bdc_records:
                st.warning("⚠️ No records found in PDF.")

    records = st.session_state.bdc_records
    if not records:
        st.info("👆 Click the button above to fetch BDC balance data")
        return

    df      = pd.DataFrame(records)
    col_bal = "ACTUAL BALANCE (LT\\KG)"
    summary = df.groupby("Product")[col_bal].sum()

    st.success(f"✅ {len(records)} records extracted")

    # Product totals
    section("📊 ANALYTICS DASHBOARD")
    cols = st.columns(3)
    for idx, prod in enumerate(["GASOIL", "LPG", "PREMIUM"]):
        with cols[idx]:
            st.markdown(metric_card(prod, f"{summary.get(prod, 0):,.0f}", "LT/KG"), unsafe_allow_html=True)

    # BDC breakdown
    section("🏢 BDC BREAKDOWN")
    bdc_sum = (
        df.groupby("BDC")
        .agg({col_bal: "sum", "DEPOT": "nunique", "Product": "nunique"})
        .rename(columns={col_bal: "Total Balance (LT/KG)", "DEPOT": "Depots", "Product": "Products"})
        .sort_values("Total Balance (LT/KG)", ascending=False)
        .reset_index()
    )
    col1, col2 = st.columns([2, 1])
    with col1:
        st.dataframe(bdc_sum, use_container_width=True, hide_index=True)
    with col2:
        st.metric("Total BDCs",   df["BDC"].nunique())
        st.metric("Total Depots", df["DEPOT"].nunique())
        st.metric("Grand Total",  f"{df[col_bal].sum():,.0f} LT/KG")

    # Product × BDC pivot
    section("📊 PRODUCT DISTRIBUTION BY BDC")
    pivot = (
        df.pivot_table(index="BDC", columns="Product", values=col_bal,
                        aggfunc="sum", fill_value=0)
        .reset_index()
    )
    for p in ["GASOIL", "LPG", "PREMIUM"]:
        if p not in pivot.columns:
            pivot[p] = 0
    pivot["TOTAL"] = pivot[["GASOIL", "LPG", "PREMIUM"]].sum(axis=1)
    st.dataframe(pivot.sort_values("TOTAL", ascending=False)[["BDC","GASOIL","LPG","PREMIUM","TOTAL"]],
                 use_container_width=True, hide_index=True)

    # Search & filter
    section("🔍 SEARCH & FILTER")
    col1, col2 = st.columns(2)
    with col1:
        search_by = st.selectbox("Search By:", ["Product", "BDC", "Depot"], key="bdc_search")
    with col2:
        col_map = {"Product": "Product", "BDC": "BDC", "Depot": "DEPOT"}
        col_key = col_map[search_by]
        value = st.selectbox(f"Select {search_by}:", ["ALL"] + sorted(df[col_key].unique()), key="bdc_val")

    filtered = df if value == "ALL" else df[df[col_key] == value]
    st.markdown(f"<h3>📋 FILTERED DATA: {value}</h3>", unsafe_allow_html=True)
    st.dataframe(
        filtered[["Product","BDC","DEPOT","AVAILABLE BALANCE (LT\\KG)", col_bal, "Date"]]
        .sort_values(["Product","BDC","DEPOT"]),
        use_container_width=True, height=400, hide_index=True,
    )
    cols = st.columns(4)
    for col, (lbl, val) in zip(cols, [
        ("RECORDS", f"{len(filtered):,}"), ("BDCs", f"{filtered['BDC'].nunique()}"),
        ("DEPOTS",  f"{filtered['DEPOT'].nunique()}"),
        ("TOTAL BALANCE", f"{filtered[col_bal].sum():,.0f}"),
    ]):
        with col:
            st.metric(lbl, val)

    # Export
    section("💾 EXPORT DATA")
    scraper = StockBalanceScraper()
    path = scraper.save_to_excel(records)
    if path:
        excel_download_button(path, os.path.basename(path))


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: OMC LOADINGS
# ═══════════════════════════════════════════════════════════════════════════════

def show_omc_loadings():
    page_header("🚚 OMC LOADINGS ANALYZER",
                "Select date range and fetch OMC loadings data")
    st.session_state.setdefault("omc_df", pd.DataFrame())
    st.session_state.setdefault("omc_start_date", datetime.now() - timedelta(days=7))
    st.session_state.setdefault("omc_end_date",   datetime.now())

    st.info("💡 Select a date range where you know there are orders.")
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=st.session_state.omc_start_date, key="omc_start")
    with col2:
        end_date = st.date_input("End Date", value=st.session_state.omc_end_date, key="omc_end")

    if st.button("🔄 FETCH OMC LOADINGS DATA", use_container_width=True):
        st.session_state.omc_start_date = start_date
        st.session_state.omc_end_date   = end_date
        start_str = start_date.strftime("%m/%d/%Y")
        end_str   = end_date.strftime("%m/%d/%Y")
        st.info(f"🔍 Requesting orders from **{start_str}** to **{end_str}**")
        pdf = fetch_pdf_or_error(NPA_CONFIG["OMC_LOADINGS_URL"], _omc_params(start_str, end_str))
        if pdf:
            st.session_state.omc_df = extract_npa_data_from_pdf(io.BytesIO(pdf))
            if st.session_state.omc_df.empty:
                st.warning("⚠️ No order records found for this date range.")

    df = st.session_state.omc_df
    if df.empty:
        st.info("👆 Select dates and click the button above to fetch OMC loadings data")
        return

    st.success(f"✅ {len(df)} records extracted")

    # KPI row
    section("📊 ANALYTICS DASHBOARD")
    cols = st.columns(4)
    kpis = [
        ("TOTAL ORDERS", f"{len(df):,}", ""),
        ("VOLUME",       f"{df['Quantity'].sum():,.0f}", "LT/KG"),
        ("OMCs",         f"{df['OMC'].nunique()}", ""),
        ("VALUE",        f"₵{(df['Quantity']*df['Price']).sum():,.0f}", ""),
    ]
    for col, (lbl, val, unit) in zip(cols, kpis):
        with col:
            st.markdown(metric_card(lbl, val, unit), unsafe_allow_html=True)

    # Product / BDC breakdowns
    for title, grp_col, label_cols in [
        ("📦 PRODUCT BREAKDOWN", "Product",
         [("Total Volume (LT/KG)", "sum"), ("Orders", "count"), ("OMCs", "nunique")]),
        ("🏦 BDC PERFORMANCE", "BDC",
         [("Total Volume (LT/KG)", "sum"), ("Orders", "count"), ("OMCs", "nunique"), ("Products", "nunique")]),
    ]:
        section(title)
        agg = df.groupby(grp_col).agg(
            **{lc: pd.NamedAgg("Quantity" if lc == "Total Volume (LT/KG)" else
                               ("Order Number" if lc == "Orders" else
                                ("OMC" if lc == "OMCs" else "Product")), fn)
               for lc, fn in label_cols}
        ).sort_values("Total Volume (LT/KG)", ascending=False).reset_index()
        st.dataframe(agg, use_container_width=True, hide_index=True)

    # Pivot
    section("📊 PRODUCT DISTRIBUTION BY BDC")
    pivot = df.pivot_table(index="BDC", columns="Product", values="Quantity",
                            aggfunc="sum", fill_value=0).reset_index()
    for p in ["GASOIL","LPG","PREMIUM"]:
        if p not in pivot.columns: pivot[p] = 0
    pivot["TOTAL"] = pivot[["GASOIL","LPG","PREMIUM"]].sum(axis=1)
    st.dataframe(pivot.sort_values("TOTAL", ascending=False)[["BDC","GASOIL","LPG","PREMIUM","TOTAL"]],
                 use_container_width=True, hide_index=True)

    # Search & filter
    section("🔍 SEARCH & FILTER")
    col1, col2 = st.columns(2)
    with col1:
        search_by = st.selectbox("Search By:", ["Product","OMC","BDC","Depot"], key="omc_search")
    with col2:
        col_map = {"Product":"Product","OMC":"OMC","BDC":"BDC","Depot":"Depot"}
        col_key = col_map[search_by]
        value = st.selectbox(f"Select {search_by}:", ["ALL"] + sorted(df[col_key].unique()), key="omc_val")

    filtered = df if value == "ALL" else df[df[col_key] == value]
    st.markdown(f"<h3>📋 FILTERED DATA: {value}</h3>", unsafe_allow_html=True)
    if not filtered.empty:
        cols = st.columns(4)
        for col, (lbl, val) in zip(cols, [
            ("Filtered Orders", f"{len(filtered):,}"),
            ("Volume",          f"{filtered['Quantity'].sum():,.0f} LT"),
            ("Unique OMCs",     f"{filtered['OMC'].nunique()}"),
            ("Value",           f"₵{(filtered['Quantity']*filtered['Price']).sum():,.0f}"),
        ]):
            with col: st.metric(lbl, val)
    st.dataframe(
        filtered[["Date","OMC","Truck","Quantity","Order Number","BDC","Depot","Price","Product"]]
        .sort_values(["Product","OMC","Date"]),
        use_container_width=True, height=400, hide_index=True,
    )

    section("💾 EXPORT DATA")
    path = save_omc_excel(df)
    if path:
        excel_download_button(path, os.path.basename(path))


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: DAILY ORDERS
# ═══════════════════════════════════════════════════════════════════════════════

def show_daily_orders():
    page_header("📅 DAILY ORDERS ANALYZER", "Select a date range to fetch daily orders")
    st.session_state.setdefault("daily_df", pd.DataFrame())
    st.session_state.setdefault("daily_start_date", datetime.now() - timedelta(days=1))
    st.session_state.setdefault("daily_end_date",   datetime.now())

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", st.session_state.daily_start_date, key="daily_start")
    with col2:
        end_date = st.date_input("End Date", st.session_state.daily_end_date, key="daily_end")

    if st.button("🔄 FETCH DAILY ORDERS", use_container_width=True):
        st.session_state.daily_start_date = start_date
        st.session_state.daily_end_date   = end_date
        start_str = start_date.strftime("%m/%d/%Y")
        end_str   = end_date.strftime("%m/%d/%Y")
        params = {
            "lngCompanyId": NPA_CONFIG["COMPANY_ID"], "szITSfromPersol": "persol",
            "strGroupBy": "DEPOT", "strGroupBy1": "",
            "strQuery1": "", "strQuery2": start_str, "strQuery3": end_str, "strQuery4": "",
            "strPicHeight": "1", "strPicWeight": "1",
            "intPeriodID": "-1", "iUserId": NPA_CONFIG["USER_ID"], "iAppId": NPA_CONFIG["APP_ID"],
        }
        pdf = fetch_pdf_or_error(NPA_CONFIG["DAILY_ORDERS_URL"], params)
        if pdf:
            st.session_state.daily_df = extract_daily_orders_from_pdf(io.BytesIO(pdf))
            if st.session_state.daily_df.empty:
                st.warning("⚠️ No daily orders found.")

    df = st.session_state.daily_df
    if df.empty:
        st.info("👆 Select a date range and click the button above.")
        return

    # OMC matching from loadings data
    loadings_df = st.session_state.get("omc_df", pd.DataFrame())
    if not loadings_df.empty:
        import re as _re
        def _prefix(order):
            if pd.isna(order): return None
            m = _re.match(r"^([A-Z]{2,})", str(order).strip().upper())
            return m.group(1) if m else None

        loadings_df = loadings_df.copy()
        loadings_df["_prefix"] = loadings_df["Order Number"].apply(_prefix)
        exact_map  = dict(zip(loadings_df["Order Number"], loadings_df["OMC"]))
        prefix_map = {
            pfx: loadings_df[loadings_df["_prefix"] == pfx]["OMC"].mode().iloc[0]
            for pfx in loadings_df["_prefix"].dropna().unique()
            if not loadings_df[loadings_df["_prefix"] == pfx]["OMC"].mode().empty
        }
        df["_prefix"] = df["Order Number"].apply(_prefix)
        df["OMC"] = df["Order Number"].map(exact_map)
        df["OMC"] = df.apply(
            lambda r: prefix_map.get(r["_prefix"]) if pd.isna(r["OMC"]) else r["OMC"], axis=1
        )
        df.drop(columns=["_prefix"], inplace=True)
        matched = df["OMC"].notna().sum()
        if matched:
            st.info(f"🔗 OMC matched for {matched}/{len(df)} orders ({matched/len(df)*100:.1f}%)")
    else:
        df["OMC"] = None
        st.warning("💡 Fetch OMC Loadings first to auto-match order numbers to OMC names.")

    st.session_state.daily_df = df
    st.success(f"✅ {len(df)} daily orders extracted")

    section("📊 DAILY ANALYTICS")
    cols = st.columns(5)
    omc_count = df["OMC"].nunique() if df["OMC"].notna().any() else 0
    for col, (lbl, val, unit) in zip(cols, [
        ("ORDERS", f"{len(df):,}", ""),
        ("VOLUME", f"{df['Quantity'].sum():,.0f}", "LT/KG"),
        ("BDCs",   f"{df['BDC'].nunique()}", ""),
        ("OMCs",   f"{omc_count}", ""),
        ("VALUE",  f"₵{(df['Quantity']*df['Price']).sum():,.0f}", ""),
    ]):
        with col:
            st.markdown(metric_card(lbl, val, unit), unsafe_allow_html=True)

    section("📦 PRODUCT SUMMARY")
    prod_sum = (df.groupby("Product")
                  .agg(Total_Volume=("Quantity","sum"), Orders=("Order Number","count"), BDCs=("BDC","nunique"))
                  .rename(columns={"Total_Volume":"Total Volume (LT/KG)"})
                  .sort_values("Total Volume (LT/KG)", ascending=False).reset_index())
    col1, col2 = st.columns([2,1])
    with col1: st.dataframe(prod_sum, use_container_width=True, hide_index=True)
    with col2:
        total = prod_sum["Total Volume (LT/KG)"].sum()
        for _, row in prod_sum.iterrows():
            st.metric(row["Product"], f"{row['Total Volume (LT/KG)']/total*100:.1f}%")

    section("📊 PRODUCT DISTRIBUTION BY BDC")
    pivot = df.pivot_table(index="BDC", columns="Product", values="Quantity",
                            aggfunc="sum", fill_value=0).reset_index()
    pivot["TOTAL"] = pivot[[c for c in pivot.columns if c != "BDC"]].sum(axis=1)
    st.dataframe(pivot.sort_values("TOTAL", ascending=False), use_container_width=True, hide_index=True)

    section("💾 EXPORT DATA")
    path = save_daily_orders_excel(df)
    if path:
        excel_download_button(path, os.path.basename(path))


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: MARKET SHARE
# ═══════════════════════════════════════════════════════════════════════════════

def show_market_share():
    page_header("📊 BDC MARKET SHARE ANALYSIS",
                "Comprehensive market share: Stock Balance + Sales Volume")

    has_balance  = bool(st.session_state.get("bdc_records"))
    has_loadings = not st.session_state.get("omc_df", pd.DataFrame()).empty

    col1, col2 = st.columns(2)
    with col1:
        st.success(f"✅ BDC Balance: {len(st.session_state.get('bdc_records',[]))} records") \
            if has_balance else st.warning("⚠️ BDC Balance Not Loaded")
    with col2:
        st.success(f"✅ OMC Loadings: {len(st.session_state.get('omc_df', pd.DataFrame()))} records") \
            if has_loadings else st.warning("⚠️ OMC Loadings Not Loaded")

    if not has_balance and not has_loadings:
        st.error("❌ No data. Fetch from BDC Balance and/or OMC Loadings first.")
        return

    balance_df   = pd.DataFrame(st.session_state.bdc_records) if has_balance else pd.DataFrame()
    loadings_df  = st.session_state.omc_df if has_loadings else pd.DataFrame()
    col_bal      = "ACTUAL BALANCE (LT\\KG)"

    all_bdcs = sorted({
        *( balance_df["BDC"].unique() if not balance_df.empty else []),
        *(loadings_df["BDC"].unique() if not loadings_df.empty else []),
    })
    if not all_bdcs:
        st.error("❌ No BDCs found.")
        return

    selected_bdc = st.selectbox("Choose BDC:", all_bdcs, key="market_bdc")
    st.markdown(f"## 📊 COMPREHENSIVE MARKET REPORT: {selected_bdc}")

    tab1, tab2, tab3 = st.tabs(["📦 Stock Balance", "🚚 Sales Volume", "📊 Combined"])

    with tab1:
        if not has_balance:
            st.warning("⚠️ BDC Balance not available.")
            return
        st.markdown("### 📦 STOCK BALANCE MARKET SHARE")
        bdc_bal_data = balance_df[balance_df["BDC"] == selected_bdc]
        total_stock  = balance_df[col_bal].sum()
        bdc_stock    = bdc_bal_data[col_bal].sum()
        all_stocks   = balance_df.groupby("BDC")[col_bal].sum().sort_values(ascending=False)
        rank = list(all_stocks.index).index(selected_bdc) + 1 if selected_bdc in all_stocks.index else 0

        cols = st.columns(3)
        for col, (lbl, val, unit) in zip(cols, [
            ("TOTAL STOCK",   f"{bdc_stock:,.0f}", "LT/KG"),
            ("MARKET SHARE",  f"{bdc_stock/total_stock*100:.2f}%" if total_stock else "N/A", "of Total"),
            ("STOCK RANK",    f"#{rank}", f"of {len(all_stocks)}"),
        ]):
            with col: st.markdown(metric_card(lbl, val, unit), unsafe_allow_html=True)

        st.markdown("#### 📦 Stock by Product")
        rows = []
        for prod in ["PREMIUM","GASOIL","LPG"]:
            market = balance_df[balance_df["Product"]==prod][col_bal].sum()
            bdc_v  = bdc_bal_data[bdc_bal_data["Product"]==prod][col_bal].sum()
            rows.append({"Product": prod, "BDC Stock (LT/KG)": bdc_v,
                          "Market Total (LT/KG)": market,
                          "Market Share (%)": round(bdc_v/market*100, 2) if market else 0})
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    with tab2:
        if not has_loadings:
            st.warning("⚠️ OMC Loadings not available.")
            return
        st.markdown("### 🚚 SALES VOLUME MARKET SHARE")
        bdc_sales    = loadings_df[loadings_df["BDC"] == selected_bdc]
        total_sales  = loadings_df["Quantity"].sum()
        bdc_vol      = bdc_sales["Quantity"].sum()
        all_sales    = loadings_df.groupby("BDC")["Quantity"].sum().sort_values(ascending=False)
        sales_rank   = list(all_sales.index).index(selected_bdc) + 1 if selected_bdc in all_sales.index else 0
        revenue      = (bdc_sales["Quantity"] * bdc_sales["Price"]).sum()

        cols = st.columns(4)
        for col, (lbl, val, unit) in zip(cols, [
            ("TOTAL SALES",   f"{bdc_vol:,.0f}", "LT/KG"),
            ("MARKET SHARE",  f"{bdc_vol/total_sales*100:.2f}%" if total_sales else "N/A", "of Total"),
            ("OVERALL RANK",  f"#{sales_rank}", f"of {len(all_sales)}"),
            ("REVENUE",       f"₵{revenue/1e6:,.1f}M", "Total Value"),
        ]):
            with col: st.markdown(metric_card(lbl, val, unit), unsafe_allow_html=True)

        st.markdown("#### 🚚 Sales by Product")
        rows = []
        for prod in ["PREMIUM","GASOIL","LPG"]:
            market = loadings_df[loadings_df["Product"]==prod]["Quantity"].sum()
            bdc_v  = bdc_sales[bdc_sales["Product"]==prod]["Quantity"].sum()
            rows.append({"Product": prod, "BDC Sales (LT/KG)": bdc_v,
                          "Market Total (LT/KG)": market,
                          "Market Share (%)": round(bdc_v/market*100, 2) if market else 0,
                          "Orders": len(bdc_sales[bdc_sales["Product"]==prod])})
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    with tab3:
        if not has_balance or not has_loadings:
            st.warning("⚠️ Both datasets required for combined analysis.")
            return
        st.markdown("### 📊 STOCK vs SALES COMPARISON")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"""
            <div style='background:rgba(22,33,62,.6);padding:20px;border-radius:15px;border:2px solid #00ffff;'>
                <h3 style='color:#00ffff'>📦 STOCK POSITION</h3>
                <p style='font-size:28px;font-weight:700'>{bdc_stock:,.0f} LT</p>
                <p style='color:#00ff88;font-size:20px'>{bdc_stock/total_stock*100:.2f}% Market Share</p>
                <p style='color:#888'>Rank #{rank} in Stock</p>
            </div>""", unsafe_allow_html=True)
        with col2:
            st.markdown(f"""
            <div style='background:rgba(22,33,62,.6);padding:20px;border-radius:15px;border:2px solid #ff00ff;'>
                <h3 style='color:#ff00ff'>🚚 SALES VOLUME</h3>
                <p style='font-size:28px;font-weight:700'>{bdc_vol:,.0f} LT</p>
                <p style='color:#00ff88;font-size:20px'>{bdc_vol/total_sales*100:.2f}% Market Share</p>
                <p style='color:#888'>Rank #{sales_rank} in Sales</p>
            </div>""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: COMPETITIVE INTEL
# ═══════════════════════════════════════════════════════════════════════════════

def show_competitive_intel():
    page_header("🎯 COMPETITIVE INTELLIGENCE CENTER",
                "Anomaly Detection, Price Intelligence & Performance Scoring")

    if st.session_state.get("omc_df", pd.DataFrame()).empty:
        st.warning("⚠️ OMC Loadings data required. Fetch it first.")
        return

    df = st.session_state.omc_df
    tab1, tab2, tab3 = st.tabs(["🚨 Anomaly Detection","💰 Price Intelligence","⭐ Performance Scores"])

    with tab1:
        st.markdown("### 🚨 ANOMALY DETECTION ENGINE")
        mean_v, std_v = df["Quantity"].mean(), df["Quantity"].std()
        threshold     = mean_v + 2 * std_v
        anomalies     = df[df["Quantity"] > threshold]
        cols = st.columns(3)
        for col, (lbl, val) in zip(cols, [
            ("Volume Anomalies",   f"{len(anomalies)}"),
            ("Anomalous Volume",   f"{anomalies['Quantity'].sum():,.0f} LT"),
            ("Threshold",          f"{threshold:,.0f} LT"),
        ]):
            with col: st.metric(lbl, val)
        if not anomalies.empty:
            st.warning(f"🚨 {len(anomalies)} abnormally large orders detected!")
            st.dataframe(anomalies.nlargest(10,"Quantity")[
                ["Date","BDC","OMC","Product","Quantity","Order Number"]],
                use_container_width=True, hide_index=True)

        st.markdown("#### 💰 Price Anomalies by Product")
        rows = []
        for prod in ["PREMIUM","GASOIL","LPG"]:
            pdf  = df[df["Product"]==prod]
            if pdf.empty: continue
            pm, ps = pdf["Price"].mean(), pdf["Price"].std()
            rows.append({"Product": prod, "Avg Price": f"₵{pm:.2f}",
                          "High Anomalies": len(pdf[pdf["Price"] > pm + 2*ps]),
                          "Low Anomalies":  len(pdf[pdf["Price"] < pm - 2*ps])})
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    with tab2:
        st.markdown("### 💰 PRICE INTELLIGENCE DASHBOARD")
        stats = (df.groupby(["BDC","Product"])["Price"]
                   .agg(["mean","min","max"]).reset_index()
                   .rename(columns={"mean":"Avg Price","min":"Min Price","max":"Max Price"}))
        overall = df["Price"].mean()
        stats["Tier"] = stats["Avg Price"].apply(
            lambda x: "🔴 Premium" if x > overall*1.1 else "🟢 Competitive")
        st.dataframe(stats.sort_values("Avg Price", ascending=False),
                     use_container_width=True, hide_index=True)

        st.markdown("#### 💡 Pricing Opportunities")
        opps = []
        for prod in ["PREMIUM","GASOIL","LPG"]:
            pdf = df[df["Product"]==prod]
            if pdf.empty: continue
            bdc_p = pdf.groupby("BDC")["Price"].mean()
            opps.append({"Product": prod,
                          "Lowest":  f"{bdc_p.idxmin()} (₵{bdc_p.min():.2f})",
                          "Highest": f"{bdc_p.idxmax()} (₵{bdc_p.max():.2f})",
                          "Gap":     f"₵{bdc_p.max()-bdc_p.min():.2f}"})
        st.dataframe(pd.DataFrame(opps), use_container_width=True, hide_index=True)

    with tab3:
        st.markdown("### ⭐ BDC PERFORMANCE LEADERBOARD")
        max_vol, max_ord = (df.groupby("BDC")["Quantity"].sum().max(),
                            df.groupby("BDC").size().max())
        scores = []
        for bdc in df["BDC"].unique():
            sub = df[df["BDC"]==bdc]
            vs  = sub["Quantity"].sum() / max_vol * 40
            os_ = len(sub)             / max_ord  * 30
            ds  = sub["Product"].nunique() / 3    * 30
            tot = vs + os_ + ds
            scores.append({"BDC": bdc, "Volume Score": round(vs,1),
                            "Orders Score": round(os_,1), "Diversity Score": round(ds,1),
                            "Total Score": round(tot,1),
                            "Grade": "A+" if tot>=90 else "A" if tot>=80 else "B" if tot>=70
                                     else "C" if tot>=60 else "D"})
        scores_df = pd.DataFrame(scores).sort_values("Total Score", ascending=False).reset_index(drop=True)
        scores_df.insert(0, "Rank", range(1, len(scores_df)+1))
        scores_df["Medal"] = scores_df["Rank"].map({1:"🥇",2:"🥈",3:"🥉"}).fillna("")
        st.dataframe(scores_df, use_container_width=True, hide_index=True)

        sel = st.selectbox("Check a BDC:", scores_df["BDC"].unique(), key="ci_bdc")
        if sel:
            row = scores_df[scores_df["BDC"]==sel].iloc[0]
            cols = st.columns(3)
            for col, (lbl, val) in zip(cols, [
                ("Volume Score",    f"{row['Volume Score']:.1f}/40"),
                ("Orders Score",    f"{row['Orders Score']:.1f}/30"),
                ("Diversity Score", f"{row['Diversity Score']:.1f}/30"),
            ]):
                with col: st.metric(lbl, val)
            st.metric(f"{sel} — Total Score", f"{row['Total Score']:.1f}/100 | Grade: {row['Grade']}")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: STOCK TRANSACTION
# ═══════════════════════════════════════════════════════════════════════════════

def show_stock_transaction():
    page_header("📈 STOCK TRANSACTION ANALYZER",
                "Track BDC transactions: Inflows, Outflows, Sales & Stockout Forecasting")
    st.session_state.setdefault("stock_txn_df", pd.DataFrame())

    tab1, tab2 = st.tabs(["🔍 BDC Transaction Report","📊 Stockout Analysis"])

    with tab1:
        st.markdown("### 🔍 BDC TRANSACTION REPORT")
        col1, col2 = st.columns(2)
        with col1:
            selected_bdc     = st.selectbox("Select BDC:",    sorted(BDC_MAP.keys()),   key="txn_bdc")
            selected_product = st.selectbox("Select Product:", PRODUCT_OPTIONS,           key="txn_prod")
        with col2:
            selected_depot = st.selectbox("Select Depot:", sorted(DEPOT_MAP.keys()), key="txn_depot")
        col3, col4 = st.columns(2)
        with col3:
            start_date = st.date_input("Start Date:", value=datetime.now()-timedelta(days=30), key="txn_start")
        with col4:
            end_date = st.date_input("End Date:", value=datetime.now(), key="txn_end")

        if st.button("📊 FETCH TRANSACTION REPORT", use_container_width=True):
            params = {
                "lngProductId": STOCK_PRODUCT_MAP[selected_product],
                "lngBDCId":     BDC_MAP[selected_bdc],
                "lngDepotId":   DEPOT_MAP[selected_depot],
                "dtpStartDate": start_date.strftime("%m/%d/%Y"),
                "dtpEndDate":   end_date.strftime("%m/%d/%Y"),
                "lngUserId":    NPA_CONFIG["USER_ID"],
            }
            pdf = fetch_pdf_or_error(NPA_CONFIG["STOCK_TRANSACTION_URL"], params, "Transaction PDF")
            if pdf:
                records = parse_stock_transaction_pdf(io.BytesIO(pdf))
                if records:
                    st.session_state.stock_txn_df      = pd.DataFrame(records)
                    st.session_state.stock_txn_bdc     = selected_bdc
                    st.session_state.stock_txn_depot   = selected_depot
                    st.session_state.stock_txn_product = selected_product
                    st.success(f"✅ {len(records)} transactions extracted!")
                else:
                    st.warning("⚠️ No transactions found.")
                    st.session_state.stock_txn_df = pd.DataFrame()

        df = st.session_state.stock_txn_df
        if df.empty:
            st.info("👆 Select options above and click Fetch.")
            return

        st.markdown(f"### 📊 ANALYSIS: {st.session_state.get('stock_txn_bdc','')}")
        inflows  = df[df["Description"].isin(["Custody Transfer In","Product Outturn"])]["Volume"].sum()
        outflows = df[df["Description"].isin(["Sale","Custody Transfer Out"])]["Volume"].sum()
        sales    = df[df["Description"]=="Sale"]["Volume"].sum()
        transfer = df[df["Description"]=="Custody Transfer Out"]["Volume"].sum()
        final_bal = df["Balance"].iloc[-1] if len(df) else 0

        cols = st.columns(5)
        for col, (lbl, val) in zip(cols, [
            ("📥 Inflows",       f"{inflows:,.0f} LT"),
            ("📤 Outflows",      f"{outflows:,.0f} LT"),
            ("💰 Sales to OMCs", f"{sales:,.0f} LT"),
            ("🔄 BDC Transfers", f"{transfer:,.0f} LT"),
            ("📊 Final Balance", f"{final_bal:,.0f} LT"),
        ]):
            with col: st.metric(lbl, val)

        st.markdown("### 📋 Transaction Breakdown")
        txn_sum = (df.groupby("Description")
                     .agg(Total_Volume=("Volume","sum"), Count=("Trans #","count"))
                     .rename(columns={"Total_Volume":"Total Volume (LT)","Count":"Count"})
                     .sort_values("Total Volume (LT)", ascending=False).reset_index())
        st.dataframe(txn_sum, use_container_width=True, hide_index=True)

        if sales > 0:
            st.markdown("### 🏢 Top Customers (OMC Sales)")
            cust = (df[df["Description"]=="Sale"].groupby("Account")["Volume"].sum()
                      .sort_values(ascending=False).head(10).reset_index()
                      .rename(columns={"Account":"Customer","Volume":"Volume Sold (LT)"}))
            st.dataframe(cust, use_container_width=True, hide_index=True)

        st.markdown("### 📄 Full Transaction History")
        st.dataframe(df, use_container_width=True, hide_index=True, height=400)

        section("💾 EXPORT")
        excel_download_button(df, f"stock_txn_{st.session_state.get('stock_txn_bdc','export')}.xlsx",
                              sheets={"Transactions": df, "Summary": txn_sum})

    with tab2:
        st.markdown("### 📊 INTELLIGENT STOCKOUT FORECASTING")
        has_balance  = bool(st.session_state.get("bdc_records"))
        has_txn      = not st.session_state.stock_txn_df.empty

        if not has_balance or not has_txn:
            st.info("Fetch BDC Balance data and transaction data first.")
            return

        bal_df      = pd.DataFrame(st.session_state.bdc_records)
        txn_df      = st.session_state.stock_txn_df
        bdc_name    = st.session_state.get("stock_txn_bdc", "")
        prod_disp   = st.session_state.get("stock_txn_product", "")
        prod_name   = PRODUCT_BALANCE_MAP.get(prod_disp, prod_disp)
        col_bal     = "ACTUAL BALANCE (LT\\KG)"

        bdc_bal = bal_df[
            bal_df["BDC"].str.contains(bdc_name, case=False, na=False) &
            bal_df["Product"].str.contains(prod_name, case=False, na=False)
        ]
        if bdc_bal.empty:
            st.warning(f"⚠️ No balance data found for {bdc_name} / {prod_name}")
            return

        current_stock = bdc_bal[col_bal].sum()
        total_sales   = txn_df[txn_df["Description"].isin(["Sale","Custody Transfer Out"])]["Volume"].sum()
        txn_copy      = txn_df.copy()
        txn_copy["_dt"] = pd.to_datetime(txn_copy["Date"], format="%d/%m/%Y", errors="coerce")
        day_range     = max((txn_copy["_dt"].max() - txn_copy["_dt"].min()).days, 1)
        daily_rate    = total_sales / day_range
        days_left     = current_stock / daily_rate if daily_rate > 0 else float("inf")

        if days_left < 7:   status, color = "🔴 CRITICAL", "red"
        elif days_left < 14: status, color = "🟡 WARNING",  "orange"
        else:                status, color = "🟢 HEALTHY",  "green"

        cols = st.columns(4)
        for col, (lbl, val, unit) in zip(cols, [
            ("CURRENT STOCK",  f"{current_stock:,.0f}", "LT/KG"),
            ("DAILY SALES",    f"{daily_rate:,.0f}",    "LT/day"),
            ("DAYS REMAINING", f"{days_left:.1f}" if days_left != float("inf") else "∞", "days"),
            ("ANALYSIS PERIOD", f"{day_range}", "days"),
        ]):
            with col: st.markdown(metric_card(lbl, val, unit), unsafe_allow_html=True)

        stockout_date = (
            (datetime.now() + timedelta(days=days_left)).strftime("%Y-%m-%d")
            if days_left != float("inf") else "N/A"
        )
        if days_left < 7:
            st.error(f"**🚨 CRITICAL:** ~{days_left:.1f} days left. Immediate replenishment required. Est. empty: {stockout_date}")
        elif days_left < 14:
            st.warning(f"**⚠️ WARNING:** ~{days_left:.1f} days left. Plan replenishment now. Est. empty: {stockout_date}")
        else:
            st.success(f"**✅ HEALTHY:** ~{days_left:.1f} days of supply. Est. empty: {stockout_date}")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: BDC INTELLIGENCE
# ═══════════════════════════════════════════════════════════════════════════════

def show_bdc_intelligence():
    page_header("🧠 BDC INTELLIGENCE CENTER",
                "Predictive analytics combining stock balance and loading patterns")

    has_balance  = bool(st.session_state.get("bdc_records"))
    has_loadings = not st.session_state.get("omc_df", pd.DataFrame()).empty

    if not has_balance or not has_loadings:
        col1, col2 = st.columns(2)
        with col1:
            if not has_balance:
                st.warning("⚠️ BDC Balance Missing")
                if st.button("🔄 FETCH BDC BALANCE", use_container_width=True, key="intel_bal"):
                    if fetch_and_store_bdc_balance():
                        st.success(f"✅ {len(st.session_state.bdc_records)} records loaded!")
                        st.rerun()
            else:
                st.success(f"✅ BDC Balance: {len(st.session_state.bdc_records)} records")
        with col2:
            if not has_loadings:
                st.warning("⚠️ OMC Loadings Missing")
                start_d = st.date_input("From", value=datetime.now()-timedelta(days=30), key="intel_start")
                end_d   = st.date_input("To",   value=datetime.now(), key="intel_end")
                if st.button("🔄 FETCH OMC LOADINGS", use_container_width=True, key="intel_omc"):
                    ss, se = start_d.strftime("%m/%d/%Y"), end_d.strftime("%m/%d/%Y")
                    pdf = fetch_pdf_bytes(NPA_CONFIG["OMC_LOADINGS_URL"], _omc_params(ss, se))
                    if pdf:
                        st.session_state.omc_df = extract_npa_data_from_pdf(io.BytesIO(pdf))
                        st.rerun()
            else:
                st.success(f"✅ OMC Loadings: {len(st.session_state.omc_df)} records")

        if not (bool(st.session_state.get("bdc_records")) and
                not st.session_state.get("omc_df", pd.DataFrame()).empty):
            return

    balance_df  = pd.DataFrame(st.session_state.bdc_records)
    loadings_df = st.session_state.omc_df
    col_bal     = "ACTUAL BALANCE (LT\\KG)"

    all_bdcs = sorted({*balance_df["BDC"].unique(), *loadings_df["BDC"].unique()})
    selected_bdc = st.selectbox("Choose BDC:", all_bdcs, key="intel_bdc")

    tab1, tab2, tab3 = st.tabs(["📊 Overview","⏱️ Stockout Prediction","📉 Consumption"])

    with tab1:
        bdc_bal  = balance_df[balance_df["BDC"]==selected_bdc]
        bdc_load = loadings_df[loadings_df["BDC"]==selected_bdc]

        if not bdc_bal.empty:
            section("📦 CURRENT STOCK")
            stocks = bdc_bal.groupby("Product")[col_bal].sum()
            cols = st.columns(3)
            for idx, (prod, val) in enumerate(stocks.items()):
                with cols[idx % 3]:
                    st.markdown(metric_card(prod, f"{val:,.0f}", "LT/KG"), unsafe_allow_html=True)
            st.markdown("#### 🏭 Stock by Depot")
            pivot = (bdc_bal.groupby(["DEPOT","Product"])[col_bal].sum().reset_index()
                            .pivot(index="DEPOT", columns="Product", values=col_bal).fillna(0))
            st.dataframe(pivot, use_container_width=True)

        if not bdc_load.empty:
            section("🚚 LOADING ACTIVITY")
            cols = st.columns(4)
            for col, (lbl, val) in zip(cols, [
                ("Total Orders", f"{len(bdc_load):,}"),
                ("Total Volume", f"{bdc_load['Quantity'].sum():,.0f} LT"),
                ("Unique OMCs",  f"{bdc_load['OMC'].nunique()}"),
                ("Avg Order",    f"{bdc_load['Quantity'].mean():,.0f} LT"),
            ]):
                with col: st.metric(lbl, val)

    with tab2:
        bdc_bal  = balance_df[balance_df["BDC"]==selected_bdc]
        bdc_load = loadings_df[loadings_df["BDC"]==selected_bdc]
        if bdc_bal.empty or bdc_load.empty:
            st.warning("⚠️ Insufficient data for stockout prediction.")
            return
        ts = bdc_load.copy()
        ts["Date"] = pd.to_datetime(ts["Date"], errors="coerce")
        ts = ts.dropna(subset=["Date"])
        day_range = max((ts["Date"].max() - ts["Date"].min()).days, 1)
        daily_rate = ts.groupby("Product")["Quantity"].sum() / day_range
        stocks = bdc_bal.groupby("Product")[col_bal].sum()

        for prod in stocks.index:
            stock = stocks[prod]
            rate  = daily_rate.get(prod, 0)
            days  = stock / rate if rate > 0 else float("inf")
            color = "#ff0000" if days < 7 else "#ffaa00" if days < 14 else "#00ff88"
            days_txt = f"{days:.1f}" if days != float("inf") else "∞"
            st.markdown(f"""
            <div style='background:rgba(22,33,62,.6);padding:20px;border-radius:10px;
                        border:2px solid {color};margin:10px 0;'>
                <h3 style='color:{color}'>{prod}</h3>
                <div style='display:grid;grid-template-columns:1fr 1fr 1fr;gap:20px;margin-top:15px;'>
                    <div><p style='color:#888;font-size:14px;margin:0'>Current Stock</p>
                         <p style='color:#00ffff;font-size:24px;font-weight:700;margin:5px 0'>{stock:,.0f} LT</p></div>
                    <div><p style='color:#888;font-size:14px;margin:0'>Daily Usage</p>
                         <p style='color:#ff00ff;font-size:24px;font-weight:700;margin:5px 0'>{rate:,.0f} LT</p></div>
                    <div><p style='color:#888;font-size:14px;margin:0'>Days Remaining</p>
                         <p style='color:{color};font-size:32px;font-weight:700;margin:5px 0'>{days_txt}</p></div>
                </div>
            </div>""", unsafe_allow_html=True)

    with tab3:
        bdc_load = loadings_df[loadings_df["BDC"]==selected_bdc].copy()
        bdc_load["Date"] = pd.to_datetime(bdc_load["Date"], errors="coerce")
        bdc_load = bdc_load.dropna(subset=["Date"])
        if bdc_load.empty:
            st.warning("⚠️ No loading data.")
            return
        daily = bdc_load.groupby([bdc_load["Date"].dt.date,"Product"])["Quantity"].sum().reset_index()
        daily.columns = ["Date","Product","Volume"]
        for prod in daily["Product"].unique():
            st.markdown(f"**{prod}**")
            st.line_chart(daily[daily["Product"]==prod].set_index("Date")["Volume"], use_container_width=True)
        section("📊 CONSUMPTION STATISTICS")
        stats = bdc_load.groupby("Product")["Quantity"].agg(
            Total="sum", Average="mean", Median="median", Min="min", Max="max"
        ).reset_index()
        st.dataframe(stats, use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: NATIONAL STOCKOUT
# ═══════════════════════════════════════════════════════════════════════════════

def show_national_stockout():
    page_header("🌍 NATIONAL STOCKOUT FORECAST")

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From", value=datetime.now()-timedelta(days=30), key="ns_start")
    with col2:
        end_date = st.date_input("To", value=datetime.now(), key="ns_end")

    start_str   = start_date.strftime("%m/%d/%Y")
    end_str     = end_date.strftime("%m/%d/%Y")
    period_days = max((end_date - start_date).days, 1)

    day_type = st.radio(
        "📅 **Day Type for Daily Rate Calculation**",
        ["📆 Calendar Days", "💼 Business Days (Mon–Fri)"],
        horizontal=True, key="ns_day_type",
    )
    use_biz = "Business" in day_type

    depletion_mode = st.radio(
        "🚚 **Depletion Rate**",
        ["📊 Average Daily Loading", "🔥 Maximum Daily Loading (stress test)", "📊 Median Daily Loading"],
        key="ns_depletion",
    )
    use_max    = "Maximum" in depletion_mode
    use_median = "Median"  in depletion_mode

    exclude_tor = st.checkbox(
        "❌ Exclude TEMA OIL REFINERY (TOR) from LPG calculation",
        key="ns_tor",
        help="TOR LPG is often refinery-internal and should not count toward commercial supply runway.",
    )

    st.info("⚡ **2 API calls:** BDC Balance (current stock) + OMC Loadings (all released orders).")

    if st.button("⚡ FETCH & ANALYSE NATIONAL FUEL SUPPLY", use_container_width=True):
        _run_national_analysis(start_str, end_str, period_days,
                                use_max, use_median, use_biz, exclude_tor, depletion_mode)

    if st.session_state.get("ns_results"):
        _display_national_results()


def _run_national_analysis(start_str, end_str, period_days,
                            use_max, use_median, use_biz, exclude_tor, depletion_mode):
    col_bal = "ACTUAL BALANCE (LT\\KG)"
    eff_days = _count_days(start_str, end_str, use_biz)

    # Step 1 — BDC Balance
    with st.status("📡 Step 1/2 — Fetching BDC Balance…", expanded=True) as s1:
        pdf = fetch_pdf_bytes(NPA_CONFIG["BDC_BALANCE_URL"], _bdc_balance_params())
        if not pdf:
            st.error("❌ Balance fetch failed.")
            s1.update(label="❌ Failed", state="error")
            return
        bal_df = pd.DataFrame(StockBalanceScraper().parse_pdf_file(io.BytesIO(pdf)))
        if exclude_tor:
            mask = bal_df["BDC"].str.contains("TOR", case=False, na=False) & (bal_df["Product"]=="LPG")
            excl = bal_df[mask][col_bal].sum()
            bal_df = bal_df[~mask].copy()
            st.info(f"TOR LPG excluded ({excl:,.0f} LT removed)")
        bal_by_prod = bal_df.groupby("Product")[col_bal].sum()
        st.write(f"✅ {len(bal_df)} rows, {bal_df['BDC'].nunique()} BDCs")
        s1.update(label=f"✅ Step 1 done — {bal_df['BDC'].nunique()} BDCs", state="running")

    # Step 2 — OMC Loadings
    with st.status("🚚 Step 2/2 — Fetching OMC Loadings (chunked)…", expanded=True) as s2:
        n_weeks   = ceil(period_days / 7)
        prog_bar  = st.progress(0, text="Starting…")
        prog_text = st.empty()

        def _on_prog(done, total):
            prog_bar.progress(done/total, text=f"Week {done}/{total}")
            prog_text.caption(f"✅ {done}/{total} chunks done")

        omc_df = fetch_national_omc_loadings(start_str, end_str, progress_cb=_on_prog)
        prog_bar.progress(1.0, text="✅ Done")

        if omc_df.empty:
            omc_by_prod = pd.Series({"PREMIUM": 0.0, "GASOIL": 0.0, "LPG": 0.0})
            dep_label   = "No Data"
        else:
            filt = omc_df[omc_df["Product"].isin(["PREMIUM","GASOIL","LPG"])].copy()
            filt["Date"] = pd.to_datetime(filt["Date"], errors="coerce")
            daily_agg = filt.groupby(["Date","Product"])["Quantity"].sum().reset_index()
            if use_median:
                omc_by_prod = daily_agg.groupby("Product")["Quantity"].median()
                dep_label   = "Median Daily Loading"
            elif use_max:
                omc_by_prod = daily_agg.groupby("Product")["Quantity"].max()
                dep_label   = "Max Daily Loading"
            else:
                omc_by_prod = filt.groupby("Product")["Quantity"].sum()
                dep_label   = f"Avg Daily Loading ({eff_days}d)"
        s2.update(label=f"✅ Step 2 done — {len(omc_df):,} records", state="complete")

    # Build forecast rows
    rows = []
    for prod in ["PREMIUM","GASOIL","LPG"]:
        stock  = float(bal_by_prod.get(prod, 0))
        depl   = float(omc_by_prod.get(prod, 0))
        daily  = depl if (use_median or use_max) else (depl / eff_days if eff_days else 0)
        days   = stock / daily if daily > 0 else float("inf")
        rows.append({"product": prod, "display_name": PRODUCT_LABELS[prod],
                      "total_balance": stock, "omc_sales": depl,
                      "daily_rate": daily, "days_remaining": days})
    forecast_df = pd.DataFrame(rows)

    bdc_pivot = (
        bal_df.pivot_table(index="BDC", columns="Product", values=col_bal, aggfunc="sum", fill_value=0)
        .reset_index()
    )
    for p in ["GASOIL","LPG","PREMIUM"]:
        if p not in bdc_pivot.columns: bdc_pivot[p] = 0
    bdc_pivot["TOTAL"] = bdc_pivot[["GASOIL","LPG","PREMIUM"]].sum(axis=1)
    nat_total = bdc_pivot["TOTAL"].sum()
    bdc_pivot["Market Share %"] = (bdc_pivot["TOTAL"] / nat_total * 100).round(2)
    bdc_pivot = bdc_pivot.sort_values("TOTAL", ascending=False)

    st.session_state.ns_results = {
        "forecast_df": forecast_df, "bal_df": bal_df, "omc_df": omc_df,
        "bdc_pivot": bdc_pivot, "period_days": period_days, "eff_days": eff_days,
        "use_biz": use_biz, "dep_label": dep_label, "exclude_tor": exclude_tor,
        "start_str": start_str, "end_str": end_str,
        "n_bdcs": bal_df["BDC"].nunique(), "n_omc": len(omc_df),
    }
    save_national_snapshot(forecast_df, f"{period_days}d")
    st.success("✅ Done! Scroll down to see the forecast.")
    st.rerun()


def _display_national_results():
    res         = st.session_state.ns_results
    forecast_df = res["forecast_df"]
    bdc_pivot   = res["bdc_pivot"]
    omc_df      = res["omc_df"]
    dep_label   = res["dep_label"]
    eff_days    = res["eff_days"]
    use_biz     = res["use_biz"]
    day_badge   = "💼 Business Days" if use_biz else "📆 Calendar Days"

    st.markdown(f"<h3>🇬🇭 NATIONAL FUEL SUPPLY — {res['start_str']} → {res['end_str']}</h3>",
                unsafe_allow_html=True)
    st.caption(f"BDCs: {res['n_bdcs']} | OMC records: {res['n_omc']:,} | "
               f"Depletion: {dep_label} | Day type: {day_badge} ({eff_days}d)")

    st.markdown("### 🛢️ DAYS OF SUPPLY — NATIONAL FORECAST")
    cols = st.columns(3)
    for col, (_, row) in zip(cols, forecast_df.iterrows()):
        prod  = row["product"]
        days  = row["days_remaining"]
        color = PRODUCT_COLORS[prod]
        days_txt = f"{days:.1f}" if days != float("inf") else "∞"
        if days < 7:    border, status = "#ff0000", "🔴 CRITICAL"
        elif days < 14: border, status = "#ffaa00", "🟡 WARNING"
        elif days < 30: border, status = "#ff6600", "🟠 MONITOR"
        else:           border, status = "#00ff88", "🟢 HEALTHY"
        empty_dt = (datetime.now() + timedelta(days=days)).strftime("%d %b %Y") \
                    if days != float("inf") else "N/A"
        with col:
            st.markdown(f"""
            <div style='background:rgba(10,14,39,.85);padding:24px 16px;border-radius:16px;
                        border:2.5px solid {border};text-align:center;
                        box-shadow:0 0 18px {border}55;'>
                <div style='font-size:36px'>{PRODUCT_ICONS[prod]}</div>
                <div style='font-family:Orbitron;font-size:16px;color:{color};font-weight:700;
                             letter-spacing:2px;margin:8px 0'>{row["display_name"]}</div>
                <div style='font-family:Orbitron;font-size:48px;color:{border};font-weight:900;
                             line-height:1.1'>{days_txt}</div>
                <div style='color:#888;font-size:11px'>DAYS OF SUPPLY</div>
                <div style='color:{border};font-size:14px;font-weight:700;margin:4px 0'>{status}</div>
                <div style='border-top:1px solid rgba(255,255,255,.08);padding-top:10px;margin-top:10px;'>
                    <div style='color:#888;font-size:11px'>📦 {row["total_balance"]:,.0f} LT stock</div>
                    <div style='color:#888;font-size:11px'>📉 {row["daily_rate"]:,.0f} LT/day avg</div>
                    <div style='color:{border};font-size:12px;font-weight:700'>🗓️ Est. empty: {empty_dt}</div>
                </div>
            </div>""", unsafe_allow_html=True)

    section("📊 NATIONAL SUMMARY TABLE")
    rows = []
    for _, row in forecast_df.iterrows():
        days = row["days_remaining"]
        days_txt = f"{days:.1f}" if days != float("inf") else "∞"
        status = ("🔴 CRITICAL" if days < 7 else "🟡 WARNING" if days < 14
                  else "🟠 MONITOR" if days < 30 else "🟢 HEALTHY")
        empty = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d") \
                 if days != float("inf") else "N/A"
        rows.append({"Product": row["display_name"],
                      f"National Stock (LT)": f"{row['total_balance']:,.0f}",
                      f"{dep_label} (LT)":    f"{row['omc_sales']:,.0f}",
                      f"Daily Rate (LT/d)":   f"{row['daily_rate']:,.0f}",
                      "Days of Supply": days_txt, "Projected Empty": empty, "Status": status})
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    section("🏦 CURRENT STOCK BY BDC")
    disp = bdc_pivot.copy()
    for c in ["GASOIL","LPG","PREMIUM","TOTAL"]:
        disp[c] = disp[c].apply(lambda x: f"{x:,.0f}")
    disp["Market Share %"] = disp["Market Share %"].apply(lambda x: f"{x:.2f}%")
    st.dataframe(disp, use_container_width=True, hide_index=True)

    section("💾 EXPORT NATIONAL REPORT")
    if st.button("📄 GENERATE EXCEL REPORT", use_container_width=True, key="ns_export"):
        export_rows = []
        for _, row in forecast_df.iterrows():
            days = row["days_remaining"]
            export_rows.append({
                "Product":          row["display_name"],
                "National Stock":   row["total_balance"],
                f"{dep_label}":     row["omc_sales"],
                "Daily Depletion":  row["daily_rate"],
                "Days of Supply":   days if days != float("inf") else 9999,
                "Projected Empty":  (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
                                     if days != float("inf") else "N/A",
            })
        sheets = {"Stockout Forecast": pd.DataFrame(export_rows),
                  "Stock by BDC": bdc_pivot}
        if not omc_df.empty:
            sheets["OMC Loadings Detail"] = omc_df
        fname = f"national_stockout_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
        excel_download_button(None, fname, sheets=sheets)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: LIVE RUNWAY MONITOR
# ═══════════════════════════════════════════════════════════════════════════════

def show_live_runway_monitor():
    page_header("🔴 LIVE RUNWAY MONITOR",
                "Auto-refreshes every 60 minutes. Always shows the latest national supply runway.")

    with st.expander("⚙️ Alert Thresholds", expanded=False):
        col1, col2, col3 = st.columns(3)
        thresholds = {}
        for col, prod in zip([col1,col2,col3], ["PMS","AGO","LPG"]):
            with col:
                crit = st.number_input(f"{prod} Critical (days)", value=7,  min_value=1, max_value=60, key=f"lr_{prod}_crit")
                warn = st.number_input(f"{prod} Warning (days)",  value=14, min_value=1, max_value=60, key=f"lr_{prod}_warn")
                prod_key = {"PMS":"PREMIUM","AGO":"GASOIL","LPG":"LPG"}[prod]
                thresholds[prod_key] = (crit, warn)

    cola, colb, colc = st.columns([2,1,1])
    with cola: auto_refresh = st.checkbox("🔄 Auto-refresh every 60 min", value=False)
    with colb: period_d = st.number_input("Lookback days", value=30, min_value=1, max_value=90, key="lr_period")
    with colc: fetch_now = st.button("⚡ FETCH NOW", key="lr_fetch")

    should_fetch = fetch_now
    if auto_refresh:
        last = st.session_state.get("lr_last_fetch")
        if last is None or (datetime.now() - last).seconds > 3600:
            should_fetch = True

    if should_fetch:
        end_dt    = datetime.now()
        start_dt  = end_dt - timedelta(days=period_d)
        col_bal   = "ACTUAL BALANCE (LT\\KG)"
        with st.spinner("Fetching BDC Balance…"):
            pdf = fetch_pdf_bytes(NPA_CONFIG["BDC_BALANCE_URL"], _bdc_balance_params())
            if not pdf:
                st.error("❌ Balance fetch failed"); return
            bal_df = pd.DataFrame(StockBalanceScraper().parse_pdf_file(io.BytesIO(pdf)))
        with st.spinner(f"Fetching OMC Loadings ({period_d}d)…"):
            omc_df = fetch_national_omc_loadings(start_dt.strftime("%m/%d/%Y"), end_dt.strftime("%m/%d/%Y"))

        bal_by = bal_df.groupby("Product")[col_bal].sum() if not bal_df.empty else pd.Series()
        omc_by = (omc_df[omc_df["Product"].isin(["PREMIUM","GASOIL","LPG"])]
                  .groupby("Product")["Quantity"].sum() if not omc_df.empty else pd.Series())
        rows = []
        for prod in ["PREMIUM","GASOIL","LPG"]:
            stock = float(bal_by.get(prod, 0))
            dep   = float(omc_by.get(prod, 0))
            daily = dep / period_d if period_d else 0
            rows.append({"product": prod, "total_balance": stock,
                          "omc_sales": dep, "daily_rate": daily,
                          "days_remaining": stock/daily if daily else float("inf")})
        forecast_df = pd.DataFrame(rows)
        st.session_state.lr_forecast   = forecast_df
        st.session_state.lr_last_fetch = datetime.now()
        st.session_state.lr_period     = period_d
        save_national_snapshot(forecast_df, f"{period_d}d")

    if not st.session_state.get("lr_forecast") is not None and st.session_state.get("lr_forecast") is None:
        st.info("👆 Click **FETCH NOW** to load live status.")
        return

    if st.session_state.get("lr_forecast") is None:
        st.info("👆 Click **FETCH NOW** to load live status.")
        return

    forecast_df = st.session_state.lr_forecast
    last_t      = st.session_state.lr_last_fetch
    st.caption(f"Last updated: **{last_t:%d %b %Y %H:%M:%S}** | Lookback: {st.session_state.lr_period}d")

    cols = st.columns(3)
    any_crit = any_warn = False
    for col, (_, row) in zip(cols, forecast_df.iterrows()):
        prod = row["product"]
        days = row["days_remaining"]
        crit, warn = thresholds.get(prod, (7, 14))
        color = PRODUCT_COLORS[prod]
        if days == float("inf"):   border, status, emoji = "#888", "NO DATA", "⚫"
        elif days < crit:          border, status, emoji = "#ff0000", "CRITICAL", "🔴"; any_crit = True
        elif days < warn:          border, status, emoji = "#ffaa00", "WARNING",  "🟡"; any_warn = True
        elif days < 30:            border, status, emoji = "#ff6600", "MONITOR",  "🟠"
        else:                      border, status, emoji = "#00ff88", "HEALTHY",  "🟢"
        days_txt = f"{days:.1f}" if days != float("inf") else "∞"
        empty_dt = (datetime.now() + timedelta(days=days)).strftime("%d %b %Y") if days != float("inf") else "N/A"

        # Delta vs previous snapshot
        hist = load_all_snapshots()
        delta_html = ""
        if not hist.empty:
            prev = hist[hist["product"]==prod].sort_values("timestamp")
            if len(prev) >= 2:
                pd_ = prev.iloc[-2]["days_remaining"]
                if days != float("inf") and pd_ != float("inf"):
                    d = days - pd_
                    dcol = "#00ff88" if d > 0 else "#ff4444"
                    delta_html = f"<span style='color:{dcol};font-size:14px;'>{'↑' if d>0 else '↓'}{abs(d):.1f}d vs prev</span>"

        with col:
            st.markdown(f"""
            <div style='background:rgba(10,14,39,.9);padding:28px 18px;border-radius:18px;
                        border:3px solid {border};text-align:center;
                        box-shadow:0 0 25px {border}66;'>
                <div style='font-size:40px'>{PRODUCT_ICONS[prod]}</div>
                <div style='font-family:Orbitron;color:{color};font-size:16px;font-weight:700;
                             letter-spacing:2px;margin:8px 0'>{PRODUCT_LABELS[prod]}</div>
                <div style='font-size:13px;color:{border};font-weight:700;
                             letter-spacing:3px;margin-bottom:12px'>{emoji} {status}</div>
                <div style='font-family:Orbitron;font-size:64px;font-weight:900;
                             color:{border};line-height:1;text-shadow:0 0 20px {border}'>{days_txt}</div>
                <div style='color:#888;font-size:12px'>DAYS OF SUPPLY</div>
                {delta_html}
                <div style='border-top:1px solid rgba(255,255,255,.1);margin-top:14px;padding-top:10px;'>
                    <div style='color:#888;font-size:11px'>📦 {row["total_balance"]:,.0f} LT</div>
                    <div style='color:#888;font-size:11px'>📉 {row["daily_rate"]:,.0f} LT/day</div>
                    <div style='color:{border};font-size:12px;font-weight:700'>🗓️ {empty_dt}</div>
                </div>
            </div>""", unsafe_allow_html=True)

    st.markdown("---")
    if any_crit:
        st.error("🚨 **CRITICAL:** Immediate action required!")
    elif any_warn:
        st.warning("⚠️ **WARNING:** Plan replenishment now.")
    else:
        st.success("✅ All products at healthy supply levels.")

    if auto_refresh:
        import time; time.sleep(3600); st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: HISTORICAL TRENDS
# ═══════════════════════════════════════════════════════════════════════════════

def show_historical_trends():
    page_header("📉 HISTORICAL TRENDS",
                "Each National Stockout / Live Runway run saves a snapshot. Plotted here over time.")

    hist = load_all_snapshots()
    if hist.empty:
        st.info("No snapshots yet. Run **🔴 Live Runway Monitor** or **🌍 National Stockout** first.")
        return

    hist = hist.sort_values("timestamp")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Snapshots", hist["timestamp"].nunique())
    col2.metric("Earliest", hist["timestamp"].min().strftime("%d %b %Y"))
    col3.metric("Latest",   hist["timestamp"].max().strftime("%d %b %Y"))

    def _line_chart(y_col, title, y_title):
        fig = go.Figure()
        for prod in ["PREMIUM","GASOIL","LPG"]:
            pdata = hist[hist["product"]==prod].copy()
            if y_col == "days_remaining":
                pdata = pdata[pdata[y_col] != float("inf")]
            if pdata.empty: continue
            pdata = pdata.sort_values("timestamp")
            fig.add_trace(go.Scatter(
                x=pdata["timestamp"], y=pdata[y_col],
                mode="lines+markers", name=prod,
                line=dict(color=PRODUCT_COLORS[prod], width=2), marker=dict(size=6),
            ))
        if y_col == "days_remaining":
            fig.add_hline(y=7,  line_dash="dash", line_color="#ff0000", annotation_text="7d CRITICAL")
            fig.add_hline(y=14, line_dash="dash", line_color="#ffaa00", annotation_text="14d WARNING")
        fig.update_layout(
            title=dict(text=title, font=dict(color="#00ffff", family="Orbitron")),
            paper_bgcolor="rgba(10,14,39,.9)", plot_bgcolor="rgba(10,14,39,.9)",
            font=dict(color="white"), height=400,
            xaxis=dict(gridcolor="rgba(255,255,255,.05)"),
            yaxis=dict(gridcolor="rgba(255,255,255,.05)", title=y_title),
        )
        st.plotly_chart(fig, use_container_width=True)

    _line_chart("days_remaining",  "📈 Days of Supply Over Time",        "Days of Supply")
    _line_chart("total_balance",   "🛢️ National Stock Volume Over Time", "Stock (LT)")
    _line_chart("daily_rate",      "📉 Daily Depletion Rate Over Time",  "LT/day")

    section("📋 RAW SNAPSHOT TABLE")
    disp = hist.copy()
    disp["timestamp"]     = disp["timestamp"].dt.strftime("%Y-%m-%d %H:%M")
    disp["days_remaining"] = disp["days_remaining"].apply(lambda x: f"{x:.1f}" if x != float("inf") else "∞")
    for c in ["total_balance","daily_rate","omc_sales"]:
        disp[c] = disp[c].apply(lambda x: f"{x:,.0f}")
    st.dataframe(disp.rename(columns={
        "timestamp":"Time","period":"Period","product":"Product",
        "total_balance":"Stock (LT)","omc_sales":"OMC Loadings (LT)",
        "daily_rate":"Daily Rate (LT/d)","days_remaining":"Days of Supply",
    }), use_container_width=True, hide_index=True)

    if st.button("🗑️ Clear All Snapshots"):
        shutil.rmtree(SNAPSHOT_DIR, ignore_errors=True)
        st.success("Snapshots cleared.")
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: DEPOT STRESS MAP
# ═══════════════════════════════════════════════════════════════════════════════

def show_depot_stress_map():
    page_header("🗺️ DEPOT STRESS MAP",
                "Geographic view of Ghana's fuel depot stock levels.")

    col_bal = "ACTUAL BALANCE (LT\\KG)"
    if not st.session_state.get("bdc_records"):
        st.info("BDC Balance data needed.")
        if st.button("⚡ FETCH BDC BALANCE", key="dsm_fetch"):
            if fetch_and_store_bdc_balance():
                st.rerun()
        return

    bal_df  = pd.DataFrame(st.session_state.bdc_records)
    prod    = st.selectbox("Product", ["ALL","PREMIUM","GASOIL","LPG"], key="dsm_prod")
    if prod != "ALL":
        bal_df = bal_df[bal_df["Product"] == prod]

    depot_agg = (bal_df.groupby("DEPOT")[col_bal].sum()
                       .reset_index().rename(columns={col_bal:"stock","DEPOT":"depot"}))
    if depot_agg.empty:
        st.warning("No data."); return

    max_stock = depot_agg["stock"].max() or 1
    map_rows, unmatched = [], []

    for _, row in depot_agg.iterrows():
        coords = _guess_coords(row["depot"])
        if coords:
            pct = row["stock"] / max_stock * 100
            map_rows.append({"depot": row["depot"], "stock": row["stock"],
                              "lat": coords[0], "lon": coords[1], "pct": pct})
        else:
            unmatched.append(row["depot"])

    if map_rows:
        map_df = pd.DataFrame(map_rows)
        map_df["color"]     = map_df["pct"].apply(
            lambda p: "#ff0000" if p<10 else "#ffaa00" if p<25 else "#ffdd00" if p<50 else "#00ff88")
        map_df["status"]    = map_df["pct"].apply(
            lambda p: "🔴 CRITICAL" if p<10 else "🟡 LOW" if p<25 else "🟠 MODERATE" if p<50 else "🟢 HEALTHY")
        map_df["stock_fmt"] = map_df["stock"].apply(lambda x: f"{x:,.0f} LT")

        fig_map = go.Figure(go.Scattergeo(
            lat=map_df["lat"], lon=map_df["lon"],
            mode="markers+text",
            text=map_df["depot"].str[:20],
            textposition="top center",
            textfont=dict(color="white", size=10),
            marker=dict(
                size=map_df["pct"].clip(0, 100) * 0.5 + 12,
                color=map_df["color"], opacity=0.85,
                line=dict(width=2, color="white"),
            ),
            customdata=map_df[["stock_fmt","pct","status"]],
            hovertemplate="<b>%{text}</b><br>Stock: %{customdata[0]}<br>"
                           "Relative: %{customdata[1]:.1f}%<br>Status: %{customdata[2]}<extra></extra>",
        ))
        fig_map.update_layout(
            geo=dict(scope="africa", center=dict(lat=7.9, lon=-1.0), projection_scale=12,
                     showland=True, landcolor="rgba(22,33,62,.9)",
                     showocean=True, oceancolor="rgba(10,14,39,.95)",
                     showcoastlines=True, coastlinecolor="rgba(0,255,255,.4)", showframe=False,
                     bgcolor="rgba(10,14,39,0)"),
            paper_bgcolor="rgba(10,14,39,0)", height=520,
            margin=dict(l=0,r=0,t=0,b=0),
        )
        st.plotly_chart(fig_map, use_container_width=True)

        section("🏭 DEPOT STOCK RANKING")
        sdf = map_df.sort_values("stock")
        fig_bar = go.Figure(go.Bar(
            x=sdf["depot"], y=sdf["stock"],
            marker_color=sdf["color"],
            text=sdf["stock_fmt"], textposition="outside",
        ))
        fig_bar.update_layout(
            paper_bgcolor="rgba(10,14,39,.9)", plot_bgcolor="rgba(10,14,39,.9)",
            font=dict(color="white"), height=380,
            xaxis=dict(tickangle=-30),
            yaxis=dict(gridcolor="rgba(255,255,255,.05)", title="Stock (LT)"),
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    if unmatched:
        st.caption(f"⚠️ No coordinates for: {', '.join(set(unmatched))}")

    section("📋 FULL DEPOT TABLE")
    disp = depot_agg.copy()
    disp["stock"] = disp["stock"].apply(lambda x: f"{x:,.0f}")
    st.dataframe(disp.rename(columns={"depot":"Depot","stock":"Stock (LT)"}),
                 use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: DEMAND FORECAST
# ═══════════════════════════════════════════════════════════════════════════════

def show_demand_forecast():
    page_header("🔮 DEMAND FORECAST",
                "Weighted moving average projection of future fuel demand.")

    if st.session_state.get("omc_df", pd.DataFrame()).empty:
        st.warning("⚠️ OMC Loadings required. Fetch from 🚚 OMC LOADINGS first."); return

    df = st.session_state.omc_df.copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])
    if df.empty:
        st.warning("⚠️ No valid dates in OMC Loadings."); return

    col1, col2 = st.columns(2)
    with col1: weeks = st.slider("Forecast horizon (weeks)", 1, 12, 4)
    with col2: view  = st.radio("View", ["National by Product","By OMC"], horizontal=True)

    df["week"] = df["Date"].dt.to_period("W").apply(lambda p: p.start_time)

    def _wma_forecast(vals, n_proj, n_trend=4):
        n = len(vals)
        weights = [0.5**(n-1-i) for i in range(n)]
        wma = sum(w*v for w,v in zip(weights, vals)) / sum(weights)
        trend = (vals[-1]-vals[0]) / max(len(vals[-n_trend:])-1, 1) if n >= 2 else 0
        return wma, trend, [max(0, wma + trend*(i+1)) for i in range(n_proj)]

    if view == "National by Product":
        weekly = df.groupby(["week","Product"])["Quantity"].sum().reset_index()
        fig = go.Figure()
        summary = []
        future_weeks_ref = []
        for prod in ["PREMIUM","GASOIL","LPG"]:
            pdata = weekly[weekly["Product"]==prod].sort_values("week")
            if len(pdata) < 2: continue
            vals   = pdata["Quantity"].values
            wma, trend, proj = _wma_forecast(vals, weeks)
            last_wk = pdata["week"].iloc[-1]
            fw = [last_wk + timedelta(weeks=i+1) for i in range(weeks)]
            future_weeks_ref = fw
            col = PRODUCT_COLORS[prod]
            fig.add_trace(go.Scatter(x=pdata["week"], y=pdata["Quantity"],
                mode="lines+markers", name=f"{prod} actual",
                line=dict(color=col, width=2), marker=dict(size=7)))
            fig.add_trace(go.Scatter(x=fw, y=proj,
                mode="lines+markers", name=f"{prod} forecast",
                line=dict(color=col, width=2, dash="dash"), marker=dict(size=7, symbol="diamond")))
            summary.append({"Product": prod, "WMA (LT/wk)": f"{wma:,.0f}",
                             "Trend": f"{trend:+,.0f}/wk",
                             f"Wk+1 (LT)": f"{proj[0]:,.0f}",
                             f"Wk+{weeks} (LT)": f"{proj[-1]:,.0f}",
                             f"{weeks}wk Total": f"{sum(proj):,.0f}"})
        if future_weeks_ref:
            fig.add_vrect(x0=future_weeks_ref[0], x1=future_weeks_ref[-1],
                           fillcolor="rgba(255,0,255,.05)", layer="below", line_width=0,
                           annotation_text="FORECAST", annotation_font_color="#ff00ff")
        fig.update_layout(
            title=dict(text="Weekly OMC Loadings + Forecast",
                        font=dict(color="#00ffff", family="Orbitron")),
            paper_bgcolor="rgba(10,14,39,.9)", plot_bgcolor="rgba(10,14,39,.9)",
            font=dict(color="white"), height=440,
            xaxis=dict(gridcolor="rgba(255,255,255,.05)"),
            yaxis=dict(gridcolor="rgba(255,255,255,.05)", title="Volume (LT)"),
        )
        st.plotly_chart(fig, use_container_width=True)
        if summary:
            st.markdown("### 📋 FORECAST SUMMARY")
            st.dataframe(pd.DataFrame(summary), use_container_width=True, hide_index=True)
    else:
        prod_f = st.selectbox("Product", ["PREMIUM","GASOIL","LPG"])
        df_p   = df[df["Product"]==prod_f]
        top10  = df_p.groupby("OMC")["Quantity"].sum().sort_values(ascending=False).head(10).index.tolist()
        sel    = st.multiselect("Select OMCs", top10, default=top10[:5])
        weekly_omc = df_p.groupby(["week","OMC"])["Quantity"].sum().reset_index()
        palette = ["#00ffff","#ff00ff","#00ff88","#ffaa00","#ff6600",
                   "#ff4488","#44ffdd","#ffdd44","#aa44ff","#ff8844"]
        fig2 = go.Figure()
        rows2 = []
        for idx, omc in enumerate(sel):
            od = weekly_omc[weekly_omc["OMC"]==omc].sort_values("week")
            if od.empty: continue
            vals = od["Quantity"].values
            wma, trend, proj = _wma_forecast(vals, weeks)
            last_wk = od["week"].iloc[-1]
            fw = [last_wk + timedelta(weeks=i+1) for i in range(weeks)]
            col = palette[idx % len(palette)]
            fig2.add_trace(go.Scatter(x=od["week"], y=od["Quantity"],
                mode="lines+markers", name=omc[:20], line=dict(color=col, width=2)))
            fig2.add_trace(go.Scatter(x=fw, y=proj, mode="lines",
                line=dict(color=col, width=2, dash="dash"), showlegend=False))
            rows2.append({"OMC": omc, "WMA (LT/wk)": f"{wma:,.0f}",
                           "Trend": f"{trend:+,.0f}/wk",
                           "Wk+1": f"{proj[0]:,.0f}", f"{weeks}wk Total": f"{sum(proj):,.0f}"})
        fig2.update_layout(paper_bgcolor="rgba(10,14,39,.9)", plot_bgcolor="rgba(10,14,39,.9)",
                            font=dict(color="white"), height=440,
                            xaxis=dict(gridcolor="rgba(255,255,255,.05)"),
                            yaxis=dict(gridcolor="rgba(255,255,255,.05)", title="Volume (LT)"))
        st.plotly_chart(fig2, use_container_width=True)
        if rows2:
            st.markdown("### 📋 OMC FORECAST TABLE")
            st.dataframe(pd.DataFrame(rows2), use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: REORDER ALERTS
# ═══════════════════════════════════════════════════════════════════════════════

def show_reorder_alerts():
    page_header("⚠️ REORDER ALERTS",
                "Per-BDC stockout forecast with individual reorder recommendations.")

    has_bal = bool(st.session_state.get("bdc_records"))
    has_omc = not st.session_state.get("omc_df", pd.DataFrame()).empty
    if not has_bal: st.warning("⚠️ BDC Balance required — fetch from 🏦 BDC BALANCE first.")
    if not has_omc: st.warning("⚠️ OMC Loadings required — fetch from 🚚 OMC LOADINGS first.")
    if not has_bal or not has_omc:
        return

    col1, col2, col3 = st.columns(3)
    with col1: crit_d  = st.number_input("Critical (days)", value=5,  min_value=1, max_value=30)
    with col2: warn_d  = st.number_input("Warning (days)",  value=10, min_value=1, max_value=60)
    with col3: buf_d   = st.number_input("Reorder buffer",  value=7,  min_value=1, max_value=30)

    bal_df  = pd.DataFrame(st.session_state.bdc_records)
    omc_df  = st.session_state.omc_df.copy()
    col_bal = "ACTUAL BALANCE (LT\\KG)"

    omc_df["Date"] = pd.to_datetime(omc_df["Date"], errors="coerce")
    omc_df = omc_df.dropna(subset=["Date"])
    period_d = max((omc_df["Date"].max() - omc_df["Date"].min()).days, 1) if not omc_df.empty else 30

    bdc_stock = bal_df.groupby(["BDC","Product"])[col_bal].sum().reset_index().rename(
        columns={col_bal:"stock"})
    bdc_dep   = (omc_df[omc_df["Product"].isin(["PREMIUM","GASOIL","LPG"])]
                  .groupby(["BDC","Product"])["Quantity"].sum().reset_index()
                  .rename(columns={"Quantity":"depletion"}))
    bdc_dep["daily_rate"] = bdc_dep["depletion"] / period_d

    merged = bdc_stock.merge(bdc_dep, on=["BDC","Product"], how="left")
    merged["daily_rate"]    = merged["daily_rate"].fillna(0)
    merged["days_remaining"] = merged.apply(
        lambda r: r["stock"]/r["daily_rate"] if r["daily_rate"]>0 else float("inf"), axis=1)
    merged["reorder_qty"]   = merged.apply(
        lambda r: max(0, r["daily_rate"]*(warn_d+buf_d)-r["stock"]) if r["daily_rate"]>0 else 0, axis=1)

    def _status(d):
        if d == float("inf"): return "⚪ NO DATA"
        if d < crit_d:  return "🔴 CRITICAL"
        if d < warn_d:  return "🟡 WARNING"
        if d < 30:      return "🟠 MONITOR"
        return "🟢 HEALTHY"
    merged["status"] = merged["days_remaining"].apply(_status)

    crit_rows = merged[merged["days_remaining"] < crit_d]
    warn_rows = merged[(merged["days_remaining"] >= crit_d) & (merged["days_remaining"] < warn_d)]

    cols = st.columns(3)
    cols[0].metric("🔴 Critical BDC-Products", len(crit_rows))
    cols[1].metric("🟡 Warning BDC-Products",  len(warn_rows))
    cols[2].metric("BDCs Analysed", merged["BDC"].nunique())

    if not crit_rows.empty:
        st.error("🚨 CRITICAL — Immediate reorder required:")
        for _, r in crit_rows.sort_values("days_remaining").iterrows():
            st.markdown(f"**{r['BDC']}** — {r['Product']}: **{r['days_remaining']:.1f} days** | "
                        f"Reorder: **{r['reorder_qty']:,.0f} LT**")
    if not warn_rows.empty:
        st.warning("⚠️ WARNING — Plan reorder within 48h:")
        for _, r in warn_rows.sort_values("days_remaining").iterrows():
            st.markdown(f"**{r['BDC']}** — {r['Product']}: **{r['days_remaining']:.1f} days** | "
                        f"Reorder: **{r['reorder_qty']:,.0f} LT**")

    section("📋 FULL BDC REORDER TABLE")
    col1, col2 = st.columns(2)
    with col1: pf = st.selectbox("Product", ["ALL","PREMIUM","GASOIL","LPG"], key="ra_prod")
    with col2: sf = st.selectbox("Status",  ["ALL","🔴 CRITICAL","🟡 WARNING","🟠 MONITOR","🟢 HEALTHY","⚪ NO DATA"], key="ra_stat")

    disp = merged.copy()
    if pf != "ALL": disp = disp[disp["Product"]==pf]
    if sf != "ALL": disp = disp[disp["status"]==sf]
    disp = disp.sort_values("days_remaining")
    for col in ["stock","depletion","daily_rate","reorder_qty"]:
        disp[col] = disp[col].fillna(0).apply(lambda x: f"{x:,.0f}")
    disp["days_remaining"] = disp["days_remaining"].apply(
        lambda x: f"{x:.1f}" if x != float("inf") else "∞")
    st.dataframe(
        disp[["BDC","Product","stock","depletion","daily_rate","days_remaining","reorder_qty","status"]]
        .rename(columns={"stock":"Current Stock (LT)","depletion":"Period Depletion (LT)",
                          "daily_rate":"Daily Rate (LT/d)","days_remaining":"Days of Supply",
                          "reorder_qty":"Reorder Qty (LT)","status":"Status"}),
        use_container_width=True, hide_index=True,
    )

    section("💾 EXPORT")
    excel_download_button(merged, f"reorder_{datetime.now():%Y%m%d_%H%M%S}.xlsx",
                          sheets={"Reorder Report": merged})


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: WEEK-ON-WEEK
# ═══════════════════════════════════════════════════════════════════════════════

def show_week_on_week():
    page_header("📆 WEEK-ON-WEEK COMPARISON",
                "Compare two periods side-by-side: BDC, OMC, and product deltas.")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### 📘 Period A")
        a_start = st.date_input("A: From", value=datetime.now()-timedelta(days=14), key="wow_a_start")
        a_end   = st.date_input("A: To",   value=datetime.now()-timedelta(days=8),  key="wow_a_end")
    with col2:
        st.markdown("#### 📗 Period B")
        b_start = st.date_input("B: From", value=datetime.now()-timedelta(days=7), key="wow_b_start")
        b_end   = st.date_input("B: To",   value=datetime.now(),                   key="wow_b_end")

    if st.button("⚡ FETCH & COMPARE", key="wow_fetch"):
        with st.status("Fetching Period A…", expanded=True) as sa:
            df_a = fetch_national_omc_loadings(a_start.strftime("%m/%d/%Y"), a_end.strftime("%m/%d/%Y"))
            sa.update(label=f"✅ A: {len(df_a):,} records", state="complete")
        with st.status("Fetching Period B…", expanded=True) as sb:
            df_b = fetch_national_omc_loadings(b_start.strftime("%m/%d/%Y"), b_end.strftime("%m/%d/%Y"))
            sb.update(label=f"✅ B: {len(df_b):,} records", state="complete")
        st.session_state.wow_a = {"df": df_a, "label": f"{a_start} → {a_end}",
                                   "days": max((a_end - a_start).days, 1)}
        st.session_state.wow_b = {"df": df_b, "label": f"{b_start} → {b_end}",
                                   "days": max((b_end - b_start).days, 1)}
        st.rerun()

    if not st.session_state.get("wow_a"):
        st.info("👆 Select two periods and click **FETCH & COMPARE**.")
        return

    wa, wb = st.session_state.wow_a, st.session_state.wow_b
    df_a, df_b = wa["df"], wb["df"]

    section("🛢️ NATIONAL VOLUME BY PRODUCT")
    PRODS = ["PREMIUM","GASOIL","LPG"]
    vol_a = df_a[df_a["Product"].isin(PRODS)].groupby("Product")["Quantity"].sum() if not df_a.empty else pd.Series()
    vol_b = df_b[df_b["Product"].isin(PRODS)].groupby("Product")["Quantity"].sum() if not df_b.empty else pd.Series()

    prod_rows = []
    cols = st.columns(3)
    for col, prod in zip(cols, PRODS):
        va, vb = float(vol_a.get(prod,0)), float(vol_b.get(prod,0))
        delta  = vb - va
        pct    = (delta/va*100) if va > 0 else (100.0 if vb > 0 else 0.0)
        dcol   = "#00ff88" if delta >= 0 else "#ff4444"
        color  = PRODUCT_COLORS[prod]
        with col:
            st.markdown(f"""
            <div style='background:rgba(10,14,39,.85);padding:20px;border-radius:14px;
                        border:2px solid {color};text-align:center;'>
                <div style='font-family:Orbitron;color:{color};font-size:15px;font-weight:700;margin-bottom:10px'>{prod}</div>
                <div style='color:#888;font-size:11px'>{wa["label"]}</div>
                <div style='color:#e0e0e0;font-size:20px;font-weight:700'>{va:,.0f} LT</div>
                <div style='color:#888;font-size:11px;margin-top:6px'>{wb["label"]}</div>
                <div style='color:#fff;font-size:24px;font-weight:700'>{vb:,.0f} LT</div>
                <div style='color:{dcol};font-size:18px;font-weight:700;margin-top:8px'>
                    {'↑' if delta>=0 else '↓'} {abs(delta):,.0f} LT ({pct:+.1f}%)</div>
            </div>""", unsafe_allow_html=True)
        prod_rows.append({"Product":prod,"Period A":f"{va:,.0f}","Period B":f"{vb:,.0f}",
                           "Delta":f"{delta:+,.0f}","Change":f"{pct:+.1f}%"})

    section("🏭 BDC-LEVEL COMPARISON")
    prod_wow = st.selectbox("Product", ["ALL"]+PRODS, key="wow_prod")

    def _vol(df, prod):
        if df.empty or "BDC" not in df.columns: return pd.Series(dtype=float)
        f = df if prod=="ALL" else df[df["Product"]==prod]
        return f.groupby("BDC")["Quantity"].sum()

    bdc_a, bdc_b = _vol(df_a, prod_wow), _vol(df_b, prod_wow)
    all_b = sorted(set(bdc_a.index)|set(bdc_b.index))
    bdc_rows = []
    for bdc in all_b:
        va, vb = float(bdc_a.get(bdc,0)), float(bdc_b.get(bdc,0))
        delta  = vb - va
        pct    = (delta/va*100) if va > 0 else (100.0 if vb > 0 else 0.0)
        bdc_rows.append({"BDC":bdc,"Period A (LT)":va,"Period B (LT)":vb,"Delta (LT)":delta,"Change %":round(pct,1)})
    bdc_cmp = pd.DataFrame(bdc_rows).sort_values("Delta (LT)", ascending=False)

    fig = go.Figure()
    fig.add_trace(go.Bar(name=wa["label"], x=bdc_cmp["BDC"], y=bdc_cmp["Period A (LT)"],
                          marker_color="rgba(0,255,255,.6)"))
    fig.add_trace(go.Bar(name=wb["label"], x=bdc_cmp["BDC"], y=bdc_cmp["Period B (LT)"],
                          marker_color="rgba(255,0,255,.6)"))
    fig.update_layout(barmode="group", paper_bgcolor="rgba(10,14,39,.9)",
                       plot_bgcolor="rgba(10,14,39,.9)", font=dict(color="white"), height=420,
                       xaxis=dict(tickangle=-30), yaxis=dict(title="Volume (LT)"))
    st.plotly_chart(fig, use_container_width=True)

    disp_bdc = bdc_cmp.copy()
    for c in ["Period A (LT)","Period B (LT)"]:
        disp_bdc[c] = disp_bdc[c].apply(lambda x: f"{x:,.0f}")
    disp_bdc["Delta (LT)"] = disp_bdc["Delta (LT)"].apply(lambda x: f"{x:+,.0f}")
    disp_bdc["Change %"]   = disp_bdc["Change %"].apply(lambda x: f"{x:+.1f}%")
    st.dataframe(disp_bdc, use_container_width=True, hide_index=True)

    section("💾 EXPORT COMPARISON")
    sheets = {"Product Summary": pd.DataFrame(prod_rows), "BDC Comparison": bdc_cmp}
    if not df_a.empty: sheets["Period A Raw"] = df_a
    if not df_b.empty: sheets["Period B Raw"] = df_b
    excel_download_button(None, f"wow_{datetime.now():%Y%m%d_%H%M%S}.xlsx", sheets=sheets)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: WORLD RISK MONITOR
# ═══════════════════════════════════════════════════════════════════════════════

def show_world_monitor():
    page_header("🌍 WORLD RISK MONITOR", "Powered by WorldMonitor.app")
    st.info("🔴 LIVE: Real-time conflicts, military, nuclear, sanctions, weather, "
            "economic indicators, waterways, outages, natural disasters.")
    st.markdown("""
    <div style='background:rgba(22,33,62,.6);padding:40px;border-radius:15px;
                border:2px solid #00ffff;text-align:center;margin:20px 0;'>
        <div style='font-size:80px;margin-bottom:20px;'>🌍</div>
        <h3 style='color:#00ffff;margin:0;'>WORLD RISK MONITOR</h3>
        <p style='color:#888;margin:10px 0 20px;'>
            25 data layers: conflicts, nuclear, military, sanctions, weather,<br>
            infrastructure &amp; more. Built with WebGL (deck.gl).
        </p>
    </div>""", unsafe_allow_html=True)
    st.link_button("🌍 OPEN WORLD RISK MONITOR", WORLD_MONITOR_URL, use_container_width=True)
    st.markdown("""
    <div style='background:rgba(22,33,62,.6);padding:20px;border-radius:15px;border:2px solid #00ffff;'>
        <h3 style='color:#00ffff;'>How to use</h3>
        <ul>
            <li>🖱️ Drag to pan / scroll to zoom</li>
            <li>🔍 25 toggleable layers (Conflicts, Nuclear, Military, Sanctions…)</li>
            <li>📅 Time range locked to last 7 days</li>
        </ul>
    </div>""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN — ROUTING
# ═══════════════════════════════════════════════════════════════════════════════

PAGES = {
    "🏦 BDC BALANCE":       show_bdc_balance,
    "🚚 OMC LOADINGS":      show_omc_loadings,
    "📅 DAILY ORDERS":      show_daily_orders,
    "📊 MARKET SHARE":      show_market_share,
    "🎯 COMPETITIVE INTEL": show_competitive_intel,
    "📈 STOCK TRANSACTION": show_stock_transaction,
    "🧠 BDC INTELLIGENCE":  show_bdc_intelligence,
    "🌍 NATIONAL STOCKOUT": show_national_stockout,
    "🔴 LIVE RUNWAY MONITOR": show_live_runway_monitor,
    "📉 HISTORICAL TRENDS": show_historical_trends,
    "🗺️ DEPOT STRESS MAP":  show_depot_stress_map,
    "🔮 DEMAND FORECAST":   show_demand_forecast,
    "⚠️ REORDER ALERTS":    show_reorder_alerts,
    "📆 WEEK-ON-WEEK":      show_week_on_week,
    "🌍 WORLD RISK MONITOR": show_world_monitor,
}


def main():
    st.markdown("""
    <div style='text-align:center;padding:30px 0;'>
        <h1 style='font-size:72px;margin:0;'>⚡ NPA ENERGY ANALYTICS ⚡</h1>
        <p style='font-size:24px;color:#ff00ff;font-family:Orbitron,sans-serif;
                  letter-spacing:3px;margin-top:10px;'>FUEL THE FUTURE WITH DATA</p>
    </div>""", unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("<h2 style='text-align:center;'>🎯 MISSION CONTROL</h2>", unsafe_allow_html=True)
        sep = "─────── NEW ───────"
        options = list(PAGES.keys())
        options.insert(8, sep)
        choice = st.radio("SELECT YOUR DATA MISSION:", options, index=0)
        st.markdown("---")
        st.markdown("""
        <div style='text-align:center;padding:20px;background:rgba(255,0,255,.1);
                    border-radius:10px;border:2px solid #ff00ff;'>
            <h3>⚙️ SYSTEM STATUS</h3>
            <p style='color:#00ff88;font-size:20px;'>🟢 OPERATIONAL</p>
        </div>""", unsafe_allow_html=True)

    if choice in PAGES:
        PAGES[choice]()


if __name__ == "__main__":
    main()