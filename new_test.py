"""
NPA ENERGY ANALYTICS - STREAMLIT DASHBOARD
===========================================
INSTALLATION:
pip install streamlit pandas pdfplumber PyPDF2 openpyxl python-dotenv plotly streamlit-js-eval psutil requests

USAGE:
streamlit run npa_dashboard.py

ARCHITECTURE:
- All NPA API calls use the same robust fetch pattern from Stock Transaction
- BDC/Depot/Product IDs loaded from .env via BDC_MAP / DEPOT_MAP / PRODUCT_MAP
- OMC Loadings uses its own endpoint (national-level, no BDC/Depot ID required)
- Chunked parallel fetch only used where date range demands it (OMC Loadings national)
"""

import streamlit as st
import os
import re
import io
import json
import concurrent.futures
from datetime import datetime, timedelta
from math import ceil

import pandas as pd
import pdfplumber
import PyPDF2
import requests
from dotenv import load_dotenv
import plotly.graph_objects as go

import psutil

# ── memory badge ────────────────────────────────────────────────────────────
_proc = psutil.Process(os.getpid())
st.caption(f"Memory: {_proc.memory_info().rss / 1024 / 1024:.1f} MB")

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — ENV / ID MAPPINGS
# ══════════════════════════════════════════════════════════════════════════════

def load_bdc_mappings():
    mappings = {}
    for key, value in os.environ.items():
        if key.startswith('BDC_'):
            name = key[4:].replace('_', ' ')
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


def load_depot_mappings():
    mappings = {}
    for key, value in os.environ.items():
        if key.startswith('DEPOT_'):
            name = key[6:].replace('_', ' ')
            if "BOST " in name and name != "BOST GLOBAL DEPOT":
                parts = name.split(' ', 1)
                if len(parts) == 2:
                    name = f"{parts[0]} - {parts[1]}"
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
            elif "BLUE_OCEAN_INVESTMENT_LTD_KOTOKA_AIRPORT_ATK" in key:
                name = "BLUE OCEAN INVESTMENT LTD-KOTOKA AIRPORT (ATK)"
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


def load_product_mappings():
    return {
        "PMS":    int(os.getenv('PRODUCT_PREMIUM_ID', '12')),
        "Gasoil": int(os.getenv('PRODUCT_GASOIL_ID',  '14')),
        "LPG":    int(os.getenv('PRODUCT_LPG_ID',     '28')),
    }


BDC_MAP     = load_bdc_mappings()
DEPOT_MAP   = load_depot_mappings()
PRODUCT_MAP = load_product_mappings()   # PMS / Gasoil / LPG → integer IDs

PRODUCT_OPTIONS = ["PMS", "Gasoil", "LPG"]

# Display name → balance product name (used in stockout cross-matching)
PRODUCT_BALANCE_MAP = {
    "PMS":    "PREMIUM",
    "Gasoil": "GASOIL",
    "LPG":    "LPG",
}

NPA_CONFIG = {
    'COMPANY_ID':        os.getenv('NPA_COMPANY_ID',        '1'),
    'USER_ID':           os.getenv('NPA_USER_ID',           '123292'),
    'APP_ID':            os.getenv('NPA_APP_ID',            '3'),
    'ITS_FROM_PERSOL':   os.getenv('NPA_ITS_FROM_PERSOL',   'Persol Systems Limited'),
    'BDC_BALANCE_URL':   os.getenv('NPA_BDC_BALANCE_URL',
                             'https://iml.npa-enterprise.com/NPAAPILIVE/Home/CreateProductBalance'),
    'OMC_LOADINGS_URL':  os.getenv('NPA_OMC_LOADINGS_URL',
                             'https://iml.npa-enterprise.com/NewNPA/home/CreateOrdersReport'),
    'DAILY_ORDERS_URL':  os.getenv('NPA_DAILY_ORDERS_URL',
                             'https://iml.npa-enterprise.com/NewNPA/home/CreateDailyOrderReport'),
    'STOCK_TXN_URL':     os.getenv('NPA_STOCK_TRANSACTION_URL',
                             'https://iml.npa-enterprise.com/NewNPA/home/CreateStockTransactionReport'),
    'OMC_NAME':          os.getenv('OMC_NAME', 'OILCORP ENERGIA LIMITED'),
}

WORLD_MONITOR_URL = os.getenv(
    'WORLD_MONITOR_URL',
    'https://www.worldmonitor.app/?lat=20.0000&lon=0.0000&zoom=1.00&view=global'
    '&timeRange=7d&layers=conflicts%2Cbases%2Chotspots%2Cnuclear%2Csanctions%2C'
    'weather%2Ceconomic%2Cwaterways%2Coutages%2Cmilitary%2Cnatural%2CiranAttacks'
)

VESSEL_SHEET_URL = "https://docs.google.com/spreadsheets/d/1z-L79N22rU3p6wLw1CEVWDIw6QSwA5CH/edit?rtpof=true"

VESSEL_CONVERSION_FACTORS = {
    'PREMIUM': 1324.50,
    'GASOIL':  1183.00,
    'LPG':     1000.00,
    'NAPHTHA':  800.00,
}
VESSEL_PRODUCT_MAPPING = {
    'PMS': 'PREMIUM', 'GASOLINE': 'PREMIUM',
    'AGO': 'GASOIL',  'GASOIL':   'GASOIL',
    'LPG': 'LPG',     'BUTANE':   'LPG',
    'NAPHTHA': 'NAPHTHA',
}
VESSEL_MONTH_MAPPING = {
    'Jan':'JAN','Feb':'FEB','Mar':'MAR','Apr':'APR',
    'May':'MAY','Jun':'JUN','Jul':'JUL','Aug':'AUG',
    'Sep':'SEP','Oct':'OCT','Nov':'NOV','Dec':'DEC',
}

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — CORE HTTP FETCH  (single authoritative pattern)
# ══════════════════════════════════════════════════════════════════════════════

_NPA_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept': 'application/pdf,text/html,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
}


def _fetch_npa_pdf(url: str, params: dict, timeout: int = 45) -> bytes | None:
    """
    Single authoritative NPA fetch.  Returns raw PDF bytes or None.
    Raises nothing — caller checks return value.
    """
    try:
        r = requests.get(url, params=params, headers=_NPA_HEADERS, timeout=timeout)
        r.raise_for_status()
        if r.content[:4] == b'%PDF':
            return r.content
        return None
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — BDC BALANCE  (stock balance scraper + fetch)
# ══════════════════════════════════════════════════════════════════════════════

class StockBalanceScraper:
    ALLOWED = {"PREMIUM", "GASOIL", "LPG"}

    def __init__(self):
        self.output_dir = os.path.join(os.getcwd(), "bdc_stock_dataset")
        os.makedirs(self.output_dir, exist_ok=True)
        product_alt = "|".join(sorted(self.ALLOWED))
        self._product_re = re.compile(
            rf"^({product_alt})\s+([\d,]+\.\d{{2}})\s+(-?[\d,]+\.\d{{2}})$",
            re.IGNORECASE
        )
        self._bost_global_re = re.compile(r"\bBOST\s*GLOBAL\s*DEPOT\b", re.IGNORECASE)

    @staticmethod
    def _norm(text: str) -> str:
        return re.sub(r"\s+", " ", (text or "").strip())

    def _norm_bdc(self, bdc: str) -> str:
        clean = self._norm(bdc)
        up = self._norm(clean.upper().replace("-", " ").replace("_", " "))
        return "BOST" if up.startswith("BOST") else clean

    def _is_bost_labeled_depot(self, depot: str) -> bool:
        d = self._norm(depot or "").replace("-", " ")
        return self._norm(d).upper().startswith("BOST ")

    def _is_bost_global(self, depot: str) -> bool:
        return bool(self._bost_global_re.search(self._norm(depot or "")))

    @staticmethod
    def _parse_date(line: str) -> str | None:
        m = re.search(r'(\w+\s+\d{1,2}\s*,\s*\d{4})', line)
        if m:
            try:
                return datetime.strptime(
                    m.group(1).replace(" ,", ","), '%B %d, %Y'
                ).strftime('%Y/%m/%d')
            except ValueError:
                pass
        return None

    def _append(self, records, date, bdc, depot, product, actual, available):
        product = (product or "").upper()
        if product not in self.ALLOWED:
            return
        if self._is_bost_labeled_depot(depot) and not self._is_bost_global(depot):
            return
        if actual <= 0:
            return
        records.append({
            'Date':                          date,
            'BDC':                           self._norm_bdc(bdc),
            'DEPOT':                         self._norm(depot),
            'Product':                       product,
            'ACTUAL BALANCE (LT\\KG)':       actual,
            'AVAILABLE BALANCE (LT\\KG)':    available,
        })

    def parse_pdf_bytes(self, pdf_bytes: bytes) -> list:
        return self.parse_pdf_file(io.BytesIO(pdf_bytes))

    def parse_pdf_file(self, pdf_file) -> list:
        records = []
        try:
            reader = PyPDF2.PdfReader(pdf_file)
            bdc = depot = date = None
            for page in reader.pages:
                text = page.extract_text() or ""
                for line in [ln.strip() for ln in text.split('\n') if ln.strip()]:
                    up = line.upper()
                    if 'DATE AS AT' in up:
                        d = self._parse_date(line)
                        if d:
                            date = d
                    if up.startswith('BDC :') or up.startswith('BDC:'):
                        bdc = re.sub(r'^BDC\s*:\s*', '', line, flags=re.IGNORECASE).strip()
                    if up.startswith('DEPOT :') or up.startswith('DEPOT:'):
                        depot = re.sub(r'^DEPOT\s*:\s*', '', line, flags=re.IGNORECASE).strip()
                    if bdc and depot and date:
                        m = self._product_re.match(line)
                        if m:
                            self._append(
                                records, date, bdc, depot,
                                m.group(1),
                                float(m.group(2).replace(',', '')),
                                float(m.group(3).replace(',', '')),
                            )
        except Exception as e:
            st.error(f"Balance PDF parse error: {e}")
        return records

    def save_to_excel(self, records, filename=None) -> str | None:
        if not records:
            return None
        filename = filename or f"stock_balance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        path = os.path.join(self.output_dir, os.path.basename(filename))
        df = pd.DataFrame(records).sort_values(
            ['Product', 'BDC', 'DEPOT', 'Date'], ignore_index=True)
        with pd.ExcelWriter(path, engine='openpyxl') as w:
            df.to_excel(w, index=False, sheet_name='Stock Balance')
            for prod in ['LPG', 'PREMIUM', 'GASOIL']:
                dff = df[df['Product'].str.upper() == prod]
                dff.to_excel(w, index=False, sheet_name=prod)
        return path


def fetch_bdc_balance() -> list:
    """Fetch BDC Balance using the standard NPA fetch pattern."""
    params = {
        'lngCompanyId':    NPA_CONFIG['COMPANY_ID'],
        'strITSfromPersol': NPA_CONFIG['ITS_FROM_PERSOL'],
        'strGroupBy':      'BDC',
        'strGroupBy1':     'DEPOT',
        'strQuery1': '', 'strQuery2': '', 'strQuery3': '', 'strQuery4': '',
        'strPicHeight': '1', 'szPicWeight': '1',
        'lngUserId': NPA_CONFIG['USER_ID'],
        'intAppId':  NPA_CONFIG['APP_ID'],
    }
    pdf_bytes = _fetch_npa_pdf(NPA_CONFIG['BDC_BALANCE_URL'], params)
    if not pdf_bytes:
        return []
    return StockBalanceScraper().parse_pdf_bytes(pdf_bytes)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — OMC LOADINGS  (national orders report — no BDC/Depot ID)
# ══════════════════════════════════════════════════════════════════════════════

_OMC_SKIP = [
    "ORDER REPORT", "National Petroleum Authority", "ORDER NUMBER",
    "ORDER DATE", "ORDER STATUS", "BDC:", "Total for :", "Printed By :",
    "Page ", "BRV NUMBER", "VOLUME",
]
_OMC_LOADED = {"Released", "Submitted"}
_OMC_PRODUCT_MAP = {"AGO": "GASOIL", "PMS": "PREMIUM", "LPG": "LPG"}
_OMC_COLUMNS = ["Date", "OMC", "Truck", "Product", "Quantity", "Price",
                "Depot", "Order Number", "BDC"]


def _omc_extract_depot(line: str) -> str | None:
    m = re.search(r"DEPOT:([^-\n]+)", line)
    return m.group(1).strip() if m else None


def _omc_extract_bdc(line: str) -> str | None:
    m = re.search(r"BDC:([^\n]+)", line)
    return m.group(1).strip() if m else None


def _omc_detect_product(line: str) -> str:
    raw = "LPG" if "LPG" in line else "AGO" if "AGO" in line else "PMS"
    return _OMC_PRODUCT_MAP.get(raw, raw)


def _omc_find_loaded_idx(tokens: list) -> int | None:
    for i, t in enumerate(tokens):
        if t in _OMC_LOADED:
            return i
    return None


def _omc_parse_line(line, product, depot, bdc) -> dict | None:
    tokens = line.split()
    if len(tokens) < 6:
        return None
    idx = _omc_find_loaded_idx(tokens)
    if idx is None or idx < 2:
        return None
    try:
        date_str = tokens[0]
        order_num = tokens[1]
        volume = float(tokens[-1].replace(",", ""))
        price  = float(tokens[-2].replace(",", ""))
        brv    = tokens[-3]
        company = " ".join(tokens[idx + 1:-3]).strip()
        try:
            date_obj = datetime.strptime(date_str, "%d-%b-%Y")
            date_str = date_obj.strftime("%Y/%m/%d")
        except ValueError:
            pass
        return {
            "Date": date_str, "OMC": company, "Truck": brv,
            "Product": product, "Quantity": volume, "Price": price,
            "Depot": depot, "Order Number": order_num, "BDC": bdc,
        }
    except Exception:
        return None


def _omc_parse_pdf_bytes(pdf_bytes: bytes) -> pd.DataFrame:
    rows = []
    depot = bdc = ""
    product = "PREMIUM"
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or page.extract_text(x_tolerance=2, y_tolerance=2)
                if not text:
                    continue
                for raw in text.split("\n"):
                    line = raw.strip()
                    if not line:
                        continue
                    if "DEPOT:" in line:
                        d = _omc_extract_depot(line)
                        if d:
                            depot = d
                        continue
                    if "BDC:" in line:
                        b = _omc_extract_bdc(line)
                        if b:
                            bdc = b
                        continue
                    if "PRODUCT" in line:
                        product = _omc_detect_product(line)
                        continue
                    if any(h in line for h in _OMC_SKIP):
                        continue
                    if any(kw in line for kw in _OMC_LOADED):
                        row = _omc_parse_line(line, product, depot, bdc)
                        if row:
                            rows.append(row)
    except Exception as e:
        st.error(f"OMC Loadings PDF parse error: {e}")
        return pd.DataFrame(columns=_OMC_COLUMNS)
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=_OMC_COLUMNS)
    for col in _OMC_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[_OMC_COLUMNS].drop_duplicates()
    try:
        df = df.assign(
            _ds=pd.to_datetime(df["Date"], format="%Y/%m/%d", errors="coerce")
        ).sort_values("_ds").drop(columns=["_ds"]).reset_index(drop=True)
    except Exception:
        df = df.reset_index(drop=True)
    return df


def fetch_omc_loadings(start_str: str, end_str: str,
                       bdc_name: str = "",
                       progress_cb=None) -> pd.DataFrame:
    """
    Fetch OMC Loadings for a date range.
    For short ranges (≤7 days) — single call.
    For longer ranges — chunked weekly parallel fetch (same pattern, just repeated).
    bdc_name is passed to strGroupBy1 for single-BDC queries (OMC Loadings page).
    For national queries pass bdc_name="" to get all BDCs.
    """
    fmt = "%m/%d/%Y"
    d_start = datetime.strptime(start_str, fmt)
    d_end   = datetime.strptime(end_str,   fmt)
    total_days = (d_end - d_start).days

    # Build weekly windows
    windows = []
    cursor = d_start
    while cursor <= d_end:
        chunk_end = min(cursor + timedelta(days=6), d_end)
        windows.append((cursor.strftime(fmt), chunk_end.strftime(fmt)))
        cursor = chunk_end + timedelta(days=1)

    def _fetch_window(w_start, w_end):
        params = {
            'lngCompanyId':    NPA_CONFIG['COMPANY_ID'],
            'szITSfromPersol': 'persol',
            'strGroupBy':      'BDC',
            'strGroupBy1':     bdc_name,
            'strQuery1':       ' and iorderstatus=4',
            'strQuery2':       w_start,
            'strQuery3':       w_end,
            'strQuery4':       '',
            'strPicHeight':    '',
            'strPicWeight':    '',
            'intPeriodID':     '4',
            'iUserId':         NPA_CONFIG['USER_ID'],
            'iAppId':          NPA_CONFIG['APP_ID'],
        }
        pdf_bytes = _fetch_npa_pdf(NPA_CONFIG['OMC_LOADINGS_URL'], params, timeout=60)
        if not pdf_bytes:
            return pd.DataFrame()
        return _omc_parse_pdf_bytes(pdf_bytes)

    if len(windows) == 1:
        df = _fetch_window(windows[0][0], windows[0][1])
        if progress_cb:
            progress_cb(1, 1)
        return df

    # Parallel chunked
    frames = []
    completed = 0
    total = len(windows)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        future_map = {ex.submit(_fetch_window, ws, we): (ws, we) for ws, we in windows}
        for future in concurrent.futures.as_completed(future_map):
            completed += 1
            try:
                chunk = future.result()
                if not chunk.empty:
                    frames.append(chunk)
            except Exception:
                pass
            if progress_cb:
                progress_cb(completed, total)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    dedup_cols = [c for c in ['Date', 'Order Number', 'Truck', 'Product', 'Depot', 'BDC']
                  if c in combined.columns]
    combined = combined.drop_duplicates(subset=dedup_cols if dedup_cols else None)
    sort_cols = [c for c in ['Date', 'BDC', 'Order Number'] if c in combined.columns]
    if sort_cols:
        combined = combined.sort_values(sort_cols).reset_index(drop=True)
    return combined


def save_omc_to_excel(df: pd.DataFrame, filename: str = None) -> str:
    out_dir = os.path.join(os.getcwd(), "omc_loadings")
    os.makedirs(out_dir, exist_ok=True)
    filename = filename or f"npa_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    path = os.path.join(out_dir, filename)
    df_f = df[df["Product"].isin(["PREMIUM", "GASOIL", "LPG"])].copy()
    pivot = (
        df_f.pivot_table(index="BDC", columns="Product",
                         values="Quantity", aggfunc="sum", fill_value=0)
        .reset_index()
    ) if not df_f.empty else pd.DataFrame(columns=["BDC", "GASOIL", "LPG", "PREMIUM"])
    if not df_f.empty:
        pcols = [c for c in pivot.columns if c in ["PREMIUM", "GASOIL", "LPG"]]
        pivot["Total"] = pivot[pcols].sum(axis=1)
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="All Orders", index=False)
        for prod in ["PREMIUM", "GASOIL", "LPG"]:
            df[df["Product"] == prod].to_excel(w, sheet_name=prod, index=False)
        pivot.to_excel(w, sheet_name="BDC Summary", index=False)
    return path


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — DAILY ORDERS  (same pattern, different endpoint)
# ══════════════════════════════════════════════════════════════════════════════

_DAILY_PRODUCT_MAP = {
    "PMS": "PREMIUM", "AGO": "GASOIL", "LPG": "LPG",
    "RFO": "RFO", "ATK": "ATK", "AVIATION": "ATK",
    "PREMIX": "PREMIX", "MGO": "GASOIL", "KEROSENE": "KEROSENE",
}


def _daily_clean_currency(s: str) -> float:
    if not s:
        return 0.0
    try:
        return float(s.replace(",", "").strip())
    except Exception:
        return 0.0


def _daily_product_category(text: str) -> str:
    up = text.upper()
    if "AVIATION" in up or "TURBINE" in up:
        return "ATK"
    if "RFO" in up:
        return "RFO"
    if "PREMIX" in up:
        return "PREMIX"
    if "LPG" in up:
        return "LPG"
    if "AGO" in up or "MGO" in up or "GASOIL" in up:
        return "GASOIL"
    if "PMS" in up or "PREMIUM" in up:
        return "PREMIUM"
    return "PREMIUM"


def _daily_parse_line(line: str, last_date: str | None) -> dict | None:
    line = line.strip()
    pv = re.search(r"(\d{1,4}\.\d{2,4})\s+(\d{1,3}(?:,\d{3})*\.\d{2})$", line)
    if not pv:
        return None
    price  = _daily_clean_currency(pv.group(1))
    volume = _daily_clean_currency(pv.group(2))
    remainder = line[:pv.start()].strip()
    tokens = remainder.split()
    if not tokens:
        return None
    brv = tokens[-1]
    remainder = " ".join(tokens[:-1])
    date_val = last_date
    dm = re.search(r"(\d{2}/\d{2}/\d{4})", remainder)
    if dm:
        date_val = dm.group(1)
        try:
            date_val = datetime.strptime(date_val, "%d/%m/%Y").strftime("%Y/%m/%d")
        except ValueError:
            pass
        remainder = remainder.replace(dm.group(1), "").strip()
    product = _daily_product_category(line)
    noise = ["PMS","AGO","LPG","RFO","ATK","PREMIX","FOREIGN",
             "(Retail Outlets)","Retail","Outlets","MGO","Local",
             "Additivated","Differentiated","MINES","Cell Sites",
             "Turbine","Kerosene"]
    order_tokens = [
        t for t in remainder.split()
        if not any(n.upper() in t.upper() or t in ["(",")","-"] for n in noise)
    ]
    return {
        "Date":         date_val,
        "Order Number": " ".join(order_tokens).strip() or remainder,
        "Product":      product,
        "Truck":        brv,
        "Price":        price,
        "Quantity":     volume,
    }


def _daily_simplify_bdc(df: pd.DataFrame) -> pd.DataFrame:
    if "BDC" not in df.columns or df.empty:
        return df
    mapping = {
        name: " ".join(name.split()[:2]).upper()
        for name in df["BDC"].unique() if name
    }
    df["BDC"] = df["BDC"].map(mapping)
    return df


def _daily_parse_pdf_bytes(pdf_bytes: bytes) -> pd.DataFrame:
    rows = []
    ctx = {"Depot": "Unknown Depot", "BDC": "Unknown BDC",
           "Status": "Unknown Status", "Date": None}
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text(x_tolerance=2, y_tolerance=2)
                if not text:
                    continue
                for line in text.split('\n'):
                    clean = line.strip()
                    if not clean:
                        continue
                    if clean.startswith("DEPOT:"):
                        raw = clean.replace("DEPOT:", "").strip()
                        ctx["Depot"] = (
                            "BOST Global"
                            if raw.startswith("BOST") or "TAKORADI BLUE OCEAN" in raw
                            else raw
                        )
                        continue
                    if clean.startswith("BDC:"):
                        ctx["BDC"] = clean.replace("BDC:", "").strip()
                        continue
                    if "Order Status" in clean:
                        parts = clean.split(":")
                        if len(parts) > 1:
                            ctx["Status"] = parts[-1].strip()
                        continue
                    if not re.search(r"\d{2}$", clean):
                        continue
                    row_data = _daily_parse_line(clean, ctx["Date"])
                    if row_data:
                        if row_data["Date"]:
                            ctx["Date"] = row_data["Date"]
                        rows.append({
                            "Date":         row_data["Date"],
                            "Truck":        row_data["Truck"],
                            "Product":      row_data["Product"],
                            "Quantity":     row_data["Quantity"],
                            "Price":        row_data["Price"],
                            "Depot":        ctx["Depot"],
                            "Order Number": row_data["Order Number"],
                            "BDC":          ctx["BDC"],
                            "Status":       ctx["Status"],
                        })
    except Exception as e:
        st.error(f"Daily Orders PDF parse error: {e}")
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return _daily_simplify_bdc(df) if not df.empty else df


def fetch_daily_orders(start_str: str, end_str: str) -> pd.DataFrame:
    """Fetch Daily Orders using the standard NPA fetch pattern."""
    params = {
        'lngCompanyId':  NPA_CONFIG['COMPANY_ID'],
        'szITSfromPersol': 'persol',
        'strGroupBy':    'DEPOT',
        'strGroupBy1':   '',
        'strQuery1':     '',
        'strQuery2':     start_str,
        'strQuery3':     end_str,
        'strQuery4':     '',
        'strPicHeight':  '1',
        'strPicWeight':  '1',
        'intPeriodID':   '-1',
        'iUserId':       NPA_CONFIG['USER_ID'],
        'iAppId':        NPA_CONFIG['APP_ID'],
    }
    pdf_bytes = _fetch_npa_pdf(NPA_CONFIG['DAILY_ORDERS_URL'], params)
    if not pdf_bytes:
        return pd.DataFrame()
    return _daily_parse_pdf_bytes(pdf_bytes)


def save_daily_orders_excel(df: pd.DataFrame, filename: str = None) -> str:
    out_dir = os.path.join(os.getcwd(), "daily_orders")
    os.makedirs(out_dir, exist_ok=True)
    filename = filename or f"daily_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    path = os.path.join(out_dir, filename)
    pivot = (
        df.pivot_table(index="BDC", columns="Product",
                       values="Quantity", aggfunc="sum", fill_value=0)
        .reset_index()
    ) if not df.empty else pd.DataFrame()
    if not pivot.empty:
        pcols = [c for c in pivot.columns if c != "BDC"]
        pivot["Grand Total"] = pivot[pcols].sum(axis=1)
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="All Orders", index=False)
        if not pivot.empty:
            pivot.to_excel(w, sheet_name="Summary by BDC", index=False)
    return path


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — STOCK TRANSACTION  (the original working pattern)
# ══════════════════════════════════════════════════════════════════════════════

_TXN_DESCRIPTIONS = sorted([
    'Balance b/fwd', 'Stock Take', 'Sale',
    'Custody Transfer In', 'Custody Transfer Out', 'Product Outturn',
], key=len, reverse=True)

_TXN_SKIP = (
    'national petroleum authority', 'stock transaction report',
    'bdc :', 'depot :', 'product :', 'printed by', 'printed on',
    'date trans #', 'actual stock balance', 'stock commitments',
    'available stock balance', 'last stock update', 'i.t.s from',
)


def _txn_parse_num(s: str):
    s = s.strip()
    neg = s.startswith('(') and s.endswith(')')
    try:
        val = int(s.strip('()').replace(',', ''))
        return -val if neg else val
    except ValueError:
        return None


def _txn_parse_line(line: str) -> dict | None:
    line = line.strip()
    if not re.match(r'^\d{2}/\d{2}/\d{4}\b', line):
        return None
    parts = line.split()
    date  = parts[0]
    trans = parts[1] if len(parts) > 1 else ''
    rest  = line[len(date):].strip()[len(trans):].strip()
    description = after_desc = None
    after_desc  = rest
    for desc in _TXN_DESCRIPTIONS:
        if rest.lower().startswith(desc.lower()):
            description = desc
            after_desc  = rest[len(desc):].strip()
            break
    if description is None or description == 'Balance b/fwd':
        return None
    nums = re.findall(r'\([\d,]+\)|[\d,]+', after_desc)
    if len(nums) < 2:
        return None
    volume  = _txn_parse_num(nums[-2])
    balance = _txn_parse_num(nums[-1])
    trail = re.search(
        re.escape(nums[-2]) + r'\s+' + re.escape(nums[-1]) + r'\s*$',
        after_desc
    )
    account = after_desc[:trail.start()].strip() if trail else ' '.join(after_desc.split()[:-2])
    return {
        'Date':        date,
        'Trans #':     trans,
        'Description': description,
        'Account':     account,
        'Volume':      volume  if volume  is not None else 0,
        'Balance':     balance if balance is not None else 0,
    }


def _txn_parse_pdf_bytes(pdf_bytes: bytes) -> list:
    records = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue
                for raw in text.split("\n"):
                    line = raw.strip()
                    if not line:
                        continue
                    if line.strip().lower().startswith(_TXN_SKIP):
                        continue
                    if re.match(r'^\d{1,2}\s+\w+,\s+\d{4}', line.strip()):
                        continue
                    row = _txn_parse_line(line)
                    if row:
                        records.append(row)
    except Exception as e:
        st.error(f"Stock Transaction PDF parse error: {e}")
    return records


def fetch_stock_transaction(bdc_name: str, depot_name: str, product_display: str,
                             start_str: str, end_str: str) -> pd.DataFrame:
    """
    Fetch Stock Transaction using BDC/Depot/Product IDs from .env mappings.
    This is the original working pattern — used as the reference for all other fetches.
    """
    bdc_id     = BDC_MAP.get(bdc_name)
    depot_id   = DEPOT_MAP.get(depot_name)
    product_id = PRODUCT_MAP.get(product_display)

    if not all([bdc_id, depot_id, product_id]):
        st.error("❌ BDC, Depot, or Product not found in .env mappings")
        return pd.DataFrame()

    params = {
        'lngProductId': product_id,
        'lngBDCId':     bdc_id,
        'lngDepotId':   depot_id,
        'dtpStartDate': start_str,
        'dtpEndDate':   end_str,
        'lngUserId':    NPA_CONFIG['USER_ID'],
    }
    pdf_bytes = _fetch_npa_pdf(NPA_CONFIG['STOCK_TXN_URL'], params)
    if not pdf_bytes:
        return pd.DataFrame()
    records = _txn_parse_pdf_bytes(pdf_bytes)
    return pd.DataFrame(records) if records else pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — SNAPSHOT / HISTORY
# ══════════════════════════════════════════════════════════════════════════════

SNAPSHOT_DIR = os.path.join(os.getcwd(), "national_snapshots")


def _save_national_snapshot(forecast_df: pd.DataFrame, period_label: str):
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    snap = {
        'ts':     datetime.now().isoformat(),
        'period': period_label,
        'rows':   forecast_df[
            ['product', 'total_balance', 'omc_sales', 'daily_rate', 'days_remaining']
        ].to_dict('records'),
    }
    fname = f"snap_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(os.path.join(SNAPSHOT_DIR, fname), 'w') as f:
        json.dump(snap, f)


def _load_all_snapshots() -> pd.DataFrame:
    if not os.path.exists(SNAPSHOT_DIR):
        return pd.DataFrame()
    rows = []
    for fname in sorted(os.listdir(SNAPSHOT_DIR)):
        if not fname.endswith('.json'):
            continue
        try:
            with open(os.path.join(SNAPSHOT_DIR, fname)) as f:
                snap = json.load(f)
            ts = pd.to_datetime(snap['ts'])
            for r in snap['rows']:
                rows.append({
                    'timestamp':     ts,
                    'period':        snap.get('period', ''),
                    'product':       r['product'],
                    'total_balance': r['total_balance'],
                    'omc_sales':     r['omc_sales'],
                    'daily_rate':    r['daily_rate'],
                    'days_remaining': r['days_remaining'],
                })
        except Exception:
            continue
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _count_period_days(start_str: str, end_str: str, use_business_days: bool) -> int:
    fmt = "%m/%d/%Y"
    d_start = datetime.strptime(start_str, fmt).date()
    d_end   = datetime.strptime(end_str,   fmt).date()
    count = (
        len(pd.bdate_range(d_start, d_end))
        if use_business_days
        else (d_end - d_start).days
    )
    return max(count, 1)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — VESSEL SUPPLY
# ══════════════════════════════════════════════════════════════════════════════

def _load_vessel_sheet(file_id_or_url=None):
    import re as _re
    from io import StringIO, BytesIO
    url_in  = file_id_or_url or VESSEL_SHEET_URL
    m_id    = _re.search(r'/d/([a-zA-Z0-9-_]+)', url_in)
    file_id = m_id.group(1) if m_id else (url_in if _re.match(r'^[a-zA-Z0-9-_]{20,}$', url_in) else None)
    m_gid   = _re.search(r'(?:(?:#|\?|&)gid=)(\d+)', url_in)
    gid     = m_gid.group(1) if m_gid else None
    if not file_id:
        return None, "Could not extract Google Sheets file ID."
    candidates = []
    if gid:
        candidates.append((f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv&gid={gid}", "csv"))
    candidates += [
        (f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv&gid=0", "csv"),
        (f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv",       "csv"),
        (f"https://docs.google.com/spreadsheets/d/{file_id}/gviz/tq?tqx=out:csv",    "gviz"),
        (f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx",      "xlsx"),
    ]
    hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    for url, mode in candidates:
        try:
            resp = requests.get(url, headers=hdrs, allow_redirects=True, timeout=30)
            if resp.status_code != 200 or not resp.content:
                continue
            if mode == "xlsx":
                return pd.read_excel(BytesIO(resp.content)), None
            from io import StringIO as SI
            df = pd.read_csv(
                SI(resp.content.decode("utf-8", errors="replace")),
                header=14, skiprows=1, skipfooter=1, engine='python'
            )
            return df, None
        except Exception:
            continue
    return None, "All fetch strategies failed. Ensure the sheet is shared publicly."


def _parse_vessel_date(date_str, default_year='2025'):
    date_str = str(date_str).strip().upper()
    if 'PENDING' in date_str or date_str in ('NAN', ''):
        month_code = VESSEL_MONTH_MAPPING.get(datetime.now().strftime('%b'),
                                               datetime.now().strftime('%b').upper())
        return month_code, default_year, 'PENDING'
    try:
        if '-' in date_str:
            parts = date_str.split('-')
            if len(parts) == 2:
                month = VESSEL_MONTH_MAPPING.get(parts[1].title(), parts[1].upper())
                return month, default_year, 'DISCHARGED'
    except Exception:
        pass
    return 'Unknown', default_year, 'DISCHARGED'


def _process_vessel_df(raw_df, year='2025') -> pd.DataFrame:
    raw_df = raw_df.copy()
    raw_df.columns = raw_df.columns.str.strip()
    col_idx = {}
    for i, col in enumerate(raw_df.columns):
        cl = str(col).lower().strip()
        if 'receiver' in cl or (i == 0 and 'unnamed' not in cl):
            col_idx['receivers'] = i
        elif 'type' in cl and 'receiver' not in cl:
            col_idx['type'] = i
        elif 'vessel' in cl and 'name' in cl:
            col_idx['vessel_name'] = i
        elif 'supplier' in cl:
            col_idx['supplier'] = i
        elif 'product' in cl:
            col_idx['product'] = i
        elif 'quantity' in cl or ('mt' in cl and 'quantity' not in cl):
            col_idx['quantity'] = i
        elif 'date' in cl or 'discharg' in cl:
            col_idx['date'] = i
    records = []
    for _, row in raw_df.dropna(how='all').iterrows():
        try:
            receivers   = str(row.iloc[col_idx.get('receivers',   0)]).strip()
            vessel_type = str(row.iloc[col_idx.get('type',        1)]).strip()
            vessel_name = str(row.iloc[col_idx.get('vessel_name', 2)]).strip()
            supplier    = str(row.iloc[col_idx.get('supplier',    3)]).strip()
            product_raw = str(row.iloc[col_idx.get('product',     4)]).strip().upper()
            qty_str     = str(row.iloc[col_idx.get('quantity',    5)]).replace(',', '').strip()
            date_cell   = str(row.iloc[col_idx.get('date',        6)]).strip()
            if receivers.upper() in {'RECEIVER(S)','RECEIVERS','NAN',''} \
                    or product_raw in {'PRODUCT','NAN',''} \
                    or qty_str.upper() in {'NAN','-','QUANTITY (MT)',''}:
                continue
            qty_mt = float(qty_str)
            if qty_mt <= 0:
                continue
            product = VESSEL_PRODUCT_MAPPING.get(product_raw, product_raw)
            if product not in VESSEL_CONVERSION_FACTORS:
                continue
            qty_lt = qty_mt * VESSEL_CONVERSION_FACTORS[product]
            month, yr, status = _parse_vessel_date(date_cell, default_year=year)
            records.append({
                'Receivers':        receivers,
                'Vessel_Type':      vessel_type,
                'Vessel_Name':      vessel_name,
                'Supplier':         supplier,
                'Product':          product,
                'Original_Product': product_raw,
                'Quantity_MT':      qty_mt,
                'Quantity_Litres':  qty_lt,
                'Date_Discharged':  date_cell,
                'Month':            month,
                'Year':             yr,
                'Status':           status,
            })
        except Exception:
            continue
    return pd.DataFrame(records)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — DEPOT STRESS MAP HELPERS
# ══════════════════════════════════════════════════════════════════════════════

DEPOT_COORDS = {
    "TEMA":       (5.6698, -0.0166),
    "TAKORADI":   (4.8845, -1.7554),
    "ACCRA":      (5.6037, -0.1870),
    "KUMASI":     (6.6885, -1.6244),
    "BUIPE":      (8.7853, -1.5420),
    "BOLGATANGA": (10.7856, -0.8514),
    "BOLGA":      (10.7856, -0.8514),
    "AKOSOMBO":   (6.3000,  0.0500),
    "MAMI WATER": (6.25,    0.10),
    "MAMIWATER":  (6.25,    0.10),
    "MAMI-WATER": (6.25,    0.10),
    "KOTOKA":     (5.6052, -0.1668),
    "AIRPORT":    (5.6052, -0.1668),
    "ATK":        (5.6052, -0.1668),
    "SEKONDI":    (4.934,  -1.715),
    "NAVAL BASE": (4.934,  -1.715),
}


def _guess_depot_coords(depot_name: str):
    if not depot_name:
        return None
    name = depot_name.upper().strip()
    if any(x in name for x in ["KOTOKA", "AIRPORT", "ATK"]) and "BLUE OCEAN" in name:
        return DEPOT_COORDS["KOTOKA"]
    if "BOLGATANGA" in name or "BOLGA" in name:
        return DEPOT_COORDS["BOLGATANGA"]
    if "AKOSOMBO" in name:
        return DEPOT_COORDS["AKOSOMBO"]
    if any(x in name for x in ["MAMI WATER", "MAMIWATER", "MAMI-WATER"]):
        return DEPOT_COORDS["MAMI WATER"]
    if "SEKONDI" in name or "NAVAL BASE" in name:
        return DEPOT_COORDS["SEKONDI"]
    if "GHANSTOCK" in name or ("TAKORADI" in name and "SEKONDI" not in name):
        return DEPOT_COORDS["TAKORADI"]
    for key in ["TEMA", "TAKORADI", "KUMASI", "BUIPE", "ACCRA"]:
        if key in name:
            return DEPOT_COORDS[key]
    return None


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 10 — PAGE CONFIG & CSS
# ══════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="NPA Energy Analytics 🛢️",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

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
    text-shadow:0 0 10px #00ffff,0 0 20px #00ffff,0 0 30px #00ffff;animation:glow 2s ease-in-out infinite alternate;}
@keyframes glow{from{text-shadow:0 0 5px #00ffff,0 0 10px #00ffff,0 0 15px #00ffff}
    to{text-shadow:0 0 10px #00ffff,0 0 20px #00ffff,0 0 30px #00ffff,0 0 40px #0ff}}
[data-testid="stSidebar"]{background:linear-gradient(180deg,#0a0e27 0%,#16213e 100%);
    border-right:2px solid #00ffff;box-shadow:5px 0 15px rgba(0,255,255,.3);}
[data-testid="stSidebar"] h1,[data-testid="stSidebar"] h2,[data-testid="stSidebar"] h3{color:#ff00ff!important;text-shadow:0 0 10px #ff00ff;}
.stButton>button{background:linear-gradient(45deg,#ff00ff,#00ffff);color:white;
    border:2px solid #00ffff;border-radius:25px;padding:15px 30px;
    font-family:'Orbitron',sans-serif;font-weight:700;font-size:18px;
    box-shadow:0 0 20px rgba(0,255,255,.5);transition:all .3s ease;
    text-transform:uppercase;letter-spacing:2px;}
.stButton>button:hover{transform:scale(1.05) translateY(-3px);
    box-shadow:0 0 30px rgba(0,255,255,.8),0 0 40px rgba(255,0,255,.5);
    background:linear-gradient(45deg,#00ffff,#ff00ff);}
.dataframe{background-color:rgba(10,14,39,.8)!important;border:2px solid #00ffff!important;
    border-radius:10px;box-shadow:0 0 20px rgba(0,255,255,.3);}
.dataframe th{background-color:#16213e!important;color:#00ffff!important;
    font-family:'Orbitron',sans-serif;text-transform:uppercase;border:1px solid #00ffff!important;}
.dataframe td{background-color:rgba(22,33,62,.6)!important;color:#ffffff!important;
    border:1px solid rgba(0,255,255,.2)!important;}
[data-testid="stMetricValue"]{font-family:'Orbitron',sans-serif;font-size:28px!important;
    color:#00ffff!important;text-shadow:0 0 15px #00ffff;}
.metric-card{background:rgba(22,33,62,.6);padding:20px;border-radius:15px;
    border:2px solid #00ffff;text-align:center;}
.metric-card h2{color:#ff00ff!important;margin:0;font-size:20px!important;}
.metric-card h1{color:#00ffff!important;margin:10px 0;font-size:32px!important;word-wrap:break-word;}
[data-testid="stMetricLabel"]{font-family:'Rajdhani',sans-serif;color:#ff00ff!important;
    font-weight:700;text-transform:uppercase;letter-spacing:2px;}
p,span,div{font-family:'Rajdhani',sans-serif;color:#e0e0e0;}
[data-testid="stFileUploader"]{border:2px dashed #00ffff;border-radius:15px;
    background:rgba(22,33,62,.3);padding:20px;}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 11 — SHARED UI HELPERS
# ══════════════════════════════════════════════════════════════════════════════

_COLORS = {'PREMIUM': '#00ffff', 'GASOIL': '#ffaa00', 'LPG': '#00ff88'}
_ICONS  = {'PREMIUM': '⛽',      'GASOIL': '🚛',      'LPG': '🔵'}
_NAMES  = {'PREMIUM': 'PREMIUM (PMS)', 'GASOIL': 'GASOIL (AGO)', 'LPG': 'LPG'}


def _metric_card(title: str, value: str, sub: str = "", border: str = "#00ffff") -> str:
    return f"""
    <div class='metric-card' style='border-color:{border};'>
        <h2>{title}</h2><h1>{value}</h1>
        <p style='color:#888;font-size:14px;margin:0;'>{sub}</p>
    </div>"""


def _fetch_error(label: str):
    st.error(f"❌ Could not fetch {label} PDF from NPA. Check network / credentials.")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 12 — PAGE: BDC BALANCE
# ══════════════════════════════════════════════════════════════════════════════

def show_bdc_balance():
    st.markdown("<h2>🏦 BDC STOCK BALANCE ANALYZER</h2>", unsafe_allow_html=True)
    st.info("📊 Click the button below to fetch current BDC Balance data from NPA")
    st.markdown("---")
    if 'bdc_records' not in st.session_state:
        st.session_state.bdc_records = []

    if st.button("🔄 FETCH BDC BALANCE DATA", use_container_width=True):
        with st.spinner("🔄 FETCHING DATA FROM NPA PORTAL..."):
            records = fetch_bdc_balance()
            if records:
                st.session_state.bdc_records = records
                st.success(f"✅ PDF received and parsed — {len(records)} records")
            else:
                _fetch_error("BDC Balance")
                st.session_state.bdc_records = []

    records = st.session_state.bdc_records
    if not records:
        st.info("👆 Click the button above to fetch BDC balance data")
        return

    df = pd.DataFrame(records)
    col_bal = 'ACTUAL BALANCE (LT\\KG)'
    st.success(f"✅ {len(records)} records loaded")
    st.markdown("---")

    # ── Product totals ─────────────────────────────────────────────────────
    st.markdown("<h3>📊 PRODUCT TOTALS</h3>", unsafe_allow_html=True)
    summary = df.groupby('Product')[col_bal].sum()
    cols = st.columns(3)
    for i, prod in enumerate(['GASOIL', 'LPG', 'PREMIUM']):
        val = summary.get(prod, 0)
        with cols[i]:
            st.markdown(_metric_card(prod, f"{val:,.0f}", "LT/KG"), unsafe_allow_html=True)

    st.markdown("---")

    # ── BDC breakdown ──────────────────────────────────────────────────────
    st.markdown("<h3>🏢 BDC BREAKDOWN</h3>", unsafe_allow_html=True)
    bdc_sum = df.groupby('BDC').agg(
        **{'Total Balance (LT/KG)': (col_bal, 'sum'),
           'Depots': ('DEPOT', 'nunique'),
           'Products': ('Product', 'nunique')}
    ).reset_index().sort_values('Total Balance (LT/KG)', ascending=False)

    col1, col2 = st.columns([2, 1])
    with col1:
        st.dataframe(bdc_sum, use_container_width=True, hide_index=True)
    with col2:
        st.metric("Total BDCs",   df['BDC'].nunique())
        st.metric("Total Depots", df['DEPOT'].nunique())
        st.metric("Grand Total",  f"{df[col_bal].sum():,.0f} LT/KG")

    st.markdown("---")

    # ── Pivot table ────────────────────────────────────────────────────────
    st.markdown("<h3>📊 PRODUCT DISTRIBUTION BY BDC</h3>", unsafe_allow_html=True)
    pivot = df.pivot_table(
        index='BDC', columns='Product', values=col_bal,
        aggfunc='sum', fill_value=0
    ).reset_index()
    for p in ['GASOIL', 'LPG', 'PREMIUM']:
        if p not in pivot.columns:
            pivot[p] = 0
    pivot['TOTAL'] = pivot[['GASOIL', 'LPG', 'PREMIUM']].sum(axis=1)
    pivot = pivot.sort_values('TOTAL', ascending=False)
    st.dataframe(pivot[['BDC', 'GASOIL', 'LPG', 'PREMIUM', 'TOTAL']],
                 use_container_width=True, hide_index=True)

    st.markdown("---")

    # ── Filter ─────────────────────────────────────────────────────────────
    st.markdown("<h3>🔍 SEARCH & FILTER</h3>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        stype = st.selectbox("Search By:", ["Product", "BDC", "Depot"], key='bdc_stype')
    with c2:
        opts = {'Product': df['Product'], 'BDC': df['BDC'], 'Depot': df['DEPOT']}[stype].unique()
        sval = st.selectbox(f"Select {stype}:", ['ALL'] + sorted(opts.tolist()), key='bdc_sval')

    filtered = df if sval == 'ALL' else df[
        df[{'Product': 'Product', 'BDC': 'BDC', 'Depot': 'DEPOT'}[stype]] == sval
    ]
    st.dataframe(
        filtered[['Product', 'BDC', 'DEPOT', 'AVAILABLE BALANCE (LT\\KG)',
                   col_bal, 'Date']].sort_values(['Product', 'BDC', 'DEPOT']),
        use_container_width=True, height=400, hide_index=True
    )

    st.markdown("---")
    st.markdown("<h3>💾 EXPORT</h3>", unsafe_allow_html=True)
    path = StockBalanceScraper().save_to_excel(records)
    if path and os.path.exists(path):
        with open(path, 'rb') as f:
            st.download_button("⬇️ DOWNLOAD EXCEL", f, os.path.basename(path),
                               "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 13 — PAGE: OMC LOADINGS
# ══════════════════════════════════════════════════════════════════════════════

def show_omc_loadings():
    st.markdown("<h2>🚚 OMC LOADINGS ANALYZER</h2>", unsafe_allow_html=True)
    st.info("📊 Select date range and fetch OMC loadings data")
    st.markdown("---")

    if 'omc_df' not in st.session_state:
        st.session_state.omc_df = pd.DataFrame()

    c1, c2 = st.columns(2)
    with c1:
        start_date = st.date_input("Start Date",
                                    value=datetime.now() - timedelta(days=7), key='omc_start')
    with c2:
        end_date = st.date_input("End Date", value=datetime.now(), key='omc_end')

    if st.button("🔄 FETCH OMC LOADINGS DATA", use_container_width=True):
        start_str = start_date.strftime("%m/%d/%Y")
        end_str   = end_date.strftime("%m/%d/%Y")
        st.info(f"🔍 Requesting orders from **{start_str}** to **{end_str}**")
        with st.spinner("🔄 FETCHING DATA FROM NPA PORTAL..."):
            df = fetch_omc_loadings(start_str, end_str,
                                     bdc_name=NPA_CONFIG['OMC_NAME'])
            st.session_state.omc_df = df
            st.session_state.omc_start_date = start_date
            st.session_state.omc_end_date   = end_date
            if df.empty:
                st.warning("⚠️ No records found — try a different date range.")
            else:
                st.success(f"✅ {len(df)} records extracted")

    df = st.session_state.omc_df
    if df.empty:
        st.info("👆 Select dates and click the button above to fetch OMC loadings data")
        return

    st.success(f"✅ {len(df)} records loaded")
    st.markdown("---")

    # ── Metrics ────────────────────────────────────────────────────────────
    cols = st.columns(4)
    with cols[0]: st.markdown(_metric_card("TOTAL ORDERS", f"{len(df):,}"), unsafe_allow_html=True)
    with cols[1]: st.markdown(_metric_card("VOLUME", f"{df['Quantity'].sum():,.0f}", "LT/KG"), unsafe_allow_html=True)
    with cols[2]: st.markdown(_metric_card("OMCs", str(df['OMC'].nunique())), unsafe_allow_html=True)
    with cols[3]:
        val = (df['Quantity'] * df['Price']).sum()
        st.markdown(_metric_card("VALUE", f"₵{val:,.0f}"), unsafe_allow_html=True)

    st.markdown("---")

    # ── Product breakdown ──────────────────────────────────────────────────
    st.markdown("<h3>📦 PRODUCT BREAKDOWN</h3>", unsafe_allow_html=True)
    prod_sum = (
        df.groupby('Product').agg(
            **{'Total Volume (LT/KG)': ('Quantity', 'sum'),
               'Orders': ('Order Number', 'count'),
               'OMCs': ('OMC', 'nunique')}
        ).reset_index().sort_values('Total Volume (LT/KG)', ascending=False)
    )
    c1, c2 = st.columns([2, 1])
    with c1:
        st.dataframe(prod_sum, use_container_width=True, hide_index=True)
    with c2:
        total_vol = prod_sum['Total Volume (LT/KG)'].sum()
        for _, row in prod_sum.iterrows():
            st.metric(row['Product'], f"{row['Total Volume (LT/KG)'] / total_vol * 100:.1f}%")

    st.markdown("---")

    # ── BDC pivot ──────────────────────────────────────────────────────────
    st.markdown("<h3>🏦 BDC PERFORMANCE</h3>", unsafe_allow_html=True)
    pivot = df.pivot_table(
        index='BDC', columns='Product', values='Quantity',
        aggfunc='sum', fill_value=0
    ).reset_index()
    for p in ['GASOIL', 'LPG', 'PREMIUM']:
        if p not in pivot.columns:
            pivot[p] = 0
    pivot['TOTAL'] = pivot[['GASOIL', 'LPG', 'PREMIUM']].sum(axis=1)
    st.dataframe(pivot.sort_values('TOTAL', ascending=False),
                 use_container_width=True, hide_index=True)

    st.markdown("---")

    # ── Filter ─────────────────────────────────────────────────────────────
    st.markdown("<h3>🔍 SEARCH & FILTER</h3>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        stype = st.selectbox("Search By:", ["Product","OMC","BDC","Depot"], key='omc_stype')
    with c2:
        col_map = {"Product":"Product","OMC":"OMC","BDC":"BDC","Depot":"Depot"}
        opts = df[col_map[stype]].unique()
        sval = st.selectbox(f"Select {stype}:", ['ALL'] + sorted(opts.tolist()), key='omc_sval')

    filtered = df if sval == 'ALL' else df[df[col_map[stype]] == sval]
    if not filtered.empty:
        c1,c2,c3,c4 = st.columns(4)
        c1.metric("Orders",  f"{len(filtered):,}")
        c2.metric("Volume",  f"{filtered['Quantity'].sum():,.0f} LT")
        c3.metric("OMCs",    str(filtered['OMC'].nunique()))
        c4.metric("Value",   f"₵{(filtered['Quantity']*filtered['Price']).sum():,.0f}")
    st.dataframe(
        filtered[['Date','OMC','Truck','Quantity','Order Number','BDC','Depot','Price','Product']]
        .sort_values(['Product','OMC','Date']),
        use_container_width=True, height=400, hide_index=True
    )

    st.markdown("---")
    path = save_omc_to_excel(df)
    if path and os.path.exists(path):
        with open(path, 'rb') as f:
            st.download_button("⬇️ DOWNLOAD EXCEL", f, os.path.basename(path),
                               "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 14 — PAGE: DAILY ORDERS
# ══════════════════════════════════════════════════════════════════════════════

def show_daily_orders():
    st.markdown("<h2>📅 DAILY ORDERS ANALYZER</h2>", unsafe_allow_html=True)
    st.info("📊 Select a date range to fetch daily orders")
    st.markdown("---")

    if 'daily_df' not in st.session_state:
        st.session_state.daily_df = pd.DataFrame()

    c1, c2 = st.columns(2)
    with c1:
        start_date = st.date_input("Start Date",
                                    value=datetime.now() - timedelta(days=1), key='daily_start')
    with c2:
        end_date = st.date_input("End Date", value=datetime.now(), key='daily_end')

    if st.button("🔄 FETCH DAILY ORDERS", use_container_width=True):
        start_str = start_date.strftime("%m/%d/%Y")
        end_str   = end_date.strftime("%m/%d/%Y")
        with st.spinner("🔄 FETCHING DAILY ORDERS FROM NPA PORTAL..."):
            df = fetch_daily_orders(start_str, end_str)
            if df.empty:
                st.warning("⚠️ No orders found for this date range.")
            else:
                # Auto-match OMC names from loaded OMC Loadings if available
                loadings = st.session_state.get('omc_df', pd.DataFrame())
                if not loadings.empty and 'Order Number' in loadings.columns:
                    exact = dict(zip(loadings['Order Number'], loadings['OMC']))
                    def _prefix(s):
                        m = re.match(r'^([A-Z]{2,})', str(s).strip().upper())
                        return m.group(1) if m else None
                    loadings['_pfx'] = loadings['Order Number'].apply(_prefix)
                    prefix_map = (
                        loadings.dropna(subset=['_pfx'])
                        .groupby('_pfx')['OMC']
                        .agg(lambda x: x.mode().iloc[0] if len(x) > 0 else None)
                        .to_dict()
                    )
                    df['OMC'] = df['Order Number'].map(exact)
                    df['OMC'] = df.apply(
                        lambda r: prefix_map.get(_prefix(r['Order Number']))
                        if pd.isna(r['OMC']) else r['OMC'],
                        axis=1
                    )
                else:
                    df['OMC'] = None
                st.session_state.daily_df = df
                st.session_state.daily_start_date = start_date
                st.session_state.daily_end_date   = end_date
                st.success(f"✅ {len(df)} orders extracted")

    df = st.session_state.daily_df
    if df.empty:
        st.info("👆 Select a date range and click the button above")
        return

    st.markdown("---")

    cols = st.columns(5)
    with cols[0]: st.markdown(_metric_card("ORDERS", f"{len(df):,}"), unsafe_allow_html=True)
    with cols[1]: st.markdown(_metric_card("VOLUME", f"{df['Quantity'].sum():,.0f}", "LT/KG"), unsafe_allow_html=True)
    with cols[2]: st.markdown(_metric_card("BDCs", str(df['BDC'].nunique())), unsafe_allow_html=True)
    with cols[3]:
        omc_n = df['OMC'].nunique() if 'OMC' in df.columns and df['OMC'].notna().any() else 0
        st.markdown(_metric_card("OMCs", str(omc_n)), unsafe_allow_html=True)
    with cols[4]:
        val = (df['Quantity'] * df['Price']).sum()
        st.markdown(_metric_card("VALUE", f"₵{val:,.0f}"), unsafe_allow_html=True)

    st.markdown("---")

    # ── Product & BDC summaries ────────────────────────────────────────────
    st.markdown("<h3>📦 PRODUCT SUMMARY</h3>", unsafe_allow_html=True)
    prod_sum = (
        df.groupby('Product').agg(
            **{'Total Volume (LT/KG)': ('Quantity', 'sum'),
               'Orders': ('Order Number', 'count'),
               'BDCs': ('BDC', 'nunique')}
        ).reset_index().sort_values('Total Volume (LT/KG)', ascending=False)
    )
    st.dataframe(prod_sum, use_container_width=True, hide_index=True)

    st.markdown("<h3>🏦 BDC SUMMARY</h3>", unsafe_allow_html=True)
    pivot = df.pivot_table(
        index='BDC', columns='Product', values='Quantity',
        aggfunc='sum', fill_value=0
    ).reset_index()
    pcols = [c for c in pivot.columns if c != 'BDC']
    pivot['TOTAL'] = pivot[pcols].sum(axis=1)
    st.dataframe(pivot.sort_values('TOTAL', ascending=False),
                 use_container_width=True, hide_index=True)

    st.markdown("---")

    # ── Filter ─────────────────────────────────────────────────────────────
    c1, c2 = st.columns(2)
    with c1:
        stype = st.selectbox("Search By:", ["Product","BDC","Depot","Status"], key='daily_stype')
    with c2:
        col_map = {"Product":"Product","BDC":"BDC","Depot":"Depot","Status":"Status"}
        opts = df[col_map[stype]].dropna().unique()
        sval = st.selectbox(f"Select {stype}:", ['ALL'] + sorted(opts.tolist()), key='daily_sval')

    filtered = df if sval == 'ALL' else df[df[col_map[stype]] == sval]
    st.dataframe(
        filtered[['Date','OMC','Truck','Quantity','Order Number','BDC','Depot','Price','Product','Status']]
        .sort_values(['Product','BDC','Date']),
        use_container_width=True, height=400, hide_index=True
    )

    st.markdown("---")
    path = save_daily_orders_excel(df)
    if path and os.path.exists(path):
        with open(path, 'rb') as f:
            st.download_button("⬇️ DOWNLOAD EXCEL", f, os.path.basename(path),
                               "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 15 — PAGE: MARKET SHARE
# ══════════════════════════════════════════════════════════════════════════════

def show_market_share():
    st.markdown("<h2>📊 BDC MARKET SHARE ANALYSIS</h2>", unsafe_allow_html=True)
    st.markdown("---")
    has_balance  = bool(st.session_state.get('bdc_records'))
    has_loadings = not st.session_state.get('omc_df', pd.DataFrame()).empty

    c1, c2 = st.columns(2)
    with c1:
        st.success(f"✅ BDC Balance: {len(st.session_state.get('bdc_records', []))} records") \
            if has_balance else st.warning("⚠️ BDC Balance not loaded")
    with c2:
        st.success(f"✅ OMC Loadings: {len(st.session_state.get('omc_df', pd.DataFrame()))} records") \
            if has_loadings else st.warning("⚠️ OMC Loadings not loaded")

    if not has_balance and not has_loadings:
        st.error("❌ Fetch data from BDC Balance and/or OMC Loadings first.")
        return

    balance_df  = pd.DataFrame(st.session_state.get('bdc_records', []))
    loadings_df = st.session_state.get('omc_df', pd.DataFrame())
    col_bal = 'ACTUAL BALANCE (LT\\KG)'

    all_bdcs = sorted(set(
        (balance_df['BDC'].unique().tolist() if has_balance else []) +
        (loadings_df['BDC'].unique().tolist() if has_loadings else [])
    ))
    if not all_bdcs:
        st.error("❌ No BDCs found.")
        return

    selected_bdc = st.selectbox("Choose BDC:", all_bdcs, key='ms_bdc')
    st.markdown("---")
    st.markdown(f"## 📊 MARKET REPORT: {selected_bdc}")

    tab1, tab2, tab3 = st.tabs(["📦 Stock Balance", "🚚 Sales Volume", "📊 Combined"])

    with tab1:
        if not has_balance:
            st.warning("⚠️ Fetch BDC Balance first.")
        else:
            bdc_bal = balance_df[balance_df['BDC'] == selected_bdc]
            nat_total = balance_df[col_bal].sum()
            bdc_total = bdc_bal[col_bal].sum()
            share = bdc_total / nat_total * 100 if nat_total > 0 else 0
            rank  = list(balance_df.groupby('BDC')[col_bal].sum()
                         .sort_values(ascending=False).index).index(selected_bdc) + 1 \
                    if selected_bdc in balance_df['BDC'].values else "N/A"
            cols = st.columns(3)
            with cols[0]: st.markdown(_metric_card("TOTAL STOCK", f"{bdc_total:,.0f}", "LT/KG"), unsafe_allow_html=True)
            with cols[1]: st.markdown(_metric_card("MARKET SHARE", f"{share:.2f}%"), unsafe_allow_html=True)
            with cols[2]: st.markdown(_metric_card("STOCK RANK", f"#{rank}"), unsafe_allow_html=True)

            rows = []
            for prod in ['PREMIUM','GASOIL','LPG']:
                nat = balance_df[balance_df['Product']==prod][col_bal].sum()
                bdc = bdc_bal[bdc_bal['Product']==prod][col_bal].sum()
                rows.append({'Product':prod,'BDC Stock (LT)':f"{bdc:,.0f}",
                             'National Total (LT)':f"{nat:,.0f}",
                             'Market Share (%)':f"{bdc/nat*100:.2f}%" if nat>0 else "0%"})
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    with tab2:
        if not has_loadings:
            st.warning("⚠️ Fetch OMC Loadings first.")
        else:
            bdc_ld = loadings_df[loadings_df['BDC'] == selected_bdc]
            nat_vol = loadings_df['Quantity'].sum()
            bdc_vol = bdc_ld['Quantity'].sum()
            share   = bdc_vol / nat_vol * 100 if nat_vol > 0 else 0
            rank    = list(loadings_df.groupby('BDC')['Quantity'].sum()
                           .sort_values(ascending=False).index).index(selected_bdc) + 1 \
                      if selected_bdc in loadings_df['BDC'].values else "N/A"
            rev = (bdc_ld['Quantity'] * bdc_ld['Price']).sum()
            cols = st.columns(4)
            with cols[0]: st.markdown(_metric_card("TOTAL SALES", f"{bdc_vol:,.0f}", "LT/KG"), unsafe_allow_html=True)
            with cols[1]: st.markdown(_metric_card("MARKET SHARE", f"{share:.2f}%"), unsafe_allow_html=True)
            with cols[2]: st.markdown(_metric_card("OVERALL RANK", f"#{rank}"), unsafe_allow_html=True)
            with cols[3]: st.markdown(_metric_card("REVENUE", f"₵{rev/1e6:.1f}M"), unsafe_allow_html=True)

            rows = []
            for prod in ['PREMIUM','GASOIL','LPG']:
                nat = loadings_df[loadings_df['Product']==prod]['Quantity'].sum()
                bdc = bdc_ld[bdc_ld['Product']==prod]['Quantity'].sum()
                rows.append({'Product':prod,'BDC Sales (LT)':f"{bdc:,.0f}",
                             'National Total (LT)':f"{nat:,.0f}",
                             'Market Share (%)':f"{bdc/nat*100:.2f}%" if nat>0 else "0%",
                             'Orders':len(bdc_ld[bdc_ld['Product']==prod])})
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    with tab3:
        if not (has_balance and has_loadings):
            st.warning("⚠️ Both datasets required for combined view.")
        else:
            st.info("Stock position vs sales volume — side by side.")
            c1, c2 = st.columns(2)
            with c1:
                bdc_b = balance_df[balance_df['BDC']==selected_bdc][col_bal].sum()
                nat_b = balance_df[col_bal].sum()
                st.markdown(f"""
                <div style='background:rgba(22,33,62,.6);padding:20px;border-radius:15px;border:2px solid #00ffff;'>
                    <h3 style='color:#00ffff;margin:0;'>📦 STOCK POSITION</h3>
                    <p style='color:#fff;font-size:28px;font-weight:bold;'>{bdc_b:,.0f} LT</p>
                    <p style='color:#00ff88;font-size:20px;'>{bdc_b/nat_b*100:.2f}% Market Share</p>
                </div>""", unsafe_allow_html=True)
            with c2:
                bdc_s = loadings_df[loadings_df['BDC']==selected_bdc]['Quantity'].sum()
                nat_s = loadings_df['Quantity'].sum()
                st.markdown(f"""
                <div style='background:rgba(22,33,62,.6);padding:20px;border-radius:15px;border:2px solid #ff00ff;'>
                    <h3 style='color:#ff00ff;margin:0;'>🚚 SALES VOLUME</h3>
                    <p style='color:#fff;font-size:28px;font-weight:bold;'>{bdc_s:,.0f} LT</p>
                    <p style='color:#00ff88;font-size:20px;'>{bdc_s/nat_s*100:.2f}% Market Share</p>
                </div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 16 — PAGE: COMPETITIVE INTEL
# ══════════════════════════════════════════════════════════════════════════════

def show_competitive_intel():
    st.markdown("<h2>🎯 COMPETITIVE INTELLIGENCE CENTER</h2>", unsafe_allow_html=True)
    st.markdown("---")
    has_loadings = not st.session_state.get('omc_df', pd.DataFrame()).empty
    if not has_loadings:
        st.warning("⚠️ Fetch OMC Loadings data first.")
        return

    df = st.session_state.omc_df
    tab1, tab2, tab3 = st.tabs(["🚨 Anomaly Detection","💰 Price Intelligence","⭐ Performance Score"])

    with tab1:
        st.markdown("### 🚨 ANOMALY DETECTION ENGINE")
        mean_v = df['Quantity'].mean()
        std_v  = df['Quantity'].std()
        thresh = mean_v + 2 * std_v
        anom   = df[df['Quantity'] > thresh]
        c1,c2,c3 = st.columns(3)
        c1.metric("Volume Anomalies", len(anom))
        c2.metric("Anomalous Volume", f"{anom['Quantity'].sum():,.0f} LT")
        c3.metric("Threshold", f"{thresh:,.0f} LT")
        if not anom.empty:
            st.warning(f"🚨 {len(anom)} abnormally large orders detected!")
            st.dataframe(anom.nlargest(10,'Quantity')[['Date','BDC','OMC','Product','Quantity','Order Number']],
                         use_container_width=True, hide_index=True)

    with tab2:
        st.markdown("### 💰 PRICE INTELLIGENCE")
        price_stats = df.groupby(['BDC','Product'])['Price'].agg(['mean','min','max']).reset_index()
        price_stats.columns = ['BDC','Product','Avg Price','Min Price','Max Price']
        overall_mean = df['Price'].mean()
        price_stats['Tier'] = price_stats['Avg Price'].apply(
            lambda x: '🔴 Premium' if x > overall_mean * 1.1 else '🟢 Competitive')
        st.dataframe(price_stats.sort_values('Avg Price',ascending=False),
                     use_container_width=True, hide_index=True)

    with tab3:
        st.markdown("### ⭐ BDC PERFORMANCE LEADERBOARD")
        scores = []
        max_vol = df.groupby('BDC')['Quantity'].sum().max()
        max_ord = df.groupby('BDC').size().max()
        for bdc in df['BDC'].unique():
            bdf = df[df['BDC']==bdc]
            vol_s  = bdf['Quantity'].sum() / max_vol * 40
            ord_s  = len(bdf) / max_ord * 30
            div_s  = bdf['Product'].nunique() / 3 * 30
            total  = vol_s + ord_s + div_s
            grade  = 'A+' if total>=90 else 'A' if total>=80 else 'B' if total>=70 else 'C' if total>=60 else 'D'
            scores.append({'BDC':bdc,'Volume Score':round(vol_s,1),
                           'Orders Score':round(ord_s,1),'Diversity Score':round(div_s,1),
                           'Total Score':round(total,1),'Grade':grade})
        sdf = pd.DataFrame(scores).sort_values('Total Score',ascending=False)
        sdf.insert(0,'Rank',range(1,len(sdf)+1))
        sdf['Medal'] = sdf['Rank'].apply(lambda x:'🥇' if x==1 else '🥈' if x==2 else '🥉' if x==3 else '')
        st.dataframe(sdf, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 17 — PAGE: STOCK TRANSACTION  (original working page, unchanged logic)
# ══════════════════════════════════════════════════════════════════════════════

def show_stock_transaction():
    st.markdown("<h2>📈 STOCK TRANSACTION ANALYZER</h2>", unsafe_allow_html=True)
    st.info("🔥 Track BDC transactions: Inflows, Outflows, Sales & Stockout Forecasting")
    st.markdown("---")

    if 'stock_txn_df' not in st.session_state:
        st.session_state.stock_txn_df = pd.DataFrame()

    tab1, tab2 = st.tabs(["🔍 BDC Transaction Report", "📊 Stockout Analysis"])

    with tab1:
        st.markdown("### 🔍 BDC TRANSACTION REPORT")
        c1, c2 = st.columns(2)
        with c1:
            selected_bdc     = st.selectbox("Select BDC:",     sorted(BDC_MAP.keys()),   key='txn_bdc')
            selected_product = st.selectbox("Select Product:",  PRODUCT_OPTIONS,          key='txn_prod')
        with c2:
            selected_depot   = st.selectbox("Select Depot:",   sorted(DEPOT_MAP.keys()), key='txn_depot')
        c3, c4 = st.columns(2)
        with c3:
            start_date = st.date_input("Start Date:", value=datetime.now()-timedelta(days=30), key='txn_start')
        with c4:
            end_date   = st.date_input("End Date:",   value=datetime.now(),                    key='txn_end')

        if st.button("📊 FETCH TRANSACTION REPORT", use_container_width=True):
            start_str = start_date.strftime('%m/%d/%Y')
            end_str   = end_date.strftime('%m/%d/%Y')
            st.info(f"🔍 Requesting: {selected_bdc} → {selected_depot} → {selected_product}")
            with st.spinner("🔄 Fetching stock transaction data..."):
                df = fetch_stock_transaction(
                    selected_bdc, selected_depot, selected_product,
                    start_str, end_str
                )
                if df.empty:
                    st.warning("⚠️ No transactions found — check selection or date range.")
                    _fetch_error("Stock Transaction")
                else:
                    st.session_state.stock_txn_df      = df
                    st.session_state.stock_txn_bdc     = selected_bdc
                    st.session_state.stock_txn_depot   = selected_depot
                    st.session_state.stock_txn_product = selected_product
                    st.success(f"✅ {len(df)} transactions extracted!")

        df = st.session_state.stock_txn_df
        if df.empty:
            st.info("👆 Select options and click the button above.")
            return

        st.markdown("---")
        st.markdown(f"### 📊 TRANSACTION ANALYSIS: {st.session_state.get('stock_txn_bdc','')}")
        inflows   = df[df['Description'].isin(['Custody Transfer In','Product Outturn'])]['Volume'].sum()
        outflows  = df[df['Description'].isin(['Sale','Custody Transfer Out'])]['Volume'].sum()
        sales     = df[df['Description']=='Sale']['Volume'].sum()
        transfers = df[df['Description']=='Custody Transfer Out']['Volume'].sum()
        final_bal = df['Balance'].iloc[-1] if len(df)>0 else 0

        cols = st.columns(5)
        cols[0].metric("📥 Inflows",     f"{inflows:,.0f} LT")
        cols[1].metric("📤 Outflows",    f"{outflows:,.0f} LT")
        cols[2].metric("💰 Sales",       f"{sales:,.0f} LT")
        cols[3].metric("🔄 Transfers",   f"{transfers:,.0f} LT")
        cols[4].metric("📊 Final Bal",   f"{final_bal:,.0f} LT")

        st.markdown("---")
        txn_sum = (
            df.groupby('Description')
            .agg(**{'Total Volume (LT)':('Volume','sum'),'Count':('Trans #','count')})
            .reset_index()
            .sort_values('Total Volume (LT)',ascending=False)
        )
        st.dataframe(txn_sum, use_container_width=True, hide_index=True)

        if sales > 0:
            st.markdown("### 🏢 Top Customers")
            cust = (
                df[df['Description']=='Sale']
                .groupby('Account')['Volume'].sum()
                .sort_values(ascending=False).head(10).reset_index()
            )
            cust.columns = ['Customer','Volume Sold (LT)']
            st.dataframe(cust, use_container_width=True, hide_index=True)

        st.markdown("### 📄 Full Transaction History")
        st.dataframe(df, use_container_width=True, hide_index=True, height=400)

        # Export
        if st.button("💾 EXPORT TO EXCEL", use_container_width=True, key='txn_export'):
            out_dir = os.path.join(os.getcwd(), "stock_transactions")
            os.makedirs(out_dir, exist_ok=True)
            fname = f"stock_txn_{st.session_state.get('stock_txn_bdc','')}_" \
                    f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            fpath = os.path.join(out_dir, fname)
            with pd.ExcelWriter(fpath, engine='openpyxl') as w:
                df.to_excel(w, sheet_name='Transactions', index=False)
                txn_sum.to_excel(w, sheet_name='Summary', index=False)
            with open(fpath,'rb') as f:
                st.download_button("⬇️ DOWNLOAD", f, fname,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True)

    with tab2:
        st.markdown("### 📊 INTELLIGENT STOCKOUT FORECASTING")
        has_balance = bool(st.session_state.get('bdc_records'))
        has_txn     = not st.session_state.stock_txn_df.empty

        if not has_balance:
            st.info("💡 Fetch BDC Balance data first (🏦 BDC BALANCE section).")
        if not has_txn:
            st.info("💡 Fetch transaction data from the Transaction Report tab first.")
        if not (has_balance and has_txn):
            return

        balance_df  = pd.DataFrame(st.session_state.bdc_records)
        txn_df      = st.session_state.stock_txn_df
        bdc_name    = st.session_state.get('stock_txn_bdc', '')
        prod_disp   = st.session_state.get('stock_txn_product', '')
        product_name = PRODUCT_BALANCE_MAP.get(prod_disp, prod_disp)
        col_bal = 'ACTUAL BALANCE (LT\\KG)'

        bdc_bal = balance_df[
            (balance_df['BDC'].str.contains(bdc_name, case=False, na=False)) &
            (balance_df['Product'].str.contains(product_name, case=False, na=False))
        ]
        if bdc_bal.empty:
            st.warning(f"⚠️ No balance data for {bdc_name} / {product_name}")
            return

        stock = bdc_bal[col_bal].sum()
        dep   = txn_df[txn_df['Description'].isin(['Sale','Custody Transfer Out'])]['Volume'].sum()

        txn_copy = txn_df.copy()
        txn_copy['_dt'] = pd.to_datetime(txn_copy['Date'], format='%d/%m/%Y', errors='coerce')
        days_range = max((txn_copy['_dt'].max() - txn_copy['_dt'].min()).days, 1)
        daily_rate = dep / days_range if days_range > 0 else 0
        days_left  = stock / daily_rate if daily_rate > 0 else float('inf')

        border = '#ff0000' if days_left<7 else '#ffaa00' if days_left<14 else '#00ff88'
        status = '🔴 CRITICAL' if days_left<7 else '🟡 WARNING' if days_left<14 else '🟢 HEALTHY'

        cols = st.columns(4)
        with cols[0]: st.markdown(_metric_card("CURRENT STOCK", f"{stock:,.0f}", "LT/KG"), unsafe_allow_html=True)
        with cols[1]: st.markdown(_metric_card("DAILY RATE", f"{daily_rate:,.0f}", "LT/day"), unsafe_allow_html=True)
        with cols[2]:
            dt = f"{days_left:.1f}" if days_left != float('inf') else "∞"
            st.markdown(_metric_card("DAYS LEFT", dt, "", border), unsafe_allow_html=True)
        with cols[3]: st.markdown(_metric_card("PERIOD", f"{days_range}", "days analysed"), unsafe_allow_html=True)

        if days_left < 7:
            st.error("🚨 CRITICAL — Immediate replenishment required!")
        elif days_left < 14:
            st.warning("⚠️ Stock below safety threshold — plan replenishment.")
        else:
            st.success("✅ Stock healthy — continue normal operations.")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 18 — PAGE: BDC INTELLIGENCE
# ══════════════════════════════════════════════════════════════════════════════

def show_bdc_intelligence():
    st.markdown("<h2>🧠 BDC INTELLIGENCE CENTER</h2>", unsafe_allow_html=True)
    st.markdown("---")
    has_balance  = bool(st.session_state.get('bdc_records'))
    has_loadings = not st.session_state.get('omc_df', pd.DataFrame()).empty

    if not (has_balance and has_loadings):
        st.markdown("### 🔄 AUTO-FETCH DATA")
        c1, c2 = st.columns(2)
        with c1:
            if not has_balance:
                st.warning("⚠️ BDC Balance Missing")
                if st.button("🔄 FETCH BDC BALANCE", use_container_width=True, key='intel_bal'):
                    with st.spinner("Fetching..."):
                        r = fetch_bdc_balance()
                        if r:
                            st.session_state.bdc_records = r
                            st.success(f"✅ {len(r)} records")
                            st.rerun()
                        else:
                            _fetch_error("BDC Balance")
            else:
                st.success(f"✅ BDC Balance: {len(st.session_state.bdc_records)} records")
        with c2:
            if not has_loadings:
                st.warning("⚠️ OMC Loadings Missing")
                d1 = st.date_input("From", value=datetime.now()-timedelta(days=30), key='intel_d1')
                d2 = st.date_input("To",   value=datetime.now(),                    key='intel_d2')
                if st.button("🔄 FETCH OMC LOADINGS", use_container_width=True, key='intel_omc'):
                    with st.spinner("Fetching..."):
                        df = fetch_omc_loadings(
                            d1.strftime("%m/%d/%Y"), d2.strftime("%m/%d/%Y"),
                            bdc_name=NPA_CONFIG['OMC_NAME']
                        )
                        if not df.empty:
                            st.session_state.omc_df = df
                            st.success(f"✅ {len(df)} records")
                            st.rerun()
                        else:
                            _fetch_error("OMC Loadings")
            else:
                st.success(f"✅ OMC Loadings: {len(st.session_state.omc_df)} records")
        if not (bool(st.session_state.get('bdc_records')) and
                not st.session_state.get('omc_df', pd.DataFrame()).empty):
            return

    balance_df  = pd.DataFrame(st.session_state.bdc_records)
    loadings_df = st.session_state.omc_df
    col_bal = 'ACTUAL BALANCE (LT\\KG)'

    all_bdcs = sorted(set(balance_df['BDC'].unique()) | set(loadings_df['BDC'].unique()))
    selected_bdc = st.selectbox("Choose BDC:", all_bdcs, key='intel_bdc')
    st.markdown("---")

    tab1, tab2, tab3 = st.tabs(["📊 Overview","⏱️ Stockout Prediction","📉 Consumption"])

    with tab1:
        bdc_b = balance_df[balance_df['BDC']==selected_bdc]
        if not bdc_b.empty:
            prods = bdc_b.groupby('Product')[col_bal].sum()
            cols  = st.columns(min(len(prods),3))
            for i,(p,v) in enumerate(prods.items()):
                with cols[i%3]:
                    st.markdown(_metric_card(p, f"{v:,.0f}", "LT/KG"), unsafe_allow_html=True)

        bdc_l = loadings_df[loadings_df['BDC']==selected_bdc]
        if not bdc_l.empty:
            st.markdown("---")
            c1,c2,c3,c4 = st.columns(4)
            c1.metric("Orders",     f"{len(bdc_l):,}")
            c2.metric("Volume",     f"{bdc_l['Quantity'].sum():,.0f} LT")
            c3.metric("OMCs",       str(bdc_l['OMC'].nunique()))
            c4.metric("Avg Order",  f"{bdc_l['Quantity'].mean():,.0f} LT")

    with tab2:
        bdc_b = balance_df[balance_df['BDC']==selected_bdc]
        bdc_l = loadings_df[loadings_df['BDC']==selected_bdc]
        if bdc_b.empty or bdc_l.empty:
            st.warning("Data missing for one or both datasets.")
        else:
            ts = bdc_l.copy()
            ts['Date'] = pd.to_datetime(ts['Date'], errors='coerce')
            ts = ts.dropna(subset=['Date'])
            days = max((ts['Date'].max()-ts['Date'].min()).days, 1)
            daily = ts.groupby('Product')['Quantity'].sum() / days
            stock = bdc_b.groupby('Product')[col_bal].sum()
            for prod in stock.index:
                s = stock[prod]
                r = daily.get(prod, 0)
                d = s/r if r>0 else float('inf')
                border = '#ff0000' if d<7 else '#ffaa00' if d<14 else '#00ff88'
                status = '🔴 CRITICAL' if d<7 else '🟡 WARNING' if d<14 else '🟢 HEALTHY'
                st.markdown(f"""
                <div style='border:2px solid {border};border-radius:10px;padding:15px;margin:8px 0;'>
                    <b style='color:{border};'>{prod}</b> — Stock: {s:,.0f} LT |
                    Daily: {r:,.0f} LT | Days left: <b style='color:{border};'>
                    {"∞" if d==float("inf") else f"{d:.1f}"}</b> {status}
                </div>""", unsafe_allow_html=True)

    with tab3:
        bdc_l = loadings_df[loadings_df['BDC']==selected_bdc].copy()
        if bdc_l.empty:
            st.warning("No loadings data.")
        else:
            bdc_l['Date'] = pd.to_datetime(bdc_l['Date'], errors='coerce')
            bdc_l = bdc_l.dropna(subset=['Date'])
            stats = bdc_l.groupby('Product')['Quantity'].agg(
                Total='sum', Average='mean', Median='median', Min='min', Max='max'
            ).reset_index()
            st.dataframe(stats, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 19 — PAGE: NATIONAL STOCKOUT
# ══════════════════════════════════════════════════════════════════════════════

def show_national_stockout():
    st.markdown("<h2>🌍 NATIONAL STOCKOUT FORECAST</h2>", unsafe_allow_html=True)
    st.markdown("---")

    c1, c2 = st.columns(2)
    with c1:
        start_date = st.date_input("From", value=datetime.now()-timedelta(days=30), key='ns_start')
    with c2:
        end_date = st.date_input("To", value=datetime.now(), key='ns_end')

    start_str   = start_date.strftime("%m/%d/%Y")
    end_str     = end_date.strftime("%m/%d/%Y")
    period_days = max((end_date - start_date).days, 1)

    day_type = st.radio(
        "📅 Day Type for Daily Rate Calculation",
        ["📆 Calendar Days (default)","💼 Business Days (Mon–Fri only)"],
        index=0, key="ns_day_type", horizontal=True,
    )
    use_business_days = "Business" in day_type

    depletion_mode = st.radio(
        "🚚 Loadings Depletion Rate",
        ["📊 Average Daily Loading","🔥 Maximum Observed Daily Loading","📊 Median Daily Loading"],
        index=0, key="ns_depletion_mode",
    )

    exclude_tor_lpg = st.checkbox(
        "❌ Exclude TOR from LPG national stock", value=False, key="ns_exclude_tor"
    )

    # Vessel pipeline toggle
    _vessel_loaded = (
        st.session_state.get('vessel_data') is not None
        and not st.session_state.vessel_data.empty
    )
    _pending_count = 0
    if _vessel_loaded:
        _pending_count = int((st.session_state.vessel_data['Status']=='PENDING').sum())

    include_vessels = st.checkbox(
        "🚢 Include pending vessels in national stock", value=False, key='ns_include_vessels'
    )
    if include_vessels and not _vessel_loaded:
        st.warning("⚠️ No vessel data. Fetch from 🚢 VESSEL SUPPLY first.")
        include_vessels = False

    st.info(
        "⚡ **2 API calls.** Step 1 = BDC Balance (current national stock). "
        "Step 2 = National OMC Loadings (fuel dispatched to OMCs — chunked by week). "
        "CTO excluded — it is an internal BDC accounting entry, not a supply reduction."
    )
    st.markdown("---")

    if st.button("⚡ FETCH & ANALYSE NATIONAL FUEL SUPPLY", use_container_width=True):
        _run_national_analysis(
            start_str, end_str, period_days,
            depletion_mode, exclude_tor_lpg, use_business_days,
            include_vessels=include_vessels,
        )

    if st.session_state.get('ns_results'):
        _display_national_results(period_days)


def _run_national_analysis(start_str, end_str, period_days, depletion_mode,
                            exclude_tor_lpg, use_business_days, include_vessels=False):
    col_bal   = 'ACTUAL BALANCE (LT\\KG)'
    DISPLAY   = {'PREMIUM':'PREMIUM (PMS)','GASOIL':'GASOIL (AGO)','LPG':'LPG'}
    use_max   = "Maximum" in depletion_mode
    use_med   = "Median"  in depletion_mode
    eff_days  = _count_period_days(start_str, end_str, use_business_days)
    day_badge = "Business Days" if use_business_days else "Calendar Days"

    # ── Step 1: BDC Balance ───────────────────────────────────────────────
    with st.status("📡 Step 1/2 — Fetching national BDC stock balance…", expanded=True) as sa:
        records = fetch_bdc_balance()
        if not records:
            st.error("❌ BDC Balance fetch failed.")
            sa.update(label="❌ Failed", state="error")
            return
        bal_df = pd.DataFrame(records)

        if exclude_tor_lpg:
            tor_mask = (
                bal_df['BDC'].str.contains('TOR', case=False, na=False) &
                (bal_df['Product'] == 'LPG')
            )
            excl = bal_df[tor_mask][col_bal].sum()
            bal_df = bal_df[~tor_mask].copy()
            st.info(f"✅ TOR LPG excluded ({excl:,.0f} LT removed)")

        balance_by_prod = bal_df.groupby('Product')[col_bal].sum()

        # Add pending vessels
        vessel_pipeline = pd.Series(dtype=float)
        if include_vessels:
            _vdf = st.session_state.get('vessel_data')
            if _vdf is not None and not _vdf.empty:
                _pend = _vdf[_vdf['Status']=='PENDING']
                if not _pend.empty:
                    vessel_pipeline = _pend.groupby('Product')['Quantity_Litres'].sum()
                    for p, v in vessel_pipeline.items():
                        balance_by_prod[p] = balance_by_prod.get(p, 0) + v

        n_bdcs = bal_df['BDC'].nunique()
        pms = balance_by_prod.get('PREMIUM', 0)
        ago = balance_by_prod.get('GASOIL',  0)
        lpg = balance_by_prod.get('LPG',     0)
        st.write(f"✅ {len(records)} rows | {n_bdcs} BDCs | "
                 f"PMS:{pms:,.0f} AGO:{ago:,.0f} LPG:{lpg:,.0f} LT")
        sa.update(label=f"✅ Step 1 done — {n_bdcs} BDCs", state="running")

    # ── Step 2: National OMC Loadings (chunked) ───────────────────────────
    n_weeks = ceil(period_days / 7)
    with st.status(f"🚚 Step 2/2 — National OMC Loadings ({n_weeks} weekly chunks)…",
                   expanded=True) as sb:
        prog_bar  = st.progress(0, text="Starting…")
        prog_text = st.empty()

        def _on_progress(done, total):
            prog_bar.progress(done/total, text=f"Week {done}/{total}")
            prog_text.caption(f"✅ {done}/{total} done")

        cache_key = f"{start_str}|{end_str}"
        if (st.session_state.get('_ns_omc_cache') is not None and
                st.session_state.get('_ns_omc_cache_key','') == cache_key):
            omc_df = st.session_state['_ns_omc_cache']
            prog_bar.progress(1.0, text="✅ Cached data reused")
        else:
            omc_df = fetch_omc_loadings(start_str, end_str,
                                         bdc_name="", progress_cb=_on_progress)
            st.session_state['_ns_omc_cache']     = omc_df
            st.session_state['_ns_omc_cache_key'] = cache_key
            prog_bar.progress(1.0, text="✅ Done")

        if omc_df.empty:
            omc_by_prod   = pd.Series({'PREMIUM':0.0,'GASOIL':0.0,'LPG':0.0})
            depletion_lbl = "No Data"
        else:
            filtered = omc_df[omc_df['Product'].isin(['PREMIUM','GASOIL','LPG'])].copy()
            filtered['Date'] = pd.to_datetime(filtered['Date'], errors='coerce')
            daily_agg = (
                filtered.groupby(['Date','Product'])['Quantity'].sum().reset_index()
            )
            if use_med:
                omc_by_prod   = daily_agg.groupby('Product')['Quantity'].median()
                depletion_lbl = "📊 Median Daily Loading"
            elif use_max:
                omc_by_prod   = daily_agg.groupby('Product')['Quantity'].max()
                depletion_lbl = "🔥 Max Daily Loading"
            else:
                omc_by_prod   = filtered.groupby('Product')['Quantity'].sum()
                depletion_lbl = f"📊 Avg Daily Loading ({eff_days} {day_badge})"

        sb.update(label=f"✅ Step 2 done — {len(omc_df):,} records", state="complete")

    # ── Build forecast ─────────────────────────────────────────────────────
    rows_out = []
    for prod in ['PREMIUM','GASOIL','LPG']:
        stock     = float(balance_by_prod.get(prod, 0))
        depletion = float(omc_by_prod.get(prod,    0))
        daily_rate = (
            depletion if (use_med or use_max)
            else (depletion / eff_days if eff_days > 0 else 0)
        )
        days = stock / daily_rate if daily_rate > 0 else float('inf')
        rows_out.append({
            'product':       prod,
            'display_name':  DISPLAY[prod],
            'total_balance': stock,
            'omc_sales':     depletion,
            'daily_rate':    daily_rate,
            'days_remaining': days,
        })
    forecast_df = pd.DataFrame(rows_out)

    bdc_pivot = (
        bal_df.pivot_table(index='BDC', columns='Product', values=col_bal,
                            aggfunc='sum', fill_value=0).reset_index()
    )
    for p in ['GASOIL','LPG','PREMIUM']:
        if p not in bdc_pivot.columns:
            bdc_pivot[p] = 0
    bdc_pivot['TOTAL'] = bdc_pivot[['GASOIL','LPG','PREMIUM']].sum(axis=1)
    bdc_pivot = bdc_pivot.sort_values('TOTAL',ascending=False)
    nat_total = bdc_pivot['TOTAL'].sum()
    bdc_pivot['Market Share %'] = (bdc_pivot['TOTAL']/nat_total*100).round(2)

    st.session_state.ns_results = {
        'forecast_df':    forecast_df,
        'bal_df':         bal_df,
        'omc_df':         omc_df,
        'bdc_pivot':      bdc_pivot,
        'period_days':    period_days,
        'eff_days':       eff_days,
        'day_badge':      day_badge,
        'start_str':      start_str,
        'end_str':        end_str,
        'n_bdcs':         n_bdcs,
        'n_omc_rows':     len(omc_df),
        'depletion_lbl':  depletion_lbl,
        'exclude_tor':    exclude_tor_lpg,
        'include_vessels': include_vessels,
        'vessel_pipeline': vessel_pipeline.to_dict() if not vessel_pipeline.empty else {},
    }
    _save_national_snapshot(forecast_df, f"{period_days}d")
    st.success("✅ Done! Scroll down to view the forecast.")
    st.rerun()


def _display_national_results(period_days_arg):
    res         = st.session_state.ns_results
    fdf         = res['forecast_df']
    omc_df      = res['omc_df']
    bdc_pivot   = res['bdc_pivot']
    eff_days    = res['eff_days']
    day_badge   = res['day_badge']
    depl_lbl    = res['depletion_lbl']
    period_days = res['period_days']
    start_str   = res['start_str']
    end_str     = res['end_str']

    st.markdown("---")
    st.markdown(
        f"<h3>🇬🇭 GHANA NATIONAL FUEL SUPPLY — {start_str} → {end_str}</h3>",
        unsafe_allow_html=True
    )

    # Cache controls
    ck = st.session_state.get('_ns_omc_cache_key','')
    if ck:
        ca, cb = st.columns([4,1])
        with ca:
            st.caption(f"📋 OMC data cached for {ck}. Same range → cached data reused for stability.")
        with cb:
            if st.button("🗑️ Clear Cache", key='ns_clear_cache'):
                st.session_state.pop('_ns_omc_cache', None)
                st.session_state.pop('_ns_omc_cache_key', None)
                st.success("Cache cleared.")
                st.rerun()

    st.caption(
        f"Balance: {res['n_bdcs']} BDCs | "
        f"OMC Loadings: {res['n_omc_rows']:,} records | "
        f"Depletion: {depl_lbl} | "
        f"Day type: {day_badge} ({eff_days} days)"
        + (" | TOR LPG excluded" if res['exclude_tor'] else "")
    )

    # Days-of-supply cards
    st.markdown("---")
    st.markdown("### 🛢️ DAYS OF SUPPLY")
    cols = st.columns(len(fdf))
    for col, (_, row) in zip(cols, fdf.iterrows()):
        days  = row['days_remaining']
        prod  = row['product']
        color = _COLORS[prod]
        border = ('#ff0000' if days<7 else '#ffaa00' if days<14
                  else '#ff6600' if days<30 else '#00ff88')
        status = ('🔴 CRITICAL' if days<7 else '🟡 WARNING' if days<14
                  else '🟠 MONITOR' if days<30 else '🟢 HEALTHY')
        days_txt = f"{days:.1f}" if days != float('inf') else "∞"
        empty_dt = (
            (datetime.now()+timedelta(days=days)).strftime('%d %b %Y')
            if days != float('inf') else "N/A"
        )
        with col:
            st.markdown(f"""
            <div style='background:rgba(10,14,39,.85);padding:24px 16px;border-radius:16px;
                        border:2.5px solid {border};text-align:center;
                        box-shadow:0 0 18px {border}55;margin-bottom:8px;'>
                <div style='font-size:36px;'>{_ICONS[prod]}</div>
                <div style='font-family:Orbitron,sans-serif;color:{color};
                             font-size:17px;font-weight:700;'>{row["display_name"]}</div>
                <div style='font-family:Orbitron,sans-serif;font-size:52px;color:{border};
                             font-weight:900;line-height:1.1;margin:14px 0 4px;'>{days_txt}</div>
                <div style='color:#888;font-size:12px;'>days of supply</div>
                <div style='color:{border};font-size:14px;font-weight:700;
                             margin:4px 0 12px;'>{status}</div>
                <table style='width:100%;font-size:12px;border-collapse:collapse;
                              border-top:1px solid rgba(255,255,255,.08);padding-top:10px;'>
                    <tr><td style='color:#888;'>📦 Stock</td>
                        <td style='color:#e0e0e0;text-align:right;'>{row["total_balance"]:,.0f} LT</td></tr>
                    <tr><td style='color:#888;'>📉 Daily rate</td>
                        <td style='color:#e0e0e0;text-align:right;'>{row["daily_rate"]:,.0f} LT/d</td></tr>
                    <tr><td style='color:#888;'>🗓️ Est. empty</td>
                        <td style='color:{border};text-align:right;font-weight:700;'>{empty_dt}</td></tr>
                </table>
            </div>""", unsafe_allow_html=True)

    # Summary table
    st.markdown("---")
    st.markdown("### 📊 NATIONAL SUMMARY TABLE")
    sumrows = []
    for _, row in fdf.iterrows():
        days = row['days_remaining']
        dt = f"{days:.1f}" if days!=float('inf') else "∞"
        empty = (datetime.now()+timedelta(days=days)).strftime('%Y-%m-%d') if days!=float('inf') else "N/A"
        status = ('🔴 CRITICAL' if days<7 else '🟡 WARNING' if days<14
                  else '🟠 MONITOR' if days<30 else '🟢 HEALTHY')
        sumrows.append({
            'Product':                    row['display_name'],
            'National Stock (LT)':        f"{row['total_balance']:,.0f}",
            f'{depl_lbl} (LT)':           f"{row['omc_sales']:,.0f}",
            f'Daily Rate ({day_badge})':   f"{row['daily_rate']:,.0f}",
            'Days of Supply':              dt,
            'Projected Empty':             empty,
            'Status':                      status,
        })
    st.dataframe(pd.DataFrame(sumrows), use_container_width=True, hide_index=True)

    # BDC table
    st.markdown("---")
    st.markdown("### 🏦 STOCK BY BDC")
    disp_bdc = bdc_pivot.copy()
    for c in ['GASOIL','LPG','PREMIUM','TOTAL']:
        if c in disp_bdc.columns:
            disp_bdc[c] = disp_bdc[c].apply(lambda x: f"{x:,.0f}")
    disp_bdc['Market Share %'] = disp_bdc['Market Share %'].apply(lambda x: f"{x:.2f}%")
    st.dataframe(disp_bdc, use_container_width=True, hide_index=True)

    # Export
    st.markdown("---")
    if st.button("📄 GENERATE EXCEL REPORT", use_container_width=True, key='ns_export'):
        out_dir = os.path.join(os.getcwd(), "national_stockout_reports")
        os.makedirs(out_dir, exist_ok=True)
        fname = f"national_stockout_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        fpath = os.path.join(out_dir, fname)
        exp_rows = []
        for _, row in fdf.iterrows():
            days = row['days_remaining']
            exp_rows.append({
                'Product':             row['display_name'],
                'National Stock (LT)': row['total_balance'],
                f'{depl_lbl} (LT)':   row['omc_sales'],
                f'Daily Rate':         row['daily_rate'],
                'Days of Supply':      days if days!=float('inf') else 9999,
                'Projected Empty':     (datetime.now()+timedelta(days=days)).strftime('%Y-%m-%d')
                                       if days!=float('inf') else 'N/A',
            })
        with pd.ExcelWriter(fpath, engine='openpyxl') as w:
            pd.DataFrame(exp_rows).to_excel(w, sheet_name='Stockout Forecast', index=False)
            bdc_pivot.to_excel(w, sheet_name='Stock by BDC', index=False)
            if not omc_df.empty:
                omc_df.to_excel(w, sheet_name='OMC Loadings Detail', index=False)
        with open(fpath,'rb') as f:
            st.download_button("⬇️ DOWNLOAD NATIONAL REPORT", f, fname,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 20 — PAGE: LIVE RUNWAY MONITOR
# ══════════════════════════════════════════════════════════════════════════════

def show_live_runway_monitor():
    st.markdown("<h2>🔴 LIVE RUNWAY MONITOR</h2>", unsafe_allow_html=True)
    st.markdown("---")
    with st.expander("⚙️ Configure Alert Thresholds"):
        c1,c2,c3 = st.columns(3)
        with c1:
            pms_crit = st.number_input("PMS Critical (days)", value=7,  min_value=1)
            pms_warn = st.number_input("PMS Warning (days)",  value=14, min_value=1)
        with c2:
            ago_crit = st.number_input("AGO Critical (days)", value=7,  min_value=1)
            ago_warn = st.number_input("AGO Warning (days)",  value=14, min_value=1)
        with c3:
            lpg_crit = st.number_input("LPG Critical (days)", value=7,  min_value=1)
            lpg_warn = st.number_input("LPG Warning (days)",  value=14, min_value=1)
    thresholds = {
        'PREMIUM': (pms_crit, pms_warn),
        'GASOIL':  (ago_crit, ago_warn),
        'LPG':     (lpg_crit, lpg_warn),
    }

    ca, cb, cc = st.columns([2,1,1])
    with ca:
        auto_refresh = st.checkbox("🔄 Auto-refresh every 60 minutes", value=False)
    with cb:
        period_lr = st.number_input("Lookback days", value=30, min_value=1, max_value=90, key='lr_period')
    with cc:
        fetch_now = st.button("⚡ FETCH NOW", key='lr_fetch')

    should_fetch = fetch_now
    if auto_refresh:
        last = st.session_state.get('lr_last_fetch')
        if last is None or (datetime.now()-last).seconds > 3600:
            should_fetch = True

    if should_fetch:
        end_dt   = datetime.now()
        start_dt = end_dt - timedelta(days=period_lr)
        ss, es   = start_dt.strftime("%m/%d/%Y"), end_dt.strftime("%m/%d/%Y")
        with st.spinner("Fetching BDC Balance…"):
            records = fetch_bdc_balance()
            bal_df  = pd.DataFrame(records) if records else pd.DataFrame()
        with st.spinner(f"Fetching OMC Loadings ({period_lr}d, chunked)…"):
            omc_df = fetch_omc_loadings(ss, es, bdc_name="")

        col_bal = 'ACTUAL BALANCE (LT\\KG)'
        bp = bal_df.groupby('Product')[col_bal].sum() if not bal_df.empty else pd.Series()
        op = (
            omc_df[omc_df['Product'].isin(['PREMIUM','GASOIL','LPG'])]
            .groupby('Product')['Quantity'].sum()
        ) if not omc_df.empty else pd.Series()

        rows_out = []
        for prod in ['PREMIUM','GASOIL','LPG']:
            s = float(bp.get(prod, 0))
            d = float(op.get(prod, 0))
            r = d/period_lr if period_lr>0 else 0
            days = s/r if r>0 else float('inf')
            rows_out.append({'product':prod,'total_balance':s,'omc_sales':d,
                             'daily_rate':r,'days_remaining':days})
        fdf = pd.DataFrame(rows_out)
        st.session_state.lr_forecast     = fdf
        st.session_state.lr_last_fetch   = datetime.now()
        st.session_state.lr_period_days  = period_lr
        _save_national_snapshot(fdf, f"{period_lr}d")

    if st.session_state.get('lr_forecast') is None:
        st.info("👆 Click FETCH NOW to load the live runway status.")
        return

    fdf = st.session_state.lr_forecast
    last_t  = st.session_state.lr_last_fetch
    p_days  = st.session_state.get('lr_period_days', period_lr)

    st.markdown(f"<p style='color:#888;'>Last updated: <b style='color:#00ffff'>"
                f"{last_t.strftime('%d %b %Y %H:%M:%S')}</b> | Lookback: {p_days} days</p>",
                unsafe_allow_html=True)

    cols = st.columns(3)
    any_crit = any_warn = False
    for col, (_, row) in zip(cols, fdf.iterrows()):
        prod  = row['product']
        days  = row['days_remaining']
        crit, warn = thresholds.get(prod, (7,14))
        if days == float('inf'):
            border,status,emoji = '#888','NO DATA','⚫'
        elif days < crit:
            border,status,emoji = '#ff0000','CRITICAL','🔴'; any_crit = True
        elif days < warn:
            border,status,emoji = '#ffaa00','WARNING','🟡'; any_warn = True
        elif days < 30:
            border,status,emoji = '#ff6600','MONITOR','🟠'
        else:
            border,status,emoji = '#00ff88','HEALTHY','🟢'
        dt_txt = f"{days:.1f}" if days!=float('inf') else "∞"
        empty  = (datetime.now()+timedelta(days=days)).strftime('%d %b %Y') if days!=float('inf') else "N/A"

        hist = _load_all_snapshots()
        delta_html = ""
        if not hist.empty:
            prev = hist[hist['product']==prod].sort_values('timestamp')
            if len(prev)>=2:
                prev_d = prev.iloc[-2]['days_remaining']
                if days!=float('inf') and prev_d!=float('inf'):
                    delta = days - prev_d
                    arrow = "↑" if delta>0 else "↓"
                    dcol  = "#00ff88" if delta>0 else "#ff4444"
                    delta_html = f"<span style='color:{dcol};font-size:14px;'>{arrow}{abs(delta):.1f}d vs prev</span>"

        with col:
            st.markdown(f"""
            <div style='background:rgba(10,14,39,.9);padding:28px 18px;border-radius:18px;
                        border:3px solid {border};text-align:center;
                        box-shadow:0 0 25px {border}66;margin-bottom:10px;'>
                <div style='font-size:40px;'>{_ICONS[prod]}</div>
                <div style='font-family:Orbitron,sans-serif;color:{_COLORS[prod]};
                             font-size:16px;font-weight:700;letter-spacing:2px;'>{_NAMES[prod]}</div>
                <div style='color:{border};font-size:13px;font-weight:700;letter-spacing:3px;
                             margin:8px 0;'>{emoji} {status}</div>
                <div style='font-family:Orbitron,sans-serif;font-size:64px;font-weight:900;
                             color:{border};line-height:1;text-shadow:0 0 20px {border};'>{dt_txt}</div>
                <div style='color:#888;font-size:12px;'>DAYS OF SUPPLY</div>
                {delta_html}
                <div style='border-top:1px solid rgba(255,255,255,.1);margin-top:14px;padding-top:10px;'>
                    <div style='color:#888;font-size:11px;'>📦 {row["total_balance"]:,.0f} LT</div>
                    <div style='color:#888;font-size:11px;'>📉 {row["daily_rate"]:,.0f} LT/day</div>
                    <div style='color:{border};font-size:12px;font-weight:700;'>🗓️ Est. empty: {empty}</div>
                </div>
            </div>""", unsafe_allow_html=True)

    if any_crit:
        st.error("🚨 CRITICAL ALERT — Immediate action required!")
    elif any_warn:
        st.warning("⚠️ WARNING — Plan replenishment now.")
    else:
        st.success("✅ All products healthy.")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 21 — PAGE: HISTORICAL TRENDS
# ══════════════════════════════════════════════════════════════════════════════

def show_historical_trends():
    st.markdown("<h2>📉 HISTORICAL TRENDS</h2>", unsafe_allow_html=True)
    st.markdown("---")
    hist = _load_all_snapshots()
    if hist.empty:
        st.info("Run National Stockout or Live Runway Monitor a few times to build history.")
        return

    hist = hist.sort_values('timestamp')
    c1,c2,c3 = st.columns(3)
    c1.metric("Snapshots",  hist['timestamp'].nunique())
    c2.metric("Earliest",   hist['timestamp'].min().strftime('%d %b %Y'))
    c3.metric("Latest",     hist['timestamp'].max().strftime('%d %b %Y'))
    st.markdown("---")

    fig = go.Figure()
    for prod in ['PREMIUM','GASOIL','LPG']:
        pdata = hist[hist['product']==prod].copy()
        pdata = pdata[pdata['days_remaining']!=float('inf')].sort_values('timestamp')
        if pdata.empty: continue
        pdata['trend'] = pdata['days_remaining'].rolling(3,min_periods=1).mean()
        fig.add_trace(go.Scatter(x=pdata['timestamp'],y=pdata['days_remaining'],
                                  mode='markers',name=f"{prod} actual",
                                  marker=dict(color=_COLORS[prod],size=8)))
        fig.add_trace(go.Scatter(x=pdata['timestamp'],y=pdata['trend'],
                                  mode='lines',name=f"{prod} trend",
                                  line=dict(color=_COLORS[prod],width=2,dash='dot')))
    fig.add_hline(y=7, line_dash="dash", line_color="#ff0000",
                  annotation_text="CRITICAL 7d", annotation_font_color="#ff0000")
    fig.add_hline(y=14,line_dash="dash", line_color="#ffaa00",
                  annotation_text="WARNING 14d",  annotation_font_color="#ffaa00")
    fig.update_layout(paper_bgcolor='rgba(10,14,39,.9)',plot_bgcolor='rgba(10,14,39,.9)',
                       font=dict(color='white'),height=420,
                       xaxis=dict(gridcolor='rgba(255,255,255,.05)'),
                       yaxis=dict(gridcolor='rgba(255,255,255,.05)',title='Days of Supply'))
    st.plotly_chart(fig, use_container_width=True)

    if st.button("🗑️ Clear All Snapshots"):
        import shutil
        shutil.rmtree(SNAPSHOT_DIR, ignore_errors=True)
        st.success("Snapshots cleared.")
        st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 22 — PAGE: DEPOT STRESS MAP
# ══════════════════════════════════════════════════════════════════════════════

def show_depot_stress_map():
    st.markdown("<h2>🗺️ DEPOT STRESS MAP</h2>", unsafe_allow_html=True)
    st.markdown("---")
    has_balance = bool(st.session_state.get('bdc_records'))
    col_bal = 'ACTUAL BALANCE (LT\\KG)'

    if not has_balance:
        if st.button("⚡ FETCH BDC BALANCE", key='dsm_fetch'):
            with st.spinner("Fetching…"):
                records = fetch_bdc_balance()
                if records:
                    st.session_state.bdc_records = records
                    st.rerun()
                else:
                    _fetch_error("BDC Balance")
        return

    bal_df   = pd.DataFrame(st.session_state.bdc_records)
    prod_sel = st.selectbox("Product", ['ALL','PREMIUM','GASOIL','LPG'], key='dsm_prod')
    if prod_sel != 'ALL':
        bal_df = bal_df[bal_df['Product']==prod_sel]

    depot_agg = (
        bal_df.groupby('DEPOT')[col_bal].sum().reset_index()
        .rename(columns={col_bal:'stock','DEPOT':'depot'})
    )
    if depot_agg.empty:
        st.warning("No data available.")
        return

    max_s = depot_agg['stock'].max() or 1
    map_rows, unmatched = [], []
    for _, row in depot_agg.iterrows():
        coords = _guess_depot_coords(row['depot'])
        if coords:
            pct = row['stock']/max_s*100
            map_rows.append({
                'depot': row['depot'], 'stock': row['stock'],
                'lat': coords[0], 'lon': coords[1], 'pct': pct,
                'color': '#ff0000' if pct<10 else '#ffaa00' if pct<25 else '#ffdd00' if pct<50 else '#00ff88',
                'status': '🔴 CRITICAL' if pct<10 else '🟡 LOW' if pct<25 else '🟠 MODERATE' if pct<50 else '🟢 HEALTHY',
                'stock_fmt': f"{row['stock']:,.0f} LT",
            })
        else:
            unmatched.append(row['depot'])

    if map_rows:
        mdf = pd.DataFrame(map_rows)
        fig = go.Figure()
        fig.add_trace(go.Scattergeo(
            lat=mdf['lat'], lon=mdf['lon'],
            mode='markers+text',
            text=mdf['depot'].str[:20],
            textposition='top center',
            textfont=dict(color='white',size=10),
            marker=dict(size=mdf['pct'].clip(0,100)*0.5+12,
                        color=mdf['color'],opacity=0.85,
                        line=dict(width=2,color='white')),
            customdata=mdf[['stock_fmt','pct','status']],
            hovertemplate="<b>%{text}</b><br>Stock: %{customdata[0]}<br>%{customdata[2]}<extra></extra>",
            showlegend=False,
        ))
        fig.update_layout(
            geo=dict(scope='africa',center=dict(lat=7.9,lon=-1.0),projection_scale=12,
                     showland=True,landcolor='rgba(22,33,62,.9)',
                     showocean=True,oceancolor='rgba(10,14,39,.95)',
                     showcoastlines=True,coastlinecolor='rgba(0,255,255,.4)',
                     showframe=False,bgcolor='rgba(10,14,39,0)'),
            paper_bgcolor='rgba(10,14,39,0)',height=520,margin=dict(l=0,r=0,t=0,b=0),
        )
        st.plotly_chart(fig, use_container_width=True)

    if unmatched:
        st.caption(f"⚠️ No coordinates (table only): {', '.join(set(unmatched))}")

    st.markdown("---")
    disp = depot_agg.copy()
    disp['stock'] = disp['stock'].apply(lambda x: f"{x:,.0f}")
    st.dataframe(disp.rename(columns={'depot':'Depot','stock':'Stock (LT)'}),
                 use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 23 — PAGE: DEMAND FORECAST
# ══════════════════════════════════════════════════════════════════════════════

def show_demand_forecast():
    st.markdown("<h2>🔮 DEMAND FORECAST</h2>", unsafe_allow_html=True)
    st.markdown("---")
    if st.session_state.get('omc_df', pd.DataFrame()).empty:
        st.warning("⚠️ Fetch OMC Loadings first.")
        return

    df = st.session_state.omc_df.copy()
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Date'])
    if df.empty:
        st.warning("⚠️ No valid dates in OMC Loadings.")
        return

    c1, c2 = st.columns(2)
    with c1:
        fw = st.slider("Forecast horizon (weeks)", 1, 12, 4, key='df_weeks')
    with c2:
        view = st.radio("View",["National by Product","By OMC"],horizontal=True,key='df_view')

    df['week'] = df['Date'].dt.to_period('W').apply(lambda p: p.start_time)
    weekly = df.groupby(['week','Product'])['Quantity'].sum().reset_index()
    products = [p for p in ['PREMIUM','GASOIL','LPG'] if p in weekly['Product'].unique()]

    fig = go.Figure()
    sumrows = []
    future_weeks = []
    for prod in products:
        pdata = weekly[weekly['Product']==prod].sort_values('week')
        if len(pdata)<2: continue
        vals = pdata['Quantity'].values
        n    = len(vals)
        weights = [0.5**(n-1-i) for i in range(n)]
        wma  = sum(w*v for w,v in zip(weights,vals))/sum(weights)
        trend = (vals[-1]-vals[0])/max(n-1,1)
        last  = pdata['week'].iloc[-1]
        fw_dates = [last+timedelta(weeks=i+1) for i in range(fw)]
        proj  = [max(0, wma+trend*(i+1)) for i in range(fw)]
        future_weeks = fw_dates
        fig.add_trace(go.Scatter(x=pdata['week'],y=pdata['Quantity'],
                                  mode='lines+markers',name=f"{prod} actual",
                                  line=dict(color=_COLORS[prod],width=2),marker=dict(size=7)))
        fig.add_trace(go.Scatter(x=fw_dates,y=proj,mode='lines+markers',
                                  name=f"{prod} forecast",
                                  line=dict(color=_COLORS[prod],width=2,dash='dash'),
                                  marker=dict(size=7,symbol='diamond')))
        sumrows.append({'Product':prod,'WMA (LT/wk)':f"{wma:,.0f}",
                         'Trend':f"{trend:+,.0f}/wk",
                         f'Wk+1':f"{proj[0]:,.0f}",
                         f'{fw}wk Total':f"{sum(proj):,.0f}"})
    if future_weeks:
        fig.add_vrect(x0=future_weeks[0],x1=future_weeks[-1],
                       fillcolor='rgba(255,0,255,.05)',layer='below',line_width=0,
                       annotation_text="FORECAST ZONE",annotation_font_color='#ff00ff')
    fig.update_layout(paper_bgcolor='rgba(10,14,39,.9)',plot_bgcolor='rgba(10,14,39,.9)',
                       font=dict(color='white'),height=440,
                       xaxis=dict(gridcolor='rgba(255,255,255,.05)',title='Week'),
                       yaxis=dict(gridcolor='rgba(255,255,255,.05)',title='Volume (LT)'))
    st.plotly_chart(fig, use_container_width=True)
    if sumrows:
        st.markdown("### 📋 FORECAST SUMMARY")
        st.dataframe(pd.DataFrame(sumrows), use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 24 — PAGE: REORDER ALERTS
# ══════════════════════════════════════════════════════════════════════════════

def show_reorder_alerts():
    st.markdown("<h2>⚠️ REORDER ALERTS</h2>", unsafe_allow_html=True)
    st.markdown("---")
    has_balance  = bool(st.session_state.get('bdc_records'))
    has_loadings = not st.session_state.get('omc_df', pd.DataFrame()).empty
    if not has_balance: st.warning("⚠️ Fetch BDC Balance first.")
    if not has_loadings: st.warning("⚠️ Fetch OMC Loadings first.")
    if not (has_balance and has_loadings): return

    bal_df = pd.DataFrame(st.session_state.bdc_records)
    omc_df = st.session_state.omc_df.copy()
    col_bal = 'ACTUAL BALANCE (LT\\KG)'

    c1,c2,c3 = st.columns(3)
    with c1: crit_days  = st.number_input("Critical (days)",value=5,min_value=1,max_value=30)
    with c2: warn_days  = st.number_input("Warning (days)", value=10,min_value=1,max_value=60)
    with c3: reorder_buf= st.number_input("Buffer (days)",  value=7,min_value=1,max_value=30)

    omc_df['Date'] = pd.to_datetime(omc_df['Date'], errors='coerce')
    omc_df = omc_df.dropna(subset=['Date'])
    period = max((omc_df['Date'].max()-omc_df['Date'].min()).days,1) if not omc_df.empty else 30

    bdc_stock = bal_df.groupby(['BDC','Product'])[col_bal].sum().reset_index()
    bdc_stock.columns = ['BDC','Product','stock']
    bdc_dep = (
        omc_df[omc_df['Product'].isin(['PREMIUM','GASOIL','LPG'])]
        .groupby(['BDC','Product'])['Quantity'].sum().reset_index()
    ) if 'BDC' in omc_df.columns else pd.DataFrame()

    if bdc_dep.empty:
        st.warning("⚠️ BDC column missing in OMC Loadings.")
        return

    bdc_dep.columns = ['BDC','Product','depletion']
    bdc_dep['daily_rate'] = bdc_dep['depletion'] / period
    merged = bdc_stock.merge(bdc_dep, on=['BDC','Product'], how='left')
    merged['daily_rate'] = merged['daily_rate'].fillna(0)
    merged['days_remaining'] = merged.apply(
        lambda r: r['stock']/r['daily_rate'] if r['daily_rate']>0 else float('inf'), axis=1)
    merged['reorder_qty'] = merged.apply(
        lambda r: max(0,r['daily_rate']*(warn_days+reorder_buf)-r['stock']) if r['daily_rate']>0 else 0, axis=1)

    def _st(d):
        if d==float('inf'): return '⚪ NO DATA'
        if d<crit_days: return '🔴 CRITICAL'
        if d<warn_days: return '🟡 WARNING'
        if d<30: return '🟠 MONITOR'
        return '🟢 HEALTHY'
    merged['status'] = merged['days_remaining'].apply(_st)

    crit = merged[merged['days_remaining']<crit_days]
    warn = merged[(merged['days_remaining']>=crit_days)&(merged['days_remaining']<warn_days)]
    c1,c2,c3 = st.columns(3)
    c1.metric("🔴 Critical",len(crit))
    c2.metric("🟡 Warning", len(warn))
    c3.metric("BDCs",       merged['BDC'].nunique())

    if not crit.empty:
        st.error("🚨 CRITICAL — Immediate reorder:")
        for _,r in crit.sort_values('days_remaining').iterrows():
            st.markdown(f"**{r['BDC']}** — {r['Product']}: **{r['days_remaining']:.1f}d** | "
                        f"Reorder: **{r['reorder_qty']:,.0f} LT**")

    st.markdown("---")
    disp = merged.copy()
    for col in ['stock','depletion','daily_rate','reorder_qty']:
        disp[col] = disp[col].fillna(0).apply(lambda x: f"{x:,.0f}")
    disp['days_remaining'] = disp['days_remaining'].apply(lambda x: f"{x:.1f}" if x!=float('inf') else "∞")
    st.dataframe(disp.sort_values('days_remaining')[
        ['BDC','Product','stock','depletion','daily_rate','days_remaining','reorder_qty','status']
    ].rename(columns={'stock':'Stock (LT)','depletion':'Period Dep (LT)',
                       'daily_rate':'Daily Rate','days_remaining':'Days',
                       'reorder_qty':'Reorder (LT)'}),
        use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 25 — PAGE: WEEK-ON-WEEK
# ══════════════════════════════════════════════════════════════════════════════

def show_week_on_week():
    st.markdown("<h2>📆 WEEK-ON-WEEK COMPARISON</h2>", unsafe_allow_html=True)
    st.markdown("---")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### 📘 Period A")
        a_start = st.date_input("A: From", value=datetime.now()-timedelta(days=14), key='wow_as')
        a_end   = st.date_input("A: To",   value=datetime.now()-timedelta(days=8),  key='wow_ae')
    with c2:
        st.markdown("#### 📗 Period B")
        b_start = st.date_input("B: From", value=datetime.now()-timedelta(days=7), key='wow_bs')
        b_end   = st.date_input("B: To",   value=datetime.now(),                   key='wow_be')

    if st.button("⚡ FETCH & COMPARE", key='wow_fetch'):
        with st.status("Fetching Period A…", expanded=True) as sa:
            df_a = fetch_omc_loadings(a_start.strftime("%m/%d/%Y"), a_end.strftime("%m/%d/%Y"), bdc_name="")
            sa.update(label=f"✅ A: {len(df_a):,} records", state="complete")
        with st.status("Fetching Period B…", expanded=True) as sb:
            df_b = fetch_omc_loadings(b_start.strftime("%m/%d/%Y"), b_end.strftime("%m/%d/%Y"), bdc_name="")
            sb.update(label=f"✅ B: {len(df_b):,} records", state="complete")
        st.session_state.wow_a = {'df':df_a,'label':f"{a_start}→{a_end}",'days':max((a_end-a_start).days,1)}
        st.session_state.wow_b = {'df':df_b,'label':f"{b_start}→{b_end}",'days':max((b_end-b_start).days,1)}
        st.rerun()

    if not st.session_state.get('wow_a'):
        st.info("👆 Select two periods and click FETCH & COMPARE.")
        return

    wa, wb = st.session_state.wow_a, st.session_state.wow_b
    df_a, df_b = wa['df'], wb['df']
    PRODUCTS = ['PREMIUM','GASOIL','LPG']

    vol_a = df_a[df_a['Product'].isin(PRODUCTS)].groupby('Product')['Quantity'].sum() if not df_a.empty else pd.Series()
    vol_b = df_b[df_b['Product'].isin(PRODUCTS)].groupby('Product')['Quantity'].sum() if not df_b.empty else pd.Series()

    st.markdown("---")
    st.markdown("### 🛢️ NATIONAL VOLUME BY PRODUCT")
    cols = st.columns(3)
    prod_rows = []
    for i, prod in enumerate(PRODUCTS):
        va = float(vol_a.get(prod,0))
        vb = float(vol_b.get(prod,0))
        delta_abs = vb - va
        delta_pct = (delta_abs/va*100) if va>0 else (100. if vb>0 else 0.)
        arrow = "↑" if delta_abs>0 else "↓"
        dcol  = "#00ff88" if delta_abs>0 else "#ff4444"
        with cols[i]:
            st.markdown(f"""
            <div style='background:rgba(10,14,39,.85);padding:20px;border-radius:14px;
                        border:2px solid {_COLORS[prod]};text-align:center;'>
                <div style='font-family:Orbitron,sans-serif;color:{_COLORS[prod]};
                             font-size:15px;font-weight:700;'>{prod}</div>
                <div style='color:#888;font-size:11px;margin-top:8px;'>{wa['label']}</div>
                <div style='color:#e0e0e0;font-size:20px;font-weight:700;'>{va:,.0f} LT</div>
                <div style='color:#888;font-size:11px;margin-top:6px;'>{wb['label']}</div>
                <div style='color:#fff;font-size:24px;font-weight:700;'>{vb:,.0f} LT</div>
                <div style='color:{dcol};font-size:18px;font-weight:700;margin-top:8px;'>
                    {arrow} {abs(delta_abs):,.0f} LT ({delta_pct:+.1f}%)</div>
            </div>""", unsafe_allow_html=True)
        prod_rows.append({'Product':prod,'Period A (LT)':f"{va:,.0f}",
                           'Period B (LT)':f"{vb:,.0f}",'Delta':f"{delta_abs:+,.0f}",
                           'Change %':f"{delta_pct:+.1f}%"})

    st.markdown("---")
    st.markdown("### 🏭 BDC-LEVEL COMPARISON")
    prod_wow = st.selectbox("Product",['ALL']+PRODUCTS,key='wow_prod')

    def _vol(df, prod):
        if df.empty or 'BDC' not in df.columns: return pd.Series(dtype=float)
        f = df if prod=='ALL' else df[df['Product']==prod]
        return f.groupby('BDC')['Quantity'].sum()

    bdc_a = _vol(df_a, prod_wow)
    bdc_b = _vol(df_b, prod_wow)
    all_bdcs = sorted(set(bdc_a.index)|set(bdc_b.index))
    brows = []
    for bdc in all_bdcs:
        va = float(bdc_a.get(bdc,0))
        vb = float(bdc_b.get(bdc,0))
        d  = vb-va
        p  = (d/va*100) if va>0 else (100. if vb>0 else 0.)
        brows.append({'BDC':bdc,'Period A (LT)':va,'Period B (LT)':vb,'Delta (LT)':d,'Change %':round(p,1)})
    bdf = pd.DataFrame(brows).sort_values('Delta (LT)',ascending=False)

    fig = go.Figure()
    fig.add_trace(go.Bar(name=wa['label'],x=bdf['BDC'],y=bdf['Period A (LT)'],marker_color='rgba(0,255,255,.6)'))
    fig.add_trace(go.Bar(name=wb['label'],x=bdf['BDC'],y=bdf['Period B (LT)'],marker_color='rgba(255,0,255,.6)'))
    fig.update_layout(barmode='group',paper_bgcolor='rgba(10,14,39,.9)',plot_bgcolor='rgba(10,14,39,.9)',
                       font=dict(color='white'),height=420,
                       xaxis=dict(tickangle=-30),yaxis=dict(title='Volume (LT)'))
    st.plotly_chart(fig, use_container_width=True)

    disp = bdf.copy()
    for c in ['Period A (LT)','Period B (LT)','Delta (LT)']:
        disp[c] = disp[c].apply(lambda x: f"{x:+,.0f}" if c=='Delta (LT)' else f"{x:,.0f}")
    disp['Change %'] = disp['Change %'].apply(lambda x: f"{x:+.1f}%")
    st.dataframe(disp, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 26 — PAGE: WORLD RISK MONITOR
# ══════════════════════════════════════════════════════════════════════════════

def show_world_monitor():
    st.markdown("<h2>🌍 WORLD RISK MONITOR</h2>", unsafe_allow_html=True)
    st.info("🔴 LIVE GLOBAL INTELLIGENCE: Conflicts, military bases, nuclear sites, sanctions, weather, waterways & more.")
    st.link_button("🌍 OPEN WORLD RISK MONITOR", WORLD_MONITOR_URL, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 27 — PAGE: VESSEL SUPPLY
# ══════════════════════════════════════════════════════════════════════════════

def show_vessel_supply():
    VCOLS  = {'PREMIUM':'#00ffff','GASOIL':'#ffaa00','LPG':'#00ff88','NAPHTHA':'#ff6600'}
    VICONS = {'PREMIUM':'⛽','GASOIL':'🚛','LPG':'🔵','NAPHTHA':'🟠'}
    MONTH_ORDER = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']

    st.markdown("<h2>🚢 VESSEL SUPPLY TRACKER</h2>", unsafe_allow_html=True)
    st.markdown("---")

    c1, c2 = st.columns([3,1])
    with c1:
        sheet_url = st.text_input("Google Sheets URL", value=VESSEL_SHEET_URL, key='vessel_url')
    with c2:
        year_sel = st.selectbox("Year", ['2025','2024','2026'], key='vessel_year_input')

    if st.button("🔄 FETCH VESSEL DATA", use_container_width=True, key='vessel_fetch'):
        with st.spinner("Loading vessel data from Google Sheets…"):
            raw_df, err = _load_vessel_sheet(sheet_url)
            if raw_df is None:
                st.error(f"❌ {err}")
                return
            processed = _process_vessel_df(raw_df, year=year_sel)
            if processed.empty:
                st.warning("⚠️ No valid records found.")
                return
            st.session_state.vessel_data = processed
            st.session_state['vessel_year'] = year_sel
            st.success(f"✅ {len(processed)} vessel records processed!")
            st.rerun()

    if st.session_state.get('vessel_data') is None or st.session_state.vessel_data.empty:
        st.info("👆 Click FETCH VESSEL DATA to load from Google Sheets.")
        return

    df         = st.session_state.vessel_data
    yr_lbl     = st.session_state.get('vessel_year','2025')
    discharged = df[df['Status']=='DISCHARGED']
    pending    = df[df['Status']=='PENDING']
    total_vol  = df['Quantity_Litres'].sum()

    c1,c2,c3,c4 = st.columns(4)
    with c1: st.markdown(_metric_card("TOTAL VESSELS", str(len(df))), unsafe_allow_html=True)
    with c2: st.markdown(_metric_card("DISCHARGED", str(len(discharged)),
                          f"{discharged['Quantity_Litres'].sum()/1e6:.2f}M LT"), unsafe_allow_html=True)
    with c3: st.markdown(_metric_card("PENDING", str(len(pending)),
                          f"{pending['Quantity_Litres'].sum()/1e6:.2f}M LT", "#ffaa00"), unsafe_allow_html=True)
    with c4: st.markdown(_metric_card("TOTAL VOLUME", f"{total_vol/1e6:.2f}M", "Litres"), unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### ⏳ PENDING VESSELS")
    if pending.empty:
        st.success("✅ No pending vessels — all discharged.")
    else:
        pb_prod = (
            pending.groupby('Product')
            .agg(Vessels=('Vessel_Name','count'),Volume_LT=('Quantity_Litres','sum'),
                  Volume_MT=('Quantity_MT','sum')).reset_index()
        )
        pcols = st.columns(min(len(pb_prod),4))
        for col,(_, r) in zip(pcols, pb_prod.iterrows()):
            with col:
                st.markdown(f"""
                <div style='background:rgba(10,14,39,.85);padding:20px;border-radius:14px;
                            border:2px solid {VCOLS.get(r["Product"],"#fff")};text-align:center;'>
                    <div style='font-size:30px;'>{VICONS.get(r["Product"],"🛢")}</div>
                    <b style='color:{VCOLS.get(r["Product"],"#fff")};'>{r["Product"]}</b><br>
                    <span style='font-size:28px;color:#e0e0e0;'>{int(r["Vessels"])}</span>
                    <span style='color:#888;font-size:12px;'> vessels</span><br>
                    <span style='color:{VCOLS.get(r["Product"],"#fff")};font-size:17px;font-weight:700;'>
                        {r["Volume_LT"]:,.0f} LT</span>
                </div>""", unsafe_allow_html=True)
        pd_disp = pending[['Vessel_Name','Vessel_Type','Receivers','Supplier',
                            'Product','Quantity_MT','Quantity_Litres','Date_Discharged','Month']].copy()
        pd_disp['Quantity_MT']     = pd_disp['Quantity_MT'].apply(lambda x: f"{x:,.0f}")
        pd_disp['Quantity_Litres'] = pd_disp['Quantity_Litres'].apply(lambda x: f"{x:,.0f}")
        st.dataframe(pd_disp, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("### ✅ DISCHARGED VESSELS")
    if not discharged.empty:
        monthly = (
            discharged.groupby(['Month','Product'])['Quantity_Litres'].sum().reset_index()
        )
        monthly['Month'] = pd.Categorical(monthly['Month'],categories=MONTH_ORDER,ordered=True)
        monthly = monthly.sort_values('Month')
        fig = go.Figure()
        for prod in monthly['Product'].unique():
            pdata = monthly[monthly['Product']==prod]
            fig.add_trace(go.Bar(name=prod,x=pdata['Month'],y=pdata['Quantity_Litres'],
                                  marker_color=VCOLS.get(prod,'#fff')))
        fig.update_layout(barmode='group',paper_bgcolor='rgba(10,14,39,.9)',
                           plot_bgcolor='rgba(10,14,39,.9)',font=dict(color='white'),
                           height=380,xaxis=dict(title='Month'),yaxis=dict(title='Volume (LT)'))
        st.plotly_chart(fig, use_container_width=True)

        dd = discharged[['Vessel_Name','Vessel_Type','Receivers','Supplier',
                          'Product','Quantity_MT','Quantity_Litres','Date_Discharged','Month']].copy()
        dd['Quantity_MT']     = dd['Quantity_MT'].apply(lambda x: f"{x:,.0f}")
        dd['Quantity_Litres'] = dd['Quantity_Litres'].apply(lambda x: f"{x:,.0f}")
        st.dataframe(dd, use_container_width=True, hide_index=True)

    # Export
    st.markdown("---")
    if st.button("📄 EXPORT TO EXCEL", key='vessel_export', use_container_width=True):
        out_dir = os.path.join(os.getcwd(),"vessel_reports")
        os.makedirs(out_dir, exist_ok=True)
        fname = f"vessel_data_{yr_lbl}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        fpath = os.path.join(out_dir, fname)
        with pd.ExcelWriter(fpath, engine='openpyxl') as w:
            df.to_excel(w,          sheet_name='All Vessels', index=False)
            discharged.to_excel(w,  sheet_name='Discharged',  index=False)
            pending.to_excel(w,     sheet_name='Pending',      index=False)
        with open(fpath,'rb') as f:
            st.download_button("⬇️ DOWNLOAD VESSEL EXCEL", f, fname,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True, key='vessel_dl')


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 28 — MAIN ROUTER
# ══════════════════════════════════════════════════════════════════════════════

def main():
    st.markdown("""
    <div style='text-align:center;padding:30px 0;'>
        <h1 style='font-size:72px;margin:0;'>⚡ NPA ENERGY ANALYTICS ⚡</h1>
        <p style='font-size:24px;color:#ff00ff;font-family:"Orbitron",sans-serif;
                   letter-spacing:3px;margin-top:10px;'>FUEL THE FUTURE WITH DATA</p>
    </div>""", unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("<h2 style='text-align:center;'>🎯 MISSION CONTROL</h2>",
                    unsafe_allow_html=True)
        choice = st.radio("SELECT YOUR DATA MISSION:", [
            "🏦 BDC BALANCE",
            "🚚 OMC LOADINGS",
            "📅 DAILY ORDERS",
            "📊 MARKET SHARE",
            "🎯 COMPETITIVE INTEL",
            "📈 STOCK TRANSACTION",
            "🧠 BDC INTELLIGENCE",
            "🌍 NATIONAL STOCKOUT",
            "─────── MONITOR ───────",
            "🔴 LIVE RUNWAY MONITOR",
            "📉 HISTORICAL TRENDS",
            "🗺️ DEPOT STRESS MAP",
            "🔮 DEMAND FORECAST",
            "⚠️ REORDER ALERTS",
            "📆 WEEK-ON-WEEK",
            "🌍 WORLD RISK MONITOR",
            "─────── SUPPLY ────────",
            "🚢 VESSEL SUPPLY",
        ], index=0)
        st.markdown("---")
        st.markdown("""
        <div style='text-align:center;padding:20px;background:rgba(255,0,255,.1);
                    border-radius:10px;border:2px solid #ff00ff;'>
            <h3>⚙️ SYSTEM STATUS</h3>
            <p style='color:#00ff88;font-size:20px;'>🟢 OPERATIONAL</p>
        </div>""", unsafe_allow_html=True)

    page_map = {
        "🏦 BDC BALANCE":        show_bdc_balance,
        "🚚 OMC LOADINGS":       show_omc_loadings,
        "📅 DAILY ORDERS":       show_daily_orders,
        "📊 MARKET SHARE":       show_market_share,
        "🎯 COMPETITIVE INTEL":  show_competitive_intel,
        "📈 STOCK TRANSACTION":  show_stock_transaction,
        "🧠 BDC INTELLIGENCE":   show_bdc_intelligence,
        "🌍 NATIONAL STOCKOUT":  show_national_stockout,
        "🔴 LIVE RUNWAY MONITOR":show_live_runway_monitor,
        "📉 HISTORICAL TRENDS":  show_historical_trends,
        "🗺️ DEPOT STRESS MAP":   show_depot_stress_map,
        "🔮 DEMAND FORECAST":    show_demand_forecast,
        "⚠️ REORDER ALERTS":     show_reorder_alerts,
        "📆 WEEK-ON-WEEK":       show_week_on_week,
        "🌍 WORLD RISK MONITOR": show_world_monitor,
        "🚢 VESSEL SUPPLY":      show_vessel_supply,
    }

    fn = page_map.get(choice)
    if fn:
        fn()
    else:
        st.info("Select a page from the sidebar.")


main()