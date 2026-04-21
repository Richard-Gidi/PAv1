"""
NPA ENERGY ANALYTICS — STREAMLIT DASHBOARD
===========================================
Optimised build:
  • Per-BDC userId credentials from .env (BDC_USER_* keys)
  • Sequential-batch fetch with per-BDC retry — no BDC silently skipped
  • Multi-layer deduplication (within-PDF + cross-BDC)
  • Plotly charts on every data page
  • Shared helpers — no repeated pivot/chart logic
  • Session-state caching — re-renders never re-fetch
  • Normalised BDC name matching for Market Share cross-join

INSTALLATION:
    pip install streamlit pandas pdfplumber PyPDF2 openpyxl python-dotenv plotly requests psutil

USAGE:
    streamlit run npa_dashboard.py
"""

import streamlit as st
import os, re, io, json, time, threading
from datetime import datetime, timedelta
import pandas as pd
import pdfplumber
import PyPDF2
from dotenv import load_dotenv
import plotly.graph_objects as go
import requests as _requests
import psutil

load_dotenv()

# ══════════════════════════════════════════════════════════════
# CONSTANTS & THEME
# ══════════════════════════════════════════════════════════════
PROD_COLORS = {"PREMIUM": "#00ffff", "GASOIL": "#ffaa00", "LPG": "#00ff88",
               "NAPHTHA": "#ff6600", "ATK": "#ff44aa", "RFO": "#aa44ff"}
PROD_ICONS  = {"PREMIUM": "⛽", "GASOIL": "🚛", "LPG": "🔵", "NAPHTHA": "🟠"}
MONTH_ORDER = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]

BATCH_SIZE  = 6
MAX_RETRIES = 3
RETRY_DELAY = 2

VESSEL_CF = {"PREMIUM": 1324.50, "GASOIL": 1183.00, "LPG": 1000.00, "NAPHTHA": 800.00}
VESSEL_PM = {"PMS":"PREMIUM","GASOLINE":"PREMIUM","AGO":"GASOIL",
             "GASOIL":"GASOIL","LPG":"LPG","BUTANE":"LPG","NAPHTHA":"NAPHTHA"}
VESSEL_MM = {m[:3].title(): m[:3].upper() for m in
             ["January","February","March","April","May","June",
              "July","August","September","October","November","December"]}

_PLOT_BASE = dict(
    paper_bgcolor="rgba(10,14,39,0.95)",
    plot_bgcolor="rgba(10,14,39,0.95)",
    font=dict(color="#e0e0e0", family="Rajdhani"),
    legend=dict(font=dict(color="#e0e0e0")),
    margin=dict(l=10, r=10, t=44, b=10),
)

_proc = psutil.Process(os.getpid())

# ══════════════════════════════════════════════════════════════
# ENVIRONMENT LOADERS
# ══════════════════════════════════════════════════════════════
_BDC_USER_FIXES = {
    "C CLEANED OIL LTD":                    "C. CLEANED OIL LTD",
    "PK JEGS ENERGY LTD":                   "P.K JEGS ENERGY LTD",
    "TEMA OIL REFINERY TOR":                "TEMA OIL REFINERY(TOR)",
    "SOCIETE NATIONAL BURKINABE SONABHY":   "SOCIETE NATIONAL BURKINABE (SONABHY)",
    "BOST G40":                             "BOST-G40",
    "DOMINION INTERNATIONAL PETROLEUM":     "DOMINION INTERNATIONAL PETR",
    "PETROLEUM WARE HOUSE AND SUPPLIES":    "PETROLEUM WARE HOUSE AND S",
    "INTERNATIONAL PETROLEUM RESOURCES":    "INTERNATIONAL PETROLEUM RES",
    "GENYSIS GLOBAL LIMITED":               "Genysis Global Limited",
    "GLORYMAY PETROLEUM COMPANY LIMITED":   "GLORYMAY PETROLEUM COMPAN",
    "HILSON PETROLEUM GHANA LIMITED":       "HILSON PETROLEUM GHANA LIM",
    "PLATON OIL AND GAS":                   "Platon Oil and Gas",
    "PORTICA OIL AND GAS RESOURCE LIMITED": "Portica Oil and Gas Resource Lim",
    "RESTON ENERGY TRADING LIMITED":        "Reston Energy Trading Limited",
    "BATTOP ENERGY LIMITED":                "Battop Energy Limited",
}
_BDC_ID_FIXES = {
    "TEMA OIL REFINERY TOR":              "TEMA OIL REFINERY (TOR)",
    "SOCIETE NATIONAL BURKINABE SONABHY": "SOCIETE NATIONAL BURKINABE (SONABHY)",
    "LIB GHANA LIMITED":                  "L.I.B. GHANA LIMITED",
    "C CLEANED OIL LTD":                  "C. CLEANED OIL LTD",
    "PK JEGS ENERGY LTD":                 "P. K JEGS ENERGY LTD",
}
_DEPOT_FIXES = {
    "GHANA OIL COLTD TAKORADI":               "GHANA OIL CO.LTD, TAKORADI",
    "GOIL LPG BOTTLING PLANT TEMA":           "GOIL LPG BOTTLING PLANT -TEMA",
    "GOIL LPG BOTTLING PLANT KUMASI":         "GOIL LPG BOTTLING PLANT- KUMASI",
    "NEWGAS CYLINDER BOTTLING LIMITED TEMA":  "NEWGAS CYLINDER BOTTLING LIMITED-TEMA",
    "CHASE PETROLEUM TEMA":                   "CHASE PETROLEUM - TEMA",
    "TEMA FUEL COMPANY TFC":                  "TEMA FUEL COMPANY (TFC)",
    "TEMA MULTI PRODUCTS TMPT":               "TEMA MULTI PRODUCTS (TMPT)",
    "TEMA OIL REFINERY TOR":                  "TEMA OIL REFINERY (TOR)",
    "GHANA OIL COMPANY LTD SEKONDI NAVAL BASE": "GHANA OIL COMPANY LTD (SEKONDI NAVAL BASE)",
    "GHANSTOCK LIMITED TAKORADI":             "GHANSTOCK LIMITED (TAKORADI)",
}


def _load_bdc_user_map():
    m = {}
    for k, v in os.environ.items():
        if not k.startswith("BDC_USER_"): continue
        raw = k[len("BDC_USER_"):].replace("_"," ").strip()
        try: m[_BDC_USER_FIXES.get(raw, raw)] = int(v)
        except ValueError: pass
    return m


def _load_bdc_map():
    m = {}
    for k, v in os.environ.items():
        if not k.startswith("BDC_") or k.startswith("BDC_USER_"): continue
        name = k[4:].replace("_"," ")
        try: m[_BDC_ID_FIXES.get(name, name)] = int(v)
        except ValueError: pass
    return m


def _load_depot_map():
    m = {}
    for k, v in os.environ.items():
        if not k.startswith("DEPOT_"): continue
        name = k[6:].replace("_"," ")
        if name in _DEPOT_FIXES:
            name = _DEPOT_FIXES[name]
        elif "BOST " in name and name != "BOST GLOBAL DEPOT":
            parts = name.split(" ",1)
            name  = f"{parts[0]} - {parts[1]}" if len(parts)==2 else name
        elif name.endswith(" TEMA") and "SENTUO" in name:
            name = name.replace(" TEMA","- TEMA")
        try: m[name] = int(v)
        except ValueError: pass
    return m


BDC_USER_MAP = _load_bdc_user_map()
BDC_MAP      = _load_bdc_map()
DEPOT_MAP    = _load_depot_map()
PRODUCT_MAP  = {
    "PMS":    int(os.getenv("PRODUCT_PREMIUM_ID","12")),
    "Gasoil": int(os.getenv("PRODUCT_GASOIL_ID", "14")),
    "LPG":    int(os.getenv("PRODUCT_LPG_ID",    "28")),
}

NPA = {
    "COMPANY_ID": os.getenv("NPA_COMPANY_ID",    "1"),
    "USER_ID":    os.getenv("NPA_USER_ID",        "123292"),
    "APP_ID":     os.getenv("NPA_APP_ID",         "3"),
    "ITS":        os.getenv("NPA_ITS_FROM_PERSOL","Persol Systems Limited"),
    "BALANCE_URL":os.getenv("NPA_BDC_BALANCE_URL",
                     "https://iml.npa-enterprise.com/NPAAPILIVE/Home/CreateProductBalance"),
    "OMC_URL":    os.getenv("NPA_OMC_LOADINGS_URL",
                     "https://iml.npa-enterprise.com/NewNPA/home/CreateOrdersReport"),
    "DAILY_URL":  os.getenv("NPA_DAILY_ORDERS_URL",
                     "https://iml.npa-enterprise.com/NewNPA/home/CreateDailyOrderReport"),
    "TXN_URL":    os.getenv("NPA_STOCK_TRANSACTION_URL",
                     "https://iml.npa-enterprise.com/NewNPA/home/CreateStockTransactionReport"),
}
WORLD_MONITOR_URL = os.getenv("WORLD_MONITOR_URL",
    "https://www.worldmonitor.app/?lat=17.7707&lon=0.0000&zoom=1.30&view=global&timeRange=7d"
    "&layers=conflicts%2Cbases%2Chotspots%2Cnuclear%2Csanctions%2Cweather%2Ceconomic"
    "%2Cwaterways%2Coutages%2Cmilitary%2Cnatural%2CiranAttacks")
VESSEL_SHEET_URL = os.getenv("VESSEL_SHEETS_URL",
    "https://docs.google.com/spreadsheets/d/1z-L79N22rU3p6wLw1CEVWDIw6QSwA5CH/edit?rtpof=true")
SNAPSHOT_DIR = os.path.join(os.getcwd(), "national_snapshots")


# ══════════════════════════════════════════════════════════════
# PAGE CONFIG & CSS
# ══════════════════════════════════════════════════════════════
st.set_page_config(page_title="NPA Energy Analytics 🛢️", page_icon="⚡",
                   layout="wide", initial_sidebar_state="expanded")
st.caption(f"🧠 {_proc.memory_info().rss/1024/1024:.1f} MB  |  📋 {len(BDC_USER_MAP)} BDCs configured")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Rajdhani:wght@300;500;700&display=swap');
.stApp{background:linear-gradient(-45deg,#0a0e27,#1a1a2e,#16213e,#0f3460);
    background-size:400% 400%;animation:gradientShift 15s ease infinite;}
@keyframes gradientShift{0%{background-position:0% 50%}50%{background-position:100% 50%}100%{background-position:0% 50%}}
h1,h2,h3{font-family:'Orbitron',sans-serif!important;color:#00ffff!important;
    text-shadow:0 0 10px #00ffff,0 0 20px #00ffff;}
p,span,div,label{font-family:'Rajdhani',sans-serif;color:#e0e0e0;}
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
.fetch-log{font-family:monospace;font-size:12px;background:rgba(0,0,0,0.5);
    border:1px solid #00ffff33;border-radius:8px;padding:10px;max-height:180px;overflow-y:auto;}
.info-box{background:rgba(0,255,255,0.04);border:1px solid #00ffff33;border-radius:10px;
    padding:12px 16px;margin-bottom:16px;font-family:'Rajdhani',sans-serif;
    font-size:15px;line-height:1.55;}
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# SHARED UI HELPERS
# ══════════════════════════════════════════════════════════════
def _info(what: str, prereq: str = ""):
    pre = f"<br><b style='color:#ff00ff;'>Prerequisite:</b> {prereq}" if prereq else ""
    st.markdown(
        f"<div class='info-box'><b style='color:#00ffff;'>What this page does</b><br>"
        f"{what}{pre}</div>",
        unsafe_allow_html=True,
    )


def _plotly(fig: go.Figure, height: int = 380):
    fig.update_layout(height=height, **_PLOT_BASE)
    st.plotly_chart(fig, use_container_width=True)


def _bar(df, x, y, color="#00ffff", title="", height=380):
    fig = go.Figure(go.Bar(x=df[x], y=df[y], marker_color=color,
                           text=df[y].apply(lambda v: f"{v:,.0f}"),
                           textposition="outside"))
    fig.update_layout(title=dict(text=title,font=dict(color="#00ffff",family="Orbitron")),
                      xaxis_title=x, yaxis_title=y)
    _plotly(fig, height)


def _pie(labels, values, title="", colors=None):
    fig = go.Figure(go.Pie(
        labels=labels, values=values,
        marker=dict(colors=colors or [PROD_COLORS.get(str(l),"#888") for l in labels]),
        textinfo="label+percent", hole=0.4,
    ))
    fig.update_layout(title=dict(text=title,font=dict(color="#00ffff",family="Orbitron")))
    _plotly(fig)


def _trend(df, date_col="Date", val_col="Quantity", group_col="Product", title=""):
    try:
        df2 = df.copy()
        df2[date_col] = pd.to_datetime(df2[date_col], errors="coerce")
        df2 = df2.dropna(subset=[date_col])
        if df2[date_col].nunique() < 2: return
        daily = df2.groupby([date_col, group_col])[val_col].sum().reset_index()
        fig   = go.Figure()
        for grp in daily[group_col].unique():
            sub = daily[daily[group_col]==grp]
            fig.add_trace(go.Scatter(x=sub[date_col], y=sub[val_col], name=grp,
                                     line=dict(color=PROD_COLORS.get(grp,"#aaa"),width=2),
                                     mode="lines+markers"))
        fig.update_layout(title=dict(text=title,font=dict(color="#00ffff",family="Orbitron")),
                          xaxis_title="Date", yaxis_title="Volume (LT)")
        _plotly(fig)
    except Exception:
        pass


def _stack_share(df_bdc, df_all, prod_col, qty_col, bdc_name, title):
    """Stacked bar: BDC share vs rest-of-market per product."""
    fig = go.Figure()
    for prod in ["PREMIUM","GASOIL","LPG"]:
        bv  = float(df_bdc[df_bdc[prod_col]==prod][qty_col].sum())
        mkt = float(df_all[df_all[prod_col]==prod][qty_col].sum())
        color = PROD_COLORS.get(prod,"#888")
        fig.add_trace(go.Bar(x=[prod], y=[bv], name=f"{prod} — {bdc_name}",
                             marker_color=color))
        fig.add_trace(go.Bar(x=[prod], y=[max(mkt-bv,0)],
                             name=f"{prod} — Rest", marker_color=color,
                             opacity=0.22, showlegend=False))
    fig.update_layout(barmode="stack",
                      title=dict(text=title,font=dict(color="#00ffff",family="Orbitron")),
                      xaxis_title="Product", yaxis_title="Litres / KG")
    _plotly(fig)


def _standard_pivot(df, index, value, products=("GASOIL","LPG","PREMIUM")):
    piv = df.pivot_table(index=index, columns="Product", values=value,
                         aggfunc="sum", fill_value=0).reset_index()
    for p in products:
        if p not in piv.columns: piv[p] = 0
    piv["TOTAL"] = piv[[p for p in products if p in piv.columns]].sum(axis=1)
    return piv.sort_values("TOTAL", ascending=False)


def _to_excel(sheets: dict) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        for name, df in sheets.items():
            df.to_excel(w, sheet_name=str(name)[:31], index=False)
    return buf.getvalue()


def _dl(sheets, filename, label="⬇️ DOWNLOAD EXCEL"):
    st.download_button(label, _to_excel(sheets), filename,
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


def _fetch_summary(summary, total, count, label):
    n_ok   = len(summary["success"])
    n_none = len(summary["no_data"])
    n_fail = len(summary["failed"])
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("BDCs Queried", total)
    c2.metric("✅ With Data", n_ok)
    c3.metric("⚠️ No Data",   n_none)
    c4.metric("❌ Failed",    n_fail)
    st.metric(f"📋 Total {label}", f"{count:,}")
    if n_fail:
        with st.expander(f"❌ {n_fail} failed"):
            st.markdown("\n".join(f"- `{b}`" for b in summary["failed"]))
    if n_none:
        with st.expander(f"⚠️ {n_none} no data"):
            st.markdown("\n".join(f"- `{b}`" for b in summary["no_data"]))


# ══════════════════════════════════════════════════════════════
# HTTP LAYER
# ══════════════════════════════════════════════════════════════
_HTTP_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
    "Accept": "application/pdf,text/html,*/*;q=0.8",
    "Connection": "keep-alive",
}


def _fetch_pdf(url, params, timeout=60):
    try:
        r = _requests.get(url, params=params, headers=_HTTP_HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.content if r.content[:4] == b"%PDF" else None
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════
# BATCH FETCHER
# ══════════════════════════════════════════════════════════════
def _batch_fetch(bdc_list, fetch_fn, prog, status_text, log_lines):
    import concurrent.futures as _cf
    total  = len(bdc_list)
    res    = {}
    lock   = threading.Lock()
    done_n = [0]

    def _attempt(name):
        last_err = None
        for att in range(1, MAX_RETRIES+1):
            try:
                return name, fetch_fn(name), att, None
            except Exception as e:
                last_err = e
                if att < MAX_RETRIES: time.sleep(RETRY_DELAY * att)
        return name, None, MAX_RETRIES, str(last_err)

    batches = [bdc_list[i:i+BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]
    for bi, batch in enumerate(batches):
        with _cf.ThreadPoolExecutor(max_workers=BATCH_SIZE) as ex:
            for fut in _cf.as_completed({ex.submit(_attempt,b):b for b in batch}):
                name, result, att, err = fut.result()
                res[name] = result
                with lock:
                    done_n[0] += 1
                    prog.progress(done_n[0]/total, text=f"Fetched {done_n[0]}/{total}…")
                icon = "❌" if err else ("⚠️" if result is None else ("🔄" if att>1 else "✅"))
                note = (f"FAILED ({err})" if err else "No data" if result is None
                        else f"OK (retry {att})" if att>1 else "OK")
                log_lines.append(f"{icon} {name}: {note}")
                status_text.markdown(
                    f"<div class='fetch-log'>{'<br>'.join(log_lines[-12:])}</div>",
                    unsafe_allow_html=True)
        if bi < len(batches)-1: time.sleep(0.4)
    return res


# ══════════════════════════════════════════════════════════════
# PDF PARSERS
# ══════════════════════════════════════════════════════════════

# ── Balance ──────────────────────────────────────────────────
class _BalanceScraper:
    _ALLOWED = {"PREMIUM","GASOIL","LPG"}
    _PAT     = re.compile(
        r"^(PREMIUM|GASOIL|LPG)\s+([\d,]+\.\d{2})\s+(-?[\d,]+\.\d{2})$", re.IGNORECASE)
    _BOST_GL = re.compile(r"\bBOST\s*GLOBAL\s*DEPOT\b", re.IGNORECASE)
    _DATE_RE = re.compile(r"(\w+\s+\d{1,2}\s*,\s*\d{4})")

    @staticmethod
    def _ns(t): return re.sub(r"\s+"," ",(t or "").strip())

    def _norm_bdc(self, b):
        c = self._ns(b)
        return "BOST" if self._ns(c.upper().replace("-"," ").replace("_"," ")).startswith("BOST") else c

    def _is_bost(self, d):
        return self._ns((d or "").replace("-"," ")).upper().startswith("BOST ")

    def _is_bost_global(self, d):
        return bool(self._BOST_GL.search(self._ns((d or "").replace("-"," "))))

    def _parse_date(self, line):
        m = self._DATE_RE.search(line)
        if m:
            try: return datetime.strptime(m.group(1).replace(" ,",","),"%B %d, %Y").strftime("%Y/%m/%d")
            except Exception: pass
        return None

    def parse(self, pdf_bytes):
        records, seen = [], set()
        try:
            reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
            cur_bdc = cur_depot = cur_date = None
            for page in reader.pages:
                for line in (page.extract_text() or "").split("\n"):
                    line = line.strip()
                    if not line: continue
                    up = line.upper()
                    if "DATE AS AT" in up:
                        cur_date = self._parse_date(line) or cur_date
                    elif up.startswith("BDC"):
                        cur_bdc  = re.sub(r"^BDC\s*:\s*","",line,flags=re.IGNORECASE).strip()
                    elif up.startswith("DEPOT"):
                        cur_depot = re.sub(r"^DEPOT\s*:\s*","",line,flags=re.IGNORECASE).strip()
                    elif cur_bdc and cur_depot and cur_date:
                        m = self._PAT.match(line)
                        if not m: continue
                        prod   = m.group(1).upper()
                        actual = float(m.group(2).replace(",",""))
                        avail  = float(m.group(3).replace(",",""))
                        if prod not in self._ALLOWED or actual <= 0: continue
                        if self._is_bost(cur_depot) and not self._is_bost_global(cur_depot): continue
                        nb, nd = self._norm_bdc(cur_bdc), self._ns(cur_depot)
                        key = (nb, nd, prod, cur_date)
                        if key in seen: continue
                        seen.add(key)
                        records.append({"Date":cur_date,"BDC":nb,"DEPOT":nd,"Product":prod,
                                        "ACTUAL BALANCE (LT\\KG)":actual,
                                        "AVAILABLE BALANCE (LT\\KG)":avail})
        except Exception: pass
        return records


# ── OMC Loadings ──────────────────────────────────────────────
_PMAP_OMC  = {"AGO":"GASOIL","PMS":"PREMIUM","LPG":"LPG"}
_OMC_COLS  = ["Date","OMC","Truck","Product","Quantity","Price","Depot","Order Number","BDC"]
_HDR_KW    = {"ORDER REPORT","National Petroleum Authority","ORDER NUMBER","ORDER DATE",
              "ORDER STATUS","Total for :","Printed By :","BRV NUMBER","VOLUME"}
_LOADED_KW = {"Released","Submitted"}


def _detect_product(line):
    return _PMAP_OMC.get("AGO" if "AGO" in line else "LPG" if "LPG" in line else "PMS","PREMIUM")


def _parse_omc_line(line, product, depot, bdc):
    tokens = line.split()
    if len(tokens) < 6: return None
    ri = next((i for i,t in enumerate(tokens) if t in _LOADED_KW), None)
    if ri is None or ri < 2: return None
    try:
        vol   = float(tokens[-1].replace(",",""))
        price = float(tokens[-2].replace(",",""))
        brv   = tokens[-3]
        omc   = " ".join(tokens[ri+1:-3]).strip()
        try:    ds = datetime.strptime(tokens[0],"%d-%b-%Y").strftime("%Y/%m/%d")
        except: ds = tokens[0]
        return {"Date":ds,"OMC":omc,"Truck":brv,"Product":product,
                "Quantity":vol,"Price":price,"Depot":depot,"Order Number":tokens[1],"BDC":bdc}
    except Exception: return None


def _parse_omc_pdf(pdf_bytes, bdc_name=""):
    rows, cur_depot, cur_bdc, cur_prod = [], "", bdc_name, "PREMIUM"
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or page.extract_text(x_tolerance=2,y_tolerance=2) or ""
                for raw in text.split("\n"):
                    line = raw.strip()
                    if not line: continue
                    if "DEPOT:" in line:
                        m = re.search(r"DEPOT:([^-\n]+)",line)
                        if m: cur_depot = m.group(1).strip()
                        continue
                    if "BDC:" in line:
                        m = re.search(r"BDC:([^\n]+)",line)
                        if m: cur_bdc = m.group(1).strip()
                        continue
                    if "PRODUCT" in line: cur_prod = _detect_product(line); continue
                    if any(h in line for h in _HDR_KW): continue
                    if any(kw in line for kw in _LOADED_KW):
                        row = _parse_omc_line(line, cur_prod, cur_depot, cur_bdc)
                        if row: rows.append(row)
    except Exception: pass
    if not rows: return pd.DataFrame(columns=_OMC_COLS)
    df = pd.DataFrame(rows)
    for c in _OMC_COLS:
        if c not in df.columns: df[c] = ""
    df = df[_OMC_COLS].drop_duplicates(subset=["Order Number","Truck","Date","Product"])
    try:
        ds = pd.to_datetime(df["Date"], format="%Y/%m/%d", errors="coerce")
        df = df.assign(_ds=ds).sort_values("_ds").drop(columns=["_ds"]).reset_index(drop=True)
    except Exception: pass
    return df


# ── Daily Orders ──────────────────────────────────────────────
def _prod_cat(text):
    t = text.upper()
    if "AVIATION" in t or "TURBINE" in t: return "ATK"
    if "RFO"    in t:                     return "RFO"
    if "PREMIX" in t:                     return "PREMIX"
    if "LPG"    in t:                     return "LPG"
    if any(x in t for x in ("AGO","MGO","GASOIL")): return "GASOIL"
    return "PREMIUM"


def _parse_daily_line(line, last_date):
    pv = re.search(r"(\d{1,4}\.\d{2,4})\s+(\d{1,3}(?:,\d{3})*\.\d{2})$", line.strip())
    if not pv: return None
    price, volume = float(pv.group(1)), float(pv.group(2).replace(",",""))
    rem = line[:pv.start()].strip().split()
    if not rem: return None
    brv, rem = rem[-1], " ".join(rem[:-1])
    date_val = last_date
    dm = re.search(r"(\d{2}/\d{2}/\d{4})", rem)
    if dm:
        try: date_val = datetime.strptime(dm.group(1),"%d/%m/%Y").strftime("%Y/%m/%d")
        except: date_val = dm.group(1)
        rem = rem.replace(dm.group(1),"").strip()
    _noise = {"PMS","AGO","LPG","RFO","ATK","PREMIX","FOREIGN","RETAIL","OUTLETS",
              "MGO","LOCAL","ADDITIVATED","DIFFERENTIATED","MINES","CELL","SITES",
              "TURBINE","KEROSENE","(",")","-"}
    order_num = " ".join(t for t in rem.split() if t.upper() not in _noise) or rem
    return {"Date":date_val,"Order Number":order_num,
            "Product":_prod_cat(line),"Truck":brv,"Price":price,"Quantity":volume}


def _parse_daily_pdf(pdf_bytes, bdc_name=""):
    rows = []
    ctx  = {"Depot":"Unknown","BDC":bdc_name,"Status":"Unknown","Date":None}
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text(x_tolerance=2,y_tolerance=2)
                if not text: continue
                for line in text.split("\n"):
                    cl = line.strip()
                    if not cl: continue
                    if cl.startswith("DEPOT:"):
                        raw = cl.replace("DEPOT:","").strip()
                        ctx["Depot"] = ("BOST Global"
                            if raw.startswith("BOST") or "TAKORADI BLUE OCEAN" in raw else raw)
                        continue
                    if cl.startswith("BDC:"):
                        ctx["BDC"] = cl.replace("BDC:","").strip(); continue
                    if "Order Status" in cl:
                        parts = cl.split(":")
                        if len(parts)>1: ctx["Status"] = parts[-1].strip()
                        continue
                    if not re.search(r"\d{2}$",cl): continue
                    row = _parse_daily_line(cl, ctx["Date"])
                    if row:
                        if row["Date"]: ctx["Date"] = row["Date"]
                        rows.append({**row,"Depot":ctx["Depot"],
                                     "BDC":ctx["BDC"],"Status":ctx["Status"]})
    except Exception: pass
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["BDC"] = df["BDC"].apply(lambda n: " ".join((n or "").split()[:2]).upper())
    return df.drop_duplicates(subset=["Date","Truck","Order Number","Product"])


# ── Stock Transaction ─────────────────────────────────────────
_TXN_DESCS    = sorted(["Balance b/fwd","Stock Take","Sale","Custody Transfer In",
                         "Custody Transfer Out","Product Outturn"], key=len, reverse=True)
_TXN_SKIP_PFX = ("national petroleum","stock transaction","bdc :","depot :","product :",
                  "printed by","printed on","date trans","actual stock","stock commitments",
                  "available stock","last stock","i.t.s from")


def _parse_txn_pdf(pdf_bytes):
    def _skip(line):
        lo = line.strip().lower()
        return lo.startswith(_TXN_SKIP_PFX) or bool(re.match(r"^\d{1,2}\s+\w+,\s+\d{4}",line.strip()))

    def _pnum(s):
        s = s.strip()
        neg = s.startswith("(") and s.endswith(")")
        try:
            v = int(s.strip("()").replace(",",""))
            return -v if neg else v
        except ValueError: return None

    def _parse_line(line):
        line = line.strip()
        if not re.match(r"^\d{2}/\d{2}/\d{4}\b",line): return None
        parts = line.split()
        date, trans = parts[0], (parts[1] if len(parts)>1 else "")
        rest = line[len(date):].strip()[len(trans):].strip()
        desc = after = None
        for d in _TXN_DESCS:
            if rest.lower().startswith(d.lower()):
                desc, after = d, rest[len(d):].strip(); break
        if desc is None or desc == "Balance b/fwd": return None
        nums = re.findall(r"\([\d,]+\)|[\d,]+", after)
        if len(nums) < 2: return None
        vol, bal = _pnum(nums[-2]), _pnum(nums[-1])
        trail = re.search(re.escape(nums[-2])+r"\s+"+re.escape(nums[-1])+r"\s*$", after)
        acct  = after[:trail.start()].strip() if trail else " ".join(after.split()[:-2])
        return {"Date":date,"Trans #":trans,"Description":desc,
                "Account":acct,"Volume":vol or 0,"Balance":bal or 0}

    records, seen = [], set()
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                for raw in (page.extract_text() or "").split("\n"):
                    line = raw.strip()
                    if not line or _skip(line): continue
                    row = _parse_line(line)
                    if row:
                        key = (row["Date"],row["Trans #"],row["Description"],row["Volume"])
                        if key not in seen:
                            seen.add(key); records.append(row)
    except Exception: pass
    return records


# ══════════════════════════════════════════════════════════════
# FETCH FACTORIES
# ══════════════════════════════════════════════════════════════
def _balance_fetcher():
    scraper = _BalanceScraper()
    def _fn(name):
        uid = BDC_USER_MAP.get(name)
        if not uid: return None
        params = {"lngCompanyId":NPA["COMPANY_ID"],"strITSfromPersol":NPA["ITS"],
                  "strGroupBy":"BDC","strGroupBy1":"DEPOT",
                  "strQuery1":"","strQuery2":"","strQuery3":"","strQuery4":"",
                  "strPicHeight":"1","szPicWeight":"1",
                  "lngUserId":str(uid),"intAppId":NPA["APP_ID"]}
        pdf = _fetch_pdf(NPA["BALANCE_URL"], params)
        if not pdf: raise RuntimeError("No PDF")
        return scraper.parse(pdf)
    return _fn


def _omc_fetcher(start, end):
    def _fn(name):
        uid = BDC_USER_MAP.get(name)
        if not uid: return None
        params = {"lngCompanyId":NPA["COMPANY_ID"],"szITSfromPersol":"persol",
                  "strGroupBy":"BDC","strGroupBy1":"",
                  "strQuery1":" and iorderstatus=4",
                  "strQuery2":start,"strQuery3":end,"strQuery4":"",
                  "strPicHeight":"","strPicWeight":"","intPeriodID":"4",
                  "iUserId":str(uid),"iAppId":NPA["APP_ID"]}
        pdf = _fetch_pdf(NPA["OMC_URL"], params)
        if not pdf: raise RuntimeError("No PDF")
        return _parse_omc_pdf(pdf, name)
    return _fn


def _daily_fetcher(start, end):
    def _fn(name):
        uid = BDC_USER_MAP.get(name)
        if not uid: return None
        params = {"lngCompanyId":NPA["COMPANY_ID"],"szITSfromPersol":"persol",
                  "strGroupBy":"DEPOT","strGroupBy1":"",
                  "strQuery1":"","strQuery2":start,"strQuery3":end,"strQuery4":"",
                  "strPicHeight":"1","strPicWeight":"1","intPeriodID":"-1",
                  "iUserId":str(uid),"iAppId":NPA["APP_ID"]}
        pdf = _fetch_pdf(NPA["DAILY_URL"], params)
        if not pdf: raise RuntimeError("No PDF")
        return _parse_daily_pdf(pdf, name)
    return _fn


# ══════════════════════════════════════════════════════════════
# RESULT COMBINERS
# ══════════════════════════════════════════════════════════════
_COL_BAL = "ACTUAL BALANCE (LT\\KG)"


def _combine_balance(results):
    records = []
    summary = {"success":[],"no_data":[],"failed":[]}
    for bdc, recs in results.items():
        if recs is None:     summary["failed"].append(bdc)
        elif len(recs) == 0: summary["no_data"].append(bdc)
        else:
            summary["success"].append(bdc); records.extend(recs)
    if records:
        df = (pd.DataFrame(records)
              .sort_values(_COL_BAL, ascending=False)
              .drop_duplicates(subset=["BDC","DEPOT","Product","Date"])
              .reset_index(drop=True))
        records = df.to_dict("records")
    return records, summary


def _combine_df(results, dedup):
    frames  = []
    summary = {"success":[],"no_data":[],"failed":[]}
    for bdc, df in results.items():
        if df is None or not isinstance(df, pd.DataFrame): summary["failed"].append(bdc)
        elif df.empty:                                      summary["no_data"].append(bdc)
        else:
            summary["success"].append(bdc); frames.append(df)
    if not frames: return pd.DataFrame(), summary
    combined = pd.concat(frames, ignore_index=True)
    valid    = [c for c in dedup if c in combined.columns]
    if valid: combined = combined.drop_duplicates(subset=valid)
    return combined.reset_index(drop=True), summary


# ══════════════════════════════════════════════════════════════
# MISC HELPERS
# ══════════════════════════════════════════════════════════════
def _save_snapshot(fcast_df, period_label):
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    snap = {"ts":datetime.now().isoformat(),"period":period_label,
            "rows":fcast_df[["product","total_balance","omc_sales",
                              "daily_rate","days_remaining"]].to_dict("records")}
    fname = f"snap_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(os.path.join(SNAPSHOT_DIR,fname),"w") as f: json.dump(snap,f)


def _biz_days(ss, es):
    fmt = "%m/%d/%Y"
    return max(len(pd.bdate_range(datetime.strptime(ss,fmt).date(),
                                  datetime.strptime(es,fmt).date())), 1)


# ── Vessel helpers ────────────────────────────────────────────
def _load_vessel_sheet(url):
    from io import StringIO, BytesIO as _BIO
    mid = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
    fid = mid.group(1) if mid else (url if re.match(r"^[a-zA-Z0-9-_]{20,}$",url) else None)
    if not fid: return None, "Could not extract Google Sheets file ID."
    mgid = re.search(r"(?:#|\?|&)gid=(\d+)", url)
    gid  = mgid.group(1) if mgid else None
    candidates = []
    if gid: candidates.append((f"https://docs.google.com/spreadsheets/d/{fid}/export?format=csv&gid={gid}","csv"))
    candidates += [
        (f"https://docs.google.com/spreadsheets/d/{fid}/export?format=csv&gid=0","csv"),
        (f"https://docs.google.com/spreadsheets/d/{fid}/export?format=csv","csv"),
        (f"https://docs.google.com/spreadsheets/d/{fid}/gviz/tq?tqx=out:csv","gviz"),
        (f"https://docs.google.com/spreadsheets/d/{fid}/export?format=xlsx","xlsx"),
    ]
    for url2, mode in candidates:
        try:
            r = _requests.get(url2, headers={"User-Agent":"Mozilla/5.0"}, timeout=30)
            if r.status_code != 200 or not r.content: continue
            if mode == "xlsx": return pd.read_excel(_BIO(r.content)), None
            df = pd.read_csv(StringIO(r.content.decode("utf-8",errors="replace")),
                             header=14,skiprows=1,skipfooter=1,engine="python")
            return df, None
        except Exception: continue
    return None, "All fetch strategies failed — check sheet is shared publicly."


def _parse_vessel_date(s, yr="2025"):
    s = str(s).strip().upper()
    if "PENDING" in s or s in ("NAN",""):
        return VESSEL_MM.get(datetime.now().strftime("%b"),datetime.now().strftime("%b").upper()), yr, "PENDING"
    try:
        if "-" in s:
            p = s.split("-")
            if len(p)==2: return VESSEL_MM.get(p[1].title(),p[1].upper()), yr, "DISCHARGED"
    except Exception: pass
    return "Unknown", yr, "DISCHARGED"


def _process_vessel_df(vdf, year="2025"):
    vdf = vdf.copy(); vdf.columns = vdf.columns.str.strip()
    ci  = {}
    for i, col in enumerate(vdf.columns):
        cl = str(col).lower().strip()
        if "receiver" in cl or (i==0 and "unnamed" not in cl): ci["r"]=i
        elif "type" in cl and "receiver" not in cl:            ci["t"]=i
        elif "vessel" in cl and "name" in cl:                  ci["v"]=i
        elif "supplier" in cl:                                 ci["s"]=i
        elif "product" in cl:                                  ci["p"]=i
        elif "quantity" in cl or ("mt" in cl and "quantity" not in cl): ci["q"]=i
        elif "date" in cl or "discharg" in cl:                 ci["d"]=i
    records, seen = [], set()
    for _, row in vdf.dropna(how="all").iterrows():
        try:
            receivers = str(row.iloc[ci.get("r",0)]).strip()
            vtype     = str(row.iloc[ci.get("t",1)]).strip()
            vname     = str(row.iloc[ci.get("v",2)]).strip()
            supplier  = str(row.iloc[ci.get("s",3)]).strip()
            prod_raw  = str(row.iloc[ci.get("p",4)]).strip().upper()
            qty_str   = str(row.iloc[ci.get("q",5)]).replace(",","").strip()
            date_cell = str(row.iloc[ci.get("d",6)]).strip()
            if receivers.upper() in {"RECEIVER(S)","RECEIVERS","NAN",""}: continue
            if prod_raw in {"PRODUCT","NAN",""}: continue
            try:   qty_mt = float(qty_str)
            except: continue
            if qty_mt <= 0: continue
            product = VESSEL_PM.get(prod_raw, prod_raw)
            if product not in VESSEL_CF: continue
            key = (vname, product, qty_mt, date_cell)
            if key in seen: continue
            seen.add(key)
            month, yr_, status = _parse_vessel_date(date_cell, yr=year)
            records.append({"Receivers":receivers,"Vessel_Type":vtype,"Vessel_Name":vname,
                            "Supplier":supplier,"Product":product,"Original_Product":prod_raw,
                            "Quantity_MT":qty_mt,"Quantity_Litres":qty_mt*VESSEL_CF[product],
                            "Date_Discharged":date_cell,"Month":month,"Year":yr_,"Status":status})
        except Exception: continue
    return pd.DataFrame(records)


# ══════════════════════════════════════════════════════════════
# PAGE: BDC BALANCE
# ══════════════════════════════════════════════════════════════
def show_bdc_balance():
    st.markdown("<h2>🏦 BDC STOCK BALANCE ANALYZER</h2>", unsafe_allow_html=True)
    _info("Shows the current live stock balance for every BDC, broken down by depot and "
          "product (PREMIUM / GASOIL / LPG), giving a unified national stock picture.")

    all_bdcs = sorted(BDC_USER_MAP.keys())
    c1,c2    = st.columns([3,1])
    with c1: selected = st.multiselect(f"Select BDCs (blank = all {len(all_bdcs)})", all_bdcs, key="bal_sel")
    with c2:
        st.markdown("<br>", unsafe_allow_html=True)
        fetch_all = st.checkbox("Fetch ALL", value=True, key="bal_all")

    to_fetch = all_bdcs if (fetch_all or not selected) else selected
    st.info(f"📋 **{len(to_fetch)} BDC(s)** will be queried")

    if st.button("🔄 FETCH BDC BALANCE DATA", key="bal_fetch"):
        prog, lb, ll = st.progress(0,"Initialising…"), st.empty(), []
        results      = _batch_fetch(to_fetch, _balance_fetcher(), prog, lb, ll)
        prog.progress(1.0, text="✅ Complete")
        recs, summary = _combine_balance(results)
        st.session_state.bdc_records    = recs
        st.session_state.bdc_summary    = summary
        st.session_state.bdc_fetched_n  = len(to_fetch)
        st.markdown("---")
        _fetch_summary(summary, len(to_fetch), len(recs), "Balance Records")

    records = st.session_state.get("bdc_records", [])
    if not records:
        st.info("👆 Click **FETCH BDC BALANCE DATA** to load the current stock position.")
        return

    if st.session_state.get("bdc_summary"):
        with st.expander("📊 Last fetch outcome", expanded=False):
            _fetch_summary(st.session_state.bdc_summary,
                           st.session_state.get("bdc_fetched_n",len(BDC_USER_MAP)),
                           len(records), "Balance Records")

    df       = pd.DataFrame(records)
    prod_sum = df.groupby("Product")[_COL_BAL].sum()
    grand    = float(df[_COL_BAL].sum())

    st.markdown("---")
    st.markdown("### 🛢️ NATIONAL STOCK TOTALS")
    cols = st.columns(4)
    for i, prod in enumerate(["PREMIUM","GASOIL","LPG"]):
        with cols[i]:
            val = prod_sum.get(prod, 0)
            st.markdown(f"<div class='metric-card'><h2>{prod}</h2><h1>{val:,.0f}</h1>"
                        f"<p style='color:#888;font-size:12px;margin:0;'>Litres / KG</p></div>",
                        unsafe_allow_html=True)
    with cols[3]:
        st.markdown(f"<div class='metric-card'><h2>GRAND TOTAL</h2><h1>{grand:,.0f}</h1>"
                    f"<p style='color:#888;font-size:12px;margin:0;'>All products</p></div>",
                    unsafe_allow_html=True)

    st.markdown("---")
    ch1, ch2 = st.columns(2)
    with ch1:
        st.markdown("#### 🥧 Product Split")
        ps = prod_sum[prod_sum>0]
        _pie(ps.index.tolist(), ps.values.tolist(), "Stock by Product")
    with ch2:
        st.markdown("#### 🏦 Top 10 BDCs")
        top10 = (df.groupby("BDC")[_COL_BAL].sum()
                 .sort_values(ascending=False).head(10).reset_index())
        _bar(top10, "BDC", _COL_BAL, "#00ffff", "Top 10 BDCs by Stock")

    st.markdown("---")
    st.markdown("### 🏢 BDC BREAKDOWN")
    bdc_sum = (df.groupby("BDC")
               .agg({_COL_BAL:"sum","DEPOT":"nunique","Product":"nunique"})
               .reset_index()
               .rename(columns={_COL_BAL:"Total Balance (LT/KG)","DEPOT":"Depots","Product":"Products"})
               .sort_values("Total Balance (LT/KG)", ascending=False))
    bdc_sum["Market Share %"] = (bdc_sum["Total Balance (LT/KG)"] / grand * 100).round(2)
    st.dataframe(bdc_sum, use_container_width=True, hide_index=True)

    st.markdown("### 📊 PRODUCT × BDC PIVOT")
    pivot = _standard_pivot(df, "BDC", _COL_BAL)
    st.dataframe(pivot, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("### 🔍 FILTER & EXPLORE")
    ft   = st.selectbox("Filter by", ["Product","BDC","Depot"], key="bal_ft")
    cmap = {"Product":"Product","BDC":"BDC","Depot":"DEPOT"}
    opts = ["ALL"] + sorted(df[cmap[ft]].unique().tolist())
    fval = st.selectbox("Value", opts, key="bal_fval")
    filt = df if fval=="ALL" else df[df[cmap[ft]]==fval]
    st.caption(f"**{len(filt):,}** records · **{filt['BDC'].nunique()}** BDCs · "
               f"**{filt['DEPOT'].nunique()}** depots · "
               f"Total: **{filt[_COL_BAL].sum():,.0f} LT/KG**")
    st.dataframe(filt[["Product","BDC","DEPOT","AVAILABLE BALANCE (LT\\KG)",_COL_BAL,"Date"]]
                 .sort_values(["Product","BDC"]),
                 use_container_width=True, height=400, hide_index=True)

    st.markdown("---")
    _dl({"All":df,"LPG":df[df["Product"]=="LPG"],"PREMIUM":df[df["Product"]=="PREMIUM"],
         "GASOIL":df[df["Product"]=="GASOIL"],"BDC Summary":pivot}, "bdc_balance.xlsx")


# ══════════════════════════════════════════════════════════════
# PAGE: OMC LOADINGS
# ══════════════════════════════════════════════════════════════
def show_omc_loadings():
    st.markdown("<h2>🚚 OMC LOADINGS ANALYZER</h2>", unsafe_allow_html=True)
    _info("Fetches released OMC loading orders for every BDC within the selected date range, "
          "combined into a single de-duplicated dataset for market share and dispatch analysis.")

    all_bdcs = sorted(BDC_USER_MAP.keys())
    c1,c2    = st.columns(2)
    with c1: start_date = st.date_input("Start Date", value=datetime.now()-timedelta(days=7), key="omc_s")
    with c2: end_date   = st.date_input("End Date",   value=datetime.now(), key="omc_e")
    c3,c4 = st.columns([3,1])
    with c3: selected = st.multiselect(f"Select BDCs (blank = all {len(all_bdcs)})", all_bdcs, key="omc_sel")
    with c4:
        st.markdown("<br>", unsafe_allow_html=True)
        fetch_all = st.checkbox("Fetch ALL", value=True, key="omc_all")

    to_fetch = all_bdcs if (fetch_all or not selected) else selected
    st.info(f"📋 **{len(to_fetch)} BDC(s)** · "
            f"**{start_date.strftime('%d %b %Y')} → {end_date.strftime('%d %b %Y')}**")

    if st.button("🔄 FETCH OMC LOADINGS", key="omc_fetch"):
        ss, es       = start_date.strftime("%m/%d/%Y"), end_date.strftime("%m/%d/%Y")
        prog, lb, ll = st.progress(0,"Initialising…"), st.empty(), []
        results      = _batch_fetch(to_fetch, _omc_fetcher(ss,es), prog, lb, ll)
        prog.progress(1.0, text="✅ Complete")
        combined, summary = _combine_df(results, ["Order Number","Truck","Date","Product"])
        st.session_state.omc_df       = combined
        st.session_state.omc_summary  = summary
        st.session_state.omc_n        = len(to_fetch)
        st.session_state.omc_start    = start_date
        st.session_state.omc_end      = end_date
        st.markdown("---")
        _fetch_summary(summary, len(to_fetch),
                       len(combined) if not combined.empty else 0, "Loading Records")

    df = st.session_state.get("omc_df", pd.DataFrame())
    if df.empty:
        st.info("👆 Select a date range and click **FETCH OMC LOADINGS**."); return

    if st.session_state.get("omc_summary"):
        with st.expander("📊 Last fetch outcome", expanded=False):
            _fetch_summary(st.session_state.omc_summary,
                           st.session_state.get("omc_n",len(BDC_USER_MAP)),
                           len(df), "Loading Records")

    st.markdown("---")
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Total Orders",    f"{len(df):,}")
    c2.metric("Total Volume LT", f"{df['Quantity'].sum():,.0f}")
    c3.metric("Unique OMCs",     f"{df['OMC'].nunique()}")
    c4.metric("Total Value ₵",   f"{(df['Quantity']*df['Price']).sum():,.0f}")

    ch1, ch2 = st.columns(2)
    with ch1:
        ps = df.groupby("Product")["Quantity"].sum(); ps = ps[ps>0]
        _pie(ps.index.tolist(), ps.values.tolist(), "Loadings by Product")
    with ch2:
        bs = df.groupby("BDC")["Quantity"].sum().sort_values(ascending=False).head(10).reset_index()
        _bar(bs, "BDC", "Quantity", "#ff00ff", "Top 10 BDCs by Dispatch")

    _trend(df, title="📈 Daily Dispatch Trend")

    st.markdown("### 📦 PRODUCT BREAKDOWN")
    ps2 = (df.groupby("Product").agg({"Quantity":"sum","Order Number":"count","OMC":"nunique"})
           .reset_index()
           .rename(columns={"Quantity":"Total Volume (LT/KG)","Order Number":"Orders","OMC":"OMCs"})
           .sort_values("Total Volume (LT/KG)", ascending=False))
    st.dataframe(ps2, use_container_width=True, hide_index=True)

    st.markdown("### 🏢 TOP OMCs BY VOLUME")
    omc_s = (df.groupby("OMC").agg({"Quantity":"sum","Order Number":"count"})
             .reset_index().sort_values("Quantity",ascending=False).head(20)
             .rename(columns={"Quantity":"Total Volume (LT/KG)","Order Number":"Orders"}))
    st.dataframe(omc_s, use_container_width=True, hide_index=True)

    st.markdown("### 🏦 BDC PERFORMANCE")
    bdc_s = (df.groupby("BDC").agg({"Quantity":"sum","Order Number":"count","OMC":"nunique"})
             .reset_index().sort_values("Quantity",ascending=False)
             .rename(columns={"Quantity":"Total Volume (LT/KG)","Order Number":"Orders","OMC":"OMCs"}))
    st.dataframe(bdc_s, use_container_width=True, hide_index=True)

    st.markdown("---")
    ft   = st.selectbox("Filter by", ["Product","OMC","BDC","Depot"], key="omc_ft")
    opts = ["ALL"] + sorted(df[ft].unique().tolist())
    fval = st.selectbox("Value", opts, key="omc_fval")
    filt = df if fval=="ALL" else df[df[ft]==fval]
    st.caption(f"**{len(filt):,}** records · Volume: **{filt['Quantity'].sum():,.0f} LT**")
    st.dataframe(filt[["Date","OMC","Truck","Quantity","Order Number","BDC","Depot","Price","Product"]]
                 .sort_values(["Product","Date"]),
                 use_container_width=True, height=400, hide_index=True)

    pivot = _standard_pivot(df, "BDC", "Quantity")
    st.markdown("---")
    _dl({"All Orders":df,"BDC Summary":pivot,
         **{p:df[df["Product"]==p] for p in ["PREMIUM","GASOIL","LPG"] if p in df["Product"].values}},
        "omc_loadings.xlsx")


# ══════════════════════════════════════════════════════════════
# PAGE: DAILY ORDERS
# ══════════════════════════════════════════════════════════════
def show_daily_orders():
    st.markdown("<h2>📅 DAILY ORDERS ANALYZER</h2>", unsafe_allow_html=True)
    _info("Fetches the daily dispatch order report grouped by depot, giving truck-level "
          "granularity of physical fuel movements out of each storage facility.")

    all_bdcs = sorted(BDC_USER_MAP.keys())
    c1,c2    = st.columns(2)
    with c1: start_date = st.date_input("Start Date", value=datetime.now()-timedelta(days=1), key="dly_s")
    with c2: end_date   = st.date_input("End Date",   value=datetime.now(), key="dly_e")
    c3,c4 = st.columns([3,1])
    with c3: selected = st.multiselect(f"Select BDCs (blank = all {len(all_bdcs)})", all_bdcs, key="dly_sel")
    with c4:
        st.markdown("<br>", unsafe_allow_html=True)
        fetch_all = st.checkbox("Fetch ALL", value=True, key="dly_all")

    to_fetch = all_bdcs if (fetch_all or not selected) else selected
    st.info(f"📋 **{len(to_fetch)} BDC(s)** · "
            f"**{start_date.strftime('%d %b %Y')} → {end_date.strftime('%d %b %Y')}**")

    if st.button("🔄 FETCH DAILY ORDERS", key="dly_fetch"):
        ss, es       = start_date.strftime("%m/%d/%Y"), end_date.strftime("%m/%d/%Y")
        prog, lb, ll = st.progress(0,"Initialising…"), st.empty(), []
        results      = _batch_fetch(to_fetch, _daily_fetcher(ss,es), prog, lb, ll)
        prog.progress(1.0, text="✅ Complete")
        combined, summary = _combine_df(results, ["Date","Truck","Order Number","Product"])
        st.session_state.daily_df      = combined
        st.session_state.daily_summary = summary
        st.session_state.daily_n       = len(to_fetch)
        st.markdown("---")
        _fetch_summary(summary, len(to_fetch),
                       len(combined) if not combined.empty else 0, "Order Records")

    df = st.session_state.get("daily_df", pd.DataFrame())
    if df.empty:
        st.info("👆 Select a date range and click **FETCH DAILY ORDERS**."); return

    if st.session_state.get("daily_summary"):
        with st.expander("📊 Last fetch outcome", expanded=False):
            _fetch_summary(st.session_state.daily_summary,
                           st.session_state.get("daily_n",len(BDC_USER_MAP)),
                           len(df), "Order Records")

    st.markdown("---")
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Orders",    f"{len(df):,}")
    c2.metric("Volume LT", f"{df['Quantity'].sum():,.0f}")
    c3.metric("BDCs",      f"{df['BDC'].nunique()}")
    c4.metric("Depots",    f"{df['Depot'].nunique()}")
    c5.metric("Value ₵",   f"{(df['Quantity']*df['Price']).sum():,.0f}")

    ch1, ch2 = st.columns(2)
    with ch1:
        ps = df.groupby("Product")["Quantity"].sum(); ps = ps[ps>0]
        _pie(ps.index.tolist(), ps.values.tolist(), "Daily Orders by Product")
    with ch2:
        bs = df.groupby("BDC")["Quantity"].sum().sort_values(ascending=False).head(10).reset_index()
        _bar(bs, "BDC", "Quantity", "#00ff88", "Top 10 BDCs by Daily Volume")

    st.markdown("### 📦 PRODUCT SUMMARY")
    ps2 = (df.groupby("Product").agg({"Quantity":"sum","Order Number":"count","BDC":"nunique"})
           .reset_index()
           .rename(columns={"Quantity":"Total Volume (LT/KG)","Order Number":"Orders","BDC":"BDCs"})
           .sort_values("Total Volume (LT/KG)", ascending=False))
    st.dataframe(ps2, use_container_width=True, hide_index=True)

    st.markdown("### 🏦 BDC SUMMARY")
    bdc_s = (df.groupby("BDC").agg({"Quantity":"sum","Order Number":"count"})
             .reset_index().sort_values("Quantity",ascending=False)
             .rename(columns={"Quantity":"Total Volume (LT/KG)","Order Number":"Orders"}))
    st.dataframe(bdc_s, use_container_width=True, hide_index=True)

    st.markdown("### 📊 BDC × PRODUCT PIVOT")
    pivot = _standard_pivot(df, "BDC", "Quantity")
    st.dataframe(pivot, use_container_width=True, hide_index=True)

    st.markdown("---")
    ft   = st.selectbox("Filter by", ["Product","BDC","Depot","Status"], key="dly_ft")
    opts = ["ALL"] + sorted(df[ft].dropna().unique().tolist())
    fval = st.selectbox("Value", opts, key="dly_fval")
    filt = df if fval=="ALL" else df[df[ft]==fval]
    st.caption(f"**{len(filt):,}** records · Volume: **{filt['Quantity'].sum():,.0f} LT**")
    st.dataframe(filt[["Date","Truck","Quantity","Order Number","BDC","Depot","Price","Product","Status"]]
                 .sort_values(["Product","Date"]),
                 use_container_width=True, height=400, hide_index=True)

    st.markdown("---")
    _dl({"All Orders":df,"BDC Pivot":pivot}, "daily_orders.xlsx")


# ══════════════════════════════════════════════════════════════
# PAGE: MARKET SHARE
# ══════════════════════════════════════════════════════════════
def show_market_share():
    st.markdown("<h2>📊 BDC MARKET SHARE ANALYSIS</h2>", unsafe_allow_html=True)
    _info(
        "Shows a selected BDC's share of national stock and dispatch volumes per product, "
        "including its ranking against all other BDCs.",
        prereq="Fetch BDC Balance and/or OMC Loadings data first.",
    )

    has_bal = bool(st.session_state.get("bdc_records"))
    has_omc = not st.session_state.get("omc_df", pd.DataFrame()).empty

    c1,c2 = st.columns(2)
    with c1:
        (st.success(f"✅ BDC Balance: {len(st.session_state.get('bdc_records',[]))} records")
         if has_bal else st.warning("⚠️ Fetch BDC Balance first"))
    with c2:
        (st.success(f"✅ OMC Loadings: {len(st.session_state.get('omc_df',pd.DataFrame()))} records")
         if has_omc else st.warning("⚠️ Fetch OMC Loadings first"))

    if not has_bal and not has_omc:
        st.error("No data — fetch from BDC Balance and/or OMC Loadings pages first.")
        return

    bal_df = pd.DataFrame(st.session_state.bdc_records) if has_bal else pd.DataFrame()
    omc_df = st.session_state.omc_df                    if has_omc else pd.DataFrame()

    all_bdcs = sorted(
        set(bal_df["BDC"].unique() if not bal_df.empty else []) |
        set(omc_df["BDC"].unique() if not omc_df.empty else [])
    )
    sel = st.selectbox("Choose BDC to analyse:", all_bdcs, key="ms_bdc")
    if not sel: return

    st.markdown(f"## 📊 MARKET REPORT — {sel}")
    st.markdown("---")
    tab1, tab2 = st.tabs(["📦 Stock Balance Share","🚚 Sales Volume Share"])

    with tab1:
        if not has_bal:
            st.warning("Fetch BDC Balance first.")
        else:
            bdc_bal   = bal_df[bal_df["BDC"]==sel]
            mkt_total = float(bal_df[_COL_BAL].sum())
            bdc_total = float(bdc_bal[_COL_BAL].sum())
            share     = bdc_total/mkt_total*100 if mkt_total else 0
            ranked    = list(bal_df.groupby("BDC")[_COL_BAL].sum()
                             .sort_values(ascending=False).index)
            rank      = ranked.index(sel)+1 if sel in ranked else "N/A"

            c1,c2,c3 = st.columns(3)
            c1.metric("Total Stock LT", f"{bdc_total:,.0f}")
            c2.metric("Market Share",   f"{share:.2f}%")
            c3.metric("National Rank",  f"#{rank} of {len(ranked)}")

            rows = []
            for prod in ["PREMIUM","GASOIL","LPG"]:
                mkt = float(bal_df[bal_df["Product"]==prod][_COL_BAL].sum())
                bv  = float(bdc_bal[bdc_bal["Product"]==prod][_COL_BAL].sum())
                rows.append({"Product":prod,"BDC Stock LT":f"{bv:,.0f}",
                              "Market Total LT":f"{mkt:,.0f}",
                              "Share %":f"{bv/mkt*100:.2f}" if mkt else "0.00"})
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            _stack_share(bdc_bal, bal_df, "Product", _COL_BAL, sel,
                         f"{sel} — Stock Share vs National Market")

    with tab2:
        if not has_omc:
            st.warning("Fetch OMC Loadings first.")
        else:
            bdc_ld    = omc_df[omc_df["BDC"]==sel]
            mkt_vol   = float(omc_df["Quantity"].sum())
            bdc_vol   = float(bdc_ld["Quantity"].sum())
            share     = bdc_vol/mkt_vol*100 if mkt_vol else 0
            all_sales = omc_df.groupby("BDC")["Quantity"].sum().sort_values(ascending=False)
            s_rank    = list(all_sales.index).index(sel)+1 if sel in all_sales.index else "N/A"
            revenue   = (bdc_ld["Quantity"]*bdc_ld["Price"]).sum()

            c1,c2,c3,c4 = st.columns(4)
            c1.metric("Dispatched LT", f"{bdc_vol:,.0f}")
            c2.metric("Market Share",  f"{share:.2f}%")
            c3.metric("Sales Rank",    f"#{s_rank} of {len(all_sales)}")
            c4.metric("Revenue ₵",     f"{revenue:,.0f}")

            rows = []
            for prod in ["PREMIUM","GASOIL","LPG"]:
                mkt = float(omc_df[omc_df["Product"]==prod]["Quantity"].sum())
                bv  = float(bdc_ld[bdc_ld["Product"]==prod]["Quantity"].sum())
                pr  = omc_df[omc_df["Product"]==prod].groupby("BDC")["Quantity"]\
                             .sum().sort_values(ascending=False)
                pr_n = list(pr.index).index(sel)+1 if sel in pr.index else "N/A"
                rows.append({"Product":prod,"Dispatched LT":f"{bv:,.0f}",
                              "Market Total LT":f"{mkt:,.0f}",
                              "Share %":f"{bv/mkt*100:.2f}" if mkt else "0.00",
                              "Rank":f"#{pr_n}/{len(pr)}"})
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            _stack_share(bdc_ld, omc_df, "Product", "Quantity", sel,
                         f"{sel} — Dispatch Share vs National Market")

            if not bdc_ld.empty:
                st.markdown("#### 🏢 Top OMC Customers")
                top_omc = (bdc_ld.groupby("OMC")["Quantity"].sum()
                           .sort_values(ascending=False).head(10).reset_index()
                           .rename(columns={"Quantity":"Volume LT"}))
                ct, cb = st.columns([3,2])
                with ct: st.dataframe(top_omc, use_container_width=True, hide_index=True)
                with cb:
                    _pie(top_omc["OMC"].tolist(), top_omc["Volume LT"].tolist(),
                         "Top OMC Share",
                         [f"hsl({i*36},80%,55%)" for i in range(len(top_omc))])


# ══════════════════════════════════════════════════════════════
# PAGE: STOCK TRANSACTION
# ══════════════════════════════════════════════════════════════
def show_stock_transaction():
    st.markdown("<h2>📈 STOCK TRANSACTION ANALYZER</h2>", unsafe_allow_html=True)
    _info(
        "Retrieves the full stock transaction ledger for a specific BDC, depot and product — "
        "showing every inflow and outflow with a running balance for reconciliation.",
        prereq="Uses BDC entity IDs (lngBDCId), not per-user credentials.",
    )

    if "stock_txn_df" not in st.session_state:
        st.session_state.stock_txn_df = pd.DataFrame()

    c1,c2 = st.columns(2)
    with c1:
        sel_bdc  = st.selectbox("BDC",     sorted(BDC_MAP.keys()),   key="txn_bdc")
        sel_prod = st.selectbox("Product",  list(PRODUCT_MAP.keys()), key="txn_prod")
    with c2:
        sel_dep  = st.selectbox("Depot",    sorted(DEPOT_MAP.keys()), key="txn_dep")
    c3,c4 = st.columns(2)
    with c3: start_date = st.date_input("Start Date", value=datetime.now()-timedelta(days=30), key="txn_s")
    with c4: end_date   = st.date_input("End Date",   value=datetime.now(), key="txn_e")

    if st.button("📊 FETCH TRANSACTION REPORT", key="txn_fetch"):
        params = {"lngProductId":PRODUCT_MAP[sel_prod],"lngBDCId":BDC_MAP[sel_bdc],
                  "lngDepotId":DEPOT_MAP[sel_dep],
                  "dtpStartDate":start_date.strftime("%m/%d/%Y"),
                  "dtpEndDate":end_date.strftime("%m/%d/%Y"),
                  "lngUserId":NPA["USER_ID"]}
        with st.spinner(f"Fetching {sel_prod} transactions for {sel_bdc}…"):
            pdf = _fetch_pdf(NPA["TXN_URL"], params)
        if not pdf:
            st.error("❌ No PDF returned — check BDC/depot/product combination.")
            st.session_state.stock_txn_df = pd.DataFrame()
        else:
            records = _parse_txn_pdf(pdf)
            if records:
                st.session_state.stock_txn_df = pd.DataFrame(records)
                st.session_state.txn_label    = f"{sel_bdc} · {sel_prod} @ {sel_dep}"
                st.success(f"✅ {len(records):,} records extracted.")
            else:
                st.warning("No transactions found — try a different date range or combination.")
                st.session_state.stock_txn_df = pd.DataFrame()

    df = st.session_state.stock_txn_df
    if df.empty:
        st.info("👆 Configure parameters and click **FETCH TRANSACTION REPORT**."); return

    st.markdown(f"### {st.session_state.get('txn_label','')}")
    inflows  = float(df[df["Description"].isin(["Custody Transfer In","Product Outturn"])]["Volume"].sum())
    outflows = float(df[df["Description"].isin(["Sale","Custody Transfer Out"])]["Volume"].sum())
    sales    = float(df[df["Description"]=="Sale"]["Volume"].sum())
    xfer_out = float(df[df["Description"]=="Custody Transfer Out"]["Volume"].sum())
    final_b  = float(df["Balance"].iloc[-1]) if len(df) else 0

    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("📥 Inflows",      f"{inflows:,.0f} LT")
    c2.metric("📤 Outflows",     f"{outflows:,.0f} LT")
    c3.metric("💰 OMC Sales",    f"{sales:,.0f} LT")
    c4.metric("🔄 Transfers Out",f"{xfer_out:,.0f} LT")
    c5.metric("📊 Closing Bal",  f"{final_b:,.0f} LT")

    # Running balance chart
    try:
        dc = df.copy()
        dc["Date_dt"] = pd.to_datetime(dc["Date"], dayfirst=True, errors="coerce")
        dc = dc.dropna(subset=["Date_dt"]).sort_values("Date_dt")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=dc["Date_dt"], y=dc["Balance"],
                                 mode="lines+markers", name="Running Balance",
                                 line=dict(color="#00ffff",width=2)))
        fig.update_layout(title=dict(text="Running Stock Balance",
                                     font=dict(color="#00ffff",family="Orbitron")),
                          xaxis_title="Date", yaxis_title="Litres")
        _plotly(fig)
    except Exception: pass

    st.markdown("### 📋 Transaction Breakdown")
    txn_s = (df.groupby("Description")
             .agg(Total_Volume=("Volume","sum"),Count=("Trans #","count"))
             .reset_index().sort_values("Total_Volume",ascending=False)
             .rename(columns={"Description":"Type","Total_Volume":"Total Volume LT"}))
    st.dataframe(txn_s, use_container_width=True, hide_index=True)

    if sales > 0:
        st.markdown("### 🏢 Top OMC Customers")
        cust = (df[df["Description"]=="Sale"].groupby("Account")["Volume"]
                .sum().sort_values(ascending=False).head(10).reset_index()
                .rename(columns={"Account":"Customer","Volume":"Volume LT"}))
        ct, cb = st.columns([3,2])
        with ct: st.dataframe(cust, use_container_width=True, hide_index=True)
        with cb:
            _pie(cust["Customer"].tolist(), cust["Volume LT"].tolist(), "Top Customers",
                 [f"hsl({i*36},80%,55%)" for i in range(len(cust))])

    st.markdown("### 📄 Full Transaction History")
    st.dataframe(df, use_container_width=True, hide_index=True, height=400)
    _dl({"Transactions":df,"Summary":txn_s}, "stock_transaction.xlsx")


# ══════════════════════════════════════════════════════════════
# PAGE: NATIONAL STOCKOUT
# ══════════════════════════════════════════════════════════════
def show_national_stockout():
    st.markdown("<h2>🌍 NATIONAL STOCKOUT FORECAST</h2>", unsafe_allow_html=True)
    _info("Calculates Ghana's national days-of-supply for PREMIUM, GASOIL and LPG by dividing "
          "current BDC stock balances by the average daily depletion rate derived from OMC "
          "loadings over the selected history window. Both datasets are fetched fresh as "
          "part of this analysis.")

    c1,c2 = st.columns(2)
    with c1: start_date = st.date_input("Loadings From", value=datetime.now()-timedelta(days=30), key="ns_s")
    with c2: end_date   = st.date_input("Loadings To",   value=datetime.now(), key="ns_e")

    ss, es       = start_date.strftime("%m/%d/%Y"), end_date.strftime("%m/%d/%Y")
    cal_days     = max((end_date - start_date).days, 1)
    biz_days_n   = _biz_days(ss, es)

    day_type = st.radio("Day-type for daily rate",["📆 Calendar Days","💼 Business Days (Mon–Fri)"],
                        horizontal=True, key="ns_dt")
    use_biz  = "Business" in day_type

    depl_mode = st.radio("Depletion rate method",
                         ["📊 Average","🔥 Maximum (stress test)","📊 Median"],
                         index=0, horizontal=True, key="ns_dm")
    use_max    = "Maximum" in depl_mode
    use_median = "Median"  in depl_mode

    exclude_tor = st.checkbox("❌ Exclude TOR LPG from national stock", value=False, key="ns_tor")

    _vdf     = st.session_state.get("vessel_data", pd.DataFrame())
    _vloaded = isinstance(_vdf,pd.DataFrame) and not _vdf.empty
    _pend_n  = int((_vdf["Status"]=="PENDING").sum()) if _vloaded else 0

    inc_vessels = st.checkbox("🚢 Add pending vessel cargo to stock", value=False, key="ns_ves")
    if inc_vessels and not _vloaded:
        st.warning("No vessel data — go to 🚢 Vessel Supply and fetch first."); inc_vessels=False
    elif inc_vessels and _pend_n==0:
        st.info("No PENDING vessels in loaded data — toggle has no effect.")

    all_bdcs = sorted(BDC_USER_MAP.keys())
    eff_days = biz_days_n if use_biz else cal_days
    day_lbl  = f"{eff_days} {'business' if use_biz else 'calendar'} days"

    st.info(f"📋 **{len(all_bdcs)} BDCs** · "
            f"**{start_date.strftime('%d %b')} → {end_date.strftime('%d %b %Y')}** "
            f"({cal_days} cal / {biz_days_n} biz days)")

    if st.button("⚡ FETCH & ANALYSE NATIONAL FUEL SUPPLY", key="ns_go"):
        # Step 1: Balance
        with st.status("📡 Step 1/2 — BDC Stock Balances", expanded=True):
            prog1, lb1, ll1 = st.progress(0,"Starting…"), st.empty(), []
            r1 = _batch_fetch(all_bdcs, _balance_fetcher(), prog1, lb1, ll1)
            prog1.progress(1.0, text="✅ Done")
            all_recs, bal_sum = _combine_balance(r1)
            bal_df = pd.DataFrame(all_recs)
            n_bdcs = bal_df["BDC"].nunique() if not bal_df.empty else 0
            st.write(f"✅ **{len(all_recs):,} records** from **{n_bdcs} BDCs**")

            if exclude_tor and not bal_df.empty:
                mask   = bal_df["BDC"].str.contains("TOR",case=False,na=False)&(bal_df["Product"]=="LPG")
                excl_v = bal_df[mask][_COL_BAL].sum()
                bal_df = bal_df[~mask].copy()
                st.write(f"TOR LPG excluded: {excl_v:,.0f} LT removed")

            bal_by_prod = (bal_df.groupby("Product")[_COL_BAL].sum()
                           if not bal_df.empty else pd.Series(dtype=float))

            if inc_vessels and _vloaded:
                pend = _vdf[_vdf["Status"]=="PENDING"]
                if not pend.empty:
                    for prod, vol in pend.groupby("Product")["Quantity_Litres"].sum().items():
                        bal_by_prod[prod] = bal_by_prod.get(prod,0) + vol
                    st.write("🚢 Pending vessels: "+
                             " | ".join(f"{p}: +{v:,.0f} LT"
                                        for p,v in pend.groupby("Product")["Quantity_Litres"].sum().items()))

        # Step 2: OMC Loadings
        with st.status("🚚 Step 2/2 — OMC Loadings", expanded=True):
            prog2, lb2, ll2 = st.progress(0,"Starting…"), st.empty(), []
            r2 = _batch_fetch(all_bdcs, _omc_fetcher(ss,es), prog2, lb2, ll2)
            prog2.progress(1.0, text="✅ Done")
            omc_df, omc_sum = _combine_df(r2, ["Order Number","Truck","Date","Product"])
            st.write(f"✅ **{len(omc_df):,} loading records**")

            if omc_df.empty:
                omc_by_prod = pd.Series({"PREMIUM":0.0,"GASOIL":0.0,"LPG":0.0})
                depl_lbl    = "No Data"
            else:
                filt = omc_df[omc_df["Product"].isin(["PREMIUM","GASOIL","LPG"])].copy()
                filt["Date"] = pd.to_datetime(filt["Date"], errors="coerce")
                daily_agg   = filt.groupby(["Date","Product"])["Quantity"].sum().reset_index()
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
            stock = float(bal_by_prod.get(prod,0))
            dep   = float(omc_by_prod.get(prod,0))
            daily = dep if (use_median or use_max) else (dep/eff_days if eff_days else 0)
            days  = stock/daily if daily > 0 else float("inf")
            rows_out.append({"product":prod,"display_name":DISPLAY[prod],
                             "total_balance":stock,"omc_sales":dep,
                             "daily_rate":daily,"days_remaining":days})

        fcast_df = pd.DataFrame(rows_out)
        bdc_piv  = pd.DataFrame()
        if not bal_df.empty:
            bdc_piv = _standard_pivot(bal_df, "BDC", _COL_BAL)
            nat_tot = bdc_piv["TOTAL"].sum()
            bdc_piv["Market Share %"] = (bdc_piv["TOTAL"]/nat_tot*100).round(2)

        st.session_state.ns_results = {
            "fcast_df":fcast_df,"bal_df":bal_df,"omc_df":omc_df,"bdc_piv":bdc_piv,
            "depl_lbl":depl_lbl,"day_lbl":day_lbl,"ss":ss,"es":es,
            "bal_sum":bal_sum,"omc_sum":omc_sum,
            "n_bal":len(all_recs),"n_omc":len(omc_df),
        }
        _save_snapshot(fcast_df, f"{cal_days}d")
        st.success("✅ Analysis complete — scroll down.")
        st.rerun()

    if not st.session_state.get("ns_results"):
        st.info("👆 Configure options and click **FETCH & ANALYSE**."); return

    res      = st.session_state.ns_results
    fcast_df = res["fcast_df"]
    bdc_piv  = res["bdc_piv"]
    omc_df   = res["omc_df"]
    depl_lbl = res["depl_lbl"]
    day_lbl  = res["day_lbl"]

    st.markdown("---")
    st.markdown(f"<h3>🇬🇭 NATIONAL FUEL SUPPLY — {res['ss']} → {res['es']}</h3>",
                unsafe_allow_html=True)

    bs, os_ = res.get("bal_sum",{}), res.get("omc_sum",{})
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Balance Records",  f"{res.get('n_bal',0):,}")
    c2.metric("Loading Records",  f"{res.get('n_omc',0):,}")
    c3.metric("Balance BDCs ✅",  len(bs.get("success",[])))
    c4.metric("Loadings BDCs ✅", len(os_.get("success",[])))

    st.markdown("### 🛢️ DAYS OF SUPPLY")
    cols = st.columns(3)
    for col,(_, row) in zip(cols, fcast_df.iterrows()):
        days  = row["days_remaining"]
        prod  = row["product"]
        color = PROD_COLORS.get(prod,"#fff")
        dt    = f"{days:.1f}" if days != float("inf") else "∞"
        wk    = f"(~{days/7:.1f} wks)" if days != float("inf") else ""
        if   days < 7:  border, status = "#ff0000","🔴 CRITICAL"
        elif days < 14: border, status = "#ffaa00","🟡 WARNING"
        elif days < 30: border, status = "#ff6600","🟠 MONITOR"
        else:           border, status = "#00ff88","🟢 HEALTHY"
        stockout = ((datetime.now()+timedelta(days=days)).strftime("%d %b %Y")
                    if days != float("inf") else "N/A")
        with col:
            st.markdown(f"""
            <div style='background:rgba(10,14,39,0.85);padding:22px 14px;border-radius:16px;
                        border:2.5px solid {border};text-align:center;
                        box-shadow:0 0 18px {border}55;margin-bottom:8px;'>
              <div style='font-size:34px;'>{PROD_ICONS.get(prod,"🛢")}</div>
              <div style='font-family:Orbitron,sans-serif;color:{color};font-size:17px;
                           font-weight:700;margin:6px 0;'>{row["display_name"]}</div>
              <div style='font-family:Orbitron,sans-serif;font-size:52px;color:{border};
                           font-weight:900;line-height:1;'>{dt}</div>
              <div style='color:#888;font-size:13px;'>{wk} days of supply</div>
              <div style='color:{border};font-size:13px;font-weight:700;margin:6px 0;'>{status}</div>
              <hr style='border-color:rgba(255,255,255,0.1);'>
              <div style='font-size:12px;color:#888;'>📦 {row["total_balance"]:,.0f} LT stock</div>
              <div style='font-size:12px;color:#888;'>📉 {row["daily_rate"]:,.0f} LT/day</div>
              <div style='font-size:12px;color:{border};font-weight:700;'>🗓️ Est. empty: {stockout}</div>
            </div>""", unsafe_allow_html=True)

    # Days-of-supply bar chart with threshold lines
    st.markdown("#### 📊 Days-of-Supply at a Glance")
    finite = fcast_df[fcast_df["days_remaining"] != float("inf")]
    if not finite.empty:
        fig = go.Figure()
        for _, row in finite.iterrows():
            days  = row["days_remaining"]
            color = "#ff0000" if days<7 else "#ffaa00" if days<14 else "#ff6600" if days<30 else "#00ff88"
            fig.add_trace(go.Bar(x=[row["display_name"]], y=[days],
                                 marker_color=color, name=row["display_name"],
                                 text=[f"{days:.1f}d"], textposition="outside"))
        fig.add_hline(y=7,  line=dict(color="#ff0000",dash="dash"), annotation_text="Critical (7d)")
        fig.add_hline(y=14, line=dict(color="#ffaa00",dash="dash"), annotation_text="Warning (14d)")
        fig.update_layout(showlegend=False, yaxis_title="Days of Supply")
        _plotly(fig)

    st.markdown("---")
    st.markdown("### 📊 SUMMARY TABLE")
    sum_rows = []
    for _, row in fcast_df.iterrows():
        days   = row["days_remaining"]
        status = ("🔴 CRITICAL" if days<7 else "🟡 WARNING" if days<14
                  else "🟠 MONITOR" if days<30 else "🟢 HEALTHY")
        sum_rows.append({
            "Product":                        row["display_name"],
            "National Stock (LT/KG)":         f"{row['total_balance']:,.0f}",
            f"{depl_lbl} (LT)":               f"{row['omc_sales']:,.0f}",
            f"Daily Rate ({day_lbl}) (LT/d)": f"{row['daily_rate']:,.0f}",
            "Days of Supply":                  f"{days:.1f}" if days!=float("inf") else "∞",
            "Projected Empty":                 (datetime.now()+timedelta(days=days)).strftime("%Y-%m-%d")
                                               if days!=float("inf") else "N/A",
            "Status": status,
        })
    st.dataframe(pd.DataFrame(sum_rows), use_container_width=True, hide_index=True)

    if isinstance(bdc_piv,pd.DataFrame) and not bdc_piv.empty:
        st.markdown("---")
        st.markdown("### 🏦 STOCK BY BDC")
        disp = bdc_piv.copy()
        for c in ["GASOIL","LPG","PREMIUM","TOTAL"]:
            if c in disp.columns: disp[c] = disp[c].apply(lambda x: f"{x:,.0f}")
        if "Market Share %" in disp.columns:
            disp["Market Share %"] = disp["Market Share %"].apply(lambda x: f"{x:.2f}%")
        st.dataframe(disp, use_container_width=True, hide_index=True)

    st.markdown("---")
    sheets = {"Stockout Forecast":pd.DataFrame(sum_rows),"Stock by BDC":bdc_piv}
    if not omc_df.empty: sheets["OMC Loadings"] = omc_df
    _dl(sheets, "national_stockout.xlsx", "⬇️ DOWNLOAD NATIONAL REPORT")


# ══════════════════════════════════════════════════════════════
# PAGE: WORLD RISK MONITOR
# ══════════════════════════════════════════════════════════════
def show_world_monitor():
    st.markdown("<h2>🌍 WORLD RISK MONITOR</h2>", unsafe_allow_html=True)
    st.markdown("""
    <div class='info-box' style='border-color:#ff000033;background:rgba(255,0,0,0.04);'>
    <b style='color:#ff4444;'>🔴 LIVE GLOBAL INTELLIGENCE</b><br>
    Real-time global threat and supply-chain risk map — conflicts, sanctions, weather,
    shipping lane disruptions, power outages and more. Use for proactive upstream
    procurement decisions.
    </div>""", unsafe_allow_html=True)
    st.markdown("""
    <div style='background:rgba(22,33,62,0.6);padding:40px;border-radius:15px;
                border:2px solid #00ffff;text-align:center;margin:20px 0;'>
        <div style='font-size:80px;'>🌍</div>
        <h3 style='color:#00ffff;margin:0;'>WORLD RISK MONITOR</h3>
        <p style='color:#888;margin:10px 0 20px;'>
            25 live data layers · 7-day rolling window · WebGL satellite base map<br>
            Conflicts · Nuclear · Military · Sanctions · Weather · Waterways · Outages
        </p>
    </div>""", unsafe_allow_html=True)
    st.link_button("🌍 OPEN WORLD RISK MONITOR", WORLD_MONITOR_URL, use_container_width=True)
    st.caption(f"Opens in a new tab · {WORLD_MONITOR_URL.split('?')[0]}")


# ══════════════════════════════════════════════════════════════
# PAGE: VESSEL SUPPLY
# ══════════════════════════════════════════════════════════════
def show_vessel_supply():
    st.markdown("<h2>🚢 VESSEL SUPPLY TRACKER</h2>", unsafe_allow_html=True)
    _info(
        "Loads the national vessel discharge schedule from a Google Sheet — showing discharged "
        "cargo and pending vessels, with quantities converted from MT to litres using standard "
        "NPA conversion factors.",
        prereq="Enable the vessel toggle on the National Stockout page to include pending cargo "
               "in the days-of-supply calculation.",
    )

    c1,c2 = st.columns([3,1])
    with c1: sheet_url = st.text_input("Google Sheets URL or File ID", value=VESSEL_SHEET_URL, key="ves_url")
    with c2: year_sel  = st.selectbox("Year", ["2025","2024","2026"], key="ves_yr")

    if st.button("🔄 FETCH VESSEL DATA", key="ves_fetch"):
        with st.spinner("Loading vessel schedule…"):
            raw_df, err = _load_vessel_sheet(sheet_url)
        if raw_df is None:
            st.error(f"❌ {err}"); return
        proc = _process_vessel_df(raw_df, year=year_sel)
        if proc.empty:
            st.warning("No valid vessel records found."); return
        st.session_state.vessel_data = proc
        st.session_state.vessel_year = year_sel
        st.success(f"✅ {len(proc)} vessel records loaded.")
        st.rerun()

    df = st.session_state.get("vessel_data", pd.DataFrame())
    if isinstance(df,pd.DataFrame) and df.empty:
        st.info("👆 Click **FETCH VESSEL DATA** to load the discharge schedule."); return

    yr_lbl     = st.session_state.get("vessel_year","2025")
    discharged = df[df["Status"]=="DISCHARGED"]
    pending    = df[df["Status"]=="PENDING"]

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Total Vessels", len(df))
    c2.metric("Discharged",    f"{len(discharged)} ({discharged['Quantity_Litres'].sum()/1e6:.2f}M LT)")
    c3.metric("⏳ Pending",    f"{len(pending)} ({pending['Quantity_Litres'].sum()/1e6:.2f}M LT)")
    c4.metric("Grand Total",   f"{df['Quantity_Litres'].sum()/1e6:.2f}M LT")

    st.markdown("---")
    st.markdown("### ⏳ PENDING VESSELS — Supply Pipeline")
    if pending.empty:
        st.success("✅ No pending vessels.")
    else:
        pp    = (pending.groupby("Product")
                 .agg(Vessels=("Vessel_Name","count"),
                      Volume_LT=("Quantity_Litres","sum"),
                      Volume_MT=("Quantity_MT","sum")).reset_index())
        pcols = st.columns(min(len(pp),4))
        for col,(_, row) in zip(pcols, pp.iterrows()):
            prod  = row["Product"]
            color = PROD_COLORS.get(prod,"#fff")
            with col:
                st.markdown(f"""
                <div style='background:rgba(10,14,39,0.85);padding:18px;border-radius:12px;
                            border:2px solid {color};text-align:center;'>
                    <div style='font-size:28px;'>{PROD_ICONS.get(prod,"🛢")}</div>
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
            ch1, ch2 = st.columns(2)
            with ch1:
                monthly = (discharged.groupby(["Month","Product"])["Quantity_Litres"]
                           .sum().reset_index())
                monthly["Month"] = pd.Categorical(monthly["Month"],
                                                  categories=MONTH_ORDER, ordered=True)
                monthly = monthly.sort_values("Month")
                fig = go.Figure()
                for prod in monthly["Product"].unique():
                    sub = monthly[monthly["Product"]==prod]
                    fig.add_trace(go.Bar(name=prod, x=sub["Month"], y=sub["Quantity_Litres"],
                                         marker_color=PROD_COLORS.get(prod,"#fff")))
                fig.update_layout(barmode="group",
                                  title=dict(text=f"Monthly Discharge — {yr_lbl}",
                                             font=dict(color="#00ffff",family="Orbitron")),
                                  xaxis_title="Month", yaxis_title="Volume (LT)")
                _plotly(fig)
            with ch2:
                pt = discharged.groupby("Product")["Quantity_Litres"].sum()
                pt = pt[pt>0]
                _pie(pt.index.tolist(), pt.values.tolist(), f"Product Mix — {yr_lbl}")
        else:
            st.info("No discharged vessels yet.")

    with tab2:
        if not discharged.empty:
            st.dataframe(discharged[["Vessel_Name","Vessel_Type","Receivers","Supplier",
                                     "Product","Quantity_MT","Quantity_Litres",
                                     "Date_Discharged","Month"]],
                         use_container_width=True, hide_index=True)

    st.markdown("---")
    _dl({"All Vessels":df,"Discharged":discharged,"Pending":pending},
        f"vessel_data_{yr_lbl}.xlsx", "⬇️ DOWNLOAD VESSEL EXCEL")


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
PAGES = [
    "🏦 BDC BALANCE","🚚 OMC LOADINGS","📅 DAILY ORDERS","📊 MARKET SHARE",
    "📈 STOCK TRANSACTION","🌍 NATIONAL STOCKOUT","🌐 WORLD RISK MONITOR","🚢 VESSEL SUPPLY",
]
DISPATCH = {
    "🏦 BDC BALANCE":       show_bdc_balance,
    "🚚 OMC LOADINGS":      show_omc_loadings,
    "📅 DAILY ORDERS":      show_daily_orders,
    "📊 MARKET SHARE":      show_market_share,
    "📈 STOCK TRANSACTION": show_stock_transaction,
    "🌍 NATIONAL STOCKOUT": show_national_stockout,
    "🌐 WORLD RISK MONITOR":show_world_monitor,
    "🚢 VESSEL SUPPLY":     show_vessel_supply,
}


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
        choice = st.radio("", PAGES, index=0, label_visibility="collapsed")

        st.markdown("---")
        has_bal = bool(st.session_state.get("bdc_records"))
        has_omc = not st.session_state.get("omc_df",   pd.DataFrame()).empty
        has_dly = not st.session_state.get("daily_df", pd.DataFrame()).empty
        has_txn = not st.session_state.get("stock_txn_df", pd.DataFrame()).empty
        has_ves = not st.session_state.get("vessel_data",  pd.DataFrame()).empty
        badges  = {"Balance":has_bal,"OMC Load":has_omc,
                   "Daily Ord":has_dly,"Stock Txn":has_txn,"Vessels":has_ves}

        st.markdown(
            "<div style='background:rgba(0,255,255,0.05);padding:14px;border-radius:10px;"
            "border:1px solid #00ffff44;font-size:13px;'>"
            "<b style='color:#00ffff;'>📊 DATA STATUS</b><br>" +
            "".join(f"<span style='color:{'#00ff88' if v else '#555'};'>"
                    f"{'✅' if v else '○'} {k}</span><br>"
                    for k,v in badges.items()) +
            f"<br><span style='color:#888;font-size:11px;'>"
            f"{len(BDC_USER_MAP)} BDCs in .env</span></div>",
            unsafe_allow_html=True,
        )

        st.markdown("---")
        st.markdown("""
        <div style='text-align:center;padding:12px;background:rgba(255,0,255,0.08);
                    border-radius:10px;border:2px solid #ff00ff;'>
            <b style='color:#ff00ff;'>⚙️ SYSTEM STATUS</b><br>
            <span style='color:#00ff88;font-size:16px;'>🟢 OPERATIONAL</span>
        </div>""", unsafe_allow_html=True)

    DISPATCH[choice]()


main()