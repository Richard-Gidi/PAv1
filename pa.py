"""
NPA ENERGY ANALYTICS - STREAMLIT DASHBOARD
===========================================
INSTALLATION:
pip install streamlit pandas pdfplumber PyPDF2 openpyxl python-dotenv plotly requests
USAGE:
streamlit run npa_dashboard.py

v2 National Stockout: 3-step architecture
  Step 1: BDC Balance (1 call) → national stock
  Step 2: OMC Loadings (1 call) → all BDC→OMC sales
  Step 3: Stock Transactions (N_BDCs × 3 products, depot_id=0) → Custody Transfer Out only
  Total: ~150 calls vs 6000+ in v1
"""
import streamlit as st
import os
import re
from datetime import datetime, timedelta
import pandas as pd
import pdfplumber
import PyPDF2
from dotenv import load_dotenv
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import concurrent.futures
import io
import time

# Load environment variables
load_dotenv()

# ==================== LOAD ID MAPPINGS FROM ENV ====================
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
            elif name == "BLUE_OCEAN_INVESTMENT_LTD_KOTOKA_AIRPORT_ATK" in key:
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
        "LPG":    int(os.getenv('PRODUCT_LPG_ID',     '28'))
    }

BDC_MAP           = load_bdc_mappings()
DEPOT_MAP         = load_depot_mappings()
STOCK_PRODUCT_MAP = load_product_mappings()
PRODUCT_OPTIONS   = ["PMS", "Gasoil", "LPG"]
PRODUCT_BALANCE_MAP = {"PMS": "PREMIUM", "Gasoil": "GASOIL", "LPG": "LPG"}

NPA_CONFIG = {
    'COMPANY_ID':        os.getenv('NPA_COMPANY_ID',        '1'),
    'USER_ID':           os.getenv('NPA_USER_ID',           '123292'),
    'APP_ID':            os.getenv('NPA_APP_ID',            '3'),
    'ITS_FROM_PERSOL':   os.getenv('NPA_ITS_FROM_PERSOL',   'Persol Systems Limited'),
    'BDC_BALANCE_URL':   os.getenv('NPA_BDC_BALANCE_URL',   'https://iml.npa-enterprise.com/NPAAPILIVE/Home/CreateProductBalance'),
    'OMC_LOADINGS_URL':  os.getenv('NPA_OMC_LOADINGS_URL',  'https://iml.npa-enterprise.com/NewNPA/home/CreateOrdersReport'),
    'DAILY_ORDERS_URL':  os.getenv('NPA_DAILY_ORDERS_URL',  'https://iml.npa-enterprise.com/NewNPA/home/CreateDailyOrderReport'),
    'STOCK_TRANSACTION_URL': os.getenv('NPA_STOCK_TRANSACTION_URL', 'https://iml.npa-enterprise.com/NewNPA/home/CreateStockTransactionReport'),
    'OMC_NAME':          os.getenv('OMC_NAME',              'OILCORP ENERGIA LIMITED')
}

# ==================== HISTORY & CACHE FUNCTIONS ====================
def save_to_history(data_type, df, metadata=None):
    history_dir = os.path.join(os.getcwd(), "data_history")
    os.makedirs(history_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename  = f"{data_type}_{timestamp}.json"
    filepath  = os.path.join(history_dir, filename)
    history_data = {
        'timestamp': timestamp, 'data_type': data_type,
        'metadata': metadata or {},
        'summary': {
            'total_records': len(df),
            'total_volume':  float(df['Quantity'].sum()) if 'Quantity' in df.columns else 0,
            'unique_bdcs':   int(df['BDC'].nunique())    if 'BDC'      in df.columns else 0
        }
    }
    with open(filepath, 'w') as f:
        json.dump(history_data, f, indent=2)
    return filepath

def load_history(data_type, limit=10):
    history_dir = os.path.join(os.getcwd(), "data_history")
    if not os.path.exists(history_dir):
        return []
    files = [f for f in os.listdir(history_dir) if f.startswith(data_type) and f.endswith('.json')]
    files.sort(reverse=True)
    history = []
    for f in files[:limit]:
        try:
            with open(os.path.join(history_dir, f), 'r') as file:
                history.append(json.load(file))
        except:
            continue
    return history

# ==================== CHART GENERATION FUNCTIONS ====================
def create_product_pie_chart(df, title="Product Distribution"):
    if 'Quantity' in df.columns:
        value_col = 'Quantity'
    elif 'ACTUAL BALANCE (LT\\KG)' in df.columns:
        value_col = 'ACTUAL BALANCE (LT\\KG)'
    else:
        fig = go.Figure()
        fig.update_layout(title=dict(text="No data available", font=dict(size=20, color='#00ffff', family='Orbitron')),
                          paper_bgcolor='rgba(10, 14, 39, 0.8)', height=400)
        return fig
    product_summary = df.groupby('Product')[value_col].sum().reset_index()
    fig = go.Figure(data=[go.Pie(
        labels=product_summary['Product'], values=product_summary[value_col],
        hole=0.4, marker=dict(colors=['#00ffff', '#ff00ff', '#00ff88', '#ffaa00']),
        textinfo='label+percent', textfont=dict(size=14, color='white', family='Orbitron')
    )])
    fig.update_layout(
        title=dict(text=title, font=dict(size=20, color='#00ffff', family='Orbitron')),
        paper_bgcolor='rgba(10, 14, 39, 0.8)', plot_bgcolor='rgba(10, 14, 39, 0.8)',
        showlegend=True, legend=dict(font=dict(color='white')), height=400
    )
    return fig

def create_bdc_bar_chart(df, title="BDC Performance"):
    if 'Quantity' in df.columns and 'BDC' in df.columns:
        bdc_summary = df.copy()
    else:
        if 'Quantity' in df.columns:
            value_col = 'Quantity'
        elif 'ACTUAL BALANCE (LT\\KG)' in df.columns:
            value_col = 'ACTUAL BALANCE (LT\\KG)'
        else:
            fig = go.Figure()
            fig.update_layout(title=dict(text="No data available", font=dict(size=20, color='#00ffff', family='Orbitron')),
                              paper_bgcolor='rgba(10, 14, 39, 0.8)', height=500)
            return fig
        bdc_summary = df.groupby('BDC')[value_col].sum().sort_values(ascending=False).head(10).reset_index()
        bdc_summary.columns = ['BDC', 'Quantity']
    fig = go.Figure(data=[go.Bar(
        x=bdc_summary['BDC'], y=bdc_summary['Quantity'],
        marker=dict(color=bdc_summary['Quantity'], colorscale='Viridis', line=dict(color='#00ffff', width=2)),
        text=bdc_summary['Quantity'].apply(lambda x: f'{x:,.0f}'), textposition='outside',
        textfont=dict(size=12, color='white')
    )])
    fig.update_layout(
        title=dict(text=title, font=dict(size=20, color='#00ffff', family='Orbitron')),
        xaxis=dict(title='BDC', color='white', tickangle=-45),
        yaxis=dict(title='Volume (LT/KG)', color='white'),
        paper_bgcolor='rgba(10, 14, 39, 0.8)', plot_bgcolor='rgba(22, 33, 62, 0.6)',
        height=500, showlegend=False
    )
    return fig

def create_trend_chart(df, date_col='Date', value_col='Quantity', title="Trend Analysis"):
    df_trend = df.copy()
    df_trend[date_col] = pd.to_datetime(df_trend[date_col], errors='coerce')
    df_trend = df_trend.dropna(subset=[date_col])
    daily_summary = df_trend.groupby(df_trend[date_col].dt.date)[value_col].sum().reset_index()
    daily_summary.columns = ['Date', 'Volume']
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=daily_summary['Date'], y=daily_summary['Volume'],
        mode='lines+markers', name='Daily Volume',
        line=dict(color='#00ffff', width=3),
        marker=dict(size=8, color='#ff00ff', line=dict(color='white', width=2)),
        fill='tozeroy', fillcolor='rgba(0, 255, 255, 0.1)'
    ))
    fig.update_layout(
        title=dict(text=title, font=dict(size=20, color='#00ffff', family='Orbitron')),
        xaxis=dict(title='Date', color='white'), yaxis=dict(title='Volume (LT/KG)', color='white'),
        paper_bgcolor='rgba(10, 14, 39, 0.8)', plot_bgcolor='rgba(22, 33, 62, 0.6)',
        height=400, hovermode='x unified'
    )
    return fig

def create_comparison_chart(df1, df2, label1="Period 1", label2="Period 2"):
    prod1 = df1.groupby('Product')['Quantity'].sum().reset_index()
    prod2 = df2.groupby('Product')['Quantity'].sum().reset_index()
    fig = go.Figure()
    fig.add_trace(go.Bar(name=label1, x=prod1['Product'], y=prod1['Quantity'],
                         marker=dict(color='#00ffff'),
                         text=prod1['Quantity'].apply(lambda x: f'{x:,.0f}'), textposition='outside'))
    fig.add_trace(go.Bar(name=label2, x=prod2['Product'], y=prod2['Quantity'],
                         marker=dict(color='#ff00ff'),
                         text=prod2['Quantity'].apply(lambda x: f'{x:,.0f}'), textposition='outside'))
    fig.update_layout(
        title=dict(text='Period Comparison', font=dict(size=20, color='#00ffff', family='Orbitron')),
        xaxis=dict(title='Product', color='white'), yaxis=dict(title='Volume (LT/KG)', color='white'),
        paper_bgcolor='rgba(10, 14, 39, 0.8)', plot_bgcolor='rgba(22, 33, 62, 0.6)',
        barmode='group', height=400, legend=dict(font=dict(color='white'))
    )
    return fig

# ==================== ALERT FUNCTIONS ====================
def check_low_stock_alerts(df, threshold=10000):
    col_name = 'ACTUAL BALANCE (LT\\KG)'
    if col_name not in df.columns:
        return []
    alerts = []
    low_stock = df[df[col_name] < threshold]
    for _, row in low_stock.iterrows():
        balance_value = row[col_name]
        alerts.append({
            'type': 'warning', 'title': f"⚠️ Low Stock Alert",
            'message': f"{row['Product']} at {row['BDC']} - {row['DEPOT']}: {balance_value:,.0f} LT/KG",
            'severity': 'high' if balance_value < threshold/2 else 'medium'
        })
    return alerts

def check_volume_spikes(df, threshold_pct=50):
    if 'Quantity' not in df.columns:
        return []
    alerts = []
    mean_vol = df['Quantity'].mean()
    high_orders = df[df['Quantity'] > mean_vol * (1 + threshold_pct/100)]
    if len(high_orders) > 0:
        total_spike = high_orders['Quantity'].sum()
        alerts.append({
            'type': 'info', 'title': f"📈 Volume Spike Detected",
            'message': f"{len(high_orders)} orders with unusually high volume (Total: {total_spike:,.0f} LT/KG)",
            'severity': 'info'
        })
    return alerts

# ==================== PAGE CONFIG ====================
st.set_page_config(
    page_title="NPA Energy Analytics 🛢️",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== CUSTOM CSS ====================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Rajdhani:wght@300;500;700&display=swap');

    .stApp {
        background: linear-gradient(-45deg, #0a0e27, #1a1a2e, #16213e, #0f3460);
        background-size: 400% 400%;
        animation: gradientShift 15s ease infinite;
    }
    @keyframes gradientShift {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    h1, h2, h3 {
        font-family: 'Orbitron', sans-serif !important;
        color: #00ffff !important;
        text-shadow: 0 0 10px #00ffff, 0 0 20px #00ffff, 0 0 30px #00ffff;
        animation: glow 2s ease-in-out infinite alternate;
    }
    @keyframes glow {
        from { text-shadow: 0 0 5px #00ffff, 0 0 10px #00ffff, 0 0 15px #00ffff; }
        to   { text-shadow: 0 0 10px #00ffff, 0 0 20px #00ffff, 0 0 30px #00ffff, 0 0 40px #0ff; }
    }
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0a0e27 0%, #16213e 100%);
        border-right: 2px solid #00ffff;
        box-shadow: 5px 0 15px rgba(0, 255, 255, 0.3);
    }
    [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3 {
        color: #ff00ff !important;
        text-shadow: 0 0 10px #ff00ff;
    }
    .stButton > button {
        background: linear-gradient(45deg, #ff00ff, #00ffff);
        color: white; border: 2px solid #00ffff; border-radius: 25px;
        padding: 15px 30px; font-family: 'Orbitron', sans-serif;
        font-weight: 700; font-size: 18px;
        box-shadow: 0 0 20px rgba(0, 255, 255, 0.5);
        transition: all 0.3s ease; text-transform: uppercase; letter-spacing: 2px;
    }
    .stButton > button:hover {
        transform: scale(1.05) translateY(-3px);
        box-shadow: 0 0 30px rgba(0, 255, 255, 0.8), 0 0 40px rgba(255, 0, 255, 0.5);
        background: linear-gradient(45deg, #00ffff, #ff00ff);
    }
    .dataframe { background-color: rgba(10, 14, 39, 0.8) !important; border: 2px solid #00ffff !important; border-radius: 10px; box-shadow: 0 0 20px rgba(0, 255, 255, 0.3); }
    .dataframe th { background-color: #16213e !important; color: #00ffff !important; font-family: 'Orbitron', sans-serif; text-transform: uppercase; border: 1px solid #00ffff !important; }
    .dataframe td { background-color: rgba(22, 33, 62, 0.6) !important; color: #ffffff !important; border: 1px solid rgba(0, 255, 255, 0.2) !important; }
    [data-testid="stMetricValue"] { font-family: 'Orbitron', sans-serif; font-size: 28px !important; color: #00ffff !important; text-shadow: 0 0 15px #00ffff; }
    .metric-card { background: rgba(22,33,62,0.6); padding: 20px; border-radius: 15px; border: 2px solid #00ffff; text-align: center; }
    .metric-card h2 { color: #ff00ff !important; margin: 0; font-size: 20px !important; }
    .metric-card h1 { color: #00ffff !important; margin: 10px 0; font-size: 32px !important; word-wrap: break-word; }
    [data-testid="stMetricLabel"] { font-family: 'Rajdhani', sans-serif; color: #ff00ff !important; font-weight: 700; text-transform: uppercase; letter-spacing: 2px; }
    p, span, div { font-family: 'Rajdhani', sans-serif; color: #e0e0e0; }
    [data-testid="stFileUploader"] { border: 2px dashed #00ffff; border-radius: 15px; background: rgba(22, 33, 62, 0.3); padding: 20px; }
</style>
""", unsafe_allow_html=True)

# ==================== BDC BALANCE CLASS ====================
class StockBalanceScraper:
    def __init__(self):
        self.output_dir = os.path.join(os.getcwd(), "bdc_stock_dataset")
        os.makedirs(self.output_dir, exist_ok=True)
        self.allowed_products = {"PREMIUM", "GASOIL", "LPG"}
        product_alt = "|".join(sorted(self.allowed_products))
        self.product_line_re = re.compile(
            rf"^({product_alt})\s+([\d,]+\.\d{{2}})\s+(-?[\d,]+\.\d{{2}})$",
            flags=re.IGNORECASE
        )
        self.bost_global_re = re.compile(r"\bBOST\s*GLOBAL\s*DEPOT\b", flags=re.IGNORECASE)

    @staticmethod
    def _normalize_spaces(text):
        return re.sub(r"\s+", " ", (text or "").strip())

    def _normalize_bdc(self, bdc):
        if not bdc: return ""
        clean = self._normalize_spaces(bdc)
        up = clean.upper().replace("-", " ").replace("_", " ")
        up = self._normalize_spaces(up)
        if up.startswith("BOST"): return "BOST"
        return clean

    def _is_bost_labeled_depot(self, depot):
        dep = self._normalize_spaces(depot or "").replace("-", " ")
        dep = self._normalize_spaces(dep)
        return dep.upper().startswith("BOST ")

    def _is_bost_global_depot(self, depot):
        dep = self._normalize_spaces(depot or "").replace("-", " ")
        dep = self._normalize_spaces(dep)
        return bool(self.bost_global_re.search(dep))

    def _parse_date_from_line(self, line):
        m = re.search(r'(\w+\s+\d{1,2}\s*,\s*\d{4})', line)
        if m:
            cleaned = m.group(1).replace(" ,", ",").replace(" ", " ")
            return datetime.strptime(cleaned, '%B %d, %Y').strftime('%Y/%m/%d')
        return None

    def _append_record(self, records, date, bdc, depot, product, actual, available):
        bdc_clean = self._normalize_bdc(bdc)
        product = (product or "").upper()
        if product not in self.allowed_products: return
        if self._is_bost_labeled_depot(depot) and not self._is_bost_global_depot(depot): return
        if actual <= 0: return
        records.append({
            'Date': date, 'BDC': bdc_clean,
            'DEPOT': self._normalize_spaces(depot), 'Product': product,
            'ACTUAL BALANCE (LT\\KG)': actual, 'AVAILABLE BALANCE (LT\\KG)': available
        })

    def parse_pdf_file(self, pdf_file):
        records = []
        try:
            reader = PyPDF2.PdfReader(pdf_file)
            current_bdc = current_depot = current_date = None
            for page in reader.pages:
                text = page.extract_text() or ""
                lines = [ln.strip() for ln in text.split('\n') if ln.strip()]
                for line in lines:
                    up = line.upper()
                    if 'DATE AS AT' in up:
                        maybe_date = self._parse_date_from_line(line)
                        if maybe_date: current_date = maybe_date
                    if up.startswith('BDC :') or up.startswith('BDC:'):
                        current_bdc = re.sub(r'^BDC\s*:\s*', '', line, flags=re.IGNORECASE).strip()
                    if up.startswith('DEPOT :') or up.startswith('DEPOT:'):
                        current_depot = re.sub(r'^DEPOT\s*:\s*', '', line, flags=re.IGNORECASE).strip()
                    if current_bdc and current_depot and current_date:
                        m = self.product_line_re.match(line)
                        if m:
                            product  = m.group(1)
                            actual   = float(m.group(2).replace(',', ''))
                            available = float(m.group(3).replace(',', ''))
                            self._append_record(records, current_date, current_bdc, current_depot, product, actual, available)
        except Exception as e:
            st.error(f"Error parsing PDF: {e}")
        return records

    def save_to_excel(self, records, filename=None):
        if not records: return None
        if filename is None:
            filename = f"stock_balance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        out_path = os.path.join(self.output_dir, os.path.basename(filename))
        df = pd.DataFrame(records)
        df = df.sort_values(['Product', 'BDC', 'DEPOT', 'Date'], ignore_index=True)
        with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Stock Balance')
            for prod in ['LPG', 'PREMIUM', 'GASOIL']:
                dff = df[df['Product'].str.upper() == prod]
                if dff.empty: dff = pd.DataFrame(columns=df.columns)
                dff.to_excel(writer, index=False, sheet_name=prod)
        return out_path

    def parse_text_data(self, text_content):
        records = []
        lines = [ln.strip() for ln in (text_content or "").split('\n') if ln.strip()]
        current_bdc = current_depot = current_date = None
        for line in lines:
            up = line.upper()
            if 'DATE AS AT' in up:
                maybe_date = self._parse_date_from_line(line)
                if maybe_date: current_date = maybe_date
            if up.startswith('BDC :') or up.startswith('BDC:'):
                current_bdc = re.sub(r'^BDC\s*:\s*', '', line, flags=re.IGNORECASE).strip()
            if up.startswith('DEPOT :') or up.startswith('DEPOT:'):
                current_depot = re.sub(r'^DEPOT\s*:\s*', '', line, flags=re.IGNORECASE).strip()
            if current_bdc and current_depot and current_date:
                m = self.product_line_re.match(line)
                if m:
                    product   = m.group(1)
                    actual    = float(m.group(2).replace(',', ''))
                    available = float(m.group(3).replace(',', ''))
                    self._append_record(records, current_date, current_bdc, current_depot, product, actual, available)
        return records

# ==================== OMC LOADINGS FUNCTIONS ====================
PRODUCT_MAP = {"AGO": "GASOIL", "PMS": "PREMIUM", "LPG": "LPG"}
ONLY_COLUMNS = ["Date", "OMC", "Truck", "Product", "Quantity", "Price", "Depot", "Order Number", "BDC"]
HEADER_KEYWORDS = ["ORDER REPORT", "National Petroleum Authority", "ORDER NUMBER", "ORDER DATE",
                   "ORDER STATUS", "BDC:", "Total for :", "Printed By :", "Page ", "BRV NUMBER", "VOLUME"]
LOADED_KEYWORDS = {"Released", "Submitted"}

def _looks_like_header(line):
    return any(h in line for h in HEADER_KEYWORDS)

def _extract_depot(line):
    m = re.search(r"DEPOT:([^-\n]+)", line)
    return m.group(1).strip() if m else None

def _extract_bdc(line):
    m = re.search(r"BDC:([^\n]+)", line)
    return m.group(1).strip() if m else None

def _detect_product(line):
    if "AGO" in line: raw = "AGO"
    elif "LPG" in line: raw = "LPG"
    else: raw = "PMS"
    return PRODUCT_MAP.get(raw, raw or "")

def _find_loaded_index(tokens):
    for i, t in enumerate(tokens):
        if t in LOADED_KEYWORDS: return i
    return None

def _parse_loaded_line(line, current_product, current_depot, current_bdc):
    tokens = line.split()
    if len(tokens) < 6: return None
    rel_idx = _find_loaded_index(tokens)
    if rel_idx is None or rel_idx < 2: return None
    try:
        date_token   = tokens[0]
        order_number = tokens[1]
        volume       = float(tokens[-1].replace(",", ""))
        price        = float(tokens[-2].replace(",", ""))
        brv_number   = tokens[-3]
        company_name = " ".join(tokens[rel_idx + 1:-3]).strip()
        try:
            date_obj = datetime.strptime(date_token, "%d-%b-%Y")
            date_str = date_obj.strftime("%Y/%m/%d")
        except:
            date_str = date_token
        return {"Date": date_str, "OMC": company_name, "Truck": brv_number,
                "Product": current_product, "Quantity": volume, "Price": price,
                "Depot": current_depot, "Order Number": order_number, "BDC": current_bdc}
    except:
        return None

def extract_npa_data_from_pdf(pdf_file):
    extracted_rows = []
    current_depot = current_bdc = ""
    current_product = PRODUCT_MAP.get("PMS", "PMS")
    try:
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or page.extract_text(x_tolerance=2, y_tolerance=2)
                if not text: continue
                for raw_line in text.split("\n"):
                    line = raw_line.strip()
                    if not line: continue
                    if "DEPOT:" in line:
                        maybe_depot = _extract_depot(line)
                        if maybe_depot: current_depot = maybe_depot
                        continue
                    if "BDC:" in line:
                        maybe_bdc = _extract_bdc(line)
                        if maybe_bdc: current_bdc = maybe_bdc
                        continue
                    if "PRODUCT" in line:
                        current_product = _detect_product(line)
                        continue
                    if _looks_like_header(line): continue
                    if any(kw in line for kw in LOADED_KEYWORDS):
                        row = _parse_loaded_line(line, current_product, current_depot, current_bdc)
                        if row: extracted_rows.append(row)
    except Exception as e:
        st.error(f"Error parsing PDF: {e}")
        return pd.DataFrame(columns=ONLY_COLUMNS)
    df = pd.DataFrame(extracted_rows)
    if df.empty: return pd.DataFrame(columns=ONLY_COLUMNS)
    for col in ONLY_COLUMNS:
        if col not in df.columns: df[col] = ""
    df = df[ONLY_COLUMNS].drop_duplicates()
    try:
        _ds = pd.to_datetime(df["Date"], format="%Y/%m/%d", errors="coerce")
        df  = df.assign(_ds=_ds).sort_values("_ds").drop(columns=["_ds"]).reset_index(drop=True)
    except:
        df = df.reset_index(drop=True)
    return df

def save_to_excel_multi(df, filename=None):
    out_dir = os.path.join(os.getcwd(), "omc_loadings")
    os.makedirs(out_dir, exist_ok=True)
    if filename is None:
        filename = f"npa_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    out_path = os.path.join(out_dir, filename)
    df_filtered = df[df["Product"].isin(["PREMIUM", "GASOIL", "LPG"])].copy()
    if not df_filtered.empty:
        pivot = df_filtered.pivot_table(index="BDC", columns="Product", values="Quantity", aggfunc="sum", fill_value=0.0).reset_index()
        product_cols = [c for c in pivot.columns if c in ["PREMIUM", "GASOIL", "LPG"]]
        pivot["Total"] = pivot[product_cols].sum(axis=1)
    else:
        pivot = pd.DataFrame(columns=["BDC", "GASOIL", "LPG", "PREMIUM", "Total"])
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="All Orders", index=False)
        for prod in ["PREMIUM", "GASOIL", "LPG"]:
            df[df["Product"] == prod].to_excel(writer, sheet_name=prod, index=False)
        pivot.to_excel(writer, sheet_name="BDC Summary", index=False)
    return out_path

def parse_text_to_dataframe(text_content):
    extracted_rows = []
    current_depot = current_bdc = ""
    current_product = PRODUCT_MAP.get("PMS", "PMS")
    for raw_line in text_content.split("\n"):
        line = raw_line.strip()
        if not line: continue
        if "DEPOT:" in line:
            maybe_depot = _extract_depot(line)
            if maybe_depot: current_depot = maybe_depot
            continue
        if "BDC:" in line:
            maybe_bdc = _extract_bdc(line)
            if maybe_bdc: current_bdc = maybe_bdc
            continue
        if "PRODUCT" in line:
            current_product = _detect_product(line)
            continue
        if _looks_like_header(line): continue
        if any(kw in line for kw in LOADED_KEYWORDS):
            row = _parse_loaded_line(line, current_product, current_depot, current_bdc)
            if row: extracted_rows.append(row)
    df = pd.DataFrame(extracted_rows)
    if df.empty: return pd.DataFrame(columns=ONLY_COLUMNS)
    for col in ONLY_COLUMNS:
        if col not in df.columns: df[col] = ""
    df = df[ONLY_COLUMNS].drop_duplicates()
    try:
        _ds = pd.to_datetime(df["Date"], format="%Y/%m/%d", errors="coerce")
        df  = df.assign(_ds=_ds).sort_values("_ds").drop(columns=["_ds"]).reset_index(drop=True)
    except:
        df = df.reset_index(drop=True)
    return df

# ==================== DAILY ORDERS FUNCTIONS ====================
DAILY_PRODUCT_MAP = {
    "PMS": "PREMIUM", "AGO": "GASOIL", "LPG": "LPG", "RFO": "RFO",
    "ATK": "ATK", "AVIATION": "ATK", "PREMIX": "PREMIX", "MGO": "GASOIL", "KEROSENE": "KEROSENE"
}

def clean_currency(value_str):
    if not value_str: return 0.0
    try: return float(value_str.replace(",", "").strip())
    except: return 0.0

def get_product_category(text):
    text_upper = text.upper()
    if "AVIATION" in text_upper or "TURBINE" in text_upper: return "ATK"
    if "RFO" in text_upper: return "RFO"
    if "PREMIX" in text_upper: return "PREMIX"
    if "LPG" in text_upper: return "LPG"
    if "AGO" in text_upper or "MGO" in text_upper or "GASOIL" in text_upper: return "GASOIL"
    if "PMS" in text_upper or "PREMIUM" in text_upper: return "PREMIUM"
    return "PREMIUM"

def parse_daily_line(line, last_known_date):
    line = line.strip()
    pv_match = re.search(r"(\d{1,4}\.\d{2,4})\s+(\d{1,3}(?:,\d{3})*\.\d{2})$", line)
    if not pv_match: return None
    price  = clean_currency(pv_match.group(1))
    volume = clean_currency(pv_match.group(2))
    remainder = line[:pv_match.start()].strip()
    tokens = remainder.split()
    if not tokens: return None
    brv = tokens[-1]
    tokens = tokens[:-1]
    remainder = " ".join(tokens)
    date_val = last_known_date
    date_match = re.search(r"(\d{2}/\d{2}/\d{4})", remainder)
    if date_match:
        date_val = date_match.group(1)
        try:
            date_obj = datetime.strptime(date_val, "%d/%m/%Y")
            date_val = date_obj.strftime("%Y/%m/%d")
        except: pass
        remainder = remainder.replace(date_match.group(1), "").strip()
    product_cat = get_product_category(line)
    noise_words = ["PMS", "AGO", "LPG", "RFO", "ATK", "PREMIX", "FOREIGN",
                   "(Retail Outlets)", "Retail", "Outlets", "MGO", "Local",
                   "Additivated", "Differentiated", "MINES", "Cell Sites", "Turbine", "Kerosene"]
    order_num_tokens = []
    for t in remainder.split():
        is_noise = any(nw.upper() in t.upper() or t in ["(", ")", "-"] for nw in noise_words)
        if not is_noise: order_num_tokens.append(t)
    order_number = " ".join(order_num_tokens).strip()
    if not order_number and len(tokens) > 0: order_number = remainder
    return {"Date": date_val, "Order Number": order_number, "Product": product_cat,
            "Truck": brv, "Price": price, "Quantity": volume}

def simplify_bdc_names(df):
    if "BDC" not in df.columns or df.empty: return df
    mapping = {}
    for name in df["BDC"].unique():
        if not name:
            mapping[name] = name
            continue
        parts = name.split()
        mapping[name] = " ".join(parts[:2]).upper()
    df["BDC"] = df["BDC"].map(mapping)
    return df

def extract_daily_orders_from_pdf(pdf_file):
    all_rows = []
    ctx = {"Depot": "Unknown Depot", "BDC": "Unknown BDC", "Status": "Unknown Status", "Date": None}
    try:
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text(x_tolerance=2, y_tolerance=2)
                if not text: continue
                for line in text.split('\n'):
                    clean = line.strip()
                    if not clean: continue
                    if clean.startswith("DEPOT:"):
                        raw_depot = clean.replace("DEPOT:", "").strip()
                        ctx["Depot"] = "BOST Global" if (raw_depot.startswith("BOST") or "TAKORADI BLUE OCEAN" in raw_depot) else raw_depot
                        continue
                    if clean.startswith("BDC:"):
                        ctx["BDC"] = clean.replace("BDC:", "").strip(); continue
                    if "Order Status" in clean:
                        parts = clean.split(":")
                        if len(parts) > 1: ctx["Status"] = parts[-1].strip()
                        continue
                    if not re.search(r"\d{2}$", clean): continue
                    row_data = parse_daily_line(clean, ctx["Date"])
                    if row_data:
                        if row_data["Date"]: ctx["Date"] = row_data["Date"]
                        all_rows.append({
                            "Date": row_data["Date"], "Truck": row_data["Truck"],
                            "Product": row_data["Product"], "Quantity": row_data["Quantity"],
                            "Price": row_data["Price"], "Depot": ctx["Depot"],
                            "Order Number": row_data["Order Number"],
                            "BDC": ctx["BDC"], "Status": ctx["Status"]
                        })
    except Exception as e:
        st.error(f"Error parsing PDF: {e}")
        return pd.DataFrame()
    df = pd.DataFrame(all_rows)
    if not df.empty: df = simplify_bdc_names(df)
    return df

def save_daily_orders_excel(df, filename=None):
    out_dir = os.path.join(os.getcwd(), "daily_orders")
    os.makedirs(out_dir, exist_ok=True)
    if filename is None:
        filename = f"daily_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    out_path = os.path.join(out_dir, filename)
    if not df.empty:
        pivot = df.pivot_table(index="BDC", columns="Product", values="Quantity", aggfunc="sum", fill_value=0).reset_index()
        product_cols = [c for c in pivot.columns if c != "BDC"]
        pivot["Grand Total"] = pivot[product_cols].sum(axis=1)
    else:
        pivot = pd.DataFrame()
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="All Orders", index=False)
        if not pivot.empty: pivot.to_excel(writer, sheet_name="Summary by BDC", index=False)
    return out_path

# ==================== MAIN APP ====================
def main():
    st.markdown("""
    <div style='text-align: center; padding: 30px 0;'>
        <h1 style='font-size: 72px; margin: 0;'>⚡ NPA ENERGY ANALYTICS ⚡</h1>
        <p style='font-size: 24px; color: #ff00ff; font-family: "Orbitron", sans-serif; letter-spacing: 3px; margin-top: 10px;'>
            FUEL THE FUTURE WITH DATA
        </p>
    </div>
    """, unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("<h2 style='text-align: center;'>🎯 MISSION CONTROL</h2>", unsafe_allow_html=True)
        choice = st.radio("SELECT YOUR DATA MISSION:", [
            "🏦 BDC BALANCE", "🚚 OMC LOADINGS", "📅 DAILY ORDERS",
            "📊 MARKET SHARE", "🎯 COMPETITIVE INTEL",
            "📈 STOCK TRANSACTION", "🧠 BDC INTELLIGENCE", "🌍 NATIONAL STOCKOUT",
        ], index=0)
        st.markdown("---")
        st.markdown("""
        <div style='text-align: center; padding: 20px; background: rgba(255, 0, 255, 0.1); border-radius: 10px; border: 2px solid #ff00ff;'>
            <h3>⚙️ SYSTEM STATUS</h3>
            <p style='color: #00ff88; font-size: 20px;'>🟢 OPERATIONAL</p>
        </div>
        """, unsafe_allow_html=True)

    if   choice == "🏦 BDC BALANCE":        show_bdc_balance()
    elif choice == "🚚 OMC LOADINGS":        show_omc_loadings()
    elif choice == "📅 DAILY ORDERS":        show_daily_orders()
    elif choice == "📊 MARKET SHARE":        show_market_share()
    elif choice == "🎯 COMPETITIVE INTEL":   show_competitive_intel()
    elif choice == "📈 STOCK TRANSACTION":   show_stock_transaction()
    elif choice == "🌍 NATIONAL STOCKOUT":   show_national_stockout()
    else:                                     show_bdc_intelligence()


# ==================== BDC BALANCE PAGE ====================
def show_bdc_balance():
    st.markdown("<h2>🏦 BDC STOCK BALANCE ANALYZER</h2>", unsafe_allow_html=True)
    st.info("📊 Click the button below to fetch BDC Balance data")
    st.markdown("---")
    if 'bdc_records' not in st.session_state:
        st.session_state.bdc_records = []
    if st.button("🔄 FETCH BDC BALANCE DATA", width="stretch"):
        with st.spinner("🔄 FETCHING DATA FROM NPA PORTAL..."):
            scraper = StockBalanceScraper()
            url = NPA_CONFIG['BDC_BALANCE_URL']
            params = {
                'lngCompanyId': NPA_CONFIG['COMPANY_ID'],
                'strITSfromPersol': NPA_CONFIG['ITS_FROM_PERSOL'],
                'strGroupBy': 'BDC', 'strGroupBy1': 'DEPOT',
                'strQuery1': '', 'strQuery2': '', 'strQuery3': '', 'strQuery4': '',
                'strPicHeight': '1', 'szPicWeight': '1',
                'lngUserId': NPA_CONFIG['USER_ID'], 'intAppId': NPA_CONFIG['APP_ID']
            }
            try:
                import requests
                headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/pdf,*/*'}
                response = requests.get(url, params=params, headers=headers, timeout=30)
                response.raise_for_status()
                if response.content[:4] == b'%PDF':
                    st.success("✅ PDF received from server")
                    st.session_state.bdc_records = scraper.parse_pdf_file(io.BytesIO(response.content))
                    if not st.session_state.bdc_records:
                        st.warning("⚠️ No records found in PDF.")
                else:
                    st.error("❌ Response is not a PDF.")
                    st.session_state.bdc_records = []
            except Exception as e:
                st.error(f"❌ Error: {e}")
                import traceback; st.code(traceback.format_exc())
                st.session_state.bdc_records = []
    records = st.session_state.bdc_records
    if records:
        df = pd.DataFrame(records)
        st.success(f"✅ SUCCESSFULLY EXTRACTED {len(records)} RECORDS")
        st.markdown("---")
        st.markdown("<h3>📊 ANALYTICS DASHBOARD</h3>", unsafe_allow_html=True)
        summary = df.groupby('Product')['ACTUAL BALANCE (LT\\KG)'].sum()
        cols = st.columns(3)
        for idx, prod in enumerate(['GASOIL', 'LPG', 'PREMIUM']):
            with cols[idx]:
                val = summary.get(prod, 0)
                st.markdown(f"<div class='metric-card'><h2>{prod}</h2><h1>{val:,.0f}</h1><p style='color:#888;font-size:14px;margin:0;'>LT/KG</p></div>", unsafe_allow_html=True)
        st.markdown("---")
        st.markdown("<h3>🏢 BDC BREAKDOWN</h3>", unsafe_allow_html=True)
        bdc_summary = df.groupby('BDC').agg({'ACTUAL BALANCE (LT\\KG)': 'sum', 'DEPOT': 'nunique', 'Product': lambda x: x.nunique()}).reset_index()
        bdc_summary.columns = ['BDC', 'Total Balance (LT/KG)', 'Depots', 'Products']
        bdc_summary = bdc_summary.sort_values('Total Balance (LT/KG)', ascending=False)
        col1, col2 = st.columns([2, 1])
        with col1: st.dataframe(bdc_summary, width="stretch", hide_index=True)
        with col2:
            st.markdown("#### 📈 Key Metrics")
            st.metric("Total BDCs", f"{df['BDC'].nunique()}")
            st.metric("Total Depots", f"{df['DEPOT'].nunique()}")
            st.metric("Grand Total", f"{df['ACTUAL BALANCE (LT\\KG)'].sum():,.0f} LT/KG")
        st.markdown("---")
        st.markdown("<h3>📊 PRODUCT DISTRIBUTION BY BDC</h3>", unsafe_allow_html=True)
        pivot_data = df.pivot_table(index='BDC', columns='Product', values='ACTUAL BALANCE (LT\\KG)', aggfunc='sum', fill_value=0).reset_index()
        for prod in ['GASOIL', 'LPG', 'PREMIUM']:
            if prod not in pivot_data.columns: pivot_data[prod] = 0
        pivot_data['TOTAL'] = pivot_data[['GASOIL', 'LPG', 'PREMIUM']].sum(axis=1)
        pivot_data = pivot_data.sort_values('TOTAL', ascending=False)
        st.dataframe(pivot_data[['BDC', 'GASOIL', 'LPG', 'PREMIUM', 'TOTAL']], width="stretch", hide_index=True)
        st.markdown("---")
        st.markdown("<h3>🔍 SEARCH & FILTER</h3>", unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1: search_type  = st.selectbox("Search By:", ["Product", "BDC", "Depot"], key='bdc_search_type')
        with col2:
            if search_type == "Product":
                search_value = st.selectbox("Select Product:", ['ALL'] + sorted(df['Product'].unique().tolist()), key='bdc_product_search')
            elif search_type == "BDC":
                search_value = st.selectbox("Select BDC:", ['ALL'] + sorted(df['BDC'].unique().tolist()), key='bdc_bdc_search')
            else:
                search_value = st.selectbox("Select Depot:", ['ALL'] + sorted(df['DEPOT'].unique().tolist()), key='bdc_depot_search')
        filtered = df if search_value == 'ALL' else (
            df[df['Product'] == search_value] if search_type == "Product" else
            df[df['BDC']     == search_value] if search_type == "BDC"     else
            df[df['DEPOT']   == search_value]
        )
        st.markdown(f"<h3>📋 FILTERED DATA: {search_value}</h3>", unsafe_allow_html=True)
        display = filtered[['Product','BDC','DEPOT','AVAILABLE BALANCE (LT\\KG)','ACTUAL BALANCE (LT\\KG)','Date']].sort_values(['Product','BDC','DEPOT'])
        st.dataframe(display, width="stretch", height=400, hide_index=True)
        st.markdown("---")
        st.markdown("<h3>📋 QUICK STATS</h3>", unsafe_allow_html=True)
        cols = st.columns(4)
        with cols[0]: st.metric("RECORDS",       f"{len(filtered):,}")
        with cols[1]: st.metric("BDCs",           f"{filtered['BDC'].nunique()}")
        with cols[2]: st.metric("DEPOTS",         f"{filtered['DEPOT'].nunique()}")
        with cols[3]: st.metric("TOTAL BALANCE",  f"{filtered['ACTUAL BALANCE (LT\\KG)'].sum():,.0f}")
        st.markdown("---")
        st.markdown("<h3>💾 EXPORT DATA</h3>", unsafe_allow_html=True)
        scraper = StockBalanceScraper()
        path = scraper.save_to_excel(records)
        if path and os.path.exists(path):
            with open(path, 'rb') as f:
                st.download_button("⬇️ DOWNLOAD EXCEL", f, os.path.basename(path),
                                   "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", width="stretch")
    else:
        st.info("👆 Click the button above to fetch BDC balance data")


# ==================== OMC LOADINGS PAGE ====================
def show_omc_loadings():
    st.markdown("<h2>🚚 OMC LOADINGS ANALYZER</h2>", unsafe_allow_html=True)
    st.info("📊 Select date range and fetch OMC loadings data")
    st.markdown("---")
    if 'omc_df'         not in st.session_state: st.session_state.omc_df         = pd.DataFrame()
    if 'omc_start_date' not in st.session_state: st.session_state.omc_start_date = datetime.now() - timedelta(days=7)
    if 'omc_end_date'   not in st.session_state: st.session_state.omc_end_date   = datetime.now()
    st.markdown("<h3>📅 SELECT DATE RANGE</h3>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1: start_date = st.date_input("Start Date", value=st.session_state.omc_start_date, key='omc_start')
    with col2: end_date   = st.date_input("End Date",   value=st.session_state.omc_end_date,   key='omc_end')
    if st.button("🔄 FETCH OMC LOADINGS DATA", width="stretch"):
        with st.spinner("🔄 FETCHING DATA FROM NPA PORTAL..."):
            st.session_state.omc_start_date = start_date
            st.session_state.omc_end_date   = end_date
            start_str = start_date.strftime("%m/%d/%Y")
            end_str   = end_date.strftime("%m/%d/%Y")
            url = NPA_CONFIG['OMC_LOADINGS_URL']
            params = {
                'lngCompanyId': NPA_CONFIG['COMPANY_ID'], 'szITSfromPersol': 'persol',
                'strGroupBy': 'BDC', 'strGroupBy1': NPA_CONFIG['OMC_NAME'],
                'strQuery1': ' and iorderstatus=4', 'strQuery2': start_str, 'strQuery3': end_str,
                'strQuery4': '', 'strPicHeight': '', 'strPicWeight': '', 'intPeriodID': '4',
                'iUserId': NPA_CONFIG['USER_ID'], 'iAppId': NPA_CONFIG['APP_ID']
            }
            try:
                import requests
                response = requests.get(url, params=params, headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'application/pdf,*/*'}, timeout=30)
                response.raise_for_status()
                if response.content[:4] == b'%PDF':
                    st.success("✅ PDF received from server")
                    st.session_state.omc_df = extract_npa_data_from_pdf(io.BytesIO(response.content))
                    if st.session_state.omc_df.empty:
                        st.warning("⚠️ No order records found in the PDF for this date range.")
                else:
                    st.error("❌ Response is not a PDF.")
                    st.session_state.omc_df = pd.DataFrame()
            except Exception as e:
                st.error(f"❌ Error: {e}")
                import traceback; st.code(traceback.format_exc())
                st.session_state.omc_df = pd.DataFrame()
    df = st.session_state.omc_df
    if not df.empty:
        st.success(f"✅ EXTRACTED {len(df)} RECORDS")
        st.markdown("---")
        st.markdown("<h3>📊 ANALYTICS DASHBOARD</h3>", unsafe_allow_html=True)
        cols = st.columns(4)
        with cols[0]: st.markdown(f"<div class='metric-card'><h2>TOTAL ORDERS</h2><h1>{len(df):,}</h1></div>", unsafe_allow_html=True)
        with cols[1]: st.markdown(f"<div class='metric-card'><h2>VOLUME</h2><h1>{df['Quantity'].sum():,.0f}</h1><p style='color:#888;font-size:14px;margin:0;'>LT/KG</p></div>", unsafe_allow_html=True)
        with cols[2]: st.markdown(f"<div class='metric-card'><h2>OMCs</h2><h1>{df['OMC'].nunique()}</h1></div>", unsafe_allow_html=True)
        with cols[3]:
            total_value = (df['Quantity'] * df['Price']).sum()
            st.markdown(f"<div class='metric-card'><h2>VALUE</h2><h1>₵{total_value:,.0f}</h1></div>", unsafe_allow_html=True)
        st.markdown("---")
        st.markdown("<h3>📦 PRODUCT BREAKDOWN</h3>", unsafe_allow_html=True)
        product_summary = df.groupby('Product').agg({'Quantity': 'sum', 'Order Number': 'count', 'OMC': 'nunique'}).reset_index()
        product_summary.columns = ['Product', 'Total Volume (LT/KG)', 'Orders', 'OMCs']
        product_summary = product_summary.sort_values('Total Volume (LT/KG)', ascending=False)
        col1, col2 = st.columns([2, 1])
        with col1: st.dataframe(product_summary, width="stretch", hide_index=True)
        with col2:
            for _, row in product_summary.iterrows():
                pct = (row['Total Volume (LT/KG)'] / product_summary['Total Volume (LT/KG)'].sum()) * 100
                st.metric(row['Product'], f"{pct:.1f}%")
        st.markdown("---")
        st.markdown("<h3>🏢 TOP OMCs BY VOLUME</h3>", unsafe_allow_html=True)
        omc_summary = df.groupby('OMC').agg({'Quantity': 'sum', 'Order Number': 'count', 'Product': lambda x: x.nunique()}).reset_index()
        omc_summary.columns = ['OMC', 'Total Volume (LT/KG)', 'Orders', 'Products']
        st.dataframe(omc_summary.sort_values('Total Volume (LT/KG)', ascending=False).head(10), width="stretch", hide_index=True)
        st.markdown("---")
        st.markdown("<h3>🏦 BDC PERFORMANCE</h3>", unsafe_allow_html=True)
        bdc_summary = df.groupby('BDC').agg({'Quantity': 'sum', 'Order Number': 'count', 'OMC': 'nunique', 'Product': lambda x: x.nunique()}).reset_index()
        bdc_summary.columns = ['BDC', 'Total Volume (LT/KG)', 'Orders', 'OMCs', 'Products']
        st.dataframe(bdc_summary.sort_values('Total Volume (LT/KG)', ascending=False), width="stretch", hide_index=True)
        st.markdown("---")
        st.markdown("<h3>📊 PRODUCT DISTRIBUTION BY BDC</h3>", unsafe_allow_html=True)
        pivot_data = df.pivot_table(index='BDC', columns='Product', values='Quantity', aggfunc='sum', fill_value=0).reset_index()
        for prod in ['GASOIL', 'LPG', 'PREMIUM']:
            if prod not in pivot_data.columns: pivot_data[prod] = 0
        pivot_data['TOTAL'] = pivot_data[['GASOIL', 'LPG', 'PREMIUM']].sum(axis=1)
        st.dataframe(pivot_data[['BDC', 'GASOIL', 'LPG', 'PREMIUM', 'TOTAL']].sort_values('TOTAL', ascending=False), width="stretch", hide_index=True)
        st.markdown("---")
        st.markdown("<h3>🔍 SEARCH & FILTER</h3>", unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1: search_type  = st.selectbox("Search By:", ["Product", "OMC", "BDC", "Depot"], key='omc_search_type')
        with col2:
            if search_type == "Product": search_value = st.selectbox("Select Product:", ['ALL'] + sorted(df['Product'].unique().tolist()), key='omc_product_search')
            elif search_type == "OMC":  search_value = st.selectbox("Select OMC:",     ['ALL'] + sorted(df['OMC'].unique().tolist()),    key='omc_omc_search')
            elif search_type == "BDC":  search_value = st.selectbox("Select BDC:",     ['ALL'] + sorted(df['BDC'].unique().tolist()),    key='omc_bdc_search')
            else:                       search_value = st.selectbox("Select Depot:",   ['ALL'] + sorted(df['Depot'].unique().tolist()),  key='omc_depot_search')
        filtered = df if search_value == 'ALL' else (
            df[df['Product'] == search_value] if search_type == "Product" else
            df[df['OMC']     == search_value] if search_type == "OMC"    else
            df[df['BDC']     == search_value] if search_type == "BDC"    else
            df[df['Depot']   == search_value]
        )
        st.markdown(f"<h3>📋 FILTERED DATA: {search_value}</h3>", unsafe_allow_html=True)
        if not filtered.empty:
            cols = st.columns(4)
            with cols[0]: st.metric("Filtered Orders", f"{len(filtered):,}")
            with cols[1]: st.metric("Filtered Volume",  f"{filtered['Quantity'].sum():,.0f} LT")
            with cols[2]: st.metric("Unique OMCs",      f"{filtered['OMC'].nunique()}")
            with cols[3]: st.metric("Filtered Value",   f"₵{(filtered['Quantity'] * filtered['Price']).sum():,.0f}")
        st.dataframe(filtered[['Date','OMC','Truck','Quantity','Order Number','BDC','Depot','Price','Product']].sort_values(['Product','OMC','Date']), width="stretch", height=400, hide_index=True)
        st.markdown("---")
        st.markdown("<h3>💾 EXPORT DATA</h3>", unsafe_allow_html=True)
        path = save_to_excel_multi(df)
        if path and os.path.exists(path):
            with open(path, 'rb') as f:
                st.download_button("⬇️ DOWNLOAD EXCEL", f, os.path.basename(path),
                                   "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", width="stretch")
    else:
        st.info("👆 Select dates and click the button above to fetch OMC loadings data")


# ==================== DAILY ORDERS PAGE ====================
def show_daily_orders():
    st.markdown("<h2>📅 DAILY ORDERS ANALYZER</h2>", unsafe_allow_html=True)
    st.info("📊 Select a date range to fetch daily orders")
    st.markdown("---")
    if 'daily_df'         not in st.session_state: st.session_state.daily_df         = pd.DataFrame()
    if 'daily_start_date' not in st.session_state: st.session_state.daily_start_date = datetime.now() - timedelta(days=1)
    if 'daily_end_date'   not in st.session_state: st.session_state.daily_end_date   = datetime.now()
    col1, col2 = st.columns(2)
    with col1: start_date = st.date_input("Start Date", value=st.session_state.daily_start_date, key='daily_start')
    with col2: end_date   = st.date_input("End Date",   value=st.session_state.daily_end_date,   key='daily_end')
    if st.button("🔄 FETCH DAILY ORDERS", width="stretch"):
        with st.spinner("🔄 FETCHING DAILY ORDERS FROM NPA PORTAL..."):
            st.session_state.daily_start_date = start_date
            st.session_state.daily_end_date   = end_date
            start_str = start_date.strftime("%m/%d/%Y")
            end_str   = end_date.strftime("%m/%d/%Y")
            url = NPA_CONFIG['DAILY_ORDERS_URL']
            params = {
                'lngCompanyId': NPA_CONFIG['COMPANY_ID'], 'szITSfromPersol': 'persol',
                'strGroupBy': 'DEPOT', 'strGroupBy1': '', 'strQuery1': '',
                'strQuery2': start_str, 'strQuery3': end_str, 'strQuery4': '',
                'strPicHeight': '1', 'strPicWeight': '1', 'intPeriodID': '-1',
                'iUserId': NPA_CONFIG['USER_ID'], 'iAppId': NPA_CONFIG['APP_ID']
            }
            try:
                import requests
                response = requests.get(url, params=params, headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'application/pdf,*/*'}, timeout=30)
                response.raise_for_status()
                if response.content[:4] == b'%PDF':
                    st.success("✅ PDF received from server")
                    st.session_state.daily_df = extract_daily_orders_from_pdf(io.BytesIO(response.content))
                    if st.session_state.daily_df.empty:
                        st.warning("⚠️ No daily orders found for this date.")
                else:
                    st.error("❌ Response is not a PDF.")
                    st.session_state.daily_df = pd.DataFrame()
            except Exception as e:
                st.error(f"❌ Error: {e}")
                import traceback; st.code(traceback.format_exc())
                st.session_state.daily_df = pd.DataFrame()
    df = st.session_state.daily_df
    if not df.empty:
        if not st.session_state.get('omc_df', pd.DataFrame()).empty:
            loadings_df = st.session_state.omc_df
            def extract_order_prefix(order_num):
                if pd.isna(order_num): return None
                m = re.match(r'^([A-Z]{2,})', str(order_num).strip().upper())
                return m.group(1) if m else None
            loadings_df['Order_Prefix'] = loadings_df['Order Number'].apply(extract_order_prefix)
            prefix_to_omc = {}
            for prefix in loadings_df['Order_Prefix'].dropna().unique():
                mc = loadings_df[loadings_df['Order_Prefix'] == prefix]['OMC'].mode()
                if len(mc) > 0: prefix_to_omc[prefix] = mc.iloc[0]
            order_to_omc_dict = dict(zip(loadings_df['Order Number'], loadings_df['OMC']))
            df['Order_Prefix'] = df['Order Number'].apply(extract_order_prefix)
            df['OMC'] = df['Order Number'].map(order_to_omc_dict)
            df['OMC'] = df.apply(
                lambda row: prefix_to_omc.get(row['Order_Prefix']) if pd.isna(row['OMC']) and row['Order_Prefix'] else row['OMC'],
                axis=1
            )
            df = df.drop(columns=['Order_Prefix'])
            st.session_state.daily_df = df
        else:
            df['OMC'] = None
            st.session_state.daily_df = df
            st.warning("💡 Fetch OMC Loadings data first to auto-match order numbers with OMC names!")
        st.success(f"✅ EXTRACTED {len(df)} DAILY ORDERS")
        st.markdown("---")
        cols = st.columns(5)
        with cols[0]: st.markdown(f"<div class='metric-card'><h2>ORDERS</h2><h1>{len(df):,}</h1></div>", unsafe_allow_html=True)
        with cols[1]: st.markdown(f"<div class='metric-card'><h2>VOLUME</h2><h1>{df['Quantity'].sum():,.0f}</h1></div>", unsafe_allow_html=True)
        with cols[2]: st.markdown(f"<div class='metric-card'><h2>BDCs</h2><h1>{df['BDC'].nunique()}</h1></div>", unsafe_allow_html=True)
        with cols[3]:
            omc_count = df['OMC'].nunique() if 'OMC' in df.columns and df['OMC'].notna().any() else 0
            st.markdown(f"<div class='metric-card'><h2>OMCs</h2><h1>{omc_count}</h1></div>", unsafe_allow_html=True)
        with cols[4]:
            st.markdown(f"<div class='metric-card'><h2>VALUE</h2><h1>₵{(df['Quantity'] * df['Price']).sum():,.0f}</h1></div>", unsafe_allow_html=True)
        st.markdown("---")
        st.markdown("<h3>📦 PRODUCT SUMMARY</h3>", unsafe_allow_html=True)
        product_summary = df.groupby('Product').agg({'Quantity': 'sum', 'Order Number': 'count', 'BDC': 'nunique'}).reset_index()
        product_summary.columns = ['Product', 'Total Volume (LT/KG)', 'Orders', 'BDCs']
        st.dataframe(product_summary.sort_values('Total Volume (LT/KG)', ascending=False), width="stretch", hide_index=True)
        st.markdown("---")
        st.markdown("<h3>🏦 BDC SUMMARY</h3>", unsafe_allow_html=True)
        bdc_summary = df.groupby('BDC').agg({'Quantity': 'sum', 'Order Number': 'count', 'Product': lambda x: x.nunique(), 'Depot': lambda x: x.nunique()}).reset_index()
        bdc_summary.columns = ['BDC', 'Total Volume (LT/KG)', 'Orders', 'Products', 'Depots']
        st.dataframe(bdc_summary.sort_values('Total Volume (LT/KG)', ascending=False), width="stretch", hide_index=True)
        st.markdown("---")
        pivot_data = df.pivot_table(index='BDC', columns='Product', values='Quantity', aggfunc='sum', fill_value=0).reset_index()
        product_cols = [c for c in pivot_data.columns if c != 'BDC']
        pivot_data['TOTAL'] = pivot_data[product_cols].sum(axis=1)
        st.dataframe(pivot_data.sort_values('TOTAL', ascending=False), width="stretch", hide_index=True)
        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1: search_type  = st.selectbox("Search By:", ["Product", "BDC", "Depot", "Status"], key='daily_search_type')
        with col2:
            if search_type == "Product": search_value = st.selectbox("Select Product:", ['ALL'] + sorted(df['Product'].unique().tolist()), key='daily_product_search')
            elif search_type == "BDC":  search_value = st.selectbox("Select BDC:",     ['ALL'] + sorted(df['BDC'].unique().tolist()),    key='daily_bdc_search')
            elif search_type == "Depot":search_value = st.selectbox("Select Depot:",   ['ALL'] + sorted(df['Depot'].unique().tolist()),  key='daily_depot_search')
            else:                       search_value = st.selectbox("Select Status:",  ['ALL'] + sorted(df['Status'].unique().tolist()), key='daily_status_search')
        filtered = df if search_value == 'ALL' else (
            df[df['Product'] == search_value] if search_type == "Product" else
            df[df['BDC']     == search_value] if search_type == "BDC"    else
            df[df['Depot']   == search_value] if search_type == "Depot"  else
            df[df['Status']  == search_value]
        )
        st.dataframe(filtered[['Date','OMC','Truck','Quantity','Order Number','BDC','Depot','Price','Product','Status']].sort_values(['Product','BDC','Date']), width="stretch", height=400, hide_index=True)
        st.markdown("---")
        path = save_daily_orders_excel(df)
        if path and os.path.exists(path):
            with open(path, 'rb') as f:
                st.download_button("⬇️ DOWNLOAD EXCEL", f, os.path.basename(path),
                                   "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", width="stretch")
    else:
        st.info("👆 Select a date range and click the button above to fetch daily orders")


# ==================== MARKET SHARE PAGE ====================
def show_market_share():
    st.markdown("<h2>📊 BDC MARKET SHARE ANALYSIS</h2>", unsafe_allow_html=True)
    st.info("🎯 Comprehensive market share analysis: Stock Balance + Sales Volume")
    st.markdown("---")
    has_balance  = bool(st.session_state.get('bdc_records'))
    has_loadings = not st.session_state.get('omc_df', pd.DataFrame()).empty
    col1, col2 = st.columns(2)
    with col1:
        if has_balance:
            balance_df = pd.DataFrame(st.session_state.bdc_records)
            st.success(f"✅ BDC Balance: {len(balance_df)} records")
        else: st.warning("⚠️ BDC Balance Data Not Loaded")
    with col2:
        if has_loadings:
            loadings_df = st.session_state.omc_df
            st.success(f"✅ OMC Loadings: {len(loadings_df)} records")
        else: st.warning("⚠️ OMC Loadings Data Not Loaded")
    if not has_balance and not has_loadings:
        st.error("❌ No data available."); return
    all_bdcs = set()
    if has_balance:  all_bdcs.update(balance_df['BDC'].unique())
    if has_loadings: all_bdcs.update(loadings_df['BDC'].unique())
    all_bdcs = sorted(list(all_bdcs))
    if not all_bdcs: st.error("❌ No BDCs found."); return
    selected_bdc = st.selectbox("Choose BDC:", all_bdcs, key='market_share_bdc')
    if not selected_bdc: return
    st.markdown(f"## 📊 COMPREHENSIVE MARKET REPORT: {selected_bdc}")
    tab1, tab2, tab3 = st.tabs(["📦 Stock Balance", "🚚 Sales Volume", "📊 Combined Analysis"])
    with tab1:
        if not has_balance: st.warning("⚠️ BDC Balance data not available.")
        else:
            col_bal = 'ACTUAL BALANCE (LT\\KG)'
            bdc_balance_data = balance_df[balance_df['BDC'] == selected_bdc]
            total_market_stock = balance_df[col_bal].sum()
            bdc_total_stock    = bdc_balance_data[col_bal].sum()
            bdc_stock_share    = (bdc_total_stock / total_market_stock * 100) if total_market_stock > 0 else 0
            all_bdc_stocks     = balance_df.groupby('BDC')[col_bal].sum().sort_values(ascending=False)
            stock_rank         = list(all_bdc_stocks.index).index(selected_bdc) + 1 if selected_bdc in all_bdc_stocks.index else 0
            cols = st.columns(3)
            with cols[0]: st.markdown(f"<div class='metric-card'><h2>TOTAL STOCK</h2><h1>{bdc_total_stock:,.0f}</h1><p style='color:#888;font-size:14px;margin:0;'>LT/KG</p></div>", unsafe_allow_html=True)
            with cols[1]: st.markdown(f"<div class='metric-card'><h2>MARKET SHARE</h2><h1>{bdc_stock_share:.2f}%</h1></div>", unsafe_allow_html=True)
            with cols[2]: st.markdown(f"<div class='metric-card'><h2>STOCK RANK</h2><h1>#{stock_rank}</h1><p style='color:#888;font-size:14px;margin:0;'>out of {len(all_bdc_stocks)}</p></div>", unsafe_allow_html=True)
            st.markdown("---")
            product_stock_data = []
            for product in ['PREMIUM', 'GASOIL', 'LPG']:
                mkt   = balance_df[balance_df['Product'] == product][col_bal].sum()
                bdc_p = bdc_balance_data[bdc_balance_data['Product'] == product][col_bal].sum()
                product_stock_data.append({'Product': product, 'BDC Stock (LT/KG)': bdc_p, 'Market Total (LT/KG)': mkt, 'Market Share (%)': (bdc_p / mkt * 100) if mkt > 0 else 0})
            st.dataframe(pd.DataFrame(product_stock_data), width="stretch", hide_index=True)
    with tab2:
        if not has_loadings: st.warning("⚠️ OMC Loadings data not available.")
        else:
            bdc_sales_data     = loadings_df[loadings_df['BDC'] == selected_bdc]
            total_market_sales = loadings_df['Quantity'].sum()
            bdc_total_sales    = bdc_sales_data['Quantity'].sum()
            bdc_sales_share    = (bdc_total_sales / total_market_sales * 100) if total_market_sales > 0 else 0
            all_bdc_sales      = loadings_df.groupby('BDC')['Quantity'].sum().sort_values(ascending=False)
            sales_rank         = list(all_bdc_sales.index).index(selected_bdc) + 1 if selected_bdc in all_bdc_sales.index else 0
            bdc_revenue        = (bdc_sales_data['Quantity'] * bdc_sales_data['Price']).sum()
            cols = st.columns(4)
            with cols[0]: st.markdown(f"<div class='metric-card'><h2>TOTAL SALES</h2><h1>{bdc_total_sales:,.0f}</h1></div>", unsafe_allow_html=True)
            with cols[1]: st.markdown(f"<div class='metric-card'><h2>MARKET SHARE</h2><h1>{bdc_sales_share:.2f}%</h1></div>", unsafe_allow_html=True)
            with cols[2]: st.markdown(f"<div class='metric-card'><h2>OVERALL RANK</h2><h1>#{sales_rank}</h1><p style='color:#888;font-size:14px;margin:0;'>out of {len(all_bdc_sales)}</p></div>", unsafe_allow_html=True)
            with cols[3]: st.markdown(f"<div class='metric-card'><h2>REVENUE</h2><h1>₵{bdc_revenue/1000000:,.1f}M</h1></div>", unsafe_allow_html=True)
            product_rank_lookup = {}
            for product in ['PREMIUM', 'GASOIL', 'LPG']:
                prod_bdc = loadings_df[loadings_df['Product'] == product].groupby('BDC')['Quantity'].sum().sort_values(ascending=False)
                rank = list(prod_bdc.index).index(selected_bdc) + 1 if selected_bdc in prod_bdc.index else None
                product_rank_lookup[product] = {'rank': rank, 'total': len(prod_bdc)}
            product_sales_data = []
            for product in ['PREMIUM', 'GASOIL', 'LPG']:
                mkt   = loadings_df[loadings_df['Product'] == product]['Quantity'].sum()
                bdc_p = bdc_sales_data[bdc_sales_data['Product'] == product]['Quantity'].sum()
                ri    = product_rank_lookup[product]
                product_sales_data.append({
                    'Product': product, 'BDC Sales (LT/KG)': bdc_p,
                    'Market Total (LT/KG)': mkt,
                    'Market Share (%)': round((bdc_p / mkt * 100) if mkt > 0 else 0, 2),
                    'Rank (by Product)': f"#{ri['rank']} / {ri['total']}" if ri['rank'] else f"N/A / {ri['total']}",
                    'Orders': len(bdc_sales_data[bdc_sales_data['Product'] == product])
                })
            st.dataframe(pd.DataFrame(product_sales_data), width="stretch", hide_index=True)
    with tab3:
        if not has_balance or not has_loadings:
            st.warning("⚠️ Both BDC Balance and OMC Loadings data required."); return
        comparison_data = []
        for product in ['PREMIUM', 'GASOIL', 'LPG']:
            bdc_bal_row = next((r for r in product_stock_data if r['Product'] == product), {}) if has_balance else {}
            bdc_sls_row = next((r for r in product_sales_data if r['Product'] == product), {}) if has_loadings else {}
            comparison_data.append({
                'Product': product,
                'Stock (LT)': bdc_bal_row.get('BDC Stock (LT/KG)', 0),
                'Stock Share (%)': bdc_bal_row.get('Market Share (%)', 0),
                'Sales (LT)': bdc_sls_row.get('BDC Sales (LT/KG)', 0),
                'Sales Share (%)': bdc_sls_row.get('Market Share (%)', 0),
            })
        st.dataframe(pd.DataFrame(comparison_data), width="stretch", hide_index=True)


# ==================== COMPETITIVE INTEL PAGE ====================
def show_competitive_intel():
    st.markdown("<h2>🎯 COMPETITIVE INTELLIGENCE CENTER</h2>", unsafe_allow_html=True)
    has_loadings = not st.session_state.get('omc_df', pd.DataFrame()).empty
    if not has_loadings:
        st.warning("⚠️ OMC Loadings data required."); return
    loadings_df = st.session_state.omc_df
    tab1, tab2, tab3 = st.tabs(["🚨 Anomaly Detection", "💰 Price Intelligence", "⭐ Performance Score & Rankings"])
    with tab1:
        st.markdown("### 🚨 ANOMALY DETECTION ENGINE")
        mean_vol = loadings_df['Quantity'].mean()
        std_vol  = loadings_df['Quantity'].std()
        threshold = mean_vol + (2 * std_vol)
        anomalies = loadings_df[loadings_df['Quantity'] > threshold]
        col1, col2, col3 = st.columns(3)
        with col1: st.metric("Volume Anomalies", len(anomalies))
        with col2: st.metric("Anomalous Volume",  f"{anomalies['Quantity'].sum():,.0f} LT")
        with col3: st.metric("Threshold",          f"{threshold:,.0f} LT")
        if not anomalies.empty:
            st.warning(f"🚨 {len(anomalies)} abnormally large orders detected!")
            st.dataframe(anomalies.nlargest(10, 'Quantity')[['Date','BDC','OMC','Product','Quantity','Order Number']], width="stretch", hide_index=True)
        price_data = []
        for product in ['PREMIUM', 'GASOIL', 'LPG']:
            pdf = loadings_df[loadings_df['Product'] == product]
            if len(pdf) > 0:
                pm, ps = pdf['Price'].mean(), pdf['Price'].std()
                price_data.append({'Product': product, 'Avg Price': f"₵{pm:.2f}",
                                   'High Price Anomalies': len(pdf[pdf['Price'] > pm + 2*ps]),
                                   'Low Price Anomalies':  len(pdf[pdf['Price'] < pm - 2*ps])})
        st.dataframe(pd.DataFrame(price_data), width="stretch", hide_index=True)
    with tab2:
        st.markdown("### 💰 PRICE INTELLIGENCE DASHBOARD")
        price_stats = loadings_df.groupby(['BDC', 'Product'])['Price'].agg(['mean','min','max']).reset_index()
        price_stats.columns = ['BDC', 'Product', 'Avg Price', 'Min Price', 'Max Price']
        overall_mean = loadings_df['Price'].mean()
        price_stats['Tier'] = price_stats['Avg Price'].apply(lambda x: '🔴 Premium' if x > overall_mean * 1.1 else '🟢 Competitive')
        st.dataframe(price_stats.sort_values('Avg Price', ascending=False), width="stretch", hide_index=True)
    with tab3:
        st.markdown("### ⭐ BDC PERFORMANCE LEADERBOARD")
        scores = []
        max_vol    = loadings_df.groupby('BDC')['Quantity'].sum().max()
        max_orders = loadings_df.groupby('BDC').size().max()
        for bdc in loadings_df['BDC'].unique():
            bdc_df = loadings_df[loadings_df['BDC'] == bdc]
            vol_s  = (bdc_df['Quantity'].sum() / max_vol) * 40
            ord_s  = (len(bdc_df) / max_orders) * 30
            div_s  = (bdc_df['Product'].nunique() / 3) * 30
            total  = vol_s + ord_s + div_s
            grade  = 'A+' if total >= 90 else 'A' if total >= 80 else 'B' if total >= 70 else 'C' if total >= 60 else 'D'
            scores.append({'BDC': bdc, 'Volume Score': round(vol_s,1), 'Orders Score': round(ord_s,1),
                           'Diversity Score': round(div_s,1), 'Total Score': round(total,1), 'Grade': grade})
        scores_df = pd.DataFrame(scores).sort_values('Total Score', ascending=False)
        scores_df.insert(0, 'Rank', range(1, len(scores_df)+1))
        scores_df['Medal'] = scores_df['Rank'].apply(lambda x: '🥇' if x==1 else '🥈' if x==2 else '🥉' if x==3 else '')
        st.dataframe(scores_df, width="stretch", hide_index=True)


# ==================== STOCK TRANSACTION PAGE ====================
def show_stock_transaction():
    st.markdown("<h2>📈 STOCK TRANSACTION ANALYZER</h2>", unsafe_allow_html=True)
    st.info("🔥 Track BDC transactions: Inflows, Outflows, Sales & Intelligent Stockout Forecasting")
    st.markdown("---")
    if 'stock_txn_df' not in st.session_state:
        st.session_state.stock_txn_df = pd.DataFrame()
    tab1, tab2 = st.tabs(["🔍 BDC Transaction Report", "📊 Stockout Analysis"])
    with tab1:
        st.markdown("### 🔍 BDC TRANSACTION REPORT")
        col1, col2 = st.columns(2)
        with col1:
            selected_bdc     = st.selectbox("Select BDC:",     sorted(BDC_MAP.keys()))
            selected_product = st.selectbox("Select Product:", PRODUCT_OPTIONS)
        with col2:
            selected_depot = st.selectbox("Select Depot:", sorted(DEPOT_MAP.keys()))
        col3, col4 = st.columns(2)
        with col3: start_date = st.date_input("Start Date:", value=datetime.now() - timedelta(days=30))
        with col4: end_date   = st.date_input("End Date:",   value=datetime.now())
        if st.button("📊 FETCH TRANSACTION REPORT", width="stretch"):
            with st.spinner("🔄 Fetching stock transaction data..."):
                bdc_id     = BDC_MAP[selected_bdc]
                depot_id   = DEPOT_MAP[selected_depot]
                product_id = STOCK_PRODUCT_MAP[selected_product]
                params = {
                    'lngProductId': product_id, 'lngBDCId': bdc_id, 'lngDepotId': depot_id,
                    'dtpStartDate': start_date.strftime('%m/%d/%Y'),
                    'dtpEndDate':   end_date.strftime('%m/%d/%Y'),
                    'lngUserId': NPA_CONFIG['USER_ID']
                }
                try:
                    import requests
                    response = requests.get(NPA_CONFIG['STOCK_TRANSACTION_URL'], params=params,
                                            headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'application/pdf'}, timeout=30)
                    response.raise_for_status()
                    if response.content[:4] != b'%PDF':
                        st.error("❌ Response is not a PDF"); st.code(response.text[:500])
                        st.session_state.stock_txn_df = pd.DataFrame()
                    else:
                        st.success(f"✅ PDF received ({len(response.content):,} bytes)")
                        records = _parse_stock_transaction_pdf(io.BytesIO(response.content))
                        if records:
                            st.session_state.stock_txn_df     = pd.DataFrame(records)
                            st.session_state.stock_txn_bdc    = selected_bdc
                            st.session_state.stock_txn_depot  = selected_depot
                            st.session_state.stock_txn_product = selected_product
                            st.success(f"✅ Extracted {len(records)} transactions!")
                        else:
                            st.warning("⚠️ No transactions found for this date range / selection.")
                            st.session_state.stock_txn_df = pd.DataFrame()
                except Exception as e:
                    st.error(f"❌ Error: {e}")
                    import traceback; st.code(traceback.format_exc())
                    st.session_state.stock_txn_df = pd.DataFrame()
        df = st.session_state.stock_txn_df
        if not df.empty:
            st.markdown("---")
            inflows    = df[df['Description'].isin(['Custody Transfer In', 'Product Outturn'])]['Volume'].sum()
            outflows   = df[df['Description'].isin(['Sale', 'Custody Transfer Out'])]['Volume'].sum()
            sales      = df[df['Description'] == 'Sale']['Volume'].sum()
            bdc_xfer   = df[df['Description'] == 'Custody Transfer Out']['Volume'].sum()
            final_bal  = df['Balance'].iloc[-1] if len(df) > 0 else 0
            cols = st.columns(5)
            with cols[0]: st.metric("📥 Inflows",       f"{inflows:,.0f} LT")
            with cols[1]: st.metric("📤 Outflows",      f"{outflows:,.0f} LT")
            with cols[2]: st.metric("💰 Sales to OMCs", f"{sales:,.0f} LT")
            with cols[3]: st.metric("🔄 BDC Transfers", f"{bdc_xfer:,.0f} LT")
            with cols[4]: st.metric("📊 Final Balance", f"{final_bal:,.0f} LT")
            txn_summary = df.groupby('Description').agg(Total_Volume=('Volume','sum'), Count=('Trans #','count')).reset_index()
            txn_summary.columns = ['Transaction Type', 'Total Volume (LT)', 'Count']
            st.dataframe(txn_summary.sort_values('Total Volume (LT)', ascending=False), width="stretch", hide_index=True)
            if sales > 0:
                st.markdown("### 🏢 Top Customers (OMC Sales)")
                cust = df[df['Description']=='Sale'].groupby('Account')['Volume'].sum().sort_values(ascending=False).head(10).reset_index()
                cust.columns = ['Customer', 'Volume Sold (LT)']
                st.dataframe(cust, width="stretch", hide_index=True)
            st.dataframe(df, width="stretch", hide_index=True, height=400)
        else:
            st.info("👆 Select options and click the button above to fetch transaction data")
    with tab2:
        st.markdown("### 📊 INTELLIGENT STOCKOUT FORECASTING")
        has_balance      = bool(st.session_state.get('bdc_records'))
        has_transactions = not st.session_state.stock_txn_df.empty
        col1, col2 = st.columns(2)
        with col1:
            if has_balance: st.success("✅ BDC Balance Data Available")
            else: st.warning("⚠️ BDC Balance Data Required")
        with col2:
            if has_transactions: st.success("✅ Transaction Data Available")
            else: st.warning("⚠️ Transaction Data Required")
        if has_balance and has_transactions:
            balance_df = pd.DataFrame(st.session_state.bdc_records)
            txn_df     = st.session_state.stock_txn_df
            bdc_name   = st.session_state.get('stock_txn_bdc', '')
            prod_disp  = st.session_state.get('stock_txn_product', '')
            prod_name  = PRODUCT_BALANCE_MAP.get(prod_disp, prod_disp)
            bdc_balance = balance_df[
                (balance_df['BDC'].str.contains(bdc_name, case=False, na=False)) &
                (balance_df['Product'].str.contains(prod_name, case=False, na=False))
            ]
            if not bdc_balance.empty:
                current_stock   = bdc_balance['ACTUAL BALANCE (LT\\KG)'].sum()
                total_sales     = txn_df[txn_df['Description'].isin(['Sale','Custody Transfer Out'])]['Volume'].sum()
                txn_copy        = txn_df.copy()
                txn_copy['_dt'] = pd.to_datetime(txn_copy['Date'], format='%d/%m/%Y', errors='coerce')
                date_range_days = max((txn_copy['_dt'].max() - txn_copy['_dt'].min()).days, 1)
                daily_rate      = total_sales / date_range_days if date_range_days > 0 else 0
                days_remaining  = (current_stock / daily_rate) if daily_rate > 0 else float('inf')
                if   days_remaining < 7:  status, sc = "🔴 CRITICAL", "red"
                elif days_remaining < 14: status, sc = "🟡 WARNING", "orange"
                else:                     status, sc = "🟢 HEALTHY", "green"
                st.markdown(f"### {status}")
                cols = st.columns(4)
                with cols[0]: st.markdown(f"<div class='metric-card'><h2>CURRENT STOCK</h2><h1>{current_stock:,.0f}</h1></div>", unsafe_allow_html=True)
                with cols[1]: st.markdown(f"<div class='metric-card'><h2>DAILY RATE</h2><h1>{daily_rate:,.0f}</h1></div>", unsafe_allow_html=True)
                with cols[2]:
                    days_text = f"{days_remaining:.1f}" if days_remaining != float('inf') else "∞"
                    st.markdown(f"<div class='metric-card' style='border-color:{sc};'><h2>DAYS LEFT</h2><h1>{days_text}</h1></div>", unsafe_allow_html=True)
                with cols[3]: st.markdown(f"<div class='metric-card'><h2>PERIOD</h2><h1>{date_range_days}</h1><p style='color:#888;font-size:14px;margin:0;'>days</p></div>", unsafe_allow_html=True)
            else:
                st.warning(f"⚠️ No balance data found for {bdc_name} — {prod_name}")


def _parse_stock_transaction_pdf(pdf_file):
    DESCRIPTIONS = sorted(['Balance b/fwd','Stock Take','Sale','Custody Transfer In','Custody Transfer Out','Product Outturn'], key=len, reverse=True)
    SKIP_PREFIXES = ('national petroleum authority','stock transaction report','bdc :','depot :','product :',
                     'printed by','printed on','date trans #','actual stock balance','stock commitments',
                     'available stock balance','last stock update','i.t.s from')
    def _should_skip(line):
        lo = line.strip().lower()
        if lo.startswith(SKIP_PREFIXES): return True
        if re.match(r'^\d{1,2}\s+\w+,\s+\d{4}', line.strip()): return True
        return False
    def _parse_num(s):
        s = s.strip()
        neg = s.startswith('(') and s.endswith(')')
        try: return -int(s.strip('()').replace(',','')) if neg else int(s.replace(',',''))
        except: return None
    def _parse_line(line):
        line = line.strip()
        if not re.match(r'^\d{2}/\d{2}/\d{4}\b', line): return None
        parts = line.split()
        date = parts[0]; trans = parts[1] if len(parts) > 1 else ''
        rest = line[len(date):].strip()[len(trans):].strip()
        description = None; after_desc = rest
        for desc in DESCRIPTIONS:
            if rest.lower().startswith(desc.lower()):
                description = desc; after_desc = rest[len(desc):].strip(); break
        if description is None or description == 'Balance b/fwd': return None
        nums = re.findall(r'\([\d,]+\)|[\d,]+', after_desc)
        if len(nums) < 2: return None
        volume = _parse_num(nums[-2]); balance = _parse_num(nums[-1])
        trail = re.search(re.escape(nums[-2]) + r'\s+' + re.escape(nums[-1]) + r'\s*$', after_desc)
        account = after_desc[:trail.start()].strip() if trail else ' '.join(after_desc.split()[:-2])
        return {'Date': date, 'Trans #': trans, 'Description': description, 'Account': account,
                'Volume': volume if volume is not None else 0, 'Balance': balance if balance is not None else 0}
    records = []
    try:
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text: continue
                for raw in text.split('\n'):
                    line = raw.strip()
                    if not line or _should_skip(line): continue
                    row = _parse_line(line)
                    if row: records.append(row)
    except Exception as e:
        st.error(f"PDF parse error: {e}")
    return records


# ==================== BDC INTELLIGENCE PAGE ====================
def show_bdc_intelligence():
    st.markdown("<h2>🧠 BDC INTELLIGENCE CENTER</h2>", unsafe_allow_html=True)
    st.info("🎯 Predictive analytics combining stock balance and loading patterns")
    st.markdown("---")
    has_balance  = bool(st.session_state.get('bdc_records'))
    has_loadings = not st.session_state.get('omc_df', pd.DataFrame()).empty
    if not has_balance or not has_loadings:
        col1, col2 = st.columns(2)
        with col1:
            if not has_balance:
                st.warning("⚠️ BDC Balance Data Missing")
                if st.button("🔄 FETCH BDC BALANCE", width="stretch", key='auto_fetch_balance'):
                    with st.spinner("🔄 Fetching..."):
                        scraper = StockBalanceScraper()
                        params = {'lngCompanyId': NPA_CONFIG['COMPANY_ID'], 'strITSfromPersol': NPA_CONFIG['ITS_FROM_PERSOL'],
                                  'strGroupBy': 'BDC', 'strGroupBy1': 'DEPOT', 'strQuery1': '', 'strQuery2': '', 'strQuery3': '', 'strQuery4': '',
                                  'strPicHeight': '1', 'szPicWeight': '1', 'lngUserId': NPA_CONFIG['USER_ID'], 'intAppId': NPA_CONFIG['APP_ID']}
                        try:
                            import requests
                            r = requests.get(NPA_CONFIG['BDC_BALANCE_URL'], params=params, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
                            r.raise_for_status()
                            if r.content[:4] == b'%PDF':
                                st.session_state.bdc_records = scraper.parse_pdf_file(io.BytesIO(r.content))
                                if st.session_state.bdc_records:
                                    st.success(f"✅ {len(st.session_state.bdc_records)} records!"); st.rerun()
                        except Exception as e: st.error(f"❌ {e}")
            else: st.success(f"✅ BDC Balance: {len(st.session_state.bdc_records)} records")
        with col2:
            if not has_loadings:
                st.warning("⚠️ OMC Loadings Data Missing")
                default_start = datetime.now() - timedelta(days=30)
                start_date = st.date_input("From", value=default_start, key='intel_start_date')
                end_date   = st.date_input("To",   value=datetime.now(), key='intel_end_date')
                if st.button("🔄 FETCH OMC LOADINGS", width="stretch", key='auto_fetch_loadings'):
                    with st.spinner("🔄 Fetching..."):
                        params = {'lngCompanyId': NPA_CONFIG['COMPANY_ID'], 'szITSfromPersol': 'persol', 'strGroupBy': 'BDC',
                                  'strGroupBy1': NPA_CONFIG['OMC_NAME'], 'strQuery1': ' and iorderstatus=4',
                                  'strQuery2': start_date.strftime("%m/%d/%Y"), 'strQuery3': end_date.strftime("%m/%d/%Y"),
                                  'strQuery4': '', 'strPicHeight': '', 'strPicWeight': '', 'intPeriodID': '4',
                                  'iUserId': NPA_CONFIG['USER_ID'], 'iAppId': NPA_CONFIG['APP_ID']}
                        try:
                            import requests
                            r = requests.get(NPA_CONFIG['OMC_LOADINGS_URL'], params=params, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
                            r.raise_for_status()
                            if r.content[:4] == b'%PDF':
                                st.session_state.omc_df = extract_npa_data_from_pdf(io.BytesIO(r.content))
                                if not st.session_state.omc_df.empty:
                                    st.success(f"✅ {len(st.session_state.omc_df)} records!"); st.rerun()
                        except Exception as e: st.error(f"❌ {e}")
            else: st.success(f"✅ OMC Loadings: {len(st.session_state.omc_df)} records")
        if not (bool(st.session_state.get('bdc_records')) and not st.session_state.get('omc_df', pd.DataFrame()).empty):
            st.info("👆 Click the buttons above to fetch required data."); return
    balance_df  = pd.DataFrame(st.session_state.bdc_records)
    loadings_df = st.session_state.omc_df
    all_bdcs = sorted(set(balance_df['BDC'].unique()) | set(loadings_df['BDC'].unique()))
    selected_bdc = st.selectbox("Choose BDC:", all_bdcs, key='intel_bdc_select')
    if not selected_bdc: return
    tab1, tab2, tab3 = st.tabs(["📊 Overview", "⏱️ Stockout Prediction", "📉 Consumption Analysis"])
    with tab1:
        bdc_balance = balance_df[balance_df['BDC'] == selected_bdc]
        if not bdc_balance.empty:
            col_name = 'ACTUAL BALANCE (LT\\KG)'
            product_stocks = bdc_balance.groupby('Product')[col_name].sum()
            cols = st.columns(3)
            for idx, (product, stock) in enumerate(product_stocks.items()):
                with cols[idx % 3]:
                    st.markdown(f"<div class='metric-card'><h2>{product}</h2><h1>{stock:,.0f}</h1><p style='color:#888;font-size:14px;margin:0;'>LT/KG</p></div>", unsafe_allow_html=True)
        bdc_loadings = loadings_df[loadings_df['BDC'] == selected_bdc]
        if not bdc_loadings.empty:
            cols = st.columns(4)
            with cols[0]: st.metric("Total Orders", f"{len(bdc_loadings):,}")
            with cols[1]: st.metric("Total Volume",  f"{bdc_loadings['Quantity'].sum():,.0f} LT")
            with cols[2]: st.metric("Unique OMCs",   f"{bdc_loadings['OMC'].nunique()}")
            with cols[3]: st.metric("Avg Order",     f"{bdc_loadings['Quantity'].mean():,.0f} LT")
    with tab2:
        bdc_balance  = balance_df[balance_df['BDC'] == selected_bdc]
        bdc_loadings = loadings_df[loadings_df['BDC'] == selected_bdc]
        if bdc_balance.empty or bdc_loadings.empty:
            st.warning("⚠️ Insufficient data for prediction."); return
        ldf = bdc_loadings.copy()
        ldf['Date'] = pd.to_datetime(ldf['Date'], errors='coerce')
        ldf = ldf.dropna(subset=['Date'])
        if ldf.empty: st.warning("⚠️ No valid dates."); return
        date_range = max((ldf['Date'].max() - ldf['Date'].min()).days, 1)
        daily_consumption = ldf.groupby('Product')['Quantity'].sum() / date_range
        col_name = 'ACTUAL BALANCE (LT\\KG)'
        current_stock = bdc_balance.groupby('Product')[col_name].sum()
        for product in current_stock.index:
            stock = current_stock[product]; rate = daily_consumption.get(product, 0)
            if rate > 0:
                days = stock / rate
                color = "#ff0000" if days < 7 else "#ffaa00" if days < 14 else "#00ff88"
                st.markdown(f"""
                <div style='background:rgba(22,33,62,0.6);padding:20px;border-radius:10px;border:2px solid {color};margin:10px 0;'>
                    <h3 style='color:{color};margin:0;'>{product}</h3>
                    <div style='display:grid;grid-template-columns:1fr 1fr 1fr;gap:20px;margin-top:15px;'>
                        <div><p style='color:#888;margin:0;font-size:14px;'>Stock</p><p style='color:#00ffff;margin:5px 0;font-size:24px;font-weight:bold;'>{stock:,.0f} LT</p></div>
                        <div><p style='color:#888;margin:0;font-size:14px;'>Daily Usage</p><p style='color:#ff00ff;margin:5px 0;font-size:24px;font-weight:bold;'>{rate:,.0f} LT</p></div>
                        <div><p style='color:#888;margin:0;font-size:14px;'>Days Left</p><p style='color:{color};margin:5px 0;font-size:32px;font-weight:bold;'>{days:.1f}</p></div>
                    </div>
                </div>""", unsafe_allow_html=True)
    with tab3:
        bdc_loadings = loadings_df[loadings_df['BDC'] == selected_bdc]
        if bdc_loadings.empty: st.warning("⚠️ No loading data."); return
        ts_df = bdc_loadings.copy()
        ts_df['Date'] = pd.to_datetime(ts_df['Date'], errors='coerce')
        ts_df = ts_df.dropna(subset=['Date'])
        if ts_df.empty: st.warning("⚠️ No valid dates."); return
        daily = ts_df.groupby([ts_df['Date'].dt.date, 'Product'])['Quantity'].sum().reset_index()
        daily.columns = ['Date', 'Product', 'Volume']
        for product in daily['Product'].unique():
            pdata = daily[daily['Product'] == product]
            if not pdata.empty:
                st.markdown(f"**{product}**")
                st.line_chart(pdata.set_index('Date')['Volume'], width="stretch")
        stats = ts_df.groupby('Product')['Quantity'].agg([('Total','sum'),('Average','mean'),('Median','median'),('Min','min'),('Max','max'),('Std Dev','std')]).reset_index()
        st.dataframe(stats, width="stretch", hide_index=True)


# ==================== NATIONAL STOCKOUT PAGE (v2) ====================
# v2 Architecture: 3-step fetch — ~150 API calls vs 6000+ in v1
#   Step 1: BDC Balance (1 call)  → national stock per product
#   Step 2: OMC Loadings (1 call) → all BDC→OMC sales
#   Step 3: Stock Transactions (N_BDCs × 3 products, depot_id=0) → Custody Transfer Out only

def _ns_get_pdf(url: str, params: dict, timeout: int = 45) -> bytes | None:
    """GET a URL, return raw bytes if PDF else None."""
    import requests
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/pdf,text/html,*/*;q=0.8',
    }
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.content if r.content[:4] == b'%PDF' else None
    except Exception:
        return None


def _ns_parse_txn_bytes_cto_only(pdf_bytes: bytes) -> list[dict]:
    """
    Parse a stock-transaction PDF (bytes).
    Returns ONLY 'Custody Transfer Out' rows.
    """
    DESCRIPTIONS = sorted([
        'Balance b/fwd', 'Stock Take', 'Sale',
        'Custody Transfer In', 'Custody Transfer Out', 'Product Outturn',
    ], key=len, reverse=True)
    SKIP_PREFIXES = (
        'national petroleum authority', 'stock transaction report',
        'bdc :', 'depot :', 'product :', 'printed by', 'printed on',
        'date trans #', 'actual stock balance', 'stock commitments',
        'available stock balance', 'last stock update', 'i.t.s from',
    )

    def _should_skip(line: str) -> bool:
        lo = line.strip().lower()
        if lo.startswith(SKIP_PREFIXES): return True
        if re.match(r'^\d{1,2}\s+\w+,\s+\d{4}', line.strip()): return True
        return False

    def _parse_num(s: str):
        s = s.strip()
        neg = s.startswith('(') and s.endswith(')')
        try: return -int(s.strip('()').replace(',', '')) if neg else int(s.replace(',', ''))
        except: return None

    def _parse_line(line: str):
        line = line.strip()
        if not re.match(r'^\d{2}/\d{2}/\d{4}\b', line): return None
        parts = line.split()
        date  = parts[0]
        trans = parts[1] if len(parts) > 1 else ''
        rest  = line[len(date):].strip()[len(trans):].strip()
        description = None; after_desc = rest
        for desc in DESCRIPTIONS:
            if rest.lower().startswith(desc.lower()):
                description = desc; after_desc = rest[len(desc):].strip(); break
        if description is None or description != 'Custody Transfer Out': return None  # ← CTO only
        nums = re.findall(r'\([\d,]+\)|[\d,]+', after_desc)
        if len(nums) < 2: return None
        volume  = _parse_num(nums[-2])
        balance = _parse_num(nums[-1])
        trail   = re.search(re.escape(nums[-2]) + r'\s+' + re.escape(nums[-1]) + r'\s*$', after_desc)
        account = after_desc[:trail.start()].strip() if trail else ' '.join(after_desc.split()[:-2])
        return {
            'Date': date, 'Trans #': trans, 'Description': description,
            'Account': account,
            'Volume':  volume  if volume  is not None else 0,
            'Balance': balance if balance is not None else 0,
        }

    records = []
    if not pdf_bytes: return records
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text: continue
                for raw in text.split('\n'):
                    line = raw.strip()
                    if not line or _should_skip(line): continue
                    row = _parse_line(line)
                    if row: records.append(row)
    except Exception:
        pass
    return records


def _ns_fetch_cto_for_bdc(bdc_id: int, product_id: int, product_key: str,
                            start_str: str, end_str: str) -> list[dict]:
    """
    Fetch Custody Transfer Out rows for one BDC × product.
    Uses depot_id=0 to fetch all depots in a single call.
    """
    params = {
        'lngProductId': product_id,
        'lngBDCId':     bdc_id,
        'lngDepotId':   0,          # ← 0 = all depots (v2 key optimisation)
        'dtpStartDate': start_str,
        'dtpEndDate':   end_str,
        'lngUserId':    NPA_CONFIG['USER_ID'],
    }
    pdf_bytes = _ns_get_pdf(NPA_CONFIG['STOCK_TRANSACTION_URL'], params)
    rows = _ns_parse_txn_bytes_cto_only(pdf_bytes) if pdf_bytes else []
    for r in rows:
        r['product_key'] = product_key
        r['bdc_id']      = bdc_id
    return rows


def show_national_stockout():
    """
    🌍 National Stockout Forecast — v2 (3-step, ~150 API calls)

    Step 1: BDC Balance         (1 call)         → national stock
    Step 2: OMC Loadings        (1 call)         → BDC→OMC sales
    Step 3: Stock Transactions  (N_BDCs × 3)    → Custody Transfer Out only, depot_id=0
    """
    st.markdown("<h2>🌍 NATIONAL STOCKOUT FORECAST</h2>", unsafe_allow_html=True)
    st.info(
        "🛢️ Industry-wide analysis: Total national stock balance vs total daily depletion "
        "(OMC sales + BDC-to-BDC transfers) — **v2: ~150 API calls, runs in 15–40 seconds.**"
    )
    st.markdown("---")

    # ── 1. Date pickers ───────────────────────────────────────────────────────
    st.markdown("### 📅 SELECT ANALYSIS PERIOD")
    st.caption("Transactions in this window compute the avg daily depletion rate. 30 days recommended.")
    col1, col2 = st.columns(2)
    with col1: start_date = st.date_input("From", value=datetime.now() - timedelta(days=30), key='ns_start')
    with col2: end_date   = st.date_input("To",   value=datetime.now(),                      key='ns_end')
    start_str  = start_date.strftime("%m/%d/%Y")
    end_str    = end_date.strftime("%m/%d/%Y")
    period_days = max((end_date - start_date).days, 1)

    n_bdcs    = len(BDC_MAP)
    n_prods   = 3
    est_calls = 2 + n_bdcs * n_prods   # balance + OMC + BDC×product
    st.caption(f"📡 Estimated API calls: **{est_calls}** (1 Balance + 1 OMC + {n_bdcs} BDCs × {n_prods} products)")
    st.markdown("---")

    # ── 2. Fetch button ───────────────────────────────────────────────────────
    if st.button(f"⚡ FETCH & ANALYSE NATIONAL FUEL SUPPLY ({est_calls} calls)", use_container_width=True):
        _ns_run(start_str, end_str, period_days)

    # ── 3. Show cached results ─────────────────────────────────────────────────
    if st.session_state.get('ns_results'):
        _ns_display()


def _ns_run(start_str: str, end_str: str, period_days: int):
    """Orchestrate the 3-step v2 data fetch and cache results."""
    cfg = NPA_CONFIG

    # ── Step 1: BDC Balance ────────────────────────────────────────────────────
    with st.status("📡 Step 1/3 — Fetching BDC Balance…", expanded=True) as status:
        bal_params = {
            'lngCompanyId': cfg['COMPANY_ID'], 'strITSfromPersol': cfg['ITS_FROM_PERSOL'],
            'strGroupBy': 'BDC', 'strGroupBy1': 'DEPOT',
            'strQuery1': '', 'strQuery2': '', 'strQuery3': '', 'strQuery4': '',
            'strPicHeight': '1', 'szPicWeight': '1',
            'lngUserId': cfg['USER_ID'], 'intAppId': cfg['APP_ID'],
        }
        bal_bytes = _ns_get_pdf(cfg['BDC_BALANCE_URL'], bal_params)
        if not bal_bytes:
            st.error("❌ Could not fetch BDC Balance PDF.")
            status.update(label="❌ Balance fetch failed", state="error"); return
        scraper    = StockBalanceScraper()
        bal_records = scraper.parse_pdf_file(io.BytesIO(bal_bytes))
        if not bal_records:
            st.error("❌ No balance records in PDF.")
            status.update(label="❌ No balance data", state="error"); return
        bal_df  = pd.DataFrame(bal_records)
        n_bdcs  = bal_df['BDC'].nunique()
        st.write(f"✅ Balance: {len(bal_df)} rows | {n_bdcs} BDCs")
        status.update(label=f"✅ Step 1 done — {len(bal_df)} balance rows", state="running")

    # ── Step 2: OMC Loadings ───────────────────────────────────────────────────
    with st.status("📡 Step 2/3 — Fetching OMC Loadings…", expanded=True) as status:
        omc_params = {
            'lngCompanyId': cfg['COMPANY_ID'], 'szITSfromPersol': 'persol',
            'strGroupBy': 'BDC', 'strGroupBy1': cfg['OMC_NAME'],
            'strQuery1': ' and iorderstatus=4',
            'strQuery2': start_str, 'strQuery3': end_str, 'strQuery4': '',
            'strPicHeight': '', 'strPicWeight': '', 'intPeriodID': '4',
            'iUserId': cfg['USER_ID'], 'iAppId': cfg['APP_ID'],
        }
        omc_bytes = _ns_get_pdf(cfg['OMC_LOADINGS_URL'], omc_params)
        if omc_bytes:
            omc_df = extract_npa_data_from_pdf(io.BytesIO(omc_bytes))
            st.write(f"✅ OMC Loadings: {len(omc_df)} rows")
        else:
            omc_df = pd.DataFrame(columns=['Product', 'Quantity', 'BDC'])
            st.warning("⚠️ OMC Loadings PDF unavailable — will use 0 for OMC sales")
        status.update(label=f"✅ Step 2 done — {len(omc_df)} OMC loading rows", state="running")

    # ── Step 3: Custody Transfer Out (BDC × product, depot_id=0) ─────────────
    products_to_fetch = {
        "PMS":    int(STOCK_PRODUCT_MAP["PMS"]),
        "Gasoil": int(STOCK_PRODUCT_MAP["Gasoil"]),
        "LPG":    int(STOCK_PRODUCT_MAP["LPG"]),
    }
    all_bdc_ids = list(BDC_MAP.values())
    jobs = [(bdc_id, prod_id, prod_key)
            for prod_key, prod_id in products_to_fetch.items()
            for bdc_id in all_bdc_ids]
    total_jobs = len(jobs)

    st.markdown(f"### 🔄 Step 3/3 — Fetching Custody Transfers")
    st.caption(f"{len(all_bdc_ids)} BDCs × {len(products_to_fetch)} products = **{total_jobs} calls** (depot_id=0)")
    progress_bar  = st.progress(0, text="Starting…")
    status_text   = st.empty()

    all_cto_rows: list[dict] = []
    completed = errors = nodata = 0
    MAX_WORKERS = 10

    def _job(args):
        bdc_id, prod_id, prod_key = args
        return _ns_fetch_cto_for_bdc(bdc_id, prod_id, prod_key, start_str, end_str)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(_job, j): j for j in jobs}
        for future in concurrent.futures.as_completed(future_map):
            completed += 1
            try:
                rows = future.result()
                if rows: all_cto_rows.extend(rows)
                else:    nodata += 1
            except Exception:
                errors += 1
            pct = completed / total_jobs
            progress_bar.progress(pct, text=f"Fetched {completed}/{total_jobs} | CTO rows: {len(all_cto_rows)}")
            if completed % 10 == 0 or completed == total_jobs:
                status_text.info(f"🔄 {completed}/{total_jobs} | ✅ rows: {len(all_cto_rows)} | ⬜ empty: {nodata} | ❌ errors: {errors}")

    progress_bar.progress(1.0, text="✅ All calls complete")

    # ── Aggregate ─────────────────────────────────────────────────────────────
    PROD_DISPLAY = {"PMS": "PREMIUM (PMS)", "Gasoil": "GASOIL (AGO)", "LPG": "LPG"}
    PROD_BALANCE = {"PMS": "PREMIUM",       "Gasoil": "GASOIL",       "LPG": "LPG"}
    BAL_TO_KEY   = {v: k for k, v in PROD_BALANCE.items()}

    col_bal = 'ACTUAL BALANCE (LT\\KG)'
    national_stock = (
        bal_df.groupby('Product')[col_bal].sum()
        .rename_axis('bal_product').reset_index(name='total_balance')
    )
    national_stock['product_key'] = national_stock['bal_product'].map(BAL_TO_KEY)
    national_stock = national_stock.dropna(subset=['product_key'])

    # OMC sales per product
    if not omc_df.empty and 'Quantity' in omc_df.columns:
        omc_sales = (
            omc_df.groupby('Product')['Quantity'].sum()
            .reset_index()
        )
        omc_sales['product_key'] = omc_sales['Product'].map(BAL_TO_KEY)
        omc_sales = omc_sales.dropna(subset=['product_key']).rename(columns={'Quantity': 'omc_volume'})
        omc_map = dict(zip(omc_sales['product_key'], omc_sales['omc_volume']))
    else:
        omc_map = {}

    # CTO per product
    if all_cto_rows:
        cto_df  = pd.DataFrame(all_cto_rows)
        cto_map = dict(cto_df.groupby('product_key')['Volume'].sum())
    else:
        cto_df  = pd.DataFrame()
        cto_map = {}

    # Build forecast df
    rows = []
    for prod_key, prod_display in PROD_DISPLAY.items():
        stock    = national_stock[national_stock['product_key'] == prod_key]['total_balance'].sum()
        omc_vol  = omc_map.get(prod_key, 0)
        cto_vol  = cto_map.get(prod_key, 0)
        total_dep = omc_vol + cto_vol
        daily     = total_dep / period_days if period_days > 0 else 0
        days_rem  = (stock / daily) if daily > 0 else float('inf')
        rows.append({
            'product_key':    prod_key,
            'display_name':   prod_display,
            'total_balance':  stock,
            'omc_volume':     omc_vol,
            'cto_volume':     cto_vol,
            'total_depletion': total_dep,
            'daily_rate':     daily,
            'days_remaining': days_rem,
        })
    forecast_df = pd.DataFrame(rows)

    st.session_state.ns_results = {
        'forecast_df': forecast_df,
        'bal_df':      bal_df,
        'omc_df':      omc_df,
        'cto_df':      cto_df,
        'period_days': period_days,
        'start_str':   start_str,
        'end_str':     end_str,
        'n_bdcs':      n_bdcs,
    }
    st.success("✅ Analysis complete! Scroll down to see results.")
    st.rerun()


def _ns_display():
    """Render the cached v2 National Stockout results."""
    res         = st.session_state.ns_results
    forecast_df = res['forecast_df']
    bal_df      = res['bal_df']
    omc_df      = res['omc_df']
    cto_df      = res['cto_df']
    period_days = res['period_days']
    start_str   = res['start_str']
    end_str     = res['end_str']

    st.markdown("---")
    st.markdown(
        f"<h3>🇬🇭 GHANA NATIONAL FUEL SUPPLY — {start_str} → {end_str} ({period_days} days)</h3>",
        unsafe_allow_html=True
    )

    PRODUCT_ICONS  = {"PMS": "⛽", "Gasoil": "🚛", "LPG": "🔵"}
    PRODUCT_COLORS = {"PMS": "#00ffff", "Gasoil": "#ffaa00", "LPG": "#00ff88"}

    # ── KPI cards ─────────────────────────────────────────────────────────────
    st.markdown("### 🛢️ NATIONAL STOCKOUT FORECAST BY PRODUCT")
    cols = st.columns(len(forecast_df))
    for col, (_, row) in zip(cols, forecast_df.iterrows()):
        days     = row['days_remaining']
        prod_key = row['product_key']
        color    = PRODUCT_COLORS.get(prod_key, "#ffffff")
        if days == float('inf'):
            days_text, status_label, border = "∞", "🔵 NO DATA", "#888888"
        elif days < 7:
            days_text, status_label, border = f"{days:.1f}", "🔴 CRITICAL", "#ff0000"
        elif days < 14:
            days_text, status_label, border = f"{days:.1f}", "🟡 WARNING", "#ffaa00"
        elif days < 30:
            days_text, status_label, border = f"{days:.1f}", "🟠 MONITOR", "#ff6600"
        else:
            days_text, status_label, border = f"{days:.1f}", "🟢 HEALTHY", "#00ff88"
        empty_date = (
            (datetime.now() + timedelta(days=days)).strftime('%d %b %Y')
            if days != float('inf') else "N/A"
        )
        omc_pct = (row['omc_volume'] / row['total_depletion'] * 100) if row['total_depletion'] > 0 else 0
        cto_pct = (row['cto_volume'] / row['total_depletion'] * 100) if row['total_depletion'] > 0 else 0
        with col:
            st.markdown(f"""
            <div style='background:rgba(10,14,39,0.85);padding:24px 16px;border-radius:16px;
                        border:2.5px solid {border};text-align:center;margin-bottom:8px;
                        box-shadow:0 0 18px {border}55;'>
                <div style='font-size:36px;margin-bottom:4px;'>{PRODUCT_ICONS.get(prod_key,"🛢️")}</div>
                <div style='font-family:Orbitron,sans-serif;font-size:18px;color:{color};font-weight:700;letter-spacing:2px;'>
                    {row["display_name"]}
                </div>
                <div style='margin:16px 0 8px;'>
                    <div style='color:#888;font-size:11px;text-transform:uppercase;letter-spacing:1px;'>Days of Supply Left</div>
                    <div style='font-family:Orbitron,sans-serif;font-size:48px;color:{border};font-weight:900;line-height:1.1;'>
                        {days_text}
                    </div>
                    <div style='color:{border};font-size:14px;font-weight:700;margin-top:4px;'>{status_label}</div>
                </div>
                <div style='border-top:1px solid rgba(255,255,255,0.08);padding-top:12px;margin-top:12px;'>
                    <table style='width:100%;font-family:Rajdhani,sans-serif;font-size:12px;border-collapse:collapse;'>
                        <tr><td style='color:#888;text-align:left;padding:2px 0;'>📦 Stock</td>
                            <td style='color:#e0e0e0;text-align:right;padding:2px 0;font-weight:600;'>{row["total_balance"]:,.0f} LT</td></tr>
                        <tr><td style='color:#888;text-align:left;padding:2px 0;'>📉 Daily Rate</td>
                            <td style='color:#e0e0e0;text-align:right;padding:2px 0;font-weight:600;'>{row["daily_rate"]:,.0f} LT/day</td></tr>
                        <tr><td style='color:#888;text-align:left;padding:2px 0;'>⛽ OMC Sales</td>
                            <td style='color:#e0e0e0;text-align:right;padding:2px 0;font-weight:600;'>{row["omc_volume"]:,.0f} LT ({omc_pct:.0f}%)</td></tr>
                        <tr><td style='color:#888;text-align:left;padding:2px 0;'>🔄 BDC Transfer</td>
                            <td style='color:#e0e0e0;text-align:right;padding:2px 0;font-weight:600;'>{row["cto_volume"]:,.0f} LT ({cto_pct:.0f}%)</td></tr>
                        <tr><td style='color:#888;text-align:left;padding:2px 0;'>🗓️ Est. Empty</td>
                            <td style='color:{border};text-align:right;padding:2px 0;font-weight:700;'>{empty_date}</td></tr>
                    </table>
                </div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    # ── Summary table ─────────────────────────────────────────────────────────
    st.markdown("### 📊 DETAILED NATIONAL SUMMARY TABLE")
    summary_rows = []
    for _, row in forecast_df.iterrows():
        days = row['days_remaining']
        if   days == float('inf'): status = "No Data"
        elif days < 7:             status = "🔴 CRITICAL"
        elif days < 14:            status = "🟡 WARNING"
        elif days < 30:            status = "🟠 MONITOR"
        else:                      status = "🟢 HEALTHY"
        empty = (datetime.now() + timedelta(days=days)).strftime('%Y-%m-%d') if days != float('inf') else "N/A"
        summary_rows.append({
            'Product':                    row['display_name'],
            'National Stock (LT)':        f"{row['total_balance']:,.0f}",
            f'OMC Sales ({period_days}d LT)': f"{row['omc_volume']:,.0f}",
            f'BDC Transfers ({period_days}d LT)': f"{row['cto_volume']:,.0f}",
            f'Total Depletion ({period_days}d LT)': f"{row['total_depletion']:,.0f}",
            'Avg Daily Depletion (LT)':   f"{row['daily_rate']:,.0f}",
            'Days of Supply':             f"{days:.1f}" if days != float('inf') else "∞",
            'Projected Empty':            empty,
            'Status':                     status,
        })
    st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)
    st.markdown("---")

    # ── Sales mix ─────────────────────────────────────────────────────────────
    st.markdown("### 🔍 DEPLETION MIX: OMC Sales vs BDC Transfers")
    st.caption("OMC Sales = BDC→OMC loadings | BDC Transfers = Custody Transfer Out (BDC→BDC)")
    if cto_df.empty:
        st.info("ℹ️ No Custody Transfer Out data found (depot_id=0 may not be supported). "
                "Forecast uses OMC Sales only — still accurate for most scenarios.")
    mix_rows = []
    for _, row in forecast_df.iterrows():
        dep = row['total_depletion']
        mix_rows.append({
            'Product':       row['display_name'],
            'OMC Sales (LT)':     f"{row['omc_volume']:,.0f}",
            'BDC Transfers (LT)': f"{row['cto_volume']:,.0f}",
            'Total Depletion (LT)': f"{dep:,.0f}",
            'OMC %':   f"{(row['omc_volume']/dep*100) if dep>0 else 0:.1f}%",
            'BDC Transfer %': f"{(row['cto_volume']/dep*100) if dep>0 else 0:.1f}%",
        })
    st.dataframe(pd.DataFrame(mix_rows), use_container_width=True, hide_index=True)
    st.markdown("---")

    # ── BDC stock breakdown ────────────────────────────────────────────────────
    st.markdown("### 🏦 CURRENT NATIONAL STOCK BY BDC")
    col_bal = 'ACTUAL BALANCE (LT\\KG)'
    bdc_pivot = (
        bal_df.groupby(['BDC', 'Product'])[col_bal].sum().reset_index()
        .pivot_table(index='BDC', columns='Product', values=col_bal, aggfunc='sum', fill_value=0)
        .reset_index()
    )
    for p in ['GASOIL', 'LPG', 'PREMIUM']:
        if p not in bdc_pivot.columns: bdc_pivot[p] = 0
    bdc_pivot['TOTAL']        = bdc_pivot[['GASOIL','LPG','PREMIUM']].sum(axis=1)
    nat_total                 = bdc_pivot['TOTAL'].sum()
    bdc_pivot['Market Share %'] = (bdc_pivot['TOTAL'] / nat_total * 100).round(2)
    bdc_pivot = bdc_pivot.sort_values('TOTAL', ascending=False)
    disp = bdc_pivot.copy()
    for c in ['GASOIL','LPG','PREMIUM','TOTAL']:
        disp[c] = disp[c].apply(lambda x: f"{x:,.0f}")
    disp['Market Share %'] = disp['Market Share %'].apply(lambda x: f"{x:.2f}%")
    st.dataframe(disp, use_container_width=True, hide_index=True)

    if not cto_df.empty:
        st.markdown("---")
        st.markdown("### 🔄 TOP 10 BDCs BY CUSTODY TRANSFER OUT VOLUME")
        top_cto = (
            cto_df.groupby('bdc_id')['Volume'].sum()
            .sort_values(ascending=False).head(10).reset_index()
        )
        id_to_name = {v: k for k, v in BDC_MAP.items()}
        top_cto['BDC']    = top_cto['bdc_id'].map(id_to_name)
        top_cto['Volume'] = top_cto['Volume'].apply(lambda x: f"{x:,.0f}")
        st.dataframe(top_cto[['BDC','Volume']], use_container_width=True, hide_index=True)

    st.markdown("---")

    # ── Export ─────────────────────────────────────────────────────────────────
    st.markdown("### 💾 EXPORT NATIONAL REPORT")
    if st.button("📄 GENERATE EXCEL REPORT", use_container_width=True):
        out_dir = os.path.join(os.getcwd(), "national_stockout_reports")
        os.makedirs(out_dir, exist_ok=True)
        filename = f"national_stockout_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        filepath = os.path.join(out_dir, filename)
        export_forecast = pd.DataFrame([{
            'Product':               r['display_name'],
            'National Stock (LT)':   r['total_balance'],
            'OMC Sales (LT)':        r['omc_volume'],
            'BDC Transfers (LT)':    r['cto_volume'],
            'Total Depletion (LT)':  r['total_depletion'],
            'Daily Rate (LT)':       r['daily_rate'],
            'Days of Supply':        r['days_remaining'] if r['days_remaining'] != float('inf') else 9999,
            'Projected Empty':       (datetime.now() + timedelta(days=r['days_remaining'])).strftime('%Y-%m-%d')
                                     if r['days_remaining'] != float('inf') else 'N/A',
        } for _, r in forecast_df.iterrows()])
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            export_forecast.to_excel(writer, sheet_name='Stockout Forecast', index=False)
            bdc_pivot.to_excel(writer,       sheet_name='Stock by BDC',      index=False)
            if not omc_df.empty:
                omc_df.to_excel(writer,      sheet_name='OMC Loadings',      index=False)
            if not cto_df.empty:
                cto_df.to_excel(writer,      sheet_name='CTO Transactions',  index=False)
            bal_df.to_excel(writer,          sheet_name='Balance Detail',     index=False)
        st.success(f"✅ Report: {filename}")
        with open(filepath, 'rb') as f:
            st.download_button(
                "⬇️ DOWNLOAD NATIONAL REPORT v2", f, filename,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )


# ==================== ENTRY POINT ====================
if __name__ == "__main__":
    main()