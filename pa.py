"""
NPA ENERGY ANALYTICS - STREAMLIT DASHBOARD
===========================================
INSTALLATION:
pip install streamlit pandas pdfplumber PyPDF2 openpyxl python-dotenv plotly
USAGE:
streamlit run npa_dashboard.py
FIXED: Product ID mapping for Stock Transaction now uses separate variable
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

# Load environment variables
load_dotenv()

# ==================== LOAD ID MAPPINGS FROM ENV ====================
def load_bdc_mappings():
    """Load BDC name to ID mappings from environment variables"""
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
    """Load Depot name to ID mappings from environment variables"""
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
    """STOCK TRANSACTION ONLY: Simple names → IDs from .env"""
    return {
        "PMS": int(os.getenv('PRODUCT_PREMIUM_ID', '12')),
        "Gasoil": int(os.getenv('PRODUCT_GASOIL_ID', '14')),
        "LPG": int(os.getenv('PRODUCT_LPG_ID', '28'))
    }

# Load all mappings at startup
BDC_MAP = load_bdc_mappings()
DEPOT_MAP = load_depot_mappings()
STOCK_PRODUCT_MAP = load_product_mappings() # FIXED: Renamed to avoid conflict

# Product options for user-friendly dropdown in Stock Transaction
PRODUCT_OPTIONS = ["PMS", "Gasoil", "LPG"]

# Mapping from display name to balance product name (for stockout analysis)
PRODUCT_BALANCE_MAP = {
    "PMS": "PREMIUM",
    "Gasoil": "GASOIL",
    "LPG": "LPG"
}

# NPA Configuration from environment
NPA_CONFIG = {
    'COMPANY_ID': os.getenv('NPA_COMPANY_ID', '1'),
    'USER_ID': os.getenv('NPA_USER_ID', '123292'),
    'APP_ID': os.getenv('NPA_APP_ID', '3'),
    'ITS_FROM_PERSOL': os.getenv('NPA_ITS_FROM_PERSOL', 'Persol Systems Limited'),
    'BDC_BALANCE_URL': os.getenv('NPA_BDC_BALANCE_URL', 'https://iml.npa-enterprise.com/NPAAPILIVE/Home/CreateProductBalance'),
    'OMC_LOADINGS_URL': os.getenv('NPA_OMC_LOADINGS_URL', 'https://iml.npa-enterprise.com/NewNPA/home/CreateOrdersReport'),
    'DAILY_ORDERS_URL': os.getenv('NPA_DAILY_ORDERS_URL', 'https://iml.npa-enterprise.com/NewNPA/home/CreateDailyOrderReport'),
    'STOCK_TRANSACTION_URL': os.getenv('NPA_STOCK_TRANSACTION_URL', 'https://iml.npa-enterprise.com/NewNPA/home/CreateStockTransactionReport'),
    'OMC_NAME': os.getenv('OMC_NAME', 'OILCORP ENERGIA LIMITED')
}

# ==================== HISTORY & CACHE FUNCTIONS ====================
def save_to_history(data_type, df, metadata=None):
    history_dir = os.path.join(os.getcwd(), "data_history")
    os.makedirs(history_dir, exist_ok=True)
  
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{data_type}_{timestamp}.json"
    filepath = os.path.join(history_dir, filename)
  
    history_data = {
        'timestamp': timestamp,
        'data_type': data_type,
        'metadata': metadata or {},
        'summary': {
            'total_records': len(df),
            'total_volume': float(df['Quantity'].sum()) if 'Quantity' in df.columns else 0,
            'unique_bdcs': int(df['BDC'].nunique()) if 'BDC' in df.columns else 0
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
        fig.update_layout(
            title=dict(text="No data available", font=dict(size=20, color='#00ffff', family='Orbitron')),
            paper_bgcolor='rgba(10, 14, 39, 0.8)',
            height=400
        )
        return fig
  
    product_summary = df.groupby('Product')[value_col].sum().reset_index()
  
    fig = go.Figure(data=[go.Pie(
        labels=product_summary['Product'],
        values=product_summary[value_col],
        hole=0.4,
        marker=dict(colors=['#00ffff', '#ff00ff', '#00ff88', '#ffaa00']),
        textinfo='label+percent',
        textfont=dict(size=14, color='white', family='Orbitron')
    )])
  
    fig.update_layout(
        title=dict(text=title, font=dict(size=20, color='#00ffff', family='Orbitron')),
        paper_bgcolor='rgba(10, 14, 39, 0.8)',
        plot_bgcolor='rgba(10, 14, 39, 0.8)',
        showlegend=True,
        legend=dict(font=dict(color='white')),
        height=400
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
            fig.update_layout(
                title=dict(text="No data available", font=dict(size=20, color='#00ffff', family='Orbitron')),
                paper_bgcolor='rgba(10, 14, 39, 0.8)',
                height=500
            )
            return fig
      
        bdc_summary = df.groupby('BDC')[value_col].sum().sort_values(ascending=False).head(10).reset_index()
        bdc_summary.columns = ['BDC', 'Quantity']
  
    fig = go.Figure(data=[go.Bar(
        x=bdc_summary['BDC'],
        y=bdc_summary['Quantity'],
        marker=dict(
            color=bdc_summary['Quantity'],
            colorscale='Viridis',
            line=dict(color='#00ffff', width=2)
        ),
        text=bdc_summary['Quantity'].apply(lambda x: f'{x:,.0f}'),
        textposition='outside',
        textfont=dict(size=12, color='white')
    )])
  
    fig.update_layout(
        title=dict(text=title, font=dict(size=20, color='#00ffff', family='Orbitron')),
        xaxis=dict(title='BDC', color='white', tickangle=-45),
        yaxis=dict(title='Volume (LT/KG)', color='white'),
        paper_bgcolor='rgba(10, 14, 39, 0.8)',
        plot_bgcolor='rgba(22, 33, 62, 0.6)',
        height=500,
        showlegend=False
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
        x=daily_summary['Date'],
        y=daily_summary['Volume'],
        mode='lines+markers',
        name='Daily Volume',
        line=dict(color='#00ffff', width=3),
        marker=dict(size=8, color='#ff00ff', line=dict(color='white', width=2)),
        fill='tozeroy',
        fillcolor='rgba(0, 255, 255, 0.1)'
    ))
  
    fig.update_layout(
        title=dict(text=title, font=dict(size=20, color='#00ffff', family='Orbitron')),
        xaxis=dict(title='Date', color='white'),
        yaxis=dict(title='Volume (LT/KG)', color='white'),
        paper_bgcolor='rgba(10, 14, 39, 0.8)',
        plot_bgcolor='rgba(22, 33, 62, 0.6)',
        height=400,
        hovermode='x unified'
    )
  
    return fig

def create_comparison_chart(df1, df2, label1="Period 1", label2="Period 2"):
    prod1 = df1.groupby('Product')['Quantity'].sum().reset_index()
    prod2 = df2.groupby('Product')['Quantity'].sum().reset_index()
  
    fig = go.Figure()
  
    fig.add_trace(go.Bar(
        name=label1,
        x=prod1['Product'],
        y=prod1['Quantity'],
        marker=dict(color='#00ffff'),
        text=prod1['Quantity'].apply(lambda x: f'{x:,.0f}'),
        textposition='outside'
    ))
  
    fig.add_trace(go.Bar(
        name=label2,
        x=prod2['Product'],
        y=prod2['Quantity'],
        marker=dict(color='#ff00ff'),
        text=prod2['Quantity'].apply(lambda x: f'{x:,.0f}'),
        textposition='outside'
    ))
  
    fig.update_layout(
        title=dict(text='Period Comparison', font=dict(size=20, color='#00ffff', family='Orbitron')),
        xaxis=dict(title='Product', color='white'),
        yaxis=dict(title='Volume (LT/KG)', color='white'),
        paper_bgcolor='rgba(10, 14, 39, 0.8)',
        plot_bgcolor='rgba(22, 33, 62, 0.6)',
        barmode='group',
        height=400,
        legend=dict(font=dict(color='white'))
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
            'type': 'warning',
            'title': f"⚠️ Low Stock Alert",
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
            'type': 'info',
            'title': f"📈 Volume Spike Detected",
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
        to { text-shadow: 0 0 10px #00ffff, 0 0 20px #00ffff, 0 0 30px #00ffff, 0 0 40px #0ff; }
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
        color: white;
        border: 2px solid #00ffff;
        border-radius: 25px;
        padding: 15px 30px;
        font-family: 'Orbitron', sans-serif;
        font-weight: 700;
        font-size: 18px;
        box-shadow: 0 0 20px rgba(0, 255, 255, 0.5);
        transition: all 0.3s ease;
        text-transform: uppercase;
        letter-spacing: 2px;
    }
  
    .stButton > button:hover {
        transform: scale(1.05) translateY(-3px);
        box-shadow: 0 0 30px rgba(0, 255, 255, 0.8), 0 0 40px rgba(255, 0, 255, 0.5);
        background: linear-gradient(45deg, #00ffff, #ff00ff);
    }
  
    .dataframe {
        background-color: rgba(10, 14, 39, 0.8) !important;
        border: 2px solid #00ffff !important;
        border-radius: 10px;
        box-shadow: 0 0 20px rgba(0, 255, 255, 0.3);
    }
  
    .dataframe th {
        background-color: #16213e !important;
        color: #00ffff !important;
        font-family: 'Orbitron', sans-serif;
        text-transform: uppercase;
        border: 1px solid #00ffff !important;
    }
  
    .dataframe td {
        background-color: rgba(22, 33, 62, 0.6) !important;
        color: #ffffff !important;
        border: 1px solid rgba(0, 255, 255, 0.2) !important;
    }
  
    [data-testid="stMetricValue"] {
        font-family: 'Orbitron', sans-serif;
        font-size: 28px !important;
        color: #00ffff !important;
        text-shadow: 0 0 15px #00ffff;
    }
  
    .metric-card {
        background: rgba(22,33,62,0.6);
        padding: 20px;
        border-radius: 15px;
        border: 2px solid #00ffff;
        text-align: center;
    }
  
    .metric-card h2 {
        color: #ff00ff !important;
        margin: 0;
        font-size: 20px !important;
    }
  
    .metric-card h1 {
        color: #00ffff !important;
        margin: 10px 0;
        font-size: 32px !important;
        word-wrap: break-word;
    }
  
    [data-testid="stMetricLabel"] {
        font-family: 'Rajdhani', sans-serif;
        color: #ff00ff !important;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 2px;
    }
  
    p, span, div {
        font-family: 'Rajdhani', sans-serif;
        color: #e0e0e0;
    }
  
    [data-testid="stFileUploader"] {
        border: 2px dashed #00ffff;
        border-radius: 15px;
        background: rgba(22, 33, 62, 0.3);
        padding: 20px;
    }
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
    def _normalize_spaces(text: str) -> str:
        return re.sub(r"\s+", " ", (text or "").strip())

    def _normalize_bdc(self, bdc: str) -> str:
        if not bdc:
            return ""
        clean = self._normalize_spaces(bdc)
        up = clean.upper().replace("-", " ").replace("_", " ")
        up = self._normalize_spaces(up)
        if up.startswith("BOST"):
            return "BOST"
        return clean

    def _is_bost_labeled_depot(self, depot: str) -> bool:
        dep = self._normalize_spaces(depot or "")
        dep = dep.replace("-", " ")
        dep = self._normalize_spaces(dep)
        return dep.upper().startswith("BOST ")

    def _is_bost_global_depot(self, depot: str) -> bool:
        dep = self._normalize_spaces(depot or "")
        dep = dep.replace("-", " ")
        dep = self._normalize_spaces(dep)
        return bool(self.bost_global_re.search(dep))

    def _parse_date_from_line(self, line: str):
        m = re.search(r'(\w+\s+\d{1,2}\s*,\s*\d{4})', line)
        if m:
            cleaned = m.group(1).replace(" ,", ",").replace(" ", " ")
            return datetime.strptime(cleaned, '%B %d, %Y').strftime('%Y/%m/%d')
        return None

    def _append_record(self, records, date, bdc, depot, product, actual, available):
        bdc_clean = self._normalize_bdc(bdc)
        product = (product or "").upper()
        if product not in self.allowed_products:
            return
        if self._is_bost_labeled_depot(depot) and not self._is_bost_global_depot(depot):
            return
        if actual <= 0:
            return
        records.append({
            'Date': date,
            'BDC': bdc_clean,
            'DEPOT': self._normalize_spaces(depot),
            'Product': product,
            'ACTUAL BALANCE (LT\\KG)': actual,
            'AVAILABLE BALANCE (LT\\KG)': available
        })

    def parse_pdf_file(self, pdf_file):
        records = []
        try:
            reader = PyPDF2.PdfReader(pdf_file)
            current_bdc = None
            current_depot = None
            current_date = None
            for page in reader.pages:
                text = page.extract_text() or ""
                lines = [ln.strip() for ln in text.split('\n') if ln.strip()]
                for line in lines:
                    up = line.upper()
                    if 'DATE AS AT' in up:
                        maybe_date = self._parse_date_from_line(line)
                        if maybe_date:
                            current_date = maybe_date
                    if up.startswith('BDC :') or up.startswith('BDC:'):
                        current_bdc = re.sub(r'^BDC\s*:\s*', '', line, flags=re.IGNORECASE).strip()
                    if up.startswith('DEPOT :') or up.startswith('DEPOT:'):
                        current_depot = re.sub(r'^DEPOT\s*:\s*', '', line, flags=re.IGNORECASE).strip()
                    if current_bdc and current_depot and current_date:
                        m = self.product_line_re.match(line)
                        if m:
                            product = m.group(1)
                            actual = float(m.group(2).replace(',', ''))
                            available = float(m.group(3).replace(',', ''))
                            self._append_record(
                                records, current_date, current_bdc, current_depot,
                                product, actual, available
                            )
            return records
        except Exception as e:
            st.error(f"Error parsing PDF: {e}")
            return []

    def save_to_excel(self, records, filename=None):
        if not records:
            return None
        if filename is None:
            filename = f"stock_balance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        out_path = os.path.join(self.output_dir, os.path.basename(filename))
        df = pd.DataFrame(records)
        df = df.sort_values(['Product', 'BDC', 'DEPOT', 'Date'], ignore_index=True)
        with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Stock Balance')
            for prod in ['LPG', 'PREMIUM', 'GASOIL']:
                dff = df[df['Product'].str.upper() == prod]
                if dff.empty:
                    dff = pd.DataFrame(columns=df.columns)
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
                if maybe_date:
                    current_date = maybe_date
            if up.startswith('BDC :') or up.startswith('BDC:'):
                current_bdc = re.sub(r'^BDC\s*:\s*', '', line, flags=re.IGNORECASE).strip()
            if up.startswith('DEPOT :') or up.startswith('DEPOT:'):
                current_depot = re.sub(r'^DEPOT\s*:\s*', '', line, flags=re.IGNORECASE).strip()
            if current_bdc and current_depot and current_date:
                m = self.product_line_re.match(line)
                if m:
                    product = m.group(1)
                    actual = float(m.group(2).replace(',', ''))
                    available = float(m.group(3).replace(',', ''))
                    self._append_record(
                        records, current_date, current_bdc, current_depot,
                        product, actual, available
                    )
        return records

# ==================== OMC LOADINGS FUNCTIONS ====================
PRODUCT_MAP = {"AGO": "GASOIL", "PMS": "PREMIUM", "LPG": "LPG"}
ONLY_COLUMNS = ["Date", "OMC", "Truck", "Product", "Quantity", "Price", "Depot", "Order Number", "BDC"]
HEADER_KEYWORDS = ["ORDER REPORT", "National Petroleum Authority", "ORDER NUMBER", "ORDER DATE", "ORDER STATUS", "BDC:", "Total for :", "Printed By :", "Page ", "BRV NUMBER", "VOLUME"]
LOADED_KEYWORDS = {"Released", "Submitted"}
def _looks_like_header(line: str) -> bool:
    return any(h in line for h in HEADER_KEYWORDS)
def _extract_depot(line: str):
    m = re.search(r"DEPOT:([^-\n]+)", line)
    return m.group(1).strip() if m else None
def _extract_bdc(line: str):
    m = re.search(r"BDC:([^\n]+)", line)
    return m.group(1).strip() if m else None
def _detect_product(line: str) -> str:
    if "AGO" in line:
        raw = "AGO"
    elif "LPG" in line:
        raw = "LPG"
    else:
        raw = "PMS"
    return PRODUCT_MAP.get(raw, raw or "")
def _find_loaded_index(tokens: list):
    for i, t in enumerate(tokens):
        if t in LOADED_KEYWORDS:
            return i
    return None
def _parse_loaded_line(line: str, current_product: str, current_depot: str, current_bdc: str):
    tokens = line.split()
    if len(tokens) < 6:
        return None
    rel_idx = _find_loaded_index(tokens)
    if rel_idx is None or rel_idx < 2:
        return None
    try:
        date_token = tokens[0]
        order_number = tokens[1]
        volume = float(tokens[-1].replace(",", ""))
        price = float(tokens[-2].replace(",", ""))
        brv_number = tokens[-3]
        company_name = " ".join(tokens[rel_idx + 1:-3]).strip()
        try:
            date_obj = datetime.strptime(date_token, "%d-%b-%Y")
            date_str = date_obj.strftime("%Y/%m/%d")
        except:
            date_str = date_token
        return {
            "Date": date_str, "OMC": company_name, "Truck": brv_number,
            "Product": current_product, "Quantity": volume, "Price": price,
            "Depot": current_depot, "Order Number": order_number, "BDC": current_bdc,
        }
    except:
        return None
def extract_npa_data_from_pdf(pdf_file) -> pd.DataFrame:
    extracted_rows = []
    current_depot = ""
    current_bdc = ""
    current_product = PRODUCT_MAP.get("PMS", "PMS")
  
    try:
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or page.extract_text(x_tolerance=2, y_tolerance=2)
                if not text:
                    continue
              
                lines = text.split("\n")
              
                for raw_line in lines:
                    line = raw_line.strip()
                    if not line:
                        continue
                  
                    if "DEPOT:" in line:
                        maybe_depot = _extract_depot(line)
                        if maybe_depot:
                            current_depot = maybe_depot
                        continue
                    if "BDC:" in line:
                        maybe_bdc = _extract_bdc(line)
                        if maybe_bdc:
                            current_bdc = maybe_bdc
                        continue
                    if "PRODUCT" in line:
                        current_product = _detect_product(line)
                        continue
                    if _looks_like_header(line):
                        continue
                    if any(kw in line for kw in LOADED_KEYWORDS):
                        row = _parse_loaded_line(line, current_product, current_depot, current_bdc)
                        if row:
                            extracted_rows.append(row)
              
    except Exception as e:
        st.error(f"Error parsing PDF: {e}")
        return pd.DataFrame(columns=ONLY_COLUMNS)
  
    df = pd.DataFrame(extracted_rows)
    if df.empty:
        return pd.DataFrame(columns=ONLY_COLUMNS)
    for col in ONLY_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[ONLY_COLUMNS].drop_duplicates()
    try:
        _ds = pd.to_datetime(df["Date"], format="%Y/%m/%d", errors="coerce")
        df = df.assign(_ds=_ds).sort_values("_ds").drop(columns=["_ds"]).reset_index(drop=True)
    except:
        df = df.reset_index(drop=True)
    return df

def save_to_excel_multi(df: pd.DataFrame, filename: str = None) -> str:
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

def parse_text_to_dataframe(text_content: str) -> pd.DataFrame:
    extracted_rows = []
    current_depot = ""
    current_bdc = ""
    current_product = PRODUCT_MAP.get("PMS", "PMS")
  
    lines = text_content.split("\n")
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        if "DEPOT:" in line:
            maybe_depot = _extract_depot(line)
            if maybe_depot:
                current_depot = maybe_depot
            continue
        if "BDC:" in line:
            maybe_bdc = _extract_bdc(line)
            if maybe_bdc:
                current_bdc = maybe_bdc
            continue
        if "PRODUCT" in line:
            current_product = _detect_product(line)
            continue
        if _looks_like_header(line):
            continue
        if any(kw in line for kw in LOADED_KEYWORDS):
            row = _parse_loaded_line(line, current_product, current_depot, current_bdc)
            if row:
                extracted_rows.append(row)
  
    df = pd.DataFrame(extracted_rows)
    if df.empty:
        return pd.DataFrame(columns=ONLY_COLUMNS)
    for col in ONLY_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[ONLY_COLUMNS].drop_duplicates()
    try:
        _ds = pd.to_datetime(df["Date"], format="%Y/%m/%d", errors="coerce")
        df = df.assign(_ds=_ds).sort_values("_ds").drop(columns=["_ds"]).reset_index(drop=True)
    except:
        df = df.reset_index(drop=True)
    return df

# ==================== DAILY ORDERS FUNCTIONS ====================
DAILY_PRODUCT_MAP = {
    "PMS": "PREMIUM",
    "AGO": "GASOIL",
    "LPG": "LPG",
    "RFO": "RFO",
    "ATK": "ATK",
    "AVIATION": "ATK",
    "PREMIX": "PREMIX",
    "MGO": "GASOIL",
    "KEROSENE": "KEROSENE"
}

def clean_currency(value_str):
    if not value_str: return 0.0
    try:
        return float(value_str.replace(",", "").strip())
    except:
        return 0.0

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
  
    if not pv_match:
        return None
    price_str = pv_match.group(1)
    vol_str = pv_match.group(2)
  
    price = clean_currency(price_str)
    volume = clean_currency(vol_str)
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
        except:
            pass
        remainder = remainder.replace(date_match.group(1), "").strip()
  
    product_cat = get_product_category(line)
  
    noise_words = [
        "PMS", "AGO", "LPG", "RFO", "ATK", "PREMIX", "FOREIGN",
        "(Retail Outlets)", "Retail", "Outlets", "MGO", "Local",
        "Additivated", "Differentiated", "MINES", "Cell Sites", "Turbine", "Kerosene"
    ]
  
    order_num_tokens = []
    for t in remainder.split():
        is_noise = False
        for nw in noise_words:
            if nw.upper() in t.upper() or t in ["(", ")", "-"]:
                is_noise = True
                break
        if not is_noise:
            order_num_tokens.append(t)
          
    order_number = " ".join(order_num_tokens).strip()
  
    if not order_number and len(tokens) > 0:
        order_number = remainder
    return {
        "Date": date_val,
        "Order Number": order_number,
        "Product": product_cat,
        "Truck": brv,
        "Price": price,
        "Quantity": volume
    }

def simplify_bdc_names(df):
    if "BDC" not in df.columns or df.empty:
        return df
    unique_bdcs = df["BDC"].unique()
    mapping = {}
  
    for name in unique_bdcs:
        if not name:
            mapping[name] = name
            continue
          
        parts = name.split()
        short_name = " ".join(parts[:2])
        mapping[name] = short_name.upper()
    df["BDC"] = df["BDC"].map(mapping)
    return df

def extract_daily_orders_from_pdf(pdf_file) -> pd.DataFrame:
    all_rows = []
  
    ctx = {
        "Depot": "Unknown Depot",
        "BDC": "Unknown BDC",
        "Status": "Unknown Status",
        "Date": None
    }
  
    try:
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text(x_tolerance=2, y_tolerance=2)
                if not text: continue
              
                lines = text.split('\n')
              
                for line in lines:
                    clean = line.strip()
                    if not clean: continue
                  
                    if clean.startswith("DEPOT:"):
                        raw_depot = clean.replace("DEPOT:", "").strip()
                      
                        if raw_depot.startswith("BOST") or "TAKORADI BLUE OCEAN" in raw_depot:
                            ctx["Depot"] = "BOST Global"
                        else:
                            ctx["Depot"] = raw_depot
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
                      
                    row_data = parse_daily_line(clean, ctx["Date"])
                  
                    if row_data:
                        if row_data["Date"]:
                            ctx["Date"] = row_data["Date"]
                      
                        final_row = {
                            "Date": row_data["Date"],
                            "Truck": row_data["Truck"],
                            "Product": row_data["Product"],
                            "Quantity": row_data["Quantity"],
                            "Price": row_data["Price"],
                            "Depot": ctx["Depot"],
                            "Order Number": row_data["Order Number"],
                            "BDC": ctx["BDC"],
                            "Status": ctx["Status"]
                        }
                        all_rows.append(final_row)
    except Exception as e:
        st.error(f"Error parsing PDF: {e}")
        return pd.DataFrame()
    df = pd.DataFrame(all_rows)
  
    if not df.empty:
        df = simplify_bdc_names(df)
      
    return df

def save_daily_orders_excel(df: pd.DataFrame, filename: str = None) -> str:
    out_dir = os.path.join(os.getcwd(), "daily_orders")
    os.makedirs(out_dir, exist_ok=True)
    if filename is None:
        filename = f"daily_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    out_path = os.path.join(out_dir, filename)
  
    if not df.empty:
        pivot = df.pivot_table(
            index="BDC",
            columns="Product",
            values="Quantity",
            aggfunc="sum",
            fill_value=0
        ).reset_index()
      
        product_cols = [c for c in pivot.columns if c != "BDC"]
        pivot["Grand Total"] = pivot[product_cols].sum(axis=1)
    else:
        pivot = pd.DataFrame()
  
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="All Orders", index=False)
        if not pivot.empty:
            pivot.to_excel(writer, sheet_name="Summary by BDC", index=False)
  
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
        choice = st.radio("SELECT YOUR DATA MISSION:", ["🏦 BDC BALANCE", "🚚 OMC LOADINGS", "📅 DAILY ORDERS", "📊 MARKET SHARE", "🎯 COMPETITIVE INTEL", "📈 STOCK TRANSACTION", "🧠 BDC INTELLIGENCE", "🌍 NATIONAL STOCKOUT"], index=0)
        st.markdown("---")
        st.markdown("""
        <div style='text-align: center; padding: 20px; background: rgba(255, 0, 255, 0.1); border-radius: 10px; border: 2px solid #ff00ff;'>
            <h3>⚙️ SYSTEM STATUS</h3>
            <p style='color: #00ff88; font-size: 20px;'>🟢 OPERATIONAL</p>
        </div>
        """, unsafe_allow_html=True)
  
    if choice == "🏦 BDC BALANCE":
        show_bdc_balance()
    elif choice == "🚚 OMC LOADINGS":
        show_omc_loadings()
    elif choice == "📅 DAILY ORDERS":
        show_daily_orders()
    elif choice == "📊 MARKET SHARE":
        show_market_share()
    elif choice == "🎯 COMPETITIVE INTEL":
        show_competitive_intel()
    elif choice == "📈 STOCK TRANSACTION":
        show_stock_transaction()
    elif choice == "🧠 BDC INTELLIGENCE":
        show_bdc_intelligence()
    else:
        show_national_stockout()

if __name__ == "__main__":
    main()