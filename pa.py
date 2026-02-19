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
        choice = st.radio("SELECT YOUR DATA MISSION:", [
            "🏦 BDC BALANCE",
            "🚚 OMC LOADINGS",
            "📅 DAILY ORDERS",
            "📊 MARKET SHARE",
            "🎯 COMPETITIVE INTEL",
            "📈 STOCK TRANSACTION",
            "🧠 BDC INTELLIGENCE",
            "🌍 NATIONAL STOCKOUT",
            "─────── NEW ───────",
            "🔴 LIVE RUNWAY MONITOR",
            "📉 HISTORICAL TRENDS",
            "🗺️ DEPOT STRESS MAP",
            "🔮 DEMAND FORECAST",
            "⚠️ REORDER ALERTS",
            "📆 WEEK-ON-WEEK",
        ], index=0)
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
    elif choice == "🌍 NATIONAL STOCKOUT":
        show_national_stockout()
    elif choice == "🔴 LIVE RUNWAY MONITOR":
        show_live_runway_monitor()
    elif choice == "📉 HISTORICAL TRENDS":
        show_historical_trends()
    elif choice == "🗺️ DEPOT STRESS MAP":
        show_depot_stress_map()
    elif choice == "🔮 DEMAND FORECAST":
        show_demand_forecast()
    elif choice == "⚠️ REORDER ALERTS":
        show_reorder_alerts()
    elif choice == "📆 WEEK-ON-WEEK":
        show_week_on_week()
    else:
        st.info("Select a page from the sidebar.")
def show_bdc_balance():
    st.markdown("<h2>🏦 BDC STOCK BALANCE ANALYZER</h2>", unsafe_allow_html=True)
    st.info("📊 Click the button below to fetch BDC Balance data")
    st.markdown("---")
 
    if 'bdc_records' not in st.session_state:
        st.session_state.bdc_records = []
 
    if st.button("🔄 FETCH BDC BALANCE DATA", width='stretch'):
        with st.spinner("🔄 FETCHING DATA FROM NPA PORTAL..."):
            scraper = StockBalanceScraper()
         
            url = NPA_CONFIG['BDC_BALANCE_URL']
            params = {
                'lngCompanyId': NPA_CONFIG['COMPANY_ID'],
                'strITSfromPersol': NPA_CONFIG['ITS_FROM_PERSOL'],
                'strGroupBy': 'BDC',
                'strGroupBy1': 'DEPOT',
                'strQuery1': '',
                'strQuery2': '',
                'strQuery3': '',
                'strQuery4': '',
                'strPicHeight': '1',
                'szPicWeight': '1',
                'lngUserId': NPA_CONFIG['USER_ID'],
                'intAppId': NPA_CONFIG['APP_ID']
            }
         
            try:
                import requests
                import io
             
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Accept': 'application/pdf,text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Connection': 'keep-alive',
                }
             
                response = requests.get(url, params=params, headers=headers, timeout=30)
                response.raise_for_status()
             
                if response.content[:4] == b'%PDF':
                    st.success("✅ PDF received from server")
                    pdf_file = io.BytesIO(response.content)
                    st.session_state.bdc_records = scraper.parse_pdf_file(pdf_file)
                 
                    if not st.session_state.bdc_records:
                        st.warning("⚠️ No records found in PDF.")
                else:
                    st.error("❌ Response is not a PDF.")
                    st.session_state.bdc_records = []
             
            except requests.exceptions.RequestException as e:
                st.error(f"❌ Network Error: {e}")
                st.session_state.bdc_records = []
            except Exception as e:
                st.error(f"❌ Error: {e}")
                import traceback
                st.code(traceback.format_exc())
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
                st.markdown(f"""
                <div class='metric-card'>
                    <h2>{prod}</h2>
                    <h1>{val:,.0f}</h1>
                    <p style='color: #888; font-size: 14px; margin: 0;'>LT/KG</p>
                </div>
                """, unsafe_allow_html=True)
     
        st.markdown("---")
     
        st.markdown("<h3>🏢 BDC BREAKDOWN</h3>", unsafe_allow_html=True)
        bdc_summary = df.groupby('BDC').agg({
            'ACTUAL BALANCE (LT\\KG)': 'sum',
            'DEPOT': 'nunique',
            'Product': lambda x: x.nunique()
        }).reset_index()
        bdc_summary.columns = ['BDC', 'Total Balance (LT/KG)', 'Depots', 'Products']
        bdc_summary = bdc_summary.sort_values('Total Balance (LT/KG)', ascending=False)
     
        col1, col2 = st.columns([2, 1])
        with col1:
            st.dataframe(bdc_summary, width='stretch', hide_index=True)
        with col2:
            st.markdown("#### 📈 Key Metrics")
            st.metric("Total BDCs", f"{df['BDC'].nunique()}")
            st.metric("Total Depots", f"{df['DEPOT'].nunique()}")
            col_name = 'ACTUAL BALANCE (LT\\KG)'
            st.metric("Grand Total", f"{df[col_name].sum():,.0f} LT/KG")
     
        st.markdown("---")
     
        st.markdown("<h3>📊 PRODUCT DISTRIBUTION BY BDC</h3>", unsafe_allow_html=True)
     
        pivot_data = df.pivot_table(
            index='BDC',
            columns='Product',
            values='ACTUAL BALANCE (LT\\KG)',
            aggfunc='sum',
            fill_value=0
        ).reset_index()
     
        for prod in ['GASOIL', 'LPG', 'PREMIUM']:
            if prod not in pivot_data.columns:
                pivot_data[prod] = 0
     
        pivot_data['TOTAL'] = pivot_data[['GASOIL', 'LPG', 'PREMIUM']].sum(axis=1)
        pivot_data = pivot_data.sort_values('TOTAL', ascending=False)
     
        st.dataframe(pivot_data[['BDC', 'GASOIL', 'LPG', 'PREMIUM', 'TOTAL']], width='stretch', hide_index=True)
     
        st.markdown("---")
     
        st.markdown("<h3>🔍 SEARCH & FILTER</h3>", unsafe_allow_html=True)
     
        col1, col2 = st.columns(2)
     
        with col1:
            search_type = st.selectbox("Search By:", ["Product", "BDC", "Depot"], key='bdc_search_type')
     
        with col2:
            if search_type == "Product":
                search_value = st.selectbox("Select Product:", ['ALL'] + sorted(df['Product'].unique().tolist()), key='bdc_product_search')
            elif search_type == "BDC":
                search_value = st.selectbox("Select BDC:", ['ALL'] + sorted(df['BDC'].unique().tolist()), key='bdc_bdc_search')
            else:
                search_value = st.selectbox("Select Depot:", ['ALL'] + sorted(df['DEPOT'].unique().tolist()), key='bdc_depot_search')
     
        if search_value == 'ALL':
            filtered = df
        else:
            if search_type == "Product":
                filtered = df[df['Product'] == search_value]
            elif search_type == "BDC":
                filtered = df[df['BDC'] == search_value]
            else:
                filtered = df[df['DEPOT'] == search_value]
     
        st.markdown(f"<h3>📋 FILTERED DATA: {search_value}</h3>", unsafe_allow_html=True)
        display = filtered[['Product', 'BDC', 'DEPOT', 'AVAILABLE BALANCE (LT\\KG)', 'ACTUAL BALANCE (LT\\KG)', 'Date']].sort_values(['Product', 'BDC', 'DEPOT'])
        st.dataframe(display, width='stretch', height=400, hide_index=True)
     
        st.markdown("---")
        st.markdown("<h3>📋 QUICK STATS</h3>", unsafe_allow_html=True)
        cols = st.columns(4)
        col_actual = 'ACTUAL BALANCE (LT\\KG)'
        with cols[0]:
            st.metric("RECORDS", f"{len(filtered):,}")
        with cols[1]:
            st.metric("BDCs", f"{filtered['BDC'].nunique()}")
        with cols[2]:
            st.metric("DEPOTS", f"{filtered['DEPOT'].nunique()}")
        with cols[3]:
            st.metric("TOTAL BALANCE", f"{filtered[col_actual].sum():,.0f}")
     
        st.markdown("---")
        st.markdown("<h3>💾 EXPORT DATA</h3>", unsafe_allow_html=True)
        scraper = StockBalanceScraper()
        path = scraper.save_to_excel(records)
        if path and os.path.exists(path):
            with open(path, 'rb') as f:
                st.download_button("⬇️ DOWNLOAD EXCEL", f, os.path.basename(path), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", width='stretch')
    else:
        st.info("👆 Click the button above to fetch BDC balance data")
def show_omc_loadings():
    st.markdown("<h2>🚚 OMC LOADINGS ANALYZER</h2>", unsafe_allow_html=True)
    st.info("📊 Select date range and fetch OMC loadings data")
    st.markdown("---")
 
    if 'omc_df' not in st.session_state:
        st.session_state.omc_df = pd.DataFrame()
    if 'omc_start_date' not in st.session_state:
        st.session_state.omc_start_date = datetime.now() - timedelta(days=7)
    if 'omc_end_date' not in st.session_state:
        st.session_state.omc_end_date = datetime.now()
 
    st.markdown("<h3>📅 SELECT DATE RANGE</h3>", unsafe_allow_html=True)
    st.info("💡 Select a date range where you know there are orders. Try last week or last month for better results.")
 
    col1, col2 = st.columns(2)
 
    with col1:
        start_date = st.date_input("Start Date", value=st.session_state.omc_start_date, key='omc_start')
    with col2:
        end_date = st.date_input("End Date", value=st.session_state.omc_end_date, key='omc_end')
 
    if st.button("🔄 FETCH OMC LOADINGS DATA", width='stretch'):
        with st.spinner("🔄 FETCHING DATA FROM NPA PORTAL..."):
            st.session_state.omc_start_date = start_date
            st.session_state.omc_end_date = end_date
         
            start_str = start_date.strftime("%m/%d/%Y")
            end_str = end_date.strftime("%m/%d/%Y")
         
            st.info(f"🔍 Requesting orders from **{start_str}** to **{end_str}**")
         
            url = NPA_CONFIG['OMC_LOADINGS_URL']
            params = {
                'lngCompanyId': NPA_CONFIG['COMPANY_ID'],
                'szITSfromPersol': 'persol',
                'strGroupBy': 'BDC',
                'strGroupBy1': NPA_CONFIG['OMC_NAME'],
                'strQuery1': ' and iorderstatus=4',
                'strQuery2': start_str,
                'strQuery3': end_str,
                'strQuery4': '',
                'strPicHeight': '',
                'strPicWeight': '',
                'intPeriodID': '4',
                'iUserId': NPA_CONFIG['USER_ID'],
                'iAppId': NPA_CONFIG['APP_ID']
            }
         
            try:
                import requests
                import io
             
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Accept': 'application/pdf,text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Connection': 'keep-alive',
                }
             
                response = requests.get(url, params=params, headers=headers, timeout=30)
                response.raise_for_status()
             
                if response.content[:4] == b'%PDF':
                    st.success("✅ PDF received from server")
                    pdf_file = io.BytesIO(response.content)
                    st.session_state.omc_df = extract_npa_data_from_pdf(pdf_file)
                 
                    if st.session_state.omc_df.empty:
                        st.warning("⚠️ No order records found in the PDF for this date range.")
                else:
                    st.error("❌ Response is not a PDF.")
                    st.session_state.omc_df = pd.DataFrame()
             
            except requests.exceptions.RequestException as e:
                st.error(f"❌ Network Error: {e}")
                st.session_state.omc_df = pd.DataFrame()
            except Exception as e:
                st.error(f"❌ Error: {e}")
                import traceback
                st.code(traceback.format_exc())
                st.session_state.omc_df = pd.DataFrame()
 
    df = st.session_state.omc_df
 
    if not df.empty:
        st.success(f"✅ EXTRACTED {len(df)} RECORDS")
        st.markdown("---")
     
        st.info(f"📊 Showing {len(df)} records from {st.session_state.omc_start_date.strftime('%Y/%m/%d')} to {st.session_state.omc_end_date.strftime('%Y/%m/%d')}")
     
        st.markdown("---")
     
        st.markdown("<h3>📊 ANALYTICS DASHBOARD</h3>", unsafe_allow_html=True)
     
        cols = st.columns(4)
        with cols[0]:
            st.markdown(f"""
            <div class='metric-card'>
                <h2>TOTAL ORDERS</h2>
                <h1>{len(df):,}</h1>
            </div>
            """, unsafe_allow_html=True)
        with cols[1]:
            st.markdown(f"""
            <div class='metric-card'>
                <h2>VOLUME</h2>
                <h1>{df['Quantity'].sum():,.0f}</h1>
                <p style='color: #888; font-size: 14px; margin: 0;'>LT/KG</p>
            </div>
            """, unsafe_allow_html=True)
        with cols[2]:
            st.markdown(f"""
            <div class='metric-card'>
                <h2>OMCs</h2>
                <h1>{df['OMC'].nunique()}</h1>
            </div>
            """, unsafe_allow_html=True)
        with cols[3]:
            total_value = (df['Quantity'] * df['Price']).sum()
            st.markdown(f"""
            <div class='metric-card'>
                <h2>VALUE</h2>
                <h1>₵{total_value:,.0f}</h1>
            </div>
            """, unsafe_allow_html=True)
     
        st.markdown("---")
     
        st.markdown("<h3>📦 PRODUCT BREAKDOWN</h3>", unsafe_allow_html=True)
        product_summary = df.groupby('Product').agg({
            'Quantity': 'sum',
            'Order Number': 'count',
            'OMC': 'nunique'
        }).reset_index()
        product_summary.columns = ['Product', 'Total Volume (LT/KG)', 'Orders', 'OMCs']
        product_summary = product_summary.sort_values('Total Volume (LT/KG)', ascending=False)
     
        col1, col2 = st.columns([2, 1])
        with col1:
            st.dataframe(product_summary, width='stretch', hide_index=True)
        with col2:
            for _, row in product_summary.iterrows():
                pct = (row['Total Volume (LT/KG)'] / product_summary['Total Volume (LT/KG)'].sum()) * 100
                st.metric(row['Product'], f"{pct:.1f}%")
     
        st.markdown("---")
     
        st.markdown("<h3>🏢 TOP OMCs BY VOLUME</h3>", unsafe_allow_html=True)
        omc_summary = df.groupby('OMC').agg({
            'Quantity': 'sum',
            'Order Number': 'count',
            'Product': lambda x: x.nunique()
        }).reset_index()
        omc_summary.columns = ['OMC', 'Total Volume (LT/KG)', 'Orders', 'Products']
        omc_summary = omc_summary.sort_values('Total Volume (LT/KG)', ascending=False).head(10)
     
        st.dataframe(omc_summary, width='stretch', hide_index=True)
     
        st.markdown("---")
     
        st.markdown("<h3>🏦 BDC PERFORMANCE</h3>", unsafe_allow_html=True)
        bdc_summary = df.groupby('BDC').agg({
            'Quantity': 'sum',
            'Order Number': 'count',
            'OMC': 'nunique',
            'Product': lambda x: x.nunique()
        }).reset_index()
        bdc_summary.columns = ['BDC', 'Total Volume (LT/KG)', 'Orders', 'OMCs', 'Products']
        bdc_summary = bdc_summary.sort_values('Total Volume (LT/KG)', ascending=False)
     
        st.dataframe(bdc_summary, width='stretch', hide_index=True)
     
        st.markdown("---")
     
        st.markdown("<h3>📊 PRODUCT DISTRIBUTION BY BDC</h3>", unsafe_allow_html=True)
        pivot_data = df.pivot_table(
            index='BDC',
            columns='Product',
            values='Quantity',
            aggfunc='sum',
            fill_value=0
        ).reset_index()
     
        for prod in ['GASOIL', 'LPG', 'PREMIUM']:
            if prod not in pivot_data.columns:
                pivot_data[prod] = 0
     
        pivot_data['TOTAL'] = pivot_data[['GASOIL', 'LPG', 'PREMIUM']].sum(axis=1)
        pivot_data = pivot_data.sort_values('TOTAL', ascending=False)
     
        st.dataframe(pivot_data[['BDC', 'GASOIL', 'LPG', 'PREMIUM', 'TOTAL']], width='stretch', hide_index=True)
     
        st.markdown("---")
     
        st.markdown("<h3>🔍 SEARCH & FILTER</h3>", unsafe_allow_html=True)
     
        col1, col2 = st.columns(2)
     
        with col1:
            search_type = st.selectbox("Search By:", ["Product", "OMC", "BDC", "Depot"], key='omc_search_type')
     
        with col2:
            if search_type == "Product":
                search_value = st.selectbox("Select Product:", ['ALL'] + sorted(df['Product'].unique().tolist()), key='omc_product_search')
            elif search_type == "OMC":
                search_value = st.selectbox("Select OMC:", ['ALL'] + sorted(df['OMC'].unique().tolist()), key='omc_omc_search')
            elif search_type == "BDC":
                search_value = st.selectbox("Select BDC:", ['ALL'] + sorted(df['BDC'].unique().tolist()), key='omc_bdc_search')
            else:
                search_value = st.selectbox("Select Depot:", ['ALL'] + sorted(df['Depot'].unique().tolist()), key='omc_depot_search')
     
        if search_value == 'ALL':
            filtered = df
        else:
            if search_type == "Product":
                filtered = df[df['Product'] == search_value]
            elif search_type == "OMC":
                filtered = df[df['OMC'] == search_value]
            elif search_type == "BDC":
                filtered = df[df['BDC'] == search_value]
            else:
                filtered = df[df['Depot'] == search_value]
     
        st.markdown(f"<h3>📋 FILTERED DATA: {search_value}</h3>", unsafe_allow_html=True)
     
        if not filtered.empty:
            cols = st.columns(4)
            with cols[0]:
                st.metric("Filtered Orders", f"{len(filtered):,}")
            with cols[1]:
                st.metric("Filtered Volume", f"{filtered['Quantity'].sum():,.0f} LT")
            with cols[2]:
                st.metric("Unique OMCs", f"{filtered['OMC'].nunique()}")
            with cols[3]:
                st.metric("Filtered Value", f"₵{(filtered['Quantity'] * filtered['Price']).sum():,.0f}")
     
        display = filtered[['Date', 'OMC', 'Truck', 'Quantity', 'Order Number', 'BDC', 'Depot', 'Price', 'Product']].sort_values(['Product', 'OMC', 'Date'])
        st.dataframe(display, width='stretch', height=400, hide_index=True)
     
        st.markdown("---")
        st.markdown("<h3>💾 EXPORT DATA</h3>", unsafe_allow_html=True)
        path = save_to_excel_multi(df)
        if path and os.path.exists(path):
            with open(path, 'rb') as f:
                st.download_button("⬇️ DOWNLOAD EXCEL", f, os.path.basename(path), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", width='stretch')
    else:
        st.info("👆 Select dates and click the button above to fetch OMC loadings data")
def show_daily_orders():
    st.markdown("<h2>📅 DAILY ORDERS ANALYZER</h2>", unsafe_allow_html=True)
    st.info("📊 Select a date range to fetch daily orders")
    st.markdown("---")
 
    if 'daily_df' not in st.session_state:
        st.session_state.daily_df = pd.DataFrame()
    if 'daily_start_date' not in st.session_state:
        st.session_state.daily_start_date = datetime.now() - timedelta(days=1)
    if 'daily_end_date' not in st.session_state:
        st.session_state.daily_end_date = datetime.now()
 
    st.markdown("<h3>📅 SELECT DATE RANGE</h3>", unsafe_allow_html=True)
    st.info("💡 Select a date range for daily orders. Try yesterday or last few days for better results.")
 
    col1, col2 = st.columns(2)
 
    with col1:
        start_date = st.date_input("Start Date", value=st.session_state.daily_start_date, key='daily_start')
    with col2:
        end_date = st.date_input("End Date", value=st.session_state.daily_end_date, key='daily_end')
 
    if st.button("🔄 FETCH DAILY ORDERS", width='stretch'):
        with st.spinner("🔄 FETCHING DAILY ORDERS FROM NPA PORTAL..."):
            st.session_state.daily_start_date = start_date
            st.session_state.daily_end_date = end_date
         
            start_str = start_date.strftime("%m/%d/%Y")
            end_str = end_date.strftime("%m/%d/%Y")
         
            st.info(f"🔍 Requesting daily orders from **{start_str}** to **{end_str}**")
         
            url = NPA_CONFIG['DAILY_ORDERS_URL']
            params = {
                'lngCompanyId': NPA_CONFIG['COMPANY_ID'],
                'szITSfromPersol': 'persol',
                'strGroupBy': 'DEPOT',
                'strGroupBy1': '',
                'strQuery1': '',
                'strQuery2': start_str,
                'strQuery3': end_str,
                'strQuery4': '',
                'strPicHeight': '1',
                'strPicWeight': '1',
                'intPeriodID': '-1',
                'iUserId': NPA_CONFIG['USER_ID'],
                'iAppId': NPA_CONFIG['APP_ID']
            }
         
            try:
                import requests
                import io
             
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Accept': 'application/pdf,text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Connection': 'keep-alive',
                }
             
                response = requests.get(url, params=params, headers=headers, timeout=30)
                response.raise_for_status()
             
                if response.content[:4] == b'%PDF':
                    st.success("✅ PDF received from server")
                    pdf_file = io.BytesIO(response.content)
                    st.session_state.daily_df = extract_daily_orders_from_pdf(pdf_file)
                 
                    if st.session_state.daily_df.empty:
                        st.warning("⚠️ No daily orders found for this date.")
                else:
                    st.error("❌ Response is not a PDF.")
                    st.session_state.daily_df = pd.DataFrame()
             
            except requests.exceptions.RequestException as e:
                st.error(f"❌ Network Error: {e}")
                st.session_state.daily_df = pd.DataFrame()
            except Exception as e:
                st.error(f"❌ Error: {e}")
                import traceback
                st.code(traceback.format_exc())
                st.session_state.daily_df = pd.DataFrame()
 
    df = st.session_state.daily_df
 
    if not df.empty:
        if not st.session_state.get('omc_df', pd.DataFrame()).empty:
            loadings_df = st.session_state.omc_df
         
            import re
         
            def extract_order_prefix(order_num):
                if pd.isna(order_num):
                    return None
                order_str = str(order_num).strip().upper()
                match = re.match(r'^([A-Z]{2,})', order_str)
                if match:
                    return match.group(1)
                return None
         
            loadings_df['Order_Prefix'] = loadings_df['Order Number'].apply(extract_order_prefix)
         
            prefix_to_omc = {}
            for prefix in loadings_df['Order_Prefix'].dropna().unique():
                prefix_orders = loadings_df[loadings_df['Order_Prefix'] == prefix]
                most_common_omc = prefix_orders['OMC'].mode()
                if len(most_common_omc) > 0:
                    prefix_to_omc[prefix] = most_common_omc.iloc[0]
         
            order_to_omc_exact = loadings_df[['Order Number', 'OMC']].drop_duplicates()
            order_to_omc_dict_exact = dict(zip(order_to_omc_exact['Order Number'], order_to_omc_exact['OMC']))
         
            df['Order_Prefix'] = df['Order Number'].apply(extract_order_prefix)
         
            df['OMC'] = df['Order Number'].map(order_to_omc_dict_exact)
         
            df['OMC'] = df.apply(
                lambda row: prefix_to_omc.get(row['Order_Prefix']) if pd.isna(row['OMC']) and row['Order_Prefix'] else row['OMC'],
                axis=1
            )
         
            df = df.drop(columns=['Order_Prefix'])
         
            matched_count = df['OMC'].notna().sum()
            match_rate = (matched_count / len(df) * 100) if len(df) > 0 else 0
         
            exact_matches = df['Order Number'].isin(order_to_omc_dict_exact.keys()).sum()
            prefix_matches = matched_count - exact_matches
         
            st.session_state.daily_df = df
         
            st.success(f"✅ EXTRACTED {len(df)} DAILY ORDERS")
         
            if matched_count > 0:
                st.info(f"🔗 **INTELLIGENT OMC MATCHING:** Matched {matched_count} orders ({match_rate:.1f}%) - {exact_matches} exact, {prefix_matches} by prefix pattern!")
             
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Orders", len(df))
                with col2:
                    st.metric("Matched", matched_count)
                with col3:
                    st.metric("Exact Match", exact_matches)
                with col4:
                    st.metric("Prefix Match", prefix_matches)
             
                if prefix_matches > 0:
                    st.caption(f"📋 **Prefix Patterns Discovered:** {', '.join([f'{k}→{v}' for k, v in list(prefix_to_omc.items())[:10]])}")
            else:
                st.warning("⚠️ No order numbers matched. OMC names will be blank.")
        else:
            df['OMC'] = None
            st.session_state.daily_df = df
         
            st.success(f"✅ EXTRACTED {len(df)} DAILY ORDERS")
            st.warning("💡 **Tip:** Fetch OMC Loadings data first to automatically match order numbers with OMC names!")
     
        st.markdown("---")
     
        st.info(f"📊 Showing {len(df)} orders from {st.session_state.daily_start_date.strftime('%Y/%m/%d')} to {st.session_state.daily_end_date.strftime('%Y/%m/%d')}")
        st.markdown("---")
     
        st.markdown("<h3>📊 DAILY ANALYTICS</h3>", unsafe_allow_html=True)
     
        cols = st.columns(5)
        with cols[0]:
            st.markdown(f"""
            <div class='metric-card'>
                <h2>ORDERS</h2>
                <h1>{len(df):,}</h1>
            </div>
            """, unsafe_allow_html=True)
        with cols[1]:
            st.markdown(f"""
            <div class='metric-card'>
                <h2>VOLUME</h2>
                <h1>{df['Quantity'].sum():,.0f}</h1>
                <p style='color: #888; font-size: 14px; margin: 0;'>LT/KG</p>
            </div>
            """, unsafe_allow_html=True)
        with cols[2]:
            st.markdown(f"""
            <div class='metric-card'>
                <h2>BDCs</h2>
                <h1>{df['BDC'].nunique()}</h1>
            </div>
            """, unsafe_allow_html=True)
        with cols[3]:
            omc_count = df['OMC'].nunique() if 'OMC' in df.columns and df['OMC'].notna().any() else 0
            st.markdown(f"""
            <div class='metric-card'>
                <h2>OMCs</h2>
                <h1>{omc_count}</h1>
            </div>
            """, unsafe_allow_html=True)
        with cols[4]:
            total_value = (df['Quantity'] * df['Price']).sum()
            st.markdown(f"""
            <div class='metric-card'>
                <h2>VALUE</h2>
                <h1>₵{total_value:,.0f}</h1>
            </div>
            """, unsafe_allow_html=True)
     
        st.markdown("---")
     
        st.markdown("<h3>📦 PRODUCT SUMMARY</h3>", unsafe_allow_html=True)
        product_summary = df.groupby('Product').agg({
            'Quantity': 'sum',
            'Order Number': 'count',
            'BDC': 'nunique'
        }).reset_index()
        product_summary.columns = ['Product', 'Total Volume (LT/KG)', 'Orders', 'BDCs']
        product_summary = product_summary.sort_values('Total Volume (LT/KG)', ascending=False)
     
        col1, col2 = st.columns([2, 1])
        with col1:
            st.dataframe(product_summary, width='stretch', hide_index=True)
        with col2:
            for _, row in product_summary.iterrows():
                pct = (row['Total Volume (LT/KG)'] / product_summary['Total Volume (LT/KG)'].sum()) * 100
                st.metric(row['Product'], f"{pct:.1f}%")
     
        st.markdown("---")
     
        st.markdown("<h3>🏦 BDC SUMMARY</h3>", unsafe_allow_html=True)
        bdc_summary = df.groupby('BDC').agg({
            'Quantity': 'sum',
            'Order Number': 'count',
            'Product': lambda x: x.nunique(),
            'Depot': lambda x: x.nunique()
        }).reset_index()
        bdc_summary.columns = ['BDC', 'Total Volume (LT/KG)', 'Orders', 'Products', 'Depots']
        bdc_summary = bdc_summary.sort_values('Total Volume (LT/KG)', ascending=False)
     
        st.dataframe(bdc_summary, width='stretch', hide_index=True)
     
        if 'OMC' in df.columns and df['OMC'].notna().any():
            st.markdown("<h3>🏢 OMC SUMMARY (MATCHED)</h3>", unsafe_allow_html=True)
            st.info("📌 OMC names matched from OMC Loadings data using order numbers")
         
            omc_summary = df[df['OMC'].notna()].groupby('OMC').agg({
                'Quantity': 'sum',
                'Order Number': 'count',
                'Product': lambda x: x.nunique(),
                'BDC': lambda x: x.nunique()
            }).reset_index()
            omc_summary.columns = ['OMC', 'Total Volume (LT/KG)', 'Orders', 'Products', 'BDCs']
            omc_summary = omc_summary.sort_values('Total Volume (LT/KG)', ascending=False)
         
            st.dataframe(omc_summary, width='stretch', hide_index=True)
         
            st.markdown("---")
        st.markdown("---")
     
        st.markdown("<h3>📊 PRODUCT DISTRIBUTION BY BDC</h3>", unsafe_allow_html=True)
        pivot_data = df.pivot_table(
            index='BDC',
            columns='Product',
            values='Quantity',
            aggfunc='sum',
            fill_value=0
        ).reset_index()
     
        product_cols = [c for c in pivot_data.columns if c != 'BDC']
        pivot_data['TOTAL'] = pivot_data[product_cols].sum(axis=1)
        pivot_data = pivot_data.sort_values('TOTAL', ascending=False)
     
        st.dataframe(pivot_data, width='stretch', hide_index=True)
     
        st.markdown("---")
     
        st.markdown("<h3>📋 ORDER STATUS BREAKDOWN</h3>", unsafe_allow_html=True)
        status_summary = df.groupby('Status').agg({
            'Order Number': 'count',
            'Quantity': 'sum'
        }).reset_index()
        status_summary.columns = ['Status', 'Orders', 'Total Volume (LT/KG)']
        st.dataframe(status_summary, width='stretch', hide_index=True)
     
        st.markdown("---")
     
        st.markdown("<h3>🔍 SEARCH & FILTER</h3>", unsafe_allow_html=True)
     
        col1, col2 = st.columns(2)
     
        with col1:
            search_type = st.selectbox("Search By:", ["Product", "BDC", "Depot", "Status"], key='daily_search_type')
     
        with col2:
            if search_type == "Product":
                search_value = st.selectbox("Select Product:", ['ALL'] + sorted(df['Product'].unique().tolist()), key='daily_product_search')
            elif search_type == "BDC":
                search_value = st.selectbox("Select BDC:", ['ALL'] + sorted(df['BDC'].unique().tolist()), key='daily_bdc_search')
            elif search_type == "Depot":
                search_value = st.selectbox("Select Depot:", ['ALL'] + sorted(df['Depot'].unique().tolist()), key='daily_depot_search')
            else:
                search_value = st.selectbox("Select Status:", ['ALL'] + sorted(df['Status'].unique().tolist()), key='daily_status_search')
     
        if search_value == 'ALL':
            filtered = df
        else:
            if search_type == "Product":
                filtered = df[df['Product'] == search_value]
            elif search_type == "BDC":
                filtered = df[df['BDC'] == search_value]
            elif search_type == "Depot":
                filtered = df[df['Depot'] == search_value]
            else:
                filtered = df[df['Status'] == search_value]
     
        st.markdown(f"<h3>📋 FILTERED DATA: {search_value}</h3>", unsafe_allow_html=True)
     
        if not filtered.empty:
            cols = st.columns(4)
            with cols[0]:
                st.metric("Filtered Orders", f"{len(filtered):,}")
            with cols[1]:
                st.metric("Filtered Volume", f"{filtered['Quantity'].sum():,.0f} LT")
            with cols[2]:
                st.metric("Unique BDCs", f"{filtered['BDC'].nunique()}")
            with cols[3]:
                st.metric("Filtered Value", f"₵{(filtered['Quantity'] * filtered['Price']).sum():,.0f}")
     
        display = filtered[['Date', 'OMC', 'Truck', 'Quantity', 'Order Number', 'BDC', 'Depot', 'Price', 'Product', 'Status']].sort_values(['Product', 'BDC', 'Date'])
        st.dataframe(display, width='stretch', height=400, hide_index=True)
     
        st.markdown("---")
        st.markdown("<h3>💾 EXPORT DATA</h3>", unsafe_allow_html=True)
        path = save_daily_orders_excel(df)
        if path and os.path.exists(path):
            with open(path, 'rb') as f:
                st.download_button("⬇️ DOWNLOAD EXCEL", f, os.path.basename(path), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", width='stretch')
    else:
        st.info("👆 Select a date range and click the button above to fetch daily orders")
def show_market_share():
    st.markdown("<h2>📊 BDC MARKET SHARE ANALYSIS</h2>", unsafe_allow_html=True)
    st.info("🎯 Comprehensive market share analysis: Stock Balance + Sales Volume")
    st.markdown("---")
    has_balance = bool(st.session_state.get('bdc_records'))
    has_loadings = not st.session_state.get('omc_df', pd.DataFrame()).empty
    st.markdown("### 📊 DATA AVAILABILITY")
    col1, col2 = st.columns(2)
    with col1:
        if has_balance:
            balance_df = pd.DataFrame(st.session_state.bdc_records)
            st.success(f"✅ BDC Balance: {len(balance_df)} records")
        else:
            st.warning("⚠️ BDC Balance Data Not Loaded")
    with col2:
        if has_loadings:
            loadings_df = st.session_state.omc_df
            st.success(f"✅ OMC Loadings: {len(loadings_df)} records")
            if 'omc_start_date' in st.session_state and 'omc_end_date' in st.session_state:
                st.caption(f"Period: {st.session_state.omc_start_date.strftime('%Y/%m/%d')} to {st.session_state.omc_end_date.strftime('%Y/%m/%d')}")
        else:
            st.warning("⚠️ OMC Loadings Data Not Loaded")
    if not has_balance and not has_loadings:
        st.error("❌ No data available for market share analysis")
        st.info("Please fetch data from **BDC Balance** and/or **OMC Loadings** sections first.")
        return
    st.markdown("---")
    st.markdown("### 🔍 SELECT BDC FOR ANALYSIS")
    all_bdcs = set()
    if has_balance:
        all_bdcs.update(balance_df['BDC'].unique())
    if has_loadings:
        all_bdcs.update(loadings_df['BDC'].unique())
    all_bdcs = sorted(list(all_bdcs))
    if not all_bdcs:
        st.error("❌ No BDCs found in data")
        return
    selected_bdc = st.selectbox("Choose BDC:", all_bdcs, key='market_share_bdc')
    if not selected_bdc:
        return
    st.markdown("---")
    st.markdown(f"## 📊 COMPREHENSIVE MARKET REPORT: {selected_bdc}")
    st.markdown("---")
    tab1, tab2, tab3 = st.tabs(["📦 Stock Balance", "🚚 Sales Volume", "📊 Combined Analysis"])
    with tab1:
        if not has_balance:
            st.warning("⚠️ BDC Balance data not available. Please fetch it first.")
        else:
            st.markdown("### 📦 STOCK BALANCE MARKET SHARE")
            balance_col = 'ACTUAL BALANCE (LT\\KG)'
            bdc_balance_data = balance_df[balance_df['BDC'] == selected_bdc]
            total_market_stock = balance_df[balance_col].sum()
            bdc_total_stock = bdc_balance_data[balance_col].sum()
            bdc_stock_share = (bdc_total_stock / total_market_stock * 100) if total_market_stock > 0 else 0
            all_bdc_stocks = balance_df.groupby('BDC')[balance_col].sum().sort_values(ascending=False)
            stock_rank = list(all_bdc_stocks.index).index(selected_bdc) + 1 if selected_bdc in all_bdc_stocks.index else 0
            cols = st.columns(3)
            with cols[0]:
                st.markdown(f"""
                <div class='metric-card'>
                    <h2>TOTAL STOCK</h2>
                    <h1>{bdc_total_stock:,.0f}</h1>
                    <p style='color: #888; font-size: 14px; margin: 0;'>LT/KG</p>
                </div>
                """, unsafe_allow_html=True)
            with cols[1]:
                st.markdown(f"""
                <div class='metric-card'>
                    <h2>MARKET SHARE</h2>
                    <h1>{bdc_stock_share:.2f}%</h1>
                    <p style='color: #888; font-size: 14px; margin: 0;'>of Total Stock</p>
                </div>
                """, unsafe_allow_html=True)
            with cols[2]:
                st.markdown(f"""
                <div class='metric-card'>
                    <h2>STOCK RANK</h2>
                    <h1>#{stock_rank}</h1>
                    <p style='color: #888; font-size: 14px; margin: 0;'>out of {len(all_bdc_stocks)}</p>
                </div>
                """, unsafe_allow_html=True)
            st.markdown("---")
            st.markdown("#### 📦 Stock by Product (PMS, AGO, LPG)")
            product_stock_data = []
            for product in ['PREMIUM', 'GASOIL', 'LPG']:
                market_product_stock = balance_df[balance_df['Product'] == product][balance_col].sum()
                bdc_product_stock = bdc_balance_data[bdc_balance_data['Product'] == product][balance_col].sum()
                product_share = (bdc_product_stock / market_product_stock * 100) if market_product_stock > 0 else 0
                product_stock_data.append({
                    'Product': product,
                    'BDC Stock (LT/KG)': bdc_product_stock,
                    'Market Total (LT/KG)': market_product_stock,
                    'Market Share (%)': product_share
                })
            stock_product_df = pd.DataFrame(product_stock_data)
            st.dataframe(stock_product_df, width='stretch', hide_index=True)
            cols = st.columns(3)
            for idx, row in stock_product_df.iterrows():
                with cols[idx]:
                    st.markdown(f"""
                    <div style='background: rgba(22,33,62,0.6); padding: 15px; border-radius: 10px;
                                border: 2px solid #00ffff; margin: 5px 0;'>
                        <h3 style='color: #ff00ff; margin: 0;'>{row['Product']}</h3>
                        <div style='margin-top: 10px;'>
                            <p style='color: #888; margin: 5px 0; font-size: 14px;'>BDC Stock</p>
                            <p style='color: #00ffff; margin: 0; font-size: 20px; font-weight: bold;'>
                                {row['BDC Stock (LT/KG)']:,.0f} LT
                            </p>
                        </div>
                        <div style='margin-top: 10px;'>
                            <p style='color: #888; margin: 5px 0; font-size: 14px;'>Market Share</p>
                            <p style='color: #00ff88; margin: 0; font-size: 24px; font-weight: bold;'>
                                {row['Market Share (%)']:.2f}%
                            </p>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
    with tab2:
        if not has_loadings:
            st.warning("⚠️ OMC Loadings data not available. Please fetch it first.")
        else:
            st.markdown("### 🚚 SALES VOLUME MARKET SHARE")
            if 'omc_start_date' in st.session_state and 'omc_end_date' in st.session_state:
                st.info(f"📅 Analysis Period: {st.session_state.omc_start_date.strftime('%Y/%m/%d')} to {st.session_state.omc_end_date.strftime('%Y/%m/%d')}")
            sales_col = 'Quantity'
            bdc_sales_data = loadings_df[loadings_df['BDC'] == selected_bdc]
            total_market_sales = loadings_df[sales_col].sum()
            bdc_total_sales = bdc_sales_data[sales_col].sum()
            bdc_sales_share = (bdc_total_sales / total_market_sales * 100) if total_market_sales > 0 else 0
            all_bdc_sales = loadings_df.groupby('BDC')[sales_col].sum().sort_values(ascending=False)
            sales_rank = list(all_bdc_sales.index).index(selected_bdc) + 1 if selected_bdc in all_bdc_sales.index else 0
            total_bdc_count = len(all_bdc_sales)
            bdc_revenue = (bdc_sales_data[sales_col] * bdc_sales_data['Price']).sum()
            cols = st.columns(4)
            with cols[0]:
                st.markdown(f"""
                <div class='metric-card'>
                    <h2>TOTAL SALES</h2>
                    <h1>{bdc_total_sales:,.0f}</h1>
                    <p style='color: #888; font-size: 14px; margin: 0;'>LT/KG Sold</p>
                </div>
                """, unsafe_allow_html=True)
            with cols[1]:
                st.markdown(f"""
                <div class='metric-card'>
                    <h2>MARKET SHARE</h2>
                    <h1>{bdc_sales_share:.2f}%</h1>
                    <p style='color: #888; font-size: 14px; margin: 0;'>of Total Sales</p>
                </div>
                """, unsafe_allow_html=True)
            with cols[2]:
                st.markdown(f"""
                <div class='metric-card'>
                    <h2>OVERALL RANK</h2>
                    <h1>#{sales_rank}</h1>
                    <p style='color: #888; font-size: 14px; margin: 0;'>out of {total_bdc_count} BDCs</p>
                </div>
                """, unsafe_allow_html=True)
            with cols[3]:
                st.markdown(f"""
                <div class='metric-card'>
                    <h2>REVENUE</h2>
                    <h1>₵{bdc_revenue/1000000:,.1f}M</h1>
                    <p style='color: #888; font-size: 14px; margin: 0;'>Total Value</p>
                </div>
                """, unsafe_allow_html=True)
            st.markdown("---")
            product_rank_lookup = {}
            for product in ['PREMIUM', 'GASOIL', 'LPG']:
                prod_bdc_sales = (
                    loadings_df[loadings_df['Product'] == product]
                    .groupby('BDC')[sales_col]
                    .sum()
                    .sort_values(ascending=False)
                )
                if selected_bdc in prod_bdc_sales.index:
                    rank = list(prod_bdc_sales.index).index(selected_bdc) + 1
                else:
                    rank = None
                product_rank_lookup[product] = {
                    'rank': rank,
                    'total': len(prod_bdc_sales)
                }
            st.markdown("#### 🚚 Sales by Product (PMS, AGO, LPG)")
            product_sales_data = []
            for product in ['PREMIUM', 'GASOIL', 'LPG']:
                market_product_sales = loadings_df[loadings_df['Product'] == product][sales_col].sum()
                bdc_product_sales = bdc_sales_data[bdc_sales_data['Product'] == product][sales_col].sum()
                product_share = (bdc_product_sales / market_product_sales * 100) if market_product_sales > 0 else 0
                bdc_orders = len(bdc_sales_data[bdc_sales_data['Product'] == product])
                rank_info = product_rank_lookup[product]
                rank_str = (
                    f"#{rank_info['rank']} / {rank_info['total']}"
                    if rank_info['rank'] is not None
                    else f"N/A / {rank_info['total']}"
                )
                product_sales_data.append({
                    'Product': product,
                    'BDC Sales (LT/KG)': bdc_product_sales,
                    'Market Total (LT/KG)': market_product_sales,
                    'Market Share (%)': round(product_share, 2),
                    'Rank (by Product)': rank_str,
                    'Orders': bdc_orders
                })
            sales_product_df = pd.DataFrame(product_sales_data)
            st.dataframe(sales_product_df, width='stretch', hide_index=True)
            st.markdown("#### 🏆 Per-Product BDC Rankings (Top 5)")
            rank_cols = st.columns(3)
            for col_idx, product in enumerate(['PREMIUM', 'GASOIL', 'LPG']):
                with rank_cols[col_idx]:
                    prod_bdc_sales = (
                        loadings_df[loadings_df['Product'] == product]
                        .groupby('BDC')[sales_col]
                        .sum()
                        .sort_values(ascending=False)
                        .head(5)
                        .reset_index()
                    )
                    prod_bdc_sales.columns = ['BDC', 'Volume (LT)']
                    prod_bdc_sales.insert(0, 'Rank', range(1, len(prod_bdc_sales) + 1))
                    rows_html = ""
                    for _, r in prod_bdc_sales.iterrows():
                        is_selected = (r['BDC'] == selected_bdc)
                        row_color = "#ff00ff" if is_selected else "#e0e0e0"
                        bg_color = "rgba(255,0,255,0.15)" if is_selected else "transparent"
                        medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(int(r['Rank']), "")
                        rows_html += f"""
                        <tr style='background:{bg_color};'>
                            <td style='color:{row_color}; padding:6px 8px; font-weight:{"bold" if is_selected else "normal"};'>
                                {medal} #{int(r['Rank'])}
                            </td>
                            <td style='color:{row_color}; padding:6px 8px; font-weight:{"bold" if is_selected else "normal"};
                                       max-width:140px; overflow:hidden; white-space:nowrap; text-overflow:ellipsis;'
                                title='{r["BDC"]}'>
                                {r['BDC'][:18] + "…" if len(r['BDC']) > 18 else r['BDC']}
                            </td>
                            <td style='color:{row_color}; padding:6px 8px; text-align:right; font-weight:{"bold" if is_selected else "normal"};'>
                                {r['Volume (LT)']:,.0f}
                            </td>
                        </tr>"""
                    rank_info = product_rank_lookup[product]
                    selected_not_in_top5 = selected_bdc not in prod_bdc_sales['BDC'].values
                    if selected_not_in_top5 and rank_info['rank'] is not None:
                        bdc_vol = bdc_sales_data[bdc_sales_data['Product'] == product][sales_col].sum()
                        rows_html += f"""
                        <tr>
                            <td colspan='3' style='color:#888; padding:4px 8px; font-size:12px;'>⋯ {rank_info['rank'] - 5} more BDCs ⋯</td>
                        </tr>
                        <tr style='background:rgba(255,0,255,0.15);'>
                            <td style='color:#ff00ff; padding:6px 8px; font-weight:bold;'>#{rank_info['rank']}</td>
                            <td style='color:#ff00ff; padding:6px 8px; font-weight:bold;'
                                title='{selected_bdc}'>
                                {selected_bdc[:18] + "…" if len(selected_bdc) > 18 else selected_bdc}
                            </td>
                            <td style='color:#ff00ff; padding:6px 8px; text-align:right; font-weight:bold;'>
                                {bdc_vol:,.0f}
                            </td>
                        </tr>"""
                    st.markdown(f"""
                    <div style='background:rgba(22,33,62,0.6); border-radius:10px;
                                border:2px solid #00ffff; padding:12px; margin-bottom:8px;'>
                        <p style='color:#00ffff; font-family:Orbitron,sans-serif; font-size:14px;
                                  margin:0 0 8px 0; font-weight:bold; text-align:center;'>{product}</p>
                        <table style='width:100%; border-collapse:collapse; font-family:Rajdhani,sans-serif; font-size:13px;'>
                            <thead>
                                <tr style='border-bottom:1px solid rgba(0,255,255,0.3);'>
                                    <th style='color:#888; padding:4px 8px; text-align:left;'>Rank</th>
                                    <th style='color:#888; padding:4px 8px; text-align:left;'>BDC</th>
                                    <th style='color:#888; padding:4px 8px; text-align:right;'>Volume (LT)</th>
                                </tr>
                            </thead>
                            <tbody>{rows_html}</tbody>
                        </table>
                    </div>
                    """, unsafe_allow_html=True)
            st.markdown("---")
            st.markdown("#### 📊 Product Detail Cards")
            cols = st.columns(3)
            for idx, row in enumerate(product_sales_data):
                rank_info = product_rank_lookup[row['Product']]
                rank_display = (
                    f"#{rank_info['rank']} of {rank_info['total']}"
                    if rank_info['rank'] is not None
                    else "No sales"
                )
                with cols[idx]:
                    st.markdown(f"""
                    <div style='background: rgba(22,33,62,0.6); padding: 15px; border-radius: 10px;
                                border: 2px solid #ff00ff; margin: 5px 0;'>
                        <h3 style='color: #00ffff; margin: 0;'>{row['Product']}</h3>
                        <div style='margin-top: 10px;'>
                            <p style='color: #888; margin: 5px 0; font-size: 14px;'>BDC Sales</p>
                            <p style='color: #00ffff; margin: 0; font-size: 20px; font-weight: bold;'>
                                {row['BDC Sales (LT/KG)']:,.0f} LT
                            </p>
                        </div>
                        <div style='margin-top: 10px;'>
                            <p style='color: #888; margin: 5px 0; font-size: 14px;'>Market Share</p>
                            <p style='color: #00ff88; margin: 0; font-size: 24px; font-weight: bold;'>
                                {row['Market Share (%)']:.2f}%
                            </p>
                        </div>
                        <div style='margin-top: 10px;'>
                            <p style='color: #888; margin: 5px 0; font-size: 14px;'>Rank (this product)</p>
                            <p style='color: #ffaa00; margin: 0; font-size: 20px; font-weight: bold;'>
                                {rank_display}
                            </p>
                        </div>
                        <div style='margin-top: 10px;'>
                            <p style='color: #888; margin: 5px 0; font-size: 14px;'>Orders</p>
                            <p style='color: #ffffff; margin: 0; font-size: 16px;'>
                                {row['Orders']:,}
                            </p>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
    with tab3:
        st.markdown("### 📊 STOCK vs SALES COMPARISON")
        if not has_balance or not has_loadings:
            st.warning("⚠️ Both BDC Balance and OMC Loadings data required for combined analysis")
            st.info("Please fetch both datasets to see the complete picture.")
        else:
            st.markdown("#### 🎯 Performance Overview")
            cols = st.columns(2)
            with cols[0]:
                st.markdown(f"""
                <div style='background: rgba(22,33,62,0.6); padding: 20px; border-radius: 15px;
                            border: 2px solid #00ffff;'>
                    <h3 style='color: #00ffff; margin: 0;'>📦 STOCK POSITION</h3>
                    <p style='color: #ffffff; margin: 10px 0; font-size: 28px; font-weight: bold;'>
                        {bdc_total_stock:,.0f} LT
                    </p>
                    <p style='color: #00ff88; margin: 5px 0; font-size: 20px;'>
                        {bdc_stock_share:.2f}% Market Share
                    </p>
                    <p style='color: #888; margin: 5px 0;'>
                        Rank #{stock_rank} in Stock
                    </p>
                </div>
                """, unsafe_allow_html=True)
            with cols[1]:
                st.markdown(f"""
                <div style='background: rgba(22,33,62,0.6); padding: 20px; border-radius: 15px;
                            border: 2px solid #ff00ff;'>
                    <h3 style='color: #ff00ff; margin: 0;'>🚚 SALES VOLUME</h3>
                    <p style='color: #ffffff; margin: 10px 0; font-size: 28px; font-weight: bold;'>
                        {bdc_total_sales:,.0f} LT
                    </p>
                    <p style='color: #00ff88; margin: 5px 0; font-size: 20px;'>
                        {bdc_sales_share:.2f}% Market Share
                    </p>
                    <p style='color: #888; margin: 5px 0;'>
                        Rank #{sales_rank} in Sales
                    </p>
                </div>
                """, unsafe_allow_html=True)
            st.markdown("---")
            st.markdown("#### 📊 Stock vs Sales by Product")
            comparison_data = []
            for product in ['PREMIUM', 'GASOIL', 'LPG']:
                bdc_stock = stock_product_df[stock_product_df['Product'] == product]['BDC Stock (LT/KG)'].values[0] if len(stock_product_df) > 0 else 0
                stock_share = stock_product_df[stock_product_df['Product'] == product]['Market Share (%)'].values[0] if len(stock_product_df) > 0 else 0
                bdc_sales_row = next((r for r in product_sales_data if r['Product'] == product), {})
                bdc_sales_vol = bdc_sales_row.get('BDC Sales (LT/KG)', 0)
                sales_share = bdc_sales_row.get('Market Share (%)', 0)
                rank_info = product_rank_lookup[product]
                rank_str = (
                    f"#{rank_info['rank']} / {rank_info['total']}"
                    if rank_info['rank'] is not None
                    else f"N/A / {rank_info['total']}"
                )
                comparison_data.append({
                    'Product': product,
                    'Stock (LT)': bdc_stock,
                    'Stock Share (%)': stock_share,
                    'Sales (LT)': bdc_sales_vol,
                    'Sales Share (%)': sales_share,
                    'Sales Rank (Product)': rank_str,
                    'Stock/Sales Ratio': f"{(bdc_stock / bdc_sales_vol):.2f}x" if bdc_sales_vol > 0 else "N/A"
                })
            comparison_df = pd.DataFrame(comparison_data)
            st.dataframe(comparison_df, width='stretch', hide_index=True)
            st.markdown("---")
            st.markdown("### 💾 EXPORT COMPLETE REPORT")
            if st.button("📄 GENERATE EXCEL REPORT", width='stretch'):
                output_dir = os.path.join(os.getcwd(), "market_share_reports")
                os.makedirs(output_dir, exist_ok=True)
                filename = f"market_share_{selected_bdc}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                filepath = os.path.join(output_dir, filename)
                with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                    stock_product_df.to_excel(writer, sheet_name='Stock Analysis', index=False)
                    pd.DataFrame(product_sales_data).to_excel(writer, sheet_name='Sales Analysis', index=False)
                    comparison_df.to_excel(writer, sheet_name='Stock vs Sales', index=False)
                st.success(f"✅ Report generated: {filename}")
                with open(filepath, 'rb') as f:
                    st.download_button(
                        "⬇️ DOWNLOAD REPORT",
                        f,
                        filename,
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        width='stretch'
                    )
def show_competitive_intel():
    st.markdown("<h2>🎯 COMPETITIVE INTELLIGENCE CENTER</h2>", unsafe_allow_html=True)
    st.info("🔥 Advanced analytics: Anomaly Detection, Price Intelligence, Performance Scoring & Trend Forecasting")
    st.markdown("---")
 
    has_loadings = not st.session_state.get('omc_df', pd.DataFrame()).empty
 
    if not has_loadings:
        st.warning("⚠️ OMC Loadings data required for competitive intelligence")
        st.info("Please fetch OMC Loadings data first to unlock these features!")
        return
 
    loadings_df = st.session_state.omc_df
 
    tab1, tab2, tab3 = st.tabs([
        "🚨 Anomaly Detection",
        "💰 Price Intelligence",
        "⭐ Performance Score & Rankings"
    ])
 
    with tab1:
        st.markdown("### 🚨 ANOMALY DETECTION ENGINE")
        st.caption("Automatically detect unusual patterns in orders and pricing")
     
        mean_vol = loadings_df['Quantity'].mean()
        std_vol = loadings_df['Quantity'].std()
        anomaly_threshold = mean_vol + (2 * std_vol)
        volume_anomalies = loadings_df[loadings_df['Quantity'] > anomaly_threshold]
     
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Volume Anomalies", len(volume_anomalies))
        with col2:
            st.metric("Anomalous Volume", f"{volume_anomalies['Quantity'].sum():,.0f} LT")
        with col3:
            st.metric("Threshold", f"{anomaly_threshold:,.0f} LT")
     
        if not volume_anomalies.empty:
            st.warning(f"🚨 {len(volume_anomalies)} abnormally large orders detected!")
            top_anomalies = volume_anomalies.nlargest(10, 'Quantity')[
                ['Date', 'BDC', 'OMC', 'Product', 'Quantity', 'Order Number']
            ]
            st.dataframe(top_anomalies, width='stretch', hide_index=True)
     
        st.markdown("---")
     
        st.markdown("#### 💰 Price Anomalies by Product")
        price_data = []
        for product in ['PREMIUM', 'GASOIL', 'LPG']:
            pdf = loadings_df[loadings_df['Product'] == product]
            if len(pdf) > 0:
                pmean = pdf['Price'].mean()
                pstd = pdf['Price'].std()
                high_anom = len(pdf[pdf['Price'] > pmean + (2 * pstd)])
                low_anom = len(pdf[pdf['Price'] < pmean - (2 * pstd)])
             
                price_data.append({
                    'Product': product,
                    'Avg Price': f"₵{pmean:.2f}",
                    'High Price Anomalies': high_anom,
                    'Low Price Anomalies': low_anom,
                    'Total Anomalies': high_anom + low_anom
                })
     
        st.dataframe(pd.DataFrame(price_data), width='stretch', hide_index=True)
 
    with tab2:
        st.markdown("### 💰 PRICE INTELLIGENCE DASHBOARD")
     
        price_stats = loadings_df.groupby(['BDC', 'Product'])['Price'].agg(['mean', 'min', 'max']).reset_index()
        price_stats.columns = ['BDC', 'Product', 'Avg Price', 'Min Price', 'Max Price']
     
        overall_mean = loadings_df['Price'].mean()
        price_stats['Tier'] = price_stats['Avg Price'].apply(
            lambda x: '🔴 Premium' if x > overall_mean * 1.1 else '🟢 Competitive'
        )
     
        st.dataframe(price_stats.sort_values('Avg Price', ascending=False), width='stretch', hide_index=True)
     
        st.markdown("---")
     
        st.markdown("#### 💡 Best Pricing Opportunities")
        opportunities = []
        for product in ['PREMIUM', 'GASOIL', 'LPG']:
            pdf = loadings_df[loadings_df['Product'] == product]
            if len(pdf) > 0:
                bdc_prices = pdf.groupby('BDC')['Price'].mean()
                min_bdc = bdc_prices.idxmin()
                max_bdc = bdc_prices.idxmax()
             
                opportunities.append({
                    'Product': product,
                    'Lowest': f"{min_bdc} (₵{bdc_prices.min():.2f})",
                    'Highest': f"{max_bdc} (₵{bdc_prices.max():.2f})",
                    'Gap': f"₵{(bdc_prices.max() - bdc_prices.min()):.2f}"
                })
     
        st.dataframe(pd.DataFrame(opportunities), width='stretch', hide_index=True)
 
    with tab3:
        st.markdown("### ⭐ BDC PERFORMANCE LEADERBOARD")
     
        scores = []
        for bdc in loadings_df['BDC'].unique():
            bdc_df = loadings_df[loadings_df['BDC'] == bdc]
         
            vol = bdc_df['Quantity'].sum()
            max_vol = loadings_df.groupby('BDC')['Quantity'].sum().max()
            vol_score = (vol / max_vol) * 40
         
            orders = len(bdc_df)
            max_orders = loadings_df.groupby('BDC').size().max()
            order_score = (orders / max_orders) * 30
         
            products = bdc_df['Product'].nunique()
            diversity_score = (products / 3) * 30
         
            total = vol_score + order_score + diversity_score
            grade = 'A+' if total >= 90 else 'A' if total >= 80 else 'B' if total >= 70 else 'C' if total >= 60 else 'D'
         
            scores.append({
                'BDC': bdc,
                'Volume Score': round(vol_score, 1),
                'Orders Score': round(order_score, 1),
                'Diversity Score': round(diversity_score, 1),
                'Total Score': round(total, 1),
                'Grade': grade
            })
     
        scores_df = pd.DataFrame(scores).sort_values('Total Score', ascending=False)
        scores_df.insert(0, 'Rank', range(1, len(scores_df) + 1))
        scores_df['Medal'] = scores_df['Rank'].apply(lambda x: '🥇' if x==1 else '🥈' if x==2 else '🥉' if x==3 else '')
     
        st.dataframe(scores_df, width='stretch', hide_index=True)
     
        st.markdown("---")
     
        st.markdown("#### 🏆 TOP 3 CHAMPIONS")
        cols = st.columns(3)
        for idx, (_, row) in enumerate(scores_df.head(3).iterrows()):
            with cols[idx]:
                border_color = "#FFD700" if idx==0 else "#C0C0C0" if idx==1 else "#CD7F32"
                st.markdown(f"""
                <div style='background: rgba(22,33,62,0.6); padding: 20px; border-radius: 15px;
                            border: 3px solid {border_color}; text-align: center;'>
                    <p style='font-size: 48px; margin: 0;'>{row['Medal']}</p>
                    <h3 style='color: #00ffff; margin: 10px 0;'>{row['BDC']}</h3>
                    <p style='color: #00ff88; font-size: 32px; margin: 10px 0;'>{row['Total Score']:.1f}</p>
                    <p style='color: #ffffff; font-size: 24px; margin: 5px 0;'>Grade: {row['Grade']}</p>
                </div>
                """, unsafe_allow_html=True)
     
        st.markdown("---")
     
        st.markdown("#### 🔍 Check Any BDC")
        selected = st.selectbox("Select BDC:", scores_df['BDC'].unique())
     
        if selected:
            bdc_score = scores_df[scores_df['BDC'] == selected].iloc[0]
         
            st.markdown(f"""
            <div style='background: rgba(22,33,62,0.6); padding: 30px; border-radius: 15px;
                        border: 2px solid #00ffff; text-align: center; margin: 20px 0;'>
                <h2 style='color: #ff00ff; margin: 0;'>{selected}</h2>
                <p style='color: #ffffff; font-size: 64px; margin: 20px 0;'>{bdc_score['Total Score']:.1f}/100</p>
                <p style='color: #00ff88; font-size: 36px; margin: 10px 0;'>Grade: {bdc_score['Grade']}</p>
                <p style='color: #888; margin: 10px 0;'>Rank #{int(bdc_score['Rank'])} of {len(scores_df)}</p>
            </div>
            """, unsafe_allow_html=True)
         
            cols = st.columns(3)
            with cols[0]:
                st.metric("Volume Score", f"{bdc_score['Volume Score']:.1f}/40")
            with cols[1]:
                st.metric("Orders Score", f"{bdc_score['Orders Score']:.1f}/30")
            with cols[2]:
                st.metric("Diversity Score", f"{bdc_score['Diversity Score']:.1f}/30")
def show_stock_transaction():
    st.markdown("<h2>📈 STOCK TRANSACTION ANALYZER</h2>", unsafe_allow_html=True)
    st.info("🔥 Track BDC transactions: Inflows, Outflows, Sales & Intelligent Stockout Forecasting")
    st.markdown("---")
    if 'stock_txn_df' not in st.session_state:
        st.session_state.stock_txn_df = pd.DataFrame()
    tab1, tab2 = st.tabs(["🔍 BDC Transaction Report", "📊 Stockout Analysis"])
    with tab1:
        st.markdown("### 🔍 BDC TRANSACTION REPORT")
        st.info("Get detailed transaction history for any BDC at a specific depot")
        col1, col2 = st.columns(2)
        with col1:
            selected_bdc = st.selectbox("Select BDC:", sorted(BDC_MAP.keys()))
            selected_product = st.selectbox("Select Product:", PRODUCT_OPTIONS)
        with col2:
            selected_depot = st.selectbox("Select Depot:", sorted(DEPOT_MAP.keys()))
        col3, col4 = st.columns(2)
        with col3:
            start_date = st.date_input("Start Date:", value=datetime.now() - timedelta(days=30))
        with col4:
            end_date = st.date_input("End Date:", value=datetime.now())
        if st.button("📊 FETCH TRANSACTION REPORT", width='stretch'):
            with st.spinner("🔄 Fetching stock transaction data..."):
                bdc_id = BDC_MAP[selected_bdc]
                depot_id = DEPOT_MAP[selected_depot]
                product_id = STOCK_PRODUCT_MAP[selected_product]
                url = NPA_CONFIG['STOCK_TRANSACTION_URL']
                params = {
                    'lngProductId': product_id,
                    'lngBDCId': bdc_id,
                    'lngDepotId': depot_id,
                    'dtpStartDate': start_date.strftime('%m/%d/%Y'),
                    'dtpEndDate': end_date.strftime('%m/%d/%Y'),
                    'lngUserId': NPA_CONFIG['USER_ID']
                }
                st.info(f"🔍 Requesting: {selected_bdc} → {selected_depot} → {selected_product}")
                try:
                    import requests
                    import io
                    response = requests.get(
                        url, params=params,
                        headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'application/pdf'},
                        timeout=30
                    )
                    response.raise_for_status()
                    if response.content[:4] != b'%PDF':
                        st.error("❌ Response is not a PDF")
                        st.code(response.text[:500])
                        st.session_state.stock_txn_df = pd.DataFrame()
                    else:
                        st.success(f"✅ PDF received ({len(response.content):,} bytes)")
                        records = _parse_stock_transaction_pdf(io.BytesIO(response.content))
                        if records:
                            df = pd.DataFrame(records)
                            st.session_state.stock_txn_df = df
                            st.session_state.stock_txn_bdc = selected_bdc
                            st.session_state.stock_txn_depot = selected_depot
                            st.session_state.stock_txn_product = selected_product
                            st.success(f"✅ Extracted {len(df)} transactions!")
                        else:
                            st.warning("⚠️ No transactions found for this date range / selection.")
                            st.session_state.stock_txn_df = pd.DataFrame()
                except Exception as e:
                    st.error(f"❌ Error: {e}")
                    import traceback
                    st.code(traceback.format_exc())
                    st.session_state.stock_txn_df = pd.DataFrame()
        df = st.session_state.stock_txn_df
        if not df.empty:
            st.markdown("---")
            st.markdown(f"### 📊 TRANSACTION ANALYSIS: {st.session_state.get('stock_txn_bdc', '')}")
            st.caption(
                f"Depot: {st.session_state.get('stock_txn_depot', '')} | "
                f"Product: {st.session_state.get('stock_txn_product', '')}"
            )
            inflows = df[df['Description'].isin(['Custody Transfer In', 'Product Outturn'])]['Volume'].sum()
            outflows = df[df['Description'].isin(['Sale', 'Custody Transfer Out'])]['Volume'].sum()
            sales = df[df['Description'] == 'Sale']['Volume'].sum()
            bdc_transfer = df[df['Description'] == 'Custody Transfer Out']['Volume'].sum()
            final_bal = df['Balance'].iloc[-1] if len(df) > 0 else 0
            cols = st.columns(5)
            with cols[0]: st.metric("📥 Inflows", f"{inflows:,.0f} LT")
            with cols[1]: st.metric("📤 Outflows", f"{outflows:,.0f} LT")
            with cols[2]: st.metric("💰 Sales to OMCs",f"{sales:,.0f} LT")
            with cols[3]: st.metric("🔄 BDC Transfers",f"{bdc_transfer:,.0f} LT")
            with cols[4]: st.metric("📊 Final Balance", f"{final_bal:,.0f} LT")
            st.markdown("---")
            st.markdown("### 📋 Transaction Breakdown")
            txn_summary = (
                df.groupby('Description')
                  .agg(Total_Volume=('Volume', 'sum'), Count=('Trans #', 'count'))
                  .reset_index()
                  .rename(columns={'Description': 'Transaction Type',
                                   'Total_Volume': 'Total Volume (LT)',
                                   'Count': 'Count'})
                  .sort_values('Total Volume (LT)', ascending=False)
            )
            st.dataframe(txn_summary, width='stretch', hide_index=True)
            st.markdown("---")
            if sales > 0:
                st.markdown("### 🏢 Top Customers (OMC Sales)")
                sales_df = df[df['Description'] == 'Sale']
                customer_summary = (
                    sales_df.groupby('Account')['Volume']
                    .sum()
                    .sort_values(ascending=False)
                    .head(10)
                    .reset_index()
                )
                customer_summary.columns = ['Customer', 'Volume Sold (LT)']
                st.dataframe(customer_summary, width='stretch', hide_index=True)
                st.markdown("---")
            st.markdown("### 📄 Full Transaction History")
            st.dataframe(df, width='stretch', hide_index=True, height=400)
            st.markdown("---")
            if st.button("💾 EXPORT TO EXCEL", width='stretch'):
                output_dir = os.path.join(os.getcwd(), "stock_transactions")
                os.makedirs(output_dir, exist_ok=True)
                filename = (
                    f"stock_txn_{st.session_state.get('stock_txn_bdc', 'export')}_"
                    f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                )
                filepath = os.path.join(output_dir, filename)
                with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name='Transactions', index=False)
                    txn_summary.to_excel(writer, sheet_name='Summary', index=False)
                with open(filepath, 'rb') as f:
                    st.download_button(
                        "⬇️ DOWNLOAD", f, filename,
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        width='stretch'
                    )
        else:
            st.info("👆 Select options and click the button above to fetch transaction data")
    with tab2:
        st.markdown("### 📊 INTELLIGENT STOCKOUT FORECASTING")
        st.info("Predict when stock will run out based on current balance and sales velocity")
        has_balance = bool(st.session_state.get('bdc_records'))
        has_transactions = not st.session_state.stock_txn_df.empty
        col1, col2 = st.columns(2)
        with col1:
            if has_balance: st.success("✅ BDC Balance Data Available")
            else: st.warning("⚠️ BDC Balance Data Required")
        with col2:
            if has_transactions: st.success("✅ Transaction Data Available")
            else: st.warning("⚠️ Transaction Data Required")
        if not has_balance:
            st.info("💡 **Step 1:** Fetch BDC Balance data from the BDC Balance section first")
        if not has_transactions:
            st.info("💡 **Step 2:** Fetch transaction data from 'BDC Transaction Report' tab first")
        if has_balance and has_transactions:
            st.markdown("---")
            balance_df = pd.DataFrame(st.session_state.bdc_records)
            txn_df = st.session_state.stock_txn_df
            bdc_name = st.session_state.get('stock_txn_bdc', '')
            depot_name = st.session_state.get('stock_txn_depot', '')
            selected_product_display = st.session_state.get('stock_txn_product', '')
            product_name = PRODUCT_BALANCE_MAP.get(selected_product_display, selected_product_display)
            bdc_balance = balance_df[
                (balance_df['BDC'].str.contains(bdc_name, case=False, na=False)) &
                (balance_df['Product'].str.contains(product_name, case=False, na=False))
            ]
            if not bdc_balance.empty:
                current_stock = bdc_balance['ACTUAL BALANCE (LT\\KG)'].sum()
                total_sales = txn_df[
                    txn_df['Description'].isin(['Sale', 'Custody Transfer Out'])
                ]['Volume'].sum()
                txn_copy = txn_df.copy()
                txn_copy['_dt'] = pd.to_datetime(txn_copy['Date'], format='%d/%m/%Y', errors='coerce')
                date_range_days = (txn_copy['_dt'].max() - txn_copy['_dt'].min()).days or 1
                daily_sales_rate = total_sales / date_range_days if date_range_days > 0 else 0
                days_remaining = (current_stock / daily_sales_rate
                                    if daily_sales_rate > 0 else float('inf'))
                if days_remaining < 7: status, sc = "🔴 CRITICAL", "red"
                elif days_remaining < 14: status, sc = "🟡 WARNING", "orange"
                else: status, sc = "🟢 HEALTHY", "green"
                st.markdown(f"### {status} - Stockout Forecast")
                cols = st.columns(4)
                with cols[0]:
                    st.markdown(f"<div class='metric-card'><h2>CURRENT STOCK</h2>"
                                f"<h1>{current_stock:,.0f}</h1>"
                                f"<p style='color:#888;font-size:14px;margin:0;'>LT/KG</p></div>",
                                unsafe_allow_html=True)
                with cols[1]:
                    st.markdown(f"<div class='metric-card'><h2>DAILY SALES RATE</h2>"
                                f"<h1>{daily_sales_rate:,.0f}</h1>"
                                f"<p style='color:#888;font-size:14px;margin:0;'>LT/KG per day</p></div>",
                                unsafe_allow_html=True)
                with cols[2]:
                    days_text = f"{days_remaining:.1f}" if days_remaining != float('inf') else "∞"
                    st.markdown(f"<div class='metric-card' style='border-color:{sc};'>"
                                f"<h2>DAYS REMAINING</h2><h1>{days_text}</h1>"
                                f"<p style='color:#888;font-size:14px;margin:0;'>days</p></div>",
                                unsafe_allow_html=True)
                with cols[3]:
                    st.markdown(f"<div class='metric-card'><h2>ANALYSIS PERIOD</h2>"
                                f"<h1>{date_range_days}</h1>"
                                f"<p style='color:#888;font-size:14px;margin:0;'>days</p></div>",
                                unsafe_allow_html=True)
                st.markdown("---")
                st.markdown("### 📊 Detailed Analysis")
                stockout_date = (
                    (datetime.now() + timedelta(days=days_remaining)).strftime('%Y-%m-%d')
                    if days_remaining != float('inf') else "N/A"
                )
                analysis_df = pd.DataFrame({
                    'Metric': ['BDC','Depot','Product','Current Stock (LT)',
                               'Total Sales (Period)','Analysis Period (days)',
                               'Daily Sales Rate','Days Until Stockout',
                               'Projected Stockout Date','Status'],
                    'Value': [bdc_name, depot_name, product_name,
                               f"{current_stock:,.0f}", f"{total_sales:,.0f}",
                               f"{date_range_days}", f"{daily_sales_rate:,.0f} LT/day",
                               f"{days_remaining:.1f} days" if days_remaining != float('inf') else "No depletion expected",
                               stockout_date, status]
                })
                st.dataframe(analysis_df, width='stretch', hide_index=True)
                st.markdown("---")
                st.markdown("### 💡 RECOMMENDATIONS")
                if days_remaining < 7:
                    st.error("**🚨 IMMEDIATE ACTION REQUIRED:**\n"
                             "- Critical stock level — replenishment urgent\n"
                             "- Expected stockout in less than 7 days\n"
                             "- Consider emergency procurement or transfers")
                elif days_remaining < 14:
                    st.warning("**⚠️ ACTION RECOMMENDED:**\n"
                               "- Stock level below safety threshold\n"
                               "- Expected stockout in 7–14 days\n"
                               "- Plan replenishment within next week")
                else:
                    st.success("**✅ STOCK LEVELS HEALTHY:**\n"
                               "- Current stock sufficient for 14+ days\n"
                               "- Continue normal operations\n"
                               "- Monitor sales trends")
            else:
                st.warning(f"⚠️ No balance data found for {bdc_name} — {product_name}")
                st.info("Make sure the BDC name and product match between Balance and Transaction data")

# ═══════════════════════════════════════════════════════════════════════════════
# NATIONAL STOCKOUT — OMC LOADINGS ONLY (2 API CALLS)
# ─────────────────────────────────────────────────────────────────────────────
# Methodology:
#   National Stock   = BDC Balance (all BDCs, current snapshot)         [1 call]
#   National Depletion = OMC Loadings (all BDCs→OMC, released orders)  [1 call]
#
# Why NOT Custody Transfer Out:
#   CTO is a BDC re-routing fuel internally from one BDC to another.
#   The fuel does NOT leave the national system — it stays within Ghana's
#   wholesale petroleum network. Only OMC Loadings represents fuel truly
#   flowing out of the wholesale system to retail / end consumers.
#
# Days of Supply = National Stock ÷ (Total OMC Loadings ÷ period_days)
# ═══════════════════════════════════════════════════════════════════════════════

import io
import requests as _requests

# ── Low-level helpers ─────────────────────────────────────────────────────────

def _fetch_pdf_bytes(url: str, params: dict, timeout: int = 45):
    """GET a URL; return raw PDF bytes or None."""
    _headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Safari/537.36'
        ),
        'Accept': 'application/pdf,text/html,*/*;q=0.8',
    }
    try:
        r = _requests.get(url, params=params, headers=_headers, timeout=timeout)
        r.raise_for_status()
        return r.content if r.content[:4] == b'%PDF' else None
    except Exception:
        return None


# ── OMC Loadings national fetch ───────────────────────────────────────────────

def _fetch_national_omc_loadings(start_str: str, end_str: str,
                                  progress_cb=None) -> pd.DataFrame:
    """
    Fetch industry-wide OMC loadings by splitting the date range into
    7-day chunks. Each chunk produces a small, manageable PDF.
    Results are concatenated and returned as a single DataFrame.

    progress_cb: optional callable(done, total) for progress updates.
    """
    cfg = NPA_CONFIG

    # Parse dates
    fmt = "%m/%d/%Y"
    d_start = datetime.strptime(start_str, fmt)
    d_end   = datetime.strptime(end_str,   fmt)

    # Build weekly windows
    windows = []
    cursor = d_start
    while cursor <= d_end:
        chunk_end = min(cursor + timedelta(days=6), d_end)
        windows.append((cursor.strftime(fmt), chunk_end.strftime(fmt)))
        cursor = chunk_end + timedelta(days=1)

    all_frames = []
    total = len(windows)

    def _fetch_window(w_start, w_end):
        params = {
            'lngCompanyId':    cfg['COMPANY_ID'],
            'szITSfromPersol': 'persol',
            'strGroupBy':      'BDC',
            'strGroupBy1':     '',
            'strQuery1':       ' and iorderstatus=4',
            'strQuery2':       w_start,
            'strQuery3':       w_end,
            'strQuery4':       '',
            'strPicHeight':    '',
            'strPicWeight':    '',
            'intPeriodID':     '4',
            'iUserId':         cfg['USER_ID'],
            'iAppId':          cfg['APP_ID'],
        }
        pdf_bytes = _fetch_pdf_bytes(cfg['OMC_LOADINGS_URL'], params, timeout=60)
        if not pdf_bytes:
            return pd.DataFrame()
        return extract_npa_data_from_pdf(io.BytesIO(pdf_bytes))

    # Fetch windows in parallel (up to 4 at a time — don't hammer the server)
    completed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_map = {executor.submit(_fetch_window, ws, we): (ws, we)
                      for ws, we in windows}
        for future in concurrent.futures.as_completed(future_map):
            completed += 1
            try:
                chunk_df = future.result()
                if not chunk_df.empty:
                    all_frames.append(chunk_df)
            except Exception:
                pass
            if progress_cb:
                progress_cb(completed, total)

    if not all_frames:
        return pd.DataFrame()
    return pd.concat(all_frames, ignore_index=True).drop_duplicates()



# ── Main page ─────────────────────────────────────────────────────────────────

def show_national_stockout():
    """
    National Stockout Forecast — OMC Loadings Only (2 API calls)
    ─────────────────────────────────────────────────────────────
    National Stock     = BDC Balance (all BDCs, current snapshot)
    National Depletion = OMC Loadings (all BDCs → all OMCs, released orders)
    Days of Supply     = Stock ÷ (OMC Loadings ÷ period_days)

    CTO (Custody Transfer Out) is excluded: it represents fuel moving between
    BDCs internally and does NOT leave the national supply system.
    """
    st.markdown("<h2>🌍 NATIONAL STOCKOUT FORECAST</h2>", unsafe_allow_html=True)

    st.markdown("""
    <div style='background:rgba(0,255,255,0.06); border:1.5px solid #00ffff;
                border-radius:12px; padding:18px 22px; margin-bottom:16px;'>
        <p style='color:#00ffff; font-family:Orbitron,sans-serif; font-size:15px;
                  font-weight:700; margin:0 0 10px;'>📐 METHODOLOGY — 2 API CALLS ONLY</p>
        <table style='width:100%; font-family:Rajdhani,sans-serif; font-size:14px; border-collapse:collapse;'>
            <tr>
                <td style='color:#ffaa00; padding:4px 8px; font-weight:700; white-space:nowrap;'>📦 National Stock</td>
                <td style='color:#e0e0e0; padding:4px 8px;'>BDC Balance report — current stock snapshot across all BDCs &amp; depots <span style='color:#888;'>[Call 1]</span></td>
            </tr>
            <tr>
                <td style='color:#00ff88; padding:4px 8px; font-weight:700; white-space:nowrap;'>🚚 National Depletion</td>
                <td style='color:#e0e0e0; padding:4px 8px;'>OMC Loadings — all released orders from all BDCs to all OMCs <span style='color:#888;'>[Call 2]</span></td>
            </tr>
            <tr style='border-top:1px solid rgba(0,255,255,0.2);'>
                <td style='color:#00ffff; padding:8px 8px 4px; font-weight:700; white-space:nowrap;'>📅 Days of Supply</td>
                <td style='color:#ffffff; padding:8px 8px 4px; font-weight:700;'>National Stock ÷ (OMC Loadings ÷ Period Days)</td>
            </tr>
            <tr>
                <td style='color:#888; padding:4px 8px; font-size:12px; white-space:nowrap;'>❌ CTO excluded</td>
                <td style='color:#888; padding:4px 8px; font-size:12px;'>Custody Transfer Out = internal BDC→BDC accounting only. Fuel stays in Ghana's national system, so it does not reduce supply.</td>
            </tr>
        </table>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    # ── Date range ────────────────────────────────────────────────────────────
    st.markdown("### 📅 SELECT ANALYSIS PERIOD")
    st.caption(
        "The period is used to compute the average daily depletion rate. "
        "30 days gives a stable estimate; shorter windows capture recent trends."
    )

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From", value=datetime.now() - timedelta(days=30), key='ns_start')
    with col2:
        end_date   = st.date_input("To",   value=datetime.now(),                       key='ns_end')

    start_str  = start_date.strftime("%m/%d/%Y")
    end_str    = end_date.strftime("%m/%d/%Y")
    period_days = max((end_date - start_date).days, 1)

    st.info(
        "⚡ **Just 2 API calls.** "
        "Step 1 fetches the current BDC Balance (national stock snapshot). "
        "Step 2 fetches industry-wide OMC Loadings (all released orders — fuel that left the wholesale system). "
        "CTO (Custody Transfer Out) is intentionally excluded: it is an internal BDC accounting entry "
        "that moves fuel between BDC books but does NOT reduce the national supply."
    )

    st.markdown("---")

    if st.button("⚡ FETCH & ANALYSE NATIONAL FUEL SUPPLY", width='stretch'):
        _run_national_analysis(start_str, end_str, period_days)

    if st.session_state.get('ns_results'):
        _display_national_results(period_days)


# ── Orchestration ─────────────────────────────────────────────────────────────

def _run_national_analysis(start_str: str, end_str: str, period_days: int):
    cfg = NPA_CONFIG
    col_bal = 'ACTUAL BALANCE (LT\\KG)'
    DISPLAY = {'PREMIUM': 'PREMIUM (PMS)', 'GASOIL': 'GASOIL (AGO)', 'LPG': 'LPG'}

    # ── STEP 1: BDC Balance (current national stock snapshot) ─────────────────
    with st.status("📡 Step 1 / 2 — Fetching national BDC stock balance…", expanded=True) as status_a:
        st.write("Connecting to NPA portal…")
        bal_params = {
            'lngCompanyId':     cfg['COMPANY_ID'],
            'strITSfromPersol': cfg['ITS_FROM_PERSOL'],
            'strGroupBy':       'BDC',
            'strGroupBy1':      'DEPOT',
            'strQuery1': '', 'strQuery2': '', 'strQuery3': '', 'strQuery4': '',
            'strPicHeight': '1', 'szPicWeight': '1',
            'lngUserId':  cfg['USER_ID'],
            'intAppId':   cfg['APP_ID'],
        }
        bal_bytes = _fetch_pdf_bytes(cfg['BDC_BALANCE_URL'], bal_params)
        if not bal_bytes:
            st.error("❌ Could not fetch BDC Balance PDF. Check network/credentials.")
            status_a.update(label="❌ Balance fetch failed", state="error")
            return

        scraper = StockBalanceScraper()
        bal_records = scraper.parse_pdf_file(io.BytesIO(bal_bytes))
        if not bal_records:
            st.error("❌ No balance records found in PDF.")
            status_a.update(label="❌ No balance records", state="error")
            return

        bal_df = pd.DataFrame(bal_records)
        n_bdcs = bal_df['BDC'].nunique()
        n_rows = len(bal_df)

        # Sum stock by product
        balance_by_product = bal_df.groupby('Product')[col_bal].sum()
        pms_stock  = balance_by_product.get('PREMIUM', 0)
        ago_stock  = balance_by_product.get('GASOIL',  0)
        lpg_stock  = balance_by_product.get('LPG',     0)

        st.write(f"✅ {n_rows} balance rows across **{n_bdcs} BDCs**")
        st.write(
            f"📦 Current stock — "
            f"PMS: **{pms_stock:,.0f} LT** | "
            f"AGO: **{ago_stock:,.0f} LT** | "
            f"LPG: **{lpg_stock:,.0f} LT**"
        )
        status_a.update(label=f"✅ Step 1 done — {n_bdcs} BDCs, stock parsed", state="running")

    # ── STEP 2: OMC Loadings (national depletion — fuel leaving wholesale) ─────
    with st.status("🚚 Step 2 / 2 — Fetching national OMC loadings (chunked by week)…", expanded=True) as status_b:
        # Calculate number of weekly chunks for the user
        from math import ceil
        n_weeks = ceil(period_days / 7)
        st.write(
            f"Splitting **{period_days}-day** period into **{n_weeks} weekly chunks** "
            f"to avoid large PDF crashes. Fetching in parallel (4 workers)…"
        )

        prog_bar   = st.progress(0, text="Starting…")
        prog_text  = st.empty()

        def _on_progress(done, total):
            pct = done / total
            prog_bar.progress(pct, text=f"Week chunk {done}/{total} fetched")
            prog_text.caption(f"✅ {done} / {total} weekly windows complete")

        omc_df = _fetch_national_omc_loadings(start_str, end_str, progress_cb=_on_progress)
        prog_bar.progress(1.0, text="✅ All chunks fetched")

        if omc_df.empty:
            st.warning(
                "⚠️ No OMC loadings returned for this period. "
                "Depletion will show as 0 — check date range or API access."
            )
            omc_by_product = pd.Series({'PREMIUM': 0.0, 'GASOIL': 0.0, 'LPG': 0.0})
        else:
            omc_by_product = (
                omc_df[omc_df['Product'].isin(['PREMIUM', 'GASOIL', 'LPG'])]
                .groupby('Product')['Quantity']
                .sum()
            )
            st.write(
                f"✅ **{len(omc_df):,} total loading records** across {n_weeks} weeks | "
                f"PMS: **{omc_by_product.get('PREMIUM', 0):,.0f} LT** | "
                f"AGO: **{omc_by_product.get('GASOIL',  0):,.0f} LT** | "
                f"LPG: **{omc_by_product.get('LPG',     0):,.0f} LT**"
            )
        status_b.update(label=f"✅ Step 2 done — {len(omc_df):,} records from {n_weeks} weekly chunks", state="complete")

    # ── Compute forecast ──────────────────────────────────────────────────────
    rows_out = []
    for prod in ['PREMIUM', 'GASOIL', 'LPG']:
        stock      = float(balance_by_product.get(prod, 0))
        depletion  = float(omc_by_product.get(prod, 0))
        daily_rate = depletion / period_days if period_days > 0 else 0
        days       = (stock / daily_rate) if daily_rate > 0 else float('inf')
        rows_out.append({
            'product':         prod,
            'display_name':    DISPLAY[prod],
            'total_balance':   stock,
            'omc_sales':       depletion,
            'total_depletion': depletion,
            'daily_rate':      daily_rate,
            'days_remaining':  days,
        })

    forecast_df = pd.DataFrame(rows_out)

    # ── BDC-level stock breakdown ─────────────────────────────────────────────
    bdc_pivot = (
        bal_df.pivot_table(index='BDC', columns='Product', values=col_bal,
                           aggfunc='sum', fill_value=0)
        .reset_index()
    )
    for p in ['GASOIL', 'LPG', 'PREMIUM']:
        if p not in bdc_pivot.columns:
            bdc_pivot[p] = 0
    bdc_pivot['TOTAL'] = bdc_pivot[['GASOIL', 'LPG', 'PREMIUM']].sum(axis=1)
    bdc_pivot = bdc_pivot.sort_values('TOTAL', ascending=False)
    nat_total = bdc_pivot['TOTAL'].sum()
    bdc_pivot['Market Share %'] = (bdc_pivot['TOTAL'] / nat_total * 100).round(2)

    # ── Cache results ─────────────────────────────────────────────────────────
    st.session_state.ns_results = {
        'forecast_df':    forecast_df,
        'bal_df':         bal_df,
        'omc_df':         omc_df,
        'bdc_pivot':      bdc_pivot,
        'period_days':    period_days,
        'start_str':      start_str,
        'end_str':        end_str,
        'n_bdcs_balance': n_bdcs,
        'n_omc_rows':     len(omc_df),
    }

    _save_national_snapshot(forecast_df, f"{period_days}d")
    st.success("✅ Done! 2 API calls completed. Snapshot saved to history. Scroll down to see the forecast.")
    st.rerun()


# ── Display ───────────────────────────────────────────────────────────────────

def _display_national_results(period_days_arg: int):
    res = st.session_state.ns_results
    forecast_df  = res['forecast_df']
    bal_df       = res['bal_df']
    omc_df       = res['omc_df']
    bdc_pivot    = res['bdc_pivot']
    period_days  = res['period_days']
    start_str    = res['start_str']
    end_str      = res['end_str']

    st.markdown("---")
    st.markdown(
        f"<h3>🇬🇭 GHANA NATIONAL FUEL SUPPLY — "
        f"{start_str} → {end_str} ({period_days} days)</h3>",
        unsafe_allow_html=True
    )
    st.caption(
        f"Balance: **{res['n_bdcs_balance']} BDCs** | "
        f"OMC Loadings: **{res['n_omc_rows']:,} records** | "
        f"Depletion source: OMC Loadings only (CTO excluded — internal BDC transfers)"
    )
    st.markdown("---")

    # ── KPI cards ─────────────────────────────────────────────────────────────
    st.markdown("### 🛢️ DAYS OF SUPPLY — NATIONAL FORECAST")
    ICONS   = {'PREMIUM': '⛽', 'GASOIL': '🚛', 'LPG': '🔵'}
    COLORS  = {'PREMIUM': '#00ffff', 'GASOIL': '#ffaa00', 'LPG': '#00ff88'}

    cols = st.columns(len(forecast_df))
    for col, (_, row) in zip(cols, forecast_df.iterrows()):
        days  = row['days_remaining']
        prod  = row['product']
        color = COLORS.get(prod, '#ffffff')

        if days == float('inf'):
            days_text, status_text, border = "∞", "🔵 NO DATA", "#888888"
        elif days < 7:
            days_text, status_text, border = f"{days:.1f}", "🔴 CRITICAL", "#ff0000"
        elif days < 14:
            days_text, status_text, border = f"{days:.1f}", "🟡 WARNING",  "#ffaa00"
        elif days < 30:
            days_text, status_text, border = f"{days:.1f}", "🟠 MONITOR",  "#ff6600"
        else:
            days_text, status_text, border = f"{days:.1f}", "🟢 HEALTHY",  "#00ff88"

        stockout_date = (
            (datetime.now() + timedelta(days=days)).strftime('%d %b %Y')
            if days != float('inf') else "N/A"
        )

        with col:
            st.markdown(f"""
            <div style='background:rgba(10,14,39,0.85); padding:24px 16px; border-radius:16px;
                        border:2.5px solid {border}; text-align:center; margin-bottom:8px;
                        box-shadow:0 0 18px {border}55;'>
                <div style='font-size:36px; margin-bottom:4px;'>{ICONS.get(prod,'🛢️')}</div>
                <div style='font-family:Orbitron,sans-serif; font-size:18px; color:{color};
                             font-weight:700; letter-spacing:2px;'>{row["display_name"]}</div>
                <div style='margin:16px 0 8px;'>
                    <div style='color:#888; font-size:11px; text-transform:uppercase; letter-spacing:1px;'>
                        Days of Supply Left</div>
                    <div style='font-family:Orbitron,sans-serif; font-size:48px; color:{border};
                                 font-weight:900; line-height:1.1;'>{days_text}</div>
                    <div style='color:{border}; font-size:14px; font-weight:700;
                                 margin-top:4px;'>{status_text}</div>
                </div>
                <div style='border-top:1px solid rgba(255,255,255,0.08); padding-top:12px; margin-top:12px;'>
                    <table style='width:100%; font-family:Rajdhani,sans-serif; font-size:12px;
                                  border-collapse:collapse;'>
                        <tr>
                            <td style='color:#888; text-align:left; padding:2px 0;'>📦 Stock</td>
                            <td style='color:#e0e0e0; text-align:right; padding:2px 0; font-weight:600;'>
                                {row["total_balance"]:,.0f} LT</td>
                        </tr>
                        <tr>
                            <td style='color:#888; text-align:left; padding:2px 0;'>📉 Daily Rate</td>
                            <td style='color:#e0e0e0; text-align:right; padding:2px 0; font-weight:600;'>
                                {row["daily_rate"]:,.0f} LT/day</td>
                        </tr>
                        <tr>
                            <td style='color:#888; text-align:left; padding:2px 0;'>🗓️ Est. Empty</td>
                            <td style='color:{border}; text-align:right; padding:2px 0; font-weight:700;'>
                                {stockout_date}</td>
                        </tr>
                    </table>
                </div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    # ── National summary table ─────────────────────────────────────────────────
    st.markdown("### 📊 NATIONAL SUMMARY TABLE")

    summary_rows = []
    for _, row in forecast_df.iterrows():
        days = row['days_remaining']
        if   days == float('inf'): status = "No Data"
        elif days < 7:             status = "🔴 CRITICAL"
        elif days < 14:            status = "🟡 WARNING"
        elif days < 30:            status = "🟠 MONITOR"
        else:                      status = "🟢 HEALTHY"
        stockout = (
            (datetime.now() + timedelta(days=days)).strftime('%Y-%m-%d')
            if days != float('inf') else "N/A"
        )
        summary_rows.append({
            'Product':                                    row['display_name'],
            'National Stock (LT/KG)':                    f"{row['total_balance']:,.0f}",
            f'OMC Loadings ({period_days}d, LT)':        f"{row['omc_sales']:,.0f}",
            'Avg Daily Depletion (LT/day)':              f"{row['daily_rate']:,.0f}",
            'Days of Supply':                            f"{days:.1f}" if days != float('inf') else "∞",
            'Projected Empty':                           stockout,
            'Status':                                    status,
        })

    st.dataframe(pd.DataFrame(summary_rows), width='stretch', hide_index=True)
    st.markdown("---")

    # ── OMC Loadings breakdown by product ────────────────────────────────────
    st.markdown("### 📦 OMC LOADINGS BREAKDOWN BY PRODUCT")
    st.caption(
        "**OMC Loadings** = all released orders (status=4) from all BDCs to all OMCs. "
        "This is the only outflow that leaves the national wholesale system and reaches consumers."
    )

    bd_cols = st.columns(3)
    COLORS  = {'PREMIUM': '#00ffff', 'GASOIL': '#ffaa00', 'LPG': '#00ff88'}
    ICONS   = {'PREMIUM': '⛽', 'GASOIL': '🚛', 'LPG': '🔵'}
    total_nat_depletion = forecast_df['omc_sales'].sum()

    for col, (_, row) in zip(bd_cols, forecast_df.iterrows()):
        prod     = row['product']
        omc_v    = row['omc_sales']
        nat_pct  = (omc_v / total_nat_depletion * 100) if total_nat_depletion > 0 else 0

        with col:
            st.markdown(f"""
            <div style='background:rgba(22,33,62,0.6); padding:18px; border-radius:12px;
                        border:2px solid {COLORS.get(prod,"#ffffff")}; margin-bottom:8px;'>
                <div style='font-family:Orbitron,sans-serif; font-size:15px; font-weight:700;
                             color:{COLORS.get(prod,"#ffffff")}; text-align:center;
                             margin-bottom:14px;'>{ICONS.get(prod,"🛢️")} {row["display_name"]}</div>
                <table style='width:100%; font-family:Rajdhani,sans-serif; font-size:13px; border-collapse:collapse;'>
                    <tr>
                        <td style='color:#888; padding:4px 0;'>🚚 OMC Loadings</td>
                        <td style='color:#00ff88; text-align:right; font-weight:700;'>{omc_v:,.0f} LT</td>
                    </tr>
                    <tr>
                        <td style='color:#888; padding:4px 0;'>📊 Share of total</td>
                        <td style='color:#00ff88; text-align:right; font-weight:700;'>{nat_pct:.1f}%</td>
                    </tr>
                    <tr style='border-top:1px solid rgba(255,255,255,0.15);'>
                        <td style='color:#ffffff; padding:6px 0 2px; font-weight:700;'>📅 Daily avg</td>
                        <td style='color:#00ffff; text-align:right; font-weight:700;'>{row["daily_rate"]:,.0f} LT/day</td>
                    </tr>
                    <tr>
                        <td style='color:#888; padding:2px 0; font-size:12px;'>📦 Current stock</td>
                        <td style='color:#e0e0e0; text-align:right; font-size:12px;'>{row["total_balance"]:,.0f} LT</td>
                    </tr>
                </table>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    # ── Outflow summary ───────────────────────────────────────────────────────
    st.markdown("### ⚖️ NATIONAL OUTFLOW SUMMARY")
    st.caption(
        "Outflow = OMC Loadings (fuel dispatched from BDCs to OMCs over the selected period). "
        "Inflow data (vessel receipts) is not yet available in this report."
    )

    flow_rows = []
    for _, row in forecast_df.iterrows():
        flow_rows.append({
            'Product':                                   row['display_name'],
            f'OMC Loadings ({period_days}d, LT)':       f"{row['omc_sales']:,.0f}",
            'Daily Avg Outflow (LT/day)':               f"{row['daily_rate']:,.0f}",
            'Current Stock (LT)':                       f"{row['total_balance']:,.0f}",
            'Days of Supply':                           f"{row['days_remaining']:.1f}" if row['days_remaining'] != float('inf') else "∞",
        })

    st.dataframe(pd.DataFrame(flow_rows), width='stretch', hide_index=True)
    st.markdown("---")

    # ── BDC-level stock ───────────────────────────────────────────────────────
    st.markdown("### 🏦 CURRENT STOCK BY BDC")
    display_bdc = bdc_pivot.copy()
    for c in ['GASOIL', 'LPG', 'PREMIUM', 'TOTAL']:
        display_bdc[c] = display_bdc[c].apply(lambda x: f"{x:,.0f}")
    display_bdc['Market Share %'] = display_bdc['Market Share %'].apply(lambda x: f"{x:.2f}%")
    st.dataframe(display_bdc, width='stretch', hide_index=True)
    st.markdown("---")

    # ── Export ────────────────────────────────────────────────────────────────
    st.markdown("### 💾 EXPORT NATIONAL REPORT")
    if st.button("📄 GENERATE EXCEL REPORT", width='stretch', key='ns_export'):
        out_dir = os.path.join(os.getcwd(), "national_stockout_reports")
        os.makedirs(out_dir, exist_ok=True)
        filename = f"national_stockout_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        filepath = os.path.join(out_dir, filename)

        summary_export = pd.DataFrame([{
            'Product':                              row['display_name'],
            'National Stock (LT/KG)':               row['total_balance'],
            f'OMC Loadings ({period_days}d, LT)':   row['omc_sales'],
            'Avg Daily Depletion (LT/day)':         row['daily_rate'],
            'Days of Supply':                       row['days_remaining'] if row['days_remaining'] != float('inf') else 9999,
            'Projected Empty':                      (
                (datetime.now() + timedelta(days=row['days_remaining'])).strftime('%Y-%m-%d')
                if row['days_remaining'] != float('inf') else 'N/A'
            ),
        } for _, row in forecast_df.iterrows()])

        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            summary_export.to_excel(writer, sheet_name='Stockout Forecast',  index=False)
            bdc_pivot.to_excel(writer,       sheet_name='Stock by BDC',       index=False)
            if not omc_df.empty:
                omc_df.to_excel(writer,      sheet_name='OMC Loadings Detail', index=False)

        st.success(f"✅ Report saved: {filename}")
        with open(filepath, 'rb') as f:
            st.download_button(
                "⬇️ DOWNLOAD NATIONAL REPORT", f, filename,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                width='stretch'
            )


# ── (kept for BDC Transaction tab) ───────────────────────────────────────────

def _parse_stock_transaction_pdf(pdf_file) -> list:
    """
    Parse an NPA Stock Transaction Report PDF.
    Used by the per-BDC Stock Transaction tab.
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
        if lo.startswith(SKIP_PREFIXES):
            return True
        if re.match(r'^\d{1,2}\s+\w+,\s+\d{4}', line.strip()):
            return True
        return False

    def _parse_num(s: str):
        s = s.strip()
        neg = s.startswith('(') and s.endswith(')')
        val_str = s.strip('()').replace(',', '')
        try:
            val = int(val_str)
            return -val if neg else val
        except ValueError:
            return None

    def _parse_line(line: str):
        line = line.strip()
        if not re.match(r'^\d{2}/\d{2}/\d{4}\b', line):
            return None
        parts = line.split()
        date  = parts[0]
        trans = parts[1] if len(parts) > 1 else ''
        rest  = line[len(date):].strip()
        rest  = rest[len(trans):].strip()
        description = None
        after_desc  = rest
        for desc in DESCRIPTIONS:
            if rest.lower().startswith(desc.lower()):
                description = desc
                after_desc  = rest[len(desc):].strip()
                break
        if description is None or description == 'Balance b/fwd':
            return None
        nums = re.findall(r'\([\d,]+\)|[\d,]+', after_desc)
        if len(nums) < 2:
            return None
        volume  = _parse_num(nums[-2])
        balance = _parse_num(nums[-1])
        vol_tok = nums[-2]
        bal_tok = nums[-1]
        trail   = re.search(
            re.escape(vol_tok) + r'\s+' + re.escape(bal_tok) + r'\s*$',
            after_desc
        )
        account = after_desc[:trail.start()].strip() if trail else ' '.join(after_desc.split()[:-2])
        return {
            'Date': date, 'Trans #': trans, 'Description': description,
            'Account': account,
            'Volume':  volume  if volume  is not None else 0,
            'Balance': balance if balance is not None else 0,
        }

    records = []
    try:
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue
                for raw in text.split("\n"):
                    line = raw.strip()
                    if not line or _should_skip(line):
                        continue
                    row = _parse_line(line)
                    if row:
                        records.append(row)
    except Exception as e:
        st.error(f"Error parsing PDF: {e}")
        return []
    return records


def show_bdc_intelligence():
    st.markdown("<h2>🧠 BDC INTELLIGENCE CENTER</h2>", unsafe_allow_html=True)
    st.info("🎯 Predictive analytics combining stock balance and loading patterns")
    st.markdown("---")
 
    has_balance = bool(st.session_state.get('bdc_records'))
    has_loadings = not st.session_state.get('omc_df', pd.DataFrame()).empty
 
    if not has_balance or not has_loadings:
        st.markdown("### 🔄 AUTO-FETCH DATA")
        st.info("BDC Intelligence needs both Stock Balance and OMC Loadings data. Let's fetch them automatically!")
     
        col1, col2 = st.columns(2)
     
        with col1:
            if not has_balance:
                st.warning("⚠️ BDC Balance Data Missing")
                if st.button("🔄 FETCH BDC BALANCE", width='stretch', key='auto_fetch_balance'):
                    with st.spinner("🔄 Fetching BDC Balance Data..."):
                        scraper = StockBalanceScraper()
                     
                        url = NPA_CONFIG['BDC_BALANCE_URL']
                        params = {
                            'lngCompanyId': NPA_CONFIG['COMPANY_ID'],
                            'strITSfromPersol': NPA_CONFIG['ITS_FROM_PERSOL'],
                            'strGroupBy': 'BDC',
                            'strGroupBy1': 'DEPOT',
                            'strQuery1': '',
                            'strQuery2': '',
                            'strQuery3': '',
                            'strQuery4': '',
                            'strPicHeight': '1',
                            'szPicWeight': '1',
                            'lngUserId': NPA_CONFIG['USER_ID'],
                            'intAppId': NPA_CONFIG['APP_ID']
                        }
                     
                        try:
                            import requests
                            import io
                         
                            headers = {
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                                'Accept': 'application/pdf,text/html,application/xhtml+xml',
                                'Accept-Language': 'en-US,en;q=0.5',
                                'Connection': 'keep-alive',
                            }
                         
                            response = requests.get(url, params=params, headers=headers, timeout=30)
                            response.raise_for_status()
                         
                            if response.content[:4] == b'%PDF':
                                pdf_file = io.BytesIO(response.content)
                                st.session_state.bdc_records = scraper.parse_pdf_file(pdf_file)
                             
                                if st.session_state.bdc_records:
                                    st.success(f"✅ Fetched {len(st.session_state.bdc_records)} BDC Balance records!")
                                    st.rerun()
                                else:
                                    st.error("❌ No records found in PDF")
                            else:
                                st.error("❌ Invalid response from server")
                             
                        except Exception as e:
                            st.error(f"❌ Error fetching BDC Balance: {e}")
            else:
                st.success("✅ BDC Balance Data Loaded")
                st.caption(f"{len(st.session_state.bdc_records)} records available")
     
        with col2:
            if not has_loadings:
                st.warning("⚠️ OMC Loadings Data Missing")
             
                st.markdown("**Select Date Range:**")
                from datetime import timedelta
                default_start = datetime.now() - timedelta(days=30)
                default_end = datetime.now()
             
                start_date = st.date_input("From", value=default_start, key='intel_start_date')
                end_date = st.date_input("To", value=default_end, key='intel_end_date')
             
                if st.button("🔄 FETCH OMC LOADINGS", width='stretch', key='auto_fetch_loadings'):
                    with st.spinner("🔄 Fetching OMC Loadings Data..."):
                        start_str = start_date.strftime("%m/%d/%Y")
                        end_str = end_date.strftime("%m/%d/%Y")
                     
                        url = NPA_CONFIG['OMC_LOADINGS_URL']
                        params = {
                            'lngCompanyId': NPA_CONFIG['COMPANY_ID'],
                            'szITSfromPersol': 'persol',
                            'strGroupBy': 'BDC',
                            'strGroupBy1': NPA_CONFIG['OMC_NAME'],
                            'strQuery1': ' and iorderstatus=4',
                            'strQuery2': start_str,
                            'strQuery3': end_str,
                            'strQuery4': '',
                            'strPicHeight': '',
                            'strPicWeight': '',
                            'intPeriodID': '4',
                            'iUserId': NPA_CONFIG['USER_ID'],
                            'iAppId': NPA_CONFIG['APP_ID']
                        }
                     
                        try:
                            import requests
                            import io
                         
                            headers = {
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                                'Accept': 'application/pdf,text/html,application/xhtml+xml',
                                'Accept-Language': 'en-US,en;q=0.5',
                                'Connection': 'keep-alive',
                            }
                         
                            response = requests.get(url, params=params, headers=headers, timeout=30)
                            response.raise_for_status()
                         
                            if response.content[:4] == b'%PDF':
                                pdf_file = io.BytesIO(response.content)
                                st.session_state.omc_df = extract_npa_data_from_pdf(pdf_file)
                             
                                if not st.session_state.omc_df.empty:
                                    st.success(f"✅ Fetched {len(st.session_state.omc_df)} OMC Loading records!")
                                    st.rerun()
                                else:
                                    st.error("❌ No records found in PDF")
                            else:
                                st.error("❌ Invalid response from server")
                             
                        except Exception as e:
                            st.error(f"❌ Error fetching OMC Loadings: {e}")
            else:
                st.success("✅ OMC Loadings Data Loaded")
                st.caption(f"{len(st.session_state.omc_df)} records available")
     
        st.markdown("---")
     
        if not (bool(st.session_state.get('bdc_records')) and not st.session_state.get('omc_df', pd.DataFrame()).empty):
            st.info("👆 Click the buttons above to fetch the required data automatically!")
            return
 
    balance_df = pd.DataFrame(st.session_state.bdc_records)
    loadings_df = st.session_state.omc_df
 
    st.markdown("### ✅ Data Ready")
    col1, col2 = st.columns(2)
    with col1:
        st.success(f"✅ BDC Balance: {len(balance_df)} records")
    with col2:
        st.success(f"✅ OMC Loadings: {len(loadings_df)} records")
 
    st.markdown("---")
 
    available_bdcs = set()
    available_bdcs.update(balance_df['BDC'].unique())
    available_bdcs.update(loadings_df['BDC'].unique())
    available_bdcs = sorted(list(available_bdcs))
 
    if not available_bdcs:
        st.warning("⚠️ No BDCs found in the data")
        return
 
    st.markdown("### 🔍 SELECT BDC FOR ANALYSIS")
    selected_bdc = st.selectbox("Choose BDC:", available_bdcs, key='intel_bdc_select')
 
    if not selected_bdc:
        return
 
    st.markdown("---")
    st.markdown(f"## 📈 INTELLIGENCE REPORT: {selected_bdc}")
    st.markdown("---")
 
    tab1, tab2, tab3 = st.tabs(["📊 Overview", "⏱️ Stockout Prediction", "📉 Consumption Analysis"])
 
    with tab1:
        st.markdown("### 📊 CURRENT STATUS")
     
        bdc_balance = balance_df[balance_df['BDC'] == selected_bdc]
     
        if not bdc_balance.empty:
                col1, col2, col3 = st.columns(3)
             
                col_name = 'ACTUAL BALANCE (LT\\KG)'
                product_stocks = bdc_balance.groupby('Product')[col_name].sum()
             
                for idx, (product, stock) in enumerate(product_stocks.items()):
                    with [col1, col2, col3][idx % 3]:
                        st.markdown(f"""
                        <div class='metric-card'>
                            <h2>{product}</h2>
                            <h1>{stock:,.0f}</h1>
                            <p style='color: #888; font-size: 14px; margin: 0;'>LT/KG in Stock</p>
                        </div>
                        """, unsafe_allow_html=True)
             
                st.markdown("---")
             
                st.markdown("#### 🏭 Stock by Depot")
                depot_breakdown = bdc_balance.groupby(['DEPOT', 'Product'])[col_name].sum().reset_index()
                depot_pivot = depot_breakdown.pivot(index='DEPOT', columns='Product', values=col_name).fillna(0)
                st.dataframe(depot_pivot, width='stretch')
        else:
            st.warning(f"⚠️ No stock balance data found for {selected_bdc}")
     
        st.markdown("---")
        st.markdown("### 🚚 LOADING ACTIVITY")
     
        bdc_loadings = loadings_df[loadings_df['BDC'] == selected_bdc]
     
        if not bdc_loadings.empty:
                cols = st.columns(4)
             
                with cols[0]:
                    st.metric("Total Orders", f"{len(bdc_loadings):,}")
                with cols[1]:
                    st.metric("Total Volume", f"{bdc_loadings['Quantity'].sum():,.0f} LT")
                with cols[2]:
                    st.metric("Unique OMCs", f"{bdc_loadings['OMC'].nunique()}")
                with cols[3]:
                    avg_order = bdc_loadings['Quantity'].mean()
                    st.metric("Avg Order Size", f"{avg_order:,.0f} LT")
             
                st.markdown("#### 📦 Loading by Product")
                product_loadings = bdc_loadings.groupby('Product').agg({
                    'Quantity': ['sum', 'mean', 'count']
                }).reset_index()
                product_loadings.columns = ['Product', 'Total Volume', 'Avg Order Size', 'Order Count']
                st.dataframe(product_loadings, width='stretch', hide_index=True)
        else:
            st.warning(f"⚠️ No loading data found for {selected_bdc}")
 
    with tab2:
        st.markdown("### ⏱️ STOCKOUT PREDICTION")
     
        bdc_balance = balance_df[balance_df['BDC'] == selected_bdc]
        bdc_loadings = loadings_df[loadings_df['BDC'] == selected_bdc]
     
        if bdc_balance.empty:
            st.warning(f"⚠️ No balance data for {selected_bdc}")
            return
     
        if bdc_loadings.empty:
            st.warning(f"⚠️ No loading data for {selected_bdc}")
            return
     
        loadings_df_copy = bdc_loadings.copy()
        loadings_df_copy['Date'] = pd.to_datetime(loadings_df_copy['Date'], errors='coerce')
        loadings_df_copy = loadings_df_copy.dropna(subset=['Date'])
     
        if loadings_df_copy.empty:
            st.warning("⚠️ No valid date information in loading data")
            return
     
        date_range = (loadings_df_copy['Date'].max() - loadings_df_copy['Date'].min()).days
        if date_range == 0:
            date_range = 1
     
        daily_consumption = loadings_df_copy.groupby('Product')['Quantity'].sum() / date_range
     
        col_name = 'ACTUAL BALANCE (LT\\KG)'
        current_stock = bdc_balance.groupby('Product')[col_name].sum()
     
        st.markdown("#### 📅 Estimated Days Until Stockout")
     
        predictions = []
        for product in current_stock.index:
            stock = current_stock[product]
            daily_rate = daily_consumption.get(product, 0)
         
            if daily_rate > 0:
                days_remaining = stock / daily_rate
             
                if days_remaining < 7:
                    status = "🔴 CRITICAL"
                    color = "#ff0000"
                elif days_remaining < 14:
                    status = "🟡 WARNING"
                    color = "#ffaa00"
                else:
                    status = "🟢 HEALTHY"
                    color = "#00ff88"
             
                predictions.append({
                    'Product': product,
                    'Current Stock (LT)': f"{stock:,.0f}",
                    'Daily Consumption (LT)': f"{daily_rate:,.0f}",
                    'Days Remaining': f"{days_remaining:.1f}",
                    'Status': status
                })
             
                st.markdown(f"""
                <div style='background: rgba(22,33,62,0.6); padding: 20px; border-radius: 10px;
                            border: 2px solid {color}; margin: 10px 0;'>
                    <h3 style='color: {color}; margin: 0;'>{product}</h3>
                    <div style='display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px; margin-top: 15px;'>
                        <div>
                            <p style='color: #888; margin: 0; font-size: 14px;'>Current Stock</p>
                            <p style='color: #00ffff; margin: 5px 0; font-size: 24px; font-weight: bold;'>{stock:,.0f} LT</p>
                        </div>
                        <div>
                            <p style='color: #888; margin: 0; font-size: 14px;'>Daily Usage</p>
                            <p style='color: #ff00ff; margin: 5px 0; font-size: 24px; font-weight: bold;'>{daily_rate:,.0f} LT</p>
                        </div>
                        <div>
                            <p style='color: #888; margin: 0; font-size: 14px;'>Days Remaining</p>
                            <p style='color: {color}; margin: 5px 0; font-size: 32px; font-weight: bold;'>{days_remaining:.1f}</p>
                        </div>
                    </div>
                    <p style='margin-top: 15px; color: {color}; font-size: 18px; font-weight: bold;'>{status}</p>
                </div>
                """, unsafe_allow_html=True)
            else:
                predictions.append({
                    'Product': product,
                    'Current Stock (LT)': f"{stock:,.0f}",
                    'Daily Consumption (LT)': "N/A",
                    'Days Remaining': "∞",
                    'Status': "ℹ️ NO DATA"
                })
     
        if predictions:
            st.markdown("---")
            st.markdown("#### 📋 Summary Table")
            pred_df = pd.DataFrame(predictions)
            st.dataframe(pred_df, width='stretch', hide_index=True)
 
    with tab3:
        st.markdown("### 📉 CONSUMPTION ANALYSIS")
     
        bdc_loadings = loadings_df[loadings_df['BDC'] == selected_bdc]
     
        if bdc_loadings.empty:
            st.warning(f"⚠️ No loading data for {selected_bdc}")
            return
     
        ts_df = bdc_loadings.copy()
        ts_df['Date'] = pd.to_datetime(ts_df['Date'], errors='coerce')
        ts_df = ts_df.dropna(subset=['Date'])
     
        if ts_df.empty:
            st.warning("⚠️ No valid dates in loading data")
            return
     
        daily_by_product = ts_df.groupby([ts_df['Date'].dt.date, 'Product'])['Quantity'].sum().reset_index()
        daily_by_product.columns = ['Date', 'Product', 'Volume']
     
        st.markdown("#### 📈 Daily Consumption Trend")
     
        for product in daily_by_product['Product'].unique():
            product_data = daily_by_product[daily_by_product['Product'] == product]
         
            if not product_data.empty:
                st.markdown(f"**{product}**")
                st.line_chart(product_data.set_index('Date')['Volume'], width='stretch')
     
        st.markdown("---")
        st.markdown("#### 📊 Consumption Statistics")
     
        stats = ts_df.groupby('Product')['Quantity'].agg([
            ('Total', 'sum'),
            ('Average', 'mean'),
            ('Median', 'median'),
            ('Min', 'min'),
            ('Max', 'max'),
            ('Std Dev', 'std')
        ]).reset_index()
     
        st.dataframe(stats, width='stretch', hide_index=True)
     
        st.markdown("---")
        st.markdown("#### 🏢 Top OMCs Loading from this BDC")
     
        top_omcs = ts_df.groupby('OMC')['Quantity'].sum().sort_values(ascending=False).head(10).reset_index()
        top_omcs.columns = ['OMC', 'Total Volume (LT)']
     
        st.dataframe(top_omcs, width='stretch', hide_index=True)
if __name__ == "__main__":
    main()


# ═══════════════════════════════════════════════════════════════════════════════
# HISTORY ENGINE — persists national snapshots to disk on every fetch
# ═══════════════════════════════════════════════════════════════════════════════

SNAPSHOT_DIR = os.path.join(os.getcwd(), "national_snapshots")

def _save_national_snapshot(forecast_df: pd.DataFrame, period_label: str):
    """Persist a national stockout result row to the snapshot archive."""
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    snap = {
        'ts': datetime.now().isoformat(),
        'period': period_label,
        'rows': forecast_df[['product','total_balance','omc_sales','daily_rate','days_remaining']].to_dict('records')
    }
    fname = f"snap_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(os.path.join(SNAPSHOT_DIR, fname), 'w') as f:
        json.dump(snap, f)

def _load_all_snapshots() -> pd.DataFrame:
    """Load every saved snapshot into a tidy DataFrame."""
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
                    'timestamp': ts,
                    'period': snap.get('period', ''),
                    'product': r['product'],
                    'total_balance': r['total_balance'],
                    'omc_sales': r['omc_sales'],
                    'daily_rate': r['daily_rate'],
                    'days_remaining': r['days_remaining'],
                })
        except Exception:
            continue
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. 🔴 LIVE RUNWAY MONITOR
# ═══════════════════════════════════════════════════════════════════════════════

def show_live_runway_monitor():
    st.markdown("<h2>🔴 LIVE RUNWAY MONITOR</h2>", unsafe_allow_html=True)
    st.markdown("""
    <p style='color:#ff00ff; font-size:16px;'>
    Auto-refreshes every 60 minutes. Alerts when any product drops below threshold.
    Always shows the latest national supply runway at a glance.
    </p>
    """, unsafe_allow_html=True)
    st.markdown("---")

    # ── Alert thresholds ──────────────────────────────────────────────────────
    with st.expander("⚙️ Configure Alert Thresholds", expanded=False):
        col1, col2, col3 = st.columns(3)
        with col1:
            pms_thresh  = st.number_input("PMS Critical (days)",  value=7,  min_value=1, max_value=60)
            pms_warn    = st.number_input("PMS Warning (days)",   value=14, min_value=1, max_value=60)
        with col2:
            ago_thresh  = st.number_input("AGO Critical (days)",  value=7,  min_value=1, max_value=60)
            ago_warn    = st.number_input("AGO Warning (days)",   value=14, min_value=1, max_value=60)
        with col3:
            lpg_thresh  = st.number_input("LPG Critical (days)",  value=7,  min_value=1, max_value=60)
            lpg_warn    = st.number_input("LPG Warning (days)",   value=14, min_value=1, max_value=60)

    thresholds = {
        'PREMIUM': (pms_thresh,  pms_warn),
        'GASOIL':  (ago_thresh,  ago_warn),
        'LPG':     (lpg_thresh,  lpg_warn),
    }

    # ── Auto-refresh controls ─────────────────────────────────────────────────
    col_a, col_b, col_c = st.columns([2, 1, 1])
    with col_a:
        auto_refresh = st.checkbox("🔄 Auto-refresh every 60 minutes", value=False)
    with col_b:
        period_days_lr = st.number_input("Lookback days", value=30, min_value=1, max_value=90, key='lr_period')
    with col_c:
        fetch_now = st.button("⚡ FETCH NOW", key='lr_fetch', width='content')

    # ── Fetch logic ───────────────────────────────────────────────────────────
    should_fetch = fetch_now
    if auto_refresh:
        last_fetch = st.session_state.get('lr_last_fetch')
        if last_fetch is None or (datetime.now() - last_fetch).seconds > 3600:
            should_fetch = True

    if should_fetch:
        end_dt   = datetime.now()
        start_dt = end_dt - timedelta(days=period_days_lr)
        start_str = start_dt.strftime("%m/%d/%Y")
        end_str   = end_dt.strftime("%m/%d/%Y")

        cfg = NPA_CONFIG
        col_bal = 'ACTUAL BALANCE (LT\\KG)'

        with st.spinner("Fetching BDC Balance…"):
            bal_params = {
                'lngCompanyId': cfg['COMPANY_ID'], 'strITSfromPersol': cfg['ITS_FROM_PERSOL'],
                'strGroupBy': 'BDC', 'strGroupBy1': 'DEPOT',
                'strQuery1': '', 'strQuery2': '', 'strQuery3': '', 'strQuery4': '',
                'strPicHeight': '1', 'szPicWeight': '1',
                'lngUserId': cfg['USER_ID'], 'intAppId': cfg['APP_ID'],
            }
            bal_bytes = _fetch_pdf_bytes(cfg['BDC_BALANCE_URL'], bal_params)
            if bal_bytes:
                scraper = StockBalanceScraper()
                bal_df = pd.DataFrame(scraper.parse_pdf_file(io.BytesIO(bal_bytes)))
            else:
                st.error("❌ Balance fetch failed"); return

        with st.spinner(f"Fetching OMC Loadings ({period_days_lr}d, chunked)…"):
            omc_df = _fetch_national_omc_loadings(start_str, end_str)

        # Compute runway
        balance_by_product = bal_df.groupby('Product')[col_bal].sum() if not bal_df.empty else pd.Series()
        omc_by_product = (
            omc_df[omc_df['Product'].isin(['PREMIUM','GASOIL','LPG'])]
            .groupby('Product')['Quantity'].sum()
        ) if not omc_df.empty else pd.Series()

        rows_out = []
        for prod in ['PREMIUM', 'GASOIL', 'LPG']:
            stock = float(balance_by_product.get(prod, 0))
            dep   = float(omc_by_product.get(prod, 0))
            daily = dep / period_days_lr if period_days_lr > 0 else 0
            days  = stock / daily if daily > 0 else float('inf')
            rows_out.append({'product': prod, 'total_balance': stock,
                             'omc_sales': dep, 'daily_rate': daily, 'days_remaining': days})

        forecast_df = pd.DataFrame(rows_out)
        st.session_state.lr_forecast  = forecast_df
        st.session_state.lr_last_fetch = datetime.now()
        st.session_state.lr_period_days = period_days_lr
        _save_national_snapshot(forecast_df, f"{period_days_lr}d")

    # ── Display ───────────────────────────────────────────────────────────────
    if st.session_state.get('lr_forecast') is None:
        st.info("👆 Click **FETCH NOW** to load the live runway status.")
        return

    forecast_df  = st.session_state.lr_forecast
    last_fetch_t = st.session_state.lr_last_fetch
    period_d     = st.session_state.get('lr_period_days', period_days_lr)

    st.markdown(
        f"<p style='color:#888; font-size:13px;'>Last updated: "
        f"<b style='color:#00ffff'>{last_fetch_t.strftime('%d %b %Y %H:%M:%S')}</b> | "
        f"Lookback: {period_d} days</p>",
        unsafe_allow_html=True
    )

    ICONS  = {'PREMIUM':'⛽','GASOIL':'🚛','LPG':'🔵'}
    COLORS = {'PREMIUM':'#00ffff','GASOIL':'#ffaa00','LPG':'#00ff88'}
    NAMES  = {'PREMIUM':'PREMIUM (PMS)','GASOIL':'GASOIL (AGO)','LPG':'LPG'}

    cols = st.columns(3)
    any_critical = any_warning = False

    for col, (_, row) in zip(cols, forecast_df.iterrows()):
        prod  = row['product']
        days  = row['days_remaining']
        crit, warn = thresholds.get(prod, (7, 14))
        color = COLORS[prod]

        if days == float('inf'):
            border, status, emoji = '#888', 'NO DATA', '⚫'
        elif days < crit:
            border, status, emoji = '#ff0000', 'CRITICAL', '🔴'
            any_critical = True
        elif days < warn:
            border, status, emoji = '#ffaa00', 'WARNING', '🟡'
            any_warning = True
        elif days < 30:
            border, status, emoji = '#ff6600', 'MONITOR', '🟠'
        else:
            border, status, emoji = '#00ff88', 'HEALTHY', '🟢'

        days_txt = f"{days:.1f}" if days != float('inf') else "∞"
        empty_dt = (datetime.now() + timedelta(days=days)).strftime('%d %b %Y') if days != float('inf') else "N/A"

        delta_html = ""
        hist = _load_all_snapshots()
        if not hist.empty:
            prev = hist[hist['product'] == prod].sort_values('timestamp')
            if len(prev) >= 2:
                prev_days = prev.iloc[-2]['days_remaining']
                delta = days - prev_days if days != float('inf') and prev_days != float('inf') else 0
                arrow = "↑" if delta > 0 else "↓"
                dcol  = "#00ff88" if delta > 0 else "#ff4444"
                delta_html = f"<span style='color:{dcol}; font-size:14px;'>{arrow}{abs(delta):.1f}d vs prev</span>"

        with col:
            st.markdown(f"""
            <div style='background:rgba(10,14,39,0.9); padding:28px 18px; border-radius:18px;
                        border:3px solid {border}; text-align:center;
                        box-shadow:0 0 25px {border}66; margin-bottom:10px;'>
                <div style='font-size:40px;'>{ICONS[prod]}</div>
                <div style='font-family:Orbitron,sans-serif; color:{color}; font-size:16px;
                             font-weight:700; letter-spacing:2px; margin:8px 0;'>{NAMES[prod]}</div>
                <div style='font-size:13px; color:{border}; font-weight:700; letter-spacing:3px;
                             margin-bottom:12px;'>{emoji} {status}</div>
                <div style='font-family:Orbitron,sans-serif; font-size:64px; font-weight:900;
                             color:{border}; line-height:1; text-shadow:0 0 20px {border};'>{days_txt}</div>
                <div style='color:#888; font-size:12px; margin:4px 0;'>DAYS OF SUPPLY</div>
                {delta_html}
                <div style='border-top:1px solid rgba(255,255,255,0.1); margin-top:14px; padding-top:10px;'>
                    <div style='color:#888; font-size:11px;'>📦 {row["total_balance"]:,.0f} LT stock</div>
                    <div style='color:#888; font-size:11px;'>📉 {row["daily_rate"]:,.0f} LT/day avg</div>
                    <div style='color:{border}; font-size:12px; font-weight:700; margin-top:4px;'>
                        🗓️ Est. empty: {empty_dt}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    if any_critical:
        st.error("🚨 **CRITICAL ALERT:** One or more products are at critical supply levels! Immediate action required.")
    elif any_warning:
        st.warning("⚠️ **WARNING:** One or more products approaching low supply. Plan replenishment now.")
    else:
        st.success("✅ All products at healthy supply levels.")

    if auto_refresh:
        import time
        st.caption("Auto-refresh active. Page will refresh in 60 minutes.")
        time.sleep(3600)
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# 2. 📉 HISTORICAL TRENDS
# ═══════════════════════════════════════════════════════════════════════════════

def show_historical_trends():
    st.markdown("<h2>📉 HISTORICAL TRENDS</h2>", unsafe_allow_html=True)
    st.markdown("""
    <p style='color:#ff00ff; font-size:16px;'>
    Every time you run National Stockout or Live Runway Monitor, a snapshot is saved.
    This page plots those snapshots over time — showing whether Ghana's fuel runway is
    shrinking or growing.
    </p>
    """, unsafe_allow_html=True)
    st.markdown("---")

    hist = _load_all_snapshots()

    if hist.empty:
        st.info(
            "📭 No snapshot history yet.\n\n"
            "Run **🔴 Live Runway Monitor** or **🌍 National Stockout** a few times "
            "and come back — each run saves a timestamped snapshot automatically."
        )
        return

    hist = hist.sort_values('timestamp')
    n_snaps = hist['timestamp'].nunique()
    oldest  = hist['timestamp'].min().strftime('%d %b %Y')
    newest  = hist['timestamp'].max().strftime('%d %b %Y')

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Snapshots", n_snaps)
    col2.metric("Earliest", oldest)
    col3.metric("Latest",   newest)

    st.markdown("---")

    COLORS = {'PREMIUM':'#00ffff','GASOIL':'#ffaa00','LPG':'#00ff88'}

    # ── Days of Supply trend ──────────────────────────────────────────────────
    st.markdown("### 📈 DAYS OF SUPPLY OVER TIME")
    st.caption("Each dot = one saved snapshot. Trend shows if supply runway is growing or shrinking.")

    fig_days = go.Figure()
    for prod in ['PREMIUM', 'GASOIL', 'LPG']:
        pdata = hist[hist['product'] == prod].copy()
        pdata = pdata[pdata['days_remaining'] != float('inf')]
        if pdata.empty: continue
        # Trend line (rolling 3)
        pdata = pdata.sort_values('timestamp')
        pdata['trend'] = pdata['days_remaining'].rolling(3, min_periods=1).mean()
        fig_days.add_trace(go.Scatter(
            x=pdata['timestamp'], y=pdata['days_remaining'],
            mode='markers', name=f"{prod} actual",
            marker=dict(color=COLORS[prod], size=8),
        ))
        fig_days.add_trace(go.Scatter(
            x=pdata['timestamp'], y=pdata['trend'],
            mode='lines', name=f"{prod} trend",
            line=dict(color=COLORS[prod], width=2, dash='dot'),
        ))

    # Alert lines
    fig_days.add_hline(y=7,  line_dash="dash", line_color="#ff0000",
                       annotation_text="CRITICAL 7d", annotation_font_color="#ff0000")
    fig_days.add_hline(y=14, line_dash="dash", line_color="#ffaa00",
                       annotation_text="WARNING 14d", annotation_font_color="#ffaa00")
    fig_days.update_layout(
        paper_bgcolor='rgba(10,14,39,0.9)', plot_bgcolor='rgba(10,14,39,0.9)',
        font=dict(color='white'), height=420,
        legend=dict(font=dict(color='white')),
        xaxis=dict(gridcolor='rgba(255,255,255,0.05)'),
        yaxis=dict(gridcolor='rgba(255,255,255,0.05)', title='Days of Supply'),
    )
    st.plotly_chart(fig_days, width='stretch')

    st.markdown("---")
    st.markdown("### 🛢️ NATIONAL STOCK VOLUME OVER TIME (LT)")

    fig_stock = go.Figure()
    for prod in ['PREMIUM', 'GASOIL', 'LPG']:
        pdata = hist[hist['product'] == prod].sort_values('timestamp')
        if pdata.empty: continue
        fig_stock.add_trace(go.Scatter(
            x=pdata['timestamp'], y=pdata['total_balance'],
            mode='lines+markers', name=prod,
            line=dict(color=COLORS[prod], width=2),
            marker=dict(size=6),
        ))
    fig_stock.update_layout(
        paper_bgcolor='rgba(10,14,39,0.9)', plot_bgcolor='rgba(10,14,39,0.9)',
        font=dict(color='white'), height=380,
        legend=dict(font=dict(color='white')),
        xaxis=dict(gridcolor='rgba(255,255,255,0.05)'),
        yaxis=dict(gridcolor='rgba(255,255,255,0.05)', title='Stock (LT)'),
    )
    st.plotly_chart(fig_stock, width='stretch')

    st.markdown("---")
    st.markdown("### 📉 DAILY DEPLETION RATE OVER TIME (LT/day)")
    st.caption("Rising depletion rate = demand is accelerating.")

    fig_dep = go.Figure()
    for prod in ['PREMIUM', 'GASOIL', 'LPG']:
        pdata = hist[hist['product'] == prod].sort_values('timestamp')
        if pdata.empty: continue
        fig_dep.add_trace(go.Bar(
            x=pdata['timestamp'], y=pdata['daily_rate'],
            name=prod, marker_color=COLORS[prod], opacity=0.8,
        ))
    fig_dep.update_layout(
        barmode='group',
        paper_bgcolor='rgba(10,14,39,0.9)', plot_bgcolor='rgba(10,14,39,0.9)',
        font=dict(color='white'), height=360,
        legend=dict(font=dict(color='white')),
        xaxis=dict(gridcolor='rgba(255,255,255,0.05)'),
        yaxis=dict(gridcolor='rgba(255,255,255,0.05)', title='LT/day'),
    )
    st.plotly_chart(fig_dep, width='stretch')

    st.markdown("---")
    st.markdown("### 📋 RAW SNAPSHOT TABLE")
    disp = hist.copy()
    disp['timestamp'] = disp['timestamp'].dt.strftime('%Y-%m-%d %H:%M')
    disp['days_remaining'] = disp['days_remaining'].apply(lambda x: f"{x:.1f}" if x != float('inf') else "∞")
    disp['total_balance'] = disp['total_balance'].apply(lambda x: f"{x:,.0f}")
    disp['daily_rate']    = disp['daily_rate'].apply(lambda x: f"{x:,.0f}")
    disp['omc_sales']     = disp['omc_sales'].apply(lambda x: f"{x:,.0f}")
    st.dataframe(disp.rename(columns={
        'timestamp':'Snapshot Time','period':'Period','product':'Product',
        'total_balance':'Stock (LT)','omc_sales':'OMC Loadings (LT)',
        'daily_rate':'Daily Rate (LT/day)','days_remaining':'Days of Supply'
    }), width='stretch', hide_index=True)

    if st.button("🗑️ Clear All Snapshots", key='clear_snaps'):
        import shutil
        shutil.rmtree(SNAPSHOT_DIR, ignore_errors=True)
        st.success("Snapshots cleared.")
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# 3. 🗺️ DEPOT STRESS MAP
# ═══════════════════════════════════════════════════════════════════════════════

# Known depot coordinates (Ghana)
DEPOT_COORDS = {
    'TEMA':        (5.6698,  -0.0166),
    'TAKORADI':    (4.8845,  -1.7554),
    'KUMASI':      (6.6885,  -1.6244),
    'ACCRA':       (5.6037,  -0.1870),
    'BOLGATANGA':  (10.7856, -0.8514),
    'TAMALE':      (9.4008,  -0.8393),
    'SUNYANI':     (7.3349,  -2.3266),
    'HO':          (6.6011,   0.4714),
    'CAPE COAST':  (5.1053,  -1.2466),
    'SEKONDI':     (4.9340,  -1.7039),
    'KOFORIDUA':   (6.0940,  -0.2588),
}

def _guess_coords(depot_name: str):
    """Match depot name to known coordinates via keyword search."""
    dn = depot_name.upper()
    for city, coords in DEPOT_COORDS.items():
        if city in dn:
            return coords
    return None

def show_depot_stress_map():
    st.markdown("<h2>🗺️ DEPOT STRESS MAP</h2>", unsafe_allow_html=True)
    st.markdown("""
    <p style='color:#ff00ff; font-size:16px;'>
    Geographic view of Ghana's fuel depot stock levels. Instantly see which physical
    locations are critically low — even when the national total looks fine.
    </p>
    """, unsafe_allow_html=True)
    st.markdown("---")

    # ── Data source ───────────────────────────────────────────────────────────
    has_balance = bool(st.session_state.get('bdc_records'))

    if not has_balance:
        st.info("📡 Fetching BDC Balance data (needed for depot-level stock)…")
        if st.button("⚡ FETCH BDC BALANCE", key='dsm_fetch'):
            cfg = NPA_CONFIG
            with st.spinner("Fetching…"):
                bal_params = {
                    'lngCompanyId': cfg['COMPANY_ID'], 'strITSfromPersol': cfg['ITS_FROM_PERSOL'],
                    'strGroupBy': 'BDC', 'strGroupBy1': 'DEPOT',
                    'strQuery1': '', 'strQuery2': '', 'strQuery3': '', 'strQuery4': '',
                    'strPicHeight': '1', 'szPicWeight': '1',
                    'lngUserId': cfg['USER_ID'], 'intAppId': cfg['APP_ID'],
                }
                bal_bytes = _fetch_pdf_bytes(cfg['BDC_BALANCE_URL'], bal_params)
                if bal_bytes:
                    scraper = StockBalanceScraper()
                    st.session_state.bdc_records = scraper.parse_pdf_file(io.BytesIO(bal_bytes))
                    st.rerun()
                else:
                    st.error("❌ Fetch failed")
        return

    bal_df = pd.DataFrame(st.session_state.bdc_records)
    col_bal = 'ACTUAL BALANCE (LT\\KG)'

    if 'DEPOT' not in bal_df.columns or col_bal not in bal_df.columns:
        st.error("❌ Balance data missing DEPOT or balance columns")
        return

    # ── Product filter ────────────────────────────────────────────────────────
    prod_sel = st.selectbox("Product", ['ALL', 'PREMIUM', 'GASOIL', 'LPG'], key='dsm_prod')
    if prod_sel != 'ALL':
        bal_df = bal_df[bal_df['Product'] == prod_sel]

    # Aggregate by depot
    depot_agg = (
        bal_df.groupby('DEPOT')[col_bal]
        .sum()
        .reset_index()
        .rename(columns={col_bal: 'stock', 'DEPOT': 'depot'})
    )

    max_stock = depot_agg['stock'].max() or 1

    # ── Build plotly map ──────────────────────────────────────────────────────
    map_rows = []
    unmatched = []
    for _, row in depot_agg.iterrows():
        coords = _guess_coords(row['depot'])
        if coords:
            map_rows.append({
                'depot': row['depot'],
                'stock': row['stock'],
                'lat': coords[0],
                'lon': coords[1],
                'pct': row['stock'] / max_stock * 100,
            })
        else:
            unmatched.append(row['depot'])

    if map_rows:
        map_df = pd.DataFrame(map_rows)
        map_df['color'] = map_df['pct'].apply(
            lambda p: '#ff0000' if p < 10 else '#ffaa00' if p < 25 else '#ffdd00' if p < 50 else '#00ff88'
        )
        map_df['status'] = map_df['pct'].apply(
            lambda p: '🔴 CRITICAL' if p < 10 else '🟡 LOW' if p < 25 else '🟠 MODERATE' if p < 50 else '🟢 HEALTHY'
        )
        map_df['stock_fmt'] = map_df['stock'].apply(lambda x: f"{x:,.0f} LT")

        fig_map = go.Figure()

        for _, r in map_df.iterrows():
            fig_map.add_trace(go.Scattergeo(
                lat=[r['lat']], lon=[r['lon']],
                mode='markers+text',
                marker=dict(
                    size=max(12, min(50, r['pct'] * 0.5 + 10)),
                    color=r['color'],
                    opacity=0.85,
                    line=dict(width=2, color='white'),
                ),
                text=r['depot'][:20],
                textposition='top center',
                textfont=dict(color='white', size=10),
                hovertemplate=(
                    f"<b>{r['depot']}</b><br>"
                    f"Stock: {r['stock_fmt']}<br>"
                    f"Relative: {r['pct']:.1f}%<br>"
                    f"Status: {r['status']}<extra></extra>"
                ),
                name=r['status'],
                showlegend=False,
            ))

        fig_map.update_layout(
            geo=dict(
                scope='africa',
                center=dict(lat=7.9, lon=-1.0),
                projection_scale=12,
                showland=True, landcolor='rgba(22,33,62,0.9)',
                showocean=True, oceancolor='rgba(10,14,39,0.95)',
                showcoastlines=True, coastlinecolor='rgba(0,255,255,0.4)',
                showframe=False,
                bgcolor='rgba(10,14,39,0)',
            ),
            paper_bgcolor='rgba(10,14,39,0)',
            height=520,
            margin=dict(l=0, r=0, t=0, b=0),
        )
        st.plotly_chart(fig_map, width='stretch')

        st.markdown("---")
        st.markdown("### 🏭 DEPOT STOCK RANKING")

        # Colour-coded bar chart
        fig_bar = go.Figure(go.Bar(
            x=map_df.sort_values('stock', ascending=True)['depot'],
            y=map_df.sort_values('stock', ascending=True)['stock'],
            marker_color=map_df.sort_values('stock', ascending=True)['color'],
            text=map_df.sort_values('stock', ascending=True)['stock_fmt'],
            textposition='outside',
        ))
        fig_bar.update_layout(
            paper_bgcolor='rgba(10,14,39,0.9)', plot_bgcolor='rgba(10,14,39,0.9)',
            font=dict(color='white'), height=380,
            xaxis=dict(tickangle=-30),
            yaxis=dict(gridcolor='rgba(255,255,255,0.05)', title='Stock (LT)'),
        )
        st.plotly_chart(fig_bar, width='stretch')

    else:
        st.warning("⚠️ No depot coordinates matched. Showing table instead.")

    if unmatched:
        st.caption(f"⚠️ Depots without map coordinates (table only): {', '.join(set(unmatched))}")

    st.markdown("---")
    st.markdown("### 📋 FULL DEPOT TABLE")
    display_tbl = depot_agg.copy()
    display_tbl['stock'] = display_tbl['stock'].apply(lambda x: f"{x:,.0f}")
    st.dataframe(display_tbl.rename(columns={'depot':'Depot','stock':'Stock (LT)'}),
                 width='stretch', hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. 🔮 DEMAND FORECAST
# ═══════════════════════════════════════════════════════════════════════════════

def show_demand_forecast():
    st.markdown("<h2>🔮 DEMAND FORECAST</h2>", unsafe_allow_html=True)
    st.markdown("""
    <p style='color:#ff00ff; font-size:16px;'>
    Uses OMC Loadings history to project future demand per OMC and nationally.
    Based on weighted moving average — more recent weeks count more.
    </p>
    """, unsafe_allow_html=True)
    st.markdown("---")

    has_loadings = not st.session_state.get('omc_df', pd.DataFrame()).empty
    if not has_loadings:
        st.warning("⚠️ OMC Loadings data required. Fetch it from 🚚 OMC LOADINGS first.")
        return

    df = st.session_state.omc_df.copy()
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Date'])

    if df.empty:
        st.warning("⚠️ No valid date rows in OMC Loadings.")
        return

    col1, col2 = st.columns(2)
    with col1:
        forecast_weeks = st.slider("Forecast horizon (weeks)", 1, 12, 4, key='df_weeks')
    with col2:
        view_mode = st.radio("View", ["National by Product", "By OMC"], horizontal=True, key='df_view')

    # ── Compute weekly actuals ────────────────────────────────────────────────
    df['week'] = df['Date'].dt.to_period('W').apply(lambda p: p.start_time)

    COLORS = {'PREMIUM':'#00ffff','GASOIL':'#ffaa00','LPG':'#00ff88'}

    if view_mode == "National by Product":
        weekly = df.groupby(['week', 'Product'])['Quantity'].sum().reset_index()
        products = [p for p in ['PREMIUM','GASOIL','LPG'] if p in weekly['Product'].unique()]

        fig = go.Figure()
        forecast_summary = []

        for prod in products:
            pdata = weekly[weekly['Product'] == prod].sort_values('week')
            if len(pdata) < 2:
                continue

            # Weighted moving average (exponential weights)
            vals = pdata['Quantity'].values
            n = len(vals)
            weights = [0.5 ** (n - 1 - i) for i in range(n)]
            wsum = sum(weights)
            wma  = sum(w * v for w, v in zip(weights, vals)) / wsum

            # Project forward
            last_week = pdata['week'].iloc[-1]
            future_weeks = [last_week + timedelta(weeks=i+1) for i in range(forecast_weeks)]
            # Simple linear trend on last 4 weeks
            if n >= 4:
                recent = vals[-4:]
                trend  = (recent[-1] - recent[0]) / 3
            else:
                trend = 0
            proj_vals = [max(0, wma + trend * (i + 1)) for i in range(forecast_weeks)]

            # Actual line
            fig.add_trace(go.Scatter(
                x=pdata['week'], y=pdata['Quantity'],
                mode='lines+markers', name=f"{prod} actual",
                line=dict(color=COLORS[prod], width=2),
                marker=dict(size=7),
            ))
            # Forecast line
            fig.add_trace(go.Scatter(
                x=future_weeks, y=proj_vals,
                mode='lines+markers', name=f"{prod} forecast",
                line=dict(color=COLORS[prod], width=2, dash='dash'),
                marker=dict(size=7, symbol='diamond'),
            ))

            forecast_summary.append({
                'Product': prod,
                'Recent Weekly Avg (LT)': f"{wma:,.0f}",
                'Weekly Trend': f"{trend:+,.0f} LT/week",
                f'Week+1 Projected (LT)': f"{proj_vals[0]:,.0f}",
                f'Week+{forecast_weeks} Projected (LT)': f"{proj_vals[-1]:,.0f}",
                f'{forecast_weeks}wk Total (LT)': f"{sum(proj_vals):,.0f}",
            })

        # Shaded forecast region
        if future_weeks:
            fig.add_vrect(
                x0=future_weeks[0], x1=future_weeks[-1],
                fillcolor='rgba(255,0,255,0.05)', layer='below',
                line_width=0, annotation_text="FORECAST ZONE",
                annotation_font_color='#ff00ff',
            )

        fig.update_layout(
            paper_bgcolor='rgba(10,14,39,0.9)', plot_bgcolor='rgba(10,14,39,0.9)',
            font=dict(color='white'), height=440,
            legend=dict(font=dict(color='white')),
            xaxis=dict(gridcolor='rgba(255,255,255,0.05)', title='Week'),
            yaxis=dict(gridcolor='rgba(255,255,255,0.05)', title='Volume (LT)'),
            title=dict(text='Weekly OMC Loadings + Forecast', font=dict(color='#00ffff', family='Orbitron')),
        )
        st.plotly_chart(fig, width='stretch')

        if forecast_summary:
            st.markdown("### 📋 FORECAST SUMMARY")
            st.dataframe(pd.DataFrame(forecast_summary), width='stretch', hide_index=True)

    else:  # By OMC
        prod_filter = st.selectbox("Product", ['PREMIUM', 'GASOIL', 'LPG'], key='df_omc_prod')
        df_p = df[df['Product'] == prod_filter]
        weekly_omc = df_p.groupby(['week', 'OMC'])['Quantity'].sum().reset_index()

        top_omcs = (
            df_p.groupby('OMC')['Quantity'].sum()
            .sort_values(ascending=False)
            .head(10).index.tolist()
        )
        omc_sel = st.multiselect("Select OMCs", top_omcs, default=top_omcs[:5], key='df_omc_sel')

        fig2 = go.Figure()
        omc_forecast_rows = []

        palette = ['#00ffff','#ff00ff','#00ff88','#ffaa00','#ff6600',
                   '#ff4488','#44ffdd','#ffdd44','#aa44ff','#ff8844']

        for idx, omc in enumerate(omc_sel):
            odata = weekly_omc[weekly_omc['OMC'] == omc].sort_values('week')
            if len(odata) < 1: continue

            vals = odata['Quantity'].values
            n    = len(vals)
            weights = [0.5 ** (n - 1 - i) for i in range(n)]
            wma = sum(w * v for w, v in zip(weights, vals)) / sum(weights)
            trend = (vals[-1] - vals[0]) / max(n - 1, 1)
            last_week = odata['week'].iloc[-1]
            future_weeks = [last_week + timedelta(weeks=i+1) for i in range(forecast_weeks)]
            proj_vals = [max(0, wma + trend * (i + 1)) for i in range(forecast_weeks)]

            col = palette[idx % len(palette)]
            fig2.add_trace(go.Scatter(
                x=odata['week'], y=odata['Quantity'],
                mode='lines+markers', name=f"{omc[:20]}",
                line=dict(color=col, width=2), marker=dict(size=6),
            ))
            fig2.add_trace(go.Scatter(
                x=future_weeks, y=proj_vals,
                mode='lines', name=f"{omc[:20]} fcst",
                line=dict(color=col, width=2, dash='dash'), showlegend=False,
            ))
            omc_forecast_rows.append({
                'OMC': omc,
                'WMA (LT/wk)': f"{wma:,.0f}",
                'Trend': f"{trend:+,.0f}/wk",
                f'Wk+1': f"{proj_vals[0]:,.0f}",
                f'{forecast_weeks}wk Total': f"{sum(proj_vals):,.0f}",
            })

        fig2.update_layout(
            paper_bgcolor='rgba(10,14,39,0.9)', plot_bgcolor='rgba(10,14,39,0.9)',
            font=dict(color='white'), height=440,
            legend=dict(font=dict(color='white', size=10)),
            xaxis=dict(gridcolor='rgba(255,255,255,0.05)'),
            yaxis=dict(gridcolor='rgba(255,255,255,0.05)', title='Volume (LT)'),
        )
        st.plotly_chart(fig2, width='stretch')

        if omc_forecast_rows:
            st.markdown("### 📋 OMC FORECAST TABLE")
            st.dataframe(pd.DataFrame(omc_forecast_rows), width='stretch', hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# 5. ⚠️ REORDER ALERTS
# ═══════════════════════════════════════════════════════════════════════════════

def show_reorder_alerts():
    st.markdown("<h2>⚠️ REORDER ALERTS</h2>", unsafe_allow_html=True)
    st.markdown("""
    <p style='color:#ff00ff; font-size:16px;'>
    Per-BDC stockout forecast. Combines each BDC's current stock with their
    OMC loading rate to give individual days-of-supply and reorder recommendations.
    </p>
    """, unsafe_allow_html=True)
    st.markdown("---")

    has_balance  = bool(st.session_state.get('bdc_records'))
    has_loadings = not st.session_state.get('omc_df', pd.DataFrame()).empty

    if not has_balance:
        st.warning("⚠️ BDC Balance required — fetch from 🏦 BDC BALANCE first.")
    if not has_loadings:
        st.warning("⚠️ OMC Loadings required — fetch from 🚚 OMC LOADINGS first.")
    if not has_balance or not has_loadings:
        return

    bal_df  = pd.DataFrame(st.session_state.bdc_records)
    omc_df  = st.session_state.omc_df.copy()
    col_bal = 'ACTUAL BALANCE (LT\\KG)'

    # ── Thresholds ────────────────────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)
    with col1:
        crit_days = st.number_input("Critical threshold (days)", value=5,  min_value=1, max_value=30)
    with col2:
        warn_days = st.number_input("Warning threshold (days)",  value=10, min_value=1, max_value=60)
    with col3:
        reorder_buffer = st.number_input("Reorder buffer (days)",       value=7,  min_value=1, max_value=30,
            help="Days of extra stock to recommend ordering")

    omc_df['Date'] = pd.to_datetime(omc_df['Date'], errors='coerce')
    omc_df = omc_df.dropna(subset=['Date'])
    period_days_ra = max((omc_df['Date'].max() - omc_df['Date'].min()).days, 1) if not omc_df.empty else 30

    # BDC stock by product
    bdc_stock = bal_df.groupby(['BDC','Product'])[col_bal].sum().reset_index()
    bdc_stock.columns = ['BDC','Product','stock']

    # BDC depletion (OMC loadings FROM each BDC)
    if 'BDC' in omc_df.columns:
        bdc_dep = (
            omc_df[omc_df['Product'].isin(['PREMIUM','GASOIL','LPG'])]
            .groupby(['BDC','Product'])['Quantity']
            .sum()
            .reset_index()
        )
        bdc_dep.columns = ['BDC','Product','depletion']
        bdc_dep['daily_rate'] = bdc_dep['depletion'] / period_days_ra
    else:
        st.warning("⚠️ BDC column not found in OMC Loadings — cannot compute per-BDC depletion.")
        return

    merged = bdc_stock.merge(bdc_dep, on=['BDC','Product'], how='left')
    merged['daily_rate'] = merged['daily_rate'].fillna(0)
    merged['days_remaining'] = merged.apply(
        lambda r: r['stock'] / r['daily_rate'] if r['daily_rate'] > 0 else float('inf'),
        axis=1
    )
    merged['reorder_qty'] = merged.apply(
        lambda r: max(0, r['daily_rate'] * (warn_days + reorder_buffer) - r['stock'])
        if r['daily_rate'] > 0 else 0,
        axis=1
    )

    def _status(d):
        if d == float('inf'): return '⚪ NO DATA'
        if d < crit_days:     return '🔴 CRITICAL'
        if d < warn_days:     return '🟡 WARNING'
        if d < 30:            return '🟠 MONITOR'
        return '🟢 HEALTHY'

    merged['status'] = merged['days_remaining'].apply(_status)

    # ── Alert summary ─────────────────────────────────────────────────────────
    critical_rows = merged[merged['days_remaining'] < crit_days]
    warning_rows  = merged[(merged['days_remaining'] >= crit_days) & (merged['days_remaining'] < warn_days)]

    c1, c2, c3 = st.columns(3)
    c1.metric("🔴 Critical BDC-Products", len(critical_rows))
    c2.metric("🟡 Warning BDC-Products",  len(warning_rows))
    c3.metric("BDCs Analysed", merged['BDC'].nunique())

    if not critical_rows.empty:
        st.error("🚨 CRITICAL — Immediate reorder required for:")
        for _, r in critical_rows.sort_values('days_remaining').iterrows():
            st.markdown(
                f"**{r['BDC']}** — {r['Product']}: "
                f"**{r['days_remaining']:.1f} days** remaining | "
                f"Reorder: **{r['reorder_qty']:,.0f} LT**"
            )

    if not warning_rows.empty:
        st.warning("⚠️ WARNING — Plan reorder within 48h for:")
        for _, r in warning_rows.sort_values('days_remaining').iterrows():
            st.markdown(
                f"**{r['BDC']}** — {r['Product']}: "
                f"**{r['days_remaining']:.1f} days** remaining | "
                f"Reorder: **{r['reorder_qty']:,.0f} LT**"
            )

    st.markdown("---")
    st.markdown("### 📋 FULL BDC REORDER TABLE")

    prod_filter_ra = st.selectbox("Filter by Product", ['ALL','PREMIUM','GASOIL','LPG'], key='ra_prod')
    stat_filter_ra = st.selectbox("Filter by Status",
        ['ALL','🔴 CRITICAL','🟡 WARNING','🟠 MONITOR','🟢 HEALTHY','⚪ NO DATA'], key='ra_stat')

    display_ra = merged.copy()
    if prod_filter_ra != 'ALL':
        display_ra = display_ra[display_ra['Product'] == prod_filter_ra]
    if stat_filter_ra != 'ALL':
        display_ra = display_ra[display_ra['status'] == stat_filter_ra]

    display_ra = display_ra.sort_values('days_remaining')
    display_ra['days_remaining'] = display_ra['days_remaining'].apply(
        lambda x: f"{x:.1f}" if x != float('inf') else "∞")
    display_ra['stock']       = display_ra['stock'].apply(lambda x: f"{x:,.0f}")
    display_ra['depletion']   = display_ra['depletion'].fillna(0).apply(lambda x: f"{x:,.0f}")
    display_ra['daily_rate']  = display_ra['daily_rate'].apply(lambda x: f"{x:,.0f}")
    display_ra['reorder_qty'] = display_ra['reorder_qty'].apply(lambda x: f"{x:,.0f}")

    st.dataframe(
        display_ra[['BDC','Product','stock','depletion','daily_rate','days_remaining','reorder_qty','status']]
        .rename(columns={
            'BDC':'BDC','Product':'Product','stock':'Current Stock (LT)',
            'depletion':'Period Depletion (LT)','daily_rate':'Daily Rate (LT/d)',
            'days_remaining':'Days of Supply','reorder_qty':'Reorder Qty (LT)','status':'Status'
        }),
        width='stretch', hide_index=True
    )

    st.markdown("---")
    if st.button("💾 EXPORT REORDER REPORT", key='ra_export'):
        out_dir = os.path.join(os.getcwd(), "reorder_reports")
        os.makedirs(out_dir, exist_ok=True)
        fname = f"reorder_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        fpath = os.path.join(out_dir, fname)
        merged.to_excel(fpath, index=False)
        with open(fpath, 'rb') as f:
            st.download_button("⬇️ DOWNLOAD", f, fname,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key='ra_dl')


# ═══════════════════════════════════════════════════════════════════════════════
# 6. 📆 WEEK-ON-WEEK COMPARISON
# ═══════════════════════════════════════════════════════════════════════════════

def show_week_on_week():
    st.markdown("<h2>📆 WEEK-ON-WEEK COMPARISON</h2>", unsafe_allow_html=True)
    st.markdown("""
    <p style='color:#ff00ff; font-size:16px;'>
    Fetch two date ranges and compare side-by-side: which BDCs loaded more,
    which OMCs dropped off, which products shifted. The delta view that answers
    every operations meeting question instantly.
    </p>
    """, unsafe_allow_html=True)
    st.markdown("---")

    st.markdown("### 📅 SELECT TWO PERIODS TO COMPARE")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### 📘 Period A (e.g. last week)")
        a_start = st.date_input("A: From", value=datetime.now() - timedelta(days=14), key='wow_a_start')
        a_end   = st.date_input("A: To",   value=datetime.now() - timedelta(days=8),  key='wow_a_end')
    with col2:
        st.markdown("#### 📗 Period B (e.g. this week)")
        b_start = st.date_input("B: From", value=datetime.now() - timedelta(days=7), key='wow_b_start')
        b_end   = st.date_input("B: To",   value=datetime.now(),                      key='wow_b_end')

    if st.button("⚡ FETCH & COMPARE", key='wow_fetch', width='content'):
        a_days = max((a_end - a_start).days, 1)
        b_days = max((b_end - b_start).days, 1)

        with st.status("Fetching Period A…", expanded=True) as sa:
            df_a = _fetch_national_omc_loadings(
                a_start.strftime("%m/%d/%Y"), a_end.strftime("%m/%d/%Y"))
            sa.update(label=f"✅ Period A: {len(df_a):,} records", state="complete")

        with st.status("Fetching Period B…", expanded=True) as sb:
            df_b = _fetch_national_omc_loadings(
                b_start.strftime("%m/%d/%Y"), b_end.strftime("%m/%d/%Y"))
            sb.update(label=f"✅ Period B: {len(df_b):,} records", state="complete")

        st.session_state.wow_a = {'df': df_a, 'label': f"{a_start} → {a_end}", 'days': a_days}
        st.session_state.wow_b = {'df': df_b, 'label': f"{b_start} → {b_end}", 'days': b_days}
        st.rerun()

    if not st.session_state.get('wow_a'):
        st.info("👆 Select two periods and click **FETCH & COMPARE**.")
        return

    wa = st.session_state.wow_a
    wb = st.session_state.wow_b
    df_a, df_b = wa['df'], wb['df']
    label_a, label_b = wa['label'], wb['label']
    days_a, days_b   = wa['days'],  wb['days']

    COLORS = {'PREMIUM':'#00ffff','GASOIL':'#ffaa00','LPG':'#00ff88'}
    PRODUCTS = ['PREMIUM','GASOIL','LPG']

    # ── National product comparison ───────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 🛢️ NATIONAL VOLUME BY PRODUCT")

    vol_a = df_a[df_a['Product'].isin(PRODUCTS)].groupby('Product')['Quantity'].sum() if not df_a.empty else pd.Series()
    vol_b = df_b[df_b['Product'].isin(PRODUCTS)].groupby('Product')['Quantity'].sum() if not df_b.empty else pd.Series()

    prod_rows = []
    cols = st.columns(3)
    for ci, prod in enumerate(PRODUCTS):
        va = float(vol_a.get(prod, 0))
        vb = float(vol_b.get(prod, 0))
        da_rate = va / days_a if days_a > 0 else 0
        db_rate = vb / days_b if days_b > 0 else 0
        delta_abs = vb - va
        delta_pct = ((vb - va) / va * 100) if va > 0 else 0
        arrow = "↑" if delta_abs > 0 else "↓"
        dcol  = "#00ff88" if delta_abs > 0 else "#ff4444"

        with cols[ci]:
            st.markdown(f"""
            <div style='background:rgba(10,14,39,0.85); padding:20px; border-radius:14px;
                        border:2px solid {COLORS[prod]}; text-align:center; margin-bottom:8px;'>
                <div style='font-family:Orbitron,sans-serif; color:{COLORS[prod]};
                             font-size:15px; font-weight:700; margin-bottom:10px;'>{prod}</div>
                <div style='color:#888; font-size:11px;'>{label_a}</div>
                <div style='color:#e0e0e0; font-size:20px; font-weight:700;'>{va:,.0f} LT</div>
                <div style='color:#888; font-size:11px; margin-top:6px;'>{label_b}</div>
                <div style='color:#ffffff; font-size:24px; font-weight:700;'>{vb:,.0f} LT</div>
                <div style='color:{dcol}; font-size:18px; font-weight:700; margin-top:8px;'>
                    {arrow} {abs(delta_abs):,.0f} LT ({delta_pct:+.1f}%)</div>
                <div style='color:#888; font-size:11px; margin-top:6px;'>
                    A: {da_rate:,.0f} LT/d → B: {db_rate:,.0f} LT/d</div>
            </div>
            """, unsafe_allow_html=True)
        prod_rows.append({'Product':prod,'Period A (LT)':f"{va:,.0f}",
                          'Period B (LT)':f"{vb:,.0f}",'Delta':f"{delta_abs:+,.0f}",
                          'Change %':f"{delta_pct:+.1f}%"})

    st.markdown("---")

    # ── BDC comparison ────────────────────────────────────────────────────────
    st.markdown("### 🏭 BDC-LEVEL COMPARISON")
    prod_wow = st.selectbox("Product", ['ALL'] + PRODUCTS, key='wow_prod')

    def _bdc_vol(df, prod):
        if df.empty: return pd.Series(dtype=float)
        f = df if prod == 'ALL' else df[df['Product'] == prod]
        return f.groupby('BDC')['Quantity'].sum() if 'BDC' in f.columns else pd.Series(dtype=float)

    bdc_a = _bdc_vol(df_a, prod_wow)
    bdc_b = _bdc_vol(df_b, prod_wow)
    all_bdcs = sorted(set(bdc_a.index) | set(bdc_b.index))

    bdc_rows = []
    for bdc in all_bdcs:
        va = float(bdc_a.get(bdc, 0))
        vb = float(bdc_b.get(bdc, 0))
        delta = vb - va
        pct   = ((vb - va) / va * 100) if va > 0 else (100.0 if vb > 0 else 0.0)
        bdc_rows.append({'BDC': bdc, 'Period A (LT)': va, 'Period B (LT)': vb,
                         'Delta (LT)': delta, 'Change %': round(pct, 1)})

    bdc_cmp = pd.DataFrame(bdc_rows).sort_values('Delta (LT)', ascending=False)

    # Visual bar chart
    fig_bdc = go.Figure()
    fig_bdc.add_trace(go.Bar(
        name=label_a, x=bdc_cmp['BDC'], y=bdc_cmp['Period A (LT)'],
        marker_color='rgba(0,255,255,0.6)',
    ))
    fig_bdc.add_trace(go.Bar(
        name=label_b, x=bdc_cmp['BDC'], y=bdc_cmp['Period B (LT)'],
        marker_color='rgba(255,0,255,0.6)',
    ))
    fig_bdc.update_layout(
        barmode='group',
        paper_bgcolor='rgba(10,14,39,0.9)', plot_bgcolor='rgba(10,14,39,0.9)',
        font=dict(color='white'), height=420,
        legend=dict(font=dict(color='white')),
        xaxis=dict(tickangle=-30, gridcolor='rgba(255,255,255,0.05)'),
        yaxis=dict(gridcolor='rgba(255,255,255,0.05)', title='Volume (LT)'),
    )
    st.plotly_chart(fig_bdc, width='stretch')

    st.markdown("#### 🔺 Biggest Movers")
    disp_bdc = bdc_cmp.copy()
    disp_bdc['Period A (LT)'] = disp_bdc['Period A (LT)'].apply(lambda x: f"{x:,.0f}")
    disp_bdc['Period B (LT)'] = disp_bdc['Period B (LT)'].apply(lambda x: f"{x:,.0f}")
    disp_bdc['Delta (LT)']    = disp_bdc['Delta (LT)'].apply(lambda x: f"{x:+,.0f}")
    disp_bdc['Change %']      = disp_bdc['Change %'].apply(lambda x: f"{x:+.1f}%")
    st.dataframe(disp_bdc, width='stretch', hide_index=True)

    st.markdown("---")

    # ── OMC comparison ────────────────────────────────────────────────────────
    st.markdown("### 🏢 TOP OMC MOVERS")

    def _omc_vol(df, prod):
        if df.empty or 'OMC' not in df.columns: return pd.Series(dtype=float)
        f = df if prod == 'ALL' else df[df['Product'] == prod]
        return f.groupby('OMC')['Quantity'].sum()

    omc_a = _omc_vol(df_a, prod_wow)
    omc_b = _omc_vol(df_b, prod_wow)
    all_omcs = sorted(set(omc_a.index) | set(omc_b.index))

    omc_rows = []
    for omc in all_omcs:
        va = float(omc_a.get(omc, 0))
        vb = float(omc_b.get(omc, 0))
        delta = vb - va
        pct   = ((vb - va) / va * 100) if va > 0 else (100.0 if vb > 0 else 0.0)
        omc_rows.append({'OMC': omc, 'Period A (LT)': va, 'Period B (LT)': vb,
                         'Delta (LT)': delta, 'Change %': round(pct, 1)})

    omc_cmp = pd.DataFrame(omc_rows).sort_values('Delta (LT)', ascending=False)

    top_gainers = omc_cmp.head(5)
    top_losers  = omc_cmp.tail(5).iloc[::-1]

    col_g, col_l = st.columns(2)
    with col_g:
        st.markdown("##### 🟢 Top 5 Gainers")
        for _, r in top_gainers.iterrows():
            st.markdown(f"**{r['OMC'][:30]}** — {r['Delta (LT)']:+,.0f} LT ({r['Change %']:+.1f}%)")
    with col_l:
        st.markdown("##### 🔴 Top 5 Decliners")
        for _, r in top_losers.iterrows():
            st.markdown(f"**{r['OMC'][:30]}** — {r['Delta (LT)']:+,.0f} LT ({r['Change %']:+.1f}%)")

    st.markdown("---")
    if st.button("💾 EXPORT COMPARISON", key='wow_export'):
        out_dir = os.path.join(os.getcwd(), "wow_reports")
        os.makedirs(out_dir, exist_ok=True)
        fname = f"wow_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        fpath = os.path.join(out_dir, fname)
        with pd.ExcelWriter(fpath, engine='openpyxl') as writer:
            pd.DataFrame(prod_rows).to_excel(writer, sheet_name='Product Summary', index=False)
            bdc_cmp.to_excel(writer,  sheet_name='BDC Comparison', index=False)
            omc_cmp.to_excel(writer,  sheet_name='OMC Comparison', index=False)
            if not df_a.empty: df_a.to_excel(writer, sheet_name='Period A Raw', index=False)
            if not df_b.empty: df_b.to_excel(writer, sheet_name='Period B Raw', index=False)
        with open(fpath, 'rb') as f:
            st.download_button("⬇️ DOWNLOAD", f, fname,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key='wow_dl')