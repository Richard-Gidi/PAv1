"""
NPA ENERGY ANALYTICS - STREAMLIT DASHBOARD
===========================================

INSTALLATION:
pip install streamlit pandas pdfplumber PyPDF2 openpyxl

USAGE:
streamlit run npa_dashboard.py
"""

import streamlit as st
import os
import re
from datetime import datetime
import pandas as pd
import pdfplumber
import PyPDF2

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
            cleaned = m.group(1).replace(" ,", ",").replace("  ", " ")
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
        """Parse text content from web page"""
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
            date_obj = datetime.strptime(date_token, "%Y/%m/%d")
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
    """Extract NPA data from PDF file or file-like object"""
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
    """Parse text content from web page like we'd parse a PDF"""
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
    """Converts '54,000.00' -> 54000.0"""
    if not value_str: return 0.0
    try:
        return float(value_str.replace(",", "").strip())
    except:
        return 0.0

def get_product_category(text):
    """Determines product category from line text."""
    text_upper = text.upper()
    if "AVIATION" in text_upper or "TURBINE" in text_upper: return "ATK"
    if "RFO" in text_upper: return "RFO"
    if "PREMIX" in text_upper: return "PREMIX"
    if "LPG" in text_upper: return "LPG"
    if "AGO" in text_upper or "MGO" in text_upper or "GASOIL" in text_upper: return "GASOIL"
    if "PMS" in text_upper or "PREMIUM" in text_upper: return "PREMIUM"
    return "PREMIUM"

def parse_daily_line(line, last_known_date):
    """Parses a single line of text to extract order details."""
    line = line.strip()
    
    # Regex to find Price and Volume at the end
    pv_match = re.search(r"(\d{1,4}\.\d{2,4})\s+(\d{1,3}(?:,\d{3})*\.\d{2})$", line)
    
    if not pv_match:
        return None

    price_str = pv_match.group(1)
    vol_str = pv_match.group(2)
    
    price = clean_currency(price_str)
    volume = clean_currency(vol_str)

    remainder = line[:pv_match.start()].strip()
    
    # Extract BRV (Truck Number)
    tokens = remainder.split()
    if not tokens: return None
    
    brv = tokens[-1]
    tokens = tokens[:-1]
    remainder = " ".join(tokens)

    # Extract Date
    date_val = last_known_date
    date_match = re.search(r"(\d{2}/\d{2}/\d{4})", remainder)
    
    if date_match:
        date_val = date_match.group(1)
        # Convert to YYYY/MM/DD format
        try:
            date_obj = datetime.strptime(date_val, "%d/%m/%Y")
            date_val = date_obj.strftime("%Y/%m/%d")
        except:
            pass
        remainder = remainder.replace(date_match.group(1), "").strip()
    
    # Extract Product and Order Number
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
    """Take the first 2 words of every BDC name."""
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
    """Extract Daily Orders from PDF file."""
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
                    
                    # Update Context Headers
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
                        
                    # Parse Data Row
                    if not re.search(r"\d{2}$", clean):
                        continue
                        
                    row_data = parse_daily_line(clean, ctx["Date"])
                    
                    if row_data:
                        if row_data["Date"]:
                            ctx["Date"] = row_data["Date"]
                        
                        final_row = {
                            "Date": row_data["Date"],
                            "OMC": ctx["BDC"],
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
        df["OMC"] = df["BDC"]
        
    return df

def save_daily_orders_excel(df: pd.DataFrame, filename: str = None) -> str:
    """Save daily orders to Excel with summary."""
    out_dir = os.path.join(os.getcwd(), "daily_orders")
    os.makedirs(out_dir, exist_ok=True)
    if filename is None:
        filename = f"daily_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    out_path = os.path.join(out_dir, filename)
    
    # Create Summary Pivot
    if not df.empty:
        pivot = df.pivot_table(
            index="BDC", 
            columns="Product", 
            values="Quantity", 
            aggfunc="sum", 
            fill_value=0
        ).reset_index()
        
        # Calculate Grand Total
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
        choice = st.radio("SELECT YOUR DATA MISSION:", ["🏦 BDC BALANCE", "🚚 OMC LOADINGS", "📅 DAILY ORDERS"], index=0)
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
    else:
        show_daily_orders()

def show_bdc_balance():
    st.markdown("<h2>🏦 BDC STOCK BALANCE ANALYZER</h2>", unsafe_allow_html=True)
    st.info("📊 Click the button below to fetch BDC Balance data")
    st.markdown("---")
    
    # Initialize session state for storing data
    if 'bdc_records' not in st.session_state:
        st.session_state.bdc_records = []
    
    if st.button("🔄 FETCH BDC BALANCE DATA", use_container_width=True):
        with st.spinner("🔄 FETCHING DATA FROM NPA PORTAL..."):
            scraper = StockBalanceScraper()
            
            # Fetch data from URL
            url = "https://iml.npa-enterprise.com/NPAAPILIVE/Home/CreateProductBalance"
            params = {
                'lngCompanyId': '1',
                'strITSfromPersol': 'Persol Systems Limited',
                'strGroupBy': 'BDC',
                'strGroupBy1': 'DEPOT',
                'strQuery1': '',
                'strQuery2': '',
                'strQuery3': '',
                'strQuery4': '',
                'strPicHeight': '1',
                'szPicWeight': '1',
                'lngUserId': '123292',
                'intAppId': '3'
            }
            
            try:
                import requests
                import io
                
                # Add headers to mimic a browser
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Accept': 'application/pdf,text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Connection': 'keep-alive',
                }
                
                response = requests.get(url, params=params, headers=headers, timeout=30)
                response.raise_for_status()
                
                # Check if response is PDF
                if response.content[:4] == b'%PDF':
                    st.success("✅ PDF received from server")
                    
                    # Create a file-like object from the response content
                    pdf_file = io.BytesIO(response.content)
                    
                    # Parse the PDF and store in session state
                    st.session_state.bdc_records = scraper.parse_pdf_file(pdf_file)
                    
                    if not st.session_state.bdc_records:
                        st.warning("⚠️ No records found in PDF. The PDF might be empty or in an unexpected format.")
                else:
                    st.error("❌ Response is not a PDF. Received content type: " + response.headers.get('Content-Type', 'unknown'))
                    st.session_state.bdc_records = []
                
            except requests.exceptions.RequestException as e:
                st.error(f"❌ Network Error: {e}")
                st.info("The NPA website might be down or blocking requests. Please try again later.")
                st.session_state.bdc_records = []
            except Exception as e:
                st.error(f"❌ Error: {e}")
                import traceback
                st.code(traceback.format_exc())
                st.session_state.bdc_records = []
    
    # Display data if available in session state
    records = st.session_state.bdc_records
    
    if records:
        df = pd.DataFrame(records)
        st.success(f"✅ SUCCESSFULLY EXTRACTED {len(records)} RECORDS")
        st.markdown("---")
        
        # ANALYTICS DASHBOARD
        st.markdown("<h3>📊 ANALYTICS DASHBOARD</h3>", unsafe_allow_html=True)
        
        # Product Totals Summary
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
        
        # BDC Analytics
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
            st.dataframe(bdc_summary, use_container_width=True, hide_index=True)
        with col2:
            st.markdown("#### 📈 Key Metrics")
            st.metric("Total BDCs", f"{df['BDC'].nunique()}")
            st.metric("Total Depots", f"{df['DEPOT'].nunique()}")
            col_name = 'ACTUAL BALANCE (LT\\KG)'
            st.metric("Grand Total", f"{df[col_name].sum():,.0f} LT/KG")
        
        st.markdown("---")
        
        # Product Distribution by BDC
        st.markdown("<h3>📊 PRODUCT DISTRIBUTION BY BDC</h3>", unsafe_allow_html=True)
        pivot_data = df.pivot_table(
            index='BDC',
            columns='Product',
            values='ACTUAL BALANCE (LT\\KG)',
            aggfunc='sum',
            fill_value=0
        ).reset_index()
        
        # Ensure all products are present
        for prod in ['GASOIL', 'LPG', 'PREMIUM']:
            if prod not in pivot_data.columns:
                pivot_data[prod] = 0
        
        pivot_data['TOTAL'] = pivot_data[['GASOIL', 'LPG', 'PREMIUM']].sum(axis=1)
        pivot_data = pivot_data.sort_values('TOTAL', ascending=False)
        
        st.dataframe(pivot_data[['BDC', 'GASOIL', 'LPG', 'PREMIUM', 'TOTAL']], use_container_width=True, hide_index=True)
        
        st.markdown("---")
        
        # SEARCH AND FILTER SECTION
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
        
        # Apply filter
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
        st.dataframe(display, use_container_width=True, height=400, hide_index=True)
        
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
                st.download_button("⬇️ DOWNLOAD EXCEL", f, os.path.basename(path), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
    else:
        st.info("👆 Click the button above to fetch BDC balance data")

def show_omc_loadings():
    st.markdown("<h2>🚚 OMC LOADINGS ANALYZER</h2>", unsafe_allow_html=True)
    st.info("📊 Select date range and fetch OMC loadings data")
    st.markdown("---")
    
    # Initialize session state for storing data
    if 'omc_df' not in st.session_state:
        st.session_state.omc_df = pd.DataFrame()
    if 'omc_start_date' not in st.session_state:
        # Default to 7 days ago for better chance of finding data
        from datetime import timedelta
        st.session_state.omc_start_date = datetime.now() - timedelta(days=7)
    if 'omc_end_date' not in st.session_state:
        st.session_state.omc_end_date = datetime.now()
    
    # Date inputs
    st.markdown("<h3>📅 SELECT DATE RANGE</h3>", unsafe_allow_html=True)
    st.info("💡 Select a date range where you know there are orders. Try last week or last month for better results.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        start_date = st.date_input("Start Date", value=st.session_state.omc_start_date, key='omc_start')
    with col2:
        end_date = st.date_input("End Date", value=st.session_state.omc_end_date, key='omc_end')
    
    if st.button("🔄 FETCH OMC LOADINGS DATA", use_container_width=True):
        with st.spinner("🔄 FETCHING DATA FROM NPA PORTAL..."):
            # Store dates in session state
            st.session_state.omc_start_date = start_date
            st.session_state.omc_end_date = end_date
            
            # Format dates for URL (DD/MM/YYYY)
            start_str = start_date.strftime("%Y/%m/%d")
            end_str = end_date.strftime("%Y/%m/%d")
            
            # Show what dates we're requesting
            st.info(f"🔍 Requesting orders from **{start_str}** to **{end_str}**")
            
            url = "https://iml.npa-enterprise.com/NewNPA/home/CreateOrdersReport"
            params = {
                'lngCompanyId': '1',
                'szITSfromPersol': 'persol',
                'strGroupBy': 'BDC',
                'strGroupBy1': 'OILCORP ENERGIA LIMITED',
                'strQuery1': ' and iorderstatus=4',
                'strQuery2': start_str,
                'strQuery3': end_str,
                'strQuery4': '',
                'strPicHeight': '',
                'strPicWeight': '',
                'intPeriodID': '4',
                'iUserId': '123292',
                'iAppId': '3'
            }
            
            try:
                import requests
                import io
                
                # Add headers to mimic a browser
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Accept': 'application/pdf,text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Connection': 'keep-alive',
                }
                
                response = requests.get(url, params=params, headers=headers, timeout=30)
                response.raise_for_status()
                
                # Check if response is PDF
                if response.content[:4] == b'%PDF':
                    st.success("✅ PDF received from server")
                    
                    # Create a file-like object from the response content
                    pdf_file = io.BytesIO(response.content)
                    
                    # Parse the PDF and store in session state
                    st.session_state.omc_df = extract_npa_data_from_pdf(pdf_file)
                    
                    if st.session_state.omc_df.empty:
                        st.warning("⚠️ No order records found in the PDF for this date range.")
                        st.info("💡 **This means there were no orders in the selected date range.**")
                        st.markdown("""
                        **Try:**
                        - Select a **wider date range** (e.g., last week or last month)
                        - Select dates you **know have order data**
                        - Check if the date format is correct (the URL expects DD/MM/YYYY)
                        - Try recent dates like yesterday or last week
                        """)
                else:
                    st.error("❌ Response is not a PDF. Received content type: " + response.headers.get('Content-Type', 'unknown'))
                    st.session_state.omc_df = pd.DataFrame()
                
            except requests.exceptions.RequestException as e:
                st.error(f"❌ Network Error: {e}")
                st.info("The NPA website might be down or blocking requests. Please try again later.")
                st.session_state.omc_df = pd.DataFrame()
            except Exception as e:
                st.error(f"❌ Error: {e}")
                import traceback
                st.code(traceback.format_exc())
                st.session_state.omc_df = pd.DataFrame()
    
    # Display data if available in session state
    df = st.session_state.omc_df
    
    if not df.empty:
        st.success(f"✅ EXTRACTED {len(df)} RECORDS")
        st.markdown("---")
        
        # Display date range used
        st.info(f"📊 Showing {len(df)} records from {st.session_state.omc_start_date.strftime('%Y/%m/%d')} to {st.session_state.omc_end_date.strftime('%Y/%m/%d')}")
        
        st.markdown("---")
        
        # ANALYTICS DASHBOARD
        st.markdown("<h3>📊 ANALYTICS DASHBOARD</h3>", unsafe_allow_html=True)
        
        # Overall Summary Metrics
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
        
        # Product Distribution
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
            st.dataframe(product_summary, use_container_width=True, hide_index=True)
        with col2:
            # Product distribution pie chart data
            for _, row in product_summary.iterrows():
                pct = (row['Total Volume (LT/KG)'] / product_summary['Total Volume (LT/KG)'].sum()) * 100
                st.metric(row['Product'], f"{pct:.1f}%")
        
        st.markdown("---")
        
        # Top OMCs
        st.markdown("<h3>🏢 TOP OMCs BY VOLUME</h3>", unsafe_allow_html=True)
        omc_summary = df.groupby('OMC').agg({
            'Quantity': 'sum',
            'Order Number': 'count',
            'Product': lambda x: x.nunique()
        }).reset_index()
        omc_summary.columns = ['OMC', 'Total Volume (LT/KG)', 'Orders', 'Products']
        omc_summary = omc_summary.sort_values('Total Volume (LT/KG)', ascending=False).head(10)
        
        st.dataframe(omc_summary, use_container_width=True, hide_index=True)
        
        st.markdown("---")
        
        # BDC Performance
        st.markdown("<h3>🏦 BDC PERFORMANCE</h3>", unsafe_allow_html=True)
        bdc_summary = df.groupby('BDC').agg({
            'Quantity': 'sum',
            'Order Number': 'count',
            'OMC': 'nunique',
            'Product': lambda x: x.nunique()
        }).reset_index()
        bdc_summary.columns = ['BDC', 'Total Volume (LT/KG)', 'Orders', 'OMCs', 'Products']
        bdc_summary = bdc_summary.sort_values('Total Volume (LT/KG)', ascending=False)
        
        st.dataframe(bdc_summary, use_container_width=True, hide_index=True)
        
        st.markdown("---")
        
        # Product Distribution by BDC
        st.markdown("<h3>📊 PRODUCT DISTRIBUTION BY BDC</h3>", unsafe_allow_html=True)
        pivot_data = df.pivot_table(
            index='BDC',
            columns='Product',
            values='Quantity',
            aggfunc='sum',
            fill_value=0
        ).reset_index()
        
        # Ensure all products are present
        for prod in ['GASOIL', 'LPG', 'PREMIUM']:
            if prod not in pivot_data.columns:
                pivot_data[prod] = 0
        
        pivot_data['TOTAL'] = pivot_data[['GASOIL', 'LPG', 'PREMIUM']].sum(axis=1)
        pivot_data = pivot_data.sort_values('TOTAL', ascending=False)
        
        st.dataframe(pivot_data[['BDC', 'GASOIL', 'LPG', 'PREMIUM', 'TOTAL']], use_container_width=True, hide_index=True)
        
        st.markdown("---")
        
        # SEARCH AND FILTER SECTION
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
        
        # Apply filter
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
        
        # Show filtered summary
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
        st.dataframe(display, use_container_width=True, height=400, hide_index=True)
        
        st.markdown("---")
        st.markdown("<h3>💾 EXPORT DATA</h3>", unsafe_allow_html=True)
        path = save_to_excel_multi(df)
        if path and os.path.exists(path):
            with open(path, 'rb') as f:
                st.download_button("⬇️ DOWNLOAD EXCEL", f, os.path.basename(path), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
    else:
        st.info("👆 Select dates and click the button above to fetch OMC loadings data")

def show_daily_orders():
    st.markdown("<h2>📅 DAILY ORDERS ANALYZER</h2>", unsafe_allow_html=True)
    st.info("📊 Select a date range to fetch daily orders")
    st.markdown("---")
    
    # Initialize session state
    if 'daily_df' not in st.session_state:
        st.session_state.daily_df = pd.DataFrame()
    if 'daily_start_date' not in st.session_state:
        from datetime import timedelta
        st.session_state.daily_start_date = datetime.now() - timedelta(days=1)
    if 'daily_end_date' not in st.session_state:
        st.session_state.daily_end_date = datetime.now()
    
    # Date inputs
    st.markdown("<h3>📅 SELECT DATE RANGE</h3>", unsafe_allow_html=True)
    st.info("💡 Select a date range for daily orders. Try yesterday or last few days for better results.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        start_date = st.date_input("Start Date", value=st.session_state.daily_start_date, key='daily_start')
    with col2:
        end_date = st.date_input("End Date", value=st.session_state.daily_end_date, key='daily_end')
    
    if st.button("🔄 FETCH DAILY ORDERS", use_container_width=True):
        with st.spinner("🔄 FETCHING DAILY ORDERS FROM NPA PORTAL..."):
            st.session_state.daily_start_date = start_date
            st.session_state.daily_end_date = end_date
            
            # Format dates for URL (MM/DD/YYYY based on your example)
            start_str = start_date.strftime("%m/%d/%Y")
            end_str = end_date.strftime("%m/%d/%Y")
            
            st.info(f"🔍 Requesting daily orders from **{start_str}** to **{end_str}**")
            
            url = "https://iml.npa-enterprise.com/NewNPA/home/CreateDailyOrderReport"
            params = {
                'lngCompanyId': '1',
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
                'iUserId': '123292',
                'iAppId': '3'
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
                        st.info("💡 Try selecting a different date with known order activity.")
                else:
                    st.error("❌ Response is not a PDF. Received content type: " + response.headers.get('Content-Type', 'unknown'))
                    st.session_state.daily_df = pd.DataFrame()
                
            except requests.exceptions.RequestException as e:
                st.error(f"❌ Network Error: {e}")
                st.info("The NPA website might be down or blocking requests. Please try again later.")
                st.session_state.daily_df = pd.DataFrame()
            except Exception as e:
                st.error(f"❌ Error: {e}")
                import traceback
                st.code(traceback.format_exc())
                st.session_state.daily_df = pd.DataFrame()
    
    # Display data
    df = st.session_state.daily_df
    
    if not df.empty:
        st.success(f"✅ EXTRACTED {len(df)} DAILY ORDERS")
        st.markdown("---")
        
        st.info(f"📊 Showing {len(df)} orders from {st.session_state.daily_start_date.strftime('%Y/%m/%d')} to {st.session_state.daily_end_date.strftime('%Y/%m/%d')}")
        st.markdown("---")
        
        # ANALYTICS DASHBOARD
        st.markdown("<h3>📊 DAILY ANALYTICS</h3>", unsafe_allow_html=True)
        
        # Overall Summary
        cols = st.columns(4)
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
            total_value = (df['Quantity'] * df['Price']).sum()
            st.markdown(f"""
            <div class='metric-card'>
                <h2>VALUE</h2>
                <h1>₵{total_value:,.0f}</h1>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Product Summary
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
            st.dataframe(product_summary, use_container_width=True, hide_index=True)
        with col2:
            for _, row in product_summary.iterrows():
                pct = (row['Total Volume (LT/KG)'] / product_summary['Total Volume (LT/KG)'].sum()) * 100
                st.metric(row['Product'], f"{pct:.1f}%")
        
        st.markdown("---")
        
        # BDC Summary
        st.markdown("<h3>🏦 BDC SUMMARY</h3>", unsafe_allow_html=True)
        bdc_summary = df.groupby('BDC').agg({
            'Quantity': 'sum',
            'Order Number': 'count',
            'Product': lambda x: x.nunique(),
            'Depot': lambda x: x.nunique()
        }).reset_index()
        bdc_summary.columns = ['BDC', 'Total Volume (LT/KG)', 'Orders', 'Products', 'Depots']
        bdc_summary = bdc_summary.sort_values('Total Volume (LT/KG)', ascending=False)
        
        st.dataframe(bdc_summary, use_container_width=True, hide_index=True)
        
        st.markdown("---")
        
        # Product Distribution by BDC
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
        
        st.dataframe(pivot_data, use_container_width=True, hide_index=True)
        
        st.markdown("---")
        
        # Status Breakdown
        st.markdown("<h3>📋 ORDER STATUS BREAKDOWN</h3>", unsafe_allow_html=True)
        status_summary = df.groupby('Status').agg({
            'Order Number': 'count',
            'Quantity': 'sum'
        }).reset_index()
        status_summary.columns = ['Status', 'Orders', 'Total Volume (LT/KG)']
        st.dataframe(status_summary, use_container_width=True, hide_index=True)
        
        st.markdown("---")
        
        # SEARCH AND FILTER
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
        
        # Apply filter
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
        st.dataframe(display, use_container_width=True, height=400, hide_index=True)
        
        st.markdown("---")
        st.markdown("<h3>💾 EXPORT DATA</h3>", unsafe_allow_html=True)
        path = save_daily_orders_excel(df)
        if path and os.path.exists(path):
            with open(path, 'rb') as f:
                st.download_button("⬇️ DOWNLOAD EXCEL", f, os.path.basename(path), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
        else:
            st.info("👆 Select a date range and click the button above to fetch daily orders")

if __name__ == "__main__":
    main()