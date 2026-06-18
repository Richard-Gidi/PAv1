"""
NPA CORE — headless fetch + parse (no Streamlit)
================================================
The data-fetching and PDF-parsing logic from new_test.py, with all Streamlit
calls removed, so it can run inside a scheduled job (GitHub Actions / cron).

Public API:
    fetch_balance_records(mode="single") -> list[dict]
    fetch_omc_loadings(start_str, end_str, mode="single") -> pd.DataFrame

`mode`:
    "single"  -> one consolidated API call using NPA_USER_ID  (needs few env vars)
    "per_bdc" -> loop every BDC_USER_* credential and aggregate (matches the app's
                 "Per-BDC Aggregation" mode; needs the full BDC_USER_* env vars)
"""

import os, re, io, time, unicodedata
from datetime import datetime

import pandas as pd
import pdfplumber
import PyPDF2
import requests as _requests
from dotenv import load_dotenv

load_dotenv()


# ── name normalisation ────────────────────────────────────────
def _normalise_name(name: str) -> str:
    if not name:
        return ""
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    for suffix in ("limited", "ltd", "company", "co", "ghana", "plc",
                   "llc", "lp", "inc", "corp", "enterprise", "enterprises"):
        s = re.sub(rf"\b{suffix}\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _build_lookup(mapping: dict) -> dict:
    return {_normalise_name(k): k for k in mapping}


# ── env loaders (subset of the app's) ─────────────────────────
def load_bdc_user_map() -> dict:
    _FIXES = {
        "C CLEANED OIL LTD": "C. CLEANED OIL LTD",
        "PK JEGS ENERGY LTD": "P.K JEGS ENERGY LTD",
        "TEMA OIL REFINERY TOR": "TEMA OIL REFINERY (TOR)",
        "SOCIETE NATIONAL BURKINABE SONABHY": "SOCIETE NATIONAL BURKINABE (SONABHY)",
        "BOST G40": "BOST-G40",
    }
    mapping = {}
    for key, value in os.environ.items():
        if not key.startswith("BDC_USER_"):
            continue
        raw_suffix = key[len("BDC_USER_"):].replace("_", " ").strip()
        display = _FIXES.get(raw_suffix, raw_suffix)
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


BDC_USER_MAP      = load_bdc_user_map()
STOCK_PRODUCT_MAP = load_product_mappings()
_BDC_USER_LOOKUP  = _build_lookup(BDC_USER_MAP)

NPA_CONFIG = {
    "COMPANY_ID":      os.getenv("NPA_COMPANY_ID", "1"),
    "USER_ID":         os.getenv("NPA_USER_ID", "123292"),
    "APP_ID":          os.getenv("NPA_APP_ID", "3"),
    "ITS_FROM_PERSOL": os.getenv("NPA_ITS_FROM_PERSOL", "Persol Systems Limited"),
    "BDC_BALANCE_URL": os.getenv("NPA_BDC_BALANCE_URL",
        "https://iml.npa-enterprise.com/NPAAPILIVE/Home/CreateProductBalance"),
    "OMC_LOADINGS_URL": os.getenv("NPA_OMC_LOADINGS_URL",
        "https://iml.npa-enterprise.com/NewNPA/home/CreateOrdersReport"),
    "OMC_NAME":        os.getenv("OMC_NAME", "OILCORP ENERGIA LIMITED"),
}


# ── HTTP ──────────────────────────────────────────────────────
_HTTP_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Accept": "application/pdf,text/html,*/*;q=0.8",
    "Connection": "keep-alive",
}
_HTTP_TIMEOUT = 90


def _fetch_pdf(url: str, params: dict, timeout: int = _HTTP_TIMEOUT):
    try:
        r = _requests.get(url, params=params, headers=_HTTP_HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.content if r.content[:4] == b"%PDF" else None
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════
# BDC BALANCE PARSER  (verbatim logic from new_test.py)
# ══════════════════════════════════════════════════════════════
class StockBalanceScraper:
    def __init__(self):
        self.allowed_products = {"PREMIUM", "GASOIL", "LPG"}
        _pat = "|".join(sorted(self.allowed_products))
        self.product_re = re.compile(
            rf"^({_pat})\s+([\d,]+\.\d{{2}})\s+(-?[\d,]+\.\d{{2}})$",
            flags=re.IGNORECASE)
        self.bost_global_re = re.compile(r"\bBOST\s*GLOBAL\s*DEPOT\b", flags=re.IGNORECASE)

    @staticmethod
    def _ns(text):
        return re.sub(r"\s+", " ", (text or "").strip())

    def _resolve_bdc_name(self, raw_bdc: str) -> str:
        clean = self._ns(raw_bdc)
        norm = _normalise_name(clean)
        if norm in _BDC_USER_LOOKUP:
            return _BDC_USER_LOOKUP[norm]
        best_key, best_len = None, 0
        for nk, display in _BDC_USER_LOOKUP.items():
            if nk and (nk in norm or norm in nk) and len(nk) > best_len:
                best_key, best_len = display, len(nk)
        return best_key if best_key else clean

    def _is_bost_depot(self, depot):
        return self._ns((depot or "").replace("-", " ")).upper().startswith("BOST ")

    def _is_bost_global(self, depot):
        return bool(self.bost_global_re.search(self._ns((depot or "").replace("-", " "))))

    def _parse_date(self, line):
        m = re.search(r"(\w+\s+\d{1,2}\s*,\s*\d{4})", line)
        if m:
            try:
                return datetime.strptime(m.group(1).replace(" ,", ","),
                                         "%B %d, %Y").strftime("%Y/%m/%d")
            except Exception:
                pass
        return None

    @staticmethod
    def _owning_bdc_is_bost(owning_bdc_name: str) -> bool:
        norm = re.sub(r"\s+", " ", (owning_bdc_name or "").strip()).upper()
        return norm == "BOST"

    def _row(self, date, bdc, depot, product, actual, avail):
        return {
            "Date": date, "BDC": bdc, "DEPOT": depot, "Product": product,
            "ACTUAL BALANCE (LT\\KG)": actual, "AVAILABLE BALANCE (LT\\KG)": avail,
        }

    def parse_pdf_bytes(self, pdf_bytes: bytes, owning_bdc_name: str = "") -> list:
        bost_mode = self._owning_bdc_is_bost(owning_bdc_name)
        bost_accum: dict = {}
        records, seen = [], set()
        try:
            reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
            cur_bdc, cur_depot, cur_date = owning_bdc_name, None, None
            for page in reader.pages:
                for line in [ln.strip() for ln in (page.extract_text() or "").split("\n") if ln.strip()]:
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
                            actual = float(m.group(2).replace(",", ""))
                            avail  = float(m.group(3).replace(",", ""))
                            if product not in self.allowed_products or actual <= 0:
                                continue
                            is_bost_dep    = self._is_bost_depot(cur_depot)
                            is_bost_global = self._is_bost_global(cur_depot)
                            if bost_mode:
                                if is_bost_dep:
                                    b = bost_accum.setdefault(product, {"actual": 0.0, "avail": 0.0, "date": cur_date})
                                    b["actual"] += actual; b["avail"] += avail
                                    if cur_date > b["date"]:
                                        b["date"] = cur_date
                                else:
                                    nd = self._ns(cur_depot)
                                    key = (cur_bdc, nd, product, cur_date)
                                    if key not in seen:
                                        seen.add(key)
                                        records.append(self._row(cur_date, cur_bdc, nd, product, actual, avail))
                            else:
                                if is_bost_dep and not is_bost_global:
                                    continue
                                nd = self._ns(cur_depot)
                                key = (cur_bdc, nd, product, cur_date)
                                if key in seen:
                                    continue
                                seen.add(key)
                                records.append(self._row(cur_date, cur_bdc, nd, product, actual, avail))
        except Exception:
            pass
        if bost_mode and bost_accum:
            for product, v in bost_accum.items():
                records.append(self._row(v["date"], owning_bdc_name, "BOST GLOBAL DEPOT",
                                         product, v["actual"], v["avail"]))
        return records

    def parse_pdf_bytes_global(self, pdf_bytes: bytes) -> list:
        records, seen = [], set()
        bost_accum: dict = {}
        cur_bdc = prev_bdc = ""
        cur_depot = cur_date = None

        def _flush(bdc_label):
            for product, v in bost_accum.items():
                records.append(self._row(v["date"], bdc_label, "BOST GLOBAL DEPOT",
                                         product, v["actual"], v["avail"]))
            bost_accum.clear()

        try:
            reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
            for page in reader.pages:
                for line in [ln.strip() for ln in (page.extract_text() or "").split("\n") if ln.strip()]:
                    up = line.upper()
                    if "DATE AS AT" in up:
                        d = self._parse_date(line)
                        if d:
                            cur_date = d
                    if up.startswith("BDC :") or up.startswith("BDC:"):
                        raw = re.sub(r"^BDC\s*:\s*", "", line, flags=re.IGNORECASE).strip()
                        resolved = self._resolve_bdc_name(raw)
                        new_bdc = resolved if resolved else raw
                        if new_bdc != prev_bdc:
                            if bost_accum and self._owning_bdc_is_bost(prev_bdc):
                                _flush(prev_bdc)
                            prev_bdc = new_bdc
                        cur_bdc = new_bdc
                    if up.startswith("DEPOT :") or up.startswith("DEPOT:"):
                        cur_depot = re.sub(r"^DEPOT\s*:\s*", "", line, flags=re.IGNORECASE).strip()
                    if cur_bdc and cur_depot and cur_date:
                        m = self.product_re.match(line)
                        if m:
                            product = m.group(1).upper()
                            actual = float(m.group(2).replace(",", ""))
                            avail  = float(m.group(3).replace(",", ""))
                            if product not in self.allowed_products or actual <= 0:
                                continue
                            is_bost_dep    = self._is_bost_depot(cur_depot)
                            is_bost_global = self._is_bost_global(cur_depot)
                            if self._owning_bdc_is_bost(cur_bdc):
                                if is_bost_dep:
                                    b = bost_accum.setdefault(product, {"actual": 0.0, "avail": 0.0, "date": cur_date})
                                    b["actual"] += actual; b["avail"] += avail
                                    if cur_date > b["date"]:
                                        b["date"] = cur_date
                                else:
                                    nd = self._ns(cur_depot)
                                    key = (cur_bdc, nd, product, cur_date)
                                    if key not in seen:
                                        seen.add(key)
                                        records.append(self._row(cur_date, cur_bdc, nd, product, actual, avail))
                            else:
                                if is_bost_dep and not is_bost_global:
                                    continue
                                nd = self._ns(cur_depot)
                                key = (cur_bdc, nd, product, cur_date)
                                if key in seen:
                                    continue
                                seen.add(key)
                                records.append(self._row(cur_date, cur_bdc, nd, product, actual, avail))
        except Exception:
            pass
        if bost_accum and self._owning_bdc_is_bost(cur_bdc):
            _flush(cur_bdc)
        return records


# ══════════════════════════════════════════════════════════════
# OMC LOADINGS PARSER  (verbatim logic from new_test.py)
# ══════════════════════════════════════════════════════════════
_PRODUCT_MAP_OMC = {"AGO": "GASOIL", "PMS": "PREMIUM", "LPG": "LPG"}
_ONLY_COLS = ["Date", "OMC", "Truck", "Product", "Quantity", "Price", "Depot", "Order Number", "BDC"]
_HEADER_KW = ["ORDER REPORT", "National Petroleum Authority", "ORDER NUMBER", "ORDER DATE",
              "ORDER STATUS", "BDC:", "Total for :", "Printed By :", "Page ", "BRV NUMBER", "VOLUME"]
_LOADED_KW = {"Released", "Submitted"}


def _detect_product(line):
    raw = "AGO" if "AGO" in line else "LPG" if "LPG" in line else "PMS"
    return _PRODUCT_MAP_OMC.get(raw, raw)


def _resolve_pdf_bdc(raw: str, fallback: str) -> str:
    norm = _normalise_name(raw)
    if norm in _BDC_USER_LOOKUP:
        return _BDC_USER_LOOKUP[norm]
    best_key, best_len = None, 0
    for nk, display in _BDC_USER_LOOKUP.items():
        if nk and (nk in norm or norm in nk) and len(nk) > best_len:
            best_key, best_len = display, len(nk)
    return best_key if best_key else (fallback or raw)


def _parse_loaded_line(line, product, depot, bdc):
    tokens = line.split()
    if len(tokens) < 6:
        return None
    rel_idx = next((i for i, t in enumerate(tokens) if t in _LOADED_KW), None)
    if rel_idx is None or rel_idx < 2:
        return None
    try:
        date_tok, order_num = tokens[0], tokens[1]
        volume = float(tokens[-1].replace(",", ""))
        price  = float(tokens[-2].replace(",", ""))
        brv    = tokens[-3]
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
    rows, cur_depot, cur_bdc, cur_prod = [], "", bdc_name, "PREMIUM"
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
                            cur_bdc = _resolve_pdf_bdc(m.group(1).strip(), bdc_name)
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
    df = df[_ONLY_COLS].drop_duplicates(subset=["Order Number", "Truck", "Date", "Product"])
    try:
        ds = pd.to_datetime(df["Date"], format="%Y/%m/%d", errors="coerce")
        df = df.assign(_ds=ds).sort_values("_ds").drop(columns=["_ds"]).reset_index(drop=True)
    except Exception:
        pass
    return df


# ══════════════════════════════════════════════════════════════
# FETCHERS
# ══════════════════════════════════════════════════════════════
def _fetch_bdc_balance_single_endpoint() -> list:
    params = {
        "lngCompanyId": NPA_CONFIG["COMPANY_ID"], "strITSfromPersol": NPA_CONFIG["ITS_FROM_PERSOL"],
        "strGroupBy": "BDC", "strGroupBy1": "DEPOT",
        "strQuery1": "", "strQuery2": "", "strQuery3": "", "strQuery4": "",
        "strPicHeight": "1", "szPicWeight": "1",
        "lngUserId": NPA_CONFIG["USER_ID"], "intAppId": NPA_CONFIG["APP_ID"],
    }
    pdf = _fetch_pdf(NPA_CONFIG["BDC_BALANCE_URL"], params)
    if not pdf:
        return []
    return StockBalanceScraper().parse_pdf_bytes_global(pdf)


def _fetch_omc_single_endpoint(start_str: str, end_str: str) -> pd.DataFrame:
    params = {
        "lngCompanyId": NPA_CONFIG["COMPANY_ID"], "szITSfromPersol": "persol",
        "strGroupBy": "BDC", "strGroupBy1": "",
        "strQuery1": " and iorderstatus=4",
        "strQuery2": start_str, "strQuery3": end_str, "strQuery4": "",
        "strPicHeight": "", "strPicWeight": "", "intPeriodID": "4",
        "iUserId": NPA_CONFIG["USER_ID"], "iAppId": NPA_CONFIG["APP_ID"],
    }
    pdf = _fetch_pdf(NPA_CONFIG["OMC_LOADINGS_URL"], params)
    if not pdf:
        return pd.DataFrame(columns=_ONLY_COLS)
    return extract_omc_loadings_from_pdf(pdf, bdc_name="")


def _dedup_balance(records: list) -> list:
    if not records:
        return []
    col = "ACTUAL BALANCE (LT\\KG)"
    df = (pd.DataFrame(records)
          .sort_values(col, ascending=False)
          .drop_duplicates(subset=["BDC", "DEPOT", "Product", "Date"], keep="first")
          .reset_index(drop=True))
    return df.to_dict("records")


def fetch_balance_per_bdc(retries: int = 2, sleep: float = 1.0) -> list:
    scraper = StockBalanceScraper()
    out = []
    for bdc_name, user_id in BDC_USER_MAP.items():
        params = {
            "lngCompanyId": NPA_CONFIG["COMPANY_ID"], "strITSfromPersol": NPA_CONFIG["ITS_FROM_PERSOL"],
            "strGroupBy": "BDC", "strGroupBy1": "DEPOT",
            "strQuery1": "", "strQuery2": "", "strQuery3": "", "strQuery4": "",
            "strPicHeight": "1", "szPicWeight": "1",
            "lngUserId": str(user_id), "intAppId": NPA_CONFIG["APP_ID"],
        }
        pdf = None
        for attempt in range(retries + 1):
            pdf = _fetch_pdf(NPA_CONFIG["BDC_BALANCE_URL"], params)
            if pdf:
                break
            time.sleep(sleep * (attempt + 1))
        if pdf:
            out.extend(scraper.parse_pdf_bytes(pdf, owning_bdc_name=bdc_name))
    return _dedup_balance(out)


def fetch_omc_per_bdc(start_str: str, end_str: str, retries: int = 2, sleep: float = 1.0) -> pd.DataFrame:
    frames = []
    for bdc_name, user_id in BDC_USER_MAP.items():
        params = {
            "lngCompanyId": NPA_CONFIG["COMPANY_ID"], "szITSfromPersol": "persol",
            "strGroupBy": "BDC", "strGroupBy1": "",
            "strQuery1": " and iorderstatus=4",
            "strQuery2": start_str, "strQuery3": end_str, "strQuery4": "",
            "strPicHeight": "", "strPicWeight": "", "intPeriodID": "4",
            "iUserId": str(user_id), "iAppId": NPA_CONFIG["APP_ID"],
        }
        pdf = None
        for attempt in range(retries + 1):
            pdf = _fetch_pdf(NPA_CONFIG["OMC_LOADINGS_URL"], params)
            if pdf:
                break
            time.sleep(sleep * (attempt + 1))
        if pdf:
            d = extract_omc_loadings_from_pdf(pdf, bdc_name)
            if not d.empty:
                frames.append(d)
    if not frames:
        return pd.DataFrame(columns=_ONLY_COLS)
    return (pd.concat(frames, ignore_index=True)
            .drop_duplicates(subset=["Order Number", "Truck", "Date", "Product"])
            .reset_index(drop=True))


# ── public wrappers ───────────────────────────────────────────
def fetch_balance_records(mode: str = "single") -> list:
    return fetch_balance_per_bdc() if mode == "per_bdc" else _fetch_bdc_balance_single_endpoint()


def fetch_omc_loadings(start_str: str, end_str: str, mode: str = "single") -> pd.DataFrame:
    return (fetch_omc_per_bdc(start_str, end_str) if mode == "per_bdc"
            else _fetch_omc_single_endpoint(start_str, end_str))
