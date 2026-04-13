"""
NPA Dashboard — ATK & RFO Optional Products Patcher
=====================================================
USAGE
-----
  python3 atk_rfo_patcher.py  npa_dashboard_patched.py

OUTPUT
------
  npa_dashboard_patched_atk_rfo.py  (ready to run with Streamlit)

WHAT THIS DOES  (8 targeted patches)
--------------------------------------
  1  PRODUCT_MAP  — add ATK and RFO entries
  2  _detect_product  — detect AVIATION/ATK/TURBINE and RFO in PDF lines
  3  show_national_stockout  — add ATK & RFO checkboxes (off by default)
  4  Button call-site  — pass extra_products to _run_national_analysis
  5  _run_national_analysis signature  — add extra_products param
  6  DISPLAY dict  — add ATK and RFO display names
  7  Balance scraper  — extend allowed_products dynamically if extras selected
  8  filtered_omc  — extend product filter to include selected extras
  9  Forecast products loop  — iterate over base + extra products
  10 ICONS / COLORS in display  — add ATK and RFO styling
  11 bdc_pivot columns  — make TOTAL dynamic so ATK/RFO are included
"""

import sys
import os

# ─────────────────────────────────────────────────────────────────────────────
#  1  PRODUCT_MAP — add ATK and RFO
# ─────────────────────────────────────────────────────────────────────────────
MAP_OLD = 'PRODUCT_MAP = {"AGO": "GASOIL", "PMS": "PREMIUM", "LPG": "LPG"}'
MAP_NEW = 'PRODUCT_MAP = {"AGO": "GASOIL", "PMS": "PREMIUM", "LPG": "LPG", "ATK": "ATK", "RFO": "RFO", "AVIATION": "ATK"}'

# ─────────────────────────────────────────────────────────────────────────────
#  2  _detect_product — add ATK/RFO detection before the PMS fallback
# ─────────────────────────────────────────────────────────────────────────────
DETECT_OLD = (
    '    else:\n'
    '        raw = "PMS"\n'
    '    return PRODUCT_MAP.get(raw, raw or "")'
)
DETECT_NEW = (
    '    elif "AVIATION" in line or "TURBINE" in line:\n'
    '        raw = "ATK"\n'
    '    elif "ATK" in line:\n'
    '        raw = "ATK"\n'
    '    elif "RFO" in line:\n'
    '        raw = "RFO"\n'
    '    else:\n'
    '        raw = "PMS"\n'
    '    return PRODUCT_MAP.get(raw, raw or "")'
)

# ─────────────────────────────────────────────────────────────────────────────
#  3  show_national_stockout — add ATK & RFO checkboxes after exclude_tor block
# ─────────────────────────────────────────────────────────────────────────────
CHECKBOX_OLD = (
    '        )\n'
    '    )\n'
    '\n'
    '    # ── Vessel pipeline toggle'
)
CHECKBOX_NEW = (
    '        )\n'
    '    )\n'
    '\n'
    '    # ── Optional extra products ───────────────────────────────────────────\n'
    '    st.markdown("#### ➕ Optional Products  *(off by default)*")\n'
    '    _extra_col1, _extra_col2 = st.columns(2)\n'
    '    with _extra_col1:\n'
    '        include_atk = st.checkbox(\n'
    '            "✈️ Aviation Turbine Kerosene (ATK)",\n'
    '            value=False,\n'
    '            key="ns_include_atk",\n'
    '            help="Adds ATK to the balance + loadings fetch. Requires ATK entries in the BDC balance PDF."\n'
    '        )\n'
    '    with _extra_col2:\n'
    '        include_rfo = st.checkbox(\n'
    '            "🔥 Residual Fuel Oil (RFO)",\n'
    '            value=False,\n'
    '            key="ns_include_rfo",\n'
    '            help="Adds RFO to the balance + loadings fetch. Requires RFO entries in the BDC balance PDF."\n'
    '        )\n'
    '    _extra_products = []\n'
    '    if include_atk: _extra_products.append("ATK")\n'
    '    if include_rfo: _extra_products.append("RFO")\n'
    '    # ────────────────────────────────────────────────────────────────────\n'
    '\n'
    '    # ── Vessel pipeline toggle'
)

# ─────────────────────────────────────────────────────────────────────────────
#  4  Button call-site — pass extra_products
# ─────────────────────────────────────────────────────────────────────────────
BUTTON_OLD = (
    '            depletion_mode, exclude_tor_lpg, use_business_days,\n'
    '            include_vessels=include_vessels\n'
    '        )'
)
BUTTON_NEW = (
    '            depletion_mode, exclude_tor_lpg, use_business_days,\n'
    '            include_vessels=include_vessels,\n'
    '            extra_products=_extra_products\n'
    '        )'
)

# ─────────────────────────────────────────────────────────────────────────────
#  5  _run_national_analysis signature — add extra_products
# ─────────────────────────────────────────────────────────────────────────────
SIG_OLD = (
    '    include_vessels: bool = False,\n'
    '):'
)
SIG_NEW = (
    '    include_vessels: bool = False,\n'
    '    extra_products: list = None,\n'
    '):'
)

# ─────────────────────────────────────────────────────────────────────────────
#  6  DISPLAY dict — add ATK and RFO
# ─────────────────────────────────────────────────────────────────────────────
DISPLAY_OLD = "    DISPLAY  = {'PREMIUM': 'PREMIUM (PMS)', 'GASOIL': 'GASOIL (AGO)', 'LPG': 'LPG'}"
DISPLAY_NEW = (
    "    extra_products = extra_products or []\n"
    "    _all_products  = ['PREMIUM', 'GASOIL', 'LPG'] + [p for p in extra_products if p not in ['PREMIUM','GASOIL','LPG']]\n"
    "    DISPLAY  = {'PREMIUM': 'PREMIUM (PMS)', 'GASOIL': 'GASOIL (AGO)', 'LPG': 'LPG',\n"
    "                'ATK': 'ATK (Jet Fuel)', 'RFO': 'RFO (Heavy Fuel)'}"
)

# ─────────────────────────────────────────────────────────────────────────────
#  7  Balance scraper — extend allowed_products if extras selected
#     Injected right after scraper is instantiated (scraper = StockBalanceScraper())
# ─────────────────────────────────────────────────────────────────────────────
SCRAPER_OLD = (
    '        scraper     = StockBalanceScraper()\n'
    '        bal_records = scraper.parse_pdf_file(io.BytesIO(bal_bytes))'
)
SCRAPER_NEW = (
    '        scraper     = StockBalanceScraper()\n'
    '        # Extend allowed products dynamically if ATK/RFO are requested\n'
    '        if extra_products:\n'
    '            scraper.allowed_products = scraper.allowed_products | set(extra_products)\n'
    '            _product_alt = "|".join(sorted(scraper.allowed_products))\n'
    '            scraper.product_line_re = re.compile(\n'
    '                rf"^({_product_alt})\\s+([\\d,]+\\.\\d{{2}})\\s+(-?[\\d,]+\\.\\d{{2}})$",\n'
    '                flags=re.IGNORECASE\n'
    '            )\n'
    '        bal_records = scraper.parse_pdf_file(io.BytesIO(bal_bytes))'
)

# ─────────────────────────────────────────────────────────────────────────────
#  8  filtered_omc — extend product filter to include extra_products
# ─────────────────────────────────────────────────────────────────────────────
FILTER_OLD = "        filtered_omc = omc_df[omc_df['Product'].isin(['PREMIUM', 'GASOIL', 'LPG'])].copy()"
FILTER_NEW = "        filtered_omc = omc_df[omc_df['Product'].isin(['PREMIUM', 'GASOIL', 'LPG'] + extra_products)].copy()"

# ─────────────────────────────────────────────────────────────────────────────
#  9  Forecast products loop — use _all_products instead of hardcoded list
# ─────────────────────────────────────────────────────────────────────────────
LOOP_OLD = (
    "    for prod in ['PREMIUM', 'GASOIL', 'LPG']:\n"
    "        stock     = float(balance_by_product.get(prod, 0))"
)
LOOP_NEW = (
    "    for prod in _all_products:\n"
    "        stock     = float(balance_by_product.get(prod, 0))"
)

# ─────────────────────────────────────────────────────────────────────────────
#  10  ICONS / COLORS in _display_national_results — add ATK and RFO
# ─────────────────────────────────────────────────────────────────────────────
ICONS_OLD = "    ICONS  = {'PREMIUM': '⛽', 'GASOIL': '🚛', 'LPG': '🔵'}"
ICONS_NEW = (
    "    ICONS  = {'PREMIUM': '⛽', 'GASOIL': '🚛', 'LPG': '🔵',\n"
    "              'ATK': '✈️', 'RFO': '🔥'}"
)

# ─────────────────────────────────────────────────────────────────────────────
#  11  bdc_pivot TOTAL — make dynamic so ATK/RFO are included when present
# ─────────────────────────────────────────────────────────────────────────────
PIVOT_OLD = (
    "    for p in ['GASOIL', 'LPG', 'PREMIUM']:\n"
    "        if p not in bdc_pivot.columns:\n"
    "            bdc_pivot[p] = 0\n"
    "    bdc_pivot['TOTAL'] = bdc_pivot[['GASOIL', 'LPG', 'PREMIUM']].sum(axis=1)"
)
PIVOT_NEW = (
    "    _base_prods = ['GASOIL', 'LPG', 'PREMIUM']\n"
    "    for p in _base_prods:\n"
    "        if p not in bdc_pivot.columns:\n"
    "            bdc_pivot[p] = 0\n"
    "    _pivot_prod_cols = [c for c in bdc_pivot.columns if c in\n"
    "                        (_base_prods + res.get('extra_products', []))]\n"
    "    bdc_pivot['TOTAL'] = bdc_pivot[_pivot_prod_cols].sum(axis=1)"
)

# Also persist extra_products in session state so display can use it
PERSIST_EXTRA_OLD = (
    "        'include_vessels':           include_vessels,\n"
    "        'vessel_pipeline_by_product': vessel_pipeline_by_product.to_dict()\n"
    "                                       if not vessel_pipeline_by_product.empty else {},\n"
    "    }"
)
PERSIST_EXTRA_NEW = (
    "        'include_vessels':           include_vessels,\n"
    "        'vessel_pipeline_by_product': vessel_pipeline_by_product.to_dict()\n"
    "                                       if not vessel_pipeline_by_product.empty else {},\n"
    "        'extra_products':            extra_products,\n"
    "    }"
)

# ─────────────────────────────────────────────────────────────────────────────
#  HELPER
# ─────────────────────────────────────────────────────────────────────────────

def apply(src: str, old: str, new: str, label: str) -> str:
    if old in src:
        print(f"  ✅  {label}")
        return src.replace(old, new, 1)
    old_s = "\n".join(line.rstrip() for line in old.splitlines())
    if old_s and old_s in src:
        print(f"  ✅  {label}  (whitespace-normalised)")
        return src.replace(old_s, new, 1)
    print(f"  ❌  MISSED: {label}")
    print(f"      Needle: {repr(old[:80])}")
    return src

# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("Usage:  python3 atk_rfo_patcher.py  npa_dashboard_patched.py")
        sys.exit(1)

    src_path = sys.argv[1]
    with open(src_path, "r", encoding="utf-8") as f:
        src = f.read()

    print(f"\nPatching: {src_path}")
    print(f"  Original: {len(src):,} chars  /  {src.count(chr(10)):,} lines\n")

    src = apply(src, MAP_OLD,           MAP_NEW,           "1  PRODUCT_MAP  — add ATK & RFO")
    src = apply(src, DETECT_OLD,        DETECT_NEW,        "2  _detect_product  — ATK/RFO detection")
    src = apply(src, CHECKBOX_OLD,      CHECKBOX_NEW,      "3  Checkboxes  — ATK & RFO toggles")
    src = apply(src, BUTTON_OLD,        BUTTON_NEW,        "4  Button  — pass extra_products")
    src = apply(src, SIG_OLD,           SIG_NEW,           "5  Signature  — extra_products param")
    src = apply(src, DISPLAY_OLD,       DISPLAY_NEW,       "6  DISPLAY dict  — ATK & RFO names")
    src = apply(src, SCRAPER_OLD,       SCRAPER_NEW,       "7  Scraper  — extend allowed_products")
    src = apply(src, FILTER_OLD,        FILTER_NEW,        "8  filtered_omc  — include extras")
    src = apply(src, LOOP_OLD,          LOOP_NEW,          "9  Forecast loop  — _all_products")
    src = apply(src, ICONS_OLD,         ICONS_NEW,         "10 ICONS/COLORS  — ATK & RFO styling")
    src = apply(src, PIVOT_OLD,         PIVOT_NEW,         "11 bdc_pivot  — dynamic TOTAL column")
    src = apply(src, PERSIST_EXTRA_OLD, PERSIST_EXTRA_NEW, "12 Persist  — extra_products in state")

    # Build output filename
    base, ext = os.path.splitext(src_path)
    out_path = base + "_atk_rfo" + ext

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(src)

    print(f"\n{'─'*60}")
    print(f"✅  Output: {out_path}")
    print(f"   Size:   {len(src):,} chars  /  {src.count(chr(10)):,} lines")
    print(f"\nLaunch:  streamlit run {out_path}")


if __name__ == "__main__":
    main()