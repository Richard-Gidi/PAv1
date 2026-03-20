"""
NPA Dashboard Vessel Patcher  (v2 — clean approach)
=====================================================
USAGE
-----
  1. Put this file AND npa_vessel_additions.py in the same folder as npa_dashboard.py
  2. Run:   python3 patcher.py npa_dashboard.py
  3. A file called npa_dashboard_patched.py is created — run it with Streamlit.

WHAT THIS DOES
--------------
  A  Injects vessel constants  (VESSEL_SHEET_URL, conversion factors, etc.)
  B  Injects vessel helper functions + show_vessel_supply()
  C  Adds  🚢 VESSEL SUPPLY  to the sidebar radio list
  D  Adds routing for the new page
  E  Adds the "include pending vessels" toggle to National Stockout
  F  Passes include_vessels flag to _run_national_analysis
  G  Extends _run_national_analysis signature + adds vessel pipeline logic
  H  Persists vessel info in session-state results dict
  I  Displays vessel contribution note in _display_national_results
"""

import sys
import os

# ─────────────────────────────────────────────────────────────────────────────
#  PATCH FRAGMENTS
# ─────────────────────────────────────────────────────────────────────────────

VESSEL_CONSTANTS = """
# ==================== VESSEL SUPPLY — CONSTANTS ====================
VESSEL_SHEET_URL = "https://docs.google.com/spreadsheets/d/1z-L79N22rU3p6wLw1CEVWDIw6QSwA5CH/edit?rtpof=true"

VESSEL_CONVERSION_FACTORS = {
    'PREMIUM': 1324.50,
    'GASOIL':  1183.00,
    'LPG':     1000.00,
    'NAPHTHA':  800.00,
}

VESSEL_PRODUCT_MAPPING = {
    'PMS':      'PREMIUM',
    'GASOLINE': 'PREMIUM',
    'AGO':      'GASOIL',
    'GASOIL':   'GASOIL',
    'LPG':      'LPG',
    'BUTANE':   'LPG',
    'NAPHTHA':  'NAPHTHA',
}

VESSEL_MONTH_MAPPING = {
    'Jan':'JAN','Feb':'FEB','Mar':'MAR','Apr':'APR',
    'May':'MAY','Jun':'JUN','Jul':'JUL','Aug':'AUG',
    'Sep':'SEP','Oct':'OCT','Nov':'NOV','Dec':'DEC',
}
"""

# C
SIDEBAR_OLD = '"🌍 WORLD RISK MONITOR"\n        ], index=0)'
SIDEBAR_NEW = (
    '"🌍 WORLD RISK MONITOR",\n'
    '            "─────── SUPPLY ────────",\n'
    '            "🚢 VESSEL SUPPLY"\n'
    '        ], index=0)'
)

# D  (original has 3 trailing spaces after the colon)
ROUTING_OLD = (
    '    elif choice == "🌍 WORLD RISK MONITOR":   \n'
    '        show_world_monitor()\n'
    '    else:\n'
    '        st.info("Select a page from the sidebar.")'
)
ROUTING_NEW = (
    '    elif choice == "🌍 WORLD RISK MONITOR":\n'
    '        show_world_monitor()\n'
    '    elif choice == "🚢 VESSEL SUPPLY":\n'
    '        show_vessel_supply()\n'
    '    else:\n'
    '        st.info("Select a page from the sidebar.")'
)

# E — vessel toggle injected just before the st.info("⚡ **Just 2 API calls…") block
TOGGLE_OLD = '    st.info(\n        "⚡ **Just 2 API calls.** "'

TOGGLE_NEW = (
    "    # ── Vessel pipeline toggle ─────────────────────────────────────────\n"
    "    _vessel_loaded = (\n"
    "        st.session_state.get('vessel_data') is not None\n"
    "        and not st.session_state.vessel_data.empty\n"
    "    )\n"
    "    _vessel_pending_count = 0\n"
    "    if _vessel_loaded:\n"
    "        _vessel_pending_count = int(\n"
    "            (st.session_state.vessel_data['Status'] == 'PENDING').sum()\n"
    "        )\n"
    "\n"
    "    include_vessels = st.checkbox(\n"
    "        \"🚢 Include pending vessels in national stock  (BDC balance + pipeline cargo)\",\n"
    "        value=False,\n"
    "        key='ns_include_vessels',\n"
    "        help=(\n"
    "            'Adds the litres of every vessel marked PENDING in the Vessel Supply tracker '\n"
    "            'to the corresponding product balance before computing days of supply. '\n"
    "            \"This gives a 'total available supply' view rather than 'in-depot stock only'.\"\n"
    "        )\n"
    "    )\n"
    "\n"
    "    if include_vessels and not _vessel_loaded:\n"
    "        st.warning(\n"
    "            '⚠️ No vessel data loaded. Go to **🚢 VESSEL SUPPLY** and fetch data first, '\n"
    "            'then come back and enable this toggle.'\n"
    "        )\n"
    "        include_vessels = False\n"
    "    elif include_vessels and _vessel_pending_count == 0:\n"
    "        st.info('ℹ️ Vessel data is loaded but there are **no pending vessels** — '\n"
    "                'the toggle has no effect on totals.')\n"
    "    elif include_vessels:\n"
    "        _pend_df = st.session_state.vessel_data[\n"
    "            st.session_state.vessel_data['Status'] == 'PENDING'\n"
    "        ]\n"
    "        _pend_summary = _pend_df.groupby('Product')['Quantity_Litres'].sum()\n"
    "        _parts = [f\"{p}: **{v:,.0f} LT**\" for p, v in _pend_summary.items()]\n"
    "        st.success(\n"
    "            f\"🚢 **{_vessel_pending_count} pending vessels** will be added to BDC stock — \"\n"
    "            + ' | '.join(_parts)\n"
    "        )\n"
    "    # ────────────────────────────────────────────────────────────────────\n"
    "\n"
    '    st.info(\n'
    '        "⚡ **Just 2 API calls.** "'
)

# F
BUTTON_OLD = (
    "    if st.button(\"⚡ FETCH & ANALYSE NATIONAL FUEL SUPPLY\", width='stretch'):\n"
    "        _run_national_analysis(\n"
    "            start_str, end_str, period_days,\n"
    "            depletion_mode, exclude_tor_lpg, use_business_days\n"
    "        )"
)
BUTTON_NEW = (
    "    if st.button(\"⚡ FETCH & ANALYSE NATIONAL FUEL SUPPLY\", width='stretch'):\n"
    "        _run_national_analysis(\n"
    "            start_str, end_str, period_days,\n"
    "            depletion_mode, exclude_tor_lpg, use_business_days,\n"
    "            include_vessels=include_vessels\n"
    "        )"
)

# G1
SIG_OLD = (
    "def _run_national_analysis(\n"
    "    start_str: str,\n"
    "    end_str: str,\n"
    "    period_days: int,\n"
    "    depletion_mode: str,\n"
    "    exclude_tor_lpg: bool,\n"
    "    use_business_days: bool = False,          # \u2190 NEW param\n"
    "):"
)
SIG_NEW = (
    "def _run_national_analysis(\n"
    "    start_str: str,\n"
    "    end_str: str,\n"
    "    period_days: int,\n"
    "    depletion_mode: str,\n"
    "    exclude_tor_lpg: bool,\n"
    "    use_business_days: bool = False,\n"
    "    include_vessels: bool = False,\n"
    "):"
)

# G2
BAL_OLD = (
    "        balance_by_product = bal_df.groupby('Product')[col_bal].sum()\n"
    "        n_bdcs = bal_df['BDC'].nunique()"
)
BAL_NEW = (
    "        balance_by_product = bal_df.groupby('Product')[col_bal].sum()\n"
    "\n"
    "        # ── Add pending vessel volumes if requested ─────────────────────\n"
    "        vessel_pipeline_by_product = pd.Series(dtype=float)\n"
    "        if include_vessels:\n"
    "            _vdf = st.session_state.get('vessel_data')\n"
    "            if _vdf is not None and not _vdf.empty:\n"
    "                _pending = _vdf[_vdf['Status'] == 'PENDING']\n"
    "                if not _pending.empty:\n"
    "                    vessel_pipeline_by_product = (\n"
    "                        _pending.groupby('Product')['Quantity_Litres'].sum()\n"
    "                    )\n"
    "                    for _prod, _vol in vessel_pipeline_by_product.items():\n"
    "                        if _prod in balance_by_product.index:\n"
    "                            balance_by_product[_prod] += _vol\n"
    "                        else:\n"
    "                            balance_by_product[_prod] = _vol\n"
    "                    _n_vp = len(_pending)\n"
    "                    st.write(\n"
    "                        f'\U0001f6a2 **Vessel pipeline added** ({_n_vp} pending vessels): '\n"
    "                        + ' | '.join([f'{p}: **{v:,.0f} LT**'\n"
    "                                      for p, v in vessel_pipeline_by_product.items()])\n"
    "                    )\n"
    "        # ───────────────────────────────────────────────────────────────\n"
    "\n"
    "        n_bdcs = bal_df['BDC'].nunique()"
)

# H
PERSIST_OLD = (
    "    st.session_state.ns_results = {\n"
    "        'forecast_df':      forecast_df,\n"
    "        'bal_df':           bal_df,\n"
    "        'omc_df':           omc_df,\n"
    "        'bdc_pivot':        bdc_pivot,\n"
    "        'period_days':      period_days,\n"
    "        'effective_days':   effective_days,          # \u2190 NEW\n"
    "        'use_business_days': use_business_days,      # \u2190 NEW\n"
    "        'day_type_label':   day_type_label,          # \u2190 NEW\n"
    "        'start_str':        start_str,\n"
    "        'end_str':          end_str,\n"
    "        'n_bdcs_balance':   n_bdcs,\n"
    "        'n_omc_rows':       len(omc_df),\n"
    "        'depletion_mode':   depletion_mode,\n"
    "        'depletion_label':  depletion_label,\n"
    "        'exclude_tor_lpg':  exclude_tor_lpg,\n"
    "    }"
)
PERSIST_NEW = (
    "    st.session_state.ns_results = {\n"
    "        'forecast_df':               forecast_df,\n"
    "        'bal_df':                    bal_df,\n"
    "        'omc_df':                    omc_df,\n"
    "        'bdc_pivot':                 bdc_pivot,\n"
    "        'period_days':               period_days,\n"
    "        'effective_days':            effective_days,\n"
    "        'use_business_days':         use_business_days,\n"
    "        'day_type_label':            day_type_label,\n"
    "        'start_str':                 start_str,\n"
    "        'end_str':                   end_str,\n"
    "        'n_bdcs_balance':            n_bdcs,\n"
    "        'n_omc_rows':                len(omc_df),\n"
    "        'depletion_mode':            depletion_mode,\n"
    "        'depletion_label':           depletion_label,\n"
    "        'exclude_tor_lpg':           exclude_tor_lpg,\n"
    "        'include_vessels':           include_vessels,\n"
    "        'vessel_pipeline_by_product': vessel_pipeline_by_product.to_dict()\n"
    "                                       if not vessel_pipeline_by_product.empty else {},\n"
    "    }"
)

# I
DISP_OLD = (
    "    st.caption(\n"
    "        f\"Balance: **{res['n_bdcs_balance']} BDCs** | \"\n"
    "        f\"OMC Loadings: **{res['n_omc_rows']:,} records** | \"\n"
    "        f\"Depletion source: {depletion_label}{tor_note} | \"\n"
    "        f\"Day type: **{day_badge}** ({effective_days} days used as denominator) | \"\n"
    '        f"CTO excluded \u2014 internal BDC transfers"\n'
    "    )"
)
DISP_NEW = (
    "    include_vessels_res = res.get('include_vessels', False)\n"
    "    vessel_pipeline     = res.get('vessel_pipeline_by_product', {})\n"
    "    if include_vessels_res and vessel_pipeline:\n"
    "        vessel_note = '  |  \U0001f6a2 Vessels: ' + '  '.join(\n"
    "            [f\"{p}+{v/1e6:.2f}M LT\" for p, v in vessel_pipeline.items() if v > 0]\n"
    "        )\n"
    "    else:\n"
    "        vessel_note = ''\n"
    "\n"
    "    # ── Cache status + clear button ────────────────────────────────────\n"
    "    _cache_key = st.session_state.get('_ns_omc_cache_key', '')\n"
    "    if _cache_key:\n"
    "        _ck_parts = _cache_key.split('|')\n"
    "        _cache_label = f\"OMC data cached for {_ck_parts[0]} → {_ck_parts[1]}\" if len(_ck_parts) == 2 else 'OMC data cached'\n"
    "        _col_a, _col_b = st.columns([4, 1])\n"
    "        with _col_a:\n"
    "            st.caption(f'📋 {_cache_label}. Re-clicking Fetch & Analyse reuses this data for stability.')\n"
    "        with _col_b:\n"
    "            if st.button('🗑️ Clear Cache', key='ns_clear_omc_cache', help='Force a fresh API fetch on next run'):\n"
    "                st.session_state.pop('_ns_omc_cache', None)\n"
    "                st.session_state.pop('_ns_omc_cache_key', None)\n"
    "                st.success('Cache cleared — next fetch will pull fresh data.')\n"
    "                st.rerun()\n"
    "    # ───────────────────────────────────────────────────────────────────\n"
    "\n"
    "    st.caption(\n"
    "        f\"Balance: **{res['n_bdcs_balance']} BDCs** | \"\n"
    "        f\"OMC Loadings: **{res['n_omc_rows']:,} records** | \"\n"
    "        f\"Depletion source: {depletion_label}{tor_note} | \"\n"
    "        f\"Day type: **{day_badge}** ({effective_days} days used as denominator) | \"\n"
    '        f"CTO excluded \u2014 internal BDC transfers"\n'
    "        f\"{vessel_note}\"\n"
    "    )\n"
    "\n"
    "    if include_vessels_res and vessel_pipeline:\n"
    "        vessel_parts = [f\"**{p}: {v:,.0f} LT**\" for p, v in vessel_pipeline.items() if v > 0]\n"
    "        st.info(\n"
    "            '\U0001f6a2 **Vessel pipeline included** \u2014 pending cargo added to BDC stock:  '\n"
    "            + ' | '.join(vessel_parts)\n"
    "        )"
)

# J — deterministic deduplication in _fetch_national_omc_loadings
# The old bare drop_duplicates() is non-deterministic across parallel runs.
# Deduplicate on meaningful columns, then sort so every run produces
# the same ordered DataFrame for the same date range.
DEDUP_OLD = (
    "    return pd.concat(all_frames, ignore_index=True).drop_duplicates()"
)
DEDUP_NEW = (
    "    combined = pd.concat(all_frames, ignore_index=True)\n"
    "    # Deduplicate on business-key columns (bare drop_duplicates() is non-deterministic\n"
    "    # across parallel chunk runs — float noise and ordering cause false 'unique' rows)\n"
    "    dedup_cols = [c for c in ['Date', 'Order Number', 'Truck', 'Product', 'Depot', 'BDC']\n"
    "                  if c in combined.columns]\n"
    "    if dedup_cols:\n"
    "        combined = combined.drop_duplicates(subset=dedup_cols)\n"
    "    else:\n"
    "        combined = combined.drop_duplicates()\n"
    "    # Sort deterministically so median/max/avg are stable on identical re-runs\n"
    "    sort_cols = [c for c in ['Date', 'BDC', 'Order Number'] if c in combined.columns]\n"
    "    if sort_cols:\n"
    "        combined = combined.sort_values(sort_cols).reset_index(drop=True)\n"
    "    return combined"
)

# K — cache OMC loadings inside _run_national_analysis so re-clicking
#     with the same dates returns the exact same dataset every time.
# We wrap the omc_df fetch call with a cache-hit check.
CACHE_OLD = (
    "        omc_df = _fetch_national_omc_loadings(start_str, end_str, progress_cb=_on_progress)\n"
    "        prog_bar.progress(1.0, text=\"✅ All chunks fetched\")"
)
CACHE_NEW = (
    "        _omc_cache_key = f\"{start_str}|{end_str}\"\n"
    "        _cached_omc    = st.session_state.get('_ns_omc_cache')\n"
    "        _cached_key    = st.session_state.get('_ns_omc_cache_key', '')\n"
    "\n"
    "        if _cached_omc is not None and _cached_key == _omc_cache_key:\n"
    "            omc_df = _cached_omc\n"
    "            prog_bar.progress(1.0, text=\"✅ Using cached data (same date range)\")\n"
    "            st.info(\n"
    "                f'📋 **Cached OMC data reused** — same date range as last fetch '\n"
    "                f'({len(omc_df):,} records). Results are deterministic. '\n"
    "                'Change the date range or clear cache to re-fetch.'\n"
    "            )\n"
    "        else:\n"
    "            omc_df = _fetch_national_omc_loadings(start_str, end_str, progress_cb=_on_progress)\n"
    "            st.session_state['_ns_omc_cache']     = omc_df\n"
    "            st.session_state['_ns_omc_cache_key'] = _omc_cache_key\n"
    "            prog_bar.progress(1.0, text=\"✅ All chunks fetched\")"
)

# ─────────────────────────────────────────────────────────────────────────────
#  HELPER
# ─────────────────────────────────────────────────────────────────────────────

def apply(src: str, old: str, new: str, label: str) -> str:
    if old in src:
        print(f"  ✅  {label}")
        return src.replace(old, new, 1)
    # Fallback: try with trailing whitespace stripped from each line
    def strip_trailing(s):
        return "\n".join(line.rstrip() for line in s.splitlines())
    old_s = strip_trailing(old)
    if old_s and old_s in src:
        print(f"  ✅  {label}  (whitespace-normalised)")
        return src.replace(old_s, new, 1)
    print(f"  ❌  MISSED: {label}")
    print(f"      Needle preview: {repr(old[:100])}")
    return src


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("Usage:  python3 patcher.py  npa_dashboard.py")
        sys.exit(1)

    src_path = sys.argv[1]
    script_dir = os.path.dirname(os.path.abspath(__file__))
    additions_path = os.path.join(script_dir, "npa_vessel_additions.py")

    if not os.path.exists(additions_path):
        print(f"❌  npa_vessel_additions.py not found at:\n   {additions_path}")
        print("    Both files must be in the same folder.")
        sys.exit(1)

    with open(src_path, "r", encoding="utf-8") as f:
        src = f.read()
    with open(additions_path, "r", encoding="utf-8") as f:
        vessel_code = f.read()

    print(f"\nPatching: {src_path}")
    print(f"  Original: {len(src):,} chars  /  {src.count(chr(10)):,} lines\n")

    # A — vessel constants after WORLD_MONITOR_URL
    wm_marker = "WORLD_MONITOR_URL = os.getenv('WORLD_MONITOR_URL'"
    pos = src.find(wm_marker)
    if pos != -1:
        eol = src.find('\n', pos) + 1
        src = src[:eol] + VESSEL_CONSTANTS + src[eol:]
        print("  ✅  A  Vessel constants")
    else:
        print("  ❌  A  MISSED — WORLD_MONITOR_URL line not found")

    # B — vessel functions before last def main():
    main_marker = "\ndef main():\n"
    pos = src.rfind(main_marker)
    if pos != -1:
        block = (
            "\n\n"
            "# ==================== VESSEL SUPPLY — FUNCTIONS ====================\n"
            + vessel_code
            + "\n"
        )
        src = src[:pos] + block + src[pos:]
        print("  ✅  B  Vessel functions + show_vessel_supply()")
    else:
        print("  ❌  B  MISSED — def main() not found")

    print()
    src = apply(src, SIDEBAR_OLD,  SIDEBAR_NEW,  "C  Sidebar — 🚢 VESSEL SUPPLY option")
    src = apply(src, ROUTING_OLD,  ROUTING_NEW,  "D  Routing — vessel page handler")
    src = apply(src, TOGGLE_OLD,   TOGGLE_NEW,   "E  National Stockout — vessel toggle")
    src = apply(src, BUTTON_OLD,   BUTTON_NEW,   "F  Button — pass include_vessels")
    src = apply(src, SIG_OLD,      SIG_NEW,      "G1 _run_national_analysis — signature")
    src = apply(src, BAL_OLD,      BAL_NEW,      "G2 _run_national_analysis — pipeline logic")
    src = apply(src, PERSIST_OLD,  PERSIST_NEW,  "H  Session state — persist vessel info")
    src = apply(src, DISP_OLD,     DISP_NEW,     "I  _display_national_results — vessel note")
    src = apply(src, DEDUP_OLD,    DEDUP_NEW,    "J  _fetch_national_omc_loadings — deterministic dedup+sort")
    src = apply(src, CACHE_OLD,    CACHE_NEW,    "K  _run_national_analysis — OMC cache by date range")

    out_path = src_path.replace(".py", "_patched.py")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(src)

    print(f"\n{'─'*60}")
    print(f"✅  Output: {out_path}")
    print(f"   Size:   {len(src):,} chars  /  {src.count(chr(10)):,} lines")
    print(f"\nLaunch:  streamlit run {out_path}")


if __name__ == "__main__":
    main()