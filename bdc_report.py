"""
BDC BALANCE REPORT GENERATOR
============================
Drop-in module for the NPA Energy Analytics dashboard.

Turns the list in `st.session_state.bdc_records` into a multi-page PDF that
mirrors the uploaded "BDC BALANCE" report:

    * One page per product  (GASOIL(AGO) -> GASOLINE(PMS) -> LPG)
    * Each page has two panels:
        - "<PRODUCT> BALANCE BY BDCs"   : total balance, BDC count, top-20 bar chart
        - "<PRODUCT> BALANCE BY DEPOT"  : total balance, depot count, depot bar chart
    * Per-product colour scheme (teal / red / indigo) and correct unit (LTRS / KG)

Only dependency added: matplotlib.
    pip install matplotlib
"""

import io
import re
import unicodedata
from datetime import datetime

import pandas as pd
import matplotlib
matplotlib.use("Agg")                       # headless / server-safe
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import FancyBboxPatch


# ──────────────────────────────────────────────────────────────
# Per-product presentation config (matches the uploaded report)
# ──────────────────────────────────────────────────────────────
_COL_BAL = "ACTUAL BALANCE (LT\\KG)"

# Render order = page order in the uploaded PDF
_REPORT_PRODUCTS = ["GASOIL", "PREMIUM", "LPG"]

_REPORT_CFG = {
    "GASOIL": {
        "display":     "GASOIL(AGO)",
        "unit":        "LTRS",
        "bdc_label":   "DIESEL BALANCE BY BDCs",
        "depot_label": "DIESEL BALANCE BY DEPOT",
        "color":       "#13A89E",   # teal
        "color_dark":  "#0E8B82",
        "panel_tint":  "#EAF8F6",
    },
    "PREMIUM": {
        "display":     "GASOLINE(PMS)",
        "unit":        "LTRS",
        "bdc_label":   "PMS BALANCE BY BDCs",
        "depot_label": "PMS BALANCE BY DEPOT",
        "color":       "#E8112D",   # red
        "color_dark":  "#C20E26",
        "panel_tint":  "#FDECEE",
    },
    "LPG": {
        "display":     "LPG",
        "unit":        "KG",
        "bdc_label":   "LPG BALANCE BY BDCs",
        "depot_label": "LPG BALANCE BY DEPOT",
        "color":       "#4A4FC0",   # indigo
        "color_dark":  "#3A3F9E",
        "panel_tint":  "#ECEDFA",
    },
}

_TOP_N_BDC   = 20
_TOP_N_DEPOT = 20


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def _ordinal(n: int) -> str:
    if 11 <= (n % 100) <= 13:
        suf = "TH"
    else:
        suf = {1: "ST", 2: "ND", 3: "RD"}.get(n % 10, "TH")
    return f"{n}{suf}"


def _fmt_date(d) -> str:
    """e.g. 18 -> '18TH JUNE 2026'."""
    if d is None:
        d = datetime.now().date()
    if isinstance(d, str):
        # try common formats coming out of the parser
        for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                d = datetime.strptime(d, fmt).date()
                break
            except ValueError:
                continue
        else:
            d = datetime.now().date()
    return f"{_ordinal(d.day)} {d.strftime('%B').upper()} {d.year}"


def _fmt_total(value: float) -> str:
    return f"{value:,.2f}"


def _fmt_m(value: float) -> str:
    """Bar label in millions, 2 dp, matching '117.05M'."""
    return f"{value / 1e6:.2f}M"


def _wrap_label(name: str, width: int = 11, max_lines: int = 4) -> str:
    """Break a long BDC/depot name into stacked short lines for the x-axis."""
    words = str(name).upper().split()
    lines, cur = [], ""
    for w in words:
        if len(cur) + len(w) + (1 if cur else 0) <= width:
            cur = f"{cur} {w}".strip()
        else:
            if cur:
                lines.append(cur)
            cur = w
        if len(lines) == max_lines - 1:
            break
    if cur:
        lines.append(cur)
    if len(lines) >= max_lines and len(words) > sum(len(l.split()) for l in lines[:max_lines]):
        lines = lines[:max_lines]
        lines[-1] = lines[-1] + "…"
    return "\n".join(lines[:max_lines])


def _card(fig, x, y, w, h, *, facecolor, edgecolor, lines, text_color):
    """Draw a rounded info-card with stacked centred text lines."""
    ax = fig.add_axes([x, y, w, h])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    ax.add_patch(FancyBboxPatch(
        (0.015, 0.06), 0.97, 0.88,
        boxstyle="round,pad=0,rounding_size=0.12",
        linewidth=1.1, edgecolor=edgecolor, facecolor=facecolor,
        clip_on=False,
    ))
    n = len(lines)
    if n == 1:
        ys = [0.5]
    else:
        ys = [0.66, 0.34] if n == 2 else [0.74, 0.5, 0.26]
    for (txt, size, weight), yy in zip(lines, ys):
        ax.text(0.5, yy, txt, ha="center", va="center",
                fontsize=size, fontweight=weight, color=text_color)


def _bar_panel(fig, rect, names, values, color, color_dark,
               label_fmt=_fmt_m, label_size=5.4, tick_size=4.6, wrap_width=11):
    """Draw a single bar chart panel with value labels above each bar."""
    ax = fig.add_axes(rect)
    x = range(len(values))
    bars = ax.bar(x, values, color=color, width=0.72,
                  edgecolor=color_dark, linewidth=0.4)

    top = max(values) if len(values) else 1
    ax.set_ylim(0, top * 1.18)
    for xi, v in zip(x, values):
        ax.text(xi, v + top * 0.015, label_fmt(v),
                ha="center", va="bottom", fontsize=label_size,
                fontweight="bold", color="#222222")

    ax.set_xticks(list(x))
    ax.set_xticklabels([_wrap_label(n, wrap_width) for n in names],
                       fontsize=tick_size, color="#333333", linespacing=0.9)
    ax.set_yticks([])
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color("#cccccc")
    ax.tick_params(axis="x", length=0, pad=2)
    ax.margins(x=0.01)


# ──────────────────────────────────────────────────────────────
# One page per product
# ──────────────────────────────────────────────────────────────
def _render_product_page(pdf, df_prod, cfg, date_str):
    fig = plt.figure(figsize=(8.27, 11.69))   # A4 portrait
    fig.patch.set_facecolor("white")

    color, color_dark = cfg["color"], cfg["color_dark"]

    # ── Title block ───────────────────────────────────────────
    fig.text(0.5, 0.965, "BDC BALANCE", ha="center", va="center",
             fontsize=30, fontweight="bold", color="#111111")
    fig.text(0.5, 0.937, date_str, ha="center", va="center",
             fontsize=12, fontweight="bold", color="#111111")
    fig.text(0.5, 0.916, cfg["display"], ha="center", va="center",
             fontsize=15, fontweight="bold", color="#111111")

    # ── BY BDC aggregation ────────────────────────────────────
    by_bdc = (df_prod.groupby("BDC")[_COL_BAL].sum()
              .sort_values(ascending=False))
    bdc_total = float(by_bdc.sum())
    n_bdc     = int(by_bdc.shape[0])
    bdc_top   = by_bdc.head(_TOP_N_BDC)

    # ── BY DEPOT aggregation ──────────────────────────────────
    by_depot = (df_prod.groupby("DEPOT")[_COL_BAL].sum()
                .sort_values(ascending=False))
    depot_total = float(by_depot.sum())
    n_depot     = int(by_depot.shape[0])
    depot_top   = by_depot.head(_TOP_N_DEPOT)

    unit = cfg["unit"]

    # ── BDC header cards ──────────────────────────────────────
    _card(fig, 0.045, 0.862, 0.345, 0.046,
          facecolor=color, edgecolor=color,
          lines=[(cfg["bdc_label"], 10, "bold")], text_color="white")
    _card(fig, 0.365, 0.862, 0.32, 0.046,
          facecolor="white", edgecolor="#dddddd",
          lines=[(f"TOTAL BALANCE ({unit})", 7.5, "bold"),
                 (_fmt_total(bdc_total), 15, "bold")],
          text_color="#222222")
    _card(fig, 0.705, 0.862, 0.25, 0.046,
          facecolor="white", edgecolor="#dddddd",
          lines=[("TOTAL BDCs", 7.5, "bold"),
                 (str(n_bdc), 15, "bold")],
          text_color="#222222")

    fig.text(0.5, 0.852, f"TOTAL BALANCE (LT\\KG) BY TOP {len(bdc_top)} BDC",
             ha="center", va="center", fontsize=7, fontweight="bold",
             color="#555555")

    _bar_panel(fig, [0.05, 0.545, 0.92, 0.295],
               bdc_top.index.tolist(), bdc_top.values.tolist(),
               color, color_dark)

    # ── Depot header cards ────────────────────────────────────
    _card(fig, 0.045, 0.452, 0.345, 0.046,
          facecolor=color, edgecolor=color,
          lines=[(cfg["depot_label"], 10, "bold")], text_color="white")
    _card(fig, 0.365, 0.452, 0.32, 0.046,
          facecolor="white", edgecolor="#dddddd",
          lines=[(f"TOTAL BALANCE ({unit})", 7.5, "bold"),
                 (_fmt_total(depot_total), 15, "bold")],
          text_color="#222222")
    _card(fig, 0.705, 0.452, 0.25, 0.046,
          facecolor="white", edgecolor="#dddddd",
          lines=[("TOTAL DEPOTS", 7.5, "bold"),
                 (str(n_depot), 15, "bold")],
          text_color="#222222")

    fig.text(0.5, 0.442, "TOTAL BALANCE (LT\\KG) BY DEPOT",
             ha="center", va="center", fontsize=7, fontweight="bold",
             color="#555555")

    _bar_panel(fig, [0.05, 0.10, 0.92, 0.315],
               depot_top.index.tolist(), depot_top.values.tolist(),
               color, color_dark)

    pdf.savefig(fig, facecolor="white")
    plt.close(fig)


# ──────────────────────────────────────────────────────────────
# Public entry point
# ──────────────────────────────────────────────────────────────
def generate_bdc_balance_report_pdf(records, report_date=None,
                                     products=None) -> bytes:
    """Build the styled multi-page BDC Balance PDF and return raw bytes.

    Parameters
    ----------
    records : list[dict] | pd.DataFrame
        The `bdc_records` collected by the BDC Balance page.
    report_date : date | str | None
        Date printed under the title. Defaults to the latest date in the data,
        else today.
    products : list[str] | None
        Subset / order of products to render. Defaults to GASOIL, PREMIUM, LPG.
    """
    df = pd.DataFrame(records) if not isinstance(records, pd.DataFrame) else records.copy()
    if df.empty or _COL_BAL not in df.columns:
        raise ValueError("No balance records to report on.")

    if report_date is None:
        report_date = df["Date"].max() if "Date" in df.columns else None
    date_str = _fmt_date(report_date)

    products = products or _REPORT_PRODUCTS

    buf = io.BytesIO()
    with PdfPages(buf) as pdf:
        for prod in products:
            cfg = _REPORT_CFG.get(prod)
            if cfg is None:
                continue
            df_prod = df[df["Product"] == prod]
            if df_prod.empty:
                continue
            _render_product_page(pdf, df_prod, cfg, date_str)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════
# STREAMLIT PAGE  (drop-in for the NPA dashboard)
# ══════════════════════════════════════════════════════════════
# Streamlit is only needed for the in-app pages; the PDF generators work without it.
try:
    import streamlit as st
except ImportError:                      # pragma: no cover
    st = None


def show_report_generator():
    st.markdown("<h2>📄 BDC BALANCE REPORT (PDF)</h2>", unsafe_allow_html=True)

    st.markdown("""
    <div style='background:rgba(0,255,255,0.05);border:1px solid #00ffff33;
                border-radius:10px;padding:14px;margin-bottom:16px;'>
    <b style='color:#00ffff;'>What this page does</b><br>
    Builds the styled <b>BDC BALANCE</b> PDF — one page per product
    (GASOIL(AGO) · GASOLINE(PMS) · LPG), each with a
    <b>BALANCE BY BDCs</b> and <b>BALANCE BY DEPOT</b> panel
    (total balance, count, and a ranked bar chart) — straight from the data
    fetched on the <b>🏦 BDC BALANCE</b> page.
    </div>
    """, unsafe_allow_html=True)

    records = st.session_state.get("bdc_records", [])
    if not records:
        st.warning("⚠️ No balance data in session. Open **🏦 BDC BALANCE** and "
                   "click **FETCH BDC BALANCE DATA** first, then come back here.")
        return

    df = pd.DataFrame(records)

    # Default report date = latest date present in the data
    default_date = datetime.now().date()
    if "Date" in df.columns and not df.empty:
        try:
            default_date = pd.to_datetime(df["Date"], errors="coerce").max().date()
        except Exception:
            pass

    c1, c2 = st.columns([1, 2])
    with c1:
        report_date = st.date_input("Report date (printed under the title)",
                                    value=default_date, key="rpt_date")
    with c2:
        avail = [p for p in _REPORT_PRODUCTS if p in set(df.get("Product", []))]
        chosen = st.multiselect(
            "Products to include (page order is fixed)",
            _REPORT_PRODUCTS,
            default=avail or _REPORT_PRODUCTS,
            key="rpt_products",
            format_func=lambda p: _REPORT_CFG[p]["display"],
        )

    # Live preview of what each page will report
    st.markdown("#### 📊 What the report will contain")
    prev_rows = []
    for prod in (chosen or _REPORT_PRODUCTS):
        sub = df[df["Product"] == prod]
        if sub.empty:
            continue
        prev_rows.append({
            "Page":          _REPORT_CFG[prod]["display"],
            "Unit":          _REPORT_CFG[prod]["unit"],
            "Total Balance": f"{sub[_COL_BAL].sum():,.2f}",
            "BDCs":          sub['BDC'].nunique(),
            "Depots":        sub['DEPOT'].nunique(),
        })
    if prev_rows:
        st.dataframe(pd.DataFrame(prev_rows), use_container_width=True, hide_index=True)

    if st.button("📄 GENERATE PDF REPORT", key="rpt_generate"):
        if not chosen:
            st.error("Select at least one product.")
            return
        with st.spinner("Rendering report…"):
            try:
                pdf_bytes = generate_bdc_balance_report_pdf(
                    df, report_date=report_date, products=chosen,
                )
            except Exception as exc:
                st.error(f"❌ Could not build report: {exc}")
                return
        st.session_state["rpt_pdf_bytes"] = pdf_bytes
        st.success(f"✅ Report ready — {len(chosen)} page(s), "
                   f"{len(pdf_bytes)/1024:.0f} KB.")

    pdf_bytes = st.session_state.get("rpt_pdf_bytes")
    if pdf_bytes:
        fname = f"bdc_balance_report_{report_date.strftime('%Y%m%d')}.pdf"
        st.download_button("⬇️ DOWNLOAD PDF REPORT", pdf_bytes, fname,
                           "application/pdf", key="rpt_download")

        # ── Copyable caption ──────────────────────────────────
        st.markdown("---")
        st.markdown("### 📋 Report Caption")
        st.caption("Click the copy icon (top-right of the box) to copy.")
        caption_text = build_balance_caption(df, report_date=report_date)
        if caption_text:
            st.code(caption_text, language=None)
        else:
            st.info("No caption could be generated from the current data.")


# ══════════════════════════════════════════════════════════════
# DAILY LOADINGS REPORT  (from OMC Loadings data / omc_df)
# ══════════════════════════════════════════════════════════════
# Layout differs from the balance report:
#   * gray page background, single white rounded chart panel
#   * THREE fully-coloured header cards (market share / total / BDC count)
#   * one "TOTAL <product> by BDC" bar chart, raw comma-formatted labels
# Data source: st.session_state.omc_df  (cols: Product, BDC, OMC, Quantity, …)
#
# NOTE: the market-share highlight is keyed on the **OMC** column (the buyer),
# NOT the BDC column (the supplier).  OILCORP is an OMC, so its share is
# (OILCORP's lifted volume) / (total loadings).  The bar chart still ranks
# BDCs (which supplier moved the most product).

_LOADINGS_PAGE_BG = "#D4D4D4"
_TOP_N_LOAD       = 15

# Page order matches the uploaded Daily Loadings Report
_LOADINGS_PRODUCTS = ["GASOIL", "PREMIUM", "LPG"]

_LOADINGS_CFG = {
    "GASOIL":  {"display": "GASOIL",  "unit": "Ltrs",
                "color": "#1FAE54", "color_dark": "#178E43"},
    "PREMIUM": {"display": "PREMIUM", "unit": "Ltrs",
                "color": "#ED1C25", "color_dark": "#C20E26"},
    "LPG":     {"display": "LPG",     "unit": "KG",
                "color": "#2196F3", "color_dark": "#1577C7"},
}

_QTY_COL = "Quantity"


def _fmt_commas0(value: float) -> str:
    return f"{value:,.0f}"


def _first_word(name, fallback: str = "OMC") -> str:
    """First word of a label, upper-cased.  Returns `fallback` for blank/empty
    values so `<x> Share %` style labels never blow up on a missing name."""
    parts = str(name).strip().split()
    return parts[0].upper() if parts else fallback


_NAME_SUFFIXES = ("limited", "ltd", "company", "co", "plc", "ghana", "gh",
                  "llc", "lp", "inc", "enterprises", "enterprise")


def _collapse_name(name: str) -> str:
    """Collapse a company name for tolerant matching.

    Removes ALL non-alphanumerics **including spaces** (so 'OIL CORP' == 'OILCORP')
    then strips trailing legal suffixes (LTD/LIMITED/CO/…).  This is deliberately
    more aggressive than a word-level normaliser: the old approach stripped 'corp'
    as a standalone word, turning 'OIL CORP ENERGIA' into 'oil energia' while
    'OILCORP ENERGIA' stayed 'oilcorp energia' — so they never matched and real
    OILCORP sales read as 0.
    """
    if not name:
        return ""
    s = unicodedata.normalize("NFKD", str(name))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "", s)          # drop spaces & punctuation entirely
    changed = True
    while changed:
        changed = False
        for suf in _NAME_SUFFIXES:
            if len(s) > len(suf) + 2 and s.endswith(suf):
                s = s[:-len(suf)]
                changed = True
    return s


def _collapsed_match(x: str, y: str) -> bool:
    """Compare two already-collapsed names. Matches on:
      1. exact equality,
      2. containment (one fully inside the other), when both are long enough,
      3. a shared leading prefix of >= 7 chars — this catches stem variants such
         as 'OILCORP ENERGIA' vs 'OILCORP ENERGY' (prefix 'oilcorpenerg') and
         truncations, without pulling in unrelated OMCs (a 7-char shared prefix
         like 'oilcorp' is highly distinctive)."""
    if not x or not y:
        return False
    if x == y:
        return True
    if len(x) >= 6 and len(y) >= 6 and (x in y or y in x):
        return True
    n = 0
    for cx, cy in zip(x, y):
        if cx != cy:
            break
        n += 1
    return n >= 7


def _names_match(a: str, b: str) -> bool:
    return _collapsed_match(_collapse_name(a), _collapse_name(b))


def _bdc_volume(df_sub: pd.DataFrame, highlight_name: str) -> float:
    """Total Quantity for a given BDC (highlight) within a product slice.
    OILCORP is a BDC (a supplier), so its market share = the volume lifted FROM
    it divided by total loadings. Matches by COLLAPSED name (space/suffix-
    insensitive) so spelling variants of the BDC all count."""
    if df_sub is None or df_sub.empty or "BDC" not in df_sub.columns:
        return 0.0
    ch = _collapse_name(highlight_name)
    if not ch:
        return 0.0
    collapsed = df_sub["BDC"].astype(str).map(_collapse_name)
    mask = collapsed.map(lambda x: _collapsed_match(x, ch))
    return float(pd.to_numeric(df_sub.loc[mask, _QTY_COL], errors="coerce").fillna(0).sum())


def _resolve_highlight_default(df: pd.DataFrame, configured: str) -> str:
    """Return the actual in-data BDC spelling that matches the configured name,
    preferring the variant with the most volume (a real row, not a zero-fill
    placeholder).  Falls back to `configured` if OILCORP never appears."""
    if df is None or df.empty or "BDC" not in df.columns:
        return configured
    ch = _collapse_name(configured)
    if not ch:
        return configured
    tmp = df.copy()
    tmp["_c"] = tmp["BDC"].astype(str).map(_collapse_name)
    tmp["_q"] = pd.to_numeric(tmp[_QTY_COL], errors="coerce").fillna(0)
    cand = tmp[tmp["_c"].map(lambda x: _collapsed_match(x, ch))]
    if cand.empty:
        return configured
    by = cand.groupby(cand["BDC"].astype(str))["_q"].sum().sort_values(ascending=False)
    return by.index[0] if len(by) else configured


def _render_loadings_page(pdf, df_prod, cfg, date_str, highlight_name, share_label):
    fig = plt.figure(figsize=(8.27, 11.69))           # A4 portrait
    fig.patch.set_facecolor(_LOADINGS_PAGE_BG)

    color, color_dark = cfg["color"], cfg["color_dark"]
    display, unit     = cfg["display"], cfg["unit"]

    # ── Title block ───────────────────────────────────────────
    fig.text(0.5, 0.958, "DAILY LOADINGS REPORT", ha="center", va="center",
             fontsize=22, fontweight="bold", color="#111111")
    fig.text(0.5, 0.930, date_str, ha="center", va="center",
             fontsize=17, fontweight="bold", color="#111111")
    fig.text(0.5, 0.882, display, ha="center", va="center",
             fontsize=14, fontweight="bold", color="#111111")

    # ── Aggregate by BDC (chart = which supplier moved the most) ──
    by_bdc = (df_prod.groupby("BDC")[_QTY_COL].sum()
              .sort_values(ascending=False))
    by_bdc = by_bdc[by_bdc > 0]                       # drops blank/zero BDCs (incl. zero-fill rows)
    total  = float(by_bdc.sum())
    n_bdc  = int(by_bdc.shape[0])

    # ── Highlight (OILCORP) market share — keyed on the BDC column ──
    #    (OILCORP is a BDC/supplier: its share = volume lifted from it / total.)
    hi_val    = _bdc_volume(df_prod, highlight_name)
    share_pct = (hi_val / total * 100) if total else 0.0

    top = by_bdc.head(_TOP_N_LOAD)

    # ── Three coloured header cards ───────────────────────────
    _card(fig, 0.045, 0.795, 0.30, 0.062,
          facecolor=color, edgecolor=color_dark,
          lines=[(share_label, 9, "bold"),
                 (f"{share_pct:.2f}", 17, "bold")],
          text_color="white")
    _card(fig, 0.365, 0.795, 0.32, 0.062,
          facecolor=color, edgecolor=color_dark,
          lines=[(f"{display}({unit})", 11, "bold"),
                 (f"{total:,.2f}", 17, "bold")],
          text_color="white")
    _card(fig, 0.705, 0.795, 0.25, 0.062,
          facecolor=color, edgecolor=color_dark,
          lines=[("BDCS", 11, "bold"),
                 (str(n_bdc), 17, "bold")],
          text_color="white")

    # ── White rounded chart panel ─────────────────────────────
    ax_bg = fig.add_axes([0, 0, 1, 1]); ax_bg.axis("off")
    ax_bg.set_xlim(0, 1); ax_bg.set_ylim(0, 1)
    ax_bg.add_patch(FancyBboxPatch(
        (0.04, 0.40), 0.92, 0.375,
        boxstyle="round,pad=0,rounding_size=0.012",
        linewidth=1.0, edgecolor="#cfcfcf", facecolor="white",
        clip_on=False, zorder=0,
    ))

    panel_title = f"TOTAL {display} by BDC"
    if n_bdc > _TOP_N_LOAD:
        panel_title += f"(TOP {_TOP_N_LOAD})"
    fig.text(0.5, 0.752, panel_title, ha="center", va="center",
             fontsize=11, fontweight="bold", color="#333333", zorder=3)

    if len(top):
        _bar_panel(fig, [0.065, 0.47, 0.875, 0.255],
                   top.index.tolist(), top.values.tolist(),
                   color, color_dark,
                   label_fmt=_fmt_commas0, label_size=6.2,
                   tick_size=5.0, wrap_width=12)

    pdf.savefig(fig, facecolor=fig.get_facecolor())
    plt.close(fig)


def generate_daily_loadings_report_pdf(records, report_date=None,
                                       highlight_name="OILCORP ENERGIA LIMITED",
                                       products=None) -> bytes:
    """Build the styled Daily Loadings PDF (one page per product) and return bytes.

    Parameters
    ----------
    records : list[dict] | pd.DataFrame
        The `omc_df` collected by the OMC Loadings page
        (needs at least Product, BDC, OMC, Quantity columns).
    report_date : date | str | None
        Date printed under the title. Defaults to the latest date in the data.
    highlight_name : str
        OMC whose market share fills the first card. The card label is derived
        from its first word, e.g. "OILCORP ENERGIA LIMITED" -> "OILCORP'S MARKET SHARE (%)".
    products : list[str] | None
        Subset / order of products. Defaults to GASOIL, PREMIUM, LPG.
    """
    df = pd.DataFrame(records) if not isinstance(records, pd.DataFrame) else records.copy()
    if df.empty or _QTY_COL not in df.columns or "BDC" not in df.columns:
        raise ValueError("No loadings records (need Product, BDC, Quantity columns).")

    df[_QTY_COL] = pd.to_numeric(df[_QTY_COL], errors="coerce").fillna(0)

    if report_date is None:
        report_date = df["Date"].max() if "Date" in df.columns else None
    date_str = _fmt_date(report_date)

    share_label = f"{_first_word(highlight_name)}'S MARKET SHARE (%)"

    products = products or _LOADINGS_PRODUCTS

    buf = io.BytesIO()
    with PdfPages(buf) as pdf:
        for prod in products:
            cfg = _LOADINGS_CFG.get(prod)
            if cfg is None:
                continue
            df_prod = df[df["Product"] == prod]
            if df_prod.empty:
                continue
            _render_loadings_page(pdf, df_prod, cfg, date_str,
                                  highlight_name, share_label)
    return buf.getvalue()


def show_loadings_report_generator():
    st.markdown("<h2>📄 DAILY LOADINGS REPORT (PDF)</h2>", unsafe_allow_html=True)

    st.markdown("""
    <div style='background:rgba(0,255,255,0.05);border:1px solid #00ffff33;
                border-radius:10px;padding:14px;margin-bottom:16px;'>
    <b style='color:#00ffff;'>What this page does</b><br>
    Builds the styled <b>DAILY LOADINGS REPORT</b> PDF — one page per product
    (GASOIL · PREMIUM · LPG), each showing a highlighted <b>OMC</b> market-share card,
    the product total, the BDC count, and a <b>TOTAL by BDC</b> bar chart —
    straight from the data fetched on the <b>🚚 OMC LOADINGS</b> page.
    </div>
    """, unsafe_allow_html=True)

    omc_df = st.session_state.get("omc_df", pd.DataFrame())
    if not isinstance(omc_df, pd.DataFrame) or omc_df.empty:
        st.warning("⚠️ No loadings data in session. Open **🚚 OMC LOADINGS** and "
                   "click **FETCH OMC LOADINGS** first, then come back here.")
        return

    df = omc_df.copy()
    df[_QTY_COL] = pd.to_numeric(df.get(_QTY_COL, 0), errors="coerce").fillna(0)

    default_date = datetime.now().date()
    if "Date" in df.columns and not df.empty:
        try:
            default_date = pd.to_datetime(df["Date"], errors="coerce").max().date()
        except Exception:
            pass

    # Pre-select the configured OMC name if the app exposes it
    default_highlight = "OILCORP ENERGIA LIMITED"
    try:
        default_highlight = NPA_CONFIG.get("OMC_NAME", default_highlight)  # noqa: F821
    except Exception:
        pass
    # Land the default on the real in-data spelling of OILCORP (variant-safe),
    # so the dropdown shows the name that actually carries the sales.
    default_highlight = _resolve_highlight_default(df, default_highlight)

    c1, c2 = st.columns([1, 1])
    with c1:
        report_date = st.date_input("Report date (printed under the title)",
                                    value=default_date, key="loadrpt_date")
    with c2:
        # Highlight is an OMC (the buyer) — list OMC names, not BDC names.
        omc_options = (
            sorted({o for o in df["OMC"].dropna().astype(str).str.strip().tolist() if o})
            if "OMC" in df.columns else []
        )
        if default_highlight not in omc_options:
            # Keep the configured OMC selectable even on a no-lift day with no
            # real rows (the zero-fill normally guarantees this already).
            omc_options = sorted(set(omc_options) | {default_highlight})

        # Heal a stale/zeroed selection: if the value currently pinned in the
        # widget's session_state matches no real-volume OMC, but the resolved
        # default does, adopt the default. This overrides leftover selections
        # from earlier sessions without clobbering a valid manual choice.
        def _has_real_match(nm):
            if not nm or "OMC" not in df.columns:
                return False
            q = pd.to_numeric(df[_QTY_COL], errors="coerce").fillna(0)
            m = df["OMC"].astype(str).map(lambda o: _names_match(o, nm))
            return bool((m & (q > 0)).any())

        _stored = st.session_state.get("loadrpt_highlight")
        if (_stored is None or not _has_real_match(_stored)) and _has_real_match(default_highlight):
            st.session_state["loadrpt_highlight"] = default_highlight

        idx = omc_options.index(default_highlight) if default_highlight in omc_options else 0
        highlight = st.selectbox(
            "Highlight OMC for the market-share card",
            omc_options or [default_highlight],
            index=idx if omc_options else 0,
            key="loadrpt_highlight",
        )

    avail  = [p for p in _LOADINGS_PRODUCTS if p in set(df.get("Product", []))]
    chosen = st.multiselect(
        "Products to include (page order is fixed)",
        _LOADINGS_PRODUCTS,
        default=avail or _LOADINGS_PRODUCTS,
        key="loadrpt_products",
        format_func=lambda p: _LOADINGS_CFG[p]["display"],
    )

    st.markdown("#### 📊 What the report will contain")
    prev = []
    for prod in (chosen or _LOADINGS_PRODUCTS):
        sub = df[df["Product"] == prod]
        if sub.empty:
            continue
        tot   = float(sub[_QTY_COL].sum())
        n_bdc = (
            int(sub[sub[_QTY_COL] > 0]["BDC"].astype(str).str.strip()
                .replace("", pd.NA).dropna().nunique())
            if "BDC" in sub.columns else 0
        )
        hi = _omc_volume(sub, highlight)
        prev.append({
            "Page":  _LOADINGS_CFG[prod]["display"],
            "Unit":  _LOADINGS_CFG[prod]["unit"],
            "Total": f"{tot:,.2f}",
            "BDCs":  n_bdc,
            f"{_first_word(highlight)} Share %": f"{(hi / tot * 100) if tot else 0:.2f}",
        })
    if prev:
        st.dataframe(pd.DataFrame(prev), use_container_width=True, hide_index=True)

    # ── Match diagnostic (ALWAYS shown) — makes a name mismatch visible instead
    #    of a silent 0, and lets you fix it here without any code change.
    st.markdown("#### 🔎 OILCORP match diagnostic")
    st.caption("matcher: collapse-v4 (space/suffix-insensitive + shared-prefix)")
    ch = _collapse_name(highlight)
    st.caption(f"Selected highlight: **{highlight}**  →  match key: `{ch or '(empty)'}`")
    if "OMC" in df.columns:
        omc_tot = (
            df.assign(_q=pd.to_numeric(df[_QTY_COL], errors="coerce").fillna(0))
              .groupby(df["OMC"].astype(str))["_q"].sum()
              .sort_values(ascending=False)
        )
        matched = [(o, v) for o, v in omc_tot.items() if _names_match(o, highlight)]
        real_matched = [(o, v) for o, v in matched if v > 0]
        if real_matched:
            st.success(
                f"✅ {len(matched)} OMC spelling(s) in the data count toward "
                f"**{_first_word(highlight)}** — total **{sum(v for _, v in matched):,.0f} LT**. "
                f"The share should be non-zero above."
            )
            st.dataframe(
                pd.DataFrame(matched, columns=["OMC (as spelled in data)", "Volume (LT)"]),
                use_container_width=True, hide_index=True,
            )
        else:
            st.error(
                "❌ **No OMC with real volume matches this highlight — that is why the "
                "share is 0.** OILCORP is almost certainly spelled differently in NPA's "
                "data than your `.env` `OMC_NAME`. Find OILCORP in the list below and "
                "**select that exact name in the dropdown above** — it will then work. "
                "For a permanent fix, set `OMC_NAME` in your `.env` to that exact spelling."
            )
            st.dataframe(
                omc_tot.head(40).reset_index()
                       .rename(columns={"OMC": "OMC (as spelled in data)", "_q": "Volume (LT)"}),
                use_container_width=True, hide_index=True,
            )

    if st.button("📄 GENERATE LOADINGS PDF", key="loadrpt_generate"):
        if not chosen:
            st.error("Select at least one product.")
            return
        with st.spinner("Rendering report…"):
            try:
                pdf_bytes = generate_daily_loadings_report_pdf(
                    df, report_date=report_date,
                    highlight_name=highlight, products=chosen,
                )
            except Exception as exc:
                st.error(f"❌ Could not build report: {exc}")
                return
        st.session_state["loadrpt_pdf_bytes"] = pdf_bytes
        st.success(f"✅ Report ready — {len(chosen)} page(s), "
                   f"{len(pdf_bytes)/1024:.0f} KB.")

    pdf_bytes = st.session_state.get("loadrpt_pdf_bytes")
    if pdf_bytes:
        fname = f"daily_loadings_report_{report_date.strftime('%Y%m%d')}.pdf"
        st.download_button("⬇️ DOWNLOAD LOADINGS PDF", pdf_bytes, fname,
                           "application/pdf", key="loadrpt_download")

        # ── Copyable caption ──────────────────────────────────
        st.markdown("---")
        st.markdown("### 📋 Report Caption")
        st.caption("Click the copy icon (top-right of the box) to copy.")
        caption_text = build_loadings_caption(df, report_date=report_date,
                                              highlight_name=highlight)
        if caption_text:
            st.code(caption_text, language=None)
        else:
            st.info("No caption could be generated from the current data.")


# ══════════════════════════════════════════════════════════════
# WHATSAPP CAPTIONS  (auto-generated narrative for each report)
# ══════════════════════════════════════════════════════════════
# The factual parts (totals, OMC counts, highlight share/volume/rank, date)
# are computed from the data. The qualitative words ("strong"/"moderate"/"low")
# are derived from the highlight's RANK within each product — adjust the
# thresholds in _sales_word / _insight_word to taste.

def _ordinal_lc(n: int) -> str:
    if 11 <= (n % 100) <= 13:
        suf = "th"
    else:
        suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suf}"


def _fmt_long_date(d) -> str:
    """e.g. -> '16th June 2026'."""
    if d is None:
        d = datetime.now().date()
    if isinstance(d, str):
        for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                d = datetime.strptime(d, fmt).date(); break
            except ValueError:
                continue
        else:
            d = datetime.now().date()
    if hasattr(d, "hour"):          # datetime / pandas Timestamp -> take date part
        d = d.date()
    return f"{_ordinal_lc(d.day)} {d.strftime('%B')} {d.year}"


def _coerce_report_date(report_date, df):
    if report_date is None and isinstance(df, pd.DataFrame) and "Date" in df.columns and not df.empty:
        try:
            report_date = pd.to_datetime(df["Date"], errors="coerce").max()
        except Exception:
            report_date = None
    return report_date


def _sales_word(rank, n):
    if not rank or not n:
        return "low"
    pct = rank / n
    if pct <= 0.25:
        return "strong"
    if pct <= 0.50:
        return "moderate"
    if pct <= 0.75:
        return "moderately low"
    return "low"


def _insight_word(rank, n):
    if not rank or not n:
        return "low"
    pct = rank / n
    if pct <= 0.25:
        return "high"
    if pct <= 0.50:
        return "moderately okay"
    if pct <= 0.75:
        return "moderately low"
    return "low"


_LOAD_CAPTION_ORDER = ["LPG", "PREMIUM", "GASOIL"]
_LOAD_WORDS = {
    "LPG":     {"header": "LPG",     "share": "LPG",      "lift": "LPG",      "unit": "kg",     "friendly": "LPG"},
    "PREMIUM": {"header": "PREMIUM", "share": "gasoline", "lift": "Gasoline", "unit": "litres", "friendly": "PMS"},
    "GASOIL":  {"header": "GASOIL",  "share": "gasoil",   "lift": "AGO",      "unit": "litres", "friendly": "AGO"},
}


def _loading_stats(df, prod, highlight_name):
    # Highlight is an OMC — rank it among OMCs (buyers), not BDCs (suppliers).
    # Group by NORMALISED name so spelling variants of one OMC don't split its
    # volume across two rows (which would zero-out the highlight's share).
    sub = df[df["Product"] == prod].copy()
    if sub.empty or "OMC" not in sub.columns:
        return {"total": 0.0, "n": 0, "hi": 0.0, "share": 0.0, "rank": None}
    sub["_c"] = sub["OMC"].astype(str).map(_collapse_name)
    sub["_q"] = pd.to_numeric(sub[_QTY_COL], errors="coerce").fillna(0)
    by = sub.groupby("_c")["_q"].sum().sort_values(ascending=False)
    by = by[by > 0]
    total = float(by.sum())
    n = int(by.shape[0])
    hk = _collapse_name(highlight_name)
    hi_vol, rank = 0.0, None
    for i, (name, val) in enumerate(by.items(), start=1):
        if _collapsed_match(name, hk):
            hi_vol, rank = float(val), i
            break
    share = (hi_vol / total * 100) if total else 0.0
    return {"total": total, "n": n, "hi": hi_vol, "share": share, "rank": rank}


def build_loadings_caption(records, report_date=None,
                           highlight_name="OILCORP ENERGIA LIMITED") -> str:
    df = pd.DataFrame(records) if not isinstance(records, pd.DataFrame) else records.copy()
    if df.empty or _QTY_COL not in df.columns:
        return ""
    df[_QTY_COL] = pd.to_numeric(df[_QTY_COL], errors="coerce").fillna(0)
    report_date = _coerce_report_date(report_date, df)
    date_str = _fmt_long_date(report_date)
    short = highlight_name.strip().split()[0].title() if highlight_name.strip() else "OMC"

    stats = {p: _loading_stats(df, p, highlight_name) for p in _LOAD_CAPTION_ORDER}

    # Intro — best & weakest product for the highlight, by rank
    ranked = [(p, stats[p]["rank"]) for p in _LOAD_CAPTION_ORDER if stats[p]["rank"]]
    if ranked:
        best  = min(ranked, key=lambda t: t[1])[0]
        weak  = max(ranked, key=lambda t: t[1])[0]
        intro = (f"{short} recorded its strongest position in "
                 f"{_LOAD_WORDS[best]['friendly']}, while {_LOAD_WORDS[weak]['friendly']} "
                 f"volume remained on the lower side.")
    else:
        intro = f"{short} had limited liftings on the stated date."

    lines = [f"DAILY LOADING SUMMARY ({date_str})", "",
             intro,
             f"The figures below represent OMC liftings from {short} and other OMCs "
             f"on the stated date, as captured from NPA's system.", ""]

    for prod in _LOAD_CAPTION_ORDER:
        w = _LOAD_WORDS[prod]
        s = stats[prod]
        lines.append(f"{w['header']} ({s['total']:,.0f} {w['unit']} total | {s['n']} OMCs)")
        if s["rank"]:
            lines.append(
                f"{short} recorded {s['share']:.2f}% ({s['hi']:,.0f} {w['unit']}) of total "
                f"{w['share']} market share ranking {_ordinal_lc(s['rank'])} overall. "
                f"{w['lift']} liftings reflected {_sales_word(s['rank'], s['n'])} sales."
            )
        else:
            lines.append(f"{short} recorded no {w['share']} liftings on this date.")
        lines.append("")

    lines.append("Summary Insight")
    i = 1
    for prod in ["PREMIUM", "GASOIL", "LPG"]:
        s = stats[prod]
        lines.append(f"{i}. {_LOAD_WORDS[prod]['friendly']} loadings were "
                     f"{_insight_word(s['rank'], s['n'])}")
        i += 1
    lines.append(f"{i}. Overall, {short} demonstrated active market participation.")

    return "\n".join(lines)


def build_balance_caption(records, report_date=None) -> str:
    df = pd.DataFrame(records) if not isinstance(records, pd.DataFrame) else records.copy()
    if df.empty or _COL_BAL not in df.columns:
        return ""
    report_date = _coerce_report_date(report_date, df)
    date_str = _fmt_long_date(report_date)
    g = df.groupby("Product")[_COL_BAL].sum()
    gasoil = float(g.get("GASOIL", 0))
    pms    = float(g.get("PREMIUM", 0))
    lpg    = float(g.get("LPG", 0))

    return (
        "Good morning team,\n\n"
        "Please find attached a summary showing the remaining stock levels for each Bulk "
        "Distribution Company (BDC) in Ghana across the three major products — Gasoil, "
        "Premium, and LPG — along with the respective depots holding these products for "
        f"this morning ({date_str}).\n\n"
        "For ease of reference, the BDCs have been arranged in ascending order (by name), "
        "allowing you to quickly locate specific companies.\n\n"
        "Summary of BDC Stock Balances\n"
        f"1. Gasoil: {gasoil:,.0f} litres\n"
        f"2. Premium (PMS): {pms:,.0f} litres\n"
        f"3. LPG: {lpg:,.0f} kg\n\n"
        "Thank you"
    )