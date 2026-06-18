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


def _bar_panel(fig, rect, names, values, color, color_dark):
    """Draw a single bar chart panel with M-formatted value labels."""
    ax = fig.add_axes(rect)
    x = range(len(values))
    bars = ax.bar(x, values, color=color, width=0.72,
                  edgecolor=color_dark, linewidth=0.4)

    top = max(values) if len(values) else 1
    ax.set_ylim(0, top * 1.18)
    for xi, v in zip(x, values):
        ax.text(xi, v + top * 0.015, _fmt_m(v),
                ha="center", va="bottom", fontsize=5.4,
                fontweight="bold", color="#222222")

    ax.set_xticks(list(x))
    ax.set_xticklabels([_wrap_label(n) for n in names], fontsize=4.6,
                       color="#333333", linespacing=0.9)
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
import streamlit as st


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
