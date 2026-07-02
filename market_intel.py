"""
MARKET INTELLIGENCE ENGINE — market_intel.py
=============================================
Drop-in module for the NPA Energy Analytics dashboard.

WHAT MAKES THIS ONE-OF-A-KIND
-----------------------------
Every other page in the dashboard FETCHES data. This page THINKS with it.
It makes ZERO API calls — it cross-references the datasets already sitting
in session_state (BDC Balance + OMC Loadings) and produces intelligence
nobody else in the market has:

  1. 📸 SNAPSHOT & MOVERS
       Every time balances are fetched, save a timestamped snapshot to disk.
       Diff the two most recent snapshots → who gained stock (vessel
       discharge / transfer-in) and who is draining, per product.
       This builds a proprietary historical database automatically —
       the longer the app runs, the smarter it gets.

  2. ⏳ BDC-LEVEL STOCKOUT LEAGUE
       National Stockout tells you when GHANA runs dry. This tells you when
       EACH COMPETITOR runs dry: balance ÷ that BDC's own daily lifting rate
       = days of cover per BDC per product, with CRITICAL / WARNING flags.
       Knowing a rival is 3 days from empty is actionable sales intel.

  3. 🎯 COMPETITIVE BRIEF (highlight BDC, default OILCORP)
       Rank per product, exact volume gap to the BDC one place above and
       one place below, market share, own days-of-cover — plus a
       WhatsApp-ready caption for management.

  4. 🧲 WINNABLE OMC CUSTOMERS
       Cross-references stock vs loadings: OMCs lifting a product at a depot
       where the highlight BDC HOLDS STOCK but who are buying it from a
       COMPETITOR. Ranked by volume = a prioritised sales call list.

  5. 🏭 DEPOT CONCENTRATION RISK
       Per product: what share of national stock sits in the single largest
       depot (and top-3). One fire / shutdown at that depot = supply shock.

INSTALLATION
------------
1. Save this file next to new_test.py as  market_intel.py
2. In new_test.py add ONE import (near the bdc_report import):

       from market_intel import show_market_intel

3. Add the menu entry string  "🧠 MARKET INTEL"  to the sidebar radio list,
   and one dispatch line at the bottom of main():

       elif choice == "🧠 MARKET INTEL":  show_market_intel()

No new pip dependencies. No .env changes. No API calls.
"""

import os
import re
import io
import json
import unicodedata
from datetime import datetime

import pandas as pd
import streamlit as st


# ══════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════
_COL_BAL      = "ACTUAL BALANCE (LT\\KG)"
_QTY_COL      = "Quantity"
_PRODUCTS     = ["PREMIUM", "GASOIL", "LPG"]
_UNIT         = {"PREMIUM": "LT", "GASOIL": "LT", "LPG": "KG"}
_HIGHLIGHT    = os.getenv("OMC_NAME", "OILCORP ENERGIA LIMITED")

_INTEL_DIR    = os.path.join(os.getcwd(), ".intel_snapshots")
_MAX_SNAPSHOTS = 120   # keep ~4 months of daily snapshots on disk


# ══════════════════════════════════════════════════════════════
# NAME NORMALISATION (self-contained copy — no import coupling)
# ══════════════════════════════════════════════════════════════
def _norm(name: str) -> str:
    if not name:
        return ""
    s = unicodedata.normalize("NFKD", str(name))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    for suffix in ("limited", "ltd", "company", "co", "ghana", "plc",
                   "llc", "lp", "inc", "corp", "enterprise", "enterprises"):
        s = re.sub(rf"\b{suffix}\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


_HI_NORM = _norm(_HIGHLIGHT)


def _is_placeholder(df: pd.DataFrame) -> pd.Series:
    """OILCORP zero-fill placeholder rows in omc_df (qty 0, empty order #)."""
    if df is None or df.empty or "BDC" not in df.columns:
        return pd.Series([], dtype=bool)
    qty   = pd.to_numeric(df.get(_QTY_COL, 0), errors="coerce").fillna(0)
    order = df.get("Order Number", pd.Series("", index=df.index)).astype(str).str.strip()
    return (qty == 0) & (order == "")


# ══════════════════════════════════════════════════════════════
# SNAPSHOT PERSISTENCE
# ══════════════════════════════════════════════════════════════
def _snapshot_from_records(records) -> dict:
    """Collapse bdc_records into {(BDC, Product): total_balance}."""
    df = pd.DataFrame(records)
    if df.empty or _COL_BAL not in df.columns:
        return {}
    g = df.groupby(["BDC", "Product"])[_COL_BAL].sum()
    return {f"{b}|||{p}": float(v) for (b, p), v in g.items()}


def _save_snapshot(records) -> str:
    os.makedirs(_INTEL_DIR, exist_ok=True)
    snap = {
        "ts":   datetime.now().isoformat(timespec="seconds"),
        "data": _snapshot_from_records(records),
    }
    fname = f"intel_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(os.path.join(_INTEL_DIR, fname), "w") as f:
        json.dump(snap, f)
    # Prune old snapshots
    files = sorted(f for f in os.listdir(_INTEL_DIR) if f.startswith("intel_"))
    for old in files[:-_MAX_SNAPSHOTS]:
        try:
            os.remove(os.path.join(_INTEL_DIR, old))
        except Exception:
            pass
    return snap["ts"]


def _load_snapshots(n: int = 2) -> list:
    """Return the n most recent snapshots, newest first."""
    if not os.path.isdir(_INTEL_DIR):
        return []
    files = sorted((f for f in os.listdir(_INTEL_DIR) if f.startswith("intel_")),
                   reverse=True)
    out = []
    for f in files[:n]:
        try:
            with open(os.path.join(_INTEL_DIR, f)) as fh:
                out.append(json.load(fh))
        except Exception:
            continue
    return out


def _diff_snapshots(new: dict, old: dict) -> pd.DataFrame:
    keys = set(new.get("data", {})) | set(old.get("data", {}))
    rows = []
    for k in keys:
        bdc, _, prod = k.partition("|||")
        nv = float(new.get("data", {}).get(k, 0.0))
        ov = float(old.get("data", {}).get(k, 0.0))
        delta = nv - ov
        if abs(delta) < 1:      # ignore noise
            continue
        rows.append({
            "BDC":        bdc,
            "Product":    prod,
            "Previous":   ov,
            "Current":    nv,
            "Δ Change":   delta,
            "Δ %":        (delta / ov * 100) if ov else float("inf"),
            "Signal":     ("📈 REPLENISHED (vessel/transfer-in likely)" if delta > 0
                           else "📉 DRAWDOWN"),
        })
    if not rows:
        return pd.DataFrame()
    return (pd.DataFrame(rows)
            .sort_values("Δ Change", key=lambda s: s.abs(), ascending=False)
            .reset_index(drop=True))


# ══════════════════════════════════════════════════════════════
# CORE ANALYTICS
# ══════════════════════════════════════════════════════════════
def _loadings_window_days(omc_df: pd.DataFrame) -> int:
    try:
        ds = pd.to_datetime(omc_df["Date"], errors="coerce").dropna()
        if ds.empty:
            return 1
        return max((ds.max() - ds.min()).days + 1, 1)
    except Exception:
        return 1


def _bdc_days_of_cover(bal_df: pd.DataFrame, omc_real: pd.DataFrame) -> pd.DataFrame:
    """Days of cover per BDC per product = balance ÷ that BDC's own avg daily liftings."""
    if bal_df.empty:
        return pd.DataFrame()
    days = _loadings_window_days(omc_real)

    bal = bal_df.groupby(["BDC", "Product"])[_COL_BAL].sum().rename("Balance")
    if not omc_real.empty:
        lift = (omc_real.groupby(["BDC", "Product"])[_QTY_COL].sum() / days).rename("Daily Rate")
    else:
        lift = pd.Series(dtype=float, name="Daily Rate")

    df = pd.concat([bal, lift], axis=1).reset_index().fillna(0)
    df = df[df["Product"].isin(_PRODUCTS)]

    def _cover(r):
        if r["Daily Rate"] <= 0:
            return float("inf") if r["Balance"] > 0 else 0.0
        return r["Balance"] / r["Daily Rate"]

    df["Days of Cover"] = df.apply(_cover, axis=1)

    def _status(d):
        if d == float("inf"):
            return "⚪ NO SALES (dormant stock)"
        if d < 3:
            return "🔴 CRITICAL (<3d)"
        if d < 7:
            return "🟡 WARNING (<7d)"
        if d < 14:
            return "🟠 MONITOR (<14d)"
        return "🟢 HEALTHY"

    df["Status"] = df["Days of Cover"].apply(_status)
    df = df.sort_values("Days of Cover").reset_index(drop=True)
    return df


def _rank_table(omc_real: pd.DataFrame, product: str) -> pd.DataFrame:
    sub = omc_real[omc_real["Product"] == product]
    if sub.empty:
        return pd.DataFrame()
    g = (sub.groupby("BDC")[_QTY_COL].sum()
         .sort_values(ascending=False).reset_index()
         .rename(columns={_QTY_COL: "Volume"}))
    g = g[g["Volume"] > 0].reset_index(drop=True)
    g.index = g.index + 1
    g["Share %"] = (g["Volume"] / g["Volume"].sum() * 100).round(2)
    return g


def _competitive_brief(omc_real: pd.DataFrame, cover_df: pd.DataFrame) -> tuple:
    """Returns (rows_for_table, caption_text)."""
    rows = []
    cap_lines = [
        f"MARKET INTELLIGENCE BRIEF — {datetime.now().strftime('%d %b %Y, %H:%M')}",
        f"Subject: {_HIGHLIGHT}",
        "",
    ]
    hi_first = _HIGHLIGHT.split()[0].title()

    for prod in _PRODUCTS:
        rk = _rank_table(omc_real, prod)
        unit = _UNIT[prod]
        if rk.empty:
            rows.append({"Product": prod, "Rank": "—", "Share %": "0.00",
                         "Volume": "0", "Gap to Next Rank ↑": "—",
                         "Lead over Rank ↓": "—", "Own Days of Cover": "—"})
            cap_lines.append(f"{prod}: no liftings recorded in window.")
            continue

        rk["_n"] = rk["BDC"].astype(str).map(_norm)
        match = rk[rk["_n"] == _HI_NORM]
        n_total = len(rk)

        if match.empty:
            rows.append({"Product": prod, "Rank": f"—/{n_total}", "Share %": "0.00",
                         "Volume": "0", "Gap to Next Rank ↑": "—",
                         "Lead over Rank ↓": "—", "Own Days of Cover": "—"})
            cap_lines.append(f"{prod}: {hi_first} recorded no liftings ({n_total} BDCs active).")
            continue

        pos    = int(match.index[0])
        vol    = float(match["Volume"].iloc[0])
        share  = float(match["Share %"].iloc[0])
        above  = rk.loc[pos - 1] if pos > 1 else None
        below  = rk.loc[pos + 1] if pos < n_total else None

        gap_up   = (f"{above['BDC']} (+{above['Volume'] - vol:,.0f} {unit} ahead)"
                    if above is not None else "— (market leader)")
        lead_dn  = (f"{below['BDC']} ({vol - below['Volume']:,.0f} {unit} behind)"
                    if below is not None else "— (last active)")

        cov = cover_df[
            (cover_df["Product"] == prod) &
            (cover_df["BDC"].astype(str).map(_norm) == _HI_NORM)
        ]
        cov_txt = "—"
        if not cov.empty:
            d = float(cov["Days of Cover"].iloc[0])
            cov_txt = "∞" if d == float("inf") else f"{d:.1f}d"

        rows.append({
            "Product":              prod,
            "Rank":                 f"#{pos}/{n_total}",
            "Share %":              f"{share:.2f}",
            "Volume":               f"{vol:,.0f} {unit}",
            "Gap to Next Rank ↑":   gap_up,
            "Lead over Rank ↓":     lead_dn,
            "Own Days of Cover":    cov_txt,
        })

        cap_lines.append(
            f"{prod}: rank #{pos} of {n_total} | {share:.2f}% share "
            f"({vol:,.0f} {unit})."
        )
        if above is not None:
            need = above["Volume"] - vol
            cap_lines.append(
                f"   → Overtaking {above['BDC']} requires +{need:,.0f} {unit} in liftings."
            )
        if below is not None:
            cap_lines.append(
                f"   → Cushion over {below['BDC']}: {vol - below['Volume']:,.0f} {unit}."
            )
        if cov_txt not in ("—",):
            cap_lines.append(f"   → Stock cover at current run-rate: {cov_txt}.")
        cap_lines.append("")

    cap_lines.append("Generated automatically by the Market Intelligence Engine.")
    return rows, "\n".join(cap_lines)


def _winnable_customers(bal_df: pd.DataFrame, omc_real: pd.DataFrame) -> pd.DataFrame:
    """OMCs lifting a product at a depot where the highlight BDC HOLDS stock,
    but buying from a competitor — a prioritised sales-call list."""
    if bal_df.empty or omc_real.empty:
        return pd.DataFrame()

    # Depot+product combos where highlight BDC has positive balance
    hb = bal_df[bal_df["BDC"].astype(str).map(_norm) == _HI_NORM]
    hb = hb[hb[_COL_BAL] > 0]
    if hb.empty:
        return pd.DataFrame()
    my_pos = set(zip(hb["DEPOT"].astype(str).map(_norm),
                     hb["Product"].astype(str)))

    # OMCs already buying from the highlight BDC (per product) — exclude
    mine = omc_real[omc_real["BDC"].astype(str).map(_norm) == _HI_NORM]
    my_customers = set(zip(mine["OMC"].astype(str).map(_norm),
                           mine["Product"].astype(str)))

    others = omc_real[omc_real["BDC"].astype(str).map(_norm) != _HI_NORM].copy()
    if others.empty:
        return pd.DataFrame()
    others["_dep_n"] = others["Depot"].astype(str).map(_norm)
    others = others[[
        (d, p) in my_pos for d, p in zip(others["_dep_n"], others["Product"])
    ]]
    if others.empty:
        return pd.DataFrame()
    others = others[[
        (o, p) not in my_customers
        for o, p in zip(others["OMC"].astype(str).map(_norm), others["Product"])
    ]]
    if others.empty:
        return pd.DataFrame()

    out = (others.groupby(["OMC", "Product", "Depot"])
           .agg(Volume=(_QTY_COL, "sum"),
                Orders=("Order Number", "count"),
                Buying_From=("BDC", lambda x: ", ".join(sorted(set(x))[:3])))
           .reset_index()
           .sort_values("Volume", ascending=False)
           .rename(columns={"Buying_From": "Currently Buying From"})
           .reset_index(drop=True))
    out["Volume"] = out["Volume"].round(0)
    return out


def _depot_concentration(bal_df: pd.DataFrame) -> pd.DataFrame:
    if bal_df.empty:
        return pd.DataFrame()
    rows = []
    for prod in _PRODUCTS:
        sub = bal_df[bal_df["Product"] == prod]
        if sub.empty:
            continue
        g = sub.groupby("DEPOT")[_COL_BAL].sum().sort_values(ascending=False)
        total = float(g.sum())
        if total <= 0:
            continue
        top1 = float(g.iloc[0]);  top1_n = g.index[0]
        top3 = float(g.head(3).sum())
        risk = ("🔴 HIGH" if top1 / total > 0.5
                else "🟡 MEDIUM" if top1 / total > 0.3
                else "🟢 LOW")
        rows.append({
            "Product":            prod,
            "Largest Depot":      top1_n,
            "Top-1 Share %":      f"{top1/total*100:.1f}",
            "Top-3 Share %":      f"{top3/total*100:.1f}",
            "National Total":     f"{total:,.0f} {_UNIT[prod]}",
            "Single-Point Risk":  risk,
        })
    return pd.DataFrame(rows)


# ══════════════════════════════════════════════════════════════
# EXCEL EXPORT
# ══════════════════════════════════════════════════════════════
def _to_excel(sheets: dict) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for name, df in sheets.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                df.to_excel(writer, sheet_name=name[:31], index=False)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════
# PAGE
# ══════════════════════════════════════════════════════════════
def show_market_intel():
    st.markdown("<h2>🧠 MARKET INTELLIGENCE ENGINE</h2>", unsafe_allow_html=True)

    st.markdown(f"""
    <div style='background:rgba(255,0,255,0.06);border:1px solid #ff00ff44;
                border-radius:10px;padding:14px;margin-bottom:16px;'>
    <b style='color:#ff00ff;'>What this page does</b><br>
    Makes <b>zero API calls</b>. Cross-references the BDC Balance and OMC Loadings
    data already in session to produce: snapshot-to-snapshot stock movers,
    a <b>per-BDC stockout league</b> (when each competitor runs dry),
    a competitive brief for <b>{_HIGHLIGHT}</b>, a <b>winnable-customer</b> sales list,
    and depot concentration risk. Every balance snapshot is archived to disk —
    the intelligence compounds over time.
    </div>
    """, unsafe_allow_html=True)

    # ── Data prerequisites ───────────────────────────────────
    records  = st.session_state.get("bdc_records", [])
    omc_df   = st.session_state.get("omc_df", pd.DataFrame())
    has_bal  = bool(records)
    has_omc  = isinstance(omc_df, pd.DataFrame) and not omc_df.empty

    c1, c2 = st.columns(2)
    with c1:
        st.success(f"✅ BDC Balance: {len(records):,} records") if has_bal \
            else st.warning("⚠️ Fetch **🏦 BDC BALANCE** first")
    with c2:
        st.success(f"✅ OMC Loadings: {len(omc_df):,} records") if has_omc \
            else st.warning("⚠️ Fetch **🚚 OMC LOADINGS** first")

    if not has_bal and not has_omc:
        st.error("No data in session. Fetch BDC Balance and OMC Loadings, then return here.")
        return

    bal_df   = pd.DataFrame(records) if has_bal else pd.DataFrame()
    omc_real = omc_df[~_is_placeholder(omc_df)] if has_omc else pd.DataFrame()

    # ══════════════════════════════════════════════════════════
    # 1. SNAPSHOT & MOVERS
    # ══════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("### 📸 SNAPSHOT & MOVERS")

    snaps = _load_snapshots(2)
    n_disk = len([f for f in os.listdir(_INTEL_DIR) if f.startswith("intel_")]) \
        if os.path.isdir(_INTEL_DIR) else 0

    sc1, sc2 = st.columns([1, 2])
    with sc1:
        if st.button("📸 SAVE BALANCE SNAPSHOT NOW", key="mi_snap",
                     disabled=not has_bal):
            ts = _save_snapshot(records)
            st.success(f"Snapshot saved @ {ts}")
            st.rerun()
    with sc2:
        st.caption(f"🗄️ {n_disk} snapshot(s) archived on disk "
                   f"(auto-pruned to last {_MAX_SNAPSHOTS}). "
                   "Save one after each balance fetch to build the movers history.")

    if len(snaps) >= 2:
        newer, older = snaps[0], snaps[1]
        st.caption(f"Comparing **{older['ts']}** → **{newer['ts']}**")
        movers = _diff_snapshots(newer, older)
        if movers.empty:
            st.info("No material stock movements between the last two snapshots.")
        else:
            gain = movers[movers["Δ Change"] > 0]["Δ Change"].sum()
            drop = movers[movers["Δ Change"] < 0]["Δ Change"].sum()
            m1, m2, m3 = st.columns(3)
            m1.metric("BDC×Product movers", f"{len(movers):,}")
            m2.metric("📈 Total replenished", f"{gain:,.0f}")
            m3.metric("📉 Total drawn down", f"{abs(drop):,.0f}")

            disp = movers.copy()
            for c in ("Previous", "Current", "Δ Change"):
                disp[c] = disp[c].apply(lambda v: f"{v:,.0f}")
            disp["Δ %"] = movers["Δ %"].apply(
                lambda v: "NEW" if v == float("inf") else f"{v:+.1f}%")
            st.dataframe(disp.head(40), use_container_width=True, hide_index=True)
    elif len(snaps) == 1:
        st.info("One snapshot archived. Save a second after your next balance "
                "fetch to unlock the movers diff.")
    else:
        st.info("No snapshots yet — click **SAVE BALANCE SNAPSHOT NOW** to start "
                "building the historical database.")

    # ══════════════════════════════════════════════════════════
    # 2. BDC-LEVEL STOCKOUT LEAGUE
    # ══════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("### ⏳ BDC STOCKOUT LEAGUE — who runs dry first")

    cover_df = pd.DataFrame()
    if has_bal and has_omc:
        cover_df = _bdc_days_of_cover(bal_df, omc_real)
        if cover_df.empty:
            st.info("Not enough overlap between balance and loadings data.")
        else:
            win_days = _loadings_window_days(omc_real)
            st.caption(f"Daily rate = each BDC's own liftings ÷ {win_days}-day "
                       "loadings window. ∞ = holds stock but recorded no sales.")

            crit = cover_df[cover_df["Status"].str.startswith(("🔴", "🟡"))]
            if not crit.empty:
                st.error(f"🚨 {len(crit)} BDC×product position(s) under 7 days of cover:")
                dc = crit.copy()
                dc["Balance"]       = dc["Balance"].apply(lambda v: f"{v:,.0f}")
                dc["Daily Rate"]    = dc["Daily Rate"].apply(lambda v: f"{v:,.0f}")
                dc["Days of Cover"] = crit["Days of Cover"].apply(lambda v: f"{v:.1f}")
                st.dataframe(dc, use_container_width=True, hide_index=True)

            with st.expander(f"📋 Full league table — {len(cover_df)} positions"):
                fl = cover_df.copy()
                fl["Balance"]    = fl["Balance"].apply(lambda v: f"{v:,.0f}")
                fl["Daily Rate"] = fl["Daily Rate"].apply(lambda v: f"{v:,.0f}")
                fl["Days of Cover"] = cover_df["Days of Cover"].apply(
                    lambda v: "∞" if v == float("inf") else f"{v:.1f}")
                st.dataframe(fl, use_container_width=True, hide_index=True, height=420)
    else:
        st.info("Needs BOTH balance and loadings data.")

    # ══════════════════════════════════════════════════════════
    # 3. COMPETITIVE BRIEF
    # ══════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown(f"### 🎯 COMPETITIVE BRIEF — {_HIGHLIGHT}")

    brief_rows, caption = [], ""
    if has_omc:
        brief_rows, caption = _competitive_brief(omc_real, cover_df)
        st.dataframe(pd.DataFrame(brief_rows), use_container_width=True, hide_index=True)
        st.markdown("**📋 WhatsApp-ready brief** (copy icon top-right):")
        st.code(caption, language=None)
    else:
        st.info("Needs OMC Loadings data.")

    # ══════════════════════════════════════════════════════════
    # 4. WINNABLE CUSTOMERS
    # ══════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("### 🧲 WINNABLE OMC CUSTOMERS — prioritised sales-call list")
    st.caption(f"OMCs lifting a product at a depot where {_HIGHLIGHT} already holds "
               "stock, but buying from a competitor. Same depot + product in stock "
               "= zero logistics barrier to switching.")

    winnable = pd.DataFrame()
    if has_bal and has_omc:
        winnable = _winnable_customers(bal_df, omc_real)
        if winnable.empty:
            st.info("No winnable-customer overlaps found — either no stock positions "
                    "overlap competitor depots, or every OMC there is already a customer.")
        else:
            w1, w2 = st.columns(2)
            w1.metric("Target OMC×product×depot combos", f"{len(winnable):,}")
            w2.metric("Total addressable volume",
                      f"{winnable['Volume'].sum():,.0f}")
            dv = winnable.copy()
            dv["Volume"] = dv["Volume"].apply(lambda v: f"{v:,.0f}")
            st.dataframe(dv.head(50), use_container_width=True, hide_index=True,
                         height=400)
    else:
        st.info("Needs BOTH balance and loadings data.")

    # ══════════════════════════════════════════════════════════
    # 5. DEPOT CONCENTRATION RISK
    # ══════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("### 🏭 DEPOT CONCENTRATION RISK")

    conc = pd.DataFrame()
    if has_bal:
        conc = _depot_concentration(bal_df)
        if not conc.empty:
            st.dataframe(conc, use_container_width=True, hide_index=True)
            st.caption("Top-1 share > 50% = one depot outage removes over half the "
                       "national stock of that product.")
    else:
        st.info("Needs BDC Balance data.")

    # ══════════════════════════════════════════════════════════
    # EXPORT
    # ══════════════════════════════════════════════════════════
    st.markdown("---")
    _ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    sheets = {}
    if len(snaps) >= 2:
        mv = _diff_snapshots(snaps[0], snaps[1])
        if not mv.empty:
            sheets["Movers"] = mv
    if not cover_df.empty:
        sheets["Stockout League"] = cover_df
    if brief_rows:
        sheets["Competitive Brief"] = pd.DataFrame(brief_rows)
    if not winnable.empty:
        sheets["Winnable Customers"] = winnable
    if not conc.empty:
        sheets["Depot Concentration"] = conc

    if sheets:
        st.download_button(
            "⬇️ DOWNLOAD INTELLIGENCE PACK (EXCEL)",
            _to_excel(sheets),
            f"market_intel_{_ts}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
