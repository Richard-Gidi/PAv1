import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime

# ========================== CONFIG ==========================
st.set_page_config(
    page_title="Brent Crude • 2008–2026",
    page_icon="🛢️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS to match your original design (dark gradient, fonts, etc.)
st.markdown("""
<style>
    .main { background: linear-gradient(145deg, #070a14 0%, #0d1222 50%, #0a0f1c 100%); color: #e2e8f0; }
    .stApp { background: transparent; }
    h1 { font-family: 'Space Mono', monospace; letter-spacing: 3px; color: #f59e0b; text-transform: uppercase; }
    .event-card {
        background: rgba(255,255,255,0.02);
        border: 1px solid rgba(255,255,255,0.05);
        border-radius: 12px;
        padding: 16px;
        transition: all 0.25s;
    }
    .event-card:hover, .event-card.selected {
        border-color: #f59e0b;
        background: rgba(245,158,11,0.08);
    }
    .live-badge {
        background: rgba(239,68,68,0.15);
        color: #ef4444;
        font-family: 'Space Mono', monospace;
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 10px;
        font-weight: 700;
    }
    .pulse {
        animation: pulse 1.5s ease-in-out infinite;
    }
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.3; }
    }
</style>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&family=Space+Mono:wght@400;700&display=swap" rel="stylesheet">
""", unsafe_allow_html=True)

# ========================== DATA ==========================
PRICE_DATA = [
  {"date": "2008-01", "price": 92}, {"date": "2008-04", "price": 112}, {"date": "2008-07", "price": 134},
  {"date": "2008-10", "price": 72}, {"date": "2009-01", "price": 44}, {"date": "2009-04", "price": 50},
  {"date": "2009-07", "price": 65}, {"date": "2009-10", "price": 73}, {"date": "2010-01", "price": 76},
  {"date": "2010-04", "price": 85}, {"date": "2010-07", "price": 75}, {"date": "2010-10", "price": 83},
  {"date": "2011-01", "price": 97}, {"date": "2011-04", "price": 123}, {"date": "2011-07", "price": 117},
  {"date": "2011-10", "price": 110}, {"date": "2012-01", "price": 111}, {"date": "2012-04", "price": 119},
  {"date": "2012-07", "price": 103}, {"date": "2012-10", "price": 112}, {"date": "2013-01", "price": 113},
  {"date": "2013-04", "price": 103}, {"date": "2013-07", "price": 108}, {"date": "2013-10", "price": 110},
  {"date": "2014-01", "price": 108}, {"date": "2014-04", "price": 108}, {"date": "2014-07", "price": 106},
  {"date": "2014-10", "price": 87}, {"date": "2015-01", "price": 48}, {"date": "2015-04", "price": 60},
  {"date": "2015-07", "price": 56}, {"date": "2015-10", "price": 48}, {"date": "2016-01", "price": 31},
  {"date": "2016-04", "price": 43}, {"date": "2016-07", "price": 46}, {"date": "2016-10", "price": 49},
  {"date": "2017-01", "price": 55}, {"date": "2017-04", "price": 52}, {"date": "2017-07", "price": 49},
  {"date": "2017-10", "price": 57}, {"date": "2018-01", "price": 69}, {"date": "2018-04", "price": 73},
  {"date": "2018-07", "price": 75}, {"date": "2018-10", "price": 76}, {"date": "2019-01", "price": 60},
  {"date": "2019-04", "price": 72}, {"date": "2019-07", "price": 64}, {"date": "2019-10", "price": 60},
  {"date": "2020-01", "price": 64}, {"date": "2020-04", "price": 26}, {"date": "2020-07", "price": 43},
  {"date": "2020-10", "price": 40}, {"date": "2021-01", "price": 55}, {"date": "2021-04", "price": 65},
  {"date": "2021-07", "price": 75}, {"date": "2021-10", "price": 83}, {"date": "2022-01", "price": 88},
  {"date": "2022-03", "price": 128}, {"date": "2022-04", "price": 105}, {"date": "2022-07", "price": 110},
  {"date": "2022-10", "price": 93}, {"date": "2023-01", "price": 84}, {"date": "2023-04", "price": 80},
  {"date": "2023-07", "price": 80}, {"date": "2023-10", "price": 90}, {"date": "2024-01", "price": 79},
  {"date": "2024-04", "price": 88}, {"date": "2024-07", "price": 82}, {"date": "2024-10", "price": 74},
  {"date": "2025-01", "price": 79}, {"date": "2025-04", "price": 68},
  {"date": "2025-06", "price": 74}, {"date": "2025-07", "price": 72}, {"date": "2025-10", "price": 65},
  {"date": "2025-12", "price": 63}, {"date": "2026-01", "price": 67}, {"date": "2026-02", "price": 71},
  {"date": "2026-03", "price": 83},
]

EVENTS = [
  {"id": 1, "date": "2008-07", "label": "Global Financial Crisis", "detail": "Lehman collapse triggered demand destruction. Brent plunged from $134 to $44 in 6 months — a 67% drawdown. The steepest demand shock in modern oil history.", "direction": "down", "magnitude": "-67%", "category": "macro", "dateRange": ["2008-07", "2009-01"]},
  {"id": 2, "date": "2011-01", "label": "Arab Spring", "detail": "Uprisings across MENA disrupted Libyan output (~1.6 mb/d offline). Brent surged above $120 on supply fears as regional instability threatened Gulf producers.", "direction": "up", "magnitude": "+27%", "category": "geopolitical", "dateRange": ["2011-01", "2011-04"]},
  {"id": 3, "date": "2014-07", "label": "OPEC Supply Glut", "detail": "Saudi Arabia refused to cut production to defend market share against US shale. Brent collapsed from $106 to $31 over 18 months as shale output surged.", "direction": "down", "magnitude": "-71%", "category": "supply", "dateRange": ["2014-07", "2016-01"]},
  {"id": 4, "date": "2016-10", "label": "OPEC+ Formation & Cuts", "detail": "Historic OPEC-Russia cooperation. Production cuts of 1.8 mb/d began rebalancing global inventories and put a floor under prices.", "direction": "up", "magnitude": "+41%", "category": "supply", "dateRange": ["2016-10", "2018-01"]},
  {"id": 5, "date": "2019-07", "label": "Saudi Aramco Attack", "detail": "Drone strike on Abqaiq knocked out 5.7 mb/d — the largest single disruption in history. Price spiked 15% intraday before swift Saudi restoration.", "direction": "up", "magnitude": "+15%", "category": "geopolitical", "dateRange": ["2019-07", "2019-10"]},
  {"id": 6, "date": "2020-01", "label": "COVID-19 Demand Shock", "detail": "Global lockdowns erased ~20 mb/d of demand. OPEC+ price war compounded the collapse. WTI briefly went negative; Brent hit $20.", "direction": "down", "magnitude": "-59%", "category": "macro", "dateRange": ["2020-01", "2020-04"]},
  {"id": 7, "date": "2022-01", "label": "Russia–Ukraine War", "detail": "Russian invasion and Western sanctions. Brent spiked to $128 as markets priced in disruption of ~3 mb/d of Russian crude and product exports.", "direction": "up", "magnitude": "+45%", "category": "geopolitical", "dateRange": ["2022-01", "2022-03"]},
  {"id": 8, "date": "2025-01", "label": "2025 Oversupply & Tariffs", "detail": "OPEC+ began unwinding voluntary cuts while US, Canada, Brazil boosted output. Trump tariffs weighed on global demand. Brent fell from $79 (Jan) to $63 (Dec). Annual avg $69/bbl — lowest since 2020.", "direction": "down", "magnitude": "-20%", "category": "supply", "dateRange": ["2025-01", "2025-12"]},
  {"id": 9, "date": "2025-06", "label": "Twelve-Day War (Israel–Iran)", "detail": "June 13: Israel struck Iran's nuclear & military sites. June 22: US hit Fordow, Natanz, Isfahan (Op. Midnight Hammer) with bunker busters. Iran retaliated — missiles on Israel & US base in Qatar. Ceasefire June 24. Hormuz stayed open. Brief price spike then settled as disruption was contained.", "direction": "up", "magnitude": "+8%", "category": "geopolitical", "dateRange": ["2025-06", "2025-07"]},
  {"id": 10, "date": "2026-02", "label": "⚠ Op. Epic Fury & Hormuz Closure", "detail": "Feb 28: US-Israel launched massive strikes on 9+ Iranian cities. Ayatollah Khamenei killed along with senior leadership. Iran retaliated with missiles across the Gulf — hitting Israel, Qatar, UAE, Bahrain, Saudi Arabia, Iraq. IRGC declared Strait of Hormuz closed. Brent surged 9% to ~$83. Barclays warns $100–$120 if Hormuz stays blocked. 14+ mb/d of seaborne crude at risk (~1/3 of global exports). India-Russia crude flows also uncertain after US trade deal linked to halting Russian oil imports.", "direction": "up", "magnitude": "+17%*", "category": "geopolitical", "dateRange": ["2026-02", "2026-03"]},
]

CATEGORIES = {
    "macro": {"color": "#ef4444", "bg": "rgba(239,68,68,0.12)", "label": "Macro / Demand"},
    "geopolitical": {"color": "#f59e0b", "bg": "rgba(245,158,11,0.12)", "label": "Geopolitical"},
    "supply": {"color": "#06b6d4", "bg": "rgba(6,182,212,0.12)", "label": "Supply / OPEC"},
}

df = pd.DataFrame(PRICE_DATA)

# ========================== SESSION STATE ==========================
if "active_categories" not in st.session_state:
    st.session_state.active_categories = {"macro", "geopolitical", "supply"}
if "selected_event" not in st.session_state:
    st.session_state.selected_event = None

# ========================== HEADER & LIVE BANNER ==========================
st.markdown("""
<div style="display:inline-flex;align-items:center;gap:10px;background:rgba(239,68,68,0.1);border:1px solid rgba(239,68,68,0.3);border-radius:8px;padding:8px 16px;margin-bottom:16px;">
    <span style="width:10px;height:10px;background:#ef4444;border-radius:50%;animation:pulse 1.5s infinite;display:inline-block;"></span>
    <span style="font-family:'Space Mono',monospace;font-size:13px;color:#ef4444;font-weight:700;letter-spacing:1px;">
        ACTIVE CONFLICT — STRAIT OF HORMUZ CLOSED — BRENT ~$78–$83
    </span>
</div>
""", unsafe_allow_html=True)

st.markdown('<h1 style="margin:0 0 4px 0;">BRENT CRUDE</h1>', unsafe_allow_html=True)
st.markdown('<p style="color:#64748b;font-size:14px;margin-bottom:20px;">Price History & Event Response • 2008 to March 2026</p>', unsafe_allow_html=True)

# ========================== CATEGORY FILTERS ==========================
cols = st.columns(len(CATEGORIES))
for i, (key, val) in enumerate(CATEGORIES.items()):
    active = key in st.session_state.active_categories
    if cols[i].button(
        f"● {val['label']}",
        key=f"cat_{key}",
        use_container_width=True,
        type="primary" if active else "secondary",
        help="Click to toggle"
    ):
        if active:
            st.session_state.active_categories.remove(key)
        else:
            st.session_state.active_categories.add(key)
        st.rerun()

filtered_events = [e for e in EVENTS if e["category"] in st.session_state.active_categories]

# ========================== CHART ==========================
fig = go.Figure()

# Area chart
fig.add_trace(go.Scatter(
    x=df["date"],
    y=df["price"],
    mode="lines",
    line=dict(color="#f59e0b", width=3),
    fill="tozeroy",
    fillcolor="rgba(245,158,11,0.18)",
    name="Brent Price",
    hovertemplate="%{x}<br>$%{y:.0f}/bbl<extra></extra>"
))

# Event highlights
for event in filtered_events:
    cat_color = CATEGORIES[event["category"]]["color"]
    is_selected = st.session_state.selected_event == event["id"]
    
    # Reference area (shaded zone)
    fig.add_vrect(
        x0=event["dateRange"][0],
        x1=event["dateRange"][1],
        fillcolor=cat_color,
        opacity=0.22 if is_selected else 0.07,
        layer="below",
        line_width=0,
    )
    
    # Reference line + label
    fig.add_vline(
        x=event["date"],
        line_dash="dash",
        line_color=cat_color,
        line_width=1.5,
        opacity=0.7
    )
    fig.add_annotation(
        x=event["date"],
        y=148,
        text=str(event["id"]),
        showarrow=False,
        font=dict(size=11, color=cat_color, family="Space Mono"),
        bgcolor="rgba(10,12,20,0.9)",
        borderpad=3,
        bordercolor=cat_color,
        borderwidth=1
    )

fig.update_layout(
    height=460,
    template="plotly_dark",
    paper_bgcolor="rgba(255,255,255,0.015)",
    plot_bgcolor="rgba(255,255,255,0)",
    margin=dict(l=10, r=10, t=30, b=10),
    xaxis=dict(
        tickformat="%Y",
        tickangle=0,
        tickfont=dict(size=11, family="Space Mono"),
        gridcolor="rgba(255,255,255,0.05)"
    ),
    yaxis=dict(
        title="Price $/bbl",
        tickprefix="$",
        range=[0, 150],
        tickfont=dict(size=11, family="Space Mono"),
        gridcolor="rgba(255,255,255,0.05)"
    ),
    showlegend=False,
    hovermode="x unified"
)

st.plotly_chart(fig, use_container_width=True)

# ========================== EVENTS TIMELINE ==========================
st.markdown("### Events Timeline")
cols = st.columns(3)

for idx, event in enumerate(filtered_events):
    cat = CATEGORIES[event["category"]]
    is_selected = st.session_state.selected_event == event["id"]
    is_live = event["id"] == 10
    
    with cols[idx % 3]:
        # Build clean card HTML
        card_html = f"""
        <div class="event-card {'selected' if is_selected else ''}">
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;flex-wrap:wrap;">
                <span style="font-family:'Space Mono',monospace;font-size:11px;background:{cat['bg']};color:{cat['color']};padding:2px 8px;border-radius:4px;">
                    {event['date']}
                </span>
                <span style="color:{'#22c55e' if event['direction']=='up' else '#ef4444'};font-weight:700;">
                    {'▲' if event['direction']=='up' else '▼'} {event['magnitude']}
                </span>
                {"<span class='live-badge'>LIVE</span>" if is_live else ""}
            </div>
            <div style="font-weight:700;font-size:14px;margin-bottom:6px;color:#f1f5f9;">{event['label']}</div>
        </div>
        """
        
        # ✅ KEY FIX: Use st.html() — this is the modern, reliable way
        st.html(card_html)
        
        # Button below the card (cleaner layout)
        if st.button(f"View details →", key=f"btn_{event['id']}", use_container_width=True):
            st.session_state.selected_event = None if is_selected else event["id"]
            st.rerun()
        
        if is_selected:
            st.info(event["detail"])

# ========================== KEY STATS ==========================
st.markdown("---")
st.markdown("### Key Stats (March 2026)")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("52-Week Range", "$58 – $85", "Brent front-month")
with col2:
    st.metric("Hormuz Flow At Risk", "14+ mb/d", "≈1/3 of seaborne crude")
with col3:
    st.metric("EIA 2026 Forecast", "$58/bbl avg", "Pre-conflict estimate")
with col4:
    st.metric("Barclays Scenario", "$100–$120", "If Hormuz prolonged")

# ========================== FOOTER ==========================
st.caption("SOURCES: EIA, IMF, ICE, OPEC MOMR, Reuters | UPDATED MARCH 3 2026 • Click event cards to highlight on chart")