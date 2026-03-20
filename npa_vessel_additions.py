def _load_vessel_sheet(file_id_or_url=None, header=14, skiprows=1, skipfooter=1):
    """Load vessel discharge data from Google Sheets (multiple fallback strategies)."""
    import re as _re
    from io import StringIO, BytesIO

    url_in = file_id_or_url or VESSEL_SHEET_URL

    if _re.match(r'^[a-zA-Z0-9-_]{20,}$', url_in):
        file_id, gid = url_in, None
    else:
        m_id  = _re.search(r'/d/([a-zA-Z0-9-_]+)', url_in)
        file_id = m_id.group(1) if m_id else None
        m_gid = _re.search(r'(?:(?:#|\?|&)gid=)(\d+)', url_in)
        gid   = m_gid.group(1) if m_gid else None

    if not file_id:
        return None, "Could not extract Google Sheets file ID from the URL provided."

    candidates = []
    if gid:
        candidates.append((f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv&gid={gid}", "csv"))
    candidates.append((f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv&gid=0", "csv"))
    candidates.append((f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv", "csv"))
    candidates.append((f"https://docs.google.com/spreadsheets/d/{file_id}/gviz/tq?tqx=out:csv", "gviz"))
    candidates.append((f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx", "xlsx"))

    hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    for url, mode in candidates:
        try:
            resp = _requests.get(url, headers=hdrs, allow_redirects=True, timeout=30)
            if resp.status_code != 200 or not resp.content:
                continue
            if mode == "xlsx":
                return pd.read_excel(BytesIO(resp.content)), None
            if mode == "gviz":
                return pd.read_csv(StringIO(resp.content.decode("utf-8", errors="replace"))), None
            df = pd.read_csv(
                StringIO(resp.content.decode("utf-8", errors="replace")),
                header=header, skiprows=skiprows, skipfooter=skipfooter, engine='python'
            )
            return df, None
        except Exception:
            continue

    return None, (
        "All fetch strategies failed.\n"
        "• Ensure the sheet is shared as 'Anyone with the link \u2192 Viewer'.\n"
        "• Try: File \u2192 Publish to the web \u2192 CSV, then use the published URL."
    )


def _parse_vessel_discharge_date(date_str, default_year='2025'):
    """Return (month_code, year, status) from a date/status cell."""
    date_str = str(date_str).strip().upper()
    if 'PENDING' in date_str or date_str in ('NAN', ''):
        month_code = VESSEL_MONTH_MAPPING.get(
            datetime.now().strftime('%b'), datetime.now().strftime('%b').upper()
        )
        return month_code, default_year, 'PENDING'
    try:
        if '-' in date_str:
            parts = date_str.split('-')
            if len(parts) == 2:
                month = VESSEL_MONTH_MAPPING.get(parts[1].title(), parts[1].upper())
                return month, default_year, 'DISCHARGED'
    except Exception:
        pass
    return 'Unknown', default_year, 'DISCHARGED'


def _process_vessel_dataframe(vessel_df, year='2025'):
    """Clean, map and enrich the raw vessel Google Sheets DataFrame."""
    vessel_df = vessel_df.copy()
    vessel_df.columns = vessel_df.columns.str.strip()

    col_idx = {}
    for i, col in enumerate(vessel_df.columns):
        cl = str(col).lower().strip()
        if 'receiver' in cl or (i == 0 and 'unnamed' not in cl):
            col_idx['receivers'] = i
        elif 'type' in cl and 'receiver' not in cl:
            col_idx['type'] = i
        elif 'vessel' in cl and 'name' in cl:
            col_idx['vessel_name'] = i
        elif 'supplier' in cl:
            col_idx['supplier'] = i
        elif 'product' in cl:
            col_idx['product'] = i
        elif 'quantity' in cl or ('mt' in cl and 'quantity' not in cl):
            col_idx['quantity'] = i
        elif 'date' in cl or 'discharg' in cl:
            col_idx['date'] = i

    records = []
    for _idx, row in vessel_df.dropna(how='all').iterrows():
        try:
            receivers    = str(row.iloc[col_idx.get('receivers', 0)]).strip()
            vessel_type  = str(row.iloc[col_idx.get('type', 1)]).strip()
            vessel_name  = str(row.iloc[col_idx.get('vessel_name', 2)]).strip()
            supplier     = str(row.iloc[col_idx.get('supplier', 3)]).strip()
            product_raw  = str(row.iloc[col_idx.get('product', 4)]).strip().upper()
            quantity_str = str(row.iloc[col_idx.get('quantity', 5)]).replace(',', '').strip()
            date_cell    = str(row.iloc[col_idx.get('date', 6)]).strip()

            if (receivers.upper() in {'RECEIVER(S)', 'RECEIVERS', 'NAN', ''} or
                    product_raw in {'PRODUCT', 'NAN', ''} or
                    quantity_str.upper() in {'NAN', '-', 'QUANTITY (MT)', ''}):
                continue
            try:
                qty_mt = float(quantity_str)
            except ValueError:
                continue
            if qty_mt <= 0:
                continue

            product = VESSEL_PRODUCT_MAPPING.get(product_raw, product_raw)
            if product not in VESSEL_CONVERSION_FACTORS:
                continue

            qty_lt = qty_mt * VESSEL_CONVERSION_FACTORS[product]
            month, yr, status = _parse_vessel_discharge_date(date_cell, default_year=year)

            records.append({
                'Receivers':        receivers,
                'Vessel_Type':      vessel_type,
                'Vessel_Name':      vessel_name,
                'Supplier':         supplier,
                'Product':          product,
                'Original_Product': product_raw,
                'Quantity_MT':      qty_mt,
                'Quantity_Litres':  qty_lt,
                'Date_Discharged':  date_cell,
                'Month':            month,
                'Year':             yr,
                'Status':           status,
            })
        except Exception:
            continue

    return pd.DataFrame(records)


def show_vessel_supply():
    VCOLS  = {'PREMIUM': '#00ffff', 'GASOIL': '#ffaa00', 'LPG': '#00ff88', 'NAPHTHA': '#ff6600'}
    VICONS = {'PREMIUM': '\u26fd', 'GASOIL': '\U0001f69b', 'LPG': '\U0001f535', 'NAPHTHA': '\U0001f7e0'}
    MONTH_ORDER = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']

    st.markdown("<h2>\U0001f6a2 VESSEL SUPPLY TRACKER</h2>", unsafe_allow_html=True)
    st.markdown("""
    <p style='color:#ff00ff; font-size:16px;'>
    Track national vessel discharge data — <b>discharged</b> cargo and <b>pending arrivals</b>.<br>
    Pending vessels represent fuel already contracted &amp; en route / at anchorage.<br>
    Enable the toggle in <b>\U0001f30d National Stockout</b> to include pending cargo in the runway forecast.
    </p>
    """, unsafe_allow_html=True)
    st.markdown("---")

    col1, col2 = st.columns([3, 1])
    with col1:
        sheet_url = st.text_input(
            "Google Sheets URL or File ID",
            value=VESSEL_SHEET_URL,
            key='vessel_sheet_url',
            help="Sheet must be shared as 'Anyone with the link \u2192 Viewer'."
        )
    with col2:
        year_sel = st.selectbox("Data Year", ['2025', '2024', '2026'], key='vessel_year_input')

    if st.button("\U0001f504 FETCH VESSEL DATA", width='stretch', key='vessel_fetch'):
        with st.spinner("\U0001f4e1 Loading vessel data from Google Sheets\u2026"):
            raw_df, err = _load_vessel_sheet(sheet_url)
            if raw_df is None:
                st.error(f"\u274c {err}")
                return
            st.info(f"Raw sheet loaded: **{len(raw_df)} rows**. Processing\u2026")
            processed = _process_vessel_dataframe(raw_df, year=year_sel)
            if processed.empty:
                st.warning("\u26a0\ufe0f No valid records found. Check the sheet format and sharing settings.")
                return
            st.session_state.vessel_data = processed
            st.session_state['vessel_year'] = year_sel   # use bracket syntax — avoids widget-key clash
            st.success(f"\u2705 Processed **{len(processed)} vessel records**!")
            st.rerun()

    if st.session_state.get('vessel_data') is None or st.session_state.vessel_data.empty:
        st.info("\U0001f446 Click **FETCH VESSEL DATA** to load from Google Sheets.")
        return

    df         = st.session_state.vessel_data
    yr_lbl     = st.session_state.get('vessel_year', '2025')
    discharged = df[df['Status'] == 'DISCHARGED']
    pending    = df[df['Status'] == 'PENDING']
    total_vol  = df['Quantity_Litres'].sum()
    dis_vol    = discharged['Quantity_Litres'].sum()
    pend_vol   = pending['Quantity_Litres'].sum()

    st.markdown("---")
    st.markdown(f"### \U0001f6e2\ufe0f NATIONAL VESSEL SUPPLY OVERVIEW \u2014 {yr_lbl}")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(
            "<div class='metric-card'><h2>TOTAL VESSELS</h2>"
            f"<h1>{len(df)}</h1></div>",
            unsafe_allow_html=True
        )
    with c2:
        st.markdown(
            "<div class='metric-card'><h2>DISCHARGED</h2>"
            f"<h1>{len(discharged)}</h1>"
            f"<p style='color:#00ff88;font-size:14px;margin:0;'>{dis_vol/1e6:.2f}M LT</p></div>",
            unsafe_allow_html=True
        )
    with c3:
        st.markdown(
            "<div class='metric-card' style='border-color:#ffaa00;'>"
            "<h2>PENDING</h2>"
            f"<h1>{len(pending)}</h1>"
            f"<p style='color:#ffaa00;font-size:14px;margin:0;'>{pend_vol/1e6:.2f}M LT</p></div>",
            unsafe_allow_html=True
        )
    with c4:
        st.markdown(
            "<div class='metric-card'><h2>TOTAL VOLUME</h2>"
            f"<h1>{total_vol/1e6:.2f}M</h1>"
            "<p style='color:#888;font-size:14px;margin:0;'>Litres</p></div>",
            unsafe_allow_html=True
        )

    # ── PENDING ──────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### \u23f3 PENDING VESSELS \u2014 Supply Pipeline")
    st.caption(
        "Vessels **not yet discharged** \u2014 committed fuel in the pipeline. "
        "Use the **\U0001f6a2 Include pending vessels** toggle in \U0001f30d **National Stockout** "
        "to add these volumes to the runway calculation."
    )

    if pending.empty:
        st.success("\u2705 No pending vessels \u2014 all recorded vessels have discharged.")
    else:
        pend_by_prod = (
            pending.groupby('Product')
            .agg(
                Vessels=('Vessel_Name', 'count'),
                Volume_LT=('Quantity_Litres', 'sum'),
                Volume_MT=('Quantity_MT', 'sum'),
            )
            .reset_index()
        )
        pcols = st.columns(min(len(pend_by_prod), 4))
        for col, (_, row) in zip(pcols, pend_by_prod.iterrows()):
            prod  = row['Product']
            color = VCOLS.get(prod, '#ffffff')
            icon  = VICONS.get(prod, '\U0001f6e2\ufe0f')
            with col:
                st.markdown(f"""
                <div style='background:rgba(10,14,39,0.85); padding:20px; border-radius:14px;
                            border:2.5px solid {color}; text-align:center; margin-bottom:8px;'>
                    <div style='font-size:30px;'>{icon}</div>
                    <div style='font-family:Orbitron,sans-serif; color:{color}; font-size:14px;
                                font-weight:700; margin:8px 0;'>{prod}</div>
                    <div style='color:#e0e0e0; font-size:30px; font-weight:700;'>{int(row['Vessels'])}</div>
                    <div style='color:#888; font-size:12px; margin-bottom:8px;'>vessels</div>
                    <div style='color:{color}; font-size:18px; font-weight:700;'>
                        {row['Volume_LT']:,.0f} LT</div>
                    <div style='color:#888; font-size:12px;'>({row['Volume_MT']:,.0f} MT)</div>
                </div>
                """, unsafe_allow_html=True)

        st.markdown("#### \U0001f4cb Pending Vessel Details")
        pd_disp = pending[['Vessel_Name', 'Vessel_Type', 'Receivers', 'Supplier',
                            'Product', 'Quantity_MT', 'Quantity_Litres',
                            'Date_Discharged', 'Month']].copy()
        pd_disp['Quantity_MT']     = pd_disp['Quantity_MT'].apply(lambda x: f"{x:,.0f}")
        pd_disp['Quantity_Litres'] = pd_disp['Quantity_Litres'].apply(lambda x: f"{x:,.0f}")
        st.dataframe(pd_disp, use_container_width=True, hide_index=True)

    # ── DISCHARGED ───────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### \u2705 DISCHARGED VESSELS")
    tab1, tab2 = st.tabs(["\U0001f4ca By Product & Month", "\U0001f4cb Full List"])

    with tab1:
        if discharged.empty:
            st.info("No discharged vessels in the dataset.")
        else:
            monthly = (
                discharged.groupby(['Month', 'Product'])['Quantity_Litres']
                .sum().reset_index()
            )
            monthly['Month'] = pd.Categorical(
                monthly['Month'], categories=MONTH_ORDER, ordered=True
            )
            monthly = monthly.sort_values('Month')
            fig_bar = go.Figure()
            for prod in monthly['Product'].unique():
                pdata = monthly[monthly['Product'] == prod]
                fig_bar.add_trace(go.Bar(
                    name=prod,
                    x=pdata['Month'],
                    y=pdata['Quantity_Litres'],
                    marker_color=VCOLS.get(prod, '#ffffff'),
                ))
            fig_bar.update_layout(
                barmode='group',
                paper_bgcolor='rgba(10,14,39,0.9)',
                plot_bgcolor='rgba(10,14,39,0.9)',
                font=dict(color='white'), height=380,
                legend=dict(font=dict(color='white')),
                xaxis=dict(gridcolor='rgba(255,255,255,0.05)', title='Month'),
                yaxis=dict(gridcolor='rgba(255,255,255,0.05)', title='Volume (LT)'),
                title=dict(
                    text=f'Monthly Vessel Discharge by Product \u2014 {yr_lbl}',
                    font=dict(color='#00ffff', family='Orbitron')
                ),
            )
            st.plotly_chart(fig_bar, use_container_width=True)

            ps = (
                discharged.groupby('Product')
                .agg(
                    Vessels=('Vessel_Name', 'count'),
                    Volume_LT=('Quantity_Litres', 'sum'),
                    Volume_MT=('Quantity_MT', 'sum'),
                )
                .reset_index()
            )
            ps['Volume_LT'] = ps['Volume_LT'].apply(lambda x: f"{x:,.0f}")
            ps['Volume_MT'] = ps['Volume_MT'].apply(lambda x: f"{x:,.0f}")
            st.dataframe(ps, use_container_width=True, hide_index=True)

    with tab2:
        if discharged.empty:
            st.info("No discharged vessels yet.")
        else:
            dd = discharged[['Vessel_Name', 'Vessel_Type', 'Receivers', 'Supplier',
                             'Product', 'Quantity_MT', 'Quantity_Litres',
                             'Date_Discharged', 'Month']].copy()
            dd['Quantity_MT']     = dd['Quantity_MT'].apply(lambda x: f"{x:,.0f}")
            dd['Quantity_Litres'] = dd['Quantity_Litres'].apply(lambda x: f"{x:,.0f}")
            st.dataframe(dd, use_container_width=True, hide_index=True)

    # ── EXPORT ───────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### \U0001f4be EXPORT VESSEL DATA")
    if st.button("\U0001f4c4 EXPORT TO EXCEL", key='vessel_export', width='stretch'):
        out_dir = os.path.join(os.getcwd(), "vessel_reports")
        os.makedirs(out_dir, exist_ok=True)
        fname = f"vessel_data_{yr_lbl}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        fpath = os.path.join(out_dir, fname)
        with pd.ExcelWriter(fpath, engine='openpyxl') as writer:
            df.to_excel(writer,         sheet_name='All Vessels', index=False)
            discharged.to_excel(writer, sheet_name='Discharged',  index=False)
            pending.to_excel(writer,    sheet_name='Pending',      index=False)
        with open(fpath, 'rb') as f_dl:
            st.download_button(
                "\u2b07\ufe0f DOWNLOAD VESSEL EXCEL",
                f_dl, fname,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key='vessel_dl'
            )