"""
DAILY REPORT JOB
================
Runs unattended (GitHub Actions / cron). Fetches NPA data, builds the two PDF
reports, and posts them to a WhatsApp group.

Run modes:
    python daily_report_job.py              # fetch live -> build -> send
    python daily_report_job.py --dry-run    # fetch live -> build -> save, DON'T send
    python daily_report_job.py --self-test  # synthetic data -> build -> save (no network)

Timezone note: Accra is UTC+0, so a GitHub Actions cron of '30 3 * * *' (UTC)
fires at 03:30 Accra time.

Key env vars:
    NPA_USER_ID, NPA_COMPANY_ID, NPA_APP_ID, NPA_ITS_FROM_PERSOL   (NPA access)
    REPORT_FETCH_MODE = single | per_bdc        (default: single)
    REPORT_LOADINGS_DAYS_BACK = 1               (loadings = yesterday by default)
    OMC_NAME = OILCORP ENERGIA LIMITED          (market-share highlight)
    WHATSAPP_PROVIDER, WHAPI_TOKEN, WHATSAPP_GROUP_ID
"""

import os
import sys
import logging
from datetime import timedelta, date

import pandas as pd

from bdc_report import (
    generate_bdc_balance_report_pdf,
    generate_daily_loadings_report_pdf,
    build_balance_caption,
    build_loadings_caption,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("daily_report")

OUT_DIR    = os.getenv("REPORT_OUT_DIR", "report_output")
FETCH_MODE = os.getenv("REPORT_FETCH_MODE", "single")
DAYS_BACK  = int(os.getenv("REPORT_LOADINGS_DAYS_BACK", "1"))
OMC_NAME   = os.getenv("OMC_NAME", "OILCORP ENERGIA LIMITED")


def _ensure_out_dir():
    os.makedirs(OUT_DIR, exist_ok=True)


def _build_balance_pdf(records, report_day):
    if not records:
        log.warning("No balance records — skipping balance report.")
        return None
    pdf = generate_bdc_balance_report_pdf(records, report_date=report_day)
    path = os.path.join(OUT_DIR, f"bdc_balance_report_{report_day:%Y%m%d}.pdf")
    with open(path, "wb") as f:
        f.write(pdf)
    n_bdc = pd.DataFrame(records)["BDC"].nunique()
    log.info("Balance report written: %s  (%d records, %d BDCs)", path, len(records), n_bdc)
    return path


def _build_loadings_pdf(omc_df, loadings_day):
    if omc_df is None or omc_df.empty:
        log.warning("No loadings records — skipping loadings report.")
        return None
    pdf = generate_daily_loadings_report_pdf(
        omc_df, report_date=loadings_day, highlight_name=OMC_NAME)
    path = os.path.join(OUT_DIR, f"daily_loadings_report_{loadings_day:%Y%m%d}.pdf")
    with open(path, "wb") as f:
        f.write(pdf)
    log.info("Loadings report written: %s  (%d rows)", path, len(omc_df))
    return path


def _fetch_live():
    """Fetch balance (today) and loadings (DAYS_BACK days ago)."""
    import npa_core
    today        = date.today()
    loadings_day = today - timedelta(days=DAYS_BACK)
    start_str = end_str = loadings_day.strftime("%m/%d/%Y")

    log.info("Fetch mode: %s", FETCH_MODE)
    log.info("Fetching BDC balance snapshot for %s …", today)
    records = npa_core.fetch_balance_records(mode=FETCH_MODE)
    log.info("  -> %d balance records", len(records))

    log.info("Fetching OMC loadings for %s …", loadings_day)
    omc_df = npa_core.fetch_omc_loadings(start_str, end_str, mode=FETCH_MODE)
    log.info("  -> %d loading rows", len(omc_df))
    return records, omc_df, today, loadings_day


def _synthetic():
    """Synthetic data so install/wiring can be verified without network."""
    import random
    random.seed(1)
    bdcs = [f"BDC {i}" for i in range(1, 21)] + ["OILCORP ENERGIA LIMITED", "BOST"]
    depots = [f"DEPOT {i}" for i in range(1, 10)] + ["BOST GLOBAL DEPOT"]
    bal, load = [], []
    for prod, scale in [("GASOIL", 4e6), ("PREMIUM", 4e6), ("LPG", 3e5)]:
        for b in bdcs:
            d = random.choice(depots)
            v = random.random() ** 2 * scale + 1
            bal.append({"Date": "2026/06/18", "BDC": b, "DEPOT": d, "Product": prod,
                        "ACTUAL BALANCE (LT\\KG)": v, "AVAILABLE BALANCE (LT\\KG)": v * .95})
            load.append({"Date": "2026/06/17", "OMC": "X", "Truck": "T", "Product": prod,
                         "Quantity": random.random() ** 2 * scale * .3 + 1, "Price": 10,
                         "Depot": d, "Order Number": f"O{b}", "BDC": b})
    return bal, pd.DataFrame(load), date(2026, 6, 18), date(2026, 6, 17)


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    self_test = "--self-test" in argv
    dry_run   = "--dry-run" in argv or self_test

    _ensure_out_dir()

    if self_test:
        log.info("SELF-TEST: building reports from synthetic data (no network).")
        records, omc_df, report_day, loadings_day = _synthetic()
    else:
        records, omc_df, report_day, loadings_day = _fetch_live()

    bal_path  = _build_balance_pdf(records, report_day)
    load_path = _build_loadings_pdf(omc_df, loadings_day)

    if not bal_path and not load_path:
        log.error("Both reports are empty — nothing to send. Exiting non-zero.")
        return 2

    items = []
    if bal_path:
        items.append((bal_path, build_balance_caption(records, report_day)))
    if load_path:
        items.append((load_path, build_loadings_caption(
            omc_df, loadings_day, highlight_name=OMC_NAME)))

    if dry_run:
        log.info("DRY-RUN: not sending. Would send %d file(s):", len(items))
        for p, cap in items:
            log.info("   • %s   (caption: %s)", p, cap)
        return 0

    from whatsapp_sender import send_documents_to_group
    log.info("Sending %d file(s) to WhatsApp group …", len(items))
    try:
        send_documents_to_group(items)
        log.info("✅ Sent successfully.")
        return 0
    except Exception as exc:
        log.exception("❌ WhatsApp send failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())