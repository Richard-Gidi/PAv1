"""
NPA BDC Balance Aggregator
---------------------------
Loops through all BDC IDs, fetches product balances from the NPA API,
and aggregates results into a single CSV + JSON output.

Usage:
    pip install requests pandas
    python npa_balance_aggregator.py

Outputs:
    npa_balances_YYYYMMDD_HHMMSS.csv
    npa_balances_YYYYMMDD_HHMMSS.json
"""

import requests
import pandas as pd
import json
import time
import logging
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
NPA_COMPANY_ID      = 1
NPA_USER_ID         = 123293
NPA_APP_ID          = 3
NPA_ITS_FROM_PERSOL = "Persol Systems Limited"
NPA_BDC_BALANCE_URL = "https://iml.npa-enterprise.com/NPAAPILIVE/Home/CreateProductBalance"

# Delay between requests (seconds) — be polite to the server
REQUEST_DELAY = 1.0

# Timeout per request (seconds)
REQUEST_TIMEOUT = 30

# ─────────────────────────────────────────────
# BDC ID MAPPINGS
# ─────────────────────────────────────────────
BDC_IDS = {
    "ALFAPETRO GHANA LIMITED":                          110,
    "BLUE OCEAN ENERGY LIMITED":                        111,
    "CHASE PETROLEUM GHANA LIMITED":                    112,
    "CHROME ENERGY RESOURCES LIMITED":                  113,
    "CIRRUS OIL SERVICES LIMITED":                      114,
    "DEEN PETROLEUM GHANA LIMITED":                     115,
    "DOME ENERGY RESOURCES LIMITED":                    116,
    "DOMINION INTERNATIONAL PETROLEUM LIMITED":         118,
    "EBONY OIL & GAS LIMITED":                          119,
    "SAGE DISTRIBUTION LIMITED":                        120,
    "FUELTRADE LIMITED":                                122,
    "GOENERGY COMPANY LIMITED":                         123,
    "HASK OIL COMPANY LIMITED":                         124,
    "JUWEL ENERGY LIMITED":                             125,
    "MARANATHA OIL SERVICES LIMITED":                   126,
    "MISYL ENERGY COMPANY LIMITED":                     127,
    "MOBILE OIL ENERGY RESOURCES GHANA LIMITED":        128,
    "NATION SERVICES COMPANY LIMITED":                  129,
    "OIL CHANNEL LIMITED":                              130,
    "OILTRADE COMPANY LIMITED":                         131,
    "PEACE PETROLEUM":                                  132,
    "PETROLEUM WARE HOUSE AND SUPPLIES LIMITED":        133,
    "RAMA ENERGY LIMITED":                              134,
    "REDFINS ENERGY LIMITED":                           135,
    "SPRINGFIELD ENERGY LIMITED":                       137,
    "VIHAMA ENERGY LIMITED":                            138,
    "XF PETROLEUM LIMITED":                             139,
    "GLOBEX ENERGY LTD":                                230,
    "FIRST DEEPWATER DISCOVERY LIMITED":                232,
    "LHS GHANA LIMITED":                                234,
    "MIMSHACH ENERGY LIMITED":                          235,
    "TIMELESS OIL COMPANY LTD":                         236,
    "MATRIX GAS GHANA LIMITED":                         6184,
    "TEMA OIL REFINERY (TOR)":                          20466,
    "WI ENERGY":                                        20468,
    "MED PETROLEUM LIMITED":                            20470,
    "EAGLE PETROLEUM COMPANY LIMITED":                  20471,
    "BATTOP ENERGY LIMITED":                            20472,
    "PLATON OIL AND GAS":                               20473,
    "RICHELLE ENERGY LIMITED":                          20476,
    "AKWAABA LINK INVESTMENTS LIMITED":                 20530,
    "IMPERIAL ENERGY":                                  20543,
    "BOST":                                             20558,
    "SA ENERGY LIMITED":                                20570,
    "SOCIETE NATIONAL BURKINABE (SONABHY)":             20614,
    "ADINKRA SUPPLY COMPANY LIMITED":                   20621,
    "STRATCON ENERGY AND TRADING LIMITED":              20638,
    "UNACCOUNTED BDC":                                  20643,
    "ASTRA OIL SERVICES LIMITED":                       20686,
    "LEMLA PETROLEUM LIMITED":                          20687,
    "LIB GHANA LIMITED":                                20696,
    "NENSER PETROLEUM GHANA LIMITED":                   20752,
    "WOODFIELDS ENERGY RESOURCES LIMITED":              20762,
    "GENYSIS GLOBAL LIMITED":                           20765,
    "HILSON PETROLEUM GHANA LIMITED":                   20771,
    "KPABULGA ENERGY LIMITED":                          20775,
    "GLORYMAY PETROLEUM COMPANY LIMITED":               20815,
    "MARIAJE LINX INVESTMENT LIMITED":                  20823,
    "EVERSTONE ENERGY LIMITED":                         20824,
    "COMANDA ENERGY LIMITED":                           20840,
    "CUBICA ENERGY LIMITED":                            20846,
    "BP GHANA":                                         20859,
    "TRAFIGURA PTE":                                    20860,
    "INTERNATIONAL PETROLEUM RESOURCES GHANA LIMITED":  20862,
    "RESTON ENERGY TRADING LIMITED":                    20863,
    "JONESBRIDGE LIMITED":                              20870,
    "BOST G40":                                         20880,
    "OILCORP ENERGIA LIMITED":                          20900,
    "GHANA NATIONAL GAS COMPANY LIMITED":               20913,
    "SENTUO OIL REFINERY":                              20919,
    "BLUE OCEAN BOTTLING PLANT":                        20938,
    "NEWGAS CYLINDER BOTTLING LIMITED":                 20939,
    "PORTICA OIL AND GAS RESOURCE LIMITED":             20942,
    "CALGARTH INVESTMENT LTD":                          20947,
    "CHRISVILLE ENERGY SOLUTIONS LTD":                  20974,
    "BAZUKA ENERGY LTD":                                20975,
    "C-CLEANED OIL LTD":                                20977,
    "PK JEGS ENERGY LTD":                               20980,
}

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# FETCH FUNCTION
# ─────────────────────────────────────────────
def fetch_bdc_balance(bdc_name: str, bdc_id: int) -> dict:
    """
    Fetch balance for a single BDC. Returns a dict with status and data.
    strQuery1 is used to filter by BDC ID.
    """
    params = {
        "lngCompanyId":     NPA_COMPANY_ID,
        "strITSfromPersol": NPA_ITS_FROM_PERSOL,
        "strGroupBy":       "BDC",
        "strGroupBy1":      "DEPOT",
        "strQuery1":        bdc_id,   # BDC ID filter
        "strQuery2":        "",
        "strQuery3":        "",
        "strQuery4":        "",
        "strPicHeight":     1,
        "szPicWeight":      1,
        "lngUserId":        NPA_USER_ID,
        "intAppId":         NPA_APP_ID,
    }

    try:
        response = requests.get(
            NPA_BDC_BALANCE_URL,
            params=params,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()

        data = response.json()

        # Normalise: API may return a list or a dict with a data key
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            # Try common wrapper keys
            records = (
                data.get("data") or
                data.get("Data") or
                data.get("result") or
                data.get("Result") or
                [data]
            )
        else:
            records = []

        # Stamp each record with BDC metadata
        for r in records:
            if isinstance(r, dict):
                r["_bdc_name"] = bdc_name
                r["_bdc_id"]   = bdc_id

        return {"status": "ok", "bdc_name": bdc_name, "bdc_id": bdc_id, "records": records}

    except requests.exceptions.Timeout:
        log.warning(f"  TIMEOUT — {bdc_name} (ID {bdc_id})")
        return {"status": "timeout", "bdc_name": bdc_name, "bdc_id": bdc_id, "records": []}

    except requests.exceptions.HTTPError as e:
        log.warning(f"  HTTP ERROR {e.response.status_code} — {bdc_name} (ID {bdc_id})")
        return {"status": f"http_{e.response.status_code}", "bdc_name": bdc_name, "bdc_id": bdc_id, "records": []}

    except Exception as e:
        log.warning(f"  ERROR — {bdc_name} (ID {bdc_id}): {e}")
        return {"status": "error", "bdc_name": bdc_name, "bdc_id": bdc_id, "records": []}


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_out     = f"npa_balances_{timestamp}.csv"
    json_out    = f"npa_balances_{timestamp}.json"

    all_records = []
    summary     = []

    total = len(BDC_IDS)
    log.info(f"Starting NPA balance fetch for {total} BDCs...")
    log.info(f"User ID: {NPA_USER_ID} | App ID: {NPA_APP_ID} | Company ID: {NPA_COMPANY_ID}")
    log.info("─" * 60)

    for i, (bdc_name, bdc_id) in enumerate(BDC_IDS.items(), start=1):
        log.info(f"[{i:02d}/{total}] Fetching: {bdc_name} (ID: {bdc_id})")
        result = fetch_bdc_balance(bdc_name, bdc_id)

        record_count = len(result["records"])
        log.info(f"         → {result['status'].upper()} | {record_count} record(s)")

        all_records.extend(result["records"])
        summary.append({
            "bdc_name":     bdc_name,
            "bdc_id":       bdc_id,
            "status":       result["status"],
            "record_count": record_count,
        })

        # Polite delay between requests
        if i < total:
            time.sleep(REQUEST_DELAY)

    log.info("─" * 60)
    log.info(f"Done. Total records collected: {len(all_records)}")

    # ── Save JSON ──
    output = {
        "fetched_at": timestamp,
        "total_bdcs": total,
        "total_records": len(all_records),
        "summary": summary,
        "data": all_records,
    }
    with open(json_out, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    log.info(f"JSON saved → {json_out}")

    # ── Save CSV ──
    if all_records:
        df = pd.DataFrame(all_records)
        # Move BDC metadata columns to front
        meta_cols = ["_bdc_name", "_bdc_id"]
        other_cols = [c for c in df.columns if c not in meta_cols]
        df = df[meta_cols + other_cols]
        df.to_csv(csv_out, index=False, encoding="utf-8-sig")
        log.info(f"CSV saved  → {csv_out}")
    else:
        log.warning("No records to write to CSV.")

    # ── Print summary table ──
    log.info("\n── FETCH SUMMARY ──")
    ok_count      = sum(1 for s in summary if s["status"] == "ok")
    failed_count  = total - ok_count
    log.info(f"  Successful : {ok_count}")
    log.info(f"  Failed     : {failed_count}")
    if failed_count:
        log.info("  Failed BDCs:")
        for s in summary:
            if s["status"] != "ok":
                log.info(f"    - {s['bdc_name']} (ID {s['bdc_id']}) → {s['status']}")


if __name__ == "__main__":
    main()