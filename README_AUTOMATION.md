# Automated Daily Reports → WhatsApp (3:30 AM)

This sends your **BDC Balance** and **Daily Loadings** PDFs to a WhatsApp group
every day at 03:30 Accra time, with no manual steps.

## Why it can't run inside your Streamlit app
Streamlit Community Cloud has no scheduler and the app sleeps when nobody is
viewing it, so a 3:30 AM job can't live there. The job runs as a **GitHub
Actions scheduled workflow** instead — free, and since Accra is UTC+0 the cron
`30 3 * * *` fires at exactly 3:30 AM your time.

## Files
| File | Purpose |
|------|---------|
| `bdc_report.py` | PDF builders (you already have this) |
| `npa_core.py` | Headless fetch + parse (your app's logic, Streamlit removed) |
| `whatsapp_sender.py` | Sends PDFs to the group via Whapi.Cloud |
| `daily_report_job.py` | Orchestrates fetch → build → send |
| `requirements-job.txt` | Dependencies for the job |
| `.github/workflows/daily-report.yml` | The 3:30 AM schedule |
| `.env.example` | Template of all settings |

## One-time setup

### 1. Verify it builds (no network needed)
```bash
pip install -r requirements-job.txt
python daily_report_job.py --self-test
```
This writes two PDFs to `report_output/` from dummy data — proves the wiring works.

### 2. Connect WhatsApp (Whapi.Cloud)
The official Meta Cloud API can only post to groups it *created*, so to reach your
**existing** group the simplest route is a QR-linked provider:
1. Sign up at https://whapi.cloud and create a channel.
2. Link your WhatsApp number by scanning the QR (like WhatsApp Web). Use a number
   that is already a member of the target group.
3. Copy the **channel token** → this is `WHAPI_TOKEN`.
4. Find the group id:
   ```bash
   WHAPI_TOKEN=xxxx python whatsapp_sender.py --list-groups
   ```
   Copy the id ending in `@g.us` → this is `WHATSAPP_GROUP_ID`.

### 3. Try a real run locally
Copy `.env.example` to `.env`, fill it in, then:
```bash
python daily_report_job.py --dry-run   # fetches live data, builds PDFs, does NOT send
python daily_report_job.py             # the real thing — fetches, builds, sends
```

### 4. Schedule it on GitHub
1. Put all the files in a GitHub repo (can be the same repo as your app).
2. In the repo: **Settings → Secrets and variables → Actions → New repository secret**,
   and add the values from your `.env`:
   - `NPA_USER_ID`, `NPA_COMPANY_ID`, `NPA_APP_ID`, `NPA_ITS_FROM_PERSOL`
   - `OMC_NAME`
   - `WHATSAPP_PROVIDER` (= `whapi`), `WHAPI_TOKEN`, `WHATSAPP_GROUP_ID`
   - optional: `REPORT_FETCH_MODE`, `REPORT_LOADINGS_DAYS_BACK`
3. The workflow runs daily at 03:30 UTC. Test it now via **Actions → Daily NPA
   Report → Run workflow**. The generated PDFs are also saved as run artifacts.

## single vs per_bdc mode
- `REPORT_FETCH_MODE=single` (default): one consolidated API call per report using
  `NPA_USER_ID`. Fastest, fewest secrets. Use this first and check coverage.
- `REPORT_FETCH_MODE=per_bdc`: loops every `BDC_USER_*` credential and aggregates,
  matching your app's "Per-BDC Aggregation". If you need this, store your whole
  `.env` (including all `BDC_USER_*` lines) as a single GitHub secret named
  `DOTENV` — the workflow writes it to `.env` before running.

## Notes
- The **balance** report is dated *today* (a live snapshot); the **loadings**
  report covers *yesterday* by default (`REPORT_LOADINGS_DAYS_BACK=1`), matching
  how your sample reports were dated.
- GitHub disables scheduled workflows after ~60 days with no repo activity — an
  occasional commit (or a monthly manual run) keeps it alive.
- GitHub's cron is best-effort and can start a few minutes late under load; for
  second-precise timing use a small always-on VM with system `cron` instead
  (same `python daily_report_job.py` command).
- Keep the linked phone's WhatsApp opened occasionally so Whapi's session stays
  valid; avoid high-volume sending to reduce any ban risk.
