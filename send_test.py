"""
SEND TEST — verify WhatsApp delivery before wiring up the daily job.

Steps:
  1. Put WHAPI_TOKEN in a .env file (copy from .env.example).
  2. Build a sample PDF:        python daily_report_job.py --self-test
  3. Send it to your number:    python send_test.py 233550993900
     ...or to a group id:       python send_test.py 120363xxxxxxxx@g.us
     (no argument -> uses WHATSAPP_GROUP_ID from .env)

Recipient format:
  * personal number  -> international digits only, no '+'   e.g. 233550993900
  * group            -> the id ending in @g.us  (get it via: python whatsapp_sender.py --list-groups)
"""
import os
import sys
import glob

from whatsapp_sender import send_document_whapi


def main():
    to = sys.argv[1] if len(sys.argv) > 1 else os.getenv("WHATSAPP_GROUP_ID")
    if not to:
        print("Recipient needed: pass a number/group id, or set WHATSAPP_GROUP_ID in .env")
        return 1

    if len(sys.argv) > 2:
        pdf = sys.argv[2]
    else:
        pdfs = sorted(glob.glob("report_output/*.pdf"), key=os.path.getmtime)
        if not pdfs:
            print("No PDF found. First run:  python daily_report_job.py --self-test")
            return 1
        pdf = pdfs[-1]

    print(f"Sending {pdf}  ->  {to}")
    try:
        resp = send_document_whapi(pdf, "✅ Test report from the NPA bot", to=to)
        print("✅ Sent. Provider response:", resp)
        return 0
    except Exception as exc:
        print("❌ Send failed:", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
