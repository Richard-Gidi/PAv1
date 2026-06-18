"""
WHATSAPP SENDER
===============
Send PDF documents to a WhatsApp group.

Default provider: Whapi.Cloud (QR-linked to your own WhatsApp number, can post to
an EXISTING group). One-time setup:
    1. Create a channel at https://whapi.cloud and link your WhatsApp number by QR.
    2. Copy the channel token  -> env WHAPI_TOKEN
    3. Find the group id (ends in '@g.us') with list_whapi_groups() below or the
       dashboard, and set it as env WHATSAPP_GROUP_ID.

Env:
    WHATSAPP_PROVIDER   = "whapi" (default)
    WHAPI_TOKEN         = your channel token
    WHATSAPP_GROUP_ID   = e.g. "120363012345678901@g.us"
    WHAPI_BASE_URL      = "https://gate.whapi.cloud" (default)
"""

import os
import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

WHAPI_BASE_URL = os.getenv("WHAPI_BASE_URL", "https://gate.whapi.cloud")


def list_whapi_groups(token: str = None) -> list:
    """Return [{'id': '...@g.us', 'name': '...'}] for every group the number is in.

    Run this once (e.g. `python whatsapp_sender.py --list-groups`) to discover the
    group id you need for WHATSAPP_GROUP_ID.
    """
    token = token or os.getenv("WHAPI_TOKEN", "")
    r = requests.get(f"{WHAPI_BASE_URL}/groups",
                     headers={"Authorization": f"Bearer {token}",
                              "accept": "application/json"},
                     timeout=60)
    r.raise_for_status()
    data = r.json()
    groups = data.get("groups", data if isinstance(data, list) else [])
    return [{"id": g.get("id"), "name": g.get("name") or g.get("subject")} for g in groups]


def send_document_whapi(file_path: str, caption: str, *, to: str = None,
                        token: str = None, filename: str = None) -> dict:
    """Send one PDF document to a WhatsApp chat/group via Whapi.Cloud."""
    token = token or os.getenv("WHAPI_TOKEN", "")
    to    = to or os.getenv("WHATSAPP_GROUP_ID", "")
    if not token or not to:
        raise RuntimeError("WHAPI_TOKEN and WHATSAPP_GROUP_ID must be set.")

    url = f"{WHAPI_BASE_URL}/messages/document"
    with open(file_path, "rb") as f:
        files = {"media": (filename or os.path.basename(file_path), f, "application/pdf")}
        data  = {"to": to, "caption": caption}
        r = requests.post(url, headers={"Authorization": f"Bearer {token}"},
                          data=data, files=files, timeout=180)
    r.raise_for_status()
    return r.json()


def send_documents_to_group(items, *, provider: str = None) -> list:
    """Send a list of (file_path, caption) tuples. Returns provider responses.

    Each item is sent as its own message so every PDF lands as a separate
    downloadable document in the group.
    """
    provider = provider or os.getenv("WHATSAPP_PROVIDER", "whapi")
    results = []
    for file_path, caption in items:
        if provider == "whapi":
            results.append(send_document_whapi(file_path, caption))
        else:
            raise ValueError(f"Unknown WHATSAPP_PROVIDER: {provider!r}")
    return results


if __name__ == "__main__":
    import sys
    if "--list-groups" in sys.argv:
        for g in list_whapi_groups():
            print(f"{g['id']}\t{g['name']}")
    else:
        print("Usage: python whatsapp_sender.py --list-groups")
