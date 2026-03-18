#!/usr/bin/env python3
"""
gmail_auth.py — One-time Gmail OAuth2 setup helper
====================================================
Run this LOCALLY (not in CI) to generate the token.json that gets stored
as the GMAIL_TOKEN_JSON GitHub secret.

STEPS:
  1. Go to https://console.cloud.google.com
  2. Select (or create) a project
  3. APIs & Services → Library → search "Gmail API" → Enable
  4. APIs & Services → Credentials → Create Credentials → OAuth 2.0 Client ID
       Application type: Desktop app
       Name: law-job-alerts (anything)
  5. Download the JSON → save as  credentials.json  in this directory
  6. Run:  python gmail_auth.py
  7. A browser window opens — sign in with your Gmail account and approve access
  8. token.json is written in this directory
  9. Copy its contents into a GitHub secret named  GMAIL_TOKEN_JSON
       Settings → Secrets and variables → Actions → New repository secret

After that, main.py will automatically read Gmail every run.
"""

import json
import os
from pathlib import Path

try:
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
except ImportError:
    print("Install dependencies first:")
    print("  pip install google-auth-oauthlib google-auth-httplib2 google-api-python-client")
    raise SystemExit(1)

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
CREDENTIALS_FILE = Path("credentials.json")
TOKEN_FILE        = Path("token.json")


def main():
    if not CREDENTIALS_FILE.exists():
        print(f"ERROR: {CREDENTIALS_FILE} not found.")
        print("Download it from Google Cloud Console → APIs & Services → Credentials.")
        raise SystemExit(1)

    creds = None
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_FILE), SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())

    print(f"\n✓ token.json written to {TOKEN_FILE.resolve()}")
    print("\nNow copy the contents below into GitHub secret  GMAIL_TOKEN_JSON:\n")
    print("─" * 60)
    print(TOKEN_FILE.read_text())
    print("─" * 60)
    print("\nSettings → Secrets and variables → Actions → New repository secret")
    print("  Name:  GMAIL_TOKEN_JSON")
    print("  Value: (paste the JSON above)")


if __name__ == "__main__":
    main()
