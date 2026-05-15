#!/usr/bin/env python3
"""Pausa o reanuda los ad sets de LEVIA_ABO_Launch_Mayo11"""
import sys, requests, os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent / ".env")
TOKEN = os.getenv("META_ACCESS_TOKEN")
AD_ACCT = os.getenv("META_AD_ACCOUNT_ID")
BASE = "https://graph.facebook.com/v21.0"

action = sys.argv[1] if len(sys.argv) > 1 else "status"
new_status = "PAUSED" if action == "pause" else "ACTIVE"

adsets = requests.get(f"{BASE}/{AD_ACCT}/adsets",
    params={"fields": "id,name,status", "access_token": TOKEN}, timeout=15).json()

for a in adsets.get("data", []):
    r = requests.post(f"{BASE}/{a['id']}",
        data={"status": new_status, "access_token": TOKEN}, timeout=10).json()
    icon = "⏸" if new_status == "PAUSED" else "▶️"
    ok = "✅" if r.get("success") else f"❌ {r}"
    print(f"{ok} {icon} {a['name']} → {new_status}")
