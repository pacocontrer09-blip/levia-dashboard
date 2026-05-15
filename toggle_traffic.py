#!/usr/bin/env python3
"""Pausa o reanuda la campaña LEVIA_Traffic_Mayo12 (campaign + ad sets)"""
import sys, requests, os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent / ".env")
TOKEN   = os.getenv("META_ACCESS_TOKEN")
AD_ACCT = os.getenv("META_AD_ACCOUNT_ID")
BASE    = "https://graph.facebook.com/v21.0"

TRAFFIC_CAMP_ID = "120244753194110762"
TRAFFIC_ADSET_IDS = [
    "120244753194350762",  # AS1_Reptil_27kg
    "120244753194600762",  # AS2_Limb_PrimeraNoche
    "120244753194950762",  # AS3_Neoct_Autoridad
]

action = sys.argv[1] if len(sys.argv) > 1 else "status"

if action == "status":
    r = requests.get(f"{BASE}/{TRAFFIC_CAMP_ID}",
        params={"fields": "name,status,effective_status", "access_token": TOKEN}, timeout=10).json()
    print(f"Campaña: {r.get('name')} | status={r.get('status')} | effective={r.get('effective_status')}")
    adsets = requests.get(f"{BASE}/{AD_ACCT}/adsets",
        params={"fields": "id,name,status", "campaign_id": TRAFFIC_CAMP_ID, "access_token": TOKEN}, timeout=10).json()
    for a in adsets.get("data", []):
        print(f"  {a['name']}: {a['status']}")
    sys.exit(0)

new_status = "ACTIVE" if action == "resume" else "PAUSED"
icon = "▶️" if new_status == "ACTIVE" else "⏸"

# 1. Toggle campaña
r = requests.post(f"{BASE}/{TRAFFIC_CAMP_ID}",
    data={"status": new_status, "access_token": TOKEN}, timeout=10).json()
ok = "✅" if r.get("success") else f"❌ {r}"
print(f"{ok} {icon} Campaña LEVIA_Traffic_Mayo12 → {new_status}")

# 2. Toggle ad sets
for adset_id in TRAFFIC_ADSET_IDS:
    r = requests.post(f"{BASE}/{adset_id}",
        data={"status": new_status, "access_token": TOKEN}, timeout=10).json()
    ok = "✅" if r.get("success") else f"❌ {r}"
    print(f"{ok} {icon} AdSet {adset_id} → {new_status}")
