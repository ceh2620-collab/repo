import os
import sys
import json
import base64
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

print("▶️ Script started:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# ============================================================
# 1. 환경변수 읽기 (Railway용)
# ============================================================

PORT = os.environ.get("PORT")
DART_API_KEY = os.environ.get("DART_API_KEY")
GDRIVE_JSON_BASE64 = os.environ.get("GDRIVE_JSON_BASE64")

missing = []
if not PORT: missing.append("PORT")
if not DART_API_KEY: missing.append("DART_API_KEY")
if not GDRIVE_JSON_BASE64: missing.append("GDRIVE_JSON_BASE64")

if missing:
    print("❌ ERROR: Missing environment variables:", missing)
    sys.exit(1)

print("✅ Environment variables loaded")


# ============================================================
# 2. service_account.json 생성 (Base64 복원)
# ============================================================

try:
    decoded = base64.b64decode(GDRIVE_JSON_BASE64)
    with open("service_account.json", "wb") as f:
        f.write(decoded)
    print("✅ Google service_account.json created")
except Exception as e:
    print("❌ ERROR decoding GDRIVE_JSON_BASE64:", e)
    sys.exit(1)


# ============================================================
# 3. 기본 경로 설정
# ============================================================

BASE_PATH = "/data"
DAILY_DIR = f"{BASE_PATH}/daily"
os.makedirs(DAILY_DIR, exist_ok=True)

TODAY = datetime.today().strftime("%Y-%m-%d")
DAILY_FILE = f"{DAILY_DIR}/{TODAY}.xlsx"
SUMMARY_FILE = f"{BASE_PATH}/summary.xlsx"


# ============================================================
# 4. DART 공시 수집
# ============================================================

def get_dart_disclosures(days=30):
    end = datetime.today()
    start = end - timedelta(days=days)

    url = "https://opendart.fss.or.kr/api/list.json"
    params = {
        "crtfc_key": DART_API_KEY,
        "bgn_de": start.strftime("%Y%m%d"),
        "end_de": end.strftime("%Y%m%d"),
        "page_count": 200
    }

    r = requests.get(url, params=params)

    try:
        data = r.json()
    except:
        print("❌ DART JSON Error:", r.text)
        return None

    if data.get("status") != "000":
        print("❌ DART API Error:", data)
        return None

    return pd.DataFrame(data["list"])


df = get_dart_disclosures()
if df is None or df.empty:
    print("❌ No DART data found")
    sys.exit(1)


# ============================================================
# 5. 점수 계산
# ============================================================

DISCLOSURE_SCORE = {
    "공급계약": 40, "매출": 40, "임상": 40,
    "승인": 40, "신규사업": 30, "사업목적": 30,
    "MOU": 10
}

def score_text(t):
    return sum(v for k, v in DISCLOSURE_SCORE.items() if k in t)

SECTOR_MAP = {
    "AI": ["AI", "인공지능", "반도체"],
    "바이오": ["임상", "신약"],
    "전력": ["전력", "인프라"],
    "우주": ["우주", "위성", "발사체"],
}

def detect_sector(text):
    for sector, keys in SECTOR_MAP.items():
        if any(k in text for k in keys):
            return sector
    return "기타"

df["report_nm"] = df["report_nm"].fillna("")
df["공시점수"] = df["report_nm"].apply(score_text)
df["섹터"] = df["report_nm"].apply(detect_sector)
df["총점"] = df["공시점수"] + df["섹터"].apply(lambda x: 60 if x != "기타" else 20)


# ============================================================
# 6. DAILY 저장
# ============================================================

df.to_excel(DAILY_FILE, index=False)
print("📁 Daily saved:", DAILY_FILE)


# ============================================================
# 7. SUMMARY 업데이트
# ============================================================

summary_cols = ["stock_code", "corp_name", "섹터", "총점"]
today_df = df[summary_cols].drop_duplicates("stock_code")
today_df["stock_code"] = today_df["stock_code"].astype(str)
today_df["등장횟수"] = 1
today_df["최근등장일"] = TODAY

if os.path.exists(SUMMARY_FILE):
    old = pd.read_excel(SUMMARY_FILE)
    old["stock_code"] = old["stock_code"].astype(str)

    merged = pd.merge(old, today_df, on="stock_code", how="outer", suffixes=("_old", ""))
    merged["등장횟수"] = merged["등장횟수_old"].fillna(0) + merged["등장횟수"].fillna(0)
    merged["최근등장일"] = TODAY
    merged["섹터"] = merged["섹터"].fillna(merged["섹터_old"])
    merged["총점"] = merged["총점"].fillna(merged["총점_old"])

    summary = merged[["stock_code", "corp_name", "섹터", "총점", "등장횟수", "최근등장일"]]
else:
    summary = today_df

summary.to_excel(SUMMARY_FILE, index=False)
print("📊 Summary saved:", SUMMARY_FILE)


print("====================================================")
print("🎉 TENBAGGER vFINAL — Completed OK")
print("====================================================")
