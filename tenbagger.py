import requests
import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime, timedelta
import json
import base64
import jwt

# ---------------------------------------------------------
# 0. 환경 변수 읽기
# ---------------------------------------------------------
DART_API_KEY = os.getenv("DART_API_KEY", "")
SHEET_ID = os.getenv("GDRIVE_SHEET_ID", "")
SERVICE_EMAIL = os.getenv("GDRIVE_SERVICE_EMAIL", "")
PRIVATE_KEY = os.getenv("GDRIVE_PRIVATE_KEY", "")

if not all([DART_API_KEY, SHEET_ID, SERVICE_EMAIL, PRIVATE_KEY]):
    print("❌ ERROR: 환경변수 누락")
    sys.exit(1)

TODAY = datetime.today().strftime("%Y-%m-%d")

# ---------------------------------------------------------
# 1. 구글 시트 API Access Token 생성
# ---------------------------------------------------------
def get_access_token():
    auth_url = "https://oauth2.googleapis.com/token"
    private_key = PRIVATE_KEY.replace("\\n", "\n")

    payload = {
        "iss": SERVICE_EMAIL,
        "scope": "https://www.googleapis.com/auth/spreadsheets",
        "aud": auth_url,
        "exp": int((datetime.utcnow().timestamp())) + 3600,
        "iat": int((datetime.utcnow().timestamp()))
    }

    signed_jwt = jwt.encode(payload, private_key, algorithm="RS256")

    data = {
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": signed_jwt
    }

    r = requests.post(auth_url, data=data)
    token = r.json().get("access_token")

    if not token:
        print("❌ 구글 액세스 토큰 생성 오류:", r.text)
        sys.exit(1)

    return token

ACCESS_TOKEN = get_access_token()

# ---------------------------------------------------------
# 2. Google Sheet 업데이트 함수
# ---------------------------------------------------------
def write_to_sheet(sheet_range, values):
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/{sheet_range}?valueInputOption=RAW"

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    body = {
        "range": sheet_range,
        "majorDimension": "ROWS",
        "values": values
    }

    r = requests.put(url, headers=headers, json=body)
    if r.status_code not in [200, 201]:
        print("❌ Google Sheet 업데이트 오류:", r.text)

# ---------------------------------------------------------
# 3. DART 데이터 수집
# ---------------------------------------------------------
def get_disclosures(days=30):
    end = datetime.today()
    start = end - timedelta(days=days)

    url = "https://opendart.fss.or.kr/api/list.json"
    params = {
        "crtfc_key": DART_API_KEY,
        "bgn_de": start.strftime("%Y%m%d"),
        "end_de": end.strftime("%Y%m%d"),
        "page_count": 200
    }

    r = requests.get(url, params=params).json()

    if r.get("status") != "000":
        print("❌ DART API 오류:", r)
        return pd.DataFrame()

    return pd.DataFrame(r["list"])

df = get_disclosures()

if df.empty:
    print("❌ DART 데이터 없음")
    sys.exit(1)

# ---------------------------------------------------------
# 4. 점수 계산
# ---------------------------------------------------------
DISCLOSURE_SCORE = {
    "공급계약":40, "매출":40, "임상":40,
    "승인":40, "신규사업":30, "사업목적":30,
    "MOU":10
}

def score_title(t):
    return sum(v for k,v in DISCLOSURE_SCORE.items() if k in t)

df["공시점수"] = df["report_nm"].apply(score_title)

# ---------------------------------------------------------
# 5. Grouping
# ---------------------------------------------------------
def group(row):
    if row["공시점수"] >= 120: return "TOP_A"
    if row["공시점수"] >= 90: return "TOP_B"
    return "TOP_C"

df["그룹"] = df.apply(group, axis=1)

# ---------------------------------------------------------
# 6. Google Sheet 저장
# ---------------------------------------------------------

# HEADER
headers = [["날짜", "종목코드", "종목명", "공시명", "점수", "그룹"]]

# DATA
rows = [
    [
        TODAY,
        row["stock_code"],
        row["corp_name"],
        row["report_nm"],
        row["공시점수"],
        row["그룹"]
    ]
    for _, row in df.iterrows()
]

write_to_sheet("Daily!A1", headers + rows)

print("📊 Google Sheet 업데이트 완료!")
