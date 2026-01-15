import requests
import pandas as pd
import os
import sys
from datetime import datetime, timedelta

# ========== 환경변수 =============
DART_API_KEY = os.getenv("DART_API_KEY", "")
GSHEET_API_KEY = os.getenv("GSHEET_API_KEY", "")
SHEET_ID = os.getenv("GDRIVE_SHEET_ID", "")

if not all([DART_API_KEY, GSHEET_API_KEY, SHEET_ID]):
    print("❌ ERROR: 환경변수 누락")
    sys.exit(1)

TODAY = datetime.today().strftime("%Y-%m-%d")

# ========== Google Sheet 업데이트 ==========
def write_to_sheet(range_name, values):
    url = (
        f"https://sheets.googleapis.com/v4/spreadsheets/"
        f"{SHEET_ID}/values/{range_name}"
        f"?valueInputOption=RAW&key={GSHEET_API_KEY}"
    )

    body = {
        "range": range_name,
        "majorDimension": "ROWS",
        "values": values,
    }

    r = requests.put(url, json=body)
    if r.status_code not in [200, 201]:
        print("❌ Google Sheet 오류:", r.text)
    else:
        print("✅ Google Sheet 업데이트 완료")

# ========== DART 데이터 수집 ==========
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
        print("❌ DART 오류:", r)
        return pd.DataFrame()

    return pd.DataFrame(r["list"])

df = get_disclosures()

if df.empty:
    print("❌ 공시 없음")
    sys.exit(1)

# ========== 점수 계산 ==========
DISCLOSURE_SCORE = {
    "공급계약":40, "매출":40, "임상":40,
    "승인":40, "신규사업":30, "사업목적":30,
    "MOU":10
}

df["공시점수"] = df["report_nm"].apply(
    lambda t: sum(v for k,v in DISCLOSURE_SCORE.items() if k in t)
)

df["그룹"] = df["공시점수"].apply(
    lambda s: "TOP_A" if s >= 120 else "TOP_B" if s >= 90 else "TOP_C"
)

# ========== Google Sheet 저장 ==========
header = [["날짜","종목코드","종목명","공시명","점수","그룹"]]
rows = [
    [TODAY, r.stock_code, r.corp_name, r.report_nm, r["공시점수"], r["그룹"]]
    for _, r in df.iterrows()
]

write_to_sheet("Daily!A1", header + rows)
