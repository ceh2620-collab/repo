import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys
import json

# ---------------------------------------------------------
# 0. Output 저장 경로 설정
# ---------------------------------------------------------
BASE_PATH = "/data"
DAILY_PATH = f"{BASE_PATH}/daily"
SUMMARY_PATH = f"{BASE_PATH}/summary.xlsx"
NEWS_PATH = f"{BASE_PATH}/news_{datetime.today().strftime('%Y-%m-%d')}.xlsx"

os.makedirs(DAILY_PATH, exist_ok=True)

TODAY = datetime.today().strftime("%Y-%m-%d")
DAILY_FILE = f"{DAILY_PATH}/{TODAY}.xlsx"

print("▶️ Script started:", TODAY)

# ---------------------------------------------------------
# 1. 환경변수 읽기
# ---------------------------------------------------------
DART_API_KEY = os.environ.get("DART_API_KEY", "").strip()
GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "").strip()
GDRIVE_JSON_STRING = os.environ.get("GDRIVE_JSON", "").strip()

if not DART_API_KEY or len(DART_API_KEY) < 40:
    print("❌ ERROR: DART_API_KEY 없음 또는 잘못됨")
    sys.exit(1)

if not GDRIVE_FOLDER_ID:
    print("❌ ERROR: GDRIVE_FOLDER_ID 없음")
    sys.exit(1)

if not GDRIVE_JSON_STRING:
    print("❌ ERROR: GDRIVE_JSON 없음")
    sys.exit(1)

# ---------------------------------------------------------
# 2. Google Drive 인증 JSON 파일 생성
# ---------------------------------------------------------
try:
    gdrive_json = json.loads(GDRIVE_JSON_STRING)
    with open("service_account.json", "w") as f:
        json.dump(gdrive_json, f)
    print("✅ Google Drive 인증 JSON 생성 완료")
except Exception as e:
    print("❌ SERVICE_ACCOUNT.JSON 생성 오류:", e)
    sys.exit(1)

# ---------------------------------------------------------
# 3. 섹터 매핑
# ---------------------------------------------------------
HTS_SECTOR_MAP = {
    "기계": ["기계", "로봇", "장비"],
    "전기전자": ["전력", "AI", "반도체"],
    "화학": ["신약", "바이오", "소재"],
    "운수장비": ["우주", "항공", "발사체"],
    "건설": ["인프라", "플랜트"]
}

TENBAGGER_SECTOR = {
    "AI 전력 인프라": ["AI전력", "데이터센터전력", "전력"],
    "우주·발사체": ["우주", "위성", "발사체"],
    "양자": ["양자", "양자보안"],
    "차세대 신약": ["신약", "플랫폼"],
}

# ---------------------------------------------------------
# 4. 점수표
# ---------------------------------------------------------
DISCLOSURE_SCORE = {
    "공급계약": 40, "매출": 40, "임상": 40,
    "승인": 40, "신규사업": 30, "사업목적": 30,
    "MOU": 10
}

# ---------------------------------------------------------
# ★ 필수 함수: 점수 계산 / 섹터 감지 ★
# ---------------------------------------------------------
def disclosure_score(text):
    return sum(v for k, v in DISCLOSURE_SCORE.items() if k in text)

def detect_sector(title, sector_map):
    for sector, keys in sector_map.items():
        if any(k in title for k in keys):
            return sector
    return "기타"

# ---------------------------------------------------------
# 5. DART 공시 수집
# ---------------------------------------------------------
def get_disclosures(days=3):
    end = datetime.today()
    start = end - timedelta(days=days)
    url = "https://opendart.fss.or.kr/api/list.json"
    params = {
        "crtfc_key": DART_API_KEY,
        "bgn_de": start.strftime("%Y%m%d"),
        "end_de": end.strftime("%Y%m%d"),
        "page_count": 100
    }
    r = requests.get(url, params=params).json()

    if r.get("status") != "000":
        print("❌ DART ERROR:", r)
        return None

    return pd.DataFrame(r["list"])

df = get_disclosures()
if df is None or df.empty:
    print("❌ 공시 데이터 없음. 종료.")
    sys.exit(1)

df["report_nm"] = df["report_nm"].fillna("")

# ---------------------------------------------------------
# 6. DART 점수 계산
# ---------------------------------------------------------
df["공시점수"] = df["report_nm"].apply(disclosure_score)
df["HTS업종"] = df["report_nm"].apply(lambda x: detect_sector(x, HTS_SECTOR_MAP))
df["텐베거추정섹터"] = df["report_nm"].apply(lambda x: detect_sector(x, TENBAGGER_SECTOR))

df["섹터점수"] = df["텐베거추정섹터"].apply(
    lambda x: 70 if x in ["AI 전력 인프라", "우주·발사체"]
    else 50 if x != "기타" else 20
)

df["총점"] = df["섹터점수"] + df["공시점수"]

df["표시"] = df.apply(
    lambda x: "★" if x["총점"] >= 120 else "☆" if x["총점"] >= 90 else "",
    axis=1
)

# ---------------------------------------------------------
# 7. 그룹화
# ---------------------------------------------------------
def group(row):
    if row["총점"] >= 120:
        return "TOP_A"
    if row["총점"] >= 90:
        return "TOP_B"
    return "TOP_C"

df["그룹"] = df.apply(group, axis=1)

# ---------------------------------------------------------
# 8. Daily 파일 저장
# ---------------------------------------------------------
with pd.ExcelWriter(DAILY_FILE, engine="openpyxl") as writer:
    for g in ["TOP_A", "TOP_B", "TOP_C"]:
        temp = df[df["그룹"] == g]
        if not temp.empty:
            temp.to_excel(writer, sheet_name=g, index=False)

print("✅ DAILY 저장 완료:", DAILY_FILE)

# ---------------------------------------------------------
# 9. Summary 누적 저장
# ---------------------------------------------------------
today_df = df[["stock_code", "corp_name", "HTS업종", "텐베거추정섹터", "표시"]].drop_duplicates()

today_df["등장횟수"] = 1
today_df["최초등장일"] = TODAY
today_df["최근등장일"] = TODAY

if os.path.exists(SUMMARY_PATH):
    old = pd.read_excel(SUMMARY_PATH)
    combined = pd.concat([old, today_df], ignore_index=True)
    summary = combined.groupby("stock_code").agg({
        "corp_name": "first",
        "HTS업종": "last",
        "텐베거추정섹터": "last",
        "표시": "last",
        "등장횟수": "sum",
        "최초등장일": "first",
        "최근등장일": "last"
    }).reset_index()
else:
    summary = today_df

summary.to_excel(SUMMARY_PATH, index=False)
print("✅ SUMMARY 저장 완료:", SUMMARY_PATH)

# ---------------------------------------------------------
# 10. Google Drive 업로드
# ---------------------------------------------------------
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

gauth = GoogleAuth()
gauth.LoadServiceConfigFile("service_account.json")
gauth.ServiceAuth()

drive = GoogleDrive(gauth)

def upload_to_drive(local_file, remote_name):
    f = drive.CreateFile({
        "title": remote_name,
        "parents": [{"id": GDRIVE_FOLDER_ID}]
    })
    f.SetContentFile(local_file)
    f.Upload()
    print("📤 업로드 완료 →", remote_name)

upload_to_drive(DAILY_FILE, f"DAILY_{TODAY}.xlsx")
upload_to_drive(SUMMARY_PATH, "SUMMARY.xlsx")

print("🎉 모든 작업 완료!")
