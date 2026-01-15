import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys
import json

# ---------------------------------------------------------
# 0. 날짜/폴더 설정
# ---------------------------------------------------------
TODAY = datetime.today().strftime("%Y-%m-%d")

BASE_PATH = "/data"
DAILY_PATH = f"{BASE_PATH}/daily"
SUMMARY_PATH = f"{BASE_PATH}/summary.xlsx"

os.makedirs(DAILY_PATH, exist_ok=True)
DAILY_FILE = f"{DAILY_PATH}/{TODAY}.xlsx"

print("▶️ 스크립트 시작:", TODAY)

# ---------------------------------------------------------
# 1. 환경변수 읽기
# ---------------------------------------------------------
DART_API_KEY = os.environ.get("DART_API_KEY", "").strip()
GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "").strip()
GDRIVE_JSON = os.environ.get("GDRIVE_JSON", "").strip()

# --- 필수 환경변수 검증 ---
if len(DART_API_KEY) < 40:
    print("❌ ERROR: DART_API_KEY가 유효하지 않음")
    sys.exit(1)

if not GDRIVE_FOLDER_ID:
    print("❌ ERROR: GDRIVE_FOLDER_ID가 없음")
    sys.exit(1)

if not GDRIVE_JSON:
    print("❌ ERROR: GDRIVE_JSON이 없음")
    sys.exit(1)

# ---------------------------------------------------------
# 2. Google 인증 JSON 생성
# ---------------------------------------------------------
try:
    with open("service_account.json", "w", encoding="utf-8") as f:
        f.write(GDRIVE_JSON)
    print("✅ Google Drive 인증 JSON 생성 완료")
except Exception as e:
    print("❌ GDRIVE_JSON 파일 생성 실패:", e)
    sys.exit(1)

# ---------------------------------------------------------
# 3. 공시 스코어/섹터 매핑
# ---------------------------------------------------------
DISCLOSURE_SCORE = {
    "공급계약": 40, "매출": 40, "임상": 40, "승인": 40,
    "신규사업": 30, "사업목적": 30, "MOU": 10
}

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
    "차세대 신약": ["신약", "플랫폼"]
}

# ---------------------------------------------------------
# 4. DART 데이터 수집
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

    r = requests.get(url, params=params)

    try:
        data = r.json()
    except:
        print("❌ DART JSON 오류:", r.text)
        return None

    if data.get("status") != "000":
        print("❌ DART ERROR:", data)
        return None

    return pd.DataFrame(data["list"])


df = get_disclosures()

if df is None or df.empty:
    print("❌ 공시 데이터 없음. 종료")
    sys.exit(1)

# ---------------------------------------------------------
# 5. 점수 계산 + 섹터 분석
# ---------------------------------------------------------
df["report_nm"] = df["report_nm"].fillna("")

def disclosure_score(title):
    return sum(v for k, v in DISCLOSURE_SCORE.items() if k in title)

def detect_sector(title, sector_map):
    for sector, keywords in sector_map.items():
        if any(kw in title for kw in keywords):
            return sector
    return "기타"

df["공시점수"] = df["report_nm"].apply(disclosure_score)
df["HTS업종"] = df["report_nm"].apply(lambda x: detect_sector(x, HTS_SECTOR_MAP))
df["텐베거추정섹터"] = df["report_nm"].apply(lambda x: detect_sector(x, TENBAGGER_SECTOR))

df["섹터점수"] = df["텐베거추정섹터"].apply(
    lambda x: 70 if x in ["AI 전력 인프라", "우주·발사체"]
    else 50 if x != "기타"
    else 20
)

df["총점"] = df["섹터점수"] + df["공시점수"]
df["표시"] = df.apply(
    lambda x: "★" if x["총점"] >= 120 else ("☆" if x["총점"] >= 90 else ""),
    axis=1
)

# ---------------------------------------------------------
# 6. 그룹 분류
# ---------------------------------------------------------
def group_label(row):
    if row["총점"] >= 120: return "TOP_A"
    if row["총점"] >= 90: return "TOP_B"
    return "TOP_C"

df["그룹"] = df.apply(group_label, axis=1)

# ---------------------------------------------------------
# 7. DAILY 저장
# ---------------------------------------------------------
with pd.ExcelWriter(DAILY_FILE, engine="openpyxl") as w:
    for group in ["TOP_A", "TOP_B", "TOP_C"]:
        part = df[df["그룹"] == group].sort_values("총점", ascending=False)
        if not part.empty:
            part.to_excel(w, sheet_name=group, index=False)

print("📁 DAILY 저장 완료:", DAILY_FILE)

# ---------------------------------------------------------
# 8. SUMMARY 누적
# ---------------------------------------------------------
cols = ["stock_code", "corp_name", "HTS업종", "텐베거추정섹터", "표시"]

today_df = df[cols].copy()
today_df["stock_code"] = today_df["stock_code"].astype(str)  # 병합 오류 해결
today_df = today_df.drop_duplicates("stock_code")

today_df["등장횟수"] = 1
today_df["최초등장일"] = TODAY
today_df["최근등장일"] = TODAY

if os.path.exists(SUMMARY_PATH):
    old = pd.read_excel(SUMMARY_PATH)
    old["stock_code"] = old["stock_code"].astype(str)

    summary = pd.concat([old, today_df], ignore_index=True)
    summary = summary.groupby("stock_code", as_index=False).agg({
        "corp_name": "last",
        "HTS업종": "last",
        "텐베거추정섹터": "last",
        "표시": "last",
        "등장횟수": "sum",
        "최초등장일": "min",
        "최근등장일": "max"
    })

else:
    summary = today_df

summary.to_excel(SUMMARY_PATH, index=False)
print("📊 SUMMARY 저장 완료:", SUMMARY_PATH)

# ---------------------------------------------------------
# 9. Google Drive 업로드 (최신 pydrive2)
# ---------------------------------------------------------
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

gauth = GoogleAuth()
gauth.settings = {
    "client_config_backend": "service",
    "service_config": {
        "client_json_file_path": "service_account.json"
    }
}
gauth.ServiceAuth()

drive = GoogleDrive(gauth)

gfile = drive.CreateFile({
    "title": f"DAILY_{TODAY}.xlsx",
    "parents": [{"id": GDRIVE_FOLDER_ID}]
})

gfile.SetContentFile(DAILY_FILE)
gfile.Upload()

print("📤 Google Drive 업로드 완료!")
print("🎉 모든 작업 성공적으로 완료됨!")
