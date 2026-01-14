import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys
import json

# ---------------------------------------------------------
# 0. Output 저장 경로
# ---------------------------------------------------------
BASE_PATH = "/data"
DAILY_PATH = f"{BASE_PATH}/daily"
SUMMARY_PATH = f"{BASE_PATH}/summary.xlsx"

os.makedirs(DAILY_PATH, exist_ok=True)

TODAY = datetime.today().strftime("%Y-%m-%d")
DAILY_FILE = f"{DAILY_PATH}/{TODAY}.xlsx"

# ---------------------------------------------------------
# 1. 환경변수 로딩
# ---------------------------------------------------------
DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "").strip()
GDRIVE_JSON_RAW = os.getenv("GDRIVE_JSON", "").strip()

if not DART_API_KEY:
    print("❌ ERROR: DART_API_KEY 환경변수 없음")
    sys.exit(1)

if len(DART_API_KEY) < 30:
    print("❌ ERROR: DART API KEY 길이 이상:", DART_API_KEY)
    sys.exit(1)

if not GDRIVE_FOLDER_ID:
    print("❌ ERROR: GDRIVE_FOLDER_ID 없음")
    sys.exit(1)

if not GDRIVE_JSON_RAW:
    print("❌ ERROR: GDRIVE_JSON 환경변수 없음")
    sys.exit(1)

# 환경변수 JSON 저장
with open("service_account.json", "w") as f:
    f.write(GDRIVE_JSON_RAW)

# ---------------------------------------------------------
# 2. 섹터 매핑
# ---------------------------------------------------------
HTS_SECTOR_MAP = {
    "기계": ["기계", "로봇", "장비"],
    "전기전자": ["전력", "AI", "반도체"],
    "화학": ["신약", "바이오", "소재"],
    "운수장비": ["우주", "항공", "발사체"],
    "건설": ["인프라", "플랜트"],
}

TEN_SECTORS = {
    "AI 전력 인프라": ["AI전력", "데이터센터", "전력"],
    "우주·발사체": ["우주", "위성", "발사체"],
    "양자": ["양자"],
    "차세대 신약": ["신약", "임상"],
}

# ---------------------------------------------------------
# 3. 공시 점수
# ---------------------------------------------------------
DISCLOSURE_SCORE = {
    "공급계약": 40,
    "매출": 40,
    "임상": 40,
    "승인": 40,
    "신규사업": 30,
    "사업목적": 30,
    "MOU": 10,
}

# ---------------------------------------------------------
# 4. DART 공시 수집
# ---------------------------------------------------------
def get_dart(days=30):
    end = datetime.today()
    start = end - timedelta(days=days)

    url = "https://opendart.fss.or.kr/api/list.json"
    params = {
        "crtfc_key": DART_API_KEY,
        "bgn_de": start.strftime("%Y%m%d"),
        "end_de": end.strftime("%Y%m%d"),
        "page_count": 100,
    }

    r = requests.get(url, params=params)
    try:
        data = r.json()
    except:
        print("❌ JSON 파싱 오류", r.text)
        return None

    if data.get("status") != "000":
        print("❌ DART 오류:", data)
        return None

    return pd.DataFrame(data["list"])

df = get_dart()

if df is None or df.empty:
    print("❌ 공시 데이터 없음")
    sys.exit(1)

# ---------------------------------------------------------
# 5. 뉴스 크롤링(키워드 기반)
# ---------------------------------------------------------
def get_news(keyword, count=10):
    url = f"https://newssearch.naver.com/search.naver?where=news&query={keyword}&sm=tab_opt&sort=1"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    if r.status_code != 200:
        return []
    return r.text[:1500]  # 간단 버전

df["news"] = df["corp_name"].apply(lambda x: get_news(x))

# ---------------------------------------------------------
# 6. 점수 계산
# ---------------------------------------------------------
def score_text(text):
    return sum(v for k, v in DISCLOSURE_SCORE.items() if k in text)

def detect_sector(text, mapping):
    for k, words in mapping.items():
        if any(w in text for w in words):
            return k
    return "기타"

df["report_nm"] = df["report_nm"].fillna("")
df["공시점수"] = df["report_nm"].apply(score_text)
df["HTS업종"] = df["report_nm"].apply(lambda x: detect_sector(x, HTS_SECTOR_MAP))
df["텐베거섹터"] = df["report_nm"].apply(lambda x: detect_sector(x, TEN_SECTORS))

df["섹터점수"] = df["텐베거섹터"].apply(
    lambda x: 70 if x in ["AI 전력 인프라", "우주·발사체"] else 50 if x != "기타" else 20
)

df["총점"] = df["공시점수"] + df["섹터점수"]

df["표시"] = df.apply(
    lambda x: "★" if x["총점"] >= 120 else "☆" if x["총점"] >= 90 else "",
    axis=1,
)

# ---------------------------------------------------------
# 7. 그룹 분류
# ---------------------------------------------------------
def group(x):
    if x >= 120:
        return "TOP_A"
    if x >= 90:
        return "TOP_B"
    return "TOP_C"

df["그룹"] = df["총점"].apply(group)

# ---------------------------------------------------------
# 8. DAILY 저장
# ---------------------------------------------------------
with pd.ExcelWriter(DAILY_FILE, engine="openpyxl") as w:
    for g in ["TOP_A", "TOP_B", "TOP_C"]:
        dfg = df[df["그룹"] == g]
        if not dfg.empty:
            dfg.to_excel(w, sheet_name=g, index=False)

# ---------------------------------------------------------
# 9. SUMMARY 누적
# ---------------------------------------------------------
cols = ["stock_code", "corp_name", "HTS업종", "텐베거섹터", "표시"]
today_df = df[cols].drop_duplicates("stock_code")
today_df["등장횟수"] = 1
today_df["최초등장일"] = TODAY
today_df["최근등장일"] = TODAY

if os.path.exists(SUMMARY_PATH):
    old = pd.read_excel(SUMMARY_PATH)
    old["stock_code"] = old["stock_code"].astype(str)
    today_df["stock_code"] = today_df["stock_code"].astype(str)

    merged = pd.merge(old, today_df, on="stock_code", how="outer", suffixes=("_old", ""))
    merged["등장횟수"] = merged["등장횟수_old"].fillna(0) + merged["등장횟수"].fillna(0)
    merged["최초등장일"] = merged["최초등장일_old"].fillna(merged["최초등장일"])
    merged["최근등장일"] = TODAY

    summary = merged[
        [
            "stock_code",
            "corp_name",
            "HTS업종",
            "텐베거섹터",
            "등장횟수",
            "최초등장일",
            "최근등장일",
            "표시",
        ]
    ]
else:
    summary = today_df

summary.to_excel(SUMMARY_PATH, index=False)

print("📁 DAILY:", DAILY_FILE)
print("📊 SUMMARY:", SUMMARY_PATH)

# ---------------------------------------------------------
# 10. Google Drive 업로드
# ---------------------------------------------------------
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

gauth = GoogleAuth()
gauth.LoadServiceConfigFile("service_account.json")
gauth.ServiceAuth()

drive = GoogleDrive(gauth)

file = drive.CreateFile(
    {"title": f"DAILY_{TODAY}.xlsx", "parents": [{"id": GDRIVE_FOLDER_ID}]}
)
file.SetContentFile(DAILY_FILE)
file.Upload()

print("📤 Google Drive Upload Completed!")
print("==============================================================")
