import requests, pandas as pd, numpy as np
from datetime import datetime, timedelta
import os, sys, json, base64

print("▶️ 스크립트 시작일:", datetime.today().strftime("%Y-%m-%d"))

# ---------------------------------------------------------
# 0. Output 저장 경로 설정
# ---------------------------------------------------------
BASE_PATH = "/data"      # Railway temp storage
DAILY_PATH = f"{BASE_PATH}/daily"
SUMMARY_PATH = f"{BASE_PATH}/summary.xlsx"

os.makedirs(DAILY_PATH, exist_ok=True)

TODAY = datetime.today().strftime("%Y-%m-%d")
DAILY_FILE = f"{DAILY_PATH}/{TODAY}.xlsx"


# ---------------------------------------------------------
# 1. 환경변수 읽기 (+ 검증)
# ---------------------------------------------------------
DART_API_KEY = os.environ.get("DART_API_KEY", "").strip()
GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "").strip()
GDRIVE_JSON_BASE64 = os.environ.get("GDRIVE_JSON", "").strip()

# DART 검증
if not DART_API_KEY:
    print("❌ ERROR: DART_API_KEY 없음")
    sys.exit(1)

if len(DART_API_KEY) < 40:
    print("❌ ERROR: DART_API_KEY 길이 오류:", DART_API_KEY)
    sys.exit(1)

# Drive Folder ID 검증
if not GDRIVE_FOLDER_ID:
    print("❌ ERROR: GDRIVE_FOLDER_ID 없음")
    sys.exit(1)

# Base64 JSON 검증
if not GDRIVE_JSON_BASE64:
    print("❌ 오류: GDRIVE_JSON 없음")
    sys.exit(1)

# ---------------------------------------------------------
# 1-1. Base64 JSON 디코딩 → service_account.json 생성
# ---------------------------------------------------------
try:
    decoded_json = base64.b64decode(GDRIVE_JSON_BASE64).decode("utf-8")
    with open("service_account.json", "w") as f:
        f.write(decoded_json)
    print("✅ Google Drive 인증 JSON 생성 완료")
except Exception as e:
    print("❌ Base64 디코딩 실패:", e)
    sys.exit(1)


# ---------------------------------------------------------
# 2. HTS/섹터 매핑
# ---------------------------------------------------------
HTS_SECTOR_MAP = {
    "기계": ["기계","로봇","장비"],
    "전기전자": ["전력","AI","반도체"],
    "화학": ["신약","바이오","소재"],
    "운수장비": ["우주","항공","발사체"],
    "건설": ["인프라","플랜트"]
}

TENBAGGER_SECTOR = {
    "AI 전력 인프라": ["AI전력","데이터센터전력","전력"],
    "우주·발사체": ["우주","위성","발사체"],
    "양자": ["양자","양자보안"],
    "차세대 신약": ["신약","플랫폼"],
}


# ---------------------------------------------------------
# 3. 공시 점수표
# ---------------------------------------------------------
DISCLOSURE_SCORE = {
    "공급계약":40, "매출":40, "임상":40,
    "승인":40, "신규사업":30, "사업목적":30,
    "MOU":10
}


# ---------------------------------------------------------
# 4. DART 공시 데이터 수집
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
        print("❌ ERROR: DART 응답 JSON 오류:", r.text)
        return None

    if data.get("status") != "000":
        print("❌ DART ERROR:", data)
        return None

    return pd.DataFrame(data["list"])


df = get_disclosures()
if df is None or df.empty:
    print("❌ 공시 데이터 없음")
    sys.exit(1)


# ---------------------------------------------------------
# 5. 점수 계산 & 섹터 탐지
# ---------------------------------------------------------
def disclosure_score(t):
    return sum(v for k, v in DISCLOSURE_SCORE.items() if k in t)


def detect_sector(title, sector_map):
    for k, keys in sector_map.items():
        if any(x in title for x in keys):
            return k
    return "기타"


df["report_nm"] = df["report_nm"].fillna("")

df["공시점수"] = df["report_nm"].apply(disclosure_score)
df["HTS업종"] = df["report_nm"].apply(lambda x: detect_sector(x, HTS_SECTOR_MAP))
df["텐베거추정섹터"] = df["report_nm"].apply(lambda x: detect_sector(x, TENBAGGER_SECTOR))

df["섹터점수"] = df["텐베거추정섹터"].apply(
    lambda x: 70 if x in ["AI 전력 인프라","우주·발사체"] else
              50 if x != "기타" else 20
)

df["총점"] = df["섹터점수"] + df["공시점수"]

df["표시"] = df.apply(
    lambda x: "★" if x["총점"] >= 120 else ("☆" if x["총점"] >= 90 else ""),
    axis=1
)


# ---------------------------------------------------------
# 6. 그룹 분류
# ---------------------------------------------------------
def group(row):
    if row["총점"] >= 120:
        return "TOP_A"
    if row["총점"] >= 90:
        return "TOP_B"
    return "TOP_C"


df["그룹"] = df.apply(group, axis=1)


# ---------------------------------------------------------
# 7. DAILY 저장
# ---------------------------------------------------------
with pd.ExcelWriter(DAILY_FILE, engine="openpyxl") as w:
    for g in ["TOP_A", "TOP_B", "TOP_C"]:
        temp = df[df["그룹"] == g].sort_values("총점", ascending=False)
        if not temp.empty:
            temp.to_excel(w, sheet_name=g, index=False)


# ---------------------------------------------------------
# 8. SUMMARY 누적
# ---------------------------------------------------------
cols = ["stock_code", "corp_name", "HTS업종", "텐베거추정섹터", "표시"]
today_df = df[cols].drop_duplicates("stock_code")
today_df["등장횟수"] = 1
today_df["최초등장일"] = TODAY
today_df["최근등장일"] = TODAY

if os.path.exists(SUMMARY_PATH):
    old = pd.read_excel(SUMMARY_PATH)
    merged = pd.merge(old, today_df, on="stock_code", how="outer", suffixes=("_old",""))

    merged["등장횟수"] = merged["등장횟수_old"].fillna(0) + merged["등장횟수"].fillna(0)
    merged["최초등장일"] = merged["최초등장일_old"].fillna(merged["최초등장일"])
    merged["최근등장일"] = TODAY

    summary = merged[[
        "stock_code","corp_name","HTS업종","텐베거추정섹터",
        "등장횟수","최초등장일","최근등장일","표시"
    ]]
else:
    summary = today_df

summary.to_excel(SUMMARY_PATH, index=False)

print("=================================================")
print("✅ TENBAGGER 생성 완료")
print("📁 DAILY 파일:", DAILY_FILE)
print("📁 SUMMARY 파일:", SUMMARY_PATH)
print("=================================================")


# ---------------------------------------------------------
# 9. Google Drive 업로드
# ---------------------------------------------------------
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

gauth = GoogleAuth()
gauth.LoadServiceConfigFile("service_account.json")
gauth.ServiceAuth()

drive = GoogleDrive(gauth)

upload_file = drive.CreateFile({
    "title": f"DAILY_{TODAY}.xlsx",
    "parents": [{"id": GDRIVE_FOLDER_ID}]
})

upload_file.SetContentFile(DAILY_FILE)
upload_file.Upload()

print("📤 Google Drive 업로드 완료!")
