import requests, pandas as pd, numpy as np
from datetime import datetime, timedelta
import os

# ---------------------------------------------------------
# 0. Output 저장 경로 설정
# ---------------------------------------------------------
BASE_PATH = "/data"      # Railway 임시 디스크
DAILY_PATH = f"{BASE_PATH}/daily"
SUMMARY_PATH = f"{BASE_PATH}/summary.xlsx"

os.makedirs(DAILY_PATH, exist_ok=True)

TODAY = datetime.today().strftime("%Y-%m-%d")
DAILY_FILE = f"{DAILY_PATH}/{TODAY}.xlsx"

# ---------------------------------------------------------
# 1. DART KEY
# ---------------------------------------------------------
DART_API_KEY = os.environ.get("DART_API_KEY")  # Railway 환경변수에서 가져오기

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
# 3. 공시 점수
# ---------------------------------------------------------
DISCLOSURE_SCORE = {
    "공급계약":40, "매출":40, "임상":40,
    "승인":40, "신규사업":30, "사업목적":30,
    "MOU":10
}

# ---------------------------------------------------------
# 4. 공시 데이터 수집 (DART)
# ---------------------------------------------------------
def get_disclosures(days=30):
    end = datetime.today()
    start = end - timedelta(days=days)
    url = "https://opendart.fss.or.kr/api/list.json"
    p = {
        "crtfc_key": DART_API_KEY,
        "bgn_de": start.strftime("%Y%m%d"),
        "end_de": end.strftime("%Y%m%d"),
        "page_count": 200
    }
    r = requests.get(url, params=p).json()
    if r.get("status") != "000":
        print("DART Error:", r)
        return pd.DataFrame()

    return pd.DataFrame(r["list"])

df = get_disclosures()

# ---------------------------------------------------------
# 5. 점수 계산
# ---------------------------------------------------------
def disclosure_score(t):
    return sum(v for k,v in DISCLOSURE_SCORE.items() if k in t)

def detect_sector(title, sector_map):
    for k,keys in sector_map.items():
        if any(x in title for x in keys):
            return k
    return "기타"

df["공시점수"] = df["report_nm"].apply(disclosure_score)
df["HTS업종"] = df["report_nm"].apply(lambda x: detect_sector(x, HTS_SECTOR_MAP))
df["텐베거추정섹터"] = df["report_nm"].apply(lambda x: detect_sector(x, TENBAGGER_SECTOR))

df["섹터점수"] = df["텐베거추정섹터"].apply(
    lambda x: 70 if x in ["AI 전력 인프라","우주·발사체"]
    else 50 if x != "기타" else 20
)

df["총점"] = df["섹터점수"] + df["공시점수"]

df["표시"] = df.apply(
    lambda x: "★" if x["섹터점수"]>=70 and x["총점"]>=120
    else "☆" if x["총점"]>=90 else "",
    axis=1
)

# ---------------------------------------------------------
# 6. 그룹 분류
# ---------------------------------------------------------
def group(row):
    if row["총점"]>=120: return "TOP_A"
    if row["총점"]>=90: return "TOP_B"
    return "TOP_C"

df["그룹"] = df.apply(group, axis=1)

# ---------------------------------------------------------
# 7. DAILY 저장
# ---------------------------------------------------------
with pd.ExcelWriter(DAILY_FILE, engine="openpyxl") as w:
    for g in ["TOP_A","TOP_B","TOP_C"]:
        out = df[df["그룹"]==g].sort_values("총점", ascending=False)
        if not out.empty:
            out.to_excel(w, sheet_name=g, index=False)

# ---------------------------------------------------------
# 8. SUMMARY 누적
# ---------------------------------------------------------
today_df = df[[
    "stock_code","corp_name","HTS업종","텐베거추정섹터","표시"
]].drop_duplicates("stock_code")

today_df["등장횟수"] = 1
today_df["최초등장일"] = TODAY
today_df["최근등장일"] = TODAY

if os.path.exists(SUMMARY_PATH):
    old = pd.read_excel(SUMMARY_PATH)
    merged = pd.merge(
        old, today_df, on="stock_code", how="outer", suffixes=("_old","")
    )
    merged["등장횟수"] = merged["등장횟수_old"].fillna(0) + merged["등장횟수"].fillna(0)
    merged["최초등장일"] = merged["최초등장일_old"].fillna(merged["최초등장일"])
    merged["최근등장일"] = TODAY

    summary = merged[[
        "stock_code","corp_name","HTS업종","텐베거추정섹터",
        "등장횟수","최초등장일","최근등장일","표시"
    ]]
else:
    summary = today_df

summary = summary.sort_values("등장횟수", ascending=False)
summary.to_excel(SUMMARY_PATH, index=False)

print("=================================================")
print("✅ TENBAGGER TRACKER vNEXT 완료")
print("📁 DAILY :", DAILY_FILE)
print("📊 SUMMARY:", SUMMARY_PATH)
print("=================================================")
