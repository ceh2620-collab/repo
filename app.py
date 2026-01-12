from flask import Flask, jsonify
import requests, pandas as pd, numpy as np
from datetime import datetime, timedelta
import os

app = Flask(__name__)

BASE_PATH = "./data"
DAILY_PATH = f"{BASE_PATH}/daily"
SUMMARY_PATH = f"{BASE_PATH}/summary.xlsx"

os.makedirs(DAILY_PATH, exist_ok=True)
TODAY = datetime.today().strftime("%Y-%m-%d")
DAILY_FILE = f"{DAILY_PATH}/{TODAY}.xlsx"

DART_API_KEY = "1e63717add99f7e948d6388e3209d55235cf3b27"

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

DISCLOSURE_SCORE = {
    "공급계약":40, "매출":40, "임상":40,
    "승인":40, "신규사업":30, "사업목적":30,
    "MOU":10
}

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
        return pd.DataFrame()
    return pd.DataFrame(r["list"])

def disclosure_score(t):
    return sum(v for k,v in DISCLOSURE_SCORE.items() if k in t)

def detect_sector(title, sector_map):
    for k,keys in sector_map.items():
        if any(x in title for x in keys):
            return k
    return "기타"

@app.route("/run")
def run():
    df = get_disclosures()

    if df.empty:
        return jsonify({"error": "DART error"}), 500

    df["공시점수"] = df["report_nm"].apply(disclosure_score)
    df["HTS업종"] = df["report_nm"].apply(lambda x: detect_sector(x, HTS_SECTOR_MAP))
    df["텐베거추정섹터"] = df["report_nm"].apply(lambda x: detect_sector(x, TENBAGGER_SECTOR))

    df["섹터점수"] = df["텐베거추정섹터"].apply(
        lambda x: 70 if x in ["AI 전력 인프라","우주·발사체"]
        else 50 if x != "기타" else 20
    )

    df["총점"] = df["섹터점수"] + df["공시점수"]

    result = df.sort_values("총점", ascending=False).head(30)
    return result.to_json(orient="records", force_ascii=False)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
