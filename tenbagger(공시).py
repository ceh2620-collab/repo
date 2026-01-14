import json
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

# ------------------------------
# Google Drive 인증
# ------------------------------
service_json = json.loads(os.environ["GDRIVE_JSON"])

with open("/tmp/service_account.json", "w") as f:
    json.dump(service_json, f)

gauth = GoogleAuth()
gauth.LoadServiceConfigFile("/tmp/service_account.json")
gauth.ServiceAuth()
drive = GoogleDrive(gauth)

# ------------------------------
# Google Drive에 업로드
# ------------------------------
folder_id = os.environ.get("GDRIVE_FOLDER_ID")

gfile = drive.CreateFile({
    "title": f"DAILY_{TODAY}.xlsx",
    "parents": [{"id": folder_id}]
})

gfile.SetContentFile(DAILY_FILE)
gfile.Upload()

print("📤 Google Drive 업로드 완료:", f"DAILY_{TODAY}.xlsx")
