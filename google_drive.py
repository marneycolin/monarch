import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# "drive.file" = can create/see files the app created (recommended)
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

def get_drive_service():
    creds = None

    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists("client_secret.json"):
                raise FileNotFoundError("client_secret.json not found in repo root")

            flow = InstalledAppFlow.from_client_secrets_file("client_secret.json", SCOPES)

            # Console flow = works even if browser can't auto-open (SSH/headless-friendly)
            try:
                # Preferred on Mac: opens a local callback server and usually launches browser
                creds = flow.run_local_server(port=0, open_browser=True)
            except Exception as e:
                # Fallback: prints a URL you can paste into a browser
                print("Browser-based OAuth failed, falling back to manual URL flow.")
                print("Error was:", e)
                auth_url, _ = flow.authorization_url(prompt="consent")
                print("\nOpen this URL in your browser:\n")
                print(auth_url)
                code = input("\nPaste the authorization code here: ").strip()
                flow.fetch_token(code=code)
                creds = flow.credentials


        with open("token.json", "w") as f:
            f.write(creds.to_json())

    return build("drive", "v3", credentials=creds)


def upload_excel_as_sheet(xlsx_path: str, sheet_name: str, folder_id: str | None = None):
    """
    Uploads an .xlsx and converts it to a Google Sheet.
    If folder_id is provided, puts the file in that Drive folder.
    Returns (file_id, webViewLink).
    """
    service = get_drive_service()

    if not os.path.exists(xlsx_path):
        raise FileNotFoundError(f"{xlsx_path} not found")

    file_metadata = {
        "name": sheet_name,
        "mimeType": "application/vnd.google-apps.spreadsheet",
    }
    if folder_id:
        file_metadata["parents"] = [folder_id]

    media = MediaFileUpload(
        xlsx_path,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        resumable=True,
    )

    created = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id, webViewLink",
    ).execute()

    return created["id"], created.get("webViewLink")
