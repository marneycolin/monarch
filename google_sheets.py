# google_sheets.py
import pandas as pd
import os
import re
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def _get_creds():
    creds = None
    token_path = "token.json"
    client_secret_path = "client_secret.json"

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(token_path, "w") as f:
            f.write(creds.to_json())
        return creds

    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, SCOPES)
        creds = flow.run_local_server(port=0)
        with open(token_path, "w") as f:
            f.write(creds.to_json())

    return creds

def _get_sheets_service():
    creds = _get_creds()
    return build("sheets", "v4", credentials=creds)

def _sanitize_title(title: str) -> str:
    title = re.sub(r'[:\\/?*\[\]]', ' ', title).strip()
    return title[:100] if len(title) > 100 else title

def ensure_tab(spreadsheet_id: str, title: str) -> int:
    svc = _get_sheets_service()
    title = _sanitize_title(title)

    ss = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sh in ss.get("sheets", []):
        props = sh.get("properties", {})
        if props.get("title") == title:
            return props["sheetId"]

    req = {"requests": [{"addSheet": {"properties": {"title": title}}}]}
    resp = svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=req).execute()
    return resp["replies"][0]["addSheet"]["properties"]["sheetId"]

def clear_tab(spreadsheet_id: str, tab_title: str):
    svc = _get_sheets_service()
    tab_title = _sanitize_title(tab_title)
    ensure_tab(spreadsheet_id, tab_title)
    svc.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_title}!A:ZZ",
        body={}
    ).execute()

def write_df(spreadsheet_id: str, tab_title: str, df):
    svc = _get_sheets_service()
    tab_title = _sanitize_title(tab_title)

    ensure_tab(spreadsheet_id, tab_title)

    # Convert NaN to empty string but keep types
    df2 = df.copy()
    
    # Build values preserving types
    values = [list(df2.columns)]  # Header row
    
    for _, row in df2.iterrows():
        row_values = []
        for val in row:
            if pd.isna(val):
                row_values.append("")
            elif isinstance(val, (int, float)):
                row_values.append(val)  # Keep as number
            else:
                row_values.append(str(val))  # Convert to string
        values.append(row_values)

    svc.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_title}!A1",
        valueInputOption="USER_ENTERED",  # Changed from "RAW" - this interprets numbers as numbers
        body={"values": values},
    ).execute()

def get_sheet_link(spreadsheet_id: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
