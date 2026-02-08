"""
Monarch Money transaction ingestion and export pipeline.

Fetches transactions from Monarch Money API, stores them in PostgreSQL,
and exports to Excel and Google Sheets for analysis.
"""
import os
import json
import asyncio
from datetime import datetime, timedelta, date as date_type

import psycopg2
import pandas as pd
from dotenv import load_dotenv
from monarchmoney import MonarchMoney
from sqlalchemy import create_engine, text
from google_sheets import write_df, clear_tab, get_sheet_link, ensure_tab

load_dotenv(dotenv_path=".env", override=True)

print("CWD:", os.getcwd())
print(".env exists:", os.path.exists(".env"))
print("GOOGLE_SHEET_ID:", repr(os.getenv("GOOGLE_SHEET_ID")))


def _safe_get(d, path, default=None):
    """
    Safely get nested dictionary value.
    path: list of keys, e.g. ["account","displayName"]
    """
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[
