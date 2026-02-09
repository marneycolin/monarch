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

# Only load .env if it exists (Local dev)
if os.path.exists(".env"):
    load_dotenv(dotenv_path=".env", override=True)
    print("Loaded .env file")
else:
    print("No .env file (using environment variables)")
        

def _safe_get(d, path, default=None):
    """
    Safely get nested dictionary value.
    path: list of keys, e.g. ["account","displayName"]
    """
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _parse_date(s):
    """Parse ISO date string to date object."""
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()


async def fetch_transactions(days_back: int):
    """
    Fetch transactions from Monarch Money API with pagination and retry logic.
    
    Args:
        days_back: Number of days to fetch historical data
        
    Returns:
        Tuple of (transactions list, start_date, end_date)
    """
    os.makedirs(".mm", exist_ok=True)
    email = os.getenv("MONARCH_EMAIL")
    password = os.getenv("MONARCH_PASSWORD")
    mfa_secret = os.getenv("MONARCH_MFA_SECRET")

    if not email or not password:
        raise ValueError("Missing MONARCH_EMAIL or MONARCH_PASSWORD")
    if not mfa_secret:
        raise ValueError("Missing MONARCH_MFA_SECRET")

    start_date = (datetime.now() - timedelta(days=days_back)).date().isoformat()
    end_date = datetime.now().date().isoformat()

    limit = 100
    offset = 0
    all_results = []
    total_count = None

    def is_unauthorized(err: Exception) -> bool:
        """Check if error is 401 unauthorized."""
        msg = str(err)
        return ("401" in msg) or ("Unauthorized" in msg)

    async def fresh_client() -> MonarchMoney:
        """Create fresh authenticated client by removing stale session."""
        try:
            os.remove(".mm/mm_session.pickle")
        except FileNotFoundError:
            pass

        mm_new = MonarchMoney()
        await mm_new.login(
            email,
            password,
            use_saved_session=False,
            save_session=True,
            mfa_secret_key=mfa_secret,
        )
        return mm_new

    # Start with saved session (fast path)
    mm = MonarchMoney()
    await mm.login(email, password, mfa_secret_key=mfa_secret)

    async def fetch_page(client: MonarchMoney, off: int):
        """Fetch single page of transactions."""
        return await client.get_transactions(
            limit=limit,
            offset=off,
            start_date=start_date,
            end_date=end_date,
        )

    # First page with 401 recovery
    try:
        data = await fetch_page(mm, offset)
    except Exception as e:
        if is_unauthorized(e):
            print("Saved session unauthorized; nuking session + logging in fresh...")
            mm = await fresh_client()
            data = await fetch_page(mm, offset)
        else:
            raise

    page = data["allTransactions"]
    total_count = page["totalCount"]
    results = page["results"]

    all_results.extend(results)
    print(f"Pulled page offset={offset} got={len(results)} total_so_far={len(all_results)} total={total_count}")
    offset += limit

    # Remaining pages
    while len(all_results) < total_count:
        try:
            data = await fetch_page(mm, offset)
        except Exception as e:
            if is_unauthorized(e):
                print("Session unauthorized mid-pagination; re-logging in fresh...")
                mm = await fresh_client()
                data = await fetch_page(mm, offset)
            else:
                raise

        results = data["allTransactions"]["results"]
        all_results.extend(results)

        print(
            f"Pulled page offset={offset} got={len(results)} "
            f"total_so_far={len(all_results)} total={total_count}"
        )

        if len(results) == 0:  # safety guard
            break

        offset += limit

    return all_results, start_date, end_date


def upsert_transactions(db_url: str, txs: list[dict]) -> int:
    """
    Insert or update transactions in PostgreSQL database.
    
    Args:
        db_url: PostgreSQL connection string
        txs: List of transaction dictionaries from Monarch API
        
    Returns:
        Number of rows upserted
    """
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    sql = """
    INSERT INTO raw.transactions (
      transaction_id, 
      txn_date, 
      amount,
      account_id,
      account_name,
      merchant_id,
      merchant_name,
      merchant_transaction_count,
      category_id,
      category_name,
      is_pending,
      is_transfer,
      is_split_transaction,
      is_recurring,
      hide_from_reports,
      needs_review,
      review_status,
      reviewed_at,
      plaid_name,
      tags,
      attachments,
      notes,
      created_at, 
      updated_at,
      ingested_at,
      raw_json
    )
    VALUES (
      %(transaction_id)s, 
      %(txn_date)s, 
      %(amount)s,
      %(account_id)s,
      %(account_name)s,
      %(merchant_id)s,
      %(merchant_name)s,
      %(merchant_transaction_count)s,
      %(category_id)s,
      %(category_name)s,
      %(is_pending)s, 
      %(is_transfer)s,
      %(is_split_transaction)s,
      %(is_recurring)s,
      %(hide_from_reports)s,
      %(needs_review)s,
      %(review_status)s,
      %(reviewed_at)s,
      %(plaid_name)s,
      %(tags)s::jsonb,
      %(attachments)s::jsonb,
      %(notes)s,
      %(created_at)s,
      %(updated_at)s,
      NOW(),
      %(raw_json)s::jsonb
    )
    ON CONFLICT (transaction_id) DO UPDATE SET
      txn_date                  = EXCLUDED.txn_date,
      amount                    = EXCLUDED.amount,
      account_id                = EXCLUDED.account_id,
      account_name              = EXCLUDED.account_name,
      merchant_id               = EXCLUDED.merchant_id,
      merchant_name             = EXCLUDED.merchant_name,
      merchant_transaction_count = EXCLUDED.merchant_transaction_count,
      category_id               = EXCLUDED.category_id,
      category_name             = EXCLUDED.category_name,
      is_pending                = EXCLUDED.is_pending,
      is_transfer               = EXCLUDED.is_transfer,
      is_split_transaction      = EXCLUDED.is_split_transaction,
      is_recurring              = EXCLUDED.is_recurring,
      hide_from_reports         = EXCLUDED.hide_from_reports,
      needs_review              = EXCLUDED.needs_review,
      review_status             = EXCLUDED.review_status,
      reviewed_at               = EXCLUDED.reviewed_at,
      plaid_name                = EXCLUDED.plaid_name,
      tags                      = EXCLUDED.tags,
      attachments               = EXCLUDED.attachments,
      notes                     = EXCLUDED.notes,
      created_at                = EXCLUDED.created_at,
      updated_at                = EXCLUDED.updated_at,
      ingested_at               = NOW(),
      raw_json                  = EXCLUDED.raw_json
    ;
    """

    rows = 0
    skipped = 0
    
    for t in txs:
        # Extract tags as JSON string (will be cast to JSONB in SQL)
        tags = t.get("tags", [])
        tags_json = json.dumps(tags) if tags else None
        
        # Extract attachments as JSON string (will be cast to JSONB in SQL)
        attachments = t.get("attachments", [])
        attachments_json = json.dumps(attachments) if attachments else None
        
        payload = {
            "transaction_id": t.get("id"),
            "txn_date": _parse_date(t.get("date")),
            "amount": t.get("amount"),
            "account_id": _safe_get(t, ["account", "id"]),
            "account_name": _safe_get(t, ["account", "displayName"]),
            "merchant_id": _safe_get(t, ["merchant", "id"]),
            "merchant_name": _safe_get(t, ["merchant", "name"]),
            "merchant_transaction_count": _safe_get(t, ["merchant", "transactionsCount"]),
            "category_id": _safe_get(t, ["category", "id"]),
            "category_name": _safe_get(t, ["category", "name"]),
            "is_pending": t.get("pending"),
            "is_transfer": t.get("isTransfer", False),
            "is_split_transaction": t.get("isSplitTransaction", False),
            "is_recurring": t.get("isRecurring", False),
            "hide_from_reports": t.get("hideFromReports", False),
            "needs_review": t.get("needsReview", False),
            "review_status": t.get("reviewStatus"),
            "reviewed_at": t.get("reviewedAt"),
            "plaid_name": t.get("plaidName"),
            "tags": tags_json,
            "attachments": attachments_json,
            "notes": t.get("notes"),
            "created_at": t.get("createdAt"),
            "updated_at": t.get("updatedAt"),
            "raw_json": json.dumps(t),
        }

        # Skip malformed rows
        if not payload["transaction_id"] or not payload["txn_date"] or payload["amount"] is None:
            print(f"Skipping invalid transaction: {t.get('id', 'unknown')}")
            skipped += 1
            continue

        cur.execute(sql, payload)
        rows += 1

    conn.commit()
    cur.close()
    conn.close()
    
    if skipped > 0:
        print(f"Skipped {skipped} invalid transactions")
    
    return rows


def export_to_excel(db_url: str, out_path: str, start_date: str, end_date: str):
    """
    Export transactions and summaries to Excel workbook.
    """
    engine = create_engine(db_url)

    # Raw transactions query
    tx_query = """
        SELECT
            t.txn_date,
            t.amount,
            t.merchant_name,
            t.category_name,
            t.account_name,
            m.owner,
            m.account_class,
            m.account_subtype,
            t.notes,
            t.is_pending,
            t.is_split_transaction,
            t.is_recurring,
            t.needs_review,
            t.review_status,
            t.plaid_name,
            t.updated_at
        FROM raw.transactions t
        LEFT JOIN mart.account_attributes m USING (account_id)
        WHERE t.txn_date >= CAST(:start_date AS date)
          AND t.txn_date <= CAST(:end_date AS date)
          AND t.merchant_name IS DISTINCT FROM 'Test Merchant'
        ORDER BY t.txn_date DESC, t.updated_at DESC NULLS LAST;
    """

    df = pd.read_sql(text(tx_query), engine, params={"start_date": start_date, "end_date": end_date})

    # Colin's monthly spending rollup
    colin_query = """
        SELECT *
        FROM mart.colin_monthly_spend
        WHERE month >= DATE_TRUNC('month', CAST(:start_date AS date))::date
          AND month <= DATE_TRUNC('month', CAST(:end_date AS date))::date
        ORDER BY month DESC, category_name;
    """
    
    colin_df = pd.read_sql(text(colin_query), engine, params={"start_date": start_date, "end_date": end_date})

    # Category attributes (for reference/editing)
    categories_query = """
        SELECT *
        FROM dim.colin_categories
        ORDER BY category_name;
    """
    
    categories_df = pd.read_sql(text(categories_query), engine)m
    # Excel cannot write tz-aware datetimes; strip tz if present
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)
            except Exception:
                df[col] = pd.to_datetime(df[col]).dt.tz_convert("UTC").dt.tz_localize(None)

    # Convert amount columns to numeric
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    colin_df['total_spent'] = pd.to_numeric(colin_df['total_spent'], errors='coerce')

    # Write to Excel
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="transactions", index=False)
        colin_df.to_excel(writer, sheet_name="colin_monthly_spend", index=False)
        categories_df.to_excel(writer, sheet_name="category_attributes", index=False)

    engine.dispose()
    return df, colin_df, categories_df


async def main():
    """Main pipeline execution."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL missing")

    days_back_str = (os.getenv("DAYS_BACK") or "").strip()
    days_back = int(days_back_str) if days_back_str else 90

    out_xlsx = os.getenv("OUT_XLSX", "monarch_transactions.xlsx")

    # Fetch transactions from Monarch API
    txs, start_date, end_date = await fetch_transactions(days_back=days_back)
    print(f"Fetched {len(txs)} Monarch txs from {start_date} to {end_date}")

    # Store in PostgreSQL
    upserted = upsert_transactions(db_url, txs)
    print(f"Upserted {upserted} rows into raw.transactions")

    # Export to Excel
tx_df, colin_df, totals_df, categories_df = export_to_excel(
    db_url,
    out_xlsx,
    start_date=start_date,
    end_date=end_date,
)
print(f"Exported {len(tx_df)} rows to {out_xlsx}")

# Publish to Google Sheets (if configured)
sheet_id = (os.getenv("GOOGLE_SHEET_ID") or "").strip()
if sheet_id:
    try:
        print("Publishing to Google Sheets...")

        ensure_tab(sheet_id, "transactions")
        clear_tab(sheet_id, "transactions")
        write_df(sheet_id, "transactions", tx_df)

        ensure_tab(sheet_id, "colin_monthly_spend")
        clear_tab(sheet_id, "colin_monthly_spend")
        write_df(sheet_id, "colin_monthly_spend", colin_df)

        ensure_tab(sheet_id, "colin_monthly_totals")
        clear_tab(sheet_id, "colin_monthly_totals")
        write_df(sheet_id, "colin_monthly_totals", totals_df)

        ensure_tab(sheet_id, "category_attributes")
        clear_tab(sheet_id, "category_attributes")
        write_df(sheet_id, "category_attributes", categories_df)

        print("Google Sheet link:", get_sheet_link(sheet_id))
    except Exception as e:
        print(f"Google Sheets export failed: {e}")
