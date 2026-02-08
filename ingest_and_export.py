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
      merchant_name,
      merchant_id,
      category_name,
      account_id,
      account_name,
      notes, 
      is_pending, 
      is_transfer,
      created_at, 
      updated_at,
      has_splits, 
      hide_from_reports,
      needs_review,
      reviewed_at,
      raw_json
    )
    VALUES (
      %(transaction_id)s, 
      %(txn_date)s, 
      %(amount)s,
      %(merchant_name)s,
      %(merchant_id)s,
      %(category_name)s,
      %(account_id)s,
      %(account_name)s,
      %(notes)s, 
      %(is_pending)s, 
      %(is_transfer)s,
      %(created_at)s,
      %(updated_at)s, 
      %(has_splits)s, 
      %(hide_from_reports)s,
      %(needs_review)s,
      %(reviewed_at)s,    
      %(raw_json)s::jsonb
    )
    ON CONFLICT (transaction_id) DO UPDATE SET
      txn_date          = EXCLUDED.txn_date,
      amount            = EXCLUDED.amount,
      merchant_name     = EXCLUDED.merchant_name,
      merchant_id       = EXCLUDED.merchant_id,
      category_name     = EXCLUDED.category_name,
      account_id        = EXCLUDED.account_id,
      account_name      = EXCLUDED.account_name,
      notes             = EXCLUDED.notes,
      is_pending        = EXCLUDED.is_pending,
      is_transfer       = EXCLUDED.is_transfer,
      created_at        = EXCLUDED.created_at,
      updated_at        = EXCLUDED.updated_at,
      has_splits        = EXCLUDED.has_splits,
      hide_from_reports = EXCLUDED.hide_from_reports,
      needs_review      = EXCLUDED.needs_review,
      reviewed_at       = EXCLUDED.reviewed_at,
      raw_json          = EXCLUDED.raw_json
    ;
    """

    rows = 0
    for t in txs:
        payload = {
            "transaction_id": t.get("id"),
            "txn_date": _parse_date(t.get("date")),
            "amount": t.get("amount"),
            "merchant_name": _safe_get(t, ["merchant", "name"]),
            "merchant_id": _safe_get(t, ["merchant", "id"]),
            "category_name": _safe_get(t, ["category", "name"]),
            "account_id": _safe_get(t, ["account", "id"]),
            "account_name": _safe_get(t, ["account", "displayName"]),
            "notes": t.get("notes"),
            "is_pending": t.get("pending"),
            "is_transfer": t.get("isTransfer") or t.get("is_transfer"),
            "created_at": t.get("createdAt"),
            "updated_at": t.get("updatedAt"),
            "has_splits": t.get("hasSplits", False),
            "hide_from_reports": t.get("hideFromReports", False),
            "needs_review": t.get("needsReview", False),
            "reviewed_at": t.get("reviewedAt"),
            "raw_json": json.dumps(t),
        }

        # Skip malformed rows
        if not payload["transaction_id"] or not payload["txn_date"] or payload["amount"] is None:
            print(f"Skipping invalid transaction: {t.get('id', 'unknown')}")
            continue

        cur.execute(sql, payload)
        rows += 1

    conn.commit()
    cur.close()
    conn.close()
    return rows


def export_to_excel(db_url: str, out_path: str, start_date: str, end_date: str):
    """
    Export transactions and summaries to Excel workbook.
    
    Args:
        db_url: PostgreSQL connection string
        out_path: Output Excel file path
        start_date: ISO format start date
        end_date: ISO format end date
        
    Returns:
        Tuple of (transactions_df, rollup_df, summary_df)
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
            t.updated_at
        FROM raw.transactions t
        LEFT JOIN mart.account_owner_map m USING (account_id)
        WHERE t.txn_date >= CAST(:start_date AS date)
          AND t.txn_date <= CAST(:end_date AS date)
          AND t.merchant_name IS DISTINCT FROM 'Test Merchant'
        ORDER BY t.txn_date DESC, t.updated_at DESC NULLS LAST;
    """

    df = pd.read_sql(text(tx_query), engine, params={"start_date": start_date, "end_date": end_date})

    # Excel cannot write tz-aware datetimes; strip tz if present
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)
            except Exception:
                df[col] = pd.to_datetime(df[col]).dt.tz_convert("UTC").dt.tz_localize(None)

    # Monthly category rollup
    rollup_query = """
        SELECT *
        FROM mart.monthly_category_rollup
        WHERE month >= date_trunc('month', CAST(:start_date AS date))::date
          AND month <= date_trunc('month', CAST(:end_date AS date))::date
        ORDER BY month DESC, spend DESC;
    """
    rollup_df = pd.read_sql(text(rollup_query), engine, params={"start_date": start_date, "end_date": end_date})

    # Monthly summary (optional)
    summary_df = None
    try:
        summary_query = """
            SELECT *
            FROM mart.monthly_summary
            WHERE month >= date_trunc('month', CAST(:start_date AS date))::date 
              AND month <= date_trunc('month', CAST(:end_date AS date))::date
            ORDER BY month DESC;
        """
        summary_df = pd.read_sql(text(summary_query), engine, params={"start_date": start_date, "end_date": end_date})
    except Exception as e:
        print(f"Could not fetch monthly_summary view: {e}")
        summary_df = None

    # Write to Excel
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="transactions", index=False)
        rollup_df.to_excel(writer, sheet_name="rollup_monthly", index=False)
        if summary_df is not None:
            summary_df.to_excel(writer, sheet_name="monthly_summary", index=False)

    engine.dispose()
    return df, rollup_df, summary_df


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
    tx_df, rollup_df, summary_df = export_to_excel(
        db_url,
        out_xlsx,
        start_date=start_date,
        end_date=end_date,
    )
    print(f"Exported {len(tx_df)} rows to {out_xlsx}")

    # Publish to Google Sheets (if configured)
    sheet_id = (os.getenv("GOOGLE_SHEET_ID") or "").strip()
    if sheet_id:
        print("Publishing to Google Sheets...")

        ensure_tab(sheet_id, "transactions")
        clear_tab(sheet_id, "transactions")
        write_df(sheet_id, "transactions", tx_df)

        ensure_tab(sheet_id, "rollup_monthly")
        clear_tab(sheet_id, "rollup_monthly")
        write_df(sheet_id, "rollup_monthly", rollup_df)

        if summary_df is not None:
            ensure_tab(sheet_id, "monthly_summary")
            clear_tab(sheet_id, "monthly_summary")
            write_df(sheet_id, "monthly_summary", summary_df)

        print("Google Sheet link:", get_sheet_link(sheet_id))
    else:
        print("GOOGLE_SHEET_ID not set; skipping Google Sheets publish.")


if __name__ == "__main__":
    asyncio.run(main())
