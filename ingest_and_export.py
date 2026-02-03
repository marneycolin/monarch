import os
import json
import asyncio
from datetime import datetime, timedelta, date as date_type

import psycopg2
import pandas as pd
from dotenv import load_dotenv
from monarchmoney import MonarchMoney

load_dotenv(dotenv_path=".env", override=False)

def _safe_get(d, path, default=None):
    """
    path: list of keys, e.g. ["account","displayName"]
    """
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def _parse_date(s):
    # Monarch returns YYYY-MM-DD
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()

async def fetch_transactions(days_back: int):
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
        msg = str(err)
        return ("401" in msg) or ("Unauthorized" in msg)

    async def fresh_client() -> MonarchMoney:
        # Delete stale session file if present
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

    # Start with fast path: try saved session
    mm = MonarchMoney()
    await mm.login(email, password, mfa_secret_key=mfa_secret)

    async def fetch_page(client: MonarchMoney, off: int):
        return await client.get_transactions(
            limit=limit,
            offset=off,
            start_date=start_date,
            end_date=end_date,
        )


    # First page with 401 recovery (this is the baked-in logic)
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
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    sql = """
    INSERT INTO raw.monarch_transactions (
      transaction_id, txn_date, amount,
      merchant_name, category_name, category_group,
      account_id, account_name, account_owner,
      notes, is_pending, is_transfer,
      created_at, updated_at, raw_json
    )
    VALUES (
      %(transaction_id)s, %(txn_date)s, %(amount)s,
      %(merchant_name)s, %(category_name)s, %(category_group)s,
      %(account_id)s, %(account_name)s, %(account_owner)s,
      %(notes)s, %(is_pending)s, %(is_transfer)s,
      %(created_at)s, %(updated_at)s, %(raw_json)s::jsonb
    )
    ON CONFLICT (transaction_id) DO UPDATE SET
      txn_date       = EXCLUDED.txn_date,
      amount         = EXCLUDED.amount,
      merchant_name  = EXCLUDED.merchant_name,
      category_name  = EXCLUDED.category_name,
      account_id     = EXCLUDED.account_id,
      account_name   = EXCLUDED.account_name,
      notes          = EXCLUDED.notes,
      is_pending     = EXCLUDED.is_pending,
      created_at     = EXCLUDED.created_at,
      updated_at     = EXCLUDED.updated_at,
      is_transfer    = EXCLUDED.is_transfer
      raw_json       = EXCLUDED.raw_json
    ;
    """

    rows = 0
    for t in txs:
        payload = {
            "transaction_id": t.get("id"),
            "txn_date": _parse_date(t.get("date")),
            "amount": t.get("amount"),
            "merchant_name": _safe_get(t, ["merchant", "name"]),
            "category_name": _safe_get(t, ["category", "name"]),
            "category_group": None,          # not present in this payload; we can add later
            "account_id": _safe_get(t, ["account", "id"]),
            "account_name": _safe_get(t, ["account", "displayName"]),
            "account_owner": None,           # we can enrich later from an accounts endpoint
            "notes": t.get("notes"),
            "is_pending": t.get("pending"),
            "is_transfer": t.get("isTransfer") or t.get("is_transfer"),
            "created_at": t.get("createdAt"),
            "updated_at": t.get("updatedAt"),
            "raw_json": json.dumps(t),
        }

        if not payload["transaction_id"] or not payload["txn_date"] or payload["amount"] is None:
            continue  # skip malformed rows

        cur.execute(sql, payload)
        rows += 1

    conn.commit()
    cur.close()
    conn.close()
    return rows

def export_to_excel(db_url: str, out_path: str, start_date: str, end_date: str):
    # Use SQLAlchemy so pandas is happy (no warning)
    from sqlalchemy import create_engine, text

    engine = create_engine(db_url)

    # Raw transactions, aligned to the same window you pulled
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
    FROM raw.monarch_transactions t
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

    # Rollup from Neon view (Option B)
    rollup_query = """
    SELECT *
    FROM mart.monthly_category_rollup
    WHERE month >= date_trunc('month', CAST(:start_date AS date))::date
     AND month <= date_trunc('month', CAST(:end_date AS date))::date
    ORDER BY month DESC, spend DESC;
    """
    rollup_df = pd.read_sql(text(rollup_query), engine, params={"start_date": start_date, "end_date": end_date})

    # (Optional) Monthly summary view
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
    except Exception:
        # If the view doesn't exist yet, just skip it
        summary_df = None

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="transactions", index=False)
        rollup_df.to_excel(writer, sheet_name="rollup_monthly", index=False)
        if summary_df is not None:
            summary_df.to_excel(writer, sheet_name="monthly_summary", index=False)

    engine.dispose()
    return len(df)

async def main():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL missing")

    days_back_str = (os.getenv("DAYS_BACK") or "").strip() 
    days_back = int(days_back_str) if days_back_str else 90 
    out_xlsx = os.getenv("OUT_XLSX", "monarch_transactions.xlsx")

    txs, start_date, end_date = await fetch_transactions(days_back=days_back)
    print(f"Fetched {len(txs)} Monarch txs from {start_date} to {end_date}")

    upserted = upsert_transactions(db_url, txs)
    print(f"Upserted {upserted} rows into raw.monarch_transactions")

    exported = export_to_excel(db_url, out_xlsx, start_date=start_date, end_date=end_date)
    print(f"Exported {exported} rows to {out_xlsx}")

if __name__ == "__main__":
    asyncio.run(main())
