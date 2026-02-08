"""
Monarch Money transaction ingestion and export pipeline.

Fetches transactions from Monarch Money API, stores them in PostgreSQL,
and exports to Excel for analysis.
"""
import os
import json
import asyncio
import logging
from datetime import datetime, timedelta, date as date_type
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

import psycopg2
import pandas as pd
from dotenv import load_dotenv
from monarchmoney import MonarchMoney
<<<<<<< HEAD
from google_sheets import write_df, clear_tab, get_sheet_link, ensure_tab

load_dotenv(dotenv_path=".env", override=True)

print("CWD:", os.getcwd())
print(".env exists:", os.path.exists(".env"))
print("GOOGLE_SHEET_ID:", repr(os.getenv("GOOGLE_SHEET_ID")))
=======
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
>>>>>>> 2a1c327d9d499afb78b5f6674bdd11ae06a8dbb7

# Constants
SESSION_DIR = Path(".mm")
SESSION_FILE = SESSION_DIR / "mm_session.pickle"
DEFAULT_DAYS_BACK = 90
DEFAULT_PAGE_LIMIT = 100
MAX_RETRIES = 3


class MonarchAPIError(Exception):
    """Custom exception for Monarch API errors."""
    pass


class MonarchClient:
    """Handles Monarch Money API interactions with session management."""
    
    def __init__(self, email: str, password: str, mfa_secret: str):
        self.email = email
        self.password = password
        self.mfa_secret = mfa_secret
        self.client: Optional[MonarchMoney] = None
        
    async def _create_fresh_client(self) -> MonarchMoney:
        """Create a new client with fresh authentication."""
        # Remove stale session
        if SESSION_FILE.exists():
            SESSION_FILE.unlink()
            logger.info("Removed stale session file")
        
        mm = MonarchMoney()
        await mm.login(
            self.email,
            self.password,
            use_saved_session=False,
            save_session=True,
            mfa_secret_key=self.mfa_secret,
        )
        logger.info("Created fresh Monarch Money session")
        return mm
    
    async def initialize(self):
        """Initialize the client with saved or new session."""
        SESSION_DIR.mkdir(exist_ok=True)
        
        self.client = MonarchMoney()
        try:
            await self.client.login(
                self.email,
                self.password,
                mfa_secret_key=self.mfa_secret
            )
            logger.info("Initialized Monarch Money client with saved session")
        except Exception as e:
            logger.warning(f"Could not use saved session: {e}")
            self.client = await self._create_fresh_client()
    
    @staticmethod
    def _is_unauthorized_error(error: Exception) -> bool:
        """Check if error is due to unauthorized access."""
        msg = str(error).lower()
        return "401" in msg or "unauthorized" in msg
    
    async def _fetch_with_retry(self, fetch_func, *args, **kwargs):
        """Execute fetch function with automatic 401 retry."""
        try:
            return await fetch_func(*args, **kwargs)
        except Exception as e:
            if self._is_unauthorized_error(e):
                logger.warning("Session unauthorized, re-authenticating...")
                self.client = await self._create_fresh_client()
                return await fetch_func(*args, **kwargs)
            raise
    
    async def fetch_transactions(
        self,
        start_date: str,
        end_date: str,
        limit: int = DEFAULT_PAGE_LIMIT
    ) -> List[Dict[str, Any]]:
        """
        Fetch all transactions for date range with pagination.
        
        Args:
            start_date: ISO format date string (YYYY-MM-DD)
            end_date: ISO format date string (YYYY-MM-DD)
            limit: Results per page
            
        Returns:
            List of transaction dictionaries
        """
        if not self.client:
            raise MonarchAPIError("Client not initialized. Call initialize() first.")
        
        all_results = []
        offset = 0
        total_count = None
        
        async def fetch_page(off: int):
            return await self.client.get_transactions(
                limit=limit,
                offset=off,
                start_date=start_date,
                end_date=end_date,
            )
        
        # Fetch first page
        data = await self._fetch_with_retry(fetch_page, offset)
        page = data["allTransactions"]
        total_count = page["totalCount"]
        results = page["results"]
        all_results.extend(results)
        
        logger.info(
            f"Page 1: fetched {len(results)} transactions "
            f"(total available: {total_count})"
        )
        offset += limit
        
        # Fetch remaining pages
        while len(all_results) < total_count:
            data = await self._fetch_with_retry(fetch_page, offset)
            results = data["allTransactions"]["results"]
            
            if not results:  # Safety guard
                logger.warning("Received empty page, stopping pagination")
                break
            
            all_results.extend(results)
            logger.info(
                f"Page {offset // limit + 1}: fetched {len(results)} transactions "
                f"({len(all_results)}/{total_count} total)"
            )
            offset += limit
        
        return all_results


<<<<<<< HEAD
    return all_results, start_date, end_date

def upsert_transactions(db_url: str, txs: list[dict]) -> int:
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    sql = """
    INSERT INTO raw.monarch_transactions (
      transaction_id, 
      txn_date, 
      amount,
      merchant_name, merchant_id, 
      category_name,
      account_id, account_name,
      notes, 
      is_pending, 
      is_transfer,
      created_at, 
      updated_at, 
      raw_json
    )
    VALUES (
      %(transaction_id)s, 
      %(txn_date)s, 
      %(amount)s,
      %(merchant_name)s, %(category_name)s,
      %(account_id)s, %(account_name)s,
      %(notes)s, 
      %(is_pending)s, 
      %(is_transfer)s,
      %(created_at)s,
      %(updated_at)s, 
      %(raw_json)s::jsonb
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
      is_transfer    = EXCLUDED.is_transfer,
      raw_json       = EXCLUDED.raw_json
    ;
=======
class TransactionRepository:
    """Handles database operations for transactions."""
    
    # SQL for upserting transactions
    UPSERT_SQL = """
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
            is_transfer    = EXCLUDED.is_transfer,
            raw_json       = EXCLUDED.raw_json
>>>>>>> 2a1c327d9d499afb78b5f6674bdd11ae06a8dbb7
    """
    
    def __init__(self, db_url: str):
        self.db_url = db_url
    
    @staticmethod
    def _safe_get(d: Dict, path: List[str], default: Any = None) -> Any:
        """Safely get nested dictionary value."""
        current = d
        for key in path:
            if not isinstance(current, dict) or key not in current:
                return default
            current = current[key]
        return current
    
    @staticmethod
    def _parse_date(date_str: Optional[str]) -> Optional[date_type]:
        """Parse ISO date string to date object."""
        if not date_str:
            return None
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    
    def _transform_transaction(self, txn: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Transform API transaction to database format.
        
        Returns None if transaction is invalid.
        """
        transaction_id = txn.get("id")
        txn_date = self._parse_date(txn.get("date"))
        amount = txn.get("amount")
        
        # Skip invalid transactions
        if not transaction_id or not txn_date or amount is None:
            logger.warning(f"Skipping invalid transaction: {txn.get('id', 'unknown')}")
            return None
        
        return {
            "transaction_id": transaction_id,
            "txn_date": txn_date,
            "amount": amount,
            "merchant_name": self._safe_get(txn, ["merchant", "name"]),
            "category_name": self._safe_get(txn, ["category", "name"]),
            "category_group": None,  # Could be enriched later
            "account_id": self._safe_get(txn, ["account", "id"]),
            "account_name": self._safe_get(txn, ["account", "displayName"]),
            "account_owner": None,  # Could be enriched from accounts endpoint
            "notes": txn.get("notes"),
            "is_pending": txn.get("pending"),
            "is_transfer": txn.get("isTransfer") or txn.get("is_transfer"),
            "created_at": txn.get("createdAt"),
            "updated_at": txn.get("updatedAt"),
            "raw_json": json.dumps(txn),
        }
    
    def upsert_transactions(self, transactions: List[Dict[str, Any]]) -> int:
        """
        Insert or update transactions in the database.
        
        Args:
            transactions: List of transaction dictionaries from API
            
        Returns:
            Number of rows upserted
        """
        conn = psycopg2.connect(self.db_url)
        cur = conn.cursor()
        
        rows_upserted = 0
        rows_skipped = 0
        
        try:
            for txn in transactions:
                payload = self._transform_transaction(txn)
                if not payload:
                    rows_skipped += 1
                    continue
                
                cur.execute(self.UPSERT_SQL, payload)
                rows_upserted += 1
            
            conn.commit()
            logger.info(
                f"Upserted {rows_upserted} transactions, skipped {rows_skipped}"
            )
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error upserting transactions: {e}")
            raise
        finally:
            cur.close()
            conn.close()
        
        return rows_upserted


class ExcelExporter:
    """Handles Excel export operations."""
    
    TRANSACTIONS_QUERY = """
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
        ORDER BY t.txn_date DESC, t.updated_at DESC NULLS LAST
    """
    
    ROLLUP_QUERY = """
        SELECT *
        FROM mart.monthly_category_rollup
        WHERE month >= date_trunc('month', CAST(:start_date AS date))::date
          AND month <= date_trunc('month', CAST(:end_date AS date))::date
        ORDER BY month DESC, spend DESC
    """
    
    SUMMARY_QUERY = """
        SELECT *
        FROM mart.monthly_summary
        WHERE month >= date_trunc('month', CAST(:start_date AS date))::date 
          AND month <= date_trunc('month', CAST(:end_date AS date))::date
        ORDER BY month DESC
    """
    
    def __init__(self, db_url: str):
        self.engine = create_engine(db_url)
    
    @staticmethod
    def _fix_timezone_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Remove timezone info from datetime columns for Excel compatibility."""
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                try:
                    df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)
                except Exception:
                    df[col] = (
                        pd.to_datetime(df[col])
                        .dt.tz_convert("UTC")
                        .dt.tz_localize(None)
                    )
        return df
    
    def export(self, output_path: str, start_date: str, end_date: str) -> int:
        """
        Export data to Excel workbook.
        
        Args:
            output_path: Path for output Excel file
            start_date: ISO format date string
            end_date: ISO format date string
            
        Returns:
            Number of transaction rows exported
        """
        params = {"start_date": start_date, "end_date": end_date}
        
        # Fetch transactions
        logger.info("Querying transactions...")
        df_transactions = pd.read_sql(
            text(self.TRANSACTIONS_QUERY),
            self.engine,
            params=params
        )
        df_transactions = self._fix_timezone_columns(df_transactions)
        
        # Fetch rollup
        logger.info("Querying monthly rollup...")
        df_rollup = pd.read_sql(
            text(self.ROLLUP_QUERY),
            self.engine,
            params=params
        )
        
        # Fetch summary (optional)
        df_summary = None
        try:
            logger.info("Querying monthly summary...")
            df_summary = pd.read_sql(
                text(self.SUMMARY_QUERY),
                self.engine,
                params=params
            )
        except Exception as e:
            logger.warning(f"Could not fetch monthly summary: {e}")
        
        # Write to Excel
        logger.info(f"Writing to {output_path}...")
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            df_transactions.to_excel(
                writer,
                sheet_name="transactions",
                index=False
            )
            df_rollup.to_excel(
                writer,
                sheet_name="rollup_monthly",
                index=False
            )
            if df_summary is not None:
                df_summary.to_excel(
                    writer,
                    sheet_name="monthly_summary",
                    index=False
                )
        
        logger.info(f"Exported {len(df_transactions)} transaction rows")
        return len(df_transactions)
    
    def __del__(self):
        """Clean up database connection."""
        if hasattr(self, 'engine'):
            self.engine.dispose()

<<<<<<< HEAD
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="transactions", index=False)
        rollup_df.to_excel(writer, sheet_name="rollup_monthly", index=False)
        if summary_df is not None:
            summary_df.to_excel(writer, sheet_name="monthly_summary", index=False)

    engine.dispose()
    return df,rollup_df,summary_df
=======
>>>>>>> 2a1c327d9d499afb78b5f6674bdd11ae06a8dbb7

async def main():
    """Main pipeline execution."""
    # Load environment
    load_dotenv(dotenv_path=".env", override=False)
    
    # Get configuration
    db_url = os.getenv("DATABASE_URL")
    email = os.getenv("MONARCH_EMAIL")
    password = os.getenv("MONARCH_PASSWORD")
    mfa_secret = os.getenv("MONARCH_MFA_SECRET")
    
    # Validate required config
    if not all([db_url, email, password, mfa_secret]):
        raise ValueError(
            "Missing required environment variables. Check .env file."
        )
    
    days_back_str = os.getenv("DAYS_BACK", "").strip()
    days_back = int(days_back_str) if days_back_str else DEFAULT_DAYS_BACK
    output_file = os.getenv("OUT_XLSX", "monarch_transactions.xlsx")
    
    # Calculate date range
    start_date = (datetime.now() - timedelta(days=days_back)).date().isoformat()
    end_date = datetime.now().date().isoformat()
    
    logger.info(f"Starting pipeline for {start_date} to {end_date}")
    
    try:
        # Fetch transactions
        client = MonarchClient(email, password, mfa_secret)
        await client.initialize()
        
        transactions = await client.fetch_transactions(start_date, end_date)
        logger.info(f"Fetched {len(transactions)} transactions from Monarch")
        
        # Store in database
        repo = TransactionRepository(db_url)
        upserted = repo.upsert_transactions(transactions)
        logger.info(f"Upserted {upserted} rows to database")
        
        # Export to Excel
        exporter = ExcelExporter(db_url)
        exported = exporter.export(output_file, start_date, end_date)
        logger.info(f"Exported {exported} rows to {output_file}")
        
        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise

<<<<<<< HEAD
    days_back_str = (os.getenv("DAYS_BACK") or "").strip()
    days_back = int(days_back_str) if days_back_str else 90

    out_xlsx = os.getenv("OUT_XLSX", "monarch_transactions.xlsx")

    txs, start_date, end_date = await fetch_transactions(days_back=days_back)
    print(f"Fetched {len(txs)} Monarch txs from {start_date} to {end_date}")

    upserted = upsert_transactions(db_url, txs)
    print(f"Upserted {upserted} rows into raw.monarch_transactions")

    tx_df, rollup_df, summary_df = export_to_excel(
        db_url,
        out_xlsx,
        start_date=start_date,
        end_date=end_date,
    )
    print(f"Exported {len(tx_df)} rows to {out_xlsx}")

    # Publish to Google Sheets (stable destination)
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
=======
>>>>>>> 2a1c327d9d499afb78b5f6674bdd11ae06a8dbb7

if __name__ == "__main__":
    asyncio.run(main())
