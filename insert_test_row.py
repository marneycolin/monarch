import os
from dotenv import load_dotenv
import psycopg2
import json
from datetime import date

load_dotenv(dotenv_path=".env")

db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise ValueError("DATABASE_URL missing")

conn = psycopg2.connect(db_url)
cur = conn.cursor()

cur.execute(
    """
    INSERT INTO raw.monarch_transactions (
      transaction_id, txn_date, amount, merchant_name, category_name,
      category_group, account_id, account_name, account_owner, notes,
      is_pending, is_transfer, created_at, updated_at, raw_json
    )
    VALUES (
      %s, %s, %s, %s, %s,
      %s, %s, %s, %s, %s,
      %s, %s, %s, %s, %s::jsonb
    )
    ON CONFLICT (transaction_id) DO UPDATE SET
      amount = EXCLUDED.amount,
      updated_at = EXCLUDED.updated_at,
      raw_json = EXCLUDED.raw_json
    ;
    """,
    (
        "test_txn_001",
        date.today().isoformat(),
        -12.34,
        "Test Merchant",
        "Test Category",
        "Test Group",
        "acct_test_001",
        "Test Account",
        "Test Owner",
        "hello world",
        False,
        False,
        None,
        None,
        json.dumps({"source": "insert_test_row.py"}),
    ),
)

conn.commit()

cur.execute("SELECT transaction_id, txn_date, amount FROM raw.monarch_transactions WHERE transaction_id = %s;", ("test_txn_001",))
print("Inserted:", cur.fetchone())

cur.close()
conn.close()
