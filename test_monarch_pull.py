import os
import asyncio
from datetime import datetime, timedelta

from dotenv import load_dotenv
from monarchmoney import MonarchMoney

load_dotenv(dotenv_path=".env")

async def main():
    email = os.getenv("MONARCH_EMAIL")
    password = os.getenv("MONARCH_PASSWORD")
    mfa_secret = os.getenv("MONARCH_MFA_SECRET")

    mm = MonarchMoney()
    await mm.login(email, password, mfa_secret_key=mfa_secret)

    start_date = (datetime.now() - timedelta(days=7)).date().isoformat()
    end_date = datetime.now().date().isoformat()

    print("Fetching transactions from", start_date, "to", end_date)
    transactions = await mm.get_transactions(start_date=start_date, end_date=end_date)

    print("transactions type:", type(transactions))

    # If it's a dict, show keys
    if isinstance(transactions, dict):
        print("top-level keys:", list(transactions.keys())[:20])

        # Common patterns: dict of items OR dict with 'transactions'
        if "transactions" in transactions:
            tx = transactions["transactions"]
            print("transactions['transactions'] type:", type(tx))
            if isinstance(tx, list) and tx:
                print("sample transaction keys:", sorted(list(tx[0].keys())))
            elif isinstance(tx, dict) and tx:
                first_key = next(iter(tx.keys()))
                print("sample transaction id:", first_key)
                print("sample transaction keys:", sorted(list(tx[first_key].keys())))
        else:
            # dict keyed by transaction id
            if transactions:
                first_key = next(iter(transactions.keys()))
                print("sample transaction id:", first_key)
                print("sample transaction keys:", sorted(list(transactions[first_key].keys())))
    else:
        # If it's a list-like
        print("len:", len(transactions))
        if transactions:
            first = transactions[0]
            print("sample transaction keys:", sorted(list(first.keys())))

if __name__ == "__main__":
    asyncio.run(main())
