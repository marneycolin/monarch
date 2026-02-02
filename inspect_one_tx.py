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

    start_date = (datetime.now() - timedelta(days=30)).date().isoformat()
    end_date = datetime.now().date().isoformat()

    data = await mm.get_transactions(start_date=start_date, end_date=end_date)

    results = data["allTransactions"]["results"]
    print("Pulled results:", len(results))

    if not results:
        return

    first = results[0]
    print("Top-level keys on a transaction:")
    print(sorted(list(first.keys())))

    # Also show a couple nested key shapes (safe)
    for k in ["merchant", "category", "account", "labels"]:
        if k in first:
            v = first[k]
            print(f"\n{k} type:", type(v))
            if isinstance(v, dict):
                print(f"{k} keys:", sorted(list(v.keys()))[:30])

if __name__ == "__main__":
    asyncio.run(main())
