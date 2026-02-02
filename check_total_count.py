import os
import asyncio
from datetime import datetime, timedelta

from dotenv import load_dotenv
from monarchmoney import MonarchMoney

load_dotenv(dotenv_path=".env")

async def main():
    days_back = 7
    email = os.getenv("MONARCH_EMAIL")
    password = os.getenv("MONARCH_PASSWORD")
    mfa_secret = os.getenv("MONARCH_MFA_SECRET")

    mm = MonarchMoney()
    await mm.login(email, password, use_saved_session=False, save_session=True, mfa_secret_key=mfa_secret)


    start_date = (datetime.now() - timedelta(days=days_back)).date().isoformat()
    end_date = datetime.now().date().isoformat()

    data = await mm.get_transactions(start_date=start_date, end_date=end_date)

    total = data["allTransactions"]["totalCount"]
    results = data["allTransactions"]["results"]

    print("Date range:", start_date, "to", end_date)
    print("API totalCount:", total)
    print("results returned:", len(results))

if __name__ == "__main__":
    asyncio.run(main())
