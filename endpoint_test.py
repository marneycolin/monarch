import os
import asyncio
import json
from dotenv import load_dotenv
from monarchmoney import MonarchMoney

load_dotenv()

async def test_transaction_splits():
    email = os.getenv("MONARCH_EMAIL")
    password = os.getenv("MONARCH_PASSWORD")
    mfa_secret = os.getenv("MONARCH_MFA_SECRET")
    
    # Delete stale session
    try:
        os.remove(".mm/mm_session.pickle")
    except FileNotFoundError:
        pass
    
    mm = MonarchMoney()
    await mm.login(
        email, 
        password, 
        use_saved_session=False,
        save_session=True,
        mfa_secret_key=mfa_se