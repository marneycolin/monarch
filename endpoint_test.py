import os
import asyncio
import json
from dotenv import load_dotenv
from monarchmoney import MonarchMoney

load_dotenv()

async def test_transaction_details():
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
        mfa_secret_key=mfa_secret
    )
    
    transaction_id = "Y235102462876472879"
    
    # Get the FULL raw response
    details = await mm.get_transaction_details(transaction_id)
    
    print("="*80)
    print("FULL RAW JSON:")
    print("="*80)
    print(json.dumps(details, indent=2))
    print("="*80)
    
    # Print all top-level keys
    print("\nTop-level keys:")
    print(list(details.keys()))

if __name__ == "__main__":
    asyncio.run(test_transaction_details())