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
    
    mm = MonarchMoney()
    await mm.login(email, password, mfa_secret_key=mfa_secret)
    
    # Replace with a real transaction_id from your split transaction query above
    transaction_id = "YOUR_SPLIT_TRANSACTION_ID_HERE"
    
    # Check if this method exists and what it returns
    try:
        details = await mm.get_transaction_details(transaction_id)
        print("Transaction details:")
        print(json.dumps(details, indent=2))
        
        # Check for splits
        if "splits" in details:
            print("\n✅ FOUND SPLITS!")
            print(f"Number of splits: {len(details['splits'])}")
        else:
            print("\n❌ No 'splits' field found")
            
    except AttributeError:
        print("❌ get_transaction_details() method doesn't exist")
        print("Available methods:", [m for m in dir(mm) if not m.startswith('_')])

if __name__ == "__main__":
    asyncio.run(test_transaction_details())