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
        mfa_secret_key=mfa_secret
    )
    
    # Replace with your split transaction ID
    transaction_id = "235102462876472879"  # The Costco one from earlier
    
    # Get the split details
    print("="*80)
    print("TESTING get_transaction_splits():")
    print("="*80)
    
    splits_data = await mm.get_transaction_splits(transaction_id)
    
    print(json.dumps(splits_data, indent=2))
    print("="*80)
    
    # Check if we have splits
    transaction = splits_data.get("getTransaction", {})
    splits = transaction.get("splitTransactions", [])
    
    if splits:
        print(f"\n✅ FOUND {len(splits)} SPLITS!")
        for i, split in enumerate(splits, 1):
            print(f"\nSplit {i}:")
            print(f"  ID: {split.get('id')}")
            print(f"  Amount: ${split.get('amount')}")
            print(f"  Category: {split.get('category', {}).get('name')}")
            print(f"  Merchant: {split.get('merchant', {}).get('name')}")
            print(f"  Notes: {split.get('notes')}")
    else:
        print("\n❌ No splits found")

if __name__ == "__main__":
    asyncio.run(test_transaction_splits())