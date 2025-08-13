import pandas as pd
import time
import json
from datetime import datetime
from dhanhq import DhanContext, dhanhq
import os
import boto3

# === AWS S3 Setup ===
s3_bucket = "nifty-option-chain-data"  # your bucket name
s3 = boto3.client('s3')

#=== Setup ===
client_id = "1107726523"
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzU3NTYyNzE2LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwNzcyNjUyMyJ9.B7hALYZyBZ_bXMJXh_5WnJ_DE-UeRFq3oT07lG7mS1pu8muhEFYzNPDy6RYey7mOu7g9kArAmTAkJ5ZHESiKKQ"

dhan_context = DhanContext(client_id, access_token)
dhan = dhanhq(dhan_context)

under_security_id = 13                # NIFTY
exchange_segment = "IDX_I"           # Index Segment
expiry = "2025-08-14"                # Your target expiry
csv_file = "AIML/Data_fetch/nifty_option_chain_13-08-2025.csv"

print("🚀 Starting Live Option Chain Logger... Press Ctrl+C to stop")

while True:
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n⏰ Fetching at {timestamp} for expiry {expiry}...")

        option_chain = dhan.option_chain(
            under_security_id=under_security_id,
            under_exchange_segment=exchange_segment,
            expiry=expiry
        )

        # If response is string, parse it
        if isinstance(option_chain, str):
            try:
                option_chain = json.loads(option_chain)
            except json.JSONDecodeError:
                print(f"⚠️ Non-JSON API response: {option_chain}")
                time.sleep(60)
                continue

        # Must be dict
        if not isinstance(option_chain, dict):
            print(f"⚠️ Unexpected response type: {type(option_chain)}, content: {option_chain}")
            time.sleep(60)
            continue

        # Debug: print once if 'oc' is missing
        if "data" not in option_chain:
            print(f"⚠️ 'data' field missing. Full response:\n{json.dumps(option_chain, indent=2)}")
            time.sleep(60)
            continue

        # Try both nesting patterns
        raw_oc = None
        if isinstance(option_chain.get("data"), dict):
            raw_oc = option_chain["data"].get("data", {}).get("oc") or option_chain["data"].get("oc")

        if not raw_oc:
            print(f"⚠️ No option chain data found. Response snippet:\n{json.dumps(option_chain.get('data', {}), indent=2)}")
            time.sleep(60)
            continue

        # Process rows
        rows = []
        for strike_str, contracts in raw_oc.items():
            try:
                strike_price = float(strike_str)
            except Exception as e:
                print(f"⚠️ Invalid strike '{strike_str}': {e}")
                continue

            for opt_type in ["ce", "pe"]:
                opt_data = contracts.get(opt_type, {})
                row = {
                    "timestamp": timestamp,
                    "expiry": expiry,
                    "strike_price": strike_price,
                    "type": opt_type.upper(),
                    **opt_data,
                    **opt_data.get("greeks", {})
                }
                row.pop("greeks", None)
                rows.append(row)

        # Save
        if rows:
            df = pd.DataFrame(rows)
            df.to_csv(csv_file, mode='a', header=not os.path.exists(csv_file), index=False)
            print(f"✅ Logged {len(df)} rows to '{csv_file}'")
        else:
            print("⚠️ No rows to log this cycle.")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")

    time.sleep(60)  # Wait for 1 minute
