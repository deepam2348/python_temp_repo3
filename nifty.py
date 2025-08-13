import pandas as pd
import time
import json
from datetime import datetime
from dhanhq import DhanContext, dhanhq
import os
import boto3

# === AWS S3 Setup ===
s3 = boto3.client('s3')
s3_bucket = "aimldepartment"  # Your S3 bucket name
s3_key = "nifty_option_chain_13-08-2025.csv"  # File name in S3

# === Dhan Setup ===
client_id = "1107726523"
access_token = "YOUR_ACCESS_TOKEN"  # Replace with your valid DhanHQ access token

dhan_context = DhanContext(client_id, access_token)
dhan = dhanhq(dhan_context)

under_security_id = 13                # NIFTY
exchange_segment = "IDX_I"            # Index Segment
expiry = "2025-08-14"                  # Target expiry
csv_file = "nifty_option_chain_13-08-2025.csv"  # Local CSV file name

print("🚀 Starting Live Option Chain Logger with S3 upload... Press Ctrl+C to stop")

while True:
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n⏰ Fetching at {timestamp} for expiry {expiry}...")

        # Fetch option chain
        option_chain = dhan.option_chain(
            under_security_id=under_security_id,
            under_exchange_segment=exchange_segment,
            expiry=expiry
        )

        # If response is string, try to parse JSON
        if isinstance(option_chain, str):
            try:
                option_chain = json.loads(option_chain)
            except json.JSONDecodeError:
                print(f"⚠️ Non-JSON API response: {option_chain}")
                time.sleep(60)
                continue

        # Validate structure
        if not isinstance(option_chain, dict):
            print(f"⚠️ Unexpected response type: {type(option_chain)}")
            time.sleep(60)
            continue

        if "data" not in option_chain:
            print(f"⚠️ 'data' field missing.")
            time.sleep(60)
            continue

        # Extract option chain data
        raw_oc = None
        if isinstance(option_chain.get("data"), dict):
            raw_oc = option_chain["data"].get("data", {}).get("oc") or option_chain["data"].get("oc")

        if not raw_oc:
            print(f"⚠️ No option chain data found.")
            time.sleep(60)
            continue

        # Process each strike and option type
        rows = []
        for strike_str, contracts in raw_oc.items():
            try:
                strike_price = float(strike_str)
            except:
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
                row.pop("greeks", None)  # Remove nested dict
                rows.append(row)

        # Save locally and upload to S3
        if rows:
            df = pd.DataFrame(rows)

            # Append to local CSV
            df.to_csv(csv_file, mode='a', header=not os.path.exists(csv_file), index=False)
            print(f"✅ Logged {len(df)} rows locally to '{csv_file}'")

            # Upload file to S3
            try:
                s3.upload_file(csv_file, s3_bucket, s3_key)
                print(f"📤 Uploaded to S3: s3://{s3_bucket}/{s3_key}")
            except Exception as e:
                print(f"❌ S3 upload failed: {e}")

        else:
            print("⚠️ No rows to log this cycle.")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")

    time.sleep(60)  # Wait for 1 minute
