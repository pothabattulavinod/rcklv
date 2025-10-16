import json
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import re
import os

# Input and output file paths
input_file = "11trns_current.json"
output_file = "11trns_current.json"

# Load previous results
if not os.path.exists(input_file):
    print(f"❌ File '{input_file}' not found.")
    exit(1)

with open(input_file, "r", encoding="utf-8") as f:
    previous_data = json.load(f)

# Filter only RCs with "Not Done"
data = [entry for entry in previous_data if entry.get("transaction_status") == "Not Done"]
total_rcs = len(data)

if not data:
    print("✅ All RCs already marked as 'Done'. Nothing to check.")
    exit(0)

print(f"🔍 Rechecking {total_rcs} RCs marked as 'Not Done'...\n")

# Detect current month (in lowercase)
current_month = datetime.now().strftime("%B").lower()
allowed_values = ["5.000", "10.000", "15.000", "20.000", "25.000", "30.000", "35.000", "40.000"]

def check_rc(rc_entry):
    rcno = rc_entry.get("CARDNO")
    head_name = rc_entry.get("HEAD OF THE FAMILY", "Unknown")

    url = f"https://aepos.ap.gov.in/Qcodesearch.jsp?rcno={rcno}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.RequestException:
        return {
            "CARDNO": rcno,
            "HEAD OF THE FAMILY": head_name,
            "transaction_status": "Unknown",
            "Avail.Commodity": None
        }

    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table")

    transaction_found = False
    avail_value = None

    for table in tables:
        table_text = table.get_text(separator=" ", strip=True).lower()

        if current_month in table_text and "transaction details" in table_text:
            rows = table.find_all("tr")
            for row in rows:
                cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
                if any("frice" in c.lower() for c in cells):
                    continue
                for cell in cells:
                    if any(val in cell for val in allowed_values):
                        avail_value = re.search(r"\b\d{1,2}\.000\b", cell)
                        if avail_value:
                            avail_value = avail_value.group()
                            transaction_found = True
                            break
                if transaction_found:
                    break

    status = "Done" if transaction_found else "Not Done"
    return {
        "CARDNO": rcno,
        "HEAD OF THE FAMILY": head_name,
        "transaction_status": status,
        "Avail.Commodity": avail_value if transaction_found else None
    }

# Process RCs concurrently
transaction_data = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(check_rc, entry): entry.get("CARDNO") for entry in data}
    for i, future in enumerate(as_completed(futures), 1):
        result = future.result()
        if result:
            transaction_data.append(result)
            print(f"Processed {i}/{total_rcs}: {result['CARDNO']} - {result['transaction_status']} ({result['Avail.Commodity']})")

# ✅ Only update entries that were "Not Done"
updated_data = []

# Create a dictionary for quick lookup of updated RCs
updated_map = {entry["CARDNO"]: entry for entry in transaction_data}

for entry in previous_data:
    rcno = entry.get("CARDNO")
    if rcno in updated_map:  # Replace only if it was rechecked
        updated_data.append(updated_map[rcno])
    else:
        updated_data.append(entry)  # Keep original if not rechecked

# Save the updated JSON
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(updated_data, f, indent=4, ensure_ascii=False)

print(f"\n✅ Recheck complete. Updated results saved in '{output_file}'. Only 'Not Done' entries were modified.")
