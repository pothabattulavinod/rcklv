import json
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# GitHub input & output paths
input_url = "https://raw.githubusercontent.com/pothabattulavinod/rcklv/refs/heads/main/12trns_current.json"
output_file = "12trns_current.json"  # will overwrite locally first

# Load master data from GitHub
response = requests.get(input_url)
response.raise_for_status()
master_data = response.json()

# Try loading existing results (if running multiple times locally)
try:
    with open(output_file, "r", encoding="utf-8") as f:
        current_results = json.load(f)
except FileNotFoundError:
    current_results = []

# Build lookup for already processed
results_lookup = {entry["CARDNO"]: entry for entry in current_results}

# Function to process a single RC
def check_rc(rc_entry):
    rcno = rc_entry.get("CARDNO")
    head_name = rc_entry.get("HEAD OF THE FAMILY", "Unknown")
    if not rcno:
        return None

    url = f"https://aepos.ap.gov.in/Qcodesearch.jsp?rcno={rcno}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return {"CARDNO": rcno, "HEAD OF THE FAMILY": head_name, "transaction_status": "Unknown"}
    except requests.exceptions.RequestException:
        return {"CARDNO": rcno, "HEAD OF THE FAMILY": head_name, "transaction_status": "Unknown"}

    soup = BeautifulSoup(response.text, "html.parser")

    # Check if October 2025 transactions exist
    has_transaction = any(
        "October" in table.get_text(separator="\n", strip=True)
        and "Transaction Details" in table.get_text(separator="\n", strip=True)
        for table in soup.find_all("table")
    )

    status = "Done" if has_transaction else "Not Done"
    return {"CARDNO": rcno, "HEAD OF THE FAMILY": head_name, "transaction_status": status}

# Separate already processed vs pending
to_check = []
final_results = []

for entry in master_data:
    rcno = entry.get("CARDNO")
    if rcno in results_lookup and results_lookup[rcno]["transaction_status"] == "Done":
        final_results.append(results_lookup[rcno])
    else:
        to_check.append(entry)

# Run parallel checks
with ThreadPoolExecutor(max_workers=20) as executor:
    futures = {executor.submit(check_rc, entry): entry.get("CARDNO") for entry in to_check}
    for i, future in enumerate(as_completed(futures), 1):
        result = future.result()
        if result:
            final_results.append(result)
            print(f"Checked {i}/{len(to_check)}: {result['CARDNO']} - {result['transaction_status']}")

# Save merged results in original order
final_results_lookup = {r["CARDNO"]: r for r in final_results}
ordered_results = [final_results_lookup[e["CARDNO"]] for e in master_data if e.get("CARDNO")]

with open(output_file, "w", encoding="utf-8") as f:
    json.dump(ordered_results, f, indent=4, ensure_ascii=False)

print(f"Processing complete. Updated {len(to_check)} records. Results saved in '{output_file}'.")
