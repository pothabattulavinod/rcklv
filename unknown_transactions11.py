import json
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# GitHub raw input URL
input_url = "https://raw.githubusercontent.com/pothabattulavinod/rcklv/refs/heads/main/11trns_current.json"
output_file = "11trns_current.json"  # repo-local file (overwritten)

# Load master data from GitHub
response = requests.get(input_url, timeout=20)
response.raise_for_status()
master_data = response.json()

# Try loading existing results if present locally (GitHub Actions will have repo checked out)
try:
    with open(output_file, "r", encoding="utf-8") as f:
        current_results = json.load(f)
except FileNotFoundError:
    current_results = []

# Build lookup for already processed results
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

    # Look for "Transaction Details" tables and check for October transactions
    has_transaction = False
    transaction_tables = [t for t in soup.find_all("table") if "Transaction Details" in t.get_text()]

    for table in transaction_tables:
        for row in table.find_all("tr"):
            cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            row_text = " ".join(cells)

            if ("Oct" in row_text or "October" in row_text or "OCT" in row_text) and len(cells) > 2:
                has_transaction = True
                break
        if has_transaction:
            break

    status = "Done" if has_transaction else "Not Done"
    return {"CARDNO": rcno, "HEAD OF THE FAMILY": head_name, "transaction_status": status}

# Prepare lists: only re-check Unknown or new
to_check = []
final_results = []

for entry in master_data:
    rcno = entry.get("CARDNO")

    if rcno in results_lookup:
        status = results_lookup[rcno]["transaction_status"]

        if status in ["Done", "Not Done"]:
            final_results.append(results_lookup[rcno])  # keep cached result
        else:
            to_check.append(entry)  # re-check Unknown
    else:
        to_check.append(entry)  # new entry

# Run checks concurrently
with ThreadPoolExecutor(max_workers=20) as executor:
    futures = {executor.submit(check_rc, entry): entry.get("CARDNO") for entry in to_check}
    for i, future in enumerate(as_completed(futures), 1):
        result = future.result()
        if result:
            final_results.append(result)
            print(f"Checked {i}/{len(to_check)}: {result['CARDNO']} - {result['transaction_status']}")

# Merge and preserve master order
final_results_lookup = {r["CARDNO"]: r for r in final_results}
ordered_results = [final_results_lookup[e["CARDNO"]] for e in master_data if e.get("CARDNO")]

# Save back to file in repo
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(ordered_results, f, indent=4, ensure_ascii=False)

print(f"Processing complete. Re-checked {len(to_check)} Unknown/new records. Results saved in '{output_file}'.")
