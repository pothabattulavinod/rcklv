import json
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import re

# 1. Fetch JSON from GitHub
json_url = "https://raw.githubusercontent.com/pothabattulavinod/rcklv/refs/heads/main/new.json"
try:
    response = requests.get(json_url, timeout=10)
    response.raise_for_status()
    data = response.json()
except requests.exceptions.RequestException as e:
    print(f"Failed to fetch JSON from GitHub: {e}")
    exit(1)

output_file = "newtrns_current.json"
total_rcs = len(data)

# 2. Detect current month
current_month = datetime.now().strftime("%B").lower()  # e.g., 'october'
allowed_values = ["5.000", "10.000", "15.000", "20.000", "25.000", "30.000", "35.000", "40.000"]

# 3. Function to check one RC
def check_rc(rc_entry):
    rcno = rc_entry.get('CARDNO')
    head_name = rc_entry.get('HEAD OF THE FAMILY', 'Unknown')
    if not rcno:
        return None

    url = f'https://aepos.ap.gov.in/Qcodesearch.jsp?rcno={rcno}'
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.RequestException:
        return {"CARDNO": rcno, "HEAD OF THE FAMILY": head_name, "transaction_status": "Unknown", "Avail.Commodity": None}

    soup = BeautifulSoup(resp.text, 'html.parser')
    tables = soup.find_all('table')

    transaction_found = False
    avail_value = None

    for table in tables:
        table_text = table.get_text(separator=' ', strip=True).lower()

        # Find current month's transaction table
        if current_month in table_text and "transaction details" in table_text:
            # Look for FRice(KG) rows
            rows = table.find_all('tr')
            for row in rows:
                cells = [c.get_text(strip=True) for c in row.find_all(['td', 'th'])]
                if any("frice" in c.lower() for c in cells):
                    continue  # Skip header
                for cell in cells:
                    # Check for valid FRice(KG) values
                    if any(val in cell for val in allowed_values):
                        avail_value = re.search(r'\b\d{1,2}\.000\b', cell)
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

# 4. Process RCs concurrently
transaction_data = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(check_rc, entry): entry.get('CARDNO') for entry in data}
    for i, future in enumerate(as_completed(futures), 1):
        result = future.result()
        if result:
            transaction_data.append(result)
            print(f"Processed {i}/{total_rcs}: {result['CARDNO']} - {result['transaction_status']} ({result['Avail.Commodity']})")

# 5. Save results
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(transaction_data, f, indent=4, ensure_ascii=False)

print(f"\n✅ Processing complete. Results saved in '{output_file}'.")
