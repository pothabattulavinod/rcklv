import json
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import re

# 1. Fetch JSON from GitHub
json_url = "https://raw.githubusercontent.com/pothabattulavinod/rcklv/refs/heads/main/sa.json"  # <-- replace with your raw GitHub URL 
try:
    response = requests.get(json_url, timeout=10)
    response.raise_for_status()
    data = response.json()
except requests.exceptions.RequestException as e:
    print(f"Failed to fetch JSON from GitHub: {e}")
    exit(1)

output_file = "11sa.json"
total_rcs = len(data)

# 2. Detect current month (e.g., 'october')
current_month = datetime.now().strftime("%B").lower()

# 3. Function to check a single RC
def check_rc(rc_entry):
    rcno = rc_entry.get('CARDNO')
    head_name = rc_entry.get('HEAD OF THE FAMILY', 'Unknown')
    if not rcno:
        return None

    url = f'https://aepos.ap.gov.in/Qcodesearch.jsp?rcno={rcno}'
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/127.0.0.1 Safari/537.36"
    }

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return {"CARDNO": rcno, "HEAD OF THE FAMILY": head_name, "transaction_status": "Unknown"}
    except requests.exceptions.RequestException:
        return {"CARDNO": rcno, "HEAD OF THE FAMILY": head_name, "transaction_status": "Unknown"}

    soup = BeautifulSoup(resp.text, 'html.parser')

    # Combine all tables' text
    tables_text = " ".join([table.get_text(separator='\n', strip=True).lower() for table in soup.find_all('table')])

    # Check for current month & 'FRice (KG)'
    month_pattern = re.search(current_month, tables_text, re.IGNORECASE) or re.search(current_month[:3], tables_text, re.IGNORECASE)
    rice_pattern = re.search(r'frice\s*\(kg\)', tables_text, re.IGNORECASE)

    if month_pattern and rice_pattern:
        status = "Done"
    else:
        status = "Not Done"

    return {"CARDNO": rcno, "HEAD OF THE FAMILY": head_name, "transaction_status": status}

# 4. Process RCs concurrently
transaction_data = []
with ThreadPoolExecutor(max_workers=20) as executor:
    futures = {executor.submit(check_rc, entry): entry.get('CARDNO') for entry in data}
    for i, future in enumerate(as_completed(futures), 1):
        result = future.result()
        if result:
            transaction_data.append(result)
            print(f"Processed {i}/{total_rcs}: {result['CARDNO']} - {result['transaction_status']}")

# 5. Save results
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(transaction_data, f, indent=4, ensure_ascii=False)

print(f"Processing complete. Results saved in '{output_file}'.")
