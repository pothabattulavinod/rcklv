import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ---------------- Files ----------------

INPUT_FILES = [
    "noutput11.json",
    "noutput12.json",
    "noutput40.json"
]

OUTPUT_FILE = "transactions.json"

BASE_URL = "https://aepos.ap.gov.in/smartepos/Qcodesearch.jsp?rcno={}"

# ---------------- Session ----------------

session = requests.Session()

retry = Retry(
    total=3,
    connect=3,
    read=3,
    backoff_factor=2,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET"]
)

adapter = HTTPAdapter(
    max_retries=retry,
    pool_connections=20,
    pool_maxsize=20
)

session.mount("http://", adapter)
session.mount("https://", adapter)

session.headers.update({
    "User-Agent": "Mozilla/5.0 (GitHub Actions)"
})

# ---------------- Distribution Month ----------------

today = datetime.today()

if today.day >= 26:
    month = today.month + 1
    year = today.year

    if month == 13:
        month = 1
        year += 1
else:
    month = today.month
    year = today.year

month_name = datetime(year, month, 1).strftime("%B")

TARGET = f"{month_name}'{year} Transaction Details"

print("Checking:", TARGET)

# ---------------- Fetch ----------------

def fetch(card):
    cardno = card["CARDNO"]

    try:
        r = session.get(
            BASE_URL.format(cardno),
            timeout=20
        )

        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")

        transaction = None

        for table in soup.find_all("table"):

            if TARGET not in table.get_text(" ", strip=True):
                continue

            rows = table.find_all("tr")

            for row in rows[3:]:

                cols = [
                    td.get_text(" ", strip=True)
                    for td in row.find_all("td")
                ]

                if len(cols) < 8:
                    continue

                rice = float(cols[7])

                units = int(card["UNITS"])
                expected = units * 5

                if rice == expected or rice == 35:
                    status = "Done"
                else:
                    status = "Not Done"

                transaction = {
                    "Member": cols[1],
                    "FPS": cols[2],
                    "Month": cols[3],
                    "Year": cols[4],
                    "Date": cols[5],
                    "Type": cols[6],
                    "Rice(KG)": rice,
                    "Expected(KG)": expected,
                    "Status": status
                }

                break

            break

        return {
            "CARDNO": card["CARDNO"],
            "HEAD OF THE FAMILY": card["HEAD OF THE FAMILY"],
            "UNITS": card["UNITS"],
            "CURRENT_MONTH_TRANSACTION": transaction
        }

    except Exception as e:
        print(f"Server Error: {cardno} -> {e}")
        return None

# ---------------- Read Input Files ----------------

cards = []
order = {}
index = 0

for file in INPUT_FILES:
    with open(file, "r", encoding="utf-8") as f:
        data = json.load(f)

    for card in data:
        cards.append(card)
        order[card["CARDNO"]] = index
        index += 1

print(f"Total cards loaded: {len(cards)}")

# ---------------- Process ----------------

results = []
server_error = False

with ThreadPoolExecutor(max_workers=5) as executor:

    futures = {
        executor.submit(fetch, card): card
        for card in cards
    }

    total = len(futures)

    for i, future in enumerate(as_completed(futures), 1):

        result = future.result()

        if result is None:
            server_error = True
        else:
            results.append(result)

        print(f"[{i}/{total}]")

# ---------------- Save Output ----------------

if server_error:
    print("\nServer not reachable.")
    print(f"{OUTPUT_FILE} NOT modified.")
    raise SystemExit(1)

# Restore original order
results.sort(key=lambda x: order[x["CARDNO"]])

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=4, ensure_ascii=False)

print(f"\n{OUTPUT_FILE} updated successfully.")
