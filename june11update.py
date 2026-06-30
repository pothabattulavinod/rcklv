import json
import os
import time
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ---------------- Files ----------------

INPUT_FILE = "transactions.json"
OUTPUT_FILE = "transactions.json"

BASE_URL = "https://aepos.ap.gov.in/smartepos/Qcodesearch.jsp?rcno={}"

# ---------------- Tunables ----------------

MAX_WORKERS = 8          # raise/lower depending on how the server tolerates load
REQUEST_TIMEOUT = 25
CARD_RETRY_ATTEMPTS = 3  # retries per-card before giving up on that card only
CHECKPOINT_EVERY = 50    # save progress to disk every N completed cards

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
    pool_connections=MAX_WORKERS * 2,
    pool_maxsize=MAX_WORKERS * 2
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

# ---------------- Save helper ----------------

def save_results(results):
    """Atomic-ish save: write to temp file then replace, so a crash mid-write
    never corrupts transactions.json."""
    tmp_file = OUTPUT_FILE + ".tmp"
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)
    os.replace(tmp_file, OUTPUT_FILE)

# ---------------- Fetch (single attempt) ----------------

def fetch_once(cardno, units):
    r = session.get(
        BASE_URL.format(cardno),
        timeout=REQUEST_TIMEOUT
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

            try:
                rice = float(cols[7])
            except ValueError:
                continue

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

    return transaction

# ---------------- Fetch with per-card retry ----------------

def fetch(card):
    cardno = card["CARDNO"]
    units = int(card["UNITS"])

    last_error = None

    for attempt in range(1, CARD_RETRY_ATTEMPTS + 1):
        try:
            transaction = fetch_once(cardno, units)

            updated = card.copy()
            updated["CURRENT_MONTH_TRANSACTION"] = transaction

            if transaction:
                print(f"{cardno} -> Updated")
            else:
                print(f"{cardno} -> Still Pending")

            return ("ok", updated)

        except Exception as e:
            last_error = e
            if attempt < CARD_RETRY_ATTEMPTS:
                # small jittered backoff before retrying just this card
                time.sleep(1.5 * attempt + random.uniform(0, 1))
                continue

    # All attempts for this card failed - don't kill the whole run,
    # just leave this card untouched (still pending) so it's retried next run.
    print(f"Skipping {cardno} after {CARD_RETRY_ATTEMPTS} failed attempts: {last_error}")
    return ("error", card)

# ---------------- Read Existing File ----------------

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    cards = json.load(f)

results = cards.copy()

# ---------------- Only Pending Cards ----------------

pending_cards = [
    (i, card)
    for i, card in enumerate(cards)
    if card.get("CURRENT_MONTH_TRANSACTION") is None
]

print(f"Pending cards to check: {len(pending_cards)}")

if not pending_cards:
    print("All cards are already updated.")
    raise SystemExit(0)

# ---------------- Process ----------------

ok_count = 0
error_count = 0
completed_since_checkpoint = 0

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

    futures = {
        executor.submit(fetch, card): index
        for index, card in pending_cards
    }

    total = len(futures)

    for count, future in enumerate(as_completed(futures), 1):

        index = futures[future]

        try:
            status, result = future.result()
        except Exception as e:
            # Should not normally happen since fetch() catches internally,
            # but guard against unexpected crashes in a single task.
            print(f"Unexpected task failure at index {index}: {e}")
            status, result = "error", cards[index]

        if status == "ok":
            results[index] = result
            ok_count += 1
        else:
            error_count += 1

        completed_since_checkpoint += 1
        print(f"[{count}/{total}] ok={ok_count} errors={error_count}")

        # Periodically persist progress so a long run isn't all-or-nothing
        if completed_since_checkpoint >= CHECKPOINT_EVERY:
            save_results(results)
            completed_since_checkpoint = 0
            print(f"Checkpoint saved at {count}/{total}")

# ---------------- Final Save ----------------

save_results(results)

done = sum(
    1 for x in results
    if x.get("CURRENT_MONTH_TRANSACTION") is not None
)

pending = len(results) - done

print("\ntransactions.json updated.")
print(f"Done: {done}")
print(f"Pending: {pending}")
print(f"This run -> succeeded: {ok_count}, failed/skipped: {error_count}")

# Only exit non-zero if literally nothing could be fetched, to flag a real
# systemic outage (e.g. site fully down) without discarding partial progress.
if ok_count == 0 and error_count > 0:
    print("\nWarning: no cards succeeded this run - site may be down.")
    raise SystemExit(1)
