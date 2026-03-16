import json
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options

FPS_ID = "0428003"
MONTH = "3"
YEAR = "2026"

# Chrome options (needed for GitHub Actions)
options = Options()
options.binary_location = "/usr/bin/google-chrome"
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")

# Start browser
driver = webdriver.Chrome(options=options)

# Open FPS Stock page
driver.get("https://aepos.ap.gov.in/FPS_Stock")

time.sleep(3)

# Select month
Select(driver.find_element(By.ID, "month")).select_by_value(MONTH)

# Select year
Select(driver.find_element(By.ID, "year")).select_by_value(YEAR)

# Enter FPS ID
driver.find_element(By.ID, "fps_id").send_keys(FPS_ID)

# Run javascript to load stock data
driver.execute_script("detailsR();")

# Wait for AJAX table
time.sleep(6)

rows = driver.find_elements(By.XPATH, '//*[@id="detailsR"]//table//tr')

print("Commodity | Alloted Qty | CB Qty")
print("----------------------------------")

data = []

for row in rows:
    cols = row.find_elements(By.TAG_NAME, "td")

    if len(cols) >= 11:
        commodity = cols[1].text.strip()
        alloted_qty = cols[4].text.strip()
        cb_qty = cols[10].text.strip()

        if commodity != "":
            print(f"{commodity} | {alloted_qty} | {cb_qty}")

            data.append({
                "commodity": commodity,
                "alloted_qty": alloted_qty,
                "cb_qty": cb_qty
            })

driver.quit()

# Save JSON
output = {
    "fps_id": FPS_ID,
    "month": MONTH,
    "year": YEAR,
    "stock": data
}

with open("stock_current.json", "w") as f:
    json.dump(output, f, indent=2)

print("\nJSON updated")
