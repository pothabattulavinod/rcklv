import json
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options

FPS_ID = "0428003"
MONTH = "3"
YEAR = "2026"

options = Options()
options.binary_location = "/usr/bin/google-chrome"

options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(options=options)

driver.get("https://aepos.ap.gov.in/FPS_Stock")

time.sleep(3)

Select(driver.find_element(By.ID,"month")).select_by_value(MONTH)
Select(driver.find_element(By.ID,"year")).select_by_value(YEAR)

driver.find_element(By.ID,"fps_id").send_keys(FPS_ID)

driver.execute_script("detailsR();")

time.sleep(6)

rows = driver.find_elements(By.XPATH,'//*[@id="detailsR"]//table//tr')

data = []

for r in rows:
    cols = r.find_elements(By.TAG_NAME,"td")

    if len(cols) >= 11:
        commodity = cols[1].text.strip()
        alloted = cols[4].text.strip()
        cb = cols[10].text.strip()

        if commodity:
            data.append({
                "commodity": commodity,
                "alloted_qty": alloted,
                "cb_qty": cb
            })

driver.quit()

output = {
    "fps_id": FPS_ID,
    "month": MONTH,
    "year": YEAR,
    "stock": data
}

with open("stock_current.json","w") as f:
    json.dump(output,f,indent=2)

print("JSON updated")
