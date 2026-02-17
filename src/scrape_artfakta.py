from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import sys
import time

def extract_results(url):
    # Setup headless Chrome
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')

    # Provide path to chromedriver if needed
    driver = webdriver.Chrome(options=options)

    try:
        driver.get(url)
        time.sleep(5)  # Wait for JS to load

        # Find rows in the "Resultat" table
        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        data = []

        for row in rows:
            columns = row.find_elements(By.TAG_NAME, "td")
            if len(columns) < 7:
                continue  # skip if malformed
            data.append({
                "Sökträff": columns[0].text,
                "Namnkategori": columns[1].text,
                "Vetenskapligt namn": columns[2].text,
                "Auktor": columns[3].text,
                "Svenskt namn": columns[4].text,
                "Taxonkategori": columns[5].text,
                "TaxonID": columns[6].text,
            })

        return data

    finally:
        driver.quit()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scrape_artfakta.py <search_url>")
        sys.exit(1)

    search_url = sys.argv[1]
    results = extract_results(search_url)

    for result in results:
        print(result)
