from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import csv
import re

REGIONS = ['global', 'australia', 'india','United States','Canada','Brazil','Argentina','United Kingdom', 'Germany', 'France', 'Spain','Japan', 'South Korea', 'Indonesia','New Zealand','South Africa', 'Nigeria','UAE '] 
WEEKS = [
    '2025-06-15', '2025-06-01', '2025-05-25', '2025-05-18',
    '2025-05-11', '2025-04-27', '2025-04-20', '2025-04-13',
    '2025-04-06', '2025-03-30', '2025-03-23', '2025-03-16',
    '2025-03-09', '2025-03-02', '2025-02-23', '2025-02-16',
    '2025-02-09', '2025-02-02', '2025-01-26', '2025-01-19',
    '2025-01-12', '2025-01-05'
]


options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--window-size=1920,1080")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
wait = WebDriverWait(driver, 15)


all_data = []

for region in REGIONS:
    for week in WEEKS:
        if region == 'global':
            url = f"https://www.netflix.com/tudum/top10?week={week}"
        else:
            url = f"https://www.netflix.com/tudum/top10/{region}?week={week}"

        print(f"Scraping: {url}")
        driver.get(url)

        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'section[data-presentation="PulseTop10TablePresentation"] table')))
            time.sleep(2)

            table_section = driver.find_element(By.CSS_SELECTOR, 'section[data-presentation="PulseTop10TablePresentation"]')
            table_html = table_section.get_attribute('innerHTML')
            soup = BeautifulSoup(table_html, 'html.parser')
            table = soup.find('table')
            rows = table.find_all('tr')[1:]  

            for i, row in enumerate(rows):
                cols = row.find_all('td')

                if region == 'global':
                    try:
                        raw_title = cols[0].text.strip()
                        match = re.match(r'^(\d+)(.+)$', raw_title)
                        if not match:
                            print(f"Could not extract rank/title from row {i}: {raw_title}")
                            continue

                        rank = match.group(1)
                        title = match.group(2).strip()
                        weeks_in_top10 = cols[1].text.strip() if len(cols) > 1 else None
                        views = cols[2].text.strip() if len(cols) > 2 else None
                        runtime = cols[3].text.strip() if len(cols) > 3 else None
                        hours_viewed = cols[4].text.strip() if len(cols) > 4 else None

                        all_data.append({
                            'Rank': rank,
                            'Title': title,
                            'Weeks in Top 10': weeks_in_top10,
                            'Views': views,
                            'Runtime': runtime,
                            'Hours Viewed': hours_viewed,
                            'Region': 'Global',
                            'Week': week
                        })
                    except Exception as e:
                        print(f"Error parsing global row {i}: {e}")
                        continue

                else:
                    try:
                        if len(cols) < 2:
                            print(f"Skipping row {i} in {region} due to unexpected columns.")
                            continue

                        raw_title = cols[0].text.strip()
                        match = re.match(r'^(\d+)(.+)$', raw_title)
                        if not match:
                            print(f"Could not parse rank/title from '{raw_title}' in row {i}")
                            continue

                        rank = match.group(1)
                        title = match.group(2).strip()
                        weeks_in_top10 = cols[1].text.strip()

                        all_data.append({
                            'Rank': rank,
                            'Title': title,
                            'Weeks in Top 10': weeks_in_top10,
                            'Views': None,
                            'Runtime': None,
                            'Hours Viewed': None,
                            'Region': region.title(),
                            'Week': week
                        })
                    except Exception as e:
                        print(f"Error parsing region row {i} ({region}): {e}")
                        continue

        except Exception as e:
            print(f"Error scraping {url}: {e}")
            continue

driver.quit()


csv_columns = ['Rank', 'Title', 'Weeks in Top 10', 'Views', 'Runtime', 'Hours Viewed', 'Region', 'Week']

global_data = [d for d in all_data if d['Region'].lower() == 'global']
region_data = [d for d in all_data if d['Region'].lower() != 'global']


with open('data/raw/netflix/netflix_global_top10.csv', mode='w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=csv_columns)
    writer.writeheader()
    writer.writerows(global_data)

with open('data/raw/netflix/netflix_region_top10.csv', mode='w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=csv_columns)
    writer.writeheader()
    writer.writerows(region_data)

print("\n Scraping completed.")
print(" Global data saved to: netflix_global_top10.csv")
print(" Region data saved to: netflix_region_top10.csv")
