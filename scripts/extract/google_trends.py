import pandas as pd
from pytrends.request import TrendReq
from datetime import datetime
import time
import os


pytrends = TrendReq(hl='en-US', tz=360)
df = pd.read_csv("unique_titles.csv")
titles = df["Title"].dropna().unique()

regions = ['US', 'IN', 'GB']  


os.makedirs("data/raw/google_trends", exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = f"data/raw/google_trends/google_trends_{timestamp}.csv"

all_data = []

for geo in regions:
    print(f"\n REGION: {geo}")
    for idx, title in enumerate(titles, 1):
        print(f" Fetching: {title} ({idx}/{len(titles)})")
        try:
            pytrends.build_payload([title], cat=0, timeframe='today 3-m', geo=geo, gprop='')
            interest_df = pytrends.interest_over_time()

            if not interest_df.empty:
                for date, row in interest_df.iterrows():
                    all_data.append({
                        "Title": title,
                        "Region": geo,
                        "Date": date.strftime("%Y-%m-%d"),
                        "Interest Score": row[title],
                        "Is Partial": row["isPartial"]
                    })

            time.sleep(1)  
        except Exception as e:
            print(f" Error for {title} in {geo}: {e}")


trend_df = pd.DataFrame(all_data)
trend_df.to_csv(output_file, index=False)
print(f"\nGoogle Trends data saved to {output_file}")
