import pandas as pd
from youtubesearchpython import VideosSearch
import time


global_df = pd.read_csv("netflix_global_top10.csv")
region_df = pd.read_csv("netflix_region_top10.csv")


all_titles = pd.concat([global_df['Title'], region_df['Title']])
unique_titles = sorted(set(all_titles.dropna()))
print(f"🎬 Unique titles extracted: {len(unique_titles)}\n")


def fetch_trailer_info(title):
    query = f"{title} official trailer"
    search = VideosSearch(query, limit=1)
    try:
        result = search.result()["result"][0]
        return {
            "OriginalTitle": title,
            "TrailerTitle": result["title"],
            "Channel": result["channel"]["name"],
            "Views": result["viewCount"]["text"],
            "PublishedTime": result["publishedTime"],
            "Duration": result["duration"],
            "Link": result["link"]
        }
    except Exception as e:
        print(f" Error fetching trailer for: {title} — {e}")
        return {
            "OriginalTitle": title,
            "TrailerTitle": None,
            "Channel": None,
            "Views": None,
            "PublishedTime": None,
            "Duration": None,
            "Link": None
        }


trailer_data = []

for title in unique_titles:
    print(f"🔍 Fetching trailer for: {title}")
    info = fetch_trailer_info(title)
    trailer_data.append(info)
    time.sleep(0.5)  


df = pd.DataFrame(trailer_data)
df.to_csv("youtube_metadata_fallback.csv", index=False)
print("\n Trailer metadata saved to youtube_metadata_fallback.csv")
