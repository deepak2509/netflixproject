import requests
import pandas as pd
import time
from difflib import SequenceMatcher
from urllib.parse import quote

API_KEY = "4f1f9f2a"
OMDB_SEARCH_URL = "https://www.omdbapi.com/?s={title}&apikey=" + API_KEY
OMDB_ID_URL = "https://www.omdbapi.com/?i={imdb_id}&apikey=" + API_KEY

global_df = pd.read_csv("netflix_global_top10.csv")
region_df = pd.read_csv("netflix_region_top10.csv")

all_titles = pd.concat([
    global_df["Title"].dropna(),
    region_df["Title"].dropna()
]).unique()

print(f" Fetching IMDb metadata for {len(all_titles)} unique titles...\n")

def get_best_match(search_results, original_title):
    matches = []
    for result in search_results:
        score = SequenceMatcher(None, original_title.lower(), result["Title"].lower()).ratio()
        matches.append((score, result))
    matches.sort(reverse=True, key=lambda x: x[0])
    return matches[:1]  

metadata_list = []

for title in all_titles:
    cleaned_title = title.replace(":", "").replace("'", "").replace("-", "").strip()
    print(f" Searching: '{title}' → Cleaned: '{cleaned_title}'")

    url = OMDB_SEARCH_URL.format(title=quote(cleaned_title))
    try:
        res = requests.get(url)
        search_res = res.json()

        if search_res.get("Response") == "True":
            best = get_best_match(search_res["Search"], title)
            if best and best[0][0] > 0.6:
                match = best[0][1]
                imdb_id = match["imdbID"]
                imdb_detail_url = OMDB_ID_URL.format(imdb_id=imdb_id)
                detail_res = requests.get(imdb_detail_url).json()

                detail_res["MatchedTitle"] = match["Title"]
                detail_res["OriginalTitle"] = title
                metadata_list.append(detail_res)

                print(f" Found: {match['Title']} (IMDb ID: {imdb_id})\n")
            else:
                print(f" No close match: {title}\n")
        else:
            print(f" Not found: {title}\n")

    except Exception as e:
        print(f"⚠️ Error fetching {title}: {str(e)}")
    
    time.sleep(0.5) 


metadata_df = pd.DataFrame(metadata_list)
metadata_df.to_csv("imdb_metadata.csv", index=False)
print(" IMDb metadata saved to imdb_metadata.csv")
