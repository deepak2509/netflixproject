

from googleapiclient.discovery import build
import pandas as pd
import os
from datetime import datetime

API_KEY = ""  # Replace with your actual API key
RAW_OUTPUT_PATH = "data/raw/youtube_data.csv"

def fetch_youtube_trailers(api_key, titles):
    print("Fetching YouTube trailer metadata...")

    youtube = build("youtube", "v3", developerKey=api_key)
    results = []

    for title in titles:
        query = f"{title} Official Trailer Netflix"
        search_response = youtube.search().list(
            q=query,
            part="snippet",
            type="video",
            maxResults=1
        ).execute()

        result = {
            "title": title,
            "youtube_title": None,
            "channel": None,
            "publish_time": None,
            "video_id": None,
            "video_url": None,
            "view_count": None,
            "like_count": None,
            "comment_count": None,
            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        if search_response["items"]:
            video = search_response["items"][0]
            snippet = video["snippet"]
            video_id = video["id"]["videoId"]

   
            stats_response = youtube.videos().list(
                part="statistics",
                id=video_id
            ).execute()

            stats = stats_response["items"][0]["statistics"] if stats_response["items"] else {}

            result.update({
                "youtube_title": snippet["title"],
                "channel": snippet["channelTitle"],
                "publish_time": snippet["publishedAt"],
                "video_id": video_id,
                "video_url": f"https://www.youtube.com/watch?v={video_id}",
                "view_count": stats.get("viewCount"),
                "like_count": stats.get("likeCount"),
                "comment_count": stats.get("commentCount")
            })

        results.append(result)

    df = pd.DataFrame(results)
    os.makedirs("data/raw", exist_ok=True)
    df.to_csv(RAW_OUTPUT_PATH, index=False)
    print(f" Saved {len(df)} records to {RAW_OUTPUT_PATH}")

if __name__ == "__main__":
    input_path = "data/cleaned/netflix_top10.csv"
    df = pd.read_csv(input_path)
    titles = df["title"].dropna().unique().tolist()

    fetch_youtube_trailers(API_KEY, titles)
