import pandas as pd
from googleapiclient.discovery import build
import time


API_KEY = ''  # Replace with your valid API key
INPUT_CSV = 'titles_missing.csv' 
OUTPUT_CSV = 'new_youtube_trailer_stats_output.csv'

# === Setup YouTube API ===
youtube = build('youtube', 'v3', developerKey=API_KEY)


df = pd.read_csv(INPUT_CSV)
titles = df['Title'].dropna().unique()
print(f" Titles to fetch: {len(titles)}")

def get_trailer_data(title):
    try:
        search_query = f"{title} Official Trailer"
        search_response = youtube.search().list(
            q=search_query,
            part='id,snippet',
            maxResults=1,
            type='video'
        ).execute()

        if not search_response['items']:
            print(f" No trailer found for {title}")
            return None

        video = search_response['items'][0]
        video_id = video['id']['videoId']
        video_stats = youtube.videos().list(
            part='statistics,snippet,contentDetails',
            id=video_id
        ).execute()['items'][0]

        return {
            'Title': title,
            'YouTube Title': video_stats['snippet']['title'],
            'Video ID': video_id,
            'Video URL': f"https://www.youtube.com/watch?v={video_id}",
            'Published At': video_stats['snippet']['publishedAt'],
            'View Count': video_stats['statistics'].get('viewCount', '0'),
            'Like Count': video_stats['statistics'].get('likeCount', '0'),
            'Comment Count': video_stats['statistics'].get('commentCount', '0'),
            'Duration': video_stats['contentDetails']['duration']
        }

    except Exception as e:
        print(f"⚠️ Error fetching trailer for {title}: {e}")
        return None


trailer_data = []
for title in titles:
    print(f" Fetching trailer for: {title}")
    data = get_trailer_data(title)
    if data:
        trailer_data.append(data)
    time.sleep(0.2)  

output_df = pd.DataFrame(trailer_data)
output_df.to_csv(OUTPUT_CSV, index=False)
print(f" YouTube trailer stats saved to '{OUTPUT_CSV}'")
