import pandas as pd
from googleapiclient.discovery import build
from urllib.parse import quote
import time

API_KEY = ''  # Replace with your actual API key
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'


globally_df = pd.read_csv('netflix_global_top10.csv')
region_df = pd.read_csv('netflix_region_top10.csv')

all_titles = pd.concat([globally_df['Title'], region_df['Title']]).dropna().unique()
print(f"🎬 Unique titles extracted: {len(all_titles)}")

youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)

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
            'Search Title': title,
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
for title in all_titles:
    print(f" Fetching trailer for: {title}")
    data = get_trailer_data(title)
    if data:
        trailer_data.append(data)
    time.sleep(5)  

df = pd.DataFrame(trailer_data)
df.to_csv('newyoutube_trailer_stats.csv', index=False)
print(" YouTube trailer stats saved to 'youtube_trailer_stats.csv'")
