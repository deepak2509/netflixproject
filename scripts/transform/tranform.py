import pandas as pd
import os


os.makedirs('data/processed', exist_ok=True)


raw_files = {
    "netflix_global": "data/raw/netflix/netflix_global_top10.csv",
    "netflix_region": "data/raw/netflix/netflix_region_top10.csv",
    "imdb_metadata": "data/raw/imdb/imdb_metadata.csv",
    "youtube_trailers": "data/raw/youtube/youtube_trailer_stats.csv",
    "reddit_mentions": "data/raw//reddit/reddit_data.csv",
    "google_trends": "data/raw/google_trends/googletrends.csv"
}


for name, path in raw_files.items():
    if os.path.exists(path):
        try:
            df = pd.read_csv(path)

            if 'Title' in df.columns:
                df = df[df['Title'].notna()]
                df['Title'] = df['Title'].astype(str).str.strip()

            processed_path = f"data/processed/{name}.csv"
            df.to_csv(processed_path, index=False)
            print(f" Cleaned and saved: {processed_path}")
        except Exception as e:
            print(f" Failed to process {name}: {e}")
    else:
        print(f" File missing: {path}")
