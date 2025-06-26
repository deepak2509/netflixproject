import praw
import yaml
import pandas as pd
from textblob import TextBlob
from datetime import datetime
import os


with open("congif/config.yaml", "r") as f:
    config = yaml.safe_load(f)
reddit_cfg = config["reddit"]


reddit = praw.Reddit(
    client_id=reddit_cfg["client_id"],
    client_secret=reddit_cfg["client_secret"],
    user_agent=reddit_cfg["user_agent"]
)

titles = pd.read_csv("unique_titles.csv")["Title"].dropna().unique()

results = []

os.makedirs("data/raw/reddit", exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_path = f"data/raw/reddit/reddit_data_{timestamp}.csv"

for idx, title in enumerate(titles, 1):
    print(f" Searching Reddit for: {title} ({idx}/{len(titles)})")
    try:
        for submission in reddit.subreddit("all").search(title, limit=5):
            submission.comments.replace_more(limit=0)
            for comment in submission.comments[:5]:  # limit to top 5 comments per post
                sentiment = TextBlob(comment.body).sentiment
                results.append({
                    "Title": title,
                    "Subreddit": submission.subreddit.display_name,
                    "Post Title": submission.title,
                    "Comment": comment.body,
                    "Comment Score": comment.score,
                    "Sentiment Polarity": sentiment.polarity,
                    "Sentiment Subjectivity": sentiment.subjectivity,
                    "Created At": datetime.fromtimestamp(comment.created_utc)
                })
    except Exception as e:
        print(f" Error fetching for {title}: {e}")

df = pd.DataFrame(results)
df.to_csv(output_path, index=False)
print(f" Reddit data saved to {output_path}")
