import pandas as pd
import numpy as np


df = pd.read_csv('data/processed/final_dataset.csv')

def clean_numeric(col):
    return pd.to_numeric(df[col].astype(str).str.replace(',', '').str.extract('(\d+\.?\d*)')[0], errors='coerce')

df['Total Hours Viewed'] = clean_numeric('Total Hours Viewed')
df['View Count'] = clean_numeric('View Count')
df['Like Count'] = clean_numeric('Like Count')
df['Comment Count'] = clean_numeric('Comment Count')
df['imdbVotes'] = clean_numeric('imdbVotes')
df['Interest Score'] = clean_numeric('Interest Score')

df['Runtime'] = df['Runtime'].str.extract('(\d+)').astype(float)

date_cols = ['Published At', 'Created At', 'Released', 'Date']
for col in date_cols:
    df[col] = pd.to_datetime(df[col], errors='coerce')


df['Genre'] = df['Genre'].fillna("Unknown")
df['Rated'] = df['Rated'].fillna("Unrated")


drop_cols = ['Website', 'Production', 'DVD', 'totalSeasons']
df.drop(columns=drop_cols, inplace=True, errors='ignore')

df.to_csv('data/processed/final_cleaned_dataset.csv', index=False)
print("Cleaned data saved to data/processed/final_cleaned_dataset.csv")
