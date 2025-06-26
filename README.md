# 🎬 Netflix Show Tracker - Data Engineering Project

This project builds a complete data pipeline that scrapes, processes, stores, and analyzes Netflix show performance using real-world public data. The pipeline is orchestrated using **Apache Airflow** and deployed with **AWS services**.

---

## 🔍 Business Questions Answered

1. **How long does a show typically stay in the Top 10?**
2. **How well does early trailer performance correlate with long-term success?**
3. **Which genres perform best across different regions?**
4. **Are high IMDb ratings correlated with longer Top 10 performance?**
5. **Are there early warning signals of a show failing?**
6. **What shows have high buzz but fail to convert into watch time?**
7. **What features drive virality — genre, cast, trailer stats, sentiment?**

---

## 📦 Data Sources

| Source         | Format | Key Info Extracted |
|----------------|--------|--------------------|
| Netflix Tudum  | CSV    | Weekly ranks, title, region, hours viewed |
| IMDb           | CSV    | Ratings, genres, cast |
| YouTube        | CSV    | Trailer views, likes, publish date |
| Reddit         | CSV    | Weekly mentions of titles |
| Google Trends  | CSV    | Regional interest over time |


Here is the URL that i've used to get the data from Netflix : https://www.netflix.com/tudum/top10


> All raw `.csv` files are stored in a folder named `Data` inside the S3 bucket **`netflixprooject`**.

---

## 🛠️ Tools & Technologies Used

- **Python**: Scripting and transformation
- **Selenium & BeautifulSoup**: Web scraping
- **Apache Airflow**: Workflow orchestration and task scheduling
- **Docker**: Containerization for Airflow
- **AWS S3**: Central data storage
- **AWS Glue**: Crawlers for table schema detection
- **Amazon Athena**: SQL analysis on S3-stored data
- **Power BI / Tableau**: Final data visualization

---

## 🚀 Workflow & Pipeline

### 1. **Extraction**
Each extractor runs as a separate Airflow task:
- `netflix_extract.py`
- `youtube_extract.py`
- `imdb_extract.py`
- `reddit_extract.py`
- `trends_extract.py`

> 📁 Outputs saved in `data/raw/` → Uploaded to S3 bucket → `s3://netflixprooject/Data/`

---

### 2. **Apache Airflow Setup**

- DAG File: `showtracker_dag.py`
- Airflow Tasks:
  - `extract_netflix`
  - `extract_youtube`
  - `extract_imdb`
  - `extract_reddit`
  - `extract_trends`
  - `transform_and_upload_to_s3` (optional)

```bash
# Run with Docker
docker-compose up airflow-init
docker-compose up
