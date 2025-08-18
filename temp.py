import pandas as pd

# Read the parquet file
df = pd.read_parquet('data/fin_news_articles_5000.parquet')

# Iterate through rows
for index, row in df.iterrows():
    # Access row data
    date = row['date']
    title = row['article_title']
    symbol = row['stock_symbol']
    url = row['url']
    article = row['article']

    # Your Weaviate insertion code would go here
    print(f"Processing: {symbol} - {title[:50]}...")
    print(article[:100])
