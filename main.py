import pandas as pd

# Read the parquet file
df = pd.read_parquet('your_file.parquet')

# Iterate through rows
for index, row in df.iterrows():
    # Access row data
    date = row['Date']
    title = row['Article_title']
    symbol = row['Stock_symbol']
    url = row['Url']
    article = row['Article']

    # Your Weaviate insertion code would go here
    print(f"Processing: {symbol} - {title[:50]}...")
