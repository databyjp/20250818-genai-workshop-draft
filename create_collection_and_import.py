#!/usr/bin/env python3

import weaviate
from weaviate.classes.config import Configure, Property, DataType
import pandas as pd
import os

def main():
    # Connect to Weaviate
    client = weaviate.connect_to_local()

    try:
        # Create collection
        collection_name = "FinancialArticles"

        client.collections.create(
            collection_name,
            properties=[
                Property(name="article_title", data_type=DataType.TEXT),
                Property(name="article", data_type=DataType.TEXT),
                Property(name="stock_symbol", data_type=DataType.TEXT),
                Property(name="url", data_type=DataType.TEXT),
                Property(name="date", data_type=DataType.DATE)
            ],
            vector_config=Configure.Vectors.text2vec_ollama(
                source_properties=["article_title", "article"],
                api_endpoint="http://host.docker.internal:11434",
                model="snowflake-arctic-embed2",
                quantizer=Configure.VectorIndex.Quantizer.rq(),
            ),
        )

        collection = client.collections.get(collection_name)

        # Load and import data
        df = pd.read_parquet("data/fin_news_articles_10k.parquet")
        df['date'] = pd.to_datetime(df['date'], utc=True)
        # df = df[:10]

        print(f"Importing {len(df)} articles...")

        with collection.batch.fixed_size(batch_size=100) as batch:
            for _, row in df.iterrows():
                batch.add_object({
                    "article_title": str(row['article_title']) if pd.notna(row['article_title']) else "",
                    "article": str(row['article']) if pd.notna(row['article']) else "",
                    "stock_symbol": str(row['stock_symbol']) if pd.notna(row['stock_symbol']) else "",
                    "url": str(row['url']) if pd.notna(row['url']) else "",
                    "date": row['date'].isoformat() if pd.notna(row['date']) else None
                })

        # Verify
        response = collection.query.fetch_objects(
            limit=3,
            return_properties=["article_title", "stock_symbol"],
            # include_vector=True
        )
        print(f"Successfully imported! Sample articles:")
        for obj in response.objects:
            print(f"- {obj.properties['article_title'][:60]}... [{obj.properties['stock_symbol']}]")
            if len(obj.vector) > 0:
                print(f"Vector: {len(obj.vector['default'])} dims; {obj.vector['default'][:3]}...")

    finally:
        client.close()

if __name__ == "__main__":
    main()
