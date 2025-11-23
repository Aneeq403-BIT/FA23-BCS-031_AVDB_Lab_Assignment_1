import os
import pandas as pd
from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING, TEXT

# Config
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "goodbooks")

# Data URLs (Raw)
URLS = {
    "books": "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/books.csv",
    "ratings": "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/ratings.csv",
    "tags": "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/tags.csv",
    "book_tags": "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/book_tags.csv",
    "to_read": "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/to_read.csv"
}

def get_database():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]

def create_indexes(db):
    print("Creating indexes...")
    
    # Books: Title/Author text search, rating sort, book_id lookup
    db.books.create_index([("title", TEXT), ("authors", TEXT)])
    db.books.create_index([("average_rating", DESCENDING)])
    db.books.create_index("book_id", unique=True)
    db.books.create_index("goodreads_book_id") # Needed for joins

    # Ratings: Unique per user+book
    db.ratings.create_index([("user_id", ASCENDING), ("book_id", ASCENDING)], unique=True)
    db.ratings.create_index("book_id")

    # Tags
    db.tags.create_index("tag_id", unique=True)
    db.tags.create_index("tag_name")

    # Book Tags
    db.book_tags.create_index([("goodreads_book_id", ASCENDING), ("tag_id", ASCENDING)], unique=True)
    
    # To Read
    db.to_read.create_index([("user_id", ASCENDING), ("book_id", ASCENDING)], unique=True)
    print("Indexes created.")

def ingest_collection(db, name, url, key_fields):
    print(f"Ingesting {name} from {url}...")
    try:
        df = pd.read_csv(url)
        
        # Clean NaNs (MongoDB doesn't like NaNs)
        df = df.fillna("")
        
        operations = []
        for _, row in df.iterrows():
            data = row.to_dict()
            
            # Create a filter based on unique keys for idempotency
            filter_doc = {k: data[k] for k in key_fields}
            
            # Add UpdateOne operation (Upsert)
            operations.append(UpdateOne(filter_doc, {"$set": data}, upsert=True))

        if operations:
            result = db[name].bulk_write(operations)
            print(f"  Processed {name}: Matched {result.matched_count}, Upserted {result.upserted_count}")
        
    except Exception as e:
        print(f"Error ingesting {name}: {e}")

def main():
    db = get_database()
    create_indexes(db)
    
    # Define collection name, URL, and the fields that make a record unique
    ingest_collection(db, "books", URLS["books"], ["book_id"])
    ingest_collection(db, "tags", URLS["tags"], ["tag_id"])
    ingest_collection(db, "ratings", URLS["ratings"], ["user_id", "book_id"])
    ingest_collection(db, "book_tags", URLS["book_tags"], ["goodreads_book_id", "tag_id"])
    ingest_collection(db, "to_read", URLS["to_read"], ["user_id", "book_id"])
    
    print("Ingestion complete.")

if __name__ == "__main__":
    main()