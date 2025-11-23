import os
import time
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Request, Query, Depends, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from pymongo import MongoClient

# --- Configuration ---
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "LabAssignment1")
API_KEY = os.getenv("API_KEY", "secret123")

# --- Database Connection ---
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

app = FastAPI(
    title="GoodBooks API",
    description="MongoDB backed API for GoodBooks-10k dataset",
    version="1.0.0"
)

# --- Pydantic Models ---
class Book(BaseModel):
    book_id: int
    goodreads_book_id: int
    title: str
    authors: str
    original_publication_year: Optional[float] = None
    average_rating: float
    ratings_count: int
    image_url: str

class RatingIn(BaseModel):
    user_id: int
    book_id: int
    rating: int = Field(ge=1, le=5)

class RatingOut(BaseModel):
    upserted: bool
    matched: int

class PaginatedResponse(BaseModel):
    items: List[dict]
    page: int
    page_size: int
    total: int

# --- Middleware & Utils ---

@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Logging middleware as per requirements."""
    t0 = time.time()
    response = await call_next(request)
    process_time = int((time.time() - t0) * 1000)
    
    log_data = {
        "route": request.url.path,
        "params": dict(request.query_params),
        "status": response.status_code,
        "latency_ms": process_time,
        "ip": request.client.host
    }
    print(log_data) # In production, send to a logger file
    return response

def verify_api_key(request: Request):
    """Simple Auth check."""
    key = request.headers.get("x-api-key")
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API Key")

# --- Endpoints ---

@app.get("/healthz", tags=["System"])
def health_check():
    """Nice-to-have: Health check."""
    try:
        client.admin.command('ping')
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "error", "detail": str(e)})

@app.get("/books", response_model=PaginatedResponse, tags=["Books"])
def list_books(
    q: Optional[str] = None,
    min_avg: Optional[float] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    sort: str = Query("avg", pattern="^(avg|ratings_count|year|title)$"),
    order: str = Query("desc", pattern="^(asc|desc)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100)
):
    query_filter = {}
    
    # Text Search (Fuzzy-ish via Regex)
    if q:
        query_filter["$or"] = [
            {"title": {"$regex": q, "$options": "i"}},
            {"authors": {"$regex": q, "$options": "i"}}
        ]

    if min_avg:
        query_filter["average_rating"] = {"$gte": min_avg}
        
    if year_from or year_to:
        query_filter["original_publication_year"] = {}
        if year_from: query_filter["original_publication_year"]["$gte"] = year_from
        if year_to: query_filter["original_publication_year"]["$lte"] = year_to

    # Sorting map
    sort_field_map = {
        "avg": "average_rating",
        "ratings_count": "ratings_count",
        "year": "original_publication_year",
        "title": "title"
    }
    mongo_order = -1 if order == "desc" else 1
    
    total = db.books.count_documents(query_filter)
    cursor = db.books.find(query_filter)\
        .sort(sort_field_map[sort], mongo_order)\
        .skip((page - 1) * page_size)\
        .limit(page_size)
    
    items = list(cursor)
    # Convert ObjectId to string if necessary, though Pydantic handles dicts mostly
    for i in items: i.pop("_id", None)

    return {"items": items, "page": page, "page_size": page_size, "total": total}

@app.get("/books/{book_id}", response_model=Book, tags=["Books"])
def get_book_details(book_id: int):
    book = db.books.find_one({"book_id": book_id}, {"_id": 0})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    return book

@app.get("/books/{book_id}/tags", tags=["Books"])
def get_book_tags(book_id: int):
    """
    Join: Books -> BookTags -> Tags
    """
    book = db.books.find_one({"book_id": book_id})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    
    gr_id = book.get("goodreads_book_id")
    
    pipeline = [
        {"$match": {"goodreads_book_id": gr_id}},
        {"$lookup": {
            "from": "tags",
            "localField": "tag_id",
            "foreignField": "tag_id",
            "as": "tag_info"
        }},
        {"$unwind": "$tag_info"},
        {"$project": {
            "_id": 0,
            "tag_id": 1,
            "count": 1,
            "tag_name": "$tag_info.tag_name"
        }},
        {"$sort": {"count": -1}}
    ]
    
    tags = list(db.book_tags.aggregate(pipeline))
    return {"book_id": book_id, "tags": tags}

@app.get("/authors/{author_name}/books", tags=["Authors"])
def get_author_books(author_name: str):
    # Case insensitive regex search for author
    books = list(db.books.find(
        {"authors": {"$regex": author_name, "$options": "i"}},
        {"_id": 0}
    ).limit(50)) # Safety limit
    return {"author": author_name, "count": len(books), "books": books}

@app.get("/tags", tags=["Tags"])
def get_all_tags(page: int = 1, page_size: int = 50):
    total = db.tags.count_documents({})
    tags = list(db.tags.find({}, {"_id": 0}).skip((page-1)*page_size).limit(page_size))
    
    # Nice-to-have: Get counts per tag is expensive without pre-calculation, 
    # so we just return the tag list as per basic requirement
    return {"items": tags, "page": page, "page_size": page_size, "total": total}

@app.get("/users/{user_id}/to-read", tags=["Users"])
def get_user_toread(user_id: int):
    # Join to_read -> books
    pipeline = [
        {"$match": {"user_id": user_id}},
        {"$lookup": {
            "from": "books",
            "localField": "book_id",
            "foreignField": "book_id",
            "as": "book"
        }},
        {"$unwind": "$book"},
        {"$project": {"_id": 0, "book": 1}}
    ]
    items = list(db.to_read.aggregate(pipeline))
    return {"user_id": user_id, "to_read": [i['book'] for i in items]}

@app.get("/books/{book_id}/ratings/summary", tags=["Ratings"])
def get_rating_summary(book_id: int):
    pipeline = [
        {"$match": {"book_id": book_id}},
        {"$group": {
            "_id": "$book_id",
            "average": {"$avg": "$rating"},
            "count": {"$sum": 1},
            "distribution": {"$push": "$rating"}
        }}
    ]
    data = list(db.ratings.aggregate(pipeline))
    if not data:
        return {"book_id": book_id, "average": 0, "count": 0, "histogram": {}}
    
    res = data[0]
    # Calculate simple histogram
    dist = res['distribution']
    hist = {i: dist.count(i) for i in range(1, 6)}
    
    return {
        "book_id": book_id,
        "average": round(res['average'], 2),
        "count": res['count'],
        "histogram": hist
    }

@app.post("/ratings", dependencies=[Depends(verify_api_key)], response_model=RatingOut, tags=["Ratings"])
def upsert_rating(r: RatingIn):
    # Upsert based on user_id + book_id
    result = db.ratings.update_one(
        {"user_id": r.user_id, "book_id": r.book_id},
        {"$set": {"rating": r.rating}},
        upsert=True
    )
    return {
        "upserted": result.upserted_id is not None,
        "matched": result.matched_count
    }

@app.get("/metrics", tags=["System"])
def metrics():
    """Nice-to-have: Simple metrics."""
    return {
        "books_count": db.books.count_documents({}),
        "ratings_count": db.ratings.count_documents({}),
        "users_count": len(db.ratings.distinct("user_id"))
    }