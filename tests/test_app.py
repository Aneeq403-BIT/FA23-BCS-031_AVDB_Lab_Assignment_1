from fastapi.testclient import TestClient
from app.main import app
import os

client = TestClient(app)

def test_health_check():
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"

def test_get_books_list():
    # Depends on data being ingested, but handles empty DB gracefully
    response = client.get("/books?page=1&page_size=10")
    assert response.status_code == 200
    data = response.json()
    assert "items" in data
    assert "total" in data

def test_create_rating_unauthorized():
    payload = {"user_id": 1, "book_id": 1, "rating": 5}
    response = client.post("/ratings", json=payload)
    assert response.status_code == 401

def test_create_rating_authorized():
    # Assumes MongoDB is running
    headers = {"x-api-key": "secret123"}
    payload = {"user_id": 99999, "book_id": 1, "rating": 5}
    
    response = client.post("/ratings", json=payload, headers=headers)
    assert response.status_code == 200
    assert response.json()["matched"] >= 0