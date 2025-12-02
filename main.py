from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
load_dotenv()
import os

# ---------------------------
# Database connection
# ---------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL, echo=False, future=True)

def execute_query(query: str, params: dict = None):
    """
    Thực thi query SQL và trả về list of dict
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            # Chuyển kết quả thành list of dict
            rows = [dict(row) for row in result.mappings()]
        return rows
    except SQLAlchemyError as e:
        print("Database error:", e)
        return []

# ---------------------------
# FastAPI app
# ---------------------------
app = FastAPI(title="Gold Layer API", version="1.0")

# ---------------------------
# DIMENSIONS
# ---------------------------
DIM_VIEWS = [
    "dim_location",
    "dim_media",
    "dim_person",
    "event_flat",
    "event_media_flat",
    "person_event_flat"
]

for dim in DIM_VIEWS:
    endpoint = f"/{dim}"

    def make_dim_endpoint(view_name):
        def dim_endpoint():
            query = f"SELECT * FROM gold.{view_name};"
            data = execute_query(query)
            return {"count": len(data), "data": data}
        return dim_endpoint

    app.get(endpoint)(make_dim_endpoint(dim))

# ---------------------------
# FACTS
# ---------------------------
FACT_VIEWS = [
    "fact_event",
    "fact_event_media",
    "fact_person_event"
]

for fact in FACT_VIEWS:
    endpoint = f"/{fact}"

    def make_fact_endpoint(view_name):
        def fact_endpoint():
            query = f"SELECT * FROM gold.{view_name};"
            data = execute_query(query)
            return {"count": len(data), "data": data}
        return fact_endpoint

    app.get(endpoint)(make_fact_endpoint(fact))
