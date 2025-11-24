import os
from fastapi import FastAPI
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

DATABASE_URL = os.getenv("DATABASE_URL")  # Railway sẽ inject trực tiếp
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set!")

engine = create_engine(DATABASE_URL, echo=False, future=True)

def execute_query(query: str, params: dict = None):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            return [dict(row) for row in result.mappings()]
    except SQLAlchemyError as e:
        print("Database error:", e)
        return []

app = FastAPI(title="Gold Layer API", version="1.0")

# ---------------------------
# FastAPI app
# ---------------------------
app = FastAPI(title="Gold Layer API", version="1.0")

# ---------------------------
# DIMENSIONS
# ---------------------------
@app.get("/locations")
def get_locations():
    query = "SELECT * FROM gold.dim_location;"
    data = execute_query(query)
    return {"count": len(data), "data": data}

@app.get("/locations/{location_id}")
def get_location_by_id(location_id: str):
    query = "SELECT * FROM gold.dim_location WHERE location_id = :location_id;"
    data = execute_query(query, {"location_id": location_id})
    if not data:
        raise HTTPException(status_code=404, detail="Location not found")
    return {"count": len(data), "data": data}

@app.get("/persons")
def get_persons():
    query = "SELECT * FROM gold.dim_person;"
    data = execute_query(query)
    return {"count": len(data), "data": data}

@app.get("/persons/{person_id}")
def get_location_by_id(person_id: str):
    query = "SELECT * FROM gold.dim_person WHERE person_id = :person_id;"
    data = execute_query(query, {"person_id": person_id})
    if not data:
        raise HTTPException(status_code=404, detail="Location not found")
    return {"count": len(data), "data": data}

@app.get("/media")
def get_media():
    query = "SELECT * FROM gold.dim_media;"
    data = execute_query(query)
    return {"count": len(data), "data": data}

@app.get("/event_media_flat")
def get_media():
    query = "SELECT * FROM gold.event_media_flat;"
    data = execute_query(query)
    return {"count": len(data), "data": data}

# ---------------------------
# FACTS
# ---------------------------
@app.get("/events")
def get_events():
    query = "SELECT * FROM gold.fact_event;"
    data = execute_query(query)
    return {"count": len(data), "data": data}

@app.get("/events/{event_id}")
def get_event_by_id(event_id: str):
    query = "SELECT * FROM gold.fact_event WHERE event_id = :event_id;"
    data = execute_query(query, {"event_id": event_id})
    if not data:
        raise HTTPException(status_code=404, detail="Event not found")
    return {"count": len(data), "data": data}

# ---------------------------
# BRIDGES
# ---------------------------
@app.get("/person-events")
def get_person_events():
    query = "SELECT * FROM gold.fact_person_event;"
    data = execute_query(query)
    return {"count": len(data), "data": data}

@app.get("/event-media")
def get_event_media():
    query = "SELECT * FROM gold.fact_event_media;"
    data = execute_query(query)
    return {"count": len(data), "data": data}

# ---------------------------
# FILTER EXAMPLES
# ---------------------------
@app.get("/events/location/{location_id}")
def get_events_by_location(location_id: str):
    query = """
        SELECT fe.*, dl.location_name
        FROM gold.fact_event fe
        LEFT JOIN gold.dim_location dl ON fe.location_key = dl.location_key
        WHERE dl.location_id = :location_id;
    """
    data = execute_query(query, {"location_id": location_id})
    if not data:
        raise HTTPException(status_code=404, detail="No events found for this location")
    return {"count": len(data), "data": data}

@app.get("/locations/city/{city_id}")
def get_locations_by_city(city_id: str):
    query = "SELECT * FROM gold.dim_location WHERE city_id = :city_id;"
    data = execute_query(query, {"city_id": city_id})
    return {"count": len(data), "data": data}
