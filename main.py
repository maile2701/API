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
@app.get("/sites")
def get_sites(skip: int = 0, limit: int = 50, db: Session = next(get_db())):
    sites = db.query(DimSite).offset(skip).limit(limit).all()
    return {"count": len(sites), "data": sites}

@app.get("/sites/{site_id}")
def get_site(site_id: str, db: Session = next(get_db())):
    site = db.query(DimSite).filter(DimSite.site_id == site_id).first()
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")
    return {"data": site}

@app.get("/persons")
def get_persons(skip: int = 0, limit: int = 50, db: Session = next(get_db())):
    persons = db.query(DimPerson).offset(skip).limit(limit).all()
    return {"count": len(persons), "data": persons}

@app.get("/persons/{person_id}")
def get_person(person_id: str, db: Session = next(get_db())):
    person = db.query(DimPerson).filter(DimPerson.person_id == person_id).first()
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")
    return {"data": person}

@app.get("/media")
def get_media(skip: int = 0, limit: int = 50, media_type: str = None, db: Session = next(get_db())):
    query = db.query(DimMedia)
    if media_type:
        query = query.filter(DimMedia.media_type == media_type)
    media_list = query.offset(skip).limit(limit).all()
    return {"count": len(media_list), "data": media_list}

# ---------------------------
# FACTS / EVENTS
# ---------------------------
@app.get("/events")
def get_events(skip: int = 0, limit: int = 50, event_type: str = None, db: Session = next(get_db())):
    query = db.query(FactEvent)\
        .options(
            joinedload(FactEvent.site),
            joinedload(FactEvent.main_person),
            selectinload(FactEvent.persons).joinedload(FactPersonEvent.person),
            selectinload(FactEvent.media).joinedload(FactEventMedia.media)
        )
    if event_type:
        query = query.filter(FactEvent.event_type == event_type)
    events = query.offset(skip).limit(limit).all()
    return {"count": len(events), "data": events}

@app.get("/events/{event_id}")
def get_event(event_id: str, db: Session = next(get_db())):
    event = db.query(FactEvent)\
        .options(
            joinedload(FactEvent.site),
            joinedload(FactEvent.main_person),
            selectinload(FactEvent.persons).joinedload(FactPersonEvent.person),
            selectinload(FactEvent.media).joinedload(FactEventMedia.media)
        )\
        .filter(FactEvent.event_id == event_id)\
        .first()
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    return {"data": event}

# ---------------------------
# FILTERS
# ---------------------------
@app.get("/events/by-site/{site_id}")
def get_events_by_site(site_id: str, skip: int = 0, limit: int = 50, db: Session = next(get_db())):
    events = db.query(FactEvent)\
        .join(DimSite, FactEvent.site_key == DimSite.site_key)\
        .filter(DimSite.site_id == site_id)\
        .options(
            joinedload(FactEvent.site),
            joinedload(FactEvent.main_person),
            selectinload(FactEvent.persons).joinedload(FactPersonEvent.person),
            selectinload(FactEvent.media).joinedload(FactEventMedia.media)
        )\
        .offset(skip).limit(limit).all()
    return {"count": len(events), "data": events}

@app.get("/events/by-person/{person_id}")
def get_events_by_person(person_id: str, skip: int = 0, limit: int = 50, db: Session = next(get_db())):
    events = db.query(FactEvent)\
        .join(FactPersonEvent, FactEvent.event_key == FactPersonEvent.event_key)\
        .join(DimPerson, FactPersonEvent.person_key == DimPerson.person_key)\
        .filter(DimPerson.person_id == person_id)\
        .options(
            joinedload(FactEvent.site),
            joinedload(FactEvent.main_person),
            selectinload(FactEvent.persons).joinedload(FactPersonEvent.person),
            selectinload(FactEvent.media).joinedload(FactEventMedia.media)
        )\
        .offset(skip).limit(limit).all()
    return {"count": len(events), "data": events}

@app.get("/events/by-media/{media_id}")
def get_events_by_media(media_id: str, skip: int = 0, limit: int = 50, db: Session = next(get_db())):
    events = db.query(FactEvent)\
        .join(FactEventMedia, FactEvent.event_key == FactEventMedia.event_key)\
        .join(DimMedia, FactEventMedia.media_key == DimMedia.media_key)\
        .filter(DimMedia.media_id == media_id)\
        .options(
            joinedload(FactEvent.site),
            joinedload(FactEvent.main_person),
            selectinload(FactEvent.persons).joinedload(FactPersonEvent.person),
            selectinload(FactEvent.media).joinedload(FactEventMedia.media)
        )\
        .offset(skip).limit(limit).all()
    return {"count": len(events), "data": events}

# ---------------------------
# MEDIA + EVENT NESTED
# ---------------------------
@app.get("/event-media-flat")
def get_event_media_flat(skip: int = 0, limit: int = 50, db: Session = next(get_db())):
    media_events = db.query(FactEventMedia)\
        .options(
            joinedload(FactEventMedia.event),
            joinedload(FactEventMedia.media)
        )\
        .offset(skip).limit(limit).all()
    return {"count": len(media_events), "data": media_events}

