from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


# ============================================================
# DIMENSIONS
# ============================================================

class DimLocation(Base):
    __tablename__ = "dim_location"
    __table_args__ = {"schema": "gold"}

    location_key = Column(String, primary_key=True)
    location_id = Column(String)
    location_name = Column(String)
    location_description = Column(String)
    address = Column(String)
    location_type = Column(String)
    latitude = Column(Float)    # double precision
    longitude = Column(Float)   # double precision
    city_id = Column(Integer)


class DimPerson(Base):
    __tablename__ = "dim_person"
    __table_args__ = {"schema": "gold"}

    person_key = Column(String, primary_key=True)
    person_id = Column(String)
    person_name = Column(String)
    birth_year = Column(String)      # text
    death_year = Column(String)      # text
    birthplace = Column(String)
    biography = Column(String)


class DimMedia(Base):
    __tablename__ = "dim_media"
    __table_args__ = {"schema": "gold"}

    media_key = Column(String, primary_key=True)
    media_id = Column(String)
    media = Column(String)
    media_type = Column(String)


# ============================================================
# FACT TABLES
# ============================================================

class FactEvent(Base):
    __tablename__ = "fact_event"
    __table_args__ = {"schema": "gold"}

    event_key = Column(String, primary_key=True)
    event_id = Column(String)
    event_name = Column(String)
    event_date = Column(String)   # text
    event_type = Column(String)
    description = Column(String)

    location_key = Column(String)       # text (FK -> dim_location.location_key is integer, but mapping from text id)
    main_person_key = Column(String)    # text (FK -> dim_person.person_key)


# ============================================================
# BRIDGES / EXTENSIONS
# ============================================================

class FactPersonEvent(Base):
    __tablename__ = "fact_person_event"
    __table_args__ = {"schema": "gold"}

    person_event_key = Column(String, primary_key=True)
    person_key = Column(String)   # text (FK -> dim_person)
    event_key = Column(String)    # text (FK -> fact_event)
    role = Column(String)


class FactEventMedia(Base):
    __tablename__ = "fact_event_media"
    __table_args__ = {"schema": "gold"}

    media_key = Column(String, primary_key=True)  # text
    event_key = Column(String, primary_key=True)  # text
