from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, Index
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

# ============================================================
# DIMENSIONS
# ============================================================

class DimSite(Base):
    __tablename__ = "dim_site"
    __table_args__ = {"schema": "gold"}

    site_key = Column(String, primary_key=True, index=True)      # surrogate key
    site_id = Column(String, unique=True, index=True)            # natural key
    site_name = Column(String, index=True)
    description = Column(String)
    address = Column(String)
    site_type = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    established_year = Column(Integer)
    status = Column(String)
    additional_info = Column(String)
    city_id = Column(Integer, index=True)

    # Relationships
    events = relationship("FactEvent", back_populates="site", lazy="selectin")


class DimPerson(Base):
    __tablename__ = "dim_person"
    __table_args__ = {"schema": "gold"}

    person_key = Column(String, primary_key=True, index=True)
    person_id = Column(String, unique=True, index=True)
    person_name = Column(String, index=True)
    birth_year = Column(Integer)
    death_year = Column(Integer)
    birthplace = Column(String)
    biography = Column(String)

    # Relationships
    main_events = relationship("FactEvent", back_populates="main_person", lazy="selectin")
    person_events = relationship("FactPersonEvent", back_populates="person", lazy="selectin")


class DimMedia(Base):
    __tablename__ = "dim_media"
    __table_args__ = {"schema": "gold"}

    media_key = Column(String, primary_key=True, index=True)
    media_id = Column(String, unique=True, index=True)
    media_url = Column(String)
    media_type = Column(String, index=True)
    event_id = Column(String, index=True)      # FK mapping via fact_event
    event_name = Column(String)
    event_date = Column(String)

    # Relationships
    event_media = relationship("FactEventMedia", back_populates="media", lazy="selectin")


class SilverCity(Base):
    __tablename__ = "city_cleaned"
    __table_args__ = {"schema": "silver"}

    city_id = Column(String, primary_key=True, index=True)
    city_name = Column(String, index=True)
    lat = Column(Float)
    lng = Column(Float)

# ============================================================
# FACT TABLES
# ============================================================

class FactEvent(Base):
    __tablename__ = "fact_event"
    __table_args__ = {"schema": "gold"}

    event_key = Column(String, primary_key=True, index=True)
    event_id = Column(String, unique=True, index=True)
    event_name = Column(String, index=True)
    event_date = Column(String, index=True)
    event_type = Column(String, index=True)
    description = Column(String)

    # Foreign keys
    site_key = Column(String, ForeignKey("gold.dim_site.site_key"))
    main_person_key = Column(String, ForeignKey("gold.dim_person.person_key"))

    # Relationships
    site = relationship("DimSite", back_populates="events", lazy="joined")
    main_person = relationship("DimPerson", back_populates="main_events", lazy="joined")
    persons = relationship("FactPersonEvent", back_populates="event", lazy="selectin")
    media = relationship("FactEventMedia", back_populates="event", lazy="selectin")

# ============================================================
# BRIDGES / EXTENSIONS
# ============================================================

class FactPersonEvent(Base):
    __tablename__ = "fact_person_event"
    __table_args__ = {"schema": "gold"}

    person_event_key = Column(String, primary_key=True)
    person_key = Column(String, ForeignKey("gold.dim_person.person_key"))
    event_key = Column(String, ForeignKey("gold.fact_event.event_key"))
    role = Column(String)

    # Relationships
    person = relationship("DimPerson", back_populates="person_events", lazy="joined")
    event = relationship("FactEvent", back_populates="persons", lazy="joined")


class FactEventMedia(Base):
    __tablename__ = "fact_event_media"
    __table_args__ = {"schema": "gold"}

    media_key = Column(String, ForeignKey("gold.dim_media.media_key"), primary_key=True)
    event_key = Column(String, ForeignKey("gold.fact_event.event_key"), primary_key=True)

    # Relationships
    event = relationship("FactEvent", back_populates="media", lazy="joined")
    media = relationship("DimMedia", back_populates="event_media", lazy="joined")

# ============================================================
# Indexes for optimization
# ============================================================

Index("ix_fact_event_event_date", FactEvent.event_date)
Index("ix_fact_event_event_type", FactEvent.event_type)
Index("ix_dim_site_city_id", DimSite.city_id)
Index("ix_dim_media_media_type", DimMedia.media_type)
