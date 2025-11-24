from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Thay thông tin database của bạn
DB_USER = "thanhmai"
DB_PASSWORD = "Maile2718@"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "data_warehouse"

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Tạo engine
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Helper query
def execute_query(query: str):
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return [dict(row) for row in result]
