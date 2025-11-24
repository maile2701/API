from sqlalchemy import create_engine, text  # <- cần import text

engine = create_engine("postgresql+psycopg2://thanhmai:Maile2718%40@localhost:5432/data warehouse")

with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))  # <- bọc SQL trong text()
    print(result.fetchone())
