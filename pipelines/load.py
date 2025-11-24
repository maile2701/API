"""
📦 load_all_entities.py
Tự động nạp toàn bộ dữ liệu liên kết (city, location, event, media, person, person_event) vào PostgreSQL.
"""

import psycopg2
import pandas as pd
import os

# ==========================
# 1️⃣ Cấu hình kết nối database
# ==========================
DB_NAME = "digital_maps_test"
DB_USER = "thanhmai"
DB_PASS = "Maile2718@"
DB_HOST = "localhost"
DB_PORT = "5432"

# ==========================
# 2️⃣ Thư mục dữ liệu
# ==========================
DATA_DIR = "/Users/thanhmai/etl_pipeline test/data"

# ==========================
# 3️⃣ Mapping file → bảng PostgreSQL
# ==========================
file_table_map = {
    "location_linked1.csv": "my_schema.location",
    "event_linked1.csv": "my_schema.event",
    "media_linked1.csv": "my_schema.media",
    "person_cleaned.csv": "my_schema.person",
    "person_event1.csv": "my_schema.person_event"
}

# ==========================
# 4️⃣ Kết nối database
# ==========================
conn = psycopg2.connect(
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT
)
cur = conn.cursor()

# ==========================
# 5️⃣ Hàm load file
# ==========================
def load_to_postgres(filepath, table):
    ext = os.path.splitext(filepath)[1].lower()

    # đọc file
    if ext in [".xlsx", ".xls"]:
        df = pd.read_excel(filepath)
    else:
        df = pd.read_csv(filepath, sep=",", engine="python", on_bad_lines="skip")

    df.columns = df.columns.str.strip()

    placeholders = ", ".join(["%s"] * len(df.columns))
    col_names = ", ".join(df.columns)

    insert_query = f"""
        INSERT INTO {table} ({col_names})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING;
    """

    print(f"📤 Loading {len(df)} rows → {table}")

    for _, row in df.iterrows():
        values = []
        for v in row:
            if pd.isna(v):
                values.append(None)
            else:
                values.append(v)
        cur.execute(insert_query, tuple(values))

    conn.commit()
    print(f"✅ Done: {table}\n")

# ==========================
# 6️⃣ Thực thi load
# ==========================
for filename, table in file_table_map.items():
    filepath = os.path.join(DATA_DIR, filename)
    if os.path.exists(filepath):
        try:
            load_to_postgres(filepath, table)
        except Exception as e:
            print(f"❌ Lỗi khi load {filename}: {e}\n")
    else:
        print(f"⚠️ Không tìm thấy file {filename}")

# ==========================
# 7️⃣ Đóng kết nối
# ==========================
cur.close()
conn.close()

print("🎉 Tất cả dữ liệu đã được nạp vào PostgreSQL thành công!")
