import pandas as pd
import numpy as np
import re

INPUT_FILE = "/Users/thanhmai/etl_pipeline test/data/locations_final.csv"
OUTPUT_FILE = "/Users/thanhmai/etl_pipeline test/data/locations_cleaned.csv"

df = pd.read_csv(INPUT_FILE)

df = df.applymap(lambda x: str(x).strip() if isinstance(x, str) else x)
df.replace(["nan", "NaN", "None", "NULL", "null", ""], np.nan, inplace=True)

df["location_name"] = df["location_name"].astype(str)
df["location_name"] = df["location_name"].str.replace(r"\s+", " ", regex=True)
df["location_name"] = df["location_name"].str.strip().str.title()

df["location_description"] = df["location_description"].astype(str)
df["location_description"] = df["location_description"].apply(
    lambda x: re.sub(r"\s+", " ", x.strip()) if isinstance(x, str) else x
)

df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

df.drop_duplicates(subset=["location_name"], inplace=True)
df.drop_duplicates(subset=["latitude", "longitude"], inplace=True)

def valid_lat(lat):
    return isinstance(lat, (float, int)) and -90 <= lat <= 90

def valid_lon(lon):
    return isinstance(lon, (float, int)) and 90 <= lon <= 110  # phạm vi VN

invalid_coords = df[~df["latitude"].apply(valid_lat) | ~df["longitude"].apply(valid_lon)]
if not invalid_coords.empty:
    print("Có tọa độ không hợp lệ (bạn nên kiểm tra lại):")
    print(invalid_coords[["location_name", "latitude", "longitude"]])

df = df[df["latitude"].apply(valid_lat) & df["longitude"].apply(valid_lon)]

df["location_description"] = df["location_description"].apply(lambda x: x[:1000] if isinstance(x, str) else x)

print("\n Thống kê giá trị null:")
print(df.isnull().sum())

df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
print(f"\n Dữ liệu đã chuẩn hóa xong! File lưu tại: {OUTPUT_FILE}")
