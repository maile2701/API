import pandas as pd
import re
import os

def extract_year(text):
    """Trích xuất năm (4 chữ số) đầu tiên tìm được"""
    if pd.isna(text):
        return None
    match = re.search(r"(1[0-9]{3}|20[0-9]{2})", str(text))
    return int(match.group(0)) if match else None

def extract_birthplace_from_text(text):
    """
    Cố gắng trích xuất 'nơi sinh' từ nội dung tiểu sử hoặc thông tin gốc.
    Ví dụ: 'Ông sinh ra tại Thanh Hóa' hoặc 'sinh ở Huế' -> 'Thanh Hóa' / 'Huế'
    """
    if pd.isna(text):
        return None

    text = str(text)

    # Một số mẫu phổ biến tiếng Việt
    patterns = [
        r"sinh ra tại\s+([A-ZĐ][^.,;0-9]+)",
        r"sinh ở\s+([A-ZĐ][^.,;0-9]+)",
        r"sinh tại\s+([A-ZĐ][^.,;0-9]+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            place = match.group(1).strip()
            # Loại bỏ phần dư (như "tỉnh", "Việt Nam" nếu cần)
            place = re.split(r"[.,;]", place)[0]
            return place.strip()
    return None


def transform_person(input_path="/Users/thanhmai/etl_pipeline test/data/person_raw.csv", output_path="data/person_cleaned1.csv"):
    """
    Transform - Chuẩn hoá dữ liệu bảng Person:
    - Đọc file raw_person.csv
    - Chuẩn hoá năm sinh, năm mất -> YYYY (int)
    - Trích xuất nơi sinh từ text nếu thiếu
    - Loại bỏ trùng lặp, bản ghi lỗi
    - Gán person_id tự động
    - Xuất ra file cleaned
    """

    print("=== TRANSFORM PERSON START ===")

    # ===== 1️⃣ Đọc file raw =====
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Không tìm thấy file: {input_path}")
    df_person = pd.read_csv(input_path)
    print(f"[Read] Loaded {len(df_person)} records từ {input_path}")

    # ===== 2️⃣ Chuẩn hoá năm sinh / năm mất =====
    df_person["birth_year"] = df_person["birth_year"].apply(extract_year)
    df_person["death_year"] = df_person["death_year"].apply(extract_year)

    # Loại bỏ phần .0 nếu có (do float)
    df_person["birth_year"] = df_person["birth_year"].astype("Int64")
    df_person["death_year"] = df_person["death_year"].astype("Int64")

    # ===== 3️⃣ Chuẩn hoá text =====
    df_person["person_name"] = df_person["person_name"].astype(str).str.strip()

    # ===== 4️⃣ Trích xuất birthplace nếu thiếu =====
    if "birthplace" not in df_person.columns:
        df_person["birthplace"] = None

    missing_bp = df_person["birthplace"].isna() | (df_person["birthplace"].astype(str).str.strip() == "")
    df_person.loc[missing_bp, "birthplace"] = df_person.loc[missing_bp, "biography"].apply(extract_birthplace_from_text)

    # ===== 5️⃣ Loại bỏ bản ghi trùng / lỗi =====
    df_person = df_person.drop_duplicates(subset=["person_name", "url"], keep="first")
    df_person = df_person.dropna(subset=["person_name"])
    df_person = df_person[df_person["birth_year"].notna()]

    # ===== 6️⃣ Gán Person ID =====
    df_person = df_person.reset_index(drop=True)
    df_person["person_id"] = [f"PS_{str(i+1).zfill(3)}" for i in range(len(df_person))]

    # ===== 7️⃣ Xuất file cleaned =====
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_person.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"[Transform] Person table cleaned: {len(df_person)} bản ghi hợp lệ.")
    print(f"[Output] File saved to {output_path}")
    print("=== TRANSFORM PERSON DONE ===")

    return df_person


if __name__ == "__main__":
    transform_person(input_path="/Users/thanhmai/etl_pipeline test/data/person_raw.csv")
