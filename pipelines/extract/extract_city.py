import csv
import os

# ✅ Đường dẫn đến thư mục data trong dự án etl_pipeline_test
BASE_DIR = "/Users/thanhmai/etl_pipeline test/data"
os.makedirs(BASE_DIR, exist_ok=True)

# ✅ Đường dẫn file CSV đầu ra
file_path = os.path.join(BASE_DIR, "city.csv")

# ✅ Dữ liệu thành phố
cities = [
    ("Bình Phước", 953),
    ("Bình Thuận", 954),
    ("TP.HCM", 955),
    ("Đồng Nai", 956),
    ("Bình Định", 957),
    ("Khánh Hòa", 958),
    ("Phú Yên", 959),
    ("Đắk Lắk", 960),
    ("Gia Lai", 961),
    ("Kon Tum", 962),
    ("Vĩnh Phúc", 963),
    ("Thái Nguyên", 964),
    ("Hà Nội", 965),
    ("Hải Phòng", 966),
    ("Quảng Ninh", 967),
    ("Thanh Hóa", 968),
    ("Nghệ An", 969),
    ("Hà Tĩnh", 970),
    ("Quảng Bình", 971),
    ("Quảng Trị", 972),
    ("Thừa Thiên Huế", 973),
    ("Đà Nẵng", 974),
    ("Cần Thơ", 975),
    ("Vĩnh Long", 976),
    ("An Giang", 977),
    ("Đồng Tháp", 978),
    ("Hậu Giang", 979),
    ("Kiên Giang", 980),
    ("Long An", 981),
    ("Sóc Trăng", 982),
    ("Tiền Giang", 983),
    ("Trà Vinh", 984),
    ("Cà Mau", 985),
    ("Bạc Liêu", 986)
]

# ✅ Ghi ra file CSV
with open(file_path, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["city_name", "city_id"])
    writer.writerows(cities)

print(f"✅ File 'city.csv' đã được tạo thành công tại: {file_path}")
