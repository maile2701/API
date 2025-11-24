import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time
from geopy.geocoders import Nominatim  

INPUT_FILE = "/Users/thanhmai/etl_pipeline test/data/locations_list.csv"
OUTPUT_FILE = "/Users/thanhmai/etl_pipeline test/data/location_raw.csv"

def clean_text(text):
    """Loại bỏ chú thích [1], [2], ký tự thừa"""
    text = re.sub(r"\[[0-9]+\]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def get_page_description(url):
    """Cào mô tả từ Wikipedia hoặc các trang khác"""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        res = requests.get(url, timeout=12, headers=headers)
        res.encoding = "utf-8"
        soup = BeautifulSoup(res.text, "html.parser")

        title = soup.find("h1")
        location_name = title.text.strip() if title else ""

        paragraphs = soup.select("p")
        description_parts = []
        for p in paragraphs[:3]:
            text = p.get_text(" ", strip=True)
            if text and len(text.split()) > 8:
                description_parts.append(text)
        description = " ".join(description_parts)

        if not description:
            for selector in [".main-content", ".detail-content", ".content-article", ".entry-content"]:
                div = soup.select_one(selector)
                if div:
                    text = div.get_text(" ", strip=True)
                    if len(text) > 50:
                        description = text[:800]
                        break

        if not description:
            meta = soup.find("meta", {"name": "description"})
            if meta:
                description = meta.get("content", "")

        return location_name, clean_text(description)

    except Exception as e:
        print(f"⚠️ Lỗi khi cào {url}: {e}")
        return "", ""

def clean_description(text):
    if not isinstance(text, str):
        return ""
    text = re.sub(r"\[[0-9]+\]", "", text)
    text = re.sub(r"[•●▪]", "", text)
    text = re.sub(r"(?<=[a-zàáâãèéêìíòóôõùúăđĩũơư])(?=[A-ZÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚĂĐĨŨƠƯ])", " ", text)
    text = re.sub(r"\.(?=[A-ZÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚĂĐĨŨƠƯ])", ". ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

if __name__ == "__main__":
    df = pd.read_csv(INPUT_FILE)
    results = []
    geolocator = Nominatim(user_agent="vietnam_timeline_project")

    for idx, row in df.iterrows():
        name_input = row["location_name"]
        url = row["wikipedia_url"]
        print(f" Đang cào: {name_input} ...")

        name, desc = get_page_description(url)

        try:
            print(f" Đang tìm địa chỉ OSM cho: {name_input}")
            location = geolocator.geocode(f"{name_input}, Việt Nam", timeout=10)
            address = location.address if location else ""
        except Exception as e:
            print(f" Lỗi lấy địa chỉ {name_input}: {e}")
            address = ""

        results.append({
            "location_name": name_input,
            "location_description": desc,
            "longitude": "",  
            "latitude": "",    
            "address": address,
            "location_type": ""
        })

        time.sleep(1)

    out_df = pd.DataFrame(results)
    out_df["location_description"] = out_df["location_description"].apply(clean_description)

    out_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"\n File lưu tại: {OUTPUT_FILE}")
