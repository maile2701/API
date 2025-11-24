"""
📌 link_entities.py
Liên kết dữ liệu giữa city → location → event → media → person_event.
"""

import pandas as pd
import os
import re

BASE_DIR = "/Users/thanhmai/etl_pipeline test/data"

def main():
    # ====== 1️⃣ Load tất cả file ======
    print("📥 Đang load dữ liệu...", flush=True)
    cities = pd.read_csv(os.path.join(BASE_DIR, "city.csv"), sep=",")
    locations = pd.read_csv(os.path.join(BASE_DIR, "location_cleaned.csv"), sep=",")
    persons = pd.read_csv(os.path.join(BASE_DIR, "person_cleaned.csv"), sep=",")
    events = pd.read_csv(os.path.join(BASE_DIR, "event_cleaned.csv"), sep=",")
    media = pd.read_excel(os.path.join(BASE_DIR, "media_cleaned.xlsx"))

    # ====== 2️⃣ Mapping city → location ======
    print("🔗 Mapping city → location...", flush=True)
    DEFAULT_CITY_ID = "973"  # Thay bằng city_id thực của Huế
    locations["city_id"] = None

    for i, loc in locations.iterrows():
        loc_name = str(loc.get("location_name", "")).lower()
        address = str(loc.get("address", "")).lower() if pd.notna(loc.get("address")) else ""
        matched = False

        for _, city in cities.iterrows():
            city_name = str(city["city_name"]).lower()
            if city_name in loc_name or city_name in address:
                locations.at[i, "city_id"] = city["city_id"]
                matched = True
                break

        # Nếu không tìm thấy hoặc address null → gán mặc định Huế
        if not matched or address.strip() == "":
            locations.at[i, "city_id"] = DEFAULT_CITY_ID

    print(f"✅ Hoàn tất mapping city → location ({locations['city_id'].isna().sum()} null còn lại)", flush=True)

    # ====== 3️⃣ Mapping location → event ======
    print("🔗 Mapping location → event...", flush=True)
    events["location_id"] = None
    for i, ev in events.iterrows():
        for _, loc in locations.iterrows():
            loc_name_lower = str(loc["location_name"]).lower()
            if pd.notna(ev.get("event_name")) and loc_name_lower in str(ev["event_name"]).lower():
                events.at[i, "location_id"] = loc["location_id"]
                break
            if pd.notna(ev.get("description")) and loc_name_lower in str(ev["description"]).lower():
                events.at[i, "location_id"] = loc["location_id"]
                break

    # ====== 4️⃣ Mapping event → media ======
    print("🔗 Mapping event → media...", flush=True)
    media["event_id"] = None

    # Nếu media có cột 'event_name' thì dùng nó để dò trực tiếp
    has_event_name = "event_name" in media.columns

    for i, m in media.iterrows():
        found = False
        m_event_name = str(m.get("event_name", "")).lower() if has_event_name else ""
        m_title = str(m.get("title", "")).lower()
        m_desc = str(m.get("description", "")).lower()

        for _, ev in events.iterrows():
            ev_name_lower = str(ev["event_name"]).lower()

            # Ưu tiên so khớp trực tiếp event_name
            if has_event_name and ev_name_lower in m_event_name:
                media.at[i, "event_id"] = ev["event_id"]
                found = True
                break

            # Sau đó mới fallback sang title và description
            if ev_name_lower in m_title or ev_name_lower in m_desc:
                media.at[i, "event_id"] = ev["event_id"]
                found = True
                break

        if not found:
            media.at[i, "event_id"] = None

    matched_count = media["event_id"].notna().sum()
    print(f"✅ Hoàn tất mapping event → media ({matched_count} matched)", flush=True)


    # ====== 5️⃣ Tạo bảng person_event ======
    print("🔗 Tạo bảng person_event...", flush=True)
    person_event_records = []
    for _, person in persons.iterrows():
        name_pattern = re.escape(str(person["person_name"]))
        matches = events[events["description"].str.contains(name_pattern, case=False, na=False)]
        for _, ev in matches.iterrows():
            record = {
                "person_event_id": f"PE_{len(person_event_records)+1:04d}",
                "person_id": person["person_id"],
                "event_id": ev["event_id"],
                "role": "Liên quan"
            }
            person_event_records.append(record)

    person_event = pd.DataFrame(person_event_records)

    # ====== 6️⃣ Lưu kết quả liên kết ======
    print("💾 Lưu kết quả liên kết...", flush=True)
    output_files = {
        "location_linked1.csv": locations,
        "event_linked1.csv": events,
        "media_linked1.csv": media,
        "person_event1.csv": person_event
    }

    for name, df in output_files.items():
        out_path = os.path.join(BASE_DIR, name)
        df.to_csv(out_path, index=False)
        print(f"✅ Saved {name} ({len(df)} records)", flush=True)

    print("\n🎯 Hoàn tất liên kết các bảng!", flush=True)

if __name__ == "__main__":
    main()
