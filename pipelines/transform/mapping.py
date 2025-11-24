import pandas as pd
import os
import re

BASE_DIR = "/Users/thanhmai/etl_pipeline test/data"

def main():
    # ====== 1️⃣ Load tất cả file ======
    print("📥 Đang load dữ liệu...", flush=True)
    events = pd.read_csv(os.path.join(BASE_DIR, "event_cleaned.csv"), sep=",")
    media = pd.read_csv(os.path.join(BASE_DIR, "media_cleaned.csv"), sep=",")

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
    # ====== 6️⃣ Lưu kết quả liên kết ======
    print("💾 Lưu kết quả liên kết...", flush=True)
    output_files = {
        "event_linked1.csv": events,
        "media_linked1.csv": media,
    }
    
    for name, df in output_files.items():
        out_path = os.path.join(BASE_DIR, name)
        df.to_csv(out_path, index=False)
        print(f"✅ Saved {name} ({len(df)} records)", flush=True)

    print("\n🎯 Hoàn tất liên kết các bảng!", flush=True)

if __name__ == "__main__":
    main()
