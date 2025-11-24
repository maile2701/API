import pandas as pd
import unicodedata
import re

df = pd.read_csv('/content/Event_cap2 - event_cap2.csv')


print("✅ Đọc file thành công! Số dòng:", len(df))
print("Tên các cột thực tế là:", df.columns)

def clean_and_summarize(text):
    if pd.isna(text):
        return ""
    text = str(text)

    # Chuẩn hoá Unicode
    text = unicodedata.normalize("NFC", text)

    # Xóa ký tự điều khiển, rác
    text = re.sub(r"[\x00-\x1f\x7fÂ€‹ï¿½]+", " ", text)

    # Xóa các nhãn không cần thiết ở đầu
    text = re.sub(r"(?i)\b(mô tả|kết quả|diễn biến|nội dung|tóm tắt)[:：]?\s*", "", text)

    # Chuẩn hóa khoảng trắng
    text = re.sub(r"\s+", " ", text).strip()

    # Xóa các dấu câu lặp lại
    text = re.sub(r"([,.!?]){2,}", r"\1", text)

    # --- Tóm tắt nội dung ---
    # Tách câu và giữ lại dấu câu
    sentences = re.split(r"([.!?])", text)
    if len(sentences) > 1:
        # Ghép lại câu và dấu câu
        sentences = [sentences[i] + (sentences[i+1] if i+1 < len(sentences) else '')
                     for i in range(0, len(sentences), 2)]
    else:
        sentences = [s for s in sentences if s.strip()]

    # Lọc các câu "key"
    key_sentences = [
        s.strip()
        for s in sentences
        if re.search(
            r"(xây|thành lập|diễn ra|trở thành|được|chiến thắng|bắt đầu|xảy ra|xây dựng|khởi công|khánh thành)",
            s,
            re.IGNORECASE,
        )
        and len(s.split()) > 4
    ]

    summary = " ".join(key_sentences)

    # Nếu không tìm thấy câu "key", lấy 2 câu đầu tiên
    if not summary.strip():
        summary = " ".join(s.strip() for s in sentences[:2] if s.strip())

    summary = re.sub(r"\s*([,.!?])\s*", r"\1 ", summary).strip()
    if summary:
        summary = summary[0].upper() + summary[1:]

    return summary

def normalize_date(text):
    if not isinstance(text, str):
        return None
    text = text.strip()
    text = re.sub(r'(\d{1,2})\s*tháng\s*(\d{1,2})\s*năm\s*(\d{4})', r'\1/\2/\3', text)
    text = re.sub(r'(\d{1,2})\s*/\s*(\d{1,2})\s*/\s*(\d{4})', r'\1/\2/\3', text)
    text = re.sub(r'^năm\s*(\d{4})$', r'\1', text, flags=re.IGNORECASE)
    return text if text else None

if 'description' in df.columns:
    print("Đang tóm tắt cột 'description'...")
    df['description_clean'] = df['description'].apply(clean_and_summarize)
    # Thay thế cột cũ
    df = df.drop(columns=['description'])
    df = df.rename(columns={'description_clean': 'description'})
else:
    print("Không tìm thấy cột 'description' để tóm tắt.")

if 'event_date' in df.columns:
    print("Đang chuẩn hóa cột 'event_date'...")
    df['event_date_clean'] = df['event_date'].apply(normalize_date)
    # Thay thế cột cũ
    df = df.drop(columns=['event_date'])
    df = df.rename(columns={'event_date_clean': 'event_date'})

df['event_id'] = (df.index + 1).map(lambda x: f"event_{x:03d}")

def classify_event(name):
    if pd.isna(name):
        return ""
    name_lower = name.lower()
    if any(k in name_lower for k in ["cầu", "lễ hội", "festival"]):
        return "Du lịch"
    else:
        return "Lịch sử"

df["event_type"] = df["event_name"].apply(classify_event)

final_columns_order = [
    'event_id',
    'event_name',
    'event_date',
    'event_location',
    'person',
    'description',
    'event_type',
    'url'
]

existing_cols = [col for col in final_columns_order if col in df.columns]
remaining_cols = [col for col in df.columns if col not in existing_cols]
df = df[existing_cols + remaining_cols]

# Xóa khoảng trắng đầu-cuối trong các ô
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Xóa cột trống hoàn toàn
df = df.dropna(axis=1, how='all')

# Xóa hàng trống hoàn toàn
df = df.dropna(axis=0, how='all')

# Xóa hàng trùng lặp
df = df.drop_duplicates()

df.to_csv("/content/event_cleaned.csv", index=False, encoding="utf-8-sig")
