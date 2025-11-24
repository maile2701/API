#!/usr/bin/env python
# coding: utf-8

# 

# In[4]:


import requests
from bs4 import BeautifulSoup
import csv
import time
import pandas as pd
import re


# In[5]:


urls = [
    "https://vi.wikipedia.org/wiki/Ch%C3%B9a_Thi%C3%AAn_M%E1%BB%A5",
    "https://vi.wikipedia.org/wiki/C%E1%BB%91_%C4%91%C3%B4_Hu%E1%BA%BF",
    "https://vi.wikipedia.org/wiki/L%C4%83ng_Gia_Long",
    "https://vi.wikipedia.org/wiki/L%C4%83ng_Minh_M%E1%BA%A1ng",
    "https://vi.wikipedia.org/wiki/L%C4%83ng_T%E1%BB%B1_%C4%90%E1%BB%A9c",
    "https://vi.wikipedia.org/wiki/Tr%E1%BA%ADn_C%E1%BB%ADa_Thu%E1%BA%ADn_An",
    "https://vi.wikipedia.org/wiki/Phong_tr%C3%A0o_C%E1%BA%A7n_V%C6%B0%C6%A1ng",
    "https://vi.wikipedia.org/wiki/C%E1%BA%A7u_Tr%C6%B0%E1%BB%9Dng_Ti%E1%BB%81n",
    "https://vi.wikipedia.org/wiki/V%E1%BB%A5_m%C6%B0u_kh%E1%BB%9Fi_ngh%C4%A9a_%E1%BB%9F_Hu%E1%BA%BF_(1916)",
    "https://vi.wikipedia.org/wiki/S%E1%BB%B1_ki%E1%BB%87n_T%E1%BA%BFt_M%E1%BA%ADu_Th%C3%A2n",
    "https://vi.wikipedia.org/wiki/Festival_Hu%E1%BA%BF"
]
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
filepath = "/Users/thanhmai/etl_pipeline test/data/wiki_data_Hue.csv"

# In[6]:


with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    writer.writerow(["Tên trang", "Thuộc tính", "Giá trị"])


# In[7]:


def crawl_wiki(url):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    # --- Tên trang ---
    title_tag = soup.find("h1", id="firstHeading")
    title = title_tag.text.strip() if title_tag else "Không rõ tiêu đề"
    rows = []
    # --- Crawl Infobox ---
    infobox = soup.find("table", class_=lambda x: x and "infobox" in x)
    if infobox:
        for row in infobox.find_all("tr"):
            label = row.find("th")
            value = row.find("td")
            if label and value:
                key = label.get_text(" ", strip=True)
                val = value.get_text(" ", strip=True)
                rows.append([title, key, val])
    # --- Crawl phần "Lịch sử" hoặc "Hình thành" ---
    description = ""
    for header in soup.find_all(["h2", "h3"]):
        section_title = header.get_text(" ", strip=True).lower()
        if any(keyword in section_title for keyword in ["lịch sử", "hình thành", "khởi lập", "quá trình xây dựng"]):
            content = []
            for sibling in header.find_next_siblings():
                if sibling.name in ["h2", "h3"]:
                    break
                if sibling.name == "p":
                    content.append(sibling.get_text(" ", strip=True))
            description = "\n".join(content).strip()
            break
    if not description and infobox:
        first_p_after_infobox = None
        # Lặp qua các thẻ sau infobox để tìm thẻ <p> đầu tiên
        for sibling in infobox.find_next_siblings():
            if sibling.name == 'p':
                first_p_after_infobox = sibling
                break # Đã tìm thấy
        if first_p_after_infobox:
            description = first_p_after_infobox.get_text(" ", strip=True)
    if not description:
        first_p = soup.find("p")
        if first_p:
            description = first_p.get_text(" ", strip=True)
    # Làm sạch ký hiệu [1], [2], [3]
    for j in range(1, 500):
        description = description.replace(f"[{j}]", "")
    # Ghi dữ liệu
    rows.append([title, "Mô tả", description])
    with open(filepath, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    print(f"✅ Đã lưu {title} ({len(rows)} dòng).")


# In[8]:


for url in urls:
    try:
        crawl_wiki(url)
        time.sleep(2)
    except Exception as e:
        print(f"❌ Lỗi khi crawl {url}: {e}")

print(f"\n🎉 Hoàn tất! Dữ liệu đã được lưu trong file: {filepath}")


# In[9]:


df = pd.read_csv('/Users/thanhmai/etl_pipeline test/data/wiki_data_Hue.csv')


# In[ ]:


DATE_ATTRS = [
    'Khởi lập', 'Xây dựng', 'Ngày xây dựng', 'Thời gian', 'Hoàn thành'
]
PERSON_ATTRS = [
    'Người sáng lập', 'Xây dựng bởi', 'Xây dựng\xa0bởi',
    'Người xây dựng', 'Tổng thầu'
]
LOCATION_ATTRS = [
    'Quốc gia', 'Địa chỉ', 'Vị trí', 'Địa điểm', 'Bắc qua', 'Tọa độ'
]
DESCRIPTION_ATTRS = [
    'Mô tả','Kết quả'
]

# --- 2. ĐỌC FILE TỪ COLAB SESSION ---
file_name = '/Users/thanhmai/etl_pipeline test/data/wiki_data_Hue.csv'

try:
    df_raw = pd.read_csv(file_name)
    print(f"--- Đã đọc file '{file_name}' thành công ---")

    # --- 3. ĐỊNH NGHĨA HÀM BIẾN ĐỔI CHUẨN HÓA (Đã cập nhật) ---
    def transform_group(group):
        record = {
            'event_name': group['Tên trang'].iloc[0],
            'description': [],
            'event_date': None,
            'event_location': [],
            'person': None
        }

        for _, row in group.iterrows():
            if 'Thuộc tính' not in row or 'Giá trị' not in row:
                continue

            attr = str(row['Thuộc tính'])
            val = str(row['Giá trị'])
            if attr in DATE_ATTRS:
                match = re.match(r'(\d+)\s+(.*)', val)
                if match:
                    record['event_date'] = match.group(1) # '1802'
                    record['event_location'].append(match.group(2)) # 'Phú Xuân'
                else:
                    record['event_date'] = val

            elif attr in PERSON_ATTRS:
                record['person'] = val

            elif attr in LOCATION_ATTRS:
                record['event_location'].append(val)

            elif attr in DESCRIPTION_ATTRS:
                record['description'].append(f"{attr}: {val}")

        record['description'] = ". ".join(record['description'])
        record['event_location'] = ", ".join(record['event_location'])

        return pd.Series(record)

    # --- 4. ÁP DỤNG HÀM BIẾN ĐỔI ---
    if 'Tên trang' not in df_raw.columns:
        print("LỖI: File của bạn không có cột 'Tên trang'.")
        raise ValueError("Missing required column: 'Tên trang'")

    df_clean = df_raw.groupby('Tên trang').apply(transform_group).reset_index(drop=True)

    print("\n" + "="*30 + "\n")
    print("--- DỮ LIỆU ĐÃ CHUẨN HÓA (HOÀN CHỈNH) ---")
    print(df_clean)

    # --- 5. LƯU FILE KẾT QUẢ ---
    output_file = "/Users/thanhmai/etl_pipeline test/data/Wiki_Hue_chuyendoi.csv"
    df_clean.to_csv(output_file, index=False)
    print(f"\nĐã lưu file chuẩn hóa tại: {output_file}")
except FileNotFoundError:
    print(f"LỖI: Không tìm thấy file tên là '{file_name}'")
except Exception as e:
    print(f"Đã xảy ra lỗi khi đọc hoặc xử lý file: {e}")


# Các lễ hội khác không có trên wiki

# In[ ]:


event_links = [
    "https://khamphahue.com.vn/Van-hoa/Chi-tiet/tid/Le-hoi-Du-Tien.html/pid/14959/cid/198",
    "https://khamphahue.com.vn/Van-hoa/Chi-tiet/tid/Le-Hoi-Dien-Hon-Chen.html/pid/3036/cid/198",
    "https://khamphahue.com.vn/Van-hoa/Chi-tiet/cid/198/pid/5780",
    "https://khamphahue.com.vn/Van-hoa/Chi-tiet/tid/Le-hoi-den-Huyen-Tran-Cong-chua.html/pid/1396/cid/198"
]


# In[ ]:


def crawl_event_detail(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
    except Exception as e:
        print(f"⚠️ Lỗi khi tải {url}: {e}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    # --- Tên sự kiện ---
    title = soup.find("h1").text.strip() if soup.find("h1") else "Không rõ"

    # --- Thời gian ---
    time_tag = soup.find("span", id="dnn_ctr1099_ViewTinBai_DsChuyenMucTinBai_lblTableThoiGian")
    event_date = time_tag.text.strip() if time_tag else "Không có thông tin thời gian"

    # --- Địa chỉ ---
    address_tag = soup.find("span", id="dnn_ctr1099_ViewTinBai_DsChuyenMucTinBai_lblTableDiaChi")
    event_location = address_tag.text.strip() if address_tag else "Không có thông tin địa chỉ"

    # --- MÔ TẢ:
    desc_box = soup.find("div", id="dnn_ctr1099_ViewTinBai_DsChuyenMucTinBai_groupGioiThieu")
    content_box = soup.find("div", class_="content-text-top")

    description = ""
    if desc_box:
        paragraphs = [p.get_text(" ", strip=True) for p in desc_box.find_all("p")]
        description = "\n".join(paragraphs)
    elif content_box:
        paragraphs = [p.get_text(" ", strip=True) for p in content_box.find_all("p")]
        description = "\n".join(paragraphs)
    else:
        description = "Không có mô tả"

    return {
        "event_name": title,
        "description": description,
        "event_date": event_date,
        "event_location": event_location,
        "url": url
    }


# In[ ]:


all_events = []
for i, link in enumerate(event_links, start=1):
    print(f"({i}/{len(event_links)}) Đang crawl: {link}")
    data = crawl_event_detail(link)
    if data:
        all_events.append(data)
    time.sleep(2)


# In[ ]:


import pandas as pd

df = pd.DataFrame(all_events)
df.to_csv("/Users/thanhmai/etl_pipeline test/data/hue_festivals.csv", index=False, encoding="utf-8-sig")

print("✅ Đã crawl xong và lưu vào hue_festivals.csv")
print(df.head())


# event Đà Nẵng

# In[ ]:


urls = [
    "https://vi.wikipedia.org/wiki/Tr%E1%BA%ADn_%C4%90%C3%A0_N%E1%BA%B5ng_(1859%E2%80%931860)",
    "https://vi.wikipedia.org/wiki/Chi%E1%BA%BFn_d%E1%BB%8Bch_Hu%E1%BA%BF_%E2%80%93_%C4%90%C3%A0_N%E1%BA%B5ng",
    "https://vi.wikipedia.org/wiki/B%C3%A0_N%C3%A0",
    "https://vi.wikipedia.org/wiki/Ch%C3%B9a_Linh_%E1%BB%A8ng",
    "https://vi.wikipedia.org/wiki/C%E1%BA%A7u_S%C3%B4ng_H%C3%A0n",
    "https://vi.wikipedia.org/wiki/C%E1%BA%A7u_Tr%E1%BA%A7n_Th%E1%BB%8B_L%C3%BD",
    "https://vi.wikipedia.org/wiki/C%E1%BA%A7u_R%E1%BB%93ng",
    "https://vi.wikipedia.org/wiki/C%E1%BA%A7u_Thu%E1%BA%ADn_Ph%C6%B0%E1%BB%9Bc"
]
headers = {"User-Agent": "Mozilla/5.0"}
filepath = "/Users/thanhmai/etl_pipeline test/data/wikipedia_data_DN.csv"


# In[ ]:


with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    writer.writerow(["Tên trang", "Thuộc tính", "Giá trị"])


# In[ ]:


def crawl_wiki(url):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    title_tag = soup.find("h1", id="firstHeading")
    title = title_tag.text.strip() if title_tag else "Không rõ tiêu đề"
    rows = []
    # --- Crawl Infobox ---
    infobox = soup.find("table", class_=lambda x: x and "infobox" in x)
    if infobox:
        for row in infobox.find_all("tr"):
            label = row.find("th")
            value = row.find("td")
            if label and value:
                key = label.get_text(" ", strip=True)
                val = value.get_text(" ", strip=True)
                rows.append([title, key, val])
    # --- Crawl phần "Lịch sử" hoặc "Hình thành" ---
    description = ""
    for header in soup.find_all(["h2", "h3"]):
        section_title = header.get_text(" ", strip=True).lower()
        if any(keyword in section_title for keyword in ["lịch sử", "hình thành", "khởi lập", "quá trình xây dựng"]):
            content = []
            for sibling in header.find_next_siblings():
                if sibling.name in ["h2", "h3"]:
                    break
                if sibling.name == "p":
                    content.append(sibling.get_text(" ", strip=True))
            description = "\n".join(content).strip()
            break
    if not description and infobox:
        first_p_after_infobox = None
        # Lặp qua các thẻ sau infobox để tìm thẻ <p> đầu tiên
        for sibling in infobox.find_next_siblings():
            if sibling.name == 'p':
                first_p_after_infobox = sibling
                break
        if first_p_after_infobox:
            description = first_p_after_infobox.get_text(" ", strip=True)
    if not description:
        first_p = soup.find("p")
        if first_p:
            description = first_p.get_text(" ", strip=True)

    for j in range(1, 500):
        description = description.replace(f"[{j}]", "")
    # Ghi dữ liệu
    rows.append([title, "Mô tả", description])
    with open(filepath, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    print(f"✅ Đã lưu {title} ({len(rows)} dòng).")


# In[ ]:


for url in urls:
    try:
        crawl_wiki(url)
        time.sleep(2)
    except Exception as e:
        print(f"❌ Lỗi khi crawl {url}: {e}")

print(f"\n🎉 Hoàn tất! Dữ liệu đã được lưu trong file: {filepath}")


# In[ ]:


import pandas as pd
import re

# === 1. Đọc file gốc ===
file_path = "/Users/thanhmai/etl_pipeline test/data/wikipedia_data_DN.csv"
df = pd.read_csv(file_path)
print("✅ Đọc file thành công! Số dòng:", len(df))

# === 2. Định nghĩa nhóm thuộc tính ===
DATE_START_ATTRS = ['Thời gian khởi công', 'Khởi công']
DATE_END_ATTRS = ['Thời gian hoàn thành', 'Hoàn thành', 'Thông xe']
LOCATION_ATTRS = ['Địa điểm', 'Vị trí', 'Bắc qua', 'Tọa độ']
PERSON_ATTRS = ['Người thiết kế', 'Người sáng lập', 'Xây dựng bởi', 'Tổng thầu']
DESCRIPTION_ATTRS = ['Mô tả', 'Kết quả', 'Thông tin']

def normalize_date(text):
    text = str(text).strip()
    text = re.sub(r'(\d{1,2})\s*tháng\s*(\d{1,2})\s*năm\s*(\d{4})', r'\1/\2/\3', text)
    text = re.sub(r'(\d{1,2})\s*/\s*(\d{1,2})\s*/\s*(\d{4})', r'\1/\2/\3', text)
    return text if text else None

def transform_group(group):
    record = {
        'event_name': group['Tên trang'].iloc[0],
        'description': [],
        'event_date_start': None,
        'event_date_end': None,
        'event_location': [],
        'person': None
    }

    for _, row in group.iterrows():
        attr = str(row['Thuộc tính']).strip()
        val = str(row['Giá trị']).strip()

        val_normalized = normalize_date(val)

        if attr in DATE_START_ATTRS and val_normalized:
            record['event_date_start'] = val_normalized
        elif attr in DATE_END_ATTRS and val_normalized:
            record['event_date_end'] = val_normalized
        elif attr in LOCATION_ATTRS:
            record['event_location'].append(val)
        elif attr in PERSON_ATTRS:
            record['person'] = val
        elif attr in DESCRIPTION_ATTRS:
            val = re.sub(r'^(Mô tả|Kết quả|Thông tin)\s*:\s*', '', val)
            record['description'].append(val)

    record['event_location'] = ", ".join(record['event_location'])
    record['description'] = ". ".join(record['description'])

    # Gộp ngày khởi công và hoàn thành
    if record['event_date_start'] and record['event_date_end']:
        record['event_date'] = f"{record['event_date_start']} - {record['event_date_end']}"
    elif record['event_date_start']:
        record['event_date'] = record['event_date_start']
    elif record['event_date_end']:
        record['event_date'] = record['event_date_end']
    else:
        record['event_date'] = None

    return pd.Series({
        'event_name': record['event_name'],
        'description': record['description'],
        'event_date': record['event_date'],
        'event_location': record['event_location'],
        'person': record['person']
    })

# === 5. Gom nhóm theo Tên trang ===
df_clean = df.groupby('Tên trang').apply(transform_group).reset_index(drop=True)

# === 6. Xuất kết quả ===
output_file = "/Users/thanhmai/etl_pipeline test/data/wikipedia_data_DN_clean.csv"
df_clean.to_csv(output_file, index=False, encoding='utf-8-sig')

print(f"\n✅ Đã lưu file chuẩn hóa tại: {output_file}")
print(df_clean.head(10))


# In[ ]:


import pandas as pd

# 1. Đọc 2 file (hoặc nhiều hơn)
df1 = pd.read_csv(f"/Users/thanhmai/etl_pipeline test/data/Wiki_Hue_chuyendoi.csv")
df2 = pd.read_csv("/Users/thanhmai/etl_pipeline test/data/hue_festivals.csv")
df3 = pd.read_csv("/Users/thanhmai/etl_pipeline test/data/wikipedia_data_DN_clean.csv")

# 2. Gộp chúng lại thành một danh sách
danh_sach_dfs = [df1, df2,df3]

# 3. Dùng pd.concat để nối
event_cap2 = pd.concat(danh_sach_dfs, ignore_index=True)
print(event_cap2)

# 5. Lưu file
event_cap2.to_csv("/Users/thanhmai/etl_pipeline test/data/event_raw.csv", index=False)

