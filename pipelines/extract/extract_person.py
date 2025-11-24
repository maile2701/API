from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from bs4 import BeautifulSoup
import pandas as pd
import time, os, re, csv
from prefect import task

# ==========================
# 🧠 STEP 1: Khởi tạo Driver
# ==========================
def init_driver(headless=False, window_size="1366,768"):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument(f"--window-size={window_size}")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    return driver


# ==========================
# 🧠 STEP 2: Cào link Wikipedia qua Google
# ==========================
def find_wikipedia_links(topic: str, driver) -> list:
    wait = WebDriverWait(driver, 10)
    query = topic.replace(" ", "+")
    search_url = f"https://www.google.com/search?q={query}+wikipedia"
    driver.get(search_url)
    time.sleep(1)

    # Đóng hộp thoại chấp nhận cookie nếu có
    try:
        consent_button = wait.until(EC.element_to_be_clickable((
            By.XPATH,
            "//button//*[contains(text(),'I agree') or contains(text(),'Tôi') or contains(text(),'Chấp nhận')]/.."
        )))
        consent_button.click()
        time.sleep(1)
    except Exception:
        pass

    elems = driver.find_elements(By.XPATH, "//a[@href]")
    hrefs = [e.get_attribute("href") for e in elems if e.get_attribute("href")]

    wiki_links = []
    for h in hrefs:
        if "wikipedia.org/wiki/" in h:
            if "/url?q=" in h:
                h = h.split("/url?q=")[1].split("&")[0]
            if h.startswith("http"):
                wiki_links.append(h)
    # loại trùng
    wiki_links = list(dict.fromkeys(wiki_links))
    return wiki_links


# ==========================
# 🧠 STEP 3: Cào nội dung trang Wikipedia
# ==========================
def scrape_wikipedia(url, driver):
    driver.get(url)
    time.sleep(1.5)
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    title_tag = soup.find("span", class_="mw-page-title-main")
    name = title_tag.text.strip() if title_tag else ""

    birth, death, hometown = None, None, None
    infobox = soup.find("table", class_="infobox")
    if infobox:
        for tr in infobox.find_all("tr"):
            th, td = tr.find("th"), tr.find("td")
            if not th or not td:
                continue
            key = th.get_text(strip=True).lower()
            val = td.get_text(" ", strip=True)
            if "born" in key or "sinh" in key:
                birth = val
            elif "died" in key or "mất" in key:
                death = val
            elif "quê" in key or "hometown" in key:
                hometown = val

    content_div = soup.find("div", id="mw-content-text")
    bio = ""
    if content_div:
        paragraphs = content_div.find_all(["p", "ul", "ol", "li", "blockquote"])
        texts = [p.get_text(" ", strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 0]
        bio = "\n\n".join(texts)
    bio = re.sub(r"\[\d+\]", "", bio)
    bio = re.sub(r"\s+", " ", bio).strip()

    return {
        "person_name": name,
        "birth_year": birth,
        "death_year": death,
        "birthplace": hometown,
        "biography": bio,
        "url": url
    }


# ==========================
# 🧠 STEP 4: Task Prefect - Extract toàn bộ
# ==========================
@task
def extract_people_data(topics: list, output_path="data/person_raw.csv"):
    driver = init_driver(headless=False)
    os.makedirs("data", exist_ok=True)

    all_data = []
    for topic in topics:
        print(f"\n🔎 Đang tìm Wikipedia cho: {topic}")
        wiki_links = find_wikipedia_links(topic, driver)
        if not wiki_links:
            print(f"⚠️ Không tìm thấy Wikipedia cho {topic}")
            continue

        for link in wiki_links[:1]:  # chỉ lấy link đầu tiên cho gọn
            print(f"📘 Đang cào: {link}")
            try:
                data = scrape_wikipedia(link, driver)
                all_data.append(data)
            except Exception as e:
                print(f"❌ Lỗi khi cào {link}: {e}")

    pd.DataFrame(all_data).to_csv(output_path, index=False, encoding="utf-8-sig")
    driver.quit()
    print(f"\n✅ Đã lưu {len(all_data)} dòng dữ liệu vào {output_path}")
    return output_path


# ==========================
# 🧠 STEP 5: Cho phép test độc lập
# ==========================
if __name__ == "__main__":
    topics = ["Vua Nguyễn Hoàng", 
              "Vua Gia Long",
              "Vua Minh Mạng",
              "Vua Tự Đức",
              "Tổng trấn Nguyễn Văn Tường",
              "Tôn Thất Thuyết",
              "Vua Hàm Nghi",
              "Thống sứ Trung Kỳ Paul Doumer",
              "Phan Bội Châu",
              "Cường Để",
              "Phan Châu Trinh",
              "Huỳnh Thúc Kháng",
              "Trần Quý Cáp",
              "Trần Cao Vân",
              "Thái Phiên",
              "Vua Duy Tân",
              "Vua Bảo Đại",
              "Chủ tịch Hồ Chí Minh",
              "Trần Huy Liệu",
              "Hoà thượng Thích Quảng Đức",
              "Thích Trí Quang",
              "Ngô Đình Diệm",
              "Tướng Ngô Quang Trưởng",
              "Tướng Trần Văn Hải",
              "Võ Nguyên Giáp",
              "Tín đồ đạo Mẫu Thiên Y A Na",
              "Cộng đồng người Minh Hương",
              "Triều Nguyễn",
              "Công chúa Huyền Trân",
              "Nhà Trần",
              "Đức Ông Nam Hải"
              ]
    extract_people_data.fn(topics)
