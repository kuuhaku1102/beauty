import os, re, json, time
from datetime import datetime, timezone
from urllib.parse import urljoin, quote_plus
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine, text

# ==========================================================
#  DB接続設定（Secrets経由）
# ==========================================================
def get_engine():
    """
    GitHub ActionsからSecrets経由でMySQL接続
    """
    db_host = os.getenv("DB_HOST", "127.0.0.1")
    db_port = os.getenv("DB_PORT", "3306")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_name = os.getenv("DB_NAME")

    if not all([db_user, db_pass, db_name]):
        raise RuntimeError("❌ DB_USER / DB_PASS / DB_NAME が未設定です。")

    db_user_enc = quote_plus(db_user)
    db_pass_enc = quote_plus(db_pass)
    url = f"mysql+pymysql://{db_user_enc}:{db_pass_enc}@{db_host}:{db_port}/{db_name}?charset=utf8mb4"

    connect_args = {
        "connect_timeout": 20,
        "read_timeout": 60,
        "write_timeout": 60,
        "autocommit": True
    }

    engine = create_engine(
        url,
        echo=False,
        pool_pre_ping=True,
        pool_recycle=280,
        connect_args=connect_args
    )
    return engine


def ensure_tables():
    """
    clinics / menus / hours テーブルを自動作成
    """
    ddl_clinics = """
    CREATE TABLE IF NOT EXISTS clinics (
      id INT AUTO_INCREMENT PRIMARY KEY,
      timestamp_utc DATETIME,
      clinic_id VARCHAR(50),
      name TEXT,
      rank INT,
      rating FLOAT,
      reviews_count INT,
      clinic_url TEXT,
      source_page_url TEXT,
      prefecture VARCHAR(50),
      city VARCHAR(50),
      station VARCHAR(50),
      access_text TEXT,
      snippet TEXT,
      snippet_author TEXT,
      images_csv TEXT,
      features_csv TEXT,
      hours_json JSON,
      breadcrumb_json JSON,
      last_seen_utc DATETIME,
      status VARCHAR(20),
      notes TEXT
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """
    ddl_menus = """
    CREATE TABLE IF NOT EXISTS menus (
      id INT AUTO_INCREMENT PRIMARY KEY,
      timestamp_utc DATETIME,
      clinic_id VARCHAR(50),
      menu_title TEXT,
      price_jpy INT,
      price_raw TEXT,
      menu_url TEXT,
      pickup_flag BOOLEAN,
      category_raw TEXT,
      menu_img TEXT
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """
    ddl_hours = """
    CREATE TABLE IF NOT EXISTS hours (
      id INT AUTO_INCREMENT PRIMARY KEY,
      timestamp_utc DATETIME,
      clinic_id VARCHAR(50),
      day VARCHAR(20),
      open_time VARCHAR(10),
      close_time VARCHAR(10),
      raw TEXT
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """

    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text(ddl_clinics))
        conn.execute(text(ddl_menus))
        conn.execute(text(ddl_hours))
    print("[DB] ensure_tables: OK")


def write_three_tables(clinics_rows, menus_rows, hours_rows):
    """
    pandas.to_sql で clinics / menus / hours を一括INSERT
    """
    eng = get_engine()
    with eng.begin() as conn:
        if clinics_rows:
            pd.DataFrame(clinics_rows).to_sql("clinics", conn, if_exists="append", index=False)
            print(f"[DB] clinics +{len(clinics_rows)}")
        if menus_rows:
            pd.DataFrame(menus_rows).to_sql("menus", conn, if_exists="append", index=False)
            print(f"[DB] menus +{len(menus_rows)}")
        if hours_rows:
            pd.DataFrame(hours_rows).to_sql("hours", conn, if_exists="append", index=False)
            print(f"[DB] hours +{len(hours_rows)}")


# ==========================================================
#  スクレイピング設定
# ==========================================================
USER_AGENT = "Mozilla/5.0 (compatible; ScraperBot/1.0; +https://github.com/your/repo)"
TIMEOUT = 30
RETRY = 3
SLEEP_BETWEEN = 2

def now_utc_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def clean_text(s):
    return re.sub(r"\s+", " ", s).strip() if s else ""

def to_int(s):
    if not s:
        return None
    m = re.search(r"\d+", s.replace(",", ""))
    return int(m.group()) if m else None

def get_clinic_id_from_url(u: str):
    m = re.search(r"/clinics/(\d+)", u or "")
    return m.group(1) if m else ""

def load_urls_from_env():
    raw = os.getenv("TARGET_URLS", "") or ""
    flat = re.sub(r"[\s,]+", " ", raw.strip())
    found = re.findall(r"https?://.*?(?=https?://|\s|$)", flat)
    uniq, seen = [], set()
    for u in found:
        u = u.strip()
        if u and u not in seen:
            uniq.append(u)
            seen.add(u)
    return uniq

def fetch(url):
    last_exc = None
    for i in range(RETRY):
        try:
            r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_exc = e
            time.sleep(SLEEP_BETWEEN * (i + 1))
    raise last_exc

def check_url_exists(url):
    try:
        r = requests.head(url, headers={"User-Agent": USER_AGENT}, timeout=5, allow_redirects=True)
        if r.status_code in (405, 403):
            r2 = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
            return r2.status_code == 200
        return r.status_code == 200
    except Exception:
        return False

def build_target_urls_auto():
    end_id_str = os.getenv("END_ID", "100")
    if not end_id_str.isdigit():
        raise SystemExit("END_ID must be numeric")
    end_id = int(end_id_str)
    base_url = "https://kireireport.com/clinics"
    valid_urls = []
    for cid in range(1, end_id + 1):
        url = f"{base_url}/{cid:04d}"
        if check_url_exists(url):
            valid_urls.append(url)
            print(f"[OK] {url}")
        else:
            print(f"[NG] {url}")
        time.sleep(0.05)
    return valid_urls

# ==========================================================
#  ページ解析
# ==========================================================
TIME_RANGE_RE = re.compile(r"(?P<open>\d{1,2}:\d{2}).*?(?P<close>\d{1,2}:\d{2})")
WEEK_DAYS = ["月", "火", "水", "木", "金", "土", "日"]

def parse_card(card, base_url=None):
    a_title = card.select_one("a.card__title")
    name = clean_text(a_title.get_text() if a_title else "")
    clinic_url = urljoin(base_url, a_title["href"]) if (a_title and a_title.has_attr("href")) else ""
    rating_el = card.select_one(".rating-number")
    rating = float(rating_el.get_text().strip()) if rating_el else None
    reviews_el = card.select_one("a.report-count")
    reviews = to_int(reviews_el.get_text()) if reviews_el else None
    return {
        "name": name,
        "clinic_url": clinic_url,
        "rating": rating,
        "reviews": reviews
    }

def parse_page(html, page_url):
    soup = BeautifulSoup(html, "html.parser")
    cards = [parse_card(c, base_url=page_url) for c in soup.select(".card.clinic-list__card")]
    if not cards:
        h1 = clean_text(soup.select_one("h1") and soup.select_one("h1").get_text())
        cards.append({"name": h1, "clinic_url": page_url, "rating": None, "reviews": None})
    return cards

# ==========================================================
#  メイン処理
# ==========================================================
def main():
    urls = load_urls_from_env() or build_target_urls_auto()
    if not urls:
        raise SystemExit("No valid clinic pages found")

    ensure_tables()
    ts = now_utc_iso()
    clinics_rows = []

    for source_page_url in urls:
        print(f"[Fetch] {source_page_url}")
        html = fetch(source_page_url)
        cards = parse_page(html, source_page_url)

        for c in cards:
            clinic_id = get_clinic_id_from_url(c.get("clinic_url"))
            clinics_rows.append({
                "timestamp_utc": ts,
                "clinic_id": clinic_id,
                "name": c.get("name",""),
                "rank": None,
                "rating": c.get("rating"),
                "reviews_count": c.get("reviews"),
                "clinic_url": c.get("clinic_url"),
                "source_page_url": source_page_url,
                "prefecture": "",
                "city": "",
                "station": "",
                "access_text": "",
                "snippet": "",
                "snippet_author": "",
                "images_csv": "",
                "features_csv": "",
                "hours_json": "{}",
                "breadcrumb_json": "[]",
                "last_seen_utc": ts,
                "status": "ok",
                "notes": ""
            })

    write_three_tables(clinics_rows, [], [])
    print("[DONE] Scraping complete ✅")


if __name__ == "__main__":
    print("[DB] Testing connection...")
    try:
        eng = get_engine()
        with eng.connect() as conn:
            res = conn.execute(text("SELECT NOW()")).scalar()
            print(f"[DB] Connected successfully: {res}")
    except Exception as e:
        print("❌ Connection test failed:", e)
        raise
    main()
