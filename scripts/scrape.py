import os
import re
import json
import base64
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import pandas as pd

# -------------------------
# 基本設定
# -------------------------
USER_AGENT = "Mozilla/5.0 (compatible; ScraperBot/1.0; +https://github.com/your/repo)"
TIMEOUT = 30
RETRY = 3
SLEEP_BETWEEN = 2  # sec


# -------------------------
# ユーティリティ
# -------------------------
def now_utc_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def clean_text(s):
    return re.sub(r"\s+", " ", s).strip() if s else ""

def to_int(s):
    if not s:
        return None
    m = re.search(r"\d+", s.replace(",", ""))
    return int(m.group()) if m else None


# -------------------------
# 取得 & 解析
# -------------------------
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

def parse_hours(table):
    hours = {}
    if not table:
        return hours
    for tr in table.select("tbody > tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue
        day = clean_text(tds[0].get_text())
        time_text = clean_text(tds[1].get_text(" "))
        if day:
            hours[day] = time_text
    return hours

def parse_card(card, base_url=None):
    # rank
    rank_el = card.select_one(".number_ranked")
    rank = to_int(rank_el.get_text()) if rank_el else None

    # name & clinic url
    a_title = card.select_one("a.card__title")
    name = clean_text(a_title.get_text() if a_title else "")
    clinic_url = ""
    if a_title and a_title.has_attr("href"):
        clinic_url = urljoin(base_url, a_title["href"]) if base_url else a_title["href"]

    # rating & reviews
    rating = None
    rating_el = card.select_one(".rating-number")
    if rating_el:
        try:
            rating = float(rating_el.get_text().strip())
        except Exception:
            pass
    reviews_el = card.select_one("a.report-count")
    reviews = to_int(reviews_el.get_text()) if reviews_el else None

    # snippet
    snippet = clean_text(card.select_one(".card__report-snippet-content") and card.select_one(".card__report-snippet-content").get_text())
    snippet_author = clean_text(card.select_one(".card__report-snippet-name") and card.select_one(".card__report-snippet-name").get_text()).lstrip("-").strip()

    # images
    images = [img.get("src") for img in card.select(".card__image-list img.card__image[src]")]

    # features
    features = [clean_text(li.get_text()) for li in card.select(".card__feature-list .card__feature")]

    # access
    access_text = clean_text(card.select_one(".card__access-text") and card.select_one(".card__access-text").get_text())

    # menus
    menus = []
    for li in card.select("ul li a.small-list__item"):
        title = clean_text(li.select_one(".small-list__title") and li.select_one(".small-list__title").get_text())
        price_text = clean_text(li.select_one(".small-list__price") and li.select_one(".small-list__price").get_text())
        price_jpy = None
        m = re.search(r"¥\s*([\d,]+)", price_text or "")
        if m:
            try:
                price_jpy = int(m.group(1).replace(",", ""))
            except Exception:
                pass
        href = li.get("href") or ""
        menu_url = urljoin(base_url, href) if base_url else href
        pickup = bool(li.select_one(".pickup-label_active"))
        cat = clean_text(li.select_one(".treatment-category") and li.select_one(".treatment-category").get_text())
        menus.append({
            "title": title,
            "price_jpy": price_jpy,
            "price_raw": price_text,
            "url": menu_url,
            "pickup_flag": pickup,
            "category_raw": cat
        })

    # hours
    hours_table = card.select_one("table.table")
    hours = parse_hours(hours_table)

    return {
        "rank": rank,
        "name": name,
        "clinic_url": clinic_url,
        "rating": rating,
        "reviews": reviews,
        "snippet": snippet,
        "snippet_author": snippet_author,
        "images": images,
        "features": features,
        "access": access_text,
        "hours": hours,
        "menus": menus,
    }

def parse_page(html, page_url):
    soup = BeautifulSoup(html, "html.parser")
    cards = [parse_card(c, base_url=page_url) for c in soup.select(".card.clinic-list__card")]

    # fallback: 1ページ1院の想定で単純にタイトルだけ
    if not cards:
        title = clean_text(soup.title.string if soup.title else "")
        h1 = clean_text(soup.select_one("h1") and soup.select_one("h1").get_text())
        cards.append({
            "rank": None,
            "name": h1 or title,
            "clinic_url": page_url,
            "rating": None,
            "reviews": None,
            "snippet": "",
            "snippet_author": "",
            "images": [],
            "features": [],
            "access": "",
            "hours": {},
            "menus": [],
        })

    return cards


# -------------------------
# Google Sheets 出力（あれば）
# -------------------------
def get_gspread_client_from_b64(json_b64: str):
    import gspread
    from google.oauth2.service_account import Credentials

    info = json.loads(base64.b64decode(json_b64).decode("utf-8"))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def ensure_header(ws, header):
    first_row = ws.row_values(1)
    if [h.strip() for h in first_row] != header:
        if first_row:
            ws.delete_rows(1)
        ws.insert_row(header, 1)

def append_rows(ws, rows):
    header = ws.row_values(1)
    values = []
    for r in rows:
        values.append([r.get(k, "") for k in header])
    if values:
        ws.append_rows(values, value_input_option="RAW")

def write_to_sheet(rows):
    json_b64 = os.getenv("GSHEET_JSON_B64")
    sheet_key = os.getenv("GSHEET_KEY")
    worksheet_name = os.getenv("GSHEET_WORKSHEET", "scrape")

    if not (json_b64 and sheet_key):
        print("[Sheets] Skipped (env not set).")
        return

    gc = get_gspread_client_from_b64(json_b64)
    sh = gc.open_by_key(sheet_key)
    try:
        ws = sh.worksheet(worksheet_name)
    except Exception:
        ws = sh.add_worksheet(title=worksheet_name, rows=2000, cols=20)

    header = ["timestamp_utc","source","url","title","h1","status","notes"]
    ensure_header(ws, header)
    append_rows(ws, rows)
    print(f"[Sheets] Appended {len(rows)} rows to '{worksheet_name}'.")


# -------------------------
# メイン
# -------------------------
def main():
    target_urls = os.getenv("TARGET_URLS", "").strip()
    if not target_urls:
        raise SystemExit("TARGET_URLS is empty")

    urls = [u.strip() for u in target_urls.split(",") if u.strip()]
    out_dir = os.getenv("OUTPUT_DIR", "output")
    os.makedirs(out_dir, exist_ok=True)

    all_cards = []
    all_rows = []
    ts = now_utc_iso()

    for url in urls:
        print(f"[Fetch] {url}")
        html = fetch(url)
        cards = parse_page(html, url)
        all_cards.extend(cards)

        # 最小スキーマに落としてSheets/CSV保存
        for c in cards:
            notes = []
            if c.get("rating") is not None:
                notes.append(f"rating={c['rating']}")
            if c.get("reviews") is not None:
                notes.append(f"reviews={c['reviews']}")
            all_rows.append({
                "timestamp_utc": ts,
                "source": "requests",
                "url": c.get("clinic_url", url),
                "title": c.get("name", ""),
                "h1": c.get("name", ""),
                "status": "ok",
                "notes": ", ".join(notes)
            })

    # CSV & JSON 保存
    df = pd.DataFrame(all_rows)
    csv_path = os.path.join(out_dir, "latest.csv")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    json_path = os.path.join(out_dir, "cards.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_cards, f, ensure_ascii=False, indent=2)

    print(f"[Saved] {csv_path}")
    print(f"[Saved] {json_path}")

    # Google Sheets に追記（環境変数があれば）
    write_to_sheet(all_rows)


if __name__ == "__main__":
    main()

