import os, re, json, base64, time
from datetime import datetime, timezone
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import pandas as pd

# ---- Config ----
USER_AGENT = "Mozilla/5.0 (compatible; ScraperBot/1.0; +https://github.com/your/repo)"
TIMEOUT = 30
RETRY = 3
SLEEP_BETWEEN = 2  # スクレイピング時のsleep（礼儀として少し）

# ---- Utils ----
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

# ---- HTTP ----
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
    """存在確認（HEAD）。一部サイトでHEAD→GETが必要ならGETに切替えてください。"""
    try:
        r = requests.head(url, headers={"User-Agent": USER_AGENT}, timeout=5, allow_redirects=True)
        # 一部環境では 405/403 を返すサイトもあるので、その場合はGETで再確認
        if r.status_code in (405, 403):
            r2 = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
            return r2.status_code == 200
        return r.status_code == 200
    except Exception:
        return False

# ---- URL discovery (0001〜9999) ----
def build_target_urls_auto():
    end_id_str = os.getenv("END_ID", "9999")
    if not end_id_str.isdigit():
        raise SystemExit("END_ID must be numeric")
    end_id = int(end_id_str)

    base_url = "https://kireireport.com/clinics"
    valid_urls = []
    for cid in range(1, end_id + 1):
        url = f"{base_url}/{cid:04d}"  # 4桁ゼロ埋め
        if check_url_exists(url):
            valid_urls.append(url)
            print(f"[OK] {url}")
        else:
            print(f"[NG] {url}")
        # 探索は速くなりがちなので、ほんの少し待つ
        time.sleep(0.05)
    return valid_urls

# ---- Parse helpers ----
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
    rank_el = card.select_one(".number_ranked")
    rank = to_int(rank_el.get_text()) if rank_el else None

    a_title = card.select_one("a.card__title")
    name = clean_text(a_title.get_text() if a_title else "")
    clinic_url = urljoin(base_url, a_title["href"]) if (base_url and a_title and a_title.has_attr("href")) else (a_title["href"] if a_title and a_title.has_attr("href") else "")

    rating = None
    rating_el = card.select_one(".rating-number")
    if rating_el:
        try:
            rating = float(rating_el.get_text().strip())
        except:
            pass

    reviews_el = card.select_one("a.report-count")
    reviews = to_int(reviews_el.get_text()) if reviews_el else None

    snippet = clean_text(card.select_one(".card__report-snippet-content") and card.select_one(".card__report-snippet-content").get_text())
    snippet_author = clean_text(card.select_one(".card__report-snippet-name") and card.select_one(".card__report-snippet-name").get_text()).lstrip("-").strip()

    images = [img.get("src") for img in card.select(".card__image-list img.card__image[src]")]
    features = [clean_text(li.get_text()) for li in card.select(".card__feature-list .card__feature")]
    access_text = clean_text(card.select_one(".card__access-text") and card.select_one(".card__access-text").get_text())

    menus = []
    for li in card.select("ul li a.small-list__item"):
        title = clean_text(li.select_one(".small-list__title") and li.select_one(".small-list__title").get_text())
        price_text = clean_text(li.select_one(".small-list__price") and li.select_one(".small-list__price").get_text())
        price_jpy = None
        m = re.search(r"¥\s*([\d,]+)", price_text or "")
        if m:
            try:
                price_jpy = int(m.group(1).replace(",", ""))
            except:
                pass
        href = li.get("href") or ""
        menu_url = urljoin(base_url, href) if base_url else href
        pickup = bool(li.select_one(".pickup-label_active"))
        cat = clean_text(li.select_one(".treatment-category") and li.select_one(".treatment-category").get_text())
        menus.append({
            "title": title, "price_jpy": price_jpy, "price_raw": price_text,
            "url": menu_url, "pickup_flag": pickup, "category_raw": cat
        })

    hours = parse_hours(card.select_one("table.table"))
    return {
        "rank": rank, "name": name, "clinic_url": clinic_url,
        "rating": rating, "reviews": reviews,
        "snippet": snippet, "snippet_author": snippet_author,
        "images": images, "features": features, "access": access_text,
        "hours": hours, "menus": menus
    }

def parse_page(html, page_url):
    soup = BeautifulSoup(html, "html.parser")
    cards = [parse_card(c, base_url=page_url) for c in soup.select(".card.clinic-list__card")]
    if not cards:  # 単体ページfallback
        title = clean_text(soup.title.string if soup.title else "")
        h1 = clean_text(soup.select_one("h1") and soup.select_one("h1").get_text())
        cards.append({
            "rank": None, "name": h1 or title, "clinic_url": page_url,
            "rating": None, "reviews": None, "snippet": "", "snippet_author": "",
            "images": [], "features": [], "access": "", "hours": {}, "menus": []
        })
    return cards

# ---- Sheets helpers ----
def get_gspread_client_from_b64(json_b64: str):
    import gspread
    from google.oauth2.service_account import Credentials
    info = json.loads(base64.b64decode(json_b64).decode("utf-8"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.readonly"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def ensure_header(ws, header):
    first = ws.row_values(1)
    if [h.strip() for h in first] != header:
        if first:
            ws.delete_rows(1)
        ws.insert_row(header, 1)

def append_rows(ws, rows):
    header = ws.row_values(1)
    values = [[r.get(k, "") for k in header] for r in rows]
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

def write_settings_sheet():
    """settings シートに機微情報を漏らさず記録（JSONはマスク表記のみ）。"""
    if os.getenv("WRITE_SETTINGS_SHEET", "").lower() != "true":
        return
    json_b64 = os.getenv("GSHEET_JSON_B64")
    sheet_key = os.getenv("GSHEET_KEY")
    worksheet_name = os.getenv("GSHEET_WORKSHEET", "scrape")
    settings_sheet_name = os.getenv("SETTINGS_SHEET_NAME", "settings")
    if not (json_b64 and sheet_key):
        print("[Settings] Skipped (env not set).")
        return
    gc = get_gspread_client_from_b64(json_b64)
    sh = gc.open_by_key(sheet_key)
    try:
        ws = sh.worksheet(settings_sheet_name)
    except Exception:
        ws = sh.add_worksheet(title=settings_sheet_name, rows=100, cols=4)

    header = ["key","value","note","updated_utc"]
    ensure_header(ws, header)

    def masked_summary(b64: str):
        s = (b64 or "").strip()
        if len(s) <= 12:
            return f"(masked) len={len(s)}"
        return f"(masked) len={len(s)}, head={s[:6]}..., tail=...{s[-6:]}"

    now = now_utc_iso()
    values = [
        {"key":"END_ID",            "value": os.getenv("END_ID",""),        "note":"探索の最終ID（0001〜END_ID）", "updated_utc": now},
        {"key":"GSHEET_KEY",        "value": sheet_key,                     "note":"スプレッドシートID",          "updated_utc": now},
        {"key":"GSHEET_WORKSHEET",  "value": worksheet_name,                "note":"結果出力シート名",            "updated_utc": now},
        {"key":"GSHEET_JSON_B64",   "value": masked_summary(json_b64),      "note":"Secretsに格納。値は保存しない。", "updated_utc": now},
    ]
    append_rows(ws, values)
    print(f"[Settings] Wrote {len(values)} rows to '{settings_sheet_name}'.")

def write_targets_sheet(urls):
    """シート 'targets' にURL一覧を追記（フラグでON/OFF）"""
    if os.getenv("WRITE_TARGETS_SHEET", "").lower() != "true":
        return
    json_b64 = os.getenv("GSHEET_JSON_B64")
    sheet_key = os.getenv("GSHEET_KEY")
    if not (json_b64 and sheet_key):
        print("[targets sheet] Skipped (env not set).")
        return
    gc = get_gspread_client_from_b64(json_b64)
    sh = gc.open_by_key(sheet_key)
    title = os.getenv("TARGETS_SHEET_NAME", "targets")
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=2000, cols=3)
    header = ["timestamp_utc", "url", "clinic_id"]
    ensure_header(ws, header)
    ts = now_utc_iso()
    rows = [{"timestamp_utc": ts, "url": u, "clinic_id": get_clinic_id_from_url(u)} for u in urls]
    append_rows(ws, rows)
    print(f"[targets sheet] +{len(rows)} rows -> {title}")

# ---- IO helpers ----
def save_targets(urls, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    ts = now_utc_iso()
    rows = [{"timestamp_utc": ts, "url": u, "clinic_id": get_clinic_id_from_url(u)} for u in urls]
    pd.DataFrame(rows).to_csv(os.path.join(out_dir, "targets.csv"), index=False, encoding="utf-8-sig")
    with open(os.path.join(out_dir, "targets.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(urls) + "\n")
    print(f"[Saved] output/targets.csv ({len(urls)} urls)")
    print(f"[Saved] output/targets.txt")

# ---- main ----
def main():
    # 0001〜END_ID まで存在確認して、あるURLだけを対象にする
    urls = build_target_urls_auto()
    if not urls:
        raise SystemExit("No valid clinic pages found")

    out_dir = os.getenv("OUTPUT_DIR", "output")
    os.makedirs(out_dir, exist_ok=True)

    # URL一覧を保存＆（任意で）スプシにも記録
    save_targets(urls, out_dir)
    write_targets_sheet(urls)

    all_rows, all_cards = [], []
    ts = now_utc_iso()

    for url in urls:
        print(f"[Fetch] {url}")
        html = fetch(url)
        cards = parse_page(html, url)
        all_cards.extend(cards)
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
                "title": c.get("name",""),
                "h1": c.get("name",""),
                "status": "ok",
                "notes": ", ".join(notes)
            })
        time.sleep(SLEEP_BETWEEN)

    # ローカル保存
    pd.DataFrame(all_rows).to_csv(os.path.join(out_dir, "latest.csv"), index=False, encoding="utf-8-sig")
    with open(os.path.join(out_dir, "cards.json"), "w", encoding="utf-8") as f:
        json.dump(all_cards, f, ensure_ascii=False, indent=2)
    print("[Saved] output/latest.csv")
    print("[Saved] output/cards.json")

    # シート書き込み
    write_to_sheet(all_rows)
    write_settings_sheet()

if __name__ == "__main__":
    main()
