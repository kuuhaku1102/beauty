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
SLEEP_BETWEEN = 2  # ページGETの間隔（マナー用）

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

def load_urls_from_env():
    """
    環境変数 TARGET_URLS からURL群を抽出。
    改行/カンマはもちろん、完全連結
    （例: https://a.comhttps://b.com）にも対応。
    """
    raw = os.getenv("TARGET_URLS", "") or ""
    # 空白やカンマをスペースに正規化（無くてもOKだが念のため）
    flat = re.sub(r"[\s,]+", " ", raw.strip())

    # http(s) で始まり、次の http(s) か空白/行末の直前までを非貪欲に取得
    # これで連結ケースも 1 件ずつ拾える
    found = re.findall(r"https?://.*?(?=https?://|\s|$)", flat)

    # 重複除去（順序維持）
    uniq, seen = [], set()
    for u in found:
        u = u.strip()
        if u and u not in seen:
            uniq.append(u)
            seen.add(u)
    return uniq

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
    """
    存在確認（HEAD→必要ならGET）
    """
    try:
        r = requests.head(url, headers={"User-Agent": USER_AGENT}, timeout=5, allow_redirects=True)
        if r.status_code in (405, 403):
            r2 = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
            return r2.status_code == 200
        return r.status_code == 200
    except Exception:
        return False

# ---- URL discovery (0001〜END_ID) ----
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
        time.sleep(0.05)  # やや控えめに
    return valid_urls

# ---- Parse helpers ----
TIME_RANGE_RE = re.compile(r"(?P<open>\d{1,2}:\d{2}).*?(?P<close>\d{1,2}:\d{2})")

def parse_hours(table):
    """
    return: dict[day] -> raw string
    """
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

def split_open_close(raw):
    if not raw:
        return "", ""
    m = TIME_RANGE_RE.search(raw)
    if not m:
        return "", ""
    return m.group("open"), m.group("close")

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
    access_text = clean_text(card.select_one(".card__detail") and card.select_one(".card__detail").get_text()) \
                  or clean_text(card.select_one(".card__access-text") and card.select_one(".card__access-text").get_text())

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

    hours_table = parse_hours(card.select_one("table.table"))
    return {
        "rank": rank, "name": name, "clinic_url": clinic_url,
        "rating": rating, "reviews": reviews,
        "snippet": snippet, "snippet_author": snippet_author,
        "images": images, "features": features, "access": access_text,
        "hours": hours_table, "menus": menus
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

# ==== 3シートへの書き込み ====
CLINICS_SHEET = os.getenv("CLINICS_SHEET_NAME", "clinics")
MENUS_SHEET   = os.getenv("MENUS_SHEET_NAME", "menus")
HOURS_SHEET   = os.getenv("HOURS_SHEET_NAME", "hours")

CLINICS_HEADER = [
    "timestamp_utc","clinic_id","name","rank","rating","reviews_count",
    "clinic_url","source_page_url","access_text","snippet","snippet_author",
    "images_csv","features_csv","hours_json","last_seen_utc","status","notes"
]

MENUS_HEADER = [
    "timestamp_utc","clinic_id","menu_title","price_jpy","price_raw","menu_url","pickup_flag","category_raw"
]

HOURS_HEADER = [
    "timestamp_utc","clinic_id","day","open_time","close_time","raw"
]

def write_three_sheets(clinics_rows, menus_rows, hours_rows):
    json_b64 = os.getenv("GSHEET_JSON_B64")
    sheet_key = os.getenv("GSHEET_KEY")
    if not (json_b64 and sheet_key):
        print("[Sheets] Skipped (env not set).")
        return
    gc = get_gspread_client_from_b64(json_b64)
    sh = gc.open_by_key(sheet_key)

    # clinics
    try:
        ws_c = sh.worksheet(CLINICS_SHEET)
    except Exception:
        ws_c = sh.add_worksheet(title=CLINICS_SHEET, rows=5000, cols=len(CLINICS_HEADER))
    ensure_header(ws_c, CLINICS_HEADER)
    append_rows(ws_c, clinics_rows)
    print(f"[Sheets] clinics +{len(clinics_rows)}")

    # menus
    try:
        ws_m = sh.worksheet(MENUS_SHEET)
    except Exception:
        ws_m = sh.add_worksheet(title=MENUS_SHEET, rows=5000, cols=len(MENUS_HEADER))
    ensure_header(ws_m, MENUS_HEADER)
    append_rows(ws_m, menus_rows)
    print(f"[Sheets] menus +{len(menus_rows)}")

    # hours
    try:
        ws_h = sh.worksheet(HOURS_SHEET)
    except Exception:
        ws_h = sh.add_worksheet(title=HOURS_SHEET, rows=5000, cols=len(HOURS_HEADER))
    ensure_header(ws_h, HOURS_HEADER)
    append_rows(ws_h, hours_rows)
    print(f"[Sheets] hours +{len(hours_rows)}")

# ---- 設定/ターゲット補助（任意）----
def write_settings_sheet():
    if os.getenv("WRITE_SETTINGS_SHEET", "").lower() != "true":
        return
    json_b64 = os.getenv("GSHEET_JSON_B64")
    sheet_key = os.getenv("GSHEET_KEY")
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
        {"key":"END_ID",              "value": os.getenv("END_ID",""), "note":"探索の最終ID（0001〜END_ID）", "updated_utc": now},
        {"key":"CLINICS_SHEET_NAME",  "value": CLINICS_SHEET,          "note":"クリニック出力シート",        "updated_utc": now},
        {"key":"MENUS_SHEET_NAME",    "value": MENUS_SHEET,            "note":"メニュー出力シート",          "updated_utc": now},
        {"key":"HOURS_SHEET_NAME",    "value": HOURS_SHEET,            "note":"営業時間出力シート",          "updated_utc": now},
        {"key":"GSHEET_KEY",          "value": sheet_key,              "note":"スプレッドシートID",           "updated_utc": now},
        {"key":"GSHEET_JSON_B64",     "value": masked_summary(json_b64),"note":"Secretsに格納。値は保存しない。", "updated_utc": now},
    ]
    append_rows(ws, values)
    print(f"[Settings] wrote to '{settings_sheet_name}'.")

def write_targets_sheet(urls):
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
def save_csv(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"[Saved] {path}")

# ---- main ----
def main():
    # 1) 手動指定URL（TARGET_URLS）を最優先
    urls = load_urls_from_env()

    # 2) 無ければ 0001〜END_ID の自動探索
    if not urls:
        urls = build_target_urls_auto()

    if not urls:
        raise SystemExit("No valid clinic pages found")

    out_dir = os.getenv("OUTPUT_DIR", "output")
    os.makedirs(out_dir, exist_ok=True)

    write_targets_sheet(urls)  # 任意でtargetsシートに記録

    ts = now_utc_iso()
    clinics_rows, menus_rows, hours_rows = [], [], []
    all_cards = []

    for source_page_url in urls:
        print(f"[Fetch] {source_page_url}")
        html = fetch(source_page_url)
        cards = parse_page(html, source_page_url)
        all_cards.extend(cards)

        for c in cards:
            clinic_id = get_clinic_id_from_url(c.get("clinic_url") or source_page_url)
            images_csv   = ",".join([x for x in c.get("images", []) if x])
            features_csv = ",".join([x for x in c.get("features", []) if x])
            hours_json   = json.dumps(c.get("hours", {}), ensure_ascii=False)

            notes = []
            if c.get("rating") is not None:
                notes.append(f"rating={c['rating']}")
            if c.get("reviews") is not None:
                notes.append(f"reviews={c['reviews']}")
            notes_str = ", ".join(notes)

            # clinics
            clinics_rows.append({
                "timestamp_utc": ts,
                "clinic_id": clinic_id,
                "name": c.get("name",""),
                "rank": c.get("rank"),
                "rating": c.get("rating"),
                "reviews_count": c.get("reviews"),
                "clinic_url": c.get("clinic_url") or source_page_url,
                "source_page_url": source_page_url,
                "access_text": c.get("access",""),
                "snippet": c.get("snippet",""),
                "snippet_author": c.get("snippet_author",""),
                "images_csv": images_csv,
                "features_csv": features_csv,
                "hours_json": hours_json,
                "last_seen_utc": ts,
                "status": "ok",
                "notes": notes_str
            })

            # menus
            for m in c.get("menus", []):
                menus_rows.append({
                    "timestamp_utc": ts,
                    "clinic_id": clinic_id,
                    "menu_title": m.get("title",""),
                    "price_jpy": m.get("price_jpy"),
                    "price_raw": m.get("price_raw",""),
                    "menu_url": m.get("url",""),
                    "pickup_flag": m.get("pickup_flag"),
                    "category_raw": m.get("category_raw",""),
                })

            # hours
            for day, raw in (c.get("hours") or {}).items():
                open_time, close_time = split_open_close(raw)
                hours_rows.append({
                    "timestamp_utc": ts,
                    "clinic_id": clinic_id,
                    "day": day,
                    "open_time": open_time,
                    "close_time": close_time,
                    "raw": raw
                })

        time.sleep(SLEEP_BETWEEN)

    # ローカル保存
    df_clinics = pd.DataFrame(clinics_rows, columns=CLINICS_HEADER)
    df_menus   = pd.DataFrame(menus_rows,   columns=MENUS_HEADER)
    df_hours   = pd.DataFrame(hours_rows,   columns=HOURS_HEADER)

    save_csv(df_clinics, os.path.join(out_dir, "clinics.csv"))
    save_csv(df_menus,   os.path.join(out_dir, "menus.csv"))
    save_csv(df_hours,   os.path.join(out_dir, "hours.csv"))

    with open(os.path.join(out_dir, "cards.json"), "w", encoding="utf-8") as f:
        json.dump(all_cards, f, ensure_ascii=False, indent=2)
    print("[Saved] output/cards.json")

    # スプレッドシートへ書き込み
    write_three_sheets(clinics_rows, menus_rows, hours_rows)
    write_settings_sheet()

if __name__ == "__main__":
    main()
