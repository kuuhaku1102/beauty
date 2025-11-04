import os, re, json, time
from datetime import datetime, timezone
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import pandas as pd

# ==========================================================
# 設定
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

# ==========================================================
# URL自動探索
# ==========================================================
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
# HTML解析
# ==========================================================
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
# main
# ==========================================================
def main():
    urls = build_target_urls_auto()
    ts = now_utc_iso()
    clinics = []

    for url in urls:
        print(f"[Fetch] {url}")
        html = fetch(url)
        cards = parse_page(html, url)
        for c in cards:
            clinics.append({
                "timestamp_utc": ts,
                "clinic_id": get_clinic_id_from_url(c.get("clinic_url")),
                "name": c.get("name", ""),
                "rating": c.get("rating"),
                "reviews": c.get("reviews"),
                "clinic_url": c.get("clinic_url")
            })
        time.sleep(1)

    os.makedirs("output", exist_ok=True)
    df = pd.DataFrame(clinics)
    df.to_csv("output/clinics.csv", index=False, encoding="utf-8-sig")
    print(f"[Saved] output/clinics.csv ({len(df)} rows)")
    print("[DONE] Scraping complete ✅")

if __name__ == "__main__":
    main()
