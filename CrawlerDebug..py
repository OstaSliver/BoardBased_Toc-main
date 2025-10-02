# bgg_crawl_api_regex_verbose.py
# -*- coding: utf-8 -*-

import re, csv, time, html, random, requests, logging
from urllib.parse import urljoin
from contextlib import contextmanager

# ----------------------------
# Logging
# ----------------------------
LOG_LEVEL = logging.INFO  # เปลี่ยนเป็น logging.DEBUG ถ้าอยากเห็นละเอียดมาก
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bgg")

def _fmt_sec(s: float) -> str:
    return f"{s:.2f}s"

@contextmanager
def _timer(msg: str):
    start = time.time()
    log.info(msg + " ...")
    try:
        yield
    finally:
        dur = time.time() - start
        log.info(f"{msg} ✓ ({_fmt_sec(dur)})")

# ----------------------------
# Config
# ----------------------------
SITE_ROOT  = "https://boardgamegeek.com"
INDEX_URL  = "https://boardgamegeek.com/browse/boardgamecategory"

START_CATEGORY = 0
END_CATEGORY   = None  # None = ไปจนจบ

MAX_PAGES_PER_CAT  = 4
SHOWCOUNT          = 25
TARGET_PER_CAT     = 50

UPGRADE_IMAGES       = True
MAX_UPGRADE_PER_CAT  = 120
UPGRADE_DELAY_RANGE  = (0.25, 0.5)

API_DELAY_RANGE      = (0.2, 0.4)
OUTFILE = "boardgame_categories_with_images_by_api_regex.csv"

# ----------------------------
# Regex (HTML)
# ----------------------------
CAT_RE = re.compile(r'href="(/boardgamecategory/\d+/[^"]+)"[^>]*>([^<]+)</a>')
OG_IMG_RE   = re.compile(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', re.IGNORECASE)
LINK_IMG_RE = re.compile(r'<link[^>]+rel=["\']image_src["\'][^>]+href=["\']([^"\']+)["\']', re.IGNORECASE)
TAG_RE  = re.compile(r"<[^>]+>")
WS_RE   = re.compile(r"\s+")

# ----------------------------
# Regex (API / resp.text)
# ----------------------------
def _rx_str(key):
    return re.compile(rf'"{re.escape(key)}"\s*:\s*"((?:\\.|[^"\\])*)"', re.IGNORECASE)
def _rx_num(key):
    return re.compile(rf'"{re.escape(key)}"\s*:\s*"?(-?\d+(?:\.\d+)?)"?', re.IGNORECASE)

RX_ID_OBJID = re.compile(r'"(?:objectid|id)"\s*:\s*"?(\d+)"?', re.IGNORECASE)
RX_NAME     = _rx_str("name")
RX_YEAR     = _rx_num("yearpublished")
RX_HREF     = _rx_str("href")
RX_URL      = _rx_str("url")
RX_SUBTYPE  = _rx_str("subtype")
RX_TYPE     = _rx_str("type")
RX_IMG_ORIGINAL = re.compile(r'"images"\s*:\s*\{[^{}]*?"original"\s*:\s*"(.*?)"', re.IGNORECASE | re.DOTALL)
RX_IMAGEURL = _rx_str("imageurl")
RX_IMAGE    = _rx_str("image")

# ----------------------------
# Utils
# ----------------------------
_hex_esc_re = re.compile(r'\\u([0-9a-fA-F]{4})')
def unescape_json_unicode(s: str) -> str:
    if not s: return s
    s = _hex_esc_re.sub(lambda m: chr(int(m.group(1), 16)), s)
    s = s.replace(r'\"', '"').replace(r"\/", "/").replace(r"\\", "\\")
    s = s.replace(r"\b", "\b").replace(r"\f", "\f").replace(r"\n", "\n").replace(r"\r", "\r").replace(r"\t", "\t")
    return s

ID_RE = re.compile(r"/boardgame(?:expansion)?/(\d+)")
def clean_text(s: str) -> str:
    s = html.unescape(TAG_RE.sub("", s))
    s = WS_RE.sub(" ", s)
    return s.strip()

def to_abs(path: str) -> str:
    if not path: return ""
    if path.startswith("//"): return "https:" + path
    if path.startswith("/"):  return urljoin(SITE_ROOT, path)
    return path

def extract_categories_from_index(session: requests.Session) -> list[tuple[str, str]]:
    r = session.get(INDEX_URL, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    html_src = r.text
    cats, seen = [], set()
    for m in CAT_RE.finditer(html_src):
        rel = m.group(1)
        name = clean_text(m.group(2))
        if not name: continue
        abs_url = urljoin(SITE_ROOT, rel)
        if abs_url not in seen:
            seen.add(abs_url)
            cats.append((name, abs_url))
    return cats

def extract_category_id(cat_url: str) -> int | None:
    m = re.search(r"/boardgamecategory/(\d+)", cat_url)
    return int(m.group(1)) if m else None

def get_game_id(it: dict) -> int | None:
    gid = it.get("objectid") or it.get("id")
    if isinstance(gid, str) and gid.isdigit(): return int(gid)
    if isinstance(gid, int): return gid
    for k in ("href", "url"):
        v = it.get(k) or ""
        m = ID_RE.search(v)
        if m:
            try: return int(m.group(1))
            except: pass
    return None

def is_expansion(it: dict) -> bool:
    st = (it.get("subtype") or it.get("type") or "").lower()
    if st == "boardgameexpansion": return True
    for k in ("href", "url"):
        v = (it.get(k) or "").lower()
        if "/boardgameexpansion/" in v: return True
    return False

def pick_image_from_item(it: dict) -> str:
    for k in ("images_original", "imageurl", "image"):
        v = it.get(k)
        if v: return to_abs(v)
    images = it.get("images") or {}
    if isinstance(images, dict):
        for kk in ("original", "large", "medium", "small"):
            if images.get(kk): return to_abs(images[kk])
    return ""

def parse_year(it: dict) -> str:
    y = it.get("yearpublished") or it.get("year") or ""
    return str(y) if y else ""

def item_url(it: dict) -> str:
    if it.get("href"): return to_abs(it["href"])
    if it.get("url"):  return to_abs(it["url"])
    gid = it.get("objectid") or it.get("id")
    if gid: return to_abs(f"/boardgame/{gid}")
    return ""

def fetch_detail_image_http(url: str, session: requests.Session, timeout=20) -> str:
    try:
        r = session.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        m = OG_IMG_RE.search(r.text) or LINK_IMG_RE.search(r.text)
        if m: return to_abs(m.group(1).strip())
    except Exception as e:
        log.debug(f"  upgrade-image failed for {url}: {e}")
    return ""

# ----------------------------
# Robust array slicing / splitting
# ----------------------------
def _slice_array_after_key(text: str, key_regex: re.Pattern) -> str | None:
    m = key_regex.search(text)
    if not m: return None
    i = m.end() - 1
    while i < len(text) and text[i] != '[':
        i += 1
    if i >= len(text): return None

    depth = 0; in_str = False; esc = False; start = i
    for j in range(i, len(text)):
        ch = text[j]
        if in_str:
            if esc: esc = False
            elif ch == '\\': esc = True
            elif ch == '"': in_str = False
            continue
        else:
            if ch == '"': in_str = True; continue
            if ch == '[': depth += 1
            elif ch == ']':
                depth -= 1
                if depth == 0: return text[start:j+1]
    return None

def _split_array_items_jsonish(array_text: str) -> list[str]:
    s = array_text.strip()
    if s.startswith('['): s = s[1:]
    if s.endswith(']'):   s = s[:-1]
    parts = []
    depth = 0; in_str = False; esc = False; obj_start = None
    for idx, ch in enumerate(s):
        if in_str:
            if esc: esc = False
            elif ch == '\\': esc = True
            elif ch == '"': in_str = False
            continue
        if ch == '"': in_str = True; continue
        if ch == '{':
            if depth == 0: obj_start = idx
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0 and obj_start is not None:
                parts.append(s[obj_start:idx+1]); obj_start = None
    return parts

def parse_api_items_from_text(text: str) -> list[dict]:
    if not text: return []
    key_rx = re.compile(r'"(?:items|linkeditems|results)"\s*:\s*\[', re.IGNORECASE)
    stripped = text.lstrip()
    if stripped.startswith('['):
        array_text = _slice_array_after_key('items:' + text, re.compile(r'items:\[', re.IGNORECASE))
    else:
        array_text = _slice_array_after_key(text, key_rx)
    if not array_text: return []

    raw_objs = _split_array_items_jsonish(array_text)
    if not raw_objs: return []

    items = []
    for chunk in raw_objs:
        item = {}
        m = RX_ID_OBJID.search(chunk)
        if m:
            gid = int(m.group(1))
            item["id"] = gid
            item["objectid"] = gid

        m = RX_NAME.search(chunk)
        if m: item["name"] = unescape_json_unicode(m.group(1))

        m = RX_YEAR.search(chunk)
        if m:
            try: item["yearpublished"] = int(float(m.group(1)))
            except: pass

        m = RX_HREF.search(chunk)
        if m: item["href"] = unescape_json_unicode(m.group(1))
        m = RX_URL.search(chunk)
        if m and "url" not in item: item["url"] = unescape_json_unicode(m.group(1))

        m = RX_SUBTYPE.search(chunk)
        if m: item["subtype"] = m.group(1)
        m = RX_TYPE.search(chunk)
        if m and "type" not in item: item["type"] = m.group(1)

        m = RX_IMG_ORIGINAL.search(chunk)
        if m:
            item["images_original"] = unescape_json_unicode(m.group(1))
        else:
            m = RX_IMAGEURL.search(chunk)
            if m: item["imageurl"] = unescape_json_unicode(m.group(1))
            else:
                m = RX_IMAGE.search(chunk)
                if m: item["image"] = unescape_json_unicode(m.group(1))

        items.append(item)
    return items

# ----------------------------
# API Calls (fetch -> regex parse)
# ----------------------------
API_BASE = "https://api.geekdo.com/api/geekitem/linkeditems"

def api_fetch_page(session: requests.Session, *, objectid: int, pageid: int, showcount: int,
                   sort="name", subtype="boardgamecategory") -> list[dict]:
    params = {
        "ajax": 1, "nosession": 1, "objecttype": "property",
        "objectid": objectid, "linkdata_index": "boardgame",
        "pageid": pageid, "showcount": showcount, "sort": sort, "subtype": subtype,
    }
    log.debug(f"  GET {API_BASE} params={params}")
    total_wait = 0.0

    for attempt in range(6):
        try:
            resp = session.get(API_BASE, params=params, timeout=25, headers={"User-Agent": "Mozilla/5.0"})
            status = resp.status_code
            if status in (429, 502, 503, 504):
                wait = (attempt + 1) * 2 + random.random()
                total_wait += wait
                log.warning(f"  API {status}; backoff {wait:.1f}s (attempt {attempt+1}/6, cum wait {_fmt_sec(total_wait)})")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            items = parse_api_items_from_text(resp.text)
            log.info(f"  API page parsed: {len(items)} raw items")
            return items or []
        except Exception as e:
            wait = (attempt + 1) * 1.5 + random.random()
            total_wait += wait
            log.error(f"  API error: {e}; retry in {wait:.1f}s (attempt {attempt+1}/6)")
            time.sleep(wait)
    log.error("  API failed after retries.")
    return []

# ----------------------------
# Crawl (with detailed stats)
# ----------------------------
def crawl_category_via_api(category_name: str, category_id: int, session: requests.Session,
                           seen_ids: set[int]) -> list[tuple[str, str, str, str, str]]:
    rows = []
    upgraded = 0

    # สถิติในหมวด
    kept = 0
    skipped_expansion = 0
    skipped_dup = 0
    skipped_no_id = 0
    skipped_missing_fields = 0
    upgrade_hit = 0
    upgrade_miss = 0

    t0 = time.time()

    for page in range(1, MAX_PAGES_PER_CAT + 1):
        log.info(f"[{category_name}] API page {page}")
        items = api_fetch_page(session, objectid=category_id, pageid=page, showcount=SHOWCOUNT)
        if not items:
            log.info("  (no items) stop this category page loop.")
            break

        page_kept_before = kept

        for it in items:
            if is_expansion(it):
                skipped_expansion += 1
                continue

            gid = get_game_id(it)
            if not gid:
                skipped_no_id += 1
                continue
            if gid in seen_ids:
                skipped_dup += 1
                continue

            name = (it.get("name") or it.get("objectname") or "").strip()
            year = parse_year(it)
            if not name or not year:
                skipped_missing_fields += 1
                continue

            url = item_url(it)
            img = pick_image_from_item(it)

            final_img = img
            if UPGRADE_IMAGES and upgraded < MAX_UPGRADE_PER_CAT and url:
                hi = fetch_detail_image_http(url, session)
                if hi:
                    final_img = hi
                    upgrade_hit += 1
                else:
                    upgrade_miss += 1
                upgraded += 1
                time.sleep(random.uniform(*UPGRADE_DELAY_RANGE))

            rows.append((category_name, name, year, url, final_img))
            kept += 1
            seen_ids.add(gid)

            if kept >= TARGET_PER_CAT:
                break

        log.info(
            f"  Page summary: kept {kept - page_kept_before}, "
            f"expansion {skipped_expansion}, dup {skipped_dup}, "
            f"no_id {skipped_no_id}, miss_fields {skipped_missing_fields}, "
            f"upgrade hit/miss {upgrade_hit}/{upgrade_miss}"
        )

        time.sleep(random.uniform(*API_DELAY_RANGE))
        if kept >= TARGET_PER_CAT:
            break

    dur = time.time() - t0
    log.info(
        f"[{category_name}] Category summary: kept={kept}, exp={skipped_expansion}, dup={skipped_dup}, "
        f"no_id={skipped_no_id}, miss_fields={skipped_missing_fields}, "
        f"upgrade hit/miss={upgrade_hit}/{upgrade_miss}, time={_fmt_sec(dur)}"
    )
    return rows

# ----------------------------
# Main
# ----------------------------
def main():
    total_t0 = time.time()
    s = requests.Session()

    with _timer("Fetching categories index"):
        categories = extract_categories_from_index(s)
        log.info(f"Found categories: {len(categories)}")

    cats_window = categories[START_CATEGORY:END_CATEGORY]
    log.info(f"Category window: [{START_CATEGORY}:{END_CATEGORY}] -> {len(cats_window)} items")

    all_rows = []
    seen_ids: set[int] = set()

    for i, (cat_name, cat_url) in enumerate(cats_window, start=1):
        cat_id = extract_category_id(cat_url)
        if not cat_id:
            log.warning(f"Skip (cannot find id): {cat_name} -> {cat_url}")
            continue

        log.info(f"[{i}/{len(cats_window)}] Start category: {cat_name} (id={cat_id})")
        cat_rows = crawl_category_via_api(cat_name, cat_id, s, seen_ids)
        all_rows.extend(cat_rows)
        log.info(f"[{cat_name}] collected rows so far: {len(all_rows)}")

    with _timer(f"Writing CSV -> {OUTFILE}"):
        with open(OUTFILE, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["category", "name", "year", "url", "image_url"])
            w.writerows(all_rows)
        log.info(f"CSV rows written: {len(all_rows)}")

    total_dur = time.time() - total_t0
    log.info(f"ALL DONE. Total rows={len(all_rows)}; total time={_fmt_sec(total_dur)}")

if __name__ == "__main__":
    main()
