# bgg_crawl_api_regex.py
# -*- coding: utf-8 -*-
"""
Crawler สำหรับ BoardGameGeek แบบใช้ API แต่ "พาร์สด้วย REGEX จาก resp.text" (ไม่ใช้ resp.json()):
- ดึงรายชื่อ "หมวด (categories)" จากหน้า index (requests + regex)
- เลือกช่วงหมวดด้วย START_CATEGORY..END_CATEGORY (exclusive)
- ต่อหมวด: เรียก API /api/geekitem/linkeditems แล้วแตก items ด้วย regex
- เลือกเอารูปจากฟิลด์ใน API (regex) และอัปเกรดรูปด้วย og:image (optional)
- กัน rate-limit: random delay + exponential backoff 429/5xx
- บันทึก CSV: [category, name, year, url, image_url]
"""

import re
import csv
import time
import html
import random
import requests
from urllib.parse import urljoin

# ----------------------------
# Config
# ----------------------------
SITE_ROOT  = "https://boardgamegeek.com"
INDEX_URL  = "https://boardgamegeek.com/browse/boardgamecategory"

# เลือกช่วงหมวดด้วย index (0-based), END_CATEGORY เป็น exclusive
START_CATEGORY = 0
END_CATEGORY   = None  # None = ไปจนจบ

# ตั้งค่าดึงต่อหมวด
MAX_PAGES_PER_CAT  = 3
SHOWCOUNT          = 25
TARGET_PER_CAT     = 50

# อัปเกรดภาพจากหน้าเกม (ดึง og:image)
UPGRADE_IMAGES       = True
MAX_UPGRADE_PER_CAT  = 120
UPGRADE_DELAY_RANGE  = (0.25, 0.5)  # หน่วงสุ่มระหว่างอัปเกรดภาพ/ต่อเกม

# ระยะห่างระหว่างเรียก API เพื่อลดโอกาสโดน 429
API_DELAY_RANGE      = (0.2, 0.4)

# ไฟล์ผลลัพธ์
OUTFILE = "boardgame_categories_with_images_by_api_regex.csv"

# ----------------------------
# Regex (HTML: หน้า index)
# ----------------------------
CAT_RE = re.compile(r'href="(/boardgamecategory/\d+/[^"]+)"[^>]*>([^<]+)</a>')

OG_IMG_RE   = re.compile(
    r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
    re.IGNORECASE
)
LINK_IMG_RE = re.compile(
    r'<link[^>]+rel=["\']image_src["\'][^>]+href=["\']([^"\']+)["\']',
    re.IGNORECASE
)

TAG_RE  = re.compile(r"<[^>]+>")
WS_RE   = re.compile(r"\s+")

# ----------------------------
# Regex (API: พาร์สจาก resp.text)
# ----------------------------

# หา array ของ items ภายใน object response
ARRAY_IN_OBJECT_RE = re.compile(
    r'"(?:items|linkeditems|results)"\s*:\s*(\[(?:.|\n|\r)*?\])',
    re.IGNORECASE
)

# split objects ชั้นบนใน array (สมมุติว่าไม่มี '}{' ชิดกันใน nested object)
TOP_OBJECT_SPLIT_RE = re.compile(r'\}\s*,\s*\{')

# ยูทิลิตี้สร้าง regex สำหรับชนิดค่าต่าง ๆ
def _rx_str(key):
    # จับสตริง JSON โดยยอมให้มีอักขระ escape ภายใน เช่น \" \\ \n \uXXXX
    return re.compile(
        rf'"{re.escape(key)}"\s*:\s*"((?:\\.|[^"\\])*)"',
        re.IGNORECASE
    )

def _rx_num(key):  return re.compile(rf'"{re.escape(key)}"\s*:\s*"?(-?\d+(?:\.\d+)?)"?', re.IGNORECASE)
def _rx_bool(key): return re.compile(rf'"{re.escape(key)}"\s*:\s*(true|false)', re.IGNORECASE)

# ฟิลด์ที่สนใจ
RX_ID_OBJID = re.compile(r'"(?:objectid|id)"\s*:\s*"?(\d+)"?', re.IGNORECASE)
RX_NAME     = _rx_str("name")
RX_YEAR     = _rx_num("yearpublished")
RX_HREF     = _rx_str("href")
RX_URL      = _rx_str("url")
RX_SUBTYPE  = _rx_str("subtype")
RX_TYPE     = _rx_str("type")

# รูป: images.original (เจอบ่อยสุดใน BGG)
RX_IMG_ORIGINAL = re.compile(
    r'"images"\s*:\s*\{[^{}]*?"original"\s*:\s*"(.*?)"',
    re.IGNORECASE | re.DOTALL
)
RX_IMAGEURL = _rx_str("imageurl")
RX_IMAGE    = _rx_str("image")

# ----------------------------
# Utils
# ----------------------------

# แปลง \uXXXX และ escape อื่น ๆ ในสตริง JSON ที่เราไม่ได้ใช้ json.loads
_hex_esc_re = re.compile(r'\\u([0-9a-fA-F]{4})')
def unescape_json_unicode(s: str) -> str:
    if not s:
        return s
    s = _hex_esc_re.sub(lambda m: chr(int(m.group(1), 16)), s)
    # แปลง escape ทั่วไป
    s = s.replace(r'\"', '"').replace(r"\/", "/").replace(r"\\", "\\")
    s = s.replace(r"\b", "\b").replace(r"\f", "\f").replace(r"\n", "\n").replace(r"\r", "\r").replace(r"\t", "\t")
    return s

# หา id จากลิงก์
ID_RE = re.compile(r"/boardgame(?:expansion)?/(\d+)")

def clean_text(s: str) -> str:
    s = html.unescape(TAG_RE.sub("", s))
    s = WS_RE.sub(" ", s)
    return s.strip()

def to_abs(path: str) -> str:
    if not path:
        return ""
    if path.startswith("//"):
        return "https:" + path
    if path.startswith("/"):
        return urljoin(SITE_ROOT, path)
    return path

def extract_categories_from_index(session: requests.Session) -> list[tuple[str, str]]:
    """
    ดึงรายชื่อหมวดจากหน้า index (HTML) อย่างเบา ๆ
    คืน: [(category_name, category_url_abs), ...]
    """
    r = session.get(INDEX_URL, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    html_src = r.text

    cats, seen = [], set()
    for m in CAT_RE.finditer(html_src):
        rel = m.group(1)
        name = clean_text(m.group(2))
        if not name:
            continue
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
    if isinstance(gid, str) and gid.isdigit():
        return int(gid)
    if isinstance(gid, int):
        return gid
    for k in ("href", "url"):
        v = it.get(k) or ""
        m = ID_RE.search(v)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass
    return None

def is_expansion(it: dict) -> bool:
    st = (it.get("subtype") or it.get("type") or "").lower()
    if st == "boardgameexpansion":
        return True
    for k in ("href", "url"):
        v = (it.get(k) or "").lower()
        if "/boardgameexpansion/" in v:
            return True
    return False

def pick_image_from_item(it: dict) -> str:
    """
    รองรับทั้งฟิลด์ที่พาร์สขึ้นมา (images_original / imageurl / image)
    และโครงสร้าง images:{original,...} เผื่อไว้
    """
    for k in ("images_original", "imageurl", "image"):
        v = it.get(k)
        if v:
            return to_abs(v)

    images = it.get("images") or {}
    if isinstance(images, dict):
        for kk in ("original", "large", "medium", "small"):
            if images.get(kk):
                return to_abs(images[kk])
    return ""

def parse_year(it: dict) -> str:
    y = it.get("yearpublished") or it.get("year") or ""
    return str(y) if y else ""

def item_url(it: dict) -> str:
    if it.get("href"):
        return to_abs(it["href"])
    if it.get("url"):
        return to_abs(it["url"])
    gid = it.get("objectid") or it.get("id")
    if gid:
        return to_abs(f"/boardgame/{gid}")
    return ""

def fetch_detail_image_http(url: str, session: requests.Session, timeout=20) -> str:
    try:
        r = session.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        m = OG_IMG_RE.search(r.text) or LINK_IMG_RE.search(r.text)
        if m:
            return to_abs(m.group(1).strip())
    except Exception:
        pass
    return ""

# ----------------------------
# พาร์สรายการจาก API resp.text ด้วย regex
# ----------------------------
def _slice_array_after_key(text: str, key_regex: re.Pattern) -> str | None:
    """
    หา array ที่ตามหลังคีย์ (เช่น "items": [ ... ]) แล้วคืนซับสตริงตั้งแต่ '[' ถึง ']' ที่ depth=0
    ใช้วิธีนับวงเล็บ [] ให้ครบคู่ (ข้ามในสตริง)
    """
    m = key_regex.search(text)
    if not m:
        return None
    i = m.end()          # ชี้หลัง '[' ตัวแรก (เพราะ regex รวม '[' ไว้แล้ว)
    # ย้อนกลับหนึ่งตัวเพื่อให้เริ่มที่ '[' จริง ๆ
    i -= 1
    if i < 0 or text[i] != '[':
        # กันเผื่อ regex ไม่ตรง
        while i < len(text) and text[i] != '[':
            i += 1
        if i >= len(text):
            return None

    depth = 0
    in_str = False
    esc = False
    start = i
    for j in range(i, len(text)):
        ch = text[j]
        if in_str:
            if esc:
                esc = False
            elif ch == '\\':
                esc = True
            elif ch == '"':
                in_str = False
            continue
        else:
            if ch == '"':
                in_str = True
                continue
            if ch == '[':
                depth += 1
            elif ch == ']':
                depth -= 1
                if depth == 0:
                    return text[start:j+1]
    return None


def _split_array_items_jsonish(array_text: str) -> list[str]:
    """
    รับสตริงของ array เช่น: [{...}, {...}, {...}] แล้วคืนลิสต์ของชิ้น object ชั้นบน
    ใช้นับวงเล็บ { } และข้ามในสตริง
    """
    s = array_text.strip()
    if s.startswith('['):
        s = s[1:]
    if s.endswith(']'):
        s = s[:-1]

    parts = []
    depth = 0
    in_str = False
    esc = False
    obj_start = None

    for idx, ch in enumerate(s):
        if in_str:
            if esc:
                esc = False
            elif ch == '\\':
                esc = True
            elif ch == '"':
                in_str = False
            continue

        if ch == '"':
            in_str = True
            continue

        if ch == '{':
            if depth == 0:
                obj_start = idx
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0 and obj_start is not None:
                parts.append(s[obj_start:idx+1])
                obj_start = None
        # เครื่องหมายคอมม่าไม่ต้องทำอะไร เพราะเราตัดตาม depth=0 อยู่แล้ว

    return parts


def parse_api_items_from_text(text: str) -> list[dict]:
    """
    พาร์ส resp.text: หาชุด "items": [ ... ] แบบนับวงเล็บ แล้วแตก object ชั้นบนอย่างปลอดภัย
    จากนั้นดึงฟิลด์สำคัญด้วย regex ตามเดิม
    """
    if not text:
        return []

    # 1) หา array ของ items โดยนับวงเล็บ แทน non-greedy regex
    key_rx = re.compile(r'"(?:items|linkeditems|results)"\s*:\s*\[', re.IGNORECASE)
    array_text = None

    stripped = text.lstrip()
    if stripped.startswith('['):
        # ทั้งไฟล์เป็น array
        array_text = _slice_array_after_key('items:' + text, re.compile(r'items:\[', re.IGNORECASE))
    else:
        array_text = _slice_array_after_key(text, key_rx)

    if not array_text:
        return []

    # 2) แยก object ชั้นบนทุกชิ้นใน array
    raw_objs = _split_array_items_jsonish(array_text)
    if not raw_objs:
        return []

    items: list[dict] = []

    for chunk in raw_objs:
        item: dict = {}

        # id/objectid
        m = RX_ID_OBJID.search(chunk)
        if m:
            gid = int(m.group(1))
            item["id"] = gid
            item["objectid"] = gid

        # name (แปลง \uXXXX → อักขระจริง)
        m = RX_NAME.search(chunk)
        if m:
            item["name"] = unescape_json_unicode(m.group(1))

        # year (อาจ quoted)
        m = RX_YEAR.search(chunk)
        if m:
            try:
                item["yearpublished"] = int(float(m.group(1)))
            except Exception:
                pass

        # href/url (แก้ \/ → /)
        m = RX_HREF.search(chunk)
        if m:
            item["href"] = unescape_json_unicode(m.group(1))
        m = RX_URL.search(chunk)
        if m and "url" not in item:
            item["url"] = unescape_json_unicode(m.group(1))

        # subtype/type
        m = RX_SUBTYPE.search(chunk)
        if m:
            item["subtype"] = m.group(1)
        m = RX_TYPE.search(chunk)
        if m and "type" not in item:
            item["type"] = m.group(1)

        # รูป
        m = RX_IMG_ORIGINAL.search(chunk)
        if m:
            item["images_original"] = unescape_json_unicode(m.group(1))
        else:
            m = RX_IMAGEURL.search(chunk)
            if m:
                item["imageurl"] = unescape_json_unicode(m.group(1))
            else:
                m = RX_IMAGE.search(chunk)
                if m:
                    item["image"] = unescape_json_unicode(m.group(1))

        items.append(item)

    return items

# ----------------------------
# API Calls (fetch -> regex parse)
# ----------------------------
API_BASE = "https://api.geekdo.com/api/geekitem/linkeditems"

def api_fetch_page(session: requests.Session, *, objectid: int, pageid: int, showcount: int, sort="name", subtype="boardgamecategory") -> list[dict]:
    """
    เรียกหน้าเดียวของรายการเกมที่ลิงก์กับหมวด (property)
    คืน list ของ item (dict) ที่ได้จากการ "regex พาร์ส resp.text"
    """
    params = {
        "ajax": 1,
        "nosession": 1,
        "objecttype": "property",
        "objectid": objectid,
        "linkdata_index": "boardgame",
        "pageid": pageid,
        "showcount": showcount,
        "sort": sort,
        "subtype": subtype,
    }

    for attempt in range(6):
        try:
            resp = session.get(API_BASE, params=params, timeout=25, headers={"User-Agent": "Mozilla/5.0"})
            status = resp.status_code
            if status in (429, 502, 503, 504):
                wait = (attempt + 1) * 2 + random.random()
                print(f"  API {status}, backoff {wait:.1f}s ...")
                time.sleep(wait)
                continue
            resp.raise_for_status()

            # ใช้ regex จาก resp.text
            # print(resp.text)
            items = parse_api_items_from_text(resp.text)
            # print(f"Parsed items22: {items}")
            return items or []
        except Exception as e:
            wait = (attempt + 1) * 1.5 + random.random()
            print(f"  API error: {e}; retry in {wait:.1f}s")
            time.sleep(wait)
    return []

# ----------------------------
# Crawl
# ----------------------------
def crawl_category_via_api(category_name: str, category_id: int, session: requests.Session,
                           seen_ids: set[int]) -> list[tuple[str, str, str, str, str]]:
    """
    ดึงเกมตามหมวดด้วย API + regex parser
    คืน list ของ (category, name, year, url, image_url)
    - กรอง expansion ออก
    - กันซ้ำโดยดูจาก game id (ข้ามหมวด/หลายหน้า)
    """
    rows = []
    upgraded = 0

    for page in range(1, MAX_PAGES_PER_CAT + 1):
        print(f"[{category_name}] API page {page}")
        items = api_fetch_page(session, objectid=category_id, pageid=page, showcount=SHOWCOUNT)
        if not items:
            print("  (empty) stop.")
            break
        
        # print(f"  Fetched {len(items)} items from API")
        # print(items)

        for it in items:
            # ข้าม expansions
            if is_expansion(it):
                continue

            gid = get_game_id(it)
            if not gid:
                # ไม่มี id เชื่อถือได้ ข้ามเพื่อตัดปัญหาซ้ำ
                continue
            if gid in seen_ids:
                # เคยเก็บไปแล้วจากหมวดก่อนหน้า/หน้าก่อนหน้า
                continue

            name = (it.get("name") or it.get("objectname") or "").strip()
            year = parse_year(it)
            if not name or not year:
                continue  # ต้องการให้เก็บแม้ไม่มีปี ให้ผ่อนกฎตรงนี้

            url = item_url(it)
            img = pick_image_from_item(it)

            final_img = img
            if UPGRADE_IMAGES and upgraded < MAX_UPGRADE_PER_CAT and url:
                hi = fetch_detail_image_http(url, session)
                if hi:
                    final_img = hi
                upgraded += 1
                time.sleep(random.uniform(*UPGRADE_DELAY_RANGE))

            rows.append((category_name, name, year, url, final_img))
            seen_ids.add(gid)  # กันซ้ำด้วย id ที่ระดับ global

            if len(rows) >= TARGET_PER_CAT:
                break

        time.sleep(random.uniform(*API_DELAY_RANGE))
        if len(rows) >= TARGET_PER_CAT:
            break

    return rows

# ----------------------------
# Main
# ----------------------------
def main():
    s = requests.Session()

    print("Fetching categories index ...")
    categories = extract_categories_from_index(s)
    print(f"Found categories: {len(categories)}")

    cats_window = categories[START_CATEGORY:END_CATEGORY]
    print(f"Category window: [{START_CATEGORY}:{END_CATEGORY}] -> {len(cats_window)} items")

    all_rows = []
    seen_ids: set[int] = set()  # กันซ้ำข้ามหมวด/หน้า

    for cat_name, cat_url in cats_window:
        cat_id = extract_category_id(cat_url)
        if not cat_id:
            print(f"Skip (cannot find id): {cat_name} -> {cat_url}")
            continue

        rows = crawl_category_via_api(cat_name, cat_id, s, seen_ids)
        all_rows.extend(rows)

    with open(OUTFILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["category", "name", "year", "url", "image_url"])
        w.writerows(all_rows)

    print(f"Saved -> {OUTFILE}")
    print("Total rows:", len(all_rows))


if __name__ == "__main__":
    main()
