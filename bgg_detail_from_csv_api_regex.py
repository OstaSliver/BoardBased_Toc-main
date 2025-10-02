# -*- coding: utf-8 -*-
"""
อ่าน CSV ที่มีคอลัมน์ 'url' (ไปหน้าเกมบน BGG)
จากนั้น:
- แงะ gid จาก url (ด้วย regex)
- เรียก XML API: /xmlapi2/thing?id=<gid>&stats=1
- ใช้ "regex" ล้วน แกะค่าออกมาจาก XML (ไม่ใช้ xml.etree/json เลย)
- เก็บ: title, players_min/max, time_min/max, age_plus, weight_5,
        description, alternate_names, designers, artists, publishers
- รูปจากหน้าเกม: og_image, primary_image (regex จาก HTML)
- รูปจากหน้า Gallery (optional): regex จาก HTML

หมายเหตุ: ยังกัน rate-limit (sleep แบบสุ่ม) และมี backoff เบื้องต้น
"""

import csv
import re
import time
import html
import random
import requests
from urllib.parse import urljoin

# --------------- Config ----------------
# INPUT_CSV = "boardgame_categories_with_images_by_api2.csv"
INPUT_CSV = "boardgame_categories_with_images_by_api_regex.csv"
OUTPUT_CSV = "bgg_details_from_urls_api_regex.csv"
SITE_ROOT = "https://boardgamegeek.com"

FETCH_GALLERY = True
MAX_GALLERY_IMAGES = 12
GALLERY_DELAY_RANGE = (0.35, 0.8)
PAGE_DELAY_RANGE = (0.25, 0.6)

# --------------- Regex -----------------
# จากหน้าเกม (HTML) สำหรับรูป/title เฉพาะ
ID_RE = re.compile(r"/boardgame(?:expansion)?/(\d+)")
OG_IMG_RE = re.compile(
    r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', re.I
)
LINK_IMG_RE = re.compile(
    r'<link[^>]+rel=["\']image_src["\'][^>]+href=["\']([^"\']+)["\']', re.I
)
TITLE_H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.I | re.S)
DESC_BLOCK_RE = re.compile(
    r"<h2[^>]*>\s*Description\s*</h2>(.*?)</section>", re.I | re.S
)
META_DESC_RE = re.compile(
    r'<meta\s+name=["\']description["\']\s+content=["\']([^"\']+)["\']', re.I
)
IMG_TAG_RE = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.I)
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")

# จาก XML API (regex ล้วน)
# ชื่อหลักและชื่อรอง
PRIMARY_NAME_RE = re.compile(
    r"<name[^>]*\stype=['\"]primary['\"][^>]*\svalue=['\"](.*?)['\"][^>]*/?>",
    re.I | re.S,
)
ALT_NAME_RE = re.compile(
    r"<name[^>]*\stype=['\"]alternate['\"][^>]*\svalue=['\"](.*?)['\"][^>]*/?>",
    re.I | re.S,
)
# ค่าตัวเลข/ตัวชี้วัด
ATTR_VAL_RE = lambda tag: re.compile(
    rf"<{tag}[^>]*\svalue=['\"](\d+)['\"][^>]*/?>", re.I
)
MINPLAY_RE = ATTR_VAL_RE("minplaytime")
MAXPLAY_RE = ATTR_VAL_RE("maxplaytime")
MINPLAYERS_RE = ATTR_VAL_RE("minplayers")
MAXPLAYERS_RE = ATTR_VAL_RE("maxplayers")
MINAGE_RE = ATTR_VAL_RE("minage")
WEIGHT_RE_XML = re.compile(
    r"<averageweight[^>]*\svalue=['\"]([0-9.]+)['\"][^>]*/?>", re.I
)

AVERAGE_RATING_RE = re.compile(
    r"<average[^>]*\svalue=['\"]([0-9.]+)['\"][^>]*/?>", re.I
)

# คำอธิบาย (อาจมี \n และ entities)
DESC_XML_RE = re.compile(r"<description>([\s\S]*?)</description>", re.I)
# ลิงก์เครดิต
LINK_RE = re.compile(
    r"<link[^>]*\stype=['\"](boardgamedesigner|boardgameartist|boardgamepublisher)['\"][^>]*\svalue=['\"](.*?)['\"][^>]*/?>",
    re.I,
)


def clean_html_text(s: str) -> str:
    if not s:
        return ""
    s = html.unescape(TAG_RE.sub(" ", s))
    return WS_RE.sub(" ", s).strip()


def to_abs(url: str) -> str:
    if not url:
        return ""
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return urljoin(SITE_ROOT, url)
    return url


# --------------- HTTP helper -----------
def http_get_text(
    session: requests.Session, url: str, *, timeout=25, max_retry=6
) -> str:
    for attempt in range(max_retry):
        try:
            r = session.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code in (429, 502, 503, 504):
                wait = (attempt + 1) * 2 + random.random()
                print(f"  HTTP {r.status_code} -> backoff {wait:.1f}s ({url})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.text
        except Exception as e:
            wait = (attempt + 1) * 1.2 + random.random()
            print(f"  HTTP error: {e} -> retry in {wait:.1f}s ({url})")
            time.sleep(wait)
    return ""


# --------------- Parsers (HTML) --------
def parse_title_from_html(html_src: str) -> str:
    m = TITLE_H1_RE.search(html_src)
    return clean_html_text(m.group(1)) if m else ""


def parse_images_from_html(html_src: str) -> tuple[str, str]:
    og = ""
    m = OG_IMG_RE.search(html_src)
    if m:
        og = to_abs(m.group(1).strip())
    primary = ""
    m2 = LINK_IMG_RE.search(html_src)
    if m2:
        primary = to_abs(m2.group(1).strip())
    if not primary:
        primary = og
    return og, primary


def parse_description_from_html(html_src: str) -> str:
    m = DESC_BLOCK_RE.search(html_src)
    if m:
        return clean_html_text(m.group(1))
    m = META_DESC_RE.search(html_src)
    if m:
        return clean_html_text(m.group(1))
    return ""


# --------------- Parsers (XML via regex) ----------
def parse_detail_from_xml_text(xml_txt: str) -> dict:
    """ดึงข้อมูลจาก XML string โดย regex ล้วน"""
    # title

    # print(xml_txt)
    m = PRIMARY_NAME_RE.search(xml_txt)
    title = html.unescape(m.group(1)).strip() if m else ""

    # alt names
    alt_names = [html.unescape(x).strip() for x in ALT_NAME_RE.findall(xml_txt)]

    # players/time/age
    def grab(re_pat):
        m = re_pat.search(xml_txt)
        return m.group(1) if m else ""

    pmin = grab(MINPLAYERS_RE)
    pmax = grab(MAXPLAYERS_RE)
    tmin = grab(MINPLAY_RE)
    tmax = grab(MAXPLAY_RE)
    age = grab(MINAGE_RE)
    # weight
    m = WEIGHT_RE_XML.search(xml_txt)
    weight = m.group(1) if m else ""

    # average rating (ถ้ามี)
    m = AVERAGE_RATING_RE.search(xml_txt)
    avg_rating = m.group(1) if m else ""

    # description
    m = DESC_XML_RE.search(xml_txt)
    desc = ""
    if m:
        # XML description ใช้ entities; unescape แล้ว normalize space
        desc = clean_html_text(m.group(1))

    # credits
    designers, artists, publishers = [], [], []
    for t, v in LINK_RE.findall(xml_txt):
        v = html.unescape(v).strip()
        if not v:
            continue
        if t == "boardgamedesigner":
            designers.append(v)
        elif t == "boardgameartist":
            artists.append(v)
        elif t == "boardgamepublisher":
            publishers.append(v)

    # de-dup รักษาลำดับ
    def uniq(xs):
        seen = set()
        out = []
        for x in xs:
            if x and x not in seen:
                seen.add(x)
                out.append(x)
        return out

    return {
        "title": title,
        "players_min": pmin,
        "players_max": pmax,
        "time_min": tmin,
        "time_max": tmax,
        "age_plus": age,
        "weight_5": weight,
        "average_rating": avg_rating,  # <-- NEW
        "description": desc,
        "alternate_names": " | ".join(uniq(alt_names)),
        "designers": " | ".join(uniq(designers)),
        "artists": " | ".join(uniq(artists)),
        "publishers": " | ".join(uniq(publishers)),
    }


# --------------- Gallery ----------------
def build_gallery_url(game_url: str):
    m = ID_RE.search(game_url)
    if not m:
        return None
    return f"{SITE_ROOT}/boardgame/{m.group(1)}/images"


def fetch_gallery_images_regex(session: requests.Session, detail_url: str, limit=12):
    gu = build_gallery_url(detail_url)
    if not gu:
        return []
    html_src = http_get_text(session, gu)
    # print(html_src)
    if not html_src:
        return []
    imgs = []
    for m in IMG_TAG_RE.finditer(html_src):
        src = m.group(1)
        if "cf.geekdo-images.com" in src:
            imgs.append(to_abs(src))
            if len(imgs) >= limit:
                break
    return imgs

# ---------------- Gallery via API (regex only) ----------------
# ตัวอย่าง API:
# https://api.geekdo.com/api/images?ajax=1&foritempage=1&galleries[]=game&nosession=1&objectid=<gid>&objecttype=thing&showcount=24&size=large&sort=recent&pageid=1

IMAGES_URL_TMPL = (
    "https://api.geekdo.com/api/images"
    "?ajax=1&foritempage=1&galleries%5B%5D={gallery}"
    "&nosession=1&objectid={gid}&objecttype=thing"
    "&showcount={per_page}&size={size}&sort={sort}&pageid={page}"
)

# --- regex จับ url รูปจาก JSON (แบบไม่ใช้ json.loads) ---
IMG_LG_RE    = re.compile(r'"imageurl_lg"\s*:\s*"([^"]+)"')
IMG_2X_RE    = re.compile(r'"imageurl@2x"\s*:\s*"([^"]+)"')
IMG_STD_RE   = re.compile(r'"imageurl"\s*:\s*"([^"]+)"')
PAG_PER_RE   = re.compile(r'"perPage"\s*:\s*(\d+)')
PAG_TOT_RE   = re.compile(r'"total"\s*:\s*(\d+)')
# (ถ้าบาง response ไม่มี total ให้ fallback จาก len(images) ที่ดึงได้)

def _json_unescape_url(u: str) -> str:
    # แก้ \" \/ \u002F ฯลฯ แบบง่าย ๆ พอใช้กับ URL
    u = (u or "").strip()
    u = u.replace(r"\/", "/")
    return to_abs(u)

def _prefer_urls_from_block(txt: str) -> list[str]:
    """
    รับ JSON text ทั้งก้อนของหน้านั้น แล้วดึง URL รูปตามลำดับความสำคัญ:
    imageurl_lg > imageurl@2x > imageurl
    """
    urls = []

    # 1) lg
    for m in IMG_LG_RE.findall(txt):
        urls.append(_json_unescape_url(m))

    # 2) @2x (แต่บางทีซ้ำกับ lg ก็กรองตอนรวม)
    for m in IMG_2X_RE.findall(txt):
        urls.append(_json_unescape_url(m))

    # 3) ปกติ
    for m in IMG_STD_RE.findall(txt):
        urls.append(_json_unescape_url(m))

    # คงไว้เฉพาะโดเมนรูปจริง
    urls = [u for u in urls if "cf.geekdo-images.com" in u]
    return urls

def _extract_pagination(txt: str) -> tuple[int, int]:
    """
    คืน (per_page, total_items) จากฟิลด์ pagination ใน JSON (regex)
    ไม่ชัวร์ → ให้ค่า default ที่พอเดาได้
    """
    per_page = 24
    total = 0
    m = PAG_PER_RE.search(txt)
    if m:
        try: per_page = int(m.group(1))
        except: pass
    m = PAG_TOT_RE.search(txt)
    if m:
        try: total = int(m.group(1))
        except: pass
    return per_page, total

def build_images_api_url(gid: str, page: int = 1, *,
                         per_page: int = 24,
                         size: str = "large",
                         gallery: str = "game",
                         sort: str = "recent") -> str:
    return IMAGES_URL_TMPL.format(
        gid=gid, page=page, per_page=per_page, size=size, gallery=gallery, sort=sort
    )

def fetch_gallery_images_via_api(session: requests.Session, detail_url: str,
                                 limit: int = 12,
                                 size: str = "large",
                                 gallery: str = "game",
                                 sort: str = "recent") -> list[str]:
    # ใช้ regex เดิมของคุณเพื่อเอา gid ให้ได้ก่อน
    m = ID_RE.search(detail_url)
    if not m:
        return []
    gid = m.group(1)

    page = 1
    out, seen = [], set()
    per_page = 24
    total = None

    while len(out) < limit:
        api = build_images_api_url(gid, page, per_page=per_page, size=size, gallery=gallery, sort=sort)
        txt = http_get_text(session, api)
        if not txt:
            break

        # ดึงรูป (ตามลำดับความสำคัญ lg > @2x > std)
        urls = _prefer_urls_from_block(txt)
        for u in urls:
            if u not in seen:
                seen.add(u)
                out.append(u)
                if len(out) >= limit:
                    break

        # อ่าน pagination เพื่อรู้ว่าจะไปต่อกี่หน้า
        if total is None:
            per_page, total = _extract_pagination(txt)
            # ถ้า total ดันเป็น 0 ให้เดาจากจำนวนรูปในหน้านี้
            if total == 0:
                total = len(urls)

        # คำนวณจำนวนหน้าทั้งหมด (ceiling)
        if per_page > 0 and total is not None:
            max_page = (total + per_page - 1) // per_page
        else:
            max_page = page  # กันพัง

        if page >= max_page:
            break

        page += 1
        # กัน rate-limit หน้า/หน้า
        time.sleep(random.uniform(*GALLERY_DELAY_RANGE))

    return out[:limit]



# --------------- Main -------------------
def main():
    s = requests.Session()

    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        in_rows = list(csv.DictReader(f))

    out_rows = []
    for i, row in enumerate(in_rows, 1):
        url = (row.get("url") or "").strip()
        if not url:
            continue
        url = to_abs(url)
        print(f"[{i}/{len(in_rows)}] {url}")

        # 1) HTML หน้าเกม → ภาพ og/primary + title fallback + desc fallback
        html_src = http_get_text(s, url)
        if not html_src:
            print("  skip (HTML fetch failed)")
            continue
        og_img, primary_img = parse_images_from_html(html_src)
        title_fallback = parse_title_from_html(html_src)
        desc_fallback = parse_description_from_html(html_src)

        # 2) gid → XML API (แล้ว regex ล้วน)
        m = ID_RE.search(url) or ID_RE.search(html_src)
        if not m:
            print("  skip (no gid)")
            continue
        gid = m.group(1)
        api_url = f"{SITE_ROOT}/xmlapi2/thing?id={gid}&stats=1"
        xml_txt = http_get_text(s, api_url)
        if not xml_txt:
            print("  warn: XML API not fetched, fallback to HTML-only values")
            details = {
                "title": title_fallback,
                "players_min": "",
                "players_max": "",
                "time_min": "",
                "time_max": "",
                "age_plus": "",
                "weight_5": "",
                "description": desc_fallback,
                "alternate_names": "",
                "designers": "",
                "artists": "",
                "publishers": "",
            }
        else:
            details = parse_detail_from_xml_text(xml_txt)
            if not details.get("title"):
                details["title"] = title_fallback
            if not details.get("description"):
                details["description"] = desc_fallback

        # 3) Gallery (optional)
        # gallery = []
        # if FETCH_GALLERY:
        #     gallery = fetch_gallery_images_regex(s, url, MAX_GALLERY_IMAGES)
        #     time.sleep(random.uniform(*GALLERY_DELAY_RANGE))

        gallery = []
        if FETCH_GALLERY:
        # API ก่อน (ได้รูปชัวร์กว่าและเร็วกว่า)
            gallery = fetch_gallery_images_via_api(s, url, MAX_GALLERY_IMAGES, size="large", gallery="game", sort="recent")
            # ไม่เจอค่อย HTML fallback
            if not gallery:
                gallery = fetch_gallery_images_regex(s, url, MAX_GALLERY_IMAGES)
            time.sleep(random.uniform(*GALLERY_DELAY_RANGE))


        out_rows.append(
            {
                "url": url,
                "title": details.get("title", ""),
                "players_min": details.get("players_min", ""),
                "players_max": details.get("players_max", ""),
                "time_min": details.get("time_min", ""),
                "time_max": details.get("time_max", ""),
                "age_plus": details.get("age_plus", ""),
                "weight_5": details.get("weight_5", ""),
                "average_rating": details.get("average_rating", ""),  # <-- NEW
                "description": details.get("description", ""),
                "og_image": og_img,
                "primary_image": primary_img,
                "gallery_images": " | ".join(gallery),
                "alternate_names": details.get("alternate_names", ""),
                "designers": details.get("designers", ""),
                "artists": details.get("artists", ""),
                "publishers": details.get("publishers", ""),
            }
        )

        time.sleep(random.uniform(*PAGE_DELAY_RANGE))

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "url",
            "title",
            "players_min",
            "players_max",
            "time_min",
            "time_max",
            "age_plus",
            "weight_5",
            "average_rating",  # <-- NEW
            "description",
            "og_image",
            "primary_image",
            "gallery_images",
            "alternate_names",
            "designers",
            "artists",
            "publishers",
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(out_rows)

    print(f"Saved -> {OUTPUT_CSV}")
    print("Total items:", len(out_rows))


if __name__ == "__main__":
    main()
