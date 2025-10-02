# import_bgg_details.py
# -*- coding: utf-8 -*-
import csv
import sqlite3
import html
from pathlib import Path

CSV_FILE = "bgg_details_from_urls_api_regex.csv"
DB_FILE  = "bgg_details.db"

# ---------------- helpers ----------------
def to_int(v):
    try:
        if v is None or v == "":
            return None
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return None

def to_float(v):
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None

def split_pipe_list(s):
    if not s:
        return []
    return [p.strip() for p in s.split("|") if p.strip()]

# ชื่อคอลัมน์ที่อาจสะกดต่างกันระหว่างไฟล์ (มีช่องว่าง/ขีดล่าง)
ALIASES = {
    "detail_url": [
        "detail_url", "detail url", "url_detail", "detail-uri",
        "url"  # ← เพิ่มบรรทัดนี้
    ],
    "title":          ["title", "name"],
    "players_min":    ["players_min", "players min", "minplayers"],
    "players_max":    ["players_max", "players max", "maxplayers"],
    "time_min":       ["time_min", "time min", "minplaytime"],
    "time_max":       ["time_max", "time max", "maxplaytime"],
    "age_plus":       ["age_plus", "age plus", "minage", "age"],
    "weight_5":       ["weight_5", "weight 5", "weight", "avgweight", "bgg_weight"],
    "average_rating": ["average_rating", "average rating", "avg_rating", "rating", "bgg_rating"],
    "description":    ["description", "desc"],
    "og_image":       ["og_image", "og image", "ogimage"],
    "primary_image":  ["primary_image", "primary image", "image", "image_url"],
    "gallery_images": ["gallery_images", "gallery images", "images_gallery"],
    "alternate_names":["alternate_names", "alternate names", "alt_names"],
    "designers":      ["designers", "designer"],
    "artists":        ["artists", "artist"],
    "publishers":     ["publishers", "publisher"],
}

def get_field(row: dict, logical_key: str, default: str = "") -> str:
    """ดึงค่าจาก DictReader โดยรองรับชื่อคอลัมน์หลายแบบ (aliases)"""
    for k in ALIASES.get(logical_key, [logical_key]):
        if k in row and row[k] not in (None, ""):
            return str(row[k])
    return default

# -------------- schema -------------------
SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS games (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  detail_url     TEXT NOT NULL UNIQUE,
  title          TEXT NOT NULL,
  players_min    INTEGER,
  players_max    INTEGER,
  time_min       INTEGER,
  time_max       INTEGER,
  age_plus       INTEGER,
  weight_5       REAL,
  average_rating REAL,
  description    TEXT,
  og_image       TEXT,
  primary_image  TEXT
);

CREATE TABLE IF NOT EXISTS gallery_images (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  game_id  INTEGER NOT NULL,
  url      TEXT NOT NULL,
  FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS alternate_names (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  game_id  INTEGER NOT NULL,
  name     TEXT NOT NULL,
  FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS designers (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  game_id  INTEGER NOT NULL,
  name     TEXT NOT NULL,
  FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS artists (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  game_id  INTEGER NOT NULL,
  name     TEXT NOT NULL,
  FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS publishers (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  game_id  INTEGER NOT NULL,
  name     TEXT NOT NULL,
  FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_games_title       ON games(title);
CREATE INDEX IF NOT EXISTS idx_games_players     ON games(players_min, players_max);
CREATE INDEX IF NOT EXISTS idx_gallery_game      ON gallery_images(game_id);
CREATE INDEX IF NOT EXISTS idx_alt_names_game    ON alternate_names(game_id);
CREATE INDEX IF NOT EXISTS idx_designers_game    ON designers(game_id);
CREATE INDEX IF NOT EXISTS idx_artists_game      ON artists(game_id);
CREATE INDEX IF NOT EXISTS idx_publishers_game   ON publishers(game_id);
"""

INSERT_GAME_SQL = """
INSERT INTO games (detail_url, title, players_min, players_max, time_min, time_max,
                   age_plus, weight_5, average_rating, description, og_image, primary_image)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(detail_url) DO UPDATE SET
  title=excluded.title,
  players_min=excluded.players_min,
  players_max=excluded.players_max,
  time_min=excluded.time_min,
  time_max=excluded.time_max,
  age_plus=excluded.age_plus,
  weight_5=excluded.weight_5,
  average_rating=excluded.average_rating,
  description=excluded.description,
  og_image=excluded.og_image,
  primary_image=excluded.primary_image
RETURNING id;
"""

def import_details(csv_path: str, db_path: str):
    # เริ่มฐานข้อมูลใหม่ (ถ้าไม่อยากรีเซ็ตทุกครั้ง ให้คอมเมนต์บรรทัดนี้)
    Path(db_path).unlink(missing_ok=True)

    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.executescript(SCHEMA_SQL)

    skipped = 0
    total = 0
    inserted = 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for r in reader:
            total += 1

            # --- fields (ใช้ alias-aware getter) ---
            detail_url = get_field(r, "detail_url").strip()
            if not detail_url:
                skipped += 1
                # เตือนแบบเบาๆ (คอมเมนต์ทิ้งได้)
                print(f"skip row#{total}: missing detail_url (check header name, e.g. 'detail url' vs 'detail_url')")
                continue

            title = html.unescape(get_field(r, "title")).strip()
            desc  = html.unescape(get_field(r, "description"))

            players_min = to_int(get_field(r, "players_min"))
            players_max = to_int(get_field(r, "players_max"))
            time_min    = to_int(get_field(r, "time_min"))
            time_max    = to_int(get_field(r, "time_max"))
            age_plus    = to_int(get_field(r, "age_plus"))
            weight_5    = to_float(get_field(r, "weight_5"))
            avg_rating  = to_float(get_field(r, "average_rating"))

            og_image      = get_field(r, "og_image").strip()
            primary_image = get_field(r, "primary_image").strip()

            cur.execute(
                INSERT_GAME_SQL,
                (detail_url, title, players_min, players_max, time_min, time_max,
                 age_plus, weight_5, avg_rating, desc, og_image, primary_image)
            )
            game_id = cur.fetchone()[0]
            inserted += 1

            # --- children ---
            cur.execute("DELETE FROM gallery_images  WHERE game_id=?", (game_id,))
            cur.execute("DELETE FROM alternate_names WHERE game_id=?", (game_id,))
            cur.execute("DELETE FROM designers       WHERE game_id=?", (game_id,))
            cur.execute("DELETE FROM artists         WHERE game_id=?", (game_id,))
            cur.execute("DELETE FROM publishers      WHERE game_id=?", (game_id,))

            for u in split_pipe_list(get_field(r, "gallery_images")):
                cur.execute("INSERT INTO gallery_images (game_id, url) VALUES (?,?)", (game_id, u))

            for name in split_pipe_list(get_field(r, "alternate_names")):
                cur.execute("INSERT INTO alternate_names (game_id, name) VALUES (?,?)", (game_id, name))

            for name in split_pipe_list(get_field(r, "designers")):
                cur.execute("INSERT INTO designers (game_id, name) VALUES (?,?)", (game_id, name))

            for name in split_pipe_list(get_field(r, "artists")):
                cur.execute("INSERT INTO artists (game_id, name) VALUES (?,?)", (game_id, name))

            for name in split_pipe_list(get_field(r, "publishers")):
                cur.execute("INSERT INTO publishers (game_id, name) VALUES (?,?)", (game_id, name))

    con.commit()
    con.close()
    print(f"✅ Imported/updated {inserted} rows into {db_path} (total={total}, skipped_missing_detail_url={skipped})")

if __name__ == "__main__":
    import_details(CSV_FILE, DB_FILE)
