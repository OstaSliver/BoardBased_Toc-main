# migrate_add_categories.py
import csv, re, sqlite3
from pathlib import Path

DB_FILE   = "bgg_details.db"                       # DB รายละเอียดที่มีอยู่
INDEX_CSV = "boardgame_categories_with_images_by_api_regex.csv" # CSV ดัชนีหมวดจากรอบแรก

ID_RE = re.compile(r"/boardgame(?:expansion)?/(\d+)")

def extract_id_from_url(u: str) -> str | None:
    if not u: return None
    m = ID_RE.search(u)
    return m.group(1) if m else None

def main():
    if not Path(DB_FILE).exists():
        raise SystemExit(f"DB not found: {DB_FILE}")

    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()

    cur.executescript("""
    PRAGMA journal_mode=WAL;
    PRAGMA foreign_keys=ON;

    CREATE TABLE IF NOT EXISTS game_categories (
      id        INTEGER PRIMARY KEY AUTOINCREMENT,
      game_id   INTEGER NOT NULL,
      category  TEXT    NOT NULL,
      FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE,
      UNIQUE (game_id, category)
    );

    CREATE INDEX IF NOT EXISTS idx_gc_cat   ON game_categories(category);
    CREATE INDEX IF NOT EXISTS idx_gc_game  ON game_categories(game_id);
    """)

    # เตรียม map จาก games.detail_url และ/หรือ bgg id -> games.id
    cur.execute("SELECT id, detail_url FROM games")
    rows = cur.fetchall()
    by_url = {}
    by_id  = {}
    for gid, du in rows:
        by_url[du] = gid
        bggid = extract_id_from_url(du)
        if bggid:
            by_id[bggid] = gid

    inserted = 0
    nomatch  = 0

    with open(INDEX_CSV, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            category = (row.get("category") or "").strip()
            url      = (row.get("url") or "").strip()
            if not category or not url:
                continue

            gid = by_url.get(url)
            if not gid:
                # ลองจับด้วย boardgame id
                bid = extract_id_from_url(url)
                if bid:
                    gid = by_id.get(bid)

            if not gid:
                nomatch += 1
                continue

            try:
                cur.execute("INSERT OR IGNORE INTO game_categories (game_id, category) VALUES (?,?)",
                            (gid, category))
                if cur.rowcount:
                    inserted += 1
            except Exception as e:
                print("insert error:", e)

    con.commit()
    con.close()
    print(f"✅ categories linked: +{inserted} rows (unmatched from CSV: {nomatch})")

if __name__ == "__main__":
    main()
