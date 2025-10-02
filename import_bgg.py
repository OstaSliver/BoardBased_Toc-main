import csv
import sqlite3
from pathlib import Path

# CSV_FILE = "boardgame_categories_with_images.csv"
CSV_FILE = "boardgame_categories_with_images_by_api_regex.csv"
DB_FILE  = "bgg_new.db"

def import_csv_to_db(csv_file: str, db_file: str):
    # ลบไฟล์ฐานข้อมูลเก่าถ้ามีอยู่
    Path(db_file).unlink(missing_ok=True)

    # สร้าง connection
    con = sqlite3.connect(db_file)
    cur = con.cursor()

    # สร้างตารางใหม่
    cur.execute("""
    CREATE TABLE games (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        category  TEXT NOT NULL,
        name      TEXT NOT NULL,
        year      INTEGER,
        url       TEXT,
        image_url TEXT
    );
    """)
    cur.execute("CREATE INDEX idx_games_category ON games(category);")
    cur.execute("CREATE INDEX idx_games_name ON games(name);")

    # อ่าน CSV แล้ว insert
    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [(r["category"], r["name"], int(r["year"] or 0), r["url"], r["image_url"]) for r in reader]

    cur.executemany("INSERT INTO games (category,name,year,url,image_url) VALUES (?,?,?,?,?)", rows)

    con.commit()
    con.close()
    print(f"✅ Imported {len(rows)} rows into {db_file}")

if __name__ == "__main__":
    import_csv_to_db(CSV_FILE, DB_FILE)
