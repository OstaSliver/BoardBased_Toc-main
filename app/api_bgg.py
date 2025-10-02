# api_bgg.py (เฉพาะส่วนที่เกี่ยวกับหมวด ปรับจากเดิม)
import sqlite3
from fastapi import FastAPI, Query, HTTPException

app = FastAPI(title="BGG API")
DB_BASE    = "bgg_new.db"
DB_DETAILS = "bgg_details.db"

def get_db(path):
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con

@app.get("/games")
def list_games(page: int = Query(1, ge=1), limit: int = Query(20, ge=1, le=100)):
    offset = (page - 1) * limit
    con1 = get_db(DB_BASE)
    cur1 = con1.cursor()
    cur1.execute("SELECT COUNT(*) FROM games")
    total = cur1.fetchone()[0]

    cur1.execute("""
        SELECT id, category, name, year, url, image_url
        FROM games
        ORDER BY name
        LIMIT ? OFFSET ?
    """, (limit, offset))
    rows = [dict(r) for r in cur1.fetchall()]
    con1.close()

    # เติม rating จาก bgg_details.db
    con2 = get_db(DB_DETAILS)
    cur2 = con2.cursor()
    for r in rows:
        url = r["url"]
        cur2.execute("SELECT average_rating FROM games WHERE detail_url=?", (url,))
        detail = cur2.fetchone()
        r["average_rating"] = detail["average_rating"] if detail else None
    con2.close()

    return {"page": page, "limit": limit, "total": total, "games": rows}

@app.get("/categories")
def list_categories():
    con = get_db(DB_DETAILS); cur = con.cursor()
    cur.execute("SELECT DISTINCT category FROM game_categories ORDER BY category")
    cats = [r[0] for r in cur.fetchall()]
    con.close()
    return {"categories": cats}

@app.get("/categories/{category}/games")
def games_by_category(category: str,
                      page: int = Query(1, ge=1),
                      limit: int = Query(20, ge=1, le=100)):
    offset = (page - 1) * limit
    con = get_db(DB_DETAILS); cur = con.cursor()
    cur.execute("""
        SELECT COUNT(*)
        FROM game_categories gc
        JOIN games g ON g.id = gc.game_id
        WHERE gc.category = ?
    """, (category,))
    total = cur.fetchone()[0]

    cur.execute("""
        SELECT g.id, g.title, g.detail_url, g.players_min, g.players_max, g.average_rating
        FROM game_categories gc
        JOIN games g ON g.id = gc.game_id
        WHERE gc.category = ?
        ORDER BY g.title
        LIMIT ? OFFSET ?
    """, (category, limit, offset))
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return {"category": category, "page": page, "limit": limit, "total": total, "games": rows}

@app.get("/games/{game_id}")
def game_detail(game_id: int):
    con = get_db(); cur = con.cursor()
    cur.execute("SELECT * FROM games WHERE id=?", (game_id,))
    game = cur.fetchone()
    if not game:
        con.close()
        raise HTTPException(404, "Game not found")

    result = dict(game)

    # เพิ่มหมวดของเกมนี้
    cur.execute("SELECT category FROM game_categories WHERE game_id=? ORDER BY category", (game_id,))
    result["categories"] = [r[0] for r in cur.fetchall()]

    # ดึงตารางลูกอื่น ๆ
    cur.execute("SELECT url FROM gallery_images WHERE game_id=?", (game_id,))
    result["gallery_images"] = [r[0] for r in cur.fetchall()]

    for tbl, key in [
        ("alternate_names", "alternate_names"),
        ("designers", "designers"),
        ("artists", "artists"),
        ("publishers", "publishers"),
    ]:
        cur.execute(f"SELECT name FROM {tbl} WHERE game_id=?", (game_id,))
        result[key] = [r[0] for r in cur.fetchall()]

    con.close()
    return result
