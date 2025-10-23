# den Spaß starten:
# cd "C:\Users\flori\OneDrive\Desktop\SoupMate\backend\soupmate-etl"
# & "C:\Users\flori\OneDrive\Desktop\SoupMate\.venv\Scripts\Activate.ps1"
# python etl_soupmate_import.py



import os, hashlib, time
from datetime import datetime
from typing import List, Dict, Any

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ---- .env laden und Variablen holen (genau in dieser Reihenfolge) ----
load_dotenv()  # sucht automatisch .env im aktuellen Ordner
PG_DSN = os.environ.get("PG_DSN")
API_KEY = os.environ.get("SPOONACULAR_API_KEY")

print("PG_DSN =", PG_DSN)  # Debug-Ausgabe, um zu sehen, was wirklich gelesen wird

if not PG_DSN:
    raise RuntimeError("PG_DSN fehlt. Liegt die .env im selben Ordner und heißt exakt '.env'?")
if not API_KEY:
    raise RuntimeError("SPOONACULAR_API_KEY fehlt in .env.")

# ---------- Hilfsfunktionen ----------
def norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())

def make_signature(title: str, ingredients_list_text: List[str]) -> str:
    base = norm(title) + "|" + "|".join(sorted(norm(x) for x in ingredients_list_text))
    return hashlib.sha256(base.encode("utf-8")).hexdigest()

def chunk_ingredients(ingredients_list_text: List[str]) -> str:
    return "\n".join(f"- {line}" for line in ingredients_list_text)

def split_instructions(instr: str, max_len=1000):
    if not instr:
        return []
    parts = [p.strip() for p in instr.replace("\r", "").split("\n") if p.strip()]
    chunks, cur = [], ""
    for p in parts:
        add = (("\n" if cur else "") + p)
        if len(cur) + len(add) > max_len:
            chunks.append(cur)
            cur = p
        else:
            cur += add
    if cur:
        chunks.append(cur)
    return chunks or [instr[:max_len]]

# ---------- API ----------
BASE = "https://api.spoonacular.com"

def spoonacular_get(path: str, params: Dict[str, Any]):
    params = dict(params or {})
    params["apiKey"] = API_KEY
    r = requests.get(f"{BASE}{path}", params=params, timeout=30)
    if r.status_code == 402:
        raise RuntimeError("Spoonacular: Payment Required / Quota exceeded.")
    r.raise_for_status()
    return r.json()

def fetch_soups_page(limit=50, offset=0):
    # Complex Search: liefert bereits viele Felder
    return spoonacular_get(
        "/recipes/complexSearch",
        {
            "query": "soup",
            "number": limit,
            "offset": offset,
            "addRecipeInformation": "true",
            "instructionsRequired": "true",
            "sort": "popularity",
        },
    ).get("results", [])

# ---------- DB I/O ----------
def ensure_source(conn, name: str):
    with conn.cursor() as cur:
        cur.execute("insert into public.source(name) values (%s) on conflict(name) do nothing;", (name,))
        cur.execute("select id from public.source where name=%s;", (name,))
        return cur.fetchone()[0]

def upsert_recipe(conn, src_id, src_recipe):
    title = src_recipe.get("title") or "Untitled"
    summary = src_recipe.get("summary")
    instructions_raw = src_recipe.get("instructions") or summary
    instructions_plain = src_recipe.get("instructions") or summary
    servings = src_recipe.get("servings")
    ready_in = src_recipe.get("readyInMinutes")
    image = src_recipe.get("image")
    cuisines = src_recipe.get("cuisines") or []
    diets = src_recipe.get("diets") or []

    ingredients_text = []
    ingredients_struct = []
    for ing in (src_recipe.get("extendedIngredients") or []):
        original = ing.get("original") or ing.get("originalName") or ing.get("name", "")
        ingredients_text.append(original)
        ingredients_struct.append({
            "name": ing.get("name") or "",
            "amount": ing.get("amount"),
            "unit": ing.get("unit"),
            "grams": None,
            "note": "",
        })

    signature = make_signature(title, ingredients_text)

    with conn.cursor() as cur:
        cur.execute("""
        insert into public.recipe (
          source_id, source_recipe_id, title, summary, instructions_raw, instructions_plain,
          servings, total_time_minutes, image_url, cuisine, diets, intolerances,
          lang, is_soup, last_fetched_at, signature
        )
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::text[],%s::text[],%s::text[],%s,%s,%s,%s)
        on conflict (source_id, source_recipe_id) do update set
          title=excluded.title,
          summary=excluded.summary,
          instructions_raw=excluded.instructions_raw,
          instructions_plain=excluded.instructions_plain,
          servings=excluded.servings,
          total_time_minutes=excluded.total_time_minutes,
          image_url=excluded.image_url,
          cuisine=excluded.cuisine,
          diets=excluded.diets,
          intolerances=excluded.intolerances,
          lang=excluded.lang,
          is_soup=excluded.is_soup,
          last_fetched_at=excluded.last_fetched_at,
          signature=excluded.signature,
          updated_at=now()
        returning id
        """, (
            src_id, str(src_recipe.get("id")),
            title, summary, instructions_raw, instructions_plain,
            src_recipe.get("servings"), ready_in, image,
            cuisines, diets, [],  # intolerances lassen wir leer
            "de", True, datetime.utcnow(), signature
        ))
        recipe_id = cur.fetchone()[0]

    return recipe_id, ingredients_struct, ingredients_text, title, instructions_plain

def upsert_ingredients_and_join(conn, recipe_id, items):
    unique_names = list({norm(i["name"]) for i in items if i.get("name")})
    if unique_names:
        with conn.cursor() as cur:
            execute_values(cur,
                "insert into public.ingredient(name) values %s on conflict(name) do nothing;",
                [(n,) for n in unique_names]
            )
            cur.execute("select id, name from public.ingredient where name = any(%s);", (unique_names,))
            id_map = {name: iid for (iid, name) in cur.fetchall()}

            rows = []
            for it in items:
                n = norm(it["name"] or "")
                if not n:
                    continue
                iid = id_map[n]
                rows.append((recipe_id, iid, it.get("amount"), it.get("unit"), it.get("grams"), it.get("note") or ""))

            if rows:
                execute_values(cur, """
                    insert into public.recipe_ingredient
                      (recipe_id, ingredient_id, quantity, unit, quantity_gram, note)
                    values %s
                    on conflict (recipe_id, ingredient_id, unit, note) do update set
                      quantity = excluded.quantity,
                      quantity_gram = excluded.quantity_gram
                """, rows)

def upsert_nutrition(conn, recipe_id, src_recipe):
    nutrition = src_recipe.get("nutrition") or {}
    nutrients = nutrition.get("nutrients") or []
    byname = {norm(n.get("name","")): n for n in nutrients}
    kcal     = (byname.get("calories") or {}).get("amount")
    protein  = (byname.get("protein") or {}).get("amount")
    carbs    = (byname.get("carbohydrates") or byname.get("carbs") or {}).get("amount")
    fat      = (byname.get("fat") or {}).get("amount")
    fiber    = (byname.get("fiber") or {}).get("amount")
    sugar    = (byname.get("sugar") or {}).get("amount")
    sodium   = (byname.get("sodium") or {}).get("amount")

    with conn.cursor() as cur:
        cur.execute("""
        insert into public.nutrition (recipe_id, kcal, protein_g, carbs_g, fat_g, fiber_g, sugar_g, sodium_mg)
        values (%s,%s,%s,%s,%s,%s,%s,%s)
        on conflict (recipe_id) do update set
          kcal=excluded.kcal, protein_g=excluded.protein_g, carbs_g=excluded.carbs_g,
          fat_g=excluded.fat_g, fiber_g=excluded.fiber_g, sugar_g=excluded.sugar_g, sodium_mg=excluded.sodium_mg
        """, (recipe_id, kcal, protein, carbs, fat, fiber, sugar, sodium))

def insert_chunks(conn, recipe_id, title, ingredients_text, instructions_plain):
    chunks = []
    chunks.append(("title", title, len(title.split())))
    ing_text = chunk_ingredients(ingredients_text)
    chunks.append(("ingredients", ing_text, len(ing_text.split())))
    for part in split_instructions(instructions_plain or ""):
        chunks.append(("instructions", part, len(part.split())))

    with conn.cursor() as cur:
        execute_values(cur, """
            insert into public.recipe_chunk (recipe_id, chunk_type, content, token_count)
            values %s
        """, [(recipe_id, t, c, tok) for (t, c, tok) in chunks])

def import_soups(total=50, page_size=25, source_name="Spoonacular", sleep_s=1.0):
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = False
    try:
        src_id = ensure_source(conn, source_name)
        imported = 0
        offset = 0
        while imported < total:
            batch = min(page_size, total - imported)
            results = fetch_soups_page(limit=batch, offset=offset)
            if not results:
                break
            for r in results:
                recipe_id, items, ingredients_text, title, instructions_plain = upsert_recipe(conn, src_id, r)
                upsert_ingredients_and_join(conn, recipe_id, items)
                upsert_nutrition(conn, recipe_id, r)
                insert_chunks(conn, recipe_id, title, ingredients_text, instructions_plain)
            conn.commit()
            imported += len(results)
            offset += len(results)
            time.sleep(sleep_s)  # Rate-Limit freundlich
            print(f"Imported {imported}/{total}")
        print("Import fertig ✅")
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    # Erst mal klein testen:
    import_soups(total=10, page_size=10)
