# den Spaß starten:
# cd "C:\Users\flori\OneDrive\Desktop\SoupMate\backend\soupmate-etl"
# & "C:\Users\flori\OneDrive\Desktop\SoupMate\.venv\Scripts\Activate.ps1"
# python etl_soupmate_import.py

import os, time, hashlib
from datetime import datetime
from typing import List, Dict, Any

import requests
from dotenv import load_dotenv

# =========================
#   ENV laden & prüfen
# =========================
load_dotenv()  # .env im aktuellen Ordner
SUPABASE_URL         = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
SUPABASE_SECRET_KEY  = os.environ.get("SUPABASE_SECRET_KEY")
SPOONACULAR_API_KEY  = os.environ.get("SPOONACULAR_API_KEY")

if not SUPABASE_URL:
    raise RuntimeError("SUPABASE_URL fehlt in .env")
if not SUPABASE_SECRET_KEY:
    raise RuntimeError("SUPABASE_SECRET_KEY fehlt in .env (Secret/Service Role Key)")
if not SPOONACULAR_API_KEY:
    raise RuntimeError("SPOONACULAR_API_KEY fehlt in .env")

print("Using:", SUPABASE_URL)

# =========================
#   HTTP-Clients
# =========================
# Supabase REST
SB_HEADERS = {
    "apikey": SUPABASE_SECRET_KEY,
    "Authorization": f"Bearer {SUPABASE_SECRET_KEY}",
    "Content-Type": "application/json",
}

def sb_get(table: str, params: Dict[str, Any]):
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=SB_HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def sb_insert(table: str, rows: List[Dict[str, Any]]):
    r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=SB_HEADERS, json=rows, timeout=30)
    r.raise_for_status()
    return r.json() if r.text else None

def sb_upsert(table: str, rows: List[Dict[str, Any]], on_conflict: str | None = None):
    params = {}
    if on_conflict:
        params["on_conflict"] = on_conflict
    headers = {**SB_HEADERS, "Prefer": "resolution=merge-duplicates"}
    r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=headers, params=params, json=rows, timeout=30)
    r.raise_for_status()
    return r.json() if r.text else None

# Spoonacular
BASE = "https://api.spoonacular.com"
def spoonacular_get(path: str, params: Dict[str, Any]):
    params = dict(params or {})
    headers = {"x-api-key": SPOONACULAR_API_KEY}  # stabiler als ?apiKey=
    r = requests.get(f"{BASE}{path}", params=params, headers=headers, timeout=30)
    if r.status_code == 402:
        raise RuntimeError("Spoonacular: Payment Required / Quota exceeded.")
    r.raise_for_status()
    return r.json()

def fetch_soups_page(limit=50, offset=0):
    data = spoonacular_get(
        "/recipes/complexSearch",
        {
            "query": "soup",
            "number": limit,
            "offset": offset,
            "addRecipeInformation": "true",
            "instructionsRequired": "true",
            "sort": "popularity",
        },
    )
    return data.get("results", [])

# =========================
#   Helper (Text/Chunks)
# =========================
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

# =========================
#   REST-ETL-Schritte
# =========================
def ensure_source_rest(name: str) -> str:
    # Upsert by unique(name)
    sb_upsert("source", [{"name": name}], on_conflict="name")
    rows = sb_get("source", {"select": "id", "name": f"eq.{name}", "limit": 1})
    return rows[0]["id"]

def upsert_recipe_rest(src_id: str, src_recipe: Dict[str, Any]):
    title = src_recipe.get("title") or "Untitled"
    summary = src_recipe.get("summary")
    instructions = src_recipe.get("instructions") or summary
    servings = src_recipe.get("servings")
    ready_in = src_recipe.get("readyInMinutes")
    image = src_recipe.get("image")
    cuisines = src_recipe.get("cuisines") or []
    diets = src_recipe.get("diets") or []

    ingredients_text: List[str] = []
    items: List[Dict[str, Any]] = []
    for ing in (src_recipe.get("extendedIngredients") or []):
        original = ing.get("original") or ing.get("originalName") or ing.get("name", "")
        ingredients_text.append(original)
        items.append({
            "name": ing.get("name") or "",
            "amount": ing.get("amount"),
            "unit": ing.get("unit"),
            "grams": None,
            "note": "",
        })

    signature = make_signature(title, ingredients_text)
    body = [{
        "source_id": src_id,
        "source_recipe_id": str(src_recipe.get("id")),
        "title": title,
        "summary": summary,
        "instructions_raw": instructions,
        "instructions_plain": instructions,
        "servings": servings,
        "total_time_minutes": ready_in,
        "image_url": image,
        "cuisine": cuisines,
        "diets": diets,
        "intolerances": [],
        "lang": "de",
        "is_soup": True,
        "last_fetched_at": datetime.utcnow().isoformat(),
        "signature": signature,
    }]
    sb_upsert("recipe", body, on_conflict="source_id,source_recipe_id")
    rid = sb_get("recipe", {
        "select": "id",
        "source_id": f"eq.{src_id}",
        "source_recipe_id": f"eq.{str(src_recipe.get('id'))}",
        "limit": 1
    })[0]["id"]
    return rid, items, ingredients_text, title, instructions

def upsert_ingredients_and_join_rest(recipe_id: str, items: List[Dict[str, Any]]):
    names = sorted({norm(it.get("name") or "") for it in items if it.get("name")})
    names = [n for n in names if n]  # ohne Leerstrings
    if not names:
        return
    # Zutatenstamm upserten (unique name)
    sb_upsert("ingredient", [{"name": n} for n in names], on_conflict="name")

    # IDs in einem Rutsch holen – PostgREST in.(a,b,c)
    in_list = ",".join(names)
    data = sb_get("ingredient", {"select": "id,name", "name": f"in.({in_list})"})
    id_map = {row["name"]: row["id"] for row in data}

    rows = []
    for it in items:
        n = norm(it.get("name") or "")
        if not n or n not in id_map:
            continue
        rows.append({
            "recipe_id": recipe_id,
            "ingredient_id": id_map[n],
            "quantity": it.get("amount"),
            "unit": it.get("unit"),
            "quantity_gram": it.get("grams"),
            "note": it.get("note") or ""
        })
    if rows:
        sb_upsert("recipe_ingredient", rows, on_conflict="recipe_id,ingredient_id,unit,note")

def upsert_nutrition_rest(recipe_id: str, src_recipe: Dict[str, Any]):
    nutrients = (src_recipe.get("nutrition") or {}).get("nutrients") or []
    byname = {norm(n.get("name","")): n for n in nutrients}
    def amt(key: str):
        return (byname.get(key) or {}).get("amount")
    row = {
        "recipe_id": recipe_id,
        "kcal":       amt("calories"),
        "protein_g":  amt("protein"),
        "carbs_g":    amt("carbohydrates") or amt("carbs"),
        "fat_g":      amt("fat"),
        "fiber_g":    amt("fiber"),
        "sugar_g":    amt("sugar"),
        "sodium_mg":  amt("sodium"),
    }
    sb_upsert("nutrition", [row], on_conflict="recipe_id")

def insert_chunks_rest(recipe_id: str, title: str, ingredients_text: List[str], instructions_plain: str | None):
    rows = []
    rows.append({
        "recipe_id": recipe_id,
        "chunk_type": "title",
        "content": title,
        "token_count": len((title or "").split())
    })
    ing = chunk_ingredients(ingredients_text)
    rows.append({
        "recipe_id": recipe_id,
        "chunk_type": "ingredients",
        "content": ing,
        "token_count": len(ing.split())
    })
    for part in split_instructions(instructions_plain or ""):
        rows.append({
            "recipe_id": recipe_id,
            "chunk_type": "instructions",
            "content": part,
            "token_count": len(part.split())
        })
    if rows:
        sb_insert("recipe_chunk", rows)

# =========================
#   Import-Loop
# =========================
def import_soups(total=50, page_size=25, source_name="Spoonacular", sleep_s=1.2):
    src_id = ensure_source_rest(source_name)
    imported, offset = 0, 0
    while imported < total:
        batch = min(page_size, total - imported)
        results = fetch_soups_page(limit=batch, offset=offset)
        if not results:
            break
        for r in results:
            rid, items, ingredients_text, title, instructions_plain = upsert_recipe_rest(src_id, r)
            upsert_ingredients_and_join_rest(rid, items)
            upsert_nutrition_rest(rid, r)
            insert_chunks_rest(rid, title, ingredients_text, instructions_plain)
        imported += len(results)
        offset += len(results)
        time.sleep(sleep_s)  # API freundlich bleiben
        print(f"Imported {imported}/{total}")
    print("Import fertig ✅")

# =========================
#   Main
# =========================
if __name__ == "__main__":
    # für den ersten Lauf klein halten
    import_soups(total=10, page_size=10)
