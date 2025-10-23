# den Spaß starten:
# cd "C:\Users\flori\OneDrive\Desktop\SoupMate\backend\soupmate-etl"
# & "C:\Users\flori\OneDrive\Desktop\SoupMate\.venv\Scripts\Activate.ps1"
# python etl_soupmate_import.py

import os
import re
import time
import hashlib
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import requests
from dotenv import load_dotenv

# =========================
#   ENV laden & prüfen
# =========================
load_dotenv()  # .env im aktuellen Ordner
SUPABASE_URL        = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
SUPABASE_SECRET_KEY = os.environ.get("SUPABASE_SECRET_KEY")
SPOONACULAR_API_KEY = os.environ.get("SPOONACULAR_API_KEY")

if not SUPABASE_URL:
    raise RuntimeError("SUPABASE_URL fehlt in .env")
if not SUPABASE_SECRET_KEY:
    raise RuntimeError("SUPABASE_SECRET_KEY fehlt in .env (Secret/Service Role Key)")
if not SPOONACULAR_API_KEY:
    raise RuntimeError("SPOONACULAR_API_KEY fehlt in .env")

print("Using Supabase:", SUPABASE_URL)

# =========================
#   HTTP-Clients
# =========================
# Supabase REST
SB_HEADERS = {
    "apikey": SUPABASE_SECRET_KEY,
    "Authorization": f"Bearer {SUPABASE_SECRET_KEY}",
    "Content-Type": "application/json",
}

def sb_get(table: str, params: Dict[str, Any], timeout=30):
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=SB_HEADERS, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def sb_insert(table: str, rows: List[Dict[str, Any]], timeout=30):
    r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=SB_HEADERS, json=rows, timeout=timeout)
    r.raise_for_status()
    return r.json() if r.text else None

def sb_upsert(table: str, rows: List[Dict[str, Any]], on_conflict: Optional[str] = None, timeout=30):
    params = {}
    if on_conflict:
        params["on_conflict"] = on_conflict
    headers = {**SB_HEADERS, "Prefer": "resolution=merge-duplicates"}
    r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=headers, params=params, json=rows, timeout=timeout)
    r.raise_for_status()
    return r.json() if r.text else None

# Spoonacular
BASE = "https://api.spoonacular.com"

def spoonacular_get(path: str, params: Dict[str, Any], timeout=30):
    params = dict(params or {})
    headers = {"x-api-key": SPOONACULAR_API_KEY}  # stabiler als ?apiKey=
    r = requests.get(f"{BASE}{path}", params=params, headers=headers, timeout=timeout)
    # nette Fehler
    if r.status_code == 402:
        raise RuntimeError("Spoonacular: Payment Required / Quota exceeded.")
    if r.status_code == 401:
        raise RuntimeError("Spoonacular: 401 Unauthorized – API Key prüfen.")
    if r.status_code == 429:
        # Rate-Limit: klein pausieren und nochmal versuchen
        time.sleep(3)
        r = requests.get(f"{BASE}{path}", params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()

# =========================
#   Helper (Text/Chunks)
# =========================
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")

def html_to_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = TAG_RE.sub(" ", s)
    s = WS_RE.sub(" ", s).strip()
    return s

def norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())

def make_signature(title: str, ingredients_list_text: List[str]) -> str:
    base = norm(title) + "|" + "|".join(sorted(norm(x) for x in ingredients_list_text))
    return hashlib.sha256(base.encode("utf-8")).hexdigest()

def chunk_ingredients(ingredients_list_text: List[str]) -> str:
    return "\n".join(f"- {line}" for line in ingredients_list_text if line and line.strip())

def split_instructions(instr: str, max_len=1000):
    if not instr:
        return []
    parts = [p.strip() for p in instr.replace("\r", "").split("\n") if p.strip()]
    chunks, cur = [], ""
    for p in parts:
        add = (("\n" if cur else "") + p)
        if len(cur) + len(add) > max_len:
            if cur:
                chunks.append(cur)
            cur = p
        else:
            cur += add
    if cur:
        chunks.append(cur)
    return chunks or [instr[:max_len]]

# =========================
#   Spoonacular Calls
# =========================
def fetch_soups_page(limit=50, offset=0):
    """
    Nur Suppen (query+type), mit Rezeptdetails & Anleitungen.
    """
    data = spoonacular_get(
        "/recipes/complexSearch",
        {
            "query": "soup",
            "type": "soup",                 # härterer Filter
            "number": limit,
            "offset": offset,
            "addRecipeInformation": "true",
            "instructionsRequired": "true",
            "sort": "popularity",
        },
    )
    return data.get("results", [])

def fetch_ingredient_widget(recipe_id: int):
    return spoonacular_get(f"/recipes/{recipe_id}/ingredientWidget.json", {})

def fetch_nutrition_widget(recipe_id: int):
    return spoonacular_get(f"/recipes/{recipe_id}/nutritionWidget.json", {})

def fetch_price_breakdown(recipe_id: int):
    return spoonacular_get(f"/recipes/{recipe_id}/priceBreakdownWidget.json", {})

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
    summary = html_to_text(src_recipe.get("summary") or "")
    instr_html = src_recipe.get("instructions") or summary
    instructions = html_to_text(instr_html)
    servings = src_recipe.get("servings")
    ready_in = src_recipe.get("readyInMinutes")
    image = src_recipe.get("image")
    cuisines = src_recipe.get("cuisines") or []
    diets = src_recipe.get("diets") or []

    ingredients_text: List[str] = []
    items: List[Dict[str, Any]] = []
    for ing in (src_recipe.get("extendedIngredients") or []):
        original = ing.get("original") or ing.get("originalName") or ing.get("name", "")
        if original:
            ingredients_text.append(html_to_text(original))
        items.append({
            "name": (ing.get("name") or "").strip(),
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
        "summary": summary or None,
        "instructions_raw": instr_html or None,
        "instructions_plain": instructions or None,
        "servings": servings,
        "total_time_minutes": ready_in,
        "image_url": image,
        "cuisine": cuisines,
        "diets": diets,
        "intolerances": [],
        "lang": "de",
        "is_soup": True,
        #"last_fetched_at": datetime.utcnow().isoformat(),
        "last_fetched_at": datetime.now(timezone.utc).isoformat(),
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
    # Zutatenstamm upserten
    names = sorted({norm(it.get("name") or "") for it in items if it.get("name")})
    names = [n for n in names if n]
    if not names:
        return
    sb_upsert("ingredient", [{"name": n} for n in names], on_conflict="name")

    # IDs holen (robust, einzeln – vermeidet IN/OR-Quoting-Probleme)
    id_map: Dict[str, str] = {}
    for n in names:
        rows = sb_get("ingredient", {"select": "id,name", "name": f"eq.{n}", "limit": 1})
        if rows:
            id_map[n] = rows[0]["id"]

    rows_join = []
    for it in items:
        n = norm(it.get("name") or "")
        if not n or n not in id_map:
            continue
        rows_join.append({
            "recipe_id": recipe_id,
            "ingredient_id": id_map[n],
            "quantity": it.get("amount"),
            "unit": it.get("unit"),
            "quantity_gram": it.get("grams"),
            "note": it.get("note") or ""
        })
    if rows_join:
        sb_upsert("recipe_ingredient", rows_join, on_conflict="recipe_id,ingredient_id,unit,note")

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

def upsert_price_breakdown_rest(recipe_uuid: str, price_json: dict):
    """
    price_json Beispiel:
    {
      "ingredients": [
        {
          "name": "onion",
          "amount": { "metric": { "value": 200.0, "unit": "g" }, "us": {...} },
          "price": 123.0    # manchmal float ODER { "value": 123.0 }
        },
        ...
      ],
      "totalCost": 9876,
      "servings": 4
    }
    """
    ingredients = price_json.get("ingredients") or []
    total_cents = price_json.get("totalCost") or 0
    servings    = price_json.get("servings") or 1
    per_serv_cents = int(round(float(total_cents) / max(int(servings), 1)))

    # Aggregat speichern
    sb_upsert("price_breakdown", [{
        "recipe_id": recipe_uuid,
        "currency": "USD_cents",
        "total_cost_cents": int(total_cents),
        "cost_per_serving_cents": int(per_serv_cents),
        "ingredients_count": len(ingredients),
    }], on_conflict="recipe_id")

    def read_metric_amount(ing: dict):
        """Robuster Parser für amount.metric."""
        amt = (ing.get("amount") or {}).get("metric") or {}
        if isinstance(amt, (int, float)):   # falls mal direkt eine Zahl
            return float(amt), None
        return amt.get("value"), amt.get("unit")

    def read_price_cents(ing: dict):
        """price ist manchmal float, manchmal {value: float}."""
        p = ing.get("price")
        if isinstance(p, dict):
            return p.get("value")
        if isinstance(p, (int, float)):
            return p
        return None

    rows = []
    for ing in ingredients:
        name = (ing.get("name") or "").strip()
        if not name:
            continue
        amount, unit = read_metric_amount(ing)
        price_cents  = read_price_cents(ing)
        rows.append({
            "recipe_id": recipe_uuid,
            "name": name,
            "amount": float(amount) if amount is not None else None,
            "unit": unit,
            "cost_cents": int(price_cents) if price_cents is not None else None,
        })

    if rows:
        sb_upsert("price_breakdown_item", rows, on_conflict="recipe_id,name,unit")


def upsert_recipe_raw_rest(recipe_uuid: str, complex_json: dict, ing_json: dict, nut_json: dict, price_json: dict):
    sb_upsert("recipe_raw", [{
        "recipe_id": recipe_uuid,
        "spoonacular_complex": complex_json,
        "ingredient_widget": ing_json,
        "nutrition_widget": nut_json,
        "price_widget": price_json
    }], on_conflict="recipe_id")

def insert_chunks_rest(recipe_id: str, title: str, ingredients_text: List[str], instructions_plain: str):
    rows = []
    # title
    rows.append({
        "recipe_id": recipe_id,
        "chunk_type": "title",
        "content": title,
        "token_count": len((title or "").split())
    })
    # ingredients
    ing = chunk_ingredients(ingredients_text)
    rows.append({
        "recipe_id": recipe_id,
        "chunk_type": "ingredients",
        "content": ing,
        "token_count": len(ing.split())
    })
    # instructions (in Teile splitten)
    for part in split_instructions(instructions_plain or ""):
        rows.append({
            "recipe_id": recipe_id,
            "chunk_type": "instructions",
            "content": part,
            "token_count": len(part.split())
        })
    if rows:
        sb_insert("recipe_chunk", rows)

def insert_price_chunk_rest(recipe_uuid: str, price_json: dict):
    total = price_json.get("totalCost") or 0
    servings = price_json.get("servings") or 1
    per_serv = int(round(float(total) / max(int(servings), 1)))
    items = price_json.get("ingredients") or []

    def read_price_cents(ing: dict):
        p = ing.get("price")
        if isinstance(p, dict):
            return p.get("value") or 0
        if isinstance(p, (int, float)):
            return p
        return 0

    # Top 5 teuerste Zutaten
    items_sorted = sorted(
        [(read_price_cents(i), i) for i in items],
        key=lambda x: x[0],
        reverse=True
    )[:5]

    lines = [f"Estimated total: {int(total)} cents; per serving: {per_serv} cents (USD_cents)"]
    if items_sorted:
        lines.append("Costliest ingredients:")
        for cost, i in items_sorted:
            nm = i.get("name")
            metric = (i.get("amount") or {}).get("metric") or {}
            val, unit = metric.get("value"), metric.get("unit")
            lines.append(f"- {nm} · {val} {unit} · {int(cost)} cents")

    content = "\n".join(lines)
    sb_insert("recipe_chunk", [{
        "recipe_id": recipe_uuid,
        "chunk_type": "price",
        "content": content,
        "token_count": len(content.split())
    }])

# =========================
#   Import-Loop
# =========================
def import_soups(total=50, page_size=25, source_name="Spoonacular", sleep_s=1.2):
    """
    Lädt nur Suppen (complexSearch) + Widgets und speichert:
      - recipe, ingredient, recipe_ingredient, nutrition
      - price_breakdown, price_breakdown_item
      - recipe_raw (optional Roh-JSON)
      - recipe_chunk (title, ingredients, instructions, price)
    """
    src_id = ensure_source_rest(source_name)
    imported, offset = 0, 0
    while imported < total:
        batch = min(page_size, total - imported)
        results = fetch_soups_page(limit=batch, offset=offset)
        if not results:
            break
        for r in results:
            rid, items, ingredients_text, title, instructions_plain = upsert_recipe_rest(src_id, r)

            # Widgets nachladen
            wid = r.get("id")
            try:
                ingw = fetch_ingredient_widget(wid)
            except Exception:
                ingw = {}
            try:
                nutw = fetch_nutrition_widget(wid)
            except Exception:
                nutw = {}
            try:
                pric = fetch_price_breakdown(wid)
            except Exception:
                pric = {}

            # speichern
            upsert_ingredients_and_join_rest(rid, items)   # aus complexSearch
            upsert_nutrition_rest(rid, r)                  # aus complexSearch (Basis)
            if pric:
                upsert_price_breakdown_rest(rid, pric)     # Preis
            # Roh-JSON (optional, aber praktisch für Debug)
            try:
                upsert_recipe_raw_rest(rid, r, ingw, nutw, pric)
            except Exception:
                pass

            # RAG-Chunks
            insert_chunks_rest(rid, title, ingredients_text, instructions_plain)
            if pric:
                insert_price_chunk_rest(rid, pric)

            time.sleep(0.3)  # kleine Pause zwischen einzelnen Rezepten (nett zur API)

        imported += len(results)
        offset += len(results)
        time.sleep(sleep_s)  # API freundlich bleiben
        print(f"Imported {imported}/{total}")
    print("Import fertig ✅")

# =========================
#   Main
# =========================
if __name__ == "__main__":
    # Für den ersten Lauf eher klein halten, um Quota zu sparen
    import_soups(total=10, page_size=10)
