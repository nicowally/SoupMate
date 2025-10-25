from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
from torchgen import model

import requests
import os

from dotenv import load_dotenv
load_dotenv()  # Lädt die .env-Datei, wenn sie im gleichen Ordner wie main.py ist
print("SUPABASE_URL:", os.getenv("SUPABASE_URL"))
print("SUPABASE_SECRET_KEY:", os.getenv("SUPABASE_SECRET_KEY"))
print("SPOONACULAR_API_KEY:", os.getenv("SPOONACULAR_API_KEY"))

SUPABASE_URL = "https://brssalvqnbxgaiwmycpf.supabase.co"
SUPABASE_SECRET_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJyc3NhbHZxbmJ4Z2Fpd215Y3BmIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MTE0OTU1MywiZXhwIjoyMDc2NzI1NTUzfQ.LOtZiPf1bx9ZV5CpEeG03Mlli-FoDIrIOcnX4Qz9Asc"

SB_HEADERS = {
    "apikey": SUPABASE_SECRET_KEY,
    "Authorization": f"Bearer {SUPABASE_SECRET_KEY}",
    "Content-Type": "application/json",
}

app = FastAPI(title="SoupMate API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

@app.get("/api/health")
def health():
    return {"status": "ok"}


class ChatRequest(BaseModel):
    query: str

def extract_keywords(prompt: str) -> list:
    # Splitte den User-Prompt in einzelne Wörter
    words = prompt.lower().split()
    return words


def search_recipes_in_db(query_embedding, query_keywords=None, topk=5):
    rpc_url = f"{SUPABASE_URL}/rest/v1/rpc/search_recipes_with_keywords"

    payload = {
        "query_embedding": query_embedding,
        "query_keywords": query_keywords,
        "topk": topk
    }

    try:
        response = requests.post(rpc_url, headers=SB_HEADERS, json=payload)
        response.raise_for_status()

        # Überprüfe, ob die Antwort Ergebnisse enthält
        if response.json():
            return response.json()
        else:
            print("Keine ähnlichen Rezepte gefunden.")
            return []

    except requests.exceptions.RequestException as e:
        print(f"Fehler bei der Supabase-Anfrage: {e}")
        return []


def sb_get(table: str, params: dict, timeout=30):
    """
    Führt eine GET-Anfrage an die Supabase-API aus, um Daten aus der angegebenen Tabelle abzurufen.
    """
    try:
        url = f"{SUPABASE_URL}/rest/v1/{table}?apikey={SUPABASE_SECRET_KEY}"
        response = requests.get(url, headers={"Content-Type": "application/json"}, params=params, timeout=timeout)

        response.raise_for_status()

        # Hier sicherstellen, dass die Antwort ein JSON-Dictionary ist
        print("Antwort von Supabase:", response.json())  # Debugging: Antwort anzeigen
        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"Fehler bei der Supabase-Anfrage: {e}")
        return []  # Bei Fehler eine leere Liste zurückgeben




STOPWORDS = {"und", "mit", "für", "ein", "eine", "der", "die", "das", "in", "auf"}
def fetch_keywords_from_db():
    # Hole alle Rezeptnamen und Inhalte aus der Supabase-Datenbank
    params = {"select": "title, instructions_raw"}  # Ändere 'content' zu 'instructions_raw'
    recipes = sb_get("recipe", params)

    keywords = []
    for recipe in recipes:
        # Stelle sicher, dass die 'recipe' Daten das richtige Dictionary sind
        title = recipe.get("title", "").lower()  # Hier funktioniert get()
        instructions_raw = recipe.get("instructions_raw", "").lower()  # Hier 'instructions_raw' verwenden

        # Füge Schlüsselwörter aus dem Rezeptnamen hinzu
        keywords.extend(title.split())

        # Füge Schlüsselwörter aus den Zubereitungsanweisungen hinzu
        keywords.extend(instructions_raw.split())

    # Entferne Duplikate
    keywords = list(set(keywords))

    return keywords


@app.post("/api/chat")
def chat(req: ChatRequest):
    # Generiere das Embedding des User-Prompts
    query_embedding = model.encode(req.query, convert_to_tensor=True).cpu().tolist()

    # Extrahiere die Keywords aus dem User-Prompt
    query_keywords = extract_keywords(req.query)

    # Hole alle Keywords aus der Rezeptdatenbank
    db_keywords = fetch_keywords_from_db()
    print("Keywords aus der Datenbank:", db_keywords)  # Debugging: Zeige die Keywords aus der DB

    # Kombiniere die Keywords aus der Rezeptdatenbank und dem User-Prompt
    all_keywords = list(set(query_keywords + db_keywords))

    # API-Call zu Supabase, um nach ähnlichen Rezepten zu suchen
    search_results = search_recipes_in_db(query_embedding, query_keywords=all_keywords)
    print("Suchergebnisse:", search_results)  # Debugging: Zeige die Ergebnisse der Rezept-Suche

    similar_recipes = []
    instructions_text = []  # Liste, um alle Anweisungen zu speichern
    for result in search_results:
        recipe_id = result.get("recipe_id")  # Rezept ID erhalten
        print(f"Verarbeite Rezept mit ID: {recipe_id}")  # Debugging: Zeige die ID des Rezepts

        if recipe_id:  # Überprüfe, ob die ID vorhanden ist
            # Führe die Abfrage durch, um 'title' und 'instructions_raw' direkt aus der 'recipe' Tabelle zu holen
            rows = sb_get("recipe", {
                "select": "id, title, instructions_raw",  # Hole 'title' und 'instructions_raw' aus der 'recipe'-Tabelle
                "id": f"eq.{recipe_id}",  # Überprüfe die ID in der 'recipe'-Tabelle
                "limit": 1  # Nur das erste Ergebnis
            })

            print(f"Supabase Antwort für Rezept ID {recipe_id}: {rows}")  # Debugging: Antwort von Supabase

            if rows:
                recipe = rows[0]  # Das erste Rezept in der Antwort
                title = recipe["title"]
                instructions_raw = recipe["instructions_raw"]  # Zubereitungsanweisung speichern

                # Füge das Rezept zur Liste hinzu, wenn sowohl Titel als auch Zubereitungsanweisung vorhanden sind
                if title and instructions_raw:
                    similar_recipes.append(title)  # Nur den Titel hinzufügen
                    instructions_text.append(instructions_raw)  # Zubereitungsanweisung zu der Liste hinzufügen
                else:
                    print(f"Kein Titel oder Anweisungen für Rezept ID {recipe_id}")  # Debugging: Kein Titel/Anweisung gefunden
            else:
                print(f"Kein Rezept gefunden für ID: {recipe_id}")  # Debugging: Keine Ergebnisse gefunden

    # Kombiniere die ähnlichen Rezepte und Zubereitungsanweisungen
    similar_recipes_text = ', '.join(similar_recipes)  # Titel der Rezepte als kommagetrennte Liste
    instructions_text_combined = '\n\n'.join(instructions_text)  # Zubereitungsanweisungen als separater Block

    # Kombiniere alles in einer Antwort
    return {
        "answer": f"Du hast gefragt: '{req.query}'. Ähnliche Rezepte: '{similar_recipes_text}'. \n Instructions: \n'{instructions_text_combined}"
    }






