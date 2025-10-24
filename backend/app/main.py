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

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SECRET_KEY = os.environ["SUPABASE_SECRET_KEY"]

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
        url = f"{SUPABASE_URL}/rest/v1/{table}"
        response = requests.get(url, headers=SB_HEADERS, params=params, timeout=timeout)
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
    params = {"select": "title, content"}
    recipes = sb_get("recipe", params)

    keywords = []
    for recipe in recipes:
        # Stelle sicher, dass die 'recipe' Daten das richtige Dictionary sind
        title = recipe.get("title", "").lower()  # Hier funktioniert get()
        content = recipe.get("content", "").lower()

        # Füge Schlüsselwörter aus dem Rezeptnamen hinzu
        keywords.extend(title.split())

        # Füge Schlüsselwörter aus dem Inhalt hinzu
        keywords.extend(content.split())

    # Entferne Duplikate
    keywords = list(set(keywords))

    return keywords





@app.post("/api/chat")
def chat(req: ChatRequest):
    # Generiere das Embedding des User-Prompts
    query_embedding = model.encode(req.query, convert_to_tensor=True).cpu().tolist()

    # Extrahiere die Keywords aus dem User-Prompt (z. B. "beef", "potatoes")
    query_keywords = extract_keywords(req.query)
    print("Extrahierte Keywords:", query_keywords)  # Debugging: Zeige extrahierte Keywords

    # Hole alle Keywords aus der Rezeptdatenbank
    db_keywords = fetch_keywords_from_db()
    print("Keywords aus der Datenbank:", db_keywords)  # Debugging: Zeige Datenbank-Keywords

    # Kombiniere die Keywords aus der Rezeptdatenbank und dem User-Prompt
    all_keywords = list(set(query_keywords + db_keywords))

    # API-Call zu Supabase, um nach ähnlichen Rezepten zu suchen
    search_results = search_recipes_in_db(query_embedding, query_keywords=all_keywords)

    # Extrahiere die Titel der gefundenen Rezepte
    similar_recipes = [result["title"] for result in search_results]
    print("Gefundene Rezepte:", similar_recipes)  # Debugging: Zeige die gefundenen Rezepte

    return {"answer": f"Du hast gefragt: '{req.query}'. Ähnliche Rezepte: {', '.join(similar_recipes)}"}






