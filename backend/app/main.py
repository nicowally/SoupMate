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


def search_recipes_in_db(query_embedding, topk=5):
    # An die Supabase-RPC-Funktion search_recipes senden
    search_url = f"{SUPABASE_URL}/rest/v1/rpc/search_recipes"
    payload = {
        "query_embedding": query_embedding,
        "topk": topk
    }

    response = requests.post(search_url, headers=SB_HEADERS, json=payload)
    response.raise_for_status()

    # Rückgabe der besten Rezepte
    return response.json()


@app.post("/api/chat")
def chat(req: ChatRequest):
    # Generiere das Embedding des User-Prompts
    query_embedding = model.encode(req.query, convert_to_tensor=True).cpu().tolist()

    # API-Call zu Supabase, um nach ähnlichen Rezepten zu suchen
    search_results = search_recipes_in_db(query_embedding)

    # Extrahiere und formatiere die besten Rezepte
    similar_recipes = [result["title"] for result in search_results]

    return {"answer": f"Du hast gefragt: '{req.query}'. Ähnliche Rezepte: {', '.join(similar_recipes)}"}


