from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="SoupMate API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/health")
def health():
    return {"status": "ok"}


class ChatRequest(BaseModel):
    query: str

@app.post("/api/chat")
def chat(req: ChatRequest):
    print("Received query from frontend:", req.query)
    return {"answer": f"Du hast gefragt: '{req.query}'. RAG-Antwort kommt später"}
