from fastapi import FastAPI, HTTPException, Depends, Header
from typing import List, Optional
from .supabase_client import get_supabase
from .schemas import CreateUserInput, UserInputOut

app = FastAPI(title="SoupMate Backend", version="0.1.0")

@app.get("/health")
def health():
  return {"status": "ok"}

@app.post("/inputs", response_model=UserInputOut)
def create_user_input(payload: CreateUserInput):
  """
  Schreibt einen Datensatz in die Tabelle public.user_inputs.
  Erwartet: user_id, title (optional), payload (jsonb).
  """
  supabase = get_supabase()
  res = supabase.table("user_inputs").insert({
    "user_id": payload.user_id,
    "title": payload.title,
    "payload": payload.payload
  }).select("*").single().execute()

  if res.error:
    raise HTTPException(status_code=400, detail=res.error.message)

  return res.data  # FastAPI mappt automatisch auf UserInputOut

@app.get("/inputs", response_model=List[UserInputOut])
def list_my_inputs(user_id: str):
  """
  Listet Datensätze für eine gegebene user_id (einfaches MVP).
  In Produktion: user_id aus Bearer-Token ableiten & RLS nutzen.
  """
  supabase = get_supabase()
  res = supabase.table("user_inputs") \
    .select("*") \
    .eq("user_id", user_id) \
    .order("created_at", desc=True) \
    .execute()

  if res.error:
    raise HTTPException(status_code=400, detail=res.error.message)

  return res.data
