import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
  raise RuntimeError("SUPABASE_URL oder SUPABASE_SERVICE_ROLE_KEY fehlt in .env")

def get_supabase() -> Client:
  return create_client(SUPABASE_URL, SUPABASE_KEY)
