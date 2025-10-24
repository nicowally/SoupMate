from pydantic import BaseModel, Field
from typing import Any, Optional, Dict

class CreateUserInput(BaseModel):
  # In einem echten Setup würdest du user_id aus dem Auth-Token ableiten.
  user_id: str = Field(..., description="Supabase Auth User UUID")
  title: Optional[str] = None
  payload: Dict[str, Any]

class UserInputOut(CreateUserInput):
  id: str
  created_at: str
