"""Data models for shipping (delivery) profile duplication."""
from typing import Optional
from pydantic import BaseModel


class ProfileSummary(BaseModel):
    """Concise representation of a Delivery Profile for listing/selection."""
    id: str
    name: str
    default: bool
    active_method_definitions_count: int = 0
    product_variants_count: int = 0


class CreateResult(BaseModel):
    """Result of a duplicate operation."""
    success: bool
    new_profile_id: Optional[str] = None
    new_profile_name: Optional[str] = None
    dry_run: bool = False
    errors: list[str] = []
