"""
Data models for product type sync.
"""
from pydantic import BaseModel
from typing import Optional


class ProductTypeUpdate(BaseModel):
    """Represents a product type update."""
    product_id: int
    product_title: str
    collection_id: str
    current_type: Optional[str]
    new_type: str
    tags: Optional[str] = None
    has_retail_tag: bool = False


class TypeSyncResult(BaseModel):
    """Result of a product type update operation."""
    product_id: int
    product_title: str
    collection_id: str
    success: bool
    old_type: Optional[str]
    new_type: str
    error: Optional[str] = None
    skipped: bool = False
    reason: Optional[str] = None
