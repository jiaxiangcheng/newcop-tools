"""
Data models for variants metafield sync.
"""
from typing import List, Optional
from pydantic import BaseModel


class VariantInfo(BaseModel):
    """Variant information."""
    id: str
    title: str


class ProductVariantsInfo(BaseModel):
    """Product with variants information."""
    id: str
    title: str
    variants: List[VariantInfo]
    current_metafield_value: Optional[List[str]] = None


class SyncResult(BaseModel):
    """Result of syncing variants to metafield."""
    product_id: str
    product_title: str
    success: bool
    variant_count: int
    variant_names: List[str]
    error: Optional[str] = None
    skipped: bool = False
    reason: Optional[str] = None
