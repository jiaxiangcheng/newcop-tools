"""
Data models for the Delete Catalog Fixed Prices job.
"""
from typing import List, Optional
from pydantic import BaseModel


class FixedPriceEntry(BaseModel):
    """A single fixed price entry inside a PriceList."""
    variant_id: str  # gid://shopify/ProductVariant/...
    price_amount: str
    currency_code: str


class CatalogPriceListInfo(BaseModel):
    """Resolved Catalog -> PriceList relationship."""
    catalog_id: str  # gid://shopify/Catalog/...
    price_list_id: str  # gid://shopify/PriceList/...
    price_list_currency: Optional[str] = None


class DeleteBatchResult(BaseModel):
    """Result of deleting a single batch of fixed prices."""
    deleted_variant_ids: List[str] = []
    errors: List[str] = []


class DeleteSummary(BaseModel):
    """Final summary of a delete operation."""
    catalog_id: str
    price_list_id: str
    total_found: int
    total_deleted: int
    total_failed: int
    dry_run: bool
