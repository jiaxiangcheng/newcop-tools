"""
Data models for inventory-based product tag sync.
"""
from typing import List, Optional
from pydantic import BaseModel


class VariantInventory(BaseModel):
    """Variant with inventory information."""
    id: str
    title: str
    inventory_quantity: int


class ProductInventoryInfo(BaseModel):
    """Product with inventory and tag information."""
    id: str
    title: str
    tags: List[str]
    variants: List[VariantInventory]
    total_inventory: int

    @property
    def has_stock(self) -> bool:
        """Check if any variant has inventory > 0."""
        return any(v.inventory_quantity > 0 for v in self.variants)

    @property
    def current_inventory_tag(self) -> Optional[str]:
        """Get the current inventory-related tag (instore-online or instore-only)."""
        for tag in self.tags:
            if tag in ("instore-online", "instore-only"):
                return tag
        return None

    @property
    def expected_tag(self) -> str:
        """Get the tag that should be set based on inventory."""
        return "instore-online" if self.has_stock else "instore-only"

    @property
    def needs_update(self) -> bool:
        """Check if the product's tag needs to be updated."""
        return self.current_inventory_tag != self.expected_tag


class TagSyncResult(BaseModel):
    """Result of syncing a single product's tag."""
    product_id: str
    product_title: str
    success: bool
    old_tag: Optional[str] = None
    new_tag: str
    error: Optional[str] = None
    skipped: bool = False
    reason: Optional[str] = None
