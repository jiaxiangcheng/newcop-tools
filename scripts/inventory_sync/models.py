from pydantic import BaseModel, Field
from typing import Dict, Optional, List
from datetime import datetime

class VariantInventory(BaseModel):
    """Model for tracking variant inventory state"""
    variant_id: int
    product_id: int
    inventory_quantity: int
    last_updated: datetime
    title: Optional[str] = None
    sku: Optional[str] = None

class ProductInventoryCache(BaseModel):
    """Model for caching product inventory state"""
    product_id: int
    product_title: str
    variants: Dict[str, VariantInventory] = Field(default_factory=dict)
    last_sync: datetime

class InventoryCache(BaseModel):
    """Complete inventory cache structure"""
    last_sync: Optional[datetime] = None
    products: Dict[str, ProductInventoryCache] = Field(default_factory=dict)
    total_products: int = 0
    total_variants: int = 0

class VariantUpdate(BaseModel):
    """Model for variant metafield update"""
    variant_id: int
    product_id: int
    old_quantity: Optional[int] = None
    new_quantity: int
    metafield_namespace: str = "custom"
    metafield_key: str = "inventory"

class SyncResult(BaseModel):
    """Results of inventory sync operation"""
    success: bool
    total_products_processed: int = 0
    total_variants_checked: int = 0
    variants_updated: int = 0
    variants_failed: int = 0
    products_with_changes: int = 0
    execution_time_seconds: float = 0.0
    errors: List[str] = Field(default_factory=list)
    updated_variants: List[VariantUpdate] = Field(default_factory=list)
    sync_timestamp: datetime = Field(default_factory=datetime.now)

class InventoryChangeDetection(BaseModel):
    """Model for detecting inventory changes"""
    variant_id: int
    product_id: int
    product_title: str
    variant_title: Optional[str] = None
    old_quantity: Optional[int] = None
    new_quantity: int
    has_changed: bool
    
    @property
    def change_description(self) -> str:
        """Generate a human-readable change description"""
        if self.old_quantity is None:
            return f"New variant: {self.new_quantity}"
        elif self.old_quantity != self.new_quantity:
            return f"{self.old_quantity} → {self.new_quantity}"
        else:
            return "No change"