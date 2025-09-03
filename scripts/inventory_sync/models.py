from pydantic import BaseModel, Field
from typing import Dict, Optional, List
from datetime import datetime
from decimal import Decimal

class VariantInventory(BaseModel):
    """Model for tracking variant inventory state"""
    variant_id: int
    product_id: int
    inventory_quantity: int
    price: Optional[str] = None  # Store as string to match Shopify API
    compare_at_price: Optional[str] = None  # Store as string to match Shopify API
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
    old_price: Optional[str] = None
    new_price: Optional[str] = None
    old_compare_at_price: Optional[str] = None
    new_compare_at_price: Optional[str] = None
    metafield_namespace: str = "custom"
    metafield_key: str = "inventory"
    updated_fields: List[str] = Field(default_factory=list)  # Track which fields were updated

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
    """Model for detecting inventory and price changes"""
    variant_id: int
    product_id: int
    product_title: str
    variant_title: Optional[str] = None
    old_quantity: Optional[int] = None
    new_quantity: int
    old_price: Optional[str] = None
    new_price: Optional[str] = None
    old_compare_at_price: Optional[str] = None
    new_compare_at_price: Optional[str] = None
    has_changed: bool
    changed_fields: List[str] = Field(default_factory=list)  # Track which fields changed
    
    @property
    def change_description(self) -> str:
        """Generate a human-readable change description"""
        if not self.has_changed:
            return "No change"
        
        changes = []
        
        # Inventory change
        if "inventory" in self.changed_fields:
            if self.old_quantity is None:
                changes.append(f"New inventory: {self.new_quantity}")
            else:
                changes.append(f"Inventory: {self.old_quantity} → {self.new_quantity}")
        
        # Price change
        if "price" in self.changed_fields:
            if self.old_price is None:
                changes.append(f"New price: €{self.new_price}")
            else:
                changes.append(f"Price: €{self.old_price} → €{self.new_price}")
        
        # Compare at price change
        if "compare_at_price" in self.changed_fields:
            if self.old_compare_at_price is None:
                changes.append(f"New compare price: €{self.new_compare_at_price}")
            else:
                changes.append(f"Compare price: €{self.old_compare_at_price} → €{self.new_compare_at_price}")
        
        return "; ".join(changes) if changes else "No change"