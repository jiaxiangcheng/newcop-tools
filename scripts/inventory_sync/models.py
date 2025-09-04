from pydantic import BaseModel, Field, validator
from typing import Dict, Optional, List, Set
from datetime import datetime
from decimal import Decimal
from enum import Enum

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
        if "compare_price" in self.changed_fields:
            if self.old_compare_at_price is None:
                changes.append(f"New compare price: €{self.new_compare_at_price}")
            else:
                changes.append(f"Compare price: €{self.old_compare_at_price} → €{self.new_compare_at_price}")
        
        return "; ".join(changes) if changes else "No change"


class SyncField(str, Enum):
    """Available fields for synchronization"""
    INVENTORY = "inventory"
    PRICE = "price"
    COMPARE_PRICE = "compare_price"


class SyncMode(str, Enum):
    """Synchronization modes"""
    MANUAL = "manual"
    SCHEDULED = "scheduled"
    DISABLED = "disabled"


class FieldSyncConfig(BaseModel):
    """Configuration for a specific field sync"""
    field: SyncField
    enabled: bool = True
    mode: SyncMode = SyncMode.MANUAL
    interval_minutes: Optional[int] = None  # Only used for scheduled mode
    
    @validator('interval_minutes')
    def validate_interval_for_scheduled(cls, v, values):
        if values.get('mode') == SyncMode.SCHEDULED and v is None:
            raise ValueError("interval_minutes is required for scheduled mode")
        return v


class FlexibleSyncConfig(BaseModel):
    """Flexible configuration for inventory sync fields"""
    inventory: FieldSyncConfig = Field(default_factory=lambda: FieldSyncConfig(field=SyncField.INVENTORY))
    price: FieldSyncConfig = Field(default_factory=lambda: FieldSyncConfig(field=SyncField.PRICE))
    compare_price: FieldSyncConfig = Field(default_factory=lambda: FieldSyncConfig(field=SyncField.COMPARE_PRICE))
    
    # Global settings
    dry_run: bool = False
    batch_size: int = 50
    max_concurrent_per_product: int = 2
    
    def get_enabled_fields(self) -> Set[SyncField]:
        """Get all fields that are enabled for syncing"""
        enabled_fields = set()
        for field_config in [self.inventory, self.price, self.compare_price]:
            if field_config.enabled and field_config.mode != SyncMode.DISABLED:
                enabled_fields.add(field_config.field)
        return enabled_fields
    
    def get_scheduled_fields(self) -> Dict[SyncField, int]:
        """Get fields configured for scheduled mode with their intervals"""
        scheduled_fields = {}
        for field_config in [self.inventory, self.price, self.compare_price]:
            if field_config.enabled and field_config.mode == SyncMode.SCHEDULED:
                scheduled_fields[field_config.field] = field_config.interval_minutes
        return scheduled_fields
    
    def is_field_enabled_for_sync(self, field: SyncField, target_mode: SyncMode) -> bool:
        """Check if a field is enabled for a specific mode"""
        field_config = getattr(self, field.value)
        return field_config.enabled and field_config.mode == target_mode
    
    @classmethod
    def from_env_vars(cls, env_dict: Dict[str, str]) -> 'FlexibleSyncConfig':
        """Create config from environment variables"""
        
        # Parse boolean from string
        def parse_bool(value: str, default: bool = True) -> bool:
            if value is None:
                return default
            return value.lower() in ['true', '1', 'yes', 'on']
        
        # Parse interval string to minutes
        def parse_interval(interval_str: str, default_hours: int = 6) -> int:
            if not interval_str:
                return default_hours * 60
            
            interval_str = interval_str.strip().lower()
            try:
                if interval_str.endswith('h'):
                    hours = int(interval_str[:-1])
                    return hours * 60
                elif interval_str.endswith('m') or interval_str.endswith('min'):
                    suffix = 'min' if interval_str.endswith('min') else 'm'
                    minutes = int(interval_str[:-len(suffix)])
                    return minutes
                else:
                    hours = int(interval_str)
                    return hours * 60
            except ValueError:
                return default_hours * 60
        
        # Parse mode
        def parse_mode(mode_str: str, enabled: bool = True) -> SyncMode:
            if not enabled:
                return SyncMode.DISABLED
            if not mode_str:
                return SyncMode.MANUAL
            mode_str = mode_str.lower()
            if mode_str in ['scheduled', 'automatic', 'auto']:
                return SyncMode.SCHEDULED
            elif mode_str in ['manual', 'once']:
                return SyncMode.MANUAL
            elif mode_str in ['disabled', 'false', 'off']:
                return SyncMode.DISABLED
            else:
                return SyncMode.MANUAL
        
        # Parse individual field configurations
        inventory_enabled = parse_bool(env_dict.get('SYNC_INVENTORY', 'true'))
        price_enabled = parse_bool(env_dict.get('SYNC_PRICE', 'true'))
        compare_price_enabled = parse_bool(env_dict.get('SYNC_COMPARE_PRICE', 'true'))
        
        inventory_mode = parse_mode(env_dict.get('INVENTORY_SYNC_MODE', 'manual'), inventory_enabled)
        price_mode = parse_mode(env_dict.get('PRICE_SYNC_MODE', 'manual'), price_enabled)
        compare_price_mode = parse_mode(env_dict.get('COMPARE_PRICE_SYNC_MODE', 'manual'), compare_price_enabled)
        
        inventory_interval = parse_interval(env_dict.get('INVENTORY_SYNC_INTERVAL', '6h'))
        price_interval = parse_interval(env_dict.get('PRICE_SYNC_INTERVAL', '6h'))
        compare_price_interval = parse_interval(env_dict.get('COMPARE_PRICE_SYNC_INTERVAL', '6h'))
        
        # Global settings
        dry_run = parse_bool(env_dict.get('INVENTORY_SYNC_DRY_RUN', 'false'), default=False)
        batch_size = int(env_dict.get('INVENTORY_SYNC_BATCH_SIZE', '50'))
        max_concurrent = int(env_dict.get('INVENTORY_SYNC_MAX_CONCURRENT', '2'))
        
        return cls(
            inventory=FieldSyncConfig(
                field=SyncField.INVENTORY,
                enabled=inventory_enabled,
                mode=inventory_mode,
                interval_minutes=inventory_interval if inventory_mode == SyncMode.SCHEDULED else None
            ),
            price=FieldSyncConfig(
                field=SyncField.PRICE,
                enabled=price_enabled,
                mode=price_mode,
                interval_minutes=price_interval if price_mode == SyncMode.SCHEDULED else None
            ),
            compare_price=FieldSyncConfig(
                field=SyncField.COMPARE_PRICE,
                enabled=compare_price_enabled,
                mode=compare_price_mode,
                interval_minutes=compare_price_interval if compare_price_mode == SyncMode.SCHEDULED else None
            ),
            dry_run=dry_run,
            batch_size=batch_size,
            max_concurrent_per_product=max_concurrent
        )