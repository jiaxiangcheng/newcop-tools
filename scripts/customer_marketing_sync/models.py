from pydantic import BaseModel, Field
from typing import Dict, Optional, List
from datetime import datetime

class CustomerMarketing(BaseModel):
    """Model for tracking customer marketing subscription state"""
    customer_id: int
    email: Optional[str] = None  # Email can be None for some customers
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email_subscribed: bool
    marketing_opt_in_level: Optional[str] = None
    last_updated: datetime

class CustomerMarketingCache(BaseModel):
    """Model for caching customer marketing state"""
    customer_id: int
    email: Optional[str] = None  # Email can be None for some customers
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email_subscribed: bool
    marketing_opt_in_level: Optional[str] = None
    last_sync: datetime

class MarketingCache(BaseModel):
    """Complete customer marketing cache structure"""
    last_sync: Optional[datetime] = None
    customers: Dict[str, CustomerMarketingCache] = Field(default_factory=dict)
    total_customers: int = 0

class CustomerUpdate(BaseModel):
    """Model for customer metafield update"""
    customer_id: int
    email: Optional[str] = None
    old_email_subscribed: Optional[bool] = None
    new_email_subscribed: bool
    metafield_namespace: str = "custom"
    metafield_key: str = "accepts_marketing"

class MarketingSyncResult(BaseModel):
    """Results of customer marketing sync operation"""
    success: bool
    total_customers_processed: int = 0
    customers_updated: int = 0
    customers_failed: int = 0
    customers_with_changes: int = 0
    execution_time_seconds: float = 0.0
    errors: List[str] = Field(default_factory=list)
    updated_customers: List[CustomerUpdate] = Field(default_factory=list)
    sync_timestamp: datetime = Field(default_factory=datetime.now)

class MarketingChangeDetection(BaseModel):
    """Model for detecting customer marketing subscription changes"""
    customer_id: int
    email: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    old_email_subscribed: Optional[bool] = None
    new_email_subscribed: bool
    has_changed: bool
    
    @property
    def change_description(self) -> str:
        """Generate a human-readable change description"""
        if self.old_email_subscribed is None:
            return f"New customer: {self.new_email_subscribed}"
        elif self.old_email_subscribed != self.new_email_subscribed:
            return f"{self.old_email_subscribed} → {self.new_email_subscribed}"
        else:
            return "No change"
    
    @property
    def customer_display_name(self) -> str:
        """Generate a display name for the customer"""
        if self.first_name or self.last_name:
            name_parts = [part for part in [self.first_name, self.last_name] if part]
            return " ".join(name_parts)
        return self.email or f"Customer ID: {self.customer_id}"