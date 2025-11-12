"""
Data models for Best Seller Badge management
"""
from typing import Optional, List
from pydantic import BaseModel, Field
from datetime import datetime


class AirtableProduct(BaseModel):
    """Product record from Airtable"""
    record_id: str = Field(..., description="Airtable record ID")
    product_name: str = Field(..., description="Product name")
    shopify_id: Optional[int] = Field(None, description="Shopify product ID")
    quarterly_sales: Optional[float] = Field(None, description="Quarterly sales amount")
    total_sales: Optional[float] = Field(None, description="Total sales amount")

    class Config:
        # Allow extra fields from Airtable that we don't use
        extra = "allow"


class BestSellerUpdate(BaseModel):
    """Represents a best seller badge update operation"""
    product_id: int = Field(..., description="Shopify product ID")
    product_name: str = Field(..., description="Product name for logging")
    current_badge_status: Optional[bool] = Field(None, description="Current best seller status")
    target_badge_status: bool = Field(..., description="Target best seller status")

    def needs_update(self) -> bool:
        """Check if this product needs an update"""
        return self.current_badge_status != self.target_badge_status


class BadgeSyncResult(BaseModel):
    """Results from a badge synchronization run"""
    total_products_in_airtable: int = Field(..., description="Total products fetched from Airtable")
    valid_products_count: int = Field(..., description="Products with valid Shopify IDs")
    invalid_products_count: int = Field(..., description="Products without valid Shopify IDs")
    invalid_products: List[str] = Field(default_factory=list, description="Names of products without valid IDs")

    badges_removed_count: int = Field(0, description="Number of badges removed")
    badges_added_count: int = Field(0, description="Number of badges added")

    successful_updates: int = Field(0, description="Successful badge updates")
    failed_updates: int = Field(0, description="Failed badge updates")
    failed_products: List[str] = Field(default_factory=list, description="Names of products that failed to update")

    execution_time_seconds: float = Field(..., description="Total execution time")
    timestamp: datetime = Field(default_factory=datetime.now, description="Execution timestamp")
    dry_run: bool = Field(False, description="Whether this was a dry run")

    def is_success(self) -> bool:
        """Check if sync was successful"""
        return self.failed_updates == 0 and self.valid_products_count > 0
