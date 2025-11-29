"""
Data models for Product Discount Calculator
"""
from typing import List, Set, Optional
from pydantic import BaseModel, Field
from datetime import datetime


class VariantDiscount(BaseModel):
    """Represents a discount calculation for a variant"""
    variant_id: int = Field(..., description="Shopify variant ID")
    variant_title: Optional[str] = Field(None, description="Variant title")
    price: float = Field(..., description="Current price")
    compare_at_price: Optional[float] = Field(None, description="Compare at price")
    discount_percentage: Optional[int] = Field(None, description="Rounded discount percentage (multiple of 5)")

    def has_discount(self) -> bool:
        """Check if variant has a valid discount"""
        return (
            self.compare_at_price is not None and
            self.compare_at_price > 0 and
            self.compare_at_price > self.price
        )

    def calculate_discount(self) -> Optional[int]:
        """Calculate discount percentage rounded to nearest 5"""
        if not self.has_discount():
            return None

        # Calculate raw discount percentage
        raw_discount = ((self.compare_at_price - self.price) / self.compare_at_price) * 100

        # Round to nearest 5
        rounded_discount = round(raw_discount / 5) * 5

        # Ensure it's between 0 and 100
        return max(0, min(100, rounded_discount))


class ProductDiscounts(BaseModel):
    """Represents discount analysis for a product"""
    product_id: int = Field(..., description="Shopify product ID")
    product_title: str = Field(..., description="Product title")
    variants: List[VariantDiscount] = Field(default_factory=list, description="Variant discounts")
    unique_discounts: Set[int] = Field(default_factory=set, description="Unique discount percentages")
    current_metafield_discounts: List[str] = Field(default_factory=list, description="Current discounts in metafield")

    class Config:
        # Allow set type
        arbitrary_types_allowed = True

    def calculate_unique_discounts(self) -> Set[int]:
        """Calculate unique discount percentages across all variants"""
        discounts = set()
        for variant in self.variants:
            discount = variant.calculate_discount()
            if discount is not None and discount > 0:
                discounts.add(discount)
        return discounts

    def needs_update(self) -> bool:
        """
        Check if metafield needs to be updated

        This method handles both old format (without %) and new format (with %)
        Returns True if:
        1. The discount values are different
        2. The old format doesn't have % symbols (needs conversion)
        """
        # Calculate current unique discounts
        current_discounts = self.calculate_unique_discounts()

        # Convert metafield values to set of ints (strip % symbol if present)
        metafield_discounts = set()
        has_percentage_symbol = False

        try:
            for d in self.current_metafield_discounts:
                if d and d.strip():
                    # Check if it has % symbol
                    if '%' in d:
                        has_percentage_symbol = True
                    # Remove % symbol if present
                    clean_value = d.strip().rstrip('%')
                    metafield_discounts.add(int(clean_value))
        except (ValueError, AttributeError):
            metafield_discounts = set()

        # Check if values differ
        values_differ = current_discounts != metafield_discounts

        # Check if format needs update (old format without %)
        # If we have values but none have %, we need to update to add %
        format_needs_update = bool(metafield_discounts) and not has_percentage_symbol

        # Update if either values differ OR format needs conversion
        return values_differ or format_needs_update

    def get_discounts_list(self) -> List[str]:
        """Get sorted list of discount percentages as strings with % symbol"""
        discounts = self.calculate_unique_discounts()
        return [f"{d}%" for d in sorted(discounts)]


class DiscountSyncResult(BaseModel):
    """Results from a discount synchronization run"""
    total_products_processed: int = Field(0, description="Total products processed")
    products_with_discounts: int = Field(0, description="Products with at least one discount")
    products_updated: int = Field(0, description="Products with metafield updated")
    products_failed: int = Field(0, description="Products that failed to update")
    failed_product_ids: List[int] = Field(default_factory=list, description="IDs of products that failed")

    total_unique_discount_percentages: Set[int] = Field(default_factory=set, description="All unique discount percentages found")

    execution_time_seconds: float = Field(..., description="Total execution time")
    timestamp: datetime = Field(default_factory=datetime.now, description="Execution timestamp")
    dry_run: bool = Field(False, description="Whether this was a dry run")

    class Config:
        arbitrary_types_allowed = True

    def is_success(self) -> bool:
        """Check if sync was successful"""
        return self.products_failed == 0 and self.total_products_processed > 0
