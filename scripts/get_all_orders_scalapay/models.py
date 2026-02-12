#!/usr/bin/env python3
"""
Data models for Scalapay Orders Script

Pydantic models for order data representation and validation.
"""

from dataclasses import dataclass
from typing import Optional, List
from datetime import datetime


@dataclass
class ScalapayOrder:
    """Represents a Shopify order paid with Scalapay"""
    order_name: str                    # Order name (e.g., #12345)
    customer_email: Optional[str]      # Customer email
    customer_first_name: Optional[str] # Customer first name
    customer_last_name: Optional[str]  # Customer last name
    fulfillment_status: Optional[str]  # Fulfillment status
    financial_status: Optional[str]    # Payment status
    payment_gateway: str               # Payment gateway name
    created_at: Optional[datetime]     # Order creation date
    total_price: Optional[str]         # Order total
    currency: Optional[str]            # Currency code
    # New fields
    subtotal_price: Optional[str]      # Subtotal (before shipping/tax)
    shipping_price: Optional[str]      # Shipping cost
    total_refunded: Optional[str]      # Total refunded amount
    shipping_address: Optional[str]    # Full shipping address
    shipping_city: Optional[str]       # Shipping city
    shipping_country: Optional[str]    # Shipping country
    line_items: Optional[str]          # Order line items (formatted string)

    @property
    def customer_full_name(self) -> str:
        """Get customer full name"""
        parts = []
        if self.customer_first_name:
            parts.append(self.customer_first_name)
        if self.customer_last_name:
            parts.append(self.customer_last_name)
        return " ".join(parts) if parts else ""

    @property
    def fulfillment_status_display(self) -> str:
        """Get display-friendly fulfillment status"""
        if not self.fulfillment_status:
            return "UNFULFILLED"
        return self.fulfillment_status.upper()

    @property
    def financial_status_display(self) -> str:
        """Get display-friendly financial status"""
        if not self.financial_status:
            return "UNKNOWN"
        return self.financial_status.upper()


@dataclass
class ScalapayOrderResult:
    """Result container for Scalapay order fetch operation"""
    orders: List[ScalapayOrder]
    total_orders_scanned: int
    scalapay_orders_found: int
    execution_time_seconds: float

    @property
    def success_rate(self) -> float:
        """Calculate success rate"""
        if self.total_orders_scanned == 0:
            return 0.0
        return self.scalapay_orders_found / self.total_orders_scanned * 100
