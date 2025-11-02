#!/usr/bin/env python3
"""
Data Models for Customer Order History

Pydantic models for order records, customer statistics, and sync results.
"""

from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
from datetime import datetime


class OrderRecord(BaseModel):
    """Represents an Airtable order record"""
    record_id: str
    shopify_order_id: Optional[str] = None  # Order number from "Order" field (e.g., "#66677")
    current_total_order_number: Optional[int] = None  # Current value in Airtable
    needs_update: bool = False

    # Additional fields for reference
    order_date: Optional[str] = None
    customer_email: Optional[str] = None


class CustomerOrderStats(BaseModel):
    """Customer order statistics from Shopify"""
    customer_id: Optional[str] = None
    customer_email: Optional[str] = None
    customer_first_name: Optional[str] = None
    customer_last_name: Optional[str] = None
    number_of_orders: int = 0
    error: Optional[str] = None

    def get_full_name(self) -> str:
        """Get customer's full name"""
        parts = []
        if self.customer_first_name:
            parts.append(self.customer_first_name)
        if self.customer_last_name:
            parts.append(self.customer_last_name)
        return " ".join(parts) if parts else ""


class OrderUpdate(BaseModel):
    """Represents an order record update to be sent to Airtable"""
    record_id: str
    total_order_number: int
    customer_name: str  # First name + Last name
    shopify_order_id: str  # Order number for logging purposes


class ProcessedRecordsCache(BaseModel):
    """Cache structure for tracking processed records"""
    last_sync: Optional[datetime] = None
    processed_record_ids: List[str] = Field(default_factory=list)
    total_processed: int = 0


class OrderHistorySyncResult(BaseModel):
    """Result of an order history sync operation"""
    success: bool = False
    total_orders_fetched: int = 0
    orders_processed: int = 0
    orders_updated: int = 0
    orders_skipped: int = 0
    orders_failed: int = 0
    execution_time_seconds: float = 0.0
    errors: List[str] = Field(default_factory=list)
    sync_timestamp: datetime = Field(default_factory=datetime.now)

    def get_summary(self) -> str:
        """Get a formatted summary string"""
        return (
            f"Sync Summary:\n"
            f"  Total Orders Fetched: {self.total_orders_fetched}\n"
            f"  Orders Processed: {self.orders_processed}\n"
            f"  Orders Updated: {self.orders_updated}\n"
            f"  Orders Skipped: {self.orders_skipped}\n"
            f"  Orders Failed: {self.orders_failed}\n"
            f"  Execution Time: {self.execution_time_seconds:.2f}s\n"
            f"  Status: {'✅ Success' if self.success else '❌ Failed'}"
        )
