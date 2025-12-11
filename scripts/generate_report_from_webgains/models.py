#!/usr/bin/env python3
"""
Data models for Webgains report enrichment

Defines Pydantic models for Webgains records and enriched order data.
"""

from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, Field


class CustomerInfo(BaseModel):
    """Customer information from Shopify"""
    email: Optional[str] = None
    first_name: Optional[str] = Field(None, alias="firstName")
    last_name: Optional[str] = Field(None, alias="lastName")
    phone: Optional[str] = None
    shipping_country: Optional[str] = None
    orders_count: Optional[int] = 0

    class Config:
        populate_by_name = True


class UTMParameters(BaseModel):
    """UTM parameters from customer journey"""
    source: Optional[str] = None
    medium: Optional[str] = None
    campaign: Optional[str] = None
    term: Optional[str] = None
    content: Optional[str] = None


class FirstVisitInfo(BaseModel):
    """First visit information from customer journey"""
    source: Optional[str] = None
    source_description: Optional[str] = Field(None, alias="sourceDescription")
    utm_parameters: Optional[UTMParameters] = Field(None, alias="utmParameters")

    class Config:
        populate_by_name = True


class CustomerJourneyInfo(BaseModel):
    """Customer journey information"""
    first_visit: Optional[FirstVisitInfo] = Field(None, alias="firstVisit")

    class Config:
        populate_by_name = True


class LineItem(BaseModel):
    """Order line item (product)"""
    title: Optional[str] = None
    variant_title: Optional[str] = Field(None, alias="variantTitle")
    sku: Optional[str] = None
    quantity: Optional[int] = None

    class Config:
        populate_by_name = True


class ShopifyOrderData(BaseModel):
    """Complete Shopify order data"""
    order_id: Optional[str] = None
    order_name: Optional[str] = None
    created_at: Optional[str] = Field(None, alias="createdAt")
    line_items: List[LineItem] = Field(default_factory=list, alias="lineItems")
    customer: Optional[CustomerInfo] = None
    customer_journey: Optional[CustomerJourneyInfo] = Field(None, alias="customerJourneySummary")
    financial_status: Optional[str] = Field(None, alias="displayFinancialStatus")
    fulfillment_status: Optional[str] = Field(None, alias="displayFulfillmentStatus")
    cancelled_at: Optional[str] = Field(None, alias="cancelledAt")
    refund_amount: Optional[float] = None  # Total refunded amount in EUR
    refund_currency: Optional[str] = None  # Currency code for refund
    return_status: Optional[str] = None  # Return status (e.g., "IN_PROGRESS", "CLOSED")
    has_active_return: bool = False  # Whether there's a return in progress
    error: Optional[str] = None

    class Config:
        populate_by_name = True

    @property
    def customer_type(self) -> str:
        """Determine if customer is first-time or repeat"""
        if not self.customer or self.customer.orders_count is None:
            return "Unknown"

        if self.customer.orders_count == 1:
            return "First Order"
        else:
            return f"Repeat Customer (Order #{self.customer.orders_count})"


class WebgainsRecord(BaseModel):
    """Original Webgains Excel record"""
    affiliate: Optional[str] = None
    sale: Optional[float] = None
    commission: Optional[float] = None
    override: Optional[float] = None
    date_time: Optional[str] = Field(None, alias="date_and_time")
    order_reference: Optional[str] = None
    country: Optional[str] = None
    commission_type: Optional[str] = None
    percentage: Optional[str] = None

    class Config:
        populate_by_name = True


class EnrichedRecord(BaseModel):
    """Enriched record with Webgains and Shopify data"""
    # Original Webgains data
    affiliate: Optional[str] = None
    sale: Optional[float] = None
    commission: Optional[float] = None
    override: Optional[float] = None
    date_time: Optional[str] = None
    order_reference: Optional[str] = None
    country: Optional[str] = None
    commission_type: Optional[str] = None
    percentage: Optional[str] = None

    # Enriched Shopify data
    shopify_order_data: Optional[ShopifyOrderData] = None

    # Computed fields for Excel export
    @property
    def customer_email(self) -> str:
        return self.shopify_order_data.customer.email if self.shopify_order_data and self.shopify_order_data.customer else ""

    @property
    def customer_first_name(self) -> str:
        return self.shopify_order_data.customer.first_name if self.shopify_order_data and self.shopify_order_data.customer else ""

    @property
    def customer_last_name(self) -> str:
        return self.shopify_order_data.customer.last_name if self.shopify_order_data and self.shopify_order_data.customer else ""

    @property
    def customer_phone(self) -> str:
        return self.shopify_order_data.customer.phone if self.shopify_order_data and self.shopify_order_data.customer else ""

    @property
    def shipping_country(self) -> str:
        return self.shopify_order_data.customer.shipping_country if self.shopify_order_data and self.shopify_order_data.customer else ""

    @property
    def customer_type(self) -> str:
        return self.shopify_order_data.customer_type if self.shopify_order_data else ""

    @property
    def order_number_for_customer(self) -> str:
        if not self.shopify_order_data or not self.shopify_order_data.customer:
            return ""
        count = self.shopify_order_data.customer.orders_count
        return str(count) if count else ""

    @property
    def order_created_at(self) -> str:
        """Order creation date/time from Shopify"""
        return self.shopify_order_data.created_at if self.shopify_order_data and self.shopify_order_data.created_at else ""

    @property
    def product_names(self) -> str:
        """Product names from line items (comma-separated)"""
        if not self.shopify_order_data or not self.shopify_order_data.line_items:
            return ""
        names = [item.title for item in self.shopify_order_data.line_items if item.title]
        return ", ".join(names) if names else ""

    @property
    def variant_names(self) -> str:
        """Variant names from line items (comma-separated), with 'EU -' suffix removed"""
        if not self.shopify_order_data or not self.shopify_order_data.line_items:
            return ""
        variants = []
        for item in self.shopify_order_data.line_items:
            if item.variant_title:
                # Remove "EU -" suffix if present
                variant = item.variant_title
                if variant.endswith("EU -"):
                    variant = variant[:-4].strip()  # Remove "EU -" and any trailing whitespace
                variants.append(variant)
        return ", ".join(variants) if variants else ""

    @property
    def product_skus(self) -> str:
        """Product SKUs from line items (comma-separated)"""
        if not self.shopify_order_data or not self.shopify_order_data.line_items:
            return ""
        skus = [item.sku for item in self.shopify_order_data.line_items if item.sku]
        return ", ".join(skus) if skus else ""

    @property
    def first_visit_source(self) -> str:
        if not self.shopify_order_data or not self.shopify_order_data.customer_journey:
            return ""
        first_visit = self.shopify_order_data.customer_journey.first_visit
        return first_visit.source if first_visit else ""

    @property
    def utm_source(self) -> str:
        if not self.shopify_order_data or not self.shopify_order_data.customer_journey:
            return ""
        first_visit = self.shopify_order_data.customer_journey.first_visit
        if first_visit and first_visit.utm_parameters:
            return first_visit.utm_parameters.source or ""
        return ""

    @property
    def utm_medium(self) -> str:
        if not self.shopify_order_data or not self.shopify_order_data.customer_journey:
            return ""
        first_visit = self.shopify_order_data.customer_journey.first_visit
        if first_visit and first_visit.utm_parameters:
            return first_visit.utm_parameters.medium or ""
        return ""

    @property
    def utm_campaign(self) -> str:
        if not self.shopify_order_data or not self.shopify_order_data.customer_journey:
            return ""
        first_visit = self.shopify_order_data.customer_journey.first_visit
        if first_visit and first_visit.utm_parameters:
            return first_visit.utm_parameters.campaign or ""
        return ""

    @property
    def utm_term(self) -> str:
        if not self.shopify_order_data or not self.shopify_order_data.customer_journey:
            return ""
        first_visit = self.shopify_order_data.customer_journey.first_visit
        if first_visit and first_visit.utm_parameters:
            return first_visit.utm_parameters.term or ""
        return ""

    @property
    def utm_content(self) -> str:
        if not self.shopify_order_data or not self.shopify_order_data.customer_journey:
            return ""
        first_visit = self.shopify_order_data.customer_journey.first_visit
        if first_visit and first_visit.utm_parameters:
            return first_visit.utm_parameters.content or ""
        return ""

    @property
    def financial_status(self) -> str:
        """Financial status of the order (paid, refunded, partially_refunded, etc.)"""
        return self.shopify_order_data.financial_status if self.shopify_order_data and self.shopify_order_data.financial_status else ""

    @property
    def fulfillment_status(self) -> str:
        """Fulfillment status of the order (fulfilled, unfulfilled, etc.)"""
        return self.shopify_order_data.fulfillment_status if self.shopify_order_data and self.shopify_order_data.fulfillment_status else ""

    @property
    def is_cancelled(self) -> str:
        """Whether the order has been cancelled"""
        if not self.shopify_order_data:
            return ""
        return "Yes" if self.shopify_order_data.cancelled_at else "No"

    @property
    def order_status_notes(self) -> str:
        """Combined status notes for refunds and cancellations"""
        if not self.shopify_order_data:
            return ""

        notes = []

        # Check if cancelled
        if self.shopify_order_data.cancelled_at:
            notes.append("CANCELLED")

        # Check financial status for refunds
        if self.shopify_order_data.financial_status:
            status = self.shopify_order_data.financial_status.upper()
            if "REFUND" in status:
                notes.append(status)

        return " | ".join(notes) if notes else ""

    @property
    def refund_amount(self) -> str:
        """Refund amount in EUR for partially refunded orders"""
        if not self.shopify_order_data or self.shopify_order_data.refund_amount is None:
            return ""

        # Only show refund amount if it's a partial refund (financial_status contains PARTIALLY)
        financial_status = self.shopify_order_data.financial_status or ""
        if "PARTIALLY" in financial_status.upper():
            # Format as EUR with 2 decimal places
            amount = self.shopify_order_data.refund_amount
            currency = self.shopify_order_data.refund_currency or "EUR"
            return f"{amount:.2f} {currency}"

        return ""

    @property
    def return_status(self) -> str:
        """Return status (IN_PROGRESS, CLOSED, etc.)"""
        if not self.shopify_order_data:
            return ""

        if self.shopify_order_data.has_active_return:
            return "RETURN IN PROCESS"

        return ""

    @property
    def issue_type(self) -> str:
        """Issue type based on financial and fulfillment status"""
        if not self.shopify_order_data:
            return ""

        financial_status = (self.shopify_order_data.financial_status or "").upper()
        fulfillment_status = (self.shopify_order_data.fulfillment_status or "").upper()

        # Priority 1: If refunded, return "PEDIDO CANCELADO"
        if financial_status == "REFUNDED":
            return "PEDIDO CANCELADO"

        # Priority 2: If unfulfilled, return "PEDIDO NO PREPARADO"
        if fulfillment_status == "UNFULFILLED":
            return "PEDIDO NO PREPARADO"

        # Priority 3: If partially refunded, return "Issue (PARTIALLY REFUNDED)"
        if "PARTIALLY" in financial_status and "REFUND" in financial_status:
            return "Issue (PARTIALLY REFUNDED)"

        return ""

    @property
    def error_message(self) -> str:
        """Return error message if order lookup failed"""
        return self.shopify_order_data.error if self.shopify_order_data and self.shopify_order_data.error else ""


class ProcessingResult(BaseModel):
    """Result of processing Webgains report"""
    total_records: int = 0
    processed_records: int = 0
    successful_lookups: int = 0
    failed_lookups: int = 0
    skipped_records: int = 0
    enriched_records: List[EnrichedRecord] = []
    errors: List[str] = []
    execution_time_seconds: float = 0.0
