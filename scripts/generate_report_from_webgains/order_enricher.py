#!/usr/bin/env python3
"""
Order Enricher

Fetches Shopify order data and enriches Webgains records with customer information.
"""

import logging
import time
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from shared.shopify_client import ShopifyClient
from scripts.generate_report_from_webgains.models import (
    WebgainsRecord,
    EnrichedRecord,
    ShopifyOrderData,
    CustomerInfo,
    CustomerJourneyInfo,
    FirstVisitInfo,
    UTMParameters,
    LineItem,
    ProcessingResult
)

logger = logging.getLogger(__name__)


class OrderEnricher:
    """Enriches Webgains records with Shopify order data"""

    def __init__(self, shopify_client: ShopifyClient, max_workers: int = 5):
        """
        Initialize order enricher

        Args:
            shopify_client: Shopify API client
            max_workers: Maximum concurrent API requests
        """
        self.shopify_client = shopify_client
        self.max_workers = max_workers

    def enrich_records(self, records: List[WebgainsRecord], dry_run: bool = False) -> ProcessingResult:
        """
        Enrich Webgains records with Shopify order data

        Args:
            records: List of Webgains records to enrich
            dry_run: If True, only log what would be processed without making API calls

        Returns:
            ProcessingResult with enriched records and statistics
        """
        start_time = time.time()
        result = ProcessingResult(total_records=len(records))

        logger.info(f"Starting enrichment of {len(records)} records")
        if dry_run:
            logger.info("DRY RUN mode: No API calls will be made")

        enriched_records = []

        if dry_run:
            # In dry run mode, just create enriched records without API calls
            for record in records:
                enriched = EnrichedRecord(
                    affiliate=record.affiliate,
                    sale=record.sale,
                    commission=record.commission,
                    override=record.override,
                    date_time=record.date_time,
                    order_reference=record.order_reference,
                    webgains_country=record.country,
                    commission_type=record.commission_type,
                    percentage=record.percentage,
                    shopify_order_data=None
                )
                enriched_records.append(enriched)
                result.skipped_records += 1

            logger.info(f"DRY RUN: Would process {len(records)} records")

        else:
            # Process records with concurrent API calls
            enriched_records = self._process_records_concurrently(records, result)

        result.enriched_records = enriched_records
        result.processed_records = len(enriched_records)
        result.execution_time_seconds = time.time() - start_time

        # Log summary
        logger.info("=" * 60)
        logger.info("Enrichment Summary")
        logger.info("=" * 60)
        logger.info(f"Total records: {result.total_records}")
        logger.info(f"Processed records: {result.processed_records}")
        logger.info(f"Successful lookups: {result.successful_lookups}")
        logger.info(f"Failed lookups: {result.failed_lookups}")
        logger.info(f"Skipped records: {result.skipped_records}")
        logger.info(f"Execution time: {result.execution_time_seconds:.2f} seconds")
        logger.info("=" * 60)

        return result

    def _process_records_concurrently(self, records: List[WebgainsRecord], result: ProcessingResult) -> List[EnrichedRecord]:
        """
        Process records with concurrent API calls

        Args:
            records: List of Webgains records
            result: ProcessingResult to update with statistics

        Returns:
            List of enriched records
        """
        enriched_records = []

        # Use ThreadPoolExecutor for concurrent API calls
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_record = {
                executor.submit(self._enrich_single_record, record): record
                for record in records
            }

            # Process completed tasks
            completed = 0
            for future in as_completed(future_to_record):
                record = future_to_record[future]
                completed += 1

                try:
                    enriched = future.result()
                    enriched_records.append(enriched)

                    # Update statistics
                    if enriched.shopify_order_data and not enriched.shopify_order_data.error:
                        result.successful_lookups += 1
                    elif enriched.shopify_order_data and enriched.shopify_order_data.error:
                        result.failed_lookups += 1
                    else:
                        result.skipped_records += 1

                    # Log progress every 10 records
                    if completed % 10 == 0:
                        progress_pct = completed / len(records) * 100
                        logger.info(f"Progress: {completed}/{len(records)} records ({progress_pct:.1f}%)")

                except Exception as e:
                    logger.error(f"Error processing record {record.order_reference}: {e}")
                    result.errors.append(f"Record {record.order_reference}: {str(e)}")
                    result.failed_lookups += 1

                    # Create enriched record with error
                    enriched = self._create_enriched_record_with_error(record, str(e))
                    enriched_records.append(enriched)

        return enriched_records

    def _enrich_single_record(self, record: WebgainsRecord) -> EnrichedRecord:
        """
        Enrich a single Webgains record with Shopify order data

        Args:
            record: Webgains record to enrich

        Returns:
            EnrichedRecord with Shopify data
        """
        # Create base enriched record
        enriched = EnrichedRecord(
            affiliate=record.affiliate,
            sale=record.sale,
            commission=record.commission,
            override=record.override,
            date_time=record.date_time,
            order_reference=record.order_reference,
            webgains_country=record.country,
            commission_type=record.commission_type,
            percentage=record.percentage
        )

        # Check if order reference exists
        if not record.order_reference or str(record.order_reference).strip() == "":
            logger.warning(f"Skipping record with empty order reference")
            enriched.shopify_order_data = ShopifyOrderData(error="Empty order reference")
            return enriched

        # Fetch Shopify order data
        try:
            order_data = self.shopify_client.get_order_details_graphql(str(record.order_reference))

            if order_data:
                # Parse order data into models
                shopify_order = self._parse_order_data(order_data)
                enriched.shopify_order_data = shopify_order
                logger.debug(f"Successfully enriched order {record.order_reference}")
            else:
                # Order not found
                enriched.shopify_order_data = ShopifyOrderData(error=f"Order not found: {record.order_reference}")
                logger.warning(f"Order not found: {record.order_reference}")

        except Exception as e:
            logger.error(f"Error fetching order {record.order_reference}: {e}")
            enriched.shopify_order_data = ShopifyOrderData(error=f"API error: {str(e)}")

        return enriched

    def _parse_order_data(self, order_data: Dict[str, Any]) -> ShopifyOrderData:
        """
        Parse Shopify GraphQL order data into ShopifyOrderData model

        Args:
            order_data: Raw order data from GraphQL

        Returns:
            ShopifyOrderData object
        """
        try:
            # Parse customer info
            customer_info = None
            customer_raw = order_data.get("customer")
            if customer_raw:
                # Get shipping country - use pre-processed shipping_country from shopify_client
                # which prioritizes order.shippingAddress over customer.defaultAddress
                shipping_country = customer_raw.get("shipping_country")

                # Fallback to defaultAddress if shipping_country wasn't pre-processed
                if not shipping_country and customer_raw.get("defaultAddress"):
                    shipping_country = customer_raw["defaultAddress"].get("countryCode")

                # Parse numberOfOrders (it's returned as string, convert to int)
                orders_count = 0
                number_of_orders = customer_raw.get("numberOfOrders")
                if number_of_orders:
                    try:
                        orders_count = int(number_of_orders)
                    except (ValueError, TypeError):
                        orders_count = 0

                customer_info = CustomerInfo(
                    email=customer_raw.get("email"),
                    firstName=customer_raw.get("firstName"),
                    lastName=customer_raw.get("lastName"),
                    phone=customer_raw.get("phone"),
                    shipping_country=shipping_country,
                    orders_count=orders_count
                )

            # Parse customer journey
            customer_journey = None
            journey_raw = order_data.get("customerJourneySummary")
            if journey_raw:
                first_visit_raw = journey_raw.get("firstVisit")
                first_visit = None

                if first_visit_raw:
                    # Parse UTM parameters
                    utm_params = None
                    utm_raw = first_visit_raw.get("utmParameters")
                    if utm_raw:
                        utm_params = UTMParameters(
                            source=utm_raw.get("source"),
                            medium=utm_raw.get("medium"),
                            campaign=utm_raw.get("campaign"),
                            term=utm_raw.get("term"),
                            content=utm_raw.get("content")
                        )

                    first_visit = FirstVisitInfo(
                        source=first_visit_raw.get("source"),
                        sourceDescription=first_visit_raw.get("sourceDescription"),
                        utmParameters=utm_params
                    )

                customer_journey = CustomerJourneyInfo(firstVisit=first_visit)

            # Parse line items
            line_items = []
            line_items_raw = order_data.get("lineItems", {}).get("edges", [])
            for edge in line_items_raw:
                node = edge.get("node", {})
                # Parse product tags
                product_tags = []
                product_raw = node.get("product")
                if product_raw and product_raw.get("tags"):
                    product_tags = product_raw["tags"]

                line_item = LineItem(
                    title=node.get("title"),
                    variantTitle=node.get("variantTitle"),
                    sku=node.get("sku"),
                    quantity=node.get("quantity"),
                    tags=product_tags
                )
                line_items.append(line_item)

            # Parse refund data
            refund_amount = None
            refund_currency = None
            refunds_raw = order_data.get("refunds", [])
            if refunds_raw:
                # Calculate total refund amount across all refunds
                total_refund = 0.0
                for refund in refunds_raw:
                    refunded_set = refund.get("totalRefundedSet", {})
                    shop_money = refunded_set.get("shopMoney", {})
                    if shop_money:
                        amount_str = shop_money.get("amount")
                        if amount_str:
                            try:
                                total_refund += float(amount_str)
                                if not refund_currency:
                                    refund_currency = shop_money.get("currencyCode", "EUR")
                            except (ValueError, TypeError):
                                pass

                if total_refund > 0:
                    refund_amount = total_refund

            # Parse return data
            return_status = None
            has_active_return = False
            returns_raw = order_data.get("returns", {}).get("edges", [])
            if returns_raw:
                for edge in returns_raw:
                    node = edge.get("node", {})
                    status = node.get("status", "")

                    # Check if return is in progress (not CLOSED, CANCELLED, etc.)
                    if status and status.upper() in ["OPEN", "IN_PROGRESS", "REQUESTED", "RETURN_IN_PROGRESS"]:
                        has_active_return = True
                        return_status = status
                        break

            # Create ShopifyOrderData
            return ShopifyOrderData(
                order_id=order_data.get("id"),
                order_name=order_data.get("name"),
                createdAt=order_data.get("createdAt"),
                lineItems=line_items,
                customer=customer_info,
                customerJourneySummary=customer_journey,
                displayFinancialStatus=order_data.get("displayFinancialStatus"),
                displayFulfillmentStatus=order_data.get("displayFulfillmentStatus"),
                cancelledAt=order_data.get("cancelledAt"),
                refund_amount=refund_amount,
                refund_currency=refund_currency,
                return_status=return_status,
                has_active_return=has_active_return
            )

        except Exception as e:
            logger.error(f"Error parsing order data: {e}")
            return ShopifyOrderData(error=f"Parsing error: {str(e)}")

    def _create_enriched_record_with_error(self, record: WebgainsRecord, error: str) -> EnrichedRecord:
        """
        Create an enriched record with error information

        Args:
            record: Original Webgains record
            error: Error message

        Returns:
            EnrichedRecord with error
        """
        return EnrichedRecord(
            affiliate=record.affiliate,
            sale=record.sale,
            commission=record.commission,
            override=record.override,
            date_time=record.date_time,
            order_reference=record.order_reference,
            webgains_country=record.country,
            commission_type=record.commission_type,
            percentage=record.percentage,
            shopify_order_data=ShopifyOrderData(error=error)
        )
