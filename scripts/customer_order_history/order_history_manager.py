#!/usr/bin/env python3
"""
Order History Manager

Core business logic for fetching orders from Airtable,
querying Shopify for customer order counts, and updating Airtable.
"""

import logging
import time
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from shared.shopify_client import ShopifyClient
from shared.airtable_client import AirtableClient
from scripts.customer_order_history.models import (
    OrderRecord,
    CustomerOrderStats,
    OrderUpdate,
    OrderHistorySyncResult
)
from scripts.customer_order_history.storage import OrderHistoryStorage

logger = logging.getLogger(__name__)


class OrderHistoryManager:
    """Manages customer order history analysis and synchronization"""

    def __init__(
        self,
        shopify_client: ShopifyClient,
        airtable_client: AirtableClient,
        airtable_table_id: str,
        airtable_view_id: str,
        storage: OrderHistoryStorage,
        max_workers: int = 5
    ):
        """
        Initialize order history manager

        Args:
            shopify_client: Shopify API client
            airtable_client: Airtable API client
            airtable_table_id: Airtable table ID for orders
            airtable_view_id: Airtable view ID (pre-filtered for last 6 months)
            storage: Storage manager for caching
            max_workers: Maximum concurrent Shopify API requests
        """
        self.shopify_client = shopify_client
        self.airtable_client = airtable_client
        self.airtable_table_id = airtable_table_id
        self.airtable_view_id = airtable_view_id
        self.storage = storage
        self.max_workers = max_workers

    def sync_order_history(self, dry_run: bool = False, force_all: bool = False, yesterday_only: bool = False) -> OrderHistorySyncResult:
        """
        Main sync operation: fetch orders, query Shopify, update Airtable

        Args:
            dry_run: If True, analyze without updating Airtable
            force_all: If True, process all records ignoring cache
            yesterday_only: If True, only process orders from yesterday (for scheduled mode)

        Returns:
            OrderHistorySyncResult with statistics and errors
        """
        start_time = time.time()
        result = OrderHistorySyncResult()

        logger.info("=" * 60)
        logger.info("Starting Customer Order History Sync")
        logger.info("=" * 60)

        if dry_run:
            logger.info("🧪 DRY RUN MODE: No updates will be made to Airtable")

        if yesterday_only:
            logger.info("📅 YESTERDAY ONLY MODE: Processing orders from yesterday only")

        try:
            # Process orders in batches to avoid loading all data at once
            logger.info("\n📥 Starting batch processing of orders from Airtable...")
            logger.info("Processing in batches of 100 records for optimal performance")

            batch_number = 0
            all_updates = []

            # Use a callback-based approach to process batches as they arrive
            def process_batch(batch_records: List[Dict[str, Any]]) -> None:
                nonlocal batch_number, all_updates
                batch_number += 1

                logger.info(f"\n📦 Batch {batch_number}: Processing {len(batch_records)} records...")

                # Parse batch records
                order_records = self._parse_airtable_records(batch_records, yesterday_only)

                if not order_records:
                    logger.info(f"⚠️  Batch {batch_number}: No valid orders after filtering")
                    return

                result.total_orders_fetched += len(order_records)

                # Filter unprocessed records (unless force_all is True)
                if not force_all:
                    record_ids = [rec.record_id for rec in order_records]
                    unprocessed_ids = set(self.storage.get_unprocessed_records(record_ids))
                    order_records = [rec for rec in order_records if rec.record_id in unprocessed_ids]

                    if not order_records:
                        logger.info(f"✅ Batch {batch_number}: All records already processed")
                        return

                    logger.info(f"📋 Batch {batch_number}: Processing {len(order_records)} unprocessed orders")
                else:
                    logger.info(f"⚡ Batch {batch_number}: Force mode - processing all {len(order_records)} orders")

                # Process orders concurrently
                logger.info(f"🔍 Batch {batch_number}: Querying Shopify for customer order counts...")
                batch_updates = self._process_orders_concurrently(order_records, result)

                # Collect updates
                if batch_updates:
                    all_updates.extend(batch_updates)
                    logger.info(f"✅ Batch {batch_number}: {len(batch_updates)} records need updating")

                    # Update Airtable immediately for this batch (if not dry run)
                    if not dry_run:
                        logger.info(f"📝 Batch {batch_number}: Updating {len(batch_updates)} records in Airtable...")
                        self._update_airtable_batch(batch_updates, result)

                        # Mark records as processed
                        processed_ids = [update.record_id for update in batch_updates]
                        self.storage.mark_records_processed(processed_ids)
                        self.storage.save_cache()
                        logger.info(f"💾 Batch {batch_number}: Cache updated")
                else:
                    logger.info(f"⚠️  Batch {batch_number}: No updates needed")

            # Fetch records with batch callback
            self.airtable_client.get_records(
                table_id=self.airtable_table_id,
                view_id=self.airtable_view_id,
                batch_callback=process_batch,
                batch_size=100
            )

            # Final summary
            if result.total_orders_fetched == 0:
                logger.warning("\n⚠️  No orders found in Airtable")
                result.success = True
                result.execution_time_seconds = time.time() - start_time
                return result

            if dry_run and all_updates:
                logger.info(f"\n🧪 DRY RUN: Would update {len(all_updates)} records in Airtable")
                result.orders_updated = len(all_updates)
            elif not all_updates:
                logger.info("\n✅ All records already up to date, no updates needed")

            result.success = True
            result.execution_time_seconds = time.time() - start_time

            # Log summary
            logger.info("\n" + "=" * 60)
            logger.info(result.get_summary())
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"❌ Sync failed: {e}", exc_info=True)
            result.errors.append(f"Sync error: {str(e)}")
            result.execution_time_seconds = time.time() - start_time

        return result

    def _parse_airtable_records(self, raw_records: List[Dict[str, Any]], yesterday_only: bool = False) -> List[OrderRecord]:
        """
        Parse raw Airtable records into OrderRecord objects

        Args:
            raw_records: Raw records from Airtable API
            yesterday_only: If True, filter for orders from yesterday only

        Returns:
            List of OrderRecord objects
        """
        from datetime import datetime, timedelta

        try:
            # If yesterday_only mode, filter by date
            if yesterday_only:
                # Calculate yesterday's date
                today = datetime.now().date()
                yesterday = today - timedelta(days=1)

                # Filter records by Date field
                filtered_records = []
                for record in raw_records:
                    fields = record.get("fields", {})
                    order_date_str = fields.get("Date")

                    if order_date_str:
                        try:
                            # Parse the date string (Airtable typically uses ISO format: YYYY-MM-DD)
                            order_date = datetime.fromisoformat(order_date_str.split('T')[0])
                            order_date_only = order_date.date()

                            # Check if order is from yesterday
                            if order_date_only == yesterday:
                                filtered_records.append(record)
                        except (ValueError, AttributeError) as e:
                            logger.debug(f"Could not parse date for record {record.get('id')}: {order_date_str}")
                            continue

                raw_records = filtered_records

            # Parse records into OrderRecord objects
            order_records = []
            for raw_record in raw_records:
                record_id = raw_record.get("id")
                fields = raw_record.get("fields", {})

                # Extract Order Number from "Order" field (e.g., "#66677" or "#66677-1")
                order_number = fields.get("Order")

                # Skip records without Order Number
                if not order_number or str(order_number).strip() == "":
                    logger.debug(f"Skipping record {record_id}: No Order Number")
                    continue

                # Parse order number: remove "-X" suffix if present
                # Example: "#66677-1" -> "#66677", "#66677-2" -> "#66677"
                order_number_str = str(order_number).strip()
                if "-" in order_number_str:
                    order_number_str = order_number_str.split("-")[0]

                # Get current Total Order Number value (if any)
                current_total = fields.get("Total Order Number")
                if current_total:
                    try:
                        current_total = int(current_total)
                    except (ValueError, TypeError):
                        current_total = None

                order_record = OrderRecord(
                    record_id=record_id,
                    shopify_order_id=order_number_str,  # Store the parsed order number
                    current_total_order_number=current_total,
                    order_date=fields.get("Date"),
                    customer_email=fields.get("Email")
                )

                order_records.append(order_record)

            logger.info(f"Parsed {len(order_records)} valid order records")
            skipped = len(raw_records) - len(order_records)
            if skipped > 0:
                logger.warning(f"⚠️  Skipped {skipped} records without Order Number")

            return order_records

        except Exception as e:
            logger.error(f"Error fetching orders from Airtable: {e}")
            raise

    def _process_orders_concurrently(
        self,
        order_records: List[OrderRecord],
        result: OrderHistorySyncResult
    ) -> List[OrderUpdate]:
        """
        Process orders with concurrent Shopify API calls

        Args:
            order_records: List of order records to process
            result: Result object to update with statistics

        Returns:
            List of OrderUpdate objects for records that need updating
        """
        updates = []

        # Use ThreadPoolExecutor for concurrent API calls
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_order = {
                executor.submit(self._process_single_order, order): order
                for order in order_records
            }

            # Process completed tasks
            completed = 0
            for future in as_completed(future_to_order):
                order = future_to_order[future]
                completed += 1

                try:
                    update = future.result()
                    result.orders_processed += 1

                    if update:
                        updates.append(update)
                        result.orders_updated += 1
                    else:
                        result.orders_skipped += 1

                    # Log progress every 10 orders
                    if completed % 10 == 0:
                        progress_pct = completed / len(order_records) * 100
                        logger.info(
                            f"Progress: {completed}/{len(order_records)} orders "
                            f"({progress_pct:.1f}%) - "
                            f"{result.orders_updated} updates, "
                            f"{result.orders_skipped} skipped, "
                            f"{result.orders_failed} failed"
                        )

                except Exception as e:
                    logger.error(f"Error processing order {order.record_id}: {e}")
                    result.orders_failed += 1
                    result.errors.append(f"Order {order.record_id}: {str(e)}")

        logger.info(
            f"✅ Processing complete: {result.orders_processed} processed, "
            f"{result.orders_updated} need updates, "
            f"{result.orders_skipped} skipped, "
            f"{result.orders_failed} failed"
        )

        return updates

    def _process_single_order(self, order: OrderRecord) -> Optional[OrderUpdate]:
        """
        Process a single order: query Shopify and determine if update is needed

        Args:
            order: OrderRecord to process

        Returns:
            OrderUpdate if record needs updating, None otherwise
        """
        try:
            # Query Shopify for order details using order number (e.g., "#66677")
            # The get_order_details_graphql method will handle the search
            order_data = self.shopify_client.get_order_details_graphql(order.shopify_order_id)

            if not order_data:
                logger.warning(f"⚠️  Order not found in Shopify: {order.shopify_order_id}")
                return None

            # Extract customer information
            customer = order_data.get("customer")
            if not customer:
                logger.warning(f"⚠️  No customer info for order {order.shopify_order_id}")
                return None

            # Get numberOfOrders
            number_of_orders = customer.get("numberOfOrders")
            if number_of_orders is None:
                logger.warning(f"⚠️  No numberOfOrders for order {order.shopify_order_id}")
                return None

            # Convert to int
            try:
                total_order_number = int(number_of_orders)
            except (ValueError, TypeError):
                logger.warning(f"⚠️  Invalid numberOfOrders value: {number_of_orders}")
                return None

            # Get customer name
            first_name = customer.get("firstName", "")
            last_name = customer.get("lastName", "")
            customer_name_parts = []
            if first_name:
                customer_name_parts.append(first_name)
            if last_name:
                customer_name_parts.append(last_name)
            customer_name = " ".join(customer_name_parts) if customer_name_parts else ""

            # Check if update is needed
            if order.current_total_order_number == total_order_number:
                logger.debug(
                    f"Skipping order {order.record_id}: "
                    f"Total Order Number already set to {total_order_number}"
                )
                return None

            # Create update
            update = OrderUpdate(
                record_id=order.record_id,
                total_order_number=total_order_number,
                customer_name=customer_name,
                shopify_order_id=order.shopify_order_id
            )

            customer_type = "new" if total_order_number == 1 else "returning"
            logger.debug(
                f"Order {order.shopify_order_id}: "
                f"{customer_type} customer ({total_order_number} orders) - {customer_name}"
            )

            return update

        except Exception as e:
            logger.error(f"Error processing order {order.record_id}: {e}")
            raise

    def _update_airtable_batch(self, updates: List[OrderUpdate], result: OrderHistorySyncResult) -> None:
        """
        Update Airtable records in batches

        Args:
            updates: List of OrderUpdate objects
            result: Result object to update with statistics
        """
        try:
            # Prepare batch update payload
            records = []
            for update in updates:
                fields = {
                    "Total Order Number": update.total_order_number
                }

                # Add customer name if available
                if update.customer_name:
                    fields["Customer"] = update.customer_name

                records.append({
                    "id": update.record_id,
                    "fields": fields
                })

            # Use batch_update_records method
            update_result = self.airtable_client.batch_update_records(
                table_id=self.airtable_table_id,
                records=records
            )

            # Update result statistics
            if update_result["success"]:
                logger.info(f"✅ Successfully updated {update_result['updated']} records in Airtable")
            else:
                logger.error(
                    f"❌ Batch update failed: {update_result['failed']} records failed"
                )
                result.errors.extend(update_result["errors"])

        except Exception as e:
            logger.error(f"Error updating Airtable: {e}")
            result.errors.append(f"Airtable update error: {str(e)}")
            raise
