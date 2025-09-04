import logging
import time
from typing import List, Dict, Any, Optional, Tuple, Set
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures
from collections import deque
import random

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from shared.shopify_client import ShopifyClient
from scripts.inventory_sync.models import (
    InventoryCache, 
    SyncResult, 
    VariantUpdate, 
    InventoryChangeDetection,
    FlexibleSyncConfig,
    SyncField,
    SyncMode
)
from scripts.inventory_sync.storage import InventoryStorage

logger = logging.getLogger(__name__)

# Ensure this logger inherits from the parent logger and shows INFO messages
logger.setLevel(logging.INFO)

class FailedVariantUpdate:
    """Represents a failed variant update for retry"""
    def __init__(self, variant_change: 'InventoryChangeDetection', attempt: int = 1, error: str = ""):
        self.variant_change = variant_change
        self.attempt = attempt
        self.error = error
        self.last_attempt_time = time.time()
    
    def should_retry(self, max_attempts: int = 3) -> bool:
        """Check if this update should be retried"""
        return self.attempt < max_attempts
    
    def get_retry_delay(self) -> float:
        """Get the delay before next retry with exponential backoff"""
        base_delay = 2.0  # Base delay in seconds
        max_delay = 60.0  # Maximum delay in seconds
        
        # Exponential backoff with jitter
        delay = min(base_delay * (2 ** (self.attempt - 1)), max_delay)
        jitter = random.uniform(0.8, 1.2)  # Add some randomness
        
        return delay * jitter

class RetryQueue:
    """Queue for managing failed variant updates with retry logic"""
    def __init__(self, max_attempts: int = 3):
        self.queue = deque()
        self.max_attempts = max_attempts
        
    def add_failed_update(self, variant_change: 'InventoryChangeDetection', error: str = ""):
        """Add a failed update to the retry queue"""
        failed_update = FailedVariantUpdate(variant_change, attempt=1, error=error)
        self.queue.append(failed_update)
        logger.debug(f"Added variant {variant_change.variant_id} to retry queue (error: {error[:100]})")
    
    def get_ready_retries(self) -> List[FailedVariantUpdate]:
        """Get all failed updates that are ready for retry"""
        ready_retries = []
        remaining_queue = deque()
        
        current_time = time.time()
        
        while self.queue:
            failed_update = self.queue.popleft()
            
            # Check if enough time has passed for retry
            time_since_last_attempt = current_time - failed_update.last_attempt_time
            required_delay = failed_update.get_retry_delay()
            
            if time_since_last_attempt >= required_delay:
                if failed_update.should_retry(self.max_attempts):
                    failed_update.attempt += 1
                    failed_update.last_attempt_time = current_time
                    ready_retries.append(failed_update)
                else:
                    logger.warning(f"Variant {failed_update.variant_change.variant_id} exceeded max attempts ({self.max_attempts}), giving up")
            else:
                # Not ready for retry yet, keep in queue
                remaining_queue.append(failed_update)
        
        # Put back items not ready for retry
        self.queue = remaining_queue
        
        if ready_retries:
            logger.info(f"🔄 Found {len(ready_retries)} variants ready for retry")
        
        return ready_retries
    
    def size(self) -> int:
        """Get the current size of the retry queue"""
        return len(self.queue)
    
    def clear(self):
        """Clear the retry queue"""
        self.queue.clear()

class InventoryManager:
    """Core manager for inventory synchronization with Shopify"""
    
    def __init__(self, shopify_client: ShopifyClient, storage: Optional[InventoryStorage] = None, 
                 sync_config: Optional[FlexibleSyncConfig] = None):
        self.shopify_client = shopify_client
        self.storage = storage or InventoryStorage()
        self.sync_config = sync_config or FlexibleSyncConfig()
        self.namespace = "custom"
        # Define metafield keys for different data
        self.inventory_key = "inventory"
        self.price_key = "price"
        self.compare_price_key = "compare_price"
        self.retry_queue = RetryQueue(max_attempts=3)
        self.rate_limit_count = 0  # Track rate limit hits for summary
    
    def sync_fields_to_metafields(self, target_fields: Optional[Set[SyncField]] = None, 
                                 dry_run: Optional[bool] = None, batch_size: Optional[int] = None) -> SyncResult:
        """
        Flexible method to sync specific fields to variant metafields
        
        Args:
            target_fields: Set of fields to sync. If None, uses fields enabled in config
            dry_run: If True, only analyze changes without making updates. Uses config if None
            batch_size: Number of products to process before saving cache. Uses config if None
            
        Returns:
            SyncResult with detailed statistics and results
        """
        # Use configuration defaults if not provided
        enabled_fields = target_fields or self.sync_config.get_enabled_fields()
        sync_dry_run = dry_run if dry_run is not None else self.sync_config.dry_run
        sync_batch_size = batch_size or self.sync_config.batch_size
        
        if not enabled_fields:
            logger.warning("No fields are enabled for synchronization!")
            result = SyncResult(success=True)
            result.execution_time_seconds = 0.0
            return result
        
        logger.info(f"🎯 Starting flexible sync for fields: {', '.join([f.value for f in enabled_fields])}")
        
        start_time = time.time()
        sync_result = SyncResult(success=False)
        
        try:
            logger.info(f"🔧 Sync Configuration:")
            logger.info(f"  - Target fields: {', '.join([f.value for f in enabled_fields])}")
            logger.info(f"  - Dry run mode: {sync_dry_run}")
            logger.info(f"  - Batch size: {sync_batch_size} products")
            
            # Step 1: Load existing cache
            logger.info("📂 Loading inventory cache...")
            cached_inventory = self.storage.load_cache()
            
            # Step 2: Fetch current products and variants from Shopify
            logger.info("🏪 Fetching active products from Shopify...")
            current_products = self.shopify_client.get_all_active_products_with_variants()
            sync_result.total_products_processed = len(current_products)
            
            # Count total variants
            total_variants = sum(len(product.get('variants', [])) for product in current_products)
            sync_result.total_variants_checked = total_variants
            logger.info(f"📦 Found {len(current_products)} products with {total_variants} variants")
            
            # Step 3: Process products in batches with progressive cache updates
            logger.info("🔍 Analyzing changes for enabled fields...")
            all_changes = []
            processed_products = 0
            
            # Process products in batches
            for batch_start in range(0, len(current_products), sync_batch_size):
                batch_end = min(batch_start + sync_batch_size, len(current_products))
                product_batch = current_products[batch_start:batch_end]
                
                logger.info(f"📊 Processing batch {batch_start//sync_batch_size + 1}/{(len(current_products) + sync_batch_size - 1)//sync_batch_size}: products {batch_start + 1}-{batch_end}")
                
                # Detect changes for this batch
                batch_changes = self.storage.detect_inventory_changes(product_batch, cached_inventory)
                
                # Filter changes to only include enabled fields
                filtered_batch_changes = self._filter_changes_for_enabled_fields(batch_changes, enabled_fields)
                all_changes.extend(filtered_batch_changes)
                
                # Update cache with this batch
                cached_inventory = self.storage.update_cache_with_product_batch(product_batch, cached_inventory)
                
                # Save cache after each batch
                cache_saved = self.storage.save_cache(cached_inventory)
                if cache_saved:
                    logger.info(f"💾 Cache saved after processing {batch_end} products")
                else:
                    logger.warning(f"⚠️  Failed to save cache after batch {batch_start//sync_batch_size + 1}")
                
                processed_products += len(product_batch)
                
                # Small delay between batches to avoid overwhelming the system
                if batch_end < len(current_products):
                    time.sleep(1.0)
            
            # Step 4: Analyze all changes
            variants_to_update = [change for change in all_changes if change.has_changed]
            sync_result.variants_updated = len(variants_to_update) if not sync_dry_run else 0
            sync_result.products_with_changes = len(set(change.product_id for change in variants_to_update))
            
            logger.info(f"📊 Found {len(variants_to_update)} variants with changes for enabled fields in {sync_result.products_with_changes} products")
            
            if not variants_to_update:
                logger.info("✅ No changes detected for enabled fields - nothing to update")
                sync_result.success = True
                sync_result.execution_time_seconds = time.time() - start_time
                return sync_result
            
            # Log summary of changes instead of detailed list
            self._log_changes_summary(variants_to_update, enabled_fields)
            
            if sync_dry_run:
                logger.info("🧪 DRY RUN: Would update metafields for variants with changes")
                sync_result.success = True
                sync_result.execution_time_seconds = time.time() - start_time
                return sync_result
            
            # Step 5: Update variant metafields for enabled fields only
            logger.info(f"⚡ Updating variant metafields for enabled fields: {', '.join([f.value for f in enabled_fields])}")
            updated_variants = self._update_variants_with_concurrency_and_retry(variants_to_update, enabled_fields)
            
            # Update sync result with actual updates
            sync_result.updated_variants = updated_variants
            successful_updates = len([u for u in updated_variants if u])
            failed_updates = len(updated_variants) - successful_updates
            
            sync_result.variants_updated = successful_updates
            sync_result.variants_failed = failed_updates
            
            # Log retry queue status
            if self.retry_queue.size() > 0:
                logger.warning(f"⚠️  {self.retry_queue.size()} variants remain in retry queue for future attempts")
            
            # Step 6: Final cache save
            logger.info("💾 Ensuring final cache state is saved...")
            cache_saved = self.storage.save_cache(cached_inventory)
            
            if not cache_saved:
                sync_result.errors.append("Failed to save final cache state")
            
            # Final result
            sync_result.success = successful_updates > 0 or len(variants_to_update) == 0
            sync_result.execution_time_seconds = time.time() - start_time
            
            # Log concise summary of successful updates
            if sync_result.variants_updated > 0:
                self._log_successful_updates_summary(updated_variants, enabled_fields)
            
            # Log summary
            logger.info("📋 Flexible sync completed!")
            logger.info(f"  ✅ Variants updated: {sync_result.variants_updated}")
            logger.info(f"  ❌ Variants failed: {sync_result.variants_failed}")
            logger.info(f"  🎯 Fields synced: {', '.join([f.value for f in enabled_fields])}")
            logger.info(f"  ⏱️  Execution time: {sync_result.execution_time_seconds:.2f} seconds")
            
            return sync_result
            
        except Exception as e:
            logger.error(f"💥 Flexible sync failed: {e}")
            sync_result.success = False
            sync_result.errors.append(str(e))
            sync_result.execution_time_seconds = time.time() - start_time
            return sync_result
    
    def sync_inventory_to_metafields(self, dry_run: bool = False, batch_size: int = 50) -> SyncResult:
        """
        Main method to sync inventory quantities to variant metafields with progressive cache saves
        
        Args:
            dry_run: If True, only analyze changes without making updates
            batch_size: Number of products to process before saving cache (default: 50)
            
        Returns:
            SyncResult with detailed statistics and results
        """
        start_time = time.time()
        sync_result = SyncResult(success=False)
        
        try:
            logger.info("🔄 Starting inventory synchronization...")
            logger.info(f"📦 Using batch size of {batch_size} products for progressive cache saves")
            
            # Step 1: Load existing cache
            logger.info("📂 Loading inventory cache...")
            cached_inventory = self.storage.load_cache()
            
            # Step 2: Fetch current products and variants from Shopify
            logger.info("🏪 Fetching active products from Shopify...")
            current_products = self.shopify_client.get_all_active_products_with_variants()
            sync_result.total_products_processed = len(current_products)
            
            # Count total variants
            total_variants = sum(len(product.get('variants', [])) for product in current_products)
            sync_result.total_variants_checked = total_variants
            logger.info(f"📦 Found {len(current_products)} products with {total_variants} variants")
            
            # Step 3: Process products in batches with progressive cache updates
            logger.info("🔍 Analyzing inventory changes and updating cache progressively...")
            all_changes = []
            processed_products = 0
            
            # Process products in batches
            for batch_start in range(0, len(current_products), batch_size):
                batch_end = min(batch_start + batch_size, len(current_products))
                product_batch = current_products[batch_start:batch_end]
                
                logger.info(f"📊 Processing batch {batch_start//batch_size + 1}/{(len(current_products) + batch_size - 1)//batch_size}: products {batch_start + 1}-{batch_end}")
                
                # Detect changes for this batch
                batch_changes = self.storage.detect_inventory_changes(product_batch, cached_inventory)
                all_changes.extend(batch_changes)
                
                # Update cache with this batch
                cached_inventory = self.storage.update_cache_with_product_batch(product_batch, cached_inventory)
                
                # Save cache after each batch
                cache_saved = self.storage.save_cache(cached_inventory)
                if cache_saved:
                    logger.info(f"💾 Cache saved after processing {batch_end} products")
                else:
                    logger.warning(f"⚠️  Failed to save cache after batch {batch_start//batch_size + 1}")
                
                processed_products += len(product_batch)
                
                # Small delay between batches to avoid overwhelming the system
                if batch_end < len(current_products):
                    time.sleep(1.0)
            
            # Step 4: Analyze all changes
            variants_to_update = [change for change in all_changes if change.has_changed]
            sync_result.variants_updated = len(variants_to_update) if not dry_run else 0
            sync_result.products_with_changes = len(set(change.product_id for change in variants_to_update))
            
            logger.info(f"📊 Found {len(variants_to_update)} variants with inventory changes in {sync_result.products_with_changes} products")
            
            if not variants_to_update:
                logger.info("✅ No inventory changes detected - nothing to update")
                sync_result.success = True
                sync_result.execution_time_seconds = time.time() - start_time
                return sync_result
            
            # Log all inventory changes with detailed information
            logger.info("📋 Detailed inventory changes:")
            for i, change in enumerate(variants_to_update, 1):
                # Get additional variant details for better logging
                variant_info = self._get_variant_display_info_from_products(change, current_products)
                logger.info(f"  [{i:3d}] 📦 {change.product_title}")
                logger.info(f"       🏷️  Variant: {variant_info}")
                logger.info(f"       📊 Change: {change.change_description}")
                logger.info(f"       🆔 IDs: Product={change.product_id}, Variant={change.variant_id}")
                if i < len(variants_to_update):  # Add separator except for last item
                    logger.info("       " + "-" * 50)
            
            if dry_run:
                logger.info("🧪 DRY RUN: Would update metafields for variants with changes")
                sync_result.success = True
                sync_result.execution_time_seconds = time.time() - start_time
                return sync_result
            
            # Step 4: Update variant metafields for inventory, price, and compare_price (concurrently per product)
            logger.info("⚡ Updating variant metafields for inventory, price, and compare_price...")
            updated_variants = self._update_variants_with_concurrency_and_retry(variants_to_update)
            
            # Update sync result with actual updates
            sync_result.updated_variants = updated_variants
            successful_updates = len([u for u in updated_variants if u])
            failed_updates = len(updated_variants) - successful_updates
            
            sync_result.variants_updated = successful_updates
            sync_result.variants_failed = failed_updates
            
            # Log retry queue status
            if self.retry_queue.size() > 0:
                logger.warning(f"⚠️  {self.retry_queue.size()} variants remain in retry queue for future attempts")
            
            # Step 5: Final cache save (already done incrementally, but ensure final state is saved)
            logger.info("💾 Ensuring final cache state is saved...")
            # Cache was already updated during batch processing, just need a final save to be sure
            cache_saved = self.storage.save_cache(cached_inventory)
            
            if not cache_saved:
                sync_result.errors.append("Failed to save final cache state")
            
            # Final result
            sync_result.success = successful_updates > 0 or len(variants_to_update) == 0
            sync_result.execution_time_seconds = time.time() - start_time
            
            # Log detailed summary of successful updates
            if sync_result.variants_updated > 0:
                logger.info("📋 Successfully updated variants summary:")
                successful_updates = [u for u in updated_variants if u]
                for i, update in enumerate(successful_updates, 1):
                    # Find the corresponding change for product title and variant info
                    matching_change = None
                    for change in variants_to_update:
                        if change.variant_id == update.variant_id:
                            matching_change = change
                            break
                    
                    product_title = matching_change.product_title if matching_change else "Unknown Product"
                    
                    # Build variant info
                    variant_info_parts = []
                    if matching_change and matching_change.variant_title and matching_change.variant_title != "Default Title":
                        variant_info_parts.append(f"'{matching_change.variant_title}'")
                    variant_info_parts.append(f"ID: {update.variant_id}")
                    variant_info = " - ".join(variant_info_parts)
                    
                    logger.info(f"  [{i:2d}] 📦 {product_title}")
                    logger.info(f"       🔸 Variant ID: {update.variant_id}")
                    
                    # Show details for each field that was updated
                    for field in update.updated_fields:
                        if field == 'inventory':
                            logger.info(f"       📊 Inventory: {update.old_quantity} → {update.new_quantity}")
                        elif field == 'price':
                            logger.info(f"       💰 Price: €{update.old_price} → €{update.new_price}")
                        elif field == 'compare_at_price':
                            logger.info(f"       🏷️  Compare Price: €{update.old_compare_at_price} → €{update.new_compare_at_price}")
                    
                    logger.info(f"       🏷️  Updated metafields: {', '.join([f'{self.namespace}.{field}' for field in update.updated_fields])}")
            
            # Log summary
            logger.info("📋 Inventory sync completed!")
            logger.info(f"  ✅ Variants updated: {sync_result.variants_updated}")
            logger.info(f"  ❌ Variants failed: {sync_result.variants_failed}")
            logger.info(f"  ⏱️  Execution time: {sync_result.execution_time_seconds:.2f} seconds")
            
            return sync_result
            
        except Exception as e:
            logger.error(f"💥 Inventory sync failed: {e}")
            sync_result.success = False
            sync_result.errors.append(str(e))
            sync_result.execution_time_seconds = time.time() - start_time
            return sync_result
    
    def _filter_changes_for_enabled_fields(self, changes: List[InventoryChangeDetection], 
                                          enabled_fields: Set[SyncField]) -> List[InventoryChangeDetection]:
        """Filter inventory changes to only include enabled fields"""
        filtered_changes = []
        
        for change in changes:
            # Filter the changed_fields to only include enabled ones
            filtered_changed_fields = [
                field for field in change.changed_fields 
                if SyncField(field) in enabled_fields
            ]
            
            if filtered_changed_fields:
                # Create a new change object with only the enabled fields
                filtered_change = InventoryChangeDetection(
                    variant_id=change.variant_id,
                    product_id=change.product_id,
                    product_title=change.product_title,
                    variant_title=change.variant_title,
                    old_quantity=change.old_quantity,
                    new_quantity=change.new_quantity,
                    old_price=change.old_price,
                    new_price=change.new_price,
                    old_compare_at_price=change.old_compare_at_price,
                    new_compare_at_price=change.new_compare_at_price,
                    has_changed=True,  # Has changes in enabled fields
                    changed_fields=filtered_changed_fields
                )
                filtered_changes.append(filtered_change)
            else:
                # No changes in enabled fields, add with has_changed=False
                no_change = InventoryChangeDetection(
                    variant_id=change.variant_id,
                    product_id=change.product_id,
                    product_title=change.product_title,
                    variant_title=change.variant_title,
                    old_quantity=change.old_quantity,
                    new_quantity=change.new_quantity,
                    old_price=change.old_price,
                    new_price=change.new_price,
                    old_compare_at_price=change.old_compare_at_price,
                    new_compare_at_price=change.new_compare_at_price,
                    has_changed=False,
                    changed_fields=[]
                )
                filtered_changes.append(no_change)
        
        return filtered_changes
    
    def _log_changes_summary(self, variants_to_update: List[InventoryChangeDetection], 
                            enabled_fields: Set[SyncField]):
        """Log concise summary of field changes"""
        field_counts = {}
        product_counts = len(set(change.product_id for change in variants_to_update))
        
        for change in variants_to_update:
            for field in change.changed_fields:
                if SyncField(field) in enabled_fields:
                    field_counts[field] = field_counts.get(field, 0) + 1
        
        logger.info("📋 Changes summary:")
        logger.info(f"  - Products with changes: {product_counts}")
        logger.info(f"  - Total variants to update: {len(variants_to_update)}")
        
        if field_counts:
            changes_str = []
            for field, count in field_counts.items():
                changes_str.append(f"{field}: {count}")
            logger.info(f"  - Field changes: {', '.join(changes_str)}")
        
        # Show a few examples
        if len(variants_to_update) > 0:
            logger.info("  - Examples:")
            for i, change in enumerate(variants_to_update[:3], 1):
                field_changes = []
                for field in change.changed_fields:
                    if SyncField(field) in enabled_fields:
                        if field == 'inventory':
                            field_changes.append(f"qty:{change.old_quantity}→{change.new_quantity}")
                        elif field == 'price':
                            field_changes.append(f"€{change.old_price}→€{change.new_price}")
                        elif field == 'compare_price':
                            field_changes.append(f"comp:€{change.old_compare_at_price}→€{change.new_compare_at_price}")
                
                logger.info(f"    [{i}] {change.product_title}: {', '.join(field_changes)}")
            
            if len(variants_to_update) > 3:
                logger.info(f"    ... and {len(variants_to_update) - 3} more variants")
    
    def _log_successful_updates_summary(self, updated_variants: List[Optional[VariantUpdate]], 
                                       enabled_fields: Set[SyncField]):
        """Log concise summary of successful updates"""
        successful_updates = [u for u in updated_variants if u]
        
        if successful_updates:
            # Count updates by field
            field_update_counts = {}
            for update in successful_updates:
                for field in update.updated_fields:
                    if SyncField(field) in enabled_fields:
                        field_update_counts[field] = field_update_counts.get(field, 0) + 1
            
            logger.info("📋 Update results:")
            logger.info(f"  - Total variants updated: {len(successful_updates)}")
            
            if field_update_counts:
                updates_str = []
                for field, count in field_update_counts.items():
                    updates_str.append(f"{field}: {count}")
                logger.info(f"  - Field updates: {', '.join(updates_str)}")
            
            # Show just a few examples
            logger.info("  - Sample updates:")
            for i, update in enumerate(successful_updates[:3], 1):
                enabled_updated_fields = [f for f in update.updated_fields if SyncField(f) in enabled_fields]
                metafields_str = ', '.join([f'{self.namespace}.{field}' for field in enabled_updated_fields])
                logger.info(f"    [{i}] Variant {update.variant_id}: {metafields_str}")
            
            if len(successful_updates) > 3:
                logger.info(f"    ... and {len(successful_updates) - 3} more variants")

    def _update_variants_with_concurrency_and_retry(self, variants_to_update: List[InventoryChangeDetection], 
                                                   enabled_fields: Optional[Set[SyncField]] = None) -> List[Optional[VariantUpdate]]:
        """Update variants with concurrent processing and retry mechanism for specific fields"""
        all_updates = []
        
        # Use all fields if none specified (for backward compatibility)
        if enabled_fields is None:
            enabled_fields = {SyncField.INVENTORY, SyncField.PRICE, SyncField.COMPARE_PRICE}
        
        # First, process any pending retries from previous runs
        ready_retries = self.retry_queue.get_ready_retries()
        if ready_retries:
            logger.info(f"🔄 Processing {len(ready_retries)} pending retries first...")
            retry_variants = [failed_update.variant_change for failed_update in ready_retries]
            retry_updates = self._update_variants_with_concurrency(retry_variants, enabled_fields, is_retry=True)
            all_updates.extend(retry_updates)
        
        # Then process new variants
        if variants_to_update:
            new_updates = self._update_variants_with_concurrency(variants_to_update, enabled_fields, is_retry=False)
            all_updates.extend(new_updates)
        
        return all_updates
    
    def _update_variants_with_concurrency(self, variants_to_update: List[InventoryChangeDetection], 
                                         enabled_fields: Set[SyncField], is_retry: bool = False) -> List[Optional[VariantUpdate]]:
        """Update variants with concurrent processing per product"""
        all_updates = []
        
        # Group variants by product for concurrent processing within each product
        products_to_update = {}
        for variant_change in variants_to_update:
            product_id = variant_change.product_id
            if product_id not in products_to_update:
                products_to_update[product_id] = []
            products_to_update[product_id].append(variant_change)
        
        logger.info(f"🔄 Updating variants in {len(products_to_update)} products...")
        
        # Process each product's variants (products sequentially, variants within product concurrently)
        for i, (product_id, product_variants) in enumerate(products_to_update.items(), 1):
            product_title = product_variants[0].product_title if product_variants else "Unknown"
            
            # Only log every 5th product or if there are few products
            if len(products_to_update) <= 5 or i % 5 == 0 or i == len(products_to_update):
                retry_prefix = "🔄 [RETRY] " if is_retry else ""
                logger.info(f"  📦 {retry_prefix}[{i}/{len(products_to_update)}] Processing '{product_title}' ({len(product_variants)} variants)")
            
            # Update this product's variants concurrently
            product_updates = self._update_product_variants_concurrently(product_variants, enabled_fields, is_retry)
            all_updates.extend(product_updates)
            
            # Increased delay between products to avoid overwhelming the API
            if i < len(products_to_update):  # Don't delay after the last product
                time.sleep(1.0)  # Increased from 0.5 to 1.0 seconds
        
        return all_updates
    
    def _update_product_variants_concurrently(self, variants: List[InventoryChangeDetection], 
                                             enabled_fields: Set[SyncField], is_retry: bool = False) -> List[Optional[VariantUpdate]]:
        """Update variants for a single product concurrently"""
        updates = []
        
        # Use ThreadPoolExecutor for concurrent variant updates within this product
        max_workers = min(len(variants), 2)  # Reduced concurrency to avoid rate limits
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all variant update tasks
            future_to_variant = {}
            for variant_change in variants:
                future = executor.submit(self._update_single_variant_metafield, variant_change, enabled_fields)
                future_to_variant[future] = variant_change
            
            # Collect results as they complete
            for future in as_completed(future_to_variant):
                variant_change = future_to_variant[future]
                try:
                    variant_update, error_message = future.result()
                    updates.append(variant_update)
                    
                    if variant_update:
                        logger.debug(f"    ✅ Updated variant {variant_change.variant_id}: {variant_change.change_description}")
                    else:
                        # Build detailed variant info for failed updates
                        variant_info_parts = []
                        if variant_change.variant_title and variant_change.variant_title != "Default Title":
                            variant_info_parts.append(f"'{variant_change.variant_title}'")
                        variant_info_parts.append(f"ID: {variant_change.variant_id}")
                        
                        variant_info = " - ".join(variant_info_parts)
                        logger.warning(f"    ❌ Failed to update variant {variant_info}: {error_message or 'Unknown error'}")
                        logger.warning(f"         📦 Product: {variant_change.product_title}")
                        # Add to retry queue if not already a retry
                        if not is_retry:
                            self.retry_queue.add_failed_update(variant_change, error_message or "Unknown error")
                        
                except Exception as e:
                    logger.error(f"    💥 Exception updating variant {variant_change.variant_id}: {e}")
                    updates.append(None)
                    # Add to retry queue if not already a retry
                    if not is_retry:
                        self.retry_queue.add_failed_update(variant_change, str(e))
        
        successful_updates = len([u for u in updates if u])
        if successful_updates < len(variants):
            logger.info(f"    📊 Product result: {successful_updates}/{len(variants)} variants updated successfully")
        else:
            logger.debug(f"    📊 Product result: {successful_updates}/{len(variants)} variants updated successfully")
        
        return updates
    
    def _update_single_variant_metafield(self, variant_change: InventoryChangeDetection, 
                                        enabled_fields: Set[SyncField]) -> Tuple[Optional[VariantUpdate], Optional[str]]:
        """Update metafields for a single variant (inventory, price, compare_price)"""
        updated_fields = []
        errors = []
        
        try:
            # Update each changed field, but only if it's in the enabled fields
            for field in variant_change.changed_fields:
                # Skip this field if it's not enabled for sync
                if SyncField(field) not in enabled_fields:
                    continue
                    
                field_success = False
                
                if field == 'inventory' and SyncField.INVENTORY in enabled_fields:
                    field_success = self.shopify_client.update_variant_metafield(
                        variant_id=variant_change.variant_id,
                        namespace=self.namespace,
                        key=self.inventory_key,
                        value=str(variant_change.new_quantity)
                    )
                elif field == 'price' and SyncField.PRICE in enabled_fields:
                    field_success = self.shopify_client.update_variant_metafield(
                        variant_id=variant_change.variant_id,
                        namespace=self.namespace,
                        key=self.price_key,
                        value=str(variant_change.new_price) if variant_change.new_price else "0.00"
                    )
                elif field == 'compare_price' and SyncField.COMPARE_PRICE in enabled_fields:
                    field_success = self.shopify_client.update_variant_metafield(
                        variant_id=variant_change.variant_id,
                        namespace=self.namespace,
                        key=self.compare_price_key,
                        value=str(variant_change.new_compare_at_price) if variant_change.new_compare_at_price else "0.00"
                    )
                
                if field_success:
                    updated_fields.append(field)
                else:
                    errors.append(f"{field} update failed")
            
            if updated_fields:
                return VariantUpdate(
                    variant_id=variant_change.variant_id,
                    product_id=variant_change.product_id,
                    old_quantity=variant_change.old_quantity,
                    new_quantity=variant_change.new_quantity,
                    old_price=variant_change.old_price,
                    new_price=variant_change.new_price,
                    old_compare_at_price=variant_change.old_compare_at_price,
                    new_compare_at_price=variant_change.new_compare_at_price,
                    metafield_namespace=self.namespace,
                    metafield_key=self.inventory_key,  # Keep for backward compatibility
                    updated_fields=updated_fields
                ), None
            else:
                error_msg = "; ".join(errors) if errors else "All metafield updates failed"
                return None, error_msg
                
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Exception updating variant {variant_change.variant_id} metafields: {error_msg}")
            return None, error_msg
    
    def get_sync_status(self) -> Dict[str, Any]:
        """Get current synchronization status and cache statistics"""
        try:
            cache_stats = self.storage.get_cache_stats()
            
            # Try to get a quick count of active products
            try:
                current_products = self.shopify_client.get_all_active_products_with_variants()
                current_product_count = len(current_products)
                current_variant_count = sum(len(p.get('variants', [])) for p in current_products)
            except Exception as e:
                logger.warning(f"Could not fetch current product count: {e}")
                current_product_count = "Unknown"
                current_variant_count = "Unknown"
            
            return {
                "cache_status": cache_stats,
                "current_shopify_products": current_product_count,
                "current_shopify_variants": current_variant_count,
                "retry_queue_size": self.retry_queue.size(),
                "sync_configuration": {
                    "metafield_namespace": self.namespace,
                    "inventory_key": self.inventory_key,
                    "price_key": self.price_key,
                    "compare_price_key": self.compare_price_key,
                    "max_retry_attempts": self.retry_queue.max_attempts
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting sync status: {e}")
            return {"error": str(e)}
    
    def clear_cache(self) -> bool:
        """Clear the local inventory cache"""
        try:
            empty_cache = InventoryCache()
            return self.storage.save_cache(empty_cache)
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")
            return False
    
    def _get_variant_display_info_from_products(self, change: InventoryChangeDetection, current_products: List[Dict[str, Any]]) -> str:
        """Get formatted display information for a variant from already loaded product data"""
        try:
            # Find the product in current_products
            product = None
            for prod in current_products:
                if str(prod.get('id')) == str(change.product_id):
                    product = prod
                    break
            
            if not product:
                return f"ID: {change.variant_id} (product not found)"
            
            # Find the variant in the product's variants
            variant = None
            for var in product.get('variants', []):
                if str(var.get('id')) == str(change.variant_id):
                    variant = var
                    break
            
            if not variant:
                return f"ID: {change.variant_id} (variant not found in product)"
            
            # Build variant description
            parts = []
            
            # Add variant title if it exists and is not "Default Title"
            if variant.get('title') and variant['title'] != 'Default Title':
                parts.append(f"Title: {variant['title']}")
            
            # Add variant options (size, color, etc.)
            if variant.get('option1'):
                parts.append(f"Option1: {variant['option1']}")
            if variant.get('option2'):
                parts.append(f"Option2: {variant['option2']}")
            if variant.get('option3'):
                parts.append(f"Option3: {variant['option3']}")
            
            # Add SKU if available
            if variant.get('sku'):
                parts.append(f"SKU: {variant['sku']}")
            
            # Add price
            if variant.get('price'):
                parts.append(f"Price: €{variant['price']}")
            
            # Add compare at price
            if variant.get('compare_at_price'):
                parts.append(f"Compare Price: €{variant['compare_at_price']}")
            
            # Add current inventory quantity
            if 'inventory_quantity' in variant:
                parts.append(f"Current Qty: {variant['inventory_quantity']}")
            
            return " | ".join(parts) if parts else f"ID: {change.variant_id}"
                
        except Exception as e:
            logger.debug(f"Could not get variant display info for {change.variant_id}: {e}")
            return f"ID: {change.variant_id} (error getting details)"