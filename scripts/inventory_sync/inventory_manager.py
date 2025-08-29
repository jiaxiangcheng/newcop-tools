import logging
import time
from typing import List, Dict, Any, Optional, Tuple
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
    InventoryChangeDetection
)
from scripts.inventory_sync.storage import InventoryStorage

logger = logging.getLogger(__name__)

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
    
    def __init__(self, shopify_client: ShopifyClient, storage: Optional[InventoryStorage] = None):
        self.shopify_client = shopify_client
        self.storage = storage or InventoryStorage()
        self.namespace = "custom"
        self.metafield_key = "inventory"
        self.retry_queue = RetryQueue(max_attempts=3)
    
    def sync_inventory_to_metafields(self, dry_run: bool = False) -> SyncResult:
        """
        Main method to sync inventory quantities to variant metafields
        
        Args:
            dry_run: If True, only analyze changes without making updates
            
        Returns:
            SyncResult with detailed statistics and results
        """
        start_time = time.time()
        sync_result = SyncResult(success=False)
        
        try:
            logger.info("🔄 Starting inventory synchronization...")
            
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
            
            # Step 3: Detect inventory changes
            logger.info("🔍 Analyzing inventory changes...")
            changes = self.storage.detect_inventory_changes(current_products, cached_inventory)
            
            # Filter only variants that have changed
            variants_to_update = [change for change in changes if change.has_changed]
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
            
            # Step 4: Update variant metafields (concurrently per product)
            logger.info("⚡ Updating variant metafields...")
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
            
            # Step 5: Update cache with current data
            logger.info("💾 Updating inventory cache...")
            updated_cache = self.storage.update_cache_with_current_data(current_products, cached_inventory)
            cache_saved = self.storage.save_cache(updated_cache)
            
            if not cache_saved:
                sync_result.errors.append("Failed to save updated cache")
            
            # Final result
            sync_result.success = successful_updates > 0 or len(variants_to_update) == 0
            sync_result.execution_time_seconds = time.time() - start_time
            
            # Log detailed summary of successful updates
            if sync_result.variants_updated > 0:
                logger.info("📋 Successfully updated variants summary:")
                successful_updates = [u for u in updated_variants if u]
                for i, update in enumerate(successful_updates, 1):
                    # Find the corresponding change for product title
                    matching_change = None
                    for change in variants_to_update:
                        if change.variant_id == update.variant_id:
                            matching_change = change
                            break
                    
                    product_title = matching_change.product_title if matching_change else "Unknown Product"
                    logger.info(f"  [{i:2d}] 📦 {product_title}")
                    logger.info(f"       🔸 Variant ID: {update.variant_id}")
                    logger.info(f"       📊 Inventory: {update.old_quantity} → {update.new_quantity}")
                    logger.info(f"       🏷️  Metafield: {update.metafield_namespace}.{update.metafield_key}")
            
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
    
    def _update_variants_with_concurrency_and_retry(self, variants_to_update: List[InventoryChangeDetection]) -> List[Optional[VariantUpdate]]:
        """Update variants with concurrent processing and retry mechanism"""
        all_updates = []
        
        # First, process any pending retries from previous runs
        ready_retries = self.retry_queue.get_ready_retries()
        if ready_retries:
            logger.info(f"🔄 Processing {len(ready_retries)} pending retries first...")
            retry_variants = [failed_update.variant_change for failed_update in ready_retries]
            retry_updates = self._update_variants_with_concurrency(retry_variants, is_retry=True)
            all_updates.extend(retry_updates)
        
        # Then process new variants
        if variants_to_update:
            new_updates = self._update_variants_with_concurrency(variants_to_update, is_retry=False)
            all_updates.extend(new_updates)
        
        return all_updates
    
    def _update_variants_with_concurrency(self, variants_to_update: List[InventoryChangeDetection], is_retry: bool = False) -> List[Optional[VariantUpdate]]:
        """Update variants with concurrent processing per product"""
        all_updates = []
        
        # Group variants by product for concurrent processing within each product
        products_to_update = {}
        for variant_change in variants_to_update:
            product_id = variant_change.product_id
            if product_id not in products_to_update:
                products_to_update[product_id] = []
            products_to_update[product_id].append(variant_change)
        
        logger.info(f"🔄 Processing {len(products_to_update)} products with inventory changes...")
        
        # Process each product's variants (products sequentially, variants within product concurrently)
        for i, (product_id, product_variants) in enumerate(products_to_update.items(), 1):
            product_title = product_variants[0].product_title if product_variants else "Unknown"
            retry_prefix = "🔄 [RETRY] " if is_retry else ""
            logger.info(f"  📦 {retry_prefix}[{i}/{len(products_to_update)}] Updating {len(product_variants)} variants in '{product_title}'...")
            
            # Update this product's variants concurrently
            product_updates = self._update_product_variants_concurrently(product_variants, is_retry)
            all_updates.extend(product_updates)
            
            # Small delay between products to avoid overwhelming the API
            if i < len(products_to_update):  # Don't delay after the last product
                time.sleep(0.5)
        
        return all_updates
    
    def _update_product_variants_concurrently(self, variants: List[InventoryChangeDetection], is_retry: bool = False) -> List[Optional[VariantUpdate]]:
        """Update variants for a single product concurrently"""
        updates = []
        
        # Use ThreadPoolExecutor for concurrent variant updates within this product
        max_workers = min(len(variants), 5)  # Limit concurrent requests per product
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all variant update tasks
            future_to_variant = {}
            for variant_change in variants:
                future = executor.submit(self._update_single_variant_metafield, variant_change)
                future_to_variant[future] = variant_change
            
            # Collect results as they complete
            for future in as_completed(future_to_variant):
                variant_change = future_to_variant[future]
                try:
                    variant_update, error_message = future.result()
                    updates.append(variant_update)
                    
                    if variant_update:
                        logger.info(f"    ✅ Updated variant {variant_change.variant_id}: {variant_change.change_description}")
                        logger.debug(f"         Product: {variant_change.product_title}")
                        logger.debug(f"         Metafield: {self.namespace}.{self.metafield_key} = {variant_change.new_quantity}")
                    else:
                        logger.warning(f"    ❌ Failed to update variant {variant_change.variant_id}: {error_message or 'Unknown error'}")
                        logger.warning(f"         Product: {variant_change.product_title}")
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
        logger.info(f"    📊 Product result: {successful_updates}/{len(variants)} variants updated successfully")
        
        return updates
    
    def _update_single_variant_metafield(self, variant_change: InventoryChangeDetection) -> Tuple[Optional[VariantUpdate], Optional[str]]:
        """Update metafield for a single variant"""
        try:
            success = self.shopify_client.update_variant_metafield(
                variant_id=variant_change.variant_id,
                namespace=self.namespace,
                key=self.metafield_key,
                value=str(variant_change.new_quantity)
            )
            
            if success:
                return VariantUpdate(
                    variant_id=variant_change.variant_id,
                    product_id=variant_change.product_id,
                    old_quantity=variant_change.old_quantity,
                    new_quantity=variant_change.new_quantity,
                    metafield_namespace=self.namespace,
                    metafield_key=self.metafield_key
                ), None
            else:
                return None, "Metafield update returned False"
                
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Exception updating variant {variant_change.variant_id} metafield: {error_msg}")
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
                    "metafield_key": self.metafield_key,
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
                parts.append(f"Price: ${variant['price']}")
            
            # Add current inventory quantity
            if 'inventory_quantity' in variant:
                parts.append(f"Current Qty: {variant['inventory_quantity']}")
            
            return " | ".join(parts) if parts else f"ID: {change.variant_id}"
                
        except Exception as e:
            logger.debug(f"Could not get variant display info for {change.variant_id}: {e}")
            return f"ID: {change.variant_id} (error getting details)"