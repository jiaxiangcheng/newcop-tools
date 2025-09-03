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
from scripts.customer_marketing_sync.models import (
    MarketingCache, 
    MarketingSyncResult, 
    CustomerUpdate, 
    MarketingChangeDetection
)
from scripts.customer_marketing_sync.storage import CustomerMarketingStorage, get_customer_email_marketing_status

logger = logging.getLogger('customer_marketing_sync')

class FailedCustomerUpdate:
    """Represents a failed customer update for retry"""
    def __init__(self, customer_change: 'MarketingChangeDetection', attempt: int = 1, error: str = ""):
        self.customer_change = customer_change
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
    """Queue for managing failed customer updates with retry logic"""
    def __init__(self, max_attempts: int = 3):
        self.queue = deque()
        self.max_attempts = max_attempts
        
    def add_failed_update(self, customer_change: 'MarketingChangeDetection', error: str = ""):
        """Add a failed update to the retry queue"""
        failed_update = FailedCustomerUpdate(customer_change, attempt=1, error=error)
        self.queue.append(failed_update)
        logger.debug(f"Added customer {customer_change.customer_id} to retry queue (error: {error[:100]})")
    
    def get_ready_retries(self) -> List[FailedCustomerUpdate]:
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
                    logger.warning(f"Customer {failed_update.customer_change.customer_id} exceeded max attempts ({self.max_attempts}), giving up")
            else:
                # Not ready for retry yet, keep in queue
                remaining_queue.append(failed_update)
        
        # Put back items not ready for retry
        self.queue = remaining_queue
        
        if ready_retries:
            logger.info(f"🔄 Found {len(ready_retries)} customers ready for retry")
        
        return ready_retries
    
    def size(self) -> int:
        """Get the current size of the retry queue"""
        return len(self.queue)
    
    def clear(self):
        """Clear the retry queue"""
        self.queue.clear()

class CustomerManager:
    """Core manager for customer marketing synchronization with Shopify"""
    
    def __init__(self, shopify_client: ShopifyClient, storage: Optional[CustomerMarketingStorage] = None, create_missing_metafields: bool = False):
        self.shopify_client = shopify_client
        self.storage = storage or CustomerMarketingStorage()
        self.namespace = "custom"
        self.metafield_key = "accepts_marketing"
        self.retry_queue = RetryQueue(max_attempts=3)
        self.create_missing_metafields = create_missing_metafields
    
    def sync_customer_marketing_to_metafields(self, dry_run: bool = False, customer_limit: Optional[int] = None) -> MarketingSyncResult:
        """
        Main method to sync customer marketing preferences to customer metafields
        
        Args:
            dry_run: If True, only analyze changes without making updates
            customer_limit: Optional limit on number of customers to process (for testing/large databases)
            
        Returns:
            MarketingSyncResult with detailed statistics and results
        """

        start_time = time.time()
        sync_result = MarketingSyncResult(success=False)
        
        # Initialize tracking for streaming processing
        self.total_processed = 0
        self.total_updated = 0
        self.total_failed = 0
        self.all_updated_customers = []
        
        try:
            logger.info("🔄 Starting customer marketing synchronization...")
            
            # Load existing cache
            cached_marketing = self.storage.load_cache()
            logger.info(f"📂 Cache loaded: {len(cached_marketing.customers)} customers")
            
            # Streaming customer fetch and processing
            if customer_limit:
                logger.info(f"👥 Processing limited to {customer_limit} customers...")
            else:
                logger.info("👥 Processing all customers...")
            
            current_customers = self.shopify_client.get_all_customers(
                limit=customer_limit, 
                batch_callback=self._process_customer_batch if not dry_run else self._analyze_customer_batch,
                batch_size=250
            )
            
            sync_result.total_customers_processed = len(current_customers)
            logger.info(f"✅ Processed {len(current_customers)} customers")
            
            # Update sync result with streaming processing results
            sync_result.customers_updated = self.total_updated
            sync_result.customers_with_changes = self.total_processed  # Total customers that had changes
            sync_result.customers_failed = self.total_failed
            sync_result.updated_customers = self.all_updated_customers
            
            if self.total_processed == 0:
                logger.info("✅ No marketing preference changes detected")
                sync_result.success = True
                sync_result.execution_time_seconds = time.time() - start_time
                return sync_result
            
            if dry_run:
                logger.info(f"✅ DRY RUN completed - {self.total_processed} changes detected but no updates made")
                sync_result.success = True
                sync_result.execution_time_seconds = time.time() - start_time
                return sync_result
            
            # Update cache with current data
            logger.info("💾 Updating cache...")
            updated_cache = self.storage.update_cache_with_current_data(current_customers, cached_marketing)
            cache_saved = self.storage.save_cache(updated_cache)
            if not cache_saved:
                logger.warning("⚠️  Cache save failed")
            
            if not cache_saved:
                sync_result.errors.append("Failed to save updated cache")
            
            # Final result
            sync_result.success = self.total_updated > 0 or self.total_processed == 0
            sync_result.execution_time_seconds = time.time() - start_time
            
            # Log summary
            logger.info(f"✅ Sync completed: {sync_result.customers_updated}/{sync_result.customers_with_changes} customers updated in {sync_result.execution_time_seconds:.1f}s")
            
            return sync_result
            
        except Exception as e:
            logger.error(f"💥 Customer marketing sync failed: {e}")
            sync_result.success = False
            sync_result.errors.append(str(e))
            sync_result.execution_time_seconds = time.time() - start_time
            return sync_result
    
    def _update_customers_with_concurrency_and_retry(self, customers_to_update: List[MarketingChangeDetection]) -> List[Optional[CustomerUpdate]]:
        """Update customers with concurrent processing and retry mechanism"""
        all_updates = []
        
        # First, process any pending retries from previous runs
        ready_retries = self.retry_queue.get_ready_retries()
        if ready_retries:
            logger.info(f"🔄 Processing {len(ready_retries)} pending retries first...")
            retry_customers = [failed_update.customer_change for failed_update in ready_retries]
            retry_updates = self._update_customers_with_concurrency(retry_customers, is_retry=True)
            all_updates.extend(retry_updates)
        
        # Then process new customers
        if customers_to_update:
            new_updates = self._update_customers_with_concurrency(customers_to_update, is_retry=False)
            all_updates.extend(new_updates)
        
        return all_updates
    
    def _update_customers_with_concurrency(self, customers_to_update: List[MarketingChangeDetection], is_retry: bool = False) -> List[Optional[CustomerUpdate]]:
        """Update customers with concurrent processing"""
        all_updates = []
        
        
        # Use ThreadPoolExecutor for concurrent customer updates (reduced concurrency to avoid 429 errors)
        max_workers = min(len(customers_to_update), 3)  # Reduced from 10 to 3 to minimize rate limiting
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all customer update tasks
            future_to_customer = {}
            for customer_change in customers_to_update:
                future = executor.submit(self._update_single_customer_metafield, customer_change)
                future_to_customer[future] = customer_change
            
            # Collect results as they complete
            for i, future in enumerate(as_completed(future_to_customer), 1):
                customer_change = future_to_customer[future]
                try:
                    customer_update, error_message = future.result()
                    all_updates.append(customer_update)
                    
                    if not customer_update and error_message:
                        if "does not have metafield" not in (error_message or ""):
                            logger.error(f"❌ Failed to update {customer_change.customer_display_name}: {error_message}")
                            # Add to retry queue if not already a retry
                            if not is_retry:
                                self.retry_queue.add_failed_update(customer_change, error_message or "Unknown error")
                        
                except Exception as e:
                    logger.error(f"💥 Exception updating customer {customer_change.customer_display_name}: {e}")
                    all_updates.append(None)
                    # Add to retry queue if not already a retry
                    if not is_retry:
                        self.retry_queue.add_failed_update(customer_change, str(e))
        
        return all_updates
    
    def _update_single_customer_metafield(self, customer_change: MarketingChangeDetection) -> Tuple[Optional[CustomerUpdate], Optional[str]]:
        """Update metafield for a single customer"""
        try:
            success = self.shopify_client.update_customer_metafield(
                customer_id=customer_change.customer_id,
                namespace=self.namespace,
                key=self.metafield_key,
                value=str(customer_change.new_email_subscribed).lower(),  # Convert to lowercase string
                create_if_missing=self.create_missing_metafields
            )
            
            if success:
                return CustomerUpdate(
                    customer_id=customer_change.customer_id,
                    email=customer_change.email,
                    old_email_subscribed=customer_change.old_email_subscribed,
                    new_email_subscribed=customer_change.new_email_subscribed,
                    metafield_namespace=self.namespace,
                    metafield_key=self.metafield_key
                ), None
            else:
                return None, "Metafield update returned False"
                
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Exception updating customer {customer_change.customer_id} metafield: {error_msg}")
            return None, error_msg
    
    def get_sync_status(self) -> Dict[str, Any]:
        """Get current synchronization status and cache statistics"""
        try:
            cache_stats = self.storage.get_cache_stats()
            
            # Try to get a quick count of customers
            try:
                current_customers = self.shopify_client.get_all_customers()
                current_customer_count = len(current_customers)
            except Exception as e:
                logger.warning(f"Could not fetch current customer count: {e}")
                current_customer_count = "Unknown"
            
            return {
                "cache_status": cache_stats,
                "current_shopify_customers": current_customer_count,
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
        """Clear the local customer marketing cache"""
        try:
            empty_cache = MarketingCache()
            return self.storage.save_cache(empty_cache)
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")
            return False
    
    def _process_customer_batch(self, customer_batch: List[dict], batch_start_idx: int):
        """
        Process a batch of customers: detect changes and update metafields immediately
        
        Args:
            customer_batch: List of customer dictionaries from Shopify
            batch_start_idx: Starting index of this batch in the overall customer list
        """
        try:
            # Load current cache
            cached_marketing = self.storage.load_cache()
            
            # Detect changes for this batch
            changes = self.storage.detect_marketing_changes(customer_batch, cached_marketing)
            customers_to_update = [change for change in changes if change.has_changed]
            
            if not customers_to_update:
                logger.info(f"📄 Batch processed: {len(customer_batch)} customers, no changes detected")
                return
            
            logger.info(f"🔄 Processing batch: {len(customers_to_update)} customers with changes out of {len(customer_batch)} total")
            
            # Update total processed count (customers with changes that we need to process)
            self.total_processed += len(customers_to_update)
            
            # Update metafields for customers with changes
            updated_customers = self._update_customers_with_concurrency(customers_to_update, is_retry=False)
            
            # Update counters
            successful_updates = len([u for u in updated_customers if u])
            failed_updates = len(updated_customers) - successful_updates
            
            self.total_updated += successful_updates
            self.total_failed += failed_updates
            self.all_updated_customers.extend([u for u in updated_customers if u])
            
            # Update cache incrementally with this batch
            updated_cache = self.storage.update_cache_with_current_data(customer_batch, cached_marketing)
            self.storage.save_cache(updated_cache)
            
        except Exception as e:
            logger.error(f"💥 Error processing batch {batch_start_idx + 1}-{batch_start_idx + len(customer_batch)}: {e}")
    
    def _analyze_customer_batch(self, customer_batch: List[dict], batch_start_idx: int):
        """
        Analyze a batch of customers for changes (DRY RUN mode)
        
        Args:
            customer_batch: List of customer dictionaries from Shopify
            batch_start_idx: Starting index of this batch in the overall customer list
        """
        try:
            # Load current cache
            cached_marketing = self.storage.load_cache()
            
            # Detect changes for this batch
            changes = self.storage.detect_marketing_changes(customer_batch, cached_marketing)
            customers_to_update = [change for change in changes if change.has_changed]
            
            if not customers_to_update:
                logger.info(f"📄 DRY RUN: Analyzed {len(customer_batch)} customers, no changes detected")
            else:
                logger.info(f"🧪 DRY RUN: Would update {len(customers_to_update)} customers with changes out of {len(customer_batch)} total")
                self.total_processed += len(customers_to_update)
            
        except Exception as e:
            logger.error(f"💥 Error analyzing batch {batch_start_idx + 1}-{batch_start_idx + len(customer_batch)}: {e}")