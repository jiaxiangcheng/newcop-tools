import json
import os
import logging
from typing import List, Optional
from datetime import datetime
from pathlib import Path

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.customer_marketing_sync.models import (
    MarketingCache,
    CustomerMarketingCache,
    MarketingChangeDetection
)

logger = logging.getLogger('customer_marketing_sync')

def get_customer_email_marketing_status(customer_data: dict) -> bool:
    """
    Get email marketing subscription status from the current Shopify API.
    
    Uses email_marketing_consent.state field (available since API 2022-04).
    
    Args:
        customer_data: Customer data dictionary from Shopify API
        
    Returns:
        bool: True if customer is subscribed to email marketing, False otherwise
    """
    # Handle None customer_data
    if customer_data is None:
        logger.warning("Customer data is None, defaulting to False for email marketing status")
        return False
        
    email_marketing_consent = customer_data.get('email_marketing_consent', {})
    marketing_state = email_marketing_consent.get('state')
    
    if marketing_state in ['subscribed', 'pending']:
        return True
    elif marketing_state == 'not_subscribed':
        return False
    else:
        # Default to False if state is unknown or missing
        return False

class CustomerMarketingStorage:
    """Manages local JSON cache for customer marketing subscription state"""
    
    def __init__(self, cache_file_path: Optional[str] = None):
        # Default cache file location
        if cache_file_path is None:
            project_root = Path(__file__).parent.parent.parent
            cache_file_path = project_root / "data" / "customer_marketing_cache.json"
        
        self.cache_file_path = Path(cache_file_path)
        self.cache_file_path.parent.mkdir(parents=True, exist_ok=True)
        
    def load_cache(self) -> MarketingCache:
        """Load customer marketing cache from JSON file"""
        try:
            if not self.cache_file_path.exists():
                logger.info("Cache file does not exist, creating new cache")
                return MarketingCache()
            
            with open(self.cache_file_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # Convert string timestamps back to datetime objects
            if cache_data.get('last_sync'):
                cache_data['last_sync'] = datetime.fromisoformat(cache_data['last_sync'])
            
            # Process customers
            for customer_id, customer_data in cache_data.get('customers', {}).items():
                if customer_data.get('last_sync'):
                    customer_data['last_sync'] = datetime.fromisoformat(customer_data['last_sync'])
            
            cache = MarketingCache(**cache_data)
            logger.info(f"Loaded cache with {len(cache.customers)} customers")
            return cache
            
        except Exception as e:
            logger.error(f"Error loading cache file: {e}")
            logger.warning("Creating new empty cache")
            return MarketingCache()
    
    def save_cache(self, cache: MarketingCache) -> bool:
        """Save customer marketing cache to JSON file with atomic write"""
        try:
            # Convert datetime objects to strings for JSON serialization
            cache_dict = cache.model_dump()
            
            if cache_dict.get('last_sync'):
                cache_dict['last_sync'] = cache_dict['last_sync'].isoformat()
            
            # Process customers
            for customer_id, customer_data in cache_dict.get('customers', {}).items():
                if customer_data.get('last_sync'):
                    customer_data['last_sync'] = customer_data['last_sync'].isoformat()
            
            # Atomic write: write to temp file, then rename
            temp_file_path = self.cache_file_path.with_suffix('.tmp')
            
            with open(temp_file_path, 'w', encoding='utf-8') as f:
                json.dump(cache_dict, f, indent=2, ensure_ascii=False)
            
            # Atomic rename
            temp_file_path.replace(self.cache_file_path)
            
            logger.info(f"Cache saved successfully to {self.cache_file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving cache file: {e}")
            return False
    
    def detect_marketing_changes(self, 
                                current_customers: List[dict], 
                                cached_marketing: MarketingCache) -> List[MarketingChangeDetection]:
        """
        Compare current customer marketing preferences with cached state to detect changes
        
        Args:
            current_customers: List of customers from Shopify API
            cached_marketing: Previously cached marketing state
            
        Returns:
            List of marketing preference changes detected
        """
        changes = []
        
        start_time = datetime.now()
        
        for idx, customer in enumerate(current_customers, 1):
            # Skip None customers (corrupted data)
            if customer is None:
                logger.warning(f"Skipping None customer at index {idx}")
                continue
                
            customer_id = str(customer.get('id'))
            email = customer.get('email', 'unknown@unknown.com')
            first_name = customer.get('first_name')
            last_name = customer.get('last_name') 
            current_email_subscribed = get_customer_email_marketing_status(customer)
            
            # Get cached customer marketing state if it exists
            cached_customer = cached_marketing.customers.get(customer_id)
            
            # Get cached marketing preference
            old_email_subscribed = None
            if cached_customer:
                old_email_subscribed = cached_customer.email_subscribed
            
            # Determine if this customer has changed marketing preference
            has_changed = old_email_subscribed != current_email_subscribed
            
            change_detection = MarketingChangeDetection(
                customer_id=int(customer_id),
                email=email,
                first_name=first_name,
                last_name=last_name,
                old_email_subscribed=old_email_subscribed,
                new_email_subscribed=current_email_subscribed,
                has_changed=has_changed
            )
            
            changes.append(change_detection)
            
        
        # Log summary
        changed_customers = [c for c in changes if c.has_changed]
        if changed_customers:
            logger.info(f"Detected {len(changed_customers)} customers with marketing preference changes")
        
        return changes
    
    def update_cache_with_current_data(self, 
                                     current_customers: List[dict], 
                                     cache: MarketingCache) -> MarketingCache:
        """Update cache with current customer marketing data"""
        now = datetime.now()
        
        # Update cache metadata
        cache.last_sync = now
        cache.total_customers = len(current_customers)
        
        for customer in current_customers:
            # Skip None customers
            if customer is None:
                logger.warning("Skipping None customer in cache update")
                continue
                
            customer_id = str(customer.get('id'))
            email = customer.get('email', 'unknown@unknown.com')
            first_name = customer.get('first_name')
            last_name = customer.get('last_name')
            email_subscribed = get_customer_email_marketing_status(customer)
            
            # Get marketing opt in level from email_marketing_consent
            email_marketing_consent = customer.get('email_marketing_consent', {})
            marketing_opt_in_level = email_marketing_consent.get('opt_in_level')
            
            # Create or update customer cache entry
            customer_cache = CustomerMarketingCache(
                customer_id=int(customer_id),
                email=email,
                first_name=first_name,
                last_name=last_name,
                email_subscribed=email_subscribed,
                marketing_opt_in_level=marketing_opt_in_level,
                last_sync=now
            )
            
            cache.customers[customer_id] = customer_cache
        
        return cache
    
    def get_cache_stats(self) -> dict:
        """Get statistics about the current cache"""
        try:
            cache = self.load_cache()
            return {
                "cache_file_exists": self.cache_file_path.exists(),
                "cache_file_path": str(self.cache_file_path),
                "last_sync": cache.last_sync.isoformat() if cache.last_sync else None,
                "total_customers": cache.total_customers,
                "file_size_bytes": self.cache_file_path.stat().st_size if self.cache_file_path.exists() else 0
            }
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {"error": str(e)}