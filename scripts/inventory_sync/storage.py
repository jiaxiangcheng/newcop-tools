import json
import os
import logging
from typing import List, Optional
from datetime import datetime
from pathlib import Path

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.inventory_sync.models import (
    InventoryCache, 
    ProductInventoryCache, 
    VariantInventory, 
    InventoryChangeDetection
)

logger = logging.getLogger(__name__)

class InventoryStorage:
    """Manages local JSON cache for inventory state"""
    
    def __init__(self, cache_file_path: Optional[str] = None):
        # Default cache file location
        if cache_file_path is None:
            project_root = Path(__file__).parent.parent.parent
            cache_file_path = project_root / "data" / "inventory_cache.json"
        
        self.cache_file_path = Path(cache_file_path)
        self.cache_file_path.parent.mkdir(parents=True, exist_ok=True)
        
    def load_cache(self) -> InventoryCache:
        """Load inventory cache from JSON file"""
        try:
            if not self.cache_file_path.exists():
                logger.info("Cache file does not exist, creating new cache")
                return InventoryCache()
            
            with open(self.cache_file_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # Convert string timestamps back to datetime objects
            if cache_data.get('last_sync'):
                cache_data['last_sync'] = datetime.fromisoformat(cache_data['last_sync'])
            
            # Process products
            for product_id, product_data in cache_data.get('products', {}).items():
                if product_data.get('last_sync'):
                    product_data['last_sync'] = datetime.fromisoformat(product_data['last_sync'])
                
                # Process variants within each product
                for variant_id, variant_data in product_data.get('variants', {}).items():
                    if variant_data.get('last_updated'):
                        variant_data['last_updated'] = datetime.fromisoformat(variant_data['last_updated'])
            
            cache = InventoryCache(**cache_data)
            logger.info(f"Loaded cache with {len(cache.products)} products and {cache.total_variants} variants")
            return cache
            
        except Exception as e:
            logger.error(f"Error loading cache file: {e}")
            logger.warning("Creating new empty cache")
            return InventoryCache()
    
    def save_cache(self, cache: InventoryCache) -> bool:
        """Save inventory cache to JSON file with atomic write"""
        try:
            # Convert datetime objects to strings for JSON serialization
            cache_dict = cache.dict()
            
            if cache_dict.get('last_sync'):
                cache_dict['last_sync'] = cache_dict['last_sync'].isoformat()
            
            # Process products
            for product_id, product_data in cache_dict.get('products', {}).items():
                if product_data.get('last_sync'):
                    product_data['last_sync'] = product_data['last_sync'].isoformat()
                
                # Process variants
                for variant_id, variant_data in product_data.get('variants', {}).items():
                    if variant_data.get('last_updated'):
                        variant_data['last_updated'] = variant_data['last_updated'].isoformat()
            
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
    
    def detect_inventory_changes(self, 
                               current_products: List[dict], 
                               cached_inventory: InventoryCache) -> List[InventoryChangeDetection]:
        """
        Compare current product inventory with cached state to detect changes
        
        Args:
            current_products: List of products from Shopify API
            cached_inventory: Previously cached inventory state
            
        Returns:
            List of inventory changes detected
        """
        changes = []
        
        for product in current_products:
            product_id = str(product.get('id'))
            product_title = product.get('title', 'Unknown Product')
            variants = product.get('variants', [])
            
            # Get cached product data if it exists
            cached_product = cached_inventory.products.get(product_id)
            
            for variant in variants:
                variant_id = variant.get('id')
                current_quantity = variant.get('inventory_quantity', 0)
                variant_title = variant.get('title')
                
                # Get cached variant inventory
                old_quantity = None
                if cached_product and str(variant_id) in cached_product.variants:
                    old_quantity = cached_product.variants[str(variant_id)].inventory_quantity
                
                # Determine if this variant has changed
                has_changed = old_quantity != current_quantity
                
                change_detection = InventoryChangeDetection(
                    variant_id=variant_id,
                    product_id=int(product_id),
                    product_title=product_title,
                    variant_title=variant_title,
                    old_quantity=old_quantity,
                    new_quantity=current_quantity,
                    has_changed=has_changed
                )
                
                changes.append(change_detection)
        
        # Log summary
        changed_variants = [c for c in changes if c.has_changed]
        logger.info(f"Detected {len(changed_variants)} variants with inventory changes out of {len(changes)} total variants")
        
        return changes
    
    def update_cache_with_current_data(self, 
                                     current_products: List[dict], 
                                     cache: InventoryCache) -> InventoryCache:
        """Update cache with current product/variant data"""
        now = datetime.now()
        
        # Update cache metadata
        cache.last_sync = now
        cache.total_products = len(current_products)
        
        total_variants = 0
        
        for product in current_products:
            product_id = str(product.get('id'))
            product_title = product.get('title', 'Unknown Product')
            variants = product.get('variants', [])
            
            # Create or update product cache entry
            product_cache = ProductInventoryCache(
                product_id=int(product_id),
                product_title=product_title,
                last_sync=now,
                variants={}
            )
            
            # Process variants
            for variant in variants:
                variant_id = str(variant.get('id'))
                inventory_quantity = variant.get('inventory_quantity', 0)
                
                variant_inventory = VariantInventory(
                    variant_id=int(variant_id),
                    product_id=int(product_id),
                    inventory_quantity=inventory_quantity,
                    last_updated=now,
                    title=variant.get('title'),
                    sku=variant.get('sku')
                )
                
                product_cache.variants[variant_id] = variant_inventory
                total_variants += 1
            
            cache.products[product_id] = product_cache
        
        cache.total_variants = total_variants
        
        logger.info(f"Updated cache with {len(current_products)} products and {total_variants} variants")
        return cache
    
    def get_cache_stats(self) -> dict:
        """Get statistics about the current cache"""
        try:
            cache = self.load_cache()
            return {
                "cache_file_exists": self.cache_file_path.exists(),
                "cache_file_path": str(self.cache_file_path),
                "last_sync": cache.last_sync.isoformat() if cache.last_sync else None,
                "total_products": cache.total_products,
                "total_variants": cache.total_variants,
                "file_size_bytes": self.cache_file_path.stat().st_size if self.cache_file_path.exists() else 0
            }
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {"error": str(e)}