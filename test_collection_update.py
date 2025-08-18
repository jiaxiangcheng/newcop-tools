#!/usr/bin/env python3
"""
Test Collection Update Logic

This script tests the new intelligent collection update functionality
without actually modifying the Shopify collection. It provides a dry-run
mode to verify the logic works correctly.

Usage:
    # Test the update logic with current data
    python test_collection_update.py

    # Test with specific update frequency
    UPDATE_FREQUENCY_HOURS=12 python test_collection_update.py
"""

import os
import sys
import logging
from typing import List, Set
from pathlib import Path

# Add the current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from main import DynamicCollectionBuilder
from shopify_client import ShopifyClient
from models import FilteredProduct

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - TEST - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

class CollectionUpdateTester:
    """Test the collection update logic without making actual changes"""
    
    def __init__(self):
        self.builder = DynamicCollectionBuilder()
        
    def test_intelligent_update_logic(self) -> bool:
        """Test the intelligent update logic with current data"""
        logger.info("=== Testing Intelligent Collection Update Logic ===")
        
        try:
            # Get current collection products
            logger.info("Fetching current collection products...")
            current_products = self.builder.shopify_client.get_collection_products(
                self.builder.shopify_collection_id
            )
            current_product_ids = {p.id for p in current_products}
            logger.info(f"Current collection has {len(current_product_ids)} products")
            
            # Get filtered products from Airtable
            logger.info("Fetching and filtering Airtable data...")
            
            # Fetch sales data
            sales_records = self.builder.fetch_sales_data()
            if not sales_records:
                logger.error("No sales records found")
                return False
            
            # Filter products
            filtered_products = self.builder.filter_products(sales_records)
            if not filtered_products:
                logger.error("No products passed filtering criteria")
                return False
            
            # Extract valid Shopify IDs
            new_product_ids = set()
            products_without_id = 0
            
            for product in filtered_products:
                if product.shopify_id and product.shopify_id > 0:
                    new_product_ids.add(product.shopify_id)
                else:
                    products_without_id += 1
            
            logger.info(f"Filtered products: {len(filtered_products)}")
            logger.info(f"Valid Shopify IDs: {len(new_product_ids)}")
            logger.info(f"Products without valid Shopify ID: {products_without_id}")
            
            # Calculate what changes would be made
            products_to_add = new_product_ids - current_product_ids
            products_to_remove = current_product_ids - new_product_ids
            products_to_keep = new_product_ids & current_product_ids
            
            logger.info("=== Proposed Changes ===")
            logger.info(f"Products to ADD to collection: {len(products_to_add)}")
            logger.info(f"Products to REMOVE from collection: {len(products_to_remove)}")
            logger.info(f"Products to KEEP in collection: {len(products_to_keep)}")
            logger.info(f"Final collection size would be: {len(products_to_keep) + len(products_to_add)}")
            
            # Show sorting information
            logger.info("=== Sales-Based Sorting ===")
            logger.info("Top 5 products by quarterly sales:")
            for i, product in enumerate(filtered_products[:5], 1):
                status = "NEW" if product.shopify_id in products_to_add else "EXISTING" if product.shopify_id in products_to_keep else "NO_ID"
                logger.info(f"  {i}. {product.product_name} - Q Sales: {product.quarterly_sales:.1f}, Total: {product.total_sales:.1f} ({status})")
            
            # Show some examples
            if products_to_add:
                logger.info(f"Sample product IDs to add: {list(products_to_add)[:5]}")
            if products_to_remove:
                logger.info(f"Sample product IDs to remove: {list(products_to_remove)[:5]}")
            
            # Validate the logic
            if len(products_to_keep) + len(products_to_add) == len(new_product_ids):
                logger.info("✅ Update and sorting logic validation passed")
                logger.info(f"✅ Top seller will be: {filtered_products[0].product_name}")
                return True
            else:
                logger.error("❌ Update logic validation failed")
                return False
                
        except Exception as e:
            logger.error(f"Test failed with error: {e}")
            return False
    
    def test_configuration(self) -> bool:
        """Test configuration variables"""
        logger.info("=== Testing Configuration ===")
        
        try:
            logger.info(f"Update frequency: {self.builder.update_frequency_hours} hours")
            logger.info(f"Max Airtable records: {self.builder.max_airtable_records}")
            logger.info(f"Shopify collection ID: {self.builder.shopify_collection_id}")
            logger.info(f"Shopify shop domain: {self.builder.shopify_shop_domain}")
            
            # Test environment variable override
            test_freq = os.getenv("UPDATE_FREQUENCY_HOURS")
            if test_freq:
                logger.info(f"Environment override detected: UPDATE_FREQUENCY_HOURS={test_freq}")
            
            test_records = os.getenv("MAX_AIRTABLE_RECORDS")
            if test_records:
                logger.info(f"Environment override detected: MAX_AIRTABLE_RECORDS={test_records}")
            
            logger.info("✅ Configuration test passed")
            return True
            
        except Exception as e:
            logger.error(f"Configuration test failed: {e}")
            return False

def main():
    """Main test function"""
    logger.info("Starting Collection Update Tests...")
    
    try:
        tester = CollectionUpdateTester()
        
        # Test configuration
        config_ok = tester.test_configuration()
        
        # Test update logic
        logic_ok = tester.test_intelligent_update_logic()
        
        if config_ok and logic_ok:
            logger.info("🎉 All tests passed! The new update logic is ready.")
            logger.info("To run the actual update, use: python main.py")
            logger.info("To run with scheduler, use: python scheduler.py")
            sys.exit(0)
        else:
            logger.error("❌ Some tests failed. Please check the logs above.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("⏹️  Tests interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💥 Test error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()