#!/usr/bin/env python3
"""
Dynamic Collection Builder Script

This script fetches sales data from Airtable for Spain (last 90 days),
filters products based on brand criteria and sales thresholds,
and updates a Shopify collection with qualifying products.

Required environment variables:
- AIRTABLE_TOKEN
- SHOPIFY_ADMIN_TOKEN
- SHOPIFY_SHOP_DOMAIN (optional, defaults to extracting from collection)
"""

import os
import sys
import logging
from typing import Dict, Any
from dotenv import load_dotenv

from airtable_client import AirtableClient
from shopify_client import ShopifyClient
from product_filter import ProductFilter
from models import SalesRecord

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('dynamic_collection.log')
    ]
)

logger = logging.getLogger(__name__)

class DynamicCollectionBuilder:
    """Main class to orchestrate the dynamic collection building process"""
    
    def __init__(self):
        # Configuration from environment variables
        self.airtable_token = os.getenv("AIRTABLE_TOKEN")
        self.shopify_admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
        self.airtable_base_id = os.getenv("AIRTABLE_BASE_ID")
        self.airtable_table_id = os.getenv("AIRTABLE_TABLE_ID")
        self.airtable_view_id = os.getenv("AIRTABLE_VIEW_ID")
        self.shopify_collection_id = os.getenv("SHOPIFY_COLLECTION_ID")
        self.shopify_shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")
        
        # Update frequency control (in hours)
        # Set this to control how often the collection should be updated
        self.update_frequency_hours = int(os.getenv("UPDATE_FREQUENCY_HOURS", "24"))  # Default: daily
        
        # Maximum records to fetch from Airtable (to control API usage)
        self.max_airtable_records = int(os.getenv("MAX_AIRTABLE_RECORDS", "500"))
        
        # Initialize clients
        self.airtable_client = AirtableClient(self.airtable_token, self.airtable_base_id)
        self.shopify_client = ShopifyClient(self.shopify_admin_token, self.shopify_shop_domain)
        self.product_filter = ProductFilter()
    
    def validate_environment(self) -> bool:
        """Validate that all required configuration is present"""
        required_configs = [
            ("AIRTABLE_TOKEN", self.airtable_token),
            ("SHOPIFY_ADMIN_TOKEN", self.shopify_admin_token),
            ("AIRTABLE_BASE_ID", self.airtable_base_id),
            ("AIRTABLE_TABLE_ID", self.airtable_table_id),
            ("AIRTABLE_VIEW_ID", self.airtable_view_id),
            ("SHOPIFY_COLLECTION_ID", self.shopify_collection_id),
            ("SHOPIFY_SHOP_DOMAIN", self.shopify_shop_domain)
        ]
        
        missing_configs = []
        for name, value in required_configs:
            if not value or value in ["your_token_here", "your_shop", ""]:
                missing_configs.append(name)
        
        if missing_configs:
            logger.error(f"Missing required environment variables: {', '.join(missing_configs)}")
            logger.error("Please create a .env file with all required variables.")
            logger.error("See .env.example for the required format.")
            return False
        
        logger.info("Environment validation passed")
        logger.info(f"Update frequency: {self.update_frequency_hours} hours")
        logger.info(f"Max Airtable records: {self.max_airtable_records}")
        return True
    
    def analyze_airtable_structure(self) -> Dict[str, Any]:
        """Analyze the structure of the Airtable data"""
        logger.info("Analyzing Airtable table structure...")
        
        try:
            structure = self.airtable_client.analyze_table_structure(
                self.airtable_table_id, 
                self.airtable_view_id
            )
            
            logger.info(f"Table analysis complete:")
            logger.info(f"- Fields found: {structure['fields']}")
            logger.info(f"- Sample record ID: {structure.get('sample_record', {}).get('id', 'N/A')}")
            
            return structure
            
        except Exception as e:
            logger.error(f"Failed to analyze table structure: {e}")
            raise
    
    def fetch_sales_data(self) -> list[SalesRecord]:
        """Fetch and process sales data from Airtable"""
        logger.info("Fetching Spain sales data from Airtable...")
        
        try:
            raw_records = self.airtable_client.get_spain_sales_data(
                self.airtable_table_id,
                self.airtable_view_id,
                max_records=self.max_airtable_records
            )
            
            # Convert to SalesRecord objects
            sales_records = []
            for record in raw_records:
                try:
                    sales_record = SalesRecord.from_airtable_record(record)
                    sales_records.append(sales_record)
                except Exception as e:
                    logger.warning(f"Failed to parse record {record.get('id', 'unknown')}: {e}")
                    continue
            
            logger.info(f"Successfully processed {len(sales_records)} sales records")
            return sales_records
            
        except Exception as e:
            logger.error(f"Failed to fetch sales data: {e}")
            raise
    
    def filter_products(self, sales_records: list[SalesRecord]) -> list:
        """Filter products based on criteria"""
        logger.info("Filtering products based on brand, tags, and sales criteria...")
        
        try:
            filtered_products = self.product_filter.filter_products(sales_records)
            
            # Log filtering summary
            summary = self.product_filter.get_filtering_summary()
            logger.info(f"Filtering summary: {summary}")
            
            # Log some examples of filtered products
            if filtered_products:
                logger.info("Sample filtered products:")
                for i, product in enumerate(filtered_products[:5]):
                    logger.info(f"  {i+1}. {product.product_name} (Brand: {product.brand}, Sales: {product.quarterly_sales})")
            
            return filtered_products
            
        except Exception as e:
            logger.error(f"Failed to filter products: {e}")
            raise
    
    def update_shopify_collection(self, filtered_products: list) -> Dict[str, Any]:
        """Update Shopify collection with filtered products"""
        logger.info(f"Updating Shopify collection {self.shopify_collection_id}...")
        
        try:
            result = self.shopify_client.update_collection_with_filtered_products(
                self.shopify_collection_id,
                filtered_products
            )
            
            logger.info("Collection update completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Failed to update Shopify collection: {e}")
            raise
    
    def run(self) -> Dict[str, Any]:
        """Run the complete dynamic collection building process"""
        logger.info("Starting dynamic collection building process...")
        
        try:
            # Validate environment
            if not self.validate_environment():
                raise ValueError("Environment validation failed")
            
            # Analyze table structure (first 10 records)
            structure = self.analyze_airtable_structure()
            
            # Fetch sales data
            sales_records = self.fetch_sales_data()
            
            if not sales_records:
                logger.warning("No sales records found")
                return {"success": False, "message": "No sales records found"}
            
            # Filter products
            filtered_products = self.filter_products(sales_records)
            
            if not filtered_products:
                logger.warning("No products passed filtering criteria")
                return {"success": False, "message": "No products passed filtering criteria"}
            
            # Update Shopify collection
            update_result = self.update_shopify_collection(filtered_products)
            
            # Prepare final result
            result = {
                "success": True,
                "airtable_structure": structure,
                "total_sales_records": len(sales_records),
                "filtered_products_count": len(filtered_products),
                "shopify_update_result": update_result,
                "collection_id": self.shopify_collection_id
            }
            
            logger.info("Dynamic collection building completed successfully!")
            logger.info(f"Final result: {result}")
            
            return result
            
        except Exception as e:
            logger.error(f"Dynamic collection building failed: {e}")
            return {"success": False, "error": str(e)}

def main():
    """Main entry point"""
    try:
        builder = DynamicCollectionBuilder()
        result = builder.run()
        
        if result["success"]:
            print("\n✅ Dynamic collection building completed successfully!")
            print(f"📊 Processed {result.get('total_sales_records', 0)} sales records")
            print(f"🎯 Filtered to {result.get('filtered_products_count', 0)} qualifying products")
            print(f"🛍️  Updated collection {result.get('collection_id', 'N/A')}")
        else:
            print(f"\n❌ Dynamic collection building failed: {result.get('error', result.get('message', 'Unknown error'))}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⏹️  Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()