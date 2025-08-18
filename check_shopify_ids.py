#!/usr/bin/env python3
"""
Check Shopify IDs in filtered products
"""

import sys
import logging
from airtable_client import AirtableClient
from product_filter import ProductFilter
from models import SalesRecord

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # Initialize clients
    airtable_client = AirtableClient(
        "pat8Pg6d8OumFPpOO.4b99b41e37fd0c93f5da770a6073c66e21741f574910736a6184347f342faee6",
        "appDE0y01TchMqX8N"
    )
    product_filter = ProductFilter()
    
    try:
        # Fetch data
        print("🔄 Fetching sales data...")
        raw_records = airtable_client.get_spain_sales_data(
            "tbljkyhWy5D6b65Im",
            "viwixRrDpjcAYwHId",
            max_records=500
        )
        
        # Parse records
        sales_records = []
        for record in raw_records:
            try:
                sales_record = SalesRecord.from_airtable_record(record)
                sales_records.append(sales_record)
            except Exception as e:
                continue
        
        print(f"📊 Total records: {len(sales_records)}")
        
        # Filter products
        filtered_products = product_filter.filter_products(sales_records)
        print(f"🎯 Qualifying products: {len(filtered_products)}")
        
        # Check Shopify IDs
        valid_shopify_ids = 0
        missing_shopify_ids = 0
        shopify_ids = []
        
        for product in filtered_products:
            if product.shopify_id and product.shopify_id > 0:
                valid_shopify_ids += 1
                shopify_ids.append(product.shopify_id)
            else:
                missing_shopify_ids += 1
                print(f"⚠️  Missing ID: {product.product_name}")
        
        print(f"\n📈 SHOPIFY ID ANALYSIS:")
        print(f"✅ Products with valid Shopify ID: {valid_shopify_ids}")
        print(f"❌ Products missing Shopify ID: {missing_shopify_ids}")
        print(f"📋 Shopify IDs ready for collection: {len(shopify_ids)}")
        
        if shopify_ids:
            print(f"\n🔢 First 10 Shopify IDs:")
            for i, shopify_id in enumerate(shopify_ids[:10], 1):
                print(f"  {i:2d}. {shopify_id}")
        
        return shopify_ids
        
    except Exception as e:
        print(f"💥 Error: {e}")
        return []

if __name__ == "__main__":
    main()