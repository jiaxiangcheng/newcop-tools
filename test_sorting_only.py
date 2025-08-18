#!/usr/bin/env python3
"""
Test Product Sorting Logic Only

This script tests just the product sorting functionality without connecting to APIs.
It creates mock data to verify that products are sorted correctly by sales performance.
"""

import logging
from typing import List
from models import SalesRecord, FilteredProduct
from product_filter import ProductFilter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - SORT_TEST - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

def create_mock_sales_records() -> List[SalesRecord]:
    """Create mock sales records for testing sorting"""
    mock_records = [
        {
            "id": "rec1",
            "createdTime": "2024-01-01T00:00:00Z",
            "fields": {
                "Product Title": "Nike Air Jordan 1 High",
                "Vendor": "Nike",
                "Tags": "sneakers,basketball",
                "∞ Shopify Id": "123456789",
                "Ventas trimestre": 25.5,
                "Total sale": 1200.0
            }
        },
        {
            "id": "rec2", 
            "createdTime": "2024-01-01T00:00:00Z",
            "fields": {
                "Product Title": "Adidas Yeezy Boost 350",
                "Vendor": "Adidas", 
                "Tags": "sneakers,lifestyle",
                "∞ Shopify Id": "123456790",
                "Ventas trimestre": 35.0,
                "Total sale": 1800.0
            }
        },
        {
            "id": "rec3",
            "createdTime": "2024-01-01T00:00:00Z", 
            "fields": {
                "Product Title": "New Balance 990v5",
                "Vendor": "New Balance",
                "Tags": "sneakers,running",
                "∞ Shopify Id": "123456791",
                "Ventas trimestre": 15.2,
                "Total sale": 800.0
            }
        },
        {
            "id": "rec4",
            "createdTime": "2024-01-01T00:00:00Z",
            "fields": {
                "Product Title": "Puma RS-X",
                "Vendor": "Puma",
                "Tags": "sneakers,retro",
                "∞ Shopify Id": "123456792", 
                "Ventas trimestre": 8.7,
                "Total sale": 450.0
            }
        },
        {
            "id": "rec5",
            "createdTime": "2024-01-01T00:00:00Z",
            "fields": {
                "Product Title": "Asics Gel-Lyte III",
                "Vendor": "Asics",
                "Tags": "sneakers,classic", 
                "∞ Shopify Id": "123456793",
                "Ventas trimestre": 12.3,
                "Total sale": 600.0
            }
        },
        {
            "id": "rec6",
            "createdTime": "2024-01-01T00:00:00Z",
            "fields": {
                "Product Title": "Nike Air Max 90",
                "Vendor": "Nike",
                "Tags": "sneakers,lifestyle",
                "∞ Shopify Id": "123456794",
                "Ventas trimestre": 20.1,
                "Total sale": 1000.0
            }
        },
        # Product that should be excluded (retail tag)
        {
            "id": "rec7",
            "createdTime": "2024-01-01T00:00:00Z",
            "fields": {
                "Product Title": "Nike Retail Item",
                "Vendor": "Nike", 
                "Tags": "retail,excluded",
                "∞ Shopify Id": "123456795",
                "Ventas trimestre": 50.0,
                "Total sale": 2000.0
            }
        },
        # Product that should be excluded (low sales)
        {
            "id": "rec8",
            "createdTime": "2024-01-01T00:00:00Z",
            "fields": {
                "Product Title": "Adidas Low Seller",
                "Vendor": "Adidas",
                "Tags": "sneakers",
                "∞ Shopify Id": "123456796", 
                "Ventas trimestre": 2.5,
                "Total sale": 100.0
            }
        }
    ]
    
    # Convert to SalesRecord objects
    sales_records = []
    for record_data in mock_records:
        try:
            record = SalesRecord.from_airtable_record(record_data)
            sales_records.append(record)
        except Exception as e:
            logger.error(f"Failed to create record: {e}")
    
    return sales_records

def test_product_sorting():
    """Test that products are sorted correctly by sales performance"""
    logger.info("=== Testing Product Sorting Logic ===")
    
    # Create mock data
    sales_records = create_mock_sales_records()
    logger.info(f"Created {len(sales_records)} mock sales records")
    
    # Filter and sort products
    product_filter = ProductFilter()
    filtered_products = product_filter.filter_products(sales_records)
    
    logger.info(f"Filtered to {len(filtered_products)} qualifying products")
    
    # Display sorted results
    logger.info("=== Final Sorted Products (Top Sellers First) ===")
    for i, product in enumerate(filtered_products, 1):
        logger.info(f"Position {i}: {product.product_name}")
        logger.info(f"  - Quarterly Sales: {product.quarterly_sales:.1f}")
        logger.info(f"  - Total Sales: {product.total_sales:.1f}")
        logger.info(f"  - Brand: {product.brand}")
        logger.info(f"  - Shopify ID: {product.shopify_id}")
        logger.info(f"  - Sort Position: {product.sort_position}")
        logger.info("")
    
    # Validate sorting
    is_correctly_sorted = True
    for i in range(len(filtered_products) - 1):
        current = filtered_products[i]
        next_product = filtered_products[i + 1]
        
        # Check if current product should be ranked higher than next
        if (current.quarterly_sales < next_product.quarterly_sales or 
            (current.quarterly_sales == next_product.quarterly_sales and 
             current.total_sales < next_product.total_sales)):
            logger.error(f"❌ Sorting error: {current.product_name} should not be ranked higher than {next_product.product_name}")
            is_correctly_sorted = False
    
    # Validate that excluded products are not present
    excluded_names = ["Nike Retail Item", "Adidas Low Seller"]
    found_excluded = [p.product_name for p in filtered_products if p.product_name in excluded_names]
    
    if found_excluded:
        logger.error(f"❌ Found excluded products: {found_excluded}")
        is_correctly_sorted = False
    
    # Expected top seller should be Yeezy Boost 350 (highest quarterly sales)
    if filtered_products and filtered_products[0].product_name != "Adidas Yeezy Boost 350":
        logger.error(f"❌ Expected top seller 'Adidas Yeezy Boost 350', got '{filtered_products[0].product_name}'")
        is_correctly_sorted = False
    
    if is_correctly_sorted:
        logger.info("✅ Product sorting validation passed!")
        logger.info(f"✅ Top seller correctly identified: {filtered_products[0].product_name}")
        return True
    else:
        logger.error("❌ Product sorting validation failed!")
        return False

def main():
    """Main test function"""
    try:
        success = test_product_sorting()
        
        if success:
            logger.info("🎉 Sorting test completed successfully!")
            logger.info("The products will be ordered correctly in the collection by sales performance.")
        else:
            logger.error("❌ Sorting test failed!")
            return 1
            
    except Exception as e:
        logger.error(f"💥 Test error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())