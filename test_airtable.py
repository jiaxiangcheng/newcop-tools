#!/usr/bin/env python3
"""
Test script to analyze Airtable data without modifying Shopify
"""

import sys
import logging
from typing import Dict, Any
import json

from airtable_client import AirtableClient
from product_filter import ProductFilter
from models import SalesRecord

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)

logger = logging.getLogger(__name__)

class AirtableAnalyzer:
    """Analyze Airtable data without making any Shopify changes"""
    
    def __init__(self):
        self.airtable_token = "pat8Pg6d8OumFPpOO.4b99b41e37fd0c93f5da770a6073c66e21741f574910736a6184347f342faee6"
        self.airtable_base_id = "appDE0y01TchMqX8N"
        self.airtable_table_id = "tbljkyhWy5D6b65Im"
        self.airtable_view_id = "viwixRrDpjcAYwHId"
        
        # Initialize clients
        self.airtable_client = AirtableClient(self.airtable_token, self.airtable_base_id)
        self.product_filter = ProductFilter()
    
    def analyze_table_structure(self) -> Dict[str, Any]:
        """Analyze the structure of the Airtable data"""
        logger.info("🔍 Analyzing Airtable table structure...")
        
        try:
            structure = self.airtable_client.analyze_table_structure(
                self.airtable_table_id, 
                self.airtable_view_id
            )
            
            print("\n📋 TABLE STRUCTURE ANALYSIS:")
            print("=" * 50)
            print(f"Total records fetched for analysis: {structure['total_records_fetched']}")
            print(f"Number of fields: {len(structure['fields'])}")
            print("\n📄 Available fields:")
            for i, field in enumerate(structure['fields'], 1):
                field_type = structure['field_types'].get(field, 'unknown')
                print(f"  {i:2d}. {field} ({field_type})")
            
            if structure.get('sample_record'):
                print(f"\n🔍 Sample record ID: {structure['sample_record'].get('id', 'N/A')}")
                print("📊 Sample record fields:")
                for field, value in structure['sample_record'].get('fields', {}).items():
                    # Truncate long values for display
                    if isinstance(value, str) and len(value) > 100:
                        display_value = value[:100] + "..."
                    else:
                        display_value = value
                    print(f"  - {field}: {display_value}")
            
            return structure
            
        except Exception as e:
            logger.error(f"Failed to analyze table structure: {e}")
            raise
    
    def fetch_and_display_sales_data(self) -> list[SalesRecord]:
        """Fetch and display sales data from Airtable"""
        logger.info("📥 Fetching Spain sales data from Airtable...")
        
        try:
            raw_records = self.airtable_client.get_spain_sales_data(
                self.airtable_table_id,
                self.airtable_view_id,
                max_records=500
            )
            
            print(f"\n📊 RAW DATA SUMMARY:")
            print("=" * 50)
            print(f"Total raw records fetched: {len(raw_records)}")
            
            # Convert to SalesRecord objects
            sales_records = []
            parsing_errors = []
            
            for i, record in enumerate(raw_records):
                try:
                    sales_record = SalesRecord.from_airtable_record(record)
                    sales_records.append(sales_record)
                except Exception as e:
                    parsing_errors.append(f"Record {i+1} ({record.get('id', 'unknown')}): {e}")
                    continue
            
            print(f"Successfully parsed: {len(sales_records)} records")
            if parsing_errors:
                print(f"Parsing errors: {len(parsing_errors)}")
                print("First 3 errors:")
                for error in parsing_errors[:3]:
                    print(f"  - {error}")
            
            # Display first few records
            if sales_records:
                print(f"\n📋 FIRST 10 PARSED RECORDS:")
                print("=" * 80)
                for i, record in enumerate(sales_records[:10], 1):
                    print(f"{i:2d}. Product: {record.product_name or 'N/A'}")
                    print(f"    Brand: {record.brand or 'N/A'}")
                    print(f"    Tags: {record.tags or []}")
                    print(f"    Quarterly Sales: {record.quarterly_sales}")
                    print(f"    Shopify ID: {record.shopify_id}")
                    print(f"    Record ID: {record.record_id}")
                    print()
            
            return sales_records
            
        except Exception as e:
            logger.error(f"Failed to fetch sales data: {e}")
            raise
    
    def analyze_filtering_results(self, sales_records: list[SalesRecord]):
        """Analyze what products would pass filtering"""
        logger.info("🔍 Analyzing filtering results...")
        
        try:
            filtered_products = self.product_filter.filter_products(sales_records)
            
            print(f"\n🎯 FILTERING ANALYSIS:")
            print("=" * 50)
            
            # Get filtering summary
            summary = self.product_filter.get_filtering_summary()
            print(f"Brand keywords: {summary['brand_keywords']}")
            print(f"Excluded tags: {summary['excluded_tags']}")
            print(f"Min quarterly sales: {summary['min_quarterly_sales']}")
            print(f"Total products after filtering: {summary['total_filtered_products']}")
            
            if filtered_products:
                print(f"\n✅ QUALIFYING PRODUCTS ({len(filtered_products)}):")
                print("=" * 80)
                for i, product in enumerate(filtered_products, 1):
                    print(f"{i:2d}. {product.product_name}")
                    print(f"    Brand: {product.brand}")
                    print(f"    Sales: {product.quarterly_sales}")
                    print(f"    Tags: {product.tags}")
                    print(f"    Shopify ID: {product.shopify_id}")
                    print(f"    Record ID: {product.record_id}")
                    print()
            else:
                print("\n❌ No products passed the filtering criteria")
            
            # Analyze why products were filtered out
            self._analyze_filter_reasons(sales_records)
            
            return filtered_products
            
        except Exception as e:
            logger.error(f"Failed to analyze filtering: {e}")
            raise
    
    def _analyze_filter_reasons(self, sales_records: list[SalesRecord]):
        """Analyze why products were filtered out"""
        print(f"\n📊 FILTER ANALYSIS:")
        print("=" * 50)
        
        total_records = len(sales_records)
        brand_fails = 0
        retail_tag_fails = 0
        sales_threshold_fails = 0
        
        for record in sales_records:
            # Check brand keyword
            has_brand = self._has_required_brand_keyword(record)
            has_retail = self._has_excluded_tags(record)
            meets_sales = record.quarterly_sales >= 5.0
            
            if not has_brand:
                brand_fails += 1
            if has_retail:
                retail_tag_fails += 1
            if not meets_sales:
                sales_threshold_fails += 1
        
        print(f"Total records analyzed: {total_records}")
        print(f"Failed brand keyword check: {brand_fails} ({brand_fails/total_records*100:.1f}%)")
        print(f"Failed retail tag check: {retail_tag_fails} ({retail_tag_fails/total_records*100:.1f}%)")
        print(f"Failed sales threshold check: {sales_threshold_fails} ({sales_threshold_fails/total_records*100:.1f}%)")
    
    def _has_required_brand_keyword(self, record: SalesRecord) -> bool:
        """Check if product name or brand contains required brand keywords"""
        brand_keywords = ["nike", "air jordan", "adidas", "yeezy", "new balance", "asics", "puma"]
        product_name = (record.product_name or "").lower()
        brand = (record.brand or "").lower()
        
        for keyword in brand_keywords:
            if keyword in product_name or keyword in brand:
                return True
        return False
    
    def _has_excluded_tags(self, record: SalesRecord) -> bool:
        """Check if product has any excluded tags"""
        if not record.tags:
            return False
        
        tags_lower = [tag.lower().strip() for tag in record.tags]
        return "retail" in tags_lower
    
    def run_analysis(self):
        """Run complete analysis without making any changes"""
        print("🚀 STARTING AIRTABLE ANALYSIS (READ-ONLY)")
        print("=" * 60)
        
        try:
            # Analyze table structure
            structure = self.analyze_table_structure()
            
            # Fetch and display sales data
            sales_records = self.fetch_and_display_sales_data()
            
            if not sales_records:
                print("\n❌ No sales records found")
                return
            
            # Analyze filtering results
            filtered_products = self.analyze_filtering_results(sales_records)
            
            print(f"\n✅ ANALYSIS COMPLETE!")
            print("=" * 50)
            print(f"📊 Total records: {len(sales_records)}")
            print(f"🎯 Qualifying products: {len(filtered_products) if filtered_products else 0}")
            print("🔒 No Shopify changes made")
            
        except Exception as e:
            print(f"\n💥 Analysis failed: {e}")
            logger.error(f"Analysis failed: {e}")

def main():
    """Main entry point"""
    try:
        analyzer = AirtableAnalyzer()
        analyzer.run_analysis()
            
    except KeyboardInterrupt:
        print("\n⏹️  Analysis interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()