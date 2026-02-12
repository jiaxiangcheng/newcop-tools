from abc import ABC, abstractmethod
import logging
from typing import Dict, Any, List
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.job_dynamic_collections.models import ProductsBySalesJobSettings, GetProductsWithTotalStockJobSettings, SalesRecord, CollectionWithJobSettings, FilteredProduct
from shared.airtable_client import AirtableClient
from shared.shopify_client import ShopifyClient
from scripts.job_dynamic_collections.product_filter import ProductFilter

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class JobExecutor(ABC):
    """Abstract base class for job executors"""
    
    def __init__(self, airtable_token: str, shopify_client: ShopifyClient, dry_run: bool = False):
        self.airtable_token = airtable_token
        self.shopify_client = shopify_client
        self.dry_run = dry_run
    
    @abstractmethod
    def execute(self, collection_with_settings: CollectionWithJobSettings) -> Dict[str, Any]:
        """Execute the job for the given collection and settings"""
        pass
    
    @abstractmethod
    def get_supported_job_type(self) -> str:
        """Return the job type this executor supports"""
        pass

class ProductsBySalesJobExecutor(JobExecutor):
    """Executor for getProductsBySales job type"""

    def get_supported_job_type(self) -> str:
        return "getProductsBySales"

    def execute(self, collection_with_settings: CollectionWithJobSettings) -> Dict[str, Any]:
        """Execute the products by sales job"""
        settings = collection_with_settings.job_settings
        collection_id = collection_with_settings.collection_id
        collection_title = collection_with_settings.collection_title

        if not isinstance(settings, ProductsBySalesJobSettings):
            raise ValueError(f"Invalid job settings type for ProductsBySalesJobExecutor: {type(settings)}")

        logger.info(f"Starting getProductsBySales job for collection '{collection_title}' (ID: {collection_id})")
        
        try:
            # Initialize clients with dynamic configuration from job settings
            airtable_client = AirtableClient(self.airtable_token, settings.AIRTABLE_BASE_ID)
            product_filter = ProductFilter(
                brand_keywords=settings.BRAND_KEYWORDS,
                excluded_brand_keywords=settings.EXCLUDED_BRAND_KEYWORDS,
                excluded_tags=settings.EXCLUDED_TAGS,
                included_tags=settings.INCLUDED_TAGS,
                min_sales_threshold=settings.MIN_SALES_THRESHOLD,
                sales_period=settings.SALES_PERIOD
            )
            
            # Fetch and process sales data from Airtable
            logger.info("Fetching Spain sales data from Airtable...")
            raw_records = airtable_client.get_spain_sales_data(
                settings.AIRTABLE_TABLE_ID,
                settings.AIRTABLE_VIEW_ID,
                max_records=settings.MAX_AIRTABLE_RECORDS
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
            
            if not sales_records:
                logger.warning("No sales records found")
                return {"success": False, "message": "No sales records found", "collection_id": collection_id}
            
            # Filter products based on criteria with newcop exception logic
            logger.info(f"🔍 Filtering {len(sales_records)} products based on brand, tags, and sales criteria...")
            filtered_products = product_filter.filter_products_with_newcop_exception(sales_records)
            logger.info(f"✅ Filtering completed: {len(filtered_products)}/{len(sales_records)} products passed criteria")
            
            if not filtered_products:
                logger.warning("No products passed filtering criteria")
                return {"success": False, "message": "No products passed filtering criteria", "collection_id": collection_id}
            
            # Update Shopify collection or simulate in dry run mode
            if self.dry_run:
                logger.info(f"🧪 DRY RUN: Would update Shopify collection {collection_id} with {len(filtered_products)} products")
                update_result = {
                    "success": True,
                    "message": f"DRY RUN: Would update collection with {len(filtered_products)} products",
                    "dry_run": True,
                    "added_count": len(filtered_products),
                    "failed_count": 0,
                    "products_preview": [f"{p.product_name} (ID: {p.shopify_id})" for p in filtered_products[:5]],
                    "total_products": len(filtered_products)
                }
            else:
                logger.info(f"🚀 Starting Shopify collection update for {collection_id}...")
                logger.info(f"📊 Will update collection with {len(filtered_products)} products")
                update_result = self.shopify_client.update_collection_with_filtered_products(
                    collection_id,
                    filtered_products
                )
                if update_result.get("success"):
                    logger.info(f"✅ Collection update completed successfully!")
                    print(f"✅ Collection update completed successfully!")
                else:
                    logger.warning(f"⚠️  Collection update completed with some issues")
                    print(f"⚠️  Collection update completed with some issues")
            
            # Prepare final result
            result = {
                "success": True,
                "job_type": self.get_supported_job_type(),
                "collection_id": collection_id,
                "collection_title": collection_title,
                "total_sales_records": len(sales_records),
                "filtered_products_count": len(filtered_products),
                "shopify_update_result": update_result,
                "job_settings": {
                    "AIRTABLE_BASE_ID": settings.AIRTABLE_BASE_ID,
                    "AIRTABLE_TABLE_ID": settings.AIRTABLE_TABLE_ID,
                    "AIRTABLE_VIEW_ID": settings.AIRTABLE_VIEW_ID,
                    "MAX_AIRTABLE_RECORDS": settings.MAX_AIRTABLE_RECORDS,
                    "UPDATE_FREQUENCY_HOURS": settings.UPDATE_FREQUENCY_HOURS
                }
            }
            
            logger.info(f"Products by sales job completed successfully for collection {collection_id}")
            return result

        except Exception as e:
            logger.error(f"Products by sales job failed for collection {collection_id}: {e}")
            return {
                "success": False, 
                "error": str(e), 
                "job_type": self.get_supported_job_type(),
                "collection_id": collection_id,
                "collection_title": collection_title
            }

class GetProductsWithTotalStockJobExecutor(JobExecutor):
    """
    Job executor for getProductsWithAtLeastTotalStock job type.

    Filters products based on total stock quantity from Airtable with
    tag-based and brand-based filtering.
    """

    def get_supported_job_type(self) -> str:
        return "getProductsWithAtLeastTotalStock"

    def execute(self, collection_with_settings: CollectionWithJobSettings) -> Dict[str, Any]:
        """
        Execute the total stock filtering job.

        Args:
            collection_with_settings: Collection configuration with job settings

        Returns:
            Dictionary with execution results including success status,
            filtered product count, and skipped products
        """
        collection = collection_with_settings.collection_title
        collection_id = collection_with_settings.collection_id
        settings = collection_with_settings.job_settings

        # Validate settings type
        if not isinstance(settings, GetProductsWithTotalStockJobSettings):
            raise ValueError(f"Invalid settings type. Expected GetProductsWithTotalStockJobSettings, got {type(settings)}")

        logger.info(f"\n{'='*60}")
        logger.info(f"🔍 Processing Collection: {collection}")
        logger.info(f"📋 Job Type: {settings.jobType}")
        logger.info(f"📊 Min Total Quantity: {settings.MIN_TOTAL_QUANTITY}")
        logger.info(f"{'='*60}\n")

        try:
            # Initialize Airtable client with collection-specific settings
            airtable_client = AirtableClient(
                token=self.airtable_token,
                base_id=settings.AIRTABLE_BASE_ID
            )

            # Fetch records from Airtable
            logger.info(f"📥 Fetching records from Airtable (max: {settings.MAX_AIRTABLE_RECORDS})...")
            raw_records = airtable_client.get_records(
                table_id=settings.AIRTABLE_TABLE_ID,
                view_id=settings.AIRTABLE_VIEW_ID,
                max_records=settings.MAX_AIRTABLE_RECORDS
            )
            logger.info(f"✅ Fetched {len(raw_records)} records from Airtable")

            # Convert to SalesRecord objects
            sales_records = []
            conversion_errors = []

            for raw_record in raw_records:
                try:
                    sales_record = SalesRecord.from_airtable_record(raw_record)
                    sales_records.append(sales_record)
                except Exception as e:
                    conversion_errors.append(f"Record {raw_record.get('id', 'unknown')}: {str(e)}")

            if conversion_errors:
                logger.warning(f"⚠️  {len(conversion_errors)} records failed conversion")
                for error in conversion_errors[:5]:  # Show first 5 errors
                    logger.warning(f"  - {error}")

            logger.info(f"✅ Converted {len(sales_records)} records to SalesRecord objects")

            # Filter products using ProductFilter
            product_filter = ProductFilter()
            filtered_records, skipped_products = product_filter.filter_products_by_total_stock(
                records=sales_records,
                min_total_quantity=settings.MIN_TOTAL_QUANTITY,
                excluded_tags=settings.EXCLUDED_TAGS,
                included_tags=settings.INCLUDED_TAGS,
                brand_keywords=settings.BRAND_KEYWORDS,
                excluded_brand_keywords=settings.EXCLUDED_BRAND_KEYWORDS
            )

            logger.info(f"\n📊 Filtering Results:")
            logger.info(f"  Total records fetched: {len(sales_records)}")
            logger.info(f"  Products passed filter: {len(filtered_records)}")
            logger.info(f"  Products skipped (no Shopify ID): {len(skipped_products)}")

            if skipped_products:
                logger.warning(f"\n⚠️  {len(skipped_products)} products skipped due to missing Shopify IDs:")
                for i, product_name in enumerate(skipped_products[:10], 1):
                    logger.warning(f"  {i}. {product_name}")
                if len(skipped_products) > 10:
                    logger.warning(f"  ... and {len(skipped_products) - 10} more")

            # Convert SalesRecord to FilteredProduct for collection update
            filtered_products = []
            for record in filtered_records:
                filtered_product = FilteredProduct(
                    record_id=record.record_id,
                    product_name=record.product_name or "",
                    brand=record.brand or "",
                    quarterly_sales=record.quarterly_sales,
                    monthly_sales=record.monthly_sales,
                    total_sales=record.total_sales,
                    tags=record.tags or [],
                    shopify_id=record.shopify_id,
                    sales_period="QUARTERLY"  # Default for total stock job
                )
                filtered_products.append(filtered_product)

            # Update Shopify collection
            if self.dry_run:
                logger.info(f"\n🧪 DRY RUN MODE - No changes will be made to collection")
                logger.info(f"Would add {len(filtered_products)} products to collection '{collection}'")
            else:
                logger.info(f"\n🔄 Updating Shopify collection '{collection}'...")
                self.shopify_client.update_collection_with_filtered_products(
                    collection_id=collection_id,
                    filtered_products=filtered_products
                )
                logger.info(f"✅ Collection updated successfully with {len(filtered_products)} products")

            return {
                'success': True,
                'job_type': self.get_supported_job_type(),
                'collection_id': collection_id,
                'collection_title': collection,
                'total_fetched': len(sales_records),
                'total_filtered': len(filtered_records),
                'total_skipped': len(skipped_products),
                'skipped_products': skipped_products
            }

        except Exception as e:
            logger.error(f"❌ Error processing collection '{collection}': {str(e)}")
            logger.exception(e)
            return {
                'success': False,
                'job_type': self.get_supported_job_type(),
                'collection_id': collection_id,
                'collection_title': collection,
                'error': str(e)
            }

class JobExecutorFactory:
    """Factory for creating job executors"""
    
    def __init__(self, airtable_token: str, shopify_client: ShopifyClient, dry_run: bool = False):
        self.airtable_token = airtable_token
        self.shopify_client = shopify_client
        self.dry_run = dry_run
        self._executors = {}
        self._register_executors()
    
    def _register_executors(self):
        """Register available job executors"""
        executors = [
            ProductsBySalesJobExecutor(self.airtable_token, self.shopify_client, self.dry_run),
            GetProductsWithTotalStockJobExecutor(self.airtable_token, self.shopify_client, self.dry_run),
            # Future executors can be added here
        ]

        for executor in executors:
            self._executors[executor.get_supported_job_type()] = executor
    
    def get_executor(self, job_type: str) -> JobExecutor:
        """Get executor for the given job type"""
        executor = self._executors.get(job_type)
        if not executor:
            raise ValueError(f"No executor found for job type: {job_type}")
        return executor
    
    def get_supported_job_types(self) -> List[str]:
        """Get list of supported job types"""
        return list(self._executors.keys())