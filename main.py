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
import concurrent.futures
from typing import Dict, Any, List
from dotenv import load_dotenv

from shopify_client import ShopifyClient
from models import CollectionWithJobSettings
from job_executor import JobExecutorFactory

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

class DynamicCollectionManager:
    """Main class to orchestrate dynamic collection jobs across multiple collections"""
    
    def __init__(self):
        # Configuration from environment variables
        self.airtable_token = os.getenv("AIRTABLE_TOKEN")
        self.shopify_admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
        self.shopify_shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")
        
        # Initialize clients
        self.shopify_client = ShopifyClient(self.shopify_admin_token, self.shopify_shop_domain)
        self.job_executor_factory = JobExecutorFactory(self.airtable_token, self.shopify_client)
    
    def validate_environment(self) -> bool:
        """Validate that all required configuration is present"""
        required_configs = [
            ("AIRTABLE_TOKEN", self.airtable_token),
            ("SHOPIFY_ADMIN_TOKEN", self.shopify_admin_token),
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
        logger.info(f"Supported job types: {self.job_executor_factory.get_supported_job_types()}")
        return True
    
    def discover_collections_with_jobs(self) -> List[CollectionWithJobSettings]:
        """Discover collections that have job_settings metafields configured"""
        logger.info("Discovering collections with job settings...")
        
        try:
            collections_with_jobs_data = self.shopify_client.get_collections_with_job_settings()
            collections_with_jobs = []
            
            for collection_data in collections_with_jobs_data:
                try:
                    collection_with_settings = CollectionWithJobSettings.from_shopify_collection_and_job_data(
                        collection_data["collection"],
                        collection_data["job_settings"]
                    )
                    collections_with_jobs.append(collection_with_settings)
                    logger.info(f"Found collection '{collection_with_settings.collection_title}' with job type '{collection_with_settings.job_settings.jobType}'")
                except Exception as e:
                    collection_title = collection_data["collection"].get("title", "Unknown")
                    logger.warning(f"Failed to parse job settings for collection '{collection_title}': {e}")
                    continue
            
            logger.info(f"Discovered {len(collections_with_jobs)} collections with valid job settings")
            return collections_with_jobs
            
        except Exception as e:
            logger.error(f"Failed to discover collections with jobs: {e}")
            raise
    
    def execute_collection_job(self, collection_with_settings: CollectionWithJobSettings) -> Dict[str, Any]:
        """Execute a job for a specific collection"""
        job_type = collection_with_settings.job_settings.jobType
        collection_title = collection_with_settings.collection_title
        
        logger.info(f"Executing job '{job_type}' for collection '{collection_title}'...")
        
        try:
            # Get appropriate executor for the job type
            executor = self.job_executor_factory.get_executor(job_type)
            
            # Execute the job
            result = executor.execute(collection_with_settings)
            
            if result.get("success"):
                logger.info(f"Job '{job_type}' completed successfully for collection '{collection_title}'")
            else:
                logger.error(f"Job '{job_type}' failed for collection '{collection_title}': {result.get('error', result.get('message', 'Unknown error'))}")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to execute job '{job_type}' for collection '{collection_title}': {e}")
            return {
                "success": False,
                "error": str(e),
                "job_type": job_type,
                "collection_id": collection_with_settings.collection_id,
                "collection_title": collection_title
            }
    
    def execute_all_collection_jobs(self, collections_with_jobs: List[CollectionWithJobSettings]) -> List[Dict[str, Any]]:
        """Execute jobs for all collections using parallel processing"""
        logger.info(f"Executing jobs for {len(collections_with_jobs)} collections using parallel processing...")
        
        results = []
        successful_jobs = 0
        
        # Use ThreadPoolExecutor to process collections in parallel
        max_workers = min(len(collections_with_jobs), 3)  # Limit to 3 concurrent threads to avoid rate limits
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all jobs
            future_to_collection = {
                executor.submit(self.execute_collection_job, collection): collection 
                for collection in collections_with_jobs
            }
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_collection):
                collection = future_to_collection[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result.get("success"):
                        successful_jobs += 1
                        logger.info(f"✅ Collection '{collection.collection_title}' completed successfully")
                    else:
                        logger.error(f"❌ Collection '{collection.collection_title}' failed: {result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    logger.error(f"❌ Collection '{collection.collection_title}' encountered exception: {e}")
                    results.append({
                        "success": False,
                        "error": str(e),
                        "collection_id": collection.collection_id,
                        "collection_title": collection.collection_title
                    })
        
        logger.info(f"Completed {successful_jobs}/{len(collections_with_jobs)} jobs successfully")
        return results
    
    def print_job_execution_summary(self, results: List[Dict[str, Any]]) -> None:
        """Print a summary of job execution results"""
        successful_jobs = [r for r in results if r.get("success")]
        failed_jobs = [r for r in results if not r.get("success")]
        
        print(f"\n📊 Job Execution Summary:")
        print(f"✅ Successful: {len(successful_jobs)}")
        print(f"❌ Failed: {len(failed_jobs)}")
        print(f"📈 Total: {len(results)}")
        
        if successful_jobs:
            print("\n✅ Successful Jobs:")
            for result in successful_jobs:
                job_type = result.get("job_type", "Unknown")
                collection_title = result.get("collection_title", "Unknown")
                products_count = result.get("filtered_products_count", 0)
                print(f"  - {collection_title}: {job_type} ({products_count} products)")
        
        if failed_jobs:
            print("\n❌ Failed Jobs:")
            for result in failed_jobs:
                collection_title = result.get("collection_title", "Unknown")
                error = result.get("error", result.get("message", "Unknown error"))
                print(f"  - {collection_title}: {error}")
    
    def run(self) -> Dict[str, Any]:
        """Run the complete dynamic collection management process"""
        logger.info("Starting dynamic collection management process...")
        
        try:
            # Validate environment
            if not self.validate_environment():
                raise ValueError("Environment validation failed")
            
            # Discover collections with job settings
            collections_with_jobs = self.discover_collections_with_jobs()
            
            if not collections_with_jobs:
                logger.warning("No collections found with job settings configured")
                return {
                    "success": True,
                    "message": "No collections found with job settings configured",
                    "collections_processed": 0,
                    "job_results": []
                }
            
            # Execute jobs for all collections
            job_results = self.execute_all_collection_jobs(collections_with_jobs)
            
            # Calculate overall success
            successful_jobs = sum(1 for result in job_results if result.get("success"))
            overall_success = successful_jobs > 0
            
            # Prepare final result
            result = {
                "success": overall_success,
                "collections_discovered": len(collections_with_jobs),
                "collections_processed": len(job_results),
                "successful_jobs": successful_jobs,
                "failed_jobs": len(job_results) - successful_jobs,
                "job_results": job_results
            }
            
            logger.info("Dynamic collection management completed!")
            logger.info(f"Final summary: {successful_jobs}/{len(job_results)} jobs successful")
            
            return result
            
        except Exception as e:
            logger.error(f"Dynamic collection management failed: {e}")
            return {"success": False, "error": str(e)}

def main():
    """Main entry point"""
    try:
        manager = DynamicCollectionManager()
        result = manager.run()
        
        if result["success"]:
            print("\n✅ Dynamic collection management completed successfully!")
            print(f"🔍 Discovered {result.get('collections_discovered', 0)} collections with job settings")
            print(f"⚙️  Processed {result.get('collections_processed', 0)} collection jobs")
            print(f"✅ Successful: {result.get('successful_jobs', 0)}")
            print(f"❌ Failed: {result.get('failed_jobs', 0)}")
            
            # Print detailed summary
            if result.get('job_results'):
                manager.print_job_execution_summary(result['job_results'])
        else:
            error_msg = result.get('error', result.get('message', 'Unknown error'))
            print(f"\n❌ Dynamic collection management failed: {error_msg}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⏹️  Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()