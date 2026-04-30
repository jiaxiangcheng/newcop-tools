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
from typing import Dict, Any, List, Optional
from datetime import datetime
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.executors.pool import ThreadPoolExecutor

# Add project root to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from shared.shopify_client import ShopifyClient
from shared.logger import setup_logger
from scripts.job_dynamic_collections.models import CollectionWithJobSettings
from scripts.job_dynamic_collections.job_executor import JobExecutorFactory

# Load environment variables
load_dotenv()

# Set up logging
logger = setup_logger('dynamic_collections', 'dynamic_collection.log')

# Also configure related loggers to use the same settings
related_loggers = [
    'shared.shopify_client',
    'scripts.job_dynamic_collections.job_executor',
    'scripts.job_dynamic_collections.product_filter'
]

for logger_name in related_loggers:
    child_logger = logging.getLogger(logger_name)
    child_logger.setLevel(logging.INFO)
    # Copy handlers from main logger
    for handler in logger.handlers:
        child_logger.addHandler(handler)

class DynamicCollectionManager:
    """Main class to orchestrate dynamic collection jobs across multiple collections"""
    
    def __init__(self, dry_run: bool = False):
        # Configuration from environment variables
        self.airtable_token = os.getenv("AIRTABLE_TOKEN")
        self.shopify_admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
        self.shopify_shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")
        self.dry_run = dry_run
        
        # Scheduling configuration
        self.sync_interval_days = int(os.getenv("DYNAMIC_COLLECTIONS_INTERVAL_DAYS", "1"))
        
        # Initialize clients
        self.shopify_client = ShopifyClient(self.shopify_admin_token, self.shopify_shop_domain)
        self.job_executor_factory = JobExecutorFactory(self.airtable_token, self.shopify_client, self.dry_run)
        
        # Scheduler components
        self.scheduler = None
        self.is_running = False
    
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
        logger.info(f"🔧 Configuration:")
        logger.info(f"  - Dry run mode: {self.dry_run}")
        logger.info(f"  - Sync interval: {self.sync_interval_days} days")
        logger.info(f"  - Shop domain: {self.shopify_shop_domain}")
        return True
    
    def discover_collections_with_jobs(self, collection_id: Optional[str] = None) -> List[CollectionWithJobSettings]:
        """Discover collections that have job_settings metafields configured

        Args:
            collection_id: Optional collection ID to filter for a specific collection
        """
        try:
            collections_with_jobs = []

            # Optimized path: If collection_id is specified, directly fetch that single collection
            if collection_id:
                logger.info(f"🎯 Fetching specific collection ID: {collection_id}...")

                collection_data = self.shopify_client.get_collection_with_job_settings(collection_id)

                if collection_data:
                    try:
                        collection_with_settings = CollectionWithJobSettings.from_shopify_collection_and_job_data(
                            collection_data["collection"],
                            collection_data["job_settings"]
                        )
                        collections_with_jobs.append(collection_with_settings)
                        logger.info(f"✅ Found collection '{collection_with_settings.collection_title}' with job type '{collection_with_settings.job_settings.jobType}'")
                    except Exception as e:
                        collection_title = collection_data["collection"].get("title", "Unknown")
                        logger.error(f"Failed to parse job settings for collection '{collection_title}': {e}")
                else:
                    logger.error(f"❌ Collection with ID '{collection_id}' not found or has no job settings configured")

            # Default path: Scan all collections
            else:
                logger.info("Discovering all collections with job settings...")
                collections_with_jobs_data = self.shopify_client.get_collections_with_job_settings()

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
        
        logger.info(f"🎯 Executing job '{job_type}' for collection '{collection_title}'...")
        logger.info(f"📋 Collection ID: {collection_with_settings.collection_id}")
        
        try:
            # Get appropriate executor for the job type
            executor = self.job_executor_factory.get_executor(job_type)
            
            # Execute the job
            logger.info(f"▶️  Starting job execution...")
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

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        try:
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
        except KeyboardInterrupt:
            logger.info("⏹️  Interrupted, cancelling remaining jobs...")
            for future in future_to_collection:
                future.cancel()
            raise
        finally:
            executor.shutdown(wait=False)

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
    
    def run(self, collection_id: Optional[str] = None) -> Dict[str, Any]:
        """Run the complete dynamic collection management process

        Args:
            collection_id: Optional collection ID to run a specific collection only
        """
        if collection_id:
            logger.info(f"Starting dynamic collection management for collection ID: {collection_id}...")
        else:
            logger.info("Starting dynamic collection management process...")

        try:
            # Validate environment
            if not self.validate_environment():
                raise ValueError("Environment validation failed")

            # Discover collections with job settings
            collections_with_jobs = self.discover_collections_with_jobs(collection_id=collection_id)

            if not collections_with_jobs:
                if collection_id:
                    logger.warning(f"Collection '{collection_id}' not found or has no job settings configured")
                else:
                    logger.warning("No collections found with job settings configured")
                return {
                    "success": False,
                    "message": f"Collection '{collection_id}' not found" if collection_id else "No collections found with job settings configured",
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

        except KeyboardInterrupt:
            logger.info("⏹️  Dynamic collection management interrupted by user")
            raise
        except Exception as e:
            logger.error(f"Dynamic collection management failed: {e}")
            return {"success": False, "error": str(e)}
    
    def setup_scheduler(self) -> bool:
        """Setup the APScheduler for automatic collection updates"""
        try:
            # Configure scheduler with thread pool executor
            executors = {
                'default': ThreadPoolExecutor(max_workers=1)  # Single worker to prevent concurrent syncs
            }
            
            job_defaults = {
                'coalesce': True,  # Combine multiple pending executions into one
                'max_instances': 1,  # Only allow one instance of the job at a time
                'misfire_grace_time': 900  # Allow 15 minutes grace time for missed executions
            }
            
            self.scheduler = BlockingScheduler(executors=executors, job_defaults=job_defaults)
            
            # Add the dynamic collections job
            self.scheduler.add_job(
                func=self._scheduled_sync_job,
                trigger=IntervalTrigger(days=self.sync_interval_days),
                id='dynamic_collections_job',
                name=f'Dynamic Collections (every {self.sync_interval_days} days)',
                replace_existing=True
            )
            
            logger.info(f"📅 Scheduler configured for {self.sync_interval_days}-day intervals")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to setup scheduler: {e}")
            return False
    
    def _scheduled_sync_job(self):
        """Job function called by the scheduler"""
        logger.info("⏰ Scheduled dynamic collections sync triggered")
        result = self.run()
        
        if result["success"]:
            logger.info("✅ Scheduled dynamic collections sync completed successfully")
        else:
            logger.error("❌ Scheduled dynamic collections sync failed")
    
    def start_scheduled_mode(self):
        """Start the scheduler to run dynamic collections automatically"""
        try:
            logger.info("🔄 Starting scheduled dynamic collections mode...")
            logger.info(f"📅 Will sync every {self.sync_interval_days} days")
            logger.info("⏹️  Press Ctrl+C to stop")

            self.is_running = True

            # Run an initial sync
            logger.info("🚀 Running initial sync...")
            self.run()

            # Start the scheduler (blocks until shutdown)
            self.scheduler.start()

        except (KeyboardInterrupt, SystemExit):
            logger.info("\n⏹️  Shutdown signal received, stopping scheduler...")
            self.stop_scheduler()
        except Exception as e:
            logger.error(f"💥 Scheduler failed: {e}")
            self.stop_scheduler()
    
    def stop_scheduler(self):
        """Stop the scheduler immediately"""
        try:
            if self.scheduler and self.scheduler.running:
                logger.info("⏹️  Stopping scheduler...")
                self.scheduler.shutdown(wait=False)
                logger.info("✅ Scheduler stopped")
            self.is_running = False
        except Exception as e:
            logger.error(f"❌ Error stopping scheduler: {e}")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current status of the dynamic collections system"""
        try:
            status = {
                "is_running": self.is_running,
                "configuration": {
                    "sync_interval_days": self.sync_interval_days,
                    "dry_run_mode": self.dry_run,
                    "shop_domain": self.shopify_shop_domain
                },
                "scheduler_status": {
                    "running": self.scheduler.running if self.scheduler else False,
                    "jobs": len(self.scheduler.get_jobs()) if self.scheduler else 0
                } if self.scheduler else None
            }
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting status: {e}")
            return {"error": str(e)}

def run_dynamic_collections(mode: str = "manual", dry_run: bool = False, collection_id: Optional[str] = None) -> bool:
    """
    Entry point for dynamic collections script

    Args:
        mode: "manual" for one-time sync, "scheduled" for continuous mode
        dry_run: Whether to run in dry-run mode (analysis only)
        collection_id: Optional collection ID to run a specific collection only

    Returns:
        Boolean indicating success
    """
    try:
        manager = DynamicCollectionManager(dry_run=dry_run)

        # Validate environment
        if not manager.validate_environment():
            return False

        if mode == "scheduled":
            if collection_id:
                logger.warning("Collection ID filtering is not supported in scheduled mode. Ignoring collection_id parameter.")

            # Setup and start scheduler
            if not manager.setup_scheduler():
                return False

            manager.start_scheduled_mode()
            return True

        else:  # manual mode
            # Run single sync
            if dry_run:
                logger.info("🧪 Running in DRY RUN mode - no changes will be made")

            if collection_id:
                logger.info(f"🎯 Running for specific collection ID: {collection_id}")

            result = manager.run(collection_id=collection_id)

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
                return False

            return True

    except KeyboardInterrupt:
        print("\n⏹️  Process interrupted by user")
        return False
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        return False

if __name__ == "__main__":
    # Support command line arguments
    import argparse

    parser = argparse.ArgumentParser(description='Dynamic Collections Management Script')
    parser.add_argument('--mode', choices=['manual', 'scheduled'], default='manual',
                      help='Execution mode (default: manual)')
    parser.add_argument('--dry-run', action='store_true',
                      help='Run in dry-run mode (analyze only, no changes)')
    parser.add_argument('--collection-id', type=str, default=None,
                      help='Specific collection ID to run (manual mode only)')

    args = parser.parse_args()

    success = run_dynamic_collections(mode=args.mode, dry_run=args.dry_run, collection_id=args.collection_id)
    sys.exit(0 if success else 1)