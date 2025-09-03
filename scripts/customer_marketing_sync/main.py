#!/usr/bin/env python3
"""
Customer Marketing Sync Script

Automatically synchronizes Shopify customer email marketing subscription preferences 
to their custom.accepts_marketing metafields with change detection optimization.

Features:
- Only updates customers with marketing preference changes
- Local JSON cache for change detection
- Concurrent customer updates
- Comprehensive logging and error handling
- Manual and scheduled execution modes

Required environment variables:
- SHOPIFY_ADMIN_TOKEN
- SHOPIFY_SHOP_DOMAIN
"""

import os
import sys
import logging
import signal
from datetime import datetime
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.executors.pool import ThreadPoolExecutor
import atexit

# Add project root to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from shared.shopify_client import ShopifyClient
from shared.logger import setup_logger
from scripts.customer_marketing_sync.customer_manager import CustomerManager
from scripts.customer_marketing_sync.storage import CustomerMarketingStorage

# Load environment variables
load_dotenv()

# Set up logging
logger = setup_logger('customer_marketing_sync', 'customer_marketing_sync.log')

# Also configure related loggers to use the same settings
related_loggers = [
    'shared.shopify_client',
    'scripts.customer_marketing_sync.customer_manager',
    'scripts.customer_marketing_sync.storage'
]

for logger_name in related_loggers:
    child_logger = logging.getLogger(logger_name)
    child_logger.setLevel(logging.INFO)
    # Copy handlers from main logger
    for handler in logger.handlers:
        child_logger.addHandler(handler)

class CustomerMarketingSyncOrchestrator:
    """Main orchestrator for customer marketing synchronization with scheduling capabilities"""
    
    def __init__(self):
        # Configuration from environment variables
        self.shopify_admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
        self.shopify_shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")
        
        # Sync configuration
        self.sync_interval_hours = int(os.getenv("CUSTOMER_MARKETING_SYNC_INTERVAL_HOURS", "6"))
        self.dry_run_mode = os.getenv("CUSTOMER_MARKETING_SYNC_DRY_RUN", "false").lower() == "true"
        
        # Initialize components
        self.shopify_client = None
        self.customer_manager = None
        self.storage = None
        self.scheduler = None
        
        # Track running state
        self.is_running = False
        self.sync_in_progress = False
    
    def validate_environment(self) -> bool:
        """Validate that all required configuration is present"""
        required_configs = [
            ("SHOPIFY_ADMIN_TOKEN", self.shopify_admin_token),
            ("SHOPIFY_SHOP_DOMAIN", self.shopify_shop_domain)
        ]
        
        missing_configs = []
        for name, value in required_configs:
            if not value or value in ["your_token_here", "your_shop", ""]:
                missing_configs.append(name)
        
        if missing_configs:
            logger.error(f"❌ Missing required environment variables: {', '.join(missing_configs)}")
            logger.error("Please create a .env file with all required variables.")
            return False
        
        logger.info("✅ Environment validation passed")
        logger.info(f"🔧 Configuration:")
        logger.info(f"  - Sync interval: {self.sync_interval_hours} hours")
        logger.info(f"  - Dry run mode: {self.dry_run_mode}")
        logger.info(f"  - Shop domain: {self.shopify_shop_domain}")
        
        return True
    
    def initialize_components(self) -> bool:
        """Initialize Shopify client, storage, and customer manager"""
        try:
            # Initialize clients
            self.shopify_client = ShopifyClient(self.shopify_admin_token, self.shopify_shop_domain)
            self.storage = CustomerMarketingStorage()
            # Allow creating missing metafields by default to handle customers without metafield values
            self.customer_manager = CustomerManager(self.shopify_client, self.storage, create_missing_metafields=True)
            
            logger.info("✅ Components initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize components: {e}")
            return False
    
    def run_single_sync(self, dry_run: Optional[bool] = None) -> Dict[str, Any]:
        """Run a single customer marketing synchronization"""
        if self.sync_in_progress:
            logger.warning("⚠️  Sync already in progress, skipping...")
            return {"success": False, "message": "Sync already in progress"}
        
        try:
            self.sync_in_progress = True
            sync_dry_run = dry_run if dry_run is not None else self.dry_run_mode
            
            logger.info("🚀 Starting customer marketing synchronization...")
            if sync_dry_run:
                logger.info("🧪 Running in DRY RUN mode - no changes will be made")
            
            # Run the sync
            result = self.customer_manager.sync_customer_marketing_to_metafields(dry_run=sync_dry_run)
            
            # Log detailed results
            if result.success:
                logger.info("✅ Customer marketing synchronization completed successfully!")
                logger.info(f"📊 Results summary:")
                logger.info(f"  - Customers processed: {result.total_customers_processed}")
                logger.info(f"  - Customers updated: {result.customers_updated}")
                logger.info(f"  - Customers failed: {result.customers_failed}")
                logger.info(f"  - Customers with changes: {result.customers_with_changes}")
                logger.info(f"  - Execution time: {result.execution_time_seconds:.2f} seconds")
                
                if result.errors:
                    logger.warning(f"⚠️  Encountered {len(result.errors)} errors:")
                    for error in result.errors:
                        logger.warning(f"    - {error}")
            else:
                logger.error("❌ Customer marketing synchronization failed!")
                if result.errors:
                    for error in result.errors:
                        logger.error(f"  💥 {error}")
            
            return {
                "success": result.success,
                "sync_result": result,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"💥 Unexpected error during sync: {e}")
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
            
        finally:
            self.sync_in_progress = False
    
    def setup_scheduler(self) -> bool:
        """Setup the APScheduler for automatic syncing"""
        try:
            # Configure scheduler with thread pool executor
            executors = {
                'default': ThreadPoolExecutor(max_workers=1)  # Single worker to prevent concurrent syncs
            }
            
            job_defaults = {
                'coalesce': True,  # Combine multiple pending executions into one
                'max_instances': 1,  # Only allow one instance of the job at a time
                'misfire_grace_time': 300  # Allow 5 minutes grace time for missed executions
            }
            
            self.scheduler = BlockingScheduler(executors=executors, job_defaults=job_defaults)
            
            # Add the customer marketing sync job
            self.scheduler.add_job(
                func=self._scheduled_sync_job,
                trigger=IntervalTrigger(hours=self.sync_interval_hours),
                id='customer_marketing_sync_job',
                name=f'Customer Marketing Sync (every {self.sync_interval_hours}h)',
                replace_existing=True
            )
            
            logger.info(f"📅 Scheduler configured for {self.sync_interval_hours}-hour intervals")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to setup scheduler: {e}")
            return False
    
    def _scheduled_sync_job(self):
        """Job function called by the scheduler"""
        logger.info("⏰ Scheduled sync triggered")
        result = self.run_single_sync()
        
        if result["success"]:
            logger.info("✅ Scheduled sync completed successfully")
        else:
            logger.error("❌ Scheduled sync failed")
    
    def start_scheduled_mode(self):
        """Start the scheduler to run syncs automatically"""
        try:
            logger.info("🔄 Starting scheduled customer marketing sync mode...")
            logger.info(f"📅 Will sync every {self.sync_interval_hours} hours")
            logger.info("⏹️  Press Ctrl+C to stop")
            
            self.is_running = True
            
            # Setup signal handlers for graceful shutdown
            def signal_handler(signum, frame):
                logger.info("\n⏹️  Shutdown signal received, stopping scheduler...")
                self.stop_scheduler()
                sys.exit(0)
            
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            
            # Run an initial sync
            logger.info("🚀 Running initial sync...")
            self.run_single_sync()
            
            # Start the scheduler
            self.scheduler.start()
            
        except KeyboardInterrupt:
            logger.info("\n⏹️  Keyboard interrupt received")
            self.stop_scheduler()
        except Exception as e:
            logger.error(f"💥 Scheduler failed: {e}")
            self.stop_scheduler()
    
    def stop_scheduler(self):
        """Stop the scheduler gracefully"""
        try:
            if self.scheduler and self.scheduler.running:
                logger.info("⏹️  Stopping scheduler...")
                self.scheduler.shutdown(wait=True)
                logger.info("✅ Scheduler stopped")
            self.is_running = False
        except Exception as e:
            logger.error(f"❌ Error stopping scheduler: {e}")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current status of the customer marketing sync system"""
        try:
            status = {
                "is_running": self.is_running,
                "sync_in_progress": self.sync_in_progress,
                "configuration": {
                    "sync_interval_hours": self.sync_interval_hours,
                    "dry_run_mode": self.dry_run_mode,
                    "shop_domain": self.shopify_shop_domain
                },
                "scheduler_status": {
                    "running": self.scheduler.running if self.scheduler else False,
                    "jobs": len(self.scheduler.get_jobs()) if self.scheduler else 0
                } if self.scheduler else None
            }
            
            # Add customer manager status if available
            if self.customer_manager:
                status["customer_status"] = self.customer_manager.get_sync_status()
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting status: {e}")
            return {"error": str(e)}

def run_customer_marketing_sync(mode: str = "manual", dry_run: bool = False) -> bool:
    """
    Entry point for customer marketing sync script
    
    Args:
        mode: "manual" for one-time sync, "scheduled" for continuous mode
        dry_run: Whether to run in dry-run mode (analysis only)
    
    Returns:
        Boolean indicating success
    """
    try:
        orchestrator = CustomerMarketingSyncOrchestrator()
        
        # Validate environment
        if not orchestrator.validate_environment():
            return False
        
        # Initialize components
        if not orchestrator.initialize_components():
            return False
        
        if mode == "scheduled":
            # Setup and start scheduler
            if not orchestrator.setup_scheduler():
                return False
            
            orchestrator.start_scheduled_mode()
            return True
            
        else:  # manual mode
            # Run single sync
            result = orchestrator.run_single_sync(dry_run=dry_run)
            return result["success"]
            
    except KeyboardInterrupt:
        print("\n⏹️  Process interrupted by user")
        return False
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        return False

if __name__ == "__main__":
    # Support command line arguments
    import argparse
    
    parser = argparse.ArgumentParser(description='Shopify Customer Marketing Sync Script')
    parser.add_argument('--mode', choices=['manual', 'scheduled'], default='manual',
                      help='Execution mode (default: manual)')
    parser.add_argument('--dry-run', action='store_true',
                      help='Run in dry-run mode (analyze only, no changes)')
    
    args = parser.parse_args()
    
    success = run_customer_marketing_sync(mode=args.mode, dry_run=args.dry_run)
    sys.exit(0 if success else 1)