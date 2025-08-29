#!/usr/bin/env python3
"""
Inventory Sync Script

Automatically synchronizes Shopify product variant inventory quantities 
to their custom.inventory metafields every 2 hours with change detection optimization.

Features:
- Only updates variants with inventory changes
- Local JSON cache for change detection
- Concurrent variant updates per product
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
from scripts.inventory_sync.inventory_manager import InventoryManager
from scripts.inventory_sync.storage import InventoryStorage

# Load environment variables
load_dotenv()

# Set up logging
logger = setup_logger('inventory_sync', 'inventory_sync.log')

class InventorySyncOrchestrator:
    """Main orchestrator for inventory synchronization with scheduling capabilities"""
    
    def __init__(self):
        # Configuration from environment variables
        self.shopify_admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
        self.shopify_shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")
        
        # Sync configuration
        self.sync_interval_hours = int(os.getenv("INVENTORY_SYNC_INTERVAL_HOURS", "6"))
        self.dry_run_mode = os.getenv("INVENTORY_SYNC_DRY_RUN", "false").lower() == "true"
        
        # Initialize components
        self.shopify_client = None
        self.inventory_manager = None
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
        """Initialize Shopify client, storage, and inventory manager"""
        try:
            # Initialize clients
            self.shopify_client = ShopifyClient(self.shopify_admin_token, self.shopify_shop_domain)
            self.storage = InventoryStorage()
            self.inventory_manager = InventoryManager(self.shopify_client, self.storage)
            
            logger.info("✅ Components initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize components: {e}")
            return False
    
    def run_single_sync(self, dry_run: Optional[bool] = None) -> Dict[str, Any]:
        """Run a single inventory synchronization"""
        if self.sync_in_progress:
            logger.warning("⚠️  Sync already in progress, skipping...")
            return {"success": False, "message": "Sync already in progress"}
        
        try:
            self.sync_in_progress = True
            sync_dry_run = dry_run if dry_run is not None else self.dry_run_mode
            
            logger.info("🚀 Starting inventory synchronization...")
            if sync_dry_run:
                logger.info("🧪 Running in DRY RUN mode - no changes will be made")
            
            # Run the sync
            result = self.inventory_manager.sync_inventory_to_metafields(dry_run=sync_dry_run)
            
            # Log detailed results
            if result.success:
                logger.info("✅ Inventory synchronization completed successfully!")
                logger.info(f"📊 Results summary:")
                logger.info(f"  - Products processed: {result.total_products_processed}")
                logger.info(f"  - Variants checked: {result.total_variants_checked}")
                logger.info(f"  - Variants updated: {result.variants_updated}")
                logger.info(f"  - Variants failed: {result.variants_failed}")
                logger.info(f"  - Products with changes: {result.products_with_changes}")
                logger.info(f"  - Execution time: {result.execution_time_seconds:.2f} seconds")
                
                if result.errors:
                    logger.warning(f"⚠️  Encountered {len(result.errors)} errors:")
                    for error in result.errors:
                        logger.warning(f"    - {error}")
            else:
                logger.error("❌ Inventory synchronization failed!")
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
            
            # Add the inventory sync job
            self.scheduler.add_job(
                func=self._scheduled_sync_job,
                trigger=IntervalTrigger(hours=self.sync_interval_hours),
                id='inventory_sync_job',
                name=f'Inventory Sync (every {self.sync_interval_hours}h)',
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
            logger.info("🔄 Starting scheduled inventory sync mode...")
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
        """Get current status of the inventory sync system"""
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
            
            # Add inventory manager status if available
            if self.inventory_manager:
                status["inventory_status"] = self.inventory_manager.get_sync_status()
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting status: {e}")
            return {"error": str(e)}

def run_inventory_sync(mode: str = "manual", dry_run: bool = False) -> bool:
    """
    Entry point for inventory sync script
    
    Args:
        mode: "manual" for one-time sync, "scheduled" for continuous mode
        dry_run: Whether to run in dry-run mode (analysis only)
    
    Returns:
        Boolean indicating success
    """
    try:
        orchestrator = InventorySyncOrchestrator()
        
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
    
    parser = argparse.ArgumentParser(description='Shopify Inventory Sync Script')
    parser.add_argument('--mode', choices=['manual', 'scheduled'], default='manual',
                      help='Execution mode (default: manual)')
    parser.add_argument('--dry-run', action='store_true',
                      help='Run in dry-run mode (analyze only, no changes)')
    
    args = parser.parse_args()
    
    success = run_inventory_sync(mode=args.mode, dry_run=args.dry_run)
    sys.exit(0 if success else 1)