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
from typing import Optional, Dict, Any, Set, List
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
from scripts.inventory_sync.models import FlexibleSyncConfig, SyncField, SyncMode

# Load environment variables
load_dotenv()

# Set up logging
logger = setup_logger('inventory_sync', 'inventory_sync.log')

# Also configure related loggers to use the same settings
related_loggers = [
    'shared.shopify_client',
    'scripts.inventory_sync.inventory_manager',
    'scripts.inventory_sync.storage'
]

for logger_name in related_loggers:
    child_logger = logging.getLogger(logger_name)
    child_logger.setLevel(logging.INFO)
    # Copy handlers from main logger
    for handler in logger.handlers:
        child_logger.addHandler(handler)

class InventorySyncOrchestrator:
    """Main orchestrator for inventory synchronization with scheduling capabilities"""
    
    def __init__(self, sync_config: Optional[FlexibleSyncConfig] = None):
        # Configuration from environment variables
        self.shopify_admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
        self.shopify_shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")
        
        # Create flexible sync configuration
        if sync_config is not None:
            self.sync_config = sync_config
        else:
            # Create config from environment variables
            env_dict = {k: v for k, v in os.environ.items() if v is not None}
            self.sync_config = FlexibleSyncConfig.from_env_vars(env_dict)
        
        # Legacy configuration for backward compatibility
        self.sync_interval_minutes = self._parse_interval_config(os.getenv("INVENTORY_SYNC_INTERVAL", "6h"))
        self.dry_run_mode = self.sync_config.dry_run
        self.batch_size = self.sync_config.batch_size
        
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
        logger.info(f"🔧 Flexible Sync Configuration:")
        
        enabled_fields = self.sync_config.get_enabled_fields()
        if enabled_fields:
            logger.info(f"  - Enabled fields: {', '.join([f.value for f in enabled_fields])}")
        else:
            logger.warning("  - No fields are enabled for synchronization!")
        
        logger.info(f"  - Inventory: {'✅' if self.sync_config.inventory.enabled else '❌'} {self.sync_config.inventory.mode.value}")
        if self.sync_config.inventory.enabled and self.sync_config.inventory.mode == SyncMode.SCHEDULED:
            logger.info(f"    Interval: {self._format_interval_display(self.sync_config.inventory.interval_minutes)}")
        
        logger.info(f"  - Price: {'✅' if self.sync_config.price.enabled else '❌'} {self.sync_config.price.mode.value}")
        if self.sync_config.price.enabled and self.sync_config.price.mode == SyncMode.SCHEDULED:
            logger.info(f"    Interval: {self._format_interval_display(self.sync_config.price.interval_minutes)}")
        
        logger.info(f"  - Compare Price: {'✅' if self.sync_config.compare_price.enabled else '❌'} {self.sync_config.compare_price.mode.value}")
        if self.sync_config.compare_price.enabled and self.sync_config.compare_price.mode == SyncMode.SCHEDULED:
            logger.info(f"    Interval: {self._format_interval_display(self.sync_config.compare_price.interval_minutes)}")
        
        logger.info(f"  - Dry run mode: {self.sync_config.dry_run}")
        logger.info(f"  - Batch size: {self.sync_config.batch_size} products")
        logger.info(f"  - Shop domain: {self.shopify_shop_domain}")
        
        return True
    
    def _parse_interval_config(self, interval_str: str) -> int:
        """Parse interval configuration string to minutes"""
        try:
            interval_str = interval_str.strip().lower()
            
            if interval_str.endswith('h'):
                # Hours format: "6h", "2h", etc.
                hours = int(interval_str[:-1])
                return hours * 60
            elif interval_str.endswith('m'):
                # Minutes format: "30m", "15m", etc.
                minutes = int(interval_str[:-1])
                return minutes
            elif interval_str.endswith('min'):
                # Minutes format: "30min", "15min", etc.
                minutes = int(interval_str[:-3])
                return minutes
            else:
                # Default: assume hours if no unit specified
                hours = int(interval_str)
                return hours * 60
                
        except (ValueError, AttributeError):
            logger.warning(f"Invalid interval format: {interval_str}, using default 6 hours")
            return 6 * 60  # Default to 6 hours
    
    def _format_interval_display(self, minutes: int) -> str:
        """Format interval for display"""
        if minutes >= 60:
            hours = minutes // 60
            remaining_minutes = minutes % 60
            if remaining_minutes == 0:
                return f"{hours} hour{'s' if hours != 1 else ''}"
            else:
                return f"{hours} hour{'s' if hours != 1 else ''} {remaining_minutes} minute{'s' if remaining_minutes != 1 else ''}"
        else:
            return f"{minutes} minute{'s' if minutes != 1 else ''}"
    
    @staticmethod
    def _format_interval_display_static(interval_str: str) -> str:
        """Static method to format interval string for display"""
        try:
            interval_str = interval_str.strip().lower()
            
            if interval_str.endswith('h'):
                # Hours format: "6h", "2h", etc.
                hours = int(interval_str[:-1])
                return f"{hours} hour{'s' if hours != 1 else ''}"
            elif interval_str.endswith('m'):
                # Minutes format: "30m", "15m", etc.
                minutes = int(interval_str[:-1])
                return f"{minutes} minute{'s' if minutes != 1 else ''}"
            elif interval_str.endswith('min'):
                # Minutes format: "30min", "15min", etc.
                minutes = int(interval_str[:-3])
                return f"{minutes} minute{'s' if minutes != 1 else ''}"
            else:
                # Default: assume hours if no unit specified
                hours = int(interval_str)
                return f"{hours} hour{'s' if hours != 1 else ''}"
                
        except (ValueError, AttributeError):
            return "2 hours"  # Default fallback
    
    def initialize_components(self) -> bool:
        """Initialize Shopify client, storage, and inventory manager"""
        try:
            # Initialize clients
            self.shopify_client = ShopifyClient(self.shopify_admin_token, self.shopify_shop_domain)
            self.storage = InventoryStorage()
            self.inventory_manager = InventoryManager(self.shopify_client, self.storage, self.sync_config)
            
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
            
            # Run the flexible sync with batch processing
            result = self.inventory_manager.sync_fields_to_metafields(
                target_fields=None,  # Use config defaults
                dry_run=sync_dry_run, 
                batch_size=self.batch_size
            )
            
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
                
                # Note: Detailed logs are now handled by the inventory manager
                
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
    
    def run_flexible_sync(self, target_fields: Optional[Set[SyncField]] = None, 
                         dry_run: Optional[bool] = None) -> Dict[str, Any]:
        """Run a flexible sync for specific fields"""
        if self.sync_in_progress:
            logger.warning("⚠️  Sync already in progress, skipping...")
            return {"success": False, "message": "Sync already in progress"}
        
        try:
            self.sync_in_progress = True
            sync_dry_run = dry_run if dry_run is not None else self.sync_config.dry_run
            
            # Use config defaults if no fields specified
            enabled_fields = target_fields or self.sync_config.get_enabled_fields()
            
            if not enabled_fields:
                logger.warning("⚠️  No fields are enabled for synchronization")
                return {"success": False, "message": "No fields enabled"}
            
            logger.info(f"🚀 Starting flexible sync for fields: {', '.join([f.value for f in enabled_fields])}")
            if sync_dry_run:
                logger.info("🧪 Running in DRY RUN mode - no changes will be made")
            
            # Run the flexible sync
            result = self.inventory_manager.sync_fields_to_metafields(
                target_fields=enabled_fields,
                dry_run=sync_dry_run, 
                batch_size=self.sync_config.batch_size
            )
            
            # Log detailed results
            if result.success:
                logger.info("✅ Flexible synchronization completed successfully!")
                logger.info(f"📊 Results summary:")
                logger.info(f"  - Products processed: {result.total_products_processed}")
                logger.info(f"  - Variants checked: {result.total_variants_checked}")
                logger.info(f"  - Variants updated: {result.variants_updated}")
                logger.info(f"  - Variants failed: {result.variants_failed}")
                logger.info(f"  - Products with changes: {result.products_with_changes}")
                logger.info(f"  - Fields synced: {', '.join([f.value for f in enabled_fields])}")
                logger.info(f"  - Execution time: {result.execution_time_seconds:.2f} seconds")
                
                if result.errors:
                    logger.warning(f"⚠️  Encountered {len(result.errors)} errors:")
                    for error in result.errors:
                        logger.warning(f"    - {error}")
            else:
                logger.error("❌ Flexible synchronization failed!")
                if result.errors:
                    for error in result.errors:
                        logger.error(f"  💥 {error}")
            
            return {
                "success": result.success,
                "sync_result": result,
                "fields_synced": [f.value for f in enabled_fields],
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"💥 Unexpected error during flexible sync: {e}")
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
                trigger=IntervalTrigger(minutes=self.sync_interval_minutes),
                id='inventory_sync_job',
                name=f'Inventory Sync (every {self._format_interval_display(self.sync_interval_minutes)})',
                replace_existing=True
            )
            
            logger.info(f"📅 Scheduler configured for {self._format_interval_display(self.sync_interval_minutes)} intervals")
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
            
            # Show detailed results if there were updates
            sync_result = result.get("sync_result")
            if sync_result and sync_result.variants_updated > 0:
                logger.info("📋 Scheduled sync update details:")
                logger.info(f"  - Variants updated: {sync_result.variants_updated}")
                logger.info(f"  - Products with changes: {sync_result.products_with_changes}")
                logger.info(f"  - Execution time: {sync_result.execution_time_seconds:.2f} seconds")
                
                # Show successful updates if available
                if hasattr(sync_result, 'updated_variants') and sync_result.updated_variants:
                     successful_updates = [u for u in sync_result.updated_variants if u]
                     if successful_updates:
                         logger.info("  📦 Successfully updated variants:")
                         for i, update in enumerate(successful_updates, 1):
                             logger.info(f"    [{i}] Variant ID: {update.variant_id}")
                             logger.info(f"        Inventory: {update.old_quantity} → {update.new_quantity}")
                         # Note: For scheduled mode, we don't have access to product/variant titles here
                         # The detailed info is already logged in the main sync process
        else:
            logger.error("❌ Scheduled sync failed")
            if result.get("error"):
                logger.error(f"  Error: {result['error']}")
    
    def start_scheduled_mode(self):
        """Start the scheduler to run syncs automatically"""
        try:
            logger.info("🔄 Starting scheduled inventory sync mode...")
            logger.info(f"📅 Will sync every {self._format_interval_display(self.sync_interval_minutes)}")
            logger.info("⏹️  Press Ctrl+C to stop")
            
            self.is_running = True
            
            # Setup signal handlers for graceful shutdown
            def signal_handler(_signum, _frame):
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
                    "sync_interval_minutes": self.sync_interval_minutes,
                    "sync_interval_display": self._format_interval_display(self.sync_interval_minutes),
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

def run_inventory_sync(mode: str = "manual", dry_run: bool = False, 
                      sync_fields: Optional[List[str]] = None, 
                      sync_config: Optional[FlexibleSyncConfig] = None) -> bool:
    """
    Entry point for inventory sync script
    
    Args:
        mode: "manual" for one-time sync, "scheduled" for continuous mode
        dry_run: Whether to run in dry-run mode (analysis only)
        sync_fields: List of field names to sync (inventory, price, compare_price)
        sync_config: Pre-configured sync configuration
    
    Returns:
        Boolean indicating success
    """
    try:
        orchestrator = InventorySyncOrchestrator(sync_config=sync_config)
        
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
            # Determine target fields
            target_fields = None
            if sync_fields:
                try:
                    target_fields = {SyncField(field.strip()) for field in sync_fields}
                    logger.info(f"🎯 Manual sync targeting specific fields: {', '.join([f.value for f in target_fields])}")
                except ValueError as e:
                    logger.error(f"❌ Invalid field name: {e}")
                    logger.error(f"Valid fields: {', '.join([f.value for f in SyncField])}")
                    return False
            
            # Run flexible sync
            if target_fields:
                result = orchestrator.run_flexible_sync(target_fields=target_fields, dry_run=dry_run)
            else:
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
    
    parser = argparse.ArgumentParser(description='Shopify Flexible Inventory Sync Script')
    parser.add_argument('--mode', choices=['manual', 'scheduled'], default='manual',
                      help='Execution mode (default: manual)')
    parser.add_argument('--dry-run', action='store_true',
                      help='Run in dry-run mode (analyze only, no changes)')
    parser.add_argument('--sync-fields', 
                      help='Comma-separated list of fields to sync (inventory,price,compare_price). '
                           'If not specified, uses configuration defaults.')
    parser.add_argument('--inventory-only', action='store_true',
                      help='Sync only inventory fields (shortcut for --sync-fields inventory)')
    parser.add_argument('--price-only', action='store_true',
                      help='Sync only price fields (shortcut for --sync-fields price)')
    parser.add_argument('--compare-price-only', action='store_true',
                      help='Sync only compare price fields (shortcut for --sync-fields compare_price)')
    
    args = parser.parse_args()
    
    # Parse sync fields
    sync_fields = None
    if args.sync_fields:
        sync_fields = [field.strip() for field in args.sync_fields.split(',')]
    elif args.inventory_only:
        sync_fields = ['inventory']
    elif args.price_only:
        sync_fields = ['price']
    elif args.compare_price_only:
        sync_fields = ['compare_price']
    
    success = run_inventory_sync(mode=args.mode, dry_run=args.dry_run, sync_fields=sync_fields)
    sys.exit(0 if success else 1)