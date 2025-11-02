#!/usr/bin/env python3
"""
Customer Order History Orchestrator

Main entry point for the customer order history analysis system.
Supports manual, scheduled, and dry-run modes.
"""

import os
import sys
import logging
import signal
import argparse
from datetime import datetime
from typing import Dict, Any, Optional
from dotenv import load_dotenv

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.executors.pool import ThreadPoolExecutor

from shared.shopify_client import ShopifyClient
from shared.airtable_client import AirtableClient
from scripts.job_customer_order_history.order_history_manager import OrderHistoryManager
from scripts.job_customer_order_history.storage import OrderHistoryStorage
from scripts.job_customer_order_history.models import OrderHistorySyncResult

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/customer_order_history.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class CustomerOrderHistoryOrchestrator:
    """Orchestrates customer order history synchronization with scheduling"""

    def __init__(self):
        """Initialize orchestrator with configuration from environment"""
        # Shopify configuration
        self.shopify_admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
        self.shopify_shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")

        # Airtable configuration
        self.airtable_token = os.getenv("AIRTABLE_TOKEN")
        self.airtable_base_id = os.getenv("CUSTOMER_ORDER_HISTORY_AIRTABLE_BASE_ID", "appDE0y01TchMqX8N")
        self.airtable_table_id = os.getenv("CUSTOMER_ORDER_HISTORY_AIRTABLE_TABLE_ID", "tblmUgqBrH47n3X5O")
        self.airtable_view_id = os.getenv("CUSTOMER_ORDER_HISTORY_AIRTABLE_VIEW_ID", "viwdyB872YBZCUWhn")

        # Sync configuration
        self.sync_interval_hours = int(os.getenv("CUSTOMER_ORDER_HISTORY_INTERVAL_HOURS", "24"))
        self.dry_run = os.getenv("CUSTOMER_ORDER_HISTORY_DRY_RUN", "false").lower() == "true"
        self.max_workers = int(os.getenv("CUSTOMER_ORDER_HISTORY_MAX_WORKERS", "5"))

        # Cache configuration
        self.cache_file_path = os.getenv(
            "CUSTOMER_ORDER_HISTORY_CACHE_FILE",
            "data/customer_order_history_cache.json"
        )

        # Components (initialized lazily)
        self.shopify_client: Optional[ShopifyClient] = None
        self.airtable_client: Optional[AirtableClient] = None
        self.storage: Optional[OrderHistoryStorage] = None
        self.manager: Optional[OrderHistoryManager] = None
        self.scheduler: Optional[BlockingScheduler] = None

        # State
        self.is_running = False
        self.sync_in_progress = False

    def validate_environment(self) -> bool:
        """
        Validate required environment variables

        Returns:
            True if all required variables are set, False otherwise
        """
        missing = []

        if not self.shopify_admin_token:
            missing.append("SHOPIFY_ADMIN_TOKEN")
        if not self.shopify_shop_domain:
            missing.append("SHOPIFY_SHOP_DOMAIN")
        if not self.airtable_token:
            missing.append("AIRTABLE_TOKEN")

        if missing:
            logger.error(f"❌ Missing required environment variables: {', '.join(missing)}")
            logger.error("Please set these variables in your .env file")
            return False

        logger.info("✅ Environment validation passed")
        return True

    def initialize_components(self) -> bool:
        """
        Initialize API clients and components

        Returns:
            True if initialization successful, False otherwise
        """
        try:
            logger.info("Initializing components...")

            # Initialize Shopify client
            self.shopify_client = ShopifyClient(
                admin_token=self.shopify_admin_token,
                shop_domain=self.shopify_shop_domain
            )
            logger.info("✅ Shopify client initialized")

            # Initialize Airtable client
            self.airtable_client = AirtableClient(
                token=self.airtable_token,
                base_id=self.airtable_base_id
            )
            logger.info("✅ Airtable client initialized")

            # Initialize storage
            self.storage = OrderHistoryStorage(cache_file_path=self.cache_file_path)
            self.storage.load_cache()
            logger.info("✅ Storage initialized")

            # Initialize manager
            self.manager = OrderHistoryManager(
                shopify_client=self.shopify_client,
                airtable_client=self.airtable_client,
                airtable_table_id=self.airtable_table_id,
                airtable_view_id=self.airtable_view_id,
                storage=self.storage,
                max_workers=self.max_workers
            )
            logger.info("✅ Order history manager initialized")

            logger.info("✅ All components initialized successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Error initializing components: {e}")
            return False

    def run_single_sync(self, dry_run: Optional[bool] = None, force_all: bool = False, yesterday_only: bool = False) -> Dict[str, Any]:
        """
        Run a single sync operation

        Args:
            dry_run: Override dry_run setting (if None, use instance setting)
            force_all: If True, process all records ignoring cache
            yesterday_only: If True, only process orders from yesterday (for scheduled mode)

        Returns:
            Dictionary with sync results
        """
        if self.sync_in_progress:
            logger.warning("⚠️  Sync already in progress, skipping")
            return {
                "success": False,
                "message": "Sync already in progress"
            }

        # Determine dry_run mode
        is_dry_run = dry_run if dry_run is not None else self.dry_run

        try:
            self.sync_in_progress = True

            logger.info("\n" + "=" * 60)
            logger.info(f"Starting sync at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Mode: {'DRY RUN' if is_dry_run else 'LIVE'}")
            logger.info(f"Force all: {force_all}")
            logger.info(f"Yesterday only: {yesterday_only}")
            logger.info("=" * 60 + "\n")

            # Run sync
            result: OrderHistorySyncResult = self.manager.sync_order_history(
                dry_run=is_dry_run,
                force_all=force_all,
                yesterday_only=yesterday_only
            )

            return {
                "success": result.success,
                "total_orders_fetched": result.total_orders_fetched,
                "orders_processed": result.orders_processed,
                "orders_updated": result.orders_updated,
                "orders_skipped": result.orders_skipped,
                "orders_failed": result.orders_failed,
                "execution_time": result.execution_time_seconds,
                "errors": result.errors
            }

        except Exception as e:
            logger.error(f"❌ Sync failed: {e}", exc_info=True)
            return {
                "success": False,
                "message": f"Sync error: {str(e)}"
            }

        finally:
            self.sync_in_progress = False

    def setup_scheduler(self) -> bool:
        """
        Setup APScheduler for daily execution at 00:00

        Returns:
            True if scheduler setup successful, False otherwise
        """
        try:
            logger.info("Setting up scheduler...")

            # Configure executors
            executors = {
                'default': ThreadPoolExecutor(max_workers=1)  # Only one sync at a time
            }

            # Configure job defaults
            job_defaults = {
                'coalesce': True,  # Combine multiple pending executions
                'max_instances': 1,  # Only one instance at a time
                'misfire_grace_time': 300  # 5 minutes grace for missed executions
            }

            # Create scheduler
            self.scheduler = BlockingScheduler(
                executors=executors,
                job_defaults=job_defaults
            )

            # Add daily job at 00:00
            self.scheduler.add_job(
                func=self._scheduled_sync_job,
                trigger=CronTrigger(hour=0, minute=0),  # Daily at 00:00
                id='customer_order_history_sync',
                name=f'Customer Order History Sync (daily at 00:00)',
                replace_existing=True
            )

            logger.info(f"✅ Scheduler configured to run daily at 00:00")
            logger.info(f"   Next run: {self.scheduler.get_jobs()[0].next_run_time}")

            return True

        except Exception as e:
            logger.error(f"❌ Error setting up scheduler: {e}")
            return False

    def _scheduled_sync_job(self):
        """Job function called by scheduler"""
        logger.info("⏰ Scheduled sync triggered at 00:00")
        logger.info("📅 Processing orders from yesterday (00:00 to 23:59:59)")

        # Scheduled mode processes only yesterday's orders
        result = self.run_single_sync(yesterday_only=True)

        if result["success"]:
            logger.info("✅ Scheduled sync completed successfully")
        else:
            logger.error("❌ Scheduled sync failed")

    def start_scheduled_mode(self):
        """Start scheduler in blocking mode"""
        self.is_running = True

        # Setup signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            logger.info("\n⏹️  Shutdown signal received, stopping scheduler...")
            self.stop_scheduler()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info("\n" + "=" * 60)
        logger.info("🚀 Starting scheduled mode")
        logger.info("=" * 60)

        # Run initial sync (process yesterday's orders)
        logger.info("\n🚀 Running initial sync for yesterday's orders...")
        self.run_single_sync(yesterday_only=True)

        # Start scheduler (blocks until stopped)
        logger.info("\n📅 Scheduler started, waiting for next execution at 00:00...")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 60 + "\n")

        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Scheduler stopped")

    def stop_scheduler(self):
        """Stop the scheduler gracefully"""
        if self.scheduler and self.scheduler.running:
            logger.info("Stopping scheduler...")
            self.scheduler.shutdown(wait=False)
            logger.info("✅ Scheduler stopped")

        self.is_running = False


def run_customer_order_history(mode: str = "manual", dry_run: bool = False, force_all: bool = False, yesterday_only: bool = False) -> bool:
    """
    Entry point function for running customer order history sync

    Args:
        mode: Execution mode ("manual" or "scheduled")
        dry_run: If True, analyze without updating Airtable
        force_all: If True, process all records ignoring cache
        yesterday_only: If True, only process orders from yesterday

    Returns:
        True if execution was successful, False otherwise
    """
    orchestrator = CustomerOrderHistoryOrchestrator()

    # Validate environment
    if not orchestrator.validate_environment():
        return False

    # Initialize components
    if not orchestrator.initialize_components():
        return False

    # Execute based on mode
    if mode == "scheduled":
        if not orchestrator.setup_scheduler():
            return False

        orchestrator.start_scheduled_mode()
        return True

    else:  # manual mode
        result = orchestrator.run_single_sync(dry_run=dry_run, force_all=force_all, yesterday_only=yesterday_only)
        return result["success"]


def main():
    """Main entry point for CLI usage"""
    parser = argparse.ArgumentParser(
        description="Customer Order History Analysis - Sync customer order counts to Airtable"
    )

    parser.add_argument(
        "--mode",
        choices=["manual", "scheduled"],
        default="manual",
        help="Execution mode: manual (run once) or scheduled (continuous at 00:00 daily)"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Analyze without updating Airtable"
    )

    parser.add_argument(
        "--force-all",
        action="store_true",
        help="Process all records ignoring cache"
    )

    parser.add_argument(
        "--yesterday-only",
        action="store_true",
        help="Process only orders from yesterday (for manual testing of scheduled behavior)"
    )

    args = parser.parse_args()

    # Run the sync
    success = run_customer_order_history(
        mode=args.mode,
        dry_run=args.dry_run,
        force_all=args.force_all,
        yesterday_only=args.yesterday_only
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
