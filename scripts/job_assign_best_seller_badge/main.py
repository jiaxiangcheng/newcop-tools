#!/usr/bin/env python3
"""
Best Seller Badge Assignment Job

Automatically assigns best seller badges to top products based on Airtable sales data.
Runs weekly on Sundays at 00:00.

Features:
- Fetches top 50 products from Airtable view sorted by sales
- Clears all existing best seller badges
- Assigns badges to top products
- Uses GraphQL for efficient metafield updates
- Scheduled and manual execution modes

Required environment variables:
- SHOPIFY_ADMIN_TOKEN
- SHOPIFY_SHOP_DOMAIN
- AIRTABLE_TOKEN
- BEST_SELLER_AIRTABLE_BASE_ID
- BEST_SELLER_AIRTABLE_TABLE_ID
- BEST_SELLER_AIRTABLE_VIEW_ID
"""

import os
import sys
import logging
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.executors.pool import ThreadPoolExecutor

# Add project root to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from shared.shopify_client import ShopifyClient
from shared.airtable_client import AirtableClient
from shared.logger import setup_logger
from scripts.job_assign_best_seller_badge.badge_manager import BadgeManager
from scripts.job_assign_best_seller_badge.models import BadgeSyncResult

# Load environment variables
load_dotenv()

# Set up logging
logger = setup_logger('best_seller_badge', 'best_seller_badge.log')

# Also configure related loggers
related_loggers = [
    'shared.shopify_client',
    'shared.airtable_client',
    'scripts.job_assign_best_seller_badge.badge_manager'
]

for logger_name in related_loggers:
    child_logger = logging.getLogger(logger_name)
    child_logger.setLevel(logging.INFO)
    # Copy handlers from main logger
    for handler in logger.handlers:
        child_logger.addHandler(handler)


class BestSellerBadgeOrchestrator:
    """Main orchestrator for best seller badge assignment with scheduling capabilities"""

    def __init__(self):
        # Configuration from environment variables
        self.shopify_admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
        self.shopify_shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")
        self.airtable_token = os.getenv("AIRTABLE_TOKEN")

        # Best seller specific configuration
        self.airtable_base_id = os.getenv("BEST_SELLER_AIRTABLE_BASE_ID", "appDE0y01TchMqX8N")
        self.airtable_table_id = os.getenv("BEST_SELLER_AIRTABLE_TABLE_ID", "tbljkyhWy5D6b65Im")
        self.airtable_view_id = os.getenv("BEST_SELLER_AIRTABLE_VIEW_ID", "viwRCdtRuUTkqLOp3")
        self.top_n_products = int(os.getenv("BEST_SELLER_TOP_N", "50"))
        self.dry_run_mode = os.getenv("BEST_SELLER_DRY_RUN", "false").lower() == "true"

        # Initialize components
        self.shopify_client = None
        self.airtable_client = None
        self.badge_manager = None
        self.scheduler = None

        # Track running state
        self.is_running = False
        self.sync_in_progress = False

    def validate_environment(self) -> bool:
        """Validate that all required configuration is present"""
        required_configs = [
            ("SHOPIFY_ADMIN_TOKEN", self.shopify_admin_token),
            ("SHOPIFY_SHOP_DOMAIN", self.shopify_shop_domain),
            ("AIRTABLE_TOKEN", self.airtable_token),
            ("BEST_SELLER_AIRTABLE_BASE_ID", self.airtable_base_id),
            ("BEST_SELLER_AIRTABLE_TABLE_ID", self.airtable_table_id),
            ("BEST_SELLER_AIRTABLE_VIEW_ID", self.airtable_view_id)
        ]

        missing_configs = []
        for name, value in required_configs:
            if not value:
                missing_configs.append(name)

        if missing_configs:
            logger.error(f"❌ Missing required environment variables: {', '.join(missing_configs)}")
            logger.error("💡 Please check your .env file")
            return False

        logger.info("✅ Environment validation passed")
        return True

    def initialize_clients(self) -> bool:
        """Initialize Shopify and Airtable clients"""
        try:
            logger.info("🔧 Initializing API clients...")

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

            # Initialize badge manager
            self.badge_manager = BadgeManager(
                shopify_client=self.shopify_client,
                airtable_client=self.airtable_client
            )
            logger.info("✅ Badge manager initialized")

            return True

        except Exception as e:
            logger.error(f"❌ Failed to initialize clients: {e}")
            return False

    def run_sync(self, dry_run: Optional[bool] = None) -> BadgeSyncResult:
        """
        Execute a single best seller badge sync

        Args:
            dry_run: Override dry run mode (None = use environment setting)

        Returns:
            BadgeSyncResult with sync statistics
        """
        if self.sync_in_progress:
            logger.warning("⚠️  Sync already in progress, skipping this run")
            return None

        self.sync_in_progress = True
        use_dry_run = dry_run if dry_run is not None else self.dry_run_mode

        try:
            logger.info("=" * 60)
            logger.info("🏅 Starting Best Seller Badge Sync")
            logger.info("=" * 60)
            logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Mode: {'🧪 DRY RUN' if use_dry_run else '✅ LIVE'}")
            logger.info(f"Top N products: {self.top_n_products}")
            logger.info(f"Airtable Base: {self.airtable_base_id}")
            logger.info(f"Airtable View: {self.airtable_view_id}")
            logger.info("=" * 60)

            # Execute sync
            result = self.badge_manager.sync_best_seller_badges(
                airtable_base_id=self.airtable_base_id,
                airtable_table_id=self.airtable_table_id,
                airtable_view_id=self.airtable_view_id,
                top_n_products=self.top_n_products,
                dry_run=use_dry_run
            )

            logger.info("=" * 60)
            if result.is_success():
                logger.info("✅ Best Seller Badge Sync completed successfully!")
            else:
                logger.warning("⚠️  Best Seller Badge Sync completed with some errors")
            logger.info("=" * 60)

            return result

        except Exception as e:
            logger.error(f"💥 Error during best seller badge sync: {e}", exc_info=True)
            raise

        finally:
            self.sync_in_progress = False

    def run_manual(self, dry_run: bool = False) -> bool:
        """
        Run a single sync manually

        Args:
            dry_run: Run in dry-run mode

        Returns:
            True if successful, False otherwise
        """
        try:
            result = self.run_sync(dry_run=dry_run)
            return result is not None and result.is_success()

        except KeyboardInterrupt:
            logger.info("\n⏹️  Manual sync interrupted by user")
            return False

        except Exception as e:
            logger.error(f"💥 Manual sync failed: {e}")
            return False

    def run_scheduled(self) -> bool:
        """
        Run in scheduled mode (weekly on Sundays at 00:00)

        Returns:
            True if scheduler started successfully, False otherwise
        """
        try:
            logger.info("🔄 Starting Best Seller Badge scheduler...")
            logger.info("📅 Schedule: Weekly on Sundays at 00:00")

            # Create scheduler
            executors = {
                'default': ThreadPoolExecutor(1)
            }

            self.scheduler = BlockingScheduler(executors=executors)

            # Add weekly job (runs every Sunday at 00:00)
            self.scheduler.add_job(
                func=self.run_sync,
                trigger=CronTrigger(day_of_week='sun', hour=0, minute=0),
                id='best_seller_badge_weekly',
                name='Best Seller Badge Weekly Sync',
                replace_existing=True
            )

            logger.info("✅ Scheduler configured successfully")
            logger.info("⏰ Next run scheduled for: Next Sunday at 00:00")
            logger.info("🔄 Scheduler starting... (Press Ctrl+C to stop)")

            # Start scheduler (blocks until shutdown)
            self.is_running = True
            self.scheduler.start()

            return True

        except (KeyboardInterrupt, SystemExit):
            logger.info("\n⏹️  Scheduler stopped by user")
            return False

        except Exception as e:
            logger.error(f"💥 Scheduler failed: {e}", exc_info=True)
            return False

        finally:
            self.is_running = False
            if self.scheduler and self.scheduler.running:
                self.scheduler.shutdown(wait=False)


def run_best_seller_badge(mode: str = "manual", dry_run: bool = False) -> bool:
    """
    Main entry point for best seller badge job

    Args:
        mode: Execution mode ("manual" or "scheduled")
        dry_run: Run in dry-run mode (only for manual mode)

    Returns:
        True if successful, False otherwise
    """
    orchestrator = BestSellerBadgeOrchestrator()

    # Validate environment
    if not orchestrator.validate_environment():
        return False

    # Initialize clients
    if not orchestrator.initialize_clients():
        return False

    # Run based on mode
    if mode == "scheduled":
        return orchestrator.run_scheduled()
    else:
        return orchestrator.run_manual(dry_run=dry_run)


def main():
    """CLI entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Best Seller Badge Assignment Job",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run once manually
  python main.py --mode manual

  # Run in scheduled mode (weekly on Sundays)
  python main.py --mode scheduled

  # Dry run (preview changes)
  python main.py --dry-run

Environment variables:
  SHOPIFY_ADMIN_TOKEN: Shopify Admin API token
  SHOPIFY_SHOP_DOMAIN: Shopify shop domain
  AIRTABLE_TOKEN: Airtable API token
  BEST_SELLER_AIRTABLE_BASE_ID: Airtable base ID (default: appDE0y01TchMqX8N)
  BEST_SELLER_AIRTABLE_TABLE_ID: Airtable table ID (default: tbljkyhWy5D6b65Im)
  BEST_SELLER_AIRTABLE_VIEW_ID: Airtable view ID (default: viwRCdtRuUTkqLOp3)
  BEST_SELLER_TOP_N: Number of top products (default: 50)
  BEST_SELLER_DRY_RUN: Enable dry run mode (default: false)
        """
    )

    parser.add_argument(
        "--mode",
        choices=["manual", "scheduled"],
        default="manual",
        help="Execution mode (default: manual)"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (analyze only, no changes)"
    )

    args = parser.parse_args()

    # Run the job
    success = run_best_seller_badge(mode=args.mode, dry_run=args.dry_run)

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
