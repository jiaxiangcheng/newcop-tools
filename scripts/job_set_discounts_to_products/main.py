#!/usr/bin/env python3
"""
Product Discount Calculator Job

Automatically calculates discount percentages for all product variants based on
compare_at_price and updates the product's custom.discounts metafield.

Features:
- Calculates discount percentage for each variant with compare_at_price
- Rounds to nearest 5% (10%, 15%, 20%, 25%, etc.)
- Stores unique discount percentages in product metafield
- Scheduled to run daily at 00:00
- Manual and scheduled execution modes

Configuration:
- No environment variables needed, all logic is self-contained
- Uses compare_at_price from variant data to calculate discounts
- Updates custom.discounts metafield (list of single_line_text_field)

Required Shopify permissions:
- read_products
- read_product_metafields
- write_product_metafields
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
from shared.logger import setup_logger
from scripts.job_set_discounts_to_products.discount_calculator import DiscountCalculator
from scripts.job_set_discounts_to_products.models import DiscountSyncResult

# Load environment variables
load_dotenv()

# Set up logging
logger = setup_logger('product_discounts', 'product_discounts.log')

# Also configure related loggers
related_loggers = [
    'shared.shopify_client',
    'scripts.job_set_discounts_to_products.discount_calculator'
]

for logger_name in related_loggers:
    child_logger = logging.getLogger(logger_name)
    child_logger.setLevel(logging.INFO)
    # Copy handlers from main logger
    for handler in logger.handlers:
        child_logger.addHandler(handler)


class ProductDiscountOrchestrator:
    """Main orchestrator for product discount calculation with scheduling capabilities"""

    def __init__(self):
        # Configuration from environment variables
        self.shopify_admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
        self.shopify_shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")

        # Initialize components
        self.shopify_client = None
        self.discount_calculator = None
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
            if not value:
                missing_configs.append(name)

        if missing_configs:
            logger.error(f"❌ Missing required environment variables: {', '.join(missing_configs)}")
            logger.error("💡 Please check your .env file")
            return False

        logger.info("✅ Environment validation passed")
        return True

    def initialize_clients(self) -> bool:
        """Initialize Shopify client and discount calculator"""
        try:
            logger.info("🔧 Initializing API clients...")

            # Initialize Shopify client
            self.shopify_client = ShopifyClient(
                admin_token=self.shopify_admin_token,
                shop_domain=self.shopify_shop_domain
            )
            logger.info("✅ Shopify client initialized")

            # Initialize discount calculator
            self.discount_calculator = DiscountCalculator(
                shopify_client=self.shopify_client
            )
            logger.info("✅ Discount calculator initialized")

            return True

        except Exception as e:
            logger.error(f"❌ Failed to initialize clients: {e}")
            return False

    def run_sync(self, dry_run: bool = False) -> DiscountSyncResult:
        """
        Execute a single discount sync

        Args:
            dry_run: If True, only analyze without making changes

        Returns:
            DiscountSyncResult with sync statistics
        """
        if self.sync_in_progress:
            logger.warning("⚠️  Sync already in progress, skipping this run")
            return None

        self.sync_in_progress = True

        try:
            logger.info("=" * 60)
            logger.info("💰 Starting Product Discount Sync")
            logger.info("=" * 60)
            logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Mode: {'🧪 DRY RUN' if dry_run else '✅ LIVE'}")
            logger.info("=" * 60)

            # Execute sync
            result = self.discount_calculator.sync_product_discounts(
                dry_run=dry_run
            )

            logger.info("=" * 60)
            if result.is_success():
                logger.info("✅ Product Discount Sync completed successfully!")
            else:
                logger.warning("⚠️  Product Discount Sync completed with some errors")
            logger.info("=" * 60)

            return result

        except Exception as e:
            logger.error(f"💥 Error during product discount sync: {e}", exc_info=True)
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
        Run in scheduled mode (daily at 00:00)

        Returns:
            True if scheduler started successfully, False otherwise
        """
        try:
            logger.info("🔄 Starting Product Discount scheduler...")
            logger.info("📅 Schedule: Daily at 00:00")

            # Create scheduler
            executors = {
                'default': ThreadPoolExecutor(1)
            }

            self.scheduler = BlockingScheduler(executors=executors)

            # Add daily job (runs at 00:00 every day)
            self.scheduler.add_job(
                func=self.run_sync,
                trigger=CronTrigger(hour=0, minute=0),
                id='product_discounts_daily',
                name='Product Discounts Daily Sync',
                replace_existing=True
            )

            logger.info("✅ Scheduler configured successfully")
            logger.info("⏰ Next run scheduled for: Today/Tomorrow at 00:00")
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


def run_product_discounts(mode: str = "manual", dry_run: bool = False) -> bool:
    """
    Main entry point for product discount job

    Args:
        mode: Execution mode ("manual" or "scheduled")
        dry_run: Run in dry-run mode (only for manual mode)

    Returns:
        True if successful, False otherwise
    """
    orchestrator = ProductDiscountOrchestrator()

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
        description="Product Discount Calculator Job",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run once manually
  python main.py --mode manual

  # Run in scheduled mode (daily at 00:00)
  python main.py --mode scheduled

  # Dry run (preview changes)
  python main.py --dry-run

How it works:
  1. Fetches all products with variants from Shopify
  2. For each variant with compare_at_price > price:
     - Calculates discount percentage: ((compare_at_price - price) / compare_at_price) * 100
     - Rounds to nearest 5% (e.g., 10%, 15%, 20%, 25%)
  3. Collects unique discount percentages per product
  4. Updates product's custom.discounts metafield with list of unique percentages

Example:
  Product with 10 variants:
  - 5 variants with discounts: 12%, 18%, 23%, 23%, 4%
  - Rounded to nearest 5: 10%, 20%, 25%, 25%, 5%
  - Unique values: 5%, 10%, 20%, 25%
  - Metafield value: ["5%", "10%", "20%", "25%"]
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
    success = run_product_discounts(mode=args.mode, dry_run=args.dry_run)

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
