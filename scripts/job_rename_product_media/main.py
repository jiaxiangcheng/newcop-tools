#!/usr/bin/env python3
"""
Rename Product Media Job

Loops over every product in the store and normalizes each media file's filename
and alt text to a consistent newcop format:

- filename: newcop-[slug(title)]-[slug(type)]-[position].[ext]  (all lowercase)
- alt text: Newcop [Title] [Type] [Position]  (every word capitalized)

product type is the Shopify product type translated to Spanish (e.g.
'Resell Sneakers' -> 'Zapatillas'); unmapped types fall back to the English type.

Shopify 2025-01 constraints:
- IMAGE      -> filename + alt updated via fileUpdate
- VIDEO/3D   -> alt only (Shopify does not allow renaming these via the API)
- EXTERNAL_VIDEO -> skipped (no filename concept)

Modes: manual (run once), scheduled (continuous via APScheduler), dry-run (analyze only).

Required Shopify permissions:
- read_products
- write_files / write_products (to update media files)
"""
import argparse
import os
import sys
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.executors.pool import ThreadPoolExecutor

# Add project root to path for imports.
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from shared.shopify_client import ShopifyClient
from shared.logger import setup_logger
from scripts.job_rename_product_media.media_manager import MediaManager
from scripts.job_rename_product_media.models import RenameSyncResult

logger = setup_logger("rename_product_media", "rename_product_media.log")


class RenameProductMediaOrchestrator:
    """Orchestrates the media rename job with optional scheduling."""

    def __init__(self):
        self.shopify_admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
        self.shopify_shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")

        self.shopify_client = None
        self.media_manager = None
        self.scheduler = None
        self.sync_in_progress = False

    def initialize_clients(self) -> bool:
        """Build the Shopify client and media manager."""
        if not self.shopify_admin_token or not self.shopify_shop_domain:
            logger.error(
                "Missing required environment variables: SHOPIFY_ADMIN_TOKEN, SHOPIFY_SHOP_DOMAIN"
            )
            return False

        try:
            logger.info("🔧 Initializing API clients...")
            self.shopify_client = ShopifyClient(
                admin_token=self.shopify_admin_token,
                shop_domain=self.shopify_shop_domain,
            )
            self.media_manager = MediaManager(self.shopify_client, logger)
            logger.info("✅ Clients initialized")
            return True
        except Exception as exc:
            logger.error(f"❌ Failed to initialize clients: {exc}")
            return False

    def run_sync(self, dry_run: bool = False, limit: Optional[int] = None) -> Optional[RenameSyncResult]:
        """Run a single pass over the store."""
        if self.sync_in_progress:
            logger.warning("⚠️  Sync already in progress, skipping this run")
            return None

        self.sync_in_progress = True
        try:
            logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            return self.media_manager.rename_all_media(dry_run=dry_run, limit=limit)
        except Exception as exc:
            logger.error(f"💥 Error during media rename: {exc}", exc_info=True)
            raise
        finally:
            self.sync_in_progress = False

    def run_scheduled(self) -> bool:
        """Run continuously, daily at 02:00 (live mode)."""
        try:
            logger.info("🔄 Starting Rename Product Media scheduler...")
            logger.info("📅 Schedule: Daily at 02:00")

            executors = {"default": ThreadPoolExecutor(1)}
            self.scheduler = BlockingScheduler(executors=executors)
            self.scheduler.add_job(
                func=lambda: self.run_sync(dry_run=False, limit=None),
                trigger=CronTrigger(hour=2, minute=0),
                id="rename_product_media_daily",
                name="Rename Product Media Daily",
                replace_existing=True,
            )

            logger.info("✅ Scheduler configured. Press Ctrl+C to stop.")
            self.scheduler.start()
            return True
        except (KeyboardInterrupt, SystemExit):
            logger.info("\n⏹️  Scheduler stopped by user")
            return False
        except Exception as exc:
            logger.error(f"💥 Scheduler failed: {exc}", exc_info=True)
            return False
        finally:
            if self.scheduler and self.scheduler.running:
                self.scheduler.shutdown(wait=False)


def run_rename_product_media(
    mode: str = "manual",
    dry_run: bool = False,
    limit: Optional[int] = None,
) -> bool:
    """Programmatic entry point imported lazily by the root launcher.

    Args:
        mode: "manual" or "scheduled".
        dry_run: Analyze only (manual mode); ignored for scheduled mode.
        limit: Optional cap on the number of products to process (manual mode).

    Returns:
        True on success / clean dry-run, False on error.
    """
    load_dotenv()

    orchestrator = RenameProductMediaOrchestrator()
    if not orchestrator.initialize_clients():
        return False

    if mode == "scheduled":
        return orchestrator.run_scheduled()

    result = orchestrator.run_sync(dry_run=dry_run, limit=limit)
    if result is None:
        return False
    return result.is_success()


def parse_arguments():
    parser = argparse.ArgumentParser(description="Rename product media filenames and alt text")
    parser.add_argument(
        "--mode",
        choices=["manual", "scheduled"],
        default="manual",
        help="Execution mode (default: manual)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Analyze and print planned changes without calling the API",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process the first N products (manual mode, useful for testing)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_arguments()
    ok = run_rename_product_media(mode=args.mode, dry_run=args.dry_run, limit=args.limit)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
