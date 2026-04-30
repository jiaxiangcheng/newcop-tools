import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Add script directory to path for direct import (directory has hyphens)
sys.path.insert(0, str(Path(__file__).parent))

from shared.shopify_client import ShopifyClient
from shared.logger import setup_logger

load_dotenv()

logger = setup_logger('duplicate_products', 'duplicate_products.log')

# Configure child loggers
related_loggers = [
    'shared.shopify_client',
    'duplicate_manager',
]
for logger_name in related_loggers:
    child_logger = logging.getLogger(logger_name)
    child_logger.setLevel(logging.INFO)
    for handler in logger.handlers:
        child_logger.addHandler(handler)


class DuplicateProductsOrchestrator:
    def __init__(self):
        self.shopify_admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
        self.shopify_shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")
        self.shopify_client = None
        self.manager = None

    def validate_environment(self) -> bool:
        required = [
            ("SHOPIFY_ADMIN_TOKEN", self.shopify_admin_token),
            ("SHOPIFY_SHOP_DOMAIN", self.shopify_shop_domain),
        ]
        missing = [name for name, val in required if not val]
        if missing:
            logger.error(f"Missing required environment variables: {missing}")
            return False
        logger.info("Environment validation passed")
        return True

    def initialize_components(self) -> bool:
        try:
            self.shopify_client = ShopifyClient(
                self.shopify_admin_token, self.shopify_shop_domain
            )
            from duplicate_manager import DuplicateManager
            self.manager = DuplicateManager(self.shopify_client)
            logger.info("Components initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize components: {e}")
            return False

    def run_duplication(self, collection_id: str, dry_run: bool = False) -> bool:
        if not self.manager:
            logger.error("Manager not initialized")
            return False

        mode_str = "DRY RUN" if dry_run else "LIVE"
        logger.info(f"Starting product duplication [{mode_str}] for collection {collection_id}")
        print(f"\nStarting product duplication [{mode_str}] for collection {collection_id}")

        result = self.manager.duplicate_collection_products(
            collection_id=collection_id, dry_run=dry_run
        )

        # Print summary
        print(f"\n{'=' * 60}")
        print(f"Duplication Summary:")
        print(f"  Collection ID: {result.collection_id}")
        print(f"  Products with stock: {result.products_with_stock}")
        print(f"  Successfully duplicated: {result.products_duplicated}")
        print(f"  Failed: {result.products_failed}")
        print(f"  Execution time: {result.execution_time_seconds:.1f}s")

        if result.errors:
            print(f"\nErrors:")
            for error in result.errors:
                print(f"  - {error}")

        if result.results:
            print(f"\nDetails:")
            for r in result.results:
                status = "OK" if r.success else "FAIL"
                print(f"  [{status}] {r.source_title} -> {r.new_title}")

        print(f"{'=' * 60}")

        return result.success


def run_duplicate_products(
    collection_id: str = None, dry_run: bool = False
) -> bool:
    """Entry point called from root main.py"""
    orchestrator = DuplicateProductsOrchestrator()

    if not orchestrator.validate_environment():
        return False
    if not orchestrator.initialize_components():
        return False

    if not collection_id:
        try:
            collection_id = input("\nEnter collection ID: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nOperation cancelled")
            return False

    if not collection_id:
        logger.error("No collection ID provided")
        return False

    return orchestrator.run_duplication(collection_id=collection_id, dry_run=dry_run)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Duplicate products from a Shopify collection")
    parser.add_argument("--collection", "-c", required=True, help="Source collection ID")
    parser.add_argument("--dry-run", action="store_true", help="Analyze without making changes")

    args = parser.parse_args()

    success = run_duplicate_products(collection_id=args.collection, dry_run=args.dry_run)
    sys.exit(0 if success else 1)
