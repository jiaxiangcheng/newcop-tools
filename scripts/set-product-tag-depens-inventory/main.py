"""
Main script for setting product tags based on inventory status.

Scans all active products in Shopify:
- If any variant has inventory > 0 -> sets "instore-online" tag
- If no variant has inventory > 0 -> sets "instore-only" tag
"""
import os
import sys
import logging
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from shared.shopify_client import ShopifyClient

# Directory name has hyphens, so add the script directory to path for direct import
sys.path.insert(0, str(Path(__file__).parent))
from tag_manager import InventoryTagManager


def setup_logging():
    """Configure logging for the script."""
    log_dir = project_root / "logs"
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / "set_product_tag_inventory.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Set product tags (instore-online / instore-only) based on inventory"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Analyze what would be updated without making actual changes"
    )

    return parser.parse_args()


def run_inventory_tag_sync(dry_run: bool = False) -> bool:
    """
    Entry point callable from main.py CLI launcher.

    Args:
        dry_run: If True, only analyze without making changes

    Returns:
        True if successful, False otherwise
    """
    load_dotenv()
    logger = setup_logging()

    try:
        shopify_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
        shopify_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")

        if not shopify_token or not shopify_domain:
            logger.error("Missing required environment variables: SHOPIFY_ADMIN_TOKEN, SHOPIFY_SHOP_DOMAIN")
            return False

        logger.info("=" * 60)
        logger.info("🏷️  STARTING INVENTORY TAG SYNC")
        logger.info("=" * 60)
        logger.info(f"Shop: {shopify_domain}")
        logger.info(f"Dry run: {dry_run}")
        logger.info("=" * 60)

        shopify_client = ShopifyClient(shopify_token, shopify_domain)
        manager = InventoryTagManager(shopify_client)

        result = manager.sync_all_products(dry_run=dry_run)

        return result.get("failed", 0) == 0

    except KeyboardInterrupt:
        logger.info("\n⚠️  Process interrupted by user")
        return False
    except Exception as e:
        logger.error(f"❌ Fatal error: {str(e)}", exc_info=True)
        return False


def main():
    """Main execution function."""
    load_dotenv()
    logger = setup_logging()
    args = parse_arguments()

    try:
        shopify_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
        shopify_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")

        if not shopify_token or not shopify_domain:
            logger.error("Missing required environment variables: SHOPIFY_ADMIN_TOKEN, SHOPIFY_SHOP_DOMAIN")
            return 1

        logger.info("=" * 60)
        logger.info("🏷️  STARTING INVENTORY TAG SYNC")
        logger.info("=" * 60)
        logger.info(f"Shop: {shopify_domain}")
        logger.info(f"Dry run: {args.dry_run}")
        logger.info("=" * 60)

        shopify_client = ShopifyClient(shopify_token, shopify_domain)
        manager = InventoryTagManager(shopify_client)

        result = manager.sync_all_products(dry_run=args.dry_run)

        if result.get("failed", 0) > 0:
            return 1

        return 0

    except KeyboardInterrupt:
        logger.info("\n⚠️  Process interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"❌ Fatal error: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
