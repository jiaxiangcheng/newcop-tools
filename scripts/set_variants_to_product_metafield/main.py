"""
Main script for syncing product variants to custom.variants metafield.

This script fetches all products from Shopify and syncs their variant names
to the custom.variants metafield (list.single_line_text_field).

By default, it only updates products with empty custom.variants metafield.
Use --all flag to update all products regardless of current value.
"""
import os
import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from shared.shopify_client import ShopifyClient
from shared.logger import setup_logger
from scripts.set_variants_to_product_metafield.variants_manager import VariantsMetafieldManager


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Sync product variant names to custom.variants metafield"
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Update all products, even if custom.variants already has values (default: only update empty)"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Analyze what would be updated without making actual changes"
    )

    return parser.parse_args()


def main():
    """Main execution function."""
    # Load environment variables
    load_dotenv()

    # Setup logging using shared logger
    logger = setup_logger('set_variants_metafield', 'set_variants_metafield.log')

    # Parse arguments
    args = parse_arguments()

    logger.info("="*60)
    logger.info("🚀 STARTING VARIANTS METAFIELD SYNC")
    logger.info("="*60)
    logger.info(f"Mode: {'UPDATE ALL' if args.all else 'UPDATE EMPTY ONLY'}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info("="*60)

    try:
        # Initialize Shopify client
        shopify_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
        shopify_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")

        if not shopify_token or not shopify_domain:
            logger.error("Missing required environment variables: SHOPIFY_ADMIN_TOKEN, SHOPIFY_SHOP_DOMAIN")
            return 1

        shopify_client = ShopifyClient(shopify_token, shopify_domain)

        # Initialize variants manager
        manager = VariantsMetafieldManager(shopify_client)

        # Execute sync
        result = manager.sync_all_products(
            update_all=args.all,
            dry_run=args.dry_run
        )

        # Return appropriate exit code
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
