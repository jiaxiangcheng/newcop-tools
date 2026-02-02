"""
Main script for setting product types based on collection rules.

This script processes three collections:
1. Collection 639759778133: All products → "Accessories"
2. Collection 639750963541: Sneakers
   - With "retail" tag → "Retail Sneakers"
   - Without "retail" tag → "Resell Sneakers"
3. Collection 639759647061: All products → "Clothing"
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
from scripts.set_product_type.type_manager import ProductTypeManager


def setup_logging():
    """Configure logging for the script."""
    log_dir = project_root / "logs"
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / "set_product_type.log"

    # Configure root logger
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
        description="Set product types based on collection rules"
    )

    parser.add_argument(
        "--collection",
        type=str,
        help="Process only a specific collection ID (default: process all collections)"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Analyze what would be updated without making actual changes"
    )

    parser.add_argument(
        "--list-empty",
        action="store_true",
        help="List all products with empty product type"
    )

    return parser.parse_args()


def main():
    """Main execution function."""
    # Load environment variables
    load_dotenv()

    # Setup logging
    logger = setup_logging()

    # Parse arguments
    args = parse_arguments()

    logger.info("="*60)
    logger.info("🚀 STARTING PRODUCT TYPE SYNC")
    logger.info("="*60)
    
    # Handle list empty type option
    if args.list_empty:
        logger.info("Mode: LIST ACTIVE PRODUCTS WITH EMPTY TYPE")
        logger.info("="*60)
        
        try:
            # Initialize Shopify client
            shopify_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
            shopify_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")

            if not shopify_token or not shopify_domain:
                logger.error("Missing required environment variables: SHOPIFY_ADMIN_TOKEN, SHOPIFY_SHOP_DOMAIN")
                return 1

            shopify_client = ShopifyClient(shopify_token, shopify_domain)
            manager = ProductTypeManager(shopify_client)

            # Get products with empty type (with auto-classification enabled)
            products, stats = manager.get_products_with_empty_type(auto_classify=True)

            if products:
                logger.info("\n📋 ACTIVE Products with empty type (no auto-classification match):")
                logger.info("-" * 60)
                for i, product in enumerate(products, 1):
                    logger.info(
                        f"{i}. ID: {product['id']} | "
                        f"Title: {product['title']} | "
                        f"Vendor: {product.get('vendor', 'N/A')}"
                    )
                logger.info("-" * 60)
                logger.info(f"\n✅ Total: {len(products)} products to export (after auto-classification)")
                
                # Export to Excel
                logger.info("\n📊 Exporting to Excel...")
                excel_path = manager.export_products_with_empty_type_to_excel(products)
                if excel_path:
                    logger.info(f"✅ Excel file exported to: {excel_path}")
            else:
                logger.info("\n✅ No products to export (all were auto-classified or no products found)")
                if stats.get("auto_classified", 0) > 0:
                    logger.info(f"   ({stats['auto_classified']} products were auto-classified and updated)")

            return 0

        except Exception as e:
            logger.error(f"❌ Fatal error: {str(e)}", exc_info=True)
            return 1

    if args.collection:
        logger.info(f"Collection: {args.collection}")
    else:
        logger.info("Mode: ALL COLLECTIONS")
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

        # Initialize type manager
        manager = ProductTypeManager(shopify_client)

        # Execute sync
        if args.collection:
            # Process single collection
            result = manager.process_collection(args.collection, dry_run=args.dry_run)
            if not result.get("success", True) and result.get("error"):
                logger.error(f"Failed to process collection: {result.get('error')}")
                return 1
        else:
            # Process all collections
            result = manager.sync_all_collections(dry_run=args.dry_run)

        # Return appropriate exit code
        total_failed = result.get("total_failed", 0) if not args.collection else result.get("failed", 0)
        if total_failed > 0:
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
