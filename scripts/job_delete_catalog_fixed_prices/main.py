"""
CLI entry point for the Delete Catalog Fixed Prices job.

Given a Shopify Catalog ID (the numeric ID seen in admin URLs, e.g.
179292701013), this script:
  1) Resolves the associated PriceList ID via GraphQL.
  2) Fetches all FIXED-origin prices on that PriceList (paginated).
  3) Deletes them in batches via the priceListFixedPricesDelete mutation.

Supports --dry-run for safe preview and requires explicit confirmation
before executing real deletions (unless --yes is passed).
"""
import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from shared.shopify_client import ShopifyClient
from shared.logger import setup_logger
from scripts.job_delete_catalog_fixed_prices.pricelist_manager import (
    PriceListFixedPricesManager,
)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Delete all fixed prices in the PriceList of a given Shopify Catalog"
    )
    parser.add_argument(
        "--catalog-id",
        type=str,
        default=None,
        help="Numeric Catalog ID from the Shopify admin URL (e.g. 179292701013). "
             "If omitted, you will be prompted interactively.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve the PriceList and count fixed prices without deleting anything",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip the interactive confirmation before deletion",
    )
    return parser.parse_args()


def run_delete_catalog_fixed_prices(
    catalog_id: str,
    dry_run: bool = False,
    skip_confirm: bool = False,
) -> bool:
    """Programmatic entry point. Returns True on success (or clean dry-run)."""
    load_dotenv()
    logger = setup_logger("delete_catalog_fixed_prices", "delete_catalog_fixed_prices.log")

    logger.info("=" * 60)
    logger.info("🚀 STARTING DELETE CATALOG FIXED PRICES")
    logger.info("=" * 60)
    logger.info(f"Catalog ID: {catalog_id}")
    logger.info(f"Dry run: {dry_run}")
    logger.info("=" * 60)

    if not catalog_id or not str(catalog_id).strip():
        logger.error("Catalog ID is required")
        return False

    shopify_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
    shopify_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")
    if not shopify_token or not shopify_domain:
        logger.error("Missing required environment variables: SHOPIFY_ADMIN_TOKEN, SHOPIFY_SHOP_DOMAIN")
        return False

    shopify_client = ShopifyClient(shopify_token, shopify_domain)
    manager = PriceListFixedPricesManager(shopify_client, logger)

    info = manager.get_price_list_id(catalog_id)
    if not info:
        return False

    entries = manager.fetch_all_fixed_prices(info.price_list_id)
    total_found = len(entries)

    if entries:
        logger.info("📋 Sample (up to 5):")
        for entry in entries[:5]:
            logger.info(
                f"   variant={entry.variant_id} price={entry.price_amount} {entry.currency_code}"
            )

    if dry_run:
        logger.info(f"🧪 [DRY RUN] Would delete {total_found} fixed prices. No changes made.")
        return True

    if total_found == 0:
        logger.info("✅ Nothing to delete.")
        return True

    if not skip_confirm:
        print()
        print(f"⚠️  About to delete {total_found} fixed prices from:")
        print(f"     Catalog:    {info.catalog_id}")
        print(f"     PriceList:  {info.price_list_id} ({info.price_list_currency})")
        print("This action is irreversible.")
        try:
            answer = input("Type 'yes' to confirm: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            logger.info("Cancelled by user")
            return False
        if answer != "yes":
            logger.info("Cancelled by user (no confirmation)")
            return False

    variant_ids = [e.variant_id for e in entries]
    deleted, failed, _errors = manager.delete_in_batches(info.price_list_id, variant_ids)

    logger.info("=" * 60)
    logger.info("🏁 SUMMARY")
    logger.info(f"   Catalog:        {info.catalog_id}")
    logger.info(f"   PriceList:      {info.price_list_id}")
    logger.info(f"   Total found:    {total_found}")
    logger.info(f"   Total deleted:  {deleted}")
    logger.info(f"   Total failed:   {failed}")
    logger.info("=" * 60)

    return failed == 0


def main():
    args = parse_arguments()

    catalog_id = args.catalog_id
    if not catalog_id:
        try:
            catalog_id = input("🔸 Enter Catalog ID (numeric, e.g. 179292701013): ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n⚠️  Cancelled")
            return 130

    if not catalog_id:
        print("❌ Catalog ID is required")
        return 1

    try:
        success = run_delete_catalog_fixed_prices(
            catalog_id=catalog_id,
            dry_run=args.dry_run,
            skip_confirm=args.yes,
        )
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        return 130
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        return 1

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
