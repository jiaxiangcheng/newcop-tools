"""
Remove Shopify product title translations

This script uses GraphQL API to remove product title translations.
The default language is Spanish, but product names should be in English without translations to other languages.
"""

import os
import sys
import logging
import argparse
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from shared.shopify_client import ShopifyClient
from check_translations import check_product_translations, print_translation_report

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def remove_product_translations(
    client: ShopifyClient,
    product_id: str,
    locales: List[str],
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Remove product title translations for specified languages

    Args:
        client: ShopifyClient instance
        product_id: Product ID
        locales: List of languages to remove (e.g., ["en", "fr", "it"])
        dry_run: If True, simulate removal without actually executing

    Returns:
        Dictionary containing removal results
    """
    # 转换为GID格式
    if not product_id.startswith("gid://"):
        product_gid = f"gid://shopify/Product/{product_id}"
    else:
        product_gid = product_id

    results = {
        "product_id": product_id,
        "product_gid": product_gid,
        "removed": [],
        "failed": [],
        "dry_run": dry_run,
    }

    for locale in locales:
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Removing {locale} translation for product {product_id}...")

        if dry_run:
            results["removed"].append(locale)
            logger.info(f"[DRY RUN] Will remove {locale} translation")
            continue

        # GraphQL mutation - 删除翻译
        # 使用translationsRemove mutation
        mutation = """
        mutation removeProductTranslation($resourceId: ID!, $locales: [String!]!, $translationKeys: [String!]!) {
          translationsRemove(resourceId: $resourceId, locales: $locales, translationKeys: $translationKeys) {
            userErrors {
              field
              message
            }
            translations {
              key
              locale
            }
          }
        }
        """

        # Prepare translation keys to remove
        variables = {
            "resourceId": product_gid,
            "locales": [locale],  # locales is an array
            "translationKeys": ["title"]  # Only need key name, no locale suffix
        }

        try:
            response = client.execute_graphql(mutation, variables)

            # Check for errors
            if "errors" in response:
                error_messages = [err.get("message", "Unknown error") for err in response["errors"]]
                logger.error(f"GraphQL errors: {', '.join(error_messages)}")
                results["failed"].append({
                    "locale": locale,
                    "error": error_messages
                })
                continue

            # Check for user errors
            user_errors = response.get("data", {}).get("translationsRemove", {}).get("userErrors", [])
            if user_errors:
                error_messages = [err.get("message", "Unknown error") for err in user_errors]
                logger.error(f"Failed to remove translation: {', '.join(error_messages)}")
                results["failed"].append({
                    "locale": locale,
                    "error": error_messages
                })
                continue

            results["removed"].append(locale)
            logger.info(f"✅ Successfully removed {locale} translation")

        except Exception as e:
            logger.error(f"Error removing {locale} translation: {e}")
            results["failed"].append({
                "locale": locale,
                "error": str(e)
            })

    return results


def print_removal_report(result: Dict[str, Any]):
    """Print removal report"""
    print(f"\n{'='*60}")
    print(f"Translation Removal Report")
    print(f"{'='*60}")
    print(f"Product ID: {result['product_id']}")
    print(f"Mode: {'DRY RUN (Simulation)' if result['dry_run'] else 'Actual Deletion'}")

    if result["removed"]:
        print(f"\n✅ Successfully removed translations ({len(result['removed'])}):")
        for locale in result["removed"]:
            print(f"  - {locale}")

    if result["failed"]:
        print(f"\n❌ Failed to remove translations ({len(result['failed'])}):")
        for failure in result["failed"]:
            locale = failure["locale"]
            error = failure["error"]
            print(f"  - {locale}: {error}")

    if not result["removed"] and not result["failed"]:
        print(f"\n⚠️  No translations to remove")

    print(f"\n{'='*60}")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Remove Shopify product title translations"
    )
    parser.add_argument(
        "-p", "--product-id",
        type=str,
        default="9941119435093",
        help="Product ID (default: 9941119435093)"
    )
    parser.add_argument(
        "-l", "--locales",
        type=str,
        nargs="+",
        default=["en", "fr", "it"],
        help="List of languages to remove (default: en fr it)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate removal without actually executing"
    )
    parser.add_argument(
        "--check-first",
        action="store_true",
        help="Check translations before removing"
    )
    parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Auto-confirm removal without asking"
    )

    args = parser.parse_args()

    # Load environment variables
    load_dotenv()

    # Get Shopify credentials
    admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
    shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")

    if not admin_token or not shop_domain:
        logger.error("Missing required environment variables: SHOPIFY_ADMIN_TOKEN and SHOPIFY_SHOP_DOMAIN")
        sys.exit(1)

    # Create Shopify client
    client = ShopifyClient(admin_token, shop_domain)

    # If check-first is enabled, check translations first
    if args.check_first:
        print(f"\n🔍 First checking translations for product {args.product_id}...")
        result = check_product_translations(client, args.product_id)
        print_translation_report(result)

        if "error" in result:
            sys.exit(1)

        # If no translations, no need to remove
        if not result.get("translations"):
            print("\n✅ This product has no translations, no action needed")
            sys.exit(0)

        # Only remove translations that actually exist
        existing_locales = list(result["translations"].keys())
        locales_to_remove = [loc for loc in args.locales if loc in existing_locales]

        if not locales_to_remove:
            print(f"\n✅ This product has no translations in specified languages ({', '.join(args.locales)})")
            sys.exit(0)

        print(f"\n⚠️  Will remove translations in: {', '.join(locales_to_remove)}")

        # Ask for confirmation if not dry-run and no --yes flag
        if not args.dry_run and not args.yes:
            response = input("\nConfirm removal? (yes/no): ")
            if response.lower() not in ["yes", "y"]:
                print("❌ Removal cancelled")
                sys.exit(0)

        args.locales = locales_to_remove

    # Remove translations
    print(f"\n🗑️  {'[DRY RUN] ' if args.dry_run else ''}Starting to remove translations for product {args.product_id}...")
    result = remove_product_translations(
        client,
        args.product_id,
        args.locales,
        dry_run=args.dry_run
    )

    # Print report
    print_removal_report(result)

    # Exit with code 1 if there were failures
    if result["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
