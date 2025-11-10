"""
Check Shopify product title translations

This script uses GraphQL API to query product title translations and detect translations in English, French, Italian, etc.
The default language is Spanish, but product names should be in English without translations.
"""

import os
import sys
import logging
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from shared.shopify_client import ShopifyClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_product_translations(client: ShopifyClient, product_id: str) -> Dict[str, Any]:
    """
    Check product title translations

    Args:
        client: ShopifyClient instance
        product_id: Product ID (can be numeric ID or GID format)

    Returns:
        Dictionary containing translation information
    """
    # 转换为GID格式
    if not product_id.startswith("gid://"):
        product_gid = f"gid://shopify/Product/{product_id}"
    else:
        product_gid = product_id

    # GraphQL查询 - 获取商品标题和翻译
    # 需要查询每个语言的翻译
    query = """
    query getProductTranslations($productId: ID!) {
      product(id: $productId) {
        id
        title
      }

      # 查询所有可用的翻译
      translatableResource(resourceId: $productId) {
        resourceId
        translatableContent {
          key
          value
          digest
          locale
        }
        translations(locale: "en") {
          key
          value
          locale
          outdated
        }
      }

      # 查询法语翻译
      translatableResourceFr: translatableResource(resourceId: $productId) {
        translations(locale: "fr") {
          key
          value
          locale
          outdated
        }
      }

      # 查询意大利语翻译
      translatableResourceIt: translatableResource(resourceId: $productId) {
        translations(locale: "it") {
          key
          value
          locale
          outdated
        }
      }
    }
    """

    variables = {"productId": product_gid}

    logger.info(f"Querying translations for product {product_id}...")

    try:
        response = client.execute_graphql(query, variables)

        # Check for errors
        if "errors" in response:
            error_messages = [err.get("message", "Unknown error") for err in response["errors"]]
            logger.error(f"GraphQL errors: {', '.join(error_messages)}")
            return {"error": error_messages}

        # Extract data
        data = response.get("data", {})
        product_data = data.get("product")
        translatable_resource = data.get("translatableResource")
        translatable_resource_fr = data.get("translatableResourceFr")
        translatable_resource_it = data.get("translatableResourceIt")

        result = {
            "product_id": product_id,
            "product_gid": product_gid,
            "title": product_data.get("title") if product_data else None,
            "translations": {},
            "translatable_content": {},
        }

        # Process translations from translatable resource
        if translatable_resource:
            # Original content (default language)
            for content in translatable_resource.get("translatableContent", []):
                key = content.get("key")
                if key == "title":
                    result["translatable_content"]["default"] = {
                        "locale": content.get("locale"),
                        "value": content.get("value"),
                    }

            # English translations
            for translation in translatable_resource.get("translations", []):
                key = translation.get("key")
                if key == "title":
                    locale = translation.get("locale")
                    result["translations"][locale] = {
                        "value": translation.get("value"),
                        "outdated": translation.get("outdated", False),
                    }

        # Process French translations
        if translatable_resource_fr:
            for translation in translatable_resource_fr.get("translations", []):
                key = translation.get("key")
                if key == "title":
                    locale = translation.get("locale")
                    result["translations"][locale] = {
                        "value": translation.get("value"),
                        "outdated": translation.get("outdated", False),
                    }

        # Process Italian translations
        if translatable_resource_it:
            for translation in translatable_resource_it.get("translations", []):
                key = translation.get("key")
                if key == "title":
                    locale = translation.get("locale")
                    result["translations"][locale] = {
                        "value": translation.get("value"),
                        "outdated": translation.get("outdated", False),
                    }

        return result

    except Exception as e:
        logger.error(f"Error querying product translations: {e}")
        return {"error": str(e)}


def print_translation_report(result: Dict[str, Any]):
    """Print translation report"""
    if "error" in result:
        print(f"\n❌ Error: {result['error']}")
        return

    print(f"\n{'='*60}")
    print(f"Product Translation Check Report")
    print(f"{'='*60}")
    print(f"Product ID: {result['product_id']}")
    print(f"Product GID: {result['product_gid']}")
    print(f"Current Title: {result['title']}")

    # Default language content
    if result.get("translatable_content", {}).get("default"):
        default_content = result["translatable_content"]["default"]
        print(f"\nDefault Language: {default_content['locale']}")
        print(f"  Value: {default_content['value']}")

    # Translation list
    translations = result.get("translations", {})
    if translations:
        print(f"\nFound {len(translations)} translation(s):")
        for locale, trans_data in translations.items():
            status = "⚠️ Outdated" if trans_data.get("outdated") else "✅ Current"
            print(f"\n  Language: {locale} {status}")
            print(f"    Value: {trans_data['value']}")
    else:
        print("\n✅ No translations found")

    print(f"\n{'='*60}")

    # Summary
    if translations:
        print(f"\n⚠️  This product has title translations in: {', '.join(translations.keys())}")
        print(f"Recommendation: Remove these translations as the default field already uses English names")
    else:
        print(f"\n✅ This product has no title translations, no action needed")


def main():
    """Main function"""
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

    # Test product ID
    test_product_id = "9941119435093"

    print(f"\n🔍 Starting translation check for product {test_product_id}...")

    # Check translations
    result = check_product_translations(client, test_product_id)

    # Print report
    print_translation_report(result)


if __name__ == "__main__":
    main()
