"""
批量删除Shopify所有商品标题的翻译

这个脚本会扫描所有商品，检测并删除标题的翻译（英语、法语、意大利语等）。
默认语言是西班牙语，但商品名应该使用英语，不需要翻译。
"""

import os
import sys
import logging
import argparse
import time
from typing import Dict, Any, List
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from shared.shopify_client import ShopifyClient
from check_translations import check_product_translations
from remove_translations import remove_product_translations

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_all_products(client: ShopifyClient, limit: int = None) -> List[Dict[str, Any]]:
    """
    获取所有商品

    Args:
        client: ShopifyClient实例
        limit: 限制商品数量，None表示获取全部

    Returns:
        商品列表
    """
    # 使用GraphQL查询商品
    query = """
    query getProducts($first: Int!, $after: String) {
      products(first: $first, after: $after) {
        pageInfo {
          hasNextPage
          endCursor
        }
        edges {
          node {
            id
            title
            handle
          }
        }
      }
    }
    """

    all_products = []
    has_next_page = True
    after_cursor = None
    page_size = 50

    logger.info(f"Starting to fetch product list{'(limit: '+str(limit)+')' if limit else '(all)'}...")

    try:
        while has_next_page:
            variables = {
                "first": page_size,
                "after": after_cursor
            }

            response = client.execute_graphql(query, variables)

            # 检查错误
            if "errors" in response:
                error_messages = [err.get("message", "Unknown error") for err in response["errors"]]
                logger.error(f"GraphQL错误: {', '.join(error_messages)}")
                break

            # 提取数据
            products_data = response.get("data", {}).get("products", {})
            edges = products_data.get("edges", [])
            page_info = products_data.get("pageInfo", {})

            # 添加商品
            for edge in edges:
                node = edge.get("node", {})
                all_products.append({
                    "id": node.get("id"),
                    "title": node.get("title"),
                    "handle": node.get("handle")
                })

            logger.info(f"Fetched {len(all_products)} products...")

            # Check if limit reached
            if limit and len(all_products) >= limit:
                all_products = all_products[:limit]
                logger.info(f"Reached limit ({limit}), stopping fetch")
                break

            # 检查是否有下一页
            has_next_page = page_info.get("hasNextPage", False)
            after_cursor = page_info.get("endCursor")

            # 短暂延迟避免API限流
            time.sleep(0.2)

        logger.info(f"Total fetched: {len(all_products)} products")
        return all_products

    except Exception as e:
        logger.error(f"Error fetching product list: {e}")
        return all_products


def batch_remove_translations(
    client: ShopifyClient,
    locales: List[str],
    dry_run: bool = False,
    limit: int = None
) -> Dict[str, Any]:
    """
    批量删除所有商品的标题翻译

    Args:
        client: ShopifyClient实例
        locales: 要删除的语言列表
        dry_run: 如果为True，只模拟不实际执行
        limit: 限制处理的商品数量

    Returns:
        处理结果统计
    """
    # 获取所有商品
    products = get_all_products(client, limit)

    if not products:
        logger.warning("No products found")
        return {
            "total_products": 0,
            "products_with_translations": 0,
            "products_processed": 0,
            "translations_removed": 0,
            "failed": 0,
        }

    stats = {
        "total_products": len(products),
        "products_with_translations": 0,
        "products_processed": 0,
        "translations_removed": 0,
        "failed": 0,
        "details": []
    }

    logger.info(f"\n{'='*60}")
    logger.info(f"Starting batch processing of {len(products)} products")
    logger.info(f"Mode: {'DRY RUN (Simulation)' if dry_run else 'Actual Deletion'}")
    logger.info(f"Target languages: {', '.join(locales)}")
    logger.info(f"{'='*60}\n")

    for i, product in enumerate(products, 1):
        product_id = product["id"].replace("gid://shopify/Product/", "")
        product_title = product["title"]

        logger.info(f"[{i}/{len(products)}] Processing: {product_title} (ID: {product_id})")

        try:
            # Check translations
            check_result = check_product_translations(client, product_id)

            if "error" in check_result:
                logger.error(f"  ❌ Failed to check translations: {check_result['error']}")
                stats["failed"] += 1
                continue

            # Get existing translations for this product
            existing_translations = check_result.get("translations", {})
            if not existing_translations:
                logger.info(f"  ✅ No translations, skipping")
                continue

            # Find translations to remove
            locales_to_remove = [loc for loc in locales if loc in existing_translations]
            if not locales_to_remove:
                logger.info(f"  ℹ️  Has translations but not in target languages, skipping")
                continue

            stats["products_with_translations"] += 1

            # Log translations being removed with their text
            for locale in locales_to_remove:
                trans_value = existing_translations[locale].get("value", "N/A")
                logger.info(f"  ⚠️  Found {locale} translation: {trans_value}")

            # Delete translations
            remove_result = remove_product_translations(
                client,
                product_id,
                locales_to_remove,
                dry_run=dry_run
            )

            # Update statistics
            if remove_result["removed"]:
                stats["products_processed"] += 1
                stats["translations_removed"] += len(remove_result["removed"])
                for locale in remove_result["removed"]:
                    trans_value = existing_translations[locale].get("value", "N/A")
                    logger.info(f"  ✅ Removed {locale}: {trans_value}")

            if remove_result["failed"]:
                stats["failed"] += len(remove_result["failed"])
                logger.error(f"  ❌ Failed to remove: {remove_result['failed']}")

            # 保存详情
            stats["details"].append({
                "product_id": product_id,
                "product_title": product_title,
                "translations_found": list(existing_translations.keys()),
                "translations_removed": remove_result["removed"],
                "translations_failed": remove_result["failed"],
            })

            # 短暂延迟避免API限流
            time.sleep(0.3)

        except Exception as e:
            logger.error(f"  ❌ Error processing product: {e}")
            stats["failed"] += 1
            continue

    return stats


def print_batch_report(stats: Dict[str, Any]):
    """Print batch processing report"""
    print(f"\n{'='*60}")
    print(f"Batch Processing Report")
    print(f"{'='*60}")
    print(f"Total products: {stats['total_products']}")
    print(f"Products with translations: {stats['products_with_translations']}")
    print(f"Successfully processed: {stats['products_processed']}")
    print(f"Translations removed: {stats['translations_removed']}")
    print(f"Failed: {stats['failed']}")
    print(f"{'='*60}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="批量删除所有Shopify商品标题的翻译"
    )
    parser.add_argument(
        "-l", "--locales",
        type=str,
        nargs="+",
        default=["en", "fr", "it"],
        help="要删除的语言列表 (默认: en fr it)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="模拟删除，不实际执行"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="限制处理的商品数量（用于测试）"
    )
    parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="自动确认删除，不询问"
    )

    args = parser.parse_args()

    # 加载环境变量
    load_dotenv()

    # 获取Shopify凭证
    admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
    shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")

    if not admin_token or not shop_domain:
        logger.error("Missing required environment variables: SHOPIFY_ADMIN_TOKEN and SHOPIFY_SHOP_DOMAIN")
        sys.exit(1)

    # 创建Shopify客户端
    client = ShopifyClient(admin_token, shop_domain)

    # Display warning
    print(f"\n⚠️  Warning: About to batch remove product title translations")
    print(f"Target languages: {', '.join(args.locales)}")
    print(f"Mode: {'DRY RUN (Simulation)' if args.dry_run else 'Actual Deletion'}")
    if args.limit:
        print(f"Limit: Only processing first {args.limit} products")

    # Ask for confirmation if not dry-run and no --yes flag
    if not args.dry_run and not args.yes:
        response = input("\nConfirm to continue? (yes/no): ")
        if response.lower() not in ["yes", "y"]:
            print("❌ Operation cancelled")
            sys.exit(0)

    # Start batch processing
    print(f"\n🔄 Starting batch processing...")
    stats = batch_remove_translations(
        client,
        args.locales,
        dry_run=args.dry_run,
        limit=args.limit
    )

    # 打印报告
    print_batch_report(stats)


if __name__ == "__main__":
    main()
