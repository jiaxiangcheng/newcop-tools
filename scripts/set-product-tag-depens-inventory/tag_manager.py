"""
Manager for setting product tags based on inventory status.

Logic:
- If any variant has inventory > 0 -> add "instore-online" tag
- If no variant has inventory > 0 -> add "instore-only" tag
- Removes the conflicting tag if present
"""
import logging
import sys
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# Ensure local directory is in path for direct import (directory has hyphens)
sys.path.insert(0, str(Path(__file__).parent))
from models import ProductInventoryInfo, VariantInventory, TagSyncResult

logger = logging.getLogger(__name__)


class InventoryTagManager:
    """Manages product tag assignment based on inventory levels."""

    TAG_IN_STOCK = "instore-online"
    TAG_OUT_OF_STOCK = "instore-only"
    MAX_CONCURRENT_UPDATES = 5

    def __init__(self, shopify_client):
        self.shopify_client = shopify_client

    def fetch_all_products_with_inventory(self) -> List[ProductInventoryInfo]:
        """
        Fetch all active products with their variant inventory and tags using GraphQL.

        Returns:
            List of ProductInventoryInfo objects
        """
        logger.info("📦 Fetching all products with inventory from Shopify...")

        query = """
        query GetProductsInventory($cursor: String) {
          products(first: 50, after: $cursor, query: "status:active") {
            edges {
              cursor
              node {
                id
                title
                tags
                totalInventory
                variants(first: 100) {
                  edges {
                    node {
                      id
                      title
                      inventoryQuantity
                    }
                  }
                }
              }
            }
            pageInfo {
              hasNextPage
            }
          }
        }
        """

        products = []
        cursor = None
        has_next_page = True

        while has_next_page:
            variables = {"cursor": cursor} if cursor else {}
            result = self.shopify_client.execute_graphql(query, variables)

            if not result or "data" not in result:
                logger.error("Failed to fetch products")
                break

            edges = result["data"]["products"]["edges"]

            for edge in edges:
                node = edge["node"]

                variants = []
                for variant_edge in node.get("variants", {}).get("edges", []):
                    vn = variant_edge["node"]
                    variants.append(VariantInventory(
                        id=vn["id"],
                        title=vn["title"],
                        inventory_quantity=vn.get("inventoryQuantity", 0)
                    ))

                products.append(ProductInventoryInfo(
                    id=node["id"],
                    title=node["title"],
                    tags=node.get("tags", []),
                    variants=variants,
                    total_inventory=node.get("totalInventory", 0)
                ))

            page_info = result["data"]["products"]["pageInfo"]
            has_next_page = page_info.get("hasNextPage", False)

            if has_next_page and edges:
                cursor = edges[-1]["cursor"]

            logger.info(f"  Fetched {len(products)} products so far...")

        logger.info(f"✅ Fetched {len(products)} products total")
        return products

    def _build_new_tags(self, product: ProductInventoryInfo) -> List[str]:
        """
        Build the new tag list for a product.
        Removes any existing instore-online/instore-only tags and adds the correct one.
        """
        # Remove both inventory tags from existing tags
        new_tags = [
            tag for tag in product.tags
            if tag not in (self.TAG_IN_STOCK, self.TAG_OUT_OF_STOCK)
        ]
        # Add the correct tag
        new_tags.append(product.expected_tag)
        return new_tags

    def update_product_tags(self, product: ProductInventoryInfo) -> TagSyncResult:
        """
        Update a single product's tags based on inventory.

        Uses GraphQL productUpdate mutation to set tags.
        """
        try:
            new_tags = self._build_new_tags(product)
            expected_tag = product.expected_tag
            old_tag = product.current_inventory_tag

            mutation = """
            mutation UpdateProductTags($input: ProductInput!) {
              productUpdate(input: $input) {
                product {
                  id
                  tags
                }
                userErrors {
                  field
                  message
                }
              }
            }
            """

            variables = {
                "input": {
                    "id": product.id,
                    "tags": new_tags
                }
            }

            result = self.shopify_client.execute_graphql(mutation, variables)

            if not result or "data" not in result:
                return TagSyncResult(
                    product_id=product.id,
                    product_title=product.title,
                    success=False,
                    old_tag=old_tag,
                    new_tag=expected_tag,
                    error="GraphQL query failed"
                )

            user_errors = result["data"]["productUpdate"].get("userErrors", [])
            if user_errors:
                error_messages = [f"{err['field']}: {err['message']}" for err in user_errors]
                return TagSyncResult(
                    product_id=product.id,
                    product_title=product.title,
                    success=False,
                    old_tag=old_tag,
                    new_tag=expected_tag,
                    error=", ".join(error_messages)
                )

            return TagSyncResult(
                product_id=product.id,
                product_title=product.title,
                success=True,
                old_tag=old_tag,
                new_tag=expected_tag
            )

        except Exception as e:
            logger.error(f"Error updating product {product.title}: {str(e)}")
            return TagSyncResult(
                product_id=product.id,
                product_title=product.title,
                success=False,
                old_tag=product.current_inventory_tag,
                new_tag=product.expected_tag,
                error=str(e)
            )

    def sync_all_products(self, dry_run: bool = False) -> Dict[str, Any]:
        """
        Scan all products and set tags based on inventory.

        Args:
            dry_run: If True, only analyze without making changes

        Returns:
            Dictionary with sync statistics
        """
        products = self.fetch_all_products_with_inventory()

        if not products:
            logger.warning("No products found")
            return {
                "total_products": 0,
                "updated": 0,
                "skipped": 0,
                "failed": 0,
                "in_stock": 0,
                "out_of_stock": 0,
            }

        # Categorize products
        in_stock_count = sum(1 for p in products if p.has_stock)
        out_of_stock_count = len(products) - in_stock_count
        to_update = [p for p in products if p.needs_update]
        skipped_count = len(products) - len(to_update)

        logger.info(f"📊 Total products: {len(products)}")
        logger.info(f"  In stock (instore-online): {in_stock_count}")
        logger.info(f"  Out of stock (instore-only): {out_of_stock_count}")
        logger.info(f"  Need tag update: {len(to_update)}")
        logger.info(f"  Already correct: {skipped_count}")

        if dry_run:
            logger.info("\n🔍 DRY RUN - No changes will be made")
            for product in to_update:
                old = product.current_inventory_tag or "(none)"
                new = product.expected_tag
                stock_info = f"inventory={product.total_inventory}"
                logger.info(f"  Would update: {product.title} [{old} -> {new}] ({stock_info})")

            return {
                "total_products": len(products),
                "to_update": len(to_update),
                "skipped": skipped_count,
                "in_stock": in_stock_count,
                "out_of_stock": out_of_stock_count,
                "dry_run": True,
            }

        # Perform updates concurrently
        updated_count = 0
        failed_count = 0
        results = []

        if to_update:
            logger.info(f"\n🔄 Updating {len(to_update)} products...")

            with ThreadPoolExecutor(max_workers=self.MAX_CONCURRENT_UPDATES) as executor:
                future_to_product = {
                    executor.submit(self.update_product_tags, product): product
                    for product in to_update
                }

                for future in as_completed(future_to_product):
                    result = future.result()
                    results.append(result)

                    if result.success:
                        updated_count += 1
                        old = result.old_tag or "(none)"
                        logger.info(
                            f"✅ [{updated_count}/{len(to_update)}] {result.product_title} "
                            f"[{old} -> {result.new_tag}]"
                        )
                    else:
                        failed_count += 1
                        logger.error(
                            f"❌ [{updated_count + failed_count}/{len(to_update)}] "
                            f"{result.product_title} - {result.error}"
                        )

        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("📊 SYNC SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total products: {len(products)}")
        logger.info(f"  In stock (instore-online): {in_stock_count}")
        logger.info(f"  Out of stock (instore-only): {out_of_stock_count}")
        logger.info(f"✅ Updated: {updated_count}")
        logger.info(f"⏭️  Already correct: {skipped_count}")
        logger.info(f"❌ Failed: {failed_count}")
        logger.info("=" * 60)

        return {
            "total_products": len(products),
            "updated": updated_count,
            "skipped": skipped_count,
            "failed": failed_count,
            "in_stock": in_stock_count,
            "out_of_stock": out_of_stock_count,
            "results": results,
        }
