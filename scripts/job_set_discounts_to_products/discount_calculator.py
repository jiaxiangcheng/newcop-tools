"""
Discount Calculator Manager

Calculates discount percentages for product variants and updates product metafields.
"""
import logging
import time
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from scripts.job_set_discounts_to_products.models import (
    VariantDiscount,
    ProductDiscounts,
    DiscountSyncResult
)

logger = logging.getLogger(__name__)


class DiscountCalculator:
    """Manages discount calculation and synchronization"""

    METAFIELD_NAMESPACE = "custom"
    METAFIELD_KEY = "discounts"
    MAX_CONCURRENT_UPDATES = 5  # Limit concurrent API calls

    def __init__(self, shopify_client):
        """
        Initialize discount calculator

        Args:
            shopify_client: Shopify API client instance
        """
        self.shopify_client = shopify_client

    def sync_product_discounts(
        self,
        dry_run: bool = False
    ) -> DiscountSyncResult:
        """
        Calculate and sync discount percentages for all products

        Args:
            dry_run: If True, only analyze without making changes

        Returns:
            DiscountSyncResult with sync statistics
        """
        start_time = time.time()
        logger.info(f"💰 Starting Product Discount Sync (dry_run={dry_run})")

        # Step 1: Fetch all products with variants
        logger.info("📥 Fetching all products with variants from Shopify...")
        products_data = self._fetch_all_products()

        logger.info(f"📦 Fetched {len(products_data)} products")

        # Step 2: Analyze discounts for each product
        logger.info("🔍 Analyzing discounts for all products...")
        products_with_discounts = []
        all_unique_discounts = set()

        for product_data in products_data:
            product_discounts = self._analyze_product_discounts(product_data)

            # Track all unique discounts
            unique_discounts = product_discounts.calculate_unique_discounts()
            all_unique_discounts.update(unique_discounts)

            # Only track products with discounts
            if unique_discounts:
                products_with_discounts.append(product_discounts)

        logger.info(f"📊 Found {len(products_with_discounts)} products with discounts")
        logger.info(f"📊 Total unique discount percentages: {sorted(all_unique_discounts)}")

        # Step 3: Filter products that need updates
        products_to_update = [
            p for p in products_with_discounts
            if p.needs_update()
        ]

        logger.info(f"🔄 {len(products_to_update)} products need metafield updates")

        # Step 4: Execute updates (if not dry run)
        products_updated = 0
        products_failed = 0
        failed_product_ids = []

        if not dry_run and products_to_update:
            logger.info("🚀 Executing metafield updates...")
            update_results = self._execute_updates(products_to_update)
            products_updated = update_results["success"]
            products_failed = update_results["failed"]
            failed_product_ids = update_results["failed_ids"]

            logger.info(f"✅ Updates completed: {products_updated} successful, {products_failed} failed")
        else:
            if dry_run:
                logger.info("🧪 Dry run mode - no changes made")
            else:
                logger.info("ℹ️  No updates needed")

        execution_time = time.time() - start_time

        # Build result
        result = DiscountSyncResult(
            total_products_processed=len(products_data),
            products_with_discounts=len(products_with_discounts),
            products_updated=products_updated,
            products_failed=products_failed,
            failed_product_ids=failed_product_ids,
            total_unique_discount_percentages=all_unique_discounts,
            execution_time_seconds=execution_time,
            dry_run=dry_run
        )

        self._log_summary(result, products_to_update)
        return result

    def _fetch_all_products(self) -> List[Dict[str, Any]]:
        """Fetch all products with variants using GraphQL"""
        query = """
        query getAllProducts($cursor: String) {
          products(first: 250, after: $cursor) {
            pageInfo {
              hasNextPage
              endCursor
            }
            edges {
              node {
                id
                legacyResourceId
                title
                variants(first: 100) {
                  edges {
                    node {
                      id
                      legacyResourceId
                      title
                      price
                      compareAtPrice
                    }
                  }
                }
                metafield(namespace: "custom", key: "discounts") {
                  id
                  value
                  type
                }
              }
            }
          }
        }
        """

        all_products = []
        cursor = None
        has_next_page = True
        page_count = 0

        while has_next_page:
            page_count += 1
            variables = {"cursor": cursor} if cursor else {}

            response = self.shopify_client.execute_graphql(query, variables)

            if "errors" in response:
                logger.error(f"GraphQL error: {response['errors']}")
                break

            data = response.get("data", {})
            products_data = data.get("products", {})
            edges = products_data.get("edges", [])

            for edge in edges:
                node = edge.get("node", {})
                all_products.append(node)

            # Check for next page
            page_info = products_data.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")

            logger.debug(f"Fetched page {page_count}, {len(edges)} products (total: {len(all_products)})")

            # Progress update every 5 pages
            if page_count % 5 == 0:
                logger.info(f"  Progress: Fetched {len(all_products)} products...")

        logger.info(f"✅ Fetched {len(all_products)} products in {page_count} pages")
        return all_products

    def _analyze_product_discounts(self, product_data: Dict[str, Any]) -> ProductDiscounts:
        """Analyze discounts for a single product"""
        product_id = int(product_data.get("legacyResourceId", 0))
        product_title = product_data.get("title", "Unknown")

        # Parse variants
        variants = []
        variant_edges = product_data.get("variants", {}).get("edges", [])

        for variant_edge in variant_edges:
            variant_node = variant_edge.get("node", {})
            variant_id = int(variant_node.get("legacyResourceId", 0))
            variant_title = variant_node.get("title")
            price = float(variant_node.get("price", 0))
            compare_at_price_str = variant_node.get("compareAtPrice")

            # Parse compare at price
            compare_at_price = None
            if compare_at_price_str:
                try:
                    compare_at_price = float(compare_at_price_str)
                except (ValueError, TypeError):
                    pass

            variant = VariantDiscount(
                variant_id=variant_id,
                variant_title=variant_title,
                price=price,
                compare_at_price=compare_at_price
            )
            variants.append(variant)

        # Parse current metafield value
        current_discounts = []
        metafield = product_data.get("metafield")
        if metafield:
            value = metafield.get("value", "")
            # Value is a JSON array string like '["10","15","20"]'
            try:
                import json
                current_discounts = json.loads(value) if value else []
            except (json.JSONDecodeError, TypeError):
                # Try splitting by comma as fallback
                if value:
                    current_discounts = [v.strip() for v in value.split(",")]

        return ProductDiscounts(
            product_id=product_id,
            product_title=product_title,
            variants=variants,
            current_metafield_discounts=current_discounts
        )

    def _execute_updates(
        self,
        products: List[ProductDiscounts]
    ) -> Dict[str, Any]:
        """
        Execute discount metafield updates concurrently

        Args:
            products: List of products to update

        Returns:
            Dictionary with success/failure counts
        """
        successful = 0
        failed = 0
        failed_ids = []

        # Process updates with controlled concurrency
        with ThreadPoolExecutor(max_workers=self.MAX_CONCURRENT_UPDATES) as executor:
            future_to_product = {
                executor.submit(
                    self._update_product_discounts,
                    product
                ): product
                for product in products
            }

            for future in as_completed(future_to_product):
                product = future_to_product[future]
                try:
                    success = future.result()
                    if success:
                        successful += 1
                        logger.debug(f"  ✅ Updated: {product.product_title}")
                    else:
                        failed += 1
                        failed_ids.append(product.product_id)
                        logger.warning(f"  ❌ Failed: {product.product_title}")
                except Exception as e:
                    failed += 1
                    failed_ids.append(product.product_id)
                    logger.error(f"  💥 Error updating {product.product_title}: {e}")

                # Progress update every 10 products
                total_processed = successful + failed
                if total_processed % 10 == 0:
                    logger.info(f"  Progress: {total_processed}/{len(products)} processed")

        return {
            "success": successful,
            "failed": failed,
            "failed_ids": failed_ids
        }

    def _update_product_discounts(self, product: ProductDiscounts) -> bool:
        """
        Update discounts metafield for a single product using GraphQL

        Args:
            product: Product with calculated discounts

        Returns:
            True if successful, False otherwise
        """
        # Get discount list as JSON array
        discounts_list = product.get_discounts_list()

        # Convert to JSON string for list of single_line_text_field
        import json
        discounts_json = json.dumps(discounts_list)

        # Use GraphQL to update metafield
        return self.shopify_client.update_product_metafield_graphql(
            product_id=product.product_id,
            namespace=self.METAFIELD_NAMESPACE,
            key=self.METAFIELD_KEY,
            value=discounts_json,
            metafield_type="list.single_line_text_field"
        )

    def _log_summary(self, result: DiscountSyncResult, products_to_update: List[ProductDiscounts]):
        """Log summary of sync results"""
        logger.info("=" * 60)
        logger.info("💰 Product Discount Sync Summary")
        logger.info("=" * 60)
        logger.info(f"Mode: {'🧪 DRY RUN' if result.dry_run else '✅ LIVE'}")
        logger.info(f"Execution time: {result.execution_time_seconds:.2f}s")
        logger.info("")
        logger.info(f"📦 Products processed: {result.total_products_processed}")
        logger.info(f"💰 Products with discounts: {result.products_with_discounts}")
        logger.info(f"📊 Unique discount percentages found: {sorted(result.total_unique_discount_percentages)}")
        logger.info("")
        logger.info(f"🔄 Products needing update: {len(products_to_update)}")

        if not result.dry_run:
            logger.info("")
            logger.info(f"📊 Update results:")
            logger.info(f"  ✅ Successful: {result.products_updated}")
            logger.info(f"  ❌ Failed: {result.products_failed}")

            if result.failed_product_ids:
                logger.warning("")
                logger.warning(f"⚠️  Failed product IDs ({len(result.failed_product_ids)}):")
                for product_id in result.failed_product_ids[:10]:
                    logger.warning(f"  - Product ID: {product_id}")
                if len(result.failed_product_ids) > 10:
                    logger.warning(f"  ... and {len(result.failed_product_ids) - 10} more")

        logger.info("=" * 60)
