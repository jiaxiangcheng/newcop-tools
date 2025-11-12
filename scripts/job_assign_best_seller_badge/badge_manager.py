"""
Best Seller Badge Manager

Manages the best seller badge metafield for Shopify products based on Airtable sales data.
"""
import logging
import time
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from scripts.job_assign_best_seller_badge.models import (
    AirtableProduct,
    BestSellerUpdate,
    BadgeSyncResult
)

logger = logging.getLogger(__name__)


class BadgeManager:
    """Manages best seller badge synchronization"""

    METAFIELD_NAMESPACE = "custom"
    METAFIELD_KEY = "best_seller"
    MAX_CONCURRENT_UPDATES = 5  # Limit concurrent API calls

    def __init__(self, shopify_client, airtable_client):
        """
        Initialize badge manager

        Args:
            shopify_client: Shopify API client instance
            airtable_client: Airtable API client instance
        """
        self.shopify_client = shopify_client
        self.airtable_client = airtable_client

    def sync_best_seller_badges(
        self,
        airtable_base_id: str,
        airtable_table_id: str,
        airtable_view_id: str,
        top_n_products: int = 50,
        dry_run: bool = False
    ) -> BadgeSyncResult:
        """
        Synchronize best seller badges based on Airtable sales data

        Args:
            airtable_base_id: Airtable base ID
            airtable_table_id: Airtable table ID
            airtable_view_id: Airtable view ID (should be sorted by sales)
            top_n_products: Number of top products to mark as best sellers (default: 50)
            dry_run: If True, only analyze without making changes

        Returns:
            BadgeSyncResult with sync statistics
        """
        start_time = time.time()
        logger.info(f"🏅 Starting Best Seller Badge sync (dry_run={dry_run})")
        logger.info(f"📊 Target: Top {top_n_products} products from Airtable view")

        # Step 1: Fetch top products from Airtable
        logger.info("📥 Fetching top products from Airtable...")
        airtable_products = self._fetch_top_products(
            airtable_base_id,
            airtable_table_id,
            airtable_view_id,
            top_n_products
        )

        # Step 2: Extract valid Shopify IDs
        valid_products, invalid_products = self._extract_valid_products(airtable_products)

        logger.info(f"✅ Found {len(valid_products)} products with valid Shopify IDs")
        if invalid_products:
            logger.warning(f"⚠️  {len(invalid_products)} products without valid Shopify IDs:")
            for product_name in invalid_products[:10]:  # Show first 10
                logger.warning(f"  - {product_name}")
            if len(invalid_products) > 10:
                logger.warning(f"  ... and {len(invalid_products) - 10} more")

        # Step 3: Get current badge status for all products
        logger.info("🔍 Checking current best seller badge status...")
        current_badge_status = self._get_current_badge_status() if not dry_run else {}

        # Step 4: Determine which products need updates
        updates_to_add = []  # Products that need badge added
        updates_to_remove = []  # Products that need badge removed

        # Products that should have badge
        target_product_ids = set(valid_products)

        # Products that currently have badge
        products_with_badge = {
            product_id for product_id, has_badge in current_badge_status.items()
            if has_badge
        }

        # Find products that need badge added
        for product_id in target_product_ids:
            if product_id not in products_with_badge:
                # Get product name from airtable_products
                product_name = next(
                    (p.product_name for p in airtable_products if p.shopify_id == product_id),
                    f"Product {product_id}"
                )
                updates_to_add.append(
                    BestSellerUpdate(
                        product_id=product_id,
                        product_name=product_name,
                        current_badge_status=False,
                        target_badge_status=True
                    )
                )

        # Find products that need badge removed
        for product_id in products_with_badge:
            if product_id not in target_product_ids:
                # Fetch product name from Shopify (since it's not in our top list)
                product_name = self._get_product_name(product_id)
                updates_to_remove.append(
                    BestSellerUpdate(
                        product_id=product_id,
                        product_name=product_name,
                        current_badge_status=True,
                        target_badge_status=False
                    )
                )

        logger.info(f"🔄 Updates needed:")
        logger.info(f"  ➕ Add badge: {len(updates_to_add)} products")
        logger.info(f"  ➖ Remove badge: {len(updates_to_remove)} products")

        # Step 5: Execute updates (if not dry run)
        successful_updates = 0
        failed_updates = 0
        failed_products = []

        if not dry_run:
            logger.info("🚀 Executing badge updates...")

            # Remove badges first
            if updates_to_remove:
                logger.info(f"➖ Removing badges from {len(updates_to_remove)} products...")
                remove_results = self._execute_updates(updates_to_remove, set_value=False)
                successful_updates += remove_results["success"]
                failed_updates += remove_results["failed"]
                failed_products.extend(remove_results["failed_products"])

            # Then add badges
            if updates_to_add:
                logger.info(f"➕ Adding badges to {len(updates_to_add)} products...")
                add_results = self._execute_updates(updates_to_add, set_value=True)
                successful_updates += add_results["success"]
                failed_updates += add_results["failed"]
                failed_products.extend(add_results["failed_products"])

            logger.info(f"✅ Updates completed: {successful_updates} successful, {failed_updates} failed")
        else:
            logger.info("🧪 Dry run mode - no changes made")

        execution_time = time.time() - start_time

        # Build result
        result = BadgeSyncResult(
            total_products_in_airtable=len(airtable_products),
            valid_products_count=len(valid_products),
            invalid_products_count=len(invalid_products),
            invalid_products=invalid_products,
            badges_removed_count=len(updates_to_remove),
            badges_added_count=len(updates_to_add),
            successful_updates=successful_updates,
            failed_updates=failed_updates,
            failed_products=failed_products,
            execution_time_seconds=execution_time,
            dry_run=dry_run
        )

        self._log_summary(result)
        return result

    def _fetch_top_products(
        self,
        base_id: str,
        table_id: str,
        view_id: str,
        max_records: int
    ) -> List[AirtableProduct]:
        """Fetch top N products from Airtable view"""
        # Update airtable client base ID
        self.airtable_client.base_id = base_id
        self.airtable_client.base_url = f"https://api.airtable.com/v0/{base_id}"

        records = self.airtable_client.get_records(
            table_id=table_id,
            view_id=view_id,
            max_records=max_records
        )

        products = []
        for record in records:
            fields = record.get("fields", {})

            # Extract Shopify ID
            shopify_id_value = fields.get("∞ Shopify Id")
            shopify_id = None
            if shopify_id_value:
                try:
                    if isinstance(shopify_id_value, list):
                        shopify_id = int(shopify_id_value[0]) if shopify_id_value else None
                    else:
                        shopify_id = int(shopify_id_value)
                except (ValueError, TypeError):
                    pass

            product = AirtableProduct(
                record_id=record.get("id"),
                product_name=fields.get("Product Title", "Unknown"),
                shopify_id=shopify_id,
                quarterly_sales=fields.get("Ventas trimestre"),
                total_sales=fields.get("Total sale")
            )
            products.append(product)

        logger.info(f"📦 Fetched {len(products)} products from Airtable")
        return products

    def _extract_valid_products(
        self,
        airtable_products: List[AirtableProduct]
    ) -> tuple[List[int], List[str]]:
        """
        Extract valid Shopify product IDs

        Returns:
            Tuple of (valid_product_ids, invalid_product_names)
        """
        valid_ids = []
        invalid_names = []

        for product in airtable_products:
            if product.shopify_id and product.shopify_id > 0:
                valid_ids.append(product.shopify_id)
            else:
                invalid_names.append(product.product_name)

        return valid_ids, invalid_names

    def _get_current_badge_status(self) -> Dict[int, bool]:
        """
        Get current best seller badge status for all products

        Returns:
            Dictionary mapping product_id -> has_badge (bool)
        """
        logger.info("🔍 Fetching all products with current badge status...")

        # Use GraphQL to efficiently query all products with best_seller metafield
        products_with_badge = self._query_products_with_badge()

        logger.info(f"📊 Found {len(products_with_badge)} products with best_seller badge")
        return products_with_badge

    def _query_products_with_badge(self) -> Dict[int, bool]:
        """
        Query all products that have the best_seller metafield using GraphQL

        Returns:
            Dictionary mapping product_id -> has_badge (bool)
        """
        query = """
        query getProductsWithBadge($cursor: String) {
          products(first: 250, after: $cursor) {
            pageInfo {
              hasNextPage
              endCursor
            }
            edges {
              node {
                id
                legacyResourceId
                metafield(namespace: "custom", key: "best_seller") {
                  id
                  value
                  type
                }
              }
            }
          }
        }
        """

        products_with_badge = {}
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
                product_id = int(node.get("legacyResourceId", 0))
                metafield = node.get("metafield")

                if metafield:
                    # Product has the metafield
                    value = metafield.get("value", "false")
                    has_badge = value.lower() == "true" if isinstance(value, str) else bool(value)
                    products_with_badge[product_id] = has_badge

            # Check for next page
            page_info = products_data.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")

            logger.debug(f"Fetched page {page_count}, found {len(edges)} products")

        logger.info(f"✅ Queried {page_count} pages, found {len(products_with_badge)} products with badge metafield")
        return products_with_badge

    def _get_product_name(self, product_id: int) -> str:
        """Get product name from Shopify (for products not in top list)"""
        query = """
        query getProduct($id: ID!) {
          product(id: $id) {
            title
          }
        }
        """

        gid = f"gid://shopify/Product/{product_id}"
        variables = {"id": gid}

        response = self.shopify_client.execute_graphql(query, variables)

        if "errors" not in response:
            product = response.get("data", {}).get("product")
            if product:
                return product.get("title", f"Product {product_id}")

        return f"Product {product_id}"

    def _execute_updates(
        self,
        updates: List[BestSellerUpdate],
        set_value: bool
    ) -> Dict[str, Any]:
        """
        Execute badge updates concurrently

        Args:
            updates: List of updates to execute
            set_value: True to add badge, False to remove badge

        Returns:
            Dictionary with success/failure counts
        """
        successful = 0
        failed = 0
        failed_products = []

        # Process updates with controlled concurrency
        with ThreadPoolExecutor(max_workers=self.MAX_CONCURRENT_UPDATES) as executor:
            future_to_update = {
                executor.submit(
                    self._update_product_badge,
                    update.product_id,
                    set_value
                ): update
                for update in updates
            }

            for future in as_completed(future_to_update):
                update = future_to_update[future]
                try:
                    success = future.result()
                    if success:
                        successful += 1
                        action = "Added" if set_value else "Removed"
                        logger.debug(f"  ✅ {action} badge: {update.product_name}")
                    else:
                        failed += 1
                        failed_products.append(update.product_name)
                        logger.warning(f"  ❌ Failed: {update.product_name}")
                except Exception as e:
                    failed += 1
                    failed_products.append(update.product_name)
                    logger.error(f"  💥 Error updating {update.product_name}: {e}")

                # Progress update every 10 products
                total_processed = successful + failed
                if total_processed % 10 == 0:
                    logger.info(f"  Progress: {total_processed}/{len(updates)} processed")

        return {
            "success": successful,
            "failed": failed,
            "failed_products": failed_products
        }

    def _update_product_badge(self, product_id: int, set_value: bool) -> bool:
        """
        Update best_seller metafield for a single product using GraphQL

        Args:
            product_id: Shopify product ID
            set_value: True to set badge, False to remove badge

        Returns:
            True if successful, False otherwise
        """
        return self.shopify_client.update_product_metafield_graphql(
            product_id=product_id,
            namespace=self.METAFIELD_NAMESPACE,
            key=self.METAFIELD_KEY,
            value=str(set_value).lower(),  # "true" or "false"
            metafield_type="boolean"
        )

    def _log_summary(self, result: BadgeSyncResult):
        """Log summary of sync results"""
        logger.info("=" * 60)
        logger.info("📊 Best Seller Badge Sync Summary")
        logger.info("=" * 60)
        logger.info(f"Mode: {'🧪 DRY RUN' if result.dry_run else '✅ LIVE'}")
        logger.info(f"Execution time: {result.execution_time_seconds:.2f}s")
        logger.info("")
        logger.info(f"📥 Airtable products fetched: {result.total_products_in_airtable}")
        logger.info(f"  ✅ Valid Shopify IDs: {result.valid_products_count}")
        logger.info(f"  ❌ Invalid Shopify IDs: {result.invalid_products_count}")
        logger.info("")
        logger.info(f"🔄 Badge updates:")
        logger.info(f"  ➕ Badges to add: {result.badges_added_count}")
        logger.info(f"  ➖ Badges to remove: {result.badges_removed_count}")

        if not result.dry_run:
            logger.info("")
            logger.info(f"📊 Update results:")
            logger.info(f"  ✅ Successful: {result.successful_updates}")
            logger.info(f"  ❌ Failed: {result.failed_updates}")

            if result.failed_products:
                logger.warning("")
                logger.warning(f"⚠️  Failed products ({len(result.failed_products)}):")
                for product_name in result.failed_products[:5]:
                    logger.warning(f"  - {product_name}")
                if len(result.failed_products) > 5:
                    logger.warning(f"  ... and {len(result.failed_products) - 5} more")

        logger.info("=" * 60)
