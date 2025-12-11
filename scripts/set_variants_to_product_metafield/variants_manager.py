"""
Manager for syncing product variants to metafields.
"""
import logging
import re
from typing import List, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from .models import ProductVariantsInfo, VariantInfo, SyncResult

logger = logging.getLogger(__name__)


class VariantsMetafieldManager:
    """Manages synchronization of product variants to custom.variants metafield."""

    METAFIELD_NAMESPACE = "custom"
    METAFIELD_KEY = "variants"
    METAFIELD_TYPE = "list.single_line_text_field"
    MAX_CONCURRENT_UPDATES = 5

    def __init__(self, shopify_client):
        """
        Initialize the variants metafield manager.

        Args:
            shopify_client: ShopifyClient instance
        """
        self.shopify_client = shopify_client

    @staticmethod
    def parse_variant_title(variant_title: str) -> str:
        """
        Parse and normalize variant title.

        Rules:
        1. Split by " - " and keep only the first part (index 0)
        2. Normalize decimal sizes:
           - .3 rounds down to .0 (e.g., 37.3 EU -> 37 EU)
           - .6 rounds to .5 (e.g., 37.6 EU -> 37.5 EU)
           - .5 stays as .5 (e.g., 35.5 EU -> 35.5 EU)
           - .0 stays as integer (e.g., 36 EU -> 36 EU)

        Args:
            variant_title: Original variant title

        Returns:
            Normalized variant title

        Examples:
            "35.5 EU - Color" -> "35.5 EU"
            "36 EU - Red" -> "36 EU"
            "37.3 EU - Blue" -> "37 EU"
            "37.6 EU - Green" -> "37.5 EU"
        """
        # Step 1: Split by " - " and keep first part
        parts = variant_title.split(" - ")
        size_part = parts[0].strip()

        # Step 2: Check if it contains a decimal number followed by "EU"
        # Pattern: number with optional decimal, followed by space and "EU"
        match = re.match(r'^(\d+)\.(\d+)\s+(EU.*)$', size_part)

        if match:
            integer_part = match.group(1)
            decimal_part = match.group(2)
            unit_part = match.group(3)  # "EU" or "EU W" etc.

            # Normalize decimal part
            decimal_value = int(decimal_part)

            if decimal_value <= 3:
                # .0, .1, .2, .3 -> round down to .0 (remove decimal)
                normalized = f"{integer_part} {unit_part}"
            elif decimal_value <= 5:
                # .4, .5 -> keep as .5
                normalized = f"{integer_part}.5 {unit_part}"
            else:
                # .6, .7, .8, .9 -> round to .5
                normalized = f"{integer_part}.5 {unit_part}"

            return normalized

        # If no decimal pattern matched, return as-is
        return size_part

    def fetch_all_products_with_variants(self) -> List[ProductVariantsInfo]:
        """
        Fetch all products with their variants and current metafield values.

        Returns:
            List of ProductVariantsInfo objects
        """
        logger.info("📦 Fetching all products with variants from Shopify...")

        query = """
        query GetProductsWithVariants($cursor: String) {
          products(first: 50, after: $cursor) {
            edges {
              cursor
              node {
                id
                title
                variants(first: 100) {
                  edges {
                    node {
                      id
                      title
                    }
                  }
                }
                metafield(namespace: "custom", key: "variants") {
                  id
                  value
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
                product_id = node["id"]
                product_title = node["title"]

                # Extract variant information
                variants = []
                for variant_edge in node.get("variants", {}).get("edges", []):
                    variant_node = variant_edge["node"]
                    variants.append(VariantInfo(
                        id=variant_node["id"],
                        title=variant_node["title"]
                    ))

                # Extract current metafield value
                current_value = None
                if node.get("metafield"):
                    try:
                        # Metafield value is returned as JSON string
                        import json
                        current_value = json.loads(node["metafield"]["value"])
                    except (json.JSONDecodeError, KeyError):
                        current_value = None

                products.append(ProductVariantsInfo(
                    id=product_id,
                    title=product_title,
                    variants=variants,
                    current_metafield_value=current_value
                ))

            # Update pagination
            page_info = result["data"]["products"]["pageInfo"]
            has_next_page = page_info.get("hasNextPage", False)

            if has_next_page and edges:
                cursor = edges[-1]["cursor"]

            logger.info(f"Fetched {len(products)} products so far...")

        logger.info(f"✅ Fetched {len(products)} products total")
        return products

    def should_update_product(self, product: ProductVariantsInfo, update_all: bool) -> tuple[bool, Optional[str]]:
        """
        Determine if a product should be updated.

        Args:
            product: ProductVariantsInfo object
            update_all: If True, update all products; if False, only update empty ones

        Returns:
            Tuple of (should_update, skip_reason)
        """
        # If update_all flag is set, always update
        if update_all:
            return True, None

        # Otherwise, only update if metafield is empty
        if product.current_metafield_value is None or len(product.current_metafield_value) == 0:
            return True, None

        return False, "Metafield already has value (use --all to force update)"

    def update_product_variants_metafield(self, product: ProductVariantsInfo) -> SyncResult:
        """
        Update a single product's variants metafield.

        Args:
            product: ProductVariantsInfo object

        Returns:
            SyncResult object
        """
        try:
            # Extract and parse variant names
            parsed_variant_names = []
            for variant in product.variants:
                parsed_name = self.parse_variant_title(variant.title)
                parsed_variant_names.append(parsed_name)

            # Remove duplicates while preserving order
            # Use dict.fromkeys() to maintain insertion order (Python 3.7+)
            unique_variant_names = list(dict.fromkeys(parsed_variant_names))

            logger.debug(
                f"Product '{product.title}': "
                f"Original variants: {len(product.variants)}, "
                f"Unique variants after parsing: {len(unique_variant_names)}"
            )

            # Prepare metafield mutation
            mutation = """
            mutation SetProductVariantsMetafield($input: ProductInput!) {
              productUpdate(input: $input) {
                product {
                  id
                  metafield(namespace: "custom", key: "variants") {
                    id
                    value
                  }
                }
                userErrors {
                  field
                  message
                }
              }
            }
            """

            # Prepare input with metafield value as JSON array
            import json
            variables = {
                "input": {
                    "id": product.id,
                    "metafields": [
                        {
                            "namespace": self.METAFIELD_NAMESPACE,
                            "key": self.METAFIELD_KEY,
                            "type": self.METAFIELD_TYPE,
                            "value": json.dumps(unique_variant_names)
                        }
                    ]
                }
            }

            result = self.shopify_client.execute_graphql(mutation, variables)

            # Check for errors
            if not result or "data" not in result:
                return SyncResult(
                    product_id=product.id,
                    product_title=product.title,
                    success=False,
                    variant_count=len(unique_variant_names),
                    variant_names=unique_variant_names,
                    error="GraphQL query failed"
                )

            user_errors = result["data"]["productUpdate"].get("userErrors", [])
            if user_errors:
                error_messages = [f"{err['field']}: {err['message']}" for err in user_errors]
                return SyncResult(
                    product_id=product.id,
                    product_title=product.title,
                    success=False,
                    variant_count=len(unique_variant_names),
                    variant_names=unique_variant_names,
                    error=", ".join(error_messages)
                )

            return SyncResult(
                product_id=product.id,
                product_title=product.title,
                success=True,
                variant_count=len(unique_variant_names),
                variant_names=unique_variant_names
            )

        except Exception as e:
            logger.error(f"Error updating product {product.title}: {str(e)}")
            return SyncResult(
                product_id=product.id,
                product_title=product.title,
                success=False,
                variant_count=len(product.variants),
                variant_names=[v.title for v in product.variants],
                error=str(e)
            )

    def sync_all_products(
        self,
        update_all: bool = False,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Sync variants to metafields for all products.

        Args:
            update_all: If True, update all products; if False, only update empty ones
            dry_run: If True, only analyze without making changes

        Returns:
            Dictionary with sync statistics and results
        """
        # Fetch all products
        products = self.fetch_all_products_with_variants()

        if not products:
            logger.warning("No products found")
            return {
                "total_products": 0,
                "updated": 0,
                "skipped": 0,
                "failed": 0,
                "results": []
            }

        logger.info(f"📊 Processing {len(products)} products...")
        logger.info(f"Mode: {'UPDATE ALL' if update_all else 'UPDATE EMPTY ONLY'}")
        logger.info(f"Dry run: {dry_run}")

        results = []
        to_update = []
        skipped_count = 0

        # Filter products to update
        for product in products:
            should_update, skip_reason = self.should_update_product(product, update_all)

            if should_update:
                to_update.append(product)
            else:
                skipped_count += 1
                variant_names = [v.title for v in product.variants]
                results.append(SyncResult(
                    product_id=product.id,
                    product_title=product.title,
                    success=True,
                    variant_count=len(variant_names),
                    variant_names=variant_names,
                    skipped=True,
                    reason=skip_reason
                ))

        logger.info(f"📝 Products to update: {len(to_update)}")
        logger.info(f"⏭️  Products to skip: {skipped_count}")

        if dry_run:
            logger.info("🔍 DRY RUN - No changes will be made")
            for product in to_update:
                variant_names = [v.title for v in product.variants]
                logger.info(f"  Would update: {product.title} ({len(variant_names)} variants)")

            return {
                "total_products": len(products),
                "to_update": len(to_update),
                "skipped": skipped_count,
                "dry_run": True,
                "results": results
            }

        # Update products concurrently
        if to_update:
            logger.info(f"🔄 Updating {len(to_update)} products...")

            updated_count = 0
            failed_count = 0

            with ThreadPoolExecutor(max_workers=self.MAX_CONCURRENT_UPDATES) as executor:
                future_to_product = {
                    executor.submit(self.update_product_variants_metafield, product): product
                    for product in to_update
                }

                for future in as_completed(future_to_product):
                    result = future.result()
                    results.append(result)

                    if result.success:
                        updated_count += 1
                        logger.info(
                            f"✅ [{updated_count}/{len(to_update)}] Updated: {result.product_title} "
                            f"({result.variant_count} variants)"
                        )
                    else:
                        failed_count += 1
                        logger.error(
                            f"❌ [{updated_count + failed_count}/{len(to_update)}] Failed: "
                            f"{result.product_title} - {result.error}"
                        )
        else:
            updated_count = 0
            failed_count = 0

        # Summary
        logger.info("\n" + "="*60)
        logger.info("📊 SYNC SUMMARY")
        logger.info("="*60)
        logger.info(f"Total products: {len(products)}")
        logger.info(f"✅ Updated: {updated_count}")
        logger.info(f"⏭️  Skipped: {skipped_count}")
        logger.info(f"❌ Failed: {failed_count}")
        logger.info("="*60)

        return {
            "total_products": len(products),
            "updated": updated_count,
            "skipped": skipped_count,
            "failed": failed_count,
            "results": results
        }
