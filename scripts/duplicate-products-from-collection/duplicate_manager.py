import logging
import time
import re
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional

# Add script directory to path for direct import (directory has hyphens)
sys.path.insert(0, str(Path(__file__).parent))

from shared.shopify_client import ShopifyClient
from models import (
    ProductInfo,
    VariantInfo,
    DuplicateResult,
    DuplicationJobResult,
)

logger = logging.getLogger(__name__)

# Name replacement mapping (order matters: longer patterns first)
NAME_REPLACEMENTS = [
    ("Fear of God Essentials", "FG"),
    ("Essentials", "FG"),
]


class DuplicateManager:
    """Core business logic for duplicating products from a collection."""

    def __init__(self, shopify_client: ShopifyClient):
        self.shopify_client = shopify_client
        self._locations_cache: Optional[List[Dict[str, Any]]] = None

    def _transform_title(self, title: str) -> str:
        """Apply name replacements to product title (case insensitive)."""
        new_title = title
        for original, replacement in NAME_REPLACEMENTS:
            new_title = re.sub(re.escape(original), replacement, new_title, flags=re.IGNORECASE)
        return new_title

    def _parse_product(self, raw_product: Dict[str, Any]) -> ProductInfo:
        """Parse raw GraphQL product data into ProductInfo model."""
        variants = []
        for edge in raw_product.get("variants", {}).get("edges", []):
            node = edge.get("node", {})
            selected_options = node.get("selectedOptions", [])
            inv_item = node.get("inventoryItem", {})
            variants.append(VariantInfo(
                id=node.get("id", ""),
                title=node.get("title", ""),
                sku=node.get("sku"),
                price=node.get("price", "0.00"),
                compare_at_price=node.get("compareAtPrice"),
                inventory_quantity=node.get("inventoryQuantity", 0),
                inventory_item_id=inv_item.get("id") if inv_item else None,
                selected_options=[{"name": o.get("name", ""), "value": o.get("value", "")} for o in selected_options],
            ))

        images = []
        for edge in raw_product.get("images", {}).get("edges", []):
            node = edge.get("node", {})
            images.append({
                "url": node.get("url"),
                "altText": node.get("altText"),
            })

        seo = raw_product.get("seo", {}) or {}

        return ProductInfo(
            id=raw_product.get("id", ""),
            title=raw_product.get("title", ""),
            handle=raw_product.get("handle", ""),
            vendor=raw_product.get("vendor"),
            product_type=raw_product.get("productType"),
            tags=raw_product.get("tags", []),
            status=raw_product.get("status", "ACTIVE"),
            options=[{"name": o.get("name"), "values": o.get("values", [])} for o in raw_product.get("options", [])],
            variants=variants,
            image_urls=images,
            seo_title=seo.get("title"),
            seo_description=seo.get("description"),
        )

    def _get_locations(self) -> List[Dict[str, Any]]:
        """Get and cache active locations."""
        if self._locations_cache is None:
            self._locations_cache = self.shopify_client.get_locations_graphql()
        return self._locations_cache

    def _get_inventory_levels_for_variant(self, inventory_item_id: str) -> List[Dict[str, Any]]:
        """Get inventory levels per location for a variant's inventory item."""
        if not inventory_item_id:
            return []
        return self.shopify_client.get_inventory_levels_graphql(inventory_item_id)

    def _build_product_create_input(self, source: ProductInfo) -> Dict[str, Any]:
        """Build the ProductCreateInput for the GraphQL productCreate mutation (2025-01 API)."""
        new_title = self._transform_title(source.title)

        # SEO: apply name replacement
        seo_title = self._transform_title(source.seo_title) if source.seo_title else new_title

        # Build product input (ProductCreateInput fields only)
        product_input: Dict[str, Any] = {
            "title": new_title,
            "vendor": source.vendor,
            "productType": source.product_type,
            "tags": source.tags,
            "status": "ACTIVE",
            "descriptionHtml": "",  # Do not copy description
            "seo": {
                "title": seo_title,
                "description": "",  # Empty SEO description
            },
        }

        # productOptions: use OptionCreateInput format
        if source.options:
            product_options = []
            for opt in source.options:
                product_options.append({
                    "name": opt["name"],
                    "values": [{"name": v} for v in opt.get("values", [])],
                })
            product_input["productOptions"] = product_options

        # Media (images) - passed as separate argument by create_product_graphql
        media_inputs = []
        for img in source.image_urls:
            if img.get("url"):
                media_input: Dict[str, Any] = {
                    "originalSource": img["url"],
                    "mediaContentType": "IMAGE",
                }
                if img.get("altText"):
                    media_input["alt"] = img["altText"]
                media_inputs.append(media_input)

        if media_inputs:
            product_input["media"] = media_inputs

        return product_input

    def _build_variant_bulk_inputs(self, source: ProductInfo) -> List[Dict[str, Any]]:
        """Build ProductVariantsBulkInput list for productVariantsBulkCreate."""
        variant_inputs = []
        for variant in source.variants:
            variant_input: Dict[str, Any] = {
                "price": variant.price,
            }
            if variant.compare_at_price:
                variant_input["compareAtPrice"] = variant.compare_at_price

            # SKU goes inside inventoryItem (not a top-level field)
            if variant.sku:
                variant_input["inventoryItem"] = {"sku": variant.sku}

            # Use optionValues format for 2025-01 API
            if variant.selected_options:
                variant_input["optionValues"] = [
                    {"name": o["value"], "optionName": o["name"]}
                    for o in variant.selected_options
                ]

            variant_inputs.append(variant_input)

        return variant_inputs

    def _set_inventory_for_variants(
        self, source_variants: List[VariantInfo], new_variants: List[Dict[str, Any]]
    ) -> None:
        """Copy inventory levels from source variants to newly created variants."""
        if len(new_variants) != len(source_variants):
            logger.warning(
                f"Variant count mismatch: source={len(source_variants)}, new={len(new_variants)}. "
                "Inventory copy may be incomplete."
            )

        for i, (src_variant, new_variant) in enumerate(zip(source_variants, new_variants)):
            new_inv_item = new_variant.get("inventoryItem", {})
            new_inv_item_id = new_inv_item.get("id") if new_inv_item else None

            if not new_inv_item_id:
                logger.warning(f"No inventory item ID for new variant {i}, skipping inventory set")
                continue

            if not src_variant.inventory_item_id:
                logger.warning(f"No source inventory item ID for variant {i}, skipping")
                continue

            # Get inventory levels from source variant
            source_levels = self._get_inventory_levels_for_variant(src_variant.inventory_item_id)
            time.sleep(0.2)

            for level in source_levels:
                location = level.get("location", {})
                location_id = location.get("id")
                quantities = level.get("quantities", [])
                available_qty = 0
                for q in quantities:
                    if q.get("name") == "available":
                        available_qty = q.get("quantity", 0)
                        break

                if location_id and available_qty > 0:
                    # First, activate inventory tracking at this location
                    activated = self.shopify_client.inventory_activate_graphql(
                        inventory_item_id=new_inv_item_id,
                        location_id=location_id,
                    )
                    if not activated:
                        logger.warning(
                            f"Failed to activate inventory for variant {new_variant.get('title', i)} "
                            f"at {location.get('name', location_id)}, skipping quantity set"
                        )
                        continue
                    time.sleep(0.2)

                    # Then set the quantity
                    success = self.shopify_client.set_inventory_quantity_graphql(
                        inventory_item_id=new_inv_item_id,
                        location_id=location_id,
                        quantity=available_qty,
                    )
                    if success:
                        logger.debug(
                            f"Set inventory for variant {new_variant.get('title', i)}: "
                            f"{available_qty} at {location.get('name', location_id)}"
                        )
                    else:
                        logger.warning(
                            f"Failed to set inventory for variant {new_variant.get('title', i)} "
                            f"at {location.get('name', location_id)}"
                        )
                    time.sleep(0.2)

    def duplicate_product(self, source: ProductInfo, dry_run: bool = False) -> DuplicateResult:
        """Duplicate a single product using the 2025-01 API two-step flow:
        1. productCreate - creates product with options and media
        2. productVariantsBulkCreate - creates all variants with SKU, price, options
        3. Set inventory levels for each variant
        """
        new_title = self._transform_title(source.title)

        if dry_run:
            logger.info(f"[DRY RUN] Would duplicate: '{source.title}' -> '{new_title}'")
            logger.info(f"  Variants: {len(source.variants)}, Images: {len(source.image_urls)}")
            return DuplicateResult(
                source_product_id=source.id,
                source_title=source.title,
                new_title=new_title,
                success=True,
            )

        try:
            # Step 1: Create product (with options + media, no variants)
            product_input = self._build_product_create_input(source)
            logger.info(f"Step 1: Creating product '{new_title}' (from '{source.title}')")
            new_product = self.shopify_client.create_product_graphql(product_input)

            if not new_product:
                return DuplicateResult(
                    source_product_id=source.id,
                    source_title=source.title,
                    new_title=new_title,
                    success=False,
                    error="productCreate returned None",
                )

            new_product_id = new_product.get("id", "")
            logger.info(f"Product created: {new_product_id}")
            time.sleep(0.5)

            # Step 2: Create variants via productVariantsBulkCreate
            variant_inputs = self._build_variant_bulk_inputs(source)
            if variant_inputs:
                logger.info(f"Step 2: Creating {len(variant_inputs)} variants...")
                created_variants = self.shopify_client.create_product_variants_bulk_graphql(
                    product_id=new_product_id,
                    variants=variant_inputs,
                )

                if not created_variants:
                    logger.warning(f"Failed to create variants for '{new_title}'")
                    return DuplicateResult(
                        source_product_id=source.id,
                        source_title=source.title,
                        new_product_id=new_product_id,
                        new_title=new_title,
                        success=False,
                        error="productVariantsBulkCreate returned None",
                    )

                logger.info(f"Created {len(created_variants)} variants")
                time.sleep(0.5)

                # Step 3: Set inventory levels
                logger.info(f"Step 3: Setting inventory for '{new_title}'...")
                self._set_inventory_for_variants(source.variants, created_variants)
            else:
                logger.info("No variants to create")

            # Step 4: Publish to Online Store (non-blocking if permission denied)
            logger.info(f"Step 4: Publishing product to Online Store...")
            published = self.shopify_client.publish_product_graphql(new_product_id)
            if not published:
                logger.warning(
                    f"Could not publish '{new_title}' to Online Store "
                    "(may need write_publications scope). Product was still created successfully."
                )
            time.sleep(0.3)

            return DuplicateResult(
                source_product_id=source.id,
                source_title=source.title,
                new_product_id=new_product_id,
                new_title=new_title,
                success=True,
            )

        except Exception as e:
            logger.error(f"Failed to duplicate product '{source.title}': {e}")
            return DuplicateResult(
                source_product_id=source.id,
                source_title=source.title,
                new_title=new_title,
                success=False,
                error=str(e),
            )

    def duplicate_collection_products(
        self, collection_id: str, dry_run: bool = False
    ) -> DuplicationJobResult:
        """
        Fetch all in-stock products from a collection and duplicate them.
        """
        start_time = time.time()
        job_result = DuplicationJobResult(collection_id=collection_id)

        # Step 1: Fetch products from collection (only those with stock > 0)
        logger.info(f"Fetching products from collection {collection_id}...")
        raw_products = self.shopify_client.get_collection_products_graphql(collection_id)

        job_result.total_source_products = len(raw_products)
        job_result.products_with_stock = len(raw_products)

        if not raw_products:
            logger.warning("No in-stock products found in collection")
            job_result.execution_time_seconds = time.time() - start_time
            return job_result

        logger.info(f"Found {len(raw_products)} in-stock products to duplicate")

        # Step 2: Parse and duplicate each product
        for i, raw_product in enumerate(raw_products, 1):
            product = self._parse_product(raw_product)
            logger.info(f"[{i}/{len(raw_products)}] Processing: {product.title}")

            result = self.duplicate_product(product, dry_run=dry_run)
            job_result.results.append(result)

            if result.success:
                job_result.products_duplicated += 1
            else:
                job_result.products_failed += 1
                if result.error:
                    job_result.errors.append(f"{product.title}: {result.error}")

            # Delay between products to respect rate limits
            if not dry_run:
                time.sleep(1.0)

        job_result.success = job_result.products_failed == 0
        job_result.execution_time_seconds = time.time() - start_time

        logger.info(
            f"Duplication complete: {job_result.products_duplicated} succeeded, "
            f"{job_result.products_failed} failed, "
            f"{job_result.execution_time_seconds:.1f}s total"
        )

        return job_result
