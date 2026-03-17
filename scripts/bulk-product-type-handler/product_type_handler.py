import csv
import os
import time
import logging
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed

from models import ProductInfo, TypeReplaceRecord

logger = logging.getLogger(__name__)

PRODUCTS_QUERY = """
query getProducts($cursor: String, $query: String) {
  products(first: 50, after: $cursor, query: $query) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      cursor
      node {
        id
        legacyResourceId
        title
        productType
        status
      }
    }
  }
}
"""

UPDATE_MUTATION = """
mutation updateProductType($input: ProductInput!) {
  productUpdate(input: $input) {
    product {
      id
      productType
    }
    userErrors {
      field
      message
    }
  }
}
"""


class ProductTypeHandler:
    MAX_CONCURRENT_UPDATES = 5

    def __init__(self, shopify_client):
        self.shopify_client = shopify_client

    def _fetch_products(self, query_filter: str) -> List[ProductInfo]:
        products = []
        cursor = None
        has_next_page = True
        page_count = 0

        while has_next_page:
            page_count += 1
            variables = {"query": query_filter}
            if cursor:
                variables["cursor"] = cursor

            result = self.shopify_client.execute_graphql(PRODUCTS_QUERY, variables)

            if not result or "data" not in result:
                logger.error("Failed to fetch products")
                break

            if "errors" in result:
                error_messages = [err.get("message", "Unknown error") for err in result["errors"]]
                logger.error(f"GraphQL errors: {', '.join(error_messages)}")
                break

            edges = result["data"]["products"]["edges"]
            page_info = result["data"]["products"]["pageInfo"]

            for edge in edges:
                node = edge["node"]
                products.append(ProductInfo(
                    product_id=str(node.get("legacyResourceId", "")),
                    gid=node.get("id", ""),
                    title=node.get("title", ""),
                    product_type=node.get("productType", "") or "",
                    status=node.get("status", ""),
                ))

            has_next_page = page_info.get("hasNextPage", False)
            if has_next_page and edges:
                cursor = edges[-1]["cursor"]

            if page_count % 5 == 0:
                logger.info(f"Fetched {len(products)} products so far (page {page_count})...")

            time.sleep(0.1)

        logger.info(f"Total products fetched: {len(products)}")
        return products

    def get_empty_type_products(self) -> List[ProductInfo]:
        logger.info("Fetching products with empty product type...")
        return self._fetch_products("product_type:''")

    def get_products_by_type(self, product_type: str) -> List[ProductInfo]:
        logger.info(f"Fetching products with type '{product_type}'...")
        return self._fetch_products(f"product_type:'{product_type}'")

    def _update_single_product_type(self, gid: str, new_type: str) -> tuple:
        variables = {
            "input": {
                "id": gid,
                "productType": new_type,
            }
        }

        try:
            response = self.shopify_client.execute_graphql(UPDATE_MUTATION, variables)

            if "errors" in response:
                error_messages = [err.get("message", "Unknown error") for err in response["errors"]]
                return False, "; ".join(error_messages)

            data = response.get("data", {})
            product_update = data.get("productUpdate", {})
            user_errors = product_update.get("userErrors", [])

            if user_errors:
                error_messages = [f"{err['field']}: {err['message']}" for err in user_errors]
                return False, "; ".join(error_messages)

            if product_update.get("product"):
                return True, None

            return False, "No product returned"

        except Exception as e:
            return False, str(e)

    def replace_product_type(self, old_type: str, new_type: str, dry_run: bool = False) -> List[TypeReplaceRecord]:
        products = self.get_products_by_type(old_type)

        if not products:
            logger.info(f"No products found with type '{old_type}'")
            return []

        logger.info(f"Found {len(products)} products with type '{old_type}'")

        if dry_run:
            logger.info("Dry run mode - no changes will be made")
            return [
                TypeReplaceRecord(
                    product_id=p.product_id,
                    title=p.title,
                    old_type=old_type,
                    new_type=new_type,
                    success=True,
                )
                for p in products
            ]

        records = []
        updated = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=self.MAX_CONCURRENT_UPDATES) as executor:
            future_to_product = {
                executor.submit(self._update_single_product_type, p.gid, new_type): p
                for p in products
            }

            for future in as_completed(future_to_product):
                product = future_to_product[future]
                success, error = future.result()

                records.append(TypeReplaceRecord(
                    product_id=product.product_id,
                    title=product.title,
                    old_type=old_type,
                    new_type=new_type,
                    success=success,
                    error=error,
                ))

                if success:
                    updated += 1
                    logger.info(f"[{updated + failed}/{len(products)}] Updated: {product.title}")
                else:
                    failed += 1
                    logger.error(f"[{updated + failed}/{len(products)}] Failed: {product.title} - {error}")

                time.sleep(0.1)

        logger.info(f"Replace complete: {updated} updated, {failed} failed out of {len(products)}")
        return records

    def write_products_csv(self, products: List[ProductInfo], filepath: str):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["product_id", "title", "product_type", "status"])
            for p in products:
                writer.writerow([p.product_id, p.title, p.product_type, p.status])
        logger.info(f"CSV written: {filepath} ({len(products)} rows)")

    def write_replace_csv(self, records: List[TypeReplaceRecord], filepath: str):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["product_id", "title", "old_type", "new_type", "success", "error"])
            for r in records:
                writer.writerow([r.product_id, r.title, r.old_type, r.new_type, r.success, r.error or ""])
        logger.info(f"CSV written: {filepath} ({len(records)} rows)")
