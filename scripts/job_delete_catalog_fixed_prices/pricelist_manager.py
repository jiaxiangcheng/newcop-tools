"""
Business logic for resolving a Catalog's PriceList and deleting all of its
fixed prices via the Shopify Admin GraphQL API (2025-01).
"""
from typing import List, Optional, Tuple

from shared.shopify_client import ShopifyClient
from scripts.job_delete_catalog_fixed_prices.models import (
    CatalogPriceListInfo,
    DeleteBatchResult,
    DeleteSummary,
    FixedPriceEntry,
)


CATALOG_PRICE_LIST_QUERY = """
query getCatalogPriceList($catalogId: ID!) {
  catalog(id: $catalogId) {
    id
    priceList {
      id
      currency
    }
  }
}
"""

FIXED_PRICES_QUERY = """
query getFixedPrices($priceListId: ID!, $cursor: String) {
  priceList(id: $priceListId) {
    id
    prices(first: 250, after: $cursor, originType: FIXED) {
      pageInfo {
        hasNextPage
        endCursor
      }
      edges {
        node {
          variant { id }
          price { amount currencyCode }
          originType
        }
      }
    }
  }
}
"""

DELETE_FIXED_PRICES_MUTATION = """
mutation deleteFixedPrices($priceListId: ID!, $variantIds: [ID!]!) {
  priceListFixedPricesDelete(priceListId: $priceListId, variantIds: $variantIds) {
    deletedFixedPriceVariantIds
    userErrors {
      field
      code
      message
    }
  }
}
"""


class PriceListFixedPricesManager:
    """Resolve Catalog -> PriceList and clear all fixed prices in batches."""

    DELETE_BATCH_SIZE = 250

    def __init__(self, shopify_client: ShopifyClient, logger):
        self.shopify_client = shopify_client
        self.logger = logger

    def _to_catalog_gid(self, catalog_id: str) -> str:
        catalog_id = catalog_id.strip()
        if catalog_id.startswith("gid://"):
            return catalog_id
        return f"gid://shopify/Catalog/{catalog_id}"

    def get_price_list_id(self, catalog_id: str) -> Optional[CatalogPriceListInfo]:
        """Resolve a Catalog ID (numeric or GID) to its PriceList GID."""
        catalog_gid = self._to_catalog_gid(catalog_id)
        self.logger.info(f"🔎 Resolving PriceList for Catalog: {catalog_gid}")

        response = self.shopify_client.execute_graphql(
            CATALOG_PRICE_LIST_QUERY,
            {"catalogId": catalog_gid},
        )

        if response.get("errors"):
            self.logger.error(f"GraphQL errors resolving catalog: {response['errors']}")
            return None

        catalog = (response.get("data") or {}).get("catalog")
        if not catalog:
            self.logger.error(f"Catalog not found: {catalog_gid}")
            return None

        price_list = catalog.get("priceList")
        if not price_list or not price_list.get("id"):
            self.logger.error(f"Catalog {catalog_gid} has no associated PriceList")
            return None

        info = CatalogPriceListInfo(
            catalog_id=catalog["id"],
            price_list_id=price_list["id"],
            price_list_currency=price_list.get("currency"),
        )
        self.logger.info(
            f"✅ Found PriceList: {info.price_list_id} (currency={info.price_list_currency})"
        )
        return info

    def fetch_all_fixed_prices(self, price_list_gid: str) -> List[FixedPriceEntry]:
        """Page through all FIXED-origin prices on the given PriceList."""
        self.logger.info(f"📥 Fetching all fixed prices for PriceList: {price_list_gid}")

        entries: List[FixedPriceEntry] = []
        cursor: Optional[str] = None
        has_next_page = True
        page_count = 0

        while has_next_page:
            page_count += 1
            variables = {"priceListId": price_list_gid}
            if cursor:
                variables["cursor"] = cursor

            response = self.shopify_client.execute_graphql(FIXED_PRICES_QUERY, variables)

            if response.get("errors"):
                self.logger.error(f"GraphQL errors on page {page_count}: {response['errors']}")
                break

            price_list = (response.get("data") or {}).get("priceList")
            if not price_list:
                self.logger.error(f"PriceList not found: {price_list_gid}")
                break

            prices = price_list.get("prices") or {}
            edges = prices.get("edges") or []

            for edge in edges:
                node = edge.get("node") or {}
                variant = node.get("variant") or {}
                price = node.get("price") or {}
                variant_id = variant.get("id")
                if not variant_id:
                    continue
                entries.append(
                    FixedPriceEntry(
                        variant_id=variant_id,
                        price_amount=str(price.get("amount", "")),
                        currency_code=str(price.get("currencyCode", "")),
                    )
                )

            page_info = prices.get("pageInfo") or {}
            has_next_page = bool(page_info.get("hasNextPage"))
            cursor = page_info.get("endCursor")

            self.logger.info(
                f"   Page {page_count}: +{len(edges)} entries (running total: {len(entries)})"
            )

        self.logger.info(f"📦 Total fixed prices found: {len(entries)}")
        return entries

    def _delete_batch(self, price_list_gid: str, variant_ids: List[str]) -> DeleteBatchResult:
        response = self.shopify_client.execute_graphql(
            DELETE_FIXED_PRICES_MUTATION,
            {"priceListId": price_list_gid, "variantIds": variant_ids},
        )

        result = DeleteBatchResult()

        if response.get("errors"):
            result.errors.append(f"GraphQL errors: {response['errors']}")
            return result

        payload = (response.get("data") or {}).get("priceListFixedPricesDelete") or {}
        result.deleted_variant_ids = payload.get("deletedFixedPriceVariantIds") or []

        for ue in payload.get("userErrors") or []:
            result.errors.append(
                f"userError field={ue.get('field')} code={ue.get('code')} message={ue.get('message')}"
            )

        return result

    def delete_in_batches(
        self,
        price_list_gid: str,
        variant_ids: List[str],
    ) -> Tuple[int, int, List[str]]:
        """Delete the given variant IDs in batches. Returns (deleted, failed, errors)."""
        total = len(variant_ids)
        if total == 0:
            return 0, 0, []

        deleted_count = 0
        failed_count = 0
        all_errors: List[str] = []

        batch_size = self.DELETE_BATCH_SIZE
        total_batches = (total + batch_size - 1) // batch_size

        for batch_index in range(total_batches):
            start = batch_index * batch_size
            end = min(start + batch_size, total)
            batch = variant_ids[start:end]

            self.logger.info(
                f"🗑️  Deleting batch {batch_index + 1}/{total_batches} "
                f"({len(batch)} variants)"
            )

            result = self._delete_batch(price_list_gid, batch)

            deleted_in_batch = len(result.deleted_variant_ids)
            failed_in_batch = len(batch) - deleted_in_batch
            deleted_count += deleted_in_batch
            failed_count += failed_in_batch

            if result.errors:
                for err in result.errors:
                    self.logger.error(f"   Batch error: {err}")
                all_errors.extend(result.errors)

            self.logger.info(
                f"   Batch result: deleted={deleted_in_batch}, failed={failed_in_batch}"
            )

        return deleted_count, failed_count, all_errors

    def run(self, catalog_id: str, dry_run: bool) -> Optional[DeleteSummary]:
        """End-to-end orchestration: resolve, fetch, (optionally) delete."""
        info = self.get_price_list_id(catalog_id)
        if not info:
            return None

        entries = self.fetch_all_fixed_prices(info.price_list_id)
        total_found = len(entries)

        if entries:
            self.logger.info("📋 Sample (up to 5):")
            for entry in entries[:5]:
                self.logger.info(
                    f"   variant={entry.variant_id} price={entry.price_amount} {entry.currency_code}"
                )

        if dry_run:
            self.logger.info(f"🧪 [DRY RUN] Would delete {total_found} fixed prices. No changes made.")
            return DeleteSummary(
                catalog_id=info.catalog_id,
                price_list_id=info.price_list_id,
                total_found=total_found,
                total_deleted=0,
                total_failed=0,
                dry_run=True,
            )

        if total_found == 0:
            self.logger.info("✅ Nothing to delete.")
            return DeleteSummary(
                catalog_id=info.catalog_id,
                price_list_id=info.price_list_id,
                total_found=0,
                total_deleted=0,
                total_failed=0,
                dry_run=False,
            )

        variant_ids = [e.variant_id for e in entries]
        deleted, failed, _errors = self.delete_in_batches(info.price_list_id, variant_ids)

        self.logger.info(
            f"🏁 Done. deleted={deleted} failed={failed} total_found={total_found}"
        )

        return DeleteSummary(
            catalog_id=info.catalog_id,
            price_list_id=info.price_list_id,
            total_found=total_found,
            total_deleted=deleted,
            total_failed=failed,
            dry_run=False,
        )
