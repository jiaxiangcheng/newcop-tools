"""Core logic: walk every product's media and normalize filename + alt text.

Per the approved plan and Shopify 2025-01 constraints:
- IMAGE (MediaImage)  -> update filename + alt via fileUpdate
- VIDEO (Video)       -> update alt only (Shopify does NOT allow renaming videos)
- MODEL_3D (Model3d)  -> update alt only (same limitation)
- EXTERNAL_VIDEO      -> skipped (no filename concept)

Products are processed sequentially to respect Shopify rate limits; the media of a
single product are processed concurrently with a small ThreadPoolExecutor.
"""
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

from scripts.job_rename_product_media.models import RenameSyncResult
from scripts.job_rename_product_media.naming import (
    build_alt,
    build_filename,
    extract_extension,
    parse_filename_from_url,
    translate_product_type,
)

# How many products to pull per GraphQL page.
PRODUCTS_PER_PAGE = 100
# How many media nodes to request per product (see note about >50 below).
MEDIA_PER_PRODUCT = 50
# Concurrent fileUpdate calls within a single product.
MAX_CONCURRENT_UPDATES = 5

# Media content types we know how to handle.
RENAMABLE_TYPES = {"IMAGE"}  # filename + alt
ALT_ONLY_TYPES = {"VIDEO", "MODEL_3D"}  # alt only (filename not supported by API)

PRODUCTS_QUERY = """
query getProductsWithMedia($cursor: String) {
  products(first: %d, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      title
      productType
      media(first: %d) {
        pageInfo { hasNextPage }
        nodes {
          id
          alt
          mediaContentType
          status
          ... on MediaImage { image { url } }
          ... on Video { filename }
          ... on Model3d { filename }
        }
      }
    }
  }
}
""" % (PRODUCTS_PER_PAGE, MEDIA_PER_PRODUCT)

FILE_UPDATE_MUTATION = """
mutation renameFiles($files: [FileUpdateInput!]!) {
  fileUpdate(files: $files) {
    files {
      id
      alt
      ... on MediaImage { image { url } }
    }
    userErrors { field message code }
  }
}
"""


class MediaManager:
    """Walk products and rename their media filenames / alt text."""

    def __init__(self, shopify_client, logger):
        self.shopify_client = shopify_client
        self.logger = logger

    # ------------------------------------------------------------------ public

    def rename_all_media(self, dry_run: bool = False, limit: Optional[int] = None) -> RenameSyncResult:
        """Process every product (or up to ``limit`` products) and return stats."""
        start = time.time()
        result = RenameSyncResult(dry_run=dry_run)

        self.logger.info("=" * 60)
        self.logger.info("🖼️  Rename Product Media")
        self.logger.info(f"Mode: {'🧪 DRY RUN' if dry_run else '✅ LIVE'}")
        if limit:
            self.logger.info(f"Limit: first {limit} product(s)")
        self.logger.info("=" * 60)

        for product in self._iter_products(limit):
            result.total_products += 1
            self._process_product(product, dry_run, result)

        result.execution_time_seconds = time.time() - start
        self._log_summary(result)
        return result

    # ----------------------------------------------------------------- product

    def _iter_products(self, limit: Optional[int]):
        """Yield products one at a time, following the GraphQL cursor."""
        cursor = None
        fetched = 0
        page = 0

        while True:
            page += 1
            variables = {"cursor": cursor} if cursor else {}
            response = self.shopify_client.execute_graphql(PRODUCTS_QUERY, variables)

            if "errors" in response and response["errors"]:
                self.logger.error(f"GraphQL error fetching products: {response['errors']}")
                break

            products_data = (response.get("data") or {}).get("products") or {}
            nodes = products_data.get("nodes") or []

            for product in nodes:
                yield product
                fetched += 1
                if limit and fetched >= limit:
                    self.logger.info(f"Reached limit of {limit} product(s)")
                    return

            page_info = products_data.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

            if page % 5 == 0:
                self.logger.info(f"  ...scanned {fetched} products so far")

    def _process_product(self, product: Dict[str, Any], dry_run: bool, result: RenameSyncResult) -> None:
        title = product.get("title") or ""
        # Translate the English product type to its Spanish label (falls back to
        # the original type when unmapped).
        type_label = translate_product_type(product.get("productType") or "")
        media_block = product.get("media") or {}
        media_nodes = media_block.get("nodes") or []

        if (media_block.get("pageInfo") or {}).get("hasNextPage"):
            self.logger.warning(
                f"⚠️  Product '{title}' has more than {MEDIA_PER_PRODUCT} media; "
                f"only the first {MEDIA_PER_PRODUCT} were processed."
            )

        if not media_nodes:
            return

        # Build the per-media work plan (position is 1-based, in media order).
        plans: List[Dict[str, Any]] = []
        for index, media in enumerate(media_nodes, start=1):
            result.total_media += 1
            plan = self._plan_media(media, title, type_label, index, result)
            if plan is not None:
                plans.append(plan)

        if not plans:
            return

        if dry_run:
            for plan in plans:
                self._log_dry(plan)
            # Count what would have happened.
            for plan in plans:
                if plan["is_image"]:
                    result.images_renamed += 1
                else:
                    result.videos_alt_updated += 1
            return

        # Live: run fileUpdate concurrently for this product's media.
        self._execute_updates(plans, title, result)

    # -------------------------------------------------------------------- plan

    def _plan_media(
        self,
        media: Dict[str, Any],
        title: str,
        type_label: str,
        position: int,
        result: RenameSyncResult,
    ) -> Optional[Dict[str, Any]]:
        """Return a work plan dict for one media node, or None if it should be skipped."""
        media_id = media.get("id")
        content_type = media.get("mediaContentType")
        status = media.get("status")

        if content_type == "EXTERNAL_VIDEO":
            result.skipped_external_video += 1
            return None

        if content_type not in RENAMABLE_TYPES and content_type not in ALT_ONLY_TYPES:
            # Unknown/unsupported type — skip quietly but don't crash.
            self.logger.debug(f"Skipping unsupported media type {content_type} ({media_id})")
            result.skipped_external_video += 1
            return None

        # Media must be READY before fileUpdate will accept it.
        if status and status != "READY":
            result.skipped_not_ready += 1
            self.logger.info(f"⏭️  Skipping {media_id}: status={status} (not READY)")
            return None

        target_alt = build_alt(title, type_label, position)
        current_alt = media.get("alt") or ""

        is_image = content_type in RENAMABLE_TYPES

        if is_image:
            image_url = ((media.get("image") or {}).get("url")) or ""
            ext = extract_extension(image_url)
            target_filename = build_filename(title, type_label, position, ext)
            current_filename = parse_filename_from_url(image_url)

            # Idempotent skip: both filename and alt already match.
            if current_filename == target_filename and current_alt == target_alt:
                result.skipped_already_ok += 1
                return None

            return {
                "media_id": media_id,
                "is_image": True,
                "target_filename": target_filename,
                "target_alt": target_alt,
                "current_filename": current_filename,
                "current_alt": current_alt,
                "content_type": content_type,
            }

        # Video / 3D model: alt only.
        if current_alt == target_alt:
            result.skipped_already_ok += 1
            return None

        return {
            "media_id": media_id,
            "is_image": False,
            "target_filename": None,
            "target_alt": target_alt,
            "current_filename": media.get("filename") or "",
            "current_alt": current_alt,
            "content_type": content_type,
        }

    # ------------------------------------------------------------------ update

    def _execute_updates(self, plans: List[Dict[str, Any]], title: str, result: RenameSyncResult) -> None:
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_UPDATES) as executor:
            future_to_plan = {executor.submit(self._update_one, plan): plan for plan in plans}
            for future in as_completed(future_to_plan):
                plan = future_to_plan[future]
                try:
                    ok = future.result()
                except Exception as exc:  # defensive: never let one media kill the run
                    self.logger.error(f"💥 Error updating {plan['media_id']}: {exc}")
                    ok = False

                if ok:
                    if plan["is_image"]:
                        result.images_renamed += 1
                    else:
                        result.videos_alt_updated += 1
                else:
                    result.failed += 1
                    result.failed_media_ids.append(plan["media_id"])

                time.sleep(0.1)  # gentle on rate limits

    def _update_one(self, plan: Dict[str, Any], _retry_suffix: int = 0) -> bool:
        """Call fileUpdate for one media. Retries once with a numeric suffix on a name clash."""
        file_input: Dict[str, Any] = {"id": plan["media_id"], "alt": plan["target_alt"]}

        if plan["is_image"]:
            filename = plan["target_filename"]
            if _retry_suffix:
                filename = self._append_suffix(filename, _retry_suffix)
            file_input["filename"] = filename

        response = self.shopify_client.execute_graphql(
            FILE_UPDATE_MUTATION, {"files": [file_input]}
        )

        if "errors" in response and response["errors"]:
            self.logger.error(f"❌ {plan['media_id']}: GraphQL errors: {response['errors']}")
            return False

        payload = (response.get("data") or {}).get("fileUpdate") or {}
        user_errors = payload.get("userErrors") or []

        if user_errors:
            # Retry once on a likely filename collision.
            if plan["is_image"] and _retry_suffix == 0 and self._is_name_clash(user_errors):
                self.logger.warning(
                    f"♻️  {plan['media_id']}: filename clash, retrying with suffix -2"
                )
                return self._update_one(plan, _retry_suffix=2)
            self.logger.error(f"❌ {plan['media_id']}: userErrors: {user_errors}")
            return False

        self.logger.info(
            f"✅ {plan['media_id']}: "
            + (f"filename → {file_input.get('filename')}, " if plan["is_image"] else "")
            + f"alt → {plan['target_alt']}"
        )
        return True

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _append_suffix(filename: str, suffix: int) -> str:
        """Insert ``-{suffix}`` before the extension: foo-1.jpg -> foo-1-2.jpg."""
        if "." in filename:
            base, ext = filename.rsplit(".", 1)
            return f"{base}-{suffix}.{ext}"
        return f"{filename}-{suffix}"

    @staticmethod
    def _is_name_clash(user_errors: List[Dict[str, Any]]) -> bool:
        """Heuristic: does a userError look like a filename uniqueness problem?"""
        for err in user_errors:
            message = (err.get("message") or "").lower()
            code = (err.get("code") or "").lower()
            if "filename" in message or "already" in message or "taken" in message or "exist" in code:
                return True
        return False

    def _log_dry(self, plan: Dict[str, Any]) -> None:
        if plan["is_image"]:
            self.logger.info(
                f"[DRY] {plan['media_id']} ({plan['content_type']}): "
                f"filename '{plan['current_filename']}' → '{plan['target_filename']}' | "
                f"alt '{plan['current_alt']}' → '{plan['target_alt']}'"
            )
        else:
            self.logger.info(
                f"[DRY] {plan['media_id']} ({plan['content_type']}): "
                f"alt '{plan['current_alt']}' → '{plan['target_alt']}' "
                f"(filename unchanged — Shopify does not allow renaming this media type)"
            )

    def _log_summary(self, result: RenameSyncResult) -> None:
        self.logger.info("=" * 60)
        self.logger.info("🏁 SUMMARY")
        self.logger.info(f"   Products scanned:        {result.total_products}")
        self.logger.info(f"   Media scanned:           {result.total_media}")
        self.logger.info(f"   Images renamed:          {result.images_renamed}")
        self.logger.info(f"   Videos/3D alt updated:   {result.videos_alt_updated}")
        self.logger.info(f"   Skipped (already ok):    {result.skipped_already_ok}")
        self.logger.info(f"   Skipped (external video):{result.skipped_external_video}")
        self.logger.info(f"   Skipped (not ready):     {result.skipped_not_ready}")
        self.logger.info(f"   Failed:                  {result.failed}")
        if result.failed_media_ids:
            self.logger.info(f"   Failed media ids:        {result.failed_media_ids}")
        self.logger.info(f"   Time:                    {result.execution_time_seconds:.1f}s")
        if not result.dry_run and result.images_renamed:
            self.logger.info("   ⚠️  Renamed images now have new CDN URLs.")
        self.logger.info("=" * 60)
