import csv
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

from openai import OpenAI

from .models import ProductSeoStatus, SeoRunSummary, SeoUpdateResult

logger = logging.getLogger(__name__)

PRODUCTS_QUERY = """
query getProducts($cursor: String) {
  products(first: 50, after: $cursor) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        id
        title
        bodyHtml
        seo {
          title
          description
        }
      }
    }
  }
}
"""

UPDATE_MUTATION = """
mutation updateProductSeo($input: ProductInput!) {
  productUpdate(input: $input) {
    product {
      id
      bodyHtml
      seo {
        title
        description
      }
    }
    userErrors {
      field
      message
    }
  }
}
"""

DESCRIPTION_PROMPT = """Busca información sobre el producto "{title}" en internet y escribe una descripción de producto atractiva en español para una tienda online de moda. Debe ser entre 80-150 palabras, en formato HTML con párrafos (<p>), destacando materiales, uso, estilo y beneficios. No incluyas precio ni información de envío. Responde SOLO con el HTML, sin explicaciones ni texto adicional."""

META_DESCRIPTION_PROMPT = """Busca información sobre "{title}" en internet y escribe un meta description en español para SEO de Shopify. Máximo 160 caracteres, sin comillas. Debe ser atractivo, incluir palabras clave naturales y motivar al clic. Responde SOLO con el texto del meta description, sin explicaciones ni texto adicional."""


class SeoContentManager:
    MAX_CONCURRENT_WORKERS = 3

    def __init__(self, shopify_client, openai_client: OpenAI):
        self.shopify_client = shopify_client
        self.openai_client = openai_client

    def run(self, dry_run: bool = False) -> SeoRunSummary:
        start_time = time.time()
        timestamp = datetime.now()

        logger.info(f"Starting SEO content generation (dry_run={dry_run})")

        products = self._fetch_all_products()
        logger.info(f"Fetched {len(products)} total products")

        needs_update = [p for p in products if p.needs_any_update]
        logger.info(f"{len(needs_update)} products need SEO content generation")

        update_results: List[SeoUpdateResult] = []

        if needs_update:
            update_results = self._process_products(needs_update, dry_run)

        successful = sum(1 for r in update_results if r.success)
        failed = sum(1 for r in update_results if not r.success)

        csv_path = self._write_csv_log(update_results, timestamp, dry_run)

        execution_time = time.time() - start_time

        summary = SeoRunSummary(
            total_products=len(products),
            products_needing_update=len(needs_update),
            successful_updates=successful,
            failed_updates=failed,
            dry_run=dry_run,
            execution_time_seconds=round(execution_time, 2),
            timestamp=timestamp,
            csv_log_path=csv_path,
            update_results=update_results,
        )

        self._print_summary(summary)
        return summary

    def _fetch_all_products(self) -> List[ProductSeoStatus]:
        products = []
        cursor = None
        has_next = True

        while has_next:
            variables = {"cursor": cursor}
            response = self.shopify_client.execute_graphql(PRODUCTS_QUERY, variables)

            if "errors" in response:
                logger.error(f"GraphQL errors fetching products: {response['errors']}")
                break

            data = response.get("data", {}).get("products", {})
            page_info = data.get("pageInfo", {})
            edges = data.get("edges", [])

            for edge in edges:
                node = edge.get("node", {})
                body_html = node.get("bodyHtml") or ""
                seo_desc = (node.get("seo") or {}).get("description") or ""
                gid = node["id"]
                product_id_int = int(gid.split("/")[-1])

                products.append(
                    ProductSeoStatus(
                        product_id=gid,
                        product_id_int=product_id_int,
                        title=node.get("title", ""),
                        has_description=bool(body_html.strip()),
                        has_meta_description=bool(seo_desc.strip()),
                        current_description=body_html.strip() or None,
                        current_meta_description=seo_desc.strip() or None,
                    )
                )

            has_next = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")

            if has_next:
                time.sleep(0.5)

        return products

    def _process_products(
        self, products: List[ProductSeoStatus], dry_run: bool
    ) -> List[SeoUpdateResult]:
        all_results: List[SeoUpdateResult] = []

        with ThreadPoolExecutor(max_workers=self.MAX_CONCURRENT_WORKERS) as executor:
            futures = {
                executor.submit(self._handle_product, product, dry_run): product
                for product in products
            }
            for future in as_completed(futures):
                product = futures[future]
                try:
                    results = future.result()
                    all_results.extend(results)
                except Exception as e:
                    logger.error(f"Unhandled error for product {product.title}: {e}")

        return all_results

    def _handle_product(
        self, product: ProductSeoStatus, dry_run: bool
    ) -> List[SeoUpdateResult]:
        results: List[SeoUpdateResult] = []
        generated_description: Optional[str] = None
        generated_meta_description: Optional[str] = None

        if product.needs_description:
            generated_description = self._generate_content(
                DESCRIPTION_PROMPT.format(title=product.title)
            )

        if product.needs_meta_description:
            generated_meta_description = self._generate_content(
                META_DESCRIPTION_PROMPT.format(title=product.title)
            )
            if generated_meta_description:
                generated_meta_description = generated_meta_description[:160]

        if dry_run:
            if generated_description:
                results.append(
                    SeoUpdateResult(
                        product_id=product.product_id,
                        product_id_int=product.product_id_int,
                        title=product.title,
                        field="description",
                        content=f"[DRY RUN] {generated_description}",
                        success=True,
                    )
                )
            if generated_meta_description:
                results.append(
                    SeoUpdateResult(
                        product_id=product.product_id,
                        product_id_int=product.product_id_int,
                        title=product.title,
                        field="meta_description",
                        content=f"[DRY RUN] {generated_meta_description}",
                        success=True,
                    )
                )
            return results

        shopify_results = self._update_shopify(
            product, generated_description, generated_meta_description
        )
        return shopify_results

    def _generate_content(self, prompt: str) -> Optional[str]:
        try:
            response = self.openai_client.responses.create(
                model="gpt-5-mini",
                tools=[{"type": "web_search"}],
                input=prompt,
            )
            content = response.output_text.strip()
            time.sleep(0.3)
            return content
        except Exception as e:
            logger.error(f"OpenAI generation error: {e}")
            return None

    def _update_shopify(
        self,
        product: ProductSeoStatus,
        description: Optional[str],
        meta_description: Optional[str],
    ) -> List[SeoUpdateResult]:
        results: List[SeoUpdateResult] = []

        if not description and not meta_description:
            return results

        mutation_input: dict = {"id": product.product_id}
        if description:
            mutation_input["bodyHtml"] = description
        if meta_description:
            mutation_input["seo"] = {"description": meta_description}

        try:
            response = self.shopify_client.execute_graphql(
                UPDATE_MUTATION, {"input": mutation_input}
            )
            user_errors = (
                response.get("data", {})
                .get("productUpdate", {})
                .get("userErrors", [])
            )

            if user_errors:
                error_msg = "; ".join(
                    f"{e['field']}: {e['message']}" for e in user_errors
                )
                logger.error(
                    f"Shopify userErrors for {product.title}: {error_msg}"
                )
                if description:
                    results.append(
                        SeoUpdateResult(
                            product_id=product.product_id,
                            product_id_int=product.product_id_int,
                            title=product.title,
                            field="description",
                            content=description,
                            success=False,
                            error=error_msg,
                        )
                    )
                if meta_description:
                    results.append(
                        SeoUpdateResult(
                            product_id=product.product_id,
                            product_id_int=product.product_id_int,
                            title=product.title,
                            field="meta_description",
                            content=meta_description,
                            success=False,
                            error=error_msg,
                        )
                    )
            else:
                if description:
                    logger.info(
                        f"Updated description for: {product.title} ({product.product_id_int})"
                    )
                    results.append(
                        SeoUpdateResult(
                            product_id=product.product_id,
                            product_id_int=product.product_id_int,
                            title=product.title,
                            field="description",
                            content=description,
                            success=True,
                        )
                    )
                if meta_description:
                    logger.info(
                        f"Updated meta_description for: {product.title} ({product.product_id_int})"
                    )
                    results.append(
                        SeoUpdateResult(
                            product_id=product.product_id,
                            product_id_int=product.product_id_int,
                            title=product.title,
                            field="meta_description",
                            content=meta_description,
                            success=True,
                        )
                    )

            time.sleep(0.3)

        except Exception as e:
            logger.error(f"Shopify update error for {product.title}: {e}")
            if description:
                results.append(
                    SeoUpdateResult(
                        product_id=product.product_id,
                        product_id_int=product.product_id_int,
                        title=product.title,
                        field="description",
                        content=description,
                        success=False,
                        error=str(e),
                    )
                )
            if meta_description:
                results.append(
                    SeoUpdateResult(
                        product_id=product.product_id,
                        product_id_int=product.product_id_int,
                        title=product.title,
                        field="meta_description",
                        content=meta_description,
                        success=False,
                        error=str(e),
                    )
                )

        return results

    def _write_csv_log(
        self,
        results: List[SeoUpdateResult],
        timestamp: datetime,
        dry_run: bool,
    ) -> str:
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)

        suffix = "_dry_run" if dry_run else ""
        filename = f"seo_content_{timestamp.strftime('%Y%m%d_%H%M%S')}{suffix}.csv"
        filepath = logs_dir / filename

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(["title", "product_id", "field", "content"])
            for r in results:
                writer.writerow([r.title, r.product_id_int, r.field, r.content])

        logger.info(f"CSV log written to: {filepath}")
        return str(filepath)

    def _print_summary(self, summary: SeoRunSummary) -> None:
        print("\n" + "=" * 60)
        print(f"{'[DRY RUN] ' if summary.dry_run else ''}SEO Content Generation Complete")
        print("=" * 60)
        print(f"Total products scanned:     {summary.total_products}")
        print(f"Products needing update:    {summary.products_needing_update}")
        print(f"Successful updates:         {summary.successful_updates}")
        print(f"Failed updates:             {summary.failed_updates}")
        print(f"Execution time:             {summary.execution_time_seconds:.1f}s")
        print(f"CSV log:                    {summary.csv_log_path}")
        print("=" * 60)
