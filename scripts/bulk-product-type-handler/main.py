#!/usr/bin/env python3
"""
Bulk Product Type Handler

Batch operations on Shopify product types:
1. Export products with empty product type to CSV
2. Find all products by a given type and export to CSV
3. Replace one product type with another and export change log to CSV
4. Import product types from Excel file(s) and set them on Shopify (works for both DRAFT and ACTIVE products)

Required environment variables:
- SHOPIFY_ADMIN_TOKEN
- SHOPIFY_SHOP_DOMAIN
"""

import os
import sys
import logging
from pathlib import Path
from typing import Optional, List
from datetime import datetime
from dotenv import load_dotenv

# Add both project root and script directory to path
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SCRIPT_DIR))

from shared.shopify_client import ShopifyClient
from shared.logger import setup_logger
from product_type_handler import ProductTypeHandler

load_dotenv()

logger = setup_logger('bulk_product_type_handler', 'bulk_product_type_handler.log')

related_loggers = [
    'product_type_handler',
]
for logger_name in related_loggers:
    child_logger = logging.getLogger(logger_name)
    child_logger.setLevel(logging.INFO)
    for handler in logger.handlers:
        child_logger.addHandler(handler)

DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "output"


def run_bulk_product_type(
    action: str,
    product_type: Optional[str] = None,
    new_type: Optional[str] = None,
    dry_run: bool = False,
    excel_files: Optional[List[str]] = None,
) -> bool:
    try:
        shopify_admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
        shopify_shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")

        if not shopify_admin_token or not shopify_shop_domain:
            logger.error("Missing SHOPIFY_ADMIN_TOKEN or SHOPIFY_SHOP_DOMAIN environment variables.")
            return False

        logger.info(f"Shop domain: {shopify_shop_domain}")
        logger.info(f"Action: {action}")

        shopify_client = ShopifyClient(shopify_admin_token, shopify_shop_domain)
        handler = ProductTypeHandler(shopify_client)

        date_str = datetime.now().strftime("%Y%m%d")
        os.makedirs(str(DEFAULT_OUTPUT_DIR), exist_ok=True)

        if action == "empty":
            products = handler.get_empty_type_products()
            if not products:
                logger.info("No products with empty type found.")
                return True
            filepath = str(DEFAULT_OUTPUT_DIR / f"empty_type_products_{date_str}.csv")
            handler.write_products_csv(products, filepath)
            return True

        elif action == "find":
            if not product_type:
                logger.error("--type is required for 'find' action.")
                return False
            products = handler.get_products_by_type(product_type)
            if not products:
                logger.info(f"No products found with type '{product_type}'.")
                return True
            safe_type = product_type.replace(" ", "_").replace("/", "_")
            filepath = str(DEFAULT_OUTPUT_DIR / f"products_type_{safe_type}_{date_str}.csv")
            handler.write_products_csv(products, filepath)
            return True

        elif action == "replace":
            if not product_type or not new_type:
                logger.error("--type and --new-type are required for 'replace' action.")
                return False
            records = handler.replace_product_type(product_type, new_type, dry_run=dry_run)
            if not records:
                return True
            safe_old = product_type.replace(" ", "_").replace("/", "_")
            safe_new = new_type.replace(" ", "_").replace("/", "_")
            prefix = "dryrun_" if dry_run else ""
            filepath = str(DEFAULT_OUTPUT_DIR / f"{prefix}replace_{safe_old}_to_{safe_new}_{date_str}.csv")
            handler.write_replace_csv(records, filepath)
            return True

        elif action == "import":
            if not excel_files:
                logger.error("--file is required for 'import' action.")
                return False
            all_products = []
            for ef in excel_files:
                if not os.path.isfile(ef):
                    logger.error(f"File not found: {ef}")
                    return False
                all_products.extend(handler.read_excel_products(ef))
            if not all_products:
                logger.info("No products found in the provided Excel file(s).")
                return True
            logger.info(f"Total products to import: {len(all_products)}")
            records = handler.import_product_types(all_products, dry_run=dry_run)
            if not records:
                return True
            prefix = "dryrun_" if dry_run else ""
            filepath = str(DEFAULT_OUTPUT_DIR / f"{prefix}import_product_types_{date_str}.csv")
            handler.write_replace_csv(records, filepath)
            return True

        else:
            logger.error(f"Unknown action: {action}")
            return False

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Bulk Product Type Handler')
    parser.add_argument('--action', choices=['empty', 'find', 'replace', 'import'], required=True,
                        help='Action: empty (list empty types), find (find by type), replace (replace type), import (set types from Excel)')
    parser.add_argument('--type', dest='product_type', default=None,
                        help='Product type to search for (required for find/replace)')
    parser.add_argument('--new-type', default=None,
                        help='New product type (required for replace)')
    parser.add_argument('--file', dest='excel_files', nargs='+', default=None,
                        help='Excel file(s) to import (required for import)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Dry run mode (replace/import: analyze without making changes)')

    args = parser.parse_args()
    success = run_bulk_product_type(
        action=args.action,
        product_type=args.product_type,
        new_type=args.new_type,
        dry_run=args.dry_run,
        excel_files=args.excel_files,
    )
    sys.exit(0 if success else 1)
