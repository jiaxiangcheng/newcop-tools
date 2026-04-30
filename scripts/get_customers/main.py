#!/usr/bin/env python3
"""
Get Customers for Meta Custom Audience Export

Fetches all Shopify customers and exports them as two Meta Custom Audience CSV files:
- visitors: customers with 0 orders
- customers: customers with 1+ orders

Required environment variables:
- SHOPIFY_ADMIN_TOKEN
- SHOPIFY_SHOP_DOMAIN
"""

import os
import sys
import logging
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from shared.shopify_client import ShopifyClient
from shared.logger import setup_logger
from scripts.get_customers.customer_exporter import CustomerExporter

load_dotenv()

logger = setup_logger('get_customers', 'get_customers.log')

related_loggers = [
    'scripts.get_customers.customer_exporter',
]
for logger_name in related_loggers:
    child_logger = logging.getLogger(logger_name)
    child_logger.setLevel(logging.INFO)
    for handler in logger.handlers:
        child_logger.addHandler(handler)

SCRIPT_DIR = Path(__file__).parent
DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "output"


def run_get_customers(dry_run: bool = False, limit: Optional[int] = None) -> bool:
    try:
        shopify_admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
        shopify_shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")

        if not shopify_admin_token or not shopify_shop_domain:
            logger.error("Missing SHOPIFY_ADMIN_TOKEN or SHOPIFY_SHOP_DOMAIN environment variables.")
            return False

        logger.info(f"Shop domain: {shopify_shop_domain}")
        logger.info(f"Dry run: {dry_run}")
        if limit:
            logger.info(f"Limit: {limit}")

        shopify_client = ShopifyClient(shopify_admin_token, shopify_shop_domain)
        exporter = CustomerExporter(shopify_client)
        return exporter.export(str(DEFAULT_OUTPUT_DIR), dry_run=dry_run, limit=limit)

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Export Shopify customers for Meta Custom Audience')
    parser.add_argument('--dry-run', action='store_true', help='Analyze only, do not write CSV files')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of customers to fetch')

    args = parser.parse_args()
    success = run_get_customers(dry_run=args.dry_run, limit=args.limit)
    sys.exit(0 if success else 1)
