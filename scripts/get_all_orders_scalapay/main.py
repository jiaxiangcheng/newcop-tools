#!/usr/bin/env python3
"""
Get All Orders Scalapay

Fetches all Shopify online store orders paid with Scalapay and exports them to Excel.

Output includes:
- Order name
- Customer email
- Customer name
- Fulfillment status
- Financial status (payment status)

Usage:
    python main.py                    # Run with default output file
    python main.py -o output.xlsx     # Specify output file
    python main.py --dry-run          # Preview without writing file
    python main.py --limit 100        # Limit to first 100 orders
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Add project root to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from shared.shopify_client import ShopifyClient
from shared.logger import setup_logger
from scripts.get_all_orders_scalapay.order_manager import ScalapayOrderManager
from scripts.get_all_orders_scalapay.excel_writer import ScalapayExcelWriter

# Load environment variables
load_dotenv()

# Set up logging
logger = setup_logger('get_all_orders_scalapay', 'scalapay_orders.log')

# Configure related loggers
related_loggers = [
    'shared.shopify_client',
    'scripts.get_all_orders_scalapay.order_manager',
    'scripts.get_all_orders_scalapay.excel_writer'
]

for logger_name in related_loggers:
    child_logger = logging.getLogger(logger_name)
    child_logger.setLevel(logging.INFO)
    for handler in logger.handlers:
        child_logger.addHandler(handler)


class ScalapayOrdersFetcher:
    """Main orchestrator for fetching Scalapay orders"""

    # Default output directory
    SCRIPT_DIR = Path(__file__).parent
    DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "reports"

    def __init__(self):
        """Initialize the fetcher"""
        self.shopify_admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
        self.shopify_shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")
        self.shopify_client = None

    def validate_environment(self) -> bool:
        """Validate required environment variables"""
        required_configs = [
            ("SHOPIFY_ADMIN_TOKEN", self.shopify_admin_token),
            ("SHOPIFY_SHOP_DOMAIN", self.shopify_shop_domain)
        ]

        missing_configs = []
        for name, value in required_configs:
            if not value or value in ["your_token_here", "your_shop", ""]:
                missing_configs.append(name)

        if missing_configs:
            logger.error(f"Missing required environment variables: {', '.join(missing_configs)}")
            logger.error("Please create a .env file with all required variables.")
            return False

        logger.info("Environment validation passed")
        logger.info(f"Shop domain: {self.shopify_shop_domain}")
        return True

    def initialize_shopify_client(self) -> bool:
        """Initialize Shopify client"""
        try:
            self.shopify_client = ShopifyClient(self.shopify_admin_token, self.shopify_shop_domain)
            logger.info("Shopify client initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Shopify client: {e}")
            return False

    def run(
        self,
        output_file: str = None,
        dry_run: bool = False,
        limit: int = None
    ) -> bool:
        """
        Run the Scalapay orders fetch and export

        Args:
            output_file: Path for output Excel file
            dry_run: If True, only analyze without writing file
            limit: Optional limit on number of orders to scan

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("=" * 60)
            logger.info("🔍 SCALAPAY ORDERS FETCH")
            logger.info("=" * 60)
            if limit:
                logger.info(f"Limit: {limit} orders")
            if dry_run:
                logger.info("Mode: DRY RUN (no file output)")
            logger.info("=" * 60)

            # Create order manager
            order_manager = ScalapayOrderManager(self.shopify_client)

            # Fetch Scalapay orders
            result = order_manager.fetch_scalapay_orders(
                limit=limit,
                dry_run=dry_run
            )

            # Write to Excel if not dry run and orders found
            if not dry_run and result.orders:
                # Generate output file path if not specified
                if not output_file:
                    self.DEFAULT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    output_file = str(self.DEFAULT_OUTPUT_DIR / f"scalapay_orders_{timestamp}.xlsx")

                # Write Excel file
                writer = ScalapayExcelWriter(output_file)
                success = writer.write_orders(result)
                writer.close()

                if success:
                    logger.info("=" * 60)
                    logger.info("✅ SUCCESS!")
                    logger.info("=" * 60)
                    logger.info(f"Output file: {output_file}")
                    logger.info(f"Total orders scanned: {result.total_orders_scanned}")
                    logger.info(f"Scalapay orders found: {result.scalapay_orders_found}")
                    logger.info("=" * 60)
                    return True
                else:
                    logger.error("Failed to write Excel file")
                    return False

            elif dry_run:
                logger.info("=" * 60)
                logger.info("🧪 DRY RUN COMPLETE")
                logger.info("=" * 60)
                logger.info(f"Total orders scanned: {result.total_orders_scanned}")
                logger.info(f"Scalapay orders found: {result.scalapay_orders_found}")
                logger.info("=" * 60)
                return True

            elif not result.orders:
                logger.warning("No Scalapay orders found")
                return True

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False


def run_scalapay_orders(
    output_file: str = None,
    dry_run: bool = False,
    limit: int = None
) -> bool:
    """
    Entry point for CLI integration

    Args:
        output_file: Path for output Excel file
        dry_run: If True, only analyze without writing file
        limit: Optional limit on number of orders to scan

    Returns:
        True if successful, False otherwise
    """
    fetcher = ScalapayOrdersFetcher()

    if not fetcher.validate_environment():
        return False

    if not fetcher.initialize_shopify_client():
        return False

    return fetcher.run(
        output_file=output_file,
        dry_run=dry_run,
        limit=limit
    )


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Fetch all Shopify online store orders paid with Scalapay and export to Excel",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                     # Run with default output file
  python main.py -o my_report.xlsx   # Specify output file
  python main.py --dry-run           # Preview without writing file
  python main.py --limit 100         # Limit to first 100 orders
        """
    )

    parser.add_argument(
        "-o", "--output",
        help="Path for output Excel file (default: reports/scalapay_orders_<timestamp>.xlsx)"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be fetched without writing file"
    )

    parser.add_argument(
        "--limit",
        type=int,
        help="Limit to first N orders (for testing)"
    )

    args = parser.parse_args()

    success = run_scalapay_orders(
        output_file=args.output,
        dry_run=args.dry_run,
        limit=args.limit
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
