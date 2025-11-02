#!/usr/bin/env python3
"""
Generate Enriched Report from Webgains

Main entry point for enriching Webgains sales reports with Shopify order data.

Usage:
    python main.py -i input_file.xlsx -o output_file.xlsx
    python main.py --input input_file.xlsx --output output_file.xlsx --dry-run
    python main.py -i input_file.xlsx --limit 10
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from typing import Optional, List
from dotenv import load_dotenv

# Add project root to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from shared.shopify_client import ShopifyClient
from shared.logger import setup_logger
from scripts.generate_report_from_webgains.report_processor import ReportProcessor
from scripts.generate_report_from_webgains.order_enricher import OrderEnricher
from scripts.generate_report_from_webgains.excel_writer import ExcelWriter

# Load environment variables
load_dotenv()

# Set up logging
logger = setup_logger('generate_report_from_webgains', 'generate_report_from_webgains.log')

# Configure related loggers
related_loggers = [
    'shared.shopify_client',
    'scripts.generate_report_from_webgains.report_processor',
    'scripts.generate_report_from_webgains.order_enricher',
    'scripts.generate_report_from_webgains.excel_writer'
]

for logger_name in related_loggers:
    child_logger = logging.getLogger(logger_name)
    child_logger.setLevel(logging.INFO)
    # Copy handlers from main logger
    for handler in logger.handlers:
        child_logger.addHandler(handler)


class WebgainsReportEnricher:
    """Main orchestrator for Webgains report enrichment"""

    # Default directories relative to script location
    SCRIPT_DIR = Path(__file__).parent
    DEFAULT_INPUT_DIR = SCRIPT_DIR / "webgains_reports"
    DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "enriched_webgains_reports"

    def __init__(self):
        """Initialize the report enricher"""
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

    def process_report(self, input_file: str, output_file: str, dry_run: bool = False, limit: Optional[int] = None) -> bool:
        """
        Process Webgains report and generate enriched output

        Args:
            input_file: Path to input Webgains Excel file
            output_file: Path for output enriched Excel file
            dry_run: If True, only analyze without making API calls
            limit: Optional limit on number of records to process

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("=" * 80)
            logger.info("WEBGAINS REPORT ENRICHMENT")
            logger.info("=" * 80)
            logger.info(f"Input file: {input_file}")
            logger.info(f"Output file: {output_file}")
            if limit:
                logger.info(f"Limit: {limit} records")
            if dry_run:
                logger.info("Mode: DRY RUN (no API calls)")
            logger.info("=" * 80)

            # Step 1: Load and parse Webgains Excel file
            logger.info("\nStep 1: Loading Webgains Excel file...")
            processor = ReportProcessor(input_file)

            if not processor.load_workbook():
                logger.error("Failed to load workbook")
                return False

            records = processor.parse_records(limit=limit)
            processor.close()

            if not records:
                logger.error("No records found in Excel file")
                return False

            logger.info(f"Successfully parsed {len(records)} records")

            # Step 2: Enrich records with Shopify order data
            logger.info("\nStep 2: Enriching records with Shopify order data...")
            enricher = OrderEnricher(self.shopify_client, max_workers=5)
            result = enricher.enrich_records(records, dry_run=dry_run)

            # Step 3: Write enriched data to output Excel file
            if not dry_run:
                logger.info(f"\nStep 3: Writing enriched data to {output_file}...")
                writer = ExcelWriter(output_file)

                if not writer.write_records(result.enriched_records):
                    logger.error("Failed to write output file")
                    return False

                writer.close()

                logger.info("=" * 80)
                logger.info("SUCCESS!")
                logger.info("=" * 80)
                logger.info(f"Enriched report saved to: {output_file}")
                logger.info(f"Total records: {result.total_records}")
                logger.info(f"Successful lookups: {result.successful_lookups}")
                logger.info(f"Failed lookups: {result.failed_lookups}")
                logger.info(f"Execution time: {result.execution_time_seconds:.2f} seconds")
                logger.info("=" * 80)
            else:
                logger.info("=" * 80)
                logger.info("DRY RUN COMPLETE")
                logger.info("=" * 80)
                logger.info(f"Would process {result.total_records} records")
                logger.info("=" * 80)

            return True

        except Exception as e:
            logger.error(f"Unexpected error during processing: {e}")
            return False

    def process_batch(self, input_dir: Optional[str] = None, output_dir: Optional[str] = None,
                     dry_run: bool = False, limit: Optional[int] = None) -> bool:
        """
        Process all Excel files in a directory

        Args:
            input_dir: Input directory path (default: webgains_reports/)
            output_dir: Output directory path (default: enriched_webgains_reports/)
            dry_run: If True, only analyze without making API calls
            limit: Optional limit on number of records per file

        Returns:
            True if all files processed successfully, False otherwise
        """
        # Use default directories if not specified
        input_path = Path(input_dir) if input_dir else self.DEFAULT_INPUT_DIR
        output_path = Path(output_dir) if output_dir else self.DEFAULT_OUTPUT_DIR

        # Ensure directories exist
        if not input_path.exists():
            logger.error(f"Input directory does not exist: {input_path}")
            return False

        output_path.mkdir(parents=True, exist_ok=True)

        # Find all Excel files
        excel_files = list(input_path.glob("*.xlsx")) + list(input_path.glob("*.xls"))

        if not excel_files:
            logger.warning(f"No Excel files found in {input_path}")
            return False

        logger.info("=" * 80)
        logger.info("BATCH PROCESSING - WEBGAINS REPORTS")
        logger.info("=" * 80)
        logger.info(f"Input directory: {input_path}")
        logger.info(f"Output directory: {output_path}")
        logger.info(f"Found {len(excel_files)} Excel file(s)")
        logger.info("=" * 80)
        print()

        # Process each file
        success_count = 0
        failed_files = []

        for i, input_file in enumerate(excel_files, 1):
            logger.info(f"\n[{i}/{len(excel_files)}] Processing: {input_file.name}")
            logger.info("-" * 80)

            # Generate output filename
            output_file = output_path / f"{input_file.stem}_enriched{input_file.suffix}"

            # Process the file
            try:
                success = self.process_report(
                    input_file=str(input_file),
                    output_file=str(output_file),
                    dry_run=dry_run,
                    limit=limit
                )

                if success:
                    success_count += 1
                    logger.info(f"✅ Successfully processed: {input_file.name}")
                else:
                    failed_files.append(input_file.name)
                    logger.error(f"❌ Failed to process: {input_file.name}")

            except Exception as e:
                failed_files.append(input_file.name)
                logger.error(f"❌ Error processing {input_file.name}: {e}")

            print()

        # Summary
        logger.info("=" * 80)
        logger.info("BATCH PROCESSING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total files: {len(excel_files)}")
        logger.info(f"Successful: {success_count}")
        logger.info(f"Failed: {len(failed_files)}")

        if failed_files:
            logger.info("\nFailed files:")
            for filename in failed_files:
                logger.info(f"  - {filename}")

        logger.info("=" * 80)

        return len(failed_files) == 0


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Generate enriched report from Webgains sales data with Shopify order information",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single file processing
  python main.py -i Transacciones_newcop_2509.xlsx -o enriched_report.xlsx
  python main.py -i input.xlsx -o output.xlsx --dry-run
  python main.py -i input.xlsx -o output.xlsx --limit 50

  # Batch processing (all files in directory)
  python main.py --batch
  python main.py --batch --input-dir ./my_reports --output-dir ./enriched
  python main.py --batch --dry-run
        """
    )

    parser.add_argument(
        "-i", "--input",
        help="Path to input Webgains Excel file (required for single file mode)"
    )

    parser.add_argument(
        "-o", "--output",
        help="Path for output enriched Excel file (default: adds '_enriched' suffix to input filename)"
    )

    parser.add_argument(
        "--batch",
        action="store_true",
        help="Batch process all Excel files in input directory"
    )

    parser.add_argument(
        "--input-dir",
        help="Input directory for batch processing (default: webgains_reports/)"
    )

    parser.add_argument(
        "--output-dir",
        help="Output directory for batch processing (default: enriched_webgains_reports/)"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be processed without making API calls"
    )

    parser.add_argument(
        "--limit",
        type=int,
        help="Process only first N records per file (for testing)"
    )

    args = parser.parse_args()

    # Create enricher
    enricher = WebgainsReportEnricher()

    # Validate environment
    if not enricher.validate_environment():
        sys.exit(1)

    # Initialize Shopify client
    if not enricher.initialize_shopify_client():
        sys.exit(1)

    # Batch or single file processing
    if args.batch:
        # Batch processing mode
        success = enricher.process_batch(
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            dry_run=args.dry_run,
            limit=args.limit
        )
    else:
        # Single file mode
        if not args.input:
            parser.error("--input is required for single file mode (or use --batch for batch processing)")

        # Determine output file path
        output_file = args.output
        if not output_file:
            # Generate default output filename
            input_path = Path(args.input)
            output_file = input_path.parent / f"{input_path.stem}_enriched{input_path.suffix}"

        # Process single report
        success = enricher.process_report(
            input_file=args.input,
            output_file=str(output_file),
            dry_run=args.dry_run,
            limit=args.limit
        )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
