#!/usr/bin/env python3
"""
Massive Airtable Files Downloader

Downloads PDF files from Airtable CSV export URLs.
"""

import os
import sys
import logging
import argparse
from pathlib import Path
from typing import Tuple
import pandas as pd
import requests
from tqdm import tqdm

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from shared.logger import setup_logger

# Configure logging with proper Unicode handling
logger = setup_logger(
    logger_name=__name__,
    log_file_name='airtable_downloader.log',
    log_level=logging.INFO
)


class AirtableFileDownloader:
    """Downloads files from Airtable CSV exports"""

    def __init__(self, csv_path: str, output_dir: str, column_name: str = "Factura"):
        """
        Initialize downloader

        Args:
            csv_path: Path to CSV file with URLs
            output_dir: Directory to save downloaded files
            column_name: Name of column containing URLs
        """
        self.csv_path = Path(csv_path)
        self.output_dir = Path(output_dir)
        self.column_name = column_name
        self.max_retries = 3
        self.timeout = 30

        # Validate inputs
        if not self.csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def load_csv(self) -> pd.DataFrame:
        """
        Load CSV file and validate column exists

        Returns:
            DataFrame with CSV data
        """
        logger.info(f"Loading CSV file: {self.csv_path}")
        df = pd.read_csv(self.csv_path)

        if self.column_name not in df.columns:
            raise ValueError(
                f"Column '{self.column_name}' not found in CSV. "
                f"Available columns: {', '.join(df.columns)}"
            )

        # Count URLs
        url_count = df[self.column_name].notna().sum()
        logger.info(f"Found {url_count} URLs in column '{self.column_name}'")

        return df

    def parse_url_from_cell(self, cell_value: str) -> Tuple[str, str]:
        """
        Parse URL and filename from Airtable cell value

        Airtable attachment cells can have formats like:
        - "filename.pdf (https://url...)"
        - "https://url..."

        Args:
            cell_value: Raw cell value from CSV

        Returns:
            Tuple of (url, filename)
        """
        cell_value = str(cell_value).strip()

        # Check if format is "filename.pdf (url)"
        if "(" in cell_value and ")" in cell_value:
            # Extract filename and URL
            parts = cell_value.split("(")
            filename = parts[0].strip()
            url = parts[1].rstrip(")").strip()
        else:
            # Direct URL
            url = cell_value
            filename = os.path.basename(url.split("?")[0])

        # Ensure .pdf extension
        if not filename.lower().endswith(".pdf"):
            filename += ".pdf"

        return url, filename

    def download_file(self, url_cell: str, retry_count: int = 0) -> Tuple[bool, str]:
        """
        Download a single file with retry logic

        Args:
            url_cell: URL or "filename (url)" from CSV cell
            retry_count: Current retry attempt

        Returns:
            Tuple of (success, filepath or error message)
        """
        try:
            # Parse URL and filename from cell
            url, filename = self.parse_url_from_cell(url_cell)

            filepath = self.output_dir / filename

            # Skip if file already exists
            if filepath.exists():
                logger.debug(f"File already exists, skipping: {filename}")
                return True, str(filepath)

            # Download with timeout
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()

            # Save file
            with open(filepath, "wb") as f:
                f.write(response.content)

            logger.debug(f"Downloaded: {filename}")
            return True, str(filepath)

        except requests.exceptions.RequestException as e:
            # Retry logic
            if retry_count < self.max_retries:
                logger.warning(
                    f"Download failed (attempt {retry_count + 1}/{self.max_retries}): {filename}\n"
                    f"URL: {url}\nError: {e}\nRetrying..."
                )
                return self.download_file(url_cell, retry_count + 1)
            else:
                error_msg = f"Failed after {self.max_retries} attempts: {str(e)}"
                logger.error(f"Download failed for {filename} ({url}): {error_msg}")
                return False, error_msg

        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(f"Download failed for {url_cell}: {error_msg}")
            return False, error_msg

    def download_all(self, dry_run: bool = False, limit: int = None) -> dict:
        """
        Download all files from CSV

        Args:
            dry_run: If True, only show what would be downloaded
            limit: Maximum number of files to download (for testing)

        Returns:
            Dictionary with download statistics
        """
        # Load CSV
        df = self.load_csv()

        # Get URLs (drop NaN values)
        urls = df[self.column_name].dropna().tolist()

        # Apply limit if specified
        if limit:
            urls = urls[:limit]
            logger.info(f"Limited to first {limit} files")

        if dry_run:
            logger.info(f"DRY RUN: Would download {len(urls)} files to {self.output_dir}")
            for i, url in enumerate(urls[:5], 1):
                logger.info(f"  {i}. {url}")
            if len(urls) > 5:
                logger.info(f"  ... and {len(urls) - 5} more")
            return {"total": len(urls), "success": 0, "failed": 0, "dry_run": True}

        # Download files
        logger.info(f"Starting download of {len(urls)} files to {self.output_dir}")

        success_count = 0
        failed_count = 0
        failed_urls = []

        for url in tqdm(urls, desc="Downloading PDFs"):
            success, result = self.download_file(url)
            if success:
                success_count += 1
            else:
                failed_count += 1
                failed_urls.append((url, result))

        # Print summary
        logger.info("\n" + "=" * 60)
        logger.info("Download Summary")
        logger.info("=" * 60)
        logger.info(f"Total URLs: {len(urls)}")
        logger.info(f"✅ Successfully downloaded: {success_count}")
        logger.info(f"❌ Failed downloads: {failed_count}")
        logger.info(f"Output directory: {self.output_dir.absolute()}")

        if failed_urls:
            logger.warning("\nFailed downloads:")
            for url, error in failed_urls:
                logger.warning(f"  - {url}")
                logger.warning(f"    Error: {error}")

        return {
            "total": len(urls),
            "success": success_count,
            "failed": failed_count,
            "failed_urls": failed_urls,
            "dry_run": False
        }


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Download PDF files from Airtable CSV export"
    )
    parser.add_argument(
        "-i", "--input",
        default="scripts/massive_download_airtable_files/Items-INVOICE.csv",
        help="Path to CSV file (default: Items-INVOICE.csv in script directory)"
    )
    parser.add_argument(
        "-o", "--output",
        default="scripts/massive_download_airtable_files/facturas_pdf",
        help="Output directory for PDFs (default: facturas_pdf in script directory)"
    )
    parser.add_argument(
        "-c", "--column",
        default="Factura",
        help="Name of column containing URLs (default: Factura)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be downloaded without actually downloading"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of files to download (for testing)"
    )

    args = parser.parse_args()

    try:
        # Create downloader
        downloader = AirtableFileDownloader(
            csv_path=args.input,
            output_dir=args.output,
            column_name=args.column
        )

        # Download files
        result = downloader.download_all(
            dry_run=args.dry_run,
            limit=args.limit
        )

        # Exit with appropriate code
        if result["failed"] > 0:
            sys.exit(1)
        else:
            sys.exit(0)

    except KeyboardInterrupt:
        logger.info("\n⚠️  Process interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
