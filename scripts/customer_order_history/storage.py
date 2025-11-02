#!/usr/bin/env python3
"""
Storage Management for Customer Order History

Manages local JSON cache for tracking processed records to ensure idempotency.
"""

import json
import logging
import os
from typing import Optional, Set
from datetime import datetime
from pathlib import Path

from scripts.customer_order_history.models import ProcessedRecordsCache

logger = logging.getLogger(__name__)


class OrderHistoryStorage:
    """Manages persistent storage for processed order records"""

    def __init__(self, cache_file_path: str = "data/customer_order_history_cache.json"):
        """
        Initialize storage manager

        Args:
            cache_file_path: Path to the cache JSON file
        """
        self.cache_file_path = cache_file_path
        self.cache: ProcessedRecordsCache = ProcessedRecordsCache()

        # Ensure data directory exists
        cache_dir = os.path.dirname(cache_file_path)
        if cache_dir and not os.path.exists(cache_dir):
            os.makedirs(cache_dir, exist_ok=True)
            logger.info(f"Created cache directory: {cache_dir}")

    def load_cache(self) -> bool:
        """
        Load cache from JSON file

        Returns:
            True if cache was loaded successfully, False otherwise
        """
        if not os.path.exists(self.cache_file_path):
            logger.info(f"No existing cache file found at {self.cache_file_path}, starting fresh")
            return False

        try:
            with open(self.cache_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Convert ISO string back to datetime
            if data.get("last_sync"):
                data["last_sync"] = datetime.fromisoformat(data["last_sync"])

            self.cache = ProcessedRecordsCache(**data)
            logger.info(f"Loaded cache with {len(self.cache.processed_record_ids)} processed records")
            logger.info(f"Last sync: {self.cache.last_sync}")
            return True

        except Exception as e:
            logger.error(f"Error loading cache: {e}")
            self.cache = ProcessedRecordsCache()
            return False

    def save_cache(self) -> bool:
        """
        Save cache to JSON file with atomic write

        Returns:
            True if cache was saved successfully, False otherwise
        """
        try:
            # Update timestamp
            self.cache.last_sync = datetime.now()

            # Convert to dict for JSON serialization
            cache_dict = self.cache.dict()

            # Convert datetime to ISO string
            if cache_dict.get("last_sync"):
                cache_dict["last_sync"] = cache_dict["last_sync"].isoformat()

            # Use atomic write (write to temp file, then rename)
            temp_file = f"{self.cache_file_path}.tmp"

            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(cache_dict, f, indent=2, ensure_ascii=False)

            # Atomic rename
            os.replace(temp_file, self.cache_file_path)

            logger.info(f"Saved cache with {len(self.cache.processed_record_ids)} processed records")
            return True

        except Exception as e:
            logger.error(f"Error saving cache: {e}")
            return False

    def is_record_processed(self, record_id: str) -> bool:
        """
        Check if a record has already been processed

        Args:
            record_id: Airtable record ID

        Returns:
            True if record was already processed, False otherwise
        """
        return record_id in self.cache.processed_record_ids

    def mark_records_processed(self, record_ids: list[str]) -> None:
        """
        Mark records as processed

        Args:
            record_ids: List of Airtable record IDs
        """
        initial_count = len(self.cache.processed_record_ids)

        # Convert to set for efficient operations
        processed_set = set(self.cache.processed_record_ids)
        processed_set.update(record_ids)

        self.cache.processed_record_ids = list(processed_set)
        self.cache.total_processed = len(self.cache.processed_record_ids)

        new_records = len(self.cache.processed_record_ids) - initial_count
        logger.info(f"Marked {new_records} new records as processed (total: {self.cache.total_processed})")

    def get_unprocessed_records(self, all_record_ids: list[str]) -> list[str]:
        """
        Filter out already processed records

        Args:
            all_record_ids: List of all record IDs to check

        Returns:
            List of unprocessed record IDs
        """
        processed_set = set(self.cache.processed_record_ids)
        unprocessed = [rid for rid in all_record_ids if rid not in processed_set]

        logger.info(f"Found {len(unprocessed)} unprocessed records out of {len(all_record_ids)} total")
        return unprocessed

    def clear_cache(self) -> bool:
        """
        Clear all cached data

        Returns:
            True if cache was cleared successfully, False otherwise
        """
        self.cache = ProcessedRecordsCache()
        logger.info("Cache cleared")
        return self.save_cache()

    def get_cache_stats(self) -> dict:
        """
        Get cache statistics

        Returns:
            Dictionary with cache statistics
        """
        return {
            "total_processed": self.cache.total_processed,
            "last_sync": self.cache.last_sync.isoformat() if self.cache.last_sync else None,
            "cache_file_path": self.cache_file_path,
            "cache_file_exists": os.path.exists(self.cache_file_path)
        }
