import requests
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class AirtableClient:
    def __init__(self, token: str, base_id: str):
        self.token = token
        self.base_id = base_id
        self.base_url = f"https://api.airtable.com/v0/{base_id}"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    
    def get_records(
        self,
        table_id: str,
        view_id: Optional[str] = None,
        max_records: Optional[int] = None,
        batch_callback: Optional[callable] = None,
        batch_size: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Fetch records from Airtable table with pagination support

        Args:
            table_id: Airtable table ID
            view_id: Optional view ID to filter records
            max_records: Optional maximum number of records to fetch
            batch_callback: Optional callback function to process each batch of records
            batch_size: Size of each batch (default: 100, max: 100 per Airtable API)

        Returns:
            List of all fetched records (empty if batch_callback is used)
        """
        url = f"{self.base_url}/{table_id}"
        params = {}

        if view_id:
            params["view"] = view_id

        all_records = []
        offset = None
        total_fetched = 0

        while True:
            # Set current request parameters
            current_params = params.copy()
            if offset:
                current_params["offset"] = offset

            # Determine page size (Airtable max is 100 per request)
            remaining = max_records - total_fetched if max_records else batch_size
            page_size = min(100, remaining, batch_size)
            current_params["pageSize"] = page_size

            try:
                response = requests.get(url, headers=self.headers, params=current_params)
                response.raise_for_status()

                data = response.json()
                records = data.get("records", [])
                total_fetched += len(records)

                logger.info(f"Fetched {len(records)} records (total: {total_fetched})")

                # If batch_callback is provided, process batch immediately
                if batch_callback and records:
                    batch_callback(records)
                else:
                    all_records.extend(records)

                # Check if we should continue
                offset = data.get("offset")
                if not offset:
                    break  # No more pages

                if max_records and total_fetched >= max_records:
                    break  # Reached desired limit

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching Airtable records: {e}")
                raise

        if batch_callback:
            logger.info(f"Total processed: {total_fetched} records via batch callback")
            return []  # Return empty list when using callback
        else:
            logger.info(f"Total fetched: {len(all_records)} records from Airtable")
            return all_records
    
    def get_spain_sales_data(self, table_id: str, view_id: str, max_records: int = 500) -> List[Dict[str, Any]]:
        """
        Fetch Spain sales data for the last 90 days
        """
        logger.info(f"Fetching up to {max_records} Spain sales records from Airtable")
        return self.get_records(table_id, view_id, max_records)
    
    def analyze_table_structure(self, table_id: str, view_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Analyze the structure of the table by fetching first 10 records
        """
        records = self.get_records(table_id, view_id, max_records=10)
        
        if not records:
            return {"fields": [], "sample_record": None}
        
        sample_record = records[0]
        fields = list(sample_record.get("fields", {}).keys())
        
        structure = {
            "total_records_fetched": len(records),
            "fields": fields,
            "sample_record": sample_record,
            "field_types": {}
        }
        
        # Analyze field types from sample data
        for field_name, field_value in sample_record.get("fields", {}).items():
            structure["field_types"][field_name] = type(field_value).__name__
        
        logger.info(f"Table structure analyzed: {len(fields)} fields found")
        return structure

    def update_record(self, table_id: str, record_id: str, fields: Dict[str, Any]) -> bool:
        """
        Update a single Airtable record

        Args:
            table_id: Airtable table ID
            record_id: Airtable record ID
            fields: Dictionary of fields to update

        Returns:
            True if successful, False otherwise
        """
        url = f"{self.base_url}/{table_id}/{record_id}"
        payload = {"fields": fields}

        try:
            response = requests.patch(url, headers=self.headers, json=payload, timeout=30)
            response.raise_for_status()
            logger.debug(f"Successfully updated record {record_id}")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error updating record {record_id}: {e}")
            return False

    def batch_update_records(self, table_id: str, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Batch update multiple Airtable records (up to 10 per batch as per Airtable API limit)

        Args:
            table_id: Airtable table ID
            records: List of record dictionaries with format:
                     [{"id": "rec123", "fields": {"Field Name": "value"}}, ...]

        Returns:
            Dictionary with success status and statistics:
            {"success": bool, "updated": int, "failed": int, "errors": List[str]}
        """
        url = f"{self.base_url}/{table_id}"

        result = {
            "success": True,
            "updated": 0,
            "failed": 0,
            "errors": []
        }

        # Airtable API limit is 10 records per batch update
        batch_size = 10

        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            payload = {"records": batch}

            try:
                response = requests.patch(url, headers=self.headers, json=payload, timeout=30)
                response.raise_for_status()

                updated_records = response.json().get("records", [])
                result["updated"] += len(updated_records)
                logger.debug(f"Successfully updated batch of {len(updated_records)} records")

            except requests.exceptions.RequestException as e:
                error_msg = f"Error updating batch {i//batch_size + 1}: {e}"
                logger.error(error_msg)
                result["failed"] += len(batch)
                result["errors"].append(error_msg)
                result["success"] = False

        logger.info(f"Batch update completed: {result['updated']} updated, {result['failed']} failed")
        return result