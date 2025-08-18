import requests
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class AirtableClient:
    def __init__(self, token: str, base_id: str):
        self.token = token
        self.base_id = base_id
        self.base_url = f"https://api.airtable.com/v0/{base_id}"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    
    def get_records(self, table_id: str, view_id: Optional[str] = None, max_records: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch records from Airtable table with pagination support
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
            remaining = max_records - total_fetched if max_records else 100
            page_size = min(100, remaining) if max_records else 100
            current_params["pageSize"] = page_size
            
            try:
                response = requests.get(url, headers=self.headers, params=current_params)
                response.raise_for_status()
                
                data = response.json()
                records = data.get("records", [])
                all_records.extend(records)
                total_fetched += len(records)
                
                logger.info(f"Fetched {len(records)} records (total: {total_fetched})")
                
                # Check if we should continue
                offset = data.get("offset")
                if not offset:
                    break  # No more pages
                
                if max_records and total_fetched >= max_records:
                    break  # Reached desired limit
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching Airtable records: {e}")
                raise
        
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