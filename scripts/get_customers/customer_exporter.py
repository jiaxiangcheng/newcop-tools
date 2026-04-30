import csv
import os
import time
import logging
from datetime import datetime
from typing import Optional

from scripts.get_customers.models import MetaAudienceRow, CustomerExportResult, META_CSV_HEADERS

logger = logging.getLogger(__name__)


class CustomerExporter:
    def __init__(self, shopify_client):
        self.shopify_client = shopify_client

    def export(self, output_dir: str, dry_run: bool = False, limit: Optional[int] = None) -> bool:
        start_time = time.time()

        logger.info("Fetching all customers from Shopify...")
        all_customers = self.shopify_client.get_all_customers(limit=limit)

        if not all_customers:
            logger.warning("No customers found.")
            return True

        visitors = []
        customers = []

        for c in all_customers:
            row = self._normalize_customer(c)
            orders_count = c.get("orders_count", 0)
            if orders_count == 0:
                visitors.append(row)
            else:
                customers.append(row)

        elapsed = time.time() - start_time
        result = CustomerExportResult(
            visitors=visitors,
            customers=customers,
            total_fetched=len(all_customers),
            execution_time_seconds=elapsed,
        )

        logger.info(f"Total customers fetched: {result.total_fetched}")
        logger.info(f"Visitors (0 orders): {len(result.visitors)}")
        logger.info(f"Customers (1+ orders): {len(result.customers)}")
        logger.info(f"Execution time: {result.execution_time_seconds:.2f}s")

        if dry_run:
            logger.info("Dry run mode - no files written.")
            return True

        os.makedirs(output_dir, exist_ok=True)
        date_str = datetime.now().strftime("%Y%m%d")

        if result.visitors:
            visitors_path = os.path.join(output_dir, f"visitors_meta_audience_{date_str}.csv")
            self._write_csv(result.visitors, visitors_path)
            logger.info(f"Visitors CSV written: {visitors_path} ({len(result.visitors)} rows)")

        if result.customers:
            customers_path = os.path.join(output_dir, f"customers_meta_audience_{date_str}.csv")
            self._write_csv(result.customers, customers_path)
            logger.info(f"Customers CSV written: {customers_path} ({len(result.customers)} rows)")

        return True

    def _normalize_customer(self, customer: dict) -> MetaAudienceRow:
        addr = customer.get("default_address") or {}

        email = customer.get("email") or ""
        phone = customer.get("phone") or addr.get("phone") or ""
        fn = (customer.get("first_name") or "").strip().lower()
        ln = (customer.get("last_name") or "").strip().lower()
        zip_code = addr.get("zip") or ""
        ct = (addr.get("city") or "").strip().lower()
        st = addr.get("province_code") or ""
        country = addr.get("country_code") or ""
        uid = str(customer.get("id", ""))

        return MetaAudienceRow(
            email=email,
            phone=phone,
            fn=fn,
            ln=ln,
            zip=zip_code,
            ct=ct,
            st=st,
            country=country,
            uid=uid,
        )

    def _write_csv(self, rows: list, filepath: str):
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(META_CSV_HEADERS)
            for row in rows:
                writer.writerow(row.to_csv_row())
