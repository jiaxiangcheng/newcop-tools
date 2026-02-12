import logging
from typing import List, Dict, Any, Optional
from scripts.job_dynamic_collections.models import SalesRecord, FilteredProduct

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ProductFilter:
    """Filter products based on configurable brand, tags, and sales criteria"""

    def __init__(self,
                 brand_keywords: List[str] = None,
                 excluded_tags: Optional[List[str]] = None,
                 included_tags: Optional[List[str]] = None,
                 excluded_brand_keywords: Optional[List[str]] = None,
                 min_sales_threshold: float = 5.0,
                 sales_period: str = "QUARTERLY"):
        """
        Initialize filter with configurable parameters

        Args:
            brand_keywords: List of brand keywords to include
            excluded_tags: List of tags to exclude (if product has any of these tags, exclude it)
            included_tags: List of tags to include (if specified, product must have at least one of these tags)
            excluded_brand_keywords: List of brand keywords to exclude (if product name/brand contains any, exclude it)
            min_sales_threshold: Minimum sales threshold (applies based on sales_period)
            sales_period: Sales period to use for filtering ("MONTHLY", "QUARTERLY", or "WEEKLY")
        """
        self.brand_keywords = brand_keywords if brand_keywords is not None else [
            "nike", "air jordan", "adidas", "yeezy", "new balance", "asics", "puma", "pop mart"
        ]
        self.excluded_tags = excluded_tags  # Can be None for no restriction
        self.included_tags = included_tags  # Can be None for no restriction
        self.excluded_brand_keywords = excluded_brand_keywords  # Can be None for no restriction
        self.min_sales_threshold = min_sales_threshold
        self.sales_period = sales_period.upper()
        self.filtered_products: List[FilteredProduct] = []
    
    def filter_products_with_newcop_exception(self, sales_records: List[SalesRecord]) -> List[FilteredProduct]:
        """
        Filter products with flexible inclusion logic:

        Step 1 - EXCLUSION (highest priority, ALWAYS applies):
          a) EXCLUDED_TAGS - products with ANY excluded tag (e.g., 'retail') are ALWAYS removed
          b) EXCLUDED_BRAND_KEYWORDS - products with ANY excluded brand keyword are removed

        Step 2 - INCLUSION (at least ONE must pass, OR logic):
          a) Has AT LEAST ONE tag from INCLUDED_TAGS (e.g., 'newcop' or 'ads')
          OR
          b) Has AT LEAST ONE brand keyword from BRAND_KEYWORDS

        Step 3 - Additional requirements (ALL must pass):
          a) Meets sales threshold (>= MIN_SALES_THRESHOLD)

        Step 4 - Sort by sales performance
        """
        all_products = []

        logger.info(f"Starting to filter {len(sales_records)} sales records with flexible OR logic")
        logger.info(f"=" * 80)
        logger.info(f"Filter Configuration:")
        logger.info(f"  - Sales Period: {self.sales_period}")
        logger.info(f"  - Min Sales Threshold: {self.min_sales_threshold}")
        logger.info(f"  - Brand Keywords (OR): {self.brand_keywords}")
        logger.info(f"  - Included Tags (OR): {self.included_tags}")
        logger.info(f"  - Excluded Tags (ALWAYS): {self.excluded_tags}")
        logger.info(f"  - Excluded Brand Keywords (ALWAYS): {self.excluded_brand_keywords}")
        logger.info(f"=" * 80)

        # Counters for debugging
        passed_all_filters = 0
        passed_by_tags = 0
        passed_by_brand = 0
        passed_by_both = 0
        excluded_by_tags = 0
        excluded_by_brand = 0
        excluded_by_missing_both = 0
        excluded_by_sales = 0

        for idx, record in enumerate(sales_records, 1):
            # Get sales value for this record
            sales_value = self._get_sales_value(record)

            # STEP 1: EXCLUSION CHECKS (highest priority, ALWAYS apply)
            # Check excluded tags - ALWAYS exclude if present
            has_excluded_tags = self._has_excluded_tags(record)
            if has_excluded_tags:
                excluded_by_tags += 1
                continue

            # Check excluded brand keywords - ALWAYS exclude if present
            has_excluded_brand = self._has_excluded_brand_keyword(record)
            if has_excluded_brand:
                excluded_by_brand += 1
                continue

            # STEP 2: INCLUSION CHECKS (OR logic - at least ONE must pass)
            # If both brand_keywords and included_tags are empty, skip inclusion checks (all products pass)
            no_inclusion_filters = (not self.brand_keywords or len(self.brand_keywords) == 0) and (not self.included_tags or len(self.included_tags) == 0)

            if no_inclusion_filters:
                # No inclusion filters configured, all products pass this step
                pass
            else:
                # Check if product has included tags (newcop/ads)
                has_included_tags = False
                if self.included_tags and len(self.included_tags) > 0:
                    has_included_tags = self._has_required_included_tags(record, self.included_tags)

                # Check if product has brand keywords
                has_brand_keyword = False
                if self.brand_keywords and len(self.brand_keywords) > 0:
                    has_brand_keyword = self._has_required_brand_keyword(record, self.brand_keywords)

                # Product must have EITHER included tags OR brand keywords (OR logic)
                if not has_included_tags and not has_brand_keyword:
                    excluded_by_missing_both += 1
                    continue

            # STEP 3: Additional requirements
            # Check sales threshold
            meets_sales = self._meets_sales_threshold(record)
            if not meets_sales:
                excluded_by_sales += 1
                continue

            # Product passed all filters, add it
            try:
                filtered_product = FilteredProduct(
                    record_id=record.record_id,
                    product_name=record.product_name or "",
                    brand=record.brand or "",
                    quarterly_sales=record.quarterly_sales,
                    monthly_sales=record.monthly_sales,
                    total_sales=record.total_sales,
                    tags=record.tags or [],
                    shopify_id=record.shopify_id,
                    sales_period=self.sales_period
                )
                all_products.append(filtered_product)
                passed_all_filters += 1

                # Track how product passed
                if has_included_tags and has_brand_keyword:
                    passed_by_both += 1
                elif has_included_tags:
                    passed_by_tags += 1
                else:
                    passed_by_brand += 1
            except Exception as e:
                logger.warning(f"Error creating filtered product '{record.product_name}': {e}")

        logger.info(f"\n✅ Filtering completed: {len(all_products)} products passed all filters")
        logger.info(f"📊 Passed breakdown:")
        logger.info(f"    - {passed_by_tags} by included tags only")
        logger.info(f"    - {passed_by_brand} by brand keywords only")
        logger.info(f"    - {passed_by_both} by both tags and brand")
        logger.info(f"📊 Exclusion breakdown:")
        logger.info(f"    - {excluded_by_tags} by excluded tags (retail)")
        logger.info(f"    - {excluded_by_brand} by excluded brand keywords")
        logger.info(f"    - {excluded_by_missing_both} by missing both tags and brand")
        logger.info(f"    - {excluded_by_sales} by low sales")
        logger.info(f"After deduplication: {len(all_products)} unique products")

        # Step 4: Sort by sales performance (using the configured sales period)
        # Primary sort: sales value based on period (descending)
        # Secondary sort: product_name (A to Z, ascending)
        if self.sales_period == "MONTHLY":
            all_products.sort(
                key=lambda p: (-p.monthly_sales, p.product_name.lower())
            )
        else:  # QUARTERLY
            all_products.sort(
                key=lambda p: (-p.quarterly_sales, p.product_name.lower())
            )

        # Assign sort positions (1-based for Shopify)
        for i, product in enumerate(all_products, 1):
            product.sort_position = i

        logger.info(f"✅ Filtered and sorted {len(all_products)} qualifying products by {self.sales_period} sales performance")

        # Log top 10 products for debugging
        if all_products:
            logger.info(f"📋 Top 10 products by {self.sales_period} sales:")
            for i, product in enumerate(all_products[:10], 1):
                sales_value = product.monthly_sales if self.sales_period == "MONTHLY" else product.quarterly_sales
                logger.info(f"  {i}. {product.product_name} - {self.sales_period}: {sales_value}")

        self.filtered_products = all_products
        return all_products

    def filter_products(self, sales_records: List[SalesRecord]) -> List[FilteredProduct]:
        """
        Filter products based on brand keywords, tags, and sales criteria
        """
        filtered_products = []

        logger.info(f"Starting to filter {len(sales_records)} sales records")

        for record in sales_records:
            if self._should_include_product(record):
                try:
                    filtered_product = FilteredProduct(
                        record_id=record.record_id,
                        product_name=record.product_name or "",
                        brand=record.brand or "",
                        quarterly_sales=record.quarterly_sales,
                        total_sales=record.total_sales,
                        tags=record.tags or [],
                        shopify_id=record.shopify_id
                    )
                    filtered_products.append(filtered_product)
                except Exception as e:
                    logger.warning(f"Error creating filtered product for record {record.record_id}: {e}")
                    continue
        
        # Sort products by sales performance (top sellers first)
        # Primary sort: quarterly_sales (descending)
        # Secondary sort: product_name (A to Z, ascending)
        filtered_products.sort(
            key=lambda p: (-p.quarterly_sales, p.product_name.lower())
        )
        
        # Assign sort positions (1-based for Shopify)
        for i, product in enumerate(filtered_products, 1):
            product.sort_position = i
        
        logger.info(f"✅ Filtered and sorted {len(filtered_products)} qualifying products by sales performance")
        
        self.filtered_products = filtered_products
        return filtered_products
    
    def _should_include_product(self, record: SalesRecord) -> bool:
        """
        Check if a sales record should be included based on filtering criteria
        """
        # Check if product name or brand contains required keywords
        if not self._has_required_brand_keyword(record):
            return False

        # Check if product has excluded brand keywords
        if self._has_excluded_brand_keyword(record):
            return False

        # Check if product has excluded tags
        if self._has_excluded_tags(record):
            return False

        # Check if product has required included tags (if specified)
        if not self._has_required_included_tags(record):
            return False

        # Check sales threshold
        if not self._meets_sales_threshold(record):
            return False

        return True
    
    def _has_required_brand_keyword(self, record: SalesRecord, brand_keywords: Optional[List[str]] = None) -> bool:
        """Check if product name or brand contains required brand keywords"""
        keywords = brand_keywords if brand_keywords is not None else self.brand_keywords

        product_name = (record.product_name or "").lower()
        brand = (record.brand or "").lower()

        for keyword in keywords:
            if keyword.lower() in product_name or keyword.lower() in brand:
                return True

        return False

    def _has_excluded_brand_keyword(self, record: SalesRecord, excluded_brand_keywords: Optional[List[str]] = None) -> bool:
        """Check if product name or brand contains any excluded brand keywords"""
        keywords = excluded_brand_keywords if excluded_brand_keywords is not None else self.excluded_brand_keywords

        if not keywords:
            return False

        product_name = (record.product_name or "").lower()
        brand = (record.brand or "").lower()

        for keyword in keywords:
            if keyword.lower() in product_name or keyword.lower() in brand:
                return True

        return False

    def _has_excluded_tags(self, record: SalesRecord, excluded_tags: Optional[List[str]] = None) -> bool:
        """Check if product has any excluded tags"""
        tags = excluded_tags if excluded_tags is not None else self.excluded_tags

        if not record.tags or not tags:
            return False

        tags_lower = [tag.lower().strip() for tag in record.tags]

        for excluded_tag in tags:
            if excluded_tag.lower() in tags_lower:
                return True

        return False
    
    def _has_required_included_tags(self, record: SalesRecord, included_tags: Optional[List[str]] = None) -> bool:
        """Check if product has required included tags (if specified)"""
        tags = included_tags if included_tags is not None else self.included_tags

        # If no included tags specified, no restriction
        if not tags:
            return True

        # If no tags on product but included tags required, exclude
        if not record.tags:
            return False

        tags_lower = [tag.lower().strip() for tag in record.tags]

        # Product must have at least one of the included tags
        for included_tag in tags:
            if included_tag.lower() in tags_lower:
                return True

        return False
    
    def _get_sales_value(self, record: SalesRecord) -> float:
        """Get the appropriate sales value based on configured sales period"""
        if self.sales_period == "MONTHLY":
            return record.monthly_sales or 0.0
        else:  # QUARTERLY
            return record.quarterly_sales or 0.0

    def _meets_sales_threshold(self, record: SalesRecord) -> bool:
        """Check if product meets minimum sales threshold"""
        sales_value = self._get_sales_value(record)
        return sales_value >= self.min_sales_threshold

    def _get_matching_excluded_tags(self, record: SalesRecord) -> List[str]:
        """Get list of excluded tags that match the product"""
        if not record.tags or not self.excluded_tags:
            return []

        tags_lower = [tag.lower().strip() for tag in record.tags]
        matching_tags = []

        for excluded_tag in self.excluded_tags:
            if excluded_tag.lower() in tags_lower:
                matching_tags.append(excluded_tag)

        return matching_tags

    def _get_matching_excluded_brand(self, record: SalesRecord) -> Optional[str]:
        """Get the excluded brand keyword that matches the product"""
        if not self.excluded_brand_keywords:
            return None

        product_name = (record.product_name or "").lower()
        brand = (record.brand or "").lower()

        for keyword in self.excluded_brand_keywords:
            if keyword.lower() in product_name or keyword.lower() in brand:
                return keyword

        return None

    def filter_products_by_total_stock(
        self,
        records: List[SalesRecord],
        min_total_quantity: float = 4.0,
        excluded_tags: Optional[List[str]] = None,
        included_tags: Optional[List[str]] = None,
        brand_keywords: Optional[List[str]] = None,
        excluded_brand_keywords: Optional[List[str]] = None
    ) -> tuple[List[SalesRecord], List[str]]:
        """
        Filter products based on total stock quantity with tag and brand filtering.

        Logic:
        1. INCLUSION: Products must meet ALL of:
           - total_stock >= min_total_quantity
           - Has AT LEAST ONE required tag (if included_tags specified) - OR logic
           - Has at least one brand keyword (if brand_keywords specified and not empty)

        2. EXCLUSION: Products are excluded if they have:
           - ANY excluded tag
           - ANY excluded brand keyword

        3. SORTING: Maintains original Airtable view order (no sorting applied)

        Args:
            records: List of sales records from Airtable (preserves order)
            min_total_quantity: Minimum total stock quantity required
            excluded_tags: List of tags to exclude (any match excludes product)
            included_tags: List of tags (at least ONE must be present) - OR logic
            brand_keywords: List of brand keywords (at least one must match, or empty/None for no filtering)
            excluded_brand_keywords: List of excluded brand keywords (any match excludes)

        Returns:
            Tuple of (filtered_records, skipped_product_names)
        """
        filtered_records = []
        skipped_products = []

        for record in records:
            # Skip products without Shopify ID
            if not record.shopify_id or record.shopify_id <= 0:
                skipped_products.append(record.product_name or "Unknown Product")
                continue

            # EXCLUSION CHECKS (priority)
            if excluded_brand_keywords and self._has_excluded_brand_keyword(record, excluded_brand_keywords):
                continue

            if excluded_tags and self._has_excluded_tags(record, excluded_tags):
                continue

            # INCLUSION CHECKS
            # Check 1: Total stock threshold
            has_sufficient_stock = (record.total_stock or 0.0) >= min_total_quantity

            # Check 2: Required tags (at least ONE must be present) - OR logic
            has_required_tags = True
            if included_tags:
                has_required_tags = self._has_required_included_tags(record, included_tags)

            # Check 3: Brand keywords (at least one must match, or empty/None means no filtering)
            has_brand_keyword = True
            if brand_keywords and len(brand_keywords) > 0:  # Only filter if list is not empty
                has_brand_keyword = self._has_required_brand_keyword(record, brand_keywords)

            # Product passes if all inclusion checks pass
            if has_sufficient_stock and has_required_tags and has_brand_keyword:
                filtered_records.append(record)

        # DO NOT SORT - preserve Airtable view order
        # filtered_records maintains the original order from the records list

        return filtered_records, skipped_products

    def get_filtering_summary(self) -> Dict[str, Any]:
        """Get summary of filtering criteria and results"""
        return {
            "brand_keywords": self.brand_keywords,
            "excluded_brand_keywords": self.excluded_brand_keywords,
            "excluded_tags": self.excluded_tags,
            "included_tags": self.included_tags,
            "min_sales_threshold": self.min_sales_threshold,
            "total_filtered_products": len(self.filtered_products)
        }