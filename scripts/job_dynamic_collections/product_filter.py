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
                 min_quarterly_sales: float = 5.0,
                 sales_period: str = "QUARTERLY"):
        """
        Initialize filter with configurable parameters

        Args:
            brand_keywords: List of brand keywords to include
            excluded_tags: List of tags to exclude (if product has any of these tags, exclude it)
            included_tags: List of tags to include (if specified, product must have at least one of these tags)
            excluded_brand_keywords: List of brand keywords to exclude (if product name/brand contains any, exclude it)
            min_quarterly_sales: Minimum sales threshold (applies to either monthly or quarterly based on sales_period)
            sales_period: Sales period to use for filtering ("MONTHLY" or "QUARTERLY")
        """
        self.brand_keywords = brand_keywords or [
            "nike", "air jordan", "adidas", "yeezy", "new balance", "asics", "puma", "pop mart"
        ]
        self.excluded_tags = excluded_tags  # Can be None for no restriction
        self.included_tags = included_tags  # Can be None for no restriction
        self.excluded_brand_keywords = excluded_brand_keywords  # Can be None for no restriction
        self.min_quarterly_sales = min_quarterly_sales  # Now used as min_sales_threshold
        self.sales_period = sales_period.upper()
        self.filtered_products: List[FilteredProduct] = []
    
    def filter_products_with_newcop_exception(self, sales_records: List[SalesRecord]) -> List[FilteredProduct]:
        """
        Filter products with correct inclusion/exclusion logic:

        Step 1 - Include products that meet ANY of these conditions:
          a) Has INCLUDED_TAGS (if specified) - no sales threshold
          b) Has BRAND_KEYWORDS (if specified) AND meets sales threshold

        Step 2 - Exclude products from Step 1 that have:
          a) EXCLUDED_TAGS
          b) EXCLUDED_BRAND_KEYWORDS

        Step 3 - Sort by sales performance
        """
        all_products = []

        logger.info(f"Starting to filter {len(sales_records)} sales records with include/exclude logic")

        # Counters for debugging
        included_by_tags = 0
        included_by_brand = 0
        excluded_by_tags = 0
        excluded_by_brand = 0
        excluded_by_sales = 0

        for record in sales_records:
            # Step 1: Check if product should be INCLUDED
            # Include if: (has included_tags) OR (has brand_keywords AND meets sales threshold)

            has_included_tags = self._has_required_included_tags(record) if self.included_tags else False
            has_brand_keyword = self._has_required_brand_keyword(record) if self.brand_keywords else False
            meets_sales = self._meets_sales_threshold(record)

            # Inclusion logic: included_tags (no sales requirement) OR (brand_keywords AND sales threshold)
            should_include = False
            inclusion_reason = ""

            if has_included_tags:
                should_include = True
                inclusion_reason = "included_tags"
                included_by_tags += 1
            elif has_brand_keyword and meets_sales:
                should_include = True
                inclusion_reason = "brand_keywords_and_sales"
                included_by_brand += 1
            elif has_brand_keyword and not meets_sales:
                excluded_by_sales += 1

            if not should_include:
                continue

            # Step 2: Check if product should be EXCLUDED
            has_excluded_tags = self._has_excluded_tags(record)
            has_excluded_brand = self._has_excluded_brand_keyword(record)

            if has_excluded_tags:
                excluded_by_tags += 1
                logger.debug(f"Excluded by tags: {record.product_name}")
                continue

            if has_excluded_brand:
                excluded_by_brand += 1
                logger.debug(f"Excluded by brand keyword: {record.product_name}")
                continue

            # Product passed all filters, add it
            try:
                # Get the actual sales value used for filtering
                sales_value = self._get_sales_value(record)

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
                logger.debug(f"Included product ({inclusion_reason}): {record.product_name} ({self.sales_period} sales: {sales_value})")
            except Exception as e:
                logger.warning(f"Error creating filtered product for record {record.record_id}: {e}")

        logger.info(f"✅ Filtering completed: {len(all_products)} products passed all filters")
        logger.info(f"📊 Inclusion: {included_by_tags} by tags, {included_by_brand} by brand+sales")
        logger.info(f"📊 Exclusion: {excluded_by_tags} by excluded tags, {excluded_by_brand} by excluded brand keywords, {excluded_by_sales} by low sales")
        logger.info(f"After deduplication: {len(all_products)} unique products")

        # Step 4: Sort by sales performance (using the configured sales period)
        if self.sales_period == "MONTHLY":
            all_products.sort(
                key=lambda p: (-p.monthly_sales, -p.total_sales, p.product_name.lower())
            )
        else:  # QUARTERLY
            all_products.sort(
                key=lambda p: (-p.quarterly_sales, -p.total_sales, p.product_name.lower())
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
                logger.info(f"  {i}. {product.product_name} - {self.sales_period}: {sales_value}, Total: {product.total_sales}")

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
                    logger.debug(f"Included product: {record.product_name}")
                except Exception as e:
                    logger.warning(f"Error creating filtered product for record {record.record_id}: {e}")
                    continue
        
        # Sort products by sales performance (top sellers first)
        # Primary sort: quarterly_sales (descending)
        # Secondary sort: total_sales (descending) 
        # Tertiary sort: product_name (ascending) for consistency
        filtered_products.sort(
            key=lambda p: (-p.quarterly_sales, -p.total_sales, p.product_name.lower())
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
    
    def _has_required_brand_keyword(self, record: SalesRecord) -> bool:
        """Check if product name or brand contains required brand keywords"""
        product_name = (record.product_name or "").lower()
        brand = (record.brand or "").lower()

        for keyword in self.brand_keywords:
            if keyword.lower() in product_name or keyword.lower() in brand:
                return True

        return False

    def _has_excluded_brand_keyword(self, record: SalesRecord) -> bool:
        """Check if product name or brand contains any excluded brand keywords"""
        if not self.excluded_brand_keywords:
            return False

        product_name = (record.product_name or "").lower()
        brand = (record.brand or "").lower()

        for keyword in self.excluded_brand_keywords:
            if keyword.lower() in product_name or keyword.lower() in brand:
                logger.debug(f"Product '{record.product_name}' excluded due to brand keyword '{keyword}'")
                return True

        return False

    def _has_excluded_tags(self, record: SalesRecord) -> bool:
        """Check if product has any excluded tags"""
        if not record.tags or not self.excluded_tags:
            return False
        
        tags_lower = [tag.lower().strip() for tag in record.tags]
        
        for excluded_tag in self.excluded_tags:
            if excluded_tag.lower() in tags_lower:
                return True
        
        return False
    
    def _has_required_included_tags(self, record: SalesRecord) -> bool:
        """Check if product has required included tags (if specified)"""
        # If no included tags specified, no restriction
        if not self.included_tags:
            return True
        
        # If no tags on product but included tags required, exclude
        if not record.tags:
            return False
        
        tags_lower = [tag.lower().strip() for tag in record.tags]
        
        # Product must have at least one of the included tags
        for included_tag in self.included_tags:
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
        return sales_value >= self.min_quarterly_sales
    
    def get_filtering_summary(self) -> Dict[str, Any]:
        """Get summary of filtering criteria and results"""
        return {
            "brand_keywords": self.brand_keywords,
            "excluded_brand_keywords": self.excluded_brand_keywords,
            "excluded_tags": self.excluded_tags,
            "included_tags": self.included_tags,
            "min_quarterly_sales": self.min_quarterly_sales,
            "total_filtered_products": len(self.filtered_products)
        }