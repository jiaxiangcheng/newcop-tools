import logging
from typing import List, Dict, Any, Optional
from scripts.dynamic_collections.models import SalesRecord, FilteredProduct

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ProductFilter:
    """Filter products based on configurable brand, tags, and sales criteria"""
    
    def __init__(self, 
                 brand_keywords: List[str] = None,
                 excluded_tags: Optional[List[str]] = None,
                 included_tags: Optional[List[str]] = None,
                 min_quarterly_sales: float = 5.0):
        """
        Initialize filter with configurable parameters
        
        Args:
            brand_keywords: List of brand keywords to include
            excluded_tags: List of tags to exclude (if product has any of these tags, exclude it)
            included_tags: List of tags to include (if specified, product must have at least one of these tags)
            min_quarterly_sales: Minimum quarterly sales threshold
        """
        self.brand_keywords = brand_keywords or [
            "nike", "air jordan", "adidas", "yeezy", "new balance", "asics", "puma", "pop mart"
        ]
        self.excluded_tags = excluded_tags  # Can be None for no restriction
        self.included_tags = included_tags  # Can be None for no restriction
        self.min_quarterly_sales = min_quarterly_sales
        self.filtered_products: List[FilteredProduct] = []
    
    def filter_products_with_newcop_exception(self, sales_records: List[SalesRecord]) -> List[FilteredProduct]:
        """
        Filter products with newcop tag exception logic:
        1. First get all products with included_tags (e.g., 'newcop') - no sales threshold required
        2. Then get products meeting regular criteria (sales threshold + other filters)
        3. Merge and deduplicate, then sort by sales performance
        """
        all_products = []
        newcop_products = []
        regular_products = []
        
        logger.info(f"Starting to filter {len(sales_records)} sales records with newcop exception logic")
        
        # Step 1: Get all products with included_tags (newcop exception - no sales requirement)
        if self.included_tags:
            logger.info(f"Step 1: Finding products with included tags {self.included_tags} (no sales threshold)")
            for record in sales_records:
                if self._has_required_included_tags(record) and not self._has_excluded_tags(record):
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
                        newcop_products.append(filtered_product)
                        logger.debug(f"Added newcop product: {record.product_name} (sales: {record.quarterly_sales})")
                    except Exception as e:
                        logger.warning(f"Error creating newcop product for record {record.record_id}: {e}")
            
            logger.info(f"Found {len(newcop_products)} products with included tags")
        
        # Step 2: Get products meeting regular criteria (sales + brand + no excluded tags, but ignore included_tags requirement)
        logger.info(f"Step 2: Finding products meeting sales threshold {self.min_quarterly_sales}")
        for record in sales_records:
            if (self._has_required_brand_keyword(record) and 
                not self._has_excluded_tags(record) and 
                self._meets_sales_threshold(record)):
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
                    regular_products.append(filtered_product)
                    logger.debug(f"Added regular product: {record.product_name} (sales: {record.quarterly_sales})")
                except Exception as e:
                    logger.warning(f"Error creating regular product for record {record.record_id}: {e}")
        
        logger.info(f"Found {len(regular_products)} products meeting regular criteria")
        
        # Step 3: Merge and deduplicate by record_id
        seen_record_ids = set()
        for product in newcop_products + regular_products:
            if product.record_id not in seen_record_ids:
                all_products.append(product)
                seen_record_ids.add(product.record_id)
        
        logger.info(f"After deduplication: {len(all_products)} unique products")
        
        # Step 4: Sort by sales performance
        all_products.sort(
            key=lambda p: (-p.quarterly_sales, -p.total_sales, p.product_name.lower())
        )
        
        # Assign sort positions (1-based for Shopify)
        for i, product in enumerate(all_products, 1):
            product.sort_position = i
        
        logger.info(f"✅ Filtered and sorted {len(all_products)} qualifying products by sales performance")
        
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
    
    def _meets_sales_threshold(self, record: SalesRecord) -> bool:
        """Check if product meets minimum sales threshold"""
        return record.quarterly_sales >= self.min_quarterly_sales
    
    def get_filtering_summary(self) -> Dict[str, Any]:
        """Get summary of filtering criteria and results"""
        return {
            "brand_keywords": self.brand_keywords,
            "excluded_tags": self.excluded_tags,
            "included_tags": self.included_tags,
            "min_quarterly_sales": self.min_quarterly_sales,
            "total_filtered_products": len(self.filtered_products)
        }