import logging
from typing import List, Dict, Any
from models import SalesRecord, FilteredProduct

logger = logging.getLogger(__name__)

class ProductFilter:
    """Filter products based on brand, tags, and sales criteria"""
    
    BRAND_KEYWORDS = [
        "nike", 
        "air jordan", 
        "adidas", 
        "yeezy", 
        "new balance", 
        "asics", 
        "puma",
        "pop mart"
    ]
    
    EXCLUDED_TAGS = ["retail"]
    MIN_QUARTERLY_SALES = 5.0
    
    def __init__(self):
        self.filtered_products: List[FilteredProduct] = []
    
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
        
        logger.info(f"Filtered and sorted {len(filtered_products)} qualifying products by sales performance")
        if filtered_products:
            logger.info("Top 3 products by sales:")
            for i, product in enumerate(filtered_products[:3], 1):
                logger.info(f"  {i}. {product.product_name} - Q Sales: {product.quarterly_sales}, Total: {product.total_sales}")
        
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
        
        # Check sales threshold
        if not self._meets_sales_threshold(record):
            return False
        
        return True
    
    def _has_required_brand_keyword(self, record: SalesRecord) -> bool:
        """Check if product name or brand contains required brand keywords"""
        product_name = (record.product_name or "").lower()
        brand = (record.brand or "").lower()
        
        for keyword in self.BRAND_KEYWORDS:
            if keyword in product_name or keyword in brand:
                return True
        
        return False
    
    def _has_excluded_tags(self, record: SalesRecord) -> bool:
        """Check if product has any excluded tags"""
        if not record.tags:
            return False
        
        tags_lower = [tag.lower().strip() for tag in record.tags]
        
        for excluded_tag in self.EXCLUDED_TAGS:
            if excluded_tag in tags_lower:
                return True
        
        return False
    
    def _meets_sales_threshold(self, record: SalesRecord) -> bool:
        """Check if product meets minimum sales threshold"""
        return record.quarterly_sales >= self.MIN_QUARTERLY_SALES
    
    def get_filtering_summary(self) -> Dict[str, Any]:
        """Get summary of filtering criteria and results"""
        return {
            "brand_keywords": self.BRAND_KEYWORDS,
            "excluded_tags": self.EXCLUDED_TAGS,
            "min_quarterly_sales": self.MIN_QUARTERLY_SALES,
            "total_filtered_products": len(self.filtered_products)
        }