from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum

class AirtableRecord(BaseModel):
    """Base model for Airtable records"""
    id: str
    createdTime: datetime
    fields: Dict[str, Any]

class SalesRecord(BaseModel):
    """Model for sales data from Airtable"""
    record_id: str = Field(alias="id")
    created_time: datetime = Field(alias="createdTime")
    
    # Product information
    product_name: Optional[str] = None
    brand: Optional[str] = None
    tags: Optional[List[str]] = None
    shopify_id: Optional[int] = None
    
    # Sales data
    weekly_sales: Optional[float] = 0.0
    quarterly_sales: Optional[float] = 0.0
    monthly_sales: Optional[float] = 0.0
    total_sales: Optional[float] = 0.0

    # Stock data
    total_stock: Optional[float] = 0.0

    # Additional fields that might be present
    category: Optional[str] = None
    country: Optional[str] = None
    date_range: Optional[str] = None
    
    @classmethod
    def from_airtable_record(cls, record: Dict[str, Any]) -> "SalesRecord":
        """Create SalesRecord from Airtable record"""
        fields = record.get("fields", {})

        # Extract tags (handle both string and list formats)
        tags = fields.get("Tags", fields.get("tags", []))
        if isinstance(tags, str):
            tags = [tag.strip() for tag in tags.split(",")]
        elif not isinstance(tags, list):
            tags = []

        # Extract shopify_id - handle both product IDs and variant IDs
        shopify_id = None
        raw_id = fields.get("∞ Shopify Id")
        if raw_id:
            try:
                # If it's a string with "-" (variant ID like "16764987244885-1"), extract product ID
                if isinstance(raw_id, str) and "-" in raw_id:
                    shopify_id = int(raw_id.split("-")[0])
                else:
                    shopify_id = int(raw_id)
            except (ValueError, TypeError):
                shopify_id = None

        return cls(
            id=record.get("id"),
            createdTime=record.get("createdTime"),
            product_name=fields.get("Product Title") or fields.get("product_name") or fields.get("nombre") or fields.get("name"),
            brand=fields.get("Vendor") or fields.get("brand") or fields.get("marca"),
            tags=tags,
            shopify_id=shopify_id,
            weekly_sales=float(fields.get("Ventas Semanales", 0) or fields.get("weekly_sales", 0) or 0),
            quarterly_sales=float(fields.get("Ventas trimestre", 0) or fields.get("quarterly_sales", 0) or fields.get("trimestre_sales", 0) or 0),
            monthly_sales=float(fields.get("Monthly total", 0) or fields.get("monthly_sales", 0) or fields.get("ventas_mensuales", 0) or 0),
            total_sales=float(fields.get("Total sale", 0) or fields.get("total_sales", 0) or fields.get("ventas_totales", 0) or 0),
            total_stock=float(fields.get("Total Stock", 0) or fields.get("total_stock", 0) or fields.get("Stock Total", 0) or fields.get("stock", 0) or 0),
            category=fields.get("category") or fields.get("categoria"),
            country=fields.get("country") or fields.get("pais"),
            date_range=fields.get("date_range") or fields.get("rango_fecha")
        )

class FilteredProduct(BaseModel):
    """Model for products that pass filtering criteria"""
    record_id: str
    product_name: str
    brand: str
    weekly_sales: float = 0.0
    quarterly_sales: float
    monthly_sales: float = 0.0
    total_sales: float = 0.0
    tags: List[str]
    shopify_id: Optional[int] = None
    sort_position: Optional[int] = None  # Position in collection for sorting
    sales_period: Optional[str] = None  # Track which period was used for filtering
    
    def should_include_in_collection(self) -> bool:
        """Check if product should be included in the collection"""
        # Check if product has required brand keywords
        brand_keywords = ["nike", "air jordan", "adidas", "yeezy", "new balance", "asics", "puma", "ugg"]
        product_name_lower = self.product_name.lower() if self.product_name else ""
        brand_lower = self.brand.lower() if self.brand else ""
        
        has_brand_keyword = any(
            keyword in product_name_lower or keyword in brand_lower 
            for keyword in brand_keywords
        )
        
        # Check if tags don't contain "retail"
        tags_lower = [tag.lower() for tag in self.tags]
        has_retail_tag = "retail" in tags_lower
        
        # Check sales threshold
        meets_sales_threshold = self.quarterly_sales >= 5.0
        
        return has_brand_keyword and not has_retail_tag and meets_sales_threshold

class ShopifyProduct(BaseModel):
    """Model for Shopify product data"""
    id: Optional[int] = None
    title: str
    handle: Optional[str] = None
    vendor: Optional[str] = None
    tags: Optional[str] = None
    
class CollectionUpdateRequest(BaseModel):
    """Model for Shopify collection update request"""
    collection_id: str
    product_ids: List[int]

class JobType(str, Enum):
    """Supported job types"""
    GET_PRODUCTS_BY_SALES = "getProductsBySales"
    GET_PRODUCTS_WITH_TOTAL_STOCK = "getProductsWithAtLeastTotalStock"
    # Future job types can be added here
    # SEASONAL_PRODUCTS = "seasonalProducts"
    # TRENDING_PRODUCTS = "trendingProducts"

class SalesPeriod(str, Enum):
    """Sales period types for filtering"""
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"
    QUARTERLY = "QUARTERLY"

class BaseJobSettings(BaseModel):
    """Base model for job settings from collection metafields"""
    jobType: JobType
    description: Optional[str] = Field(alias="Description", default=None)
    UPDATE_FREQUENCY_HOURS: int = Field(default=24, ge=1, le=168)  # 1 hour to 1 week
    MAX_AIRTABLE_RECORDS: int = Field(default=500, ge=10, le=5000)
    
    class Config:
        validate_by_name = True
        use_enum_values = True

class ProductsBySalesJobSettings(BaseJobSettings):
    """Job settings for getProductsBySales job type"""
    AIRTABLE_BASE_ID: str
    AIRTABLE_TABLE_ID: str
    AIRTABLE_VIEW_ID: str
    MIN_SALES_THRESHOLD: float = 5.0
    SALES_PERIOD: SalesPeriod = SalesPeriod.QUARTERLY  # Default to QUARTERLY for backward compatibility
    EXCLUDED_TAGS: Optional[List[str]] = None
    INCLUDED_TAGS: Optional[List[str]] = None
    BRAND_KEYWORDS: List[str] = ["nike", "air jordan", "adidas", "yeezy", "new balance", "asics", "puma", "pop mart"]
    EXCLUDED_BRAND_KEYWORDS: Optional[List[str]] = None

    @validator('jobType')
    def validate_job_type(cls, v):
        if v != JobType.GET_PRODUCTS_BY_SALES:
            raise ValueError(f"Invalid job type for ProductsBySalesJobSettings: {v}")
        return v

    @validator('MIN_SALES_THRESHOLD', pre=True)
    def parse_min_sales_threshold(cls, v):
        """Convert string to float if needed"""
        if isinstance(v, str):
            return float(v)
        return v

    @validator('SALES_PERIOD', pre=True)
    def parse_sales_period(cls, v):
        """Convert string to SalesPeriod enum if needed"""
        if isinstance(v, str):
            return SalesPeriod(v.upper())
        return v

class GetProductsWithTotalStockJobSettings(BaseJobSettings):
    """Job settings for getProductsWithAtLeastTotalStock job type"""
    AIRTABLE_BASE_ID: str
    AIRTABLE_TABLE_ID: str
    AIRTABLE_VIEW_ID: str
    MIN_TOTAL_QUANTITY: float = 4.0
    EXCLUDED_TAGS: Optional[List[str]] = None
    INCLUDED_TAGS: Optional[List[str]] = None
    BRAND_KEYWORDS: Optional[List[str]] = None
    EXCLUDED_BRAND_KEYWORDS: Optional[List[str]] = None

    @validator('jobType')
    def validate_job_type(cls, v):
        if v != JobType.GET_PRODUCTS_WITH_TOTAL_STOCK:
            raise ValueError(f"Invalid job type for GetProductsWithTotalStockJobSettings: {v}")
        return v

    @validator('MIN_TOTAL_QUANTITY', pre=True)
    def parse_min_total_quantity(cls, v):
        """Convert string to float if needed"""
        if isinstance(v, str):
            return float(v)
        return v

class CollectionWithJobSettings(BaseModel):
    """Model representing a collection with its job settings"""
    collection_id: str
    collection_title: str
    collection_handle: Optional[str] = None
    job_settings: BaseJobSettings
    
    @staticmethod
    def create_job_settings_from_dict(job_data: Dict[str, Any]) -> BaseJobSettings:
        """Factory method to create appropriate job settings based on jobType"""
        job_type = job_data.get("jobType")

        if job_type == JobType.GET_PRODUCTS_BY_SALES:
            return ProductsBySalesJobSettings(**job_data)
        elif job_type == JobType.GET_PRODUCTS_WITH_TOTAL_STOCK:
            return GetProductsWithTotalStockJobSettings(**job_data)
        else:
            raise ValueError(f"Unknown job type: {job_type}")
    
    @classmethod
    def from_shopify_collection_and_job_data(cls, collection: Dict[str, Any], job_data: Dict[str, Any]) -> "CollectionWithJobSettings":
        """Create from Shopify collection data and job settings data"""
        job_settings = cls.create_job_settings_from_dict(job_data)
        
        return cls(
            collection_id=str(collection.get("collection_id") or collection.get("id")),
            collection_title=collection.get("title", ""),
            collection_handle=collection.get("handle"),
            job_settings=job_settings
        )