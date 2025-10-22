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
    quarterly_sales: Optional[float] = 0.0
    total_sales: Optional[float] = 0.0
    
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
        
        return cls(
            id=record.get("id"),
            createdTime=record.get("createdTime"),
            product_name=fields.get("Product Title") or fields.get("product_name") or fields.get("nombre") or fields.get("name"),
            brand=fields.get("Vendor") or fields.get("brand") or fields.get("marca"),
            tags=tags,
            shopify_id=int(fields.get("∞ Shopify Id", 0) or 0) if fields.get("∞ Shopify Id") else None,
            quarterly_sales=float(fields.get("Ventas trimestre", 0) or fields.get("quarterly_sales", 0) or fields.get("trimestre_sales", 0) or 0),
            total_sales=float(fields.get("Total sale", 0) or fields.get("total_sales", 0) or fields.get("ventas_totales", 0) or 0),
            category=fields.get("category") or fields.get("categoria"),
            country=fields.get("country") or fields.get("pais"),
            date_range=fields.get("date_range") or fields.get("rango_fecha")
        )

class FilteredProduct(BaseModel):
    """Model for products that pass filtering criteria"""
    record_id: str
    product_name: str
    brand: str
    quarterly_sales: float
    total_sales: float = 0.0
    tags: List[str]
    shopify_id: Optional[int] = None
    sort_position: Optional[int] = None  # Position in collection for sorting
    
    def should_include_in_collection(self) -> bool:
        """Check if product should be included in the collection"""
        # Check if product has required brand keywords
        brand_keywords = ["nike", "air jordan", "adidas", "yeezy", "new balance", "asics", "puma"]
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
    GET_TOP_RESELL_PRODUCTS = "getTopResellProducts"
    # Future job types can be added here
    # SEASONAL_PRODUCTS = "seasonalProducts"
    # TRENDING_PRODUCTS = "trendingProducts"

class BaseJobSettings(BaseModel):
    """Base model for job settings from collection metafields"""
    jobType: JobType
    description: Optional[str] = Field(alias="Description", default=None)
    UPDATE_FREQUENCY_HOURS: int = Field(default=24, ge=1, le=168)  # 1 hour to 1 week
    MAX_AIRTABLE_RECORDS: int = Field(default=500, ge=10, le=5000)
    
    class Config:
        validate_by_name = True
        use_enum_values = True

class TopResellProductsJobSettings(BaseJobSettings):
    """Job settings for getTopResellProducts job type"""
    AIRTABLE_BASE_ID: str
    AIRTABLE_TABLE_ID: str
    AIRTABLE_VIEW_ID: str
    MIN_QUARTERLY_SALES: float = 5.0
    EXCLUDED_TAGS: Optional[List[str]] = None
    INCLUDED_TAGS: Optional[List[str]] = None
    BRAND_KEYWORDS: List[str] = ["nike", "air jordan", "adidas", "yeezy", "new balance", "asics", "puma", "pop mart"]
    
    @validator('jobType')
    def validate_job_type(cls, v):
        if v != JobType.GET_TOP_RESELL_PRODUCTS:
            raise ValueError(f"Invalid job type for TopResellProductsJobSettings: {v}")
        return v
    
    @validator('MIN_QUARTERLY_SALES', pre=True)
    def parse_min_quarterly_sales(cls, v):
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
        
        if job_type == JobType.GET_TOP_RESELL_PRODUCTS:
            return TopResellProductsJobSettings(**job_data)
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