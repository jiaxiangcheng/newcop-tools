from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime


class VariantInfo(BaseModel):
    """Variant data from source product"""
    id: str
    title: str
    sku: Optional[str] = None
    price: str
    compare_at_price: Optional[str] = None
    inventory_quantity: int = 0
    inventory_item_id: Optional[str] = None
    selected_options: List[Dict[str, str]] = Field(default_factory=list)


class ProductInfo(BaseModel):
    """Source product data extracted from GraphQL"""
    id: str
    title: str
    handle: str
    vendor: Optional[str] = None
    product_type: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    status: str = "ACTIVE"
    options: List[Dict[str, Any]] = Field(default_factory=list)
    variants: List[VariantInfo] = Field(default_factory=list)
    image_urls: List[Dict[str, Optional[str]]] = Field(default_factory=list)
    seo_title: Optional[str] = None
    seo_description: Optional[str] = None


class DuplicateResult(BaseModel):
    """Result of a single product duplication"""
    source_product_id: str
    source_title: str
    new_product_id: Optional[str] = None
    new_title: Optional[str] = None
    success: bool = False
    error: Optional[str] = None


class DuplicationJobResult(BaseModel):
    """Overall result of the duplication job"""
    success: bool = False
    collection_id: str
    total_source_products: int = 0
    products_with_stock: int = 0
    products_duplicated: int = 0
    products_failed: int = 0
    execution_time_seconds: float = 0.0
    results: List[DuplicateResult] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.now)
