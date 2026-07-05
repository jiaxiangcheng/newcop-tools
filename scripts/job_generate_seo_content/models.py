from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field


class ProductSeoStatus(BaseModel):
    product_id: str
    product_id_int: int
    title: str
    has_description: bool
    has_meta_description: bool
    current_description: Optional[str] = None
    current_meta_description: Optional[str] = None

    @property
    def needs_description(self) -> bool:
        return not self.has_description

    @property
    def needs_meta_description(self) -> bool:
        return not self.has_meta_description

    @property
    def needs_any_update(self) -> bool:
        return self.needs_description or self.needs_meta_description


class SeoUpdateResult(BaseModel):
    product_id: str
    product_id_int: int
    title: str
    field: str
    content: str
    success: bool
    error: Optional[str] = None


class SeoRunSummary(BaseModel):
    total_products: int
    products_needing_update: int
    successful_updates: int
    failed_updates: int
    dry_run: bool
    execution_time_seconds: float
    timestamp: datetime
    csv_log_path: str
    update_results: List[SeoUpdateResult] = Field(default_factory=list)
