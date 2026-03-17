from pydantic import BaseModel
from typing import Optional


class ProductInfo(BaseModel):
    product_id: str
    gid: str
    title: str
    product_type: str = ""
    status: str = ""


class TypeReplaceRecord(BaseModel):
    product_id: str
    title: str
    old_type: str
    new_type: str
    success: bool
    error: Optional[str] = None
