import os
import re
from pathlib import Path
from typing import Optional

def get_domain_cache_folder(shopify_domain: Optional[str] = None) -> Path:
    """
    Get domain-specific cache folder path based on SHOPIFY_SHOP_DOMAIN

    Args:
        shopify_domain: Optional domain override, otherwise uses environment variable

    Returns:
        Path: Domain-specific cache folder path
    """
    if not shopify_domain:
        shopify_domain = os.getenv('SHOPIFY_SHOP_DOMAIN')

    if not shopify_domain:
        raise ValueError("SHOPIFY_SHOP_DOMAIN environment variable not set")

    # Clean domain name for folder name (remove .myshopify.com and other special chars)
    # Convert to lowercase and replace special characters with underscores
    clean_domain = re.sub(r'\.myshopify\.com$', '', shopify_domain.lower())
    clean_domain = re.sub(r'[^a-z0-9]', '_', clean_domain)

    # Get project root and create domain-specific cache folder
    project_root = Path(__file__).parent.parent
    cache_folder = project_root / "data" / clean_domain

    # Create folder if it doesn't exist
    cache_folder.mkdir(parents=True, exist_ok=True)

    return cache_folder

def get_inventory_cache_path(shopify_domain: Optional[str] = None) -> Path:
    """Get domain-specific inventory cache file path"""
    cache_folder = get_domain_cache_folder(shopify_domain)
    return cache_folder / "inventory_cache.json"

def get_customer_marketing_cache_path(shopify_domain: Optional[str] = None) -> Path:
    """Get domain-specific customer marketing cache file path"""
    cache_folder = get_domain_cache_folder(shopify_domain)
    return cache_folder / "customer_marketing_cache.json"