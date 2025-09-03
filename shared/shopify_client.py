import requests
import logging
import re
import time
import json
from typing import List, Dict, Any, Optional
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.dynamic_collections.models import FilteredProduct, ShopifyProduct

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ShopifyClient:
    """Client for interacting with Shopify Admin API"""
    
    def __init__(self, admin_token: str, shop_domain: str):
        self.admin_token = admin_token
        self.shop_domain = shop_domain.replace('.myshopify.com', '')
        self.base_url = f"https://{self.shop_domain}.myshopify.com/admin/api/2025-01"
        self.headers = {
            "X-Shopify-Access-Token": admin_token,
            "Content-Type": "application/json"
        }
    
    def search_products_by_name(self, product_names: List[str]) -> List[ShopifyProduct]:
        """
        Search for products in Shopify by name
        """
        found_products = []
        
        for product_name in product_names:
            try:
                products = self._search_single_product(product_name)
                found_products.extend(products)
            except Exception as e:
                logger.warning(f"Error searching for product '{product_name}': {e}")
                continue
        
        logger.info(f"Found {len(found_products)} Shopify products")
        return found_products
    
    def _search_single_product(self, product_name: str) -> List[ShopifyProduct]:
        """Search for a single product by name"""
        url = f"{self.base_url}/products.json"
        params = {
            "title": product_name,
            "limit": 10
        }
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            products = []
            
            for product_data in data.get("products", []):
                product = ShopifyProduct(
                    id=product_data.get("id"),
                    title=product_data.get("title"),
                    handle=product_data.get("handle"),
                    vendor=product_data.get("vendor"),
                    tags=product_data.get("tags")
                )
                products.append(product)
            
            return products
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error searching Shopify products: {e}")
            raise
    
    def get_collection_products(self, collection_id: str) -> List[ShopifyProduct]:
        """Get current products in a collection with pagination support"""
        url = f"{self.base_url}/collections/{collection_id}/products.json"
        
        all_products = []
        page_info = None
        page_size = 250  # Maximum allowed by Shopify API
        
        try:
            while True:
                # Set up parameters for this request
                params = {"limit": page_size}
                if page_info:
                    params["page_info"] = page_info
                
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                
                data = response.json()
                products_batch = data.get("products", [])
                
                # Convert to ShopifyProduct objects
                for product_data in products_batch:
                    product = ShopifyProduct(
                        id=product_data.get("id"),
                        title=product_data.get("title"),
                        handle=product_data.get("handle"),
                        vendor=product_data.get("vendor"),
                        tags=product_data.get("tags")
                    )
                    all_products.append(product)
                
                if len(all_products) % 50 == 0 or products_batch:  # Log every 50 products or at end
                    logger.debug(f"Fetched {len(products_batch)} products (total: {len(all_products)})")
                
                # Check if there are more pages
                # Shopify uses Link header for pagination
                link_header = response.headers.get("Link", "")
                if "rel=\"next\"" in link_header:
                    # Extract page_info from Link header
                    import re
                    next_link_match = re.search(r'<[^>]*[?&]page_info=([^&>]*)>[^>]*rel="next"', link_header)
                    if next_link_match:
                        page_info = next_link_match.group(1)
                    else:
                        break
                else:
                    break  # No more pages
            
            logger.info(f"Found {len(all_products)} products in collection {collection_id} (with pagination)")
            return all_products
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching collection products: {e}")
            raise
    
    

    def add_products_to_collection(self, collection_id: str, product_ids: List[int]) -> bool:
        """
        Add products to a collection
        """
        if not product_ids:
            logger.info("No products to add to collection")
            return True
        
        success_count = 0
        
        for product_id in product_ids:
            try:
                if self._add_single_product_to_collection(collection_id, product_id):
                    success_count += 1
            except Exception as e:
                logger.warning(f"Failed to add product {product_id} to collection: {e}")
                continue
        
        logger.info(f"Successfully added {success_count}/{len(product_ids)} products to collection")
        return success_count == len(product_ids)
    
    def _add_single_product_to_collection(self, collection_id: str, product_id: int) -> bool:
        """Add a single product to collection"""
        url = f"{self.base_url}/collects.json"
        
        payload = {
            "collect": {
                "product_id": product_id,
                "collection_id": int(collection_id)
            }
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            return True
            
        except requests.exceptions.RequestException as e:
            # Handle specific error cases
            if hasattr(e, 'response') and e.response is not None:
                status_code = e.response.status_code
                
                if status_code == 422:
                    # Product might already be in collection
                    logger.debug(f"Product {product_id} may already be in collection (422)")
                    return True
                elif status_code == 429:
                    # Rate limit exceeded, wait and retry
                    logger.warning(f"Rate limit exceeded adding product {product_id}, retrying after delay")
                    time.sleep(1)
                    return self._add_single_product_to_collection(collection_id, product_id)
                else:
                    # Log detailed error information
                    try:
                        error_data = e.response.json()
                        logger.error(f"Error adding product {product_id} to collection: {status_code} - {error_data}")
                    except:
                        logger.error(f"Error adding product {product_id} to collection: {status_code} - {e.response.text}")
            else:
                logger.error(f"Error adding product {product_id} to collection: {e}")
            return False
    
    def remove_products_from_collection(self, collection_id: str, product_ids: List[int]) -> bool:
        """
        Remove products from a collection
        """
        if not product_ids:
            logger.info("No products to remove from collection")
            return True
        
        # First get all collects for the collection
        collects = self._get_collection_collects(collection_id)
        
        success_count = 0
        
        for product_id in product_ids:
            collect_id = None
            for collect in collects:
                if collect.get("product_id") == product_id:
                    collect_id = collect.get("id")
                    break
            
            if collect_id:
                try:
                    if self._remove_collect(collect_id):
                        success_count += 1
                except Exception as e:
                    logger.warning(f"Failed to remove product {product_id} from collection: {e}")
                    continue
        
        logger.info(f"Successfully removed {success_count}/{len(product_ids)} products from collection")
        return success_count == len(product_ids)
    
    def _get_collection_collects(self, collection_id: str) -> List[Dict[str, Any]]:
        """Get all collects for a collection with pagination support"""
        url = f"{self.base_url}/collects.json"
        
        all_collects = []
        page_info = None
        page_size = 250  # Maximum allowed by Shopify API
        
        try:
            while True:
                # Set up parameters for this request
                params = {
                    "collection_id": collection_id,
                    "limit": page_size
                }
                if page_info:
                    params["page_info"] = page_info
                
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                
                data = response.json()
                collects_batch = data.get("collects", [])
                all_collects.extend(collects_batch)
                
                logger.debug(f"Fetched {len(collects_batch)} collects (total: {len(all_collects)})")
                
                # Check if there are more pages
                # Shopify uses Link header for pagination
                link_header = response.headers.get("Link", "")
                if "rel=\"next\"" in link_header:
                    # Extract page_info from Link header
                    import re
                    next_link_match = re.search(r'<[^>]*[?&]page_info=([^&>]*)>[^>]*rel="next"', link_header)
                    if next_link_match:
                        page_info = next_link_match.group(1)
                    else:
                        break
                else:
                    break  # No more pages
            
            logger.info(f"Found {len(all_collects)} collects for collection {collection_id} (with pagination)")
            return all_collects
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching collection collects: {e}")
            raise
    
    def _remove_collect(self, collect_id: int) -> bool:
        """Remove a collect by ID"""
        url = f"{self.base_url}/collects/{collect_id}.json"
        
        try:
            response = requests.delete(url, headers=self.headers)
            response.raise_for_status()
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error removing collect: {e}")
            raise
    
    def _remove_single_collect(self, collect_id: str) -> bool:
        """Remove a single collect by ID"""
        url = f"{self.base_url}/collects/{collect_id}.json"
        
        try:
            response = requests.delete(url, headers=self.headers)
            response.raise_for_status()
            return True
            
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response is not None:
                status_code = e.response.status_code
                if status_code == 429:
                    # Rate limit exceeded, wait and retry once
                    logger.warning(f"Rate limit exceeded removing collect {collect_id}, retrying after delay")
                    time.sleep(1)
                    try:
                        response = requests.delete(url, headers=self.headers)
                        response.raise_for_status()
                        return True
                    except:
                        return False
                else:
                    logger.warning(f"Error removing collect {collect_id}: {status_code}")
                    return False
            else:
                logger.warning(f"Error removing collect {collect_id}: {e}")
                return False
    
    def remove_all_products_from_collection(self, collection_id: str, product_ids: List[int]) -> bool:
        """
        Fast batch removal of all products from collection using parallel processing.
        """
        if not product_ids:
            logger.info("No products to remove")
            return True
        
        logger.info(f"Starting batch removal of {len(product_ids)} products from collection {collection_id}")
        
        # Get all collects for this collection
        try:
            collects = self._get_collection_collects(collection_id)
            collect_ids_to_remove = [str(collect.get('id')) for collect in collects if collect.get('product_id') in product_ids]
            
            if not collect_ids_to_remove:
                logger.warning("No matching collects found to remove")
                return True
                
            logger.info(f"Found {len(collect_ids_to_remove)} collects to remove")
            
            # Remove collects in batches
            batch_size = 50  # Increased batch size for faster processing
            success_count = 0
            
            for i in range(0, len(collect_ids_to_remove), batch_size):
                batch = collect_ids_to_remove[i:i + batch_size]
                batch_num = i//batch_size + 1
                total_batches = (len(collect_ids_to_remove) + batch_size - 1) // batch_size
                progress_pct = i / len(collect_ids_to_remove) * 100
                
                logger.info(f"🗑️  Removing batch {batch_num}/{total_batches}: {len(batch)} products ({progress_pct:.1f}%)")
                print(f"🗑️  Removing batch {batch_num}/{total_batches}: {len(batch)} products ({progress_pct:.1f}%)")
                
                for collect_id in batch:
                    try:
                        self._remove_single_collect(collect_id)
                        success_count += 1
                    except Exception as e:
                        logger.warning(f"Failed to remove collect {collect_id}: {e}")
                        continue
                
                # Small delay between batches to respect rate limits
                if i + batch_size < len(collect_ids_to_remove):
                    time.sleep(0.1)
            
            success_rate = success_count / len(collect_ids_to_remove)
            logger.info(f"Batch removal completed: {success_count}/{len(collect_ids_to_remove)} collects removed ({success_rate:.1%})")
            
            return success_rate >= 0.9  # Consider successful if 90%+ removed
            
        except Exception as e:
            logger.error(f"Failed to remove products from collection: {e}")
            return False
    
    def update_collection_with_filtered_products(self, collection_id: str, filtered_products: List[FilteredProduct]) -> Dict[str, Any]:
        """
        Simple collection update strategy: Clear all products then add new ones in correct order.
        This approach is faster and avoids position update errors.
        """
        logger.info(f"Starting collection replacement for {collection_id} with {len(filtered_products)} filtered products")
        print(f"🔄 Starting collection replacement for {collection_id} with {len(filtered_products)} filtered products")
        
        # Extract valid Shopify IDs
        new_product_ids = []
        products_without_id = []
        
        for product in filtered_products:
            if product.shopify_id and product.shopify_id > 0:
                new_product_ids.append(product.shopify_id)
            else:
                products_without_id.append(product.product_name)
                logger.warning(f"❌ Product '{product.product_name}' has no valid Shopify ID (current ID: {product.shopify_id}), skipping")
        
        if products_without_id:
            logger.warning(f"⚠️  {len(products_without_id)} products skipped due to missing Shopify IDs:")
            for i, product_name in enumerate(products_without_id, 1):
                logger.warning(f"  {i}. {product_name}")
        
        if not new_product_ids:
            logger.warning("No valid Shopify product IDs found")
            return {
                "filtered_products_count": len(filtered_products),
                "valid_shopify_ids": 0,
                "products_without_id": len(products_without_id),
                "products_added": 0,
                "products_removed": 0,
                "success": False
            }
        
        # Step 1: Get current products and remove ALL of them
        logger.info("Getting current products in collection...")
        try:
            current_products = self.get_collection_products(collection_id)
            current_product_ids = [p.id for p in current_products]
            logger.info(f"Found {len(current_product_ids)} current products in collection")
        except Exception as e:
            logger.error(f"Failed to get current collection products: {e}")
            current_product_ids = []
        
        removed_count = 0
        if current_product_ids:
            logger.info(f"Removing ALL {len(current_product_ids)} products from collection...")
            print(f"🗑️  Removing ALL {len(current_product_ids)} products from collection...")
            try:
                remove_success = self.remove_all_products_from_collection(collection_id, current_product_ids)
                if remove_success:
                    removed_count = len(current_product_ids)
                    logger.info(f"Successfully removed {removed_count} products")
                else:
                    logger.warning("Some products failed to be removed")
            except Exception as e:
                logger.error(f"Failed to remove products: {e}")
        
        # Step 2: Add new products in correct order (no position updates needed)
        logger.info(f"Adding {len(new_product_ids)} products to collection in sales order...")
        print(f"➕ Adding {len(new_product_ids)} products to collection in sales order...")
        try:
            add_result = self.add_products_to_collection_in_order(collection_id, new_product_ids)
            add_success = add_result.get("success", False)
            added_count = add_result.get("added_count", 0)
            failed_count = add_result.get("failed_count", 0)
            failed_products = add_result.get("failed_products", [])
            
            if add_success:
                logger.info(f"Successfully added {added_count} products in correct order")
            else:
                logger.warning(f"Addition partially failed: {added_count} added, {failed_count} failed")
                if failed_products:
                    logger.warning(f"Failed product IDs: {failed_products}")
        except Exception as e:
            logger.error(f"Failed to add products: {e}")
            add_success = False
            added_count = 0
            failed_count = len(new_product_ids)
            failed_products = new_product_ids
        
        operation_success = add_success and (not current_product_ids or removed_count == len(current_product_ids))
        
        result = {
            "filtered_products_count": len(filtered_products),
            "valid_shopify_ids": len(new_product_ids),
            "products_without_id": len(products_without_id),
            "current_products_count": len(current_product_ids),
            "products_added": added_count,
            "products_removed": removed_count,
            "final_collection_size": added_count,
            "failed_to_add": failed_count,
            "failed_product_ids": failed_products if 'failed_products' in locals() else [],
            "top_seller": filtered_products[0].product_name if filtered_products else "N/A",
            "success": operation_success,
            "strategy": "clear_and_rebuild"
        }
        
        logger.info(f"Collection replacement completed: {result}")
        return result
    
    def get_all_collections(self) -> List[Dict[str, Any]]:
        """Get all collections from Shopify using collection_listings endpoint with pagination support"""
        url = f"{self.base_url}/collection_listings.json"
        
        all_collections = []
        page_info = None
        page_size = 250  # Maximum allowed by Shopify API
        
        try:
            while True:
                # Set up parameters for this request
                params = {"limit": page_size}
                if page_info:
                    params["page_info"] = page_info
                
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                
                data = response.json()
                collections_batch = data.get("collection_listings", [])
                all_collections.extend(collections_batch)
                
                logger.debug(f"Fetched {len(collections_batch)} collections (total: {len(all_collections)})")
                
                # Check if there are more pages
                # Shopify uses Link header for pagination
                link_header = response.headers.get("Link", "")
                if "rel=\"next\"" in link_header:
                    # Extract page_info from Link header
                    next_link_match = re.search(r'<[^>]*[?&]page_info=([^&>]*)>[^>]*rel="next"', link_header)
                    if next_link_match:
                        page_info = next_link_match.group(1)
                    else:
                        break
                else:
                    break  # No more pages
            
            logger.info(f"Found {len(all_collections)} collections total")
            return all_collections
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching collections: {e}")
            raise
    
    def get_collection_metafields(self, collection_id: str) -> List[Dict[str, Any]]:
        """Get metafields for a specific collection"""
        url = f"{self.base_url}/collections/{collection_id}/metafields.json"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            data = response.json()
            metafields = data.get("metafields", [])
            
            logger.debug(f"Found {len(metafields)} metafields for collection {collection_id}")
            return metafields
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching metafields for collection {collection_id}: {e}")
            raise
    
    def get_collection_job_settings(self, collection_id: str) -> Optional[Dict[str, Any]]:
        """Get job_settings from collection's custom.job_settings metafield"""
        try:
            metafields = self.get_collection_metafields(collection_id)
            
            # Look for custom.job_settings metafield
            for metafield in metafields:
                namespace = metafield.get("namespace")
                key = metafield.get("key")
                
                if namespace == "custom" and key == "job_settings":
                    value = metafield.get("value")
                    if value:
                        try:
                            # Parse JSON value
                            job_settings = json.loads(value) if isinstance(value, str) else value
                            logger.info(f"Found job_settings for collection {collection_id}: {job_settings}")
                            return job_settings
                        except (json.JSONDecodeError, TypeError) as e:
                            logger.warning(f"Failed to parse job_settings JSON for collection {collection_id}: {e}")
                            return None
            
            logger.debug(f"No job_settings metafield found for collection {collection_id}")
            return None
            
        except Exception as e:
            logger.error(f"Error getting job_settings for collection {collection_id}: {e}")
            return None
    
    def get_collections_with_job_settings(self) -> List[Dict[str, Any]]:
        """Get all collections that have job_settings metafield configured"""
        try:
            all_collections = self.get_all_collections()
            collections_with_jobs = []
            
            # First, filter collections that start with "Auto -" for efficiency
            auto_collections = [
                collection for collection in all_collections 
                if collection.get("title", "").startswith("Auto -")
            ]
            
            logger.info(f"Found {len(auto_collections)} collections starting with 'Auto -' out of {len(all_collections)} total collections")
            
            # Check metafields only for "Auto -" collections (never check all collections)
            collections_to_check = auto_collections
            
            for collection in collections_to_check:
                # Handle both collection_listings and regular collections API response formats
                collection_id = str(collection.get("collection_id") or collection.get("id"))
                collection_title = collection.get("title", "Unknown")
                
                logger.debug(f"Checking job_settings for collection: {collection_title}")
                job_settings = self.get_collection_job_settings(collection_id)
                
                if job_settings:
                    collections_with_jobs.append({
                        "collection": collection,
                        "job_settings": job_settings
                    })
                    logger.info(f"✅ Found job_settings for collection: {collection_title}")
            
            logger.info(f"Found {len(collections_with_jobs)} collections with job_settings")
            return collections_with_jobs
            
        except Exception as e:
            logger.error(f"Error getting collections with job_settings: {e}")
            raise
    
    def add_products_to_collection_in_order(self, collection_id: str, product_ids: List[int]) -> Dict[str, Any]:
        """
        Fast batch addition of products to collection in specified order.
        Products are added sequentially to maintain order.
        Returns detailed results including failed product IDs.
        """
        if not product_ids:
            logger.info("No products to add")
            return {"success": True, "added_count": 0, "failed_count": 0, "failed_products": []}
        
        logger.info(f"Starting batch addition of {len(product_ids)} products to collection {collection_id}")
        
        try:
            batch_size = 50  # Increased batch size for faster processing
            success_count = 0
            failed_products = []
            
            for i, product_id in enumerate(product_ids):
                try:
                    # Use position i+1 (1-based) to maintain order
                    success = self._add_single_product_to_collection_with_position(collection_id, product_id, i + 1)
                    if success:
                        success_count += 1
                        # Progress update every 10 products for better visibility
                        if (i + 1) % 10 == 0:
                            progress_pct = (i + 1) / len(product_ids) * 100
                            logger.info(f"➕ Added {i + 1}/{len(product_ids)} products ({progress_pct:.1f}%)")
                            print(f"➕ Added {i + 1}/{len(product_ids)} products ({progress_pct:.1f}%)")
                    else:
                        failed_products.append(product_id)
                        logger.warning(f"❌ Failed to add product {product_id} to collection")
                except Exception as e:
                    failed_products.append(product_id)
                    logger.warning(f"❌ Failed to add product {product_id}: {e}")
                    continue
                
                # Add small delay every batch_size products to respect rate limits
                if (i + 1) % batch_size == 0 and i + 1 < len(product_ids):
                    time.sleep(0.05)  # Reduced delay since batch size is larger
            
            success_rate = success_count / len(product_ids)
            failed_count = len(failed_products)
            
            logger.info(f"Batch addition completed: {success_count}/{len(product_ids)} products added ({success_rate:.1%})")
            
            if failed_count > 0:
                logger.warning(f"⚠️  {failed_count} products failed to be added to collection:")
                for product_id in failed_products:
                    logger.warning(f"  - Product ID: {product_id}")
            
            return {
                "success": success_rate >= 0.95,  # Require 95% success rate
                "added_count": success_count,
                "failed_count": failed_count,
                "failed_products": failed_products,
                "success_rate": success_rate
            }
            
        except Exception as e:
            logger.error(f"Failed to add products to collection: {e}")
            return {
                "success": False,
                "added_count": 0,
                "failed_count": len(product_ids),
                "failed_products": product_ids,
                "error": str(e)
            }
    
    def _add_single_product_to_collection_with_position(self, collection_id: str, product_id: int, position: int) -> bool:
        """Add a single product to collection with specific position using Collect API"""
        url = f"{self.base_url}/collects.json"
        
        payload = {
            "collect": {
                "product_id": product_id,
                "collection_id": int(collection_id),
                "position": position
            }
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            return True
            
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response is not None:
                status_code = e.response.status_code
                
                if status_code == 422:
                    # Product might already be in collection, this is ok
                    logger.debug(f"Product {product_id} may already be in collection (422)")
                    return True
                elif status_code == 429:
                    # Rate limit exceeded, wait and retry once
                    logger.warning(f"Rate limit exceeded, retrying product {product_id} after delay")
                    time.sleep(1)
                    try:
                        response = requests.post(url, headers=self.headers, json=payload)
                        response.raise_for_status()
                        return True
                    except:
                        return False
                else:
                    logger.warning(f"Error adding product {product_id}: {status_code}")
                    return False
            else:
                logger.warning(f"Error adding product {product_id}: {e}")
                return False
    
    def get_all_active_products_with_variants(self) -> List[Dict[str, Any]]:
        """Get all active products with their variants and inventory information"""
        url = f"{self.base_url}/products.json"
        
        all_products = []
        page_info = None
        page_size = 250  # Maximum allowed by Shopify API
        
        try:
            while True:
                # Set up parameters for this request
                params = {
                    "limit": page_size,
                    "fields": "id,title,handle,vendor,tags,variants"  # Include variants field
                }
                
                # Only add status parameter on first request (not with page_info)
                if not page_info:
                    params["status"] = "active"  # Only get active products
                else:
                    params["page_info"] = page_info
                
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                
                data = response.json()
                products_batch = data.get("products", [])
                
                if not products_batch:
                    break
                
                all_products.extend(products_batch)
                logger.debug(f"Fetched batch of {len(products_batch)} active products")
                
                # Check for pagination
                link_header = response.headers.get("Link")
                if link_header:
                    next_page_info = self._extract_page_info(link_header, "next")
                    if next_page_info:
                        page_info = next_page_info
                    else:
                        break
                else:
                    break
            
            logger.info(f"Retrieved {len(all_products)} active products with variants")
            return all_products
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching active products: {e}")
            raise
    
    def update_variant_metafield(self, variant_id: int, namespace: str, key: str, value: str) -> bool:
        """Update a single variant's metafield using the metafields endpoint"""
        # First, check if metafield already exists
        existing_metafields = self.get_variant_metafields(variant_id)
        metafield_id = None
        
        for metafield in existing_metafields:
            if metafield.get("namespace") == namespace and metafield.get("key") == key:
                metafield_id = metafield.get("id")
                break
        
        if metafield_id:
            # Update existing metafield
            url = f"{self.base_url}/variants/{variant_id}/metafields/{metafield_id}.json"
            # Determine value and type based on key
            metafield_value, metafield_type = self._get_metafield_value_and_type(key, value)
            payload = {
                "metafield": {
                    "id": metafield_id,
                    "value": metafield_value,
                    "type": metafield_type
                }
            }
            method = "PUT"
        else:
            # Create new metafield
            url = f"{self.base_url}/variants/{variant_id}/metafields.json"
            # Determine value and type based on key
            metafield_value, metafield_type = self._get_metafield_value_and_type(key, value)
            payload = {
                "metafield": {
                    "namespace": namespace,
                    "key": key,
                    "value": metafield_value,
                    "type": metafield_type
                }
            }
            method = "POST"
        
        try:
            if method == "PUT":
                response = requests.put(url, headers=self.headers, json=payload)
            else:
                response = requests.post(url, headers=self.headers, json=payload)
            
            response.raise_for_status()
            
            logger.debug(f"Successfully {'updated' if method == 'PUT' else 'created'} metafield {namespace}.{key} for variant {variant_id}")
            return True
            
        except requests.exceptions.RequestException as e:
            # Get detailed error information
            status_code = None
            error_details = {}
            error_text = str(e)
            
            if hasattr(e, 'response') and e.response is not None:
                status_code = e.response.status_code
                error_text = e.response.text
                try:
                    error_details = e.response.json()
                except:
                    error_details = {"raw_response": e.response.text}
            
            logger.error(f"Error updating variant {variant_id} metafield (method: {method}): {status_code} - {error_details} - URL: {url}")
            logger.error(f"Raw error text: {error_text}")
            logger.error(f"Payload sent: {json.dumps(payload, indent=2)}")
            
            if status_code == 429:
                # Rate limit exceeded, wait and retry once
                logger.warning(f"Rate limit exceeded for variant {variant_id}, retrying after delay")
                time.sleep(2)
                try:
                    if method == "PUT":
                        response = requests.put(url, headers=self.headers, json=payload)
                    else:
                        response = requests.post(url, headers=self.headers, json=payload)
                    response.raise_for_status()
                    return True
                except Exception as retry_error:
                    logger.error(f"Failed to update variant {variant_id} metafield after retry: {retry_error}")
                    return False
            else:
                return False
    
    def _get_metafield_value_and_type(self, key: str, value: str) -> tuple:
        """Determine the appropriate value and type for a metafield based on the key"""
        if key == "inventory":
            return int(value), "number_integer"
        elif key in ["price", "compare_price"]:
            # For prices, use number_decimal type to match Shopify's definition
            return str(value), "number_decimal"
        else:
            return str(value), "single_line_text_field"
    
    def get_variant_metafields(self, variant_id: int) -> List[Dict[str, Any]]:
        """Get metafields for a specific variant"""
        url = f"{self.base_url}/variants/{variant_id}/metafields.json"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            data = response.json()
            metafields = data.get("metafields", [])
            
            logger.debug(f"Found {len(metafields)} metafields for variant {variant_id}")
            return metafields
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching metafields for variant {variant_id}: {e}")
            return []
    
    def get_product_variant_by_id(self, product_id: int, variant_id: int) -> Optional[Dict[str, Any]]:
        """Get detailed information for a specific variant"""
        url = f"{self.base_url}/products/{product_id}/variants/{variant_id}.json"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            data = response.json()
            variant = data.get("variant", {})
            
            logger.debug(f"Retrieved variant details for variant {variant_id}")
            return variant
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.debug(f"Variant {variant_id} not found in product {product_id}")
                return None
            else:
                logger.error(f"HTTP error fetching variant {variant_id}: {e}")
                return None
        except Exception as e:
            logger.error(f"Error fetching variant {variant_id} details: {e}")
            return None
    
    def batch_update_variant_metafields(self, updates: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Update multiple variant metafields with controlled concurrency
        
        Args:
            updates: List of update dictionaries with keys:
                - variant_id: int
                - namespace: str  
                - key: str
                - value: str
        
        Returns:
            Dictionary with success/failure statistics
        """
        successful_updates = 0
        failed_updates = []
        
        for update in updates:
            try:
                success = self.update_variant_metafield(
                    variant_id=update["variant_id"],
                    namespace=update["namespace"],
                    key=update["key"],
                    value=update["value"]
                )
                
                if success:
                    successful_updates += 1
                else:
                    failed_updates.append({
                        "variant_id": update["variant_id"],
                        "error": "Update failed"
                    })
                
                # Small delay to avoid rate limiting
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Exception updating variant {update.get('variant_id')}: {e}")
                failed_updates.append({
                    "variant_id": update.get("variant_id"),
                    "error": str(e)
                })
        
        return {
            "successful_updates": successful_updates,
            "failed_updates": len(failed_updates),
            "failures": failed_updates,
            "total_attempted": len(updates)
        }
    
    def _extract_page_info(self, link_header: str, rel_type: str) -> Optional[str]:
        """Extract page_info from Link header for specific relation type"""
        if not link_header:
            return None
        
        # Look for the specific rel type (e.g., 'next', 'prev')
        pattern = rf'<[^>]*[?&]page_info=([^&>]*)>[^>]*rel="{rel_type}"'
        match = re.search(pattern, link_header)
        
        if match:
            return match.group(1)
        return None

    # Customer API Methods
    
    def get_all_customers(self, limit: Optional[int] = None, batch_callback: Optional[callable] = None, batch_size: int = 250) -> List[Dict[str, Any]]:
        """
        Get all customers from Shopify with pagination support and robust error handling
        
        Args:
            limit: Optional limit on number of customers to fetch (None for all customers)
            batch_callback: Optional callback function to process customers in batches
            batch_size: Size of batches to process with callback (default: 250)
        """
        url = f"{self.base_url}/customers.json"
        
        all_customers = []
        page_info = None
        page_size = 100  # Reduced from 250 to be more conservative
        
        try:
            limit_msg = f" (limit: {limit})" if limit else " (no limit - fetching all customers)"
            logger.info(f"🔄 Starting customer fetch from Shopify{limit_msg}...")
            
            # Initialize batch processing counter
            self._last_processed_count = 0
            
            if limit:
                logger.info(f"⚠️  Limited mode: Will stop after fetching {limit} customers")
            else:
                logger.info("📈 Fetching ALL customers - this will take several minutes with frequent progress updates")
                logger.info("📈 You will see progress for each page and milestone updates every 100 customers")
            page_count = 0
            start_time = time.time()
            
            while True:
                page_count += 1
                logger.info(f"📄 Fetching page {page_count} (batch size: {page_size})...")
                
                # Set up parameters for this request
                params = {"limit": page_size}
                if page_info:
                    params["page_info"] = page_info
                    logger.info(f"🔗 Using page_info for pagination: {page_info[:50]}...")
                else:
                    logger.info("🏁 This is the first page (no page_info)")
                
                # Add retry logic for network issues with more aggressive retries
                max_retries = 5
                response = None
                
                for attempt in range(max_retries):
                    try:
                        # Always show API request details for better visibility
                        logger.info(f"🔗 Making API request (attempt {attempt + 1}/{max_retries}) for page {page_count}")
                        logger.info(f"📡 Request URL: {url}")
                        logger.info(f"📊 Request params: {params}")
                        response = requests.get(url, headers=self.headers, params=params, timeout=120)
                        logger.info(f"📨 Response status: {response.status_code}")
                        
                        # Check for rate limiting
                        if response.status_code == 429:
                            try:
                                try:
                            try:
                        retry_after = int(float(response.headers.get('Retry-After', 2)))
                    except (ValueError, TypeError):
                        retry_after = 2  # Default fallback
                        except (ValueError, TypeError):
                            retry_after = 2  # Default fallback
                            except (ValueError, TypeError):
                                retry_after = 2  # Default fallback
                            logger.warning(f"⏸️  Rate limited, waiting {retry_after}s before retry...")
                            time.sleep(retry_after)
                            continue
                        
                        response.raise_for_status()
                        break  # Success, exit retry loop
                        
                    except requests.exceptions.Timeout as e:
                        wait_time = min(5 * (2 ** attempt), 60)  # Cap at 60 seconds
                        if attempt < max_retries - 1:
                            logger.warning(f"⏱️  Request timeout (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s...")
                            time.sleep(wait_time)
                        else:
                            logger.error(f"💥 Request timed out after {max_retries} attempts")
                            raise
                            
                    except requests.exceptions.ConnectionError as e:
                        wait_time = min(3 * (2 ** attempt), 30)  # Cap at 30 seconds
                        if attempt < max_retries - 1:
                            logger.warning(f"🔌 Connection error (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s: {e}")
                            time.sleep(wait_time)
                        else:
                            logger.error(f"💥 Connection failed after {max_retries} attempts: {e}")
                            raise
                            
                    except requests.exceptions.RequestException as e:
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt
                            logger.warning(f"🚨 API error (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s: {e}")
                            time.sleep(wait_time)
                        else:
                            logger.error(f"💥 API request failed after {max_retries} attempts: {e}")
                            raise
                
                # Parse response
                try:
                    data = response.json()
                    customers_batch = data.get("customers", [])
                    logger.info(f"📦 Successfully parsed {len(customers_batch)} customers from page {page_count}")
                except json.JSONDecodeError as e:
                    logger.error(f"💥 Failed to parse JSON response: {e}")
                    logger.error(f"Response content: {response.text[:500]}...")
                    raise
                
                if not customers_batch:
                    logger.info(f"📄 No more customers found on page {page_count}")
                    break
                
                # Process batch callback immediately for each page if provided
                if batch_callback:
                    batch_start_idx = len(all_customers)  # Starting index for this batch
                    logger.info(f"🔄 Processing page callback for customers {batch_start_idx + 1}-{batch_start_idx + len(customers_batch)}...")
                    try:
                        batch_callback(customers_batch, batch_start_idx)
                        logger.info(f"✅ Page callback completed for {len(customers_batch)} customers")
                    except Exception as e:
                        logger.error(f"💥 Page callback failed: {e}")
                        # Continue processing even if callback fails
                
                all_customers.extend(customers_batch)
                batch_time = time.time() - start_time
                
                # More frequent progress updates
                customers_per_sec = len(all_customers) / max(batch_time, 0.1)  # Avoid division by zero
                eta_remaining = ""
                if not limit:  # Only estimate for unlimited fetches
                    if len(all_customers) >= 500:  # Estimate earlier after we have some data
                        estimated_total = 12000  # Rough estimate based on previous runs
                        if len(all_customers) > 0:
                            estimated_remaining_time = (estimated_total - len(all_customers)) / customers_per_sec
                            eta_remaining = f", ETA: {estimated_remaining_time/60:.1f}min"
                
                # Show page progress for every page in large fetches
                if not limit or page_count <= 10:  # Always show for limited fetches, or first 10 pages of unlimited
                    logger.info(f"✅ Page {page_count}: +{len(customers_batch)} customers (total: {len(all_customers)}, {customers_per_sec:.1f}/sec{eta_remaining})")
                elif page_count % 5 == 0:  # Every 5th page for unlimited fetches
                    logger.info(f"✅ Page {page_count}: +{len(customers_batch)} customers (total: {len(all_customers)}, {customers_per_sec:.1f}/sec{eta_remaining})")
                
                # Check if we've reached the limit
                if limit and len(all_customers) >= limit:
                    logger.info(f"🎯 Reached customer limit ({limit}), stopping fetch...")
                    all_customers = all_customers[:limit]  # Trim to exact limit
                    break
                
                # More frequent progress milestone updates
                if len(all_customers) % 100 == 0:  # Every 100 customers for better visibility
                    avg_time = batch_time / len(all_customers)
                    customers_per_sec = len(all_customers) / max(batch_time, 0.1)
                    progress_pct = ""
                    eta_str = ""
                    if not limit:
                        estimated_total = 12000
                        progress_pct = f" ({len(all_customers)/estimated_total*100:.1f}%)"
                        if len(all_customers) > 200:  # Only show ETA after some data
                            remaining = estimated_total - len(all_customers)
                            eta_seconds = remaining / max(customers_per_sec, 1)
                            eta_str = f", ETA: {eta_seconds/60:.1f}min"
                    logger.info(f"🎯 MILESTONE: {len(all_customers)} customers fetched{progress_pct} in {batch_time:.1f}s ({customers_per_sec:.1f}/sec{eta_str})")
                
                # Check for next page using Link header
                link_header = response.headers.get("Link", "")
                next_page_info = self._extract_page_info(link_header, "next")
                
                if next_page_info:
                    page_info = next_page_info
                    # Add small delay between requests to be API-friendly
                    time.sleep(0.1)
                else:
                    logger.info(f"📄 Reached last page (page {page_count})")
                    break  # No more pages
            
            # Process any remaining customers that didn't reach batch_size
            if batch_callback and len(all_customers) > 0:
                last_processed = getattr(self, '_last_processed_count', 0)
                if last_processed < len(all_customers):
                    remaining_batch = all_customers[last_processed:]
                    logger.info(f"🔄 Processing final batch callback for remaining {len(remaining_batch)} customers...")
                    try:
                        batch_callback(remaining_batch, last_processed)
                        logger.info(f"✅ Final batch callback completed for {len(remaining_batch)} customers")
                    except Exception as e:
                        logger.error(f"💥 Final batch callback failed: {e}")
            
            total_time = time.time() - start_time
            logger.info(f"🎉 Successfully fetched {len(all_customers)} customers in {page_count} pages ({total_time:.1f}s total)")
            return all_customers
            
        except Exception as e:
            logger.error(f"💥 Critical error fetching customers: {e}")
            logger.error(f"📊 Partial result: {len(all_customers)} customers fetched before error")
            raise
    
    def get_customer_by_id(self, customer_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific customer by ID from Shopify
        
        Args:
            customer_id: The Shopify customer ID
            
        Returns:
            Customer data or None if not found
        """
        url = f"{self.base_url}/customers/{customer_id}.json"
        
        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return response.json().get("customer")
            elif response.status_code == 404:
                logger.warning(f"Customer {customer_id} not found")
                return None
            else:
                logger.error(f"Failed to fetch customer {customer_id}: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching customer {customer_id}: {e}")
            return None

    def update_customer_metafield(self, customer_id: int, namespace: str, key: str, value: str, create_if_missing: bool = False) -> bool:
        """Update a metafield for a specific customer with enhanced rate limiting and retry logic"""
        
        # Add small delay before each request to reduce API pressure
        time.sleep(0.2)  # 200ms delay to reduce 429 errors
        
        # First, check if metafield already exists
        existing_metafield = self._get_customer_metafield(customer_id, namespace, key)
        
        if existing_metafield:
            # Update existing metafield - use minimal payload to avoid type conflicts
            metafield_id = existing_metafield.get("id")
            existing_type = existing_metafield.get("type", "single_line_text_field")
            update_url = f"{self.base_url}/customers/{customer_id}/metafields/{metafield_id}.json"
            
            # Convert value based on existing metafield type
            converted_value = value
            if existing_type == "boolean":
                # Convert string to boolean for boolean type metafields
                converted_value = value.lower() == "true" if isinstance(value, str) else bool(value)
            
            payload = {
                "metafield": {
                    "id": metafield_id,
                    "value": converted_value
                }
            }
            
            # Retry logic with exponential backoff for 429 errors
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = requests.put(update_url, headers=self.headers, json=payload)
                    
                    if response.status_code == 429:
                        # Rate limit exceeded, use exponential backoff
                        try:
                            try:
                        retry_after = int(float(response.headers.get('Retry-After', 2)))
                    except (ValueError, TypeError):
                        retry_after = 2  # Default fallback
                        except (ValueError, TypeError):
                            retry_after = 2  # Default fallback
                        wait_time = max(retry_after, 2 ** attempt)  # At least 2^attempt seconds
                        logger.warning(f"⏸️  Rate limited updating customer {customer_id}, waiting {wait_time}s (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    
                    response.raise_for_status()
                    logger.debug(f"✅ Updated customer {customer_id} metafield {namespace}.{key} = {value}")
                    return True
                    
                except requests.exceptions.RequestException as e:
                    if attempt == max_retries - 1:  # Last attempt
                        logger.error(f"❌ Error updating customer {customer_id} metafield {namespace}.{key}: {e}")
                        return False
                    else:
                        # Wait before retry
                        wait_time = 2 ** attempt
                        logger.warning(f"⚠️  Request failed for customer {customer_id}, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries}): {e}")
                        time.sleep(wait_time)
        else:
            # Metafield does not exist
            if create_if_missing:
                # Create new metafield with boolean type (matching Shopify Admin definition)
                url = f"{self.base_url}/customers/{customer_id}/metafields.json"
                
                # Convert value to boolean for boolean type metafields
                converted_value = value.lower() == "true" if isinstance(value, str) else bool(value)
                
                payload = {
                    "metafield": {
                        "namespace": namespace,
                        "key": key,
                        "value": converted_value,
                        "type": "boolean"
                    }
                }
                
                # Retry logic with exponential backoff for 429 errors
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        response = requests.post(url, headers=self.headers, json=payload)
                        
                        if response.status_code == 429:
                            # Rate limit exceeded, use exponential backoff
                            try:
                                try:
                            try:
                        retry_after = int(float(response.headers.get('Retry-After', 2)))
                    except (ValueError, TypeError):
                        retry_after = 2  # Default fallback
                        except (ValueError, TypeError):
                            retry_after = 2  # Default fallback
                            except (ValueError, TypeError):
                                retry_after = 2  # Default fallback
                            wait_time = max(retry_after, 2 ** attempt)  # At least 2^attempt seconds
                            logger.warning(f"⏸️  Rate limited creating customer {customer_id} metafield, waiting {wait_time}s (attempt {attempt + 1}/{max_retries})")
                            time.sleep(wait_time)
                            continue
                        
                        response.raise_for_status()
                        logger.debug(f"✅ Created customer {customer_id} metafield {namespace}.{key} = {converted_value}")
                        return True
                        
                    except requests.exceptions.RequestException as e:
                        if attempt == max_retries - 1:  # Last attempt
                            logger.error(f"❌ Error creating customer {customer_id} metafield {namespace}.{key}: {e}")
                            return False
                        else:
                            # Wait before retry
                            wait_time = 2 ** attempt
                            logger.warning(f"⚠️  Create request failed for customer {customer_id}, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries}): {e}")
                            time.sleep(wait_time)
            else:
                # Don't create new ones, only update existing ones
                logger.warning(f"⚠️  Customer {customer_id} does not have metafield {namespace}.{key} - skipping (only updating existing metafields)")
                return False

    def _get_customer_metafield(self, customer_id: int, namespace: str, key: str) -> Optional[Dict[str, Any]]:
        """Get a specific metafield for a customer with rate limiting protection"""
        url = f"{self.base_url}/customers/{customer_id}/metafields.json"
        
        # Add small delay before GET request
        time.sleep(0.1)  # 100ms delay for GET requests
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.headers)
                
                if response.status_code == 429:
                    # Rate limit exceeded
                    try:
                        retry_after = int(float(response.headers.get('Retry-After', 2)))
                    except (ValueError, TypeError):
                        retry_after = 2  # Default fallback
                    wait_time = max(retry_after, 2 ** attempt)
                    logger.warning(f"⏸️  Rate limited getting customer {customer_id} metafields, waiting {wait_time}s")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                
                data = response.json()
                metafields = data.get("metafields", [])
                
                # Look for the specific metafield
                for metafield in metafields:
                    if (metafield.get("namespace") == namespace and 
                        metafield.get("key") == key):
                        return metafield
                
                return None
                
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:  # Last attempt
                    logger.debug(f"Error fetching customer {customer_id} metafields: {e}")
                    return None
                else:
                    # Wait before retry
                    wait_time = 2 ** attempt
                    logger.warning(f"⚠️  GET metafields failed for customer {customer_id}, retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)

    def get_customer_metafields(self, customer_id: int) -> List[Dict[str, Any]]:
        """Get all metafields for a specific customer"""
        url = f"{self.base_url}/customers/{customer_id}/metafields.json"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            data = response.json()
            metafields = data.get("metafields", [])
            
            logger.debug(f"Found {len(metafields)} metafields for customer {customer_id}")
            return metafields
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching metafields for customer {customer_id}: {e}")
            raise