import requests
import logging
import re
import time
import json
from typing import List, Dict, Any, Optional
from models import FilteredProduct, ShopifyProduct

logger = logging.getLogger(__name__)

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
                # Progress update every few batches
                if (i//batch_size + 1) % 3 == 0 or i//batch_size + 1 == 1:
                    logger.info(f"🗑️  Removing batch {i//batch_size + 1}: {len(batch)} collects")
                
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
        
        # Extract valid Shopify IDs
        new_product_ids = []
        products_without_id = 0
        
        for product in filtered_products:
            if product.shopify_id and product.shopify_id > 0:
                new_product_ids.append(product.shopify_id)
            else:
                products_without_id += 1
                logger.warning(f"Product '{product.product_name}' has no valid Shopify ID, skipping")
        
        if products_without_id > 0:
            logger.warning(f"⚠️  {products_without_id} products skipped due to missing Shopify IDs")
        
        if not new_product_ids:
            logger.warning("No valid Shopify product IDs found")
            return {
                "filtered_products_count": len(filtered_products),
                "valid_shopify_ids": 0,
                "products_without_id": products_without_id,
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
        try:
            add_success = self.add_products_to_collection_in_order(collection_id, new_product_ids)
            added_count = len(new_product_ids) if add_success else 0
            if add_success:
                logger.info(f"Successfully added {added_count} products in correct order")
            else:
                logger.warning("Some products failed to be added")
        except Exception as e:
            logger.error(f"Failed to add products: {e}")
            add_success = False
            added_count = 0
        
        operation_success = add_success and (not current_product_ids or removed_count == len(current_product_ids))
        
        result = {
            "filtered_products_count": len(filtered_products),
            "valid_shopify_ids": len(new_product_ids),
            "products_without_id": products_without_id,
            "current_products_count": len(current_product_ids),
            "products_added": added_count,
            "products_removed": removed_count,
            "final_collection_size": added_count,
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
    
    def add_products_to_collection_in_order(self, collection_id: str, product_ids: List[int]) -> bool:
        """
        Fast batch addition of products to collection in specified order.
        Products are added sequentially to maintain order.
        """
        if not product_ids:
            logger.info("No products to add")
            return True
        
        logger.info(f"Starting batch addition of {len(product_ids)} products to collection {collection_id}")
        
        try:
            batch_size = 50  # Increased batch size for faster processing
            success_count = 0
            
            for i, product_id in enumerate(product_ids):
                try:
                    # Use position i+1 (1-based) to maintain order
                    success = self._add_single_product_to_collection_with_position(collection_id, product_id, i + 1)
                    if success:
                        success_count += 1
                        # Progress update every 50 products
                        if (i + 1) % 50 == 0:
                            logger.info(f"➕ Added {i + 1}/{len(product_ids)} products")
                    else:
                        logger.warning(f"Failed to add product {product_id}")
                except Exception as e:
                    logger.warning(f"Failed to add product {product_id}: {e}")
                    continue
                
                # Add small delay every batch_size products to respect rate limits
                if (i + 1) % batch_size == 0 and i + 1 < len(product_ids):
                    time.sleep(0.05)  # Reduced delay since batch size is larger
            
            success_rate = success_count / len(product_ids)
            logger.info(f"Batch addition completed: {success_count}/{len(product_ids)} products added ({success_rate:.1%})")
            
            return success_rate >= 0.9  # Consider successful if 90%+ added
            
        except Exception as e:
            logger.error(f"Failed to add products to collection: {e}")
            return False
    
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