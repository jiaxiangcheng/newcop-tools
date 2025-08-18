import requests
import logging
import re
import time
from typing import List, Dict, Any, Optional
from models import FilteredProduct, ShopifyProduct

logger = logging.getLogger(__name__)

class ShopifyClient:
    """Client for interacting with Shopify Admin API"""
    
    def __init__(self, admin_token: str, shop_domain: str):
        self.admin_token = admin_token
        self.shop_domain = shop_domain.replace('.myshopify.com', '')
        self.base_url = f"https://{self.shop_domain}.myshopify.com/admin/api/2023-10"
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
    
    def add_products_to_collection_with_order(self, collection_id: str, ordered_products: List[FilteredProduct]) -> bool:
        """
        Add products to collection with specific sort order using Collect API
        """
        if not ordered_products:
            logger.info("No products to add to collection")
            return True
        
        success_count = 0
        
        for product in ordered_products:
            if not product.shopify_id:
                logger.warning(f"Product '{product.product_name}' has no Shopify ID, skipping")
                continue
                
            try:
                if self._add_product_to_collection_with_position(collection_id, product.shopify_id, product.sort_position):
                    success_count += 1
                    logger.debug(f"Added product {product.shopify_id} at position {product.sort_position}")
            except Exception as e:
                logger.warning(f"Failed to add product {product.shopify_id} to collection: {e}")
                continue
        
        total_valid_products = len([p for p in ordered_products if p.shopify_id])
        logger.info(f"Successfully added {success_count}/{total_valid_products} products to collection with ordering")
        return success_count == total_valid_products
    
    def _add_product_to_collection_with_position(self, collection_id: str, product_id: int, position: int) -> bool:
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
            logger.debug(f"Successfully added product {product_id} at position {position}")
            return True
            
        except requests.exceptions.RequestException as e:
            # Handle specific error cases
            if hasattr(e, 'response') and e.response is not None:
                status_code = e.response.status_code
                
                if status_code == 422:
                    # Product might already be in collection, try to update position
                    logger.debug(f"Product {product_id} may already be in collection (422), trying to update position")
                    return self._update_product_position_in_collection(collection_id, product_id, position)
                elif status_code == 429:
                    # Rate limit exceeded, wait and retry
                    logger.warning(f"Rate limit exceeded, retrying product {product_id} after delay")
                    time.sleep(1)
                    return self._add_product_to_collection_with_position(collection_id, product_id, position)
                else:
                    # Log detailed error information
                    try:
                        error_data = e.response.json()
                        logger.error(f"Error adding product {product_id} to collection: {status_code} - {error_data}")
                    except:
                        logger.error(f"Error adding product {product_id} to collection: {status_code} - {e.response.text}")
            else:
                logger.error(f"Error adding product {product_id} to collection with position: {e}")
            return False
    
    def _update_product_position_in_collection(self, collection_id: str, product_id: int, position: int) -> bool:
        """Update the position of an existing product in collection"""
        try:
            # First get the collect ID for this product in the collection
            collects = self._get_collection_collects(collection_id)
            collect_id = None
            
            for collect in collects:
                if collect.get("product_id") == product_id:
                    collect_id = collect.get("id")
                    break
            
            if not collect_id:
                logger.warning(f"Could not find collect for product {product_id} in collection {collection_id}")
                # If we can't find the collect, try adding the product instead
                logger.debug(f"Attempting to add product {product_id} to collection instead")
                return self._add_single_product_to_collection(collection_id, product_id)
            
            # Update the position
            url = f"{self.base_url}/collects/{collect_id}.json"
            payload = {
                "collect": {
                    "position": position
                }
            }
            
            response = requests.put(url, headers=self.headers, json=payload)
            response.raise_for_status()
            logger.debug(f"Updated position for product {product_id} to {position}")
            return True
            
        except requests.exceptions.RequestException as e:
            # Handle specific error cases
            if hasattr(e, 'response') and e.response is not None:
                status_code = e.response.status_code
                
                if status_code == 429:
                    # Rate limit exceeded, wait and retry
                    logger.warning(f"Rate limit exceeded updating position for product {product_id}, retrying after delay")
                    time.sleep(1)
                    return self._update_product_position_in_collection(collection_id, product_id, position)
                else:
                    # Log detailed error information
                    try:
                        error_data = e.response.json()
                        logger.error(f"Error updating position for product {product_id}: {status_code} - {error_data}")
                    except:
                        logger.error(f"Error updating position for product {product_id}: {status_code} - {e.response.text}")
            else:
                logger.error(f"Error updating position for product {product_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error updating position for product {product_id}: {e}")
            return False

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
    
    def update_collection_with_filtered_products(self, collection_id: str, filtered_products: List[FilteredProduct]) -> Dict[str, Any]:
        """
        Intelligently update collection with filtered products from Airtable using direct Shopify IDs.
        This method performs atomic replacement to avoid empty collection states:
        1. Get current products in collection
        2. Compare with new filtered products
        3. Remove products that should no longer be in collection
        4. Add new products that should be in collection
        """
        logger.info(f"Starting intelligent collection update {collection_id} with {len(filtered_products)} filtered products")
        
        # Extract Shopify IDs directly from filtered products
        new_product_ids = []
        products_without_id = 0
        
        for product in filtered_products:
            if product.shopify_id and product.shopify_id > 0:
                new_product_ids.append(product.shopify_id)
            else:
                products_without_id += 1
                logger.warning(f"Product '{product.product_name}' has no valid Shopify ID")
        
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
        
        # Get current products in collection
        logger.info("Getting current products in collection...")
        try:
            current_products = self.get_collection_products(collection_id)
            current_product_ids = [p.id for p in current_products]
            logger.info(f"Found {len(current_product_ids)} current products in collection")
        except Exception as e:
            logger.error(f"Failed to get current collection products: {e}")
            # If we can't get current products, fall back to adding only
            success = self.add_products_to_collection(collection_id, new_product_ids)
            return {
                "filtered_products_count": len(filtered_products),
                "valid_shopify_ids": len(new_product_ids),
                "products_without_id": products_without_id,
                "products_added": len(new_product_ids) if success else 0,
                "products_removed": 0,
                "success": success,
                "fallback_mode": True
            }
        
        # Calculate differences
        new_product_ids_set = set(new_product_ids)
        current_product_ids_set = set(current_product_ids)
        
        products_to_add = list(new_product_ids_set - current_product_ids_set)
        products_to_remove = list(current_product_ids_set - new_product_ids_set)
        products_to_keep = list(new_product_ids_set & current_product_ids_set)
        
        logger.info(f"Products to add: {len(products_to_add)}")
        logger.info(f"Products to remove: {len(products_to_remove)}")
        logger.info(f"Products to keep: {len(products_to_keep)}")
        
        # Perform atomic update with proper ordering
        # Strategy: Remove products that shouldn't be there, then add/update all products with correct positions
        
        removed_count = 0
        added_count = 0
        
        # Step 1: Remove products that should no longer be in collection
        if products_to_remove:
            logger.info(f"Removing {len(products_to_remove)} products from collection...")
            try:
                remove_success = self.remove_products_from_collection(collection_id, products_to_remove)
                if remove_success:
                    removed_count = len(products_to_remove)
                    logger.info(f"Successfully removed {removed_count} products")
                else:
                    logger.warning("Some products failed to be removed")
            except Exception as e:
                logger.error(f"Failed to remove products: {e}")
        
        # Step 2: Add new products with correct ordering (includes both new and existing products)
        # This ensures all products have the correct position based on sales ranking
        products_to_add_or_update = [p for p in filtered_products if p.shopify_id]
        
        if products_to_add_or_update:
            logger.info(f"Adding/updating {len(products_to_add_or_update)} products with sales-based ordering...")
            try:
                add_success = self.add_products_to_collection_with_order(collection_id, products_to_add_or_update)
                if add_success:
                    added_count = len([p for p in products_to_add_or_update if p.shopify_id in products_to_add])
                    updated_count = len([p for p in products_to_add_or_update if p.shopify_id in products_to_keep])
                    logger.info(f"Successfully added {added_count} new products and updated {updated_count} existing positions")
                else:
                    logger.warning("Some products failed to be added or positioned correctly")
            except Exception as e:
                logger.error(f"Failed to add/update products with ordering: {e}")
        
        # Calculate final success
        operation_success = True
        if products_to_remove and removed_count != len(products_to_remove):
            operation_success = False
        if products_to_add_or_update and not add_success:
            operation_success = False
        
        actual_added_count = len([p for p in products_to_add_or_update if p.shopify_id in products_to_add]) if products_to_add_or_update else 0
        
        result = {
            "filtered_products_count": len(filtered_products),
            "valid_shopify_ids": len(new_product_ids),
            "products_without_id": products_without_id,
            "current_products_count": len(current_product_ids),
            "products_added": actual_added_count,
            "products_removed": removed_count,
            "products_kept": len(products_to_keep),
            "products_repositioned": len([p for p in products_to_add_or_update if p.shopify_id in products_to_keep]) if products_to_add_or_update else 0,
            "final_collection_size": len(new_product_ids),  # Final size should be all valid new products
            "top_seller": filtered_products[0].product_name if filtered_products else "N/A",
            "success": operation_success
        }
        
        logger.info(f"Intelligent collection update completed: {result}")
        return result