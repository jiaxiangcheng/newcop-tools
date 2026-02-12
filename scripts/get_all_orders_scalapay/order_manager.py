#!/usr/bin/env python3
"""
Order Manager for Scalapay Orders

Core logic for fetching Shopify orders and filtering by Scalapay payment gateway.
Uses GraphQL API for efficient order retrieval.
"""

import logging
import time
from typing import List, Optional
from datetime import datetime

from shared.shopify_client import ShopifyClient
from scripts.get_all_orders_scalapay.models import ScalapayOrder, ScalapayOrderResult

logger = logging.getLogger(__name__)


class ScalapayOrderManager:
    """Manager for fetching and filtering Scalapay orders from Shopify"""

    # Scalapay gateway identifier (case-insensitive)
    SCALAPAY_GATEWAY_KEYWORD = "scalapay"

    def __init__(self, shopify_client: ShopifyClient):
        """
        Initialize the order manager

        Args:
            shopify_client: Initialized Shopify client
        """
        self.shopify_client = shopify_client

    def fetch_scalapay_orders(
        self,
        limit: Optional[int] = None,
        dry_run: bool = False
    ) -> ScalapayOrderResult:
        """
        Fetch all orders that were paid using Scalapay

        Args:
            limit: Optional limit on number of orders to scan
            dry_run: If True, only scan without full processing

        Returns:
            ScalapayOrderResult with found orders and statistics
        """
        start_time = time.time()
        scalapay_orders: List[ScalapayOrder] = []
        total_scanned = 0

        logger.info("=" * 60)
        logger.info("Starting Scalapay Orders Fetch")
        logger.info("=" * 60)
        if limit:
            logger.info(f"Limit: {limit} orders")
        if dry_run:
            logger.info("Mode: DRY RUN")
        logger.info("=" * 60)

        try:
            # Use GraphQL to fetch orders with payment gateway info
            cursor = None
            has_next_page = True
            page_count = 0

            while has_next_page:
                page_count += 1
                logger.info(f"📄 Fetching page {page_count}...")

                # Fetch orders using GraphQL
                orders_data, cursor, has_next_page = self._fetch_orders_page(
                    cursor=cursor,
                    page_size=50
                )

                if not orders_data:
                    logger.info("No more orders found")
                    break

                # Process each order
                for order in orders_data:
                    total_scanned += 1

                    # Check if limit reached
                    if limit and total_scanned > limit:
                        logger.info(f"Reached limit of {limit} orders")
                        has_next_page = False
                        break

                    # Check if order was paid with Scalapay
                    payment_gateway = self._get_payment_gateway(order)

                    if self._is_scalapay_payment(payment_gateway):
                        scalapay_order = self._parse_order(order, payment_gateway)
                        scalapay_orders.append(scalapay_order)

                        if not dry_run:
                            logger.debug(
                                f"✅ Found Scalapay order: {scalapay_order.order_name} "
                                f"- {scalapay_order.customer_email}"
                            )

                # Progress update
                if total_scanned % 100 == 0:
                    logger.info(
                        f"📊 Progress: Scanned {total_scanned} orders, "
                        f"found {len(scalapay_orders)} Scalapay orders"
                    )

        except Exception as e:
            logger.error(f"Error fetching orders: {e}")
            raise

        execution_time = time.time() - start_time

        # Summary
        logger.info("=" * 60)
        logger.info("FETCH COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Total orders scanned: {total_scanned}")
        logger.info(f"Scalapay orders found: {len(scalapay_orders)}")
        logger.info(f"Execution time: {execution_time:.2f} seconds")
        logger.info("=" * 60)

        return ScalapayOrderResult(
            orders=scalapay_orders,
            total_orders_scanned=total_scanned,
            scalapay_orders_found=len(scalapay_orders),
            execution_time_seconds=execution_time
        )

    def _fetch_orders_page(
        self,
        cursor: Optional[str] = None,
        page_size: int = 50
    ) -> tuple:
        """
        Fetch a page of orders using GraphQL

        Args:
            cursor: Pagination cursor
            page_size: Number of orders per page

        Returns:
            Tuple of (orders, next_cursor, has_next_page)
        """
        # GraphQL query for orders with payment and customer info
        query = """
        query getOrders($first: Int!, $after: String, $query: String) {
          orders(first: $first, after: $after, query: $query, sortKey: CREATED_AT, reverse: true) {
            pageInfo {
              hasNextPage
              endCursor
            }
            edges {
              node {
                id
                name
                createdAt
                displayFinancialStatus
                displayFulfillmentStatus
                totalPriceSet {
                  shopMoney {
                    amount
                    currencyCode
                  }
                }
                subtotalPriceSet {
                  shopMoney {
                    amount
                  }
                }
                totalShippingPriceSet {
                  shopMoney {
                    amount
                  }
                }
                totalRefundedSet {
                  shopMoney {
                    amount
                  }
                }
                paymentGatewayNames
                customer {
                  email
                  firstName
                  lastName
                }
                shippingAddress {
                  address1
                  address2
                  city
                  province
                  country
                  zip
                }
                lineItems(first: 50) {
                  edges {
                    node {
                      title
                      variantTitle
                      sku
                      quantity
                      originalUnitPriceSet {
                        shopMoney {
                          amount
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """

        variables = {
            "first": page_size,
            "after": cursor,
            "query": None  # Fetch all orders, filter by payment gateway later
        }

        try:
            response = self.shopify_client.execute_graphql(query, variables)

            if "errors" in response:
                logger.error(f"GraphQL errors: {response['errors']}")
                return [], None, False

            orders_data = response.get("data", {}).get("orders", {})
            edges = orders_data.get("edges", [])
            page_info = orders_data.get("pageInfo", {})

            orders = [edge.get("node", {}) for edge in edges]
            next_cursor = page_info.get("endCursor")
            has_next = page_info.get("hasNextPage", False)

            logger.info(f"Fetched {len(orders)} orders in this page, hasNextPage: {has_next}")

            # Debug: log first order's payment gateway if available
            if orders:
                first_order = orders[0]
                gateways = first_order.get("paymentGatewayNames", [])
                logger.info(f"Sample order payment gateways: {gateways}")

            return orders, next_cursor, has_next

        except Exception as e:
            logger.error(f"Error in GraphQL query: {e}")
            return [], None, False

    def _get_payment_gateway(self, order: dict) -> str:
        """
        Extract payment gateway name from order

        Args:
            order: Order data dictionary

        Returns:
            Payment gateway name or empty string
        """
        gateways = order.get("paymentGatewayNames", [])
        if gateways:
            return ", ".join(gateways)
        return ""

    def _is_scalapay_payment(self, payment_gateway: str) -> bool:
        """
        Check if payment gateway is Scalapay (case-insensitive)

        Args:
            payment_gateway: Payment gateway name

        Returns:
            True if Scalapay payment
        """
        if not payment_gateway:
            return False

        return self.SCALAPAY_GATEWAY_KEYWORD in payment_gateway.lower()

    def _parse_order(self, order: dict, payment_gateway: str) -> ScalapayOrder:
        """
        Parse order data into ScalapayOrder object

        Args:
            order: Order data dictionary
            payment_gateway: Payment gateway name

        Returns:
            ScalapayOrder object
        """
        customer = order.get("customer") or {}
        total_price_set = order.get("totalPriceSet", {}).get("shopMoney", {})
        subtotal_price_set = order.get("subtotalPriceSet", {}).get("shopMoney", {})
        shipping_price_set = order.get("totalShippingPriceSet", {}).get("shopMoney", {})
        refunded_set = order.get("totalRefundedSet", {}).get("shopMoney", {})

        # Parse created_at
        created_at = None
        created_at_str = order.get("createdAt")
        if created_at_str:
            try:
                created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                pass

        # Parse shipping address
        shipping_addr = order.get("shippingAddress") or {}
        shipping_address_parts = []
        if shipping_addr.get("address1"):
            shipping_address_parts.append(shipping_addr["address1"])
        if shipping_addr.get("address2"):
            shipping_address_parts.append(shipping_addr["address2"])
        if shipping_addr.get("zip"):
            shipping_address_parts.append(shipping_addr["zip"])
        if shipping_addr.get("city"):
            shipping_address_parts.append(shipping_addr["city"])
        if shipping_addr.get("province"):
            shipping_address_parts.append(shipping_addr["province"])
        if shipping_addr.get("country"):
            shipping_address_parts.append(shipping_addr["country"])

        shipping_address = ", ".join(shipping_address_parts) if shipping_address_parts else None

        # Parse line items
        line_items_data = order.get("lineItems", {}).get("edges", [])
        line_items_list = []
        for edge in line_items_data:
            item = edge.get("node", {})
            title = item.get("title", "")
            variant = item.get("variantTitle", "")
            sku = item.get("sku", "")
            qty = item.get("quantity", 1)
            price = item.get("originalUnitPriceSet", {}).get("shopMoney", {}).get("amount", "")

            # Format: "Product Name (Variant) x Qty @ Price [SKU]"
            item_str = f"{title}"
            if variant:
                item_str += f" ({variant})"
            item_str += f" x{qty}"
            if price:
                item_str += f" @{price}"
            if sku:
                item_str += f" [{sku}]"
            line_items_list.append(item_str)

        line_items = " | ".join(line_items_list) if line_items_list else None

        return ScalapayOrder(
            order_name=order.get("name", ""),
            customer_email=customer.get("email"),
            customer_first_name=customer.get("firstName"),
            customer_last_name=customer.get("lastName"),
            fulfillment_status=order.get("displayFulfillmentStatus"),
            financial_status=order.get("displayFinancialStatus"),
            payment_gateway=payment_gateway,
            created_at=created_at,
            total_price=total_price_set.get("amount"),
            currency=total_price_set.get("currencyCode"),
            subtotal_price=subtotal_price_set.get("amount"),
            shipping_price=shipping_price_set.get("amount"),
            total_refunded=refunded_set.get("amount"),
            shipping_address=shipping_address,
            shipping_city=shipping_addr.get("city"),
            shipping_country=shipping_addr.get("country"),
            line_items=line_items
        )
