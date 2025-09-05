#!/usr/bin/env python3
"""
Shopify GraphQL Script
Execute GraphQL queries and mutations against Shopify Admin API
"""

import os
import sys
import json
import argparse
import requests
from pathlib import Path
from typing import Dict, Any, Optional

# Add parent directories to path for imports
current_dir = Path(__file__).parent
project_root = current_dir.parent.parent
sys.path.append(str(project_root))

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class ShopifyGraphQLClient:
    def __init__(self, shop_domain: str, access_token: str):
        self.shop_domain = shop_domain
        self.access_token = access_token
        self.base_url = f"https://{shop_domain}/admin/api/2025-01/graphql.json"
        
    def execute(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute GraphQL query or mutation"""
        headers = {
            "X-Shopify-Access-Token": self.access_token,
            "Content-Type": "application/json"
        }
        
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
            
        try:
            response = requests.post(
                self.base_url,
                headers=headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"errors": [{"message": f"Request failed: {str(e)}"}]}

def load_graphql_from_file(file_path: str) -> str:
    """Load GraphQL query/mutation from file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read().strip()
    except FileNotFoundError:
        raise FileNotFoundError(f"GraphQL file not found: {file_path}")
    except Exception as e:
        raise Exception(f"Error reading GraphQL file: {str(e)}")

def load_variables_from_file(file_path: str) -> Optional[Dict[str, Any]]:
    """Load variables from JSON file"""
    if not file_path or not os.path.exists(file_path):
        return None
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: Could not load variables file: {str(e)}")
        return None

def format_response(response: Dict[str, Any]) -> str:
    """Format GraphQL response for display"""
    return json.dumps(response, indent=2, ensure_ascii=False)

def main():
    parser = argparse.ArgumentParser(
        description="Execute GraphQL queries and mutations against Shopify Admin API"
    )
    parser.add_argument(
        "graphql_file",
        help="Path to GraphQL file containing query or mutation"
    )
    parser.add_argument(
        "--variables", "-v",
        help="Path to JSON file containing GraphQL variables"
    )
    parser.add_argument(
        "--query", "-q",
        help="GraphQL query/mutation string (alternative to file)"
    )
    parser.add_argument(
        "--shop-domain",
        help="Shopify shop domain (overrides environment variable)"
    )
    parser.add_argument(
        "--access-token",
        help="Shopify access token (overrides environment variable)"
    )
    parser.add_argument(
        "--output", "-o",
        help="Output file path (prints to stdout if not specified)"
    )
    
    args = parser.parse_args()
    
    # Get credentials
    shop_domain = args.shop_domain or os.getenv("SHOPIFY_SHOP_DOMAIN")
    access_token = args.access_token or os.getenv("SHOPIFY_ADMIN_TOKEN")
    
    if not shop_domain or not access_token:
        print("Error: SHOPIFY_SHOP_DOMAIN and SHOPIFY_ADMIN_TOKEN must be provided")
        print("Either set environment variables or use --shop-domain and --access-token flags")
        sys.exit(1)
    
    # Get GraphQL query/mutation
    if args.query:
        graphql_query = args.query
    else:
        try:
            graphql_query = load_graphql_from_file(args.graphql_file)
        except Exception as e:
            print(f"Error: {str(e)}")
            sys.exit(1)
    
    # Load variables if provided
    variables = load_variables_from_file(args.variables) if args.variables else None
    
    # Execute GraphQL
    client = ShopifyGraphQLClient(shop_domain, access_token)
    print(f"Executing GraphQL against {shop_domain}...")
    
    if variables:
        print(f"Using variables: {json.dumps(variables, indent=2)}")
    
    response = client.execute(graphql_query, variables)
    
    # Format and output response
    formatted_response = format_response(response)
    
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(formatted_response)
        print(f"Response written to: {args.output}")
    else:
        print("\nResponse:")
        print(formatted_response)
    
    # Check for errors
    if "errors" in response:
        print(f"\n⚠️  GraphQL execution completed with errors")
        sys.exit(1)
    else:
        print(f"\n✅ GraphQL execution successful")

if __name__ == "__main__":
    main()