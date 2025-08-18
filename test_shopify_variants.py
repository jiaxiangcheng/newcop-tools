#!/usr/bin/env python3
"""
Test different Shopify API configurations to find the working combination
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")

print(f"Testing token: ...{admin_token[-10:] if admin_token else 'NONE'}")
print(f"Testing domain: {shop_domain}")

# Different domain formats to try
domain_variants = [
    shop_domain,
    shop_domain.replace('.myshopify.com', ''),
    'newcopstore',
    'newcopstore.myshopify.com'
]

# Different API versions to try
api_versions = ['2023-10', '2023-07', '2023-04', '2024-01', '2024-04']

headers = {
    "X-Shopify-Access-Token": admin_token,
    "Content-Type": "application/json"
}

for domain in domain_variants:
    for version in api_versions:
        # Clean domain
        clean_domain = domain.replace('.myshopify.com', '')
        url = f"https://{clean_domain}.myshopify.com/admin/api/{version}/shop.json"
        
        try:
            print(f"\nTesting: {url}")
            response = requests.get(url, headers=headers, timeout=10)
            print(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                shop_data = response.json().get('shop', {})
                print(f"✅ SUCCESS! Shop: {shop_data.get('name')}")
                print(f"   Domain: {clean_domain}.myshopify.com")
                print(f"   API Version: {version}")
                print(f"   Shop ID: {shop_data.get('id')}")
                break
            else:
                print(f"   Error: {response.text[:100]}...")
                
        except Exception as e:
            print(f"   Exception: {e}")
    else:
        continue
    break