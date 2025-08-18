# Dynamic Collection Builder

This script builds a Shopify dynamic collection based on Airtable sales data from Spain (last 90 days).

## Features

- Fetches sales data from Airtable
- Filters products by brand keywords (Nike, Air Jordan, Adidas, Yeezy, New Balance, Asics, Puma)
- Excludes products with "retail" tags
- Only includes products with trimestre sales >= 5
- Updates Shopify collection automatically

## Setup

1. Create and activate virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment (optional):
```bash
cp .env.example .env
# Edit .env with your actual values
```

## Usage

1. Activate virtual environment:
```bash
source venv/bin/activate
```

2. Test Airtable data first (without modifying Shopify):
```bash
python test_airtable.py
```

3. Run the complete script:
```bash
python main.py
```

## Virtual Environment Commands

- Create environment: `python3 -m venv venv`
- Activate environment: `source venv/bin/activate`
- Deactivate environment: `deactivate`
- Install packages: `pip install -r requirements.txt`

The script will:
1. Analyze Airtable data structure (first 10 records)
2. Fetch all Spain sales data
3. Filter products based on criteria
4. Update Shopify collection (ID: 650542809429)

## Configuration

The script uses hardcoded tokens and IDs as specified:
- Airtable Token: `pat8Pg6d8OumFPpOO.4b99b41e37fd0c93f5da770a6073c66e21741f574910736a6184347f342faee6`
- Shopify Admin Token: `810ce8ca2df27f5dd5129dd4f6d2193e`
- Airtable Base: `appDE0y01TchMqX8N`
- Airtable Table: `tbljkyhWy5D6b65Im`
- Airtable View: `viwixRrDpjcAYwHId`
- Shopify Collection: `650542809429`

## Logging

Logs are output to both console and `dynamic_collection.log` file.

## Files

- `main.py` - Main script entry point
- `airtable_client.py` - Airtable API client
- `shopify_client.py` - Shopify API client  
- `product_filter.py` - Product filtering logic
- `models.py` - Data models for records
- `requirements.txt` - Python dependencies# newcop-tools
