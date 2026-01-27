# Set Product Type Script

This script automatically sets product types based on collection membership and product tags.

## Overview

The script processes three collections with specific type rules:

1. **Collection 639759778133** (Accessories Collection)
   - All products → Type: `Accessories`

2. **Collection 639750963541** (Sneakers Collection)
   - Products with "retail" tag → Type: `Retail Sneakers`
   - Products without "retail" tag → Type: `Resell Sneakers`

3. **Collection 639759647061** (Clothing Collection)
   - All products → Type: `Clothing`

## Product Types

The script sets the following product types:
- `Accessories`
- `Retail Sneakers`
- `Resell Sneakers`
- `Clothing`

## Usage

### Via Main CLI Launcher (Recommended)

```bash
python main.py
# Select option 10: 🏷️ Set Product Type
# Then choose from:
# 1. Process All 3 Configured Collections
# 2. Process Specific Collection
# 3. Dry Run All 3 Collections
# 4. Dry Run Specific Collection
# 5. List Products with Empty Type (NEW!)
```

### Direct Execution

```bash
# Activate virtual environment
source venv/bin/activate

# Process all collections (default)
python scripts/set_product_type/main.py

# Process a specific collection
python scripts/set_product_type/main.py --collection 639759778133

# Dry run (preview changes without updating)
python scripts/set_product_type/main.py --dry-run

# Dry run for specific collection
python scripts/set_product_type/main.py --collection 639750963541 --dry-run

# List all products with empty product type (NEW!)
python scripts/set_product_type/main.py --list-empty
```

## Command Line Arguments

| Argument | Description |
|----------|-------------|
| `--collection COLLECTION_ID` | Process only a specific collection ID (default: process all collections) |
| `--dry-run` | Analyze what would be updated without making actual changes |
| `--list-empty` | List all ACTIVE products with empty or null product type |

## Features

### Smart Type Detection
- Automatically determines the correct type based on collection and tags
- Case-insensitive tag matching (e.g., "Retail", "RETAIL", "retail" all match)
- Skips products that already have the correct type set

### Find Products with Empty Type
- Query all **ACTIVE** products in your Shopify store
- Filter and list **ACTIVE** products with empty or null product type
- **Automatically exports results to Excel file** in `data/` directory
- Useful for identifying active products that need type assignment

### Concurrent Processing
- Updates up to 5 products simultaneously for faster processing
- Built-in rate limiting to avoid API throttling
- Individual product failures don't stop the entire process

### Comprehensive Logging
- Real-time progress updates during execution
- Detailed error messages with product information
- Summary statistics for each collection and overall results
- Logs saved to `logs/set_product_type.log`

## Example Output

### Processing Collections

```
============================================================
🚀 STARTING PRODUCT TYPE SYNC
============================================================
Mode: ALL COLLECTIONS
Dry run: False
============================================================
📦 Processing collection: Accessories Collection (ID: 639759778133)
Found 150 products in collection 639759778133
📝 Products to update: 45
⏭️  Products to skip: 105
🔄 Updating 45 products...
✅ [1/45] Updated: Product Name (Old Type → Accessories)
...
============================================================
📊 COLLECTION SUMMARY: Accessories Collection
============================================================
Total products: 150
✅ Updated: 45
⏭️  Skipped: 105
❌ Failed: 0
============================================================
```

### Listing Products with Empty Type

```
============================================================
🚀 STARTING PRODUCT TYPE SYNC
============================================================
Mode: LIST ACTIVE PRODUCTS WITH EMPTY TYPE
============================================================
🔍 Fetching all ACTIVE products with empty product type...
============================================================
Checked 250 products, found 12 with empty type...
Checked 500 products, found 28 with empty type...
...
============================================================
✅ Checked 1728 ACTIVE products total
📊 Found 45 ACTIVE products with empty product type
============================================================

📋 ACTIVE Products with empty product type:
------------------------------------------------------------
1. ID: 123456 | Title: Product Name 1 | Handle: product-name-1
2. ID: 123457 | Title: Product Name 2 | Handle: product-name-2
...
------------------------------------------------------------

✅ Total: 45 ACTIVE products with empty type

📊 Exporting to Excel...
📝 Creating Excel file: data/products_empty_type_20250127_143022.xlsx
✅ Excel file created successfully: data/products_empty_type_20250127_143022.xlsx
✅ Excel file exported to: data/products_empty_type_20250127_143022.xlsx
```

### Excel Export Format

The exported Excel file contains the following columns:
- **Product ID**: Shopify product ID (numeric)
- **Product Title**: Product name
- **Handle**: Product URL handle
- **Product Type**: Current product type (empty for these products)
- **Status**: Product status (ACTIVE)

The file is saved to `data/products_empty_type_YYYYMMDD_HHMMSS.xlsx` with a timestamp in the filename.

## Error Handling

- **Individual Isolation**: Failed product updates don't stop the entire process
- **Comprehensive Logging**: All errors logged with product details
- **Graceful Continuation**: Process continues even if individual products fail
- **Rate Limiting**: Automatic retry logic for 429 responses

## Required Shopify Permissions

- `read_products`: To fetch product and collection information
- `write_products`: To update product type field

## Technical Details

### GraphQL Queries

The script uses GraphQL to:
1. Fetch product details (type, tags)
2. Update product type field

### Product Type Update Mutation

```graphql
mutation updateProductType($input: ProductInput!) {
  productUpdate(input: $input) {
    product {
      id
      productType
    }
    userErrors {
      field
      message
    }
  }
}
```

## Performance

- **Fetch Speed**: ~50 products per GraphQL request
- **Concurrent Updates**: Max 5 products simultaneously
- **Typical Runtime**: ~2-5 minutes for 500+ products across all collections
- **API Efficiency**: Single GraphQL mutation per product

## Troubleshooting

### Common Issues

- **Collection Not Found**: Verify collection IDs are correct
- **Permission Errors**: Ensure `write_products` permission is granted
- **Tag Not Detected**: Tags are matched case-insensitively, ensure "retail" tag exists
- **Type Not Updating**: Check if product already has the correct type (will be skipped)

### Log Files

Check `logs/set_product_type.log` for detailed execution logs and error messages.
