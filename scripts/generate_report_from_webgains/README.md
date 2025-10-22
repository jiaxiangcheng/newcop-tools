# Webgains Report Enricher

This tool enriches Webgains affiliate sales reports with Shopify order data including customer information, order history, and customer journey tracking.

## Directory Structure

```
generate_report_from_webgains/
├── webgains_reports/           # Place your Webgains Excel files here
└── enriched_webgains_reports/  # Enriched reports will be saved here
```

## Usage

### Option 1: Main Menu (Recommended)

```bash
source venv/bin/activate
python main.py
# Select option 4: 📊 Webgains Report Enricher
```

The menu will:
1. Show you all Excel files in `webgains_reports/` directory
2. Let you select which file to process (1, 2, 3, etc.)
3. Ask for processing options (dry-run, limit)
4. Save enriched report to `enriched_webgains_reports/`

### Option 2: Command Line

**Single File Processing:**
```bash
# Process a specific file
python scripts/generate_report_from_webgains/main.py -i path/to/file.xlsx

# With options
python scripts/generate_report_from_webgains/main.py -i file.xlsx --dry-run --limit 10
```

**Batch Processing:**
```bash
# Process all files in webgains_reports/ directory
python scripts/generate_report_from_webgains/main.py --batch

# With custom directories
python scripts/generate_report_from_webgains/main.py --batch \
  --input-dir ./my_reports \
  --output-dir ./enriched
```

## Features

### Data Enrichment

For each order in the Webgains report, the tool fetches from Shopify:

1. **Order Information**
   - Order Creation Date/Time
   - Product Names (comma-separated for multiple products)
   - Variant Names (comma-separated for multiple variants)
   - Product SKUs (comma-separated)
   - Financial Status (e.g., PAID, REFUNDED, PARTIALLY_REFUNDED)
   - Fulfillment Status (e.g., FULFILLED, UNFULFILLED)
   - Cancellation Status (Yes/No)
   - Order Status Notes (highlights CANCELLED or REFUNDED orders)

2. **Customer Information**
   - Email
   - First Name
   - Last Name
   - Phone
   - Shipping Country

3. **Order History**
   - Customer Type: "First Order" or "Repeat Customer (Order #N)"
   - Total number of orders for this customer

4. **Customer Journey**
   - First Visit Source
   - UTM Parameters (source, medium, campaign, term, content)

### Order Reference Handling

The tool supports two formats:
- **Order Number** (<8 digits, e.g., "79365" or "11876")
  - Searches Shopify orders by name (#79365, #DB11876, etc.)
- **Shopify ID** (13 digits, e.g., "6195849527365")
  - Direct lookup using Shopify GID

### Output

Enriched Excel file with **three sheets**:

**Sheet 1: Analysis Summary**
- Statistical analysis and insights from the report data:
  - **Country Distribution**: Order count and percentage by country
  - **Commission Type Distribution**: Retail vs Resell breakdown
  - **Customer Type Distribution**: New customers (first order) vs Repeat customers
  - **Top Products Distribution**: Top 20 products by order count (grouped by product name, not variant)
  - **Orders Requiring Review**: 🆕 Clickable list of all orders that need attention
    - Shows order reference, issue type, financial status, and fulfillment status
    - 🔴 Red highlighted: Urgent issues (unfulfilled orders)
    - 🟡 Yellow highlighted: Warnings (payment issues)
    - **Hyperlinks**: Click on order reference to jump directly to the order in Newcop Data sheet

**Sheet 2: Newcop Data** (Enriched Data with Conditional Formatting)
- All original Webgains columns plus enriched Shopify data:
- Product Names (comma-separated if multiple products)
- Variant Names (comma-separated if multiple variants, with "EU -" suffix removed)
- Product SKUs (comma-separated)
- Financial Status (e.g., PAID, REFUNDED, PARTIALLY_REFUNDED)
- Fulfillment Status (e.g., FULFILLED, UNFULFILLED)
- Is Cancelled (Yes/No)
- Order Status Notes (highlights CANCELLED or REFUNDED orders)
- Customer Email, First Name, Last Name, Phone
- Customer Type (First Order or Repeat Customer)
- Order Number for Customer
- First Visit Source
- UTM Source, Medium, Campaign, Term, Content
- Error (if order lookup failed)

**Conditional Formatting:**
- 🟨 **Yellow rows**: Orders where Financial Status is NOT "PAID" (requires review)
- 🟥 **Red rows**: Orders where Fulfillment Status is "UNFULFILLED" (urgent - overrides yellow)

**Sheet 3: Original Webgains Data**
- Contains all original Webgains columns exactly as provided
- Affiliate, Sale, Commission, Override, Date & Time, Order Reference, Country, Commission Type, %

## Examples

### Example 1: Process Single File from Menu

1. Put your Webgains report in `webgains_reports/`
2. Run: `python main.py`
3. Select option 4
4. Select option 1 (Process file from directory)
5. Choose the file number (e.g., "1" for first file)
6. Press Enter to process all records (or enter a number to limit)
7. Press "N" for full processing (or "y" for dry-run)
8. Find enriched report in `enriched_webgains_reports/`

### Example 2: Batch Process All Files

1. Put multiple Webgains reports in `webgains_reports/`
2. Run: `python main.py`
3. Select option 4
4. Select option 3 (Batch process all files)
5. Configure options
6. All files will be processed and saved to `enriched_webgains_reports/`

### Example 3: Command Line with Limit

```bash
# Test with first 10 records only
python scripts/generate_report_from_webgains/main.py \
  -i webgains_reports/my_report.xlsx \
  --limit 10
```

## Configuration

The tool uses environment variables from `.env`:
- `SHOPIFY_ADMIN_TOKEN`: Your Shopify Admin API token
- `SHOPIFY_SHOP_DOMAIN`: Your Shopify shop domain

## Troubleshooting

### Orders Not Found

If many orders are not found, it could be because:
1. Order references in Webgains report don't match Shopify orders
2. Orders have been deleted from Shopify
3. Orders are from a different Shopify store

The tool will continue processing and mark failed orders in the "Error" column.

### Performance

- Processing speed: ~0.5-1 second per order
- Uses concurrent API requests (max 5 parallel)
- Batch processing handles multiple files automatically

## Technical Details

### GraphQL Query

The tool uses Shopify GraphQL API to fetch order details:

```graphql
query getOrderDetails($orderId: ID!) {
  order(id: $orderId) {
    id
    name
    createdAt
    displayFinancialStatus
    displayFulfillmentStatus
    cancelledAt
    lineItems(first: 10) {
      edges {
        node {
          title
          variantTitle
          sku
          quantity
        }
      }
    }
    customer {
      id
      email
      firstName
      lastName
      phone
      numberOfOrders
      defaultAddress {
        countryCode
      }
    }
    customerJourneySummary {
      firstVisit {
        source
        sourceDescription
        utmParameters {
          source
          medium
          campaign
          term
          content
        }
      }
    }
  }
}
```

### Order Name Search

For order numbers, the tool tries multiple patterns:
1. `name:#79365` (standard format)
2. `name:79365` (plain number)
3. `name:DB79365` (DB prefix)
4. `name:*79365` (wildcard)
