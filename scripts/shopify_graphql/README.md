# Shopify GraphQL Script

A command-line tool for executing GraphQL queries and mutations against Shopify's Admin API.

## Features

- Execute GraphQL queries and mutations from files
- Support for GraphQL variables via JSON files
- Command-line interface with flexible options
- Automatic error handling and response formatting
- Uses environment variables for credentials

## Usage

### Basic Usage

```bash
# Execute a GraphQL query from file
python scripts/shopify_graphql/main.py scripts/shopify_graphql/queries/shopify_functions.graphql

# Execute with variables
python scripts/shopify_graphql/main.py query.graphql --variables variables.json

# Execute inline GraphQL
python scripts/shopify_graphql/main.py --query "query { shop { name } }"

# Save output to file
python scripts/shopify_graphql/main.py query.graphql --output response.json
```

### Environment Variables

The script uses the same environment variables as other scripts in this project:

- `SHOPIFY_SHOP_DOMAIN`: Your Shopify shop domain (e.g., "yourshop.myshopify.com")
- `SHOPIFY_ADMIN_TOKEN`: Your Shopify Admin API access token

### Command Line Options

```
positional arguments:
  graphql_file          Path to GraphQL file containing query or mutation

optional arguments:
  -h, --help            show this help message and exit
  --variables, -v       Path to JSON file containing GraphQL variables
  --query, -q           GraphQL query/mutation string (alternative to file)
  --shop-domain         Shopify shop domain (overrides environment variable)
  --access-token        Shopify access token (overrides environment variable)
  --output, -o          Output file path (prints to stdout if not specified)
```

## Example Files

### Query Example (`queries/shopify_functions.graphql`)
```graphql
query {
  shopifyFunctions(first: 25) {
    nodes {
      app {
        title
      }
      apiType
      title
      id
    }
  }
}
```

### Mutation Example (`mutations/create_discount_code.graphql`)
```graphql
mutation {
  discountCodeAppCreate(codeAppDiscount: {
    title: "Newcop 10€ newsletter discount",
    code: "NEWCOP10",
    startsAt: "2025-01-01",
    functionId: "b277d742-68ae-4ab2-853e-a015367aebd3",
    discountClasses: [PRODUCT, ORDER, SHIPPING]
  }) {
    codeAppDiscount {
      discountId
    }
  }
}
```

### Variables Example (`variables.json`)
```json
{
  "first": 10,
  "title": "My Custom Title",
  "functionId": "your-function-id-here"
}
```

## Directory Structure

```
scripts/shopify_graphql/
├── main.py                    # Main GraphQL script
├── README.md                  # This documentation
├── queries/                   # Example queries
│   └── shopify_functions.graphql
└── mutations/                 # Example mutations
    └── create_discount_code.graphql
```

## Error Handling

The script provides comprehensive error handling:

- Network errors and timeouts
- Invalid GraphQL syntax
- Missing files or credentials
- Shopify API errors
- JSON parsing errors

Errors are clearly displayed with descriptive messages, and the script exits with appropriate status codes.

## Response Format

Responses are formatted as pretty-printed JSON for readability. The script also displays:

- Success/error status indicators (✅/⚠️)
- Request cost information from Shopify
- Execution feedback

## Integration

This script integrates seamlessly with the existing project structure:

- Uses the same `.env` file for credentials
- Follows the same logging and error patterns
- Can be easily integrated into the main CLI menu if needed