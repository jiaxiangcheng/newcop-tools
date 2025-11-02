# Airtable Files Downloader

This tool downloads PDF files (or other files) from URLs stored in Airtable CSV exports.

## Usage

### Option 1: Main Menu (Recommended)

```bash
source venv/bin/activate
python main.py
# Select option 5: 📥 Airtable Files Downloader
```

The menu will guide you through:
1. Specifying the CSV file path
2. Specifying the output directory
3. Options for dry-run and limiting downloads

### Option 2: Command Line

**Basic usage:**
```bash
python scripts/massive_download_airtable_files/main.py
```

**With custom paths:**
```bash
python scripts/massive_download_airtable_files/main.py \
  -i path/to/your/file.csv \
  -o path/to/output/directory
```

**Dry run (preview without downloading):**
```bash
python scripts/massive_download_airtable_files/main.py --dry-run
```

**Limit downloads for testing:**
```bash
python scripts/massive_download_airtable_files/main.py --limit 10
```

**Custom column name:**
```bash
python scripts/massive_download_airtable_files/main.py -c "AttachmentColumn"
```

## Features

### Smart Download Management
- **Resume Support**: Skips files that already exist
- **Retry Logic**: Automatically retries failed downloads (up to 3 attempts)
- **Progress Bar**: Shows real-time download progress with `tqdm`
- **Timeout Handling**: 30-second timeout per file

### Error Handling
- Individual file failures don't stop the entire batch
- Comprehensive error logging to `logs/airtable_downloader.log`
- Summary report shows success/failure counts

### File Handling
- Automatic PDF extension detection
- Query parameter removal from URLs
- Organized output directory structure

## Configuration

### Default Paths

- **CSV File**: `scripts/massive_download_airtable_files/Items-INVOICE.csv`
- **Output Directory**: `scripts/massive_download_airtable_files/facturas_pdf`
- **Column Name**: `Factura`

### CSV Format

Your CSV file should have a column containing URLs to download. Example:

```csv
Order,Factura,Date
12345,https://example.com/file1.pdf,2024-01-01
12346,https://example.com/file2.pdf,2024-01-02
```

## Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `-i, --input` | Path to CSV file | `scripts/massive_download_airtable_files/Items-INVOICE.csv` |
| `-o, --output` | Output directory for downloads | `scripts/massive_download_airtable_files/facturas_pdf` |
| `-c, --column` | Name of column containing URLs | `Factura` |
| `--dry-run` | Preview what would be downloaded | `False` |
| `--limit` | Limit number of files to download | None (all files) |

## Examples

### Example 1: Basic Download

```bash
python scripts/massive_download_airtable_files/main.py
```

### Example 2: Custom CSV and Output

```bash
python scripts/massive_download_airtable_files/main.py \
  -i invoices/march_2024.csv \
  -o downloads/march_pdfs
```

### Example 3: Test with Limited Downloads

```bash
python scripts/massive_download_airtable_files/main.py --dry-run --limit 5
```

### Example 4: Different Column Name

If your CSV has a different column name for URLs:

```bash
python scripts/massive_download_airtable_files/main.py -c "PDF_Link"
```

## Logging

The tool creates detailed logs in `logs/airtable_downloader.log`:
- INFO level: Download progress and summary
- WARNING level: Retry attempts
- ERROR level: Failed downloads with reasons

Console output shows:
- Progress bar during download
- Final summary with success/failure counts
- List of failed URLs (if any)

## Troubleshooting

### Common Issues

**"CSV file not found"**
- Verify the CSV file path is correct
- Use absolute path if relative path doesn't work

**"Column 'Factura' not found in CSV"**
- Check your CSV column names
- Use `-c` option to specify the correct column name

**Downloads timing out**
- Check your internet connection
- Large files may need longer timeout (modify `self.timeout` in code)

**Permission denied on output directory**
- Ensure you have write permissions
- Try a different output directory

### Network Issues

The tool handles common network issues automatically:
- **Connection errors**: Retries up to 3 times
- **HTTP errors**: Logs the status code and error message
- **Timeouts**: Retries with exponential backoff

## Integration

This tool is integrated into the main CLI menu system. It follows the same architecture patterns as other tools in this repository:
- Logging to `logs/` directory
- Command-line argument support
- Dry-run mode for testing
- Comprehensive error handling

## Dependencies

Required packages (install via `pip install -r requirements.txt`):
- `pandas>=2.0.0` - CSV processing
- `requests>=2.31.0` - HTTP downloads
- `tqdm>=4.65.0` - Progress bars
