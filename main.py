#!/usr/bin/env python3
"""
Newcop Backend Jobs CLI Launcher

A centralized CLI tool to manage various Shopify and Airtable automation scripts.
Each script functionality is organized in its own module for better maintainability.
"""

import os
import sys
from typing import Dict, Callable
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def format_interval_display(interval_str: str) -> str:
    """Format interval string for display"""
    try:
        interval_str = interval_str.strip().lower()
        
        if interval_str.endswith('h'):
            # Hours format: "6h", "2h", etc.
            hours = int(interval_str[:-1])
            return f"{hours} hour{'s' if hours != 1 else ''}"
        elif interval_str.endswith('m'):
            # Minutes format: "30m", "15m", etc.
            minutes = int(interval_str[:-1])
            return f"{minutes} minute{'s' if minutes != 1 else ''}"
        elif interval_str.endswith('min'):
            # Minutes format: "30min", "15min", etc.
            minutes = int(interval_str[:-3])
            return f"{minutes} minute{'s' if minutes != 1 else ''}"
        else:
            # Default: assume hours if no unit specified
            hours = int(interval_str)
            return f"{hours} hour{'s' if hours != 1 else ''}"
            
    except (ValueError, AttributeError):
        return "2 hours"  # Default fallback

def show_banner():
    """Display the application banner"""
    print("=" * 60)
    print("🏪 Newcop Backend Jobs - CLI Launcher")
    print("=" * 60)
    print("Manage Shopify and Airtable automation scripts")
    print("=" * 60)

def show_menu():
    inventory_sync_interval_hours = os.getenv("INVENTORY_SYNC_INTERVAL_HOURS", "6")
    customer_marketing_sync_interval_hours = os.getenv("CUSTOMER_MARKETING_SYNC_INTERVAL_HOURS", "6")
    customer_order_history_interval_hours = os.getenv("CUSTOMER_ORDER_HISTORY_INTERVAL_HOURS", "24")
    """Display the main menu options"""
    print("\n📋 Available Scripts:")
    print("1. 🔄 Dynamic Collections - Auto-update Shopify collections based on Airtable sales data")
    print(f"2. 📦 Variant Sync - Sync inventory quantities, price or compare price to variant metafields every {inventory_sync_interval_hours} hours")
    print(f"3. 👥 Customer Marketing Sync - Sync customer marketing preferences to metafields every {customer_marketing_sync_interval_hours} hours")
    print("4. 📊 Webgains Report Enricher - Enrich Webgains sales reports with Shopify order data")
    print("5. 📥 Airtable Files Downloader - Download PDF files from Airtable CSV export")
    print(f"6. 📈 Customer Order History - Analyze and sync customer order counts (runs daily at 00:00)")
    print("7. 🏅 Best Seller Badge - Assign best seller badges to top products (runs monthly on 1st)")
    print("8. 💰 Product Discounts - Calculate and sync product discount percentages (runs daily at 00:00)")
    print("9. 📝 Set Variants Metafield - Sync product variant names to custom.variants metafield")
    print("10. 🏷️  Set Product Type - Set product types based on collection and tags")
    print("11. 💳 Scalapay Orders - Export all orders paid with Scalapay to Excel")
    print("12. 🏷️  Inventory Tags - Set instore-online/instore-only tags based on inventory")
    print("13. 📋 Duplicate Products - Duplicate in-stock products from a collection")
    print("14. 👥 Get Customers - Export customers for Meta Custom Audience CSV")
    print("15. 🏷️  Bulk Product Type - Batch operations on product types (list/find/replace)")
    print("16. 🧹 Delete Catalog Fixed Prices - Clear all fixed prices in a Catalog's PriceList")
    print("\n0. 🚪 Exit")
    print("-" * 60)

def run_dynamic_collections() -> bool:
    """Run the dynamic collections script with user mode selection"""
    try:
        print("\n🔄 Starting Dynamic Collections Script...")
        print("=" * 60)

        # Get interval from environment variable
        dynamic_collections_interval_days = os.getenv("DYNAMIC_COLLECTIONS_INTERVAL_DAYS", "1")

        # Ask user for execution mode
        print("Select execution mode:")
        print("1. 🔧 Manual Sync (run once for all collections)")
        print(f"2. 🔄 Scheduled Mode (run every {dynamic_collections_interval_days} days)")
        print("3. 🧪 Dry Run (analyze changes only)")
        print("4. 🎯 Specific Collection (run once for a specific collection ID)")
        print("0. ↩️  Return to main menu")

        while True:
            try:
                mode_choice = input("\n🔸 Choose mode: ").strip()

                if mode_choice == "0":
                    return True  # Return to main menu
                elif mode_choice == "1":
                    # Manual sync - all collections
                    from scripts.job_dynamic_collections.main import run_dynamic_collections
                    success = run_dynamic_collections(mode="manual", dry_run=False, collection_id=None)
                    break
                elif mode_choice == "2":
                    # Scheduled mode
                    print(f"\n⚠️  Scheduled mode will run continuously every {dynamic_collections_interval_days} days. Press Ctrl+C to stop.")
                    confirm = input("Continue? (y/N): ").strip().lower()
                    if confirm in ['y', 'yes']:
                        from scripts.job_dynamic_collections.main import run_dynamic_collections
                        success = run_dynamic_collections(mode="scheduled", dry_run=False, collection_id=None)
                    else:
                        success = True  # User cancelled
                    break
                elif mode_choice == "3":
                    # Dry run
                    from scripts.job_dynamic_collections.main import run_dynamic_collections
                    success = run_dynamic_collections(mode="manual", dry_run=True, collection_id=None)
                    break
                elif mode_choice == "4":
                    # Specific collection ID
                    collection_id = input("\n🎯 Enter collection ID: ").strip()
                    if not collection_id:
                        print("❌ No collection ID provided.")
                        continue

                    print(f"\n🎯 Running for collection ID: {collection_id}")
                    from scripts.job_dynamic_collections.main import run_dynamic_collections
                    success = run_dynamic_collections(mode="manual", dry_run=False, collection_id=collection_id)
                    break
                else:
                    print(f"❌ Invalid choice: '{mode_choice}'. Please select 0-4.")
                    continue

            except KeyboardInterrupt:
                print("\n⏹️  Operation cancelled by user")
                return True

        print("\n" + "=" * 60)
        if success:
            print("✅ Dynamic Collections Script completed successfully!")
        else:
            print("❌ Dynamic Collections Script completed with errors.")

        return success

    except ImportError as e:
        print(f"❌ Error importing dynamic collections script: {e}")
        print("💡 Make sure you have installed the required dependencies: pip install APScheduler")
        return False
    except Exception as e:
        print(f"❌ Unexpected error running dynamic collections: {e}")
        return False

def run_inventory_sync() -> bool:
    """Run the inventory sync script with user mode selection"""
    try:
        print("\n📦 Starting Inventory Sync Script...")
        print("=" * 60)
        
        # Get interval from environment variable
        inventory_sync_interval = os.getenv("INVENTORY_SYNC_INTERVAL", "2h")
        interval_display = format_interval_display(inventory_sync_interval)
        
        # Ask user for execution mode
        print("Select execution mode:")
        print("1. 🔧 Manual Sync (run once)")
        print(f"2. 🔄 Scheduled Mode (run every {interval_display})")
        print("3. 🧪 Dry Run (analyze changes only)")
        print("0. ↩️  Return to main menu")
        
        while True:
            try:
                mode_choice = input("\n🔸 Choose mode: ").strip()
                
                if mode_choice == "0":
                    return True  # Return to main menu
                elif mode_choice == "1":
                    # Manual sync
                    from scripts.job_inventory_sync.main import run_inventory_sync
                    success = run_inventory_sync(mode="manual", dry_run=False, sync_fields=None, sync_config=None)
                    break
                elif mode_choice == "2":
                    # Scheduled mode
                    print(f"\n⚠️  Scheduled mode will run continuously every {interval_display}. Press Ctrl+C to stop.")
                    confirm = input("Continue? (y/N): ").strip().lower()
                    if confirm in ['y', 'yes']:
                        from scripts.job_inventory_sync.main import run_inventory_sync
                        success = run_inventory_sync(mode="scheduled", dry_run=False, sync_fields=None, sync_config=None)
                    else:
                        success = True  # User cancelled
                    break
                elif mode_choice == "3":
                    # Dry run
                    from scripts.job_inventory_sync.main import run_inventory_sync
                    success = run_inventory_sync(mode="manual", dry_run=True, sync_fields=None, sync_config=None)
                    break
                else:
                    print(f"❌ Invalid choice: '{mode_choice}'. Please select 0-3.")
                    continue
                    
            except KeyboardInterrupt:
                print("\n⏹️  Operation cancelled by user")
                return True
        
        print("\n" + "=" * 60)
        if success:
            print("✅ Inventory Sync Script completed successfully!")
        else:
            print("❌ Inventory Sync Script completed with errors.")
        
        return success
        
    except ImportError as e:
        print(f"❌ Error importing inventory sync script: {e}")
        print("💡 Make sure you have installed the required dependencies: pip install APScheduler")
        return False
    except Exception as e:
        print(f"❌ Unexpected error running inventory sync: {e}")
        return False

def run_customer_marketing_sync() -> bool:
    """Run the customer marketing sync script with user mode selection"""
    try:
        print("\n👥 Starting Customer Marketing Sync Script...")
        print("=" * 60)
        
        # Ask user for execution mode
        print("Select execution mode:")
        print("1. 🔧 Manual Sync (run once)")
        print("2. 🔄 Scheduled Mode (run every 6 hours)")
        print("3. 🧪 Dry Run (analyze changes only)")
        print("0. ↩️  Return to main menu")
        
        while True:
            try:
                mode_choice = input("\n🔸 Choose mode: ").strip()
                
                if mode_choice == "0":
                    return True  # Return to main menu
                elif mode_choice == "1":
                    # Manual sync
                    from scripts.customer_marketing_sync.main import run_customer_marketing_sync
                    success = run_customer_marketing_sync(mode="manual", dry_run=False)
                    break
                elif mode_choice == "2":
                    # Scheduled mode
                    print("\n⚠️  Scheduled mode will run continuously. Press Ctrl+C to stop.")
                    confirm = input("Continue? (y/N): ").strip().lower()
                    if confirm in ['y', 'yes']:
                        from scripts.customer_marketing_sync.main import run_customer_marketing_sync
                        success = run_customer_marketing_sync(mode="scheduled", dry_run=False)
                    else:
                        success = True  # User cancelled
                    break
                elif mode_choice == "3":
                    # Dry run
                    from scripts.customer_marketing_sync.main import run_customer_marketing_sync
                    success = run_customer_marketing_sync(mode="manual", dry_run=True)
                    break
                else:
                    print(f"❌ Invalid choice: '{mode_choice}'. Please select 0-3.")
                    continue
                    
            except KeyboardInterrupt:
                print("\n⏹️  Operation cancelled by user")
                return True
        
        print("\n" + "=" * 60)
        if success:
            print("✅ Customer Marketing Sync Script completed successfully!")
        else:
            print("❌ Customer Marketing Sync Script completed with errors.")
        
        return success
        
    except ImportError as e:
        print(f"❌ Error importing customer marketing sync script: {e}")
        print("💡 Make sure you have installed the required dependencies: pip install APScheduler")
        return False
    except Exception as e:
        print(f"❌ Unexpected error running customer marketing sync: {e}")
        return False

def run_webgains_report_enricher() -> bool:
    """Run the Webgains report enricher script"""
    try:
        from pathlib import Path
        from scripts.generate_report_from_webgains.main import WebgainsReportEnricher

        print("\n📊 Webgains Report Enricher")
        print("=" * 60)

        # Initialize enricher
        enricher = WebgainsReportEnricher()

        # Check for files in default directory
        input_dir = enricher.DEFAULT_INPUT_DIR
        output_dir = enricher.DEFAULT_OUTPUT_DIR

        # Find Excel files
        excel_files = []
        if input_dir.exists():
            excel_files = list(input_dir.glob("*.xlsx")) + list(input_dir.glob("*.xls"))

        # Ask user what they want to do
        print("\nSelect mode:")
        if excel_files:
            print(f"1. 📂 Process file(s) from: {input_dir}")
            print("2. 📄 Process specific file (enter path)")
            print("3. 🔄 Batch process all files")
            print("0. ↩️  Return to main menu")

            while True:
                try:
                    mode_choice = input("\n🔸 Choose mode: ").strip()

                    if mode_choice == "0":
                        return True
                    elif mode_choice in ["1", "2", "3"]:
                        break
                    else:
                        print(f"❌ Invalid choice: '{mode_choice}'. Please select 0-3.")
                except (EOFError, KeyboardInterrupt):
                    print("\n⏹️  Operation cancelled by user")
                    return True
        else:
            print(f"⚠️  No files found in {input_dir}")
            print("1. 📄 Process specific file (enter path)")
            print("0. ↩️  Return to main menu")

            while True:
                try:
                    mode_choice = input("\n🔸 Choose mode: ").strip()

                    if mode_choice == "0":
                        return True
                    elif mode_choice == "1":
                        mode_choice = "2"  # Map to "process specific file"
                        break
                    else:
                        print(f"❌ Invalid choice: '{mode_choice}'. Please select 0-1.")
                except (EOFError, KeyboardInterrupt):
                    print("\n⏹️  Operation cancelled by user")
                    return True

        # Handle mode selection
        input_file = None
        batch_mode = False

        if mode_choice == "1" and excel_files:
            # Show files and let user select
            print("\n📋 Available files:")
            print("-" * 60)
            for i, file in enumerate(excel_files, 1):
                print(f"{i}. {file.name}")
            print("-" * 60)

            while True:
                try:
                    file_choice = input(f"\n🔸 Select file (1-{len(excel_files)}): ").strip()
                    file_idx = int(file_choice) - 1
                    if 0 <= file_idx < len(excel_files):
                        input_file = str(excel_files[file_idx])
                        break
                    else:
                        print(f"❌ Invalid selection. Please enter 1-{len(excel_files)}")
                except ValueError:
                    print("❌ Please enter a valid number")
                except (EOFError, KeyboardInterrupt):
                    print("\n⏹️  Operation cancelled by user")
                    return True

        elif mode_choice == "2":
            # Manual file path input
            try:
                print("\nEnter the path to your Webgains Excel report file:")
                print("(Example: /path/to/Transacciones_newcop_2509.xlsx)")
                input_file = input("\n📥 Input file path: ").strip()

                if not input_file:
                    print("❌ No input file specified.")
                    return False
            except (EOFError, KeyboardInterrupt):
                print("\n⏹️  Operation cancelled by user")
                return True

        elif mode_choice == "3":
            batch_mode = True

        # Ask for merged output if batch mode
        merged = False
        if batch_mode:
            print("\n📦 Output format:")
            print("1. 📅 Separate files per month (default)")
            print("2. 📋 Single merged file (all data combined)")
            while True:
                try:
                    format_choice = input("\n🔸 Choose output format (1/2): ").strip()
                    if format_choice in ["", "1"]:
                        merged = False
                        break
                    elif format_choice == "2":
                        merged = True
                        break
                    else:
                        print(f"❌ Invalid choice: '{format_choice}'. Please select 1 or 2.")
                except (EOFError, KeyboardInterrupt):
                    print("\n⏹️  Operation cancelled by user")
                    return True

        # Validate environment
        if not enricher.validate_environment():
            return False

        # Initialize Shopify client
        if not enricher.initialize_shopify_client():
            return False

        # Ask for processing options
        print("\n⚙️  Processing options:")

        try:
            # Ask for optional limit
            print("\nProcess all records or limit to first N records?")
            print("(Enter a number or press Enter for all records)")
            limit_input = input("🔢 Limit (optional): ").strip()
            limit = None
            if limit_input and limit_input.isdigit():
                limit = int(limit_input)

            # Ask for dry run
            print("\nRun in dry-run mode (preview only, no API calls)?")
            dry_run_input = input("🧪 Dry run? (y/N): ").strip().lower()
            dry_run = dry_run_input in ['y', 'yes']
        except (EOFError, KeyboardInterrupt):
            print("\n⏹️  Operation cancelled by user")
            return True

        print("\n" + "=" * 60)
        print("Starting enrichment process...")
        print("=" * 60)

        # Process based on mode
        if batch_mode:
            # Batch process all files
            success = enricher.process_batch(
                input_dir=None,  # Use default
                output_dir=None,  # Use default
                dry_run=dry_run,
                limit=limit,
                merged=merged
            )
        else:
            # Process single file
            # Generate output filename
            input_path = Path(input_file)
            output_file = output_dir / f"{input_path.stem}_enriched{input_path.suffix}"

            success = enricher.process_report(
                input_file=input_file,
                output_file=str(output_file),
                dry_run=dry_run,
                limit=limit
            )

        print("\n" + "=" * 60)
        if success:
            print("✅ Webgains Report Enricher completed successfully!")
        else:
            print("❌ Webgains Report Enricher completed with errors.")

        return success

    except ImportError as e:
        print(f"❌ Error importing Webgains report enricher script: {e}")
        print("💡 Make sure you have installed the required dependencies: pip install openpyxl")
        return False
    except Exception as e:
        print(f"❌ Unexpected error running Webgains report enricher: {e}")
        return False

def run_airtable_downloader() -> bool:
    """Run the Airtable files downloader script"""
    try:
        print("\n📥 Starting Airtable Files Downloader...")
        print("=" * 60)

        from scripts.massive_download_airtable_files.main import AirtableFileDownloader

        # Default paths
        default_csv = "scripts/massive_download_airtable_files/Items-INVOICE.csv"
        default_output = "scripts/massive_download_airtable_files/facturas_pdf"

        # Ask user for options
        print(f"Default CSV file: {default_csv}")
        print(f"Default output directory: {default_output}")
        print()

        use_defaults = input("Use default paths? (Y/n): ").strip().lower()

        if use_defaults in ['', 'y', 'yes']:
            csv_path = default_csv
            output_dir = default_output
        else:
            csv_path = input(f"CSV file path [{default_csv}]: ").strip() or default_csv
            output_dir = input(f"Output directory [{default_output}]: ").strip() or default_output

        # Ask for dry run
        dry_run_choice = input("\nDry run (preview without downloading)? (y/N): ").strip().lower()
        dry_run = dry_run_choice in ['y', 'yes']

        # Ask for limit
        limit_choice = input("Limit number of files (leave empty for all): ").strip()
        limit = int(limit_choice) if limit_choice.isdigit() else None

        print()
        print("=" * 60)

        # Create downloader and run
        downloader = AirtableFileDownloader(
            csv_path=csv_path,
            output_dir=output_dir
        )

        result = downloader.download_all(dry_run=dry_run, limit=limit)

        print()
        print("=" * 60)

        if result.get("dry_run"):
            print("✅ Dry run completed successfully!")
            return True
        elif result["failed"] == 0:
            print("✅ All files downloaded successfully!")
            return True
        else:
            print(f"⚠️  Download completed with {result['failed']} failures.")
            return False

    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        print("💡 Make sure the CSV file exists at the specified path.")
        return False
    except ImportError as e:
        print(f"❌ Error importing downloader script: {e}")
        print("💡 Make sure you have installed the required dependencies: pip install pandas tqdm")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def run_customer_order_history() -> bool:
    """Run the customer order history script with user mode selection"""
    try:
        print("\n📈 Starting Customer Order History Script...")
        print("=" * 60)

        # Get interval from environment variable
        interval_hours = os.getenv("CUSTOMER_ORDER_HISTORY_INTERVAL_HOURS", "24")

        # Ask user for execution mode
        print("Select execution mode:")
        print("1. 🔧 Manual Sync (process all orders in view)")
        print(f"2. 🔄 Scheduled Mode (run daily at 00:00, process yesterday's orders)")
        print("3. 🧪 Dry Run (analyze yesterday's orders without updating)")
        print("4. ⚡ Force All (process all orders ignoring cache)")
        print("0. ↩️  Return to main menu")

        while True:
            try:
                mode_choice = input("\n🔸 Choose mode: ").strip()

                if mode_choice == "0":
                    return True  # Return to main menu
                elif mode_choice == "1":
                    # Manual sync - process ALL orders in view
                    from scripts.job_customer_order_history.main import run_customer_order_history
                    success = run_customer_order_history(mode="manual", dry_run=False, force_all=False, yesterday_only=False)
                    break
                elif mode_choice == "2":
                    # Scheduled mode - daily at 00:00, process yesterday's orders only
                    print(f"\n⚠️  Scheduled mode will run continuously daily at 00:00. Press Ctrl+C to stop.")
                    confirm = input("Continue? (y/N): ").strip().lower()
                    if confirm in ['y', 'yes']:
                        from scripts.job_customer_order_history.main import run_customer_order_history
                        success = run_customer_order_history(mode="scheduled", dry_run=False, force_all=False, yesterday_only=False)
                    else:
                        success = True  # User cancelled
                    break
                elif mode_choice == "3":
                    # Dry run - analyze yesterday's orders only
                    from scripts.job_customer_order_history.main import run_customer_order_history
                    success = run_customer_order_history(mode="manual", dry_run=True, force_all=False, yesterday_only=True)
                    break
                elif mode_choice == "4":
                    # Force all - process ALL orders ignoring cache
                    print("\n⚠️  This will process ALL records regardless of cache. Continue?")
                    confirm = input("Continue? (y/N): ").strip().lower()
                    if confirm in ['y', 'yes']:
                        from scripts.job_customer_order_history.main import run_customer_order_history
                        success = run_customer_order_history(mode="manual", dry_run=False, force_all=True, yesterday_only=False)
                    else:
                        success = True  # User cancelled
                    break
                else:
                    print(f"❌ Invalid choice: '{mode_choice}'. Please select 0-4.")
                    continue

            except KeyboardInterrupt:
                print("\n⏹️  Operation cancelled by user")
                return True

        print("\n" + "=" * 60)
        if success:
            print("✅ Customer Order History Script completed successfully!")
        else:
            print("❌ Customer Order History Script completed with errors.")

        return success

    except ImportError as e:
        print(f"❌ Error importing customer order history script: {e}")
        print("💡 Make sure you have installed the required dependencies: pip install APScheduler")
        return False
    except Exception as e:
        print(f"❌ Unexpected error running customer order history: {e}")
        return False

def run_best_seller_badge() -> bool:
    """Run the best seller badge script with user mode selection"""
    try:
        print("\n🏅 Starting Best Seller Badge Script...")
        print("=" * 60)

        # Ask user for execution mode
        print("Select execution mode:")
        print("1. 🔧 Manual Sync (run once)")
        print("2. 🔄 Scheduled Mode (run weekly on Sundays at 00:00)")
        print("3. 🧪 Dry Run (analyze changes only)")
        print("0. ↩️  Return to main menu")

        while True:
            try:
                mode_choice = input("\n🔸 Choose mode: ").strip()

                if mode_choice == "0":
                    return True  # Return to main menu
                elif mode_choice == "1":
                    # Manual sync
                    from scripts.job_assign_best_seller_badge.main import run_best_seller_badge
                    success = run_best_seller_badge(mode="manual", dry_run=False)
                    break
                elif mode_choice == "2":
                    # Scheduled mode
                    print("\n⚠️  Scheduled mode will run weekly on Sundays at 00:00. Press Ctrl+C to stop.")
                    confirm = input("Continue? (y/N): ").strip().lower()
                    if confirm in ['y', 'yes']:
                        from scripts.job_assign_best_seller_badge.main import run_best_seller_badge
                        success = run_best_seller_badge(mode="scheduled", dry_run=False)
                    else:
                        success = True  # User cancelled
                    break
                elif mode_choice == "3":
                    # Dry run
                    from scripts.job_assign_best_seller_badge.main import run_best_seller_badge
                    success = run_best_seller_badge(mode="manual", dry_run=True)
                    break
                else:
                    print(f"❌ Invalid choice: '{mode_choice}'. Please select 0-3.")
                    continue

            except KeyboardInterrupt:
                print("\n⏹️  Operation cancelled by user")
                return True

        print("\n" + "=" * 60)
        if success:
            print("✅ Best Seller Badge Script completed successfully!")
        else:
            print("❌ Best Seller Badge Script completed with errors.")

        return success

    except ImportError as e:
        print(f"❌ Error importing best seller badge script: {e}")
        print("💡 Make sure you have installed the required dependencies: pip install APScheduler")
        return False
    except Exception as e:
        print(f"❌ Unexpected error running best seller badge: {e}")
        return False

def run_product_discounts() -> bool:
    """Run the product discounts script with user mode selection"""
    try:
        print("\n💰 Starting Product Discounts Script...")
        print("=" * 60)

        # Ask user for execution mode
        print("Select execution mode:")
        print("1. 🔧 Manual Sync (run once)")
        print("2. 🔄 Scheduled Mode (run daily at 00:00)")
        print("3. 🧪 Dry Run (analyze changes only)")
        print("0. ↩️  Return to main menu")

        while True:
            try:
                mode_choice = input("\n🔸 Choose mode: ").strip()

                if mode_choice == "0":
                    return True  # Return to main menu
                elif mode_choice == "1":
                    # Manual sync
                    from scripts.job_set_discounts_to_products.main import run_product_discounts
                    success = run_product_discounts(mode="manual", dry_run=False)
                    break
                elif mode_choice == "2":
                    # Scheduled mode
                    print("\n⚠️  Scheduled mode will run daily at 00:00. Press Ctrl+C to stop.")
                    confirm = input("Continue? (y/N): ").strip().lower()
                    if confirm in ['y', 'yes']:
                        from scripts.job_set_discounts_to_products.main import run_product_discounts
                        success = run_product_discounts(mode="scheduled", dry_run=False)
                    else:
                        success = True  # User cancelled
                    break
                elif mode_choice == "3":
                    # Dry run
                    from scripts.job_set_discounts_to_products.main import run_product_discounts
                    success = run_product_discounts(mode="manual", dry_run=True)
                    break
                else:
                    print(f"❌ Invalid choice: '{mode_choice}'. Please select 0-3.")
                    continue

            except KeyboardInterrupt:
                print("\n⏹️  Operation cancelled by user")
                return True

        print("\n" + "=" * 60)
        if success:
            print("✅ Product Discounts Script completed successfully!")
        else:
            print("❌ Product Discounts Script completed with errors.")

        return success

    except ImportError as e:
        print(f"❌ Error importing product discounts script: {e}")
        print("💡 Make sure you have installed the required dependencies: pip install APScheduler")
        return False
    except Exception as e:
        print(f"❌ Unexpected error running product discounts: {e}")
        return False

def run_set_variants_metafield() -> bool:
    """Run the set variants metafield script with user mode selection"""
    try:
        print("\n📝 Starting Set Variants Metafield Script...")
        print("=" * 60)

        # Ask user for execution mode
        print("Select execution mode:")
        print("1. 🔧 Update Empty Only (default - only update products with empty custom.variants)")
        print("2. 🔄 Update All (force update all products, even with existing values)")
        print("3. 🧪 Dry Run Empty Only (analyze empty products only)")
        print("4. 🧪 Dry Run All (analyze all products)")
        print("0. ↩️  Return to main menu")

        while True:
            try:
                mode_choice = input("\n🔸 Choose mode: ").strip()

                if mode_choice == "0":
                    return True  # Return to main menu
                elif mode_choice == "1":
                    # Update empty only
                    import subprocess
                    result = subprocess.run(
                        ["python", "scripts/set_variants_to_product_metafield/main.py"],
                        cwd=os.getcwd()
                    )
                    success = result.returncode == 0
                    break
                elif mode_choice == "2":
                    # Update all
                    print("\n⚠️  This will update ALL products, even those with existing custom.variants values!")
                    confirm = input("Continue? (y/N): ").strip().lower()
                    if confirm in ['y', 'yes']:
                        import subprocess
                        result = subprocess.run(
                            ["python", "scripts/set_variants_to_product_metafield/main.py", "--all"],
                            cwd=os.getcwd()
                        )
                        success = result.returncode == 0
                    else:
                        success = True  # User cancelled
                    break
                elif mode_choice == "3":
                    # Dry run empty only
                    import subprocess
                    result = subprocess.run(
                        ["python", "scripts/set_variants_to_product_metafield/main.py", "--dry-run"],
                        cwd=os.getcwd()
                    )
                    success = result.returncode == 0
                    break
                elif mode_choice == "4":
                    # Dry run all
                    import subprocess
                    result = subprocess.run(
                        ["python", "scripts/set_variants_to_product_metafield/main.py", "--all", "--dry-run"],
                        cwd=os.getcwd()
                    )
                    success = result.returncode == 0
                    break
                else:
                    print(f"❌ Invalid choice: '{mode_choice}'. Please select 0-4.")
                    continue

            except KeyboardInterrupt:
                print("\n⏹️  Operation cancelled by user")
                return True

        print("\n" + "=" * 60)
        if success:
            print("✅ Set Variants Metafield Script completed successfully!")
        else:
            print("❌ Set Variants Metafield Script completed with errors.")

        return success

    except ImportError as e:
        print(f"❌ Error importing set variants metafield script: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error running set variants metafield: {e}")
        return False

def run_set_product_type() -> bool:
    """Run the set product type script with user mode selection"""
    try:
        print("\n🏷️  Starting Set Product Type Script...")
        print("=" * 60)

        # Ask user for execution mode
        print("Select execution mode:")
        print("1. 🔧 Process All 3 Configured Collections (639759778133, 639750963541, 639759647061)")
        print("2. 🎯 Process Specific Collection (enter collection ID)")
        print("3. 🧪 Dry Run All 3 Collections (analyze changes only)")
        print("4. 🧪 Dry Run Specific Collection (analyze changes only)")
        print("5. 📋 List Products with Empty Type (find all ACTIVE products with empty product type)")
        print("0. ↩️  Return to main menu")

        while True:
            try:
                mode_choice = input("\n🔸 Choose mode: ").strip()

                if mode_choice == "0":
                    return True  # Return to main menu
                elif mode_choice == "1":
                    # Process all collections
                    import subprocess
                    result = subprocess.run(
                        ["python", "scripts/set_product_type/main.py"],
                        cwd=os.getcwd()
                    )
                    success = result.returncode == 0
                    break
                elif mode_choice == "2":
                    # Process specific collection
                    collection_id = input("\n🎯 Enter collection ID: ").strip()
                    if not collection_id:
                        print("❌ No collection ID provided.")
                        continue
                    import subprocess
                    result = subprocess.run(
                        ["python", "scripts/set_product_type/main.py", "--collection", collection_id],
                        cwd=os.getcwd()
                    )
                    success = result.returncode == 0
                    break
                elif mode_choice == "3":
                    # Dry run all collections
                    import subprocess
                    result = subprocess.run(
                        ["python", "scripts/set_product_type/main.py", "--dry-run"],
                        cwd=os.getcwd()
                    )
                    success = result.returncode == 0
                    break
                elif mode_choice == "4":
                    # Dry run specific collection
                    collection_id = input("\n🎯 Enter collection ID: ").strip()
                    if not collection_id:
                        print("❌ No collection ID provided.")
                        continue
                    import subprocess
                    result = subprocess.run(
                        ["python", "scripts/set_product_type/main.py", "--collection", collection_id, "--dry-run"],
                        cwd=os.getcwd()
                    )
                    success = result.returncode == 0
                    break
                elif mode_choice == "5":
                    # List products with empty type
                    import subprocess
                    result = subprocess.run(
                        ["python", "scripts/set_product_type/main.py", "--list-empty"],
                        cwd=os.getcwd()
                    )
                    success = result.returncode == 0
                    break
                else:
                    print(f"❌ Invalid choice: '{mode_choice}'. Please select 0-5.")
                    continue

            except KeyboardInterrupt:
                print("\n⏹️  Operation cancelled by user")
                return True

        print("\n" + "=" * 60)
        if success:
            print("✅ Set Product Type Script completed successfully!")
        else:
            print("❌ Set Product Type Script completed with errors.")

        return success

    except ImportError as e:
        print(f"❌ Error importing set product type script: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error running set product type: {e}")
        return False

def run_scalapay_orders() -> bool:
    """Run the Scalapay orders export script with user mode selection"""
    try:
        print("\n💳 Starting Scalapay Orders Export Script...")
        print("=" * 60)

        # Ask user for execution mode
        print("Select execution mode:")
        print("1. 🔧 Export All (fetch all Scalapay orders and export to Excel)")
        print("2. 🧪 Dry Run (analyze orders without writing file)")
        print("3. 🔢 Limited Export (export first N orders)")
        print("0. ↩️  Return to main menu")

        while True:
            try:
                mode_choice = input("\n🔸 Choose mode: ").strip()

                if mode_choice == "0":
                    return True  # Return to main menu
                elif mode_choice == "1":
                    # Export all
                    from scripts.get_all_orders_scalapay.main import run_scalapay_orders as run_scalapay
                    success = run_scalapay(output_file=None, dry_run=False, limit=None)
                    break
                elif mode_choice == "2":
                    # Dry run
                    from scripts.get_all_orders_scalapay.main import run_scalapay_orders as run_scalapay
                    success = run_scalapay(output_file=None, dry_run=True, limit=None)
                    break
                elif mode_choice == "3":
                    # Limited export
                    limit_input = input("\n🔢 Enter limit (number of orders to scan): ").strip()
                    if not limit_input.isdigit():
                        print("❌ Please enter a valid number.")
                        continue
                    limit = int(limit_input)
                    from scripts.get_all_orders_scalapay.main import run_scalapay_orders as run_scalapay
                    success = run_scalapay(output_file=None, dry_run=False, limit=limit)
                    break
                else:
                    print(f"❌ Invalid choice: '{mode_choice}'. Please select 0-3.")
                    continue

            except KeyboardInterrupt:
                print("\n⏹️  Operation cancelled by user")
                return True

        print("\n" + "=" * 60)
        if success:
            print("✅ Scalapay Orders Export completed successfully!")
        else:
            print("❌ Scalapay Orders Export completed with errors.")

        return success

    except ImportError as e:
        print(f"❌ Error importing Scalapay orders script: {e}")
        print("💡 Make sure you have installed the required dependencies: pip install openpyxl")
        return False
    except Exception as e:
        print(f"❌ Unexpected error running Scalapay orders export: {e}")
        return False


def run_inventory_tags() -> bool:
    """Run the inventory tag sync script with user mode selection"""
    try:
        print("\n🏷️  Starting Inventory Tag Sync Script...")
        print("=" * 60)

        print("Select execution mode:")
        print("1. 🔧 Sync Tags (run once)")
        print("2. 🧪 Dry Run (analyze changes only)")
        print("0. ↩️  Return to main menu")

        while True:
            try:
                mode_choice = input("\n🔸 Choose mode: ").strip()

                if mode_choice == "0":
                    return True
                elif mode_choice == "1":
                    import subprocess
                    result = subprocess.run(
                        ["python", "scripts/set-product-tag-depens-inventory/main.py"],
                        cwd=os.getcwd()
                    )
                    success = result.returncode == 0
                    break
                elif mode_choice == "2":
                    import subprocess
                    result = subprocess.run(
                        ["python", "scripts/set-product-tag-depens-inventory/main.py", "--dry-run"],
                        cwd=os.getcwd()
                    )
                    success = result.returncode == 0
                    break
                else:
                    print(f"❌ Invalid choice: '{mode_choice}'. Please select 0-2.")
                    continue

            except KeyboardInterrupt:
                print("\n⏹️  Operation cancelled by user")
                return True

        print("\n" + "=" * 60)
        if success:
            print("✅ Inventory Tag Sync completed successfully!")
        else:
            print("❌ Inventory Tag Sync completed with errors.")

        return success

    except Exception as e:
        print(f"❌ Unexpected error running inventory tag sync: {e}")
        return False


def run_duplicate_products() -> bool:
    """Run the duplicate products from collection script"""
    try:
        print("\n📋 Starting Duplicate Products from Collection...")
        print("=" * 60)

        print("Select execution mode:")
        print("1. 📋 Duplicate Products (enter collection ID)")
        print("2. 🧪 Dry Run (analyze without making changes)")
        print("0. ↩️  Return to main menu")

        while True:
            try:
                mode_choice = input("\n🔸 Choose mode: ").strip()

                if mode_choice == "0":
                    return True
                elif mode_choice in ["1", "2"]:
                    collection_id = input("\n🎯 Enter source collection ID: ").strip()
                    if not collection_id:
                        print("❌ No collection ID provided.")
                        continue

                    dry_run = mode_choice == "2"
                    import subprocess
                    args = ["python", "scripts/duplicate-products-from-collection/main.py", "--collection", collection_id]
                    if dry_run:
                        args.append("--dry-run")
                    result = subprocess.run(args, cwd=os.getcwd())
                    success = result.returncode == 0
                    break
                else:
                    print(f"❌ Invalid choice: '{mode_choice}'. Please select 0-2.")
                    continue

            except KeyboardInterrupt:
                print("\n⏹️  Operation cancelled by user")
                return True

        print("\n" + "=" * 60)
        if success:
            print("✅ Duplicate Products completed successfully!")
        else:
            print("❌ Duplicate Products completed with errors.")

        return success

    except Exception as e:
        print(f"❌ Unexpected error running duplicate products: {e}")
        return False


def run_get_customers_menu() -> bool:
    """Run the get customers for Meta audience export script"""
    try:
        print("\n👥 Starting Get Customers for Meta Audience Export...")
        print("=" * 60)

        print("Select execution mode:")
        print("1. 📤 Export All (fetch all customers and export to CSV)")
        print("2. 🧪 Dry Run (analyze customers without writing files)")
        print("3. 🔢 Limited Export (export first N customers)")
        print("0. ↩️  Return to main menu")

        while True:
            try:
                mode_choice = input("\n🔸 Choose mode: ").strip()

                if mode_choice == "0":
                    return True
                elif mode_choice == "1":
                    from scripts.get_customers.main import run_get_customers
                    success = run_get_customers(dry_run=False, limit=None)
                    break
                elif mode_choice == "2":
                    from scripts.get_customers.main import run_get_customers
                    success = run_get_customers(dry_run=True, limit=None)
                    break
                elif mode_choice == "3":
                    limit_input = input("\n🔢 Enter limit (number of customers to fetch): ").strip()
                    if not limit_input.isdigit():
                        print("❌ Please enter a valid number.")
                        continue
                    limit = int(limit_input)
                    from scripts.get_customers.main import run_get_customers
                    success = run_get_customers(dry_run=False, limit=limit)
                    break
                else:
                    print(f"❌ Invalid choice: '{mode_choice}'. Please select 0-3.")
                    continue

            except KeyboardInterrupt:
                print("\n⏹️  Operation cancelled by user")
                return True

        print("\n" + "=" * 60)
        if success:
            print("✅ Get Customers Export completed successfully!")
        else:
            print("❌ Get Customers Export completed with errors.")

        return success

    except ImportError as e:
        print(f"❌ Error importing get customers script: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error running get customers export: {e}")
        return False


def run_bulk_product_type_menu() -> bool:
    """Run the bulk product type handler script"""
    try:
        print("\n🏷️  Starting Bulk Product Type Handler...")
        print("=" * 60)

        print("Select operation:")
        print("1. 📋 List Empty Type Products (export to CSV)")
        print("2. 🔍 Find Products by Type (export to CSV)")
        print("3. 🔄 Replace Product Type (update and export change log)")
        print("4. 🧪 Dry Run Replace (analyze without making changes)")
        print("5. 📥 Import Product Types from Excel (set types from file)")
        print("6. 🧪 Dry Run Import (analyze Excel without making changes)")
        print("0. ↩️  Return to main menu")

        while True:
            try:
                mode_choice = input("\n🔸 Choose operation: ").strip()

                if mode_choice == "0":
                    return True
                elif mode_choice == "1":
                    import subprocess
                    result = subprocess.run(
                        ["python", "scripts/bulk-product-type-handler/main.py", "--action", "empty"],
                        cwd=os.getcwd()
                    )
                    success = result.returncode == 0
                    break
                elif mode_choice == "2":
                    product_type = input("\n🏷️  Enter product type to search: ").strip()
                    if not product_type:
                        print("❌ No product type provided.")
                        continue
                    import subprocess
                    result = subprocess.run(
                        ["python", "scripts/bulk-product-type-handler/main.py",
                         "--action", "find", "--type", product_type],
                        cwd=os.getcwd()
                    )
                    success = result.returncode == 0
                    break
                elif mode_choice in ["3", "4"]:
                    old_type = input("\n🏷️  Enter current product type: ").strip()
                    if not old_type:
                        print("❌ No product type provided.")
                        continue
                    new_type = input("🏷️  Enter new product type: ").strip()
                    if not new_type:
                        print("❌ No new product type provided.")
                        continue

                    dry_run = mode_choice == "4"
                    if not dry_run:
                        print(f"\n⚠️  This will replace '{old_type}' → '{new_type}' for ALL matching products!")
                        confirm = input("Continue? (y/N): ").strip().lower()
                        if confirm not in ['y', 'yes']:
                            print("Operation cancelled.")
                            return True

                    import subprocess
                    args = ["python", "scripts/bulk-product-type-handler/main.py",
                            "--action", "replace", "--type", old_type, "--new-type", new_type]
                    if dry_run:
                        args.append("--dry-run")
                    result = subprocess.run(args, cwd=os.getcwd())
                    success = result.returncode == 0
                    break
                elif mode_choice in ["5", "6"]:
                    file_paths = input("\n📁 Enter Excel file path(s) (comma-separated): ").strip()
                    if not file_paths:
                        print("❌ No file path provided.")
                        continue
                    files = [f.strip() for f in file_paths.split(",") if f.strip()]

                    dry_run = mode_choice == "6"
                    if not dry_run:
                        print(f"\n⚠️  This will set product types from {len(files)} file(s) for ALL products in the Excel!")
                        confirm = input("Continue? (y/N): ").strip().lower()
                        if confirm not in ['y', 'yes']:
                            print("Operation cancelled.")
                            return True

                    import subprocess
                    args = ["python", "scripts/bulk-product-type-handler/main.py",
                            "--action", "import", "--file"] + files
                    if dry_run:
                        args.append("--dry-run")
                    result = subprocess.run(args, cwd=os.getcwd())
                    success = result.returncode == 0
                    break
                else:
                    print(f"❌ Invalid choice: '{mode_choice}'. Please select 0-6.")
                    continue

            except KeyboardInterrupt:
                print("\n⏹️  Operation cancelled by user")
                return True

        print("\n" + "=" * 60)
        if success:
            print("✅ Bulk Product Type operation completed successfully!")
        else:
            print("❌ Bulk Product Type operation completed with errors.")

        return success

    except Exception as e:
        print(f"❌ Unexpected error running bulk product type handler: {e}")
        return False


def get_user_choice() -> str:
    """Get user input with validation"""
    while True:
        try:
            choice = input("\n🔸 Enter your choice: ").strip()
            return choice
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            sys.exit(0)
        except EOFError:
            print("\n\n👋 Goodbye!")
            sys.exit(0)

def wait_for_enter():
    """Wait for user to press Enter to continue"""
    try:
        input("\n📥 Press Enter to return to main menu...")
    except KeyboardInterrupt:
        print("\n\n👋 Goodbye!")
        sys.exit(0)
    except EOFError:
        print("\n\n👋 Goodbye!")
        sys.exit(0)

def run_delete_catalog_fixed_prices_menu() -> bool:
    """Run the Delete Catalog Fixed Prices script with user mode selection"""
    try:
        print("\n🧹 Starting Delete Catalog Fixed Prices Script...")
        print("=" * 60)

        catalog_id = input("🔸 Enter Catalog ID (numeric, e.g. 179292701013): ").strip()
        if not catalog_id:
            print("❌ No Catalog ID provided.")
            return False

        print("\nSelect execution mode:")
        print("1. 🧪 Dry Run (resolve PriceList and count, no deletion)")
        print("2. 🗑️  Delete (real deletion, requires typing 'yes' to confirm)")
        print("0. ↩️  Return to main menu")

        while True:
            try:
                mode_choice = input("\n🔸 Choose mode: ").strip()

                if mode_choice == "0":
                    return True
                elif mode_choice == "1":
                    from scripts.job_delete_catalog_fixed_prices.main import (
                        run_delete_catalog_fixed_prices,
                    )
                    success = run_delete_catalog_fixed_prices(
                        catalog_id=catalog_id,
                        dry_run=True,
                        skip_confirm=True,
                    )
                    break
                elif mode_choice == "2":
                    from scripts.job_delete_catalog_fixed_prices.main import (
                        run_delete_catalog_fixed_prices,
                    )
                    success = run_delete_catalog_fixed_prices(
                        catalog_id=catalog_id,
                        dry_run=False,
                        skip_confirm=False,
                    )
                    break
                else:
                    print(f"❌ Invalid choice: '{mode_choice}'. Please select 0-2.")
                    continue

            except KeyboardInterrupt:
                print("\n⏹️  Operation cancelled by user")
                return True

        print("\n" + "=" * 60)
        if success:
            print("✅ Delete Catalog Fixed Prices Script completed successfully!")
        else:
            print("❌ Delete Catalog Fixed Prices Script completed with errors.")

        return success

    except ImportError as e:
        print(f"❌ Error importing delete catalog fixed prices script: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error running delete catalog fixed prices: {e}")
        return False


def main():
    """Main CLI loop"""

    # Dictionary mapping choices to functions
    script_functions: Dict[str, Callable] = {
        "1": run_dynamic_collections,
        "2": run_inventory_sync,
        "3": run_customer_marketing_sync,
        "4": run_webgains_report_enricher,
        "5": run_airtable_downloader,
        "6": run_customer_order_history,
        "7": run_best_seller_badge,
        "8": run_product_discounts,
        "9": run_set_variants_metafield,
        "10": run_set_product_type,
        "11": run_scalapay_orders,
        "12": run_inventory_tags,
        "13": run_duplicate_products,
        "14": run_get_customers_menu,
        "15": run_bulk_product_type_menu,
        "16": run_delete_catalog_fixed_prices_menu,
    }
    
    # Check if we're in a virtual environment
    if not os.environ.get('VIRTUAL_ENV'):
        print("⚠️  Warning: Not in a virtual environment. Consider running 'source venv/bin/activate' first.")
        print()
    
    try:
        while True:
            show_banner()
            show_menu()
            
            choice = get_user_choice()
            
            if choice == "0":
                print("\n👋 Goodbye!")
                break
            elif choice in script_functions:
                # Clear screen before running script
                os.system('clear' if os.name == 'posix' else 'cls')

                # Run the selected script
                try:
                    script_functions[choice]()
                except KeyboardInterrupt:
                    print("\n\n⏹️  Operation cancelled by user")

                # Wait for user input before returning to menu
                wait_for_enter()
                
                # Clear screen before showing menu again
                os.system('clear' if os.name == 'posix' else 'cls')
            else:
                print(f"\n❌ Invalid choice: '{choice}'. Please select a valid option.")
                wait_for_enter()
                os.system('clear' if os.name == 'posix' else 'cls')
                
    except KeyboardInterrupt:
        print("\n\n👋 Goodbye!")
    except Exception as e:
        print(f"\n💥 Unexpected error in main menu: {e}")
        print("Please check your setup and try again.")
        sys.exit(1)

if __name__ == "__main__":
    main()