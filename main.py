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
        print("1. 🔧 Manual Sync (run once)")
        print(f"2. 🔄 Scheduled Mode (run every {dynamic_collections_interval_days} days)")
        print("3. 🧪 Dry Run (analyze changes only)")
        print("0. ↩️  Return to main menu")
        
        while True:
            try:
                mode_choice = input("\n🔸 Choose mode: ").strip()
                
                if mode_choice == "0":
                    return True  # Return to main menu
                elif mode_choice == "1":
                    # Manual sync
                    from scripts.job_dynamic_collections.main import run_dynamic_collections
                    success = run_dynamic_collections(mode="manual", dry_run=False)
                    break
                elif mode_choice == "2":
                    # Scheduled mode
                    print(f"\n⚠️  Scheduled mode will run continuously every {dynamic_collections_interval_days} days. Press Ctrl+C to stop.")
                    confirm = input("Continue? (y/N): ").strip().lower()
                    if confirm in ['y', 'yes']:
                        from scripts.job_dynamic_collections.main import run_dynamic_collections
                        success = run_dynamic_collections(mode="scheduled", dry_run=False)
                    else:
                        success = True  # User cancelled
                    break
                elif mode_choice == "3":
                    # Dry run
                    from scripts.job_dynamic_collections.main import run_dynamic_collections
                    success = run_dynamic_collections(mode="manual", dry_run=True)
                    break
                else:
                    print(f"❌ Invalid choice: '{mode_choice}'. Please select 0-3.")
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
                limit=limit
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
                script_functions[choice]()
                
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