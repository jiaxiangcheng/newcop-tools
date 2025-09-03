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
    """Display the main menu options"""
    print("\n📋 Available Scripts:")
    print("1. 🔄 Dynamic Collections - Auto-update Shopify collections based on Airtable sales data")
    print(f"2. 📦 Inventory Sync - Sync inventory quantities to variant metafields every {inventory_sync_interval_hours} hours")
    print(f"3. 👥 Customer Marketing Sync - Sync customer marketing preferences to metafields every {customer_marketing_sync_interval_hours} hours")
    print("4. 🚀 More scripts coming soon...")
    print("\n0. 🚪 Exit")
    print("-" * 60)

def run_dynamic_collections() -> bool:
    """Run the dynamic collections script with user mode selection"""
    try:
        print("\n🔄 Starting Dynamic Collections Script...")
        print("=" * 60)
        
        # Ask user for execution mode
        print("Select execution mode:")
        print("1. 🔧 Manual Sync (run once)")
        print("2. 🔄 Scheduled Mode (run every 15 days)")
        print("3. 🧪 Dry Run (analyze changes only)")
        print("0. ↩️  Return to main menu")
        
        while True:
            try:
                mode_choice = input("\n🔸 Choose mode: ").strip()
                
                if mode_choice == "0":
                    return True  # Return to main menu
                elif mode_choice == "1":
                    # Manual sync
                    from scripts.dynamic_collections.main import run_dynamic_collections
                    success = run_dynamic_collections(mode="manual", dry_run=False)
                    break
                elif mode_choice == "2":
                    # Scheduled mode
                    print("\n⚠️  Scheduled mode will run continuously every 15 days. Press Ctrl+C to stop.")
                    confirm = input("Continue? (y/N): ").strip().lower()
                    if confirm in ['y', 'yes']:
                        from scripts.dynamic_collections.main import run_dynamic_collections
                        success = run_dynamic_collections(mode="scheduled", dry_run=False)
                    else:
                        success = True  # User cancelled
                    break
                elif mode_choice == "3":
                    # Dry run
                    from scripts.dynamic_collections.main import run_dynamic_collections
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
        
        # Ask user for execution mode
        print("Select execution mode:")
        print("1. 🔧 Manual Sync (run once)")
        print("2. 🔄 Scheduled Mode (run every 2 hours)")
        print("3. 🧪 Dry Run (analyze changes only)")
        print("0. ↩️  Return to main menu")
        
        while True:
            try:
                mode_choice = input("\n🔸 Choose mode: ").strip()
                
                if mode_choice == "0":
                    return True  # Return to main menu
                elif mode_choice == "1":
                    # Manual sync
                    from scripts.inventory_sync.main import run_inventory_sync
                    success = run_inventory_sync(mode="manual", dry_run=False)
                    break
                elif mode_choice == "2":
                    # Scheduled mode
                    print("\n⚠️  Scheduled mode will run continuously. Press Ctrl+C to stop.")
                    confirm = input("Continue? (y/N): ").strip().lower()
                    if confirm in ['y', 'yes']:
                        from scripts.inventory_sync.main import run_inventory_sync
                        success = run_inventory_sync(mode="scheduled", dry_run=False)
                    else:
                        success = True  # User cancelled
                    break
                elif mode_choice == "3":
                    # Dry run
                    from scripts.inventory_sync.main import run_inventory_sync
                    success = run_inventory_sync(mode="manual", dry_run=True)
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