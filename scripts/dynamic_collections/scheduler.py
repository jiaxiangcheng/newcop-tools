#!/usr/bin/env python3
"""
Dynamic Collection Scheduler

This script provides scheduling functionality for the dynamic collection builder.
It can be used with cron jobs or other scheduling systems to run collection updates
at regular intervals.

Usage:
    # Run once
    python scheduler.py

    # Run with custom frequency (hours)
    UPDATE_FREQUENCY_HOURS=12 python scheduler.py

    # Run with custom max records
    MAX_AIRTABLE_RECORDS=1000 python scheduler.py

Example cron job (runs every 24 hours at 2 AM):
    0 2 * * * cd /path/to/project && python scheduler.py >> /var/log/collection_update.log 2>&1

Example environment variables (.env file):
    UPDATE_FREQUENCY_HOURS=24
    MAX_AIRTABLE_RECORDS=500
    SHOPIFY_SHOP_DOMAIN=your-shop
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Add the current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from main import DynamicCollectionBuilder
from shared.logger import setup_logger

# Set up logging
logger = setup_logger('scheduler', 'scheduler.log', format_string='%(asctime)s - SCHEDULER - %(levelname)s - %(message)s')

class CollectionScheduler:
    """Scheduler for dynamic collection updates"""
    
    def __init__(self):
        self.builder = DynamicCollectionBuilder()
        self.last_run_file = "last_collection_update.txt"
        
    def should_run_update(self) -> bool:
        """Check if update should run based on frequency settings"""
        
        # Check if we should force run (for testing or manual execution)
        if os.getenv("FORCE_UPDATE", "false").lower() == "true":
            logger.info("Force update enabled, running collection update")
            return True
        
        # Check last run time
        try:
            if os.path.exists(self.last_run_file):
                with open(self.last_run_file, 'r') as f:
                    last_run_str = f.read().strip()
                    last_run = datetime.fromisoformat(last_run_str)
                    
                    time_since_last_run = datetime.now() - last_run
                    hours_since_last_run = time_since_last_run.total_seconds() / 3600
                    
                    logger.info(f"Last update was {hours_since_last_run:.1f} hours ago")
                    logger.info(f"Update frequency is set to {self.builder.update_frequency_hours} hours")
                    
                    if hours_since_last_run >= self.builder.update_frequency_hours:
                        logger.info("Time for scheduled update")
                        return True
                    else:
                        next_update = last_run + timedelta(hours=self.builder.update_frequency_hours)
                        logger.info(f"Next update scheduled for: {next_update}")
                        return False
            else:
                logger.info("No previous run found, running initial update")
                return True
                
        except Exception as e:
            logger.warning(f"Error checking last run time: {e}, running update")
            return True
    
    def record_update_time(self):
        """Record the current time as last update time"""
        try:
            with open(self.last_run_file, 'w') as f:
                f.write(datetime.now().isoformat())
            logger.info("Recorded update time")
        except Exception as e:
            logger.error(f"Failed to record update time: {e}")
    
    def run_scheduled_update(self) -> bool:
        """Run a scheduled collection update"""
        logger.info("=== Starting Scheduled Collection Update ===")
        logger.info(f"Update frequency: {self.builder.update_frequency_hours} hours")
        logger.info(f"Max Airtable records: {self.builder.max_airtable_records}")
        
        try:
            if not self.should_run_update():
                logger.info("Update not needed at this time")
                return True
            
            # Run the collection update
            result = self.builder.run()
            
            if result["success"]:
                self.record_update_time()
                logger.info("✅ Scheduled collection update completed successfully!")
                logger.info(f"📊 Processed {result.get('total_sales_records', 0)} sales records")
                logger.info(f"🎯 Filtered to {result.get('filtered_products_count', 0)} qualifying products")
                
                update_result = result.get('shopify_update_result', {})
                logger.info(f"🛍️  Collection update details:")
                logger.info(f"   - Products added: {update_result.get('products_added', 0)}")
                logger.info(f"   - Products removed: {update_result.get('products_removed', 0)}")
                logger.info(f"   - Products kept: {update_result.get('products_kept', 0)}")
                logger.info(f"   - Final collection size: {update_result.get('final_collection_size', 0)}")
                
                return True
            else:
                logger.error(f"❌ Scheduled collection update failed: {result.get('error', result.get('message', 'Unknown error'))}")
                return False
                
        except Exception as e:
            logger.error(f"💥 Unexpected error during scheduled update: {e}")
            return False
        finally:
            logger.info("=== Scheduled Collection Update Complete ===")

def main():
    """Main entry point for scheduler"""
    try:
        scheduler = CollectionScheduler()
        success = scheduler.run_scheduled_update()
        
        if success:
            sys.exit(0)
        else:
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("⏹️  Scheduler interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💥 Scheduler error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()