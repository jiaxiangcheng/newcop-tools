#!/usr/bin/env python3
"""
Test script for the new dynamic collections functionality

This script tests the ability to:
1. Discover collections with job_settings metafields
2. Parse job settings correctly
3. Execute jobs based on dynamic configuration
"""

import os
import logging
import sys
from dotenv import load_dotenv
from shopify_client import ShopifyClient
from models import CollectionWithJobSettings, TopResellProductsJobSettings, JobType
from job_executor import JobExecutorFactory

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

def test_environment_setup():
    """Test that required environment variables are present"""
    logger.info("Testing environment setup...")
    
    required_vars = [
        "AIRTABLE_TOKEN",
        "SHOPIFY_ADMIN_TOKEN", 
        "SHOPIFY_SHOP_DOMAIN"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return False
    
    logger.info("✅ Environment setup complete")
    return True

def test_shopify_client_creation():
    """Test that ShopifyClient can be created successfully"""
    logger.info("Testing Shopify client creation...")
    
    try:
        shopify_client = ShopifyClient(
            os.getenv("SHOPIFY_ADMIN_TOKEN"),
            os.getenv("SHOPIFY_SHOP_DOMAIN")
        )
        logger.info("✅ Shopify client created successfully")
        return shopify_client
    except Exception as e:
        logger.error(f"❌ Failed to create Shopify client: {e}")
        return None

def test_collections_discovery(shopify_client):
    """Test discovering collections with job settings"""
    logger.info("Testing collections discovery...")
    
    try:
        # Test getting all collections first
        all_collections = shopify_client.get_all_collections()
        logger.info(f"Found {len(all_collections)} total collections")
        
        # Test getting collections with job settings
        collections_with_jobs = shopify_client.get_collections_with_job_settings()
        logger.info(f"Found {len(collections_with_jobs)} collections with job settings")
        
        if collections_with_jobs:
            logger.info("Collections with job settings:")
            for collection_data in collections_with_jobs:
                collection = collection_data["collection"]
                job_settings = collection_data["job_settings"]
                logger.info(f"  - {collection.get('title', 'N/A')} (ID: {collection.get('id', 'N/A')})")
                logger.info(f"    Job Type: {job_settings.get('jobType', 'N/A')}")
                logger.info(f"    Description: {job_settings.get('Description', 'N/A')}")
        else:
            logger.warning("⚠️ No collections found with job_settings metafield")
        
        logger.info("✅ Collections discovery test complete")
        return collections_with_jobs
        
    except Exception as e:
        logger.error(f"❌ Collections discovery failed: {e}")
        return []

def test_job_settings_parsing(collections_with_jobs_data):
    """Test parsing job settings into model objects"""
    logger.info("Testing job settings parsing...")
    
    collections_with_jobs = []
    
    for collection_data in collections_with_jobs_data:
        try:
            collection_with_settings = CollectionWithJobSettings.from_shopify_collection_and_job_data(
                collection_data["collection"],
                collection_data["job_settings"]
            )
            collections_with_jobs.append(collection_with_settings)
            
            logger.info(f"✅ Successfully parsed job settings for collection '{collection_with_settings.collection_title}'")
            logger.info(f"  - Job Type: {collection_with_settings.job_settings.jobType}")
            logger.info(f"  - Update Frequency: {collection_with_settings.job_settings.UPDATE_FREQUENCY_HOURS} hours")
            logger.info(f"  - Max Records: {collection_with_settings.job_settings.MAX_AIRTABLE_RECORDS}")
            
            if isinstance(collection_with_settings.job_settings, TopResellProductsJobSettings):
                logger.info(f"  - Airtable Base: {collection_with_settings.job_settings.AIRTABLE_BASE_ID}")
                logger.info(f"  - Airtable Table: {collection_with_settings.job_settings.AIRTABLE_TABLE_ID}")
                logger.info(f"  - Airtable View: {collection_with_settings.job_settings.AIRTABLE_VIEW_ID}")
            
        except Exception as e:
            collection_title = collection_data["collection"].get("title", "Unknown")
            logger.error(f"❌ Failed to parse job settings for collection '{collection_title}': {e}")
    
    logger.info(f"✅ Job settings parsing test complete ({len(collections_with_jobs)} successful)")
    return collections_with_jobs

def test_job_executor_factory(shopify_client):
    """Test job executor factory creation and functionality"""
    logger.info("Testing job executor factory...")
    
    try:
        job_executor_factory = JobExecutorFactory(
            os.getenv("AIRTABLE_TOKEN"),
            shopify_client
        )
        
        supported_types = job_executor_factory.get_supported_job_types()
        logger.info(f"✅ Job executor factory created with supported types: {supported_types}")
        
        # Test getting an executor for getTopResellProducts
        if JobType.GET_TOP_RESELL_PRODUCTS in supported_types:
            executor = job_executor_factory.get_executor(JobType.GET_TOP_RESELL_PRODUCTS)
            logger.info(f"✅ Successfully got executor for {JobType.GET_TOP_RESELL_PRODUCTS}")
        else:
            logger.warning(f"⚠️ No executor found for {JobType.GET_TOP_RESELL_PRODUCTS}")
        
        return job_executor_factory
        
    except Exception as e:
        logger.error(f"❌ Job executor factory test failed: {e}")
        return None

def test_dry_run_job_execution(collections_with_jobs, job_executor_factory):
    """Test job execution in dry-run mode (without actually executing)"""
    logger.info("Testing dry-run job execution...")
    
    if not collections_with_jobs:
        logger.warning("⚠️ No collections with job settings to test")
        return True
    
    try:
        for collection_with_settings in collections_with_jobs:
            job_type = collection_with_settings.job_settings.jobType
            collection_title = collection_with_settings.collection_title
            
            logger.info(f"Would execute job '{job_type}' for collection '{collection_title}'")
            
            # Test that we can get the appropriate executor
            executor = job_executor_factory.get_executor(job_type)
            logger.info(f"✅ Found executor for job type '{job_type}': {type(executor).__name__}")
        
        logger.info("✅ Dry-run job execution test complete")
        return True
        
    except Exception as e:
        logger.error(f"❌ Dry-run job execution test failed: {e}")
        return False

def main():
    """Run all tests"""
    logger.info("Starting dynamic collections functionality tests...")
    
    # Test environment setup
    if not test_environment_setup():
        sys.exit(1)
    
    # Test Shopify client creation
    shopify_client = test_shopify_client_creation()
    if not shopify_client:
        sys.exit(1)
    
    # Test collections discovery
    collections_with_jobs_data = test_collections_discovery(shopify_client)
    
    # Test job settings parsing
    collections_with_jobs = test_job_settings_parsing(collections_with_jobs_data)
    
    # Test job executor factory
    job_executor_factory = test_job_executor_factory(shopify_client)
    if not job_executor_factory:
        sys.exit(1)
    
    # Test dry-run job execution
    if not test_dry_run_job_execution(collections_with_jobs, job_executor_factory):
        sys.exit(1)
    
    logger.info("🎉 All dynamic collections functionality tests passed!")
    
    # Print summary
    print(f"\n📊 Test Summary:")
    print(f"  ✅ Environment setup: OK")
    print(f"  ✅ Shopify client: OK") 
    print(f"  ✅ Collections discovery: {len(collections_with_jobs_data)} found")
    print(f"  ✅ Job settings parsing: {len(collections_with_jobs)} successful")
    print(f"  ✅ Job executor factory: OK")
    print(f"  ✅ Dry-run execution: OK")
    
    if collections_with_jobs:
        print(f"\n🔍 Discovered Collections:")
        for collection in collections_with_jobs:
            print(f"  - {collection.collection_title}: {collection.job_settings.jobType}")
    else:
        print(f"\n⚠️  No collections found with job_settings metafield.")
        print(f"   To test the full functionality, you need to:")
        print(f"   1. Create a collection in Shopify")
        print(f"   2. Add a metafield with namespace 'custom' and key 'job_settings'")
        print(f"   3. Set the value to a JSON like:")
        print(f'      {{"jobType": "getTopResellProducts", "AIRTABLE_BASE_ID": "...", ...}}')

if __name__ == "__main__":
    main()