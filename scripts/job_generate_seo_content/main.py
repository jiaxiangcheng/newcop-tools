import os
import sys
import logging
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


def _setup_logger() -> logging.Logger:
    from shared.logger import setup_logger
    return setup_logger("job_generate_seo_content", "seo_content.log")


class SeoContentOrchestrator:
    def __init__(self):
        self.shopify_admin_token: Optional[str] = os.getenv("SHOPIFY_ADMIN_TOKEN")
        self.shopify_shop_domain: Optional[str] = os.getenv("SHOPIFY_SHOP_DOMAIN")
        self.openai_api_key: Optional[str] = os.getenv("OPENAI_API_KEY")
        self.logger = _setup_logger()
        self.shopify_client = None
        self.openai_client = None
        self.seo_manager = None

    def validate_environment(self) -> bool:
        missing = []
        if not self.shopify_admin_token:
            missing.append("SHOPIFY_ADMIN_TOKEN")
        if not self.shopify_shop_domain:
            missing.append("SHOPIFY_SHOP_DOMAIN")
        if not self.openai_api_key:
            missing.append("OPENAI_API_KEY")

        if missing:
            print(f"❌ Missing environment variables: {', '.join(missing)}")
            print("   Please set them in your .env file.")
            return False
        return True

    def initialize_clients(self) -> bool:
        try:
            from shared.shopify_client import ShopifyClient
            from openai import OpenAI
            from .seo_manager import SeoContentManager

            self.shopify_client = ShopifyClient(
                admin_token=self.shopify_admin_token,
                shop_domain=self.shopify_shop_domain,
            )
            self.openai_client = OpenAI(api_key=self.openai_api_key)
            self.seo_manager = SeoContentManager(
                shopify_client=self.shopify_client,
                openai_client=self.openai_client,
            )
            self.logger.info("Clients initialized successfully")
            return True
        except ImportError as e:
            print(f"❌ Import error: {e}")
            print("   Run: pip install openai")
            return False
        except Exception as e:
            print(f"❌ Error initializing clients: {e}")
            self.logger.error(f"Error initializing clients: {e}")
            return False

    def run_manual(self, dry_run: bool = False) -> bool:
        print(f"\n{'🧪 DRY RUN — ' if dry_run else ''}Scanning all products for missing SEO content...")
        self.logger.info(f"Starting manual run (dry_run={dry_run})")

        try:
            summary = self.seo_manager.run(dry_run=dry_run)
            return summary.failed_updates == 0
        except Exception as e:
            print(f"❌ Error during run: {e}")
            self.logger.error(f"Error during run: {e}", exc_info=True)
            return False


def run_generate_seo_content(mode: str = "manual", dry_run: bool = False) -> bool:
    orchestrator = SeoContentOrchestrator()

    if not orchestrator.validate_environment():
        return False

    if not orchestrator.initialize_clients():
        return False

    return orchestrator.run_manual(dry_run=dry_run)
