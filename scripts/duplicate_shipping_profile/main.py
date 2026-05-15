"""CLI entry point for duplicating a Shopify Delivery (Shipping) Profile."""
import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from shared.shopify_client import ShopifyClient  # noqa: E402
from scripts.duplicate_shipping_profile.profile_manager import (  # noqa: E402
    ShippingProfileManager,
)
from scripts.duplicate_shipping_profile.models import ProfileSummary  # noqa: E402


def setup_logging() -> logging.Logger:
    log_dir = project_root / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "duplicate_shipping_profile.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )
    return logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Duplicate a Shopify Delivery (Shipping) Profile",
    )
    parser.add_argument(
        "--new-name",
        type=str,
        default=None,
        help="Name for the new profile (default: '<source> (Copy)')",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not call deliveryProfileCreate; print transformed input instead",
    )
    return parser.parse_args()


def _print_profiles(profiles: List[ProfileSummary]) -> None:
    print("\n📋 Available Delivery Profiles:")
    print("-" * 80)
    for idx, profile in enumerate(profiles, 1):
        tag = " [DEFAULT]" if profile.default else ""
        print(
            f"{idx:>2}. {profile.name}{tag}\n"
            f"     id: {profile.id}\n"
            f"     methods: {profile.active_method_definitions_count} | "
            f"variants: {profile.product_variants_count}"
        )
    print("-" * 80)


def _select_profile_index(profiles: List[ProfileSummary]) -> Optional[int]:
    while True:
        raw = input(f"\n🔸 Pick a profile (1-{len(profiles)}, 0 to cancel): ").strip()
        if raw == "0":
            return None
        try:
            idx = int(raw)
        except ValueError:
            print("❌ Please enter a number.")
            continue
        if 1 <= idx <= len(profiles):
            return idx - 1
        print(f"❌ Out of range. Enter 1-{len(profiles)}.")


def _build_admin_url(shop_domain: str, profile_gid: str) -> str:
    legacy_id = profile_gid.rsplit("/", 1)[-1] if profile_gid else ""
    clean_domain = shop_domain.replace(".myshopify.com", "")
    return f"https://{clean_domain}.myshopify.com/admin/settings/shipping/profiles/{legacy_id}"


def run_duplicate_shipping_profile(
    new_name: Optional[str] = None,
    dry_run: bool = False,
) -> bool:
    """Interactive flow used by both the CLI and the root menu launcher."""
    load_dotenv()
    logger = setup_logging()

    shopify_token = os.getenv("SHOPIFY_ADMIN_TOKEN")
    shopify_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")
    if not shopify_token or not shopify_domain:
        logger.error("Missing SHOPIFY_ADMIN_TOKEN or SHOPIFY_SHOP_DOMAIN")
        return False

    client = ShopifyClient(shopify_token, shopify_domain)
    manager = ShippingProfileManager(client)

    print("=" * 60)
    print("🚚 DUPLICATE SHIPPING PROFILE")
    print(f"Mode: {'DRY-RUN' if dry_run else 'LIVE'}")
    print("=" * 60)

    try:
        profiles = manager.list_profiles()
    except Exception as exc:
        logger.error("Failed to list delivery profiles: %s", exc)
        return False

    if not profiles:
        print("⚠️  No delivery profiles found in this store.")
        return False

    _print_profiles(profiles)
    idx = _select_profile_index(profiles)
    if idx is None:
        print("👋 Cancelled.")
        return True

    source = profiles[idx]
    if source.default:
        print(
            "\n⚠️  The default profile cannot host a second 'default'. "
            "The copy will be created as a custom profile."
        )
        confirm = input("Continue? (y/N): ").strip().lower()
        if confirm not in ("y", "yes"):
            print("👋 Cancelled.")
            return True

    print(f"\n🔍 Fetching full structure of '{source.name}'...")
    try:
        full_source = manager.fetch_profile(source.id)
    except Exception as exc:
        logger.error("Failed to fetch profile %s: %s", source.id, exc)
        return False

    location_groups = full_source.get("profileLocationGroups") or []
    total_zones = 0
    total_methods = 0
    for plg in location_groups:
        zone_edges = (plg.get("locationGroupZones") or {}).get("edges") or []
        total_zones += len(zone_edges)
        for zedge in zone_edges:
            method_edges = ((zedge.get("node") or {}).get("methodDefinitions") or {}).get("edges") or []
            total_methods += len(method_edges)

    print("\n📦 Source profile summary:")
    print(f"   Location groups: {len(location_groups)}")
    print(f"   Zones: {total_zones}")
    print(f"   Method definitions: {total_methods}")

    target_name = new_name or f"{source.name} (Copy)"
    while True:
        answer = input(
            f"\n🔸 New profile name [{target_name}] (Enter to accept, or type a new one): "
        ).strip()
        if not answer:
            break
        target_name = answer
        break

    confirm = input(
        f"\n⚠️  Will {'simulate' if dry_run else 'create'} a copy named '{target_name}'. Proceed? (y/N): "
    ).strip().lower()
    if confirm not in ("y", "yes"):
        print("👋 Cancelled.")
        return True

    try:
        profile_input = manager.transform_to_input(full_source, target_name)
    except Exception as exc:
        logger.error("Transform failed: %s", exc)
        return False

    if dry_run:
        print("\n📝 Transformed DeliveryProfileInput (dry-run):")
        print(json.dumps(profile_input, indent=2, ensure_ascii=False))

    result = manager.create_profile(profile_input, dry_run=dry_run)

    if not result.success:
        print("\n❌ Duplicate failed:")
        for err in result.errors:
            print(f"   - {err}")
        return False

    if result.dry_run:
        print(f"\n✅ Dry-run OK. Would create '{result.new_profile_name}'.")
        return True

    print("\n✅ Duplicate created!")
    print(f"   id: {result.new_profile_id}")
    print(f"   name: {result.new_profile_name}")
    if result.new_profile_id:
        print(f"   admin: {_build_admin_url(shopify_domain, result.new_profile_id)}")
    return True


def main() -> int:
    args = parse_arguments()
    ok = run_duplicate_shipping_profile(new_name=args.new_name, dry_run=args.dry_run)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
