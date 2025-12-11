"""
Test script for variant title parsing logic.
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from scripts.set_variants_to_product_metafield.variants_manager import VariantsMetafieldManager


def test_parse_variant_title():
    """Test the parse_variant_title method with various inputs."""

    test_cases = [
        # (input, expected_output, description)
        ("35.5 EU - Color", "35.5 EU", "35.5 EU should stay as 35.5 EU"),
        ("36 EU - Red", "36 EU", "36 EU should stay as 36 EU"),
        ("37.3 EU - Blue", "37 EU", "37.3 EU should round down to 37 EU"),
        ("37.6 EU - Green", "37.5 EU", "37.6 EU should round to 37.5 EU"),
        ("38.0 EU", "38 EU", "38.0 EU should become 38 EU"),
        ("38.1 EU", "38 EU", "38.1 EU should become 38 EU"),
        ("38.2 EU", "38 EU", "38.2 EU should become 38 EU"),
        ("38.4 EU", "38.5 EU", "38.4 EU should become 38.5 EU"),
        ("38.5 EU", "38.5 EU", "38.5 EU should stay as 38.5 EU"),
        ("38.7 EU", "38.5 EU", "38.7 EU should become 38.5 EU"),
        ("38.8 EU", "38.5 EU", "38.8 EU should become 38.5 EU"),
        ("38.9 EU", "38.5 EU", "38.9 EU should become 38.5 EU"),
        ("40 EU W - Wide", "40 EU W", "40 EU W should keep the W suffix"),
        ("40.5 EU W", "40.5 EU W", "40.5 EU W should stay as 40.5 EU W"),
        ("41.3 EU W", "41 EU W", "41.3 EU W should round down to 41 EU W"),
        ("Default Title", "Default Title", "Non-size variants should remain unchanged"),
        ("Small", "Small", "Text-only variants should remain unchanged"),
    ]

    print("=" * 80)
    print("Testing Variant Title Parsing")
    print("=" * 80)

    passed = 0
    failed = 0

    for input_val, expected, description in test_cases:
        result = VariantsMetafieldManager.parse_variant_title(input_val)
        status = "✅ PASS" if result == expected else "❌ FAIL"

        if result == expected:
            passed += 1
        else:
            failed += 1

        print(f"\n{status}: {description}")
        print(f"  Input:    '{input_val}'")
        print(f"  Expected: '{expected}'")
        print(f"  Got:      '{result}'")

    print("\n" + "=" * 80)
    print(f"Test Results: {passed} passed, {failed} failed")
    print("=" * 80)

    return failed == 0


if __name__ == "__main__":
    success = test_parse_variant_title()
    exit(0 if success else 1)
