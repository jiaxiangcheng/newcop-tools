"""Pure helpers for building media filenames and alt text.

Naming rules (confirmed by the user):
- filename: ``newcop-[slug(title)]-[slug(type)]-[position].[ext]`` (all lowercase,
  hyphen-joined, keeps the original file extension, position starts at 1).
- alt text: ``Newcop [Title] [Type] [Position]`` (every word capitalized).

``type`` is the product's Shopify product type, translated to Spanish via
``translate_product_type``. Unknown/unmapped types fall back to the original
English product type.

Slugify is implemented with the standard library only (``unicodedata`` + ``re``) to
avoid adding a new dependency.
"""
import re
import unicodedata
from urllib.parse import urlparse

# Fallback extension when an image URL has no recognizable extension.
DEFAULT_IMAGE_EXTENSION = "jpg"

# Shopify product type (English) -> Spanish label used in media names.
PRODUCT_TYPE_TRANSLATIONS = {
    "Newcop Clothing": "Ropa Newcop",
    "Resell Accessories": "Accesorios",
    "Resell Clothing": "Ropa",
    "Resell Sneakers": "Zapatillas",
    "Retail Accessories": "Accesorios",
    "Retail Clothing": "Ropa",
    "Retail Sneakers": "Zapatillas",
}


def translate_product_type(product_type: str) -> str:
    """Translate a Shopify product type to its Spanish label.

    Matching is case-insensitive on the trimmed value. Unknown/unmapped types
    fall back to the original (trimmed) English product type.
    """
    if not product_type or not product_type.strip():
        return ""

    trimmed = product_type.strip()
    # Case-insensitive lookup against the known mapping.
    for english, spanish in PRODUCT_TYPE_TRANSLATIONS.items():
        if english.lower() == trimmed.lower():
            return spanish
    return trimmed


def slugify(text: str) -> str:
    """Turn arbitrary text into a lowercase, url-safe slug.

    'Nike Air Max' -> 'nike-air-max', 'Café' -> 'cafe'.
    Accents are stripped, non-alphanumeric runs collapse to a single hyphen.
    """
    if not text:
        return ""

    # Decompose accented characters then drop the combining marks.
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")

    # Lowercase, replace any run of non-alphanumeric chars with a single hyphen.
    ascii_text = ascii_text.lower()
    ascii_text = re.sub(r"[^a-z0-9]+", "-", ascii_text)

    # Trim leading/trailing hyphens.
    return ascii_text.strip("-")


def extract_extension(image_url: str) -> str:
    """Extract a clean lowercase file extension from a Shopify image URL.

    Strips the ``?v=...`` query string first. Returns ``DEFAULT_IMAGE_EXTENSION``
    when no extension can be determined.
    """
    if not image_url:
        return DEFAULT_IMAGE_EXTENSION

    path = urlparse(image_url).path
    # Last path segment, e.g. 'my-photo.jpg'.
    last_segment = path.rsplit("/", 1)[-1]

    if "." not in last_segment:
        return DEFAULT_IMAGE_EXTENSION

    ext = last_segment.rsplit(".", 1)[-1].lower()
    # Guard against weird/empty extensions.
    if not ext or not re.fullmatch(r"[a-z0-9]+", ext):
        return DEFAULT_IMAGE_EXTENSION
    return ext


def _capitalize_words(text: str) -> str:
    """Capitalize the first letter of every word, lowercasing the rest.

    'air MAX 90' -> 'Air Max 90'. Empty input returns ''.
    """
    if not text or not text.strip():
        return ""
    return " ".join(word.capitalize() for word in text.split())


def build_filename(title: str, type_label: str, position: int, ext: str) -> str:
    """Build the target filename: newcop-[slug(title)]-[slug(type)]-[position].[ext]."""
    parts = ["newcop", slugify(title), slugify(type_label), str(position)]
    # Drop empty slug pieces so we never produce 'newcop--ropa-1.jpg'.
    base = "-".join(part for part in parts if part)
    return f"{base}.{ext}"


def build_alt(title: str, type_label: str, position: int) -> str:
    """Build the target alt text: 'Newcop [Title] [Type] [Position]'.

    Every word is capitalized (including the words inside title/type).
    """
    pieces = ["Newcop"]
    title_cap = _capitalize_words(title)
    if title_cap:
        pieces.append(title_cap)
    type_cap = _capitalize_words(type_label)
    if type_cap:
        pieces.append(type_cap)
    pieces.append(str(position))
    return " ".join(pieces)


def parse_filename_from_url(image_url: str) -> str:
    """Return the current filename (with extension, no query string) from an image URL."""
    if not image_url:
        return ""
    path = urlparse(image_url).path
    return path.rsplit("/", 1)[-1]
