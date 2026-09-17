"""
Pure Amazon invoice-text logic. Three jobs:
  1. Shared Drive folder-name - date-range parsing (parse_folder_date_range,
     find_matching_folders).
  2. OCR'd invoice text - parsed field dict (parse_invoice_text).
  3. Deciding whether a parsed invoice matches the search inputs
     (invoice_matches).
"""
 
import re
from datetime import date, datetime
from typing import Optional
 
from dateutil import parser as dateparser
 
# --------------------------------------------------------------------------
# Folder-name date parsing
# --------------------------------------------------------------------------
# Folder name: "YYYY-MM-DD d MMMM YYYY - d MMMM YYYY" (leading ISO date and
# first spelled-out date are redundant; only the ISO prefix and the final
# spelled-out end date are actually parsed).
 
_FOLDER_NAME_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\s+.+?\s+[-–]\s+(.+)$")
 
 
def parse_folder_date_range(folder_name: str) -> Optional[tuple[date, date]]:
    match = _FOLDER_NAME_RE.match(folder_name.strip())
    if not match:
        return None
 
    start_str, end_str = match.groups()
    try:
        start = datetime.strptime(start_str, "%Y-%m-%d").date()
        end = dateparser.parse(end_str.strip(), dayfirst=True).date()
    except (ValueError, OverflowError):
        return None
 
    return start, end


def _parse_date_value(date_str: Optional[str]) -> Optional[date]:
    """Parses an order_date-style field into a date, or None if missing/unparseable."""
    if not date_str:
        return None
    try:
        return dateparser.parse(date_str.strip(), dayfirst=True).date()
    except (ValueError, OverflowError):
        return None
 
 
def find_matching_folders(folders: list[dict], start_date: date, end_date: date) -> list[dict]:
    matched = []
    for folder in folders:
        parsed_range = parse_folder_date_range(folder["name"])
        if not parsed_range:
            continue
        folder_start, folder_end = parsed_range
        if folder_start <= end_date and folder_end >= start_date:
            matched.append(folder)
    return matched
 
 
# --------------------------------------------------------------------------
# Invoice text parsing
# --------------------------------------------------------------------------
 
def _first_match(pattern: str, text: str) -> Optional[str]:
    m = re.search(pattern, text, re.IGNORECASE)
    return m.group(1).strip() if m else None


def _collapse_whitespace(value: Optional[str]) -> Optional[str]:
    """Collapses runs of whitespace (double spaces, tabs) down to a single space."""
    if not value:
        return value
    return re.sub(r"\s+", " ", value).strip()
 

_MONEY = r"-?£?[\d,]+\.\d{2}"
 
# Two date formats seen in practice: "02.01.2026" and "03 January 2026".
_DATE_VALUE = r"(\d{1,2}[./]\d{1,2}[./]\d{2,4}|\d{1,2}\s+[A-Za-z]+\s+\d{4})"
 
# Literal "ASIN: B07PZ94SHX" — reliable anchor for each line item.
_ASIN_LABEL_RE = re.compile(r"ASIN:\s*([A-Z0-9]{10})")
 
# A full item row: Qty, Unit price (excl VAT), VAT rate, Unit price (incl VAT), Item subtotal.
_ITEM_ROW_RE = re.compile(rf"(\d+)\s+({_MONEY})\s+(\d+%)\s+({_MONEY})\s+({_MONEY})")
 
# Column-header text that can end up glued onto a description line.
_TABLE_HEADER_NOISE_RE = re.compile(
    r"\(excl\.?\s*VAT\)|\(incl\.?\s*VAT\)|\bDescription\b|\bQty\b|\bUnit price\b|\bVAT rate\b|\bItem subtotal\b",
    re.IGNORECASE,
)
 
 
def _clean_product_name_fragment(line: str) -> str:
    line = _ITEM_ROW_RE.sub(" ", line)
    line = _TABLE_HEADER_NOISE_RE.sub(" ", line)
    return re.sub(r"\s+", " ", line).strip()
 
 
def _extract_line_items(text: str) -> list[dict]:
    """
    Extract each line item: product_name, asin, quantity, unit prices, VAT
    rate, subtotal, from OCR'd invoice text, returning one dict per item in
    the order they appear.
 
    Each item is anchored on its "ASIN: ..." occurrence, with product_name
    built from the lines immediately preceding it. Quantity/price fields are
    then matched positionally (Nth item row <-> Nth ASIN) rather than by
    proximity, since on real Drive OCR output both items' rows landed on one
    shared line with no separator between them. If the ASIN and row counts 
    don't line up, the affected item's price fields are left as None rather 
    than guessed.
    """
    asin_matches = list(_ASIN_LABEL_RE.finditer(text))
    row_matches = list(_ITEM_ROW_RE.finditer(text))
 
    items = []
    for i, asin_match in enumerate(asin_matches):
        asin = asin_match.group(1)
        start = asin_match.start()
        prev_boundary = asin_matches[i - 1].end() if i > 0 else max(0, start - 400)
        preceding_lines = [l.strip() for l in text[prev_boundary:start].splitlines() if l.strip()]
 
        cleaned = [c for c in (_clean_product_name_fragment(l) for l in preceding_lines[-4:]) if c]
        product_name = " ".join(cleaned) if cleaned else None
        if product_name:
            # Strip a trailing "[M] | B07PZ94SHX" catalog-code echo.
            product_name = re.sub(
                r"\s*\[[A-Za-z0-9]+\]\s*\|\s*[A-Z0-9]{10}\s*$", "", product_name
            ).strip()
 
        if i < len(row_matches):
            qty, excl_vat, vat_rate, incl_vat, subtotal = row_matches[i].groups()
        else:
            qty = excl_vat = vat_rate = incl_vat = subtotal = None
 
        items.append({
            "product_name": product_name,
            "asin": asin,
            "quantity": qty,
            "unit_price_excl_vat": excl_vat,
            "vat_rate": vat_rate,
            "unit_price_incl_vat": incl_vat,
            "subtotal": subtotal,
        })
    return items


_MARKETPLACE_DOMAIN_RE = re.compile(
    r"amazon\.(co\.uk|com\.au|com\.br|com\.mx|co\.jp|com|de|fr|it|es|nl|se|pl|ca|in)",
    re.IGNORECASE,
)
_MARKETPLACE_CURRENCY = {
    "co.uk": "GBP", "com": "USD", "de": "EUR", "fr": "EUR", "it": "EUR",
    "es": "EUR", "nl": "EUR", "se": "SEK", "pl": "PLN", "ca": "CAD",
    "in": "INR", "co.jp": "JPY", "com.au": "AUD", "com.br": "BRL", "com.mx": "MXN",
}


def _extract_currency_code(text: str) -> Optional[str]:
    """The invoice's currency code (e.g. "GBP"), from the marketplace domain in
    the "visit amazon.xx/contact-us" footer line, or None if not found."""
    domain_match = _MARKETPLACE_DOMAIN_RE.search(text)
    if not domain_match:
        return None
    return _MARKETPLACE_CURRENCY.get(domain_match.group(1).lower())


_CURRENCY_SYMBOLS = "£€$¥"


def _strip_currency_symbol(value: Optional[str]) -> Optional[str]:
    """
    Removes any currency symbol from a parsed money value, leaving a plain
    numeric string (e.g. "£12.74" -> "12.74"). The currency itself is
    recorded once, separately, in currency_code -- repeating it on every
    money field would just be duplication.
    """
    if not value:
        return value
    result = value
    for symbol in _CURRENCY_SYMBOLS:
        result = result.replace(symbol, "")
    return result

 
# Three name/address blocks on the invoice, each matched by its own regex:
#   _BUYER_BLOCK_RE    - unlabeled buyer name/address printed once near the
#                        top; source of buyer_name, and a fallback address.
#   _BILLING_BLOCK_RE  - the labeled "Billing address" block further down.
#   _DELIVERY_BLOCK_RE - the labeled "Delivery address" block further down.
# In real Drive OCR output, all three read cleanly as label -> name ->
# address line(s) -> next label, with no column scrambling between them —
# confirmed against the real OCR sample.
_BUYER_BLOCK_RE = re.compile(
    r"VAT\s*#\s*[A-Z0-9]+\s*\n(.*?)(?:For [^\n]*contact-us|Billing address)",
    re.IGNORECASE | re.DOTALL,
)
_BILLING_BLOCK_RE = re.compile(
    r"Billing address\s*\n(.*?)(?:Order information|Delivery address|Sold by)",
    re.IGNORECASE | re.DOTALL,
)
_DELIVERY_BLOCK_RE = re.compile(
    r"Delivery address\s*\n(.*?)(?:Sold by|VAT\s*#|Invoice details)",
    re.IGNORECASE | re.DOTALL,
)
_BUYER_NOISE_LABEL_RE = re.compile(
    r"^(Invoice|Total payable|Order|VAT|Credit note|Original invoice|Refunded)\b",
    re.IGNORECASE,
)
_DATE_ONLY_LINE_RE = re.compile(r"^\d{1,2}[./]\d{1,2}[./]\d{2,4}$|^\d{1,2}\s+[A-Za-z]+\s+\d{4}$")
 
 
def _filter_address_lines(lines: list[str], known_values: set) -> list[str]:
    return [
        l for l in lines
        if not _BUYER_NOISE_LABEL_RE.match(l)
        and not re.fullmatch(_MONEY, l)
        and not _DATE_ONLY_LINE_RE.match(l)
        and l not in known_values
    ]
 
 
def _name_and_address_from_block(pattern: "re.Pattern", text: str, known_values: set) -> tuple:
    """
    (name, address) from a "<label>\\n<name>\\n<address line(s)>" block; None, None if not found.
    """
    match = pattern.search(text)
    if not match:
        return None, None
 
    lines = _filter_address_lines(
        [l.strip() for l in match.group(1).splitlines() if l.strip()], known_values
    )
    if not lines:
        return None, None
 
    return lines[0], (", ".join(lines[1:]) if len(lines) > 1 else None)
 
 
def _extract_buyer_block(text: str, header_fields: dict) -> tuple:
    """
    Extract the buyer's name and address from the unlabeled block printed
    once near the top of the invoice (see _BUYER_BLOCK_RE). Returns
    (buyer_name, buyer_address), or (None, None) if the block isn't found.
 
    header_fields' already-parsed values (invoice_number,
    invoice_date_delivery_date, total_payable) are passed through so
    _filter_address_lines can drop them; those header rows can land inside
    the same captured window as the buyer block in the raw text.
    """
    known_values = {
        v for v in (
            header_fields.get("invoice_number"),
            header_fields.get("invoice_date_delivery_date"),
            header_fields.get("total_payable"),
        ) if v
    }
    return _name_and_address_from_block(_BUYER_BLOCK_RE, text, known_values)
 
 
def _extract_billing_address(text: str, known_values: set) -> Optional[str]:
    """
    address only — the "Billing address" block's own name line is dropped (buyer_name covers it).
    """
    _, address = _name_and_address_from_block(_BILLING_BLOCK_RE, text, known_values)
    return address
 
 
def _extract_delivery_address(text: str, known_values: set) -> Optional[str]:
    """
    address only, same shape as _extract_billing_address.
    """
    _, address = _name_and_address_from_block(_DELIVERY_BLOCK_RE, text, known_values)
    return address
 
 
def parse_invoice_text(text: str) -> dict:
    """
    Field extraction from OCR'd invoice text.
    """
    header = {
        "payment_reference_id": _first_match(r"Payment\s+reference\s+ID\s+([A-Za-z0-9]+)", text),
        "sold_by": _first_match(r"Sold\s+by\s+([^\n]+)", text),
        "invoice_date_delivery_date": _first_match(
            rf"Invoice\s+date\s*/\s*Delivery\s+date\s*{_DATE_VALUE}", text
        ),
        "invoice_number": _first_match(r"(?<!Original\s)(?<!for\s)Invoice\s*#\s*([A-Za-z0-9]+)", text),
        "total_payable": _first_match(rf"Total\s+payable\s*({_MONEY})", text),
        "order_date": _first_match(rf"Order\s+date\s*{_DATE_VALUE}", text),
        "order_number": _first_match(r"Order\s*#\s*(\d{3}-\d{7}-\d{7})", text),
        "invoice_total": _first_match(rf"Invoice\s+total\s*({_MONEY})", text),
        "shipping_charges": _first_match(rf"Shipping\s+Charges?\s*({_MONEY})", text),
        "promotions": _first_match(rf"Promotions?\s*({_MONEY})", text),
    }
 
    buyer_name, buyer_address = _extract_buyer_block(text, header)
    header["buyer_name"] = buyer_name
    header["buyer_address"] = buyer_address
 
    known_values = {v for v in (header.get("invoice_number"), header.get("order_number")) if v}
    header["billing_address"] = _extract_billing_address(text, known_values) or buyer_address
    header["delivery_address"] = _extract_delivery_address(text, known_values) or buyer_address

    header["currency_code"] = _extract_currency_code(text)
    for money_field in ("total_payable", "invoice_total", "shipping_charges", "promotions"):
        header[money_field] = _strip_currency_symbol(header[money_field])

    header["line_items"] = _extract_line_items(text)
    for item in header["line_items"]:
        for price_field in ("unit_price_excl_vat", "unit_price_incl_vat", "subtotal"):
            item[price_field] = _strip_currency_symbol(item[price_field])

    for key, value in header.items():
        if isinstance(value, str):
            header[key] = _collapse_whitespace(value)
    for item in header["line_items"]:
        for field_name, value in item.items():
            if isinstance(value, str):
                item[field_name] = _collapse_whitespace(value)

    return header
 
 
# --------------------------------------------------------------------------
# Matching
# --------------------------------------------------------------------------
 
def invoice_matches(
    text: str,
    parsed: dict,
    asin: str,
    name: Optional[str],
    start_date: date,
    end_date: date,
) -> bool:
    """
    True if this parsed invoice matches the search inputs. ASIN is checked
    against the parsed line items first, then, in case parsing missed it,
    against a plain substring search over the raw OCR text. name, if given,
    is checked only against the parsed buyer_name.
    order_date or invoice_date must fall within [start_date, end_date] - an invoice whose
    own order_date can't be parsed, or falls outside the range, is rejected.
    This is needed because the folder-level date filter upstream is only an
    approximate.
    """
    line_item_asins = {li["asin"].upper() for li in parsed.get("line_items", []) if li.get("asin")}
    if asin.upper() not in line_item_asins and asin.upper() not in text.upper():
        return False

    if name:
        buyer_name = (parsed.get("buyer_name") or "").upper()
        if name.upper() not in buyer_name:
            return False

    order_date = _parse_date_value(parsed.get("order_date"))
    invoice_date = _parse_date_value(parsed.get("invoice_date_delivery_date"))

    order_in_range = order_date is not None and start_date <= order_date <= end_date
    invoice_in_range = invoice_date is not None and start_date <= invoice_date <= end_date

    return order_in_range or invoice_in_range