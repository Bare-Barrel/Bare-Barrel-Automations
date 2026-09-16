"""
Amazon Invoice Search — Backend
================================
 
Given a child ASIN, a date range, and an optional name, finds matching
Amazon invoice PDFs in a Google Shared Drive, OCRs them via Drive's
copy-to-Google-Doc mechanism, parses key fields, and loads results into
BigQuery and a Google Sheet.
 
search_invoices() is the entry point; call it from the CLI below, a
Streamlit form, or a future CS Agent tool definition.
 
This file has the data model, config, Google auth, and all Drive/BigQuery/
Sheet I/O, plus the orchestrator that ties it together. invoice_parsing.py
is kept separate: it's pure text parsing and matching logic.
 
Auth is a single path: get_credentials() calls google.auth.default(), the
same call bigquery_utils.load_to_bigquery makes internally. Both pick up
where GOOGLE_APPLICATION_CREDENTIALS points at. load_to_bigquery also
logs and swallows load errors instead of raising, so a BigQuery failure
won't stop a search or block the Sheet write; check logs if BigQuery ever
looks out of date with the Sheet.
"""
 
import argparse
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from typing import Optional

import google.auth
import pandas as pd
from googleapiclient.discovery import build

import logger_setup
from amazon_invoice_parsing import (
    find_matching_folders,
    invoice_matches,
    parse_invoice_text,
)
from bigquery_utils import load_to_bigquery

logger_setup.setup_logging(__file__)
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR) # mutes this specific harmless warning
logger = logging.getLogger(__name__)
 
 
# --------------------------------------------------------------------------
# Data model
# --------------------------------------------------------------------------
 
@dataclass
class LineItem:
    product_name: Optional[str]
    asin: Optional[str]
    quantity: Optional[str]
    unit_price_excl_vat: Optional[str]
    vat_rate: Optional[str]
    unit_price_incl_vat: Optional[str]
    subtotal: Optional[str]
 
 
@dataclass
class InvoiceMatch:
    # file/search bookkeeping
    file_id: str
    file_name: str
    folder_name: str
    web_view_link: str
    matched_asin: str
 
    payment_reference_id: Optional[str]
    sold_by: Optional[str]
    invoice_date_delivery_date: Optional[str]
    invoice_number: Optional[str]
    currency_code: Optional[str]
    total_payable: Optional[str]
    buyer_name: Optional[str]
    buyer_address: Optional[str]
    billing_address: Optional[str]
    delivery_address: Optional[str]
    order_date: Optional[str]
    order_number: Optional[str]
    invoice_total: Optional[str]
    shipping_charges: Optional[str]
    promotions: Optional[str]
    line_items: list[LineItem] = field(default_factory=list)
 
    raw_text: str = ""  # debugging only; not written to BQ/Sheet
 
 
# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------
 
# Drive + Sheets scopes for this script's own calls. BigQuery auth is
# handled separately, inside load_to_bigquery (see module docstring). Both
# paths resolve to whatever GOOGLE_APPLICATION_CREDENTIALS points at.
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]
 
SHARED_DRIVE_ID = ""  # "" if you only have access to a specific folder (see README)
INVOICES_ROOT_FOLDER_ID = "0AJo5oEkE171QUk9PVA"  # "" to scan the whole Shared Drive; required if SHARED_DRIVE_ID is ""
SHARED_DRIVE_STAGING_FOLDER_ID = "1MqdVdKjaYM3C-CkWXnv1XA6BTjYHU-VD"  # "" = My Drive root (see README Setup step 5)
 
BQ_PROJECT = "modern-sublime-383117"
BQ_DATASET = "customer_service"
BQ_TABLE = "amazon_invoices"
BQ_TABLE_ID = f"{BQ_DATASET}.{BQ_TABLE}"
 
SHEET_ID = "1wuc8t3sMcZzNN3Riihtv29aeDr-6xM1nGiVvffnW7Uw"
SHEET_TAB = "Invoices"
 
# One row per invoice for both BigQuery and the Sheet; line items go into a
# single JSON column at the end (see _match_to_flat_dict).
SHEET_COLUMNS = [
    "file_id", "file_name", "folder_name", "web_view_link",
    "payment_reference_id", "sold_by", "invoice_date_delivery_date",
    "invoice_number", "currency_code", "total_payable", "buyer_name", "buyer_address",
    "billing_address", "delivery_address", "order_date", "order_number",
    "invoice_total", "shipping_charges", "promotions",
    "matched_asin", "loaded_at", "line_items_json",
]

OCR_WORKERS = 8  # concurrent Drive requests — tune down if you start seeing 429s
 
 
# --------------------------------------------------------------------------
# Auth
# --------------------------------------------------------------------------
 
def get_credentials():
    # ADC — picks up GOOGLE_APPLICATION_CREDENTIALS, same as load_to_bigquery.
    # A key-file identity comes back unscoped, so scopes are requested here.
    credentials, _ = google.auth.default(scopes=SCOPES)
    return credentials
 
 
# --------------------------------------------------------------------------
# Drive: folder/file listing + OCR (copy PDF -> Google Doc triggers OCR -> export as text)
# --------------------------------------------------------------------------
 
def _drive_list_all(drive_service, **list_kwargs) -> list[dict]:
    """
    Paginate through files().list() and return every result.
    """
    results: list[dict] = []
    page_token = None
    while True:
        response = drive_service.files().list(pageToken=page_token, **list_kwargs).execute()
        results.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return results
 
 
def _shared_drive_scope_kwargs() -> dict:
    """
    corpora="drive" + driveId requires membership in the whole Shared Drive.
    If the account only has access to a specific folder in it (SHARED_DRIVE_ID
    left blank — see config comment), omit both: includeItemsFromAllDrives +
    supportsAllDrives alone are enough for a query scoped by a known parent
    folder ID, and don't require Shared-Drive-level membership.
    """
    if SHARED_DRIVE_ID:
        return {"corpora": "drive", "driveId": SHARED_DRIVE_ID}
    return {}
 
 
def list_invoice_folders(drive_service) -> list[dict]:
    query = "mimeType='application/vnd.google-apps.folder' and trashed=false"
    if INVOICES_ROOT_FOLDER_ID:
        query += f" and '{INVOICES_ROOT_FOLDER_ID}' in parents"
    return _drive_list_all(
        drive_service,
        q=query,
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        fields="nextPageToken, files(id, name)",
        pageSize=1000,
        **_shared_drive_scope_kwargs(),
    )
 
 
def list_pdfs_in_folder(drive_service, folder_id: str, asin: Optional[str] = None) -> list[dict]:
    """
    List PDFs directly under folder_id. If asin is given, pre-filters
    server-side using Drive's own full-text search index (fullText contains)
    -- the same index behind Drive's search bar -- so only PDFs whose indexed
    content already mentions the ASIN come back.
    """
    query = f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false"
    if asin:
        query += f" and fullText contains '{asin}'"
    return _drive_list_all(
        drive_service,
        q=query,
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        fields="nextPageToken, files(id, name, webViewLink)",
        pageSize=1000,
        **_shared_drive_scope_kwargs(),
    )
 
 
def ocr_extract_text(drive_service, file_id: str, file_name: str) -> str:
    # "root" = My Drive root of the service account. Without an explicit
    # parent, the copy would land back in the Shared Drive.
    copy_metadata = {
        "name": f"__ocr_tmp__{file_name}",
        "mimeType": "application/vnd.google-apps.document",
        "parents": [SHARED_DRIVE_STAGING_FOLDER_ID or "root"],
    }
 
    copied = drive_service.files().copy(
        fileId=file_id, body=copy_metadata, supportsAllDrives=True
    ).execute()
    copy_id = copied["id"]
 
    try:
        exported = drive_service.files().export(fileId=copy_id, mimeType="text/plain").execute()
        text = exported.decode("utf-8") if isinstance(exported, bytes) else exported
    finally:
        # Content Manager access can only trash items, not permanently delete.
        drive_service.files().update(
            fileId=copy_id, body={"trashed": True}, supportsAllDrives=True
        ).execute()
 
    return text
 
 
# --------------------------------------------------------------------------
# Loading matched invoices into BigQuery and the Sheet
# --------------------------------------------------------------------------
 
def _match_to_flat_dict(m: InvoiceMatch, loaded_at: str) -> dict:
    """
    One flat dict per invoice — shared by both the BigQuery DataFrame and
    the Sheet row. Line items go into a single line_items_json string, each item
    flagged with is_matched_asin, so both destinations stay in the same
    shape pandas_gbq.to_gbq can load without a manual schema.
 
    loaded_at is computed once in search_invoices() and passed here, so
    every row from the same search, across both BigQuery and the Sheet,
    carries the identical timestamp, rather than each call stamping its own.
    """
    items_json = []
    for li in m.line_items:
        item = asdict(li)
        item["is_matched_asin"] = bool(li.asin) and li.asin.upper() == m.matched_asin.upper()
        items_json.append(item)
 
    return {
        "file_id": m.file_id,
        "file_name": m.file_name,
        "folder_name": m.folder_name,
        "web_view_link": m.web_view_link,
        "payment_reference_id": m.payment_reference_id,
        "sold_by": m.sold_by,
        "invoice_date_delivery_date": m.invoice_date_delivery_date,
        "invoice_number": m.invoice_number,
        "currency_code": m.currency_code,
        "total_payable": m.total_payable,
        "buyer_name": m.buyer_name,
        "buyer_address": m.buyer_address,
        "billing_address": m.billing_address,
        "delivery_address": m.delivery_address,
        "order_date": m.order_date,
        "order_number": m.order_number,
        "invoice_total": m.invoice_total,
        "shipping_charges": m.shipping_charges,
        "promotions": m.promotions,
        "matched_asin": m.matched_asin,
        "loaded_at": loaded_at,
        "line_items_json": json.dumps(items_json, ensure_ascii=False),
    }
 
 
def _match_to_sheet_row(m: InvoiceMatch, loaded_at: str) -> list:
    row = _match_to_flat_dict(m, loaded_at)
    # Forces dates to stay literal text.
    date_like_fields = {"invoice_date_delivery_date", "order_date"}
    return [
        f"'{row[col]}" if col in date_like_fields and row[col] else row[col]
        for col in SHEET_COLUMNS
    ]
 
 
def load_matches_to_bigquery(matches: list[InvoiceMatch], loaded_at: str) -> None:
    """
    Reuses bigquery_utils.load_to_bigquery (ADC auth, logs+swallows errors).
    """
    df = pd.DataFrame([_match_to_flat_dict(m, loaded_at) for m in matches])

    # df.to_csv("output.csv", index=False, encoding="utf-8")
    load_to_bigquery(df, table_id=BQ_TABLE_ID, project_id=BQ_PROJECT, load_type="append")
 
 
def _column_letter(index: int) -> str:
    """
    0 -> A, 25 -> Z, 26 -> AA, ...
    """
    index += 1
    letters = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters
 
 
def load_matches_to_sheet(matches: list[InvoiceMatch], credentials, loaded_at: str) -> None:
    """
    Append one row per match to the Sheet. Rows from every past search accumulate;
    the same invoice matching two different searches shows up as two separate rows.
    """
    sheets_service = build("sheets", "v4", credentials=credentials)
    values = [_match_to_sheet_row(m, loaded_at) for m in matches]
    if not values:
        return
 
    last_col = _column_letter(len(SHEET_COLUMNS) - 1)
    sheets_service.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_TAB}!A:{last_col}",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": values},
    ).execute()


def _check_pdf(credentials, pdf, folder_name, asin, name, start_date, end_date):
    drive_service = build("drive", "v3", credentials=credentials)
    text = ocr_extract_text(drive_service, pdf["id"], pdf["name"])
    parsed = parse_invoice_text(text)
    if not invoice_matches(text, parsed, asin, name, start_date, end_date):
        return None

    line_items = [LineItem(**li) for li in parsed.pop("line_items", [])]
    return InvoiceMatch(
        file_id=pdf["id"],
        file_name=pdf["name"],
        folder_name=folder_name,
        web_view_link=pdf.get("webViewLink", ""),
        matched_asin=asin,
        raw_text=text,
        line_items=line_items,
        **parsed,
    )


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
 
def search_invoices(
    asin: str,
    start_date: date,
    end_date: date,
    name: Optional[str] = None,
) -> list[InvoiceMatch]:
    """
    Find, parse, and load matching invoices. Call this from a UI, CLI, or agent tool.
    """
    logger.info("Searching invoices for ASIN %s between %s and %s", asin, start_date, end_date)
 
    credentials = get_credentials()
    identity = getattr(credentials, "service_account_email", None) or "a personal/user account"
    logger.info("Authenticated as: %s", identity)

    drive_service = build("drive", "v3", credentials=credentials)
    try:
        info = drive_service.files().get(
            fileId=INVOICES_ROOT_FOLDER_ID, supportsAllDrives=True, fields="id, name, mimeType"
        ).execute()
        logger.info("Root folder resolved: %r", info)
    except Exception as e:
        logger.error("Could not access INVOICES_ROOT_FOLDER_ID %r: %s", INVOICES_ROOT_FOLDER_ID, e)
 
    all_folders = list_invoice_folders(drive_service)
    logger.info("%d total folder(s) found under INVOICES_ROOT_FOLDER_ID", len(all_folders))
    for f in all_folders:
        logger.info("  %r", f["name"])

    candidate_folders = find_matching_folders(all_folders, start_date, end_date)
    logger.info("%d candidate folder(s) in range", len(candidate_folders))
 
    matches: list[InvoiceMatch] = []
    for folder_num, folder in enumerate(candidate_folders, start=1):
        logger.info("Folder %d/%d: %r", folder_num, len(candidate_folders), folder["name"])

        pdfs = list_pdfs_in_folder(drive_service, folder["id"], asin=asin)
        logger.info("  %d PDF(s) in this folder", len(pdfs))

        done = 0
        with ThreadPoolExecutor(max_workers=OCR_WORKERS) as executor:
            futures = {
                executor.submit(_check_pdf, credentials, pdf, folder["name"], asin, name, start_date, end_date): pdf
                for pdf in pdfs
            }
            for future in as_completed(futures):
                pdf = futures[future]
                done += 1
                try:
                    result = future.result()
                except Exception as e:
                    logger.error("  [%d/%d] %s: error — %s", done, len(pdfs), pdf["name"], e)
                    continue
                logger.info("  [%d/%d] %s: %s", done, len(pdfs), pdf["name"], "match" if result else "no match")
                if result:
                    matches.append(result)

    logger.info("%d match(es) found", len(matches))

    if matches:
        loaded_at = datetime.now(timezone.utc).isoformat()  # shared by both destinations for this search
        load_matches_to_bigquery(matches, loaded_at)
        load_matches_to_sheet(matches, credentials, loaded_at)
 
    return matches
 
 
# --------------------------------------------------------------------------
# CLI, for standalone testing or a Rundeck/Airflow job pointed at this file
# --------------------------------------------------------------------------
 
if __name__ == "__main__":
    cli = argparse.ArgumentParser(
        description="Search Amazon invoice PDFs in Google Drive by ASIN / date range / name."
    )
    cli.add_argument("--asin", required=True, help="Child ASIN to search for")
    cli.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    cli.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    cli.add_argument("--name", default=None, help="Optional name string to further filter matches")
    args = cli.parse_args()
 
    found = search_invoices(
        asin=args.asin,
        start_date=datetime.strptime(args.start_date, "%Y-%m-%d").date(),
        end_date=datetime.strptime(args.end_date, "%Y-%m-%d").date(),
        name=args.name,
    )
 
    # printable = [{**asdict(r), "raw_text": f"<{len(r.raw_text)} chars omitted>"} for r in found]
    # print(json.dumps(printable, indent=2, default=str))
    if found:
        print(f"\n{len(found)} match(es) found and loaded to BigQuery + Sheets.")
    else:
        print("\nNo matching invoices found.")