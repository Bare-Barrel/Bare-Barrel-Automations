"""
Streamlit frontend for the Amazon invoice search backend.
 
Assumed contract with amazon_invoice_search.py:
 
    search_invoices(asin: str, start_date: date, end_date: date,
                     name: Optional[str] = None) -> list[InvoiceMatch]
 
    logger = logging.getLogger("amazon_invoice_search")
        - search_invoices() already logs progress (folder/PDF counters) via
          this logger's .info() calls, per the earlier logging work.
 
    SHEET_COLUMNS: list[str]        - ordered column names for display/export
    _match_to_flat_dict(m, loaded_at) -> dict   - flattens an InvoiceMatch
"""

import logging
import queue
import threading
import time
from datetime import date, timedelta
 
import pandas as pd
import streamlit as st
 
from amazon_invoice_search import (
    SHEET_COLUMNS,
    _match_to_flat_dict,
    search_invoices,
)
 
st.set_page_config(page_title="Amazon Invoice Search", page_icon="🔎", layout="wide")
 
BACKEND_LOGGER_NAME = "amazon_invoice_search"
 
 
class QueueHandler(logging.Handler):
    """Pushes formatted log records into a thread-safe queue for the UI to poll."""
 
    def __init__(self, log_queue: "queue.Queue"):
        super().__init__()
        self.log_queue = log_queue
 
    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.log_queue.put(self.format(record))
        except Exception:  # noqa: BLE001 - never let logging crash the search
            pass
 
 
def _run_search(asin, start_date, end_date, name, log_queue, result_holder):
    """Runs in a background thread; streams progress through log_queue."""
    handler = QueueHandler(log_queue)
    handler.setFormatter(logging.Formatter("%(asctime)s  %(message)s", "%H:%M:%S"))
    backend_logger = logging.getLogger(BACKEND_LOGGER_NAME)
    backend_logger.setLevel(logging.INFO)
    backend_logger.addHandler(handler)
    try:
        matches = search_invoices(
            asin=asin,
            start_date=start_date,
            end_date=end_date,
            name=name or None,
        )
        result_holder["matches"] = matches
    except Exception as exc:  # noqa: BLE001
        result_holder["error"] = str(exc)
        backend_logger.exception("Search failed")
    finally:
        backend_logger.removeHandler(handler)
        log_queue.put(None)  # sentinel: search finished
 
 
def _init_state():
    defaults = {
        "running": False,
        "matches": None,
        "error": None,
        "logs": [],
        "log_queue": None,
        "result_holder": None,
        "thread": None,
        "last_asin": "",
    }
    for key, value in defaults.items():
        st.session_state.setdefault(key, value)
 
 
_init_state()
 
st.title("Amazon Invoice Search")
st.caption(
    "Search Amazon invoice PDFs in Shared Drive by ASIN, order/delivery date range, and "
    "optional buyer name."
)
 
with st.form("search_form"):
    col1, col2 = st.columns(2)
    with col1:
        asin_input = st.text_input("ASIN", placeholder="e.g. B0FJ6L8LH8")
    with col2:
        name_input = st.text_input(
            "Buyer name (optional)", placeholder="e.g. Claire Travis"
        )
 
    default_end = date.today()
    default_start = default_end - timedelta(days=30)
    date_range = st.date_input(
        "Date range (matches order date or invoice/delivery date)",
        value=(default_start, default_end),
        max_value=default_end,
    )
 
    submitted = st.form_submit_button(
        "Search", disabled=st.session_state.running, use_container_width=True
    )
 
if submitted:
    asin = (asin_input or "").strip()
    name = (name_input or "").strip()
 
    if not asin:
        st.error("ASIN is required.")
    elif not isinstance(date_range, tuple) or len(date_range) != 2:
        st.error("Please select both a start and end date.")
    else:
        start_date, end_date = date_range
        st.session_state.running = True
        st.session_state.matches = None
        st.session_state.error = None
        st.session_state.logs = []
        st.session_state.last_asin = asin
 
        log_q: "queue.Queue" = queue.Queue()
        result_holder: dict = {}
        st.session_state.log_queue = log_q
        st.session_state.result_holder = result_holder
 
        thread = threading.Thread(
            target=_run_search,
            args=(asin, start_date, end_date, name, log_q, result_holder),
            daemon=True,
        )
        st.session_state.thread = thread
        thread.start()
        st.rerun()
 
if st.session_state.running:
    st.subheader("Progress")
    log_placeholder = st.empty()
 
    log_q = st.session_state.log_queue
    result_holder = st.session_state.result_holder
    finished = False
 
    # Drain everything currently sitting in the queue without blocking long.
    while True:
        try:
            item = log_q.get(timeout=0.2)
        except queue.Empty:
            break
        if item is None:
            finished = True
            break
        st.session_state.logs.append(item)
 
    log_placeholder.code("\n".join(st.session_state.logs[-500:]) or "Starting…")
 
    if finished:
        st.session_state.running = False
        st.session_state.matches = result_holder.get("matches")
        st.session_state.error = result_holder.get("error")
        st.rerun()
    else:
        time.sleep(1)
        st.rerun()
 
if not st.session_state.running and st.session_state.logs:
    with st.expander("Run log", expanded=False):
        st.code("\n".join(st.session_state.logs[-500:]))
 
if not st.session_state.running and st.session_state.error:
    st.error(f"Search failed: {st.session_state.error}")
 
if not st.session_state.running and st.session_state.matches is not None:
    matches = st.session_state.matches
    count = len(matches)
    st.subheader(f"Results ({count} invoice{'s' if count != 1 else ''} found)")
 
    if matches:
        display_columns = [c for c in SHEET_COLUMNS if c != "loaded_at"]
        rows = [_match_to_flat_dict(m, loaded_at="") for m in matches]
        df = pd.DataFrame(rows)
        df = df[[c for c in display_columns if c in df.columns]]
        st.dataframe(df, use_container_width=True, hide_index=True)
 
        SHEET_URL = "https://docs.google.com/spreadsheets/d/1wuc8t3sMcZzNN3Riihtv29aeDr-6xM1nGiVvffnW7Uw/edit?gid=0#gid=0"
        st.link_button("Open results in Google Sheets", SHEET_URL)
    else:
        st.info("No matching invoices found for the given filters.")