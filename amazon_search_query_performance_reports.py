import datetime as dt
from sp_api.base import Marketplaces
from amazon_reports import request_report, get_report, download_report
from sp_api.base.reportTypes import ReportType
import time
import pandas as pd
import postgresql
import requests
import gzip
import json
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

base_table_name = 'brand_analytics.search_query_performance'
tenants = postgresql.get_tenants()

"""
Provides overall query performance, such as impressions, clicks, cart adds, and purchases for a given ASIN and specified date range. Data is available at different reporting periods: WEEK, MONTH, and QUARTER. Requests cannot span multiple periods. For example, a request at WEEK level could not start on 2025-01-05 and end on 2025-01-18 as this would span two weeks.

This report accepts the following reportOptions values:

asin - (Required) The Amazon Standard Identification Number (ASIN) for which you want data.
reportPeriod - Specifies the reporting period for the report. Values include WEEK, MONTH, and QUARTER. Example: "reportOptions":{"reportPeriod": "WEEK"}
Requests must include the reportPeriod in the reportsOptions. Use the dataStartTimeand dataEndTime parameters to specify the date boundaries for the report. The dataStartTime and dataEndTime values must correspond to valid first and last days in the specified reportPeriod. For example, dataStartTime** must be a Sunday and dataEndTime must be a Saturday when reportPeriod=WEEK.
"""


report_id = request_report(ReportType.GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT, 
                            account,
                            marketplace,
                            start_date=current_date, 
                            end_date=current_date,
                            reportPeriod="WEEK")