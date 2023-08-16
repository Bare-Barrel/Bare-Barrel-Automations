from datetime import datetime, timedelta
from sp_api.api import ReportsV2
from sp_api.base.reportTypes import ReportType
from sp_api.base import Marketplaces
import time


response = ReportsV2(marketplace=Marketplaces['US']).create_report(
                reportType=ReportType.GET_SALES_AND_TRAFFIC_REPORT,
                dateGranularity='DAY',
                asinGranularity='CHILD',
                # optionally, you can set a start and end time for your report
                dataStartTime=(datetime.utcnow() - timedelta(days=7)).isoformat(),
                dataEndTime=(datetime.utcnow() - timedelta(days=1)).isoformat()
)

report_id = response.payload['reportId']
while True:
    result = ReportsV2().get_report(report_id)
    payload = result.payload
    status = payload['processingStatus']
    print(f"Report Processing Status: {status}")
    if payload['processingStatus'] == 'DONE':
        document_id = result.payload['reportDocumentId']
        break
    time.sleep(15)

print(result.payload)
response = ReportsV2().get_report_document(document_id)
print(response)
print(response.payload) # object containing a report id 