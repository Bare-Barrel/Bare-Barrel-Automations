import requests
# import json
import pandas as pd
import datetime as dt
# import io
# import gzip
import time
from ad_api.api.reports import Reports
from ad_api.base.exceptions import (
    AdvertisingApiTooManyRequestsException,
    AdvertisingApiException,
)
from ad_api.base import Marketplaces
from amazon_advertising_report_types_v3 import table_names
from utility import to_list
import postgresql
import re
import os
import ast
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

tenants = postgresql.get_tenants()


def request_report(
    ad_product,
    report_type_id,
    group_by,
    start_date,
    end_date,
    time_unit="DAILY",
    account="Bare Barrel",
    marketplace="US",
):
    """
    Requests a Sponsored Products report.
    Request the creation of a performance report for all entities of a single type which have
    performance data to report. Record types can be one of campaigns, adGroups, keywords,
    productAds, asins, and targets. Note that for asin reports, the report currently can not
    include metrics associated with both keywords and targets. If the targetingId value is set
    in the request, the report filters on targets and does not return sales associated with keywords.
    If the targetingId value is not set in the request, the report filters on keywords and does not
    return sales associated with targets. Therefore, the default behavior filters the report on keywords.
    Also note that if both keywordId and targetingId values are passed, the report filters on targets only
    and does not return keywords.

    Args:
        ad_product (str): ["SPONSORED_PRODUCTS", "SPONSORED_BRANDS", "SPONSORED_DISPLAY"]
        report_type_id (str): ['spCampaigns', 'spTargeting', 'spSearchTerm', 'spAdvertisedProduct', 'spPurchasedProduct', 'sbPurchasedProduct']
        group_by (str): will convert into a list spCampaigns: [campaign, adGroup, ad, productAds, placement, category, asin]
        marketplace (str): ['US', 'CA']. It will retrieve the profile_id of the marketplace
    Returns:
        ad_api.api.ApiResponse.payload (dict)
    """
    metrics = table_names[ad_product][report_type_id][group_by]['metrics']
    # Selects date columns as per time unit
    if time_unit == 'DAILY':
        columns = metrics.replace(' startDate,', '').replace(' endDate,', '')
    elif time_unit == 'SUMMARY':
        columns = metrics.replace(' date,', '')

    body = {
        "name": f"{ad_product} ({account}-{marketplace}) {report_type_id} {group_by} {start_date} - {end_date}",
        "startDate": str(start_date),
        "endDate": str(end_date),
        "configuration": {
            "adProduct": ad_product,
            "groupBy": ast.literal_eval(group_by),  # converts to list
            "columns": columns.split(', '),
            # "filters": [
            #     {
            #         "field": "keywordType",
            #         "values": filters[report_type_id]
            #     }
            # ],
            "reportTypeId": report_type_id,
            "timeUnit": time_unit,
            "format": "GZIP_JSON",
        },
    }
    logger.info(f"Requesting for {account}-{marketplace} \n{body['name']}")

    sleep_multiplier = 1
    while True:
        try:
            response = Reports(
                account=f'{account}-{marketplace}', 
                marketplace=Marketplaces[marketplace],
                ).post_report(body=body)
            return response.payload

        except AdvertisingApiTooManyRequestsException as e:
            logger.warning(e)
            time.sleep(60 * sleep_multiplier)
            sleep_multiplier += 0.05

        # Duplicate request error
        except AdvertisingApiException as e:
            logger.warning(e)
            error_message = e.error

            if error_message['code'] != '425':
                logger.error("Error code is not 425: Too Early - Request is a duplicate of a processing request")
                raise e

            # Gets duplicate requested reportId
            report_id = error_message['detail'].split()[-1]
            response = Reports(
                account=f'{account}-{marketplace}', 
                marketplace=Marketplaces[marketplace],
                ).get_report(report_id)
            return response.payload


def download_report(report_id, root_directory, report_name, account, marketplace):
    """
    Once you have made a successful POST call, report generation can take up to three hours.
    You can check the report generation status by using the reportId returned in the initial
    request to call the GET report endpoint: GET /reporting/reports/{reportId}.
    If the report is still generating, status is set to PENDING or PROCESSING.
    When your report is ready to download, status returns as COMPLETED, and you will see an
    address in the url field.

    Args:
        report_id (str): id of the report to be downloaded after posting a request report
        root_directory (str | os.path): save directory
        report_name (str)

    Returns:
        file_path (str)
    """
    # regexing save path
    match = re.search(r'(SPONSORED_\w+)\s\(.*(\w{2})\)\s(\w+)\s(\[.+\])', report_name)
    ad_product, marketplace, report_type_id, group_by = (
        match[1],
        match[2],
        match[3],
        match[4],
        )
    table_name = table_names[ad_product][report_type_id][group_by]['table_name']
    directory = os.path.join(root_directory, ad_product, table_name, marketplace)

    if not os.path.exists(directory) and directory:
        logger.info(f"Creating new directory: {directory}")
        os.makedirs(directory)

    while True:
        logger.info(f"Downloading {report_name}")

        try:
            response = Reports(
                account=f'{account}-{marketplace}',
                marketplace=Marketplaces[marketplace],
            ).get_report(reportId=report_id)

            if not response:
                logger.warning("\tSkipping. . .")
                continue

            status, url = response.payload['status'], response.payload['url']
            logger.info(f"\tReport status: {status}")

            if status == 'COMPLETED':
                # Download the report
                response = requests.get(url)

                if response.status_code == 200:
                    file_path = os.path.join(directory, report_name + '.json.gz')

                    with open(file_path, 'wb') as file:
                        file.write(response.content)

                    logger.info("File downloaded successfully.")

                    return file_path

                else:
                    logger.info("Failed to download file.")
                    logger.info("\tRedownloading...")

            elif status == 'FAILED':
                break

        except Exception as error:
            logger.error(error)

        time.sleep(60)


def request_download_reports(
    ad_product,
    report_type_id,
    group_by,
    start_date,
    end_date,
    directory,
    time_unit="DAILY",
    account="Bare Barrel",
    marketplace="US",
):
    """Streamlines the process of downloading reports."""
    response = request_report(
        ad_product,
        report_type_id,
        group_by,
        start_date,
        end_date,
        time_unit,
        account,
        marketplace,
    )

    report_id, report_name = response['reportId'], response['name']

    file_path = download_report(report_id, directory, report_name, account, marketplace)

    return file_path


def combine_data(directory=None, file_paths=[], file_extension='.json.gz'):
    """
    Combines files in a directory and/or in file_paths.
    Inserts `marketplace` and `date` to the returned dataframe.
    """
    # gets all similar file types in a directory
    if directory:
        for root, dirs, files in os.walk(directory):
            for filename in files:
                if file_extension in filename:
                    print(os.path.join(root, filename))
                    file_paths.append(os.path.join(root, filename))

    # combines data
    combined_data = pd.DataFrame()

    for file_path in file_paths:
        df = pd.read_json(file_path)

        # manually adds account, marketplace and date
        match = re.search(r'\((.*)-(\w{2})\).+(20\d\d-\d\d-\d\d)', file_path)
        account, marketplace = match[1], match[2]
        df.insert(1, 'tenant_id', tenants[account])
        df.insert(2, 'marketplace', marketplace)

        combined_data = pd.concat([df, combined_data], ignore_index=True)

    return combined_data


def update_data(
    ad_product,
    report_type_id,
    start_date,
    end_date,
    account="Bare Barrel",
    marketplaces=["US", "CA", "UK"],
):
    """
    Adds upsert data step to request_download_reports by
    combining the downloaded files
    """
    report_ids = {}

    # requests reports
    for group_by in table_names[ad_product][report_type_id]:
        for marketplace in to_list(marketplaces):
            response = request_report(
                ad_product,
                report_type_id,
                group_by,
                start_date,
                end_date,
                account=account,
                marketplace=marketplace,
            )
            report_ids[response['name']] = response['reportId']
            time.sleep(10)

    # download reports
    gzipped_directory = os.path.join('PPC Data', 'RAW Gzipped JSON Reports')

    for report_name in report_ids:
        # regexing table name
        match = re.search(
            r'(SPONSORED_\w+)\s\(.*(\w{2})\)\s(\w+)\s(\[.+\])', report_name
        )
        ad_product, marketplace, report_type_id, group_by = (
            match[1],
            match[2],
            match[3],
            match[4],
        )
        table_name = table_names[ad_product][report_type_id][group_by]['table_name']
        table_name = f"{ad_product.lower()}.{table_name}"

        # download and save
        file_path = download_report(
            report_ids[report_name],
            gzipped_directory,
            report_name,
            account,
            marketplace,
        )

        try:
            # manually adds marketplace
            data = pd.read_json(file_path)
            data['marketplace'] = marketplace
            data['tenant_id'] = tenants[account]

            # removes null
            if 'ad_group_id' in data.columns:
                data.fillna({"ad_group_id": 0}, inplace=True)

            if 'campaign_id' in data.columns:
                data.fillna({"campaign_id": 0}, inplace=True)

            # upserts data
            postgresql.upsert_bulk(table_name, data, file_extension='pandas')
        except Exception as error:
            logger.error(error)


def update_all_data(
    start_date,
    end_date,
    ad_products=["SPONSORED_PRODUCTS", "SPONSORED_BRANDS", "SPONSORED_DISPLAY"],
    account="Bare Barrel",
    marketplaces=["US", "CA", "UK"],
):
    """
    Download all reports & upserts to database.
    It can only update up to 31 days at a time.

    Note: SD tables aren't created yet
    """
    max_date_range = 31
    data_retention = {
        'SPONSORED_PRODUCTS': 95,
        'SPONSORED_BRANDS': 60,
        'SPONSORED_DISPLAY': 65
    }

    report_ids = {}
    # requests reports
    for ad_product in to_list(ad_products):
        data_retention_start_date = dt.datetime.now(
            dt.timezone.utc
        ).date() - dt.timedelta(days=data_retention[ad_product])

        for report_type_id in table_names[ad_product]:
            for group_by in table_names[ad_product][report_type_id]:
                for marketplace in to_list(marketplaces):
                    current_start_date = (
                        start_date
                        if start_date >= data_retention_start_date
                        else data_retention_start_date
                    )
                    current_end_date = min(
                        end_date, current_start_date + dt.timedelta(days=max_date_range)
                    )

                    while current_start_date < end_date:
                        response = request_report(
                            ad_product,
                            report_type_id,
                            group_by,
                            current_start_date,
                            current_end_date,
                            account=account,
                            marketplace=marketplace,
                        )
                        report_ids[response['name']] = response['reportId']

                        current_start_date = current_end_date + dt.timedelta(days=1)
                        current_end_date = min(
                            end_date,
                            current_end_date + dt.timedelta(days=max_date_range),
                        )
                        time.sleep(12)

    # download reports
    gzipped_directory = os.path.join('PPC Data', 'RAW Gzipped JSON Reports')

    for report_name in report_ids:
        # regexing table name
        match = re.search(
            r'(SPONSORED_\w+)\s\(.*(\w{2})\)\s(\w+)\s(\[.+\])', report_name
        )
        ad_product, marketplace, report_type_id, group_by = (
            match[1],
            match[2],
            match[3],
            match[4],
        )
        table_name = table_names[ad_product][report_type_id][group_by]['table_name']
        table_name = f"{ad_product.lower()}.{table_name}"

        # download and save
        file_path = download_report(
            report_ids[report_name],
            gzipped_directory,
            report_name,
            account,
            marketplace,
        )

        # manually ads marketplace and tenant_id
        data = pd.read_json(file_path)
        data['marketplace'] = marketplace
        data['tenant_id'] = tenants[account]

        # removes null
        if 'ad_group_id' in data.columns:
            data.fillna({"ad_group_id": 0}, inplace=True)

        if 'campaign_id' in data.columns:
            data.fillna({"campaign_id": 0}, inplace=True)

        # upserts data
        postgresql.upsert_bulk(table_name, data, file_extension='pandas')


def create_table(directory, drop_table_if_exists=False):
    """
    Creates table by iterating all files in a directory & combining data.
    Upserts data by default.
    IMPORTANT!: Overlapping data may result to Primary Key error.
    """
    # regexing table name
    match = re.search(r'(SPONSORED_\w*)/(.+)', str(directory))
    ad_product, report_type = match[1], match[2]
    table_name = (
        f"{ad_product.lower()}.{table_names[ad_product][report_type]['table_name']}"
    )

    # combines data in the directory
    combined_data = combine_data(directory, file_extension='.json.gz')

    if combined_data.empty:
        logger.info(
            f"Table {table_name} is empty.\n\tCancelling table creation & upsertion. . ."
        )
        return

    with postgresql.setup_cursor() as cur:
        if drop_table_if_exists:
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")

        logger.info(f"Creating table {table_name}")
        postgresql.create_table(
            cur, file_path=combined_data, file_extension='pandas', table_name=table_name
        )

        logger.info("\tAdding triggers...")
        postgresql.update_updated_at_trigger(cur, table_name)

        logger.info("\tUpserting data")
        postgresql.upsert_bulk(table_name, combined_data, file_extension='pandas')



if __name__ == '__main__':
    datetime_now = dt.datetime.now()
    day = datetime_now.day
    hour = datetime_now.hour
    date_today = dt.date.today()

    for account in tenants.keys():
        # Updates 90 days at the start of the month
        if day == 1 and hour == 1:
            logger.info("-UPDATING SP LAST 90 DAYS-")
            start_date, end_date = (
                date_today - dt.timedelta(days=90),
                date_today - dt.timedelta(days=60),
            )
            update_all_data(start_date, end_date, 'SPONSORED_PRODUCTS', account=account)
            logger.info("-UPDATING SP, SB & SD LAST 60 DAYS-")
            update_all_data(
                start_date + dt.timedelta(days=30),
                end_date + dt.timedelta(days=30),
                account=account,
            )
            logger.info("-UPDATING SP, SB & SD LAST 30 DAYS-")
            update_all_data(
                start_date + dt.timedelta(days=60), date_today, account=account
            )

        # Updates last 30 days at the middle of the month
        elif day == 15 and hour == 1:
            logger.info("-UPDATING SP, SB & SD LAST 30 DAYS-")
            start_date, end_date = (
                date_today - dt.timedelta(days=30),
                date_today,
            )
            update_all_data(start_date, end_date, account=account)

        # Updates last 7 days daily
        elif day not in (1, 15) and hour == 1:
            logger.info("-UPDATING SP, SB & SD LAST 7 DAYS-")
            start_date, end_date = (
                date_today - dt.timedelta(days=7),
                date_today,
            )
            update_all_data(start_date, end_date, account=account)

        # Updates reports in `campaign` & `advertised_product` every hour
        elif day not in (1, 15) and hour != 1:
            logger.info("-UPDATING SP Campaigns & Advertised Product LAST 2 DAYS-")
            start_date, end_date = (
                date_today - dt.timedelta(days=1),
                date_today,
            )
            update_data(
                'SPONSORED_PRODUCTS',
                'spCampaigns',
                start_date,
                end_date,
                account=account,
            )
            update_data(
                'SPONSORED_PRODUCTS',
                'spAdvertisedProduct',
                start_date,
                end_date,
                account=account,
            )