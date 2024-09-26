from playwright.sync_api import Playwright, sync_playwright
from playwright.async_api import async_playwright, expect
import json
import pandas as pd
import datetime as dt
import re
import amazon
import time
import asyncio
import os
import re
import shutil
import postgresql
from utility import get_day_of_week, reposition_columns
from playwright_setup import setup_playwright, login_amazon
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

with open('config.json') as f:
    config = json.load(f)

storage_sellercentral = 'storage_sellercentral_amazon.json'

table_names = {
    'asin': 'brand_analytics.search_query_performance_asin_view',
    'brand': 'brand_analytics.search_query_performance_brand_view'
}

async def generate_download(page, marketplace='US', reporting_range='weekly', date_report=None, view='brand', asin=''):
    """
    Downloads Search Query Performance Reports.

    Args:
    - page (Playwright.page)
    - marketplace (str): 'US', 'CA', 'UK'. Country code for UK is GB.
    - reporting_range (str): 'weekly', 'monthly', 'quarterly'.
    - dates (str|list|dt.date): date to download YYYY-MM-DD.
    - view (str): `brand` or `asin` reporting view.
    - asin (str): asin to download.

    Return:
    - bool
    """
    logger.info(f"@SearchQueryPerformance Querying {view} {asin} ({marketplace}) {date_report}")
    
    country_id = 'GB' if marketplace == 'UK' else marketplace

    reporting_range_formats = {'weekly': 'weekly-week', 'monthly': 'monthly-year=2023&2023-month=2023-04-30', 'quarterly': 'quarterly-year=2023&2023-quarter=2023-03-31'} # unfinished
    sqp_url = {
                "brand": "https://sellercentral.amazon.com/brand-analytics/dashboard/query-performance?view-id=query-performance-brands-view" + 
                            "&brand={}&reporting-range={}&{}={}&country-id={}".format(
                            config['amazon_brand_id'], reporting_range, reporting_range_formats[reporting_range], 
                            date_report, country_id.lower()
                            ),
                "asin": "https://sellercentral.amazon.com/brand-analytics/dashboard/query-performance?view-id=query-performance-asin-view" +
                            "&asin={}&reporting-range={}&{}={}&country-id={}".format(
                            asin, reporting_range, reporting_range_formats[reporting_range], 
                            date_report, country_id.lower()
                            )
    }

    await page.goto(sqp_url[view])
    await expect(page.get_by_role("button", name="Apply")).to_be_visible(timeout=60000)

    if await page.is_visible('text=Error:'):
        logger.info('\tInvalid input. \n\t\tSkipping...')
        return False

    await page.get_by_role("button", name="Apply").click()
    await asyncio.sleep(2)

    if await page.is_visible('text=No data found'):
        logger.info('\tNo data found. \n\t\tSkipping...')
        return False

    await page.get_by_role("button", name="Generate Download").click()
    await asyncio.sleep(5)

    try: 
        logger.info("Generating Download. . .")

        async with page.expect_popup(timeout=150000) as page2_info:
            # selects comprehensive view that's only available in brand view
            if view == 'brand':
                radio_button = page.locator('input[value="COMPREHENSIVE"]')
                await radio_button.click(force=True)

            await page.locator("#downloadModalGenerateDownloadButton").get_by_role("button", name="Generate Download").click()
            await asyncio.sleep(5)

            # spam error to force download
            for _ in range(1, 100):
                try:
                    error_exists = await page.locator('#downloadModalGenerateDownloadButton').is_visible()
                    if not error_exists:
                        break
                    logger.warning("\t#ERROR: Trying again")
                    await page.locator("#downloadModalGenerateDownloadButton").click(force=True)
                    await asyncio.sleep(5)

                except Exception as error:
                    logger.error(error) 
  
            # closes download manager page every popup            
            page2 = await page2_info.value 
            await page2.close()
            logger.info('\tDownload success!')

    except Exception as error:
        logger.error(f"Failed to download\n{error}")
        pass

    return True


async def download_reports(page=None, n_downloads=100):
    """
    Waits for all downloaded reports to be ready to download and then downloads all files in the download manager.
    
    Args:
    - page (Playwright.page)
    - n_downloads (int): Number of reports to download. Max is 100. No pagination on Views 100.

    Returns: 
    - downloaded_filepaths (list)
    """
    if not page:
        page, browser, context = await setup_playwright(storage_state=storage_sellercentral, 
                                                                    headless=config['playwright_headless'], default_timeout=60000)
        await login_amazon(page)

    await page.goto('https://sellercentral.amazon.com/brand-analytics/download-manager')
    await asyncio.sleep(10)

    # waits for progress to be completed
    while await page.is_visible('text=In Progress'):
        logger.info("Download is still In Progress. . .\n\tRefreshing the page")
        await page.reload()
        await asyncio.sleep(15)

    # selects view 100 rows [10, 25 (default), 50, 100]
    await page.click('.select-header')
    await page.click('text=View 100 rows')

    # locates all download buttons
    download_buttons = await page.locator("role=row >> text=Download").all()
    logger.info(f"Commencing Downloads.\n Total Download Buttons: {len(download_buttons) - 1}")

    # downloads report from top to bottom
    downloaded_filepaths = []   # download_button starts at index 1
    max_downloads_per_page = 100 + 1
    count = 1   # total counter
    inner_count = 1 # counter for current iteration from 1 to 101

    while count <= n_downloads:
        try:
            await download_buttons[count].click()

            async with page.expect_download() as download_info:
                async with page.expect_popup() as page2_info:
                    page2 = await page2_info.value    # download popup closes immediately

            # download file path and name
            download = await download_info.value
            csv_path = await download.path() # random guid in a temp folder
            filename = download.suggested_filename # US_Search_Query_Performance_ASIN_View_Week_2022_12_31.csv
            marketplace = filename.split('_')[0]
            view = 'Brand'

            # retrieves asin to filename if it exists
            if 'ASIN' in filename.upper():
                view = 'ASIN'
                metadata = pd.read_csv(csv_path, nrows=0)
                pattern = r'\["(.*?)"\]'
                asin = re.search(pattern, metadata.columns[0]).group(1)
                filename = filename.split('.')[0] + f' [{asin}].' + filename.split('.')[1] # US_Search_Query_Performance_ASIN_View_Week_2022_12_31 [B0B2B9X6P4].csv

            # copies file to folder
            logger.info(f"Copying {filename} to SQP Downloads Folder")
            saved_filepath = os.path.join(os.getcwd(), 'SQP Downloads', view, marketplace, f'{filename}')
            os.makedirs(os.path.dirname(saved_filepath), exist_ok=True)
            shutil.copyfile(csv_path, saved_filepath)
            downloaded_filepaths.append(saved_filepath)

            # next page
            if inner_count == max_downloads_per_page:
                await page.click('span[part="pagination-nav-right"].nav.item')
                download_buttons = await page.locator("role=row >> text=Download").all()
                inner_count = 0

            inner_count += 1
            count += 1


        except Exception as error:
            logger.error(error)

    return downloaded_filepaths


def combine_data(directory=None, file_paths=[], file_extension='.csv'):
    """
    Combines files in a directory and/or in file_paths.
    Inserts `marketplace` and `date`.
    Returns pandas dataframe.
    """
    # gets all similar file types in a directory
    if directory:
        for dirpath, dirnames, filenames in os.walk(directory):
            for filename in filenames:
                if file_extension in filename:
                    file_paths.append(os.path.join(dirpath, filename))
    
    # combines data
    combined_data = pd.DataFrame()

    for file_path in file_paths:
        logger.info(f"Combining {file_path}")
        metadata = pd.read_csv(file_path, nrows=0)
        data     = pd.read_csv(file_path, skiprows=1)

        if data.empty:
            logger.info("\tEmpty data")
            continue

        # cleans & adds metadata
        for col in metadata.columns:
            splitted_col = col.split('=')
            if len(splitted_col) > 1:
                value = re.sub(r'\W', '', splitted_col[1])

            if re.search('ASIN', col, re.IGNORECASE):
                data['asin'] = value

            elif re.search('Reporting Range', col, re.IGNORECASE):
                data['reporting_range'] = value

        data['marketplace'] = re.search(r'/(\w*)_Search_Query', file_path)[1]

        # calculates week number, start & end date
        data.rename(columns={'Reporting Date': 'reporting_date'}, inplace=True)
        data['reporting_date'] = pd.to_datetime(data['reporting_date'])
        data['end_date']       = data['reporting_date']
        data['start_date']     = data['end_date'] - dt.timedelta(days=6)
        data['week']           = data['end_date'].dt.isocalendar().week

        # reposition
        view = re.search(r'(ASIN|Brand)', file_path)[1].lower()
        col_positions = {
            'asin': {'reporting_date': 0, 'reporting_range': 1, 'marketplace': 2, 'asin': 3, 'Search Query': 4},
            'brand': {'reporting_date': 0, 'reporting_range': 1, 'marketplace': 2, 'Search Query': 3}
        }
        data = reposition_columns(data, col_positions[view])

        combined_data = pd.concat([data, combined_data], ignore_index=True)

    # drop duplicates on primary keys
    pkeys = [key for key in col_positions[view]]
    combined_data = combined_data.drop_duplicates(subset=pkeys)
    
    return combined_data



async def scrape_sqp(playwright: Playwright, marketplaces=['US', 'CA', 'UK'], date_reports=[], view=['brand', 'asin'], headless=config['playwright_headless']) -> None:
    """
    Download weekly Search Query Performance per ASIN for each marketplace.
    Then inserts data to search_query_performance

    Args:
    - playwright (Playwright)
    - marketpalces (str): abbreviated countries to download from.
    - date_reports (str|dt.date): weekly dates to download.
    - view (str): select which viewing report to download.

    Returns:
    - None
    """
    logger.info("Starting Search Query Performance Analytics. . .")

    page, browser, context = await setup_playwright(storage_state=storage_sellercentral, 
                                                                    headless=headless, default_timeout=180000)
    await login_amazon(page)

    # downloads for each marketplace, date_report, asin
    downloads = 0
    downloaded_filepaths = []


    async def download_and_upsert(page, n_downloads):
        # download reports via `Downloads Manager`
        logger.info(f"@SearchQueryPerformance Downloading {n_downloads} reports")
        downloaded_filepaths = await download_reports(page, n_downloads=downloads)

        # inserts to amazon db
        table_name = table_names[view]
        data = combine_data(file_paths=downloaded_filepaths)
        postgresql.upsert_bulk(table_name, data, 'pandas')

        # resets download counter
        return 0


    for marketplace in marketplaces:

        if view == 'asin':

            for date_report in date_reports:
                # gets all active asins
                logger.info(f"Getting all active ASINs in {marketplace} {date_report}")

                with postgresql.setup_cursor() as cur:
                    cur.execute(f"""SELECT DISTINCT asin FROM listings_items.summaries 
                                        WHERE status @> ARRAY['DISCOVERABLE', 'BUYABLE'] AND marketplace = '{marketplace}'
                                                AND date >= '{date_report}'::DATE - INTERVAL '6 days' AND date <= '{date_report}'::DATE
                                                AND asin NOT IN (SELECT DISTINCT ASIN FROM {table_names['asin']} 
                                                                    WHERE marketplace = '{marketplace}' AND reporting_date = '{date_report}');""")
                    active_asins = [asin['asin'] for asin in cur.fetchall()]

                logger.info(f"\t{len(active_asins)} ASINs are discoverable")

                if active_asins:
                    for asin in active_asins:
                        downloads_generated = await generate_download(page, marketplace, date_report=date_report, view=view, asin=asin)
                        downloads += 1 if downloads_generated else 0

                        if downloads == 50:
                            downloads = await download_and_upsert(page, downloads) # resets download counter to 0

        elif view == 'brand':
            with postgresql.setup_cursor() as cur:
                cur.execute(f"""SELECT DISTINCT reporting_date FROM brand_analytics.search_query_performance_brand_view 
                                    WHERE marketplace = '{marketplace}'
                                        AND reporting_date >= '{min(date_reports)}'::DATE AND reporting_date <= '{max(date_reports)}'::DATE;""")
                available_date_reports = [reporting_date['reporting_date'] for reporting_date in cur.fetchall()]
                date_reports_to_download = [date_report for date_report in date_reports if date_report not in available_date_reports]

            for date_report in date_reports_to_download:

                downloads += await generate_download(page, marketplace, date_report=date_report, view=view)
                # downloads += 1 if downloads_generated else 0

                if downloads == 50:
                        downloads = await download_and_upsert(page, downloads) # resets download counter to 0


    if downloads != 0:
        await download_and_upsert(page, downloads)


    # saves storage & closes browser.
    await context.storage_state(path = storage_sellercentral)
    await context.close()
    await browser.close()


def create_table(view, drop_table_if_exists=False):
    views_name = {'asin': 'ASIN', 'brand': 'Brand'}
    sqp_directory = os.path.join(os.getcwd(), 'SQP Downloads', views_name[view])
    data = combine_data(sqp_directory)

    table_name = table_names[view]
    with postgresql.setup_cursor() as cur:
        if drop_table_if_exists:
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")

        primary_keys = {
            'asin': 'PRIMARY KEY (reporting_date, reporting_range, marketplace, asin, search_query)',
            'brand': 'PRIMARY KEY (reporting_date, reporting_range, marketplace, search_query)'
        }

        postgresql.create_table(cur, data, file_extension='pandas', table_name=table_name, keys=primary_keys[view])

        postgresql.update_updated_at_trigger(cur, table_name)

        postgresql.upsert_bulk(table_name, data, file_extension='pandas')


async def main():
    """
    Updates ASIN & Brand view last two weeks of data
    """
    async with async_playwright() as playwright:
        last_week = dt.date.today() - dt.timedelta(weeks=1)
        end_date = get_day_of_week(last_week, 'Saturday')
        start_date = end_date - dt.timedelta(weeks=2)

        saturdays = []
        while start_date <= end_date:
            saturdays.append(start_date)
            start_date += dt.timedelta(days=7)

        for view in ['asin', 'brand']:
            task = asyncio.create_task(scrape_sqp(playwright, marketplaces=['US', 'CA', 'UK'], date_reports=saturdays, view=view))
            await task


if __name__ == '__main__':
    asyncio.run(main())