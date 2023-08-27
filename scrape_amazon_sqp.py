from playwright.sync_api import Playwright, sync_playwright, expect
from playwright.async_api import async_playwright
import json
import pandas as pd
import datetime as dt
import re
import amazon
import time
import asyncio
import os
import shutil
import postgresql
from utility import get_day_of_week
from playwright_setup import setup_playwright, login_amazon
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

with open('config.json') as f:
    config = json.load(f)


# generates downloads for each active product
async def download_sqp(page, country='US', reporting_range='weekly', date_report=None, view='brand', asin=None):
    """Downloads Search Query Performance Reports and save it to a temporary path

    Args:
        page (Playwright.page)
        country (str): 'US', 'CA'.
        reporting_range (str): 'weekly', 'monthly', 'quarterly'.
        dates (str|list|dt.date): date to download YYYY-MM-DD.
        view (str): `brand` or `asin` reporting view.
        asin (str): asin to download.

    Return:
        path
    """
    reporting_range_formats = {'weekly': 'weekly-week', 'monthly': 'monthly-year=2023&2023-month=2023-04-30', 'quarterly': 'quarterly-year=2023&2023-quarter=2023-03-31'}
    
    sqp_url = {'brand': "https://sellercentral.amazon.com/brand-analytics/dashboard/query-performance?view-id=query-performance-brands-view&brand={}&reporting-range={}&{}={}&country-id={}",
                'asin': "https://sellercentral.amazon.com/brand-analytics/dashboard/query-performance?view-id=query-performance-asin-view&asin={}&reporting-range={}&{}={}&country-id={}"}

    logger.info(f"@SearchQueryPerformance Querying {asin} ({country}) {date_report}")
    await page.goto(sqp_url[view].format(config['amazon_brand_id'] if view == 'brand' else asin, 
                                            reporting_range, reporting_range_formats[reporting_range], date_report, country.lower()))  # refreshes page as soon as it changes marketplace
    # await page.click('[id*="katal-id-"]')  # *= starts with e.g. id=katal-id-0
    # await page.click(f'[value="{country}" i][role="option"]') # deletes inputs after changing 
    if view == 'asin':
        await page.click('[id="asin"]')
        await page.get_by_text(asin).click()
    # await page.locator('kat-dropdown[id="reporting-range"]').last.click()
    # await page.locator(f'kat-option[value="{reporting_range}" i]').last.click()
    # await page.click(f'[id="{reporting_range_formats[reporting_range]}" i]')
    # await page.click(f'kat-option[value="{date_report}"]')
    await page.get_by_role("button", name="Apply").click()
    await page.get_by_role("button", name="Generate Download").click()
    await asyncio.sleep(5)

    try: 
        logger.info("TRYING")
        async with page.expect_popup() as page2_info:
            await page.locator("#downloadModalGenerateDownloadButton").get_by_role("button", name="Generate Download").click()
            await asyncio.sleep(5)
            # error, click once
            error_exists = await page.locator('#downloadModalGenerateDownloadButton').is_visible(timeout=300)
            if error_exists:
                logger.info("\t#ERROR: Try again")
                await page.locator("#downloadModalGenerateDownloadButton").click()
                await asyncio.sleep(5)
            # closes download manager page every popup            
            page2 = await page2_info.value 
            await page2.close()
            logger.info('\tDownload success!')
    except:
        logger.info("Failed to download")
        pass
    return


async def download_reports(page, n_view_rows=25, n_pages=1):
    """Waits for all downloaded reports to be ready to download and then downloads all files in the download manager.
    
    Args:
        page (Playwright.page)
        n_view_rows (int): Number of rows in the page to download [10, 25, 50, 100].
        pages (int): Number of pages to download.
        path (str, os.path): Path to downloads are saved.

    Return: downloaded file paths (list)"""
    downloaded_filepaths = []
    await page.goto('https://sellercentral.amazon.com/brand-analytics/download-manager')
    await asyncio.sleep(10)
    # waits for progress to be completed
    while await page.is_visible('text=In Progress'):
        logger.info("Download is still In Progress. . .\n\tRefreshing the page")
        await page.reload()
        await asyncio.sleep(15)
    # selects view N rows
    if n_view_rows != 25:
        await page.click('.select-header')
        await page.click(f'text=View {n_view_rows} rows')
    # downloads each report
    download_buttons = await page.get_by_role("row").get_by_text("Download").all()
    logger.info(f"LENGTH OF DOWNLOAD BUTTONS: {len(download_buttons)}")
    for download_button in download_buttons:
        try:
            await download_button.click()
            async with page.expect_download() as download_info:
                async with page.expect_popup() as page2_info:
                    page2 = await page2_info.value    # download popup closes immediately
            download = await download_info.value
            csv_path = await download.path() # random guid in a temp folder
            filename = download.suggested_filename # US_Search_Query_Performance_ASIN_View_Week_2022_12_31.csv
            country = filename.split('_')[0]
            # retrieves asin if it exists
            asin = ''
            if 'ASIN' in filename.upper():
                metadata = pd.read_csv(csv_path, nrows=0)
                pattern = r'\["(.*?)"\]'
                asin = re.search(pattern, metadata.columns[0]).group(1) + '_'
                filename = filename.split('.')[0] + f' [{asin}]' + filename.split('.')[1] # US_Search_Query_Performance_ASIN_View_Week_2022_12_31 [B0B2B9X6P4].csv
            # copies file to folder
            logger.info(f"Copying {filename} to SQP Downloads Folder")
            saved_filepath = os.path.join(os.getcwd(), 'SQP Downloads', f'{filename}')
            shutil.copyfile(csv_path, saved_filepath)
            downloaded_filepaths.append(saved_filepath)
        except Exception as e:
            logger.error(e)
    return downloaded_filepaths


async def scrape_sqp(playwright: Playwright, marketplaces=['US', 'CA'], date_reports=[], view=['brand', 'asin']) -> None:
    """Download weekly Search Query Performance per ASIN for each marketplace.
        Then inserts data to search_query_performance
    Args:
        playwright (Playwright)
        marketpalces (str): abbreviated countries to download from.
        date_reports (str|dt.date): weekly datesto download.
        view (str): select which viewing report to download.
    Return:"""
    logger.info("Starting Search Query Performance Analytics. . .")

    page, browser, context = await setup_playwright(storage_state='storage_sellercentral_amazon.json', headless=False, default_timeout=60000)

    logger.info("@SearchQueryPerformance Logging in")
    await login_amazon(page)


    # downloads for each marketplace, date_report, asin
    downloads = 0
    downloaded_filepaths = []
    asinView_maxDownloads = 100
    for country in marketplaces:
        if view == 'asin':
            active_asins = amazon.get_amazon_products(country=country)
            for date_report in date_reports:
                for asin in active_asins:
                    # checks if it exists on the db
                    query = """SELECT EXISTS(SELECT * FROM search_query_performance_asin_view WHERE asin = %s AND reporting_date = %s)"""
                    exists = postgresql.sql_to_dataframe('ppc', query, (asin, date_report))['exists'].item()
                    if exists:
                        logger.info("@SQP {} {} already exists \n\tSkipping...".format(asin, date_report))
                        continue
                    await download_sqp(page, country, date_report=date_report, view=view, asin=asin)
                    downloads += 1
                    if downloads == asinView_maxDownloads:
                        # download reports via `Downloads Manager`
                        logger.info("@SearchQueryPerformance Downloading reports")
                        downloaded_filepaths += await download_reports(page, n_view_rows=asinView_maxDownloads)
                        downloads = 0

        elif view == 'brand':
            for date_report in date_reports:
                await download_sqp(page, country, date_report=date_report, view=view)

    # download reports via `Downloads Manager`
    logger.info("@SearchQueryPerformance Downloading reports")
    downloaded_filepaths = await download_reports(page, n_view_rows=100)

    # inserts to amazon db
    table_name = 'search_query_performance_{}_view'.format(view)
    for downloaded_filepath in downloaded_filepaths:
        amazon.insert_sqp_reports(downloaded_filepath)

    # ---------------------
    # saves storage & closes browser.
    # await context.storage_state(path=storage)
    # await context.close()
    # await browser.close()


async def main():
    async with async_playwright() as playwright:
        last_week = dt.date.today() - dt.timedelta(weeks=1)
        date_report  = amazon.end_of_week_date(last_week)
        # task1 = asyncio.create_task(scrape_cerebro(playwright, ['CA']))
        # date_report = ['2022-09-10', '2022-09-17', '2022-09-24', '2022-10-01', '2022-10-08', '2022-10-15', '2022-10-22', '2022-09-10', '2023-01-07', '2023-01-14', '2023-01-21', '2023-01-28']
        import postgresql
        # cur = setup_cursor().connect('ppc')
        # cur.execute("SELECT DISTINCT(reporting_date) FROM search_query_performance_asin_view GROUP BY reporting_date HAVING COUNT(DISTINCT(asin)) < 10 ORDER BY reporting_date;")
        # date_reports = cur.fetchall()
        current_date = dt.date.today()
        start_date = dt.date(2023, 7, 1)
        sundays = []
        while start_date < current_date:
            sundays.append(str(start_date))
            start_date += dt.timedelta(days=7)
            # date_report = amazon.end_of_week_date(start_date)
            # date_report = str(date_report['reporting_date'])
        task2 = asyncio.create_task(scrape_sqp(playwright, marketplaces=['US'], date_reports=sundays, view='brand'))
        # to insert to db OR NOT????
        # cerebro_temps = await task1
        sqp_temps     = await task2


if __name__ == '__main__':
    # with sync_playwright() as playwright:
    #     asyncio.run(scrape_sqp(playwright))
    asyncio.run(main())