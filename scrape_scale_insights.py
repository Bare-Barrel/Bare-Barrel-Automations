from playwright.sync_api import Playwright, sync_playwright, expect
from playwright.async_api import async_playwright
import json
import pandas as pd
import datetime as dt
import time
import asyncio
import os
import re
import shutil
import postgresql
from bs4 import BeautifulSoup
from utility import get_day_of_week, reposition_columns
from playwright_setup import setup_playwright, login_scale_insights
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)


async def goto_search_term(page, date, asin):
    """
    Args:
    - date (str | dt.date) - YYYY-MM-DD
    """
    date = str(date)
    ads_search_term_url = f"https://portal.scaleinsights.com/Ads/SearchTerms?option=0&from={date}&to={date}&asinList={asin}"
    logger.info(f"Going to: {ads_search_term_url}")
    try:
        await page.goto(ads_search_term_url)
    except:
        logger.info('Page failed to load, reloading...')
        await page.reload()


async def expand_links(page):
    # Query all elements by role and name
    expand_links = await page.query_selector_all('a[aria-label="Expand"]')
    logger.info(f"\tExpanding {len(expand_links)} links")

    # Perform actions on each element (like clicking)
    for link in expand_links:
        try:
            await link.click()
        except Exception as error:
            logger.error(error)

async def dropdown_1000(page):
    """
    Clicks dropdown and choose option 1000
    """
    logger.info("\tSelecting 1000 per page")
    dropdowns = await page.query_selector_all('span[class="k-widget k-dropdown"]')

    for dropdown in dropdowns:
        await dropdown.click()  # open the dropdown

        for i in range(1, 7):
            await dropdown.press("ArrowDown")

        await dropdown.press("Enter")


async def scrape_rows(page):
    """
    Returns scraped data of the current page
    """
    logger.info("Scraping table. . .")
    # Get the HTML content of the page
    content = await page.content()

    # Use BeautifulSoup to parse the HTML
    soup = BeautifulSoup(content, 'html.parser')

    main_table = soup.find('tbody', {'role': 'rowgroup'})
    table = main_table.find('tbody', {'role': 'rowgroup'})

    # Extracting main headers
    top_header = main_table.find('thead', {'class': 'k-grid-header', 'role': 'rowgroup'})
    top_columns = [row.text.strip() for row in top_header.find_all('th', {'role': 'columnheader'})]
    top_columns = [col if col in ['Sponsored', 'Search Terms', 'Actions'] else f'Total {col}' for col in top_columns]

    data = pd.DataFrame()

    for row in table.find_all('tr', recursive=False):
        class_name = row.attrs.get('class', None)

        # Scraping top-level search term data
        if 'k-master-row' in class_name:
            top_data = [cell.text.strip() for cell in row.find_all('td', {'role': 'gridcell'}, recursive=False)]

        if 'k-detail-row' in class_name:
            campaign_table = row.find('table', {'role': 'grid'})
            header_cols = [row.text.strip() for row in campaign_table.find_all('th', {'role': 'columnheader'})]
            for campaign_row in campaign_table.find_all('tr', {'class': 'k-master-row'}):
                row_data = [cell.text.strip() for cell in campaign_row]
                df = pd.DataFrame([row_data + top_data], columns=header_cols + top_columns)
                data = pd.concat([data, df], ignore_index=True)

    return data


def clean_data(data):
    logger.info("\tCleaning data")
    # cleaning data
    text_cols = ['Campaign', 'Keyword', 'State', 'Match', 'Last Activity', 'Sponsored', 'Search Terms', 'Actions', 'AdGroup']
    numeric_cols = [col for col in data.columns if col not in text_cols]
    percentage_cols = ['Total CTR', 'CTR', 'Conversion', 'Total Conversion', 'ACOS', 'Total ACOS']

    for col in numeric_cols:
        data[col] = data[col].str.replace('$', '').str.replace('%', '').str.replace(',', '').str.replace('(', '').str.replace(')', '')
        data[col] = pd.to_numeric(data[col])
        if col in percentage_cols:
            data[col] = data[col] / 100

    # calculating campaign impressions
    data['Impressions'] = round(data['Cost'] / data['Total Spend'] * data['Total Impressions'])

    return data


async def scrape_search_terms(start_date, end_date):
    # Retrieving ORIG asins
    with postgresql.setup_cursor() as cur:
        cur.execute("""select distinct asin from listings_items.summaries where status @> ARRAY['DISCOVERABLE', 'BUYABLE']
                    and created_date < '2023-01-01';""")
        asins = [row['asin'] for row in cur.fetchall()]


    # Logins
    page, browser, context = await setup_playwright(headless=True)
    await login_scale_insights(page)

    
    current_date = start_date

    while current_date <= end_date:
        # Checks if file exists
        file_name = f'Search Terms {current_date}.csv'
        file_path = os.path.join(os.getcwd(), 'PPC Data', 'Scale Insights Downloads', 'Search Terms', file_name)
        if os.path.isfile(file_path):
            logger.info(f"{file_name} exists.\n\tSkipping. . .")
            current_date += dt.timedelta(days=1)
            continue

        combined_data = pd.DataFrame()
        for asin in asins:
            await goto_search_term(page, current_date, asin)
            await asyncio.sleep(2.5)

            # checks for `No records found.`
            if await page.locator('div[class="k-grid-norecords"]').count():
                logger.info("\nNo records found.\n")
                continue

            await expand_links(page)
            await dropdown_1000(page)
            await asyncio.sleep(2)
            await expand_links(page)
            await asyncio.sleep(5)
            await expand_links(page)    # double check for unexpanded links
            await asyncio.sleep(2)
            data = await scrape_rows(page)
            data = clean_data(data)
            # inserting columns
            data[['date', 'marketplace']] = [current_date, 'US']

            combined_data = pd.concat([data, combined_data], ignore_index=True)
            logger.info("Done combining.\n")

        # saving data
        logger.info(f"SAVING DATA\n\t{file_path}")
        combined_data.to_csv(file_path, index=False)

        current_date += dt.timedelta(days=1)
    return


async def main():
    # Create a date range
    start_date = '2022-12-01'
    end_date = '2023-06-01'
    date_range = pd.date_range(start_date, end_date)

    # Find unique months in the date range
    unique_months = date_range.to_period('M').unique()
    
    tasks = []
    for month in unique_months:
        start_of_month = month.to_timestamp(how='start').date()
        end_of_month = month.to_timestamp(how='end').date()
        logger.info(f"Creating task for {start_of_month} - {end_of_month}")
        task = asyncio.ensure_future(scrape_search_terms(start_of_month, end_of_month))
        tasks.append(task)

    await asyncio.gather(*tasks)


if __name__ == '__main__': 
    asyncio.run(main())