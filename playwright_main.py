from playwright.sync_api import Playwright, sync_playwright, expect
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import json
import pandas as pd
import datetime as dt
import re
import cerebro
import amazon
import time
import asyncio

with open('config.json') as f:
    config = json.load(f)

async def scrape_cerebro(playwright: Playwright, marketplaces=['US', 'CA'], asins=[]) -> list:
    print("Scraping Cerebro. . .")
    # playwright = sync_playwright().start() # PLS COMMENT THIS! FOR TESTING PURPOSES ONLY!
    storage = 'storage_helium10.json'
    browser = await playwright.chromium.launch(headless=False, slow_mo=50)
    context = await browser.new_context(storage_state=storage)
    page = await context.new_page()
    page.set_default_timeout(60000)
    # logs in
    await page.goto(f'https://members.helium10.com/')
    if await page.is_visible('input#loginform-email'):
        await page.fill('input#loginform-email', config['helium_email'])
        await page.fill('input#loginform-password', config['helium_password'])
        await page.click('button[type=submit]')
    # goes to cerebro main account
    await asyncio.sleep(5)
    await page.goto(f'https://members.helium10.com/cerebro?accountId={config["helium_id"]}')

    # download cerebro for each product category in each marketplaces
    for country in marketplaces:
        marketplace_options = {'US': 'United States www.amazon.com',
                               'CA': 'Canada www.amazon.ca'}
        await page.click('img[src*=Flag]')  # div that contains image of country flag selected
        await page.get_by_role("option", name=marketplace_options[country]).click()
        competitors_list = pd.read_excel('Competitor ASINs.xlsx', sheet_name=country).fillna('')
        if asins:
            competitors_list = competitors_list[competitors_list.ASIN.str.contains('|'.join(asins))]
        for index, row in competitors_list.iterrows():
            # retrieves competitor asins
            competitors = competitors_list[competitors_list.Category == row.Category].iloc[:, 1:].values
            competitors = re.sub(r'\W+', ' ', str(competitors)).strip()
            print(competitors)
            print(f"\tDownloading Cerebro {competitors}")
            # deletes asins
            input_box = page.locator('input[placeholder="Enter up to 10 product ASINs"]')
            await input_box.click()
            for _ in range(50):
                await page.keyboard.press('Backspace')
            # fills competitors asins
            await input_box.fill(competitors)
            get_keywords_button = page.locator('button[data-testid="getkeywords"]')
            box = await get_keywords_button.bounding_box() # clicks outside to bypass greyed out button
            await page.mouse.click(box["x"] + box["width"] / 2, box["y"] + box["height"])
            await get_keywords_button.click()
            # checks for run new search dialog
            await asyncio.sleep(5)
            if await page.is_visible('button[data-testid="runnewsearch"]'):
                await page.click('button[data-testid="runnewsearch"]')
            # downloads data
            await page.click('button[data-testid="exportdata"]')
            async with page.expect_download(timeout=180000) as download_info:   # saves in tmp folder
                await page.click('div[data-testid="csv"]')
            download = await download_info.value
            # wait for download to complete
            path = await download.path()
            # read to tmp csv, then update to db
            cerebro.insert_data(path, country=country, category=row.Category, asin=row.ASIN,
                                        date=dt.datetime.now().date(), platform='amazon')
    # ---------------------
    # save storage & closes browser
    await context.storage_state(path=storage)
    await context.close()
    await browser.close()


async def scrape_sqp(playwright: Playwright, marketplaces=['US', 'CA'], date_reports=[], asins=[]) -> None:
    """Download weekly Search Query Performance per ASIN for each marketplace.
        Then inserts data to search_query_performance
        Download Manager has only maximum of 100 items to download"""
    # playwright = sync_playwright().start() # PLS COMMENT THIS! FOR TESTING PURPOSES ONLY!
    print("Starting Search Query Performance Analytics. . .")
    storage = 'storage_sellercentral_amazon.json'
    browser = await playwright.chromium.launch(headless=False)
    context = await browser.new_context(storage_state=storage) # >>> storage_state=storage
    page = await context.new_page()
    page.set_default_timeout(60000)
    # logs in
    print("@SearchQueryPerformance Logging in")
    await page.goto("https://sellercentral.amazon.com/signin?")
    await asyncio.sleep(5)
    account_button = page.get_by_role("button", name=f"Calvin Del Rosario {config['amazon_email']}")
    if await account_button.count():
        await account_button.click()
        await page.get_by_label("Password").fill(config['amazon_password'])
        await page.get_by_role("button", name="Sign in").click()
    elif await page.is_visible('input#signInSubmit'):
        await page.get_by_label("Email or mobile phone number").fill(config['amazon_email'])
        await page.get_by_label("Password").fill(config['amazon_password'])
        await page.get_by_role("button", name="Sign in").click()
        await asyncio.sleep(10)
        if await page.is_visible(f"button[name='{config['store_name']}']"):
            await page.get_by_role("button", name=f"{config['store_name']}").click()
            await page.get_by_role("button", name="United States").click()
            await page.get_by_role("button", name="Select Account").click()
    await asyncio.sleep(10)

    # generates downloads for each active product
    async def download_sqp(country, dates=[], asins=asins):
        "Optional: Input list of dates to download YYYY-MM-DD"
        sqp_url = "https://sellercentral.amazon.com/brand-analytics/dashboard/query-performance?view-id=query-performance-asin-view&asin={}&reporting-range=weekly&weekly-week={}"
        if asins == []:
            asins = amazon.get_amazon_products(country=country) # retrieves active asins

        for asin in asins:
            for date_report in date_reports:
                print(date_report)
                print(f"@SearchQueryPerformance Querying {asin} ({country}) {date_report}")
                await page.goto(sqp_url.format(asin, date_report))  # refreshes page as soon as it changes marketplace
                await page.click('[id*="katal-id-"]')  # *= starts with e.g. id=katal-id-0
                await page.click(f'[value="{country}" i][role="option"]') # deletes inputs after changing marketplace
                await page.click('[id="asin"]')
                await page.get_by_text(asin).click()
                await page.locator('kat-dropdown[id="reporting-range"]').last.click()
                await page.locator('kat-option[value="weekly" i]').last.click()
                await page.click('[id="weekly-week" i]')
                await page.click(f'kat-option[value="{date_report}"]')
                await page.get_by_role("button", name="Apply").click()
                await page.get_by_role("button", name="Generate Download").click()
                await asyncio.sleep(5)

                try: 
                    print("@SQP Generating Download")
                    async with page.expect_popup() as page2_info:
                        await page.locator("#downloadModalGenerateDownloadButton").get_by_role("button", name="Generate Download").click()
                        await asyncio.sleep(5)

                        # checks for errors 'Try again'
                        tries = 0
                        while await page.locator('#downloadModalGenerateDownloadButton').is_visible(timeout=300) and tries <= 30:
                            print("\t#ERROR: Try again")
                            await page.locator("#downloadModalGenerateDownloadButton").click()
                            await asyncio.sleep(3)
                        await asyncio.sleep(5)

                        # closes download manager page every popup            
                        page2 = await page2_info.value 
                        await page2.close()
                        print('\t@SQP Download success!')
                except:
                    print("@SQP Failed to download!")
                    pass
        return

    # downloads for each marketplace
    for country in marketplaces:
        await download_sqp(country)

    # download reports
    print("@SearchQueryPerformance Downloading reports")
    # waits for progress to be completed
    await page.goto('https://sellercentral.amazon.com/brand-analytics/download-manager')
    await asyncio.sleep(30)
    while await page.is_visible('text=In Progress'):
        print("Download is still In Progress. . .\n\tRefreshing the page")
        await page.reload()
        await asyncio.sleep(15)
    # selects view 100 rows
    await page.click('.select-header')
    await page.click('text=View 100 rows')
    # counts pages
    page_count = await page.locator('ul[class="pages"]').locator('li').count()
    # downloads each report per page
    for page_no in range(1, page_count+1):
        print(f"@SQP Download Manager page {page_no}. . .")
        download_buttons = await page.get_by_role("row").get_by_text("Download").all()
        print(f"\tNumber of download buttons: {len(download_buttons)}")
        for download_button in download_buttons:
            try:
                await download_button.click()
                async with page.expect_download() as download_info:
                    async with page.expect_popup() as page2_info:
                        page2 = await page2_info.value    # download popup closes immediately
                download = await download_info.value
                path = await download.path() # random guid in a temp folder
                filename = download.suggested_filename # US_Search_Query_Performance_ASIN_View_Week_2022_12_31.csv
                country = filename.split('_')[0]
                print(f"filename: {filename}")
                # inserts to amazon db
                amazon.insert_sqp_reports(path, country)
            except Exception as e:
                print(e)
    next_page = page.locator('span[part="pagination-nav-right"][tabindex="0"]')
    if await next_page.is_visible():
        await next_page.click()
    # ---------------------
    # saves storage & closes browser.
    await context.storage_state(path=storage)
    await context.close()
    await browser.close()


async def main():
    async with async_playwright() as playwright:
        last_week = dt.date.today() - dt.timedelta(weeks=1)
        date_report  = amazon.end_of_week_date(last_week)
        # task1 = asyncio.create_task(scrape_cerebro(playwright, ['US', 'CA'], asins=['B0BPK1R7MK', 'B0B4T3MF8P']))
        # cerebro_temps = await task1
        # Define start and end dates
        start_date = dt.date(2023, 2, 5)
        end_date = dt.date(2023, 2, 26)
        # Generate a list of all dates between start and end dates
        all_dates = [start_date + dt.timedelta(days=x) for x in range((end_date - start_date).days + 1)]
        # Filter out only the Saturdays
        date_reports = [date for date in all_dates if date.weekday() == 5]
        for date_report in date_reports:
            date_report = [date_report]
            task2 = asyncio.create_task(scrape_sqp(playwright, marketplaces=['US'], date_reports=date_report))
            sqp_temps     = await task2
        # to insert to db OR NOT????


if __name__ == '__main__':
    # with sync_playwright() as playwright:
    #     asyncio.run(scrape_sqp(playwright))
    asyncio.run(main())