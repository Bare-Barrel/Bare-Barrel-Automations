import pandas as pd
import numpy as np
import datetime as dt
import os
from postgresql import setup_cursor
import psycopg2
import logging
import logger_setup

logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

async def scrape_cerebro(playwright: Playwright, marketplaces=['US', 'CA']) -> list:
    logger.info("Scraping Cerebro. . .")
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
        await page.locator(".sc-dwkDbJ").click()  # Watchout for change in
        await page.get_by_role("option", name=marketplace_options[country]).click()
        competitors_list = pd.read_excel('Competitor ASINs.xlsx', sheet_name=country)
        for index, row in competitors_list.iterrows():
            # retrieves competitor asins
            competitors = competitors_list[competitors_list.Category == row.Category].iloc[:, 1:].values
            competitors = re.sub(r'\W+', ' ', str(competitors)).strip()
            logger.info(f"\tDownloading Cerebro {competitors}")
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


def insert_data(csv : str, **kwargs) -> None:
    """Cleans & insert downloaded cerebro csv file according to database structure
    **kwargs - additional columns"""
    metadata = "{} ({}) - {} - {} - {}".format(kwargs['platform'], kwargs['country'], kwargs['asin'], kwargs['category'], kwargs['date'])
    logger.info(f"\tINSERTING {metadata}")
    cur = setup_cursor().connect('ppc')
    table_name = f"cerebro_{kwargs['platform']}"
    # removes competitors' organic ranking
    data = pd.read_csv(csv)
    data = data.loc[:, :'Competitor Performance Score']
    # adds columns
    for col in kwargs.keys():
        data[col] = kwargs[col]
    logger.info(data.head(5))
    data['created'] = dt.datetime.now()
    data.replace('-', '', inplace=True)
    data.dropna(subset='Competitor Performance Score', inplace=True) # removes 0 competitor performance score
    # arranges column name following db column order
    cur.execute("""SELECT column_name FROM information_schema.columns 
                    WHERE table_name = %s
                    ORDER BY ordinal_position;""", (table_name, ))
    column_names = [row['column_name'] for row in cur.fetchall()]
    # adding null values on missing columns in other marketplaces
    missing_cols = [col for col in column_names if col not in data.columns]
    if missing_cols:
        data[missing_cols] = None
    data = data[column_names]
    # inserts to db
    file = 'cerebro_temp.csv'
    temp_csv = os.path.join(os.getcwd(), file)
    data.to_csv(temp_csv, index=False)
    try:
        logger.info(f"\tCopying {file} to {table_name}")
        cur.execute(f"""COPY {table_name} FROM '{temp_csv}' DELIMITER ',' CSV HEADER;""")
    except psycopg2.errors.UniqueViolation as error:
        logger.error(error)
    cur.close()
    return


if __name__ == '__main__':
    csv = os.path.join(os.getcwd(), 'US_AMAZON_cerebro_B08611LCC7_2023-01-12.csv')
    insert_data(csv, country='US', category='Canisters', asin='B08611LCC7',
                                        date=dt.datetime.now().date(), platform='amazon')