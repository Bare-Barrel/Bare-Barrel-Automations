from playwright.sync_api import Playwright, sync_playwright, expect
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import json
import postgresql
import logging
import logger_setup
import asyncio


logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

with open('config.json') as f:
    config = json.load(f)


async def setup_playwright(storage_state=None, headless=False, default_timeout=None):
    """
    Initialize a new playwright's chromium page
    
    Args:
    - storage_state (json): storage state of the last session.
    - headless (bool): with or without a browser.
    - default_timeout (int): default timeout in seconds before raising an error.
        
    Return:
    - Playwright.chromium.launch.browser.new_context.new_page()
    - browser
    - context
    """
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(headless=headless)
    context = await browser.new_context(storage_state=storage_state) # >>> storage_state=storage
    page = await context.new_page()

    if default_timeout:
        page.set_default_timeout(default_timeout)

    return page, browser, context


async def login_amazon(page):
    """
    Logs in to Amazon Sellercentral using the credentials stored in `config.json'

    Args:
    - page (playwright.page)
    
    Returns:
    - None
    """
    logger.info("Logging in to Amazon Sellercentral")

    await page.goto("https://sellercentral.amazon.com/signin?")
    await asyncio.sleep(5)

    account_button = page.get_by_role("button", name=f"{config['amazon_name']} {config['amazon_email']}")

    # login page requires only password
    if await account_button.count():
        await account_button.click()
        await page.get_by_label("Password").fill(config['amazon_password'])
        await page.get_by_role("button", name="Sign in").click()

    # login page requires email & password
    elif await page.is_visible('input[type="email"]'):
        await page.locator('input[type="email"]').fill(config['amazon_email'])
        await page.get_by_label("Password").fill(config['amazon_password'])
        await page.get_by_role("button", name="Sign in").click()
        await asyncio.sleep(10)

        # selects store account and country
        if await page.is_visible(f"button[name='{config['store_name']}']"):
            await page.get_by_role("button", name=f"{config['store_name']}").click()
            await page.get_by_role("button", name="United States").click()
            await page.get_by_role("button", name="Select Account").click()
            checkbox = page.locator('input[type="checkbox"][name="rememberMe"]')
            if await check.is_visible():
                await checkbox.click()


    await asyncio.sleep(10)
    return
