import asyncio
import json
import os
import random
import hashlib
from playwright.async_api import async_playwright, Page

# ==========================================
#   [1] TARGET & SELECTORS
# ==========================================
START_URLS = [
    "https://findahomeopath.org/search/postcode?postcode=LA"
]

NEXT_BTN_PATTERNS = [
    "div.e-loop__load_more a",
    "div#btnToLoadMorePost",
    "li.page-next a",
    "a.navlink:has-text('>')",
    "a.navlink",
    "text='>'",
]

COOKIE_BTN_PATTERNS = "text=/allow all cookies|accept|agree|consent|okay|got it/i"

# ==========================================
#   [2] UNIVERSAL CONFIGURATION
# ==========================================
IS_LOAD_MORE_SITE = False
RESET_SCROLL_ON_ITERATION = True
MAX_PAGES = 200
HEADLESS = True

# ==========================================
#   [3] TIMING & SPEED
# ==========================================
SCROLL_PAUSE_MIN = 0.5
SCROLL_PAUSE_MAX = 1.0
BOTTOM_WAIT_TIME = 2
PAGE_LOAD_WAIT = 4
PAGINATION_CHECK_RETRIES = 5

# ==========================================
#   [4] FOLDERS & FILES
# ==========================================
DIR_RAW = "Grabber_FindHomeo"
DIR_LINKS = "Links_FindHomeo"
FILE_LINKS = os.path.join(DIR_LINKS, "all_mind.csv")
JSON_FILENAME = "Grab_HTML_Data"


# ==========================================
#   [5] THE ENGINE
# ==========================================

class UniversalVacuum:
    def __init__(self):
        os.makedirs(DIR_RAW, exist_ok=True)
        os.makedirs(DIR_LINKS, exist_ok=True)

    @staticmethod
    async def smart_scroll_until_button(page: Page):
        print("  [🌊] Smart Scrolling (Looking for button)...")
        while True:
            found_button = False
            for selector in NEXT_BTN_PATTERNS:
                try:
                    if await page.is_visible(selector, timeout=100):
                        print(f"  [👀] Spotter: Found button '{selector}'! Stopping scroll.")
                        found_button = True
                        break
                except Exception as e:
                    print(f"Error at smart_scroll_until_button: {e}")
                    continue

            if found_button:
                break

            await page.evaluate("window.scrollBy({ top: 300, behavior: 'smooth' });")
            await asyncio.sleep(0.4)

            at_bottom = await page.evaluate("window.scrollY + window.innerHeight >= document.body.scrollHeight - 50")
            if at_bottom:
                print("  [End] Reached bottom of page.")
                break

        await asyncio.sleep(BOTTOM_WAIT_TIME)

    @staticmethod
    async def force_pagination(page: Page, old_hash: str):
        for selector in NEXT_BTN_PATTERNS:
            try:
                if await page.is_visible(selector, timeout=2000):
                    btn = page.locator(selector).first
                    print(f" Clicking: {selector}")

                    await btn.scroll_into_view_if_needed()
                    await asyncio.sleep(0.5)

                    await btn.click(timeout=5000, force=True)

                    print(" Waiting for new content...")
                    for _ in range(PAGINATION_CHECK_RETRIES):
                        await asyncio.sleep(PAGE_LOAD_WAIT)
                        new_html = await page.content()
                        new_hash = hashlib.md5(new_html.encode('utf-8')).hexdigest()
                        if new_hash != old_hash:
                            print("  [Success] Content changed.")
                            return True
                    print("Warning: Clicked, but content didn't change.")
                    return False
            except Exception as e:
                print(f"Error at force_pagination: {e}")
                continue
        return False

    async def run(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=HEADLESS)
            page = await browser.new_page()

            for url in START_URLS:
                print(f"\n[*] TARGET: {url}")
                page_num = 1
                try:
                    await page.goto(url, timeout=60000, wait_until="domcontentloaded")

                    while page_num <= MAX_PAGES:
                        print(f"\n--- Iteration {page_num} ---")

                        if RESET_SCROLL_ON_ITERATION:
                            await page.evaluate("window.scrollTo(0, 0)")
                            await asyncio.sleep(0.5)

                        await self.smart_scroll_until_button(page)

                        html_content = await page.content()
                        current_hash = hashlib.md5(html_content.encode('utf-8')).hexdigest()

                        safe_name = "".join([c if c.isalnum() else "_" for c in url])[:30]
                        filename = f"{DIR_RAW}/{JSON_FILENAME}_{safe_name}_p{page_num}.json"

                        with open(filename, 'w', encoding='utf-8') as f:
                            json.dump({"url": url, "page": page_num, "html": html_content}, f)

                        print(f" Saved Data for Iteration {page_num}")

                        success = await self.force_pagination(page, current_hash)
                        if not success:
                            print(" No more buttons or content. Stopping.")
                            break
                        page_num += 1

                except Exception as e:
                    print(f"Error: {e}")

            await browser.close()


if __name__ == "__main__":
    bot = UniversalVacuum()
    asyncio.run(bot.run())