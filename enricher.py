import asyncio
import json
import os
import hashlib
import glob
import random
from urllib.parse import urljoin
from playwright.async_api import async_playwright, Page

INPUT_FOLDER = "Grabber_Mind"
DIR_DEEP_DATA = "Enricher_Mind"
PROFILE_LINK_SELECTOR = "a.row"
JSON_FILENAME_PREFIX = "Enrich_Profile_Data"

HEADLESS = True  # Set to True for maximum speed
SCROLL_PROFILE = True

# Speed Settings
CONCURRENT_TABS = 10
MIN_DELAY = 0.5
MAX_DELAY = 1.2


class ProfileEnricher:
    def __init__(self):
        os.makedirs(DIR_DEEP_DATA, exist_ok=True)
        self.semaphore = None
    @staticmethod
    async def smooth_scroll(page: Page):
        for _ in range(2):
            await page.mouse.wheel(0, 600)
            await asyncio.sleep(0.2)
    @staticmethod
    def get_links_from_json():
        links = set()
        files = glob.glob(os.path.join(INPUT_FOLDER, "*.json"))
        from bs4 import BeautifulSoup

        for f_path in files:
            try:
                with open(f_path, 'r', encoding='utf-8') as f:
                    packet = json.load(f)
                    soup = BeautifulSoup(packet.get("html", ""), 'lxml')
                    elements = soup.select(PROFILE_LINK_SELECTOR)
                    for el in elements:
                        href = el.get("href")
                        if href:
                            full_url = urljoin(packet.get("url"), href)
                            links.add(full_url)
            except Exception as e:
                print(f"[!] Error on {f_path}: {e}")
                continue
        return links

    async def process_profile(self, context, url, index, total):
        async with self.semaphore:
            page = await context.new_page()
            try:
                print(f"[{index}/{total}] Visiting: {url}")
                await page.goto(url, timeout=60000, wait_until="domcontentloaded")

                if SCROLL_PROFILE:
                    await self.smooth_scroll(page)

                full_html = await page.content()
                safe_hash = hashlib.md5(url.encode()).hexdigest()
                filename = f"{DIR_DEEP_DATA}/{JSON_FILENAME_PREFIX}_{safe_hash}.json"

                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump({"url": url, "html": full_html}, f)

                await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

            except Exception as e:
                print(f" [!] Error on {url}: {e}")
            finally:
                await page.close()

    async def run(self):
        urls_to_visit = list(self.get_links_from_json())
        print(f"[*] Found {len(urls_to_visit)} unique profiles. Enriching with {CONCURRENT_TABS} concurrent tabs...")

        self.semaphore = asyncio.Semaphore(CONCURRENT_TABS)

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=HEADLESS)
            context = await browser.new_context()

            tasks = []
            for i, url in enumerate(urls_to_visit):
                task = asyncio.create_task(self.process_profile(context, url, i + 1, len(urls_to_visit)))
                tasks.append(task)

            await asyncio.gather(*tasks)
            await browser.close()


if __name__ == "__main__":
    enricher = ProfileEnricher()
    asyncio.run(enricher.run())