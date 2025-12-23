import pandas as pd
import asyncio
import re
import aiohttp
import os
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# ==============================================================================
# [1] PATHS & DATA CONFIGURATION
# ==============================================================================
INPUT_CSV = "GoodCVS/GoodCVS.cvs"
OUTPUT_CSV = "FinalCVS/FinalCVS.cvs"

CSV_HEADERS = [
    "Name", "First_Name", "Last_Name",
    "Address", "Company",
    "Email", "Mobile", "Residental Tel", "Practice",
    "Website", "Title", "Specialism", "Details", "Image_URL",
    "instagram", "facebook", "twitter", "youtube", "linkedin"
]

# ==============================================================================
# [2] HUNTING & REGEX CONFIGURATION
# ==============================================================================
EMAIL_REGEX = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}'
PHONE_REGEX = r'(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'

JUNK_EMAIL_PATTERNS = ['domain.com', 'example.com', 'email.com', 'yourdomain.com', 'website.com', 'sentry', 'wixpress', 'jpg', 'png', 'gif']
ADDRESS_LABELS = ['Location', 'Address', 'Our Office', 'Find Us', 'Visit Us', 'Contact Details', 'Postal']
ADDRESS_INDICATORS = ['Street', 'St.', 'Road', 'Rd', 'Ave', 'Dr', 'Lane', 'Suite', 'Floor', 'Box', 'P.O.']
POSTAL_CODE_PATTERNS = [r'\b\d{5}(?:[-\s]\d{4})?\b', r'\b\d{4}\b', r'\b[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}\b']

DEEP_HUNT_KEYWORDS = [
    'contact', 'about', 'team', 'staff', 'meet', 'provider', 'location',
    'clinic', 'info', 'touch', 'support', 'legal', 'privacy', 'impressum',
    'connect', 'write', 'hello', 'directory'
]

SOCIAL_PLATFORMS = ["instagram.com", "facebook.com", "twitter.com", "x.com", "youtube.com", "linkedin.com"]
SKIP_DOMAIN_PATTERNS = ["googleusercontent", "maps.google", "apple.com", "nan", "wix.com", "squarespace.com"]

# ==============================================================================
# [3] NETWORK & SPEED CONFIGURATION
# ==============================================================================
MAX_CONCURRENT_CONNECTIONS = 20
REQUEST_TIMEOUT = 15
MAX_SUBPAGES_TO_VISIT = 20
USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'


# ==============================================================================
# THE ENGINE
# ==============================================================================

class WebsiteHunter:

    def __init__(self):
        os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
        self.results = []

    @staticmethod
    async def fetch(session: aiohttp.ClientSession, url: str) -> str:
        if any(x in url for x in SKIP_DOMAIN_PATTERNS): return ""
        if not url.startswith("http"): return ""

        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT) as response:
                if response.status == 200:
                    return await response.text()
                return ""
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
            return ""
        except Exception as e:
            print(f"Error at fetch: {e}")
            return ""

    @staticmethod
    def decode_obfuscated_emails(text):
        text = text.replace(" [at] ", "@").replace(" (at) ", "@").replace(" at ", "@")
        text = text.replace(" [dot] ", ".").replace(" (dot) ", ".").replace(" dot ", ".")
        return text

    def extract_info(self, html):
        try:
            clean_html = self.decode_obfuscated_emails(html)
            soup = BeautifulSoup(clean_html, 'lxml')

            text_content = " ".join(soup.stripped_strings)

            raw_emails = set(re.findall(EMAIL_REGEX, text_content))
            for a in soup.find_all('a', href=True):
                if "mailto:" in a['href']:
                    raw_emails.add(a['href'].split('mailto:')[-1].split('?')[0])

            emails = set()
            for e in raw_emails:
                e_lower = e.lower()
                prefix = e_lower.split('@')[0]
                if any(junk in e_lower for junk in JUNK_EMAIL_PATTERNS): continue
                if len(prefix) > 25 and re.match(r'^[a-f0-9]+$', prefix): continue
                if e_lower.endswith(('.png', '.jpg', '.jpeg', '.js', '.css')): continue
                emails.add(e_lower)

            addresses = set()
            for s in soup(['script', 'style', 'nav', 'footer']):
                s.extract()

            for label in ADDRESS_LABELS:
                element = soup.find(string=re.compile(rf'{label}', re.I))
                if element:
                    parent = element.find_parent()
                    if parent:
                        candidate = " ".join(parent.stripped_strings)[:150]
                        if any(ind in candidate for ind in ADDRESS_INDICATORS):
                            addresses.add(candidate)

            if not addresses:
                for indicator in ADDRESS_INDICATORS:
                    match = re.search(rf'\d{{1,5}}\s[^<>\n]{{5,50}}{indicator}[^<>\n]{{0,50}}', text_content, re.I)
                    if match:
                        addresses.add(match.group().strip())

            phones = set(re.findall(PHONE_REGEX, text_content))
            socials = {p.split('.')[0]: "" for p in SOCIAL_PLATFORMS}
            for a in soup.find_all('a', href=True):
                href = a['href']
                for platform in SOCIAL_PLATFORMS:
                    if platform in href and not socials[platform.split('.')[0]]:
                        socials[platform.split('.')[0]] = href

            return emails, phones, socials, addresses

        except Exception as e:
            print(f"Error at extract_info: {e}")
            return set(), set(), {}, set()

    async def process_doctor(self, session, row):
        base_url = str(row.get('Website', ''))

        if not base_url or len(base_url) < 5 or "http" not in base_url:
            return row
        if any(x in base_url for x in SKIP_DOMAIN_PATTERNS):
            return row

        print(f"[*] HUNTING: {base_url}")

        html = await self.fetch(session, base_url)
        if not html: return row

        emails, phones, socials, addresses = self.extract_info(html)

        soup = BeautifulSoup(html, 'lxml')
        all_links = set()

        for a in soup.find_all('a', href=True):
            full_link = urljoin(base_url, a['href'])
            if urlparse(full_link).netloc == urlparse(base_url).netloc:
                all_links.add(full_link)

        priority_links = []
        backup_links = []

        for link in all_links:
            if any(k in link.lower() for k in DEEP_HUNT_KEYWORDS):
                priority_links.append(link)
            else:
                backup_links.append(link)

        targets = (priority_links + backup_links)[:MAX_SUBPAGES_TO_VISIT]

        if targets:
            sub_tasks = [self.fetch(session, link) for link in targets]
            sub_pages_html = await asyncio.gather(*sub_tasks)

            for sub_html in sub_pages_html:
                if sub_html:
                    e, p, s, a = self.extract_info(sub_html)
                    emails.update(e)
                    phones.update(p)
                    addresses.update(a)
                    for k, v in s.items():
                        if v and not socials[k]: socials[k] = v

        if not row.get('Address') or pd.isna(row['Address']):
            row['Address'] = " | ".join(list(addresses)[:1])

        existing_email = str(row.get('Email', ''))
        if pd.isna(existing_email) or existing_email == "nan": existing_email = ""

        new_emails = list(emails)
        if existing_email: new_emails.append(existing_email)

        final_emails = list(set([e for e in new_emails if e]))
        row['Email'] = ", ".join(final_emails)

        if not row.get('Mobile') or pd.isna(row['Mobile']):
            row['Mobile'] = ", ".join(list(phones)[:2])

        for platform, link in socials.items():
            if platform in row and (not row[platform] or pd.isna(row[platform])):
                row[platform] = link

        return row

    async def run(self):
        try:
            df = pd.read_csv(INPUT_CSV)
        except FileNotFoundError:
            print(f"[!] Error: {INPUT_CSV} not found.")
            return

        for col in CSV_HEADERS:
            if col not in df.columns: df[col] = ""
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_CONNECTIONS, ttl_dns_cache=300, ssl=False)

        async with aiohttp.ClientSession(headers={'User-Agent': USER_AGENT}, connector=connector) as session:
            tasks = [self.process_doctor(session, row.to_dict()) for _, row in df.iterrows()]
            self.results = await asyncio.gather(*tasks)
        final_df = pd.DataFrame(self.results)

        final_df = final_df.reindex(columns=CSV_HEADERS)
        final_df.fillna("", inplace=True)

        final_df.to_csv(OUTPUT_CSV, index=False)
        print(f"[SUCCESS] Saved deep-enriched data to {OUTPUT_CSV}")


if __name__ == "__main__":
    print("Initializing The Deep Hunter...")
    hunter = WebsiteHunter()
    asyncio.run(hunter.run())
