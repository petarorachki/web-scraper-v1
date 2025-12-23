import json
import csv
import glob
import os
import re
from bs4 import BeautifulSoup, NavigableString, Tag
from urllib.parse import urljoin

# ==============================================================================
#   [1] CONFIGURATION AREA (CHANGE THIS FOR EVERY NEW SITE)
# ==============================================================================

INPUT_FOLDER = "Grabber_FindHomeo"
OUTPUT_FILE = "GoodCVS/Extractor_FindHomeodd.csv"

# The CSS selector for the ONE card/box that contains a single person's data
CONTAINER_SELECTOR = "div.row"

CSV_HEADERS = [
    "Name", "First_Name", "Last_Name",
    "Address", "Company",
    "Email", "Mobile", "Residental Tel", "Practice",
    "Website", "Title", "Specialism", "Details", "Image_URL",
    "instagram", "facebook", "twitter", "youtube", "linkedin"
]

PERSON_DETAILS = {
    "Name": ("span.memberTitle", "text"),

    # Title (Naturopath) is in the next Heading Widget
    "Title": ("div[data-id='fd37797'] h2", "text"),

    # Image
    "Image_URL": ("img", "src"),

    # Company ("Mr Vitamins")
    "Company": ("div[data-id='14cdc7c']", "text"),

    # Address ("394 Victoria Ave...")
    "Address": ("div[data-id='054bd27']", "text"),

    # Landline Phone
    "Residental Tel": ("div[data-id='3db4f19']", "text"),

    # Email
    "Email": ("div[data-id='f17d03e']", "text"),

    # Website (It's a link inside a Heading widget)
    "Website": ("", ""),

    "Details": ("div[data-id='5fcc990']", "text"),

    "Practice": ("", ""), "Specialism": ("", "")
}

LABEL_SEARCH = {
    "Residental Tel": "t:",
    "Mobile": "m:",
    "Email": "e:",
    "Website": "w:",
}

# ==============================================================================
#   [2] THE ENGINE (DO NOT TOUCH BELOW THIS LINE)
# ==============================================================================

SOCIAL_PLATFORMS = ["instagram", "facebook", "twitter", "youtube", "linkedin"]


class UniversalRefinery:
    def __init__(self):
        self.files = glob.glob(os.path.join(INPUT_FOLDER, "*.json"))
        self.data = []

    @staticmethod
    def clean(txt):
        if not txt: return ""
        # Remove leading punctuation like ": " or "- "
        txt = re.sub(r"^[:\s\-]+", "", txt.strip())
        # Collapse multiple spaces
        return " ".join(txt.split())

    @staticmethod
    def extract_by_label(soup_item, label_text):
        """
        Smart Extraction: Finds a label (like 'Phone:') and grabs the text
        immediately following it, stopping at the next <br> or <b> tag.
        This prevents mixing Phone and Email if they are in the same <p>.
        """
        # Find the tag containing the label text
        label_tag = soup_item.find(
            lambda tag: tag.name in ["strong", "b", "span", "em"] and label_text in tag.get_text())

        if not label_tag: return ""

        captured_text = []

        # Walk through the "next siblings" (the text/tags coming after the label)
        for sibling in label_tag.next_siblings:
            # STOP CONDITION: Hit a line break or the start of a new bold label
            if sibling.name == "br" or sibling.name == "strong" or sibling.name == "b":
                break

            # If it is just text, grab it
            if isinstance(sibling, NavigableString):
                text_part = sibling.strip()
                if text_part: captured_text.append(text_part)

            # If it is a link (like an email <a> tag), grab the text inside
            elif isinstance(sibling, Tag):
                captured_text.append(sibling.get_text(strip=True))

        final_val = " ".join(captured_text)

        # Cleanup specific to Address (removing internal field names if they got grabbed)
        if label_text == "Address:":
            for junk in ["City:", "State:", "Zip Code:", "Province:", "Postal Code:", "County:"]:
                final_val = final_val.replace(junk, "")

        return UniversalRefinery.clean(final_val)

    @staticmethod
    def extract_global_contacts(soup_item):
        """Fallback: Finds any emails/phones/links inside the container if specific selectors failed."""
        email, phone, website = "", "", ""
        social_links = {s: "" for s in SOCIAL_PLATFORMS}

        for a in soup_item.find_all("a", href=True):
            h = a['href'].lower()

            if "mailto:" in h:
                if not email: email = h.replace("mailto:", "").split("?")[0]
            elif "tel:" in h:
                if not phone: phone = h.replace("tel:", "")
            elif any(s in h for s in SOCIAL_PLATFORMS):
                for s in SOCIAL_PLATFORMS:
                    if s in h: social_links[s] = a['href']
            elif "http" in h or "https" in h:
                # Basic filter to avoid junk links
                if "w3.org" not in h and "google" not in h and "facebook" not in h:
                    if not website: website = a['href']

        return email, phone, website, social_links

    def run(self):
        print(f"[*] Processing {len(self.files)} files...")
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

        for f_path in self.files:
            try:
                with open(f_path, 'r', encoding='utf-8') as f:
                    packet = json.load(f)

                html = packet.get("html", "") or packet.get("full_html_content", "")
                base = packet.get("url", "")
                soup = BeautifulSoup(html, 'lxml')

                # Find all people cards
                items = soup.select(CONTAINER_SELECTOR)

                for item in items:
                    temp_row = {"Source_File": os.path.basename(f_path)}

                    # --- 1. RUN CSS SELECTORS (PERSON_DETAILS) ---
                    for field, (css, attr) in PERSON_DETAILS.items():
                        if not css: continue

                        el = item.select_one(css)
                        if el:
                            if attr == "text_only":
                                direct_text = "".join(el.find_all(string=True, recursive=False))
                                temp_row[field] = self.clean(direct_text)
                            elif attr == "text":
                                temp_row[field] = self.clean(el.get_text())
                            elif el.has_attr(attr):
                                val = el[attr]
                                if attr == "href": val = urljoin(base, val)

                                # Auto-handle background images
                                if attr == "style" and "background-image" in val:
                                    clean_match = re.search(r"url\(['\"]?(.*?)['\"]?\)", val)
                                    if clean_match: val = clean_match.group(1)

                                temp_row[field] = val
                        else:
                            temp_row[field] = ""
                    raw_name = temp_row.get("Name", "")
                    if not raw_name: continue

                    name_parts = raw_name.split(',', 1)
                    clean_name = name_parts[0].strip()
                    temp_row["Name"] = clean_name
                    if len(name_parts) > 1:
                        credential = name_parts[1].strip()
                        if not temp_row.get("Specialism"):
                            temp_row["Specialism"] = credential
                    parts = clean_name.split(" ", 1)
                    temp_row["First_Name"] = parts[0]
                    temp_row["Last_Name"] = parts[1] if len(parts) > 1 else ""

                    for field, label_text in LABEL_SEARCH.items():
                        val = self.extract_by_label(item, label_text)
                        if val: temp_row[field] = val

                    em, ph, web, soc = self.extract_global_contacts(item)
                    if not temp_row.get("Email"): temp_row["Email"] = em
                    if not temp_row.get("Mobile"): temp_row["Mobile"] = ph
                    if not temp_row.get("Website"): temp_row["Website"] = web

                    temp_row.update(soc)

                    final_row = {header: temp_row.get(header, "") for header in CSV_HEADERS}
                    self.data.append(final_row)

            except Exception as e:
                print(f"[!] Error in {f_path}: {e}")

        if self.data:
            with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
                writer.writeheader()
                writer.writerows(self.data)
            print(f"\n[SUCCESS] Extracted {len(self.data)} items to {OUTPUT_FILE}")
        else:
            print(f"[!] No data found. Check CONTAINER_SELECTOR.")

if __name__ == "__main__":
    eng = UniversalRefinery()
    eng.run()