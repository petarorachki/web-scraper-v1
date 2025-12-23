# ⚡ Async ETL Data Pipeline

## 📖 The Gist
This is a high-performance, asynchronous pipeline designed to harvest and clean data from complex directory websites.

**I designed this system with a decoupled architecture.**
Instead of scraping and parsing in a single fragile loop, I separated the **Network Layer** (Playwright) from the **Logic Layer** (BeautifulSoup). 

This ensures data safety: I scrape the raw data once and save it to disk. This allows me to iterate on the extraction logic (parsing) offline without needing to re-crawl the website if a selector error occurs.

The system is broken down into 4 modular steps:
1.  **Ingest:** Crawl the directory and dump raw HTML.
2.  **Enrich:** Visit every profile link in parallel (Async).
3.  **Refine:** Parse the raw HTML into structured CSVs.
4.  **Hunt:** (Optional) Crawl external websites to find hidden emails.

---

## ⚙️ Configuration (Required)

The engine is universal, but the CSS selectors are specific to the target. These variables must be mapped to the site being scraped.

### 1. `grabber.py` (The Crawler)
Controls main list navigation.
| Variable | Description |
| :--- | :--- |
| `START_URLS` | List of entry points (e.g., `["https://site.com/search"]`). |
| `NEXT_BTN_PATTERNS` | CSS selectors for the "Next" or "Load More" button. |
| `IS_LOAD_MORE_SITE` | **True** = Infinite Scroll behavior. **False** = Pagination (clicking pages). |
| `RESET_SCROLL_ON_ITERATION` | **False** = Keep scrolling down (use for Infinite Scroll). **True** = Reset to top (use for Pagination). |

### 2. `enricher.py` (The Detail Fetcher)
Controls profile discovery.
| Variable | Description |
| :--- | :--- |
| `PROFILE_LINK_SELECTOR` | The CSS selector for the specific link that leads to a user profile. |
| `INPUT_FOLDER` | Must match the output folder name from the Grabber. |

### 3. `extractor.py` (The Parser)
Controls the HTML-to-Data conversion.
| Variable | Description |
| :--- | :--- |
| `CONTAINER_SELECTOR` | The specific card or box wrapping a single person's data. |
| `PERSON_DETAILS` | The master map. Connects CSV headers to CSS selectors. |
| `LABEL_SEARCH` | Text keywords to hunt for if CSS fails (e.g., finding "Tel:" in a paragraph). |

---

## 🚀 The Architecture

### Phase 1: Ingestion (`grabber.py`)
This script handles the browser automation. It’s smart enough to detect if a "Click" actually loaded new content by comparing MD5 hashes of the page before and after the action.
* **Output:** Dumps raw HTML files locally.

### Phase 2: Enrichment (`enricher.py`)
This is where the speed comes in. I used **Asyncio Semaphores** to manage a "Fan-Out" architecture. It opens up to 10 concurrent tabs to visit profile links simultaneously.
* **Why?** Doing this synchronously takes hours. Doing this asynchronously takes minutes.

### Phase 3: Extraction (`extractor.py`)
This runs offline. It reads the raw files and applies the `PERSON_DETAILS` logic.
* **Smart Feature:** Includes a "Label Matcher" fallback. If a phone number isn't in a nice `<span>`, it looks for the text "Phone:" and grabs whatever is next to it.

### Phase 4: Verification (`hunter.py`)
The "Closer." It takes the website URLs found in the data and visits them to find contact info that wasn't on the directory.
* **Logic:** Scans "Contact" and "About" pages using Regex to find obfuscated emails (e.g., `jane [at] domain`).

---

## 🛠️ Setup & Run

1.  **Dependencies**
    ```bash
    pip install playwright aiohttp pandas beautifulsoup4 lxml requests
    playwright install chromium
    ```

2.  **Execution Order**
    ```bash
    python grabber.py   # 1. Get the lists
    python enricher.py  # 2. Get the details
    python extractor.py # 3. Make the CSV
    python hunter.py    # 4. Deep search (Optional)
    ```
