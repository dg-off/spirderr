import os
import json

BOT_NAME = "avvo_guides"

SPIDER_MODULES = ["scraper.spiders"]
NEWSPIDER_MODULE = "scraper.spiders"

ROBOTSTXT_OBEY = False  # can override via -s ROBOTSTXT_OBEY=1 for tests

# concurrency + delay, we also use playwright for delays 
CONCURRENT_REQUESTS = int(os.getenv("CONCURRENCY", "2"))
DOWNLOAD_DELAY = 1  

# using playwright instead of scrapy's built-in downloader
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

# Required reactor for asyncio / Playwright
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

PLAYWRIGHT_BROWSER_TYPE = "chromium"

# Headless toggle (set HEADLESS=0 for headed / visible browser in env)
_headless_env = os.getenv("HEADLESS", "1").lower() in ("1", "true", "yes")

# Common realistic desktop viewports – pick one once per run for stability.
_VIEWPORT_CHOICES = [
    {"width": 1366, "height": 824},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1600, "height": 900},
    {"width": 1680, "height": 1050},
]
import random as _rnd
_viewport = _rnd.choice(_VIEWPORT_CHOICES)

# A recent (static) real Chrome UA can be overridden by USER_AGENT env.
_DEFAULT_REAL_UA = (
    os.getenv(
        "USER_AGENT",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    )
)

PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": _headless_env,
    # A few low‑risk flags to reduce obvious automation fingerprints.
    "args": [
        "--disable-blink-features=AutomationControlled",
        "--no-default-browser-check",
        "--disable-infobars",
        "--disable-dev-shm-usage",
    ],
}

# Reusable named context so spider can set meta['playwright_context'] = 'default'
_STORAGE_STATE_FILE = "playwright_storage_state.json"
# Bootstrap an empty storage_state file if missing to avoid FileNotFoundError on first run.
if not os.path.exists(_STORAGE_STATE_FILE):
    try:
        with open(_STORAGE_STATE_FILE, "w", encoding="utf-8") as _f:
            json.dump({"cookies": [], "origins": []}, _f)
    except Exception:
        pass

PLAYWRIGHT_CONTEXTS = {
    "default": {
        "user_agent": _DEFAULT_REAL_UA,
        "viewport": _viewport,
        "locale": "en-US",
        "timezone_id": os.getenv("TZ", "America/New_York"),
        # Persist storage to keep cookies between spider restarts.
        "storage_state": _STORAGE_STATE_FILE,
    }
}

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    # Keep header UA aligned with Playwright context for consistency.
    "User-Agent": _DEFAULT_REAL_UA,
    "DNT": "1",
}

ITEM_PIPELINES = {"scraper.pipelines.JSONLinesPipeline": 300}

PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 30000  # ms

LOG_LEVEL = "INFO"
