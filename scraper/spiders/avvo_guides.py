"""Primary (new) Avvo guides spider (basic rotation version).

Implements the foundational crawl flow requested:
1. Visit the all-topics page and collect topic (name, url).
2. Randomize topic order (rotation queue).
3. For each topic in round–robin fashion:
   - Request its current page (tracked per topic, starting at 1 or resuming from DB last_page_number+1).
   - Parse all guide URLs on that listing page; schedule each guide detail request.
   - After finishing scheduling guides for that page, increment the page counter for that topic.
   - Re-queue the topic at the end of the rotation (unless exhausted / no new guides found on that page).
4. Continue rotating until every topic yields an empty page (no new guides) or configured limits reached.
"""

from __future__ import annotations

import random
import os
import glob
import time
import email.utils
import re
import statistics
from collections import deque
import json
import asyncio
import heapq
from dataclasses import dataclass, field
from typing import Deque, Dict, Iterable, List, Optional, Set
from urllib.parse import urljoin, urlparse, urlunparse

import scrapy
from scrapy import Request, signals
from bs4 import BeautifulSoup

from scraper.parse_helpers import parse_guide, _extract_guide_urls
from scraper.db import (
    init_db,
    upsert_topic,
    mark_topic_visit,
    update_topic_progress,
    get_topic_progress,
    insert_or_update_guide,
    record_topic_guides,
    set_topic_total_pages,
    get_topic_total_pages,
)


ALL_TOPICS_URL = "https://www.avvo.com/free-legal-advice/all-topics"


@dataclass
class TopicState:
    slug: str  # Unique topic identifier (slug)
    url: str  # Topic URL
    next_page: int = 1  # Next page number to fetch for this topic
    exhausted: bool = False  # True if all pages for this topic are done
    pages_seen: int = 0  # Number of listing pages crawled for this topic
    guides_seen: int = 0  # Number of guide detail pages crawled for this topic
    session_guides: Set[str] = field(default_factory=set)  # All guide slugs/URLs seen in this session
    inflight_guides: int = 0  # Number of guide detail requests currently in progress
    waiting_requeue: bool = False  # True if waiting to requeue next page until all guides finish
    consecutive_empty: int = 0  # Number of consecutive empty pages seen (for exhaustion logic)
    total_pages_detected: Optional[int] = None  # Total number of pages detected for this topic (if known)
    current_page_guides: List[str] = field(default_factory=list)  # Guide slugs/URLs found on the current page
    expected_current_page_guides: int = 0  # Number of guide requests scheduled for the current page


class AvvoGuidesSpider(scrapy.Spider):
    name = "avvo_guides"
    # Accept HTTP 429 responses for custom rate limit handling (applies to all request types)
    handle_httpstatus_list = [429]

    custom_settings = {
        # Conservative concurrency for stability (can be increased if needed)
        "CONCURRENT_REQUESTS": 4,
        # Try HTML parse first; use Playwright only as fallback for incomplete/JS-heavy pages
        # Add a small download delay to reduce server load
        "DOWNLOAD_DELAY": 0.5,
        # Remove 429 from Scrapy's default retry codes so our custom backoff logic is used
        # (Scrapy's RETRY_HTTP_CODES includes 429 by default)
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 522, 524, 408],
        # Output is handled by the pipeline (JSONL)
    }

    def __init__(
        self,
        all_topics_url: str = ALL_TOPICS_URL,  # Entrypoint for topic discovery
        max_topics: int = 0,                   # Max topics to crawl (0 = unlimited)
        per_topic_pages: int = 0,              # Max pages per topic (0 = unlimited)
        max_guides_per_page: int = 0,          # Max guide detail requests per topic page (0 = unlimited)
        topic_parallelism: int = 2,            # Max topics with active page fetches at once
        shuffle_seed: Optional[int] = None,    # Shuffle seed for reproducibility (None = random)
        skip_existing: int = 1,                # Skip guides already in backups/data (1 = skip, 0 = always fetch)
        seed_file: str = "config/topics_seed.json", # Fallback topic list file
        use_seed_fallback: int = 1,            # Use seed file if discovered topics < min_topics_expected
        min_topics_expected: int = 500,        # Minimum topics expected before fallback triggers
        scroll_discovery: int = 1,             # Use Playwright scroll for lazy-loaded topics (1 = enabled)
        humanize: int = 0,                     # Enable human-like delays/mouse moves (1 = enabled)
        jitter_page_min: float = 0.0,          # Min random delay between topic pages (seconds)
        jitter_page_max: float = 0.0,          # Max random delay between topic pages (seconds)
        guide_think_min: float = 0.0,          # Min random delay before guide detail fetch (seconds)
        guide_think_max: float = 0.0,          # Max random delay before guide detail fetch (seconds)
        schedule_guide_jitter_min: float = 0.0,# Min jitter when scheduling guide requests (seconds)
        schedule_guide_jitter_max: float = 0.0,# Max jitter when scheduling guide requests (seconds)
        mouse_moves_min: int = 1,              # Min mouse moves per page (if humanize enabled)
        mouse_moves_max: int = 3,              # Max mouse moves per page (if humanize enabled)
        mouse_scribble_steps_min: int = 4,     # Min steps per mouse scribble
        mouse_scribble_steps_max: int = 9,     # Max steps per mouse scribble
        scroll_jitter: int = 1,                # Randomize scroll depth/rounds (1 = enabled)
        *args,
        **kwargs,
    ):
        """Initialize AvvoGuidesSpider instance and all internal state.

        Parameters are documented inline in the __init__ signature.
        """
        super().__init__(*args, **kwargs)

        # --- Core crawl configuration ---
        self.all_topics_url = all_topics_url
        self.max_topics = int(max_topics)
        self.per_topic_pages = int(per_topic_pages)
        self.max_guides_per_page = int(max_guides_per_page)
        self.topic_parallelism = int(topic_parallelism) if int(topic_parallelism) > 0 else 1
        self.shuffle_seed = shuffle_seed
        self.skip_existing = int(skip_existing)

        # --- Topic discovery and fallback ---
        self.seed_file = seed_file
        self.use_seed_fallback = int(use_seed_fallback)
        self.min_topics_expected = int(min_topics_expected)
        self.scroll_discovery = int(scroll_discovery)

        # --- Humanization and pacing ---
        self.humanize = int(humanize)
        self.jitter_page_min = float(jitter_page_min)
        self.jitter_page_max = float(jitter_page_max)
        self.guide_think_min = float(guide_think_min)
        self.guide_think_max = float(guide_think_max)
        self.schedule_guide_jitter_min = float(schedule_guide_jitter_min)
        self.schedule_guide_jitter_max = float(schedule_guide_jitter_max)
        self.mouse_moves_min = int(mouse_moves_min)
        self.mouse_moves_max = int(mouse_moves_max)
        self.mouse_scribble_steps_min = int(mouse_scribble_steps_min)
        self.mouse_scribble_steps_max = int(mouse_scribble_steps_max)
        self.scroll_jitter = int(scroll_jitter)

        # --- Rotation and topic state ---
        self._topics = {}  # slug -> TopicState
        self._rotation = deque()  # topic slug rotation order
        self._discovery_complete = False
        self._active_topics: Set[str] = set()  # topics with in-flight guide requests

        # --- Progress and timing ---
        self._started_ts = time.time()
        self._stat_pages = 0
        self._stat_guides = 0
        self._summary_interval = 60  # seconds between progress summaries
        self._next_summary = self._started_ts + self._summary_interval

        # --- Adaptive throttling and 429 handling ---
        self._recent_429: List[float] = []  # timestamps of recent 429s
        self._429_window = 300  # seconds to track 429s
        self._429_threshold = 5  # 429s in window to trigger storm
        self._throttle_until = 0.0  # global cooloff end timestamp
        self._storm_active = False
        self._storm_extensions = 0
        self._storm_max_extensions = 6  # max storm window extensions
        self._cooloff_min = 45  # min storm cooloff (seconds)
        self._cooloff_max = 90  # max storm cooloff (seconds)
        self._max_429_delay = 300  # max per-request retry delay (seconds)
        self._storm_retry_level = 0  # global storm escalation counter

        # --- Delayed request queue for backoff/cooloff ---
        self._delayed_seq = 0
        self._delayed: List[tuple[float, int, Request]] = []  # (ready_ts, seq, Request)
        self._delayed_release_batch = 3  # release N delayed requests per idle tick

        # --- Metrics and stats ---
        self._metric = {
            "pages": 0,
            "guides": 0,
            "topics_active": 0,
            "topics_total": 0,
            "429_total": 0,
            "429_storms": 0,
            "video_skipped": 0,
            "playwright_fallbacks": 0,
            "existing_skipped": 0,
        }
        self._existing_guides: Set[str] = set()  # canonical URLs of already-seen guides

        # --- Rate limiting and pacing ---
        self._guide_bucket_interval = 1.0  # seconds between guide requests
        self._global_bucket_interval = 0.8  # seconds between any requests
        self._max_global_bucket_interval = 15.0  # max global interval (seconds)
        self._min_global_bucket_interval = 0.3   # min global interval (seconds)
        self._last_global_req_ts = 0.0
        self._guides_inflight_global = 0
        self._max_concurrent_guides = 6
        self._clean_windows = 0  # consecutive clean summary intervals

        # --- Token bucket and latency adaptation ---
        self._token_capacity = 12
        self._tokens = self._token_capacity * 0.7
        self._base_token_refill_rate = 1.2
        self._token_refill_rate = self._base_token_refill_rate
        self._last_refill_ts = time.time()
        self._latency_samples: List[float] = []
        self._latency_window = 50
        self._latency_avg = None

        # --- User-agent, locale, and timezone rotation ---
        self._ua_pool = [
            # Curated realistic desktop Chrome UA variants (2024–2025 era)
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
        ]
        try:
            extra_ua = os.getenv("AVVO_EXTRA_UA")
            if extra_ua:
                self._ua_pool.append(extra_ua.strip())
        except Exception:
            pass
        self._accept_languages = [
            "en-US,en;q=0.9",
            "en-US,en-GB;q=0.9,en;q=0.8",
            "en-US,en;q=0.8,es;q=0.6",
        ]
        self._timezones = ["America/New_York", "America/Chicago", "America/Los_Angeles", "UTC"]

        # --- CAPTCHA detection and escalation ---
        self._metric["captcha_hits"] = 0
        self._captcha_storm_threshold = 3  # consecutive CAPTCHAs to trigger slowdown
        self._recent_captcha = []  # timestamps of recent CAPTCHAs
        self._captcha_window = 900  # seconds to track CAPTCHAs
        self._captcha_consecutive = 0  # consecutive CAPTCHAs
        self._last_captcha_ts = 0.0

        # --- Default jitter for guide scheduling if not set ---
        if self.schedule_guide_jitter_max == 0.0:
            self.schedule_guide_jitter_min = 0.05
            self.schedule_guide_jitter_max = 0.25

    # -------------- Scrapy lifecycle hooks ----------------
    # These methods integrate the spider with Scrapy's engine and event system.
    # They handle signal registration and define the initial crawl entrypoint.

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):  # type: ignore[override]
        """
        Scrapy hook: called to instantiate the spider and attach it to the crawler.
        Registers custom signal handlers (e.g., for idle events).
        """
        spider = super().from_crawler(crawler, *args, **kwargs)
        # Connect the spider_idle handler so we can manage delayed requests and throttling.
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def start_requests(self) -> Iterable[Request]:  # type: ignore[override]
        """
        Scrapy entrypoint: yields the first requests to start the crawl.
        - Initializes the DB connection.
        - Optionally preloads existing guides for deduplication if skip_existing is set.
        - Yields the initial request to the all-topics page (with Playwright meta for JS rendering if needed).
        """
        init_db()
        if self.skip_existing:
            try:
                # Preload canonical URLs of already-seen guides to avoid redundant work.
                self._preload_existing_guides()
            except Exception as e:
                self.logger.warning(f"[DEDUPE-PRELOAD] failed: {e}")
    # Start crawl at the all-topics page.
        yield Request(
            self.all_topics_url,
            callback=self.parse_all_topics,
            meta=self._playwright_meta(base_meta={}),
            dont_filter=True,
        )

    async def start(self):  # Scrapy 2.13+ async entrypoint to silence deprecation warning
        """Bridge method: delegate to existing start_requests generator for compatibility.

        Provided because start_requests() is deprecated in Scrapy 2.13+. Keeping both
        maintains backward compatibility with older Scrapy versions while removing the
        runtime warning on newer versions.
        """
        for r in self.start_requests():
            yield r

    def spider_idle(self):
        """
        Scrapy signal handler: called when the engine is idle (no scheduled requests).
        This method manages delayed request release, adaptive throttling, and spider keepalive.
        """
        # --- 1. Release delayed requests if possible (e.g., after 429 backoff) ---
        now = time.time()

        # Auto-reset storm state if the cooloff window has elapsed and no recent 429s remain.
        try:
            if self._storm_active and self._throttle_until and now >= self._throttle_until:
                recent429 = [t for t in self._recent_429 if now - t < 30]
                if not recent429:
                    self.logger.debug("[ADAPT] auto-reset storm state on idle (no recent 429s)")
                    self._storm_active = False
                    self._storm_extensions = 0
                    self._storm_retry_level = 0
        except Exception:
            pass

        released = 0
        # Only release delayed requests if not in a global throttle window.
        while self._delayed:
            # If still cooling off, do not release any delayed requests yet.
            if self._throttle_until and now < self._throttle_until:
                # Log suppression once per idle tick to avoid log spam.
                self.logger.debug(
                    f"[DELAYED-SUPPRESS] throttle_active remaining={int(self._throttle_until - now)}s queued={len(self._delayed)}"
                )
                break
            # If the next delayed request is not yet ready, stop.
            if self._delayed[0][0] > now:
                break
            # Ready & not throttled: release one delayed request.
            _, _, req = heapq.heappop(self._delayed)
            self.crawler.engine.crawl(req)  # public engine API
            released += 1
            if released >= self._delayed_release_batch:
                # Defer remaining ready items to a future idle tick to smooth out bursts.
                break
        if released:
            self.logger.debug(f"[DELAYED] released={released} remaining={len(self._delayed)}")

        # --- 2. Refill token bucket for pacing/throttling ---
        try:
            self._refill_tokens()
        except Exception:
            pass

        # --- 3. Watchdog: detect and force completion of stalled topics ---
        try:
            now_ts = time.time()
            stalled = []
            for slug, st in self._topics.items():
                if st.waiting_requeue and not st.exhausted:
                    # If all expected guides were collected but inflight counter never hit zero (e.g., lost Request), force completion.
                    if st.expected_current_page_guides and len(st.current_page_guides) >= st.expected_current_page_guides:
                        stalled.append((slug, 'expected_met'))
                    # If inflight guides > 0 but no progress for >180s, force completion (likely stuck).
                    last_progress = getattr(st, 'last_progress_ts', None)
                    if last_progress and (now_ts - last_progress) > 180:
                        stalled.append((slug, 'timeout'))
            for slug, reason in stalled:
                self.logger.warning(f"[WATCHDOG] forcing page completion topic={slug} reason={reason}")
                self._force_page_completion(slug)
        except Exception:
            pass

        # --- 4. If discovery is complete, schedule more topic requests if we have capacity ---
        if self._discovery_complete and len(self._active_topics) < self.topic_parallelism:
            next_req = self._next_topic_request()
            if next_req:
                self.crawler.engine.crawl(next_req)

        # --- 5. Keep spider alive if there are delayed requests and discovery is not done ---
        if not self._discovery_complete and self._delayed:
            # Prevent closure until the earliest delayed request matures.
            raise scrapy.exceptions.DontCloseSpider()

        # --- 6. Periodic progress summary logging ---
        now = time.time()
        if now >= self._next_summary:
            self._log_progress_summary()
            self._next_summary = now + self._summary_interval

        # --- 7. Keep spider alive if there is still pending work (active topics or delayed requests) ---
        if any(not t.exhausted for t in self._topics.values()) or self._delayed:
            # Keep open while there is active work OR pending delayed retries.
            raise scrapy.exceptions.DontCloseSpider()

    # -------------- Discovery ----------------
    async def parse_all_topics(self, response: scrapy.http.Response):  # type: ignore[override]
        """
        Discover all topic URLs from the master topics page.

        - Handles rate limiting (429) and retries.
        - Optionally uses Playwright to scroll and trigger lazy loading for full topic discovery.
        - If too few topics are found, optionally merges in fallback topics from a seed file.
        - Persists/resumes topic progress from DB, shuffles rotation, and schedules the initial batch respecting topic_parallelism.
        """
        # --- 1. Handle rate limiting (429) for discovery requests ---
        if response.status == 429:
            self._handle_429(kind="discovery", url=response.url, meta=response.meta, callback=self.parse_all_topics)
            return

        # --- 2. Optionally use Playwright to scroll and simulate user for full topic loading ---
        page = response.meta.get("playwright_page")
        html = response.text
        if page:
            try:
                if self.scroll_discovery:
                    # Scroll to bottom (multiple rounds) to trigger lazy loading of all topics.
                    await self._auto_scroll(page, max_rounds=10, idle_ms=300)
                if self.humanize:
                    # Simulate random mouse movement and pauses to appear less robotic.
                    await self._human_mouse_wiggle(page)
                    await self._human_random_pause()
                # Get the fully rendered HTML after scrolling/humanization.
                html = await page.content()
            except Exception:
                pass
            finally:
                # Always close Playwright page asynchronously to avoid resource leaks.
                try:
                    awaitable_close = getattr(page, "close", None)
                    if callable(awaitable_close):
                        import asyncio
                        asyncio.create_task(page.close())
                except Exception:
                    pass

        # --- 3. Extract topics from the (possibly rendered) HTML ---
        topics = self._extract_topics(html, base_url=response.url)
        primary_count = len(topics)

        # --- 4. Fallback: If too few topics found, merge in seed file topics (if enabled) ---
        if self.use_seed_fallback and primary_count < self.min_topics_expected:
            added = 0
            try:
                import json, os
                if os.path.exists(self.seed_file):
                    with open(self.seed_file, "r") as f:
                        seed_urls = json.load(f)
                    existing_urls = {u for _, u in topics}
                    for su in seed_urls:
                        if su not in existing_urls:
                            slug = su.rstrip('/').split('/')[-1]
                            topics.append((slug, su))
                            existing_urls.add(su)
                            added += 1
                    if added:
                        self.logger.info(f"[DISCOVERY] Seed fallback added {added} topics (primary={primary_count} < expected={self.min_topics_expected}).")
            except Exception as e:
                self.logger.warning(f"[DISCOVERY] Seed fallback failed: {e}")

        # --- 5. Cap total topics if max_topics is set ---
        if self.max_topics > 0:
            topics = topics[: self.max_topics]
        if not topics:
            self.logger.warning("[DISCOVERY] No topics found – stopping.")
            self._discovery_complete = True
            return

        # --- 6. Persist/resume topic progress from DB and initialize TopicState objects ---
        for name, url in topics:
            up = upsert_topic(url)
            slug = up["topic_slug"]
            prog = get_topic_progress(slug) or {}
            # Resume from last page if available, else start at page 1.
            start_page = (prog.get("last_page_number") or 0) + 1
            st = TopicState(slug=slug, url=url, next_page=start_page)
            if prog.get("is_complete"):
                st.exhausted = True
            # Load stored total pages if available.
            try:
                tp = get_topic_total_pages(slug)
                if tp:
                    st.total_pages_detected = tp
            except Exception:
                pass
            self._topics[slug] = st

        # --- 7. Shuffle topic rotation for crawl order (optionally deterministic) ---
        slugs = list(self._topics.keys())
        if self.shuffle_seed is not None:
            rnd = random.Random(self.shuffle_seed)
            rnd.shuffle(slugs)
        else:
            random.shuffle(slugs)
        self._rotation.extend(slugs)
        self._discovery_complete = True
        self.logger.info(f"[DISCOVERY] Collected {len(slugs)} topics (primary={primary_count}). Starting rotation.")
        for idx, slug in enumerate(slugs, start=1):
            st = self._topics[slug]
            self.logger.debug(f"[DISCOVERY-TOPIC] #{idx} slug={slug} start_page={st.next_page} url={st.url}")

        # --- 8. Schedule the initial batch of topic requests (limit by topic_parallelism) ---
        initial = []
        for _ in range(min(len(self._rotation), self.topic_parallelism)):
            req = self._next_topic_request()
            if req:
                initial.append(req)
        for r in initial:
            # Optionally add a human-like pause before yielding each request.
            if self.humanize and (self.jitter_page_max > 0):
                await self._human_random_pause()
            yield r

    # -------------- Rotation scheduling ----------------
    def _next_topic_request(self) -> Optional[Request]:
        """
        Select and schedule the next topic page request, respecting rotation, exhaustion, and global throttling.

        - Pops the next topic from the rotation queue (skipping exhausted topics).
        - Skips topics that have reached their per-session page cap.
        - Applies global throttle window: returns None if throttling is active.
        - Constructs a Scrapy Request for the next topic page, with Playwright meta if needed.
        - Uses a token bucket for pacing: if no token is available, the request is queued/delayed and the loop continues.
        - Returns the next ready Request, or None if no eligible topics remain.
        """
        now = time.time()
        # --- 1. If a global throttle window is active, do not schedule any new topic requests ---
        if self._throttle_until and now < self._throttle_until:
            return None

        # --- 2. Pop topics from the rotation queue until we find one that is eligible ---
        while self._rotation:
            slug = self._rotation.popleft()
            state = self._topics.get(slug)
            # Skip topics that are exhausted or missing state
            if not state or state.exhausted:
                continue
            # If per-topic page cap is set and reached, mark as exhausted and skip
            if self.per_topic_pages > 0 and state.pages_seen >= self.per_topic_pages:
                state.exhausted = True
                continue
            # --- 3. Build the next page request for this topic ---
            page_number = state.next_page
            url = self._topic_page_url(state.url, page_number)
            self.logger.debug(f"[SCHEDULE] slug={slug} page={page_number} url={url}")
            req = Request(
                url,
                callback=self.parse_topic_page,
                meta={
                    **self._playwright_meta(),
                    "topic_slug": slug,
                    "topic_url": state.url,
                    "page_number": page_number,
                },
                dont_filter=True,
            )
            # --- 4. Apply token bucket pacing: if no token, queue/delay and continue loop ---
            if self._consume_token_or_delay(req):
                # Token available: schedule this request now
                return req
            else:
                # No token: request was queued for later, try next topic in rotation
                continue
        # No eligible topics found
        return None

    # -------------- Topic listing parsing ----------------
    def _topic_page_url(self, topic_url: str, page_number: int) -> str:
        """
        Construct a topic listing URL for a given page number, always forcing the Guides tab.

        - For page 1: Avvo omits the 'page=' param, but 'guide=1' is always required to stay on the Guides tab.
        - For page >1: Both 'guide=1' and 'page=N' are included.
        """
        if page_number <= 1:
            # Page 1: only need to force 'guide=1' param (no 'page=1' needed)
            return self._add_query_param(topic_url, guide=1)
        # Subsequent pages: add both 'guide=1' and 'page=N'
        return self._add_query_param(topic_url, guide=1, page=page_number)

    async def parse_topic_page(self, response: scrapy.http.Response):  # type: ignore[override]
        """
        Parse a single topic listing page, extract guide URLs, handle pagination, and schedule guide detail requests.

        This method enforces the Guides tab, handles rate limiting (429), CAPTCHA detection, deduplication,
        per-page and per-topic caps, and robustly manages topic/session state. It also schedules guide detail
        requests and determines when a topic is exhausted.
        """
        slug: str = response.meta["topic_slug"]
        page_number: int = response.meta["page_number"]
        state = self._topics.get(slug)
        if not state:
            # Defensive: If topic state is missing, abort.
            return

        # --- 1. Enforce 'guide=1' param to stay on Guides tab (not Q&A) ---
        try:
            if "guide=1" not in response.url:
                enforced_url = self._topic_page_url(response.meta.get("topic_url"), page_number)
                if enforced_url and enforced_url != response.url:
                    self.logger.debug(f"[ENFORCE-GUIDE] redirecting topic={slug} page={page_number} to {enforced_url}")
                    yield Request(
                        enforced_url,
                        callback=self.parse_topic_page,
                        meta={**response.meta, **self._playwright_meta()},
                        dont_filter=True,
                    )
                    return
        except Exception:
            pass

        # --- 2. Latency tracking for pacing/throttling metrics ---
        try:
            sched = response.meta.get("scheduled_ts")
            if sched:
                self._record_latency(time.time() - sched, kind="topic")
        except Exception:
            pass

        # --- 3. Handle HTTP 429 (rate limit) responses ---
        if response.status == 429:
            self._handle_429(kind="topic", url=response.url, meta=response.meta, callback=self.parse_topic_page)
            return

        # --- 4. CAPTCHA detection (HTML body inspection) ---
        if self._looks_like_captcha(response.text):
            self._handle_captcha(kind="topic", url=response.url, meta=response.meta, callback=self.parse_topic_page)
            return

        # --- 5. If Playwright page is present, activate Guides tab, scroll, and humanize if enabled ---
        html = response.text
        page = response.meta.get("playwright_page")
        if page:
            try:
                await self._activate_guides_tab(page)
                await self._auto_scroll(page)
                if self.humanize:
                    await self._human_mouse_wiggle(page)
                    # Small random pause to mimic reading time before parsing
                    await self._human_random_pause(kind="page")
                html = await page.content()
            except Exception:
                pass

        # --- 6. Extract guide URLs from the topic page ---
        soup = BeautifulSoup(html, "lxml")
        guide_urls, raw_total, video_skipped = self._extract_guide_urls(soup, response.url, state)

        # --- 7. Deduplicate: filter out guides already seen if skip_existing is enabled ---
        if self.skip_existing and self._existing_guides:
            filtered = []
            skipped = 0
            for u in guide_urls:
                can = self._canonical_url(u)
                if can in self._existing_guides:
                    skipped += 1
                    continue
                filtered.append(u)
            if skipped:
                self._metric["existing_skipped"] += skipped
                self.logger.info(
                    f"[DEDUPE] topic={slug} page={page_number} skipped_existing={skipped} remaining_new={len(filtered)} raw_cards={raw_total}"
                )
            guide_urls = filtered

        # --- 8. Optional per-page cap (after deduplication) ---
        if self.max_guides_per_page > 0 and len(guide_urls) > self.max_guides_per_page:
            original_len = len(guide_urls)
            guide_urls = guide_urls[: self.max_guides_per_page]
            self.logger.debug(
                f"[PAGE-CAP] slug={slug} page={page_number} capped={len(guide_urls)}/{original_len} max={self.max_guides_per_page}"
            )

        # --- 9. Persist observed (non-video) guide URLs for aggregate counts ---
        try:
            record_topic_guides(slug, guide_urls)
        except Exception:
            pass

        # --- 10. Pagination detection & total pages inference (once per topic) ---
        if state.total_pages_detected is None:
            detected_total = self._extract_total_pages(soup)
            if detected_total:
                state.total_pages_detected = detected_total
                try:
                    set_topic_total_pages(slug, detected_total)
                except Exception:
                    pass
                self.logger.info(f"[PAGINATION] slug={slug} total_pages={detected_total}")
        has_next = self._detect_next_link(soup, page_number)

        # --- 11. Log per-page summary and video skips ---
        if guide_urls:
            self.logger.info(
                f"[TOPIC-PAGE] slug={slug} page={page_number} new_guides={len(guide_urls)}/{raw_total} cumulative_guides={len(state.session_guides)}"
            )
        else:
            self.logger.info(
                f"[TOPIC-PAGE] slug={slug} page={page_number} EMPTY raw_cards={raw_total} cumulative_guides={len(state.session_guides)}"
            )
        if video_skipped:
            self._metric["video_skipped"] += video_skipped
            self.logger.debug(f"[VIDEO-SKIP] slug={slug} page={page_number} skipped={video_skipped}")

        # --- 12. Schedule guide detail requests for this page ---
        for g_url in guide_urls:
            # Log the schedule order for guides on this page
            self.logger.debug(f"[SCHEDULE-GUIDE] topic={slug} page={page_number} queue_index={len(state.current_page_guides)} url={g_url}")
            if self.humanize and self.guide_think_max > 0:
                await self._human_random_pause(kind="guide")
            if state.inflight_guides == 0 and not state.current_page_guides:
                state.current_page_guides = []
            # Optional small jitter to avoid scheduling a burst of guide requests at once
            if self.schedule_guide_jitter_max > 0 and self.schedule_guide_jitter_max >= self.schedule_guide_jitter_min:
                try:
                    wait = random.uniform(self.schedule_guide_jitter_min, self.schedule_guide_jitter_max)
                    if wait > 0:
                        await asyncio.sleep(wait)
                except Exception:
                    pass
            # Annotate guide request with page context and sequence index
            req_meta = {"topic_slug": slug, "page_number": page_number, "page_seq": len(state.current_page_guides)}
            guide_req = Request(g_url, callback=self.parse_guide, meta={**self._playwright_meta(), **req_meta}, dont_filter=True)
            # Respect global throttle / storm: defer scheduling if cooling off.
            immediate = self._schedule_or_delay_guide(guide_req)
            if immediate:
                yield guide_req

        # --- 13. Mark visit & progress persistently ---
        mark_topic_visit(slug, pages_crawled_increment=1)
        state.pages_seen += 1
        self._stat_pages += 1
        update_topic_progress(slug, last_page_number=page_number)
        self.logger.info(
            f"[TOPIC] slug={slug} page={page_number} guides_found={len(guide_urls)} total_pages_seen={state.pages_seen}"
        )

        # --- 14. Per-topic page cap (session bound): if reached, mark exhausted for this session ---
        # We deliberately DO NOT mark_complete in DB because further pages may exist; this is a session limit only.
        if self.per_topic_pages > 0 and state.pages_seen >= self.per_topic_pages:
            state.exhausted = True
            self.logger.info(
                f"[TOPIC] slug={slug} reached per_topic_pages cap={self.per_topic_pages}; marking session exhausted (not persisted)."
            )
        else:
            # --- 15. Robust exhaustion logic (only when not capped): ---
            # Mark exhausted only if:
            #   a) no new guides AND (no next link OR consecutive_empty>=2 OR known total pages reached)
            if guide_urls:
                state.consecutive_empty = 0
                state.next_page = page_number + 1
                state.inflight_guides = len(guide_urls)
                state.expected_current_page_guides = len(guide_urls)
                state.waiting_requeue = True
                # Mark topic active (guides for this page outstanding)
                if slug not in self._active_topics:
                    self._active_topics.add(slug)
                self.logger.debug(
                    f"[TOPIC] slug={slug} waiting_guides={state.inflight_guides} will_requeue_next_page={state.next_page} has_next={has_next} detected_total={state.total_pages_detected} active_topics={len(self._active_topics)}/{self.topic_parallelism}"
                )
            else:
                state.consecutive_empty += 1
                will_exhaust = (not has_next) or (state.consecutive_empty >= 2)
                if state.total_pages_detected and page_number >= state.total_pages_detected:
                    will_exhaust = True
                if will_exhaust:
                    state.exhausted = True
                    update_topic_progress(slug, last_page_number=page_number, mark_complete=True)
                    self.logger.info(
                        f"[TOPIC] slug={slug} exhausted at page={page_number} (empty, has_next={has_next})."
                    )
                else:
                    # Optimistic attempt: schedule next page even if first empty but next link exists
                    state.next_page = page_number + 1
                    state.waiting_requeue = False  # allow immediate next scheduling
                    self._rotation.append(slug)
                    self.logger.debug(
                        f"[TOPIC] slug={slug} empty_page_but_next exists -> probing next page={state.next_page}"
                    )

            # Emit rotation snapshot for visibility (small sample)
            try:
                rot_snapshot = list(self._rotation)[:50]
                self.logger.debug(f"[ROTATION-SNAPSHOT] len={len(self._rotation)} next={rot_snapshot}")
            except Exception:
                pass

        # --- 16. Schedule additional topic pages if we have capacity (do NOT exceed topic_parallelism) ---
        if len(self._active_topics) < self.topic_parallelism:
            next_req = self._next_topic_request()
            if next_req:
                if self.humanize and (self.jitter_page_max > 0):
                    await self._human_random_pause()
                yield next_req

    # -------------- Guide detail parsing ----------------
    async def parse_guide(self, response: scrapy.http.Response):  # type: ignore[override]
        """
        Parse a single guide detail page, extract all relevant fields, handle retries, and update state.

        Handles:
          - 429 (rate limit) and CAPTCHA detection with backoff and retry
          - Playwright fallback for JS-rendered or incomplete content
          - Extraction and normalization of all guide fields
          - Per-guide and per-page progress tracking, logging, and completion triggers
        """
        # --- 1. Handle HTTP 429 (rate limit) responses for guide detail ---
        if response.status == 429:
            try:
                if self._guides_inflight_global > 0:
                    self._guides_inflight_global -= 1
            except Exception:
                pass
            self._handle_429(kind="guide", url=response.url, meta=response.meta, callback=self.parse_guide)
            return

        html = response.text

        # --- 2. Latency tracking for pacing/throttling metrics ---
        try:
            sched = response.meta.get("scheduled_ts")
            if sched:
                self._record_latency(time.time() - sched, kind="guide")
        except Exception:
            pass

        # --- 3. CAPTCHA detection (HTML body inspection) ---
        if self._looks_like_captcha(html):
            self._handle_captcha(kind="guide", url=response.url, meta=response.meta, callback=self.parse_guide)
            return

        # --- 4. Parse the guide HTML into structured data ---
        data = parse_guide(html)

        # --- 5. If HTTP fetch returned minimal/empty content, retry once with Playwright for JS render ---
        title = (data.get("title") or "").strip()
        sections = data.get("sections", []) or []
        combined_text = "\n\n".join([s.get("text", "") for s in sections if s.get("text")]).strip() or None
        if (not title or not combined_text or len(combined_text) < 80) and not response.meta.get("retry_playwright"):
            # Schedule a Playwright-backed retry once for fuller render
            self._metric["playwright_fallbacks"] = self._metric.get("playwright_fallbacks", 0) + 1
            self.logger.debug(f"[FALLBACK] requeueing {response.url} with Playwright for fuller render")
            yield Request(
                response.url,
                callback=self.parse_guide,
                meta={**self._playwright_meta(), "retry_playwright": True, "topic_slug": response.meta.get("topic_slug")},
                dont_filter=True,
            )
            return

        # --- 6. Normalize and extract all guide fields for output ---
        canonical = self._canonical_url(response.url)
        slug = canonical.rstrip("/").split("/")[-1]
        authors_list = data.get("authors", []) or []
        jurisdiction = None
        for a in authors_list:
            st = a.get("state")
            if st:
                jurisdiction = st
                break
        sections = data.get("sections", [])
        combined_text = "\n\n".join([s.get("text", "") for s in sections if s.get("text")]).strip() or None
        item = {
            "canonical_url": canonical,
            "slug": slug,
            "title": data.get("title"),
            "published_date": data.get("published_date"),
            "updated_date": data.get("updated_date"),
            "topics": data.get("topics", []),
            "primary_topic": data.get("primary_topic"),
            "jurisdiction": jurisdiction,
            "authors": authors_list,
            "sections": sections,
            "combined_text": combined_text,
            "additional_resources": data.get("additional_resources", []),
            "stats": data.get("stats", {}),
        }

        # --- 7. Insert or update guide in DB, annotate with versioning info ---
        up = insert_or_update_guide(canonical, data.get("content_hash"), item.get("primary_topic"))
        item["version"] = up["version"]
        item["is_updated"] = up["is_updated"]
        yield item

        # --- 8. Decrement global inflight counter at the very end ---
        try:
            if self._guides_inflight_global > 0:
                self._guides_inflight_global -= 1
        except Exception:
            pass

        # --- 9. Per-guide and per-page progress tracking, logging, and completion triggers ---
        topic_slug = response.meta.get("topic_slug")
        page_number_meta = response.meta.get("page_number")
        page_seq = response.meta.get("page_seq")
        if topic_slug and topic_slug in self._topics:
            state = self._topics[topic_slug]
            state.guides_seen += 1
            update_topic_progress(topic_slug, guides_inc=1)
            # Record this guide for per-page summary (store slug for compactness)
            try:
                state.current_page_guides.append(slug)
            except Exception:
                pass
            if state.inflight_guides > 0:
                state.inflight_guides -= 1
            # Track last progress timestamp for watchdog
            try:
                state.last_progress_ts = time.time()
            except Exception:
                pass
            # Log per-guide completion ordering
            try:
                self.logger.info(f"[GUIDE-PROGRESS] topic={topic_slug} page={page_number_meta} seq={page_seq} done={state.guides_seen}/{state.expected_current_page_guides or 'unknown'} slug={slug}")
            except Exception:
                pass

            # --- 10. Primary completion trigger: finalize page if all guides done ---
            if state.waiting_requeue and not state.exhausted:
                completed = (state.inflight_guides == 0) or (
                    state.expected_current_page_guides and len(state.current_page_guides) >= state.expected_current_page_guides
                )
                if completed:
                    self._finalize_page(topic_slug)

        # --- 11. Guide-level log (after potential requeue so we don't inflate spider_idle churn) ---
        stats_wc = item.get("stats", {}).get("word_count")
        self._stat_guides += 1
        self.logger.info(
            f"[GUIDE] slug={slug} v={item.get('version')} updated={item.get('is_updated')} words={stats_wc} canonical={canonical}"
        )

        # --- 12. Optional periodic summary after each guide if interval passed ---
        now = time.time()
        if now >= self._next_summary:
            self._log_progress_summary()
            self._next_summary = now + self._summary_interval

    # -------------- Utilities ----------------

    def _extract_topics(self, html: str, base_url: str) -> List[tuple[str, str]]:
        """
        Extract a list of (topic_name, topic_url) pairs from the given HTML.

        Heuristics:
          - Only include anchors whose href contains '/legal-advice/' or '/topics/' (Avvo topic/guide pages)
          - Exclude anchors that are internal navigation, JavaScript, or the all-topics index itself
          - Deduplicate by absolute URL
          - Only include anchors with non-empty visible text
        Args:
            html: HTML string of the topics page
            base_url: Base URL for resolving relative links
        Returns:
            List of (topic_name, topic_url) tuples
        """
        soup = BeautifulSoup(html, "lxml")
        anchors = soup.find_all("a", href=True)
        results: List[tuple[str, str]] = []
        seen_urls = set()
        for a in anchors:
            href = a.get("href")
            if not href:
                # Skip anchors without href
                continue
            if any(token in href for token in ("#", "javascript:")):
                # Skip internal navigation and JS links
                continue
            if "/free-legal-advice/all-topics" in href:
                # Skip the all-topics index link (not a real topic)
                continue
            if "/legal-advice/" not in href and "/topics/" not in href:
                # Only include links to Avvo topic/guide pages
                continue
            url = urljoin(base_url, href)
            if url in seen_urls:
                # Deduplicate by absolute URL
                continue
            name = a.get_text(" ", strip=True)
            if not name:
                # Skip anchors with no visible text
                continue
            seen_urls.add(url)
            results.append((name, url))
        return results


    def _add_query_param(self, url: str, **params) -> str:
        """
        Return a new URL with the given query parameters added or updated.

        - Preserves all existing query parameters unless overwritten by params
        - Ignores any param with value None
        - Leaves path, fragment, and other URL parts unchanged
        Args:
            url: The original URL
            **params: Query parameters to add or update
        Returns:
            The new URL with updated query string
        """
        from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
        parts = urlsplit(url)
        # Parse existing query parameters
        q = dict(parse_qsl(parts.query, keep_blank_values=True))
        # Update with new params, skipping any with value None
        q.update({k: v for k, v in params.items() if v is not None})
        new_query = urlencode(q)
        # Reconstruct the URL with the new query string
        return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))

    def _canonical_url(self, url: str) -> str:
        """
        Return a normalized canonical URL for a guide or topic.

        - Keeps only the scheme, netloc, and path (removes query, params, fragment)
        - Removes any trailing slash from the path
        - Defensive fallback for malformed URLs or unexpected input

        Args:
            url: The original URL (may be relative, absolute, or malformed)
        Returns:
            Canonicalized absolute URL string
        """
        try:
            # Parse the URL into components
            parts = urlparse(url)
            # Remove trailing slash from the path for canonical form
            clean_path = parts.path.rstrip('/')
            # Recompose the URL with only scheme, netloc, and path; drop params, query, fragment
            return urlunparse((parts.scheme, parts.netloc, clean_path, '', '', ''))
        except Exception:
            # Defensive fallback: manual compose for malformed or non-standard URLs
            if '://' in url:
                # Split scheme and the rest
                base = url.split('://', 1)[1]
                base = base.split('/', 1)
                host = base[0]
                # Remove query and fragment from the path
                path = '' if len(base) == 1 else '/' + base[1].split('?',1)[0].split('#',1)[0]
                # Assume https for fallback, remove trailing slash
                return f"https://{host}{path.rstrip('/')}"
            # If no scheme, just strip trailing slash
            return url.rstrip('/')

    # -------------- Logging helpers ----------------
    def _log_progress_summary(self):
        """
        Log a periodic summary of crawl progress, including active topics, pages, guides, and rotation state.

        - Updates metrics for pages, guides, and topic counts
        - Logs a compact summary line for up to 25 topics (with page/next-page/exhausted state)
        - Emits a structured metrics event for downstream analysis
        - Implements adaptive speed-up: if no 429s in the last interval, gently increases crawl rate
        - If a storm throttle window has elapsed, resets storm state
        """
        # --- 1. Compute and log high-level crawl progress ---
        elapsed = time.time() - self._started_ts
        active = sum(1 for t in self._topics.values() if not t.exhausted)
        total = len(self._topics)
        # Update metrics for reporting and structured logging
        self._metric.update({
            "pages": self._stat_pages,
            "guides": self._stat_guides,
            "topics_active": active,
            "topics_total": total
        })
        # Build a compact summary for up to 25 topics (showing page, next page, exhausted state)
        parts = []
        for slug, st in list(self._topics.items())[:25]:
            parts.append(f"{slug}:p{st.pages_seen}->n{st.next_page}{'X' if st.exhausted else ''}")
        more = '' if len(self._topics) <= 25 else f" ...(+{len(self._topics)-25})"
        self.logger.info(
            f"[PROGRESS] elapsed={int(elapsed)}s topics_active={active}/{total} pages={self._stat_pages} guides={self._stat_guides} rotation_len={len(self._rotation)} :: {' | '.join(parts)}{more}"
        )

        # --- 2. Emit a structured metrics event for downstream analysis ---
        self._log_event("progress", **self._metric)

        # --- 3. Adaptive speed-up: if no 429s in the last interval, gently increase crawl rate ---
        try:
            now = time.time()
            # Find any recent 429s in the last summary interval
            recent = [t for t in self._recent_429 if now - t < self._summary_interval]
            if recent:
                # If any 429s, reset clean window counter
                self._clean_windows = 0
                # If storm flag set but throttle window has elapsed, reset storm state
                if self._storm_active and self._throttle_until and now >= self._throttle_until:
                    self.logger.info("[ADAPT] clearing stale storm flag during summary (throttle elapsed)")
                    self._storm_active = False
                    self._storm_extensions = 0
                    self._storm_retry_level = 0
            else:
                # No 429s: increment clean window counter
                self._clean_windows += 1
                # After two consecutive clean windows, gently speed up crawl rate
                if self._clean_windows >= 2:
                    old_g = self._global_bucket_interval
                    old_guide = self._guide_bucket_interval
                    self._global_bucket_interval = max(self._min_global_bucket_interval, self._global_bucket_interval * 0.9)
                    self._guide_bucket_interval = max(0.5, self._guide_bucket_interval * 0.9)
                    # Gradually restore token refill rate toward base after stable period
                    try:
                        if self._token_refill_rate < self._base_token_refill_rate:
                            self._token_refill_rate = min(self._base_token_refill_rate, self._token_refill_rate * 1.15)
                    except Exception:
                        pass
                    # Only log if the interval actually changed (avoid log spam)
                    if int(old_g*100) != int(self._global_bucket_interval*100):
                        self.logger.info(f"[ADAPT] speed-up global={old_g:.2f}->{self._global_bucket_interval:.2f} guide={old_guide:.2f}->{self._guide_bucket_interval:.2f}")
                        self._clean_windows = 0
        except Exception:
            pass

    # ---------------- New helpers: pagination, scrolling, adaptive, structured logging ---------------
    def _extract_total_pages(self, soup: BeautifulSoup) -> Optional[int]:
        """
        Extract the total number of pages from a topic listing page's pagination controls.

        - Looks for the pagination bar ('.tlp-pagination') and collects all anchor tags with digit text.
        - Returns the highest page number found, or None if pagination is missing or malformed.
        - Defensive: returns None on any error or if no page numbers are found.

        Args:
            soup: BeautifulSoup-parsed HTML of the topic page.
        Returns:
            The maximum page number (int) if found, else None.
        """
        try:
            pag = soup.select_one(".tlp-pagination")
            if not pag:
                # No pagination bar found; likely only one page exists.
                return None
            nums = []
            for a in pag.find_all("a", href=True):
                txt = a.get_text(strip=True)
                if txt.isdigit():
                    # Collect all numeric page links.
                    nums.append(int(txt))
            if not nums:
                # No numeric page links found; fallback to None.
                return None
            return max(nums)
        except Exception:
            # Defensive: on any parsing error, treat as unknown page count.
            return None

    def _detect_next_link(self, soup: BeautifulSoup, page_number: int) -> bool:
        """
        Detect whether a 'next page' link exists in the pagination controls for the current topic page.

        - Checks for a '.next' anchor that is not disabled (preferred Avvo markup).
        - As a fallback, looks for an anchor whose text matches the next page number.
        - Returns True if a next page is available, else False.
        - Defensive: returns False on any error or if pagination is missing.

        Args:
            soup: BeautifulSoup-parsed HTML of the topic page.
            page_number: The current page number (int).
        Returns:
            True if a next page is detected, else False.
        """
        try:
            pag = soup.select_one(".tlp-pagination")
            if not pag:
                # No pagination bar; assume no next page.
                return False
            # Preferred: look for a 'next' anchor that is not disabled.
            nxt = pag.find("a", class_="next")
            if nxt and 'disabled' not in nxt.get('class', []):
                return True
            # Fallback: look for a link whose text matches the next page number.
            target = str(page_number + 1)
            for a in pag.find_all("a"):
                if a.get_text(strip=True) == target:
                    return True
            # No next page link found.
            return False
        except Exception:
            # Defensive: on any error, assume no next page.
            return False

    async def _activate_guides_tab(self, page):  # best-effort
        """
        (Playwright) Ensure the 'Guides' tab is active on the topic page.

        - If the guides tab radio button exists and is not checked, clicks it to reveal guide cards.
        - Waits briefly after clicking to allow DOM update.
        - Best-effort: silently ignores any errors (e.g., selector missing, JS error).

        Args:
            page: Playwright page object.
        """
        try:
            # If the guides tab radio exists and is not checked, click it to ensure guide cards are visible.
            await page.evaluate(
                "() => { const lg = document.getElementById('tlp-legal-guides-tab'); if(lg && !lg.checked){ lg.click(); } }"
            )
            # Wait briefly to allow the DOM to update after clicking.
            await page.wait_for_timeout(200)
        except Exception:
            # Silently ignore any errors (e.g., selector not found, JS error).
            pass

    async def _auto_scroll(self, page, max_rounds: int = 6, idle_ms: int = 350):
        """
        (Playwright) Scroll to the bottom of the page iteratively to trigger lazy loading of content.

        - Scrolls in multiple rounds, checking if the page height has stabilized (no more content loads).
        - If humanization is enabled, adds random jitter to the number of scroll rounds and simulates partial/incremental scrolls.
        - Waits between scrolls to allow content to load.
        - Stops early if the scroll height is stable for 2 consecutive rounds.
        - Defensive: silently ignores any errors (e.g., JS errors, Playwright issues).

        Args:
            page: Playwright page object.
            max_rounds: Maximum number of scroll rounds (default 6).
            idle_ms: Milliseconds to wait after each scroll (default 350).
        """
        try:
            last_height = -1
            stable_rounds = 0
            rounds = max_rounds
            if self.humanize and self.scroll_jitter:
                # Randomly reduce or extend scroll depth slightly for human-like behavior.
                jitter = random.randint(-2, 2)
                rounds = max(1, max_rounds + jitter)
            for _ in range(rounds):
                # Measure current scroll height.
                height = await page.evaluate("() => document.body.scrollHeight")
                if height == last_height:
                    # If height hasn't changed, increment stable counter.
                    stable_rounds += 1
                    if stable_rounds >= 2:
                        # Stop if height is stable for 2 rounds (no more lazy loads).
                        break
                else:
                    stable_rounds = 0
                if self.humanize:
                    # Simulate partial/incremental scrolls to mimic user behavior.
                    increments = random.randint(2, 4) if self.scroll_jitter else 1
                    current_pos = await page.evaluate("() => window.scrollY")
                    total_target = height
                    for i in range(increments):
                        frac = (i + 1) / increments
                        y = int(total_target * frac)
                        await page.evaluate(f"(y) => window.scrollTo(0, y)", y)
                        # Wait a random fraction of idle_ms between increments.
                        await page.wait_for_timeout(int(idle_ms * random.uniform(0.4, 0.9)))
                else:
                    # Simple full scroll to bottom.
                    await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(idle_ms)
                last_height = height
        except Exception:
            # Defensive: ignore any errors (e.g., JS/Playwright failures).
            pass

    async def _human_mouse_wiggle(self, page):
        """
        (Playwright) Simulate random mouse movements and scribbles to reduce bot detection risk.

        - Only runs if humanization is enabled.
        - Moves the mouse in a series of random paths and scribbles within the viewport.
        - Each move consists of several small steps with random offsets.
        - Occasionally pauses briefly to mimic human hesitation.
        - Defensive: silently ignores any errors (e.g., Playwright failures).

        Args:
            page: Playwright page object.
        """
        if not self.humanize:
            return
        try:
            # Choose a random number of mouse moves per session.
            moves = random.randint(self.mouse_moves_min, max(self.mouse_moves_min, self.mouse_moves_max))
            # Get viewport size for realistic movement bounds.
            viewport = await page.evaluate("() => ({w: window.innerWidth, h: window.innerHeight})")
            w = viewport.get("w", 1200) or 1200
            h = viewport.get("h", 800) or 800
            for _ in range(moves):
                # Each move is a scribble with several steps.
                steps = random.randint(self.mouse_scribble_steps_min, max(self.mouse_scribble_steps_min, self.mouse_scribble_steps_max))
                base_x = random.randint(int(w*0.1), int(w*0.9))
                base_y = random.randint(int(h*0.1), int(h*0.9))
                for _s in range(steps):
                    dx = random.randint(-30, 30)
                    dy = random.randint(-20, 20)
                    await page.mouse.move(base_x + dx, base_y + dy, steps=random.randint(1,3))
                # Occasionally pause briefly to mimic human hesitation.
                if random.random() < 0.3:
                    await page.wait_for_timeout(random.randint(40,140))
        except Exception:
            # Defensive: ignore any errors (e.g., Playwright failures).
            pass

    async def _human_random_pause(self, kind: str | None = None):
        """
        (Playwright) Pause for a random interval to simulate human think time or page load jitter.

        - Only runs if humanization is enabled.
        - For guide detail requests, uses guide_think_min/max; otherwise uses jitter_page_min/max.
        - Skips if max delay is zero or negative.
        - Defensive: silently ignores any errors (e.g., asyncio failures).

        Args:
            kind: If 'guide', uses guide_think_min/max; else uses page jitter params.
        """
        if not self.humanize:
            return
        lo = 0.0
        hi = 0.0
        if kind == "guide" and self.guide_think_max > 0:
            # Use guide-specific think time range.
            lo, hi = self.guide_think_min, self.guide_think_max
        else:
            # Use page-level jitter range.
            lo, hi = self.jitter_page_min, self.jitter_page_max
        if hi <= 0:
            # No pause needed if max is zero or negative.
            return
        try:
            delay = random.uniform(max(0.0, lo), hi)
            if delay > 0:
                await asyncio.sleep(delay)
        except Exception:
            # Defensive: ignore any errors (e.g., asyncio failures).
            pass

    def _handle_429(self, kind: str, url: str, meta: dict, callback):
        """
        Handle HTTP 429 (rate limit) responses with adaptive backoff, storm logic, and retry scheduling.

        - Tracks recent 429s and triggers a "storm" mode if too many occur in a short window.
        - In storm mode, applies a longer global throttle and escalates retry delays.
        - Uses exponential backoff with jitter for retry delays, honoring Retry-After headers if present.
        - Schedules the retry via the delayed queue, ensuring no requests are sent before the throttle window ends.
        - Logs all events and updates metrics for downstream analysis.
        - Defensive: handles all edge cases and avoids infinite escalation.

        Args:
            kind: The type of request (e.g., 'discovery', 'topic', 'guide').
            url: The URL that triggered the 429.
            meta: Scrapy meta dict for the request.
            callback: The callback function for the retry request.
        """
        now = time.time()
        self._recent_429.append(now)
        # Purge old 429 timestamps outside the tracking window.
        cutoff = now - self._429_window
        self._recent_429 = [t for t in self._recent_429 if t >= cutoff]

        # If a previous storm cooloff window has elapsed, allow a fresh storm cycle.
        if self._storm_active and self._throttle_until and now >= self._throttle_until:
            # Reset storm state so new clustering logic can trigger a new cooloff.
            self.logger.info(
                f"[ADAPT] storm window elapsed (ended={int(self._throttle_until)}) recent_429={len(self._recent_429)} resetting storm state"
            )
            self._storm_active = False
            self._storm_extensions = 0
            self._storm_retry_level = 0
            # Trim very old 429 timestamps so a stale long history doesn't immediately retrigger large storms.
            self._recent_429 = [t for t in self._recent_429 if now - t < 60]

        storms_before = self._metric.get("429_storms", 0)
        # Early cooloff: on very first 429 if no storm active, immediately enter a longer calm window
        if len(self._recent_429) == 1 and not self._storm_active:
            self._storm_active = True
            self._metric["429_storms"] = storms_before + 1
            # Conservative initial cooloff 60-90s
            self._throttle_until = now + random.uniform(60, 90)
            self._storm_retry_level = 1
            self.logger.warning(
                f"[ADAPT] early-first-429 cooloff until={int(self._throttle_until)} kind={kind}"
            )
        # Storm trigger: if too many 429s in window, enter storm mode with longer cooloff
        elif len(self._recent_429) >= self._429_threshold and not self._storm_active:
            self._storm_active = True
            self._metric["429_storms"] = storms_before + 1
            self._throttle_until = now + random.uniform(self._cooloff_min, self._cooloff_max)
            self._storm_retry_level = 1  # initialize global escalation
            self.logger.warning(f"[ADAPT] 429 storm trigger count={len(self._recent_429)} cooloff_until={int(self._throttle_until)} kind={kind}")
        # If already in storm mode and throttle window not elapsed, extend storm and escalate
        elif self._storm_active and now < self._throttle_until and self._storm_extensions < self._storm_max_extensions:
            # Extend storm window for continued 429s
            self._storm_extensions += 1
            extend = random.uniform(10, 30)
            self._throttle_until += extend
            self.logger.info(f"[ADAPT] extending storm ext={self._storm_extensions} +{extend:.1f}s new_until={int(self._throttle_until)}")
            # Escalate global retry level each additional 429 during storm (capped)
            if self._storm_retry_level < 8:
                self._storm_retry_level += 1
            # Reduce token refill rate progressively during storm to lower discovery pressure
            try:
                self._token_refill_rate = max(self._base_token_refill_rate * 0.2, self._token_refill_rate * 0.85)
            except Exception:
                pass

        # Exponential backoff with jitter (base):
        new_meta = dict(meta)
        prev_retries = int(new_meta.get("retry_429", 0))
        next_retry = prev_retries + 1
        new_meta["retry_429"] = next_retry
        # Determine effective exponential level: per-request retries OR global storm level (whichever larger)
        if self._storm_active:
            base_unit = 5
            exponent_level = max(self._storm_retry_level - 1, next_retry - 1)
        else:
            base_unit = 2
            exponent_level = next_retry - 1
        raw_delay = base_unit * (2 ** exponent_level)
        # Cap raw delay then apply jitter
        capped = min(self._max_429_delay, raw_delay)
        delay = capped * random.uniform(0.75, 1.25)

        # Honor Retry-After header if present & larger
        retry_after = None
        try:
            # meta might contain headers if we pass them; defensive guard
            headers = meta.get('response_headers') or {}
            ra = headers.get('Retry-After') or headers.get('retry-after')
            if ra:
                if ra.isdigit():
                    retry_after = float(ra)
                else:
                    # HTTP-date format attempt
                    try:
                        retry_after = max(0.0, time.mktime(email.utils.parsedate(ra)) - now)
                    except Exception:
                        retry_after = None
        except Exception:
            pass
        if retry_after and retry_after > delay:
            # Use Retry-After header if it exceeds computed delay.
            delay = retry_after + random.uniform(1.0, 3.0)

        # If in a global throttle window, ensure we don't schedule earlier than its end.
        if self._throttle_until and delay < (self._throttle_until - now):
            # Schedule just after throttle end plus small random cushion.
            delay = (self._throttle_until - now) + random.uniform(1.0, 5.0)
        ready_at = now + delay
        self._metric["429_total"] = self._metric.get("429_total", 0) + 1

        # For topic listing retries, recompute URL with guide=1 to ensure Guides tab.
        retry_url = url
        try:
            if kind == "topic" and new_meta.get("topic_url"):
                retry_url = self._topic_page_url(new_meta.get("topic_url"), new_meta.get("page_number", 1))
        except Exception:
            pass
        req = Request(retry_url, meta=new_meta, callback=callback, dont_filter=True)
        self._delayed_seq += 1
        heapq.heappush(self._delayed, (ready_at, self._delayed_seq, req))
        self.logger.warning(f"[429] kind={kind} retry_in={delay:.1f}s url={url}")
        # Structured log for downstream analysis.
        self._log_event("429", kind=kind, url=url, delay=delay, storm=self._storm_active)
        # Adaptive global pacing: slow down a bit after each 429 (bounded)
        try:
            self._global_bucket_interval = min(self._max_global_bucket_interval, self._global_bucket_interval * 1.2)
            self._guide_bucket_interval = min(5.0, self._guide_bucket_interval * 1.25)
        except Exception:
            pass

    # -------------- New scheduling helpers for better storm throttling --------------
    def _queue_delayed(self, req: Request, ready_at: float):
        """
        Internal helper: schedule a request for future execution by pushing it into the delayed heap.

        - Uses a heap (priority queue) to efficiently manage delayed requests by their ready timestamp.
        - Each request is assigned a unique sequence number to break ties and preserve FIFO order for same-time requests.
        - Logs the scheduling event for debugging and visibility.

        Args:
            req: The Scrapy Request to delay.
            ready_at: The UNIX timestamp when the request should become eligible for execution.
        """
        self._delayed_seq += 1
        # Push (ready_at, sequence, request) tuple into the heap for efficient scheduling.
        heapq.heappush(self._delayed, (ready_at, self._delayed_seq, req))
        self.logger.debug(
            f"[DELAYED-QUEUE] scheduled_at={int(time.time())} ready_at={int(ready_at)} in={ready_at-time.time():.1f}s url={req.url}"
        )

    def _schedule_or_delay_guide(self, req: Request) -> bool:
        """
        Decide whether to schedule a guide request immediately or delay it based on pacing, throttling, and concurrency.

        - Enforces global and guide-specific pacing buckets to avoid bursts and mimic human-like request intervals.
        - If a global throttle window is active, always delays until just after it ends.
        - If storm mode is active, spreads guide requests with a modest random delay to avoid burst after throttle.
        - Enforces a concurrency cap on in-flight guide requests; delays if cap is reached.
        - Returns True if the caller should yield the request now, False if it was queued for later.

        Args:
            req: The Scrapy Request for a guide detail page.
        Returns:
            True if the request should be yielded now, False if it was delayed.
        """
        now = time.time()
        # 1. Global request bucket: ensure minimum spacing between ANY requests (all types)
        g_last = self._last_global_req_ts
        if g_last + self._global_bucket_interval > now:
            ready_at = g_last + self._global_bucket_interval + random.uniform(0.05, 0.25)
            self._queue_delayed(req, ready_at)
            return False
        # 2. Guide-specific pacing bucket (adaptive): ensure spacing between guide requests
        last = getattr(self, "_last_guide_ts", 0.0)
        if last + self._guide_bucket_interval > now:
            ready_at = last + self._guide_bucket_interval + random.uniform(0.05, 0.35)
            self._queue_delayed(req, ready_at)
            self._last_guide_ts = ready_at
            return False
        # 3. Global throttle window: if active, delay until just after it ends
        if self._throttle_until and now < self._throttle_until:
            ready_at = self._throttle_until + random.uniform(0.5, 3.0)
            self._queue_delayed(req, ready_at)
            return False
        # 4. Storm mode: after throttle, spread requests with modest random delay to avoid burst
        if self._storm_active:
            base_delay = random.uniform(2.0, 8.0)
            ready_at = now + base_delay
            self._queue_delayed(req, ready_at)
            return False
        # 5. Mark guide scheduled time for pacing
        self._last_guide_ts = now
        self._last_global_req_ts = now
        # 6. Concurrency cap: if too many in-flight guides, delay
        if self._guides_inflight_global >= self._max_concurrent_guides:
            ready_at = now + random.uniform(1.0, 3.0)
            self._queue_delayed(req, ready_at)
            return False
        # 7. Otherwise, schedule immediately and increment in-flight counter
        self._guides_inflight_global += 1
        return True

    def _log_event(self, event: str, **fields):
        """
        Emit a structured log event for downstream analysis or debugging.

        - Formats the event as a JSON object with a timestamp, event type, spider name, and any additional fields.
        - Uses info-level logging for visibility.
        - Defensive: ignores any errors in logging to avoid disrupting the crawl.

        Args:
            event: The event type string (e.g., 'progress', '429', 'captcha').
            **fields: Additional fields to include in the event payload.
        """
        try:
            payload = {
                "ts": int(time.time()),
                "event": event,
                "spider": self.name,
                **fields,
            }
            self.logger.info(json.dumps(payload, sort_keys=True))
        except Exception:
            # Defensive: ignore any logging errors.
            pass

    # -------------- Playwright meta / stealth helpers --------------
    def _playwright_meta(self, base_meta: Optional[dict] = None) -> dict:
        """
        Construct a Scrapy meta dict enabling Playwright with stealth context and anti-bot spoofing.

        - Randomizes user agent, language, and timezone for each request to reduce fingerprinting.
        - Assigns a context key to encourage session isolation (bucketed by UA).
        - Injects a stealth JS script to remove navigator.webdriver, spoof plugins, and add a pseudo chrome object.
        - Adds a small randomized delay to further reduce automation fingerprints.
        - Marks the scheduling timestamp for latency measurement.
        - Defensive: does not handle proxy selection (removed).

        Args:
            base_meta: Optional dict to merge into the returned meta.
        Returns:
            A dict suitable for Scrapy Request meta enabling Playwright and stealth features.
        """
        m = {"playwright": True}
        # Randomized per-request spoof values for anti-bot evasion.
        ua = random.choice(self._ua_pool)
        lang = random.choice(self._accept_languages)
        tz = random.choice(self._timezones)
        # Distinct context key to encourage separate isolated sessions; bucket by UA to limit explosion.
        ctx_key = f"ctx_{abs(hash(ua)) % 5}"
        m["playwright_context"] = ctx_key
        if base_meta:
            m.update(base_meta)
        # Inject stealth JS and a small randomized delay as Playwright page coroutines.
        delay_ms = int(random.uniform(150, 450))
        # Dynamic stealth script includes UA / language / timezone overrides.
        stealth_js = (
            "() => {"
            "try { Object.defineProperty(navigator,'webdriver',{get:()=>undefined}); } catch(e){}"
            f"try {{ Object.defineProperty(navigator,'userAgent',{{get:()=>'{ua}'}}); }} catch(e){{}}"
            f"try {{ Object.defineProperty(navigator,'language',{{get:()=>'{lang.split(',')[0]}'}}); }} catch(e){{}}"
            f"try {{ Object.defineProperty(navigator,'languages',{{get:()=>{list({lang.split(',')[0]})}}}); }} catch(e){{}}"
            "try { if(!navigator.plugins || navigator.plugins.length===0){ Object.defineProperty(navigator,'plugins',{get:()=>[{name:'Chrome PDF Plugin'}]}); } } catch(e){}"
            "try { if(!window.chrome){ window.chrome={runtime:{}}; } } catch(e){}"
            "try { Intl.DateTimeFormat = (function(orig){ return function(locale,opts){ return orig.call(this, locale||undefined, opts); }; })(Intl.DateTimeFormat); } catch(e){}"
            "}"
        )
        m["playwright_page_coroutines"] = [
            {"method": "add_init_script", "args": [stealth_js]},
            {"method": "wait_for_timeout", "args": [delay_ms]},
        ]
        # Mark scheduling timestamp for latency measurement.
        m['scheduled_ts'] = time.time()
        return m

    # ---------------- CAPTCHA detection & handling ----------------
    def _looks_like_captcha(self, html: str) -> bool:
        """
        Heuristically detect if the given HTML looks like a CAPTCHA or anti-bot challenge page.

        - Uses fast substring search for common CAPTCHA and anti-bot tokens (Cloudflare, hCaptcha, reCAPTCHA, etc).
        - To reduce false positives, requires either a <form> tag or a script reference to a known provider.
        - Defensive: returns False on any error or if HTML is empty.

        Args:
            html: The HTML string to inspect.
        Returns:
            True if a CAPTCHA is detected, else False.
        """
        try:
            if not html:
                return False
            # Fast substring heuristics for common CAPTCHA/anti-bot tokens.
            haystack = html.lower()
            captcha_tokens = [
                "captcha",
                "are you a human",
                "verify you are a human",
                "recaptcha",
                "hcaptcha",
                "cf-challenge",
                "data-sitekey",  # generic site key attribute for captcha widgets
                "cloudflare ray id",
            ]
            if any(token in haystack for token in captcha_tokens):
                # Require either a <form> or a script tag referencing a known provider to reduce false positives.
                if re.search(r"<form[^>]*>", haystack) or re.search(r"(recaptcha|hcaptcha|cf-challenge)", haystack):
                    return True
            return False
        except Exception:
            # Defensive: on any error, assume not a CAPTCHA.
            return False

    def _handle_captcha(self, kind: str, url: str, meta: dict, callback):
        """
        Handle detected CAPTCHA/anti-bot challenge by escalating throttle, scheduling a delayed retry, and adapting pacing.

        - Tracks recent CAPTCHAs and triggers storm mode with a long cooloff if too many occur in a short window.
        - Escalates the global throttle and retry level to slow down the crawl.
        - Schedules a retry with a heavy backoff (longer than 429), using a delayed queue.
        - Logs the event and updates metrics for downstream analysis.
        - Adaptively increases pacing intervals based on consecutive CAPTCHA streaks.
        - Defensive: handles all edge cases and avoids infinite escalation.

        Args:
            kind: The type of request (e.g., 'discovery', 'topic', 'guide').
            url: The URL that triggered the CAPTCHA.
            meta: Scrapy meta dict for the request.
            callback: The callback function for the retry request.
        """
        now = time.time()
        self._metric["captcha_hits"] = self._metric.get("captcha_hits", 0) + 1
        self._recent_captcha.append(now)
        # Purge old captcha timestamps outside the tracking window.
        cutoff = now - self._captcha_window
        self._recent_captcha = [t for t in self._recent_captcha if t >= cutoff]
        # Consecutive tracking: reset if >120s since last captcha.
        if now - self._last_captcha_ts > 120:
            self._captcha_consecutive = 0
        self._captcha_consecutive += 1
        self._last_captcha_ts = now
        # Escalate stronger throttle: immediate storm mode & long cooloff.
        self._storm_active = True
        self._storm_retry_level = max(self._storm_retry_level, 2)
        base_cool = random.uniform(90, 150)
        # If many captchas recently, extend the cooloff window.
        if len(self._recent_captcha) >= self._captcha_storm_threshold:
            base_cool += random.uniform(120, 240)
        self._throttle_until = max(self._throttle_until, now + base_cool)
        # Backoff scheduling similar to 429 but heavier (longer delay).
        delay = base_cool * random.uniform(0.8, 1.3)
        ready_at = now + delay
        new_meta = dict(meta)
        new_meta['retry_captcha'] = int(new_meta.get('retry_captcha', 0)) + 1
        captcha_url = url
        try:
            if kind == "topic" and new_meta.get("topic_url"):
                captcha_url = self._topic_page_url(new_meta.get("topic_url"), new_meta.get("page_number", 1))
        except Exception:
            pass
        req = Request(captcha_url, meta=new_meta, callback=callback, dont_filter=True)
        self._delayed_seq += 1
        heapq.heappush(self._delayed, (ready_at, self._delayed_seq, req))
        self.logger.warning(f"[CAPTCHA] kind={kind} detected cooloff={base_cool:.1f}s retry_in={delay:.1f}s url={url}")
        self._log_event("captcha", kind=kind, url=url, delay=delay)
        # Adaptive: exponential slowdown based on consecutive captcha streak length.
        try:
            # Scale factor grows with streak (1,2,3,...) but capped.
            streak = min(self._captcha_consecutive, 6)
            # Base multiplier exponential 1.5^(streak) applied relative to original pace, but we only move towards that gradually.
            target_global = min(self._max_global_bucket_interval, self._min_global_bucket_interval * (1.5 ** streak))
            # Move 50% of the way from current to target to avoid jump discontinuity.
            self._global_bucket_interval = min(self._max_global_bucket_interval, self._global_bucket_interval + (target_global - self._global_bucket_interval) * 0.5)
            target_guide = min(6.0, 0.5 * (1.6 ** streak))  # separate cap for guide pacing
            self._guide_bucket_interval = min(6.0, self._guide_bucket_interval + (target_guide - self._guide_bucket_interval) * 0.6)
        except Exception:
            pass
    # Proxy rotation/escalation removed.

    # ---------------- Token bucket & latency adaptation -------------
    def _refill_tokens(self):
        """
        Refill the token bucket for non-guide (topic/discovery) request pacing.

        - Uses a leaky bucket algorithm: tokens accumulate at a fixed rate up to a max capacity.
        - Called before scheduling any non-guide request to ensure pacing.
        - Updates the last refill timestamp only if tokens are actually added.
        - Defensive: skips refill if no time has elapsed.
        """
        now = time.time()
        elapsed = now - self._last_refill_ts
        if elapsed <= 0:
            # No time has passed since last refill; skip.
            return
        add = elapsed * self._token_refill_rate
        if add > 0:
            # Add tokens, but do not exceed capacity.
            self._tokens = min(self._token_capacity, self._tokens + add)
            self._last_refill_ts = now

    def _consume_token_or_delay(self, req: Request) -> bool:
        """
        Consume a token for non-guide (topic/discovery) scheduling; if none are available, delay the request.

        - Refills the token bucket before attempting to consume.
        - If a token is available, consumes it and returns True (request should be scheduled now).
        - If not, computes the wait time until a token will be available and schedules the request for that time.
        - Returns False if the request was delayed.

        Args:
            req: The Scrapy Request to schedule or delay.
        Returns:
            True if the request should be scheduled now, False if it was delayed.
        """
        self._refill_tokens()
        if self._tokens >= 1:
            # Token available: consume and proceed.
            self._tokens -= 1
            return True
        # No token: compute wait time until next token is available.
        needed = 1 - self._tokens
        wait = needed / max(0.01, self._token_refill_rate)
        ready_at = time.time() + wait + random.uniform(0.05, 0.25)
        self._queue_delayed(req, ready_at)
        return False

    def _record_latency(self, value: float, kind: str):
        """
        Record a latency sample and adaptively adjust pacing based on recent performance.

        - Maintains a rolling window of recent latency samples (for both topic and guide requests).
        - If the average or 90th percentile latency is high, slows down refill and increases intervals.
        - If latency is low and not in storm mode, gently speeds up refill and reduces intervals.
        - Defensive: ignores any errors in statistics calculation.

        Args:
            value: The observed latency (seconds) for a request.
            kind: The type of request (e.g., 'topic', 'guide').
        """
        if value <= 0:
            # Ignore non-positive latencies.
            return
        self._latency_samples.append(value)
        if len(self._latency_samples) > self._latency_window:
            # Maintain a fixed-size rolling window.
            self._latency_samples.pop(0)
        if len(self._latency_samples) >= 5:
            try:
                self._latency_avg = statistics.mean(self._latency_samples)
                p90 = sorted(self._latency_samples)[int(0.9 * (len(self._latency_samples)-1))]
                # If slow, degrade pace; if fast and not in storm, speed up.
                if self._latency_avg > 3.0 or p90 > 5.0:
                    # Degrade pace: slow down refill and increase intervals.
                    self._token_refill_rate = max(0.4, self._token_refill_rate * 0.9)
                    self._global_bucket_interval = min(self._max_global_bucket_interval, self._global_bucket_interval * 1.1)
                elif self._latency_avg < 1.2 and not self._storm_active:
                    # Speed up: increase refill and reduce intervals.
                    self._token_refill_rate = min(3.0, self._token_refill_rate * 1.05)
                    self._global_bucket_interval = max(self._min_global_bucket_interval, self._global_bucket_interval * 0.95)
            except Exception:
                # Defensive: ignore any errors in statistics calculation.
                pass

    
    # -------------- Existing guides preload --------------
    def _preload_existing_guides(self):
        """
        Load canonical URLs of already-scraped guides from backup and data JSONL files for deduplication.

        - Scans all matching JSONL files in backups/ and data/ for lines containing a 'canonical_url' field.
        - Adds each canonical URL to the deduplication set (_existing_guides) if not already present.
        - Handles malformed lines and missing fields defensively.
        - Logs the number of loaded guides and files scanned for visibility.
        """
        root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        patterns = [
            os.path.join(root, 'backups', 'guides*.jsonl'),
            os.path.join(root, 'backups', 'guides_*.jsonl'),
            os.path.join(root, 'backups', 'guides-*.jsonl'),
            os.path.join(root, 'backups', 'guides_merged*.jsonl'),
        ]
        files = set()
        for pat in patterns:
            for f in glob.glob(pat):
                if os.path.isfile(f):
                    files.add(f)
        data_file = os.path.join(root, 'data', 'guides.jsonl')
        if os.path.exists(data_file):
            files.add(data_file)
        loaded = 0
        for fpath in files:
            try:
                with open(fpath, 'r', encoding='utf-8', errors='ignore') as fh:
                    for line in fh:
                        if 'canonical_url' not in line:
                            # Skip lines without canonical_url field.
                            continue
                        try:
                            obj = json.loads(line.strip())
                        except Exception:
                            # Skip malformed JSON lines.
                            continue
                        cu = obj.get('canonical_url')
                        if not cu:
                            continue
                        can = self._canonical_url(cu)
                        if can not in self._existing_guides:
                            self._existing_guides.add(can)
                            loaded += 1
            except Exception as e:
                # Log but do not fail on file read errors.
                self.logger.debug(f"[DEDUPE-PRELOAD] failed file={fpath} err={e}")
        self.logger.info(f"[DEDUPE-PRELOAD] existing_guides_loaded={loaded} files={len(files)}")

    # -------------- Page completion helpers --------------
    def _finalize_page(self, topic_slug: str):
        """
        Handle the end-of-page lifecycle: log summary, update rotation, and fill capacity if needed.

        - Logs a summary of the completed page and its guides.
        - Clears the current page's guide list and resets waiting state.
        - Removes the topic from the active set if present.
        - If the topic is not exhausted and session cap not reached, re-enqueues for next page.
        - If there is capacity, schedules the next topic request immediately.
        - Defensive: handles all edge cases and logs errors without failing.

        Args:
            topic_slug: The slug of the topic whose page was completed.
        """
        st = self._topics.get(topic_slug)
        if not st or st.exhausted or not st.waiting_requeue:
            return
        try:
            if st.current_page_guides:
                self.logger.info(
                    f"[PAGE-SUMMARY] topic={topic_slug} page={st.pages_seen} guides_count={len(st.current_page_guides)}/{st.expected_current_page_guides} guides={st.current_page_guides}"
                )
        except Exception:
            pass
        st.current_page_guides = []
        st.waiting_requeue = False
        if topic_slug in self._active_topics:
            self._active_topics.remove(topic_slug)
        # If not exhausted and session cap not reached, re-enqueue for next page.
        if (self.per_topic_pages == 0 or st.pages_seen < self.per_topic_pages) and not st.exhausted:
            if self.humanize:
                try:
                    pause = random.uniform(1.5, 4.0)
                    asyncio.create_task(asyncio.sleep(pause))
                    self.logger.debug(f"[IDLE] topic={topic_slug} post-page pause={pause:.2f}s")
                except Exception:
                    pass
            self._rotation.append(topic_slug)
            self.logger.debug(
                f"[ROTATE] slug={topic_slug} enqueue_next_page={st.next_page} queue_len={len(self._rotation)} active_topics={len(self._active_topics)}"
            )
        # If there is capacity, schedule the next topic request immediately.
        if len(self._active_topics) < self.topic_parallelism:
            next_req = self._next_topic_request()
            if next_req:
                self.crawler.engine.crawl(next_req)

    def _force_page_completion(self, topic_slug: str):
        """
        Force completion of a topic page if the watchdog detects it is stuck (e.g., guides not finishing).

        - Sets inflight_guides to 0 to trigger the normal finalize pathway.
        - Defensive: does nothing if topic is missing or already exhausted.

        Args:
            topic_slug: The slug of the topic to force completion for.
        """
        st = self._topics.get(topic_slug)
        if not st or st.exhausted:
            return
        # Reset inflight to 0 to trigger normal finalize pathway.
        st.inflight_guides = 0
        self._finalize_page(topic_slug)

    # Expose imported extractor as a method for backward compatibility.
    # The real implementation lives in `scraper.parse_helpers._extract_guide_urls`.
    _extract_guide_urls = staticmethod(_extract_guide_urls)


