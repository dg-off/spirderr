## Thought process behind the approach:
I wanted both breadth and depth, so I started by poking around the all_topics page and the topic/guide pages to find patterns. I noticed every topic URL is listed and that ?guide=1 opens the Guides tab while page=N jumps pages, so I could build direct links to topic pages instead of clicking through the UI. My first plan was to scrape one page per topic in a round-robin, but with ~20 guides/page and 1k+ topics that would take ages. To make it practical I implemented a rotation scheduler and added flags to control how many topics to visit, how many pages per topic, and how many guides per page, so you can run fast samples, deep crawls, or long resumable jobs. The full flag list is below.

After getting the basic rotation working the next big problem I ran into was rate limits. The site started returning 429s and occasional CAPTCHAs, which would quickly stall a naive crawler. To deal with that I added several mitigations: optional human-like behaviour using Playwright (random mouse moves, small pauses, scroll jitter), configurable scheduling jitter and per-request delays, an adaptive token-bucket throttler that paces topic and global requests, and an exponential backoff / delayed-request queue (for guides) that pauses and gradually retries when we hit storms of 429s. I also added simple CAPTCHA detection and escalation logic. All of these behaviors are tunable via the spider flags so you can trade speed for reliability depending on the run.

I also experimented with a lower-profile HTML-only approach (no Playwright) to reduce resource usage and the footprint of requests. The plan was to rely on the server-rendered HTML where possible and only use Playwright for pages that looked incomplete. In practice many pages returned minimal or template-like content and critical guide sections were missing without JS, which made parsing brittle and error-prone. I began a conservative fallback flow but couldn't make it reliably accurate across the site, so the spider defaults to Playwright rendering for now while keeping the hooks needed to try an HTML-only mode later.

## Data

Example of the data that I scraped can be found under example.json

The File with all of the guide can be found under avvo-legal-guides-scraper/backups/all_of_the_guides.jsonl

---
## High‑Level Crawl Flow

1. Discovery: Load the "all topics" page, optionally scroll to trigger lazy loads, extract topic anchors, and (if below an expected minimum) merge in a JSON seed list.
2. Initialization: For every topic insert/upsert DB row; load last visited page & completion flags to compute the next page (`last_page_number + 1`).
3. Rotation Queue: Shuffle topic slugs (stable if `shuffle_seed` provided) and push them into a deque.
4. Round‑Robin Pagination:
	- Pop next active topic; request its next listing page with Playwright.
	- Parse guide (card) URLs, filter out video guides, de‑duplicate per session.
	- Schedule guide detail requests (one batch per listing page) and WAIT (do not request the next page for that topic until all those guide details finish).
	- When the batch finishes, requeue the topic with an incremented `next_page` unless exhaustion heuristics say to stop.
5. Guide Parsing: Extract structured fields + sections + computed combined text; version via checksum in DB; emit JSONL items.
6. Continuation: The spider keeps rotating among topics, gradually advancing page numbers across all topics in a fair interleaved fashion.
7. Termination: When every topic is marked exhausted (or optional limits reached) and no delayed / guide requests remain, the spider allows Scrapy to close.

## Setup

Quick steps to prepare a development environment (tested on macOS / zsh):

```bash
# cd into avvo-legal...
cd avvo-legal-guides-scraper
# create and activate a virtualenv
python3 -m venv .venv
source .venv/bin/activate

# upgrade pip and install Python dependencies from the repo
pip install --upgrade pip
pip install -r requirements.txt

# Install Playwright browsers (required for Playwright rendering)
python -m playwright install
```

## Basic run

From avvo-legal-guides-scraper run the following 

```bash
HEADLESS=1 scrapy crawl avvo_guides \   
  -a topic_parallelism=2 \
  -a humanize=1 \
  -a jitter_page_min=1.2 \
  -a jitter_page_max=5.7 \
  -a guide_think_min=0.9 \
  -a guide_think_max=5.7 \
  -a schedule_guide_jitter_min=0.75 \
  -a schedule_guide_jitter_max=4.80 \
  -a max_guides_per_page=3 \                                    
```


## Usage

Argument reference (name — type — default — notes):

- `all_topics_url` — string — `ALL_TOPICS_URL` (project default)
- `max_topics` — int — `0` (0 = unlimited)
- `per_topic_pages` — int — `0` (0 = unlimited)
- `max_guides_per_page` — int — `0` (0 = unlimited)
- `topic_parallelism` — int — `2` (concurrent topic listing pages)
- `shuffle_seed` — int or None — `None` (provide int for deterministic shuffle)
- `skip_existing` — int (0 or 1) — `1` (1 = skip guides already in backups/data)
- `seed_file` — string — `config/topics_seed.json` (fallback topics file)
- `use_seed_fallback` — int (0 or 1) — `1` (merge seed file if discovered topics < min_topics_expected)
- `min_topics_expected` — int — `500` (minimum topics before seed fallback triggers)
- `scroll_discovery` — int (0 or 1) — `1` (use Playwright scroll to discover lazy-loaded topics)
- `humanize` — int (0 or 1) — `0` (enable simulated human delays/mouse moves)
- `jitter_page_min` — float — `0.0` (min random delay between topic pages)
- `jitter_page_max` — float — `0.0` (max random delay between topic pages)
- `guide_think_min` — float — `0.0` (min random delay before guide detail fetch)
- `guide_think_max` — float — `0.0` (max random delay before guide detail fetch)
- `schedule_guide_jitter_min` — float — `0.05` (min jitter when scheduling guide requests; default set if not provided)
- `schedule_guide_jitter_max` — float — `0.25` (max jitter when scheduling guide requests)
- `mouse_moves_min` — int — `1` (min mouse moves per page when humanize enabled)
- `mouse_moves_max` — int — `3` (max mouse moves per page when humanize enabled)
- `mouse_scribble_steps_min` — int — `4` (min steps per mouse scribble)
- `mouse_scribble_steps_max` — int — `9` (max steps per mouse scribble)
- `scroll_jitter` — int (0 or 1) — `1` (randomize scroll depth/rounds)


