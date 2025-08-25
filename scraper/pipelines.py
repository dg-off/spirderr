import os
import orjson
from pathlib import Path
from typing import Any

from scrapy import Item

OUTPUT_PATH = Path(os.getenv("OUTPUT_JSONL", "data/guides.jsonl"))
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

class JSONLinesPipeline:
    def open_spider(self, spider):
        self.f = OUTPUT_PATH.open("ab")

    def close_spider(self, spider):
        if hasattr(self, "f"):
            self.f.close()

    def process_item(self, item: Item, spider) -> Any:
        # basic validation: ensure minimal fields exist
        if not item.get("canonical_url") or not item.get("title"):
            try:
                spider.logger.warning(f"[PIPELINE-SKIP] missing fields canonical={item.get('canonical_url')} title_present={bool(item.get('title'))}")
                if spider.crawler and getattr(spider.crawler, 'stats', None):
                    spider.crawler.stats.inc_value('pipeline/dropped_missing_fields', 1)
            except Exception:
                pass
            return item  # skip writing
        line = orjson.dumps(item, option=orjson.OPT_APPEND_NEWLINE)
        self.f.write(line)
        try:
            self.f.flush()
        except Exception:
            pass
        return item
