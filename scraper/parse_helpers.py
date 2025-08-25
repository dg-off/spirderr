from __future__ import annotations

from bs4 import BeautifulSoup
from urllib.parse import urljoin
import hashlib
from datetime import datetime
from typing import Dict, Any, List, Optional
import re
from math import ceil

from trafilatura import extract as trafilatura_extract


def parse_guide(html: str) -> Dict[str, Any]:
    """Parse guide HTML into structured dict with enrichment (Tier 1-2).

    Added per-section: word_count, content_hash, heading_slug
    Added stats: avg_section_word_count, max_section_word_count, authors_count
    """
    soup = BeautifulSoup(html, "lxml")

    # Title
    title_el = soup.find("h1")
    title = title_el.get_text(strip=True) if title_el else None

    # Published date (updated date currently not exposed separately)
    published_date = None
    time_el = soup.find("time")
    if time_el and time_el.get_text(strip=True):
        published_date = _extract_date(time_el.get_text(" ", strip=True))
    if not published_date:
        header_details = soup.find(class_=re.compile(r"header-details", re.I))
        if header_details:
            published_date = _extract_date(header_details.get_text(" ", strip=True))
    updated_date = None

    # Topics (filter UI toggles like 'Show 2 moreShow 2 less')
    topics: List[Dict[str, Any]] = []
    topic_container = soup.find(id="filed-under") or soup.find(class_=re.compile(r"content-topic-tags"))
    primary_topic = None
    if topic_container:
        seen_topics = set()
        raw_links = topic_container.select("a")
        order = 0
        for a in raw_links:
            name = a.get_text(" ", strip=True)
            if not name:
                continue
            if re.search(r"^Show \d+ more|Show \d+ less", name, re.I):
                continue
            if "Show" in name and "more" in name:
                continue
            if name in seen_topics:
                continue
            seen_topics.add(name)
            order += 1
            is_primary = (order == 1)
            if is_primary:
                primary_topic = name
            topics.append({"name": name, "is_primary": is_primary})

    # Authors
    authors: List[Dict[str, Any]] = []
    author_blocks = []
    for sel in ["#guide-author", "div.header-details", "div[data-gtm-subcontext='author data']"]:
        blk = soup.select_one(sel)
        if blk:
            author_blocks.append(blk)
    if not author_blocks:
        wb = soup.find(string=re.compile(r"Written by", re.I))
        if wb:
            author_blocks.append(wb.parent)

    def _extract_author(block) -> Optional[Dict[str, Any]]:
        if not block:
            return None
        text_full = block.get_text(" ", strip=True)
        # normalize
        norm = re.sub(r"\s+", " ", text_full)
        # remove review counts/noise
        norm = re.sub(r"\b\d+ reviews?\b", "", norm, flags=re.I)
        norm = norm.replace("  ", " ")
        links = block.find_all("a", href=re.compile(r"/attorneys/"))
        link = None
        for lk in links:
            t = lk.get_text(strip=True)
            if t and "Attorney" not in t:  # prefer name link
                link = lk
                break
        if not link and links:
            link = links[0]
        if not link:
            return None
        name = link.get_text(strip=True)
        profile_url = link.get("href", "")
        m = re.search(r"-(\d+)(?:\.html)?$", profile_url)
        avvo_author_id = int(m.group(1)) if m else None
        practice_area = city = state = None
        # Pattern: 'Real Estate Attorney in Coral Gables, FL'
        pa_match = re.search(r"([A-Za-z/&' -]+ Attorney) in ([A-Za-z .'-]+),\s*([A-Z]{2})", norm)
        if pa_match:
            practice_area, city, state = pa_match.group(1), pa_match.group(2), pa_match.group(3)
        else:
            # fallback: just grab first 'X Attorney' phrase
            pa_only = re.search(r"([A-Za-z/&' -]+ Attorney)\b", norm)
            if pa_only:
                practice_area = pa_only.group(1)
            # Cleanup practice_area: remove 'About the author' prefix and embedded name
            if practice_area:
                pa_clean = practice_area
                pa_clean = re.sub(r"^About the author\s+", "", pa_clean, flags=re.I)
                # Remove author name if it appears inside the practice area string
                # e.g. 'About the author John Q Smith Immigration Attorney' -> 'Immigration Attorney'
                name_escaped = re.escape(name)
                pa_clean = re.sub(rf"\b{name_escaped}\b", "", pa_clean)
                # Collapse multiple spaces and stray punctuation
                pa_clean = re.sub(r"\s+/\s+", "/", pa_clean)
                pa_clean = re.sub(r"\s+", " ", pa_clean).strip()
                if not pa_clean or pa_clean.lower() == "attorney":
                    pa_clean = None
                practice_area = pa_clean
        phone = None
        phone_match = re.search(r"\(\d{3}\)\s*\d{3}[- ]?\d{4}", norm)
        if phone_match:
            phone = phone_match.group(0)
        return {
            "name": name,
            "avvo_author_id": avvo_author_id,
            "practice_area": practice_area,
            "city": city,
            "state": state,
            "contact_phone": phone,
            "profile_url": profile_url,
        }

    seen_auth = set()
    for blk in author_blocks:
        a = _extract_author(blk)
        if a:
            key = (a.get("avvo_author_id"), a.get("name"))
            if key not in seen_auth:
                seen_auth.add(key)
                authors.append(a)

    # Derive location components from first author (fallbacks)
    location_city = None
    location_state = None
    if authors:
        first = authors[0]
        if first.get("city"):
            location_city = first.get("city")
        if first.get("state"):
            location_state = first.get("state")

    # Sections
    main = None
    for sel in [
        "div.content-main",
        "div.gtm-subcontext[data-gtm-subcontext='legal guide content']",
        "article",
        "main",
        "div.card[data-gtm-context='legal guide card']",
    ]:
        cand = soup.select_one(sel)
        if cand:
            main = cand
            break
    if not main:
        fb = soup.find("div", class_=re.compile(r"content-main|legal-guide", re.I))
        if fb and "dropdown-content" not in " ".join(fb.get("class", [])):
            main = fb

    sections: List[Dict[str, Any]] = []
    plain_parts: List[str] = []
    if main:
        li_blocks = main.select("ol li")
        li_with_h2 = [li for li in li_blocks if li.find("h2")]
        if li_with_h2:
            first_ol = li_with_h2[0].find_parent("ol")
            if first_ol:
                collected = []
                sib = first_ol.previous_sibling
                while sib:
                    if getattr(sib, 'get_text', None):
                        t = sib.get_text(" ", strip=True)
                        if t:
                            collected.append(sib)
                    sib = sib.previous_sibling
                collected.reverse()
                intro_nodes = [n for n in collected if getattr(n, 'name', None) not in {"script", "style", "noscript", "ol"}]
                if intro_nodes:
                    intro_html = "".join(str(n) for n in intro_nodes)
                    intro_soup = BeautifulSoup(intro_html, "lxml")
                    for s in intro_soup.find_all(["script", "style", "noscript"]):
                        s.decompose()
                    intro_text = intro_soup.get_text("\n", strip=True)
                    if len(intro_text) >= 40:
                        plain_parts.append(intro_text)
                        sections.append({
                            "order_index": 1,
                            "heading": None,
                            "html": str(intro_soup),
                            "text": intro_text,
                        })
            base_index = len(sections)
            for idx, li in enumerate(li_with_h2, start=1):
                h = li.find("h2")
                heading = h.get_text(" ", strip=True) if h else None
                for s in li.find_all(["script", "style", "noscript"]):
                    s.decompose()
                html_fragment = str(li)
                text_fragment = BeautifulSoup(html_fragment, "lxml").get_text("\n", strip=True)
                plain_parts.append(text_fragment)
                sections.append({
                    "order_index": base_index + idx,
                    "heading": heading,
                    "html": html_fragment,
                    "text": text_fragment,
                })
        else:
            h2s = main.find_all("h2")
            if h2s:
                for idx, h2 in enumerate(h2s, start=1):
                    nodes = [h2]
                    sib = h2.find_next_sibling()
                    while sib and sib.name != "h2":
                        nodes.append(sib)
                        sib = sib.find_next_sibling()
                    html_fragment = "".join(str(n) for n in nodes)
                    frag_soup = BeautifulSoup(html_fragment, "lxml")
                    for s in frag_soup.find_all(["script", "style", "noscript"]):
                        s.decompose()
                    heading = h2.get_text(" ", strip=True)
                    text_fragment = frag_soup.get_text("\n", strip=True)
                    plain_parts.append(text_fragment)
                    sections.append({
                        "order_index": idx,
                        "heading": heading,
                        "html": str(frag_soup),
                        "text": text_fragment,
                    })
            else:
                frag_soup = BeautifulSoup(str(main), "lxml")
                for s in frag_soup.find_all(["script", "style", "noscript"]):
                    s.decompose()
                text_fragment = frag_soup.get_text("\n", strip=True)
                plain_parts.append(text_fragment)
                sections.append({
                    "order_index": 1,
                    "heading": title,
                    "html": str(frag_soup),
                    "text": text_fragment,
                })

    # Combined text for hashing + stats
    combined_text = "\n\n".join(plain_parts).strip()
    if not combined_text:
        extracted = trafilatura_extract(html, include_links=False, include_images=False) or ""
        if extracted:
            combined_text = extracted
            if not sections:
                sections.append({
                    "order_index": 1,
                    "heading": title,
                    "html": "",
                    "text": extracted,
                })

    # Enrich sections
    for sec in sections:
        txt = sec.get("text") or ""
        sec["word_count"] = len(txt.split())
        sec["content_hash"] = hashlib.sha256(txt.encode("utf-8")).hexdigest() if txt else None
        heading = sec.get("heading")
        sec["heading_slug"] = _slugify(heading) if heading else "intro"

    total_words = len(combined_text.split())
    section_word_counts = [s.get("word_count", 0) for s in sections]
    stats = {
        "word_count": total_words,
        "section_count": len(sections),
        "reading_time_minutes": ceil(total_words / 225) if total_words else 0,
        "avg_section_word_count": int(round(sum(section_word_counts) / len(section_word_counts))) if section_word_counts else 0,
        "max_section_word_count": max(section_word_counts) if section_word_counts else 0,
        "authors_count": len(authors),
    }

    # Additional resources
    additional_resources: List[Dict[str, Any]] = []
    resources_block = soup.find(id="additional-resources") or soup.find(string=re.compile(r"Additional resources", re.I))
    if resources_block and getattr(resources_block, "parent", None):
        parent_block = resources_block if isinstance(resources_block, BeautifulSoup) else resources_block.parent
        links = parent_block.find_all("a", href=True)
        order = 1
        seen_urls = set()
        for a in links:
            href = a.get("href")
            if not href or href in seen_urls:
                continue
            seen_urls.add(href)
            txt = a.get_text(" ", strip=True)
            if not txt:
                continue
            additional_resources.append({
                "order_index": order,
                "anchor_text": txt,
                "url": href,
                "rel": a.get("rel", [None])[0] if a.get("rel") else None,
            })
            order += 1

    content_hash = hashlib.sha256(combined_text.encode("utf-8")).hexdigest() if combined_text else None

    return {
        "title": title,
        "published_date": published_date,
        "updated_date": updated_date,
        "topics": topics,
    "primary_topic": primary_topic,
    "location_city": location_city,
    "location_state": location_state,
        "authors": authors,
        "sections": sections,
        "additional_resources": additional_resources,
        "stats": stats,
        "content_hash": content_hash,
    }


def _extract_date(s: str):
    s_clean = s.strip()
    m = re.search(r"(\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},\s+20\d{2})", s_clean, re.I)
    if m:
        try:
            return datetime.strptime(m.group(1), "%b %d, %Y").date().isoformat()
        except ValueError:
            pass
    m2 = re.search(r"(20\d{2}-\d{2}-\d{2})", s_clean)
    if m2:
        return m2.group(1)
    return None


def _slugify(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    slug = s.lower()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug or None


def _extract_guide_urls(soup: BeautifulSoup, base_url: str, state: Optional[object] = None) -> tuple[List[str], int, int]:
    """
    Extract all non-video guide absolute URLs from a topic listing soup.

    - Selects all anchors with class 'legal-guide-card' and href containing '/legal-guides/'.
    - If none found, falls back to any anchor with '/legal-guides/' in href.
    - Skips video guides by checking for video icon or label in the card.
    - Applies duplicate filtering if a state object is provided (session_guides).
    - Returns a tuple: (list of guide URLs, total cards found, number of video guides skipped).

    Args:
        soup: BeautifulSoup-parsed HTML of the topic page.
        base_url: The base URL for resolving relative links.
        state: Optional TopicState-like object for session deduplication (must expose .session_guides set).
    Returns:
        (urls, raw_total_cards, video_skipped_count)
    """
    anchors = soup.select("a.legal-guide-card[href*='/legal-guides/']")
    if not anchors:
        # Fallback: any anchor with '/legal-guides/' in href.
        anchors = [a for a in soup.find_all("a", href=True) if "/legal-guides/" in a["href"]]
    raw_total = len(anchors)
    urls: List[str] = []
    video_skipped = 0
    for a in anchors:
        href = a.get("href")
        if not href:
            continue
        abs_url = urljoin(base_url, href.split("?#")[0])
        skip = False
        try:
            # Check for video icon or label in the card to skip video guides.
            parent = a if "legal-guide-card" in a.get("class", []) else a.find_parent(class_="legal-guide-card")
            if parent and (parent.find(class_="icon-play-video") or "Video Legal Guide" in parent.get_text(" ", strip=True)):
                skip = True
        except Exception:
            pass
        if skip:
            video_skipped += 1
            continue
        if state is not None:
            # Deduplicate: skip if already seen in this session.
            try:
                sess = getattr(state, 'session_guides', None)
                if sess is not None:
                    if abs_url in sess:
                        continue
                    sess.add(abs_url)
            except Exception:
                # Defensive: if state isn't shaped as expected, just proceed
                pass
        urls.append(abs_url)
    return urls, raw_total, video_skipped
