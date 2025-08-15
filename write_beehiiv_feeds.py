#!/usr/bin/env python3
"""
Morning Briefing — Writer
Reads sqlite DB and emits Beehiiv-ready RSS feeds:
- beehiiv_top.xml
- beehiiv_property.xml
- beehiiv_finance.xml
- beehiiv_startups.xml
- beehiiv_markets.xml
- beehiiv_macro.xml   (never empty; safe placeholders)
"""

import os, sqlite3, logging, html
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
from email.utils import format_datetime
from email.utils import parsedate_to_datetime

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s - %(levelname)s - %(message)s")

DB_PATH           = os.getenv("RSS_DB_PATH", "rss_items.db")
OUT_PREFIX        = os.getenv("OUT_PREFIX", "beehiiv")
NEWSLETTER_NAME   = os.getenv("NEWSLETTER_NAME", "Morning Briefing")
BASE_URL          = os.getenv("BASE_URL", "https://example.com")
SCAN_WINDOW_HRS   = int(os.getenv("SCAN_WINDOW_HRS", "36"))
MAX_ITEMS_PER_FEED= int(os.getenv("MAX_ITEMS_PER_FEED", "80"))
REQUIRE_AI        = os.getenv("REQUIRE_AI", "1") == "1"  # drop items without ai_title/ai_desc

def parse_iso_or_rfc(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    # Try ISO first
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        pass
    # Try RFC
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def read_recent(conn: sqlite3.Connection) -> List[Dict]:
    since = datetime.now(timezone.utc) - timedelta(hours=SCAN_WINDOW_HRS)
    rows = conn.execute("""
        SELECT link, link_canonical, source_name, source_feed, title, raw_summary, published_at,
               category, interest_score, ai_title, ai_desc, ai_summary, ai_package
          FROM items
         WHERE COALESCE(published_at,'') <> ''
         ORDER BY datetime(published_at) DESC
    """).fetchall()

    items: List[Dict] = []
    for r in rows:
        d = {
            "link": r[0], "link_canonical": r[1], "source_name": r[2], "source_feed": r[3],
            "title": r[4] or "", "raw_summary": r[5] or "", "published_at": r[6] or "",
            "category": (r[7] or "").lower() or "finance", "interest_score": int(r[8] or 0),
            "ai_title": r[9] or "", "ai_desc": r[10] or "", "ai_summary": r[11] or "", "ai_package": r[12] or "{}",
        }
        pub_dt = parse_iso_or_rfc(d["published_at"])
        if not pub_dt:  # skip if no date
            continue
        # window
        if pub_dt < since:
            continue
        # AI requirement
        if REQUIRE_AI and (not d["ai_title"].strip() or not d["ai_desc"].strip()):
            continue
        # Final content (AI-preferred)
        d["final_title"] = (d["ai_title"] or d["title"]).strip()
        # Use AI desc; if missing, fall back to a very short version of raw summary
        if d["ai_desc"].strip():
            body = d["ai_desc"].strip()
        else:
            body = d["raw_summary"].strip()[:500]
        # normalise to <p>…</p>
        if not body.startswith("<"):
            body = f"<p>{html.escape(body)}</p>"
        d["final_body_html"] = body
        d["pub_dt"] = pub_dt
        items.append(d)
    return items

def escape_text(s: str) -> str:
    # Escape XML special chars for text nodes (title, link, category)
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def cdata(s: str) -> str:
    if s is None:
        s = ""
    # Guard if body contains ']]>'
    s = s.replace("]]>", "]]]]><![CDATA[>")
    return f"<![CDATA[ {s} ]]>"

def write_rss(filename: str, title_suffix: str, items: List[Dict]):
    path = f"{OUT_PREFIX}_{filename}.xml"
    channel_title = f"{NEWSLETTER_NAME} · {title_suffix}" if title_suffix else NEWSLETTER_NAME
    build_date = format_datetime(datetime.now(timezone.utc))

    # Build XML by hand to control CDATA and escaping
    parts = []
    parts.append('<?xml version="1.0" encoding="UTF-8"?>')
    parts.append('<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">')
    parts.append("<channel>")
    parts.append(f"<title>{escape_text(channel_title)}</title>")
    parts.append(f"<link>{escape_text(BASE_URL)}</link>")
    parts.append("<description>Daily AU commercial property briefing — assets, funds, deals, development.</description>")
    parts.append("<language>en-au</language>")
    parts.append(f"<lastBuildDate>{build_date}</lastBuildDate>")
    parts.append("<ttl>60</ttl>")

    count = 0
    for it in items[:MAX_ITEMS_PER_FEED]:
        if not it["final_title"] or not it["final_body_html"]:
            continue
        t = escape_text(it["final_title"])
        link = escape_text(it["link"])
        guid = escape_text(it["link_canonical"] or it["link"])
        pub = format_datetime(it["pub_dt"])
        body_html = it["final_body_html"]
        # Basic double-check: remove stray bare ampersands inside HTML that might not be CDATA-escaped
        # (we wrap in CDATA below, so this is a belt-and-braces)
        body_html = body_html.replace("&nbsp;", " ").replace("& ", "&amp; ")

        parts.append("<item>")
        parts.append(f"<title>{t}</title>")
        parts.append(f"<link>{link}</link>")
        parts.append(f'<guid isPermaLink="false">{guid}</guid>')
        parts.append(f"<pubDate>{pub}</pubDate>")
        # description and content:encoded both set to the AI body
        parts.append("<description>")
        parts.append(cdata(body_html))
        parts.append("</description>")
        parts.append("<content:encoded>")
        parts.append(cdata(body_html))
        parts.append("</content:encoded>")
        # categories
        if it.get("category"):
            parts.append(f"<category>{escape_text(it['category'])}</category>")
        parts.append("</item>")
        count += 1

    parts.append("</channel>")
    parts.append("</rss>")

    xml = "\n".join(parts)
    with open(path, "w", encoding="utf-8") as f:
        f.write(xml)
    logging.info("Wrote %s (items=%d)", path, count)

def pick(items: List[Dict], predicate) -> List[Dict]:
    return [it for it in items if predicate(it)]

def main():
    conn = sqlite3.connect(DB_PATH)
    items = read_recent(conn)
    logging.info("Pulled %d items (after window/AI filtering)", len(items))

    # Sort: interest_score desc, then recency
    items_sorted = sorted(items, key=lambda x: (x.get("interest_score", 0), x["pub_dt"]), reverse=True)

    # Build category-specific lists
    prop_items     = [it for it in items_sorted if it["category"] == "property"]
    finance_items  = [it for it in items_sorted if it["category"] == "finance"]
    startups_items = [it for it in items_sorted if it["category"] == "startups"]
    markets_items  = [it for it in items_sorted if it["category"] == "markets"]

    # Top feed = top N across all
    write_rss("top", "Top", items_sorted)
    write_rss("property", "Property", prop_items)
    write_rss("finance", "Finance", finance_items)
    write_rss("startups", "Startups & Fintech", startups_items)
    write_rss("markets", "Global & Macro", markets_items)

    # Macro-in-a-minute: always emit at least one placeholder item so Beehiiv never chokes
    macro_body = (
        "<ul>"
        "<li>ASX200: —</li>"
        "<li>AUDUSD: —</li>"
        "<li>AU 10-yr: —</li>"
        "<li>RBA cash rate: —</li>"
        "<li>Next move probability: —</li>"
        "</ul>"
        "<p><em>One-liner:</em> Risk-on morning; lenders bid; dev feasos ease a touch.</p>"
    )
    macro_items = [{
        "final_title": "Macro in a Minute",
        "final_body_html": macro_body,
        "pub_dt": datetime.now(timezone.utc),
        "link": BASE_URL,
        "link_canonical": BASE_URL,
        "category": "macro"
    }]
    write_rss("macro", "Macro in a Minute", macro_items)

if __name__ == "__main__":
    main()
