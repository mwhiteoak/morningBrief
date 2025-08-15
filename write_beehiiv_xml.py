#!/usr/bin/env python3
import os, sqlite3, logging
from datetime import datetime, timedelta
from xml.sax.saxutils import escape

DB_PATH   = os.getenv("RSS_DB_PATH", "rss_items.db")
OUT_PATH  = os.getenv("OUT_PATH", "beehiiv.xml")
SITE_LINK = os.getenv("BASE_URL", "https://example.github.io/repo")
TITLE     = os.getenv("NEWSLETTER_NAME", "Morning Briefing")
LANG      = "en-au"
WINDOW_HR = int(os.getenv("SCAN_WINDOW_HRS", "36"))

# Sort categories in a CRE-friendly order
CAT_ORDER = [
    "deals_capital","development_planning","reits","leasing",
    "industrial","office","retail","residential","proptech_finance","macro"
]
CAT_INDEX = {c:i for i,c in enumerate(CAT_ORDER)}

logging.basicConfig(level=logging.INFO, format="%(message)s")

def fetch_items(conn):
    since = (datetime.utcnow() + timedelta(hours=10)) - timedelta(hours=WINDOW_HR)
    cur = conn.execute("""
        SELECT url, source, title, published_at, ai_title, ai_desc_html, category_main, ai_tags
        FROM items
        WHERE ai_title IS NOT NULL
          AND ai_desc_html IS NOT NULL
          AND LENGTH(TRIM(ai_desc_html)) > 60
          AND datetime(published_at) >= datetime(?)
    """, (since.isoformat(),))
    rows = [dict(r) for r in cur.fetchall()]
    # Fallback category
    for r in rows:
        r["category_main"] = r.get("category_main") or "macro"
    # Sort: category bucket, then newest first
    rows.sort(key=lambda r: (CAT_INDEX.get(r["category_main"], 999), r["published_at"]), reverse=True)
    return rows

def iso_to_rfc2822(iso_s: str) -> str:
    try:
        dt = datetime.fromisoformat(iso_s)
    except Exception:
        dt = datetime.utcnow()
    # naive AU time already baked into stored iso; render as +1000
    return dt.strftime("%a, %d %b %Y %H:%M:%S +1000")

def build_xml(items):
    now = datetime.utcnow() + timedelta(hours=10)
    head = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">',
        "<channel>",
        f"<title>{escape(TITLE)}</title>",
        f"<link>{escape(SITE_LINK)}</link>",
        "<description>Daily AU commercial property briefing — asset, fund, and acquisitions focused.</description>",
        f"<language>{LANG}</language>",
        f"<lastBuildDate>{iso_to_rfc2822(now.isoformat())}</lastBuildDate>",
        "<ttl>60</ttl>",
    ]
    body = []
    for it in items:
        ai_title = it["ai_title"].strip()
        ai_desc  = it["ai_desc_html"].strip()
        if not ai_title or not ai_desc:  # double-guard
            continue
        body.extend([
            "<item>",
            f"<title>{escape(ai_title)}</title>",
            f"<link>{escape(it['url'])}</link>",
            f"<guid isPermaLink=\"false\">{escape(it['url'])}</guid>",
            f"<pubDate>{iso_to_rfc2822(it['published_at'])}</pubDate>",
            "<description><![CDATA[ " + ai_desc + " ]]></description>",
            "<content:encoded><![CDATA[ " + ai_desc + " ]]></content:encoded>",
            f"<category>{escape(it['source'])}</category>",
            f"<category>{escape(it['category_main'])}</category>",
            "</item>"
        ])
    tail = ["</channel>", "</rss>"]
    return "\n".join(head + body + tail)

def main():
    if not os.path.exists(DB_PATH):
        logging.info("No DB found, writing empty shell RSS.")
        open(OUT_PATH, "w").write(build_xml([]))
        return
    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row
    items = fetch_items(conn)
    xml = build_xml(items)
    open(OUT_PATH, "w", encoding="utf-8").write(xml)
    logging.info("Wrote RSS to %s (items=%d)", OUT_PATH, len(items))

if __name__ == "__main__":
    main()
