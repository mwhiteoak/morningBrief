#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Write multiple Beehiiv-ready RSS feeds from sqlite.

ENV:
  RSS_DB_PATH=rss_items.db
  OUT_PREFIX=beehiiv
  NEWSLETTER_NAME=Morning Briefing
  BASE_URL=https://mwhiteoak.github.io/morningBrief
  SCAN_WINDOW_HRS=36
  MAX_ITEMS_PER_FEED=80
  MARK_EXPORTED=1  (mark items as exported_to_rss=1 so they won't repeat)
"""

from __future__ import annotations
import os, re, sqlite3
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple
from xml.sax.saxutils import escape

BAD_XML_CHARS_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]")

DB_PATH = os.getenv("RSS_DB_PATH", "rss_items.db")
OUT_PREFIX = os.getenv("OUT_PREFIX", "beehiiv")
NEWSLETTER_NAME = os.getenv("NEWSLETTER_NAME", "Morning Briefing")
BASE_URL = os.getenv("BASE_URL", "")
SCAN_WINDOW_HRS = int(os.getenv("SCAN_WINDOW_HRS", "36"))
MAX_ITEMS_PER_FEED = int(os.getenv("MAX_ITEMS_PER_FEED", "80"))
MARK_EXPORTED = os.getenv("MARK_EXPORTED", "1") == "1"

# category -> feed file suffix
FEED_MAP = {
    "top": "top",
    "property": "property",
    "deals": "property",
    "development": "property",
    "reit": "reit",
    "finance": "finance",
    "markets": "markets",
    "policy": "policy",
    "startups": "startups",
    "tech": "startups",
    "global": "top",
    "other": "top",
}

CHANNEL_DESC = "Daily AU commercial property briefing — assets, funds, deals, development."

def sanitize_cdata(s: str) -> str:
    if not s:
        return ""
    s = BAD_XML_CHARS_RE.sub("", s)
    # protect CDATA terminator
    s = s.replace("]]>", "]]]]><![CDATA[>")
    return s

def parse_iso_utc(s: str) -> datetime:
    # Always return naive UTC for comparisons/formatting
    if not s:
        return datetime.utcnow()
    try:
        # Handle both ...Z and ...+00:00
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return datetime.utcnow()

def fmt_rfc822(dt: datetime) -> str:
    # assume dt naive UTC
    return dt.strftime("%a, %d %b %Y %H:%M:%S +0000")

def read_recent(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    since = datetime.utcnow() - timedelta(hours=SCAN_WINDOW_HRS)
    cur = conn.execute("""
        SELECT id, url, url_canonical, source_name, title, description,
               ai_title, ai_summary, category, score, published_at, exported_to_rss
        FROM items
        WHERE (exported_to_rss=0) AND datetime(COALESCE(published_at, processed_at)) >= datetime(?)
        ORDER BY score DESC, datetime(COALESCE(published_at, processed_at)) DESC
    """, (since.replace(microsecond=0).isoformat(),))
    cols = [c[0] for c in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return rows

def filter_valid(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for it in items:
        # choose AI fields, fallback to originals
        title = (it.get("ai_title") or it.get("title") or "").strip()
        summary = (it.get("ai_summary") or it.get("description") or "").strip()
        if not summary or summary.lower().startswith("insufficient data"):
            continue
        # tighten length (aim ~60 words)
        words = summary.split()
        if len(words) > 70:
            summary = " ".join(words[:70])
        it["_title"] = title
        it["_summary"] = summary
        it["_pub_dt"] = parse_iso_utc(it.get("published_at") or "")
        out.append(it)
    return out

def group_by_feed(items: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    buckets: Dict[str, List[Dict[str, Any]]] = {s: [] for s in set(FEED_MAP.values())}
    for it in items:
        cat = (it.get("category") or "other").lower()
        feed = FEED_MAP.get(cat, "top")
        buckets.setdefault(feed, []).append(it)
    # also build a "top" that mixes the best (score + recency already sorted)
    buckets["top"] = items[:MAX_ITEMS_PER_FEED]
    # cap each bucket
    for k in list(buckets.keys()):
        buckets[k] = buckets[k][:MAX_ITEMS_PER_FEED]
    return buckets

def write_feed(feed_name: str, items: List[Dict[str, Any]]) -> Tuple[str,int]:
    filename = f"{OUT_PREFIX}_{feed_name}.xml"
    with open(filename, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<rss xmlns:content="http://purl.org/rss/1.0/modules/content/" version="2.0">\n')
        f.write("<channel>\n")
        f.write(f"<title>{escape(NEWSLETTER_NAME)} · {escape(feed_name.capitalize())}</title>\n")
        f.write(f"<link>{escape(BASE_URL)}</link>\n")
        f.write(f"<description>{escape(CHANNEL_DESC)}</description>\n")
        f.write("<language>en-au</language>\n")
        f.write(f"<lastBuildDate>{fmt_rfc822(datetime.utcnow())}</lastBuildDate>\n")
        f.write("<ttl>60</ttl>\n")

        for it in items:
            t = escape(it["_title"])
            link = escape(it.get("url") or "")
            pub = it["_pub_dt"]
            cat = escape(it.get("category") or "other")
            # content inside CDATA
            content = sanitize_cdata(it["_summary"])

            f.write("<item>\n")
            f.write(f"<title>{t}</title>\n")
            f.write(f"<link>{link}</link>\n")
            f.write(f"<guid isPermaLink=\"false\">{link}</guid>\n")
            f.write(f"<pubDate>{fmt_rfc822(pub)}</pubDate>\n")
            f.write("<description><![CDATA[" + content + "]]></description>\n")
            f.write("<content:encoded><![CDATA[" + content + "]]></content:encoded>\n")
            f.write(f"<category>{cat}</category>\n")
            f.write("</item>\n")

        f.write("</channel>\n</rss>\n")
    print(f"Wrote {filename} (items={len(items)})")
    return filename, len(items)

def mark_exported(conn: sqlite3.Connection, item_ids: List[int], batch: str):
    if not item_ids:
        return
    qmarks = ",".join("?" for _ in item_ids)
    conn.execute(
        f"UPDATE items SET exported_to_rss=1, export_batch=? WHERE id IN ({qmarks})",
        (batch, *item_ids)
    )
    conn.commit()

def main():
    conn = sqlite3.connect(DB_PATH)
    items = filter_valid(read_recent(conn))
    buckets = group_by_feed(items)

    used_ids: List[int] = []
    for feed_name, bucket_items in buckets.items():
        fn, n = write_feed(feed_name, bucket_items)
        used_ids.extend([it["id"] for it in bucket_items])

    if MARK_EXPORTED:
        batch = datetime.utcnow().strftime("%Y%m%d")
        mark_exported(conn, list(set(used_ids)), batch)

if __name__ == "__main__":
    main()
