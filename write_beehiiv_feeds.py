#!/usr/bin/env python3
# write_beehiiv_feeds.py — multi-bucket Beehiiv RSS generator (XML-safe)

import os, sqlite3, logging, re, html
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# ------------ config via env ------------
DB_PATH            = os.getenv("RSS_DB_PATH", "rss_items.db")
OUT_PREFIX         = os.getenv("OUT_PREFIX", "beehiiv")          # beehiiv_*.xml
NEWSLETTER_NAME    = os.getenv("NEWSLETTER_NAME", "Morning Briefing")
BASE_URL           = os.getenv("BASE_URL", "https://example.com")
SCAN_WINDOW_HRS    = int(os.getenv("SCAN_WINDOW_HRS", "36"))
MAX_ITEMS_PER_FEED = int(os.getenv("MAX_ITEMS_PER_FEED", "80"))
TZ_REGION          = os.getenv("TZ_REGION", "Australia/Brisbane")
# ---------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Buckets you want to publish
BUCKETS = {
    "top":     {"title": f"{NEWSLETTER_NAME} · Top",     "filename": f"{OUT_PREFIX}_top.xml"},
    "property":{"title": f"{NEWSLETTER_NAME} · Property","filename": f"{OUT_PREFIX}_property.xml"},
    "finance": {"title": f"{NEWSLETTER_NAME} · Finance", "filename": f"{OUT_PREFIX}_finance.xml"},
    "reits":   {"title": f"{NEWSLETTER_NAME} · A-REITs", "filename": f"{OUT_PREFIX}_reits.xml"},
    "policy":  {"title": f"{NEWSLETTER_NAME} · Policy",  "filename": f"{OUT_PREFIX}_policy.xml"},
}

SAFE_PLACEHOLDER_RE = re.compile(r"^\s*(insufficient data|no summary|n/a)\b", re.I)

# ---------- helpers ----------
def zone():
    try:
        return ZoneInfo(TZ_REGION)
    except Exception:
        return timezone.utc

def now_local():
    return datetime.now(zone())

def to_rfc822(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%a, %d %b %Y %H:%M:%S %z")

def parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    s = s.strip()
    # Common cases: ISO8601 (with or without tz), RFC822-ish
    try:
        d = datetime.fromisoformat(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(zone())
    except Exception:
        pass
    for fmt in (
        "%a, %d %b %Y %H:%M:%S %z",
        "%d %b %Y %H:%M:%S %z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            d = datetime.strptime(s, fmt)
            if d.tzinfo is None:
                d = d.replace(tzinfo=timezone.utc)
            return d.astimezone(zone())
        except Exception:
            continue
    return None

CTRL_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]")

def scrub(s: str) -> str:
    # Remove control chars that XML 1.0 disallows
    return CTRL_RE.sub("", s)

def xml_text(s: str) -> str:
    # Escape &, <, >, " in text nodes
    return html.escape(scrub(s), quote=True)

def cdata(html_str: str) -> str:
    if html_str is None:
        html_str = ""
    # Escape accidental CDATA close
    safe = scrub(html_str).replace("]]>", "]]]]><![CDATA[>")
    return f"<![CDATA[ {safe} ]]>"

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

# --------- DB read ----------
def fetch_recent_items(conn):
    conn.row_factory = dict_factory
    # Pull a reasonably large recent slice; filter in Python for date window/content
    cur = conn.execute("""
        SELECT * FROM items
        ORDER BY COALESCE(processed_at, published_at, created_at) DESC
        LIMIT 2000
    """)
    return cur.fetchall()

def unify_item(row):
    # Tolerate old/new schemas
    link = row.get("link") or row.get("url") or row.get("link_canonical") or row.get("url_canonical")
    guid = row.get("fp") or (row.get("link_canonical") or row.get("url_canonical") or link)
    title = row.get("title") or ""
    source = row.get("source_name") or row.get("source") or row.get("source_feed") or ""
    cat   = (row.get("category") or row.get("categories") or "").strip()
    ai    = (row.get("ai_summary") or "").strip()
    desc  = (row.get("description") or "").strip()
    interest = row.get("interest_score") or 0

    # published date
    pd = parse_dt(row.get("published_at") or row.get("published") or row.get("created_at") or row.get("processed_at"))

    # choose body (skip placeholders)
    body = ai or desc
    if body and SAFE_PLACEHOLDER_RE.search(body):
        body = ""

    return {
        "link": link or "",
        "guid": guid or (link or ""),
        "title": title,
        "source": source,
        "category": cat.lower(),
        "interest": int(interest) if isinstance(interest, (int, float, str)) and str(interest).isdigit() else 0,
        "pub_dt": pd,
        "body_html": body,
        "title_lc": (title or "").lower(),
        "ai_lc": (ai or "").lower(),
    }

# --------- bucketing ----------
def bucket_for(it):
    lc_title = it["title_lc"]
    lc_cat   = it["category"]
    lc_ai    = it["ai_lc"]
    src      = (it["source"] or "").lower()

    is_property = any([
        "property" in lc_cat or "real estate" in lc_cat,
        "realcommercial" in src or "real commercial" in src,
        "realestate.com.au" in src or "domain" in src or "urban developer" in src,
        "pca" in src or "commercial real estate" in src,
        "development" in lc_ai or "leasing" in lc_ai or "vacancy" in lc_ai or "cap rate" in lc_ai,
    ])

    is_reit = any([
        "reit" in lc_cat or "a-reit" in lc_cat or "a-reit" in lc_cat,
        "reit" in lc_title or "a-reit" in lc_title or "a-reit" in lc_title,
        "trust" in lc_title and "property" in lc_title,
    ])

    is_finance = any([
        "finance" in lc_cat or "bank" in lc_cat or "markets" in lc_cat or "asx" in lc_cat,
        "rba" in lc_ai or "bond" in lc_ai or "yields" in lc_ai or "equity" in lc_ai,
        "afr - fintech" in src or "ausfintech" in src or "motley fool" in src,
    ])

    is_policy = any([
        "policy" in lc_cat or "regulat" in lc_cat or "planning" in lc_cat,
        "rba media releases" in src or "sec" in src or "abc news" in src,
        "minister" in lc_ai or "rezoning" in lc_ai or "stamp duty" in lc_ai,
    ])

    if is_reit:
        return "reits"
    if is_property:
        return "property"
    if is_policy:
        return "policy"
    if is_finance:
        return "finance"
    return "top"

# --------- XML build ----------
def build_channel_xml(bucket_key, items):
    channel_title = BUCKETS[bucket_key]["title"]
    channel_link  = BASE_URL
    channel_desc  = "Daily AU commercial property briefing — assets, funds, deals, development."
    now_dt        = now_local()

    parts = []
    parts.append('<?xml version="1.0" encoding="UTF-8"?>')
    parts.append('<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">')
    parts.append("<channel>")
    parts.append(f"<title>{xml_text(channel_title)}</title>")
    parts.append(f"<link>{xml_text(channel_link)}</link>")
    parts.append(f"<description>{xml_text(channel_desc)}</description>")
    parts.append("<language>en-au</language>")
    parts.append(f"<lastBuildDate>{xml_text(to_rfc822(now_dt))}</lastBuildDate>")
    parts.append("<ttl>60</ttl>")

    for it in items:
        title = it["title"].strip() or (it["source"] or "(untitled)")
        link  = it["link"] or it["guid"] or ""
        guid  = it["guid"] or link
        pub   = to_rfc822(it["pub_dt"]) if it["pub_dt"] else to_rfc822(now_dt)
        src   = it["source"] or ""
        cats  = [src]
        # Add light-weight categories per bucket
        if bucket_key != "top":
            cats.append(bucket_key)

        parts.append("<item>")
        parts.append(f"<title>{xml_text(title)}</title>")
        parts.append(f"<link>{xml_text(link)}</link>")
        parts.append(f'<guid isPermaLink="false">{xml_text(guid)}</guid>')
        parts.append(f"<pubDate>{xml_text(pub)}</pubDate>")

        # Body -> both description and content:encoded
        body = it["body_html"].strip()
        parts.append("<description>")
        parts.append(cdata(body))
        parts.append("</description>")
        parts.append("<content:encoded>")
        parts.append(cdata(body))
        parts.append("</content:encoded>")

        for c in cats:
            if c:
                parts.append(f"<category>{xml_text(c)}</category>")

        parts.append("</item>")

    parts.append("</channel>")
    parts.append("</rss>")
    return "\n".join(parts)

# --------- main ---------
def main():
    cutoff = now_local() - timedelta(hours=SCAN_WINDOW_HRS)
    conn = sqlite3.connect(DB_PATH)

    rows = fetch_recent_items(conn)
    items = []
    for r in rows:
        it = unify_item(r)
        # Date-window filter (tolerant of missing/naive tz)
        if it["pub_dt"] is not None and it["pub_dt"] < cutoff:
            continue
        # Must have link, title, and body
        if not it["link"] and not it["guid"]:
            continue
        if not it["title"].strip():
            continue
        if not it["body_html"].strip():
            continue  # <- SKIP empty body items entirely

        # hard-drop any obvious “insufficient data” bodies
        if SAFE_PLACEHOLDER_RE.search(it["body_html"]):
            continue

        it["bucket"] = bucket_for(it)
        items.append(it)

    # Sort “top” by interest then recency; others by recency
    by_bucket = {k: [] for k in BUCKETS.keys()}
    for it in items:
        by_bucket[it["bucket"]].append(it)

    def recency_key(x):
        return x["pub_dt"] or now_local()

    # Build top: include everything, sort by interest then recency
    all_for_top = sorted(items, key=lambda x: (x.get("interest", 0), recency_key(x)), reverse=True)
    by_bucket["top"] = all_for_top

    # Sort the rest newest first
    for b in ("property", "finance", "reits", "policy"):
        by_bucket[b] = sorted(by_bucket[b], key=recency_key, reverse=True)

    # Truncate per-feed
    for b in by_bucket:
        if MAX_ITEMS_PER_FEED > 0:
            by_bucket[b] = by_bucket[b][:MAX_ITEMS_PER_FEED]

    # Write files
    total_written = 0
    for b, cfg in BUCKETS.items():
        out_path = cfg["filename"]
        data = build_channel_xml(b, by_bucket[b])
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(data)
        logging.info("Wrote %s (bucket=%s, items=%d)", out_path, b, len(by_bucket[b]))
        total_written += len(by_bucket[b])

    logging.info("Done. Buckets: %s | Total items written: %d",
                 {k: len(v) for k,v in by_bucket.items()}, total_written)

if __name__ == "__main__":
    main()
