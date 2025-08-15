#!/usr/bin/env python3
import os, re, sqlite3, logging
from datetime import datetime, timedelta
from xml.sax.saxutils import escape

# ---- Env / defaults ----
DB_PATH = os.getenv("RSS_DB_PATH", "rss_items.db")
OUT_PREFIX = os.getenv("OUT_PREFIX", "beehiiv")  # files like beehiiv_all.xml, beehiiv_deals.xml, ...
BASE_URL = os.getenv("BASE_URL", "https://mwhiteoak.github.io/morningBrief")
TITLE = os.getenv("NEWSLETTER_NAME", "Morning Briefing")
LANG = "en-au"
WINDOW_HR = int(os.getenv("SCAN_WINDOW_HRS", "36"))
MAX_ITEMS_PER_FEED = int(os.getenv("MAX_ITEMS_PER_FEED", "80"))

# Category order (most relevant for CRE pros first)
CAT_ORDER = [
    "deals_capital","development_planning","reits","leasing",
    "industrial","office","retail","residential","proptech_finance","macro"
]
CAT_INDEX = {c:i for i,c in enumerate(CAT_ORDER)}

# Feed groups -> which categories to include (None = all)
GROUPS = {
    "all": None,
    "deals": ["deals_capital"],
    "development": ["development_planning"],
    "reits": ["reits"],
    "leasing": ["leasing"],
    "industrial": ["industrial"],
    "office": ["office"],
    "retail": ["retail"],
    "residential": ["residential"],
    "proptech": ["proptech_finance"],
    # keep macro separate via write_macro_xml.py
}

logging.basicConfig(level=logging.INFO, format="%(message)s")

# ---- Helpers ----
def now_au():
    return datetime.utcnow() + timedelta(hours=10)

def strip_html(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s or "").strip()

def iso_to_rfc2822(iso_s: str) -> str:
    try:
        dt = datetime.fromisoformat(iso_s)
    except Exception:
        dt = now_au()
    return dt.strftime("%a, %d %b %Y %H:%M:%S +1000")

def build_xml(items):
    now = now_au()
    head = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">',
        "<channel>",
        f"<title>{escape(TITLE)}</title>",
        f"<link>{escape(BASE_URL)}</link>",
        "<description>Daily AU commercial property briefing — asset, fund, and acquisitions focused.</description>",
        f"<language>{LANG}</language>",
        f"<lastBuildDate>{iso_to_rfc2822(now.isoformat())}</lastBuildDate>",
        "<ttl>60</ttl>",
    ]
    body = []
    for it in items:
        ai_title = it["ai_title"].strip()
        ai_desc  = it["ai_desc_html"].strip()
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

def read_recent(conn):
    since = now_au() - timedelta(hours=WINDOW_HR)
    # grab everything; adapt to whatever schema is there
    cur = conn.execute("SELECT * FROM items WHERE datetime(COALESCE(published_at,published,created_at)) >= datetime(?)", (since.isoformat(),))
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()

    def pick(d, *names):
        for n in names:
            if n in d and d[n]: return d[n]
        return None

    out = []
    for r in rows:
        rec = {cols[i]: r[i] for i in range(len(cols))}
        url = pick(rec, "url","link","url_canonical","link_canonical")
        if not url: continue

        ai_title = pick(rec, "ai_title")
        ai_desc  = pick(rec, "ai_desc_html")
        # Require AI-ready entries only (prevents filler)
        if not ai_title or not ai_desc: 
            continue
        if len(strip_html(ai_desc)) < 60:
            continue

        source = pick(rec, "source","source_name","source_feed") or ""
        category_main = pick(rec, "category_main","category") or "macro"
        published_at = pick(rec, "published_at","published","created_at") or now_au().isoformat()
        ai_tags = pick(rec, "ai_tags","tags") or "[]"

        out.append({
            "url": url,
            "source": source,
            "ai_title": ai_title,
            "ai_desc_html": ai_desc,
            "category_main": category_main,
            "published_at": published_at,
            "ai_tags": ai_tags,
        })

    # de-dupe by URL
    seen, deduped = set(), []
    for it in out:
        if it["url"] in seen: continue
        seen.add(it["url"]); deduped.append(it)

    # sort by bucket, then newest
    deduped.sort(key=lambda r: (CAT_INDEX.get(r["category_main"], 999), r["published_at"]), reverse=True)
    return deduped

def write_feed(items, out_path, cats=None):
    if cats is not None:
        items = [i for i in items if i["category_main"] in cats]
    items = items[:MAX_ITEMS_PER_FEED]
    xml = build_xml(items)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(xml)
    logging.info("Wrote %s (items=%d)", out_path, len(items))

def main():
    if not os.path.exists(DB_PATH):
        logging.info("No DB present; writing empty RSS shells.")
        items = []
    else:
        conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row
        items = read_recent(conn)

    # all + category feeds
    for key, cats in GROUPS.items():
        out = f"{OUT_PREFIX}_{key}.xml" if key != "all" else f"{OUT_PREFIX}_all.xml"
        write_feed(items, out, cats)

if __name__ == "__main__":
    main()
