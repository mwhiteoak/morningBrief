#!/usr/bin/env python3
"""
Create a single Beehiiv-ready RSS file (beehiiv.xml)
- Only items with AI rewrites (relevant=1 AND ai_desc present)
- XML-safe (properly escaped)
- Skips items with thin content
"""

import os, sqlite3, html
from datetime import datetime, timedelta, timezone

RSS_DB_PATH   = os.getenv("RSS_DB_PATH", "rss_items.db")
OUT_PATH      = os.getenv("OUT_PATH", "beehiiv.xml")
NEWSLETTER    = os.getenv("NEWSLETTER_NAME", "Morning Briefing")
BASE_URL      = os.getenv("BASE_URL", "https://mwhiteoak.github.io/morningBrief")
SCAN_WINDOW_HRS = int(os.getenv("SCAN_WINDOW_HRS", "24"))
TZ            = timezone(timedelta(hours=10))  # Australia/Brisbane

def esc(t: str) -> str:
    return html.escape(t or "", quote=True)

def cdata(s: str) -> str:
    # protect CDATA terminator
    return "<![CDATA[" + (s or "").replace("]]>", "]]]]><![CDATA[>") + "]]>"

def fetch(conn: sqlite3.Connection):
    since = datetime.now(tz=TZ) - timedelta(hours=SCAN_WINDOW_HRS)
    cur = conn.execute("""
        SELECT source_name, link, link_canonical, ai_title, ai_desc, ai_tags, published_at
        FROM items
        WHERE relevant=1 AND ai_desc IS NOT NULL AND length(ai_desc) > 40
    """)
    rows = []
    for r in cur.fetchall():
        try:
            pub = datetime.fromisoformat(r[6])
        except Exception:
            pub = datetime.now(tz=TZ)
        if pub < since:
            continue
        rows.append({
            "source": r[0],
            "link": r[1] or "",
            "canon": r[2] or r[1] or "",
            "title": r[3] or "",
            "desc": r[4] or "",
            "tags": (r[5] or "").split(",") if r[5] else [],
            "pub": pub,
        })
    # Sort: most recent, then property-weighted by presence of classic CRE terms in title
    def score_title(t: str) -> int:
        t = (t or "").lower()
        bump = 0
        for kw in ["acquires","sells","leasing","development","a-reit","reit","industrial","retail","office","warehouse","logistics","capital raising","placement","rba","rate"]:
            if kw in t:
                bump += 1
        return bump
    rows.sort(key=lambda x: (x["pub"], score_title(x["title"])), reverse=True)
    return rows

def write_feed(items):
    now = datetime.now(tz=TZ)
    header = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
<channel>
<title>{esc(NEWSLETTER)}</title>
<link>{esc(BASE_URL)}</link>
<description>Daily AU commercial property briefing — assets, funds, deals, development.</description>
<language>en-au</language>
<lastBuildDate>{now.strftime('%a, %d %b %Y %H:%M:%S %z')}</lastBuildDate>
<ttl>60</ttl>
"""
    items_xml = []
    for it in items:
        link = it["canon"] or it["link"]
        pub = it["pub"].strftime('%a, %d %b %Y %H:%M:%S %z')
        cats = "".join([f"<category>{esc(t)}</category>" for t in it["tags"][:5]])
        body_html = f"<p>{esc(it['desc'])}</p>"
        items_xml.append(
            f"<item>\n"
            f"  <title>{esc(it['title'])}</title>\n"
            f"  <link>{esc(link)}</link>\n"
            f"  <guid isPermaLink=\"false\">{esc(link)}</guid>\n"
            f"  <pubDate>{pub}</pubDate>\n"
            f"  <description>\n{cdata(body_html)}\n  </description>\n"
            f"  <content:encoded>\n{cdata(body_html)}\n  </content:encoded>\n"
            f"  {cats}\n"
            f"</item>\n"
        )
    footer = "</channel>\n</rss>\n"
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(header + "".join(items_xml) + footer)

def main():
    conn = sqlite3.connect(RSS_DB_PATH)
    items = fetch(conn)
    write_feed(items)
    print(f"Wrote RSS to {OUT_PATH} (items={len(items)})")

if __name__ == "__main__":
    main()
