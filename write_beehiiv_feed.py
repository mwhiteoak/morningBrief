#!/usr/bin/env python3
"""
Create a single Beehiiv-ready RSS file (beehiiv.xml)
- Only items with AI rewrites (relevant=1 AND ai_desc present)
- Incorporates AI-generated headline and subhead from newsletter_metadata
- XML-safe (properly escaped)
- Skips items with thin content
"""
import os, sqlite3, html
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple

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

def get_newsletter_metadata(conn: sqlite3.Connection) -> Tuple[str, str]:
    """Get the latest headline and subhead from newsletter_metadata"""
    try:
        cur = conn.execute("""
            SELECT headline, subhead FROM newsletter_metadata 
            ORDER BY created_at DESC 
            LIMIT 1
        """)
        result = cur.fetchone()
        if result:
            return result[0] or "Morning Property Brief", result[1] or "Latest updates from Australian commercial property"
    except sqlite3.OperationalError:
        # Table doesn't exist yet
        pass
    return "Morning Property Brief", "Latest updates from Australian commercial property"

def fetch(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    since = datetime.now(tz=TZ) - timedelta(hours=SCAN_WINDOW_HRS)
    
    try:
        # Try with AI columns first
        cur = conn.execute("""
            SELECT source_name, link, link_canonical, ai_title, ai_desc, ai_tags, published_at
            FROM items
            WHERE relevant=1 AND ai_desc IS NOT NULL AND length(ai_desc) > 40
        """)
        rows = cur.fetchall()
        use_ai_columns = True
    except sqlite3.OperationalError:
        # Fall back to basic columns
        cur = conn.execute("""
            SELECT source_name, link, link_canonical, title, summary, '', published_at
            FROM items
            WHERE length(summary) > 40
        """)
        rows = cur.fetchall()
        use_ai_columns = False
        logging.info("Using basic columns (AI columns not available yet)")
    
    items = []
    for r in rows:
        try:
            pub = datetime.fromisoformat(r[6])
        except Exception:
            pub = datetime.now(tz=TZ)
        if pub < since:
            continue
        
        # Handle both AI and basic column formats
        if use_ai_columns:
            title = r[3] or r[0]  # ai_title or fallback
            desc = r[4] or r[1]   # ai_desc or fallback
            tags = (r[5] or "").split(",") if r[5] else []
        else:
            title = r[3] or ""    # basic title
            desc = r[4] or ""     # basic summary
            tags = []
            
        items.append({
            "source": r[0],
            "link": r[1] or "",
            "canon": r[2] or r[1] or "",
            "title": title,
            "desc": desc,
            "tags": tags,
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
    
    items.sort(key=lambda x: (x["pub"], score_title(x["title"])), reverse=True)
    return items

def create_newsletter_summary(items: List[Dict[str, Any]], headline: str, subhead: str) -> str:
    """Create a newsletter summary item with headline and subhead"""
    now = datetime.now(tz=TZ)
    
    # Create a summary of key stories
    summary_content = f"<h2>{esc(headline)}</h2>"
    summary_content += f"<p><em>{esc(subhead)}</em></p>"
    
    if items:
        summary_content += "<h3>Today's Key Stories:</h3><ul>"
        for item in items[:5]:  # Top 5 stories
            summary_content += f'<li><strong>{esc(item["title"])}</strong> - {esc(item["desc"][:100])}...</li>'
        summary_content += "</ul>"
    
    summary_content += f"<p><small>Generated on {now.strftime('%d %B %Y at %H:%M AEST')}</small></p>"
    
    return f"""<item>
  <title>{esc(headline)}</title>
  <link>{esc(BASE_URL)}</link>
  <guid isPermaLink="false">newsletter-{now.strftime('%Y%m%d')}</guid>
  <pubDate>{now.strftime('%a, %d %b %Y %H:%M:%S %z')}</pubDate>
  <description>
{cdata(summary_content)}
  </description>
  <content:encoded>
{cdata(summary_content)}
  </content:encoded>
  <category>newsletter</category>
  <category>summary</category>
</item>
"""

def write_feed(items: List[Dict[str, Any]], headline: str, subhead: str):
    now = datetime.now(tz=TZ)
    
    # Use AI-generated headline as the feed title
    feed_title = headline if headline != "Morning Property Brief" else f"{NEWSLETTER} - {headline}"
    
    header = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
<channel>
<title>{esc(feed_title)}</title>
<link>{esc(BASE_URL)}</link>
<description>{esc(subhead)}</description>
<language>en-au</language>
<lastBuildDate>{now.strftime('%a, %d %b %Y %H:%M:%S %z')}</lastBuildDate>
<ttl>60</ttl>
"""

    items_xml = []
    
    # Add newsletter summary as first item
    items_xml.append(create_newsletter_summary(items, headline, subhead))
    
    # Add individual items
    for it in items:
        link = it["canon"] or it["link"]
        pub = it["pub"].strftime('%a, %d %b %Y %H:%M:%S %z')
        cats = "".join([f"<category>{esc(t.strip())}</category>" for t in it["tags"][:5] if t.strip()])
        
        # Enhanced body with source attribution
        body_html = f"""<div class="property-item">
  <p class="source"><small>Source: {esc(it['source'])}</small></p>
  <p>{esc(it['desc'])}</p>
  <p><a href="{esc(link)}" target="_blank">Read full article →</a></p>
</div>"""
        
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
    
    # Get AI-generated headline and subhead
    headline, subhead = get_newsletter_metadata(conn)
    
    # Get items
    items = fetch(conn)
    
    # Write feed with AI-generated metadata
    write_feed(items, headline, subhead)
    
    print(f"Wrote RSS to {OUT_PATH}")
    print(f"Headline: {headline}")
    print(f"Subhead: {subhead}")
    print(f"Items: {len(items)}")

if __name__ == "__main__":
    main()
