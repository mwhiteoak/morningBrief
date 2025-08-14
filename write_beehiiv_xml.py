from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import sqlite3, os, re, html, json

DB_PATH    = os.getenv("RSS_DB_PATH", "rss_items.db")
OUT_PATH   = os.getenv("OUT_PATH", "public/beehiiv.xml")
SITE_TITLE = os.getenv("NEWSLETTER_NAME", "Morning Briefing")
SITE_LINK  = os.getenv("BASE_URL", "https://yourname.github.io/yourrepo")
FEED_DESC  = "Daily AU commercial property briefing — asset, fund, and acquisitions focused."
TZ_REGION  = os.getenv("TZ_REGION", "Australia/Brisbane")
REQUIRE_AI = os.getenv("REQUIRE_AI", "1") == "1"  # exclude items without AI package

def rfc822(dt_local: datetime) -> str:
    return dt_local.strftime("%a, %d %b %Y %H:%M:%S %z")

def today_bounds_utc():
    tz = ZoneInfo(TZ_REGION)
    now_local = datetime.now(tz)
    start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S")
    end_utc   = end_local.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S")
    return start_local, start_utc, end_utc

def item_block_from_pkg(pkg: dict, link: str) -> str:
    block = pkg.get("newsletter_block_html") or f'<p><a href="{html.escape(link)}">Read more</a></p>'
    return block.replace("<script", "&lt;script")

start_local, start_utc, end_utc = today_bounds_utc()
batch_id = start_local.strftime("%Y%m%d")

conn = sqlite3.connect(DB_PATH)
cur = conn.execute("""
    SELECT id, title, link, description, interest_score, ai_summary, source_name, processed_at, ai_package, exported_to_rss
    FROM items
    WHERE processed_at >= ? AND processed_at < ? AND exported_to_rss = 0
    ORDER BY interest_score DESC, processed_at DESC
""", (start_utc, end_utc))
rows = cur.fetchall()

items_xml = []
selected_ids = []

for row in rows:
    _id, title, link, desc, score, summary, source, processed_at, ai_pkg, exported = row
    pkg = None
    if ai_pkg:
        try: pkg = json.loads(ai_pkg)
        except Exception: pkg = None
    if REQUIRE_AI and not (pkg and isinstance(pkg, dict) and pkg.get("hed")):
        continue

    hed = (pkg.get("hed") if pkg else title) or "Untitled"
    block = item_block_from_pkg(pkg, link) if pkg else f"<p>{html.escape((summary or desc or '')[:400])}…</p><p><a href=\"{html.escape(link)}\">Read more</a></p>"
    cats = ""
    if pkg:
        cats += "".join(f"<category>{html.escape(c)}</category>" for c in (pkg.get("sector_tags") or []))
        cats += "".join(f"<category>{html.escape(c)}</category>" for c in (pkg.get("geo_tags") or []))

    pub_local = start_local  # set all items to today's issue date/time window for consistency
    items_xml.append(f"""
<item>
  <title>{html.escape(hed)}</title>
  <link>{html.escape(link)}</link>
  <guid isPermaLink="false">{html.escape(link)}</guid>
  <pubDate>{rfc822(pub_local)}</pubDate>
  <description><![CDATA[{block}]]></description>
  <content:encoded><![CDATA[{block}]]></content:encoded>
  <category>{html.escape(source)}</category>
  {cats}
</item>""")
    selected_ids.append(str(_id))

xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"
     xmlns:content="http://purl.org/rss/1.0/modules/content/">
  <channel>
    <title>{html.escape(SITE_TITLE)}</title>
    <link>{html.escape(SITE_LINK)}</link>
    <description>{html.escape(FEED_DESC)}</description>
    <language>en-au</language>
    <lastBuildDate>{rfc822(datetime.now(ZoneInfo(TZ_REGION)))}</lastBuildDate>
    <ttl>60</ttl>
    {''.join(items_xml)}
  </channel>
</rss>"""

os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
with open(OUT_PATH, "w", encoding="utf-8") as f:
    f.write(xml)

# Mark exported (prevents repeats tomorrow)
if selected_ids:
    q = f"UPDATE items SET exported_to_rss = 1, export_batch = ? WHERE id IN ({','.join(['?']*len(selected_ids))})"
    conn.execute(q, (batch_id, *selected_ids))
    conn.commit()

conn.close()
print(f"Wrote RSS to {OUT_PATH} (items={len(items_xml)}), batch={batch_id}")
