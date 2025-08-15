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
    cur = conn.execute("""
        SELECT headline, subhead FROM newsletter_metadata 
        ORDER BY created_at DESC 
        LIMIT 1
    """)
    result = cur.fetchone()
    if result:
        return result[0] or "Morning Property Brief", result[1] or "Latest updates from Australian commercial property"
    return "Morning Property Brief", "Latest updates from Australian commercial property"

def fetch(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
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

def create_newsletter_item
