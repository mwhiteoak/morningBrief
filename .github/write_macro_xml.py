#!/usr/bin/env python3
import os
from datetime import datetime, timedelta
from xml.sax.saxutils import escape

OUT_PATH = os.getenv("MACRO_OUT_PATH", "beehiiv_macro.xml")
BASE_URL = os.getenv("BASE_URL", "https://mwhiteoak.github.io/morningBrief")
TITLE = os.getenv("NEWSLETTER_NAME", "Morning Briefing")
LANG = "en-au"

def now_au():
    return datetime.utcnow() + timedelta(hours=10)

def rfc2822(dt):
    return dt.strftime("%a, %d %b %Y %H:%M:%S +1000")

def val(name):
    return os.getenv(name, "—")

def main():
    asx200   = val("ASX200")
    audusd   = val("AUDUSD")
    au10y    = val("AU10Y")
    rba_cash = val("RBA_CASH")
    rba_prob = val("RBA_NEXT_MOVE_PROB")
    take     = os.getenv("MACRO_TAKE", "Risk-on morning; lenders bid; dev feasos ease a touch.")

    now = now_au()
    item_html = f"""
<p><strong>ASX200:</strong> {escape(asx200)} &nbsp;|&nbsp; <strong>AUD/USD:</strong> {escape(audusd)}
&nbsp;|&nbsp; <strong>AU 10-yr:</strong> {escape(au10y)} &nbsp;|&nbsp; <strong>RBA cash:</strong> {escape(rba_cash)}
&nbsp;|&nbsp; <strong>Next move prob:</strong> {escape(rba_prob)}</p>
<p><em>{escape(take)}</em></p>
""".strip()

    xml = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">',
        "<channel>",
        f"<title>{escape(TITLE)} — Macro in a Minute</title>",
        f"<link>{escape(BASE_URL)}</link>",
        "<description>Tiny daily macro block for Beehiiv.</description>",
        f"<language>{LANG}</language>",
        f"<lastBuildDate>{rfc2822(now)}</lastBuildDate>",
        "<ttl>30</ttl>",
        "<item>",
        f"<title>Macro in a Minute — {now.strftime('%Y-%m-%d')}</title>",
        f"<link>{escape(BASE_URL)}</link>",
        f"<guid isPermaLink=\"false\">macro-{now.strftime('%Y%m%d')}</guid>",
        f"<pubDate>{rfc2822(now)}</pubDate>",
        "<description><![CDATA[ " + item_html + " ]]></description>",
        "<content:encoded><![CDATA[ " + item_html + " ]]></content:encoded>",
        "<category>macro</category>",
        "</item>",
        "</channel>",
        "</rss>",
    ]
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(xml))

if __name__ == "__main__":
    main()
