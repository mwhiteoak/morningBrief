#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Emit a tiny macro feed Beehiiv can always pull.

ENV (optional data fetch toggles; defaults to placeholders):
  ALLOW_FETCH=0
  OUT_FILE=beehiiv_macro.xml
  NEWSLETTER_NAME=Morning Briefing
  BASE_URL=https://mwhiteoak.github.io/morningBrief

If ALLOW_FETCH=1 you can implement live fetchers; for now we keep placeholders "—".
"""

import os
from datetime import datetime
from xml.sax.saxutils import escape

OUT_FILE = os.getenv("OUT_FILE", "beehiiv_macro.xml")
NEWSLETTER_NAME = os.getenv("NEWSLETTER_NAME", "Morning Briefing")
BASE_URL = os.getenv("BASE_URL", "")

def fmt_rfc822(dt: datetime) -> str:
    return dt.strftime("%a, %d %b %Y %H:%M:%S +0000")

def main():
    asx200 = "—"
    audusd = "—"
    au10y  = "—"
    rba    = "—"
    odds   = "—"
    one_liner = "Risk-on morning; lenders bid; dev feasos ease a touch."

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<rss xmlns:content="http://purl.org/rss/1.0/modules/content/" version="2.0">\n')
        f.write("<channel>\n")
        f.write(f"<title>{escape(NEWSLETTER_NAME)} · Macro</title>\n")
        f.write(f"<link>{escape(BASE_URL)}</link>\n")
        f.write("<description>Macro in a Minute: ASX200, AUDUSD, AU 10y, RBA cash, next move odds.</description>\n")
        f.write("<language>en-au</language>\n")
        f.write(f"<lastBuildDate>{fmt_rfc822(datetime.utcnow())}</lastBuildDate>\n")
        f.write("<ttl>30</ttl>\n")

        content = (
            f"<p><strong>ASX200:</strong> {asx200} | "
            f"<strong>AUDUSD:</strong> {audusd} | "
            f"<strong>AU 10y:</strong> {au10y} | "
            f"<strong>RBA cash:</strong> {rba} | "
            f"<strong>Next move odds:</strong> {odds}</p>"
            f"<p><em>{one_liner}</em></p>"
        )

        f.write("<item>\n")
        f.write("<title>Macro in a Minute</title>\n")
        f.write(f"<link>{escape(BASE_URL)}</link>\n")
        f.write(f"<guid isPermaLink=\"false\">{escape(BASE_URL)}/macro/{datetime.utcnow().strftime('%Y%m%d')}</guid>\n")
        f.write(f"<pubDate>{fmt_rfc822(datetime.utcnow())}</pubDate>\n")
        f.write("<description><![CDATA[" + content + "]]></description>\n")
        f.write("<content:encoded><![CDATA[" + content + "]]></content:encoded>\n")
        f.write("<category>macro</category>\n")
        f.write("</item>\n")

        f.write("</channel>\n</rss>\n")

    print(f"Wrote {OUT_FILE}")

if __name__ == "__main__":
    main()
