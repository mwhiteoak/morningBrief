#!/usr/bin/env python3
"""
Writes multiple Beehiiv-ready RSS files from sqlite:
- beehiiv_top.xml (smart sorted, ~5-min skim total)
- beehiiv_{category}.xml for each category present
- beehiiv_macro.xml ("Macro in a Minute": ASX200, A-REIT (^AXPJ), AUDUSD, AU 10-yr, RBA cash rate & next-move prob)
Notes:
- Excludes items without AI content or marked 'Insufficient data'
- No hallucinations: descriptions come from rss_analyzer's constrained rewrites
- Macro feed shows '—' for any missing datapoint
"""

import os, json, sqlite3, logging, math
from datetime import datetime, timedelta
from xml.sax.saxutils import escape
import httpx

NEWSLETTER_NAME = os.getenv("NEWSLETTER_NAME", "Morning Briefing")
BASE_URL        = os.getenv("BASE_URL", "https://mwhiteoak.github.io/morningBrief")
DB_PATH         = os.getenv("RSS_DB_PATH", "rss_items.db")
TZ_REGION       = os.getenv("TZ_REGION", "Australia/Brisbane")

LOOKBACK_HRS    = int(os.getenv("FEED_WINDOW_HOURS", "36"))
TOP_WORD_BUDGET = int(os.getenv("TOP_WORD_BUDGET", "1100"))  # ~5 mins @ 200–250 wpm
MIN_DESC_CHARS  = int(os.getenv("MIN_DESC_CHARS", "60"))

# Optional overrides if you want to set these from Actions secrets
RBA_CASH_RATE_OVERRIDE = os.getenv("RBA_CASH_RATE", None)       # e.g. "3.85%"
RBA_NEXT_MOVE_PROB_OVR = os.getenv("RBA_NEXT_MOVE_PROB", None)  # e.g. "Cut 55%"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CATEGORY_META = {
    "deals_capital":      ("Deals & Capital", "beehiiv_deals.xml"),
    "development_planning":("Development & Planning", "beehiiv_development.xml"),
    "leasing":            ("Leasing", "beehiiv_leasing.xml"),
    "macro":              ("Macro & Policy", "beehiiv_macro_news.xml"),
    "reits":              ("A-REITs & Listed", "beehiiv_reits.xml"),
    "retail":             ("Retail", "beehiiv_retail.xml"),
    "industrial":         ("Industrial & Logistics", "beehiiv_industrial.xml"),
    "office":             ("Office", "beehiiv_office.xml"),
    "residential":        ("Residential (market)", "beehiiv_residential.xml"),
    "proptech_finance":   ("Proptech & Finance", "beehiiv_proptech.xml"),
}

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def au_now():
    return datetime.utcnow() + timedelta(hours=10)  # AEST naive

def read_recent(conn):
    since = au_now() - timedelta(hours=LOOKBACK_HRS)
    rows = conn.execute("""
        SELECT url, url_canonical, source, title, published_at, score, category_main, ai_title, ai_desc_html, ai_tags
        FROM items
        WHERE published_at >= ? 
        ORDER BY datetime(published_at) DESC
    """, (since.isoformat(),)).fetchall()
    out = []
    for r in rows:
        ai_html = (r["ai_desc_html"] or "").strip()
        if not ai_html: 
            continue
        if "Insufficient data" in ai_html:
            continue
        if len(ai_html) < MIN_DESC_CHARS:
            continue
        out.append(r)
    return out

def words(text_html: str) -> int:
    txt = text_html
    # simple text-ish word count
    txt = txt.replace("<br>", " ").replace("<br/>", " ").replace("</p>"," ").replace("<p>"," ")
    return len([w for w in txt.split() if w.strip()])

def build_rss(channel_title: str, items: list) -> str:
    now_rfc = au_now().strftime("%a, %d %b %Y %H:%M:%S +1000")
    head = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
<channel>
<title>{escape(channel_title)}</title>
<link>{escape(BASE_URL)}</link>
<description>Daily AU commercial property briefing — asset, fund, deals, development.</description>
<language>en-au</language>
<lastBuildDate>{now_rfc}</lastBuildDate>
<ttl>60</ttl>
"""
    body = []
    for it in items:
        title = it["ai_title"] or it["title"]
        link  = it["url_canonical"] or it["url"]
        pub   = datetime.fromisoformat(it["published_at"]).strftime("%a, %d %b %Y %H:%M:%S +1000")
        desc  = it["ai_desc_html"]
        cats  = [it["source"], it["category_main"] or ""]
        try:
            tags = json.loads(it["ai_tags"] or "[]")
            cats += tags
        except Exception:
            pass

        body.append(f"""<item>
<title>{escape(title)}</title>
<link>{escape(link)}</link>
<guid isPermaLink="false">{escape(link)}</guid>
<pubDate>{pub}</pubDate>
<description><![CDATA[ {desc} ]]></description>
<content:encoded><![CDATA[ {desc} ]]></content:encoded>
""" + "".join(f"<category>{escape(c)}</category>\n" for c in cats if c) + "</item>\n")
    tail = "</channel>\n</rss>\n"
    return head + "".join(body) + tail

# ---- Macro in a Minute -------------------------------------------------------

def yahoo_quotes(symbols: list) -> dict:
    # Returns {symbol: {"price": float, "pct": float}}
    s = ",".join(symbols)
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={s}"
    out = {}
    try:
        with httpx.Client(timeout=8) as x:
            r = x.get(url)
            r.raise_for_status()
            data = r.json().get("quoteResponse", {}).get("result", [])
            for q in data:
                sym = q.get("symbol")
                out[sym] = {
                    "price": q.get("regularMarketPrice"),
                    "pct":   q.get("regularMarketChangePercent"),
                }
    except Exception as e:
        logging.warning("Yahoo quotes failed: %s", e)
    return out

def fmt_pct(v):
    try:
        if v is None: return "—"
        return f"{v:+.2f}%"
    except Exception:
        return "—"

def fmt_price(v, digits=4):
    try:
        if v is None: return "—"
        if v >= 100: return f"{v:.1f}"
        if v >= 10:  return f"{v:.2f}"
        return f"{v:.{digits}f}"
    except Exception:
        return "—"

def macro_fetch():
    # ^AXJO = ASX200, ^AXPJ = S&P/ASX 200 A-REIT, AUDUSD=X = AUDUSD spot
    q = yahoo_quotes(["^AXJO", "^AXPJ", "AUDUSD=X"])
    asx2k = q.get("^AXJO", {})
    areit = q.get("^AXPJ", {})
    aud   = q.get("AUDUSD=X", {})

    au10y = "—"  # hard to fetch reliably without a paid API; leave as em dash
    rba   = RBA_CASH_RATE_OVERRIDE or "—"
    prob  = RBA_NEXT_MOVE_PROB_OVR or "—"

    # one-liner take
    # Simple rule-based vibe read
    ax = asx2k.get("pct")
    ap = areit.get("pct")
    if ax is not None and ap is not None:
        if ax > 0 and ap > 0:
            take = "Risk-on; lenders bid; dev feasos ease a touch."
        elif ax < 0 and ap < 0:
            take = "Risk-off; yields bite; expect quieter bids."
        else:
            take = "Mixed tape; stock pickers’ day; watch debt costs."
    else:
        take = "Mixed open; watch rates and credit prints."

    payload = {
        "ASX200": fmt_pct(asx2k.get("pct")),
        "A-REIT": fmt_pct(areit.get("pct")),
        "AUDUSD": fmt_price(aud.get("price")),
        "AU10Y":  au10y,
        "RBA_Cash_Rate": rba,
        "Next_Move_Prob": prob,
        "take": take
    }
    return payload

def write_macro_feed():
    p = macro_fetch()
    now_rfc = au_now().strftime("%a, %d %b %Y %H:%M:%S +1000")
    title = f"{NEWSLETTER_NAME}: Macro in a Minute"
    body_html = f"""
<p><strong>ASX200:</strong> {p['ASX200']} &nbsp; | &nbsp; <strong>A-REIT:</strong> {p['A-REIT']} &nbsp; | &nbsp;
<strong>AUDUSD:</strong> {p['AUDUSD']} &nbsp; | &nbsp; <strong>AU 10-yr:</strong> {p['AU10Y']} &nbsp; | &nbsp;
<strong>RBA cash:</strong> {p['RBA_Cash_Rate']} &nbsp; | &nbsp; <strong>Next move prob:</strong> {p['Next_Move_Prob']}</p>
<p><em>{p['take']}</em></p>
""".strip()

    rss = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
<channel>
<title>{escape(NEWSLETTER_NAME)} — Macro in a Minute</title>
<link>{escape(BASE_URL)}</link>
<description>Tiny daily macro block for Beehiiv. Never empty; em dashes if inputs missing.</description>
<language>en-au</language>
<lastBuildDate>{now_rfc}</lastBuildDate>
<ttl>15</ttl>
<item>
<title>Macro in a Minute — {datetime.utcnow().strftime('%Y-%m-%d')}</title>
<link>{escape(BASE_URL)}</link>
<guid isPermaLink="false">{escape(BASE_URL)}/macro-{datetime.utcnow().strftime('%Y%m%d')}</guid>
<pubDate>{now_rfc}</pubDate>
<description><![CDATA[ {body_html} ]]></description>
<content:encoded><![CDATA[ {body_html} ]]></content:encoded>
<category>macro</category>
</item>
</channel>
</rss>
"""
    with open("beehiiv_macro.xml", "w", encoding="utf-8") as f:
        f.write(rss)
    logging.info("Wrote beehiiv_macro.xml")

# ---- Main write --------------------------------------------------------------

def main():
    conn = db()
    rows = read_recent(conn)
    if not rows:
        logging.info("No eligible items; still write macro.")
        write_macro_feed()
        # also write an empty top for Beehiiv to accept
        with open("beehiiv_top.xml","w",encoding="utf-8") as f:
            f.write(build_rss(f"{NEWSLETTER_NAME} — Top", []))
        return

    # GROUP by category
    by_cat = {}
    for r in rows:
        cat = r["category_main"] or "macro"
        by_cat.setdefault(cat, []).append(r)

    # Top feed (budget ~5 min read)
    # Sort by (score desc, recency), light publisher bias already in score
    all_sorted = sorted(rows, key=lambda r: (int(r["score"] or 0), r["published_at"]), reverse=True)
    bag, total_words = [], 0
    for r in all_sorted:
        w = words(r["ai_desc_html"] or "")
        if total_words + w > TOP_WORD_BUDGET:
            continue
        bag.append(r); total_words += w
    with open("beehiiv_top.xml","w",encoding="utf-8") as f:
        f.write(build_rss(f"{NEWSLETTER_NAME} — Top", bag))
    logging.info("Wrote beehiiv_top.xml (items=%d, ~%d words)", len(bag), total_words)

    # Per-category feeds
    for cat, (label, filename) in CATEGORY_META.items():
        items = by_cat.get(cat, [])
        if not items:
            continue
        items = sorted(items, key=lambda r: (int(r["score"] or 0), r["published_at"]), reverse=True)
        # Soft cap per feed: keep it snackable (~600–800 words)
        picked, wc = [], 0
        for r in items:
            w = words(r["ai_desc_html"] or "")
            if wc + w > 800: 
                continue
            picked.append(r); wc += w
        with open(filename, "w", encoding="utf-8") as f:
            f.write(build_rss(f"{NEWSLETTER_NAME} — {label}", picked))
        logging.info("Wrote %s (cat=%s, items=%d, ~%d words)", filename, cat, len(picked), wc)

    # Always write macro block
    write_macro_feed()

if __name__ == "__main__":
    main()
