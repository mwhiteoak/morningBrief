#!/usr/bin/env python3
"""
Write multiple Beehiiv-friendly RSS feeds from sqlite (rss_items.db):

- beehiiv_top.xml
- beehiiv_deals.xml
- beehiiv_dev.xml
- beehiiv_finance.xml
- beehiiv_policy.xml
- beehiiv_areit.xml
- beehiiv_macro.xml  (tiny block; best-effort market snapshot; shows "—" on missing)

Rules:
- Only include items with AI content (ai_summary present and non-trivial)
- No hallucinations: descriptions are AI rewrites of feed text; already enforced upstream
- XML-safe (CDATA + entity escaping)
"""

import os, re, sqlite3, logging, json
from datetime import datetime, timedelta, timezone
from xml.sax.saxutils import escape as xml_escape

import httpx  # used for macro best-effort

LOG_LEVEL        = os.getenv("LOG_LEVEL","INFO").upper()
RSS_DB_PATH      = os.getenv("RSS_DB_PATH","rss_items.db")
OUT_PREFIX       = os.getenv("OUT_PREFIX","beehiiv")
NEWSLETTER_NAME  = os.getenv("NEWSLETTER_NAME","Morning Briefing")
BASE_URL         = os.getenv("BASE_URL","https://example.com")
SCAN_WINDOW_HRS  = int(os.getenv("SCAN_WINDOW_HRS","24"))
MAX_ITEMS_PER_FEED = int(os.getenv("MAX_ITEMS_PER_FEED","80"))

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s - %(levelname)s - %(message)s")

def rfc822(dt: datetime) -> str:
    return dt.strftime("%a, %d %b %Y %H:%M:%S %z")

def parse_iso(dt_s: str) -> datetime:
    try:
        # always force timezone-aware
        if dt_s.endswith("Z"):
            return datetime.fromisoformat(dt_s.replace("Z","+00:00"))
        return datetime.fromisoformat(dt_s).astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)

def cdata(txt: str) -> str:
    if txt is None:
        txt = ""
    # ensure "]]>" safe
    safe = txt.replace("]]>", "]]]]><![CDATA[>")
    return f"<![CDATA[ {safe} ]]>"

def clean_html(s: str) -> str:
    # We keep it simple: remove script/style, leave basic tags
    s = re.sub(r"(?is)<(script|style).*?>.*?</\1>", "", s)
    return s.strip()

def clamp_words(s: str, max_words: int = 130) -> str:
    words = re.split(r"\s+", s.strip())
    if len(words) <= max_words:
        return s.strip()
    return " ".join(words[:max_words]) + "…"

# ---- DB ----
def connect_db():
    conn = sqlite3.connect(RSS_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def read_recent(conn):
    since = datetime.now(timezone.utc) - timedelta(hours=SCAN_WINDOW_HRS)
    rows = conn.execute("""
        SELECT *
        FROM items
        WHERE ai_summary IS NOT NULL
          AND length(trim(ai_summary)) > 20
          AND datetime(COALESCE(processed_at, created_at)) >= datetime(?)
    """, (since.astimezone(timezone.utc).isoformat(),)).fetchall()
    out = []
    for r in rows:
        pub = None
        if r["published_at"]:
            try:
                pub = parse_iso(r["published_at"])
            except Exception:
                pub = None
        out.append({
            "title": r["title"] or "",
            "ai_title": r["ai_title"] or "",
            "desc": r["ai_summary"] or "",
            "category": (r["category"] or "other").lower(),
            "source_name": r["source_name"] or "",
            "link": r["link_canonical"] or r["link"] or r["url_canonical"] or r["url"] or "",
            "pub_dt": pub,
            "interest": int(r["interest_score"] or 0),
        })
    return out

# ---- Classify buckets (heuristics + AI category) ----
AREIT_NAMES = ["dexus","gpt","mirvac","stockland","charter hall","goodman","scentre","vicinity","homeco","hmc","arena","bwp","waypoint","cromwell","centuria","carindale"]

def classify_bucket(it):
    cat = it["category"]
    title_blob = f"{it['ai_title']} {it['title']}".lower()
    if cat in {"deals","deal"} or any(k in title_blob for k in ["acquires","acquisition","buys","sells","mandate","fund","raises","recapitalisation","portfolio","sale","sold","purchases","picks up","offloads"]):
        return "deals"
    if cat in {"development","dev"} or any(k in title_blob for k in ["da ","approval","rezon","development","tower","apartments","residential project"]):
        return "development"
    if cat in {"finance"} or any(k in title_blob for k in ["loan","refinance","bond","debt","interest rate","valuation","cap rate","yield"]):
        return "finance"
    if cat in {"policy"} or any(k in title_blob for k in ["rba","budget","policy","government","planning minister","inquiry","regulator","asic","accc","treasury"]):
        return "policy"
    if cat in {"areit"} or any(n in title_blob for n in AREIT_NAMES) or "reit" in title_blob:
        return "areit"
    if cat in {"retail"}:
        return "retail"
    if cat in {"industrial"}:
        return "industrial"
    if cat in {"top"}:
        return "top"
    return "other"

# ---- Render RSS ----
def render_feed(title_suffix: str, items, channel_desc="Daily AU commercial property briefing — assets, funds, deals, development."):
    now = datetime.now(timezone.utc)
    out = []
    out.append('<?xml version="1.0" encoding="UTF-8"?>')
    out.append('<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">')
    out.append("<channel>")
    out.append(f"<title>{xml_escape(NEWSLETTER_NAME)} · {xml_escape(title_suffix)}</title>")
    out.append(f"<link>{xml_escape(BASE_URL)}</link>")
    out.append(f"<description>{xml_escape(channel_desc)}</description>")
    out.append("<language>en-au</language>")
    out.append(f"<lastBuildDate>{rfc822(now)}</lastBuildDate>")
    out.append("<ttl>60</ttl>")
    for it in items[:MAX_ITEMS_PER_FEED]:
        title = it["ai_title"].strip() or it["title"].strip()
        desc = clamp_words(clean_html(it["desc"] or ""), 140)
        link = it["link"]
        pub = rfc822(it["pub_dt"]) if it["pub_dt"] else rfc822(now)
        if not desc or len(desc) < 20:
            continue
        # Build item
        out.append("<item>")
        out.append(f"<title>{cdata(title)}</title>")
        out.append(f"<link>{cdata(link)}</link>")
        out.append(f'<guid isPermaLink="false">{cdata(link)}</guid>')
        out.append(f"<pubDate>{pub}</pubDate>")
        out.append("<description>")
        out.append(cdata(f"<p>{desc}</p>"))
        out.append("</description>")
        out.append("<content:encoded>")
        out.append(cdata(f"<p>{desc}</p>"))
        out.append("</content:encoded>")
        # simple categories
        out.append(f"<category>{xml_escape(classify_bucket(it))}</category>")
        if it.get("source_name"):
            out.append(f"<category>{xml_escape(it['source_name'])}</category>")
        out.append("</item>")
    out.append("</channel>")
    out.append("</rss>")
    return "\n".join(out)

# ---- Macro feed (best-effort) ----
def fetch_quote(symbols: list[str]) -> dict:
    out = {}
    try:
        s = ",".join(symbols)
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={s}"
        r = httpx.get(url, timeout=8)
        r.raise_for_status()
        data = r.json().get("quoteResponse",{}).get("result",[])
        for q in data:
            sym = q.get("symbol")
            out[sym] = {
                "price": q.get("regularMarketPrice"),
                "chgPct": q.get("regularMarketChangePercent")
            }
    except Exception:
        pass
    return out

def write_macro_feed(path: str):
    # Symbols: ASX200 (^AXJO), AUDUSD (AUDUSD=X)
    q = fetch_quote(["^AXJO","AUDUSD=X"])
    asx = q.get("^AXJO",{})
    aud = q.get("AUDUSD=X",{})
    au10y = "—"   # left as em dash if not fetched
    rba = "—"
    prob = "—"

    one_liner = "Neutral morning."
    try:
        if isinstance(asx.get("chgPct"), (float,int)) and isinstance(aud.get("chgPct"), (float,int)):
            tone = []
            if asx["chgPct"] >= 0.3: tone.append("risk-on; equities bid")
            elif asx["chgPct"] <= -0.3: tone.append("risk-off; equities soft")
            if aud["chgPct"] <= -0.2: tone.append("USD bid; import costs rise")
            elif aud["chgPct"] >= 0.2: tone.append("AUD firmer; imported costs ease")
            one_liner = "; ".join(tone) or one_liner
    except Exception:
        pass

    now = datetime.now(timezone.utc)
    items = [{
        "ai_title": "Macro in a Minute",
        "title": "Macro in a Minute",
        "desc": f"""
        <ul>
          <li>ASX200: <strong>{asx.get('price','—')}</strong> ({asx.get('chgPct','—')}%)</li>
          <li>AUD/USD: <strong>{aud.get('price','—')}</strong> ({aud.get('chgPct','—')}%)</li>
          <li>AU 10-yr: <strong>{au10y}</strong></li>
          <li>RBA cash rate: <strong>{rba}</strong> | Next move prob: <strong>{prob}</strong></li>
        </ul>
        <p><em>{one_liner}</em></p>
        """,
        "link": f"{BASE_URL}",
        "pub_dt": now,
        "source_name": "macro",
        "category": "macro",
        "interest": 99
    }]
    xml = render_feed("Macro", items, channel_desc="Macro snapshot: ASX200, AUD/USD, AU 10-yr, RBA cash & odds.")
    with open(path, "w", encoding="utf-8") as f:
        f.write(xml)
    logging.info("Wrote %s", path)

# ---- Main ----
def main():
    conn = connect_db()
    items = read_recent(conn)
    if not items:
        logging.warning("No eligible AI items in window; writing empty shells with zero items.")
    # Sort master by (interest desc, pub recency)
    items.sort(key=lambda x: (x["interest"], x["pub_dt"] or datetime.now(timezone.utc)), reverse=True)

    # Partition
    buckets = {"top":[], "deals":[], "development":[], "finance":[], "policy":[], "areit":[], "other":[]}
    for it in items:
        buckets.setdefault(classify_bucket(it), []).append(it)
        buckets["top"].append(it)  # top is “best of all”

    # Render each
    out_map = [
        ("Top",        buckets["top"],          f"{OUT_PREFIX}_top.xml"),
        ("Deals",      buckets["deals"],        f"{OUT_PREFIX}_deals.xml"),
        ("Development",buckets["development"],  f"{OUT_PREFIX}_dev.xml"),
        ("Finance",    buckets["finance"],      f"{OUT_PREFIX}_finance.xml"),
        ("Policy",     buckets["policy"],       f"{OUT_PREFIX}_policy.xml"),
        ("A-REIT",     buckets["areit"],        f"{OUT_PREFIX}_areit.xml"),
    ]
    for suffix, bucket, filename in out_map:
        xml = render_feed(suffix, bucket)
        with open(filename, "w", encoding="utf-8") as f:
            f.write(xml)
        logging.info("Wrote %s (items=%d)", filename, min(len(bucket), MAX_ITEMS_PER_FEED))

    # Macro feed (always write)
    write_macro_feed(f"{OUT_PREFIX}_macro.xml")

if __name__ == "__main__":
    main()
