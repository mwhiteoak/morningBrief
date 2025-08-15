#!/usr/bin/env python3
"""
Morning Briefing — AU CRE Intelligence (Beehiiv-ready, multi-feed)
- Fetches feeds (incremental, parallel)
- Filters & scores for AU property pros (CRE-first)
- Rewrites in a Shaan Puri-ish voice *without* inventing facts (Aussie English)
- Classifies into sections for multi-RSS output
- Stores everything in sqlite: rss_items.db
"""

import os, re, json, time, sqlite3, logging, socket, concurrent.futures
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import feedparser
from dotenv import load_dotenv
from openai import OpenAI

from feeds import RSS_FEEDS

load_dotenv()

# ---------- Tunables via env ----------
LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO").upper()
OPENAI_MODEL     = os.getenv("OPENAI_MODEL", "gpt-4o")
NEWSLETTER_NAME  = os.getenv("NEWSLETTER_NAME", "Morning Briefing")
OAI_RPM          = int(os.getenv("OAI_RPM", "25"))               # requests/min cap
OAI_MAX_RETRIES  = int(os.getenv("OAI_MAX_RETRIES", "4"))
MAX_WORKERS      = int(os.getenv("MAX_WORKERS", "8"))            # feed fetch concurrency
FEED_TIMEOUT     = int(os.getenv("FEED_TIMEOUT", "15"))          # per-feed seconds
MIN_SCORE_FOR_AI = int(os.getenv("MIN_SCORE_FOR_AI", "5"))       # below this: skip AI
TZ_REGION        = os.getenv("TZ_REGION", "Australia/Brisbane")
SCAN_WINDOW_HRS  = int(os.getenv("SCAN_WINDOW_HRS", "36"))       # look-back
# --------------------------------------

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s"
)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ---- DB helpers --------------------------------------------------------------
DB_PATH = os.getenv("RSS_DB_PATH", "rss_items.db")

def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_schema(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS items(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      url TEXT UNIQUE,
      url_canonical TEXT,
      source TEXT,
      title TEXT,
      published_at TEXT,
      raw_summary TEXT,
      score INTEGER,
      category_main TEXT,
      ai_title TEXT,
      ai_desc_html TEXT,
      ai_tags TEXT,                -- JSON list
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    # Safe schema evolutions (no-op if columns already exist)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(items)").fetchall()}
    for col, ddl in [
        ("category_main", "ALTER TABLE items ADD COLUMN category_main TEXT"),
        ("ai_title",      "ALTER TABLE items ADD COLUMN ai_title TEXT"),
        ("ai_desc_html",  "ALTER TABLE items ADD COLUMN ai_desc_html TEXT"),
        ("ai_tags",       "ALTER TABLE items ADD COLUMN ai_tags TEXT"),
    ]:
        if col not in cols:
            try: conn.execute(ddl)
            except sqlite3.OperationalError: pass
    conn.commit()

# ---- Utilities ---------------------------------------------------------------
def now_au() -> datetime:
    # naive but effective for Actions; we only need local-ish wall time
    return datetime.utcnow() + timedelta(hours=10)  # AEST (+10); set TZ_REGION in workflows if needed

def canonicalise_url(u: str) -> str:
    try:
        p = urlparse(u)
        q = [(k,v) for (k,v) in parse_qsl(p.query) if k.lower() not in {"utm_source","utm_medium","utm_campaign","utm_term","utm_content","fbclid","gclid"}]
        return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(q), ""))
    except Exception:
        return u

PUBLISHER_TIER = {
    # Tiered preference (higher is better surfacing)
    "AFR": 5, "Australian Financial Review Property": 5, "Real Commercial": 5, "The Urban Developer": 4,
    "Domain Business": 4, "realestate.com.au News": 4, "PCA News": 4, "Business News - Property": 4,
    "The Australian - Business": 3, "The Age - Business": 3, "The Age Federal Politics": 3, "ABC News": 3,
    "Small Caps": 2, "MIT Technology Review": 2, "AusFintech": 2, "Cointelegraph": 1, "WSJ - Markets": 1, "WSJ - World News": 1,
}

CRE_KEYWORDS = [
    "property","real estate","commercial","office","industrial","retail","logistics",
    "warehouse","shopping centre","shopping center","mall","leasing","vacancy","cap rate",
    "yield","transaction","acquisition","divestment","REIT","A-REIT","development","DA","planning",
    "construction","build-to-rent","BTR","student accommodation","self-storage","hotel","hospitality",
    "zoning","infrastructure","tenancy","pre-commitment","valuation","debt","lenders","syndicate","capital",
    "fund","equity","acquirer","merger","JV","residential supply","housing","auction clearance"
]

CATEGORY_MAP = {
    "deals_capital": ["deal","transaction","acquisition","divestment","sale","equity","debt","raise","JV","syndicate","fund","capital","merger"],
    "development_planning": ["development","DA","planning","approval","construction","rezoning","masterplan"],
    "leasing": ["lease","leasing","pre-commitment","tenant","tenancy","occupancy","rent"],
    "macro": ["rates","inflation","bond","employment","GDP","CPI","RBA","economy","AUD"],
    "reits": ["REIT","A-REIT","distribution","NTA","FFO","earnings","guidance","vape? nope"],
    "retail": ["retail","shopping","mall","centre","center","foot traffic"],
    "industrial": ["industrial","logistics","warehouse","sheds"],
    "office": ["office","CBD","fringe","workspace","fitted","coworking"],
    "residential": ["residential","housing","apartments","BTR","house","units"],
    "proptech_finance": ["proptech","fintech","mortgage","broker","lender","platform","token","AI","data"],
}

def score_item(title: str, summary: str, source: str) -> int:
    txt = f"{title} {summary}".lower()
    base = 0
    # CRE signal
    for kw in CRE_KEYWORDS:
        if kw in txt: base += 2
    # Publisher weight
    base += PUBLISHER_TIER.get(source, 1)
    # Obvious de-prioritisers
    if "podcast" in txt: base -= 2
    if "newsletter" in txt and "subscribe" in txt: base -= 2
    return max(0, base)

def classify_guess(title: str, summary: str) -> str:
    t = f"{title} {summary}".lower()
    best, hit = "macro", 0
    for cat, kws in CATEGORY_MAP.items():
        sc = sum(k in t for k in kws)
        if sc > hit:
            hit, best = sc, cat
    return best

# ---- OpenAI summariser (no new facts; Aussie EN) -----------------------------
SYS_PROMPT = """You are a sharp finance-desk editor for an AUSTRALIAN commercial real estate (CRE) briefing.

GOALS:
- Rewrite ONLY what’s in the provided source text (title + snippet). Do NOT add new facts, numbers or claims.
- Keep Aussie English spellings.
- Voice: energetic, Shaan Puri-ish (punchy, a zinger or two), but strictly faithful to source.
- Audience: property professionals (lenders, fund managers, asset managers, developers).
- If the source is too thin to be useful for CRE readers, mark "exclude": true.

OUTPUT JSON with fields:
{
 "ai_title": "spicy but faithful headline (<= 12 words)",
 "ai_desc_html": "<p>2–4 tight sentences. No invented facts. One fun line OK.</p>",
 "category_main": "one of [deals_capital, development_planning, leasing, macro, reits, retail, industrial, office, residential, proptech_finance]",
 "tags": ["a few", "lower_snake_case"],
 "exclude": false
}
Rules:
- No swearing or crude language.
- If the original text has no CRE relevance or is too vague, set exclude = true and ai_desc_html = "Insufficient data..."
"""

def ai_rewrite(title: str, summary: str, source: str, url: str) -> Optional[Dict]:
    user = f"""SOURCE:
title: {title}
snippet: {summary}
publisher: {source}
url: {url}
"""
    for i in range(OAI_MAX_RETRIES):
        try:
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                temperature=0.4,
                messages=[
                    {"role":"system","content":SYS_PROMPT},
                    {"role":"user","content":user}
                ]
            )
            txt = resp.choices[0].message.content.strip()
            data = json.loads(txt)
            return data
        except Exception as e:
            logging.warning("AI error (%s/%s): %s", i+1, OAI_MAX_RETRIES, e)
            time.sleep(1.5*(i+1))
    return None

# ---- Fetch & process ---------------------------------------------------------
def fetch_feed(feed: Dict) -> List[Dict]:
    url = feed["url"]
    name = feed["name"]
    out = []
    try:
        socket.setdefaulttimeout(FEED_TIMEOUT)
        fp = feedparser.parse(url)
        for e in fp.entries:
            link = canonicalise_url(e.get("link") or e.get("id") or "")
            if not link: continue
            title = (e.get("title") or "").strip()
            if not title: continue
            # prefer summary/description
            summary = (e.get("summary") or e.get("description") or "").strip()
            # published
            pub = None
            for k in ("published_parsed","updated_parsed"):
                if e.get(k):
                    pub = datetime(*e.get(k)[:6])
                    break
            if not pub:
                pub = now_au()
            out.append({
                "source": name,
                "url": link,
                "title": title,
                "summary": summary,
                "published_at": pub.isoformat()
            })
    except Exception as ex:
        logging.error("Fetch error %s: %s", name, ex)
    return out

def process_items(conn: sqlite3.Connection, items: List[Dict]) -> Tuple[int,int,int]:
    new, saved = 0, 0
    kept = 0
    with conn:
        for it in items:
            url = it["url"]
            title = it["title"]
            summary = it["summary"]
            source = it["source"]
            pub = it["published_at"]
            score = score_item(title, summary, source)
            if score < MIN_SCORE_FOR_AI:
                conn.execute("""INSERT OR IGNORE INTO items(url,url_canonical,source,title,published_at,raw_summary,score)
                                VALUES(?,?,?,?,?,?,?)""",
                             (url, canonicalise_url(url), source, title, pub, summary, score))
                continue

            data = ai_rewrite(title, summary, source, url)
            if not data:
                logging.info("AI skip (no response): %s", title[:120])
                conn.execute("""INSERT OR IGNORE INTO items(url,url_canonical,source,title,published_at,raw_summary,score)
                                VALUES(?,?,?,?,?,?,?)""",
                             (url, canonicalise_url(url), source, title, pub, summary, score))
                continue

            exclude = bool(data.get("exclude"))
            cat = data.get("category_main") or classify_guess(title, summary)
            ai_title = data.get("ai_title") or title
            ai_desc_html = data.get("ai_desc_html") or ""
            tags = json.dumps(data.get("tags") or [])

            if exclude:
                # still record but without AI body
                conn.execute("""INSERT OR IGNORE INTO items(url,url_canonical,source,title,published_at,raw_summary,score,category_main)
                                VALUES(?,?,?,?,?,?,?,?)""",
                             (url, canonicalise_url(url), source, title, pub, summary, score, cat))
            else:
                kept += 1
                conn.execute("""
                    INSERT OR REPLACE INTO items
                    (url,url_canonical,source,title,published_at,raw_summary,score,category_main,ai_title,ai_desc_html,ai_tags)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """, (url, canonicalise_url(url), source, title, pub, summary, score, cat, ai_title, ai_desc_html, tags))
            new += 1
            saved += 1
    return new, saved, kept

def main():
    conn = db()
    ensure_schema(conn)
    start = time.time()

    # fetch window
    since = now_au() - timedelta(hours=SCAN_WINDOW_HRS)

    logging.info("Loaded %d feeds | MAX_WORKERS=%s FEED_TIMEOUT=%ss OAI_RPM=%s MIN_SCORE_FOR_AI=%s",
                 len(RSS_FEEDS), MAX_WORKERS, FEED_TIMEOUT, OAI_RPM, MIN_SCORE_FOR_AI)

    items = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(fetch_feed, f) for f in RSS_FEEDS]
        for fut in concurrent.futures.as_completed(futures):
            items.extend(fut.result())

    # restrict by time window
    cut = []
    for it in items:
        try:
            if datetime.fromisoformat(it["published_at"]) >= since:
                cut.append(it)
        except Exception:
            cut.append(it)

    logging.info("Fetched recent items from %d feeds (items=%d)", len(RSS_FEEDS), len(cut))
    new, saved, kept = process_items(conn, cut)
    logging.info("Process: {'scanned': %d, 'new': %d, 'saved': %d, 'kept_for_ai': %d}", len(cut), new, saved, kept)
    logging.info("Done in %.1fs", time.time() - start)

if __name__ == "__main__":
    main()
