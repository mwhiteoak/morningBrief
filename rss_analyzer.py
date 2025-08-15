#!/usr/bin/env python3
"""
Morning Briefing — AU CRE Intelligence (Beehiiv-ready, multi-feed capable)
- Fetch feeds (parallel)
- Score for AU CRE relevance
- Summarise in a punchy Shaan-Puri-ish voice (Aussie English), NO new facts
- Store into sqlite (auto-migrates | self-heals)
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
OAI_RPM          = int(os.getenv("OAI_RPM", "25"))               # requests/min cap (sequential here)
OAI_MAX_RETRIES  = int(os.getenv("OAI_MAX_RETRIES", "4"))
MAX_WORKERS      = int(os.getenv("MAX_WORKERS", "8"))            # feed fetch concurrency
FEED_TIMEOUT     = int(os.getenv("FEED_TIMEOUT", "15"))          # per-feed seconds
MIN_SCORE_FOR_AI = int(os.getenv("MIN_SCORE_FOR_AI", "5"))       # below this: skip AI
SCAN_WINDOW_HRS  = int(os.getenv("SCAN_WINDOW_HRS", "36"))       # look-back horizon
MAX_PER_FEED     = int(os.getenv("MAX_PER_FEED", "20"))          # safety cap per feed
DB_PATH          = os.getenv("RSS_DB_PATH", "rss_items.db")
# --------------------------------------

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s"
)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ---- DB helpers (self-healing schema) ---------------------------------------
REQUIRED_COLS = [
    "url","url_canonical","source","title","published_at","raw_summary","score",
    "category_main","ai_title","ai_desc_html","ai_tags","created_at"
]

CREATE_SQL = """
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
  ai_tags TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _cols(conn) -> set:
    try:
        return {r[1] for r in conn.execute("PRAGMA table_info(items)").fetchall()}
    except Exception:
        return set()

def _create_fresh(conn):
    conn.executescript(CREATE_SQL)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_pub ON items(published_at)")
    conn.commit()

def ensure_schema() -> sqlite3.Connection:
    fresh = not os.path.exists(DB_PATH)
    conn = _connect()
    if fresh:
        logging.info("DB not found; creating fresh schema.")
        _create_fresh(conn)
        return conn

    conn.execute("CREATE TABLE IF NOT EXISTS items(id INTEGER PRIMARY KEY AUTOINCREMENT)")
    conn.commit()

    cols = _cols(conn)
    severe = any(req in ("url","title","published_at") and req not in cols for req in ["url","title","published_at"])
    if severe:
        logging.warning("DB schema mismatch (missing critical cols in 'items': %s). Recreating DB.", ", ".join(sorted(cols)))
        try:
            conn.close()
        finally:
            try: os.remove(DB_PATH)
            except FileNotFoundError: pass
        conn = _connect()
        _create_fresh(conn)
        return conn

    for col in REQUIRED_COLS:
        if col not in cols:
            try:
                if col == "created_at":
                    conn.execute("ALTER TABLE items ADD COLUMN created_at TEXT DEFAULT CURRENT_TIMESTAMP")
                elif col in ("category_main","ai_title","ai_desc_html","ai_tags","url_canonical","raw_summary","source"):
                    conn.execute(f"ALTER TABLE items ADD COLUMN {col} TEXT")
                elif col == "score":
                    conn.execute("ALTER TABLE items ADD COLUMN score INTEGER")
                else:
                    logging.warning("Missing base column '%s'; recreating DB.", col)
                    conn.close()
                    try: os.remove(DB_PATH)
                    except FileNotFoundError: pass
                    conn = _connect()
                    _create_fresh(conn)
                    return conn
            except sqlite3.OperationalError:
                logging.warning("ALTER TABLE failed for '%s'; recreating DB.", col)
                conn.close()
                try: os.remove(DB_PATH)
                except FileNotFoundError: pass
                conn = _connect()
                _create_fresh(conn)
                return conn
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_pub ON items(published_at)")
    conn.commit()
    return conn

# ---- Utilities ---------------------------------------------------------------
def now_au() -> datetime:
    # Naive AU time offset (+10) to keep it simple inside Actions runners
    return datetime.utcnow() + timedelta(hours=10)

def canonicalise_url(u: str) -> str:
    try:
        p = urlparse(u)
        q = [(k,v) for (k,v) in parse_qsl(p.query) if k.lower() not in {
            "utm_source","utm_medium","utm_campaign","utm_term","utm_content","fbclid","gclid"}]
        return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(q), ""))
    except Exception:
        return u

PUBLISHER_TIER = {
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
    "reits": ["REIT","A-REIT","distribution","NTA","FFO","earnings","guidance"],
    "retail": ["retail","shopping","mall","centre","center","foot traffic"],
    "industrial": ["industrial","logistics","warehouse","sheds"],
    "office": ["office","CBD","fringe","workspace","fitted","coworking"],
    "residential": ["residential","housing","apartments","BTR","house","units"],
    "proptech_finance": ["proptech","fintech","mortgage","broker","lender","platform","token","AI","data"],
}

def score_item(title: str, summary: str, source: str) -> int:
    txt = f"{title} {summary}".lower()
    base = 0
    for kw in CRE_KEYWORDS:
        if kw in txt: base += 2
    base += PUBLISHER_TIER.get(source, 1)
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

# ---- AI: force JSON + robust fallback ---------------------------------------
SYS_PROMPT = """You are a sharp finance-desk editor for an AUSTRALIAN commercial real estate (CRE) briefing.

GOALS:
- Rewrite ONLY what’s in the provided source text (title + snippet). Do NOT add new facts, numbers or claims.
- Keep Aussie English spellings.
- Voice: energetic, Shaan Puri-ish (punchy, a zinger), but strictly faithful to source.
- Audience: property professionals (lenders, fund managers, asset managers, developers).
- If the source is too thin to be useful for CRE readers, set "exclude": true.

Return a SINGLE JSON OBJECT ONLY (no code fences), shape:
{
 "ai_title": "spicy but faithful headline (<= 12 words)",
 "ai_desc_html": "<p>2–4 tight sentences. No invented facts. One fun line OK.</p>",
 "category_main": "one of [deals_capital, development_planning, leasing, macro, reits, retail, industrial, office, residential, proptech_finance]",
 "tags": ["a_few", "lower_snake_case"],
 "exclude": false
}
"""

def _extract_json_block(s: str) -> Optional[str]:
    if not s: return None
    # Fast path: already looks like a JSON object
    s = s.strip()
    if s.startswith("{") and s.endswith("}"):
        return s
    # Otherwise, scan for first balanced {...}
    start = s.find("{")
    if start == -1: return None
    depth = 0
    for i in range(start, len(s)):
        if s[i] == "{": depth += 1
        elif s[i] == "}":
            depth -= 1
            if depth == 0:
                return s[start:i+1]
    return None

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
                response_format={"type": "json_object"},
                messages=[
                    {"role":"system","content":SYS_PROMPT},
                    {"role":"user","content":user}
                ]
            )
            txt = (resp.choices[0].message.content or "").strip()
            try:
                return json.loads(txt)
            except Exception:
                blk = _extract_json_block(txt)
                if blk:
                    return json.loads(blk)
                raise
        except Exception as e:
            logging.warning("AI error (%s/%s): %s", i+1, OAI_MAX_RETRIES, e)
            time.sleep(1.2*(i+1))
    return None

# ---- Fetch & process ---------------------------------------------------------
def _parse_pub(e) -> Optional[datetime]:
    for k in ("published_parsed","updated_parsed"):
        if e.get(k):
            try:
                return datetime(*e.get(k)[:6])
            except Exception:
                pass
    return None

def fetch_feed(feed: Dict) -> List[Dict]:
    url = feed["url"]
    name = feed["name"]
    bag = []
    try:
        socket.setdefaulttimeout(FEED_TIMEOUT)
        fp = feedparser.parse(url)
        for e in fp.entries:
            link = canonicalise_url(e.get("link") or e.get("id") or "")
            if not link: continue
            title = (e.get("title") or "").strip()
            if not title: continue
            summary = (e.get("summary") or e.get("description") or "").strip()
            pub = _parse_pub(e) or None
            # Keep all for now; sort & trim after
            bag.append({
                "source": name,
                "url": link,
                "title": title,
                "summary": summary,
                "published_at": (pub or datetime(1970,1,1)).isoformat()
            })
    except Exception as ex:
        logging.error("Fetch error %s: %s", name, ex)

    # Sort newest first (unknown dates go last) and cap per-feed
    bag.sort(key=lambda x: x["published_at"], reverse=True)
    return bag[:MAX_PER_FEED]

def process_items(conn: sqlite3.Connection, items: List[Dict]) -> Tuple[int,int,int,int,int]:
    since = now_au() - timedelta(hours=SCAN_WINDOW_HRS)
    def _within_window(iso: str) -> bool:
        try:
            return datetime.fromisoformat(iso) >= since
        except Exception:
            return False

    scanned = len(items)
    new, saved, kept, low, excluded = 0,0,0,0,0
    with conn:
        for it in items:
            if not _within_window(it["published_at"]):
                continue
            url = it["url"]; title = it["title"]; summary = it["summary"]; source = it["source"]; pub = it["published_at"]
            score = score_item(title, summary, source)

            if score < MIN_SCORE_FOR_AI:
                low += 1
                conn.execute("""INSERT OR IGNORE INTO items(url,url_canonical,source,title,published_at,raw_summary,score)
                                VALUES(?,?,?,?,?,?,?)""",
                             (url, canonicalise_url(url), source, title, pub, summary, score))
                continue

            data = ai_rewrite(title, summary, source, url)
            if not data:
                excluded += 1
                conn.execute("""INSERT OR IGNORE INTO items(url,url_canonical,source,title,published_at,raw_summary,score)
                                VALUES(?,?,?,?,?,?,?)""",
                             (url, canonicalise_url(url), source, title, pub, summary, score))
                continue

            # Normalise / guardrails
            cat = (data.get("category_main") or classify_guess(title, summary)).strip()
            ai_title = (data.get("ai_title") or title).strip()
            ai_desc_html = (data.get("ai_desc_html") or "").strip()
            tags = json.dumps(data.get("tags") or [])
            exclude = bool(data.get("exclude"))

            # Auto-exclude thin or placeholder content
            thin = (len(re.sub(r"<[^>]+>", "", ai_desc_html).strip()) < 60) or ("insufficient data" in ai_desc_html.lower())

            if exclude or thin:
                excluded += 1
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
            new += 1; saved += 1

    return scanned, new, saved, kept, low+excluded

def main():
    conn = ensure_schema()
    start = time.time()

    logging.info("Loaded %d feeds | MAX_WORKERS=%s FEED_TIMEOUT=%ss OAI_RPM=%s MIN_SCORE_FOR_AI=%s",
                 len(RSS_FEEDS), MAX_WORKERS, FEED_TIMEOUT, OAI_RPM, MIN_SCORE_FOR_AI)

    items: List[Dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(fetch_feed, f) for f in RSS_FEEDS]
        for fut in concurrent.futures.as_completed(futures):
            items.extend(fut.result())

    logging.info("Fetched recent items from %d feeds (items=%d)", len(RSS_FEEDS), len(items))
    scanned, new, saved, kept, dropped = process_items(conn, items)
    logging.info("SUMMARY: scanned=%d new=%d saved=%d kept_for_ai=%d dropped=%d | duration=%.1fs",
                 scanned, new, saved, kept, dropped, time.time()-start)

if __name__ == "__main__":
    main()
