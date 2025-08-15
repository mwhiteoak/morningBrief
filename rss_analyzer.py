#!/usr/bin/env python3
"""
Morning Briefing — AU CRE Intelligence (Beehiiv-ready)
- Fetches feeds (incremental, parallel)
- Filters & scores with AU/role weighting
- Summarises with OpenAI (throttled, cached, no hallucinations)
- Stores everything in sqlite: rss_items.db
"""

import os, re, sqlite3, time, json, logging, socket, hashlib, concurrent.futures
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urlunparse, urlencode, parse_qsl
from dataclasses import dataclass
from typing import List, Dict, Optional

import feedparser
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# ---------- Tunables via env ----------
LOG_LEVEL         = os.getenv("LOG_LEVEL", "INFO").upper()
OPENAI_MODEL      = os.getenv('OPENAI_MODEL', 'gpt-4o-mini')  # fast + good
NEWSLETTER_NAME   = os.getenv('NEWSLETTER_NAME', 'Morning Briefing')
OAI_RPM           = int(os.getenv('OAI_RPM', '60'))           # requests/min cap
OAI_MAX_RETRIES   = int(os.getenv('OAI_MAX_RETRIES', '3'))
OAI_TIMEOUT_SECS  = int(os.getenv("OAI_TIMEOUT_SECS", "15"))

MAX_WORKERS       = int(os.getenv("MAX_WORKERS", "12"))       # feed fetch concurrency
FEED_TIMEOUT      = int(os.getenv("FEED_TIMEOUT", "10"))      # per-feed seconds
SCAN_WINDOW_HRS   = int(os.getenv("SCAN_WINDOW_HRS", "18"))   # recency window
MIN_SCORE_FOR_AI  = int(os.getenv("MIN_SCORE_FOR_AI", "7"))   # skip AI below this
MAX_AI_ITEMS      = int(os.getenv("MAX_AI_ITEMS", "28"))      # hard cap per run
# --------------------------------------

# ---- CRE hard filters for speed/precision ----
HARD_FILTER_TERMS = [
    "REIT","A-REIT","AREIT","cap rate","yield","WALE","vacancy","leasing","pre-commit",
    "industrial","logistics","retail","office","hospitality","student","data centre",
    "development","DA","rezoning","acquisition","disposal","portfolio","mandate","JV",
    "anchor tenant","sqm","hectare","NLA","IRR","stabilised","value-add","core-plus",
    "transaction","deal","fund","asset","valuation","auction","campaign","leasing deal",
    "loan","refinance","capital raise","LVR","covenant",
    # AU tickers/names
    "Dexus","DXS","GPT","Mirvac","MGR","Stockland","SGP","Charter Hall","CHC","Centuria",
    "CIP","COF","CNI","Cromwell","CMW","Goodman","GMG","Lendlease","LLC","Scentre","SCG",
    "Vicinity","VCX","HomeCo","HMC","HMC Capital","Arena REIT","ARF","BWP","BWP Trust",
    "Waypoint","WPR","Carindale","CDP","Rialto","Elanor","ENX"
]

TRUSTED_SOURCES = {
    "Real Commercial","The Urban Developer","AFR - Commercial Property",
    "Business News - Property","Real Commercial","realestate.com.au News",
    "Domain Business","PCA News","Business News - Retail"
}
# ----------------------------------------------

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s"
)
socket.setdefaulttimeout(FEED_TIMEOUT)

# Your feeds list
from feeds import RSS_FEEDS

@dataclass
class Item:
    link: str
    link_canonical: str
    source_name: str
    source_feed: str
    title: str
    raw_summary: str
    published_at: Optional[str]
    score: int

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def to_iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.astimezone(timezone.utc).isoformat() if dt else None

def parse_rfc2822(dt_struct) -> Optional[datetime]:
    if not dt_struct:
        return None
    try:
        return datetime(*dt_struct[:6], tzinfo=timezone.utc)
    except Exception:
        return None

def canonicalise_url(url: str) -> str:
    try:
        u = urlparse(url)
        # drop tracking
        q = [(k, v) for (k, v) in parse_qsl(u.query, keep_blank_values=True)
             if not k.lower().startswith(("utm_", "gclid", "fbclid", "mc_eid"))]
        new_q = urlencode(q, doseq=True)
        scheme = "https" if u.scheme in ("http", "https") else u.scheme
        canon = urlunparse((scheme, u.netloc.lower(), u.path, "", new_q, ""))
        return canon.rstrip("/")
    except Exception:
        return url

def fingerprint(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", "ignore")).hexdigest()[:16]

def looks_like_cre(title: str, summary: str, source_name: str) -> bool:
    if source_name in TRUSTED_SOURCES:
        return True
    blob = f"{title} {summary}".lower()
    for term in HARD_FILTER_TERMS:
        if term.lower() in blob:
            return True
    return False

def score_item(title: str, source_name: str) -> int:
    t = title.lower()
    score = 0
    # base AU CRE boosts
    for k in ["reit","yield","cap rate","leasing","pre-commit","acquisition","sale","portfolio","loan","refinance","dexus","gpt","mirvac","stockland","charter hall","goodman","vicinity","scentre"]:
        if k in t: score += 2
    # source boost
    if source_name in TRUSTED_SOURCES: score += 3
    # deal/dev keywords
    for k in ["buys","sells","acquires","acquisition","approval","da ","rezoning","mandate","fund","raises","recapitalisation","joint venture","jv"]:
        if k in t: score += 1
    return min(score, 10)

# ------------- DB -------------
def connect_db(path: str="rss_items.db") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    return conn

def ensure_schema(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS items(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT,                 -- legacy
        url_canonical TEXT,
        link TEXT,
        link_canonical TEXT UNIQUE,
        source_name TEXT,
        source_feed TEXT,
        title TEXT,
        raw_summary TEXT,
        published_at TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        processed_at TEXT,
        score INTEGER,
        interest_score INTEGER,
        category TEXT,
        ai_title TEXT,
        ai_summary TEXT,
        description TEXT,
        ai_package TEXT,
        fp TEXT,
        exported_to_rss INTEGER DEFAULT 0,
        export_batch TEXT
    );
    """)
    # add missing columns defensively
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(items)")}
    want = {
        "url","url_canonical","link","link_canonical","source_name","source_feed","title",
        "raw_summary","published_at","created_at","processed_at","score","interest_score",
        "category","ai_title","ai_summary","description","ai_package","fp","exported_to_rss","export_batch"
    }
    missing = [c for c in want if c not in cols]
    for c in missing:
        conn.execute(f"ALTER TABLE items ADD COLUMN {c} TEXT;")
        logging.info("DB: added missing column %s", c)
    conn.commit()

# ------------- OpenAI -------------
client = OpenAI(timeout=OAI_TIMEOUT_SECS)

SYS_PROMPT = (
    "You are an assistant rewriting RSS snippets strictly from the provided source text, "
    "for an Australian commercial real estate audience (asset managers, fund managers, developers). "
    "Do NOT invent facts. If details are missing, keep it high-level or return skip:true.\n"
    "Tone: sharp, Shaan Puri-esque: punchy, practical, a touch cheeky.\n"
    "Return ONLY JSON with keys: {skip, title, desc, category}. "
    "Categories: one of [top, deals, development, finance, policy, areit, retail, industrial, other]."
)

def ai_rewrite(pkg: Dict) -> Optional[Dict]:
    prompt = {
        "role": "user",
        "content": json.dumps(pkg, ensure_ascii=False)
    }
    for attempt in range(OAI_MAX_RETRIES):
        try:
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role":"system","content":SYS_PROMPT},
                    prompt
                ],
                temperature=0.4,
                max_tokens=280,
                response_format={"type": "json_object"}
            )
            text = resp.choices[0].message.content.strip()
            data = json.loads(text)
            if isinstance(data, dict):
                return data
            return None
        except Exception as e:
            if attempt < OAI_MAX_RETRIES - 1:
                time.sleep(0.8 * (attempt + 1))
                continue
            logging.warning("AI error (final): %s", e)
            return None

# ------------- Fetch & Process -------------
def fetch_feed(feed) -> List[Item]:
    name, url = feed["name"], feed["url"]
    try:
        d = feedparser.parse(url)
        items: List[Item] = []
        cutoff = now_utc() - timedelta(hours=SCAN_WINDOW_HRS)
        for e in d.entries:
            link = e.get("link") or e.get("id") or ""
            if not link:
                continue
            pub = parse_rfc2822(e.get("published_parsed")) or parse_rfc2822(e.get("updated_parsed"))
            if pub and pub < cutoff:
                continue
            title = (e.get("title") or "").strip()
            summary = (e.get("summary") or e.get("description") or "").strip()
            can = canonicalise_url(link)
            items.append(Item(
                link=link,
                link_canonical=can,
                source_name=name,
                source_feed=url,
                title=title,
                raw_summary=summary,
                published_at=to_iso(pub) if pub else None,
                score=score_item(title, name),
            ))
        return items
    except Exception as e:
        logging.error("Fetch error %s: %s", name, e)
        return []

def fetch_all(feeds: List[Dict]) -> List[Item]:
    t0 = time.time()
    out: List[Item] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for items in pool.map(fetch_feed, feeds):
            out.extend(items)
    logging.info("Fetched recent items from %d feeds (items=%d) in %.1fs",
                 len(feeds), len(out), time.time() - t0)
    return out

def process_items(conn: sqlite3.Connection, items: List[Item]):
    t_ai = time.time()
    kept_for_ai, saved = 0, 0
    ai_used = 0
    calls_in_window = 0
    window_started = time.time()

    for it in items:
        fp = fingerprint(it.link_canonical or it.link)
        # initial insert
        conn.execute("""
            INSERT OR IGNORE INTO items(link, link_canonical, url, url_canonical,
                source_name, source_feed, title, raw_summary, published_at,
                created_at, score, interest_score, fp, exported_to_rss)
            VALUES(?,?,?,?,?,?,?,?,?, datetime('now'), ?, ?, ?, 0)
        """, (it.link, it.link_canonical, it.link, it.link_canonical,
              it.source_name, it.source_feed, it.title, it.raw_summary, it.published_at,
              it.score, it.score, fp))
        conn.commit()

        # fetch row
        row = conn.execute("SELECT * FROM items WHERE link_canonical=?", (it.link_canonical,)).fetchone()
        if not row:
            continue

        already_done = bool(row["ai_summary"])
        eligible = (
            it.score >= MIN_SCORE_FOR_AI and
            looks_like_cre(it.title, it.raw_summary, it.source_name)
        )

        if already_done or not eligible or ai_used >= MAX_AI_ITEMS:
            continue

        # simple RPM throttle
        calls_in_window += 1
        elapsed = time.time() - window_started
        if calls_in_window >= OAI_RPM:
            sleep_for = max(0.0, 60.0 - elapsed)
            if sleep_for > 0:
                time.sleep(sleep_for)
            window_started = time.time()
            calls_in_window = 0

        # AI call
        pkg = {
            "source_name": it.source_name,
            "source_title": it.title,
            "source_summary": it.raw_summary,
            "constraints": {
                "no_hallucinations": True,
                "aussie_english": True,
                "max_words": 140
            }
        }
        res = ai_rewrite(pkg)
        ai_used += 1
        if not res or res.get("skip") is True:
            continue

        ai_title = (res.get("title") or "").strip()
        ai_desc  = (res.get("desc") or "").strip()
        cat      = (res.get("category") or "other").strip().lower()

        conn.execute("""
            UPDATE items SET
              processed_at = datetime('now'),
              ai_title = ?,
              ai_summary = ?,
              description = ?,
              category = ?,
              ai_package = ?,
              interest_score = COALESCE(interest_score, score)
            WHERE id = ?
        """, (ai_title, ai_desc, ai_desc, cat, json.dumps(res, ensure_ascii=False), row["id"]))
        conn.commit()
        kept_for_ai += 1

    logging.info("TIMING: ai phase %.1fs | ai_calls=%d", time.time() - t_ai, ai_used)
    logging.info("SUMMARY: scanned=%d new/updated=%d kept_for_ai=%d",
                 len(items), saved, kept_for_ai)

def main():
    conn = connect_db(os.getenv("RSS_DB_PATH","rss_items.db"))
    t0 = time.time()
    items = fetch_all(RSS_FEEDS)
    logging.info("TIMING: fetch phase %.1fs", time.time() - t0)
    process_items(conn, items)
    logging.info("TIMING: total %.1fs", time.time() - t0)

if __name__ == "__main__":
    main()
