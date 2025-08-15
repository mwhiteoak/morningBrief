#!/usr/bin/env python3
"""
Morning Briefing — Analyzer
- Fetches feeds
- Scores & categorises for AU CRE audience
- Summarises/re-writes with OpenAI (en-AU, Shaan-style, but grounded)
- Persists to sqlite: rss_items.db
"""

import os, re, json, time, logging, sqlite3, concurrent.futures, socket
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse, urlunparse
import feedparser
from dotenv import load_dotenv
from openai import OpenAI
from email.utils import parsedate_to_datetime

# ----- config/env
load_dotenv()
LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO").upper()
OPENAI_MODEL     = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OAI_RPM          = int(os.getenv("OAI_RPM", "25"))        # requests/min cap
OAI_MAX_RETRIES  = int(os.getenv("OAI_MAX_RETRIES", "4"))
MAX_WORKERS      = int(os.getenv("MAX_WORKERS", "8"))
FEED_TIMEOUT     = int(os.getenv("FEED_TIMEOUT", "15"))
MIN_SCORE_FOR_AI = int(os.getenv("MIN_SCORE_FOR_AI", "3"))
ALWAYS_AI        = os.getenv("ALWAYS_AI", "1") == "1"     # force AI attempt
TZ_REGION        = os.getenv("TZ_REGION", "Australia/Brisbane")

from feeds import RSS_FEEDS  # expects list of {'name','url'}

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s"
)

client = OpenAI()
AI_SLEEP = max(0.0, 60.0 / max(1, OAI_RPM))  # basic rate-limit

DB_PATH = os.getenv("RSS_DB_PATH", "rss_items.db")

def now_aware() -> datetime:
    # sqlite stores ISO with offset; use local offset via system tz for consistency
    return datetime.now(timezone.utc)

def canonicalise(url: str) -> str:
    try:
        u = urlparse(url)
        # strip fragments; keep queries (publishers often use them)
        return urlunparse((u.scheme, u.netloc.lower(), u.path, "", u.query, ""))
    except Exception:
        return url

def ensure_db(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        link TEXT UNIQUE,
        link_canonical TEXT,
        source_name TEXT,
        source_feed TEXT,
        title TEXT,
        raw_summary TEXT,
        published_at TEXT,
        category TEXT,
        interest_score INTEGER DEFAULT 0,
        ai_title TEXT,
        ai_desc TEXT,
        ai_summary TEXT,
        ai_package TEXT,
        processed_at TEXT,
        exported_to_rss INTEGER DEFAULT 0,
        export_batch TEXT,
        fp TEXT
    );
    """)
    # Add missing columns safely
    cols = {r[1] for r in conn.execute("PRAGMA table_info(items);")}
    wanted = [
        "ai_summary","category","description","export_batch","exported_to_rss",
        "fp","interest_score","processed_at","source_feed","source_name","ai_title","ai_desc"
    ]
    # "description" not used anymore, but add to avoid legacy queries breaking
    for c in wanted:
        if c not in cols:
            conn.execute(f"ALTER TABLE items ADD COLUMN {c} TEXT;")
            logging.info("DB: added missing column %s", c)
    conn.commit()

def guess_category(source: str, title: str, summary: str) -> str:
    s = f"{source} {title} {summary}".lower()
    if any(k in s for k in [
        "realcommercial", "realestate.com.au", "domain", "pca", "urban developer",
        "commercial property", "leasing", "reit", "a-reit", "yield", "cap rate",
        "mixed-use", "industrial", "office", "retail", "logistics", "residential"
    ]):
        return "property"
    if any(k in s for k in ["rba", "bond", "asx", "markets", "inflation", "rate", "bank", "suncorp", "westpac"]):
        return "finance"
    if any(k in s for k in ["startups", "startup", "fintech", "proptech", "technology review", "lex fridman"]):
        return "startups"
    if any(k in s for k in ["wsj - world news", "geopolitics", "oil", "opec", "china", "us "]):
        return "markets"
    return "finance"  # default bias for CRE readers

def score_item(source: str, title: str, summary: str, category: str) -> int:
    s = f"{title} {summary}".lower()
    score = 0
    if category == "property":
        score += 8
    if any(k in s for k in ["reit", "a-reit", "lease", "leasing", "cap rate", "net yield", "acquisition", "portfolio", "fund"]):
        score += 6
    if any(k in s for k in ["rba", "cash rate", "ten-year", "10-yr", "bond", "inflation", "asx", "credit"]):
        score += 4
    if any(k in s for k in ["sydney", "melbourne", "brisbane", "perth", "adelaide"]):
        score += 1
    return score

def parse_published(entry) -> Optional[str]:
    # Try feedparser's fields; return ISO 8601 with offset if possible
    for key in ("published_parsed", "updated_parsed"):
        if getattr(entry, key, None):
            try:
                dt = datetime(*getattr(entry, key)[:6], tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                pass
    for key in ("published", "updated"):
        val = entry.get(key)
        if val:
            try:
                dt = parsedate_to_datetime(val)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                continue
    # Fallback: now
    return now_aware().isoformat()

def ai_rewrite_safe(title: str, summary: str, source: str, link: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[Dict]]:
    """
    Returns (ai_title, ai_desc, ai_summary, ai_package_json)
    """
    sys = (
        "You are an editorial assistant for an AU commercial real estate newsletter.\n"
        "Audience: property fund managers, REIT execs, developers, brokers.\n"
        "Tone: sharp, Shaan-Puri-esque zingers when appropriate, but never clickbait.\n"
        "Spelling: en-AU.\n"
        "Grounding: STRICTLY use only the provided title/summary text. If detail is not present, do not invent it.\n"
        "Output strict JSON only."
    )
    user = {
        "source_name": source,
        "title": title,
        "summary": summary,
        "link": link,
        "instructions": {
            "rewrite": {
                "title": "6–12 words, factual, punchy; preserve entities; no emojis.",
                "desc":  "1–2 sentences (≈45–80 words) on what happened and why CRE readers should care.",
                "style": "Shaan Puri, en-AU; crisp verbs; no fluff; no invented facts."
            },
            "categorise": "one of ['property','finance','startups','markets','policy','other']",
            "tags": "2–5 kebab-case tags"
        }
    }
    messages = [
        {"role": "system", "content": sys},
        {"role": "user", "content": json.dumps(user)}
    ]
    for attempt in range(1, OAI_MAX_RETRIES + 1):
        try:
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=messages,
                temperature=0.3,
                response_format={"type": "json_object"},
                timeout=30
            )
            raw = resp.choices[0].message.content
            pkg = json.loads(raw)
            ai_t = (pkg.get("title") or "").strip()
            ai_d = (pkg.get("desc") or pkg.get("description") or "").strip()
            ai_s = (pkg.get("summary") or ai_d or "").strip()
            # Guardrails: if desc empty or looks like refusal, treat as failure
            bad = {"insufficient", "not enough", "cannot", "no data"}
            if not ai_t or not ai_d or any(x in ai_d.lower() for x in bad):
                return None, None, None, pkg
            return ai_t, ai_d, ai_s, pkg
        except Exception as e:
            logging.warning("AI error (%d/%d): %s", attempt, OAI_MAX_RETRIES, str(e))
            time.sleep(AI_SLEEP * (attempt + 1))
    return None, None, None, None

def fetch_feed(feed: Dict) -> List[Dict]:
    name, url = feed["name"], feed["url"]
    parsed = feedparser.parse(url)
    out = []
    for e in parsed.entries:
        link = e.get("link") or ""
        title = (e.get("title") or "").strip()
        summary = (e.get("summary") or e.get("description") or "").strip()
        if not link or not title:
            continue
        out.append({
            "source_name": name,
            "source_feed": url,
            "link": link,
            "link_canonical": canonicalise(link),
            "title": title,
            "raw_summary": summary,
            "published_at": parse_published(e),
        })
    return out

def upsert_item(conn: sqlite3.Connection, item: Dict):
    conn.execute("""
    INSERT OR IGNORE INTO items(link, link_canonical, source_name, source_feed, title, raw_summary, published_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        item["link"], item["link_canonical"], item["source_name"], item["source_feed"],
        item["title"], item["raw_summary"], item["published_at"]
    ))

def update_ai_fields(conn: sqlite3.Connection, link: str, category: str, score: int,
                     ai_title: Optional[str], ai_desc: Optional[str], ai_summary: Optional[str], pkg: Optional[Dict]):
    conn.execute("""
    UPDATE items
       SET category=?,
           interest_score=?,
           ai_title=?,
           ai_desc=?,
           ai_summary=?,
           ai_package=?,
           processed_at=?
     WHERE link=?
    """, (
        category, score,
        ai_title, ai_desc, ai_summary,
        json.dumps(pkg or {}, ensure_ascii=False),
        now_aware().isoformat(),
        link
    ))

def main():
    socket.setdefaulttimeout(FEED_TIMEOUT)
    conn = sqlite3.connect(DB_PATH)
    ensure_db(conn)

    logging.info("Loaded %d feeds | MAX_WORKERS=%d FEED_TIMEOUT=%ds OAI_RPM=%d MIN_SCORE_FOR_AI=%d",
                 len(RSS_FEEDS), MAX_WORKERS, FEED_TIMEOUT, OAI_RPM, MIN_SCORE_FOR_AI)

    # Fetch all feeds
    all_items: List[Dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(fetch_feed, f) for f in RSS_FEEDS]
        for fut in concurrent.futures.as_completed(futs):
            try:
                all_items.extend(fut.result())
            except Exception as e:
                logging.error("Fetch error: %s", e)

    logging.info("Fetched recent items (items=%d)", len(all_items))

    # dedupe by canonical link
    seen = set()
    unique_items = []
    for it in all_items:
        if it["link_canonical"] in seen:
            continue
        seen.add(it["link_canonical"])
        unique_items.append(it)

    # Upsert + compute AI
    ai_attempted = 0
    saved = 0
    kept_for_ai = 0

    for it in unique_items:
        upsert_item(conn, it)

        cat = guess_category(it["source_name"], it["title"], it["raw_summary"])
        score = score_item(it["source_name"], it["title"], it["raw_summary"], cat)
        need_ai = ALWAYS_AI or (score >= MIN_SCORE_FOR_AI)

        ai_t = ai_d = ai_s = None
        pkg = None
        if need_ai:
            kept_for_ai += 1
            # basic rate limiting
            if ai_attempted:
                time.sleep(AI_SLEEP)
            ai_attempted += 1
            ai_t, ai_d, ai_s, pkg = ai_rewrite_safe(
                it["title"], it["raw_summary"] or it["title"], it["source_name"], it["link"]
            )

        update_ai_fields(conn, it["link"], cat, score, ai_t, ai_d, ai_s, pkg)
        saved += 1

    conn.commit()

    logging.info("SUMMARY: scanned=%d new_saved=%d kept_for_ai=%d ai_attempted=%d",
                 len(unique_items), saved, kept_for_ai, ai_attempted)

if __name__ == "__main__":
    main()
