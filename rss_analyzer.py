#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Morning Briefing — AU Property Intelligence (Beehiiv-ready)

- Fetches feeds (parallel)
- Normalises + dedupes links
- AI: rewrites title + summary in Shaan Puri/Milk Road tone (but factual, no guesses)
- AI: classifies each item for multi-feed RSS export
- Scores for property pros (CRE bias)
- Stores everything in sqlite: rss_items.db

ENV:
  LOG_LEVEL=INFO|DEBUG
  OPENAI_API_KEY=...
  OPENAI_MODEL=gpt-4o
  OAI_RPM=25
  OAI_MAX_RETRIES=4
  MAX_WORKERS=8
  FEED_TIMEOUT=15
  MIN_SCORE_FOR_AI=5
  RSS_DB_PATH=rss_items.db
"""

from __future__ import annotations
import os, re, json, time, logging, hashlib, html, socket, concurrent.futures
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

import feedparser
import sqlite3
from dotenv import load_dotenv
from openai import OpenAI

# ---- project feeds ----
from feeds import RSS_FEEDS

load_dotenv()

LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO").upper()
OPENAI_MODEL     = os.getenv("OPENAI_MODEL", "gpt-4o")
OAI_RPM          = int(os.getenv("OAI_RPM", "25"))        # requests/min
OAI_MAX_RETRIES  = int(os.getenv("OAI_MAX_RETRIES", "4"))
MAX_WORKERS      = int(os.getenv("MAX_WORKERS", "8"))
FEED_TIMEOUT     = int(os.getenv("FEED_TIMEOUT", "15"))
MIN_SCORE_FOR_AI = int(os.getenv("MIN_SCORE_FOR_AI", "5"))

DB_PATH = os.getenv("RSS_DB_PATH", "rss_items.db")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s"
)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# -------------------------- helpers --------------------------

BAD_XML_CHARS_RE = re.compile(
    r"[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]"
)

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def clean_text(s: str) -> str:
    if not s:
        return ""
    s = html.unescape(s)
    s = BAD_XML_CHARS_RE.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def canonicalise_url(u: str) -> str:
    try:
        if not u:
            return ""
        p = urlparse(u)
        q = parse_qs(p.query, keep_blank_values=True)
        # Drop common tracking params
        for k in list(q.keys()):
            if k.lower().startswith(("utm_", "mc_", "fbclid", "gclid", "igshid")):
                q.pop(k, None)
        new_query = urlencode({k:v[0] for k,v in q.items()}, doseq=False)
        return urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, ""))  # drop fragment
    except Exception:
        return u

def fp_for_url(u: str) -> str:
    return hashlib.sha1(u.encode("utf-8", errors="ignore")).hexdigest()

def get_entry_body(entry: Any) -> str:
    # Try typical RSS fields
    cand = ""
    if getattr(entry, "summary", None):
        cand = entry.summary
    if not cand and getattr(entry, "content", None):
        try:
            cand = entry.content[0].value or ""
        except Exception:
            pass
    return clean_text(cand)

def get_entry_published(entry: Any) -> str:
    # Always store as UTC ISO 8601 with Z
    try:
        if getattr(entry, "published_parsed", None):
            tt = entry.published_parsed
        elif getattr(entry, "updated_parsed", None):
            tt = entry.updated_parsed
        else:
            return now_utc_iso()
        dt = datetime(*tt[:6], tzinfo=timezone.utc)
        return dt.replace(microsecond=0).isoformat()
    except Exception:
        return now_utc_iso()

# Simple CRE-biased score: source weight + numbers + recency-ish (handled at export)
SOURCE_WEIGHTS = {
    "Real Commercial": 6,
    "AFR - Latest": 5,
    "AFR - Commercial Property": 7,
    "The Australian - Business": 4,
    "The Age - Business": 3,
    "Business News - Property": 4,
    "realestate.com.au News": 5,
    "Real Commercial": 6,
    "The Urban Developer": 5,
    "Domain Business": 4,
    "PCA News": 4,
    "RBA Media Releases": 6,
    "WSJ - Markets": 2,
    "WSJ - World News": 1,
    "Motley Fool": 1,
    "Small Caps": 2,
    "Cointelegraph": 1,
}

def score_item(source_name: str, title: str, desc: str, category: str) -> int:
    s = SOURCE_WEIGHTS.get(source_name, 1)
    if category.lower() in ("property", "development", "deals"):
        s += 3
    if category.lower() in ("reit", "finance", "markets"):
        s += 2
    if re.search(r"\b\d{2,}\b", title or ""):
        s += 1
    if re.search(r"\$\d", desc or ""):
        s += 1
    return max(1, s)

# ------------- AI rewrite + classify (single call) -------------
AI_SYSTEM = (
    "You write for Australian commercial property professionals (developers, fund managers, REIT CIOs). "
    "Voice: Shaan Puri / Milk Road—punchy, clear, a bit witty, never cringe. "
    "Constraints: 100% factual; no speculation or hallucinations; keep finance/asset numbers intact. "
    "Aim: 10-word headline max; 60-word summary max. "
    "Classify into one of: top, property, deals, development, reit, finance, markets, policy, startups, tech, global, other."
)

AI_USER_TMPL = """Article data:
Title: {title}
Summary/body: {body}

Return ONLY JSON:
{{
  "title": "Shaan-style headline, ≤10 words, factual",
  "summary": "≤60 words, concise and useful for CRE pros",
  "category": "one_of[top,property,deals,development,reit,finance,markets,policy,startups,tech,global,other]"
}}"""

def extract_json(s: str) -> Dict[str, Any]:
    # Try raw JSON first
    try:
        return json.loads(s)
    except Exception:
        pass
    # Fallback: grab first {...} block
    m = re.search(r"\{.*\}", s, flags=re.S)
    if not m:
        return {}
    try:
        return json.loads(m.group(0))
    except Exception:
        return {}

_last_ai_ts: float = 0.0
def ai_rewrite_and_classify(title: str, body: str) -> Tuple[str, str, str]:
    global _last_ai_ts
    if not title and not body:
        return "", "", "other"

    # rate-limit to ~OAI_RPM
    min_gap = 60.0 / max(1, OAI_RPM)
    dt = time.time() - _last_ai_ts
    if dt < min_gap:
        time.sleep(min_gap - dt)

    payload = {
        "model": OPENAI_MODEL,
        "messages": [
            {"role": "system", "content": AI_SYSTEM},
            {"role": "user", "content": AI_USER_TMPL.format(title=title or "", body=body or "")}
        ],
        "temperature": 0.7,
        "max_tokens": 220,
    }

    for attempt in range(1, OAI_MAX_RETRIES + 1):
        try:
            resp = client.chat.completions.create(**payload)
            _last_ai_ts = time.time()
            content = (resp.choices[0].message.content or "").strip()
            data = extract_json(content)
            ai_title = clean_text(data.get("title") or title)
            ai_summary = clean_text(data.get("summary") or body)
            cat = (data.get("category") or "other").strip().lower()
            if cat not in {"top","property","deals","development","reit","finance","markets","policy","startups","tech","global","other"}:
                cat = "other"
            return ai_title, ai_summary, cat
        except Exception as e:
            logging.warning(f"AI error ({attempt}/{OAI_MAX_RETRIES}): {e}")
            time.sleep(2 * attempt)

    # fallback: trimmed originals
    fallback_title = (title or "")[:120]
    fallback_sum = " ".join((body or "").split()[:60])
    return clean_text(fallback_title), clean_text(fallback_sum), "other"

# -------------------------- DB schema --------------------------

CREATE_ITEMS_SQL = """
CREATE TABLE IF NOT EXISTS items (
  id INTEGER PRIMARY KEY,
  url TEXT UNIQUE,
  url_canonical TEXT,
  fp TEXT,                           -- fingerprint of canonical url
  source_name TEXT,
  source_feed TEXT,
  title TEXT,
  description TEXT,
  ai_title TEXT,
  ai_summary TEXT,
  category TEXT,
  score INTEGER,
  interest_score INTEGER,
  published_at TEXT,                 -- UTC ISO (with Z or +00:00)
  processed_at TEXT DEFAULT (datetime('now')),
  exported_to_rss INTEGER DEFAULT 0, -- 0/1
  export_batch TEXT                  -- e.g., YYYYMMDD
);
"""

REQUIRED_COLS = {
    "url","url_canonical","fp","source_name","source_feed","title","description",
    "ai_title","ai_summary","category","score","interest_score","published_at",
    "processed_at","exported_to_rss","export_batch"
}

def ensure_schema(conn: sqlite3.Connection):
    conn.execute(CREATE_ITEMS_SQL)
    cur = conn.execute("PRAGMA table_info(items)")
    have = {r[1] for r in cur.fetchall()}
    missing = REQUIRED_COLS - have
    for col in sorted(missing):
        # minimal types
        coltype = "TEXT"
        if col in {"score","interest_score","exported_to_rss"}:
            coltype = "INTEGER"
        sql = f"ALTER TABLE items ADD COLUMN {col} {coltype};"
        conn.execute(sql)
        logging.info(f"DB: added missing column {col}")
    conn.commit()

# ----------------------- feed fetching ------------------------

def fetch_one(feed: Dict[str,str]) -> Tuple[Dict[str,str], List[Any]]:
    url = feed.get("url")
    socket.setdefaulttimeout(FEED_TIMEOUT)
    try:
        parsed = feedparser.parse(url)
        return feed, (parsed.entries or [])
    except Exception as e:
        logging.error(f"Fetch error {feed.get('name','?')}: {e}")
        return feed, []

def upsert_item(conn: sqlite3.Connection, row: Dict[str, Any]):
    conn.execute(
        """
        INSERT INTO items
          (url,url_canonical,fp,source_name,source_feed,title,description,ai_title,ai_summary,category,score,interest_score,published_at)
        VALUES
          (:url,:url_canonical,:fp,:source_name,:source_feed,:title,:description,:ai_title,:ai_summary,:category,:score,:interest_score,:published_at)
        ON CONFLICT(url) DO UPDATE SET
          url_canonical=excluded.url_canonical,
          fp=excluded.fp,
          source_name=excluded.source_name,
          source_feed=excluded.source_feed,
          title=COALESCE(items.title, excluded.title),
          description=CASE WHEN (items.description IS NULL OR items.description='') THEN excluded.description ELSE items.description END,
          ai_title=CASE WHEN (items.ai_title IS NULL OR items.ai_title='') THEN excluded.ai_title ELSE items.ai_title END,
          ai_summary=CASE WHEN (items.ai_summary IS NULL OR items.ai_summary='') THEN excluded.ai_summary ELSE items.ai_summary END,
          category=CASE WHEN (items.category IS NULL OR items.category='') THEN excluded.category ELSE items.category END,
          score=MAX(items.score, excluded.score),
          interest_score=MAX(items.interest_score, excluded.interest_score),
          published_at=COALESCE(items.published_at, excluded.published_at);
        """,
        row
    )

def process_feed_entries(conn: sqlite3.Connection, feed_meta: Dict[str,str], entries: List[Any]) -> Tuple[int,int,int]:
    new, kept, ai_calls = 0, 0, 0
    src_name = feed_meta.get("name") or urlparse(feed_meta.get("url","")).netloc
    src_feed = feed_meta.get("url") or ""

    for e in entries:
        link = clean_text(getattr(e, "link", "") or "")
        if not link:
            continue
        url_canon = canonicalise_url(link)
        body = get_entry_body(e)
        title = clean_text(getattr(e, "title", "") or "")
        published_at = get_entry_published(e)

        # Decide if we even bother the model
        base_cat = "property" if "realcommercial" in link or "realestate.com.au" in link else "other"
        base_score = score_item(src_name, title, body, base_cat)

        if base_score >= MIN_SCORE_FOR_AI:
            ai_title, ai_summary, cat = ai_rewrite_and_classify(title, body)
            ai_calls += 1
        else:
            ai_title, ai_summary, cat = title, " ".join(body.split()[:60]), base_cat

        score = score_item(src_name, ai_title or title, ai_summary or body, cat)
        row = {
            "url": link,
            "url_canonical": url_canon,
            "fp": fp_for_url(url_canon),
            "source_name": src_name,
            "source_feed": src_feed,
            "title": title,
            "description": body,
            "ai_title": ai_title,
            "ai_summary": ai_summary,
            "category": cat,
            "score": score,
            "interest_score": score,  # alias for now
            "published_at": published_at,
        }
        upsert_item(conn, row)
        kept += 1

    conn.commit()
    return new, kept, ai_calls

# ----------------------------- main ---------------------------

def main():
    conn = sqlite3.connect(DB_PATH)
    ensure_schema(conn)

    logging.info(f"Loaded {len(RSS_FEEDS)} feeds | MAX_WORKERS={MAX_WORKERS} FEED_TIMEOUT={FEED_TIMEOUT}s OAI_RPM={OAI_RPM} MIN_SCORE_FOR_AI={MIN_SCORE_FOR_AI}")

    total_items = 0
    total_kept = 0
    total_ai = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(fetch_one, feed) for feed in RSS_FEEDS]
        results = [f.result() for f in futures]

    for feed_meta, entries in results:
        total_items += len(entries)
        _, kept, ai_calls = process_feed_entries(conn, feed_meta, entries)
        total_kept += kept
        total_ai += ai_calls

    logging.info(f"SUMMARY: scanned={total_items} saved/updated={total_kept} ai_calls={total_ai}")

if __name__ == "__main__":
    main()
