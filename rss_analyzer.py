#!/usr/bin/env python3
"""
Fetch → AI-filter → AI-rewrite → store in sqlite (rss_items.db)
Designed for AU commercial property professionals.

- Heuristic prefilter trims obvious noise fast
- AI filter + rewrite only on likely-relevant items
- Rewrites are grounded strictly on original feed text
- Output later via write_beehiiv_feed.py
"""

import os, re, json, time, hashlib, sqlite3, logging, feedparser
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Tuple, Optional
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

from dotenv import load_dotenv
from openai import OpenAI
from feeds import RSS_FEEDS

load_dotenv()

# ---------- Config ----------
LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO").upper()
RSS_DB_PATH      = os.getenv("RSS_DB_PATH", "rss_items.db")
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL     = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # fast + good
OAI_RPM          = int(os.getenv("OAI_RPM", "25"))           # throttle
OAI_MAX_RETRIES  = int(os.getenv("OAI_MAX_RETRIES", "4"))
SCAN_WINDOW_HRS  = int(os.getenv("SCAN_WINDOW_HRS", "24"))
MAX_WORKERS      = int(os.getenv("MAX_WORKERS", "6"))
FEED_TIMEOUT     = int(os.getenv("FEED_TIMEOUT", "15"))
ALWAYS_AI        = os.getenv("ALWAYS_AI", "1") == "1"
MIN_SCORE_FOR_AI = int(os.getenv("MIN_SCORE_FOR_AI", "4"))   # heuristic gate
TZ               = timezone(timedelta(hours=10))             # Australia/Brisbane (no DST)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s"
)

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ---------- Heuristic scoring ----------
PROPERTY_WEIGHTED_SOURCES = {
    "AFR - Commercial Property": 3,
    "Real Commercial": 3,
    "The Urban Developer": 3,
    "PCA News": 3,
    "Business News - Property": 2,
    "Business News - Retail": 2,
    "realestate.com.au News": 2,
    "Domain Business": 2,
    "RE Source": 2,
}

KEYWORDS = [
    # hard CRE
    "industrial","warehouse","logistics","distribution centre","office","retail","shopping centre",
    "landlord","tenant","leasing","lease","rent","rents","vacancy","cap rate","yield","valuation",
    "assets under management","AUM","fund","fund manager","acquisition","acquires","sells","divests",
    "portfolio","transaction","deal","greenfield","brownfield","DA","planning","rezoning","BTR","build-to-rent",
    "student accommodation","data centre","self-storage","aged care","retirement village","hotels","hospitality",
    # finance that moves CRE
    "RBA","cash rate","rate cut","rate hike","inflation","CPI","bond","10-year","unemployment","GDP",
    "construction costs","materials","labour","insolvency","developer","pre-commit","NABERS","ESG",
    # AREITs and capital markets
    "REIT","A-REIT","distribution","NTA","capital raising","placement","buyback","guidance",
    # macro/FX snapshot
    "ASX","AUDUSD","S&P/ASX 200","yield curve","credit spreads",
]

STOPWORDS = [
    "celebrity","gossip","movie","music","football","tennis","cricket","NRL","AFL","crime",
    "recipe","fashion","horoscope","royal family","gaming",
]

def norm_url(u: str) -> str:
    if not u: return ""
    try:
        p = urlparse(u)
        q = [(k,v) for k,v in parse_qsl(p.query, keep_blank_values=True)
             if not k.lower().startswith(("utm_", "mc_", "fbclid"))]
        q.sort()
        clean = p._replace(query=urlencode(q, doseq=True), fragment="")
        return urlunparse(clean)
    except Exception:
        return u

def fingerprint(link: str, title: str) -> str:
    base = (norm_url(link) + "||" + (title or "")).lower().strip()
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def heuristic_score(source: str, title: str, summary: str) -> int:
    s = (title or "") + " " + (summary or "")
    s_l = s.lower()

    score = 0
    score += PROPERTY_WEIGHTED_SOURCES.get(source, 0)

    for kw in KEYWORDS:
        if kw.lower() in s_l:
            score += 1
    for sw in STOPWORDS:
        if sw.lower() in s_l:
            score -= 2

    # very short items tend to be fluff
    if len(s) < 50:
        score -= 1
    return score

# ---------- DB ----------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_name TEXT,
  source_feed TEXT,
  link TEXT,
  link_canonical TEXT UNIQUE,
  title TEXT,
  summary TEXT,
  published_at TEXT,
  fetched_at TEXT,
  interest_score INTEGER DEFAULT 0,
  relevant INTEGER DEFAULT 0,
  ai_json TEXT,
  ai_title TEXT,
  ai_desc TEXT,
  ai_tags TEXT,
  processed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_items_pub ON items(published_at);
CREATE INDEX IF NOT EXISTS idx_items_rel ON items(relevant);
"""

NEEDED_COLS = set([
    "source_name","source_feed","link","link_canonical","title","summary","published_at",
    "fetched_at","interest_score","relevant","ai_json","ai_title","ai_desc","ai_tags","processed_at"
])

def ensure_schema(conn: sqlite3.Connection):
    conn.executescript(SCHEMA_SQL)
    # add any missing columns (safe for existing DBs)
    cur = conn.execute("PRAGMA table_info(items)")
    have = {r[1] for r in cur.fetchall()}
    missing = [c for c in NEEDED_COLS if c not in have]
    for c in missing:
        if c == "interest_score":
            conn.execute("ALTER TABLE items ADD COLUMN interest_score INTEGER DEFAULT 0")
        elif c == "relevant":
            conn.execute("ALTER TABLE items ADD COLUMN relevant INTEGER DEFAULT 0")
        else:
            conn.execute(f"ALTER TABLE items ADD COLUMN {c} TEXT")
        logging.info(f"DB: added missing column {c}")
    conn.commit()

# ---------- OpenAI ----------
def throttle_sleep(last_ts: List[float]):
    # naive RPM throttle: at most OAI_RPM per minute
    if OAI_RPM <= 0: 
        return
    min_gap = 60.0 / float(OAI_RPM)
    now = time.time()
    if last_ts and (now - last_ts[0]) < min_gap:
        time.sleep(min_gap - (now - last_ts[0]))
    last_ts[:] = [time.time()]

AI_SYSTEM = (
    "You are a senior AU commercial property desk editor.\n"
    "Task 1: decide if the item is RELEVANT for AU property professionals.\n"
    "Include transactions, leasing, development/DA/planning, A-REIT & capital markets, macro that moves cap rates and feasos, construction costs, major policy/reg changes, and property-adjacent fintech if it impacts CRE. Exclude generic tech, consumer fluff, sports, gossip.\n"
    "Task 2: If relevant and text has enough facts, rewrite in a concise, punchy, Australian-English finance tone (a little Shaan-Puri zing), but STRICTLY based on the original text only. No new facts. No speculation. If insufficient info, mark relevant=false.\n"
    "Output JSON only."
)

def ai_classify_and_rewrite(title: str, summary: str, source: str) -> Optional[Dict[str, Any]]:
    if not client:
        return None
    original = (title or "") + "\n\n" + (summary or "")
    original = original.strip()
    if not original:
        return None

    user = {
        "role": "user",
        "content": (
            "Respond with compact JSON only: "
            '{"relevant":true|false,'
            '"title":"<rewritten 6–12 words>",'
            '"desc":"<1–3 sentences, <=90 words, no new facts>",'
            '"tags":["a_reit","macro","finance","industrial","retail","office","res_dev","policy","markets","tech","alt"]}'
            "}\n\n"
            f"---SOURCE---\n"
            f"feed: {source}\n"
            f"title: {title}\n"
            f"summary: {summary}\n"
            f"------------"
        )
    }

    last = [0.0]
    for attempt in range(1, OAI_MAX_RETRIES+1):
        try:
            throttle_sleep(last)
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                temperature=0.2,
                messages=[{"role":"system","content":AI_SYSTEM}, user]
            )
            txt = resp.choices[0].message.content.strip()
            # guard: sometimes models wrap code-fences
            txt = re.sub(r"^```(?:json)?|```$", "", txt).strip()
            data = json.loads(txt)
            # sanity
            if not isinstance(data, dict): 
                raise ValueError("Not a dict")
            if not data.get("relevant"):
                return {"relevant": False}
            # must have content, and must be different enough from original
            desc = (data.get("desc") or "").strip()
            if not desc or len(desc) < 30:
                return {"relevant": False}
            new_title = (data.get("title") or "").strip()
            if not new_title:
                new_title = title
            tags = data.get("tags") or []
            if not isinstance(tags, list):
                tags = []
            return {
                "relevant": True,
                "title": new_title,
                "desc": desc,
                "tags": tags[:6]
            }
        except Exception as e:
            if attempt == OAI_MAX_RETRIES:
                logging.warning(f"AI failed after retries: {e}")
                return None
            time.sleep(min(2**attempt, 8))
    return None

# ---------- Fetch ----------
def parse_dt(entry) -> Optional[str]:
    # returns ISO string in Brisbane tz (no DST)
    dt = None
    if getattr(entry, "published_parsed", None):
        dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).astimezone(TZ)
    elif getattr(entry, "updated_parsed", None):
        dt = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc).astimezone(TZ)
    else:
        dt = datetime.now(tz=TZ)
    return dt.isoformat()

def fetch_recent() -> List[Dict[str, Any]]:
    since = datetime.now(tz=TZ) - timedelta(hours=SCAN_WINDOW_HRS)
    items: List[Dict[str,Any]] = []
    for f in RSS_FEEDS:
        name = f.get("name","Feed")
        url  = f.get("url","")
        try:
            fp = feedparser.parse(url)
            for e in fp.entries[:200]:
                link = getattr(e, "link", "") or ""
                if not link: 
                    continue
                title = (getattr(e, "title", "") or "").strip()
                summary = (getattr(e, "summary", "") or "")
                if not summary and getattr(e, "content", None):
                    try:
                        summary = e.content[0].value or ""
                    except Exception:
                        summary = ""
                pub_iso = parse_dt(e)
                try:
                    pub_dt = datetime.fromisoformat(pub_iso)
                except Exception:
                    pub_dt = datetime.now(tz=TZ)
                if pub_dt < since:
                    continue
                items.append({
                    "source_name": name,
                    "source_feed": url,
                    "link": link,
                    "link_canonical": norm_url(link),
                    "title": title,
                    "summary": summary.strip(),
                    "published_at": pub_iso,
                    "fetched_at": datetime.now(tz=TZ).isoformat(),
                })
        except Exception as ex:
            logging.error(f"Fetch error {name}: {ex}")
    logging.info(f"Fetched recent items (items={len(items)})")
    return items

# ---------- Main pipeline ----------
def upsert(conn: sqlite3.Connection, row: Dict[str, Any]):
    conn.execute("""
        INSERT OR IGNORE INTO items
          (source_name,source_feed,link,link_canonical,title,summary,published_at,fetched_at,interest_score,relevant)
        VALUES (?,?,?,?,?,?,?,?,?,0)
    """, (row["source_name"], row["source_feed"], row["link"], row["link_canonical"],
          row["title"], row["summary"], row["published_at"], row["fetched_at"], 0))
    conn.commit()

def update_ai(conn: sqlite3.Connection, link_canonical: str, interest_score: int,
              relevant: int, ai_pkg: Optional[Dict[str,Any]]):
    if ai_pkg and ai_pkg.get("relevant"):
        conn.execute("""
          UPDATE items SET
            interest_score=?, relevant=1, ai_json=?, ai_title=?, ai_desc=?, ai_tags=?, processed_at=?
          WHERE link_canonical=?
        """, (interest_score, json.dumps(ai_pkg, ensure_ascii=False),
              ai_pkg.get("title"), ai_pkg.get("desc"),
              ",".join(ai_pkg.get("tags") or []),
              datetime.now(tz=TZ).isoformat(),
              link_canonical))
    else:
        conn.execute("""
          UPDATE items SET
            interest_score=?, relevant=0, processed_at=?
          WHERE link_canonical=?
        """, (interest_score, datetime.now(tz=TZ).isoformat(), link_canonical))
    conn.commit()

def main():
    if not OPENAI_API_KEY:
        logging.warning("OPENAI_API_KEY missing — AI steps will be skipped.")
    conn = sqlite3.connect(RSS_DB_PATH)
    ensure_schema(conn)

    items = fetch_recent()
    kept = 0
    ai_calls = 0
    last_call = [0.0]

    for it in items:
        upsert(conn, it)
        score = heuristic_score(it["source_name"], it["title"], it["summary"])

        ai_pkg = None
        if ALWAYS_AI and score >= MIN_SCORE_FOR_AI and OPENAI_API_KEY:
            ai_calls += 1
            # throttle inside call
            ai_pkg = ai_classify_and_rewrite(it["title"], it["summary"], it["source_name"])

        update_ai(conn, it["link_canonical"], score, 1 if (ai_pkg and ai_pkg.get("relevant")) else 0, ai_pkg)
        if ai_pkg and ai_pkg.get("relevant"): 
            kept += 1

    logging.info(f"SUMMARY: scanned={len(items)} ai_calls={ai_calls} kept={kept} (score>={MIN_SCORE_FOR_AI})")

if __name__ == "__main__":
    main()
