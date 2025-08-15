#!/usr/bin/env python3
"""
Fetch → AI-filter → AI-rewrite → store in sqlite (rss_items.db)
Designed for AU commercial property professionals.

Enhanced features:
- Smart retrieval: populate categories if DB empty, otherwise stop at duplicates
- AI-generated headline and subhead based on retrieved content
- Heuristic prefilter trims obvious noise fast
- AI filter + rewrite only on likely-relevant items
- Rewrites are grounded strictly on original feed text
- Output later via write_beehiiv_feed.py
"""

import os, re, json, time, hashlib, sqlite3, logging, feedparser
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Tuple, Optional
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from collections import defaultdict

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
MIN_ITEMS_PER_CATEGORY = int(os.getenv("MIN_ITEMS_PER_CATEGORY", "2"))  # for initial population
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

# Define main categories for content organization
MAIN_CATEGORIES = [
    "a_reit", "macro", "finance", "industrial", "retail", 
    "office", "res_dev", "policy", "markets", "tech", "alt"
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
def check_database_compatibility(conn: sqlite3.Connection) -> bool:
    """Check if the existing database is compatible enough to migrate"""
    try:
        # Check if items table exists
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='items'")
        if not cur.fetchone():
            return True  # No table, we can create fresh
        
        # Check if basic required columns exist
        cur = conn.execute("PRAGMA table_info(items)")
        existing_cols = {row[1] for row in cur.fetchall()}
        
        # Must have at least link column to be worth migrating
        essential_cols = {'link'}
        if not essential_cols.issubset(existing_cols):
            logging.warning(f"Database missing essential columns: {essential_cols - existing_cols}")
            return False
            
        return True
    except sqlite3.OperationalError as e:
        logging.error(f"Database compatibility check failed: {e}")
        return False

def create_fresh_database(conn: sqlite3.Connection):
    """Create a completely fresh database schema"""
    logging.info("Creating fresh database schema")
    
    # Drop existing tables if they exist
    conn.execute("DROP TABLE IF EXISTS items")
    conn.execute("DROP TABLE IF EXISTS newsletter_metadata")
    
    # Create fresh schema
    conn.execute("""
        CREATE TABLE items (
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
        )
    """)
    
    conn.execute("""
        CREATE TABLE newsletter_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date TEXT,
            headline TEXT,
            subhead TEXT,
            created_at TEXT,
            item_count INTEGER DEFAULT 0
        )
    """)
    
    # Create indexes
    conn.execute("CREATE INDEX idx_items_pub ON items(published_at)")
    conn.execute("CREATE INDEX idx_items_rel ON items(relevant)")
    conn.execute("CREATE INDEX idx_items_canonical ON items(link_canonical)")
    conn.execute("CREATE INDEX idx_newsletter_date ON newsletter_metadata(run_date)")
    
    conn.commit()
    logging.info("Fresh database schema created successfully")
    """Create base tables if they don't exist"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT,
            source_feed TEXT,
            link TEXT,
            link_canonical TEXT UNIQUE,
            title TEXT,
            summary TEXT,
            published_at TEXT,
            fetched_at TEXT
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS newsletter_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date TEXT,
            headline TEXT,
            subhead TEXT,
            created_at TEXT,
            item_count INTEGER DEFAULT 0
        )
    """)
    
    # Create indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_pub ON items(published_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_canonical ON items(link_canonical)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_newsletter_date ON newsletter_metadata(run_date)")
    
    conn.commit()

NEEDED_COLS = set([
    "source_name","source_feed","link","link_canonical","title","summary","published_at",
    "fetched_at","interest_score","relevant","ai_json","ai_title","ai_desc","ai_tags","processed_at"
])

def create_base_schema(conn: sqlite3.Connection):
    """Create base tables if they don't exist"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT,
            source_feed TEXT,
            link TEXT,
            link_canonical TEXT UNIQUE,
            title TEXT,
            summary TEXT,
            published_at TEXT,
            fetched_at TEXT
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS newsletter_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date TEXT,
            headline TEXT,
            subhead TEXT,
            created_at TEXT,
            item_count INTEGER DEFAULT 0
        )
    """)
    
    # Create indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_pub ON items(published_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_canonical ON items(link_canonical)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_newsletter_date ON newsletter_metadata(run_date)")
    
    conn.commit()

def ensure_schema(conn: sqlite3.Connection):
    """Safely migrate database schema"""
    # Check if database is compatible enough to migrate
    if not check_database_compatibility(conn):
        logging.info("Database incompatible - creating fresh schema")
        create_fresh_database(conn)
        return
    
    # First create base tables
    create_base_schema(conn)
    
    # Check existing columns in items table
    cur = conn.execute("PRAGMA table_info(items)")
    existing_cols = {row[1]: row[2] for row in cur.fetchall()}
    
    # Define ALL columns we need (including basic ones that might be missing)
    all_columns_needed = {
        'source_name': 'TEXT',
        'source_feed': 'TEXT', 
        'link': 'TEXT',
        'link_canonical': 'TEXT',
        'title': 'TEXT',
        'summary': 'TEXT',  # This might be missing!
        'published_at': 'TEXT',
        'fetched_at': 'TEXT',
        'interest_score': 'INTEGER DEFAULT 0',
        'relevant': 'INTEGER DEFAULT 0', 
        'ai_json': 'TEXT',
        'ai_title': 'TEXT',
        'ai_desc': 'TEXT',
        'ai_tags': 'TEXT',
        'processed_at': 'TEXT'
    }
    
    # Add missing columns one by one (safe for existing DBs)
    for col_name, col_def in all_columns_needed.items():
        if col_name not in existing_cols:
            try:
                conn.execute(f"ALTER TABLE items ADD COLUMN {col_name} {col_def}")
                logging.info(f"DB: added missing column {col_name}")
            except sqlite3.OperationalError as e:
                logging.warning(f"DB: could not add column {col_name}: {e}")
    
    # Create additional indexes
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_items_rel ON items(relevant)")
    except sqlite3.OperationalError:
        pass  # Index might not be creatable if column doesn't exist yet
    
    conn.commit()

def is_database_empty(conn: sqlite3.Connection) -> bool:
    """Check if database has any relevant items"""
    try:
        cur = conn.execute("SELECT COUNT(*) FROM items WHERE relevant = 1")
        count = cur.fetchone()[0]
        return count == 0
    except sqlite3.OperationalError:
        # Column doesn't exist yet, database is effectively empty
        return True

def has_sufficient_categories(conn: sqlite3.Connection) -> bool:
    """Check if we have minimum items per main category"""
    try:
        category_counts = defaultdict(int)
        
        cur = conn.execute("""
            SELECT ai_tags FROM items 
            WHERE relevant = 1 AND ai_tags IS NOT NULL AND ai_tags != ''
        """)
        
        for row in cur.fetchall():
            tags = [tag.strip() for tag in (row[0] or "").split(",")]
            for tag in tags:
                if tag in MAIN_CATEGORIES:
                    category_counts[tag] += 1
        
        # Check if we have at least MIN_ITEMS_PER_CATEGORY for each main category
        sufficient_categories = sum(1 for count in category_counts.values() 
                                   if count >= MIN_ITEMS_PER_CATEGORY)
        
        logging.info(f"Category counts: {dict(category_counts)}")
        logging.info(f"Categories with sufficient items: {sufficient_categories}/{len(MAIN_CATEGORIES)}")
        
        return sufficient_categories >= len(MAIN_CATEGORIES) // 2  # At least half the categories
    except sqlite3.OperationalError:
        # Columns don't exist yet, not sufficient
        return False

def item_exists(conn: sqlite3.Connection, canonical_link: str) -> bool:
    """Check if item already exists in database"""
    cur = conn.execute("SELECT 1 FROM items WHERE link_canonical = ?", (canonical_link,))
    return cur.fetchone() is not None

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

HEADLINE_SYSTEM = (
    "You are a newsletter editor creating engaging subject lines for Australian commercial property professionals.\n"
    "Create a compelling, punchy headline that captures the key themes from the provided content.\n"
    "Style: Professional but engaging, Australian tone, 6-12 words max.\n"
    "Focus on the most significant news, deals, or market movements.\n"
    "Output JSON only with 'headline' field."
)

SUBHEAD_SYSTEM = (
    "You are a newsletter editor creating subheadings for Australian commercial property professionals.\n"
    "Create a compelling subhead that summarizes the key themes and gives readers a preview of what's inside.\n"
    "Style: Professional, informative, Australian tone, 15-25 words.\n"
    "Should complement the headline and entice readers to continue.\n"
    "Output JSON only with 'subhead' field."
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

def generate_headline_and_subhead(conn: sqlite3.Connection) -> Tuple[str, str]:
    """Generate AI headline and subhead based on recent relevant content"""
    if not client:
        return "Morning Property Brief", "Latest updates from Australian commercial property"
    
    try:
        # Get recent relevant items for context
        cur = conn.execute("""
            SELECT ai_title, ai_desc, ai_tags, source_name
            FROM items 
            WHERE relevant = 1 AND ai_desc IS NOT NULL 
            ORDER BY published_at DESC 
            LIMIT 15
        """)
        
        items = cur.fetchall()
    except sqlite3.OperationalError:
        # AI columns don't exist yet, fallback to basic items
        try:
            cur = conn.execute("""
                SELECT title, summary, '', source_name
                FROM items 
                ORDER BY published_at DESC 
                LIMIT 15
            """)
            items = cur.fetchall()
        except sqlite3.OperationalError:
            items = []
    
    if not items:
        return "Morning Property Brief", "Latest updates from Australian commercial property"
    
    # Prepare content summary for AI
    content_summary = []
    for item in items:
        title = item[0] or ""
        desc = item[1] or ""
        tags = item[2] or ""
        source = item[3] or ""
        content_summary.append(f"• {title}: {desc[:100]}... [Tags: {tags}] [Source: {source}]")
    
    content_text = "\n".join(content_summary)
    
    # Generate headline
    headline = "Morning Property Brief"  # fallback
    try:
        throttle_sleep([0.0])
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.3,
            messages=[
                {"role": "system", "content": HEADLINE_SYSTEM},
                {"role": "user", "content": f"Create a headline based on these stories:\n\n{content_text}"}
            ]
        )
        headline_json = json.loads(resp.choices[0].message.content.strip())
        headline = headline_json.get("headline", "Morning Property Brief")
    except Exception as e:
        logging.warning(f"Failed to generate headline: {e}")
    
    # Generate subhead
    subhead = "Latest updates from Australian commercial property"  # fallback
    try:
        throttle_sleep([0.0])
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.3,
            messages=[
                {"role": "system", "content": SUBHEAD_SYSTEM},
                {"role": "user", "content": f"Create a subhead for headline '{headline}' based on these stories:\n\n{content_text}"}
            ]
        )
        subhead_json = json.loads(resp.choices[0].message.content.strip())
        subhead = subhead_json.get("subhead", "Latest updates from Australian commercial property")
    except Exception as e:
        logging.warning(f"Failed to generate subhead: {e}")
    
    return headline, subhead

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

def fetch_with_smart_logic(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """
    Smart fetch logic:
    - If DB is empty: fetch until we have sufficient category representation
    - If DB has content: fetch recent items but stop when we hit existing content
    """
    db_empty = is_database_empty(conn)
    has_sufficient = has_sufficient_categories(conn)
    
    if db_empty or not has_sufficient:
        logging.info("Database empty or insufficient categories - fetching comprehensively")
        return fetch_comprehensive()
    else:
        logging.info("Database populated - fetching recent until duplicates")
        return fetch_until_duplicates(conn)

def fetch_comprehensive() -> List[Dict[str, Any]]:
    """Fetch more items to populate categories"""
    items: List[Dict[str,Any]] = []
    # Look back further when building initial dataset
    since = datetime.now(tz=TZ) - timedelta(hours=SCAN_WINDOW_HRS * 3)  # 3x window
    
    for f in RSS_FEEDS:
        name = f.get("name","Feed")
        url  = f.get("url","")
        try:
            fp = feedparser.parse(url)
            for e in fp.entries[:500]:  # More entries for initial population
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
    
    logging.info(f"Comprehensive fetch: {len(items)} items")
    return items

def fetch_until_duplicates(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Fetch recent items but stop when we encounter existing content"""
    since = datetime.now(tz=TZ) - timedelta(hours=SCAN_WINDOW_HRS)
    items: List[Dict[str,Any]] = []
    
    for f in RSS_FEEDS:
        name = f.get("name","Feed")
        url  = f.get("url","")
        try:
            fp = feedparser.parse(url)
            feed_items = 0
            duplicate_count = 0
            
            for e in fp.entries[:200]:
                link = getattr(e, "link", "") or ""
                if not link: 
                    continue
                
                canonical_link = norm_url(link)
                
                # Stop if we hit existing content
                if item_exists(conn, canonical_link):
                    duplicate_count += 1
                    # Stop after 3 consecutive duplicates to avoid missing new items mixed with old
                    if duplicate_count >= 3:
                        logging.info(f"Stopping {name} after {duplicate_count} duplicates ({feed_items} new items)")
                        break
                    continue
                else:
                    duplicate_count = 0  # Reset counter on new item
                
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
                    "link_canonical": canonical_link,
                    "title": title,
                    "summary": summary.strip(),
                    "published_at": pub_iso,
                    "fetched_at": datetime.now(tz=TZ).isoformat(),
                })
                feed_items += 1
                
        except Exception as ex:
            logging.error(f"Fetch error {name}: {ex}")
    
    logging.info(f"Smart fetch: {len(items)} new items")
    return items

# ---------- Main pipeline ----------
def upsert(conn: sqlite3.Connection, row: Dict[str, Any]):
    """Insert item, handling potential missing columns gracefully"""
    # Get current table schema
    cur = conn.execute("PRAGMA table_info(items)")
    existing_cols = {r[1] for r in cur.fetchall()}
    
    # Build dynamic insert based on available columns
    available_fields = []
    values = []
    
    field_mapping = {
        'source_name': row.get("source_name"),
        'source_feed': row.get("source_feed"), 
        'link': row.get("link"),
        'link_canonical': row.get("link_canonical"),
        'title': row.get("title"),
        'summary': row.get("summary"),
        'published_at': row.get("published_at"),
        'fetched_at': row.get("fetched_at"),
        'interest_score': 0,
        'relevant': 0
    }
    
    for field, value in field_mapping.items():
        if field in existing_cols:
            available_fields.append(field)
            values.append(value)
    
    if available_fields:
        placeholders = ','.join(['?'] * len(available_fields))
        fields_str = ','.join(available_fields)
        
        try:
            conn.execute(f"""
                INSERT OR IGNORE INTO items ({fields_str})
                VALUES ({placeholders})
            """, values)
            conn.commit()
        except sqlite3.OperationalError as e:
            logging.error(f"Failed to insert item: {e}")
            logging.error(f"Available fields: {available_fields}")
            logging.error(f"Values: {values}")
    else:
        logging.error("No valid columns available for insert")

def update_ai(conn: sqlite3.Connection, link_canonical: str, interest_score: int,
              relevant: int, ai_pkg: Optional[Dict[str,Any]]):
    """Update AI results, handling potential missing columns gracefully"""
    # Get current table schema
    cur = conn.execute("PRAGMA table_info(items)")
    existing_cols = {r[1] for r in cur.fetchall()}
    
    # Build dynamic update based on available columns
    update_fields = []
    values = []
    
    # Always try to update interest_score if available
    if 'interest_score' in existing_cols:
        update_fields.append('interest_score=?')
        values.append(interest_score)
    
    if ai_pkg and ai_pkg.get("relevant"):
        # Try to update AI fields if they exist
        field_mapping = {
            'relevant': 1,
            'ai_json': json.dumps(ai_pkg, ensure_ascii=False),
            'ai_title': ai_pkg.get("title"),
            'ai_desc': ai_pkg.get("desc"), 
            'ai_tags': ",".join(ai_pkg.get("tags") or []),
            'processed_at': datetime.now(tz=TZ).isoformat()
        }
        
        for field, value in field_mapping.items():
            if field in existing_cols:
                update_fields.append(f'{field}=?')
                values.append(value)
    else:
        # Mark as not relevant
        if 'relevant' in existing_cols:
            update_fields.append('relevant=?')
            values.append(0)
        if 'processed_at' in existing_cols:
            update_fields.append('processed_at=?')
            values.append(datetime.now(tz=TZ).isoformat())
    
    if update_fields:
        values.append(link_canonical)  # for WHERE clause
        update_sql = f"UPDATE items SET {','.join(update_fields)} WHERE link_canonical=?"
        
        try:
            conn.execute(update_sql, values)
            conn.commit()
        except sqlite3.OperationalError as e:
            logging.warning(f"Could not update AI fields: {e}")
    else:
        logging.warning("No valid columns available for AI update")

def save_newsletter_metadata(conn: sqlite3.Connection, headline: str, subhead: str, item_count: int):
    """Save the generated headline and subhead for this run"""
    today = datetime.now(tz=TZ).date().isoformat()
    conn.execute("""
        INSERT OR REPLACE INTO newsletter_metadata
        (run_date, headline, subhead, created_at, item_count)
        VALUES (?, ?, ?, ?, ?)
    """, (today, headline, subhead, datetime.now(tz=TZ).isoformat(), item_count))
    conn.commit()

def main():
    if not OPENAI_API_KEY:
        logging.warning("OPENAI_API_KEY missing — AI steps will be skipped.")
    
    conn = sqlite3.connect(RSS_DB_PATH)
    ensure_schema(conn)

    # Smart fetching based on database state
    items = fetch_with_smart_logic(conn)
    kept = 0
    ai_calls = 0

    for it in items:
        upsert(conn, it)
        score = heuristic_score(it["source_name"], it["title"], it["summary"])

        ai_pkg = None
        if ALWAYS_AI and score >= MIN_SCORE_FOR_AI and OPENAI_API_KEY:
            ai_calls += 1
            ai_pkg = ai_classify_and_rewrite(it["title"], it["summary"], it["source_name"])

        update_ai(conn, it["link_canonical"], score, 1 if (ai_pkg and ai_pkg.get("relevant")) else 0, ai_pkg)
        if ai_pkg and ai_pkg.get("relevant"): 
            kept += 1

    # Generate headline and subhead based on all relevant content
    if OPENAI_API_KEY:
        headline, subhead = generate_headline_and_subhead(conn)
        save_newsletter_metadata(conn, headline, subhead, kept)
        logging.info(f"Generated headline: '{headline}'")
        logging.info(f"Generated subhead: '{subhead}'")

    logging.info(f"SUMMARY: scanned={len(items)} ai_calls={ai_calls} kept={kept} (score>={MIN_SCORE_FOR_AI})")

if __name__ == "__main__":
    main()
