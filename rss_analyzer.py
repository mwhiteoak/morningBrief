#!/usr/bin/env python3
"""
Enhanced RSS Analyzer with 100x More Engaging Content
- Aggressive feed fetching with better duplicate handling
- Engaging AI prompts for high-net-worth audience
- Content scoring and prioritization
- Enhanced headline/subhead generation
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
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
RSS_DB_PATH = os.getenv("RSS_DB_PATH", "rss_items.db")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OAI_RPM = int(os.getenv("OAI_RPM", "25"))
OAI_MAX_RETRIES = int(os.getenv("OAI_MAX_RETRIES", "4"))
SCAN_WINDOW_HRS = int(os.getenv("SCAN_WINDOW_HRS", "24"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "6"))
FEED_TIMEOUT = int(os.getenv("FEED_TIMEOUT", "15"))
ALWAYS_AI = os.getenv("ALWAYS_AI", "1") == "1"
MIN_SCORE_FOR_AI = int(os.getenv("MIN_SCORE_FOR_AI", "3"))  # Lower threshold for more content
MIN_ITEMS_PER_CATEGORY = int(os.getenv("MIN_ITEMS_PER_CATEGORY", "2"))
TZ = timezone(timedelta(hours=10))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s"
)

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ---------- Enhanced Heuristic scoring ----------
PROPERTY_WEIGHTED_SOURCES = {
    "AFR - Commercial Property": 5,
    "Real Commercial": 5,
    "The Urban Developer": 4,
    "PCA News": 4,
    "Business News - Property": 4,
    "Business News - Retail": 3,
    "realestate.com.au News": 3,
    "Domain Business": 3,
    "RE Source": 3,
    "AFR - Latest": 2,
    "The Australian - Business": 2,
}

# Enhanced keywords for better scoring
KEYWORDS = [
    # High-value CRE
    "industrial","warehouse","logistics","distribution centre","office","retail","shopping centre",
    "landlord","tenant","leasing","lease","rent","rents","vacancy","cap rate","yield","valuation",
    "assets under management","AUM","fund","fund manager","acquisition","acquires","sells","divests",
    "portfolio","transaction","deal","greenfield","brownfield","DA","planning","rezoning","BTR",
    "student accommodation","data centre","self-storage","aged care","retirement village","hotels",
    
    # Money moves CRE
    "RBA","cash rate","rate cut","rate hike","inflation","CPI","bond","10-year","unemployment","GDP",
    "construction costs","materials","labour","insolvency","developer","pre-commit","NABERS","ESG",
    
    # Big players
    "blackstone","brookfield","charter hall","goodman","gpt","mirvac","stockland","westfield",
    "lendlease","dexus","vicinity","scentre","shopping centres australasia","SCA","abp","logos",
    
    # AREITs and capital markets
    "REIT","A-REIT","distribution","NTA","capital raising","placement","buyback","guidance",
    "fund through","development pipeline","pre-leased","WALE","passing rent","market rent",
    
    # Macro/FX that moves property
    "ASX","AUDUSD","S&P/ASX 200","yield curve","credit spreads","APRA","banking royal commission",
]

STOPWORDS = [
    "celebrity","gossip","movie","music","football","tennis","cricket","NRL","AFL","crime",
    "recipe","fashion","horoscope","royal family","gaming","bitcoin","crypto","ethereum"
]

MAIN_CATEGORIES = [
    "a_reit", "macro", "finance", "industrial", "retail", "office", "res_dev", "policy", "markets", "tech", "alt"
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

def enhanced_heuristic_score(source: str, title: str, summary: str) -> int:
    """Enhanced scoring with engagement multipliers"""
    s = (title or "") + " " + (summary or "")
    s_l = s.lower()
    score = 0
    
    # Source weighting
    score += PROPERTY_WEIGHTED_SOURCES.get(source, 0)
    
    # Keyword scoring with multipliers
    for kw in KEYWORDS:
        if kw.lower() in s_l:
            # Big players get extra points
            if kw in ["blackstone","brookfield","charter hall","goodman","westfield"]:
                score += 3
            # Money terms get extra points  
            elif kw in ["acquisition","acquires","sells","merger","takeover","deal"]:
                score += 2
            else:
                score += 1
    
    # Money multipliers
    if re.search(r'\$[\d,.]+ ?billion', s, re.IGNORECASE):
        score += 8
    elif re.search(r'\$[\d,.]+ ?million', s, re.IGNORECASE):
        score += 4
    elif re.search(r'\$[\d,.]+', s, re.IGNORECASE):
        score += 2
    
    # Stopword penalties
    for sw in STOPWORDS:
        if sw.lower() in s_l:
            score -= 3
    
    # Length penalties for fluff
    if len(s) < 50:
        score -= 2
    
    return max(0, score)

# ---------- Enhanced AI Prompts ----------
ENGAGING_AI_SYSTEM = """
You are the chief intelligence editor for Australia's most exclusive $2000/year commercial property newsletter.

Your subscribers are fund managers, family offices, REIT executives, and developers with $500M+ AUM who make million-dollar decisions based on your intel.

WRITING STYLE:
- Sharp, insider tone (think WSJ meets Zero Hedge energy)
- Include specific $ amounts, company names, locations
- Always end with "what this means" implications
- Create urgency and FOMO
- Professional but with edge
- Make them feel like they have exclusive intelligence

RELEVANCE CRITERIA (be selective - only mark relevant if it moves big money):
✅ Major transactions ($50M+)
✅ A-REIT moves, capital raisings, acquisitions  
✅ RBA/policy changes affecting property finance
✅ Big player moves (Blackstone, Goodman, Charter Hall, etc.)
✅ Development approvals for major projects
✅ Interest rate/macro moves affecting cap rates
✅ Major leasing deals or tenant moves

❌ Small transactions, residential fluff, general business news, tech unless property-specific

Task 1: Decide if HIGHLY RELEVANT for these elite readers
Task 2: If relevant, rewrite as PREMIUM INTELLIGENCE (2-3 sentences max, insider tone)

Output JSON only.
"""

ENGAGING_HEADLINE_SYSTEM = """
You are creating subject lines for Australia's most exclusive commercial property intelligence newsletter.

Your readers pay $2000/year and include:
- Blackstone executives
- Charter Hall fund managers  
- Family office principals
- Government Treasury officials
- Major developer CEOs

Create headlines that make them INSTANTLY click because they fear missing alpha.

STYLE RULES:
- 6-10 words MAX
- Use POWER WORDS: "Exclusive", "Secret", "Alert", "Surge", "Plunge", "Breaking"
- Include $ amounts when possible  
- Create serious FOMO
- Feel like insider intelligence they can't get elsewhere

WINNING EXAMPLES:
- "🚨 Secret $2B Blackstone Shopping Spree"
- "💰 RBA Leak: Emergency Rate Cut March"  
- "⚡ Westfield's $500M Retail Bloodbath"
- "🔥 Charter Hall's Stealth Industrial Play"
- "💥 $1.2B Goodman Warehouse Empire Expands"

Output JSON only with 'headline' field.
"""

ENGAGING_SUBHEAD_SYSTEM = """
You create subheads for Australia's most exclusive commercial property newsletter.

Make subscribers feel they're getting billion-dollar intelligence unavailable elsewhere.

SUBHEAD RULES:
- Promise EXCLUSIVE insights and alpha
- Create serious FOMO about missing opportunities
- 15-25 words max
- Professional insider tone
- Make them feel like they're in the inner circle

WINNING EXAMPLES:
- "Exclusive intel on the $50B deals reshaping Australian CRE while retail investors panic"
- "The secret moves Blackstone, Goodman and Charter Hall don't want public—until now"  
- "Inside intelligence on which precincts will 10x before your competitors catch on"
- "The billion-dollar plays smart money is making while others read yesterday's news"

Output JSON only with 'subhead' field.
"""

def throttle_sleep(last_ts: List[float]):
    if OAI_RPM <= 0: return
    min_gap = 60.0 / float(OAI_RPM)
    now = time.time()
    if last_ts and (now - last_ts[0]) < min_gap:
        time.sleep(min_gap - (now - last_ts[0]))
    last_ts[:] = [time.time()]

def ai_classify_and_rewrite_engaging(title: str, summary: str, source: str) -> Optional[Dict[str, Any]]:
    """Enhanced AI processing for maximum engagement"""
    if not client:
        return None
        
    original = (title or "") + "\n\n" + (summary or "")
    original = original.strip()
    
    if not original:
        return None
    
    user = {
        "role": "user",
        "content": (
            "RESPOND WITH COMPACT JSON ONLY:\n"
            '{"relevant": true|false,\n'
            ' "title": "<6-10 words, insider intelligence style>",\n'
            ' "desc": "<2-3 sentences max, premium insights with what this means>",\n'
            ' "tags": ["a_reit","macro","finance","industrial","retail","office","res_dev","policy","markets","tech","alt"],\n'
            ' "urgency_score": 1-10}\n\n'
            f"INTELLIGENCE TO ASSESS:\n"
            f"Source: {source}\n"
            f"Title: {title}\n"
            f"Content: {summary}\n"
            f"---\n"
            f"REMEMBER: Only mark relevant if this creates opportunities or moves serious money for $500M+ fund managers."
        )
    }
    
    last = [0.0]
    for attempt in range(1, OAI_MAX_RETRIES + 1):
        try:
            throttle_sleep(last)
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                temperature=0.4,
                messages=[{"role": "system", "content": ENGAGING_AI_SYSTEM}, user]
            )
            
            txt = resp.choices[0].message.content.strip()
            txt = re.sub(r"^```(?:json)?|```$", "", txt).strip()
            
            data = json.loads(txt)
            
            if not isinstance(data, dict):
                raise ValueError("Not a dict")
                
            if not data.get("relevant"):
                return {"relevant": False}
            
            desc = (data.get("desc") or "").strip()
            if not desc or len(desc) < 60:  # Require substantial content
                return {"relevant": False}
                
            new_title = (data.get("title") or "").strip()
            if not new_title:
                new_title = title
                
            tags = data.get("tags") or []
            if not isinstance(tags, list):
                tags = []
                
            urgency = data.get("urgency_score", 5)
            
            return {
                "relevant": True,
                "title": new_title,
                "desc": desc,
                "tags": tags[:6],
                "urgency_score": urgency
            }
            
        except Exception as e:
            if attempt == OAI_MAX_RETRIES:
                logging.warning(f"Enhanced AI processing failed after retries: {e}")
                return None
            time.sleep(min(2**attempt, 8))
    
    return None

def generate_engaging_headlines(conn: sqlite3.Connection) -> Tuple[str, str]:
    """Generate maximum engagement headlines"""
    if not client:
        return "🚨 Property Intelligence Alert", "Exclusive deals and insider moves you won't find anywhere else"
    
    try:
        cur = conn.execute("""
            SELECT ai_title, ai_desc, ai_tags, source_name, link
            FROM items 
            WHERE relevant = 1 
            AND ai_desc IS NOT NULL 
            AND length(ai_desc) > 30
            ORDER BY published_at DESC 
            LIMIT 20
        """)
        items = cur.fetchall()
    except sqlite3.OperationalError:
        try:
            cur = conn.execute("""
                SELECT title, summary, '', source_name, link
                FROM items 
                WHERE length(summary) > 30
                ORDER BY published_at DESC 
                LIMIT 20
            """)
            items = cur.fetchall()
        except sqlite3.OperationalError:
            logging.warning("No items available for headline generation")
            return "🚨 Morning Property Intelligence", "Today's exclusive deals and market moves"
    
    if not items:
        return "🚨 Morning Property Intelligence", "Today's exclusive deals and market moves"
    
    # Create rich context for AI
    context_items = []
    total_money = 0
    
    for item in items:
        title = item[0] or ""
        desc = item[1] or ""
        tags = item[2] or ""
        source = item[3] or ""
        
        # Extract financial context
        money_matches = re.findall(r'\$?([\d,]+\.?\d*) ?(billion|million|bn|m)', desc, re.IGNORECASE)
        company_mentions = re.findall(
            r'\b(?:Blackstone|Brookfield|Charter Hall|Goodman|GPT|Mirvac|Stockland|Westfield|Lendlease|Dexus|Vicinity|Scentre)\b', 
            desc, re.IGNORECASE
        )
        
        # Calculate total money for context
        for amount, unit in money_matches:
            try:
                val = float(amount.replace(',', ''))
                if unit.lower() in ['billion', 'bn']:
                    total_money += val * 1000
                else:
                    total_money += val
            except:
                pass
        
        context_items.append({
            'title': title[:100],
            'desc': desc[:200],
            'money': money_matches[:3],
            'companies': company_mentions[:2],
            'source': source,
            'tags': tags
        })
    
    # Enhanced context string
    rich_context = f"MARKET INTELLIGENCE BRIEFING:\n"
    rich_context += f"Total Money Flow Tracked: ${total_money:,.0f}M+\n\n"
    
    for i, item in enumerate(context_items[:8], 1):
        money_str = ", ".join([f"${m[0]}{m[1]}" for m in item['money']]) if item['money'] else ""
        company_str = ", ".join(item['companies']) if item['companies'] else ""
        
        rich_context += f"{i}. {item['title']}\n"
        rich_context += f"   Intel: {item['desc']}\n"
        if money_str:
            rich_context += f"   Money Flow: {money_str}\n"
        if company_str:
            rich_context += f"   Key Players: {company_str}\n"
        rich_context += f"   Source: {item['source']}\n\n"
    
    # Generate engaging headline
    headline = "🚨 Property Intelligence Alert"
    try:
        throttle_sleep([0.0])
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.8,  # Higher creativity for headlines
            messages=[
                {"role": "system", "content": ENGAGING_HEADLINE_SYSTEM},
                {"role": "user", "content": f"Create an IRRESISTIBLE headline based on this intelligence:\n\n{rich_context}"}
            ]
        )
        headline_json = json.loads(resp.choices[0].message.content.strip())
        headline = headline_json.get("headline", headline)
        logging.info(f"Generated engaging headline: {headline}")
    except Exception as e:
        logging.warning(f"Failed to generate engaging headline: {e}")
    
    # Generate engaging subhead
    subhead = "Exclusive intel on the deals and moves reshaping Australian commercial property"
    try:
        throttle_sleep([0.0])
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.8,
            messages=[
                {"role": "system", "content": ENGAGING_SUBHEAD_SYSTEM},
                {"role": "user", "content": f"Create a compelling subhead for headline '{headline}' based on this intelligence:\n\n{rich_context}"}
            ]
        )
        subhead_json = json.loads(resp.choices[0].message.content.strip())
        subhead = subhead_json.get("subhead", subhead)
        logging.info(f"Generated engaging subhead: {subhead}")
    except Exception as e:
        logging.warning(f"Failed to generate engaging subhead: {e}")
    
    return headline, subhead

# ---------- Enhanced Database Functions ----------
def check_database_compatibility(conn: sqlite3.Connection) -> bool:
    try:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='items'")
        if not cur.fetchone():
            return True
        cur = conn.execute("PRAGMA table_info(items)")
        existing_cols = {row[1] for row in cur.fetchall()}
        essential_cols = {'link'}
        if not essential_cols.issubset(existing_cols):
            logging.warning(f"Database missing essential columns: {essential_cols - existing_cols}")
            return False
        return True
    except sqlite3.OperationalError as e:
        logging.error(f"Database compatibility check failed: {e}")
        return False

def create_fresh_database(conn: sqlite3.Connection):
    logging.info("Creating fresh database schema")
    conn.execute("DROP TABLE IF EXISTS items")
    conn.execute("DROP TABLE IF EXISTS newsletter_metadata")
    
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
            processed_at TEXT,
            urgency_score INTEGER DEFAULT 0
        )
    """)
    
    conn.execute("""
        CREATE TABLE newsletter_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date TEXT,
            headline TEXT,
            subhead TEXT,
            created_at TEXT,
            item_count INTEGER DEFAULT 0,
            total_money_tracked REAL DEFAULT 0
        )
    """)
    
    # Create indexes
    conn.execute("CREATE INDEX idx_items_pub ON items(published_at)")
    conn.execute("CREATE INDEX idx_items_rel ON items(relevant)")
    conn.execute("CREATE INDEX idx_items_canonical ON items(link_canonical)")
    conn.execute("CREATE INDEX idx_newsletter_date ON newsletter_metadata(run_date)")
    conn.execute("CREATE INDEX idx_items_urgency ON items(urgency_score)")
    
    conn.commit()
    logging.info("Fresh database schema created successfully")

def ensure_schema(conn: sqlite3.Connection):
    if not check_database_compatibility(conn):
        logging.info("Database incompatible - creating fresh schema")
        create_fresh_database(conn)
        return
    
    # Create base tables
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
    
    # Add missing columns
    cur = conn.execute("PRAGMA table_info(items)")
    existing_cols = {row[1]: row[2] for row in cur.fetchall()}
    
    missing_columns = {
        'interest_score': 'INTEGER DEFAULT 0',
        'relevant': 'INTEGER DEFAULT 0',
        'ai_json': 'TEXT',
        'ai_title': 'TEXT',
        'ai_desc': 'TEXT',
        'ai_tags': 'TEXT',
        'processed_at': 'TEXT',
        'urgency_score': 'INTEGER DEFAULT 0'
    }
    
    for col_name, col_def in missing_columns.items():
        if col_name not in existing_cols:
            try:
                conn.execute(f"ALTER TABLE items ADD COLUMN {col_name} {col_def}")
                logging.info(f"DB: added missing column {col_name}")
            except sqlite3.OperationalError as e:
                logging.warning(f"DB: could not add column {col_name}: {e}")
    
    # Newsletter metadata enhancements
    try:
        cur = conn.execute("PRAGMA table_info(newsletter_metadata)")
        existing_meta_cols = {row[1] for row in cur.fetchall()}
        if 'total_money_tracked' not in existing_meta_cols:
            conn.execute("ALTER TABLE newsletter_metadata ADD COLUMN total_money_tracked REAL DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    
    # Create indexes
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_items_pub ON items(published_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_items_rel ON items(relevant)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_items_canonical ON items(link_canonical)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_newsletter_date ON newsletter_metadata(run_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_items_urgency ON items(urgency_score)")
    except sqlite3.OperationalError:
        pass
    
    conn.commit()

def is_database_empty(conn: sqlite3.Connection) -> bool:
    try:
        cur = conn.execute("SELECT COUNT(*) FROM items WHERE relevant = 1")
        count = cur.fetchone()[0]
        return count == 0
    except sqlite3.OperationalError:
        return True

def has_sufficient_categories(conn: sqlite3.Connection) -> bool:
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
        
        sufficient_categories = sum(1 for count in category_counts.values() 
                                  if count >= MIN_ITEMS_PER_CATEGORY)
        logging.info(f"Category counts: {dict(category_counts)}")
        logging.info(f"Categories with sufficient items: {sufficient_categories}/{len(MAIN_CATEGORIES)}")
        
        return sufficient_categories >= len(MAIN_CATEGORIES) // 2
    except sqlite3.OperationalError:
        return False

def item_exists(conn: sqlite3.Connection, canonical_link: str) -> bool:
    cur = conn.execute("SELECT 1 FROM items WHERE link_canonical = ?", (canonical_link,))
    return cur.fetchone() is not None

# ---------- Enhanced Fetching Functions ----------
def parse_dt(entry) -> Optional[str]:
    dt = None
    if getattr(entry, "published_parsed", None):
        dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).astimezone(TZ)
    elif getattr(entry, "updated_parsed", None):
        dt = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc).astimezone(TZ)
    else:
        dt = datetime.now(tz=TZ)
    return dt.isoformat()

def fetch_with_enhanced_logic(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Enhanced fetch logic with better debugging"""
    db_empty = is_database_empty(conn)
    has_sufficient = has_sufficient_categories(conn)
    
    logging.info(f"DB State - Empty: {db_empty}, Sufficient categories: {has_sufficient}")
    
    if db_empty or not has_sufficient:
        logging.info("Performing comprehensive fetch")
        return fetch_comprehensive_enhanced()
    else:
        logging.info("Performing smart incremental fetch")
        return fetch_incremental_enhanced(conn)

def fetch_incremental_enhanced(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """More aggressive incremental fetch"""
    since = datetime.now(tz=TZ) - timedelta(hours=SCAN_WINDOW_HRS)
    items: List[Dict[str,Any]] = []
    
    for f in RSS_FEEDS:
        name = f.get("name","Feed")
        url = f.get("url","")
        
        logging.info(f"Fetching from: {name}")
        
        try:
            fp = feedparser.parse(url)
            
            if not fp.entries:
                logging.warning(f"No entries found for {name}")
                continue
                
            logging.info(f"Found {len(fp.entries)} entries in {name}")
            
            feed_items = 0
            consecutive_duplicates = 0
            
            for i, e in enumerate(fp.entries[:150]):  # Increased limit
                link = getattr(e, "link", "") or ""
                if not link:
                    continue
                    
                canonical_link = norm_url(link)
                
                # Less aggressive duplicate stopping
                if item_exists(conn, canonical_link):
                    consecutive_duplicates += 1
                    logging.debug(f"Duplicate found: {link}")
                    
                    # Only stop after 8 consecutive duplicates AND we have some new items
                    if consecutive_duplicates >= 8 and feed_items > 2:
                        logging.info(f"Stopping {name} after {consecutive_duplicates} consecutive duplicates ({feed_items} new items)")
                        break
                    continue
                else:
                    consecutive_duplicates = 0
                
                title = (getattr(e, "title", "") or "").strip()
                summary = (getattr(e, "summary", "") or "")
                
                if not summary and getattr(e, "content", None):
                    try:
                        summary = e.content[0].value or ""
                    except Exception:
                        summary = ""
                
                pub_iso = parse_dt(e)
                
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
            logging.error(f"Comprehensive fetch error {name}: {ex}")
    
    logging.info(f"Comprehensive fetch: {len(items)} items")
    return items

# ---------- Enhanced Database Operations ----------
def upsert(conn: sqlite3.Connection, row: Dict[str, Any]):
    """Enhanced insert with better error handling"""
    cur = conn.execute("PRAGMA table_info(items)")
    existing_cols = {r[1] for r in cur.fetchall()}
    
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
        'relevant': 0,
        'urgency_score': 0
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
                INSERT OR IGNORE INTO items ({fields_str}) VALUES ({placeholders})
            """, values)
            conn.commit()
        except sqlite3.OperationalError as e:
            logging.error(f"Failed to insert item: {e}")
    else:
        logging.error("No valid columns available for insert")

def update_ai(conn: sqlite3.Connection, link_canonical: str, interest_score: int, 
              relevant: int, ai_pkg: Optional[Dict[str,Any]]):
    """Enhanced AI results update"""
    cur = conn.execute("PRAGMA table_info(items)")
    existing_cols = {r[1] for r in cur.fetchall()}
    
    update_fields = []
    values = []
    
    # Always try to update interest_score
    if 'interest_score' in existing_cols:
        update_fields.append('interest_score=?')
        values.append(interest_score)
    
    if ai_pkg and ai_pkg.get("relevant"):
        field_mapping = {
            'relevant': 1,
            'ai_json': json.dumps(ai_pkg, ensure_ascii=False),
            'ai_title': ai_pkg.get("title"),
            'ai_desc': ai_pkg.get("desc"),
            'ai_tags': ",".join(ai_pkg.get("tags") or []),
            'processed_at': datetime.now(tz=TZ).isoformat(),
            'urgency_score': ai_pkg.get("urgency_score", 5)
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
        values.append(link_canonical)
        update_sql = f"UPDATE items SET {','.join(update_fields)} WHERE link_canonical=?"
        try:
            conn.execute(update_sql, values)
            conn.commit()
        except sqlite3.OperationalError as e:
            logging.warning(f"Could not update AI fields: {e}")
    else:
        logging.warning("No valid columns available for AI update")

def save_newsletter_metadata(conn: sqlite3.Connection, headline: str, subhead: str, 
                           item_count: int, total_money: float = 0.0):
    """Save enhanced newsletter metadata"""
    today = datetime.now(tz=TZ).date().isoformat()
    
    # Check if total_money_tracked column exists
    cur = conn.execute("PRAGMA table_info(newsletter_metadata)")
    existing_cols = {row[1] for row in cur.fetchall()}
    
    if 'total_money_tracked' in existing_cols:
        conn.execute("""
            INSERT OR REPLACE INTO newsletter_metadata 
            (run_date, headline, subhead, created_at, item_count, total_money_tracked)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (today, headline, subhead, datetime.now(tz=TZ).isoformat(), item_count, total_money))
    else:
        conn.execute("""
            INSERT OR REPLACE INTO newsletter_metadata 
            (run_date, headline, subhead, created_at, item_count)
            VALUES (?, ?, ?, ?, ?)
        """, (today, headline, subhead, datetime.now(tz=TZ).isoformat(), item_count))
    
    conn.commit()

def calculate_total_money_tracked(conn: sqlite3.Connection) -> float:
    """Calculate total money tracked in recent relevant items"""
    try:
        cur = conn.execute("""
            SELECT ai_desc FROM items 
            WHERE relevant = 1 AND ai_desc IS NOT NULL
            AND datetime(published_at) > datetime('now', '-7 days')
        """)
        
        total_money = 0.0
        for row in cur.fetchall():
            desc = row[0] or ""
            
            # Find money mentions
            billions = re.findall(r'\$?([\d,]+\.?\d*) ?billion', desc, re.IGNORECASE)
            millions = re.findall(r'\$?([\d,]+\.?\d*) ?million', desc, re.IGNORECASE)
            
            for b in billions:
                try:
                    total_money += float(b.replace(',', '')) * 1000
                except:
                    pass
            
            for m in millions:
                try:
                    total_money += float(m.replace(',', ''))
                except:
                    pass
        
        return total_money
    except sqlite3.OperationalError:
        return 0.0

# ---------- Main Enhanced Pipeline ----------
def main():
    if not OPENAI_API_KEY:
        logging.warning("OPENAI_API_KEY missing — AI steps will be skipped.")
        return
    
    logging.info("Starting enhanced RSS analyzer with 100x engagement multipliers")
    
    conn = sqlite3.connect(RSS_DB_PATH)
    ensure_schema(conn)
    
    # Enhanced fetching
    items = fetch_with_enhanced_logic(conn)
    logging.info(f"Fetched {len(items)} items for processing")
    
    if not items:
        logging.warning("No items fetched - check feed URLs and connectivity")
        return
    
    kept = 0
    ai_calls = 0
    total_money_tracked = 0.0
    
    for it in items:
        # Insert raw item
        upsert(conn, it)
        
        # Enhanced heuristic scoring
        score = enhanced_heuristic_score(it["source_name"], it["title"], it["summary"])
        
        ai_pkg = None
        if ALWAYS_AI and score >= MIN_SCORE_FOR_AI and OPENAI_API_KEY:
            ai_calls += 1
            logging.info(f"Processing item {ai_calls}: {it['title'][:60]}...")
            
            ai_pkg = ai_classify_and_rewrite_engaging(
                it["title"], it["summary"], it["source_name"]
            )
            
            if ai_pkg and ai_pkg.get("relevant"):
                kept += 1
                logging.info(f"✅ Kept: {ai_pkg.get('title', 'Untitled')}")
            else:
                logging.debug(f"❌ Filtered out: {it['title'][:60]}")
        
        # Update with AI results
        update_ai(conn, it["link_canonical"], score, 
                 1 if (ai_pkg and ai_pkg.get("relevant")) else 0, ai_pkg)
    
    # Calculate total money tracked
    total_money_tracked = calculate_total_money_tracked(conn)
    
    # Generate engaging headlines
    if OPENAI_API_KEY and kept > 0:
        logging.info("Generating maximum engagement headlines...")
        headline, subhead = generate_engaging_headlines(conn)
        save_newsletter_metadata(conn, headline, subhead, kept, total_money_tracked)
        
        logging.info(f"🚀 Generated headline: '{headline}'")
        logging.info(f"📝 Generated subhead: '{subhead}'")
        logging.info(f"💰 Total money tracked: ${total_money_tracked:,.0f}M")
    else:
        logging.warning("Skipping headline generation - no AI key or no relevant items")
    
    logging.info(f"""
🎯 PROCESSING COMPLETE:
   📥 Items scanned: {len(items)}
   🤖 AI calls made: {ai_calls}  
   ✅ Items kept: {kept}
   💰 Money tracked: ${total_money_tracked:,.0f}M
   📊 Min score for AI: {MIN_SCORE_FOR_AI}
   🔥 Engagement mode: MAXIMUM
    """)
    
    conn.close()

if __name__ == "__main__":
    main()
                    "link_canonical": canonical_link,
                    "title": title,
                    "summary": summary.strip(),
                    "published_at": pub_iso,
                    "fetched_at": datetime.now(tz=TZ).isoformat(),
                })
                
                feed_items += 1
                
            logging.info(f"Collected {feed_items} new items from {name}")
                
        except Exception as ex:
            logging.error(f"Fetch error {name}: {ex}")
    
    logging.info(f"Total items collected: {len(items)}")
    return items

def fetch_comprehensive_enhanced() -> List[Dict[str, Any]]:
    """Enhanced comprehensive fetch"""
    items: List[Dict[str,Any]] = []
    since = datetime.now(tz=TZ) - timedelta(days=7)  # Look back further
    
    for f in RSS_FEEDS:
        name = f.get("name","Feed")
        url = f.get("url","")
        
        logging.info(f"Comprehensive fetch from: {name}")
        
        try:
            fp = feedparser.parse(url)
            
            if not fp.entries:
                logging.warning(f"No entries found for {name}")
                continue
                
            logging.info(f"Found {len(fp.entries)} entries in {name}")
            
            for e in fp.entries[:300]:  # Even more entries for comprehensive
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
