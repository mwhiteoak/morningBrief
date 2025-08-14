#!/usr/bin/env python3
"""
Morning Briefing — AU CRE Intelligence (Beehiiv-ready)
- Fetches feeds (incremental, parallel)
- Filters & scores with AU/role weighting
- Summarises with OpenAI (throttled, cached, no fallbacks)
- Stores everything in sqlite: rss_items.db
"""

import feedparser, sqlite3, openai, os, re, json, time, socket, logging, concurrent.futures
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import List, Dict, Optional
from dotenv import load_dotenv
from urllib.parse import urlparse, parse_qsl, urlunparse, urlencode

from feeds import RSS_FEEDS
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler('rss_analyzer.log', encoding='utf-8'),
              logging.StreamHandler()]
)

NEWSLETTER_NAME = os.getenv('NEWSLETTER_NAME', 'Morning Briefing')
OPENAI_MODEL = os.getenv('OPENAI_MODEL', 'gpt-4o')
openai.api_key = os.getenv('OPENAI_API_KEY')

# OpenAI throttle to stay under limits
OAI_RPM = int(os.getenv('OAI_RPM', '25'))          # requests per minute (keep conservative)
OAI_MAX_RETRIES = int(os.getenv('OAI_MAX_RETRIES', '4'))

# AU relevance
AU_TERMS = [
    'australia','australian','a-reit','asx','rba','abs','treasury','brisbane','sydney','melbourne','perth','adelaide',
    'queensland','new south wales','victoria','western australia','south australia','tasmania','canberra',
    'lease','leasing','vacancy','wale','nabers','cap rate','valuation','capital raising','debt refinance','development approval',
    'da approval','planning permit','zoning','logistics','industrial','retail','office','shopping centre','neighbourhood centre'
]
DOWNWEIGHT_GLOBAL_NOISE = ['u.s.','united states','europe','uk','london','new york','nasdaq','s&p 500','dow']
ROLE_SIGNALS = {
    'asset_manager': ['opex','capex','tenant retention','leasing spread','arrears','waivers','incentives','nla','occupancy','nabers','opex ratio'],
    'fund_manager':  ['distribution','yield','irr','nav','cap rate','valuation','gearing','lvr','debt','refinance','covenant','liquidity','dpu'],
    'acquisitions':  ['off-market','acquisition','disposal','buyout','merger','takeover','sale','price','book value','pipeline','heads of agreement']
}
SENSITIVE = [
    'manslaughter','murder','child abuse','dies','died','death','killed','sexual assault','rape','domestic violence','suicide',
    'homicide','overdose','fatal','shooting','stabbing','assault','kidnapping','trafficking','abuse','violence','crash victim',
    'car accident','plane crash','drowning','fire death','explosion death'
]

@dataclass
class FeedItem:
    title: str
    link: str
    description: str
    published: datetime
    source_feed: str
    source_name: str
    interest_score: Optional[int] = None
    ai_summary: Optional[str] = None
    category: Optional[str] = None

def canonicalize_link(url: str) -> str:
    """Strip tracking params & fragments to create stable GUIDs & fingerprints."""
    try:
        u = urlparse(url)
        keep = set(['id','p','article','story'])  # keep only essential params
        q = [(k,v) for k,v in parse_qsl(u.query, keep_blank_values=False) if k.lower() in keep]
        clean = u._replace(query=urlencode(q), fragment='')
        return urlunparse(clean)
    except Exception:
        return url

def fingerprint(title: str, link: str, source: str) -> str:
    base = canonicalize_link(link).lower().strip()
    tnorm = re.sub(r'\s+', ' ', (title or '').lower()).strip()
    return f"{source.lower()}::{tnorm}::{base}"

class OpenAIThrottle:
    def __init__(self, rpm: int):
        self.rpm = max(1, rpm)
        self.window = []
    def wait(self):
        now = time.time()
        self.window = [t for t in self.window if now - t < 60]
        if len(self.window) >= self.rpm:
            sleep_for = 60 - (now - self.window[0]) + 0.05
            time.sleep(max(0.05, sleep_for))
        self.window.append(time.time())

class RSSAnalyzer:
    def __init__(self):
        self.conn = sqlite3.connect('rss_items.db', check_same_thread=False)
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                link TEXT NOT NULL,
                link_canonical TEXT,
                fp TEXT UNIQUE,  -- fingerprint
                description TEXT,
                published DATETIME,
                source_feed TEXT,
                source_name TEXT,
                interest_score INTEGER,
                ai_summary TEXT,
                category TEXT,
                processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                ai_package TEXT,
                exported_to_rss INTEGER DEFAULT 0,
                export_batch TEXT
            )
        ''')
        # backfill columns/index (no-op if exist)
        try: self.conn.execute("ALTER TABLE items ADD COLUMN link_canonical TEXT")
        except sqlite3.OperationalError: pass
        try: self.conn.execute("ALTER TABLE items ADD COLUMN fp TEXT")
        except sqlite3.OperationalError: pass
        try: self.conn.execute("ALTER TABLE items ADD COLUMN exported_to_rss INTEGER DEFAULT 0")
        except sqlite3.OperationalError: pass
        try: self.conn.execute("ALTER TABLE items ADD COLUMN export_batch TEXT")
        except sqlite3.OperationalError: pass
        try: self.conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_items_fp ON items(fp)")
        except sqlite3.OperationalError: pass
        self.conn.commit()

        self.rss_feeds = RSS_FEEDS
        self.oai = OpenAIThrottle(OAI_RPM)
        logging.info(f"Loaded {len(self.rss_feeds)} feeds")

    # --- DB checks ---
    def item_exists_fp(self, fp: str) -> bool:
        return self.conn.execute('SELECT 1 FROM items WHERE fp = ?', (fp,)).fetchone() is not None

    def save_item(self, item: FeedItem, ai_package: Optional[dict] = None):
        link_can = canonicalize_link(item.link)
        fpv = fingerprint(item.title, link_can, item.source_name)
        self.conn.execute('''
            INSERT OR IGNORE INTO items
            (title,link,link_canonical,fp,description,published,source_feed,source_name,
             interest_score,ai_summary,category,ai_package)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        ''', (
            item.title, item.link, link_can, fpv, item.description, item.published, item.source_feed, item.source_name,
            item.interest_score, item.ai_summary, item.category,
            json.dumps(ai_package, ensure_ascii=False) if ai_package else None
        ))
        self.conn.commit()

    # --- relevance & scoring ---
    def property_relevance_boost(self, text: str) -> int:
        t = text.lower()
        score = 0
        for kw in AU_TERMS:
            if kw in t: score += 2
        for kw in ROLE_SIGNALS['asset_manager'] + ROLE_SIGNALS['fund_manager'] + ROLE_SIGNALS['acquisitions']:
            if kw in t: score += 1
        for kw in DOWNWEIGHT_GLOBAL_NOISE:
            if kw in t and 'australia' not in t: score -= 2
        return score

    def auto_score(self, item: FeedItem):
        combined = (item.title + " " + (item.description or "")).lower()
        if any(k in combined for k in SENSITIVE):
            item.interest_score = 1; item.category = 'Filtered'; item.ai_summary = f"Filtered: {item.title}"; return
        score = 4 + max(-3, min(6, self.property_relevance_boost(combined)))
        item.interest_score = max(1, min(10, score))
        item.category = 'AU CRE' if score >= 5 else 'General'

    # --- OpenAI summariser (throttled, cached, retries) ---
    def _safe_json_extract(self, txt: str) -> Optional[dict]:
        try:
            s = txt.find('{'); e = txt.rfind('}')
            if s != -1 and e != -1 and e > s:
                return json.loads(txt[s:e+1])
        except Exception:
            pass
        return None

    def summarise_for_beehiiv(self, item: FeedItem) -> Optional[dict]:
        if not openai.api_key: return None

        # cached?
        row = self.conn.execute(
            "SELECT ai_package FROM items WHERE link_canonical = ? OR link = ?",
            (canonicalize_link(item.link), item.link)
        ).fetchone()
        if row and row[0]:
            try: return json.loads(row[0])
            except Exception: pass

        title = item.title or ""
        description = (item.description or item.ai_summary or "")[:1200]
        source_name = item.source_name or ""
        link = canonicalize_link(item.link)
        published_iso = item.published.replace(tzinfo=timezone.utc).isoformat() if isinstance(item.published, datetime) else ""

        prompt = f"""
You are an Australian commercial property analyst writing for asset managers, fund managers, and acquisitions teams.
Use AU English. Be concise, finance-desk sharp, with occasional light zingers. No hype.

You will receive: {title}, {description}, {source_name}, {link}, {published_iso}.
Only summarise what’s present in the inputs. If information is missing, say "insufficient data". Do not fabricate anything.

Required OUTPUT: valid JSON only with fields: hed, dek, summary_120w, bullets (3-5), why_it_matters{{asset_manager,fund_manager,acquisitions}},
sector_tags, geo_tags, risk_flags, confidence, suggested_subject, social{{twitter,linkedin}}, image_hint, newsletter_block_html.

Rules: AU CRE context (RBA/ABS, cap-rate, leasing/vacancy, WALE, DA/permit, industrial/logistics, shopping centres). Downweight global noise.
No crime/fatality coverage. If unsure, mark "insufficient data".

INPUT:
title: "{title}"
description: "{description}"
source_name: "{source_name}"
link: "{link}"
published_iso: "{published_iso}"
""".strip()

        for attempt in range(OAI_MAX_RETRIES):
            try:
                self.oai.wait()
                resp = openai.ChatCompletion.create(
                    model=OPENAI_MODEL,
                    messages=[{"role":"user","content":prompt}],
                    max_tokens=800,
                    temperature=0.2,
                )
                data = self._safe_json_extract(resp.choices[0].message.content)
                if data and isinstance(data, dict):
                    return data
                logging.warning("JSON extract failed; retrying…")
            except Exception as e:
                logging.warning(f"OAI error (try {attempt+1}/{OAI_MAX_RETRIES}): {e}")
                time.sleep(1.0 + attempt * 0.7)
        # No fallback — we exclude items without a package at publish time
        return None

    # --- fetching ---
    def fetch_feed_items_recent_only(self, feed_config: Dict, cutoff_time: datetime, max_items: int = 20) -> List[FeedItem]:
        feed_url, feed_name = feed_config['url'], feed_config['name']
        try:
            old_t = socket.getdefaulttimeout(); socket.setdefaulttimeout(15)
            try: feed = feedparser.parse(feed_url)
            finally: socket.setdefaulttimeout(old_t)
            items, too_old, now = [], 0, datetime.now()
            for e in feed.entries:
                if not hasattr(e, 'title') or not hasattr(e, 'link'): continue
                published = now
                if getattr(e, 'published_parsed', None): published = datetime(*e.published_parsed[:6])
                elif getattr(e, 'updated_parsed', None): published = datetime(*e.updated_parsed[:6])
                if published < cutoff_time:
                    too_old += 1
                    if too_old >= 3: break
                    continue
                desc = getattr(e, 'description', '') or getattr(e, 'summary', '') or ''
                items.append(FeedItem(
                    title=e.title, link=e.link, description=desc, published=published,
                    source_feed=feed_url, source_name=feed_name
                ))
                if len(items) >= max_items: break
            items.sort(key=lambda x: x.published, reverse=True)
            return items
        except Exception as e:
            logging.error(f"Fetch error {feed_name}: {e}")
            return []

    def fetch_all_parallel(self, cutoff_time: datetime, max_workers: int = 5) -> List[FeedItem]:
        all_items = []
        def runner(cfg): return self.fetch_feed_items_recent_only(cfg, cutoff_time, 15)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
            for items in ex.map(runner, self.rss_feeds):
                all_items.extend(items)
        return all_items

    # --- main ---
    def process_once(self) -> dict:
        cutoff = datetime.now() - timedelta(hours=6)
        raw = self.fetch_all_parallel(cutoff)
        if not raw: return {'scanned':0,'new':0,'saved':0}

        new_items, saved = [], 0
        for it in raw:
            fpv = fingerprint(it.title, canonicalize_link(it.link), it.source_name)
            if self.item_exists_fp(fpv): continue
            combined = (it.title + " " + (it.description or "")).lower()
            if any(k in combined for k in SENSITIVE): continue
            self.auto_score(it)
            new_items.append(it)

        for it in new_items:
            pkg = self.summarise_for_beehiiv(it)  # throttled + cached + retries
            self.save_item(it, ai_package=pkg)
            saved += 1

        return {'scanned':len(raw),'new':len(new_items),'saved':saved}

def main():
    stats = RSSAnalyzer().process_once()
    logging.info(f"Process: {stats}")

if __name__ == "__main__":
    main()
