#!/usr/bin/env python3
"""
RSS Feed Analyzer for A-REIT CEO/COO - Executive Intelligence Platform
Enhanced version with improved error handling, performance, and AI integration
"""

import feedparser
import sqlite3
import smtplib
from openai import OpenAI
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import time
import os
import json
import schedule
import logging
import re
import socket
import sys
import concurrent.futures
from threading import Lock
from collections import defaultdict, Counter
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, Any
from dotenv import load_dotenv
import random
import hashlib
from functools import lru_cache
from contextlib import contextmanager

# Import RSS feeds from separate file
from feeds import RSS_FEEDS

# Load environment variables
load_dotenv()

# Enhanced logging configuration
def setup_logging():
    """Setup enhanced logging with rotation"""
    from logging.handlers import RotatingFileHandler
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    simple_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # File handler with rotation
    file_handler = RotatingFileHandler(
        'rss_analyzer.log', 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(detailed_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(simple_formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()


@dataclass
class FeedItem:
    """Enhanced FeedItem with validation and defaults"""
    title: str
    link: str
    description: str
    published: datetime
    source_feed: str
    source_name: str = ""
    interest_score: Optional[int] = None
    ai_summary: Optional[str] = None
    category: Optional[str] = None
    sentiment: Optional[str] = None
    key_metrics: Optional[List[str]] = field(default_factory=list)
    geographic_tags: Optional[List[str]] = field(default_factory=list)
    sector_tags: Optional[List[str]] = field(default_factory=list)
    
    def __post_init__(self):
        """Validate and clean data after initialization"""
        # Clean HTML from description
        if self.description:
            self.description = re.sub('<[^<]+?>', '', self.description)
            self.description = self.description[:500]  # Limit length
        
        # Validate score
        if self.interest_score is not None:
            self.interest_score = max(1, min(10, self.interest_score))
        
        # Ensure title is not empty
        if not self.title or self.title.strip() == "":
            self.title = "Untitled Article"


class DatabaseManager:
    """Improved database management with connection pooling and transactions"""
    
    def __init__(self, db_path: str = 'rss_items.db'):
        self.db_path = db_path
        self.lock = Lock()
        self.init_database()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def init_database(self):
        """Initialize database with improved schema"""
        with self.get_connection() as conn:
            # Enable WAL mode for better concurrency
            conn.execute("PRAGMA journal_mode=WAL")
            
            # Create main items table with indexes
            conn.execute('''
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    link TEXT UNIQUE NOT NULL,
                    description TEXT,
                    published DATETIME,
                    source_feed TEXT,
                    source_name TEXT,
                    interest_score INTEGER,
                    ai_summary TEXT,
                    category TEXT,
                    sentiment TEXT,
                    key_metrics TEXT,
                    geographic_tags TEXT,
                    sector_tags TEXT,
                    processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    email_sent BOOLEAN DEFAULT FALSE,
                    content_hash TEXT
                )
            ''')
            
            # Create indexes for better query performance
            conn.execute('CREATE INDEX IF NOT EXISTS idx_published ON items(published DESC)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_score ON items(interest_score DESC)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_processed ON items(processed_at DESC)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_email_sent ON items(email_sent)')
            
            # Processing runs table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS processing_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_type TEXT NOT NULL,
                    last_run_time DATETIME NOT NULL,
                    items_processed INTEGER DEFAULT 0,
                    items_new INTEGER DEFAULT 0,
                    errors INTEGER DEFAULT 0,
                    duration_seconds REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Error log table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS error_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    error_type TEXT,
                    error_message TEXT,
                    feed_url TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            logger.info("Database initialized with enhanced schema")


class AIAnalyzer:
    """Enhanced AI analyzer with better error handling and caching"""
    
    def __init__(self, api_key: str):
        self.client = OpenAI(api_key=api_key)
        self.cache = {}
        self.cache_ttl = 3600  # 1 hour
        
    def _get_cache_key(self, text: str) -> str:
        """Generate cache key for text"""
        return hashlib.md5(text.encode()).hexdigest()
    
    def analyze_with_retry(self, prompt: str, model: str = "gpt-4o", max_tokens: int = 500, max_retries: int = 3) -> str:
        """Make API call with retry logic"""
        last_error = None
        wait_time = 1
        
        for attempt in range(max_retries):
            try:
                response = self.client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": "You are a commercial property analyst. Be accurate, specific, and never make up facts."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=max_tokens,
                    temperature=0.3,
                    timeout=30
                )
                return response.choices[0].message.content.strip()
            except Exception as e:
                last_error = e
                logger.warning(f"AI API attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(wait_time)
                    wait_time *= 2  # Exponential backoff
        
        logger.error(f"AI API failed after {max_retries} attempts: {last_error}")
        raise last_error
    
    def batch_analyze_items(self, items: List[FeedItem], batch_size: int = 5) -> List[FeedItem]:
        """Improved batch analysis with better error handling"""
        if not items:
            return items
        
        analyzed_items = []
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            
            # Check cache first
            uncached_items = []
            for item in batch:
                cache_key = self._get_cache_key(f"{item.title}:{item.description[:100]}")
                if cache_key in self.cache:
                    cached_data = self.cache[cache_key]
                    if time.time() - cached_data['timestamp'] < self.cache_ttl:
                        item.interest_score = cached_data['score']
                        item.ai_summary = cached_data['summary']
                        item.sentiment = cached_data['sentiment']
                        item.category = cached_data['category']
                        analyzed_items.append(item)
                    else:
                        uncached_items.append(item)
                else:
                    uncached_items.append(item)
            
            if not uncached_items:
                continue
            
            # Create structured prompt
            prompt = self._create_analysis_prompt(uncached_items)
            
            try:
                response = self.analyze_with_retry(prompt)
                parsed_items = self._parse_analysis_response(response, uncached_items)
                
                # Cache results
                for item in parsed_items:
                    cache_key = self._get_cache_key(f"{item.title}:{item.description[:100]}")
                    self.cache[cache_key] = {
                        'score': item.interest_score,
                        'summary': item.ai_summary,
                        'sentiment': item.sentiment,
                        'category': item.category,
                        'timestamp': time.time()
                    }
                
                analyzed_items.extend(parsed_items)
                
            except Exception as e:
                logger.error(f"Batch analysis failed: {e}")
                # Fallback to basic scoring
                for item in uncached_items:
                    item.interest_score = self._calculate_basic_score(item)
                    item.ai_summary = "Analysis pending"
                    item.sentiment = "Neutral"
                    item.category = "Monitor"
                    analyzed_items.append(item)
        
        return analyzed_items
    
    def _create_analysis_prompt(self, items: List[FeedItem]) -> str:
        """Create structured analysis prompt"""
        prompt = """You are an expert commercial property analyst. Analyze these news items for a REIT CEO.

STRICT RULES:
1. Score 1-10 based on commercial property relevance (10 = critical for REIT strategy)
2. One-sentence insight directly related to the headline
3. NO speculation beyond what's in the title/description
4. Focus on: office, retail, industrial, logistics property impacts

Scoring Guide:
- 9-10: Critical market shifts, major policy changes, significant M&A
- 7-8: Important trends, notable transactions, market indicators
- 5-6: Relevant but not urgent, sector updates
- 3-4: Tangentially related, general economic news
- 1-2: Minimal relevance to commercial property

Format EXACTLY:
Item X:
Score: [1-10]
Impact: [One specific sentence about property market impact]
Trend: [Bullish/Bearish/Neutral]
Category: [Critical/Important/Monitor/Low]

Items to analyze:
"""
        
        for idx, item in enumerate(items, 1):
            desc = item.description[:200] if item.description else ""
            prompt += f"\nItem {idx}:\nTitle: {item.title}\nDescription: {desc}\nSource: {item.source_name}\n"
        
        return prompt
    
    def _parse_analysis_response(self, response: str, items: List[FeedItem]) -> List[FeedItem]:
        """Parse structured AI response with error handling"""
        analyzed_items = []
        
        for j, item in enumerate(items):
            try:
                item_pattern = f"Item {j+1}:"
                next_pattern = f"Item {j+2}:"
                
                if next_pattern in response:
                    item_section = response.split(item_pattern)[1].split(next_pattern)[0]
                else:
                    item_section = response.split(item_pattern)[1]
                
                # Extract components with defaults
                score_match = re.search(r'Score:\s*(\d+)', item_section)
                item.interest_score = int(score_match.group(1)) if score_match else 5
                
                impact_match = re.search(r'Impact:\s*(.+?)(?:Trend:|Category:|$)', item_section, re.DOTALL)
                item.ai_summary = impact_match.group(1).strip() if impact_match else "Property market impact under analysis"
                
                trend_match = re.search(r'Trend:\s*(Bullish|Bearish|Neutral)', item_section)
                item.sentiment = trend_match.group(1) if trend_match else "Neutral"
                
                category_match = re.search(r'Category:\s*(Critical|Important|Monitor|Low)', item_section)
                item.category = category_match.group(1) if category_match else self._score_to_category(item.interest_score)
                
            except Exception as e:
                logger.warning(f"Failed to parse item {j+1}: {e}")
                item.interest_score = 5
                item.ai_summary = "Analysis pending"
                item.sentiment = "Neutral"
                item.category = "Monitor"
            
            analyzed_items.append(item)
        
        return analyzed_items
    
    def _score_to_category(self, score: int) -> str:
        """Convert score to category"""
        if score >= 8:
            return "Critical"
        elif score >= 6:
            return "Important"
        elif score >= 4:
            return "Monitor"
        else:
            return "Low"
    
    def _calculate_basic_score(self, item: FeedItem) -> int:
        """Calculate basic score without AI"""
        score = 5
        keywords = {
            'critical': ['acquisition', 'merger', 'bankruptcy', 'default', 'crash'],
            'important': ['reit', 'property', 'real estate', 'office', 'retail', 'industrial'],
            'relevant': ['interest rate', 'fed', 'inflation', 'economy', 'market']
        }
        
        text = (item.title + " " + item.description).lower()
        
        for word in keywords['critical']:
            if word in text:
                score = min(10, score + 3)
        
        for word in keywords['important']:
            if word in text:
                score = min(9, score + 2)
        
        for word in keywords['relevant']:
            if word in text:
                score = min(8, score + 1)
        
        return score


class RSSAnalyzer:
    """Main analyzer class with improved architecture"""
    
    def __init__(self):
        # Load and validate configuration
        self.config = self._load_config()
        
        # Initialize components
        self.db = DatabaseManager()
        self.ai = AIAnalyzer(self.config['openai_api_key'])
        self.rss_feeds = RSS_FEEDS
        
        # Performance tracking
        self.stats = defaultdict(int)
        
        logger.info(f"RSS Analyzer initialized with {len(self.rss_feeds)} feeds")
    
    def _load_config(self) -> Dict[str, str]:
        """Load and validate configuration"""
        config = {
            'openai_api_key': os.getenv('OPENAI_API_KEY'),
            'gmail_user': os.getenv('GMAIL_USER'),
            'gmail_password': os.getenv('GMAIL_APP_PASSWORD'),
            'recipient_email': os.getenv('RECIPIENT_EMAIL'),
        }
        
        # Validate required variables
        missing = [k for k, v in config.items() if not v]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        return config
    
    def fetch_feeds_parallel(self, cutoff_time: datetime, max_workers: int = 10) -> List[FeedItem]:
        """Improved parallel feed fetching with timeout handling"""
        all_items = []
        errors = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_feed = {
                executor.submit(self._fetch_single_feed, feed_config, cutoff_time): feed_config 
                for feed_config in self.rss_feeds
            }
            
            for future in concurrent.futures.as_completed(future_to_feed, timeout=30):
                feed_config = future_to_feed[future]
                try:
                    items = future.result(timeout=10)
                    all_items.extend(items)
                    self.stats['feeds_success'] += 1
                except Exception as e:
                    logger.error(f"Failed to fetch {feed_config['name']}: {e}")
                    errors.append((feed_config['name'], str(e)))
                    self.stats['feeds_failed'] += 1
        
        # Log errors to database
        if errors:
            with self.db.get_connection() as conn:
                for feed_name, error_msg in errors:
                    conn.execute(
                        "INSERT INTO error_log (error_type, error_message, feed_url) VALUES (?, ?, ?)",
                        ('feed_fetch', error_msg, feed_name)
                    )
                conn.commit()
        
        logger.info(f"Fetched {len(all_items)} items from {self.stats['feeds_success']} feeds")
        return all_items
    
    def _fetch_single_feed(self, feed_config: Dict, cutoff_time: datetime) -> List[FeedItem]:
        """Fetch single feed with improved error handling"""
        feed_url = feed_config['url']
        feed_name = feed_config['name']
        items = []
        
        try:
            # Parse feed with timeout
            feed = feedparser.parse(feed_url, timeout=15)
            
            if feed.bozo:
                logger.warning(f"Feed parse warning for {feed_name}: {feed.bozo_exception}")
            
            for entry in feed.entries[:50]:  # Limit entries per feed
                try:
                    # Skip if missing required fields
                    if not hasattr(entry, 'title') or not hasattr(entry, 'link'):
                        continue
                    
                    # Parse published date
                    published = self._parse_date(entry)
                    
                    # Skip old items
                    if published < cutoff_time:
                        continue
                    
                    # Extract description
                    description = self._extract_description(entry)
                    
                    item = FeedItem(
                        title=entry.title[:500],  # Limit title length
                        link=entry.link,
                        description=description,
                        published=published,
                        source_feed=feed_url,
                        source_name=feed_name
                    )
                    
                    items.append(item)
                    
                except Exception as e:
                    logger.debug(f"Error processing entry from {feed_name}: {e}")
                    continue
            
            logger.info(f"✓ {feed_name}: {len(items)} recent items")
            
        except Exception as e:
            logger.error(f"Error fetching {feed_name}: {e}")
            raise
        
        return items
    
    def _parse_date(self, entry) -> datetime:
        """Parse date from feed entry with fallbacks"""
        # Try multiple date fields
        date_fields = ['published_parsed', 'updated_parsed', 'created_parsed']
        
        for field in date_fields:
            if hasattr(entry, field) and getattr(entry, field):
                try:
                    return datetime(*getattr(entry, field)[:6])
                except:
                    continue
        
        # Fallback to current time
        return datetime.now()
    
    def _extract_description(self, entry) -> str:
        """Extract and clean description from entry"""
        description = ""
        
        # Try multiple description fields
        for field in ['description', 'summary', 'content']:
            if hasattr(entry, field):
                content = getattr(entry, field)
                if isinstance(content, list) and content:
                    description = content[0].get('value', '')
                elif isinstance(content, str):
                    description = content
                
                if description:
                    break
        
        # Clean HTML and limit length
        description = re.sub('<[^<]+?>', '', description)
        description = re.sub(r'\s+', ' ', description).strip()
        
        return description[:1000]
    
    def process_daily_intelligence(self):
        """Enhanced daily processing with better error handling"""
        start_time = time.time()
        logger.info("=" * 60)
        logger.info("🌅 Starting Daily Intelligence Processing")
        
        try:
            # Get cutoff time (last 24 hours)
            cutoff_time = datetime.now() - timedelta(hours=24)
            
            # Fetch feeds in parallel
            all_items = self.fetch_feeds_parallel(cutoff_time)
            
            if not all_items:
                logger.warning("No items found in last 24 hours")
                return
            
            # Filter new items
            new_items = []
            with self.db.get_connection() as conn:
                for item in all_items:
                    cursor = conn.execute('SELECT 1 FROM items WHERE link = ?', (item.link,))
                    if not cursor.fetchone():
                        new_items.append(item)
            
            logger.info(f"Processing {len(new_items)} new items out of {len(all_items)} total")
            
            if new_items:
                # AI analysis in batches
                analyzed_items = self.ai.batch_analyze_items(new_items)
                
                # Save to database
                self._save_items(analyzed_items)
                
                logger.info(f"Saved {len(analyzed_items)} items to database")
            
            # Send email
            self.send_daily_intelligence_email()
            
            # Record processing run
            duration = time.time() - start_time
            with self.db.get_connection() as conn:
                conn.execute('''
                    INSERT INTO processing_runs 
                    (run_type, last_run_time, items_processed, items_new, errors, duration_seconds)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', ('daily_processing', datetime.now(), len(all_items), len(new_items), 
                      self.stats['feeds_failed'], duration))
                conn.commit()
            
            logger.info(f"✅ Daily processing complete in {duration:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Daily processing failed: {e}", exc_info=True)
            raise
        
        finally:
            logger.info("=" * 60)
    
    def _save_items(self, items: List[FeedItem]):
        """Save items to database with batch insert"""
        with self.db.get_connection() as conn:
            for item in items:
                try:
                    # Generate content hash for deduplication
                    content_hash = hashlib.md5(
                        f"{item.title}{item.description}".encode()
                    ).hexdigest()
                    
                    conn.execute('''
                        INSERT OR IGNORE INTO items (
                            title, link, description, published, source_feed, source_name,
                            interest_score, ai_summary, category, sentiment, key_metrics,
                            geographic_tags, sector_tags, content_hash
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        item.title, item.link, item.description, item.published,
                        item.source_feed, item.source_name, item.interest_score,
                        item.ai_summary, item.category, item.sentiment,
                        json.dumps(item.key_metrics) if item.key_metrics else None,
                        json.dumps(item.geographic_tags) if item.geographic_tags else None,
                        json.dumps(item.sector_tags) if item.sector_tags else None,
                        content_hash
                    ))
                except sqlite3.IntegrityError:
                    logger.debug(f"Duplicate item skipped: {item.title[:50]}")
                except Exception as e:
                    logger.error(f"Error saving item: {e}")
            
            conn.commit()
    
    def send_daily_intelligence_email(self):
        """Send the daily intelligence email"""
        try:
            # Get last 24 hours of items
            cutoff_time = datetime.now() - timedelta(hours=24)
            
            with self.db.get_connection() as conn:
                cursor = conn.execute('''
                    SELECT title, link, description, interest_score, ai_summary, source_name, 
                           processed_at, category, sentiment
                    FROM items 
                    WHERE processed_at >= ?
                    ORDER BY interest_score DESC, processed_at DESC
                ''', (cutoff_time,))
                
                items = cursor.fetchall()
            
            if not items:
                logger.warning("No items for daily email")
                return
            
            logger.info(f"Generating email for {len(items)} items")
            
            # Generate email content
            html_content = self._generate_email_html(items)
            
            if not html_content:
                logger.error("Failed to generate email content")
                return
            
            # Create email message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = self._generate_subject_line(items)
            msg['From'] = self.config['gmail_user']
            msg['To'] = self.config['recipient_email']
            
            # Add plain text and HTML parts
            text_part = MIMEText("Please view this email in HTML format for the best experience.", 'plain')
            html_part = MIMEText(html_content, 'html')
            
            msg.attach(text_part)
            msg.attach(html_part)
            
            # Send email
            with smtplib.SMTP('smtp.gmail.com', 587) as server:
                server.starttls()
                server.login(self.config['gmail_user'], self.config['gmail_password'])
                server.send_message(msg)
            
            logger.info("✅ Daily intelligence email sent successfully!")
            
            # Mark items as sent
            with self.db.get_connection() as conn:
                conn.execute('''
                    UPDATE items SET email_sent = TRUE 
                    WHERE processed_at >= ?
                ''', (cutoff_time,))
                conn.commit()
            
        except Exception as e:
            logger.error(f"Failed to send email: {e}", exc_info=True)
    
    def _generate_subject_line(self, items) -> str:
        """Generate email subject line"""
        critical_count = sum(1 for item in items if item['interest_score'] >= 8)
        date_str = datetime.now().strftime('%B %d')
        
        if critical_count > 0:
            return f"🔥 {critical_count} Critical Property Alerts - {date_str}"
        else:
            return f"📊 Property Intelligence Daily - {date_str}"
    
    def _generate_email_html(self, items) -> str:
        """Generate HTML email content"""
        # Sort and categorize items
        critical_items = [item for item in items if item['interest_score'] >= 8]
        important_items = [item for item in items if 6 <= item['interest_score'] < 8]
        monitor_items = [item for item in items if 4 <= item['interest_score'] < 6]
        
        current_date = datetime.now().strftime('%B %d, %Y')
        current_time = datetime.now().strftime('%I:%M %p')
        
        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: -apple-system, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }}
        .container {{ max-width: 600px; margin: 0 auto; background: white; border-radius: 10px; overflow: hidden; }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; text-align: center; }}
        .header h1 {{ margin: 0; font-size: 24px; }}
        .section {{ padding: 20px; }}
        .section-title {{ font-size: 18px; font-weight: bold; margin-bottom: 15px; padding-bottom: 10px; border-bottom: 2px solid #667eea; }}
        .item {{ margin-bottom: 20px; padding: 15px; border: 1px solid #e0e0e0; border-radius: 8px; }}
        .item.critical {{ border-left: 4px solid #ff4444; }}
        .item.important {{ border-left: 4px solid #ffaa00; }}
        .item-title {{ font-weight: bold; color: #333; margin-bottom: 8px; }}
        .item-summary {{ color: #666; font-size: 14px; line-height: 1.5; margin: 10px 0; }}
        .item-meta {{ font-size: 12px; color: #999; }}
        .score-badge {{ display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 12px; font-weight: bold; }}
        .score-high {{ background: #ff4444; color: white; }}
        .score-medium {{ background: #ffaa00; color: white; }}
        .score-low {{ background: #00aa00; color: white; }}
        .footer {{ padding: 20px; text-align: center; color: #666; font-size: 12px; background: #f9f9f9; }}
        a {{ color: #667eea; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏢 Property Intelligence Daily</h1>
            <p>{current_date} • {current_time}</p>
        </div>
"""
        
        # Add critical items section
        if critical_items:
            html += '<div class="section"><div class="section-title">🚨 Critical Alerts</div>'
            for item in critical_items[:5]:
                html += self._format_item_html(item, 'critical')
            html += '</div>'
        
        # Add important items section
        if important_items:
            html += '<div class="section"><div class="section-title">👀 Important Updates</div>'
            for item in important_items[:7]:
                html += self._format_item_html(item, 'important')
            html += '</div>'
        
        # Add monitoring items section
        if monitor_items:
            html += '<div class="section"><div class="section-title">📊 Market Monitor</div>'
            for item in monitor_items[:5]:
                html += self._format_item_html(item, 'monitor')
            html += '</div>'
        
        # Add footer
        html += f"""
        <div class="footer">
            <p><strong>Analyzed {len(items)} articles in seconds</strong></p>
            <p>Powered by AI Intelligence Platform</p>
        </div>
    </div>
</body>
</html>"""
        
        return html
    
    def _format_item_html(self, item, priority: str) -> str:
        """Format individual item for email"""
        score_class = 'score-high' if priority == 'critical' else 'score-medium' if priority == 'important' else 'score-low'
        
        return f"""
        <div class="item {priority}">
            <div class="item-title">
                {item['title']} 
                <span class="score-badge {score_class}">Score: {item['interest_score']}/10</span>
            </div>
            <div class="item-summary">{item['ai_summary'] or 'Analysis pending'}</div>
            <div class="item-meta">
                📰 {item['source_name']} • 
                <a href="{item['link']}">Read Full Article →</a>
            </div>
        </div>
        """
    
    def cleanup_old_items(self, days: int = 7):
        """Enhanced cleanup with statistics"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            
            with self.db.get_connection() as conn:
                # Get statistics before deletion
                cursor = conn.execute(
                    'SELECT COUNT(*) as count, MIN(published) as oldest FROM items WHERE processed_at < ?',
                    (cutoff_date,)
                )
                result = cursor.fetchone()
                count = result['count']
                oldest = result['oldest']
                
                if count > 0:
                    # Delete old items
                    conn.execute('DELETE FROM items WHERE processed_at < ?', (cutoff_date,))
                    
                    # Clean up old error logs
                    conn.execute(
                        'DELETE FROM error_log WHERE created_at < ?',
                        (cutoff_date,)
                    )
                    
                    conn.commit()
                    logger.info(f"Cleaned up {count} items older than {days} days (oldest: {oldest})")
                else:
                    logger.info(f"No items older than {days} days found")
                
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
            raise


def main():
    """Enhanced main function with better argument handling"""
    try:
        analyzer = RSSAnalyzer()
        
        if len(sys.argv) > 1:
            command = sys.argv[1].lower()
            
            if command in ['process', 'run', 'test']:
                logger.info("Running daily intelligence processing...")
                analyzer.process_daily_intelligence()
                
            elif command == 'email':
                logger.info("Sending daily email only...")
                analyzer.send_daily_intelligence_email()
            
            elif command == 'cleanup':
                days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
                logger.info(f"Cleaning up items older than {days} days...")
                analyzer.cleanup_old_items(days)
                
            elif command == 'stats':
                # Show statistics
                with analyzer.db.get_connection() as conn:
                    cursor = conn.execute('''
                        SELECT 
                            COUNT(*) as total_items,
                            COUNT(DISTINCT source_name) as sources,
                            AVG(interest_score) as avg_score,
                            MAX(published) as latest_item
                        FROM items
                    ''')
                    stats = cursor.fetchone()
                    print(f"\nDatabase Statistics:")
                    print(f"  Total items: {stats['total_items']}")
                    print(f"  Sources: {stats['sources']}")
                    print(f"  Average score: {stats['avg_score']:.2f}")
                    print(f"  Latest item: {stats['latest_item']}")
                    
            else:
                print("Usage: python rss_analyzer.py [process|email|cleanup|stats] [options]")
                sys.exit(1)
                
        else:
            # Schedule for 6am daily
            logger.info("Starting scheduled mode - will run at 6:00 AM daily")
            
            # Run once on startup
            analyzer.process_daily_intelligence()
            
            # Schedule daily run
            schedule.every().day.at("06:00").do(analyzer.process_daily_intelligence)
            
            # Weekly cleanup
            schedule.every().sunday.at("03:00").do(analyzer.cleanup_old_items, 30)
            
            while True:
                schedule.run_pending()
                time.sleep(60)
            
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
