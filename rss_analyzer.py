#!/usr/bin/env python3
"""
RSS Feed Analyzer for A-REIT CEO/COO - Executive Intelligence Platform
Monitors RSS feeds, evaluates content with OpenAI, and sends daily emails
OPTIMIZED VERSION - 3x Daily Incremental Processing with Plain Text Email
"""

import feedparser
import sqlite3
import smtplib
import openai
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
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from dotenv import load_dotenv

# Import RSS feeds from separate file
from feeds import RSS_FEEDS

# Load environment variables
load_dotenv()

# Configure logging (fixed Unicode issues for Windows)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('rss_analyzer.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


@dataclass
class FeedItem:
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
    key_metrics: Optional[List[str]] = None
    geographic_tags: Optional[List[str]] = None
    sector_tags: Optional[List[str]] = None


@dataclass
class ExecutiveSummary:
    market_pulse_score: float
    key_alerts: List[str]
    major_deals: List[str]
    regulatory_items: List[str]
    sentiment_overview: str
    trending_topics: List[Tuple[str, int]]


class IncrementalProcessor:
    """Processes only new items since last run - much faster!"""
    
    def __init__(self, rss_analyzer):
        self.analyzer = rss_analyzer
        self.init_tracking_table()
    
    def init_tracking_table(self):
        """Initialize table to track last processing times"""
        self.analyzer.conn.execute('''
            CREATE TABLE IF NOT EXISTS processing_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_type TEXT NOT NULL,
                last_run_time DATETIME NOT NULL,
                items_processed INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.analyzer.conn.commit()
        logging.info("Processing tracking table initialized")
    
    def get_last_run_time(self, run_type: str = 'feed_processing') -> Optional[datetime]:
        """Get the last time we processed feeds"""
        cursor = self.analyzer.conn.execute('''
            SELECT last_run_time FROM processing_runs 
            WHERE run_type = ? 
            ORDER BY created_at DESC 
            LIMIT 1
        ''', (run_type,))
        
        result = cursor.fetchone()
        if result:
            try:
                return datetime.fromisoformat(result[0].replace('Z', '+00:00').replace('+00:00', ''))
            except:
                return datetime.fromisoformat(result[0])
        return None
    
    def update_last_run_time(self, run_type: str = 'feed_processing', items_processed: int = 0):
        """Update the last run time"""
        now = datetime.now()
        self.analyzer.conn.execute('''
            INSERT INTO processing_runs (run_type, last_run_time, items_processed)
            VALUES (?, ?, ?)
        ''', (run_type, now, items_processed))
        self.analyzer.conn.commit()
        logging.info(f"Updated last run time: {now}, items processed: {items_processed}")
    
    def get_incremental_cutoff_time(self) -> datetime:
        """Get the cutoff time for incremental processing"""
        last_run = self.get_last_run_time()
        
        if last_run:
            # Process items since last run, with small overlap for safety
            cutoff_time = last_run - timedelta(minutes=30)  # 30min overlap
            logging.info(f"Incremental processing: items since {cutoff_time}")
        else:
            # First run - get last 6 hours
            cutoff_time = datetime.now() - timedelta(hours=6)
            logging.info(f"First run: items from last 6 hours since {cutoff_time}")
        
        return cutoff_time
    
    def should_send_email(self) -> Tuple[bool, str]:
        """Determine if we should send email based on time of day - MORE FLEXIBLE"""
        now = datetime.now()
        hour = now.hour
        
        # For GitHub Actions, always send email when processing
        if os.getenv('GITHUB_ACTIONS'):
            return True, "github_actions"
        
        # Send email during multiple time windows (more flexible)
        # Morning: 6-9 AM AEST (most important)
        # Midday: 12-2 PM AEST  
        # Evening: 6-8 PM AEST
        
        # Convert to AEST equivalent (assuming UTC+10)
        aest_hour = (hour + 10) % 24
        
        # Morning run (6-9 AM AEST = 20-23 UTC previous day, or 0-2 UTC)
        if 20 <= hour <= 23 or 0 <= hour <= 2:
            return True, "morning"
        # Midday run (12-2 PM AEST = 2-4 UTC)
        elif 2 <= hour <= 4:
            return True, "midday"
        # Evening run (6-8 PM AEST = 8-10 UTC)
        elif 8 <= hour <= 10:
            return True, "evening"
        else:
            return False, "off_hours"


class RSSAnalyzer:
    def __init__(self):
        # Load configuration from environment variables
        self.config = {
            'openai_api_key': os.getenv('OPENAI_API_KEY'),
            'gmail_user': os.getenv('GMAIL_USER'),
            'gmail_password': os.getenv('GMAIL_APP_PASSWORD'),
            'recipient_email': os.getenv('RECIPIENT_EMAIL'),
        }
        
        # Validate required environment variables
        required_vars = ['OPENAI_API_KEY', 'GMAIL_USER', 'GMAIL_APP_PASSWORD', 'RECIPIENT_EMAIL']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"""
Missing required environment variables: {', '.join(missing_vars)}

Please ensure you have:
1. Copied .env.template to .env
2. Added your actual credentials to .env
3. Set up Gmail App Password (not regular password)
4. Created OpenAI API key

See README.md for detailed setup instructions.
            """)
        
        # Validate configuration format
        self.validate_config()
        
        # Initialize OpenAI
        openai.api_key = self.config['openai_api_key']
        
        # Initialize database
        self.init_database()
        
        # Load RSS feeds from separate file
        self.rss_feeds = RSS_FEEDS
        
        logging.info(f"Initialized RSS Analyzer with {len(self.rss_feeds)} feeds")
    
    def validate_config(self):
        """Validate configuration and provide helpful error messages"""
        errors = []
        
        # Check OpenAI API key format
        api_key = self.config.get('openai_api_key', '')
        if not api_key.startswith('sk-'):
            errors.append("OpenAI API key should start with 'sk-'")
        
        # Check email format
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        gmail_user = self.config.get('gmail_user', '')
        if not re.match(email_pattern, gmail_user):
            errors.append(f"Invalid Gmail user email format: {gmail_user}")
        
        recipient = self.config.get('recipient_email', '')
        if not re.match(email_pattern, recipient):
            errors.append(f"Invalid recipient email format: {recipient}")
        
        # Check Gmail app password (should be 16 characters)
        app_password = self.config.get('gmail_password', '')
        if len(app_password) != 16:
            errors.append("Gmail app password should be 16 characters long")
        
        if errors:
            error_msg = "Configuration errors found:\n" + "\n".join(f"  - {error}" for error in errors)
            error_msg += "\n\nPlease check your .env file and fix these issues."
            raise ValueError(error_msg)
        
        logging.info("Configuration validation passed")
        
    def init_database(self):
        """Initialize SQLite database with enhanced schema"""
        self.conn = sqlite3.connect('rss_items.db', check_same_thread=False)
        self.conn.execute('''
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
                email_sent BOOLEAN DEFAULT FALSE
            )
        ''')
        
        # Add new columns if they don't exist (for existing databases)
        try:
            self.conn.execute('ALTER TABLE items ADD COLUMN category TEXT')
            self.conn.execute('ALTER TABLE items ADD COLUMN sentiment TEXT')
            self.conn.execute('ALTER TABLE items ADD COLUMN key_metrics TEXT')
            self.conn.execute('ALTER TABLE items ADD COLUMN geographic_tags TEXT')
            self.conn.execute('ALTER TABLE items ADD COLUMN sector_tags TEXT')
        except sqlite3.OperationalError:
            pass  # Columns already exist
        
        self.conn.commit()
        logging.info("Database initialized successfully")
    
    def item_exists(self, link: str) -> bool:
        """Check if item exists in database"""
        cursor = self.conn.execute('SELECT 1 FROM items WHERE link = ?', (link,))
        return cursor.fetchone() is not None
    
    def title_exists(self, title: str, source_name: str) -> bool:
        """Check if similar title exists from same source"""
        cursor = self.conn.execute(
            'SELECT 1 FROM items WHERE title = ? AND source_name = ?', 
            (title, source_name)
        )
        return cursor.fetchone() is not None
    
    def save_item(self, item: FeedItem):
        """Save item to database"""
        try:
            self.conn.execute('''
                INSERT INTO items (
                    title, link, description, published, source_feed, source_name,
                    interest_score, ai_summary, category, sentiment, key_metrics,
                    geographic_tags, sector_tags
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                item.title, item.link, item.description, item.published,
                item.source_feed, item.source_name, item.interest_score,
                item.ai_summary, item.category, item.sentiment,
                json.dumps(item.key_metrics) if item.key_metrics else None,
                json.dumps(item.geographic_tags) if item.geographic_tags else None,
                json.dumps(item.sector_tags) if item.sector_tags else None
            ))
            self.conn.commit()
        except sqlite3.IntegrityError:
            logging.warning(f"Duplicate item skipped: {item.title[:50]}")
    
    def fetch_feed_items(self, feed_config: Dict) -> List[FeedItem]:
        """Legacy fetch method - gets all items (for compatibility)"""
        feed_url = feed_config['url']
        feed_name = feed_config['name']
        
        try:
            logging.info(f"Fetching feed: {feed_name}")
            feed = feedparser.parse(feed_url)
            
            if feed.bozo:
                logging.warning(f"Feed parsing warning for {feed_name}: {feed.bozo_exception}")
            
            items = []
            now = datetime.now()
            
            for entry in feed.entries:
                try:
                    # Check if entry has required attributes
                    if not hasattr(entry, 'title') or not hasattr(entry, 'link'):
                        logging.warning(f"Skipping malformed entry in {feed_name}: missing title or link")
                        continue
                    
                    # Parse published date
                    published = now  # Default to now if no date found
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        try:
                            published = datetime(*entry.published_parsed[:6])
                        except (TypeError, ValueError):
                            pass
                    elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                        try:
                            published = datetime(*entry.updated_parsed[:6])
                        except (TypeError, ValueError):
                            pass
                    
                    # Get description with fallback to summary
                    description = ""
                    if hasattr(entry, 'description'):
                        description = entry.description
                    elif hasattr(entry, 'summary'):
                        description = entry.summary
                    
                    item = FeedItem(
                        title=entry.title,
                        link=entry.link,
                        description=description,
                        published=published,
                        source_feed=feed_url,
                        source_name=feed_name
                    )
                    items.append(item)
                    
                except Exception as e:
                    logging.warning(f"Error processing entry in {feed_name}: {e}")
                    continue
            
            # Sort by published date (newest first)
            items.sort(key=lambda x: x.published, reverse=True)
            
            logging.info(f"Fetched {len(items)} items from {feed_name}")
            return items
            
        except Exception as e:
            logging.error(f"Error fetching feed {feed_name} ({feed_url}): {e}")
            return []

    def fetch_feed_items_recent_only(self, feed_config: Dict, cutoff_time: datetime, max_items: int = 20) -> List[FeedItem]:
        """OPTIMIZED: Fetch ONLY recent RSS feed items - much faster!"""
        feed_url = feed_config['url']
        feed_name = feed_config['name']
        
        try:
            logging.info(f"Fetching recent items from: {feed_name} (since {cutoff_time.strftime('%H:%M')})")
            
            # Set timeout for slow feeds
            old_timeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout(15)  # 15 second timeout
            
            try:
                feed = feedparser.parse(feed_url)
            finally:
                socket.setdefaulttimeout(old_timeout)
            
            if feed.bozo:
                logging.warning(f"Feed parsing warning for {feed_name}: {feed.bozo_exception}")
            
            items = []
            processed_count = 0
            too_old_count = 0
            
            # Process entries but STOP when we hit old items
            for entry in feed.entries:
                try:
                    processed_count += 1
                    
                    # Check if entry has required attributes
                    if not hasattr(entry, 'title') or not hasattr(entry, 'link'):
                        logging.warning(f"Skipping malformed entry in {feed_name}: missing title or link")
                        continue
                    
                    # Parse published date
                    published = datetime.now()  # Default to now
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        try:
                            published = datetime(*entry.published_parsed[:6])
                        except (TypeError, ValueError):
                            pass
                    elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                        try:
                            published = datetime(*entry.updated_parsed[:6])
                        except (TypeError, ValueError):
                            pass
                    
                    # STOP processing if item is too old (RSS feeds are usually chronological)
                    if published < cutoff_time:
                        too_old_count += 1
                        # If we hit 3 old items in a row, stop processing (feeds are chronological)
                        if too_old_count >= 3:
                            logging.info(f"   Stopping - hit {too_old_count} old items (feed is chronological)")
                            break
                        continue
                    else:
                        too_old_count = 0  # Reset counter
                    
                    # Item is recent enough - process it
                    description = ""
                    if hasattr(entry, 'description'):
                        description = entry.description
                    elif hasattr(entry, 'summary'):
                        description = entry.summary
                    
                    item = FeedItem(
                        title=entry.title,
                        link=entry.link,
                        description=description,
                        published=published,
                        source_feed=feed_url,
                        source_name=feed_name
                    )
                    items.append(item)
                    
                    # Limit items per feed to prevent memory issues
                    if len(items) >= max_items:
                        logging.info(f"   Reached max items limit ({max_items}) for {feed_name}")
                        break
                        
                except Exception as e:
                    logging.warning(f"Error processing entry in {feed_name}: {e}")
                    continue
            
            # Sort by published date (newest first)
            items.sort(key=lambda x: x.published, reverse=True)
            
            if items:
                newest = items[0].published
                oldest = items[-1].published
                time_span = (newest - oldest).total_seconds() / 3600
                logging.info(f"✓ {feed_name}: {len(items)} recent items (processed {processed_count} entries, spanning {time_span:.1f}h)")
            else:
                logging.info(f"✓ {feed_name}: No recent items (processed {processed_count} entries)")
            
            return items
            
        except Exception as e:
            logging.error(f"✗ Error fetching {feed_name}: {e}")
            return []

    def fetch_feeds_parallel_recent(self, cutoff_time: datetime, max_workers: int = 4) -> List[FeedItem]:
        """OPTIMIZED: Fetch recent items from all feeds in parallel - FAST!"""
        all_items = []
        start_time = time.time()
        
        logging.info(f"🚀 Fetching recent items from {len(self.rss_feeds)} feeds in parallel...")
        logging.info(f"   Cutoff time: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        def fetch_with_timeout(feed_config):
            """Wrapper to fetch feed with individual timeout"""
            try:
                return self.fetch_feed_items_recent_only(feed_config, cutoff_time, max_items=15)
            except Exception as e:
                logging.error(f"Feed fetch timeout/error: {feed_config['name']} - {e}")
                return []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all feed fetch tasks
            future_to_feed = {
                executor.submit(fetch_with_timeout, feed_config): feed_config 
                for feed_config in self.rss_feeds
            }
            
            # Collect results with timeout
            completed = 0
            for future in concurrent.futures.as_completed(future_to_feed, timeout=60):
                feed_config = future_to_feed[future]
                completed += 1
                
                try:
                    items = future.result(timeout=5)
                    all_items.extend(items)
                    
                    percentage = (completed / len(self.rss_feeds)) * 100
                    logging.info(f"   Progress: {completed}/{len(self.rss_feeds)} feeds ({percentage:.0f}%)")
                    
                except Exception as e:
                    logging.warning(f"   Failed: {feed_config['name']} - {e}")
        
        elapsed = time.time() - start_time
        logging.info(f"✅ Parallel fetch completed in {elapsed:.1f}s: {len(all_items)} total recent items")
        
        return all_items

    def should_use_ai_analysis_quick(self, item) -> bool:
        """Quick decision: does this item need AI analysis?"""
        title_lower = item.title.lower()
        desc_lower = item.description.lower()
        combined = title_lower + " " + desc_lower
        
        # High-value items that need nuanced analysis
        high_value_keywords = [
            'merger', 'acquisition', 'takeover', 'buyout',
            'policy', 'regulation', 'government', 'legislation',
            'market outlook', 'forecast', 'prediction', 'outlook',
            'investment grade', 'credit rating', 'valuation',
            'reit dividend', 'distribution', 'capital raising',
            'development approval', 'planning permit', 'zoning'
        ]
        
        # Technology items that might impact real estate
        tech_keywords = [
            'proptech', 'artificial intelligence', 'machine learning',
            'blockchain', 'automation', 'digital transformation',
            'smart building', 'iot', 'internet of things',
            'data analytics', 'predictive analytics', 'ai'
        ]
        
        # Check for high-value keywords
        if any(keyword in combined for keyword in high_value_keywords):
            return True
        
        # Check for tech keywords
        if any(keyword in combined for keyword in tech_keywords):
            return True
        
        # High interest score potential (long content with business keywords)
        if len(item.description) > 200 and any(keyword in combined for keyword in [
            'commercial property', 'office market', 'retail property',
            'industrial property', 'property investment', 'real estate'
        ]):
            return True
        
        return False

    def auto_score_item_quick(self, item):
        """Quick auto-scoring without AI - with sensitive content filtering"""
        title_lower = item.title.lower()
        desc_lower = item.description.lower()
        combined = title_lower + " " + desc_lower
        
        # Check for sensitive content first and score very low
        sensitive_keywords = [
            'manslaughter', 'murder', 'child abuse', 'dies', 'died', 'death', 'killed',
            'sexual assault', 'rape', 'domestic violence', 'suicide', 'homicide',
            'overdose', 'fatal', 'shooting', 'stabbing', 'assault', 'kidnapping',
            'trafficking', 'abuse', 'violence', 'crash victim', 'car accident',
            'plane crash', 'drowning', 'fire death', 'explosion death'
        ]
        
        if any(keyword in combined for keyword in sensitive_keywords):
            item.interest_score = 1  # Very low score for sensitive content
            item.category = 'Filtered Content'
            item.sentiment = 'Neutral'
            item.ai_summary = f"Filtered: {item.title}"
            return
        
        # Critical market-moving keywords
        if any(keyword in title_lower for keyword in [
            'interest rate', 'rba cuts', 'rba raises', 'cash rate',
            'property crash', 'property boom', 'house prices surge'
        ]):
            item.interest_score = 9
            item.category = 'Market Movers'
            item.sentiment = 'Positive' if any(pos in combined for pos in ['cut', 'lower', 'boom', 'surge']) else 'Negative'
            item.ai_summary = f"Critical market development: {item.title}"
            return
        
        # High priority A-REIT specific
        if any(keyword in combined for keyword in [
            'a-reit', 'reit dividend', 'commercial property', 'office occupancy',
            'retail vacancy', 'cap rates', 'property valuation'
        ]):
            item.interest_score = 8
            item.category = 'A-REIT Specific'
            item.sentiment = 'Neutral'
            item.ai_summary = f"A-REIT sector news: {item.title}"
            return
        
        # Technology with property relevance
        if any(keyword in combined for keyword in [
            'proptech', 'smart building', 'building automation',
            'property technology', 'real estate tech'
        ]):
            item.interest_score = 7
            item.category = 'Technology Impact'
            item.sentiment = 'Positive'
            item.ai_summary = f"Property technology development: {item.title}"
            return
        
        # General property-related
        if any(keyword in combined for keyword in [
            'property', 'real estate', 'construction', 'development'
        ]):
            item.interest_score = 6
            item.category = 'A-REIT Specific'
            item.sentiment = 'Neutral'
            item.ai_summary = f"Property sector update: {item.title}"
            return
        
        # Technology (general)
        if any(keyword in combined for keyword in [
            'artificial intelligence', 'automation', 'digital', 'innovation'
        ]):
            item.interest_score = 5
            item.category = 'Technology Impact'
            item.sentiment = 'Positive'
            item.ai_summary = f"Technology news: {item.title}"
            return
        
        # Default scoring
        item.interest_score = 4
        item.category = 'General Business'
        item.sentiment = 'Neutral'
        item.ai_summary = f"Business news: {item.title}"

    def process_ai_batch_quick(self, items: List[FeedItem]) -> int:
        """Quick AI batch processing with larger batches - with sensitive content filtering"""
        if not items:
            return 0
        
        # Pre-filter for sensitive content
        filtered_items = []
        sensitive_keywords = [
            'manslaughter', 'murder', 'child abuse', 'dies', 'died', 'death', 'killed',
            'sexual assault', 'rape', 'domestic violence', 'suicide', 'homicide',
            'overdose', 'fatal', 'shooting', 'stabbing', 'assault', 'kidnapping',
            'trafficking', 'abuse', 'violence', 'crash victim', 'car accident',
            'plane crash', 'drowning', 'fire death', 'explosion death'
        ]
        
        for item in items:
            combined_text = (item.title + ' ' + item.description).lower()
            if any(keyword in combined_text for keyword in sensitive_keywords):
                # Score sensitive content very low without AI analysis
                item.interest_score = 1
                item.category = 'Filtered Content'
                item.sentiment = 'Neutral'
                item.ai_summary = f"Filtered: {item.title}"
                logging.debug(f"Filtered sensitive content from AI batch: {item.title[:50]}...")
            else:
                filtered_items.append(item)
        
        batch_size = 15  # Larger batches for efficiency
        processed_count = 0
        
        for i in range(0, len(filtered_items), batch_size):
            batch = filtered_items[i:i + batch_size]
            
            # Create very concise prompt for speed
            prompt = f"Score these {len(batch)} news items for A-REIT CEO (1-10). Format: Item X: Score=Y\n\n"
            
            for idx, item in enumerate(batch, 1):
                prompt += f"{idx}. {item.title} - {item.description[:150]}...\n"
            
            try:
                response = openai.ChatCompletion.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=200,  # Very concise for speed
                    temperature=0.1
                )
                
                content = response.choices[0].message.content.strip()
                
                # Parse scores quickly
                lines = content.split('\n')
                for j, item in enumerate(batch):
                    # Default values
                    item.interest_score = 6
                    item.category = 'General Business'
                    item.sentiment = 'Neutral'
                    item.ai_summary = f"AI analyzed: {item.title}"
                    
                    # Look for score
                    item_pattern = f"Item {j+1}:"
                    for line in lines:
                        if item_pattern in line and 'Score=' in line:
                            try:
                                score_part = line.split('Score=')[1].split()[0]
                                item.interest_score = int(score_part)
                            except:
                                pass
                            break
                    
                    processed_count += 1
                
                logging.info(f"✓ AI batch {i//batch_size + 1}: {len(batch)} items scored")
                
            except Exception as e:
                logging.error(f"AI batch error: {e}")
                # Fallback scoring
                for item in batch:
                    self.auto_score_item_quick(item)
                    processed_count += 1
        
        # Include the filtered sensitive items in the total count
        processed_count += len(items) - len(filtered_items)
        
        return processed_count

    def process_feeds_optimized_recent(self):
        """MAIN OPTIMIZED METHOD: Only fetch and process recent items - MUCH FASTER!"""
        logging.info("=" * 60)
        logging.info("🚀 Starting OPTIMIZED recent-only RSS processing...")
        
        start_time = time.time()
        
        # Get cutoff time for this run
        processor = IncrementalProcessor(self)
        cutoff_time = processor.get_incremental_cutoff_time()
        
        # Fetch only recent items from all feeds in parallel
        all_recent_items = self.fetch_feeds_parallel_recent(cutoff_time, max_workers=5)
        
        if not all_recent_items:
            logging.info("No recent items found from any feeds")
            processor.update_last_run_time('feed_processing', 0)
            return {'total_scanned': 0, 'new_items': 0, 'processed': 0}
        
        logging.info(f"📊 Found {len(all_recent_items)} recent items across all feeds")
        
        # Filter for truly new items (not in database)
        new_items = []
        duplicate_count = 0
        
        for item in all_recent_items:
            if not self.item_exists(item.link) and not self.title_exists(item.title, item.source_name):
                new_items.append(item)
            else:
                duplicate_count += 1
        
        logging.info(f"📊 After deduplication: {len(new_items)} new items, {duplicate_count} duplicates")
        
        if not new_items:
            logging.info("No new items to process after deduplication")
            processor.update_last_run_time('feed_processing', 0)
            return {'total_scanned': len(all_recent_items), 'new_items': 0, 'processed': 0}
        
        # Smart processing - auto-score obvious items, AI for complex ones
        ai_items = []
        auto_items = []
        
        for item in new_items:
            if self.should_use_ai_analysis_quick(item):
                ai_items.append(item)
            else:
                self.auto_score_item_quick(item)
                auto_items.append(item)
        
        logging.info(f"📊 Processing plan: {len(auto_items)} auto-scored, {len(ai_items)} for AI analysis")
        
        # Process AI items in batches
        ai_processed = 0
        if ai_items:
            try:
                ai_processed = self.process_ai_batch_quick(ai_items)
            except Exception as e:
                logging.error(f"AI processing error: {e}")
                # Fallback - auto-score the AI items
                for item in ai_items:
                    self.auto_score_item_quick(item)
                    auto_items.append(item)
                ai_items = []
        
        # Save all processed items to database
        total_saved = 0
        for item in auto_items + ai_items:
            try:
                self.save_item(item)
                total_saved += 1
            except Exception as e:
                logging.error(f"Save error for {item.title[:50]}: {e}")
        
        # Update tracking
        processor.update_last_run_time('feed_processing', total_saved)
        
        elapsed = time.time() - start_time
        
        logging.info("✅ OPTIMIZED processing completed!")
        logging.info(f"   Duration: {elapsed:.1f} seconds")
        logging.info(f"   Recent items fetched: {len(all_recent_items)}")
        logging.info(f"   New items processed: {total_saved}")
        logging.info(f"   Auto-scored: {len(auto_items)}")
        logging.info(f"   AI-analyzed: {ai_processed}")
        logging.info("=" * 60)
        
        return {
            'total_scanned': len(all_recent_items),
            'new_items': len(new_items),
            'processed': total_saved
        }

    def emergency_simple_process_fallback(self):
        """Emergency fallback if optimized processing fails - with sensitive content filtering"""
        logging.info("🚨 EMERGENCY FALLBACK: Simple processing...")
        
        cutoff_time = datetime.now() - timedelta(hours=6)
        total_items = 0
        
        # Sensitive content keywords for filtering
        sensitive_keywords = [
            'manslaughter', 'murder', 'child abuse', 'dies', 'died', 'death', 'killed',
            'sexual assault', 'rape', 'domestic violence', 'suicide', 'homicide',
            'overdose', 'fatal', 'shooting', 'stabbing', 'assault', 'kidnapping',
            'trafficking', 'abuse', 'violence', 'crash victim', 'car accident',
            'plane crash', 'drowning', 'fire death', 'explosion death'
        ]
        
        # Process only first 5 feeds with simple scoring
        emergency_feeds = self.rss_feeds[:5]
        
        for feed_config in emergency_feeds:
            try:
                # Use basic fetch_feed_items method
                items = self.fetch_feed_items(feed_config)
                recent_items = [item for item in items if item.published >= cutoff_time]
                
                for item in recent_items[:3]:  # Max 3 items per feed for speed
                    if not self.item_exists(item.link):
                        # Check for sensitive content
                        combined_text = (item.title + ' ' + item.description).lower()
                        if any(keyword in combined_text for keyword in sensitive_keywords):
                            logging.debug(f"Emergency fallback: Filtered sensitive content: {item.title[:50]}...")
                            continue
                        
                        # Simple scoring without AI
                        score = 7 if any(keyword in item.title.lower() for keyword in [
                            'property', 'reit', 'real estate', 'commercial', 'office', 'retail'
                        ]) else 4
                        
                        item.interest_score = score
                        item.ai_summary = f"Emergency mode: {item.title}"
                        item.category = 'General Business'
                        item.sentiment = 'Neutral'
                        
                        self.save_item(item)
                        total_items += 1
                        
            except Exception as e:
                logging.error(f"Emergency processing error for {feed_config['name']}: {e}")
        
        logging.info(f"✅ Emergency fallback completed: {total_items} items (sensitive content filtered)")
        
        return {
            'total_scanned': total_items,
            'new_items': total_items,
            'processed': total_items
        }

    def should_send_email_now(self) -> Tuple[bool, str]:
        """Check if we should send email based on schedule - MORE FLEXIBLE"""
        processor = IncrementalProcessor(self)
        return processor.should_send_email()

    def get_items_for_email(self, hours_back: int = 24) -> List[Tuple]:
        """Get items for email from the last N hours - ENHANCED with debugging"""
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        
        cursor = self.conn.execute('''
            SELECT title, link, description, interest_score, ai_summary, source_name, 
                   processed_at, category, sentiment, key_metrics, geographic_tags, sector_tags
            FROM items 
            WHERE processed_at >= ? AND email_sent = FALSE
            ORDER BY interest_score DESC, processed_at DESC
        ''', (cutoff_time,))
        
        items = cursor.fetchall()
        
        # Debug logging
        logging.info(f"Email items query: found {len(items)} items since {cutoff_time}")
        if items:
            scores = [item[3] for item in items]
            logging.info(f"Score distribution: min={min(scores)}, max={max(scores)}, avg={sum(scores)/len(scores):.1f}")
            high_scores = len([s for s in scores if s >= 7])
            medium_scores = len([s for s in scores if 5 <= s < 7])
            logging.info(f"High scores (≥7): {high_scores}, Medium scores (5-6): {medium_scores}")
        
        return items

    def calculate_title_similarity(self, title1: str, title2: str) -> float:
        """Calculate similarity between two titles using word overlap"""
        # Normalize titles
        words1 = set(title1.lower().split())
        words2 = set(title2.lower().split())
        
        # Remove common stop words
        stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might', 'must', 'can', 'this', 'that', 'these', 'those'}
        words1 = words1 - stop_words
        words2 = words2 - stop_words
        
        if not words1 or not words2:
            return 0.0
        
        # Calculate Jaccard similarity (intersection / union)
        intersection = words1.intersection(words2)
        union = words1.union(words2)
        
        similarity = len(intersection) / len(union) if union else 0.0
        return similarity

    def is_duplicate_content(self, item: Tuple, existing_items: List[Tuple], similarity_threshold: float = 0.7) -> bool:
        """Check if item is a duplicate using smart fuzzy matching"""
        title, link, description, score, summary, source = item[:6]
        
        for existing_item in existing_items:
            existing_title, existing_link, existing_desc, existing_score, existing_summary, existing_source = existing_item[:6]
            
            # Skip if same source (already handled by exact title matching)
            if source == existing_source:
                continue
            
            # Check title similarity
            title_similarity = self.calculate_title_similarity(title, existing_title)
            
            # Check if titles are very similar
            if title_similarity >= similarity_threshold:
                logging.debug(f"Fuzzy duplicate detected: '{title[:40]}...' similar to '{existing_title[:40]}...' (similarity: {title_similarity:.2f})")
                return True
            
            # Check for exact substring matches (one title contains the other)
            title_clean = title.lower().strip()
            existing_title_clean = existing_title.lower().strip()
            
            if len(title_clean) > 20 and len(existing_title_clean) > 20:
                if title_clean in existing_title_clean or existing_title_clean in title_clean:
                    logging.debug(f"Substring duplicate detected: '{title[:40]}...' and '{existing_title[:40]}...'")
                    return True
        
        return False

    def deduplicate_items_smart(self, items: List[Tuple]) -> List[Tuple]:
        """Smart deduplication using fuzzy matching and quality scoring"""
        if not items:
            return items
        
        logging.info(f"Starting smart deduplication for {len(items)} items...")
        
        # Sort by score (highest first) to prioritize higher quality items
        sorted_items = sorted(items, key=lambda x: x[3], reverse=True)
        
        deduplicated_items = []
        
        for item in sorted_items:
            title, link, description, score, summary, source = item[:6]
            
            # Skip if it's a duplicate
            if self.is_duplicate_content(item, deduplicated_items):
                continue
            
            # Add to deduplicated list
            deduplicated_items.append(item)
        
        removed_count = len(items) - len(deduplicated_items)
        logging.info(f"Smart deduplication completed: {removed_count} duplicates removed, {len(deduplicated_items)} items remaining")
        
        return deduplicated_items

    def generate_key_takeaways(self, top_items: List[Tuple]) -> List[str]:
        """Generate key takeaways from top items for executive summary"""
        if not top_items:
            return ["No significant developments identified in current market analysis."]
        
        takeaways = []
        
        # Analyze themes from top items
        themes = {
            'monetary_policy': {'items': [], 'keywords': ['interest rate', 'rba', 'fed', 'cash rate', 'monetary', 'inflation']},
            'property_market': {'items': [], 'keywords': ['property', 'real estate', 'reit', 'commercial', 'residential', 'office', 'retail']},
            'economic_outlook': {'items': [], 'keywords': ['economy', 'gdp', 'growth', 'recession', 'outlook', 'forecast']},
            'technology': {'items': [], 'keywords': ['technology', 'ai', 'digital', 'automation', 'proptech', 'innovation']},
            'regulation': {'items': [], 'keywords': ['regulation', 'policy', 'government', 'law', 'compliance', 'planning']},
            'market_activity': {'items': [], 'keywords': ['acquisition', 'merger', 'deal', 'transaction', 'investment', 'capital']}
        }
        
        # Categorize top items by theme
        for item in top_items[:10]:  # Look at top 10 items
            title_desc = (item[0] + ' ' + (item[2] or '')).lower()
            
            for theme, data in themes.items():
                if any(keyword in title_desc for keyword in data['keywords']):
                    data['items'].append(item)
                    break
        
        # Generate takeaways for each theme with items
        for theme, data in themes.items():
            if data['items']:
                takeaway = self.generate_theme_takeaway(theme, data['items'])
                if takeaway:
                    takeaways.append(takeaway)
        
        # If no specific themes, generate generic takeaways
        if not takeaways:
            takeaways.append(f"Market analysis reveals {len(top_items)} developments requiring executive attention across commercial property sectors.")
        
        # Limit to 5 takeaways maximum
        return takeaways[:5]

    def generate_theme_takeaway(self, theme: str, items: List[Tuple]) -> str:
        """Generate a specific takeaway for a theme"""
        if not items:
            return ""
        
        count = len(items)
        highest_score = max(item[3] for item in items)
        
        # Theme-specific takeaway generation
        if theme == 'monetary_policy':
            if highest_score >= 8:
                return f"🏦 Critical monetary policy developments ({count} items) - Direct impact on commercial property valuations and investment strategies expected."
            else:
                return f"🏦 Monetary policy updates ({count} items) - Monitor for commercial property financing and cap rate implications."
        
        elif theme == 'property_market':
            if highest_score >= 8:
                return f"🏢 Major commercial property market activity ({count} items) - Significant sector developments requiring immediate strategic review."
            else:
                return f"🏢 Commercial property market updates ({count} items) - Sector activity indicates stable fundamentals with growth opportunities."
        
        elif theme == 'economic_outlook':
            if highest_score >= 8:
                return f"📊 Critical economic developments ({count} items) - Broad market conditions may significantly impact property investment strategies."
            else:
                return f"📊 Economic context updates ({count} items) - Macroeconomic trends support continued commercial property investment confidence."
        
        elif theme == 'technology':
            return f"💻 Technology sector developments ({count} items) - Digital transformation trends creating new opportunities in proptech and smart building investments."
        
        elif theme == 'regulation':
            if highest_score >= 8:
                return f"⚖️ Important regulatory changes ({count} items) - Policy developments require compliance review and strategic adjustment considerations."
            else:
                return f"⚖️ Regulatory updates ({count} items) - Policy environment remains supportive of commercial property development and investment."
        
        elif theme == 'market_activity':
            if highest_score >= 8:
                return f"🤝 Major transaction activity ({count} items) - Significant M&A and investment deals indicate strong market confidence and liquidity."
            else:
                return f"🤝 Market transaction updates ({count} items) - Steady deal flow demonstrates healthy commercial property investment appetite."
        
        return f"📈 Market development ({count} items) - Sector activity indicates continued commercial property market evolution."
        """Generate plain text formatted email that works across all email clients - with enhanced filtering"""
        logging.info(f"Starting email generation from {len(items)} items...")
        
        if not items:
            logging.warning("No items provided for email generation")
            return None
        
        start_time = time.time()
        
        try:
            # MUCH MORE INCLUSIVE FILTERING - include everything potentially relevant
            filtered_items = []
            seen_titles = set()
            
            for item in items:
                title, link, description, score, summary, source_name = item[:6]
                
                # Exclude specific AFR commercial real estate URL
                if link == "https://www.afr.com/topic/commercial-real-estate-5vu":
                    logging.debug(f"Skipping AFR commercial real estate topic page: {title[:50]}...")
                    continue
                
                # Combine title and description for comprehensive filtering
                combined_text = (title + ' ' + (description or '') + ' ' + (summary or '')).lower()
                
                # Skip obvious errors - be very permissive
                if any(phrase in title.lower() for phrase in [
                    'not found', 'sign up to rss.app', 'error 404', 'access denied', 
                    'page not found', 'forbidden', 'unauthorized'
                ]):
                    continue
                
                # Skip items with image summaries
                if (summary and summary.strip().startswith('<img src=')) or (description and description.strip().startswith('<img src=')):
                    logging.debug(f"Skipping item with image summary: {title[:50]}...")
                    continue
                
                # Skip sensitive content not relevant for commercial property intelligence
                sensitive_keywords = [
                    'manslaughter', 'murder', 'child abuse', 'dies', 'died', 'death', 'killed',
                    'sexual assault', 'rape', 'domestic violence', 'suicide', 'homicide',
                    'overdose', 'fatal', 'shooting', 'stabbing', 'assault', 'kidnapping',
                    'trafficking', 'abuse', 'violence', 'crash victim', 'car accident',
                    'plane crash', 'drowning', 'fire death', 'explosion death'
                ]
                
                if any(keyword in combined_text for keyword in sensitive_keywords):
                    logging.debug(f"Skipping sensitive content: {title[:50]}...")
                    continue
                
                # Skip exact duplicates only
                title_key = title.lower().strip()
                if title_key in seen_titles:
                    continue
                seen_titles.add(title_key)
                
                # Include ALL items with score >= 4 (much more inclusive)
                if score >= 4:
                    filtered_items.append(item)
            
            logging.info(f"Filtered items for email: {len(filtered_items)} (enhanced filtering applied)")
            
            if not filtered_items:
                logging.warning("No items remaining after filtering")
                return None
            
            # Generate email with timeout protection
            email_content = self.build_plain_text_email(filtered_items)
            
            elapsed_time = time.time() - start_time
            logging.info(f"Email generation completed in {elapsed_time:.1f} seconds")
            
            return email_content
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            logging.error(f"Email generation failed after {elapsed_time:.1f} seconds: {e}")
            
            # Return a simple fallback email
            return self.generate_fallback_email(items)
    
    def generate_fallback_email(self, items: List[Tuple]) -> str:
        """Generate a simple fallback email if main generation fails"""
        logging.info("Generating fallback email due to main generation failure")
        
        current_date = datetime.now().strftime('%B %d, %Y')
        current_time = datetime.now().strftime('%I:%M %p AEST')
        
        # Filter and sort items simply
        filtered_items = [item for item in items if item[3] >= 4][:20]  # Limit to 20 items
        sorted_items = sorted(filtered_items, key=lambda x: x[3], reverse=True)
        
        email_content = f"""🏢 COMMERCIAL PROPERTY INTELLIGENCE BRIEFING
{current_date} • {current_time}

===============================================================================

📊 EXECUTIVE SUMMARY

Total Items Analyzed: {len(sorted_items)}
Status: FALLBACK MODE - Simplified Analysis

Due to processing limitations, this briefing contains simplified analysis.
For full AI-powered insights, please check the system logs.

===============================================================================

📋 TOP PRIORITY ITEMS

"""
        
        # Add top items without AI analysis
        for i, item in enumerate(sorted_items[:10], 1):
            title, link, description, score, summary, source = item[:6]
            
            clean_title = title.strip()
            clean_desc = (description or summary or "")[:150].strip()
            
            email_content += f"""{i}. {clean_title}
SCORE: {score}/10 | SOURCE: {source}
LINK: {link}

SUMMARY: {clean_desc}...

-------------------------------------------------------------------------------

"""
        
        email_content += f"""===============================================================================

🤖 AI-POWERED INTELLIGENCE PLATFORM

Generated: {current_time} AEST (Fallback Mode)
Items Processed: {len(sorted_items)}
Status: Simplified analysis due to processing constraints

💼 Connect with Matt Whiteoak
LinkedIn: https://www.linkedin.com/in/mattwhiteoak

===============================================================================
"""
        
        return email_content

    def build_plain_text_email(self, items: List[Tuple]) -> str:
        """Build clean plain text email that works across ALL email clients - with global AI limit"""
        
        logging.info(f"Building plain text email for {len(items)} items...")
        
        current_date = datetime.now().strftime('%B %d, %Y')
        current_time = datetime.now().strftime('%I:%M %p AEST')
        
        # Sort items by score (highest first)
        sorted_items = sorted(items, key=lambda x: x[3], reverse=True)
        
        # Group items by priority for better organization
        critical_items = [item for item in sorted_items if item[3] >= 8]
        high_items = [item for item in sorted_items if item[3] == 7]
        medium_items = [item for item in sorted_items if item[3] == 6]
        normal_items = [item for item in sorted_items if item[3] == 5]
        other_items = [item for item in sorted_items if item[3] == 4]
        
        # Calculate summary stats
        total_items = len(sorted_items)
        critical_count = len(critical_items)
        high_count = len(high_items)
        medium_count = len(medium_items)
        total_high_priority = critical_count + high_count
        total_ai_eligible = critical_count + high_count + medium_count  # Now includes medium priority
        sentiment = self.calculate_simple_sentiment(sorted_items)
        
        logging.info(f"Email stats: {total_items} total, {critical_count} critical, {high_count} high, {medium_count} medium priority")
        logging.info(f"Total AI-eligible items (score ≥6): {total_ai_eligible}")
        
        # GLOBAL AI LIMIT: Prioritize AI for high-priority items first, then medium priority
        global_ai_limit = 50  # Increased limit to accommodate medium priority items
        self.global_ai_calls_used = 0
        
        # Build plain text email (no markdown)
        email_content = f"""🏢 COMMERCIAL PROPERTY INTELLIGENCE BRIEFING
{current_date} • {current_time}

===============================================================================

📊 EXECUTIVE SUMMARY

Total Items Analyzed: {total_items}
Critical Priority: {critical_count} 
High Priority: {high_count}
Medium Priority: {medium_count}
Total AI-Eligible Items: {total_ai_eligible}
Market Sentiment: {sentiment}

{self.generate_plain_text_executive_summary(sorted_items[:5], sentiment)}

===============================================================================

"""

        # Add each priority section with progress logging
        try:
            if critical_items:
                logging.info(f"Processing {len(critical_items)} critical items...")
                email_content += f"""🚨 CRITICAL PRIORITY ITEMS ({len(critical_items)})

{self.generate_plain_text_news_section_with_global_limit(critical_items, global_ai_limit)}

===============================================================================

"""

            if high_items:
                logging.info(f"Processing {len(high_items)} high priority items...")
                email_content += f"""🔴 HIGH PRIORITY ITEMS ({len(high_items)})

{self.generate_plain_text_news_section_with_global_limit(high_items, global_ai_limit)}

===============================================================================

"""

            if medium_items:
                logging.info(f"Processing {len(medium_items)} medium priority items...")
                email_content += f"""🟡 MEDIUM PRIORITY ITEMS ({len(medium_items)})

{self.generate_plain_text_news_section_with_global_limit(medium_items, global_ai_limit)}

===============================================================================

"""

            if normal_items:
                logging.info(f"Processing {len(normal_items)} normal priority items...")
                email_content += f"""🟢 NORMAL PRIORITY ITEMS ({len(normal_items)})

{self.generate_plain_text_news_section_with_global_limit(normal_items, global_ai_limit)}

===============================================================================

"""

            if other_items:
                logging.info(f"Processing {len(other_items)} other relevant items...")
                email_content += f"""📋 OTHER RELEVANT ITEMS ({len(other_items)})

{self.generate_plain_text_news_section_with_global_limit(other_items, global_ai_limit)}

===============================================================================

"""
        
        except Exception as e:
            logging.error(f"Error generating email sections: {e}")
            # Add fallback content if section generation fails
            email_content += f"""⚠️ ERROR IN EMAIL GENERATION

An error occurred while generating detailed analysis for some items.
{len(sorted_items)} items were found but detailed analysis failed.

Please check the logs for more details.

===============================================================================

"""
        
        # Add footer with AI usage stats
        email_content += f"""🤖 AI-POWERED INTELLIGENCE PLATFORM

Generated: {current_time} AEST
Sources Analyzed: {len(set(item[5] for item in sorted_items)) if sorted_items else 0}
AI Processing: OpenAI GPT-4o with Enhanced Market Context Analysis
AI Calls Used: {getattr(self, 'global_ai_calls_used', 0)}/{global_ai_limit}
AI Priority: Critical (≥8) > High (7) > Medium (6) > Others (≤5)
Intelligence Features: Smart deduplication, key takeaways, enhanced executive summary
Content Filters: AFR topic pages, image summaries, sensitive content, fuzzy duplicates

💼 Connect with Matt Whiteoak
LinkedIn: https://www.linkedin.com/in/mattwhiteoak

===============================================================================
"""
        
        logging.info(f"Completed email generation: {len(email_content)} characters, AI calls: {getattr(self, 'global_ai_calls_used', 0)}/{global_ai_limit}")
        return email_content

    def generate_plain_text_news_section_with_global_limit(self, items: List[Tuple], global_limit: int) -> str:
        """Generate news items with global AI limit tracking and custom medium priority analysis"""
        if not items:
            return "No items in this category."
        
        text_content = ""
        
        # Initialize global counter if not exists
        if not hasattr(self, 'global_ai_calls_used'):
            self.global_ai_calls_used = 0
            
        logging.info(f"Generating section for {len(items)} items (Global AI: {self.global_ai_calls_used}/{global_limit})")
        
        for i, item in enumerate(items, 1):
            title, link, description, score, summary, source = item[:6]
            
            # Clean up title and description - exclude image summaries
            clean_title = title.strip()
            clean_desc = (description or summary or "")[:200].strip()
            
            # Skip items with image summaries
            if clean_desc.startswith('<img src='):
                clean_desc = ""
            
            if clean_desc:
                clean_desc = clean_desc.replace('\n', ' ').replace('\r', ' ')
                # Remove HTML tags
                clean_desc = re.sub('<[^<]+?>', '', clean_desc)
            
            # Generate context with global AI limit and custom medium priority handling
            try:
                # Use AI for high-priority items (score ≥ 7) OR medium priority items (score 6) if AI available
                if score >= 7 and self.global_ai_calls_used < global_limit:
                    relevance = self.generate_ai_market_context(title, description, score)
                    self.global_ai_calls_used += 1
                    logging.debug(f"Used AI for item {i} (score {score}) - Global AI: {self.global_ai_calls_used}/{global_limit}")
                elif score == 6 and self.global_ai_calls_used < global_limit:
                    # Custom AI analysis for medium priority items
                    relevance = self.generate_medium_priority_market_context(title, description, score)
                    self.global_ai_calls_used += 1
                    logging.debug(f"Used custom AI for medium priority item {i} (score {score}) - Global AI: {self.global_ai_calls_used}/{global_limit}")
                else:
                    # Use fallback
                    relevance = self.generate_fallback_market_context(title, description, score)
                    reason = "low score" if score < 6 else "global limit reached"
                    logging.debug(f"Used fallback for item {i} (score {score}) - {reason}")
                    
            except Exception as e:
                logging.warning(f"Failed to generate context for item {i}: {e}")
                relevance = "MARKET CONTEXT: This development provides important context for commercial property strategic decision-making."
            
            text_content += f"""{i}. {clean_title}
SCORE: {score}/10 | SOURCE: {source}
LINK: {link}

COMMERCIAL PROPERTY MARKET CONTEXT:
{relevance}

"""
            
            if clean_desc:
                text_content += f"SUMMARY: {clean_desc}...\n\n"
            
            text_content += "-------------------------------------------------------------------------------\n\n"
        
        logging.info(f"Completed section: {len(items)} items processed (Global AI used: {self.global_ai_calls_used}/{global_limit})")
        return text_content

    def generate_medium_priority_market_context(self, title: str, description: str, score: int) -> str:
        """Generate AI-powered market context specifically for medium priority items (score 6)"""
        try:
            # Create a specialized prompt for medium priority items
            prompt = f"""As a commercial property market analyst, this news item has medium relevance (score 6/10). Provide a focused 1-2 sentence analysis of its potential commercial property implications.

Title: {title}
Description: {description[:250]}

Consider: indirect market effects, broader economic context, supply chain impacts, regulatory implications, or technology trends that could influence commercial real estate. Be concise and identify the most relevant connection to property markets."""

            response = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=100,  # Shorter for medium priority
                temperature=0.1,
                timeout=8  # Shorter timeout for medium priority
            )
            
            ai_context = response.choices[0].message.content.strip()
            
            # Clean up the response
            ai_context = ai_context.replace('\n', ' ').replace('\r', ' ')
            ai_context = re.sub(r'\s+', ' ', ai_context)  # Remove extra whitespace
            
            return f"MEDIUM PRIORITY ANALYSIS: {ai_context}"
            
        except Exception as e:
            logging.warning(f"Medium priority AI analysis failed: {e}")
            return self.generate_enhanced_fallback_for_medium_priority(title, description, score)

    def generate_enhanced_fallback_for_medium_priority(self, title: str, description: str, score: int) -> str:
        """Generate enhanced fallback analysis specifically for medium priority items"""
        title_lower = title.lower()
        desc_lower = (description or "").lower()
        combined = title_lower + " " + desc_lower
        
        # More nuanced analysis for medium priority items
        if any(keyword in combined for keyword in ['employment', 'jobs', 'workforce', 'hiring', 'unemployment']):
            return "EMPLOYMENT CONTEXT: Labor market dynamics influence commercial property demand through tenant employment levels, office space requirements, and regional economic activity affecting property fundamentals."
        elif any(keyword in combined for keyword in ['supply chain', 'logistics', 'shipping', 'transport', 'freight']):
            return "SUPPLY CHAIN IMPACT: Transportation and logistics developments affect industrial property demand, distribution hub locations, and last-mile delivery requirements for commercial real estate portfolios."
        elif any(keyword in combined for keyword in ['china', 'asia', 'trade', 'import', 'export', 'global']):
            return "GLOBAL TRADE CONTEXT: International trade dynamics impact port-adjacent industrial properties, import/export facility demand, and multinational corporate real estate strategies in key markets."
        elif any(keyword in combined for keyword in ['energy', 'oil', 'gas', 'renewable', 'electricity', 'power']):
            return "ENERGY SECTOR INFLUENCE: Energy market developments affect commercial property operating costs, sustainability requirements, and tenant attraction in energy-efficient buildings."
        elif any(keyword in combined for keyword in ['consumer', 'spending', 'retail sales', 'shopping', 'consumption']):
            return "CONSUMER BEHAVIOR: Consumer spending patterns influence retail property performance, tenant viability, and demand for experiential retail spaces versus traditional formats."
        elif any(keyword in combined for keyword in ['healthcare', 'medical', 'hospital', 'pharmaceutical', 'health']):
            return "HEALTHCARE SECTOR: Medical sector developments create specialized property demand including medical offices, research facilities, and healthcare-adjacent commercial spaces."
        elif any(keyword in combined for keyword in ['education', 'university', 'school', 'student', 'campus']):
            return "EDUCATION IMPACT: Educational sector changes affect student housing demand, campus-adjacent commercial properties, and research facility requirements in university markets."
        elif any(keyword in combined for keyword in ['government', 'public sector', 'policy', 'regulation', 'compliance']):
            return "REGULATORY ENVIRONMENT: Government policy changes may affect commercial property regulations, tax implications, development approvals, and public sector tenancy requirements."
        elif any(keyword in combined for keyword in ['mining', 'resources', 'commodity', 'materials', 'extraction']):
            return "RESOURCES SECTOR: Commodity market developments influence regional commercial property demand in resource-dependent markets and industrial facility requirements."
        else:
            return "MARKET CONTEXT: This development provides broader economic context that may indirectly influence commercial property market conditions and investment considerations."

    def generate_plain_text_executive_summary(self, top_items: List[Tuple], sentiment: str) -> str:
        """Generate executive summary in plain text format"""
        if not top_items:
            return "No significant developments requiring immediate attention. Market monitoring continues across all commercial property sectors."
        
        summary_parts = []
        
        # Key themes analysis
        themes = {
            'monetary_policy': 0,
            'property_market': 0,
            'technology': 0,
            'regulatory': 0,
            'economic': 0
        }
        
        for item in top_items:
            title_desc = (item[0] + ' ' + (item[2] or '')).lower()
            score = item[3]
            
            if any(keyword in title_desc for keyword in ['interest rate', 'rba', 'fed', 'monetary', 'inflation']):
                themes['monetary_policy'] += score
            elif any(keyword in title_desc for keyword in ['property', 'real estate', 'reit', 'commercial', 'residential']):
                themes['property_market'] += score
            elif any(keyword in title_desc for keyword in ['technology', 'ai', 'digital', 'automation']):
                themes['technology'] += score
            elif any(keyword in title_desc for keyword in ['regulation', 'policy', 'government', 'law']):
                themes['regulatory'] += score
            else:
                themes['economic'] += score
        
        # Find dominant themes
        dominant_themes = sorted(themes.items(), key=lambda x: x[1], reverse=True)[:2]
        
        # Build summary
        if dominant_themes[0][1] > 0:
            theme_names = {
                'monetary_policy': 'monetary policy developments',
                'property_market': 'commercial property market activity',
                'technology': 'technology and innovation impacts',
                'regulatory': 'regulatory and policy changes',
                'economic': 'broader economic developments'
            }
            
            primary_theme = theme_names.get(dominant_themes[0][0], 'market developments')
            summary_parts.append(f"PRIMARY FOCUS: {primary_theme.upper()}")
            
            if dominant_themes[1][1] > 0:
                secondary_theme = theme_names.get(dominant_themes[1][0], 'market activity')
                summary_parts.append(f"SECONDARY FOCUS: {secondary_theme.upper()}")
        
        summary_parts.append(f"MARKET SENTIMENT: {sentiment.upper()}")
        
        # Add action context
        critical_count = len([item for item in top_items if item[3] >= 8])
        if critical_count > 0:
            summary_parts.append(f"⚠️ {critical_count} CRITICAL ITEMS REQUIRE IMMEDIATE EXECUTIVE ATTENTION")
        
        return "\n".join(summary_parts)

    def generate_plain_text_news_section(self, items: List[Tuple]) -> str:
        """Generate news items in clean plain text format with rate limiting and AI call limits"""
        if not items:
            return "No items in this category."
        
        text_content = ""
        
        logging.info(f"Generating news section for {len(items)} items with AI context...")
        
        # Limit OpenAI API calls to prevent hanging - only use AI for high-priority items
        ai_call_count = 0
        max_ai_calls = 20  # Limit to 20 AI calls per section to prevent hanging
        
        for i, item in enumerate(items, 1):
            title, link, description, score, summary, source = item[:6]
            
            # Clean up title and description
            clean_title = title.strip()
            clean_desc = (description or summary or "")[:200].strip()
            if clean_desc:
                clean_desc = clean_desc.replace('\n', ' ').replace('\r', ' ')
                # Remove HTML tags
                clean_desc = re.sub('<[^<]+?>', '', clean_desc)
            
            # Generate AI-powered commercial property relevance with limits
            try:
                # Only use AI for high-priority items or if we haven't hit the limit
                if score >= 7 and ai_call_count < max_ai_calls:
                    relevance = self.generate_ai_market_context(title, description, score)
                    ai_call_count += 1
                    logging.debug(f"Used AI for item {i}/{len(items)} (AI calls: {ai_call_count}/{max_ai_calls}): {title[:30]}...")
                else:
                    # Use fallback for lower priority items or when we've hit the AI limit
                    relevance = self.generate_fallback_market_context(title, description, score)
                    logging.debug(f"Used fallback for item {i}/{len(items)}: {title[:30]}...")
                    
            except Exception as e:
                logging.warning(f"Failed to generate context for item {i}: {e}")
                relevance = "MARKET CONTEXT: This development provides important context for commercial property strategic decision-making."
            
            text_content += f"""{i}. {clean_title}
SCORE: {score}/10 | SOURCE: {source}
LINK: {link}

COMMERCIAL PROPERTY MARKET CONTEXT:
{relevance}

"""
            
            if clean_desc:
                text_content += f"SUMMARY: {clean_desc}...\n\n"
            
            text_content += "-------------------------------------------------------------------------------\n\n"
            
            # Progress logging every 10 items
            if i % 10 == 0:
                logging.info(f"Processed {i}/{len(items)} items (AI calls used: {ai_call_count}/{max_ai_calls})")
        
        logging.info(f"Completed news section generation for {len(items)} items (Total AI calls: {ai_call_count})")
        return text_content

    def generate_ai_market_context(self, title: str, description: str, score: int) -> str:
        """Generate AI-powered market context using OpenAI with better error handling"""
        try:
            # Create a concise prompt for OpenAI
            prompt = f"""As a commercial property market analyst, provide a 2-3 sentence analysis of how this news impacts commercial property markets (office, retail, industrial, logistics). Be specific and actionable for A-REIT executives.

Title: {title}
Description: {description[:300]}

Focus on: investment implications, market dynamics, tenant demand, property valuations, or operational impacts. Be concise and professional."""

            response = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=150,
                temperature=0.1,
                timeout=10  # 10 second timeout
            )
            
            ai_context = response.choices[0].message.content.strip()
            
            # Clean up the response
            ai_context = ai_context.replace('\n', ' ').replace('\r', ' ')
            ai_context = re.sub(r'\s+', ' ', ai_context)  # Remove extra whitespace
            
            return ai_context
            
        except openai.error.RateLimitError as e:
            logging.warning(f"OpenAI rate limit hit: {e}")
            time.sleep(2)  # Wait 2 seconds and use fallback
            return self.generate_fallback_market_context(title, description, score)
            
        except openai.error.APIError as e:
            logging.warning(f"OpenAI API error: {e}")
            return self.generate_fallback_market_context(title, description, score)
            
        except Exception as e:
            logging.warning(f"AI market context generation failed: {e}")
            return self.generate_fallback_market_context(title, description, score)

    def generate_fallback_market_context(self, title: str, description: str, score: int) -> str:
        """Generate fallback market context without OpenAI"""
        title_lower = title.lower()
        desc_lower = (description or "").lower()
        combined = title_lower + " " + desc_lower
        
        # Enhanced fallback analysis
        if any(keyword in combined for keyword in ['interest rate', 'rba', 'fed', 'monetary', 'inflation', 'cash rate']):
            return "MONETARY POLICY IMPACT: Interest rate changes directly affect commercial property valuations through cap rate movements and debt servicing costs. Higher rates typically compress property values while lower rates support asset prices and investment activity."
        elif any(keyword in combined for keyword in ['office', 'workplace', 'remote work', 'hybrid', 'cbd', 'co-working']):
            return "WORKPLACE EVOLUTION: Changing work patterns influence office space demand, lease structures, and CBD vs suburban preferences. Flexible work arrangements may reduce space requirements but increase demand for premium, technology-enabled buildings."
        elif any(keyword in combined for keyword in ['retail', 'shopping', 'consumer', 'e-commerce', 'mall', 'high street']):
            return "RETAIL TRANSFORMATION: Consumer behavior shifts impact retail property demand, requiring asset repositioning strategies. E-commerce growth affects traditional retail formats while creating opportunities in logistics and last-mile delivery hubs."
        elif any(keyword in combined for keyword in ['industrial', 'logistics', 'warehouse', 'supply chain', 'distribution']):
            return "INDUSTRIAL DEMAND: Supply chain developments drive industrial property requirements, particularly in logistics hubs and distribution centers. Automation trends may reduce labor needs but increase demand for specialized, technology-integrated facilities."
        elif any(keyword in combined for keyword in ['construction', 'development', 'planning', 'zoning', 'building approvals']):
            return "DEVELOPMENT FUNDAMENTALS: Construction activity and planning policies affect supply pipelines, development feasibility, and project timelines. Regulatory changes can significantly impact development margins and market dynamics."
        elif any(keyword in combined for keyword in ['technology', 'ai', 'automation', 'digital', 'proptech', 'smart building']):
            return "TECHNOLOGY INTEGRATION: Digital transformation affects property operations, tenant expectations, and investment requirements. Smart building technologies can improve operational efficiency and tenant satisfaction while requiring significant capital investment."
        elif any(keyword in combined for keyword in ['investment', 'capital', 'funding', 'finance', 'reit', 'portfolio']):
            return "CAPITAL MARKET DYNAMICS: Investment flows and financing conditions directly impact property acquisition strategies, portfolio optimization, and return expectations. Capital availability affects market liquidity and pricing dynamics."
        elif any(keyword in combined for keyword in ['economy', 'gdp', 'employment', 'inflation', 'recession', 'growth']):
            return "ECONOMIC FUNDAMENTALS: Broader economic conditions influence tenant demand, rental growth, and property market performance. Economic strength supports occupancy rates and rent growth while downturns may pressure fundamentals."
        else:
            return "MARKET CONTEXT: This development provides important context for commercial property strategic decision-making, potentially affecting market sentiment, investment flows, or operational considerations for property portfolios."

    def calculate_simple_sentiment(self, items: List[Tuple]) -> str:
        """Calculate market sentiment from news items"""
        if not items:
            return "Neutral"
        
        positive_keywords = ['growth', 'increase', 'strong', 'boost', 'positive', 'up', 'gain', 'improvement', 'rising', 'surge']
        negative_keywords = ['decline', 'fall', 'drop', 'weak', 'negative', 'down', 'loss', 'concern', 'risk', 'falling', 'crash']
        
        positive_score = 0
        negative_score = 0
        
        for item in items:
            title_desc = (item[0] + ' ' + item[2]).lower()
            interest_score = item[3]
            
            # Weight by interest score
            weight = interest_score / 10.0
            
            for keyword in positive_keywords:
                positive_score += title_desc.count(keyword) * weight
            
            for keyword in negative_keywords:
                negative_score += title_desc.count(keyword) * weight
        
        if positive_score > negative_score * 1.3:
            return "Positive"
        elif negative_score > positive_score * 1.3:
            return "Negative"
        else:
            return "Neutral"

    def send_email_plain_text(self, content: str):
        """Send email with plain text content (no markdown)"""
        try:
            logging.info(f"Preparing to send plain text email ({len(content)} characters)")
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"Commercial Property Intelligence - {datetime.now().strftime('%B %d, %Y')}"
            msg['From'] = self.config['gmail_user']
            msg['To'] = self.config['recipient_email']
            
            # Send as plain text with proper formatting
            text_part = MIMEText(content, 'plain', 'utf-8')
            msg.attach(text_part)
            
            # Send email
            with smtplib.SMTP('smtp.gmail.com', 587) as server:
                server.starttls()
                server.login(self.config['gmail_user'], self.config['gmail_password'])
                server.send_message(msg)
            
            logging.info("Plain text email sent successfully")
            
        except Exception as e:
            logging.error(f"Failed to send plain text email: {e}")
            raise

    def send_daily_brief_enhanced_plain_text(self, include_all: bool = False):
        """Enhanced daily brief using plain text formatting - simple fallback version"""
        try:
            # Get more items (last 24 hours) to be more inclusive
            items = self.get_items_for_email(24)
            if items:
                logging.info(f"Generating simple plain text email from {len(items)} items")
                
                # Use simple email generation if enhanced method fails
                try:
                    if hasattr(self, 'generate_daily_email_from_items_enhanced'):
                        content = self.generate_daily_email_from_items_enhanced(items)
                    else:
                        content = self.generate_simple_email_from_items(items)
                except Exception as e:
                    logging.error(f"Enhanced email generation failed: {e}")
                    content = self.generate_simple_email_from_items(items)
                
                if content:
                    # Use plain text email sender
                    self.send_email_plain_text(content)
                    
                    # Mark items as sent
                    cutoff_time = datetime.now() - timedelta(hours=24)
                    self.conn.execute('''
                        UPDATE items SET email_sent = TRUE 
                        WHERE processed_at >= ? AND email_sent = FALSE
                    ''', (cutoff_time,))
                    self.conn.commit()
                    
                    logging.info(f"Plain text email sent successfully")
                else:
                    logging.info("No content generated for email")
            else:
                logging.info("No items found for email")
        except Exception as e:
            logging.error(f"Plain text email error: {e}")
            raise

    def generate_simple_email_from_items(self, items: List[Tuple]) -> str:
        """Simple email generation that always works"""
        current_date = datetime.now().strftime('%B %d, %Y')
        current_time = datetime.now().strftime('%I:%M %p AEST')
        
        # Filter items simply
        filtered_items = []
        for item in items:
            title, link, description, score, summary, source_name = item[:6]
            
            # Basic filtering
            if score >= 4 and title and link:
                # Skip sensitive content
                combined_text = (title + ' ' + (description or '') + ' ' + (summary or '')).lower()
                sensitive_keywords = ['manslaughter', 'murder', 'child abuse', 'dies', 'died', 'death', 'killed']
                
                if not any(keyword in combined_text for keyword in sensitive_keywords):
                    filtered_items.append(item)
        
        # Sort by score
        sorted_items = sorted(filtered_items, key=lambda x: x[3], reverse=True)
        
        # Limit to reasonable number
        display_items = sorted_items[:30]
        
        email_content = f"""🏢 COMMERCIAL PROPERTY INTELLIGENCE BRIEFING
{current_date} • {current_time}

===============================================================================

📊 EXECUTIVE SUMMARY

Total Items Analyzed: {len(display_items)}
Market Analysis: Automated processing of commercial property intelligence

===============================================================================

📋 PRIORITY ITEMS

"""
        
        # Add items
        for i, item in enumerate(display_items, 1):
            title, link, description, score, summary, source = item[:6]
            
            clean_title = title.strip()
            clean_desc = (description or summary or "")[:150].strip()
            
            if clean_desc:
                clean_desc = clean_desc.replace('\n', ' ').replace('\r', ' ')
                clean_desc = re.sub('<[^<]+?>', '', clean_desc)
            
            email_content += f"""{i}. {clean_title}
SCORE: {score}/10 | SOURCE: {source}
LINK: {link}

"""
            
            if clean_desc:
                email_content += f"SUMMARY: {clean_desc}...\n\n"
            
            email_content += "-------------------------------------------------------------------------------\n\n"
        
        # Add footer
        email_content += f"""===============================================================================

🤖 AI-POWERED INTELLIGENCE PLATFORM

Generated: {current_time} AEST
Items Processed: {len(display_items)}
Processing Mode: Simple email generation

💼 Connect with Matt Whiteoak
LinkedIn: https://www.linkedin.com/in/mattwhiteoak

===============================================================================
"""
        
        return email_content

    def send_daily_brief_incremental_plain_text(self):
        """Send plain text email with enhanced logic"""
        should_send, time_period = self.should_send_email_now()
        
        logging.info(f"Email decision: should_send={should_send}, time_period={time_period}")
        
        if should_send:
            logging.info(f"Sending {time_period} plain text email brief...")
            
            # Get items from last 24 hours for email
            items = self.get_items_for_email(24)
            
            if items:
                logging.info(f"Generating plain text email from {len(items)} items")
                content = self.generate_daily_email_from_items_enhanced(items)
                if content:
                    # Check content size and warn if too large
                    content_size = len(content.encode('utf-8'))
                    logging.info(f"Email content size: {content_size/1024:.1f}KB")
                    
                    # Use plain text email sender
                    self.send_email_plain_text(content)
                    
                    # Mark items as sent
                    cutoff_time = datetime.now() - timedelta(hours=24)
                    self.conn.execute('''
                        UPDATE items SET email_sent = TRUE 
                        WHERE processed_at >= ? AND email_sent = FALSE
                    ''', (cutoff_time,))
                    self.conn.commit()
                    
                    logging.info("Plain text email sent successfully")
                else:
                    logging.warning("No content generated for email - all items filtered out")
            else:
                logging.warning("No items found for email")
        else:
            logging.info(f"Skipping email send - {time_period} run")

    def run_scheduled_processing_plain_text(self):
        """Main method for scheduled processing with plain text email"""
        try:
            # Process feeds with optimized method
            result = self.process_feeds_optimized_recent()
            
            # Always try to send email for GitHub Actions, or based on schedule for local runs
            if os.getenv('GITHUB_ACTIONS') or self.should_send_email_now()[0]:
                logging.info("Attempting to send plain text email...")
                
                # Check if enhanced email method exists
                if hasattr(self, 'generate_daily_email_from_items_enhanced'):
                    self.send_daily_brief_incremental_plain_text()
                else:
                    logging.warning("Enhanced email method not found, using fallback")
                    self.send_daily_brief_enhanced_plain_text()
            else:
                logging.info("Skipping email send based on schedule")
            
            logging.info(f"Scheduled processing completed: {result}")
            
        except Exception as e:
            logging.error(f"Scheduled processing error: {e}")
            # Try emergency fallback
            try:
                self.emergency_simple_process_fallback()
            except Exception as fallback_error:
                logging.error(f"Emergency fallback also failed: {fallback_error}")

    def cleanup_old_items(self, days: int = 7):
        """Remove items older than specified days"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            
            # Count items to be deleted
            cursor = self.conn.execute(
                'SELECT COUNT(*) FROM items WHERE processed_at < ?', 
                (cutoff_date,)
            )
            count = cursor.fetchone()[0]
            
            if count > 0:
                # Delete old items
                self.conn.execute(
                    'DELETE FROM items WHERE processed_at < ?', 
                    (cutoff_date,)
                )
                self.conn.commit()
                logging.info(f"Cleaned up {count} items older than {days} days")
            else:
                logging.info(f"No items older than {days} days found")
                
        except Exception as e:
            logging.error(f"Cleanup error: {e}")
            raise


def main():
    """Main function to run the RSS analyzer with plain text email support"""
    try:
        analyzer = RSSAnalyzer()
        
        # Handle command line arguments for GitHub Actions
        if len(sys.argv) > 1:
            command = sys.argv[1].lower()
            
            if command in ['process', 'run', 'test']:
                logging.info("Running single processing cycle with plain text email for GitHub Actions...")
                analyzer.run_scheduled_processing_plain_text()
                logging.info("Processing completed successfully!")
                
            elif command == 'email':
                logging.info("Sending daily plain text email...")
                analyzer.send_daily_brief_enhanced_plain_text()
                logging.info("Email sent successfully!")
                
            elif command == 'cleanup':
                days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
                logging.info(f"Cleaning up items older than {days} days...")
                analyzer.cleanup_old_items(days)
                logging.info("Cleanup completed!")
                
            else:
                print("Usage: python rss_analyzer.py [process|email|cleanup] [days]")
                print("  process - Run RSS processing and send plain text email")
                print("  email   - Send daily plain text email only")  
                print("  cleanup - Remove old database entries (default: 7 days)")
                sys.exit(1)
        else:
            # Run continuous scheduled mode with plain text emails
            logging.info("Starting continuous scheduled mode with plain text emails...")
            
            # Schedule processing 3 times daily
            schedule.every().day.at("06:00").do(analyzer.run_scheduled_processing_plain_text)  # Morning AEST
            schedule.every().day.at("12:00").do(analyzer.run_scheduled_processing_plain_text)  # Midday AEST  
            schedule.every().day.at("18:00").do(analyzer.run_scheduled_processing_plain_text)  # Evening AEST
            
            logging.info("RSS Analyzer started - running scheduled processing with plain text emails...")
            
            # Run immediately on startup
            analyzer.run_scheduled_processing_plain_text()
            
            # Keep running scheduled tasks
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        logging.info("RSS Analyzer stopped by user")
    except Exception as e:
        logging.error(f"RSS Analyzer error: {e}")
        raise


if __name__ == "__main__":
    main()
