#!/usr/bin/env python3
"""
RSS Feed Analyzer for A-REIT CEO/COO - Executive Intelligence Platform
Monitors RSS feeds, evaluates content with OpenAI, and sends daily emails
ENHANCED VERSION - Shaan Puri Style with HTML Formatting
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
import random

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

    def get_spicy_subject_suffix(self, items: List[Tuple]) -> str:
        """Generate a Shaan Puri style subject line suffix based on top news"""
        if not items or len(items) == 0:
            return "The Market's Moving (You Should Too)"
        
        top_item = items[0]
        title = top_item[0].lower()
        
        # Generate contextual subject lines
        if 'interest rate' in title or 'rba' in title:
            return "Rate Cuts = Property Party 🎉"
        elif 'crash' in title or 'fall' in title:
            return "Blood in the Streets (Time to Buy?)"
        elif 'boom' in title or 'surge' in title:
            return "Everyone's Getting Rich (Except You?)"
        elif any(word in title for word in ['ai', 'technology', 'digital']):
            return "Robots Are Coming for Real Estate"
        elif 'china' in title or 'asia' in title:
            return "The Dragon Moves (Markets Follow)"
        else:
            return "Big Moves You're Missing"

    def get_ai_powered_opening(self, sentiment: str, critical_count: int, top_items: List[Tuple]) -> str:
        """Generate AI-powered custom opening based on actual news"""
        try:
            # Build context from top items
            news_context = ""
            for i, item in enumerate(top_items[:3], 1):
                news_context += f"{i}. {item[0]}\n"
            
            prompt = f"""You're a sharp, witty property market analyst with personality like Shaan Puri. 
Write a 2-3 sentence opening hook for today's property intelligence briefing.

Market sentiment: {sentiment}
Critical alerts: {critical_count}
Top headlines:
{news_context}

Make it conversational, slightly edgy, and engaging. Reference the actual news but keep it punchy.
No corporate speak. Talk like you're texting a smart friend who needs to know what's up.
If there's big news, lead with it. If it's quiet, acknowledge that but hint at opportunity."""

            response = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=100,
                temperature=0.7,
                timeout=5
            )
            
            opening = response.choices[0].message.content.strip()
            
            if critical_count > 0:
                opening += f"\n\n🚨 HOLD UP - {critical_count} thing{'s' if critical_count > 1 else ''} you NEED to see right now."
            
            return opening
            
        except Exception as e:
            logging.warning(f"AI opening generation failed: {e}, using fallback")
            return self.get_fallback_opening(sentiment, critical_count)

    def get_fallback_opening(self, sentiment: str, critical_count: int) -> str:
        """Fallback opening if AI fails"""
        openings = {
            'Positive': [
                "Alright, the market's looking juicy today. Here's what's popping:",
                "Bulls are running wild. Let me break down what's actually happening:",
                "Everyone's making money today (are you?). Here's the scoop:",
            ],
            'Negative': [
                "Oof. The market's taking a beating. But here's where the opportunity is:",
                "Blood in the water. Smart money knows what to do. Here's the play:",
                "Everyone's panicking. You know what that means? Opportunity. Let's dive in:",
            ],
            'Neutral': [
                "Market's playing it cool today. But underneath? Big moves brewing:",
                "Quiet on the surface, chaos underneath. Here's what matters:",
                "While everyone's sleeping, smart money is moving. Pay attention:",
            ]
        }
        
        base = random.choice(openings.get(sentiment, openings['Neutral']))
        
        if critical_count > 0:
            base += f"\n\n🚨 HOLD UP - {critical_count} thing{'s' if critical_count > 1 else ''} you NEED to see right now."
        
        return base

    def generate_shaan_style_context(self, title: str, description: str, score: int) -> str:
        """Generate market context in Shaan Puri's conversational style with AI enhancement"""
        try:
            # For high-priority items, use AI for custom analysis
            if score >= 7:
                prompt = f"""You're a sharp commercial property analyst with personality like Shaan Puri.
Give me a 2-sentence hot take on how this impacts commercial property markets.
Be specific, conversational, and slightly edgy. No corporate speak.
Talk like you're explaining to a smart friend over drinks.

Title: {title}
Description: {description[:300]}

Focus on the REAL impact - what actually changes for property investors/operators?
Start with "THE PLAY:" or "REALITY CHECK:" or "WATCH THIS:" based on the news."""

                response = openai.ChatCompletion.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=100,
                    temperature=0.5,
                    timeout=5
                )
                
                return response.choices[0].message.content.strip()
        except Exception as e:
            logging.debug(f"AI context generation failed, using fallback: {e}")
        
        # Fallback to pattern-based responses
        title_lower = title.lower()
        desc_lower = (description or "").lower()
        combined = title_lower + " " + desc_lower
        
        # Interest rates - the big one
        if any(keyword in combined for keyword in ['interest rate', 'rba', 'fed', 'monetary', 'cash rate']):
            if 'cut' in combined or 'lower' in combined or 'fall' in combined:
                return "THE PLAY: Rate cuts = cheap money = property prices go brrr 📈. Every 0.25% cut adds ~3% to property values. Time to refi everything."
            else:
                return "REALITY CHECK: Higher rates = property values get crushed. But the best deals happen when everyone else is scared. Start stockpiling cash."
        
        # Office/workplace
        elif any(keyword in combined for keyword in ['office', 'workplace', 'remote work', 'hybrid', 'cbd']):
            return "THE OFFICE GAME: Premium offices are printing money while B-grade buildings become zombie assets. Flight to quality is real. Own the best or own nothing."
        
        # Retail
        elif any(keyword in combined for keyword in ['retail', 'shopping', 'consumer', 'e-commerce', 'mall']):
            return "RETAIL REALITY: Dead malls becoming fulfillment centers = 3x the rent. E-commerce didn't kill retail, it just changed the game. Adapt or die."
        
        # Technology
        elif any(keyword in combined for keyword in ['technology', 'ai', 'automation', 'digital', 'proptech']):
            if score >= 7:
                return "TECH TSUNAMI: Properties without smart tech are becoming the Nokia phones of real estate. The companies adapting? They're printing money."
            else:
                return "TECH WATCH: 90% of PropTech dies, but the 10% that survive reshape entire markets. Keep an eye on this."
        
        # Default but make it spicy
        else:
            if score >= 8:
                return "PAY ATTENTION: This is the kind of thing that triggers domino effects. The pros are already positioning."
            elif score >= 6:
                return "CONTEXT MATTERS: This is the undercurrent stuff that shapes next quarter's headlines."
            else:
                return "BREADCRUMB: Small move, but markets are conversations. File it, might matter later."

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

    def build_html_email(self, items: List[Tuple]) -> str:
        """Build engaging HTML email in Shaan Puri style"""
        
        logging.info(f"Building Shaan-style HTML email for {len(items)} items...")
        
        current_date = datetime.now().strftime('%B %d, %Y')
        current_time = datetime.now().strftime('%I:%M %p AEST')
        
        # Sort and group items
        sorted_items = sorted(items, key=lambda x: x[3], reverse=True)
        
        critical_items = [item for item in sorted_items if item[3] >= 8]
        high_items = [item for item in sorted_items if item[3] == 7]
        medium_items = [item for item in sorted_items if item[3] == 6]
        normal_items = [item for item in sorted_items if item[3] == 5]
        
        total_items = len(sorted_items)
        critical_count = len(critical_items)
        sentiment = self.calculate_simple_sentiment(sorted_items)
        
        # Get AI-powered opening
        opening_hook = self.get_ai_powered_opening(sentiment, critical_count, sorted_items[:5])
        
        # Build HTML email
        html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #2c3e50;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            background: #f8f9fa;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        }}
        .header h1 {{
            margin: 0;
            font-size: 28px;
            font-weight: 700;
        }}
        .header .tagline {{
            margin-top: 10px;
            font-size: 16px;
            opacity: 0.95;
        }}
        .scorecard {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 30px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.08);
        }}
        .scorecard-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin-top: 15px;
        }}
        .stat-box {{
            background: #f8f9fa;
            padding: 15px;
            border-radius: 8px;
            text-align: center;
        }}
        .stat-number {{
            font-size: 24px;
            font-weight: bold;
            color: #667eea;
        }}
        .stat-label {{
            font-size: 12px;
            color: #6c757d;
            text-transform: uppercase;
        }}
        .opening {{
            background: white;
            padding: 25px;
            border-left: 4px solid #667eea;
            margin-bottom: 30px;
            border-radius: 5px;
            font-size: 16px;
            color: #2c3e50;
            box-shadow: 0 5px 15px rgba(0,0,0,0.08);
        }}
        .section {{
            background: white;
            padding: 25px;
            margin-bottom: 30px;
            border-radius: 10px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.08);
        }}
        .section-header {{
            font-size: 20px;
            font-weight: bold;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid #f0f0f0;
        }}
        .critical {{ color: #dc3545; }}
        .high {{ color: #fd7e14; }}
        .medium {{ color: #ffc107; }}
        .normal {{ color: #28a745; }}
        .news-item {{
            margin-bottom: 25px;
            padding: 20px;
            background: #f8f9fa;
            border-radius: 8px;
            border-left: 3px solid #667eea;
        }}
        .news-title {{
            font-size: 16px;
            font-weight: bold;
            color: #2c3e50;
            margin-bottom: 10px;
        }}
        .news-context {{
            background: white;
            padding: 15px;
            border-radius: 5px;
            margin: 10px 0;
            font-size: 14px;
            line-height: 1.5;
            color: #495057;
            border: 1px solid #e9ecef;
        }}
        .news-meta {{
            font-size: 12px;
            color: #6c757d;
            margin-top: 10px;
        }}
        .news-link {{
            color: #667eea;
            text-decoration: none;
            font-weight: 500;
        }}
        .news-link:hover {{
            text-decoration: underline;
        }}
        .score-badge {{
            display: inline-block;
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 11px;
            font-weight: bold;
            margin-left: 10px;
        }}
        .score-high {{ background: #dc3545; color: white; }}
        .score-medium {{ background: #ffc107; color: #000; }}
        .score-low {{ background: #28a745; color: white; }}
        .bottom-line {{
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
            color: white;
            padding: 25px;
            border-radius: 10px;
            margin: 30px 0;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        }}
        .footer {{
            text-align: center;
            padding: 20px;
            color: #6c757d;
            font-size: 14px;
        }}
        .footer a {{
            color: #667eea;
            text-decoration: none;
        }}
        @media (max-width: 600px) {{
            body {{ padding: 10px; }}
            .header {{ padding: 20px; }}
            .section {{ padding: 15px; }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🏢 Property Intelligence Briefing</h1>
        <div class="tagline">Your unfair advantage in commercial real estate</div>
    </div>
    
    <div class="scorecard">
        <strong style="font-size: 18px;">📊 THE SCORECARD</strong>
        <div class="scorecard-grid">
            <div class="stat-box">
                <div class="stat-number">{total_items}</div>
                <div class="stat-label">Items Analyzed</div>
            </div>
            <div class="stat-box">
                <div class="stat-number">{critical_count}</div>
                <div class="stat-label">Red Alerts</div>
            </div>
            <div class="stat-box">
                <div class="stat-number">{len(high_items)}</div>
                <div class="stat-label">High Priority</div>
            </div>
            <div class="stat-box">
                <div class="stat-number">{sentiment}</div>
                <div class="stat-label">Market Vibe</div>
            </div>
        </div>
    </div>
    
    <div class="opening">
        {opening_hook.replace(chr(10), '<br>')}
    </div>
"""

        # Add sections based on priority
        if critical_items:
            html_content += f"""
    <div class="section">
        <div class="section-header critical">🔥 STOP EVERYTHING - THIS MATTERS ({len(critical_items)} items)</div>
        {self.generate_html_news_items(critical_items)}
    </div>
"""

        if high_items:
            html_content += f"""
    <div class="section">
        <div class="section-header high">👀 WORTH YOUR ATTENTION ({len(high_items)} items)</div>
        {self.generate_html_news_items(high_items)}
    </div>
"""

        if medium_items[:10]:  # Limit medium items
            html_content += f"""
    <div class="section">
        <div class="section-header medium">📌 QUICK HITS ({min(len(medium_items), 10)} items)</div>
        {self.generate_html_news_items(medium_items[:10])}
    </div>
"""

        # Add bottom line
        action_rec = self.get_action_recommendation(sentiment, critical_count)
        html_content += f"""
    <div class="bottom-line">
        <strong style="font-size: 18px;">THE BOTTOM LINE</strong><br><br>
        Market sentiment: <strong>{sentiment}</strong><br>
        Your move: <strong>{action_rec}</strong><br><br>
        <em>Fortune favors the brave (and the informed). You're now both.</em>
    </div>
    
    <div class="footer">
        <strong>Keep building,</strong><br>
        Your AI Property Intel System<br><br>
        <em>P.S. - While your competitors are reading newspapers, you just got the cliff notes.<br>
        Use this 15-minute advantage wisely.</em><br><br>
        Built with 🤖 by <a href="https://www.linkedin.com/in/mattwhiteoak">Matt Whiteoak</a>
    </div>
</body>
</html>"""

        return html_content

    def generate_html_news_items(self, items: List[Tuple]) -> str:
        """Generate HTML for news items with Shaan style"""
        html = ""
        
        for i, item in enumerate(items[:20], 1):  # Limit items per section
            title, link, description, score, summary, source = item[:6]
            
            # Get context
            context = self.generate_shaan_style_context(title, description, score)
            
            # Score badge color
            if score >= 8:
                badge_class = "score-high"
            elif score >= 6:
                badge_class = "score-medium"
            else:
                badge_class = "score-low"
            
            html += f"""
        <div class="news-item">
            <div class="news-title">
                {i}. {title}
                <span class="score-badge {badge_class}">Score: {score}/10</span>
            </div>
            <div class="news-context">
                {context}
            </div>
            <div class="news-meta">
                Source: {source} | 
                <a href="{link}" class="news-link">Read full story →</a>
            </div>
        </div>
"""
        
        return html

    def get_action_recommendation(self, sentiment: str, critical_count: int) -> str:
        """Get Shaan-style action recommendation"""
        if critical_count >= 2:
            return "Multiple red flags. Time to call your CFO. Like, right now."
        elif critical_count == 1:
            return "One big thing to handle today. Block 30 minutes, handle it, then grab coffee."
        elif sentiment == 'Positive':
            return "Market's hot. Time to be aggressive (but not stupid)."
        elif sentiment == 'Negative':
            return "Market's scared. You know what Buffett says about fear and greed, right?"
        else:
            return "Stay alert, stay liquid. Big moves coming, direction unclear."

    def generate_daily_email_from_items_enhanced(self, items: List[Tuple]) -> Optional[str]:
        """Generate HTML formatted email with Shaan Puri style"""
        logging.info(f"Starting enhanced email generation from {len(items)} items...")
        
        if not items:
            logging.warning("No items provided for email generation")
            return None
        
        start_time = time.time()
        
        try:
            # Filter items
            filtered_items = []
            seen_titles = set()
            
            for item in items:
                title, link, description, score, summary, source_name = item[:6]
                
                # Skip specific URLs and sensitive content
                if link == "https://www.afr.com/topic/commercial-real-estate-5vu":
                    continue
                
                combined_text = (title + ' ' + (description or '') + ' ' + (summary or '')).lower()
                
                # Skip errors
                if any(phrase in title.lower() for phrase in [
                    'not found', 'sign up to rss.app', 'error 404', 'access denied'
                ]):
                    continue
                
                # Skip sensitive content
                sensitive_keywords = [
                    'manslaughter', 'murder', 'child abuse', 'dies', 'died', 'death',
                    'sexual assault', 'rape', 'domestic violence', 'suicide'
                ]
                
                if any(keyword in combined_text for keyword in sensitive_keywords):
                    continue
                
                # Skip duplicates
                title_key = title.lower().strip()
                if title_key in seen_titles:
                    continue
                seen_titles.add(title_key)
                
                # Include items with score >= 4
                if score >= 4:
                    filtered_items.append(item)
            
            logging.info(f"Filtered items for email: {len(filtered_items)}")
            
            if not filtered_items:
                logging.warning("No items remaining after filtering")
                return None
            
            # Generate HTML email
            email_content = self.build_html_email(filtered_items)
            
            elapsed_time = time.time() - start_time
            logging.info(f"Email generation completed in {elapsed_time:.1f} seconds")
            
            return email_content
            
        except Exception as e:
            logging.error(f"Email generation failed: {e}")
            return self.generate_fallback_email(items)
    
    def generate_fallback_email(self, items: List[Tuple]) -> str:
        """Generate a simple fallback email if main generation fails"""
        logging.info("Generating fallback email")
        
        current_date = datetime.now().strftime('%B %d, %Y')
        current_time = datetime.now().strftime('%I:%M %p AEST')
        
        filtered_items = [item for item in items if item[3] >= 4][:20]
        sorted_items = sorted(filtered_items, key=lambda x: x[3], reverse=True)
        
        html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }}
        h1 {{ color: #333; }}
        .item {{ margin-bottom: 20px; padding: 15px; background: #f5f5f5; }}
    </style>
</head>
<body>
    <h1>🏢 Property Intelligence - {current_date}</h1>
    <p>Fallback mode - {len(sorted_items)} items</p>
"""
        
        for i, item in enumerate(sorted_items[:10], 1):
            title, link, description, score, summary, source = item[:6]
            html_content += f"""
    <div class="item">
        <strong>{i}. {title}</strong><br>
        Score: {score}/10
            | Source: {source}<br>
       <a href="{link}">Read more</a>
   </div>
"""
       
       html_content += """
</body>
</html>"""
       
       return html_content

   def send_email_html(self, content: str):
       """Send email with HTML content"""
       try:
           logging.info(f"Preparing to send HTML email ({len(content)} characters)")
           
           # Get subject line
           items = self.get_items_for_email(24)
           subject_suffix = self.get_spicy_subject_suffix(items)
           
           msg = MIMEMultipart('alternative')
           msg['Subject'] = f"🔥 Property Intel: {subject_suffix} - {datetime.now().strftime('%B %d')}"
           msg['From'] = self.config['gmail_user']
           msg['To'] = self.config['recipient_email']
           
           # Create plain text version (fallback)
           plain_text = """
Property Intelligence Briefing

This email is best viewed in HTML format.
Please enable HTML viewing in your email client for the full experience.

---
Built with AI by Matt Whiteoak
"""
           
           # Attach parts
           text_part = MIMEText(plain_text, 'plain', 'utf-8')
           html_part = MIMEText(content, 'html', 'utf-8')
           
           msg.attach(text_part)
           msg.attach(html_part)
           
           # Send email
           with smtplib.SMTP('smtp.gmail.com', 587) as server:
               server.starttls()
               server.login(self.config['gmail_user'], self.config['gmail_password'])
               server.send_message(msg)
           
           logging.info("HTML email sent successfully")
           
       except Exception as e:
           logging.error(f"Failed to send HTML email: {e}")
           raise

   def send_daily_brief_enhanced(self):
       """Enhanced daily brief with HTML formatting and Shaan Puri style"""
       try:
           # Get items from last 24 hours
           items = self.get_items_for_email(24)
           if items:
               content = self.generate_daily_email_from_items_enhanced(items)
               if content:
                   # Send HTML email
                   self.send_email_html(content)
                   
                   # Mark items as sent
                   cutoff_time = datetime.now() - timedelta(hours=24)
                   self.conn.execute('''
                       UPDATE items SET email_sent = TRUE 
                       WHERE processed_at >= ? AND email_sent = FALSE
                   ''', (cutoff_time,))
                   self.conn.commit()
                   
                   logging.info(f"Enhanced HTML email sent successfully")
               else:
                   logging.info("No content generated for email")
           else:
               logging.info("No items found for email")
       except Exception as e:
           logging.error(f"Enhanced email error: {e}")
           raise

   def send_daily_brief_incremental(self):
       """Send HTML email with enhanced logic"""
       should_send, time_period = self.should_send_email_now()
       
       logging.info(f"Email decision: should_send={should_send}, time_period={time_period}")
       
       if should_send:
           logging.info(f"Sending {time_period} HTML email brief...")
           
           # Get items from last 24 hours
           items = self.get_items_for_email(24)
           
           if items:
               logging.info(f"Generating HTML email from {len(items)} items")
               content = self.generate_daily_email_from_items_enhanced(items)
               if content:
                   # Check content size
                   content_size = len(content.encode('utf-8'))
                   logging.info(f"Email content size: {content_size/1024:.1f}KB")
                   
                   # Send HTML email
                   self.send_email_html(content)
                   
                   # Mark items as sent
                   cutoff_time = datetime.now() - timedelta(hours=24)
                   self.conn.execute('''
                       UPDATE items SET email_sent = TRUE 
                       WHERE processed_at >= ? AND email_sent = FALSE
                   ''', (cutoff_time,))
                   self.conn.commit()
                   
                   logging.info("HTML email sent successfully")
               else:
                   logging.warning("No content generated for email")
           else:
               logging.warning("No items found for email")
       else:
           logging.info(f"Skipping email send - {time_period} run")

   def run_scheduled_processing(self):
       """Main method for scheduled processing with HTML email"""
       try:
           # Process feeds with optimized method
           result = self.process_feeds_optimized_recent()
           
           # Always try to send email for GitHub Actions, or based on schedule
           if os.getenv('GITHUB_ACTIONS') or self.should_send_email_now()[0]:
               logging.info("Attempting to send HTML email...")
               self.send_daily_brief_incremental()
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
   """Main function to run the RSS analyzer with HTML email support"""
   try:
       analyzer = RSSAnalyzer()
       
       # Handle command line arguments for GitHub Actions
       if len(sys.argv) > 1:
           command = sys.argv[1].lower()
           
           if command in ['process', 'run', 'test']:
               logging.info("Running single processing cycle with HTML email for GitHub Actions...")
               analyzer.run_scheduled_processing()
               logging.info("Processing completed successfully!")
               
           elif command == 'email':
               logging.info("Sending daily HTML email...")
               analyzer.send_daily_brief_enhanced()
               logging.info("Email sent successfully!")
               
           elif command == 'cleanup':
               days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
               logging.info(f"Cleaning up items older than {days} days...")
               analyzer.cleanup_old_items(days)
               logging.info("Cleanup completed!")
               
           else:
               print("Usage: python rss_analyzer.py [process|email|cleanup] [days]")
               print("  process - Run RSS processing and send HTML email")
               print("  email   - Send daily HTML email only")  
               print("  cleanup - Remove old database entries (default: 7 days)")
               sys.exit(1)
       else:
           # Run continuous scheduled mode with HTML emails
           logging.info("Starting continuous scheduled mode with HTML emails...")
           
           # Schedule processing 3 times daily
           schedule.every().day.at("06:00").do(analyzer.run_scheduled_processing)  # Morning AEST
           schedule.every().day.at("12:00").do(analyzer.run_scheduled_processing)  # Midday AEST  
           schedule.every().day.at("18:00").do(analyzer.run_scheduled_processing)  # Evening AEST
           
           logging.info("RSS Analyzer started - running scheduled processing with HTML emails...")
           
           # Run immediately on startup
           analyzer.run_scheduled_processing()
           
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
