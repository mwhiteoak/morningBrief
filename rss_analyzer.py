#!/usr/bin/env python3
"""
RSS Feed Analyzer for A-REIT CEO/COO - Executive Intelligence Platform
Monitors RSS feeds, evaluates content with OpenAI, and sends daily emails
OPTIMIZED VERSION - 3x Daily Incremental Processing
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
        """Determine if we should send email based on time of day"""
        now = datetime.now()
        hour = now.hour
        
        # Convert to AEST equivalent (assuming UTC+10)
        aest_hour = (hour + 10) % 24
        
        # Send email only at morning run (6 AM AEST = 20 UTC previous day)
        if 20 <= hour <= 23 or hour <= 2:  # Around 6 AM AEST
            return True, "morning"
        elif 0 <= hour <= 4:  # Around 12 PM AEST  
            return False, "midday"
        elif 6 <= hour <= 10:  # Around 6 PM AEST
            return False, "evening"
        else:
            return False, "other"


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
        """Quick auto-scoring without AI"""
        title_lower = item.title.lower()
        desc_lower = item.description.lower()
        combined = title_lower + " " + desc_lower
        
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
        """Quick AI batch processing with larger batches"""
        if not items:
            return 0
        
        batch_size = 15  # Larger batches for efficiency
        processed_count = 0
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            
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
        """Emergency fallback if optimized processing fails"""
        logging.info("🚨 EMERGENCY FALLBACK: Simple processing...")
        
        cutoff_time = datetime.now() - timedelta(hours=6)
        total_items = 0
        
        # Process only first 5 feeds with simple scoring
        emergency_feeds = self.rss_feeds[:5]
        
        for feed_config in emergency_feeds:
            try:
                # Use basic fetch_feed_items method
                items = self.fetch_feed_items(feed_config)
                recent_items = [item for item in items if item.published >= cutoff_time]
                
                for item in recent_items[:3]:  # Max 3 items per feed for speed
                    if not self.item_exists(item.link):
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
        
        logging.info(f"✅ Emergency fallback completed: {total_items} items")
        
        return {
            'total_scanned': total_items,
            'new_items': total_items,
            'processed': total_items
        }

    def should_send_email_now(self) -> Tuple[bool, str]:
        """Check if we should send email based on schedule"""
        processor = IncrementalProcessor(self)
        return processor.should_send_email()

    def get_items_for_email(self, hours_back: int = 24) -> List[Tuple]:
        """Get items for email from the last N hours"""
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        
        cursor = self.conn.execute('''
            SELECT title, link, description, interest_score, ai_summary, source_name, 
                   processed_at, category, sentiment, key_metrics, geographic_tags, sector_tags
            FROM items 
            WHERE processed_at >= ? AND email_sent = FALSE
            ORDER BY interest_score DESC, processed_at DESC
        ''', (cutoff_time,))
        
        return cursor.fetchall()

    def generate_daily_email_from_items_enhanced(self, items: List[Tuple]) -> Optional[str]:
        """Enhanced email generation that prevents clipping"""
        if not items:
            return None
        
        # CRITICAL: Limit total content to prevent email clipping
        # Most email clients clip after ~102KB, so we need to be aggressive about limiting content
        
        # Filter and prioritize items more aggressively
        high_priority = [item for item in items if item[3] >= 8][:8]  # Max 8 high priority
        medium_priority = [item for item in items if 6 <= item[3] < 8][:6]  # Max 6 medium
        low_priority = [item for item in items if 4 <= item[3] < 6][:4]  # Max 4 low
        
        # Total: Maximum 18 items to prevent clipping
        filtered_items = high_priority + medium_priority + low_priority
        
        # Skip obvious errors and duplicates
        final_items = []
        seen_titles = set()
        
        for item in filtered_items:
            title, link, description, score, summary, source_name = item[:6]
            
            # Skip errors
            if any(phrase in title.lower() for phrase in [
                'not found', 'sign up to rss.app', 'error', 'access denied', '404'
            ]):
                continue
            
            # Skip duplicates (fuzzy matching)
            title_key = title.lower()[:50]  # First 50 chars for fuzzy matching
            if title_key in seen_titles:
                continue
            seen_titles.add(title_key)
            
            final_items.append(item)
            
            # Hard limit to prevent clipping
            if len(final_items) >= 15:
                break
        
        if not final_items:
            return None
        
        return self.build_executive_email_html(final_items)

    def build_executive_email_html(self, items: List[Tuple]) -> str:
        """Build clean, focused executive intelligence briefing"""
        
        current_date = datetime.now().strftime('%B %d, %Y')
        current_time = datetime.now().strftime('%I:%M %p AEST')
        
        # Calculate key metrics
        total_items = len(items)
        high_priority_count = len([item for item in items if item[3] >= 7])
        critical_count = len([item for item in items if item[3] >= 8])
        
        # Market sentiment
        sentiment = self.calculate_simple_sentiment(items)
        sentiment_emoji = {"Positive": "📈", "Negative": "📉", "Neutral": "📊"}[sentiment]
        sentiment_color = {"Positive": "#10b981", "Negative": "#ef4444", "Neutral": "#6366f1"}[sentiment]
        
        # Sort by relevance
        sorted_items = sorted(items, key=lambda x: x[3], reverse=True)
        
        # Filter for only relevant items (score >= 6)
        relevant_items = [item for item in sorted_items if item[3] >= 6][:10]
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Commercial Property Intelligence • {current_date}</title>
            <style>
                * {{ box-sizing: border-box; }}
                body {{ 
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
                    margin: 0; padding: 20px; 
                    background: #f8fafc; color: #1e293b; line-height: 1.6;
                }}
                .container {{ 
                    max-width: 700px; margin: 0 auto; 
                    background: white; border-radius: 12px;
                    box-shadow: 0 4px 20px rgba(0,0,0,0.08);
                    overflow: hidden;
                }}
                
                .header {{
                    background: linear-gradient(135deg, #1e293b 0%, #475569 100%);
                    color: white; padding: 30px; text-align: center;
                }}
                .header h1 {{ 
                    margin: 0; font-size: 24px; font-weight: 700; 
                }}
                .header .date {{ 
                    margin: 8px 0 0 0; opacity: 0.9; font-size: 14px;
                }}
                
                .summary-bar {{
                    background: {sentiment_color}15;
                    padding: 20px; border-bottom: 1px solid #e2e8f0;
                    display: flex; justify-content: space-between; align-items: center;
                    flex-wrap: wrap; gap: 15px;
                }}
                .metric {{
                    text-align: center; flex: 1; min-width: 80px;
                }}
                .metric-value {{
                    font-size: 20px; font-weight: 700; 
                    color: {sentiment_color}; margin: 0;
                }}
                .metric-label {{
                    font-size: 11px; color: #64748b; 
                    text-transform: uppercase; margin: 4px 0 0 0;
                }}
                
                .content {{ padding: 30px; }}
                
                .executive-summary {{
                    background: #f1f5f9; border-radius: 8px; 
                    padding: 20px; margin-bottom: 30px;
                    border-left: 4px solid {sentiment_color};
                }}
                .summary-title {{
                    font-size: 16px; font-weight: 700; 
                    color: #1e293b; margin: 0 0 12px 0;
                }}
                .summary-text {{
                    font-size: 14px; line-height: 1.6; color: #475569;
                }}
                
                .news-section {{
                    margin-bottom: 30px;
                }}
                .section-title {{
                    font-size: 18px; font-weight: 700; color: #1e293b; 
                    margin: 0 0 20px 0; padding-bottom: 10px;
                    border-bottom: 2px solid #e2e8f0;
                }}
                
                .news-item {{
                    background: white; border: 1px solid #e2e8f0;
                    border-radius: 8px; padding: 20px; margin-bottom: 16px;
                    transition: box-shadow 0.2s;
                }}
                .news-item:hover {{
                    box-shadow: 0 4px 12px rgba(0,0,0,0.1);
                }}
                .news-critical {{ border-left: 4px solid #ef4444; }}
                .news-high {{ border-left: 4px solid #f59e0b; }}
                .news-medium {{ border-left: 4px solid #10b981; }}
                
                .news-header {{
                    display: flex; justify-content: space-between; 
                    align-items: flex-start; margin-bottom: 12px; gap: 15px;
                }}
                .news-title {{
                    font-size: 15px; font-weight: 600; 
                    line-height: 1.4; margin: 0; flex: 1;
                }}
                .news-title a {{
                    color: #1e293b; text-decoration: none;
                }}
                .news-title a:hover {{ color: {sentiment_color}; }}
                
                .news-score {{
                    background: {sentiment_color}; color: white; 
                    padding: 4px 10px; border-radius: 12px;
                    font-size: 11px; font-weight: 600; white-space: nowrap;
                }}
                
                .relevance-box {{
                    background: #fef3c7; border: 1px solid #f59e0b;
                    border-radius: 6px; padding: 12px; margin-top: 12px;
                }}
                .relevance-title {{
                    font-size: 12px; font-weight: 600; 
                    color: #92400e; margin: 0 0 6px 0;
                }}
                .relevance-text {{
                    font-size: 13px; line-height: 1.5; 
                    color: #78350f; margin: 0;
                }}
                
                .news-meta {{
                    font-size: 12px; color: #64748b; 
                    margin-bottom: 10px;
                }}
                
                .footer {{
                    background: #1e293b; color: white; 
                    padding: 20px; text-align: center;
                }}
                .footer-text {{
                    font-size: 12px; opacity: 0.8; margin: 0;
                }}
                
                @media (max-width: 600px) {{
                    body {{ padding: 10px; }}
                    .summary-bar {{ flex-direction: column; }}
                    .news-header {{ flex-direction: column; align-items: flex-start; }}
                    .content {{ padding: 20px; }}
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🏢 Commercial Property Intelligence</h1>
                    <div class="date">{current_date} • {current_time}</div>
                </div>
                
                <div class="summary-bar">
                    <div class="metric">
                        <div class="metric-value">{len(relevant_items)}</div>
                        <div class="metric-label">Relevant Items</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value">{critical_count}</div>
                        <div class="metric-label">Critical</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value">{sentiment_emoji}</div>
                        <div class="metric-label">Market Sentiment</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value">🤖</div>
                        <div class="metric-label">AI Analyzed</div>
                    </div>
                </div>
                
                <div class="content">
                    <div class="executive-summary">
                        <div class="summary-title">📊 Executive Summary</div>
                        <div class="summary-text">
                            {self.generate_clean_executive_summary(relevant_items, sentiment)}
                        </div>
                    </div>
                    
                    <div class="news-section">
                        <div class="section-title">🎯 Commercial Property Impact Analysis</div>
                        {self.generate_clean_news_items(relevant_items)}
                    </div>
                </div>
                
                <div class="footer">
                    <div class="footer-text">
                        AI-Powered Commercial Property Intelligence • {len(relevant_items)} sources analyzed<br>
                        Generated at {current_time} AEST • Confidential Executive Briefing
                    </div>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html

    def generate_clean_executive_summary(self, items: List[Tuple], sentiment: str) -> str:
        """Generate clean, focused executive summary"""
        if not items:
            return "No significant commercial property developments identified in current market analysis."
        
        high_impact = [item for item in items if item[3] >= 8]
        medium_impact = [item for item in items if 6 <= item[3] < 8]
        
        summary = f"Market analysis reveals {len(items)} developments with commercial property implications. "
        
        if high_impact:
            summary += f"{len(high_impact)} high-impact items require immediate attention. "
        
        if sentiment == "Positive":
            summary += "Overall market sentiment indicates favorable conditions for commercial property investment and development."
        elif sentiment == "Negative":
            summary += "Market conditions suggest caution in commercial property decisions with heightened risk monitoring required."
        else:
            summary += "Market sentiment remains stable with balanced risk-reward scenarios across commercial property sectors."
        
        return summary

    def generate_clean_news_items(self, items: List[Tuple]) -> str:
        """Generate clean news items with commercial property relevance"""
        if not items:
            return "<p>No relevant commercial property developments identified.</p>"
        
        html = ""
        
        for item in items:
            title, link, description, score, summary, source = item[:6]
            
            # Determine priority styling
            if score >= 8:
                priority_class = "news-critical"
                priority_label = "CRITICAL"
            elif score >= 7:
                priority_class = "news-high"
                priority_label = "HIGH"
            else:
                priority_class = "news-medium"
                priority_label = "MEDIUM"
            
            # Generate AI commercial property relevance
            relevance = self.generate_commercial_property_relevance(title, description, score)
            
            # Clean summary
            clean_summary = (summary or description)[:150] + "..." if (summary or description) else ""
            clean_summary = re.sub('<[^<]+?>', '', clean_summary)
            
            html += f'''
            <div class="news-item {priority_class}">
                <div class="news-header">
                    <div class="news-title">
                        <a href="{link}" target="_blank">{title}</a>
                    </div>
                    <div class="news-score">{score}/10</div>
                </div>
                
                <div class="news-meta">
                    {priority_label} • {source} • {datetime.now().strftime('%H:%M')}
                </div>
                
                <div class="relevance-box">
                    <div class="relevance-title">🏢 Commercial Property Impact</div>
                    <div class="relevance-text">{relevance}</div>
                </div>
            </div>
            '''
        
        return html

    def generate_commercial_property_relevance(self, title: str, description: str, score: int) -> str:
        """Generate AI-powered commercial property relevance analysis"""
        title_lower = title.lower()
        desc_lower = (description or "").lower()
        combined = title_lower + " " + desc_lower
        
        # Use AI to generate specific commercial property relevance
        try:
            prompt = f"""Analyze this news item for commercial property relevance in 1-2 sentences:
            
Title: {title}
Description: {description[:200]}

Explain specifically HOW this impacts commercial property (office, retail, industrial, logistics) investment, development, or operations. Be concrete and actionable for A-REIT executives."""

            response = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=100,
                temperature=0.1
            )
            
            relevance = response.choices[0].message.content.strip()
            return relevance
            
        except Exception as e:
            logging.warning(f"AI relevance analysis failed: {e}")
            
            # Fallback analysis based on keywords
            if any(keyword in combined for keyword in ['interest rate', 'rba', 'fed', 'monetary']):
                return "Interest rate changes directly impact commercial property valuations, cap rates, and debt servicing costs across all asset classes."
            elif any(keyword in combined for keyword in ['office', 'workplace', 'remote work', 'hybrid']):
                return "Workplace trends affect office demand, occupancy rates, and rental growth prospects in CBD and suburban markets."
            elif any(keyword in combined for keyword in ['retail', 'shopping', 'consumer', 'e-commerce']):
                return "Consumer behavior shifts influence retail property demand, tenant mix strategies, and asset repositioning requirements."
            elif any(keyword in combined for keyword in ['industrial', 'logistics', 'warehouse', 'supply chain']):
                return "Supply chain developments impact industrial property demand, logistics hub valuations, and last-mile delivery real estate."
            elif any(keyword in combined for keyword in ['construction', 'development', 'planning', 'zoning']):
                return "Development regulations and construction factors affect project feasibility, delivery timelines, and development margins."
            elif any(keyword in combined for keyword in ['technology', 'ai', 'automation', 'digital']):
                return "Technology adoption influences property operations efficiency, tenant requirements, and smart building investment priorities."
            elif any(keyword in combined for keyword in ['investment', 'capital', 'funding', 'finance']):
                return "Capital market conditions affect property acquisition financing, portfolio strategies, and investment return expectations."
            else:
                return "Broader economic trends provide important context for commercial property market dynamics and strategic decision-making."

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

    def calculate_risk_level(self, items: List[Tuple]) -> str:
        """Calculate overall risk level from news items"""
        if not items:
            return "Low"
        
        risk_keywords = ['risk', 'concern', 'uncertainty', 'volatility', 'decline', 'fall', 'crisis', 'threat', 'warning']
        total_risk_score = 0
        
        for item in items:
            title_desc = (item[0] + ' ' + item[2]).lower()
            interest_score = item[3]
            
            # Count risk keywords weighted by interest score
            risk_count = sum(1 for keyword in risk_keywords if keyword in title_desc)
            total_risk_score += risk_count * (interest_score / 10.0)
        
        if total_risk_score > 15:
            return "High"
        elif total_risk_score > 8:
            return "Medium"
        else:
            return "Low"

    def calculate_market_pulse(self, items: List[Tuple], sentiment: str, high_priority_count: int) -> str:
        """Calculate overall market pulse"""
        if high_priority_count >= 5 and sentiment == "Negative":
            return "⚡ Highly Active"
        elif high_priority_count >= 3 or sentiment == "Positive":
            return "📈 Active"
        elif len(items) >= 10:
            return "📊 Stable"
        else:
            return "😴 Quiet"

    def generate_executive_summary_text(self, top_items: List[Tuple]) -> str:
        """Generate executive summary from top items"""
        if not top_items:
            return "No significant developments in commercial property sector today."
        
        # Extract key themes
        themes = []
        for item in top_items[:3]:
            title = item[0].lower()
            if any(keyword in title for keyword in ['interest rate', 'rba', 'cash rate']):
                themes.append("monetary policy developments")
            elif any(keyword in title for keyword in ['property', 'real estate', 'reit']):
                themes.append("property sector activity")
            elif any(keyword in title for keyword in ['office', 'retail', 'industrial']):
                themes.append("commercial property fundamentals")
            elif any(keyword in title for keyword in ['technology', 'ai', 'digital']):
                themes.append("technology disruption")
        
        unique_themes = list(set(themes))
        
        if unique_themes:
            theme_text = ", ".join(unique_themes[:2])
            return f"Key developments today focused on {theme_text}. {len(top_items)} high-priority items require executive attention."
        else:
            return f"{len(top_items)} priority items identified across commercial property and related sectors."

    def generate_action_items_html(self, top_items: List[Tuple]) -> str:
        """Generate action items section"""
        if not top_items:
            return ""
        
        actions = []
        
        for item in top_items[:3]:
            title, link, description, score = item[0], item[1], item[2], item[3]
            
            if score >= 9:
                actions.append(f"<strong>URGENT:</strong> Review implications of '{title[:60]}...'")
            elif score >= 8:
                actions.append(f"<strong>Monitor:</strong> Track developments in '{title[:60]}...'")
            elif score >= 7:
                actions.append(f"<strong>Consider:</strong> Assess impact of '{title[:60]}...'")
        
        if actions:
            action_html = "<br>• ".join(actions)
            return f'''
            <div class="action-items">
                <strong>🎯 Executive Action Items</strong><br>
                • {action_html}
            </div>
            '''
        
        return ""

    def generate_news_items_html(self, items: List[Tuple]) -> str:
        """Generate news items HTML"""
        html = ""
        
        for item in items:
            title, link, description, score, summary, source = item[:6]
            
            # Determine priority class
            if score >= 8:
                priority_class = "priority-high"
                priority_icon = "🔴"
            elif score >= 6:
                priority_class = "priority-medium"  
                priority_icon = "🟡"
            else:
                priority_class = ""
                priority_icon = "🟢"
            
            # Clean and truncate summary
            clean_summary = (summary or description)[:200] + "..." if (summary or description) else "No summary available"
            clean_summary = re.sub('<[^<]+?>', '', clean_summary)  # Remove HTML tags
            
            html += f'''
            <div class="news-item {priority_class}">
                <div class="news-title">
                    <a href="{link}" target="_blank">{title}</a>
                </div>
                <div class="news-meta">
                    {priority_icon} Score: {score}/10 • {source} • {datetime.now().strftime('%H:%M')}
                </div>
                <div class="news-summary">
                    {clean_summary}
                </div>
            </div>
            '''
        
        return html

    def generate_social_media_html(self, items: List[Tuple]) -> str:
        """Generate simple social media content"""
        if len(items) < 2:
            return ""
        
        top_item = items[0]
        title = top_item[0]
        
        # Generate simple social posts
        twitter_post = f"Key development in commercial property: {title[:180]}... What are your thoughts on the implications? #CommercialProperty #AREIT"
        
        linkedin_post = f"Interesting development in our sector:\n\n{title}\n\nThis highlights the importance of staying informed about market dynamics. How do you see this impacting commercial property strategies?\n\n#RealEstate #CommercialProperty #Leadership"
        
        return f'''
        <div class="social-section">
            <h3 style="margin: 0 0 10px 0; color: #495057; font-size: 14px;">📱 Ready-to-Share Content</h3>
            
            <div class="social-post">
                <strong>🐦 Twitter:</strong><br>
                {twitter_post}
            </div>
            
            <div class="social-post">
                <strong>💼 LinkedIn:</strong><br>
                {linkedin_post[:300]}...
            </div>
        </div>
        '''

    def generate_additional_items_html(self, items: List[Tuple]) -> str:
        """Generate additional items section"""
        if not items:
            return ""
        
        html = '''
        <div style="margin-top: 20px;">
            <h3 style="margin: 0 0 10px 0; color: #495057; font-size: 14px;">📋 Additional Items</h3>
        '''
        
        for item in items:
            title, link, score, source = item[0], item[1], item[3], item[5]
            
            html += f'''
            <div style="border-bottom: 1px solid #e9ecef; padding: 8px 0; font-size: 12px;">
                <a href="{link}" target="_blank" style="color: #495057; text-decoration: none; font-weight: 500;">{title}</a>
                <span style="color: #6c757d; margin-left: 8px;">• {source} • {score}/10</span>
            </div>
            '''
        
        html += "</div>"
        return html

    def generate_advanced_executive_summary(self, top_items: List[Tuple], sentiment: str, risk_level: str) -> str:
        """Generate sophisticated AI-powered executive summary"""
        if not top_items:
            return "Market conditions are stable with no significant developments requiring immediate attention. Monitoring continues across all sectors."
        
        # Analyze themes with AI-level sophistication
        themes = {
            'monetary_policy': 0,
            'property_market': 0,
            'technology_disruption': 0,
            'regulatory_changes': 0,
            'market_sentiment': 0
        }
        
        for item in top_items:
            title_desc = (item[0] + ' ' + item[2]).lower()
            score = item[3]
            
            if any(keyword in title_desc for keyword in ['interest rate', 'rba', 'fed', 'monetary', 'inflation']):
                themes['monetary_policy'] += score
            if any(keyword in title_desc for keyword in ['property', 'real estate', 'reit', 'commercial', 'residential']):
                themes['property_market'] += score
            if any(keyword in title_desc for keyword in ['technology', 'ai', 'digital', 'innovation', 'automation']):
                themes['technology_disruption'] += score
            if any(keyword in title_desc for keyword in ['regulation', 'policy', 'government', 'law', 'compliance']):
                themes['regulatory_changes'] += score
            if any(keyword in title_desc for keyword in ['market', 'sentiment', 'outlook', 'forecast', 'prediction']):
                themes['market_sentiment'] += score
        
        # Find dominant theme
        dominant_theme = max(themes.items(), key=lambda x: x[1])
        
        # Generate intelligent summary based on analysis
        theme_insights = {
            'monetary_policy': f"Central bank policy developments are driving market dynamics with {sentiment.lower()} sentiment prevailing.",
            'property_market': f"Property sector activity shows {sentiment.lower()} momentum with {len(top_items)} key developments requiring strategic attention.",
            'technology_disruption': f"Technology transformation continues reshaping real estate fundamentals, creating both opportunities and competitive pressures.",
            'regulatory_changes': f"Regulatory landscape shifts present {risk_level.lower()}-risk scenarios requiring proactive strategic positioning.",
            'market_sentiment': f"Market sentiment indicators point to {sentiment.lower()} conditions with {risk_level.lower()} volatility expectations."
        }
        
        base_summary = theme_insights.get(dominant_theme[0], "Market conditions reflect mixed signals requiring careful monitoring.")
        
        # Add risk and opportunity context
        if risk_level == "High":
            base_summary += " Critical risk factors identified require immediate executive attention and potential strategic adjustments."
        elif risk_level == "Medium":
            base_summary += " Moderate risk factors suggest enhanced monitoring protocols and contingency planning."
        
        return base_summary

    def generate_ai_insights_section(self, items: List[Tuple]) -> str:
        """Generate AI-powered insights section"""
        if not items:
            return ""
        
        insights = []
        
        for item in items[:3]:
            title = item[0]
            score = item[3]
            
            if score >= 9:
                insights.append(f"<strong>Critical Signal:</strong> {title} represents a market-moving development requiring immediate strategic assessment.")
            elif score >= 8:
                insights.append(f"<strong>Strategic Opportunity:</strong> {title} indicates emerging market dynamics with potential competitive advantages.")
            elif score >= 7:
                insights.append(f"<strong>Market Intelligence:</strong> {title} provides valuable sector insights for informed decision-making.")
        
        if not insights:
            insights = ["Market conditions remain within normal parameters with no exceptional developments requiring immediate intervention."]
        
        insight_html = ""
        for insight in insights[:3]:
            insight_html += f'<div class="insight-item">{insight}</div>'
        
        return f'''
        <div class="ai-insights">
            <div class="insights-title">
                🤖 AI Strategic Insights
            </div>
            {insight_html}
        </div>
        '''

    def generate_strategic_actions_section(self, items: List[Tuple]) -> str:
        """Generate strategic action items"""
        if not items:
            return ""
        
        actions = []
        
        for item in items[:3]:
            title = item[0]
            score = item[3]
            
            if score >= 9:
                actions.append({
                    'priority': 'CRITICAL',
                    'action': f"Immediate executive review of '{title[:60]}...' implications for strategic positioning and risk management."
                })
            elif score >= 8:
                actions.append({
                    'priority': 'HIGH',
                    'action': f"Schedule strategic assessment of '{title[:60]}...' within 24-48 hours for potential opportunity capture."
                })
            elif score >= 7:
                actions.append({
                    'priority': 'MEDIUM',
                    'action': f"Monitor developments in '{title[:60]}...' and assess relevance to current strategic initiatives."
                })
        
        if not actions:
            actions = [{
                'priority': 'MONITOR',
                'action': 'Continue systematic market monitoring protocols. No immediate strategic actions required.'
            }]
        
        action_html = ""
        for action in actions[:3]:
            action_html += f'''
            <div class="action-item">
                <div class="action-priority">{action['priority']}</div>
                <div>{action['action']}</div>
            </div>
            '''
        
        return f'''
        <div class="strategic-actions">
            <div class="actions-title">
                🎯 Strategic Action Framework
            </div>
            {action_html}
        </div>
        '''

    def generate_enhanced_news_items(self, items: List[Tuple]) -> str:
        """Generate enhanced news items with AI analysis"""
        if not items:
            return ""
        
        html = ""
        
        for i, item in enumerate(items):
            title, link, description, score, summary, source = item[:6]
            
            # Determine priority styling
            if score >= 9:
                priority_class = "news-critical"
                priority_label = "🚨 CRITICAL"
            elif score >= 8:
                priority_class = "news-high"
                priority_label = "🔴 HIGH"
            elif score >= 6:
                priority_class = "news-medium"
                priority_label = "🟡 MEDIUM"
            else:
                priority_class = "news-normal"
                priority_label = "🟢 NORMAL"
            
            # Generate AI analysis
            ai_analysis = self.generate_ai_analysis_snippet(title, description, score)
            
            # Clean summary
            clean_summary = (summary or description)[:180] + "..." if (summary or description) else "Analysis pending..."
            clean_summary = re.sub('<[^<]+?>', '', clean_summary)
            
            # Time ago calculation
            time_processed = datetime.now().strftime('%H:%M')
            
            html += f'''
            <div class="news-item {priority_class}">
                <div class="news-header">
                    <div class="news-title">
                        <a href="{link}" target="_blank">{title}</a>
                    </div>
                    <div class="news-score">{score}/10</div>
                </div>
                
                <div class="news-meta">
                    <span>{priority_label}</span>
                    <span>📰 {source}</span>
                    <span>⏰ {time_processed}</span>
                    <span>🔍 AI Analyzed</span>
                </div>
                
                <div class="news-summary">
                    {clean_summary}
                </div>
                
                <div class="ai-analysis">
                    <strong>🤖 AI Analysis:</strong> {ai_analysis}
                </div>
            </div>
            '''
        
        return html

    def generate_ai_analysis_snippet(self, title: str, description: str, score: int) -> str:
        """Generate AI analysis snippet for each news item"""
        title_lower = title.lower()
        
        # AI-style analysis based on content patterns
        if score >= 9:
            if 'interest rate' in title_lower or 'rba' in title_lower:
                return "Critical monetary policy development with direct impact on commercial property valuations and investment strategies."
            elif 'merger' in title_lower or 'acquisition' in title_lower:
                return "Significant market consolidation event requiring competitive landscape reassessment and strategic positioning review."
            else:
                return "High-impact development requiring immediate executive attention and potential strategic response planning."
        
        elif score >= 8:
            if 'property' in title_lower or 'real estate' in title_lower:
                return "Relevant property sector development with moderate strategic implications for A-REIT positioning."
            elif 'technology' in title_lower or 'ai' in title_lower:
                return "Technology advancement with potential disruption implications for traditional real estate operations."
            else:
                return "Important market development warranting strategic monitoring and assessment protocols."
        
        elif score >= 6:
            return "Moderate relevance to real estate sector. Suggests continued monitoring of market trends and dynamics."
        
        else:
            return "General market intelligence. Provides context for broader economic and business environment."
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

    def generate_executive_summary_text(self, top_items: List[Tuple]) -> str:
        """Generate executive summary from top items"""
        if not top_items:
            return "No significant developments in commercial property sector today."
        
        # Extract key themes
        themes = []
        for item in top_items[:3]:
            title = item[0].lower()
            if any(keyword in title for keyword in ['interest rate', 'rba', 'cash rate']):
                themes.append("monetary policy developments")
            elif any(keyword in title for keyword in ['property', 'real estate', 'reit']):
                themes.append("property sector activity")
            elif any(keyword in title for keyword in ['office', 'retail', 'industrial']):
                themes.append("commercial property fundamentals")
            elif any(keyword in title for keyword in ['technology', 'ai', 'digital']):
                themes.append("technology disruption")
        
        unique_themes = list(set(themes))
        
        if unique_themes:
            theme_text = ", ".join(unique_themes[:2])
            return f"Key developments today focused on {theme_text}. {len(top_items)} high-priority items require executive attention."
        else:
            return f"{len(top_items)} priority items identified across commercial property and related sectors."

    def generate_action_items_html(self, top_items: List[Tuple]) -> str:
        """Generate action items section"""
        if not top_items:
            return ""
        
        actions = []
        
        for item in top_items[:3]:
            title, link, description, score = item[0], item[1], item[2], item[3]
            
            if score >= 9:
                actions.append(f"<strong>URGENT:</strong> Review implications of '{title[:60]}...'")
            elif score >= 8:
                actions.append(f"<strong>Monitor:</strong> Track developments in '{title[:60]}...'")
            elif score >= 7:
                actions.append(f"<strong>Consider:</strong> Assess impact of '{title[:60]}...'")
        
        if actions:
            action_html = "<br>• ".join(actions)
            return f'''
            <div class="action-items">
                <strong>🎯 Executive Action Items</strong><br>
                • {action_html}
            </div>
            '''
        
        return ""

    def generate_news_items_html(self, items: List[Tuple]) -> str:
        """Generate news items HTML"""
        html = ""
        
        for item in items:
            title, link, description, score, summary, source = item[:6]
            
            # Determine priority class
            if score >= 8:
                priority_class = "priority-high"
                priority_icon = "🔴"
            elif score >= 6:
                priority_class = "priority-medium"  
                priority_icon = "🟡"
            else:
                priority_class = ""
                priority_icon = "🟢"
            
            # Clean and truncate summary
            clean_summary = (summary or description)[:200] + "..." if (summary or description) else "No summary available"
            clean_summary = re.sub('<[^<]+?>', '', clean_summary)  # Remove HTML tags
            
            html += f'''
            <div class="news-item {priority_class}">
                <div class="news-title">
                    <a href="{link}" target="_blank">{title}</a>
                </div>
                <div class="news-meta">
                    {priority_icon} Score: {score}/10 • {source} • {datetime.now().strftime('%H:%M')}
                </div>
                <div class="news-summary">
                    {clean_summary}
                </div>
            </div>
            '''
        
        return html

    def generate_social_media_html(self, items: List[Tuple]) -> str:
        """Generate simple social media content"""
        if len(items) < 2:
            return ""
        
        top_item = items[0]
        title = top_item[0]
        
        # Generate simple social posts
        twitter_post = f"Key development in commercial property: {title[:180]}... What are your thoughts on the implications? #CommercialProperty #AREIT"
        
        linkedin_post = f"Interesting development in our sector:\n\n{title}\n\nThis highlights the importance of staying informed about market dynamics. How do you see this impacting commercial property strategies?\n\n#RealEstate #CommercialProperty #Leadership"
        
        return f'''
        <div class="social-section">
            <h3 style="margin: 0 0 10px 0; color: #495057; font-size: 14px;">📱 Ready-to-Share Content</h3>
            
            <div class="social-post">
                <strong>🐦 Twitter:</strong><br>
                {twitter_post}
            </div>
            
            <div class="social-post">
                <strong>💼 LinkedIn:</strong><br>
                {linkedin_post[:300]}...
            </div>
        </div>
        '''

    def generate_additional_items_html(self, items: List[Tuple]) -> str:
        """Generate additional items section"""
        if not items:
            return ""
        
        html = '''
        <div style="margin-top: 20px;">
            <h3 style="margin: 0 0 10px 0; color: #495057; font-size: 14px;">📋 Additional Items</h3>
        '''
        
        for item in items:
            title, link, score, source = item[0], item[1], item[3], item[5]
            
            html += f'''
            <div style="border-bottom: 1px solid #e9ecef; padding: 8px 0; font-size: 12px;">
                <a href="{link}" target="_blank" style="color: #495057; text-decoration: none; font-weight: 500;">{title}</a>
                <span style="color: #6c757d; margin-left: 8px;">• {source} • {score}/10</span>
            </div>
            '''
        
        html += "</div>"
        return html

    def send_email(self, content: str):
        """Send email with HTML content"""
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"Matt's Memo - {datetime.now().strftime('%B %d, %Y')}"
            msg['From'] = self.config['gmail_user']
            msg['To'] = self.config['recipient_email']
            
            # Attach HTML content
            html_part = MIMEText(content, 'html', 'utf-8')
            msg.attach(html_part)
            
            # Send email
            with smtplib.SMTP('smtp.gmail.com', 587) as server:
                server.starttls()
                server.login(self.config['gmail_user'], self.config['gmail_password'])
                server.send_message(msg)
            
            logging.info("Email sent successfully")
            
        except Exception as e:
            logging.error(f"Failed to send email: {e}")
            raise

    def send_daily_brief_enhanced(self, include_all: bool = False):
        """Enhanced daily brief that won't get clipped"""
        try:
            items = self.get_items_for_email(24)
            if items:
                content = self.generate_daily_email_from_items_enhanced(items)
                if content:
                    # Check content size and warn if too large
                    content_size = len(content.encode('utf-8'))
                    if content_size > 100000:  # 100KB limit
                        logging.warning(f"Email content size: {content_size/1024:.1f}KB - may be clipped")
                    
                    self.send_email(content)
                    
                    # Mark items as sent
                    cutoff_time = datetime.now() - timedelta(hours=24)
                    self.conn.execute('''
                        UPDATE items SET email_sent = TRUE 
                        WHERE processed_at >= ? AND email_sent = FALSE
                    ''', (cutoff_time,))
                    self.conn.commit()
                    
                    logging.info(f"Enhanced email sent successfully ({content_size/1024:.1f}KB)")
                else:
                    logging.info("No content generated for enhanced email")
            else:
                logging.info("No items found for enhanced email")
        except Exception as e:
            logging.error(f"Enhanced email error: {e}")
            raise

    def send_daily_brief_incremental(self):
        """Send email only if it's the right time"""
        should_send, time_period = self.should_send_email_now()
        
        if should_send:
            logging.info(f"Sending {time_period} email brief...")
            
            # Get items from last 24 hours for morning email
            items = self.get_items_for_email(24)
            
            if items:
                content = self.generate_daily_email_from_items_enhanced(items)
                if content:
                    self.send_email(content)
                    
                    # Mark items as sent
                    cutoff_time = datetime.now() - timedelta(hours=24)
                    self.conn.execute('''
                        UPDATE items SET email_sent = TRUE 
                        WHERE processed_at >= ? AND email_sent = FALSE
                    ''', (cutoff_time,))
                    self.conn.commit()
                    
                    logging.info("Email sent successfully")
                else:
                    logging.info("No content generated for email")
            else:
                logging.info("No items found for email")
        else:
            logging.info(f"Skipping email send - {time_period} run (email only sent in morning)")

    def run_scheduled_processing(self):
        """Main method for scheduled processing"""
        try:
            # Process feeds with optimized method
            result = self.process_feeds_optimized_recent()
            
            # Send email if appropriate
            self.send_daily_brief_incremental()
            
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
    """Main function to run the RSS analyzer"""
    try:
        analyzer = RSSAnalyzer()
        
        # Handle command line arguments for GitHub Actions
        if len(sys.argv) > 1:
            command = sys.argv[1].lower()
            
            if command in ['process', 'run', 'test']:
                logging.info("Running single processing cycle for GitHub Actions...")
                analyzer.run_scheduled_processing()
                logging.info("Processing completed successfully!")
                
            elif command == 'email':
                logging.info("Sending daily email...")
                analyzer.send_daily_brief_enhanced()
                logging.info("Email sent successfully!")
                
            elif command == 'cleanup':
                days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
                logging.info(f"Cleaning up items older than {days} days...")
                analyzer.cleanup_old_items(days)
                logging.info("Cleanup completed!")
                
            else:
                print("Usage: python rss_analyzer.py [process|email|cleanup] [days]")
                print("  process - Run RSS processing and send email")
                print("  email   - Send daily email only")  
                print("  cleanup - Remove old database entries (default: 7 days)")
                sys.exit(1)
        else:
            # Run continuous scheduled mode
            logging.info("Starting continuous scheduled mode...")
            
            # Schedule processing 3 times daily
            schedule.every().day.at("06:00").do(analyzer.run_scheduled_processing)  # Morning AEST
            schedule.every().day.at("12:00").do(analyzer.run_scheduled_processing)  # Midday AEST  
            schedule.every().day.at("18:00").do(analyzer.run_scheduled_processing)  # Evening AEST
            
            logging.info("RSS Analyzer started - running scheduled processing...")
            
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
