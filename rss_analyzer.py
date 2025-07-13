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
                processed_count += 1
                
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

    def send_daily_brief_incremental(self):
        """Send email only if it's the right time"""
        should_send, time_period = self.should_send_email_now()
        
        if should_send:
            logging.info(f"Sending {time_period} email brief...")
            
            # Get items from last 24 hours for morning email
            items = self.get_items_for_email(24)
            
            if items:
                content = self.generate_daily_email_from_items(items)
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

    def generate_daily_email_from_items(self, items: List[Tuple]) -> Optional[str]:
        """Generate email from provided items list"""
        if not items:
            return None
        
        # Filter items (same logic as before)
        filtered_items = []
        for item in items:
            title, link, description, score, summary, source_name = item[:6]
            
            # Skip obvious errors
            if any(phrase in title.lower() for phrase in [
                'not found', 'sign up to rss.app', 'error', 'access denied'
            ]):
                continue
            
            # Include items with score 4+ (slightly more inclusive for 3x daily)
            if score >= 4:
                filtered_items.append(item)
        
        if not filtered_items:
            return None
        
        # For now, use existing email generation
        # This would use your existing generate_daily_email logic
        # but with the provided filtered_items
        return self.build_simple_email_html(filtered_items)

    def build_simple_email_html(self, items: List[Tuple]) -> str:
        """Build a simple HTML email for testing"""
        html = f"""
        <html>
        <body>
        <h1>Matt's Memo - {datetime.now().strftime('%B %d, %Y')}</h1>
        <h2>Executive Intelligence Brief - {len(items)} Items</h2>
        """
        
        for item in items[:20]:  # Limit to top 20 items
            title, link, description, score, summary, source = item[:6]
            html += f"""
            <div style="border: 1px solid #ccc; margin: 10px; padding: 10px;">
                <h3><a href="{link}">{title}</a></h3>
                <p><strong>Score:</strong> {score}/10 | <strong>Source:</strong> {source}</p>
                <p>{summary}</p>
            </div>
            """
        
        html += """
        </body>
        </html>
        """
        return html

    # Legacy compatibility methods
    def process_feeds_incremental(self):
        """Legacy method - redirect to optimized version"""
        return self.process_feeds_optimized_recent()

    def process_feeds(self):
        """Legacy method for compatibility"""
        try:
            return self.process_feeds_optimized_recent()
        except Exception as e:
            logging.error(f"Optimized processing failed, using emergency fallback: {e}")
            return self.emergency_simple_process_fallback()

    def save_item(self, item: FeedItem):
        """Save item to database with enhanced fields"""
        try:
            self.conn.execute('''
                INSERT OR REPLACE INTO items 
                (title, link, description, published, source_feed, source_name, 
                 interest_score, ai_summary, category, sentiment, key_metrics, 
                 geographic_tags, sector_tags)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                item.title, item.link, item.description, 
                item.published, item.source_feed, item.source_name,
                item.interest_score, item.ai_summary, item.category,
                item.sentiment, 
                ','.join(item.key_metrics) if item.key_metrics else None,
                ','.join(item.geographic_tags) if item.geographic_tags else None,
                ','.join(item.sector_tags) if item.sector_tags else None
            ))
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error saving item: {e}")
    
    def item_exists(self, link: str) -> bool:
        """Check if item already exists in database by URL"""
        cursor = self.conn.execute('SELECT id FROM items WHERE link = ?', (link,))
        return cursor.fetchone() is not None
    
    def title_exists(self, title: str, source_name: str) -> bool:
        """Check if item with similar title already exists from same source (last 7 days)"""
        week_ago = datetime.now() - timedelta(days=7)
        cursor = self.conn.execute('''
            SELECT id FROM items 
            WHERE title = ? AND source_name = ? AND processed_at >= ?
        ''', (title, source_name, week_ago))
        return cursor.fetchone() is not None

    def send_email(self, content: str):
        """Send email via Gmail SMTP with proper HTML formatting"""
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"Matt's Memo • {datetime.now().strftime('%B %d, %Y')}"
            msg['From'] = self.config['gmail_user']
            msg['To'] = self.config['recipient_email']
            
            # Create both plain text and HTML versions
            text_content = re.sub('<[^<]+?>', '', content)
            text_content = re.sub(r'\s+', ' ', text_content).strip()
            
            # Create message parts
            text_part = MIMEText(text_content, 'plain')
            html_part = MIMEText(content, 'html', 'utf-8')
            
            # Attach parts
            msg.attach(text_part)
            msg.attach(html_part)
            
            # Send via Gmail SMTP
            with smtplib.SMTP('smtp.gmail.com', 587) as server:
                server.starttls()
                server.login(self.config['gmail_user'], self.config['gmail_password'])
                server.send_message(msg)
            
            logging.info("Email sent successfully")
            
        except Exception as e:
            logging.error(f"Error sending email: {e}")
            raise

    def send_daily_brief(self, include_all: bool = False):
        """Legacy email method for compatibility"""
        try:
            items = self.get_items_for_email(24)
            if items:
                content = self.generate_daily_email_from_items(items)
                if content:
                    self.send_email(content)
                    logging.info("Daily brief sent successfully")
                else:
                    logging.info("No content for daily brief")
            else:
                logging.info("No items for daily brief")
        except Exception as e:
            logging.error(f"Error in send_daily_brief: {e}")

    def preview_items(self):
        """Preview what items would be included in the email without sending"""
        cutoff_time = datetime.now() - timedelta(hours=24)
        
        cursor = self.conn.execute('''
            SELECT title, link, description, interest_score, ai_summary, source_name, 
                   processed_at, category, sentiment, key_metrics, geographic_tags, sector_tags
            FROM items 
            WHERE processed_at >= ? AND email_sent = FALSE
            ORDER BY interest_score DESC, processed_at DESC
        ''', (cutoff_time,))
        
        all_items = cursor.fetchall()
        
        print(f"\n📊 EMAIL PREVIEW - Items from last 24 hours")
        print("=" * 80)
        print(f"Total items in database: {len(all_items)}")
        
        if not all_items:
            print("❌ No items found from the last 24 hours")
            return
        
        included_count = 0
        for item in all_items:
            title, link, description, score, summary, source_name = item[:6]
            
            # Skip obvious errors
            if any(phrase in title.lower() for phrase in [
                'not found', 'sign up to rss.app', 'error', 'access denied'
            ]):
                continue
            
            if score >= 4:
                included_count += 1
                priority = "🔴" if score >= 8 else "🟡" if score >= 6 else "🟢"
                print(f"{priority} Score {score}: {title[:70]}... ({source_name})")
        
        print(f"\n📈 SUMMARY:")
        print(f"✅ Would be included in email: {included_count}")
        print(f"📊 Total available: {len(all_items)}")

    def cleanup_old_items(self, days_to_keep=30):
        """Clean up old items from database"""
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        cursor = self.conn.execute('DELETE FROM items WHERE processed_at < ?', (cutoff_date,))
        deleted_count = cursor.rowcount
        self.conn.commit()
        logging.info(f"Cleaned up {deleted_count} items older than {days_to_keep} days")

    def run_scheduler(self):
        """Legacy scheduler method"""
        logging.info("Legacy scheduler mode - consider using 3x daily incremental processing")
        
        # Schedule incremental processing 3x daily
        schedule.every().day.at("06:00").do(self.process_feeds_optimized_recent)
        schedule.every().day.at("12:00").do(self.process_feeds_optimized_recent)
        schedule.every().day.at("18:00").do(self.process_feeds_optimized_recent)
        
        # Schedule daily email at 6 AM
        schedule.every().day.at("06:00").do(self.send_daily_brief_incremental)
        
        logging.info("Scheduler started with 3x daily processing")
        
        while True:
            schedule.run_pending()
            time.sleep(60)


if __name__ == "__main__":
    import sys
    
    try:
        analyzer = RSSAnalyzer()
        
        # Check for command line arguments
        if len(sys.argv) > 1:
            command = sys.argv[1].lower()
            
            if command == "incremental":
                # NEW: Optimized incremental processing - ONLY recent items!
                logging.info("🚀 Running OPTIMIZED incremental processing (recent items only)...")
                start_time = time.time()
                
                try:
                    results = analyzer.process_feeds_optimized_recent()
                    elapsed = time.time() - start_time
                    
                    logging.info("✅ Optimized incremental processing completed!")
                    logging.info(f"   Duration: {elapsed:.1f} seconds")
                    logging.info(f"   Recent items fetched: {results['total_scanned']}")
                    logging.info(f"   New items found: {results['new_items']}")
                    logging.info(f"   Items processed: {results['processed']}")
                    
                    print(f"✅ Success: Processed {results['processed']} items in {elapsed:.1f}s")
                    print(f"   📊 Fetched {results['total_scanned']} recent items, {results['new_items']} were new")
                    
                    # Show efficiency gain
                    if results['total_scanned'] > 0:
                        efficiency = (results['total_scanned'] - results['processed']) / results['total_scanned'] * 100
                        print(f"   ⚡ Efficiency: Skipped {efficiency:.1f}% duplicate/old items")
                    
                except Exception as e:
                    logging.error(f"❌ Optimized incremental processing failed: {e}")
                    print(f"❌ Error: {e}")
                    print("💡 Try: python rss_analyzer.py emergency")
                    sys.exit(1)
                
            elif command == "email-check":
                # Check if we should send email based on time
                logging.info("📧 Checking email send schedule...")
                
                try:
                    should_send, time_period = analyzer.should_send_email_now()
                    
                    if should_send:
                        logging.info(f"📧 Sending {time_period} email brief...")
                        analyzer.send_daily_brief_incremental()
                        print(f"✅ Email sent for {time_period} period")
                    else:
                        logging.info(f"⏭️ Skipping email - {time_period} period (email only sent in morning)")
                        print(f"⏭️ No email sent - {time_period} period")
                    
                except Exception as e:
                    logging.error(f"❌ Email check failed: {e}")
                    print(f"❌ Email error: {e}")
                    # Don't exit - email failure shouldn't break the workflow
                
            elif command == "test":
                # LEGACY: Original test mode - process feeds once
                logging.info("🧪 Running in LEGACY TEST mode...")
                
                try:
                    start_time = time.time()
                    analyzer.process_feeds()
                    elapsed = time.time() - start_time
                    
                    logging.info(f"✅ Legacy test completed in {elapsed:.1f} seconds")
                    print(f"✅ Legacy test completed - check rss_analyzer.log for details")
                    
                except Exception as e:
                    logging.error(f"❌ Legacy test failed: {e}")
                    print(f"❌ Test failed: {e}")
                    sys.exit(1)
                
            elif command == "email":
                # Send test email with normal filtering
                logging.info("📧 Generating and sending test email...")
                
                try:
                    analyzer.send_daily_brief(include_all=False)
                    logging.info("✅ Test email completed")
                    print("✅ Test email sent successfully")
                    
                except Exception as e:
                    logging.error(f"❌ Email test failed: {e}")
                    print(f"❌ Email failed: {e}")
                    sys.exit(1)
                
            elif command == "email-full":
                # Send test email with minimal filtering (include everything)
                logging.info("📧 Generating and sending FULL test email...")
                
                try:
                    analyzer.send_daily_brief(include_all=True)
                    logging.info("✅ Full email test completed")
                    print("✅ Full test email sent successfully")
                    
                except Exception as e:
                    logging.error(f"❌ Full email test failed: {e}")
                    print(f"❌ Full email failed: {e}")
                    sys.exit(1)
                
            elif command == "preview":
                # Preview what would be included in email without sending
                logging.info("👀 Previewing email content...")
                
                try:
                    analyzer.preview_items()
                    print("✅ Preview completed - see output above")
                    
                except Exception as e:
                    logging.error(f"❌ Preview failed: {e}")
                    print(f"❌ Preview error: {e}")
                
            elif command == "feeds-recent":
                # NEW: Test recent-only fetching specifically
                logging.info("🕒 Testing recent-only feed fetching...")
                
                try:
                    cutoff_time = datetime.now() - timedelta(hours=6)
                    print(f"🕒 Testing recent-only fetching (since {cutoff_time.strftime('%H:%M')})")
                    print("=" * 60)
                    
                    start_time = time.time()
                    recent_items = analyzer.fetch_feeds_parallel_recent(cutoff_time, max_workers=4)
                    elapsed = time.time() - start_time
                    
                    print(f"✅ Parallel fetch completed in {elapsed:.1f}s")
                    print(f"📊 Total recent items: {len(recent_items)}")
                    
                    # Show breakdown by source
                    source_counts = Counter(item.source_name for item in recent_items)
                    
                    print(f"\n📰 Items per source:")
                    for source, count in source_counts.most_common(10):
                        print(f"   {source}: {count} items")
                    
                    # Show time distribution
                    if recent_items:
                        newest = max(item.published for item in recent_items)
                        oldest = min(item.published for item in recent_items)
                        span_hours = (newest - oldest).total_seconds() / 3600
                        print(f"\n🕒 Time span: {span_hours:.1f} hours")
                        print(f"   Newest: {newest.strftime('%H:%M')}")
                        print(f"   Oldest: {oldest.strftime('%H:%M')}")
                    
                except Exception as e:
                    logging.error(f"❌ Recent fetch test failed: {e}")
                    print(f"❌ Recent fetch error: {e}")
                
            elif command == "feeds-test":
                # Test individual feeds for debugging
                logging.info("🔍 Testing individual RSS feeds...")
                
                try:
                    total_feeds = len(analyzer.rss_feeds)
                    print(f"🔍 Testing {total_feeds} RSS feeds...")
                    print("=" * 60)
                    
                    working_feeds = 0
                    failing_feeds = 0
                    total_recent_items = 0
                    
                    # Test with recent-only fetching
                    cutoff_time = datetime.now() - timedelta(hours=6)
                    
                    for i, feed_config in enumerate(analyzer.rss_feeds, 1):
                        print(f"[{i}/{total_feeds}] Testing: {feed_config['name']}")
                        
                        try:
                            start_time = time.time()
                            items = analyzer.fetch_feed_items_recent_only(feed_config, cutoff_time, max_items=10)
                            elapsed = time.time() - start_time
                            
                            total_recent_items += len(items)
                            print(f"   ✅ Success: {len(items)} recent items in {elapsed:.1f}s")
                            working_feeds += 1
                            
                            # Show timing warning for slow feeds
                            if elapsed > 10:
                                print(f"   ⚠️  Slow feed: {elapsed:.1f}s (consider removing if consistently slow)")
                            
                        except Exception as feed_error:
                            print(f"   ❌ Failed: {feed_error}")
                            failing_feeds += 1
                    
                    print("=" * 60)
                    print(f"📊 Feed Test Summary:")
                    print(f"   ✅ Working feeds: {working_feeds}")
                    print(f"   ❌ Failing feeds: {failing_feeds}")
                    print(f"   📈 Success rate: {(working_feeds/total_feeds)*100:.1f}%")
                    print(f"   📊 Total recent items: {total_recent_items}")
                    print(f"   ⏱️  Average items per working feed: {total_recent_items/max(working_feeds,1):.1f}")
                    
                    if failing_feeds > 0:
                        print(f"\n💡 Consider removing {failing_feeds} failing feeds to improve performance")
                    
                except Exception as e:
                    logging.error(f"❌ Feed test failed: {e}")
                    print(f"❌ Feed test error: {e}")
                
            elif command == "emergency":
                # Emergency simple processing without AI
                logging.info("🚨 Running EMERGENCY processing mode (no AI)...")
                
                try:
                    start_time = time.time()
                    results = analyzer.emergency_simple_process_fallback()
                    elapsed = time.time() - start_time
                    
                    logging.info(f"✅ Emergency processing completed: {results['processed']} items in {elapsed:.1f}s")
                    print(f"✅ Emergency mode completed: {results['processed']} items processed in {elapsed:.1f}s")
                    
                except Exception as e:
                    logging.error(f"❌ Emergency processing failed: {e}")
                    print(f"❌ Emergency error: {e}")
                    sys.exit(1)
                
            elif command == "stats":
                # Show comprehensive database statistics
                logging.info("📊 Generating database statistics...")
                
                try:
                    # Basic item counts
                    cursor = analyzer.conn.execute("SELECT COUNT(*) FROM items")
                    total_items = cursor.fetchone()[0]
                    
                    cursor = analyzer.conn.execute("SELECT COUNT(*) FROM items WHERE processed_at >= ?", 
                                                 (datetime.now() - timedelta(hours=6),))
                    last_6h = cursor.fetchone()[0]
                    
                    cursor = analyzer.conn.execute("SELECT COUNT(*) FROM items WHERE processed_at >= ?", 
                                                 (datetime.now() - timedelta(hours=24),))
                    last_24h = cursor.fetchone()[0]
                    
                    cursor = analyzer.conn.execute("SELECT COUNT(*) FROM items WHERE email_sent = FALSE")
                    pending_email = cursor.fetchone()[0]
                    
                    cursor = analyzer.conn.execute("SELECT AVG(interest_score) FROM items WHERE interest_score IS NOT NULL")
                    avg_score = cursor.fetchone()[0] or 0
                    
                    # Get incremental processing info
                    processor = IncrementalProcessor(analyzer)
                    last_run = processor.get_last_run_time()
                    
                    print("📊 DATABASE STATISTICS")
                    print("=" * 50)
                    print(f"📈 Total items in database: {total_items:,}")
                    print(f"🕕 Last 6 hours: {last_6h}")
                    print(f"📅 Last 24 hours: {last_24h}")
                    print(f"📧 Pending email: {pending_email}")
                    print(f"⭐ Average interest score: {avg_score:.1f}/10")
                    print(f"🕐 Last incremental run: {last_run or 'Never'}")
                    
                    # Show recent processing runs
                    cursor = analyzer.conn.execute('''
                        SELECT run_type, last_run_time, items_processed, created_at 
                        FROM processing_runs 
                        ORDER BY created_at DESC 
                        LIMIT 10
                    ''')
                    recent_runs = cursor.fetchall()
                    
                    if recent_runs:
                        print(f"\n🔄 RECENT PROCESSING RUNS")
                        print("-" * 50)
                        for run_type, run_time, items, created in recent_runs:
                            print(f"   {created}: {items} items ({run_type})")
                    
                    print("=" * 50)
                    
                except Exception as e:
                    logging.error(f"❌ Stats generation failed: {e}")
                    print(f"❌ Stats error: {e}")
                
            elif command == "cleanup":
                # Clean up old items
                days = 30
                if len(sys.argv) > 2:
                    try:
                        days = int(sys.argv[2])
                    except ValueError:
                        print("❌ Invalid days parameter, using default 30 days")
                        days = 30
                
                logging.info(f"🧹 Cleaning up items older than {days} days...")
                
                try:
                    # Count items before cleanup
                    cursor = analyzer.conn.execute("SELECT COUNT(*) FROM items")
                    before_count = cursor.fetchone()[0]
                    
                    analyzer.cleanup_old_items(days_to_keep=days)
                    
                    # Count items after cleanup
                    cursor = analyzer.conn.execute("SELECT COUNT(*) FROM items")
                    after_count = cursor.fetchone()[0]
                    
                    deleted_count = before_count - after_count
                    
                    logging.info(f"✅ Cleanup completed: {deleted_count} items removed")
                    print(f"✅ Cleanup completed: {deleted_count} items removed, {after_count} remaining")
                    
                except Exception as e:
                    logging.error(f"❌ Cleanup failed: {e}")
                    print(f"❌ Cleanup error: {e}")
                
            elif command == "reset-tracking":
                # Reset incremental processing tracking (for testing)
                logging.info("🔄 Resetting incremental processing tracking...")
                
                try:
                    cursor = analyzer.conn.execute('SELECT COUNT(*) FROM processing_runs')
                    count_before = cursor.fetchone()[0]
                    
                    analyzer.conn.execute('DELETE FROM processing_runs')
                    analyzer.conn.commit()
                    
                    logging.info(f"✅ Tracking reset: {count_before} run records cleared")
                    print(f"✅ Tracking reset - next incremental run will process last 6 hours")
                    print(f"   Cleared {count_before} previous run records")
                    
                except Exception as e:
                    logging.error(f"❌ Reset tracking failed: {e}")
                    print(f"❌ Reset error: {e}")
                
            else:
                # Show help for unknown commands
                print("❓ Unknown command. Available commands:")
                print("")
                print("🚀 MAIN COMMANDS (3x daily system):")
                print("  python rss_analyzer.py incremental       - Optimized processing (recent items only)")
                print("  python rss_analyzer.py email-check       - Check if email should be sent")
                print("")
                print("📧 EMAIL COMMANDS:")
                print("  python rss_analyzer.py email             - Send test email (normal filtering)")
                print("  python rss_analyzer.py email-full        - Send test email (include everything)")
                print("  python rss_analyzer.py preview           - Preview email content without sending")
                print("")
                print("🧪 TESTING & DEBUG:")
                print("  python rss_analyzer.py test              - Legacy full processing test")
                print("  python rss_analyzer.py feeds-test        - Test individual RSS feeds")
                print("  python rss_analyzer.py feeds-recent      - Test recent-only fetching")
                print("  python rss_analyzer.py emergency         - Emergency processing (no AI)")
                print("  python rss_analyzer.py stats             - Show database statistics")
                print("")
                print("🛠️ MAINTENANCE:")
                print("  python rss_analyzer.py cleanup [days]    - Clean old items (default: 30 days)")
                print("  python rss_analyzer.py reset-tracking    - Reset incremental processing tracking")
                print("")
                print("⏰ SCHEDULER:")
                print("  python rss_analyzer.py                   - Run scheduler (legacy mode)")
                print("")
                print("💡 Recommended workflow:")
                print("   1. Test feeds: python rss_analyzer.py feeds-recent")
                print("   2. Run optimized: python rss_analyzer.py incremental")
                print("   3. Check email: python rss_analyzer.py email-check")
                print("   4. View stats: python rss_analyzer.py stats")
                print("")
                print("🚨 If feeds are slow:")
                print("   - Use feeds-test to identify slow feeds")
                print("   - Remove slow/failing feeds from feeds.py")
                print("   - Use emergency mode as backup")
                
        else:
            # Normal mode - run scheduler (legacy)
            logging.info("🕐 Starting legacy scheduler mode...")
            print("🕐 Running in scheduler mode (legacy)")
            print("💡 For 3x daily mode, use: python rss_analyzer.py incremental")
            analyzer.run_scheduler()
            
    except KeyboardInterrupt:
        logging.info("⏹️ RSS Analyzer stopped by user")
        print("\n⏹️ Stopped by user")
        
    except Exception as e:
        logging.error(f"💥 Fatal error: {e}")
        print(f"💥 Fatal error: {e}")
        print("🔍 Check rss_analyzer.log for detailed error information")
        raise
