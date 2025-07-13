#!/usr/bin/env python3
"""
RSS Feed Analyzer for A-REIT CEO/COO - Executive Intelligence Platform
Monitors RSS feeds, evaluates content with OpenAI, and sends daily emails
OPTIMIZED VERSION - 3x Daily Incremental Processing
"""

import concurrent.futures
import json
import logging
import os
import re
import schedule
import smtplib
import socket
import sqlite3
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Optional, Tuple

import feedparser
import openai
from dotenv import load_dotenv

# Import RSS feeds from separate file
from feeds import RSS_FEEDS

# Load environment variables
load_dotenv()

# Configure logging
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
    source_name: str
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
    """Handles incremental processing of RSS feeds."""
    
    def __init__(self, rss_analyzer):
        self.analyzer = rss_analyzer
        self.init_tracking_table()
    
    def init_tracking_table(self):
        """Initialize table to track processing runs."""
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
        """Retrieve the last processing time."""
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
            except ValueError:
                return datetime.fromisoformat(result[0])
        return None
    
    def update_last_run_time(self, run_type: str = 'feed_processing', items_processed: int = 0):
        """Update the last run time."""
        now = datetime.now()
        self.analyzer.conn.execute('''
            INSERT INTO processing_runs (run_type, last_run_time, items_processed)
            VALUES (?, ?, ?)
        ''', (run_type, now.isoformat(), items_processed))
        self.analyzer.conn.commit()
        logging.info(f"Updated last run time: {now}, items processed: {items_processed}")
    
    def get_incremental_cutoff_time(self) -> datetime:
        """Determine the cutoff time for incremental processing."""
        last_run = self.get_last_run_time()
        cutoff_time = (last_run - timedelta(minutes=30)) if last_run else (datetime.now() - timedelta(hours=6))
        logging.info(f"Incremental processing cutoff: {cutoff_time}")
        return cutoff_time
    
    def should_send_email(self) -> Tuple[bool, str]:
        """Check if an email should be sent based on the time of day."""
        hour = (datetime.now().hour + 10) % 24  # Convert to AEST
        if 20 <= hour <= 23 or hour <= 2:  # Around 6 AM AEST
            return True, "morning"
        elif 0 <= hour <= 4:  # Around 12 PM AEST
            return False, "midday"
        elif 6 <= hour <= 10:  # Around 6 PM AEST
            return False, "evening"
        return False, "other"

class RSSAnalyzer:
    def __init__(self):
        self.config = {
            'openai_api_key': os.getenv('OPENAI_API_KEY'),
            'gmail_user': os.getenv('GMAIL_USER'),
            'gmail_password': os.getenv('GMAIL_APP_PASSWORD'),
            'recipient_email': os.getenv('RECIPIENT_EMAIL'),
        }
        self.validate_config()
        openai.api_key = self.config['openai_api_key']
        self.init_database()
        self.rss_feeds = RSS_FEEDS
        logging.info(f"Initialized RSS Analyzer with {len(self.rss_feeds)} feeds")
    
    def validate_config(self):
        """Validate configuration settings."""
        errors = []
        if not self.config['openai_api_key'].startswith('sk-'):
            errors.append("OpenAI API key must start with 'sk-'")
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, self.config['gmail_user']):
            errors.append(f"Invalid Gmail user email: {self.config['gmail_user']}")
        if not re.match(email_pattern, self.config['recipient_email']):
            errors.append(f"Invalid recipient email: {self.config['recipient_email']}")
        if len(self.config['gmail_password']) != 16:
            errors.append("Gmail app password must be 16 characters")
        if errors:
            raise ValueError(f"Configuration errors:\n  - {'\n  - '.join(errors)}")
        logging.info("Configuration validated successfully")
    
    def init_database(self):
        """Initialize SQLite database with enhanced schema."""
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
        for column in ['category', 'sentiment', 'key_metrics', 'geographic_tags', 'sector_tags']:
            try:
                self.conn.execute(f'ALTER TABLE items ADD COLUMN {column} TEXT')
            except sqlite3.OperationalError:
                pass
        self.conn.commit()
        logging.info("Database initialized successfully")
    
    def item_exists(self, link: str) -> bool:
        """Check if an item exists in the database by link."""
        cursor = self.conn.execute('SELECT 1 FROM items WHERE link = ?', (link,))
        return bool(cursor.fetchone())
    
    def title_exists(self, title: str, source_name: str) -> bool:
        """Check if an item with the same title and source exists."""
        cursor = self.conn.execute('SELECT 1 FROM items WHERE title = ? AND source_name = ?', (title, source_name))
        return bool(cursor.fetchone())
    
    def save_item(self, item: FeedItem):
        """Save an item to the database."""
        self.conn.execute('''
            INSERT INTO items (title, link, description, published, source_feed, source_name,
                            interest_score, ai_summary, category, sentiment, key_metrics,
                            geographic_tags, sector_tags)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            item.title, item.link, item.description, item.published.isoformat(),
            item.source_feed, item.source_name, item.interest_score, item.ai_summary,
            item.category, item.sentiment, json.dumps(item.key_metrics) if item.key_metrics else None,
            json.dumps(item.geographic_tags) if item.geographic_tags else None,
            json.dumps(item.sector_tags) if item.sector_tags else None
        ))
        self.conn.commit()
    
    def fetch_feed_items(self, feed_config: Dict) -> List[FeedItem]:
        """Fetch all items from a feed (legacy method)."""
        try:
            logging.info(f"Fetching feed: {feed_config['name']}")
            feed = feedparser.parse(feed_config['url'])
            if feed.bozo:
                logging.warning(f"Feed parsing warning for {feed_config['name']}: {feed.bozo_exception}")
            items = []
            now = datetime.now()
            for entry in feed.entries:
                published = now
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
                description = getattr(entry, 'description', getattr(entry, 'summary', ''))
                items.append(FeedItem(
                    title=entry.title,
                    link=entry.link,
                    description=description,
                    published=published,
                    source_feed=feed_config['url'],
                    source_name=feed_config['name']
                ))
            items.sort(key=lambda x: x.published, reverse=True)
            logging.info(f"Fetched {len(items)} items from {feed_config['name']}")
            return items
        except Exception as e:
            logging.error(f"Error fetching feed {feed_config['name']}: {e}")
            return []

    def fetch_feed_items_recent_only(self, feed_config: Dict, cutoff_time: datetime, max_items: int = 20) -> List[FeedItem]:
        """Fetch only recent items from a feed."""
        try:
            logging.info(f"Fetching recent items from: {feed_config['name']} (since {cutoff_time.strftime('%H:%M')})")
            old_timeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout(15)
            try:
                feed = feedparser.parse(feed_config['url'])
            finally:
                socket.setdefaulttimeout(old_timeout)
            if feed.bozo:
                logging.warning(f"Feed parsing warning for {feed_config['name']}: {feed.bozo_exception}")
            items = []
            processed_count = too_old_count = 0
            for entry in feed.entries:
                processed_count += 1
                published = datetime.now()
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
                if published < cutoff_time:
                    too_old_count += 1
                    if too_old_count >= 3:
                        logging.info(f"Stopping: hit {too_old_count} old items")
                        break
                    continue
                too_old_count = 0
                description = getattr(entry, 'description', getattr(entry, 'summary', ''))
                items.append(FeedItem(
                    title=entry.title,
                    link=entry.link,
                    description=description,
                    published=published,
                    source_feed=feed_config['url'],
                    source_name=feed_config['name']
                ))
                if len(items) >= max_items:
                    logging.info(f"Reached max items limit ({max_items}) for {feed_config['name']}")
                    break
            items.sort(key=lambda x: x.published, reverse=True)
            if items:
                logging.info(f"✓ {feed_config['name']}: {len(items)} recent items (processed {processed_count} entries)")
            else:
                logging.info(f"✓ {feed_config['name']}: No recent items (processed {processed_count} entries)")
            return items
        except Exception as e:
            logging.error(f"Error fetching {feed_config['name']}: {e}")
            return []

    def fetch_feeds_parallel_recent(self, cutoff_time: datetime, max_workers: int = 4) -> List[FeedItem]:
        """Fetch recent items from all feeds in parallel."""
        all_items = []
        start_time = time.time()
        logging.info(f"Fetching recent items from {len(self.rss_feeds)} feeds in parallel...")
        def fetch_with_timeout(feed_config):
            try:
                return self.fetch_feed_items_recent_only(feed_config, cutoff_time, max_items=15)
            except Exception as e:
                logging.error(f"Feed fetch error: {feed_config['name']} - {e}")
                return []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_feed = {executor.submit(fetch_with_timeout, feed): feed for feed in self.rss_feeds}
            completed = 0
            for future in concurrent.futures.as_completed(future_to_feed, timeout=60):
                feed_config = future_to_feed[future]
                completed += 1
                try:
                    items = future.result(timeout=5)
                    all_items.extend(items)
                    logging.info(f"Progress: {completed}/{len(self.rss_feeds)} feeds ({(completed/len(self.rss_feeds)*100):.0f}%)")
                except Exception as e:
                    logging.warning(f"Failed: {feed_config['name']} - {e}")
        elapsed = time.time() - start_time
        logging.info(f"Parallel fetch completed in {elapsed:.1f}s: {len(all_items)} total recent items")
        return all_items

    def should_use_ai_analysis_quick(self, item: FeedItem) -> bool:
        """Determine if an item requires AI analysis."""
        combined = (item.title.lower() + " " + item.description.lower())
        high_value_keywords = [
            'merger', 'acquisition', 'takeover', 'buyout', 'policy', 'regulation',
            'government', 'legislation', 'market outlook', 'forecast', 'prediction',
            'investment grade', 'credit rating', 'valuation', 'reit dividend',
            'distribution', 'capital raising', 'development approval', 'planning permit', 'zoning'
        ]
        tech_keywords = [
            'proptech', 'artificial intelligence', 'machine learning', 'blockchain',
            'automation', 'digital transformation', 'smart building', 'iot', 'data analytics',
            'predictive analytics', 'ai'
        ]
        return (any(keyword in combined for keyword in high_value_keywords) or
                any(keyword in combined for keyword in tech_keywords) or
                (len(item.description) > 200 and any(keyword in combined for keyword in [
                    'commercial property', 'office market', 'retail property',
                    'industrial property', 'property investment', 'real estate'
                ])))

    def auto_score_item_quick(self, item: FeedItem):
        """Assign a score to an item without AI."""
        combined = (item.title.lower() + " " + item.description.lower())
        if any(keyword in item.title.lower() for keyword in [
            'interest rate', 'rba cuts', 'rba raises', 'cash rate', 'property crash',
            'property boom', 'house prices surge'
        ]):
            item.interest_score = 9
            item.category = 'Market Movers'
            item.sentiment = 'Positive' if any(pos in combined for pos in ['cut', 'lower', 'boom', 'surge']) else 'Negative'
            item.ai_summary = f"Critical market development: {item.title}"
        elif any(keyword in combined for keyword in [
            'a-reit', 'reit dividend', 'commercial property', 'office occupancy',
            'retail vacancy', 'cap rates', 'property valuation'
        ]):
            item.interest_score = 8
            item.category = 'A-REIT Specific'
            item.sentiment = 'Neutral'
            item.ai_summary = f"A-REIT sector news: {item.title}"
        elif any(keyword in combined for keyword in [
            'proptech', 'smart building', 'building automation', 'property technology',
            'real estate tech'
        ]):
            item.interest_score = 7
            item.category = 'Technology Impact'
            item.sentiment = 'Positive'
            item.ai_summary = f"Property technology development: {item.title}"
        elif any(keyword in combined for keyword in ['property', 'real estate', 'construction', 'development']):
            item.interest_score = 6
            item.category = 'A-REIT Specific'
            item.sentiment = 'Neutral'
            item.ai_summary = f"Property sector update: {item.title}"
        elif any(keyword in combined for keyword in ['artificial intelligence', 'automation', 'digital', 'innovation']):
            item.interest_score = 5
            item.category = 'Technology Impact'
            item.sentiment = 'Positive'
            item.ai_summary = f"Technology news: {item.title}"
        else:
            item.interest_score = 4
            item.category = 'General Business'
            item.sentiment = 'Neutral'
            item.ai_summary = f"Business news: {item.title}"

    def process_ai_batch_quick(self, items: List[FeedItem]) -> int:
        """Process items in batches using AI."""
        if not items:
            return 0
        batch_size = 15
        processed_count = 0
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            prompt = f"Score these {len(batch)} news items for A-REIT CEO (1-10). Format: Item X: Score=Y\n\n"
            for idx, item in enumerate(batch, 1):
                prompt += f"{idx}. {item.title} - {item.description[:150]}...\n"
            try:
                response = openai.ChatCompletion.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=200,
                    temperature=0.1
                )
                content = response.choices[0].message.content.strip()
                for j, item in enumerate(batch):
                    item.interest_score = 6
                    item.category = 'General Business'
                    item.sentiment = 'Neutral'
                    item.ai_summary = f"AI analyzed: {item.title}"
                    for line in content.split('\n'):
                        if f"Item {j+1}:" in line and 'Score=' in line:
                            try:
                                item.interest_score = int(line.split('Score=')[1].split()[0])
                            except ValueError:
                                pass
                            break
                    processed_count += 1
                logging.info(f"AI batch {i//batch_size + 1}: {len(batch)} items scored")
            except Exception as e:
                logging.error(f"AI batch error: {e}")
                for item in batch:
                    self.auto_score_item_quick(item)
                    processed_count += 1
        return processed_count

    def process_feeds_optimized_recent(self):
        """Process only recent feed items."""
        logging.info("=" * 60)
        logging.info("Starting optimized recent-only RSS processing...")
        start_time = time.time()
        processor = IncrementalProcessor(self)
        cutoff_time = processor.get_incremental_cutoff_time()
        all_recent_items = self.fetch_feeds_parallel_recent(cutoff_time, max_workers=5)
        if not all_recent_items:
            logging.info("No recent items found")
            processor.update_last_run_time('feed_processing', 0)
            return {'total_scanned': 0, 'new_items': 0, 'processed': 0}
        logging.info(f"Found {len(all_recent_items)} recent items")
        new_items = [item for item in all_recent_items if not (self.item_exists(item.link) or self.title_exists(item.title, item.source_name))]
        logging.info(f"After deduplication: {len(new_items)} new items, {len(all_recent_items) - len(new_items)} duplicates")
        if not new_items:
            processor.update_last_run_time('feed_processing', 0)
            return {'total_scanned': len(all_recent_items), 'new_items': 0, 'processed': 0}
        ai_items = [item for item in new_items if self.should_use_ai_analysis_quick(item)]
        auto_items = [item for item in new_items if item not in ai_items]
        for item in auto_items:
            self.auto_score_item_quick(item)
        logging.info(f"Processing plan: {len(auto_items)} auto-scored, {len(ai_items)} for AI analysis")
        ai_processed = self.process_ai_batch_quick(ai_items) if ai_items else 0
        total_saved = 0
        for item in auto_items + ai_items:
            try:
                self.save_item(item)
                total_saved += 1
            except Exception as e:
                logging.error(f"Save error for {item.title[:50]}: {e}")
        processor.update_last_run_time('feed_processing', total_saved)
        elapsed = time.time() - start_time
        logging.info(f"Processing completed in {elapsed:.1f}s: {total_saved} items processed")
        return {'total_scanned': len(all_recent_items), 'new_items': len(new_items), 'processed': total_saved}

    def emergency_simple_process_fallback(self):
        """Fallback processing method for emergencies."""
        logging.info("Emergency fallback processing...")
        cutoff_time = datetime.now() - timedelta(hours=6)
        total_items = 0
        for feed_config in self.rss_feeds[:5]:
            try:
                items = self.fetch_feed_items(feed_config)
                recent_items = [item for item in items if item.published >= cutoff_time][:3]
                for item in recent_items:
                    if not self.item_exists(item.link):
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
        logging.info(f"Emergency fallback completed: {total_items} items")
        return {'total_scanned': total_items, 'new_items': total_items, 'processed': total_items}

    def get_items_for_email(self, hours_back: int = 24) -> List[Tuple]:
        """Retrieve items for email from the last N hours."""
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        cursor = self.conn.execute('''
            SELECT title, link, description, interest_score, ai_summary, source_name, 
                   processed_at, category, sentiment, key_metrics, geographic_tags, sector_tags
            FROM items 
            WHERE processed_at >= ? AND email_sent = FALSE
            ORDER BY interest_score DESC, processed_at DESC
        ''', (cutoff_time.isoformat(),))
        return cursor.fetchall()

    def generate_daily_email_from_items_enhanced(self, items: List[Tuple]) -> Optional[str]:
        """Generate an enhanced email that avoids clipping."""
        if not items:
            return None
        high_priority = [item for item in items if item[3] >= 8][:8]
        medium_priority = [item for item in items if 6 <= item[3] < 8][:6]
        low_priority = [item for item in items if 4 <= item[3] < 6][:4]
        filtered_items = high_priority + medium_priority + low_priority
        final_items = []
        seen_titles = set()
        for item in filtered_items:
            title = item[0].lower()[:50]
            if (title not in seen_titles and
                not any(phrase in item[0].lower() for phrase in [
                    'not found', 'sign up to rss.app', 'error', 'access denied', '404'
                ])):
                final_items.append(item)
                seen_titles.add(title)
            if len(final_items) >= 15:
                break
        return self.build_executive_email_html(final_items) if final_items else None

    def build_executive_email_html(self, items: List[Tuple]) -> str:
        """Build HTML email content."""
        current_date = datetime.now().strftime('%B %d, %Y')
        current_time = datetime.now().strftime('%I:%M %p AEST')
        total_items = len(items)
        high_priority_count = len([item for item in items if item[3] >= 8])
        avg_score = sum(item[3] for item in items) / len(items) if items else 0
        sentiment = self.calculate_simple_sentiment(items)
        sentiment_color = {"Positive": "#28a745", "Negative": "#dc3545", "Neutral": "#6c757d"}[sentiment]
        sorted_items = sorted(items, key=lambda x: x[3], reverse=True)
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Matt's Memo - {current_date}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; line-height: 1.5; margin: 0; padding: 0; background-color: #f8f9fa; color: #333; font-size: 14px; }}
        .container {{ max-width: 700px; margin: 0 auto; background-color: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; text-align: center; padding: 25px 15px; }}
        .header h1 {{ margin: 0; font-size: 28px; font-weight: 700; }}
        .subtitle {{ margin: 5px 0 0 0; opacity: 0.9; font-size: 14px; }}
        .dashboard {{ display: flex; background-color: #e9ecef; margin: 0; }}
        .metric {{ flex: 1; background: white; padding: 15px; text-align: center; border-right: 1px solid #e9ecef; }}
        .metric:last-child {{ border-right: none; }}
        .metric-value {{ font-size: 22px; font-weight: 700; color: #667eea; margin: 0; }}
        .metric-label {{ font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.3px; margin: 3px 0 0 0; }}
        .content {{ padding: 20px 15px; }}
        .summary-box {{ background: #f8f9fa; border-left: 4px solid #667eea; padding: 15px; margin-bottom: 20px; border-radius: 0 6px 6px 0; }}
        .news-item {{ border: 1px solid #e9ecef; border-radius: 6px; padding: 15px; margin-bottom: 12px; background: white; }}
        .news-title {{ font-weight: 600; font-size: 15px; margin-bottom: 6px; line-height: 1.3; }}
        .news-title a {{ color: #495057; text-decoration: none; }}
        .news-title a:hover {{ color: #667eea; }}
        .news-meta {{ font-size: 11px; color: #6c757d; margin-bottom: 8px; }}
        .news-summary {{ font-size: 13px; color: #495057; line-height: 1.4; }}
        .priority-high {{ border-left: 4px solid #dc3545; }}
        .priority-medium {{ border-left: 4px solid #ffc107; }}
        .social-section {{ background: #f8f9fa; padding: 15px; border-radius: 6px; margin-top: 20px; }}
        .social-post {{ background: white; border: 1px solid #e9ecef; border-radius: 4px; padding: 12px; margin-bottom: 10px; font-size: 13px; }}
        .footer {{ background: #495057; color: white; text-align: center; padding: 15px; font-size: 12px; }}
        .action-items {{ background: #e8f5e8; border: 1px solid #c3e6c3; border-radius: 6px; padding: 12px; margin-bottom: 15px; }}
        @media (max-width: 600px) {{ .dashboard {{ flex-direction: column; }} .metric {{ border-right: none; border-bottom: 1px solid #e9ecef; }} .metric:last-child {{ border-bottom: none; }} }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Matt's Memo</h1>
            <div class="subtitle">Strategic Intelligence for Real Estate Leaders</div>
            <div class="subtitle">{current_date} • {current_time}</div>
        </div>
        <div class="dashboard">
            <div class="metric">
                <div class="metric-value">{total_items}</div>
                <div class="metric-label">Items Analyzed</div>
            </div>
            <div class="metric">
                <div class="metric-value">{high_priority_count}</div>
                <div class="metric-label">High Priority</div>
            </div>
            <div class="metric">
                <div class="metric-value">{avg_score:.1f}/10</div>
                <div class="metric-label">Avg Relevance</div>
            </div>
            <div class="metric">
                <div class="metric-value" style="color: {sentiment_color}">{sentiment}</div>
                <div class="metric-label">Market Sentiment</div>
            </div>
        </div>
        <div class="content">
            <div class="summary-box">
                <strong>📊 Executive Summary</strong><br>
                {self.generate_executive_summary_text(sorted_items[:5])}
            </div>
            {self.generate_action_items_html(sorted_items[:3])}
            <h3 style="margin: 20px 0 15px 0; color: #495057; font-size: 16px;">📰 Priority Items</h3>
            {self.generate_news_items_html(sorted_items[:10])}
            {self.generate_social_media_html(sorted_items[:3])}
            {self.generate_additional_items_html(sorted_items[10:15])}
        </div>
        <div class="footer">
            <strong>Matt's Memo</strong> • Strategic Intelligence Platform<br>
            Powered by AI • {total_items} sources analyzed • Executive focused<br>
            <em>This briefing contains AI-generated insights. Verify independently.</em>
        </div>
    </div>
</body>
</html>
"""
        return html

    def calculate_simple_sentiment(self, items: List[Tuple]) -> str:
        """Calculate overall market sentiment."""
        if not items:
            return "Neutral"
        positive_keywords = ['growth', 'increase', 'strong', 'boost', 'positive', 'up', 'gain', 'improvement', 'rising', 'surge']
        negative_keywords = ['decline', 'fall', 'drop', 'weak', 'negative', 'down', 'loss', 'concern', 'risk', 'falling', 'crash']
        positive_score = negative_score = 0
        for item in items:
            title_desc = (item[0] + ' ' + item[2]).lower()
            weight = item[3] / 10.0
            positive_score += sum(title_desc.count(keyword) * weight for keyword in positive_keywords)
            negative_score += sum(title_desc.count(keyword) * weight for keyword in negative_keywords)
        if positive_score > negative_score * 1.3:
            return "Positive"
        elif negative_score > positive_score * 1.3:
            return "Negative"
        return "Neutral"

    def generate_executive_summary_text(self, top_items: List[Tuple]) -> str:
        """Generate executive summary text."""
        if not top_items:
            return "No significant developments in commercial property sector today."
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
        return (f"Key developments today focused on {', '.join(unique_themes[:2])}. "
                f"{len(top_items)} high-priority items require executive attention.") if unique_themes else \
               f"{len(top_items)} priority items identified across commercial property and related sectors."

    def generate_action_items_html(self, top_items: List[Tuple]) -> str:
        """Generate HTML for action items."""
        if not top_items:
            return ""
        actions = []
        for item in top_items[:3]:
            title, score = item[0], item[3]
            if score >= 9:
                actions.append(f"<strong>URGENT:</strong> Review implications of '{title[:60]}...'")
            elif score >= 8:
                actions.append(f"<strong>Monitor:</strong> Track developments in '{title[:60]}...'")
            elif score >= 7:
                actions.append(f"<strong>Consider:</strong> Assess impact of '{title[:60]}...'")
        return f'''
        <div class="action-items">
            <strong>🎯 Executive Action Items</strong><br>
            • {'<br>• '.join(actions)}
        </div>
        ''' if actions else ""

    def generate_news_items_html(self, items: List[Tuple]) -> str:
        """Generate HTML for news items."""
        html = ""
        for item in items:
            title, link, description, score, summary, source = item[:6]
            priority_class = "priority-high" if score >= 8 else "priority-medium" if score >= 6 else ""
            priority_icon = "🔴" if score >= 8 else "🟡" if score >= 6 else "🟢"
            clean_summary = re.sub('<[^<]+?>', '', (summary or description)[:200] + "...") if (summary or description) else "No summary available"
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
        """Generate HTML for social media content."""
        if len(items) < 2:
            return ""
        top_item = items[0]
        title = top_item[0]
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
        """Generate HTML for additional items."""
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
        """Send email with the provided HTML content."""
        msg = MIMEMultipart()
        msg['From'] = self.config['gmail_user']
        msg['To'] = self.config['recipient_email']
        msg['Subject'] = f"Matt's Memo - {datetime.now().strftime('%B %d, %Y')}"
        msg.attach(MIMEText(content, 'html'))
        try:
            with smtplib.SMTP('smtp.gmail.com', 587) as server:
                server.starttls()
                server.login(self.config['gmail_user'], self.config['gmail_password'])
                server.send_message(msg)
        except Exception as e:
            logging.error(f"Failed to send email: {e}")
            raise

    def send_daily_brief_enhanced(self, include_all: bool = False):
        """Send enhanced daily brief email."""
        try:
            items = self.get_items_for_email(24)
            if items:
                content = self.generate_daily_email_from_items_enhanced(items)
                if content:
                    content_size = len(content.encode('utf-8'))
                    if content_size > 100000:
                        logging.warning(f"Email content size: {content_size/1024:.1f}KB - may be clipped")
                    self.send_email(content)
                    cutoff_time = datetime.now() - timedelta(hours=24)
                    self.conn.execute('''
                        UPDATE items SET email_sent = TRUE 
                        WHERE processed_at >= ? AND email_sent = FALSE
                    ''', (cutoff_time.isoformat(),))
                    self.conn.commit()
                    logging.info(f"Enhanced email sent successfully ({content_size/1024:.1f}KB)")
                else:
                    logging.info("No content generated for enhanced email")
            else:
                logging.info("No items found for enhanced email")
        except Exception as e:
            logging.error(f"Enhanced email error: {e}")
            raise

    def should_send_email_now(self) -> Tuple[bool, str]:
        """Check if an email should be sent now."""
        return IncrementalProcessor(self).should_send_email()

    def send_daily_brief_incremental(self):
        """Send email based on schedule."""
        should_send, time_period = self.should_send_email_now()
        if should_send:
            logging.info(f"Sending {time_period} email brief...")
            self.send_daily_brief_enhanced()
        else:
            logging.info(f"Skipping email send - {time_period} run")

if __name__ == "__main__":
    analyzer = RSSAnalyzer()
    schedule.every().day.at("20:00").do(analyzer.process_feeds_optimized_recent)  # 6 AM AEST
    schedule.every().day.at("02:00").do(analyzer.process_feeds_optimized_recent)   # 12 PM AEST
    schedule.every().day.at("08:00").do(analyzer.process_feeds_optimized_recent)   # 6 PM AEST
    while True:
        schedule.run_pending()
        time.sleep(60)
