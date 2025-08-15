#!/usr/bin/env python3
"""
RSS Feed Analyzer for A-REIT CEO/COO - Executive Intelligence Platform
ULTRA-ENGAGING VERSION - Dynamic AI content with hallucination prevention
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
import hashlib

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
    source_name: str = ""
    interest_score: Optional[int] = None
    ai_summary: Optional[str] = None
    category: Optional[str] = None
    sentiment: Optional[str] = None
    key_metrics: Optional[List[str]] = None
    geographic_tags: Optional[List[str]] = None
    sector_tags: Optional[List[str]] = None


class IncrementalProcessor:
    """Processes only new items since last run"""
    
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
        """Get cutoff time for daily email (last 24 hours)"""
        # For 6am daily email, get last 24 hours of content
        return datetime.now() - timedelta(hours=24)


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
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Initialize OpenAI
        openai.api_key = self.config['openai_api_key']
        
        # Initialize database
        self.init_database()
        
        # Load RSS feeds
        self.rss_feeds = RSS_FEEDS
        
        # Cache for AI responses to prevent repeated calls
        self.ai_cache = {}
        
        logging.info(f"Initialized RSS Analyzer with {len(self.rss_feeds)} feeds")
    
    def init_database(self):
        """Initialize SQLite database"""
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
        self.conn.commit()
        logging.info("Database initialized successfully")
    
    def item_exists(self, link: str) -> bool:
        """Check if item exists in database"""
        cursor = self.conn.execute('SELECT 1 FROM items WHERE link = ?', (link,))
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

    def fetch_feed_items_recent_only(self, feed_config: Dict, cutoff_time: datetime, max_items: int = 30) -> List[FeedItem]:
        """Fetch recent RSS feed items"""
        feed_url = feed_config['url']
        feed_name = feed_config['name']
        
        try:
            logging.info(f"Fetching from: {feed_name}")
            
            # Set timeout
            old_timeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout(15)
            
            try:
                feed = feedparser.parse(feed_url)
            finally:
                socket.setdefaulttimeout(old_timeout)
            
            items = []
            
            for entry in feed.entries:
                try:
                    if not hasattr(entry, 'title') or not hasattr(entry, 'link'):
                        continue
                    
                    # Parse published date
                    published = datetime.now()
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        try:
                            published = datetime(*entry.published_parsed[:6])
                        except:
                            pass
                    
                    # Only get items from last 24 hours
                    if published < cutoff_time:
                        continue
                    
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
                    
                    if len(items) >= max_items:
                        break
                        
                except Exception as e:
                    logging.warning(f"Error processing entry: {e}")
                    continue
            
            items.sort(key=lambda x: x.published, reverse=True)
            logging.info(f"✓ {feed_name}: {len(items)} items from last 24h")
            
            return items
            
        except Exception as e:
            logging.error(f"Error fetching {feed_name}: {e}")
            return []

    def fetch_feeds_parallel_recent(self, cutoff_time: datetime, max_workers: int = 5) -> List[FeedItem]:
        """Fetch recent items from all feeds in parallel"""
        all_items = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_feed = {
                executor.submit(self.fetch_feed_items_recent_only, feed_config, cutoff_time): feed_config 
                for feed_config in self.rss_feeds
            }
            
            for future in concurrent.futures.as_completed(future_to_feed):
                try:
                    items = future.result(timeout=10)
                    all_items.extend(items)
                except Exception as e:
                    logging.warning(f"Feed fetch failed: {e}")
        
        logging.info(f"Total items fetched: {len(all_items)}")
        return all_items

    def ai_score_and_analyze_batch(self, items: List[FeedItem]) -> List[FeedItem]:
        """Score and analyze items using AI with structured prompts to prevent hallucination"""
        if not items:
            return items
        
        batch_size = 10
        analyzed_items = []
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            
            # Create structured prompt with clear instructions
            prompt = f"""You are an expert commercial property analyst. Score these news items for a REIT CEO.

STRICT RULES:
1. Score 1-10 based ONLY on commercial property relevance
2. Provide a 1-sentence insight that is DIRECTLY related to the headline
3. DO NOT make up information not in the title/description
4. Focus on: office, retail, industrial, logistics property impacts
5. Be specific about WHY it matters to property investors

Format EXACTLY as shown:
Item X:
Score: [1-10]
Impact: [One specific sentence about property market impact]
Trend: [Bullish/Bearish/Neutral]

Items to analyze:
"""
            
            for idx, item in enumerate(batch, 1):
                # Clean description for prompt
                desc = re.sub('<[^<]+?>', '', item.description)[:200] if item.description else ""
                prompt += f"\nItem {idx}:\nTitle: {item.title}\nDescription: {desc}\n"
            
            try:
                response = openai.ChatCompletion.create(
                    model="gpt-4o",
                    messages=[
                        {"role": "system", "content": "You are a commercial property analyst. Be accurate, specific, and never make up facts."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=500,
                    temperature=0.3
                )
                
                content = response.choices[0].message.content.strip()
                
                # Parse structured response
                for j, item in enumerate(batch):
                    item_pattern = f"Item {j+1}:"
                    
                    # Extract score, impact, and trend
                    try:
                        item_section = content.split(item_pattern)[1].split(f"Item {j+2}:")[0] if j+1 < len(batch) else content.split(item_pattern)[1]
                        
                        # Extract score
                        score_match = re.search(r'Score:\s*(\d+)', item_section)
                        if score_match:
                            item.interest_score = int(score_match.group(1))
                        else:
                            item.interest_score = 5
                        
                        # Extract impact
                        impact_match = re.search(r'Impact:\s*(.+?)(?:Trend:|$)', item_section, re.DOTALL)
                        if impact_match:
                            item.ai_summary = impact_match.group(1).strip()
                        
                        # Extract trend
                        trend_match = re.search(r'Trend:\s*(Bullish|Bearish|Neutral)', item_section)
                        if trend_match:
                            item.sentiment = trend_match.group(1)
                        else:
                            item.sentiment = "Neutral"
                        
                        # Set category based on score
                        if item.interest_score >= 8:
                            item.category = "Critical"
                        elif item.interest_score >= 6:
                            item.category = "Important"
                        else:
                            item.category = "Monitor"
                            
                    except Exception as e:
                        logging.warning(f"Failed to parse AI response for item: {e}")
                        item.interest_score = 5
                        item.ai_summary = "Property market impact under analysis"
                        item.sentiment = "Neutral"
                        item.category = "Monitor"
                    
                    analyzed_items.append(item)
                
            except Exception as e:
                logging.error(f"AI batch analysis failed: {e}")
                # Basic scoring fallback
                for item in batch:
                    item.interest_score = 5
                    item.ai_summary = "Analysis pending"
                    item.sentiment = "Neutral"
                    item.category = "Monitor"
                    analyzed_items.append(item)
        
        return analyzed_items

    def generate_dynamic_email_content(self, items: List[Tuple]) -> str:
        """Generate ultra-engaging HTML email with dynamic AI content"""
        
        if not items:
            return None
        
        # Sort and categorize
        sorted_items = sorted(items, key=lambda x: x[3], reverse=True)
        
        critical_items = [item for item in sorted_items if item[3] >= 8]
        important_items = [item for item in sorted_items if 6 <= item[3] < 8]
        monitor_items = [item for item in sorted_items if 4 <= item[3] < 6]
        
        # Get market summary using AI
        market_summary = self.generate_ai_market_summary(sorted_items[:10])
        
        # Get personalized greeting
        greeting = self.generate_ai_greeting(critical_items, market_summary)
        
        # Get the big story if there is one
        big_story = self.generate_ai_big_story(critical_items) if critical_items else None
        
        # Generate subject line
        subject_line = self.generate_ai_subject_line(critical_items, market_summary)
        
        # Store subject for email sending
        self.email_subject = subject_line
        
        current_date = datetime.now().strftime('%B %d, %Y')
        
        # Build HTML email
        html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #1a1a1a;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
        }}
        
        .container {{
            max-width: 650px;
            margin: 0 auto;
            background: #ffffff;
            border-radius: 20px;
            overflow: hidden;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
        }}
        
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px 30px;
            text-align: center;
        }}
        
        .header h1 {{
            font-size: 32px;
            font-weight: 800;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
        }}
        
        .header .date {{
            font-size: 14px;
            opacity: 0.9;
            font-weight: 500;
        }}
        
        .greeting {{
            padding: 30px;
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            font-size: 18px;
            line-height: 1.8;
            color: #2c3e50;
            border-bottom: 3px solid #667eea;
        }}
        
        .market-pulse {{
            padding: 30px;
            background: #fff;
            border-bottom: 1px solid #e0e0e0;
        }}
        
        .pulse-grid {{
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 20px;
            margin-top: 20px;
        }}
        
        .pulse-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 15px;
            text-align: center;
            transform: scale(1);
            transition: transform 0.3s;
        }}
        
        .pulse-card:hover {{
            transform: scale(1.05);
        }}
        
        .pulse-number {{
            font-size: 36px;
            font-weight: 800;
            margin-bottom: 5px;
        }}
        
        .pulse-label {{
            font-size: 12px;
            opacity: 0.95;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        
        .big-story {{
            padding: 30px;
            background: #fff3cd;
            border-left: 5px solid #ffc107;
            margin: 20px;
            border-radius: 10px;
        }}
        
        .big-story h2 {{
            color: #856404;
            margin-bottom: 15px;
            font-size: 24px;
        }}
        
        .section {{
            padding: 30px;
            background: white;
        }}
        
        .section-header {{
            font-size: 22px;
            font-weight: 700;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 3px solid #667eea;
            color: #2c3e50;
        }}
        
        .news-item {{
            background: #f8f9fa;
            padding: 20px;
            margin-bottom: 20px;
            border-radius: 12px;
            border-left: 4px solid #667eea;
            transition: all 0.3s;
        }}
        
        .news-item:hover {{
            box-shadow: 0 5px 15px rgba(102, 126, 234, 0.2);
            transform: translateX(5px);
        }}
        
        .news-item.critical {{
            border-left-color: #dc3545;
            background: #fff5f5;
        }}
        
        .news-item.important {{
            border-left-color: #ffc107;
            background: #fffbf0;
        }}
        
        .news-title {{
            font-size: 18px;
            font-weight: 600;
            color: #2c3e50;
            margin-bottom: 10px;
            line-height: 1.4;
        }}
        
        .news-insight {{
            background: white;
            padding: 15px;
            border-radius: 8px;
            margin: 12px 0;
            font-size: 15px;
            color: #495057;
            border: 1px solid #e0e0e0;
            position: relative;
            padding-left: 35px;
        }}
        
        .news-insight:before {{
            content: "💡";
            position: absolute;
            left: 10px;
            top: 15px;
            font-size: 18px;
        }}
        
        .news-meta {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 12px;
            padding-top: 12px;
            border-top: 1px solid #e0e0e0;
        }}
        
        .source {{
            font-size: 13px;
            color: #6c757d;
        }}
        
        .read-more {{
            display: inline-block;
            padding: 8px 16px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            text-decoration: none;
            border-radius: 20px;
            font-size: 13px;
            font-weight: 600;
            transition: all 0.3s;
        }}
        
        .read-more:hover {{
            transform: scale(1.05);
            box-shadow: 0 5px 15px rgba(102, 126, 234, 0.3);
        }}
        
        .score-badge {{
            display: inline-block;
            padding: 4px 10px;
            border-radius: 15px;
            font-size: 12px;
            font-weight: 700;
            margin-left: 10px;
        }}
        
        .score-critical {{ background: #dc3545; color: white; }}
        .score-important {{ background: #ffc107; color: #000; }}
        .score-monitor {{ background: #28a745; color: white; }}
        
        .bottom-line {{
            padding: 30px;
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
            color: white;
            text-align: center;
        }}
        
        .bottom-line h3 {{
            font-size: 24px;
            margin-bottom: 15px;
        }}
        
        .action-items {{
            background: rgba(255, 255, 255, 0.2);
            padding: 20px;
            border-radius: 10px;
            margin-top: 20px;
            backdrop-filter: blur(10px);
        }}
        
        .footer {{
            padding: 30px;
            background: #f8f9fa;
            text-align: center;
            color: #6c757d;
            font-size: 14px;
        }}
        
        .footer a {{
            color: #667eea;
            text-decoration: none;
            font-weight: 600;
        }}
        
        @media (max-width: 600px) {{
            .pulse-grid {{ grid-template-columns: 1fr; }}
            .header h1 {{ font-size: 24px; }}
            .greeting {{ font-size: 16px; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏢 Property Intelligence Daily</h1>
            <div class="date">{current_date} Edition</div>
        </div>
        
        <div class="greeting">
            {greeting}
        </div>
        
        <div class="market-pulse">
            <h2 style="margin: 0; color: #2c3e50;">📊 Market Pulse</h2>
            <div class="pulse-grid">
                <div class="pulse-card">
                    <div class="pulse-number">{len(sorted_items)}</div>
                    <div class="pulse-label">Stories Analyzed</div>
                </div>
                <div class="pulse-card">
                    <div class="pulse-number">{len(critical_items)}</div>
                    <div class="pulse-label">Critical Alerts</div>
                </div>
                <div class="pulse-card">
                    <div class="pulse-number">{self.calculate_market_temp(sorted_items)}°</div>
                    <div class="pulse-label">Market Temp</div>
                </div>
            </div>
        </div>
"""

        # Add big story if exists
        if big_story:
            html_content += f"""
        <div class="big-story">
            <h2>🎯 The Big Story</h2>
            {big_story}
        </div>
"""

        # Add critical items
        if critical_items:
            html_content += f"""
        <div class="section">
            <div class="section-header">🚨 Critical: Act Today</div>
"""
            for item in critical_items[:5]:
                html_content += self.generate_news_item_html(item, 'critical')
            html_content += "</div>"

        # Add important items
        if important_items:
            html_content += f"""
        <div class="section">
            <div class="section-header">👀 Important: On Your Radar</div>
"""
            for item in important_items[:7]:
                html_content += self.generate_news_item_html(item, 'important')
            html_content += "</div>"

        # Add quick scan items
        if monitor_items:
            html_content += f"""
        <div class="section">
            <div class="section-header">⚡ Quick Scan</div>
            <div style="font-size: 14px; color: #6c757d; margin-bottom: 15px;">
                Lower priority but worth a glance:
            </div>
"""
            for item in monitor_items[:5]:
                html_content += self.generate_news_item_html(item, 'monitor')
            html_content += "</div>"

        # Add bottom line
        action_recommendation = self.generate_ai_action_recommendation(critical_items, market_summary)
        
        html_content += f"""
        <div class="bottom-line">
            <h3>📈 Your Move</h3>
            <div class="action-items">
                {action_recommendation}
            </div>
        </div>
        
        <div class="footer">
            <strong>That's your intelligence edge for today.</strong><br><br>
            Remember: Information is power, but action is profit.<br><br>
            <em>P.S. - This email was crafted by AI that read {len(sorted_items)} articles in 0.3 seconds.<br>
            What took you 2 minutes to read would've taken 2 hours to research.</em><br><br>
            Built with 🤖 by <a href="https://www.linkedin.com/in/mattwhiteoak">Matt Whiteoak</a>
        </div>
    </div>
</body>
</html>"""

        return html_content

    def generate_news_item_html(self, item: Tuple, priority: str) -> str:
        """Generate HTML for a single news item"""
        title, link, description, score, ai_summary, source = item[:6]
        
        # Clean AI summary
        if ai_summary and len(ai_summary) > 10:
            insight = ai_summary
        else:
            insight = self.generate_quick_insight(title, description)
        
        score_class = 'score-' + priority
        
        html = f"""
        <div class="news-item {priority}">
            <div class="news-title">
                {title}
                <span class="score-badge {score_class}">Score: {score}/10</span>
            </div>
            <div class="news-insight">
                {insight}
            </div>
            <div class="news-meta">
                <span class="source">📰 {source}</span>
                <a href="{link}" class="read-more">Read Full Story →</a>
            </div>
        </div>
"""
        return html

    def generate_quick_insight(self, title: str, description: str) -> str:
        """Generate a quick insight if AI summary is missing"""
        # Use cached response if available
        cache_key = hashlib.md5(f"{title}:{description[:100]}".encode()).hexdigest()
        if cache_key in self.ai_cache:
            return self.ai_cache[cache_key]
        
        try:
            prompt = f"""In ONE sentence, explain why this matters to commercial property investors:
Title: {title}
Be specific and actionable. No fluff."""

            response = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=50,
                temperature=0.3
            )
            
            insight = response.choices[0].message.content.strip()
            self.ai_cache[cache_key] = insight
            return insight
            
        except:
            return "This development could impact property market dynamics and investment strategies."

    def generate_ai_market_summary(self, top_items: List[Tuple]) -> str:
        """Generate market summary using AI"""
        if not top_items:
            return "Markets are quiet today."
        
        headlines = "\n".join([f"- {item[0]}" for item in top_items[:5]])
        
        try:
            prompt = f"""Based on these top property news headlines, write a 2-sentence market summary:

{headlines}

Focus on the overall market direction and key theme. Be specific but concise."""

            response = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=100,
                temperature=0.5
            )
            
            return response.choices[0].message.content.strip()
            
        except:
            return "Property markets are showing mixed signals with several developments worth monitoring."

    def generate_ai_greeting(self, critical_items: List[Tuple], market_summary: str) -> str:
        """Generate personalized greeting using AI"""
        
        try:
            context = "critical alerts today" if critical_items else "steady market conditions"
            
            prompt = f"""Write a 2-3 sentence engaging opening for a property market briefing email.

Context: {context}
Market summary: {market_summary}

Make it conversational, slightly witty, and action-oriented. Like you're talking to a smart friend who runs a REIT.
Start with something like "Morning champion" or "Hey there" - keep it fresh."""

            response = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=100,
                temperature=0.7
            )
            
            return response.choices[0].message.content.strip()
            
        except:
            if critical_items:
                return "Good morning! Buckle up - we've got some critical developments that need your attention today. Let's dive into what's moving the market."
            else:
                return "Morning champion! Markets are steady but there are opportunities hiding in today's news. Here's what you need to know."

    def generate_ai_big_story(self, critical_items: List[Tuple]) -> Optional[str]:
        """Generate the big story narrative if there's critical news"""
        if not critical_items:
            return None
        
        top_story = critical_items[0]
        
        try:
            prompt = f"""Write a 3-4 sentence narrative about why this is THE story to watch today:

Title: {top_story[0]}
Context: {top_story[4] if top_story[4] else top_story[2][:200]}

Make it compelling and specific about the commercial property impact. Use active voice and strong verbs."""

            response = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=150,
                temperature=0.5
            )
            
            return response.choices[0].message.content.strip()
            
        except:
            return None

    def generate_ai_subject_line(self, critical_items: List[Tuple], market_summary: str) -> str:
        """Generate engaging subject line using AI"""
        
        try:
            context = critical_items[0][0] if critical_items else market_summary[:100]
            
            prompt = f"""Write a compelling email subject line for a property market briefing.

Top story: {context}

Make it urgent but not clickbait. Max 60 characters. Use an emoji."""

            response = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=30,
                temperature=0.8
            )
            
            subject = response.choices[0].message.content.strip()
            # Ensure it's not too long
            if len(subject) > 70:
                subject = subject[:67] + "..."
            return subject
            
        except:
            fallbacks = [
                "🔥 Property Alert: Big Moves in Today's Market",
                "📊 Your Daily Edge: Critical Property Intel Inside",
                "🎯 Market Shift: What You Need to Know Today",
                "⚡ Breaking: Key Developments in Commercial Property"
            ]
            return random.choice(fallbacks)

    def generate_ai_action_recommendation(self, critical_items: List[Tuple], market_summary: str) -> str:
        """Generate specific action recommendations"""
        
        try:
            context = "Critical items: " + ", ".join([item[0][:50] for item in critical_items[:3]]) if critical_items else "No critical items"
            
            prompt = f"""Based on today's property market developments, write 2-3 specific action items for a REIT CEO.

{context}
Market: {market_summary}

Be specific and actionable. Format as bullet points. Focus on what they should DO today."""

            response = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=150,
                temperature=0.4
            )
            
            return response.choices[0].message.content.strip().replace('•', '→').replace('- ', '→ ')
            
        except:
            return """→ Review your portfolio exposure to interest rate changes
→ Schedule team discussion on market positioning
→ Monitor tenant stability in retail properties"""

    def calculate_market_temp(self, items: List[Tuple]) -> int:
        """Calculate market temperature (0-100)"""
        if not items:
            return 50
        
        high_scores = len([item for item in items if item[3] >= 7])
        total = len(items)
        
        # Temperature based on proportion of high-impact news
        temp = 50 + (high_scores / max(total, 1)) * 50
        
        # Adjust based on sentiment
        sentiments = [item[8] for item in items[:10] if len(item) > 8 and item[8]]
        if sentiments:
            bullish = sentiments.count('Bullish')
            bearish = sentiments.count('Bearish')
            temp += (bullish - bearish) * 5
        
        return min(100, max(0, int(temp)))

    def process_daily_intelligence(self):
        """Main processing for daily 6am email"""
        logging.info("=" * 60)
        logging.info("🌅 Starting Daily Intelligence Processing (6am Edition)")
        
        # Get last 24 hours of content
        processor = IncrementalProcessor(self)
        cutoff_time = processor.get_incremental_cutoff_time()
        
        # Fetch all items from last 24 hours
        all_items = self.fetch_feeds_parallel_recent(cutoff_time)
        
        if not all_items:
            logging.warning("No items found in last 24 hours")
            return
        
        logging.info(f"Found {len(all_items)} items from last 24 hours")
        
        # Filter for new items only
        new_items = []
        for item in all_items:
            if not self.item_exists(item.link):
                new_items.append(item)
        
        logging.info(f"New items to process: {len(new_items)}")
        
        if new_items:
            # AI analysis
            analyzed_items = self.ai_score_and_analyze_batch(new_items)
            
            # Save to database
            for item in analyzed_items:
                self.save_item(item)
            
            logging.info(f"Saved {len(analyzed_items)} items to database")
        
        # Get all items from last 24 hours for email
        self.send_daily_intelligence_email()
        
        # Update last run time
        processor.update_last_run_time('feed_processing', len(new_items))
        
        logging.info("✅ Daily intelligence processing complete!")
        logging.info("=" * 60)

    def send_daily_intelligence_email(self):
        """Send the daily 6am intelligence email"""
        # Get last 24 hours of items
        cutoff_time = datetime.now() - timedelta(hours=24)
        
        cursor = self.conn.execute('''
            SELECT title, link, description, interest_score, ai_summary, source_name, 
                   processed_at, category, sentiment
            FROM items 
            WHERE processed_at >= ?
            ORDER BY interest_score DESC, processed_at DESC
        ''', (cutoff_time,))
        
        items = cursor.fetchall()
        
        if not items:
            logging.warning("No items for daily email")
            return
        
        logging.info(f"Generating email for {len(items)} items")
        
        # Generate dynamic content
        html_content = self.generate_dynamic_email_content(items)
        
        if not html_content:
            logging.error("Failed to generate email content")
            return
        
        # Send email
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = getattr(self, 'email_subject', f"🏢 Property Intelligence - {datetime.now().strftime('%B %d')}")
            msg['From'] = self.config['gmail_user']
            msg['To'] = self.config['recipient_email']
            
            # Plain text fallback
            text_part = MIMEText("Please view this email in HTML format for the best experience.", 'plain')
            html_part = MIMEText(html_content, 'html')
            
            msg.attach(text_part)
            msg.attach(html_part)
            
            with smtplib.SMTP('smtp.gmail.com', 587) as server:
                server.starttls()
                server.login(self.config['gmail_user'], self.config['gmail_password'])
                server.send_message(msg)
            
            logging.info("✅ Daily intelligence email sent successfully!")
            
            # Mark items as sent
            self.conn.execute('''
                UPDATE items SET email_sent = TRUE 
                WHERE processed_at >= ?
            ''', (cutoff_time,))
            self.conn.commit()
            
        except Exception as e:
            logging.error(f"Failed to send email: {e}")


def main():
    """Main function"""
    try:
        analyzer = RSSAnalyzer()
        
        if len(sys.argv) > 1:
            command = sys.argv[1].lower()
            
            if command in ['process', 'run', 'test']:
                logging.info("Running daily intelligence processing...")
                analyzer.process_daily_intelligence()
                
            elif command == 'email':
                logging.info("Sending daily email only...")
                analyzer.send_daily_intelligence_email()
                
            else:
                print("Usage: python rss_analyzer.py [process|email]")
                sys.exit(1)
        else:
            # Schedule for 6am daily
            logging.info("Starting scheduled mode - will run at 6:00 AM daily")
            
            schedule.every().day.at("06:00").do(analyzer.process_daily_intelligence)
            
            # Run once on startup for testing
            analyzer.process_daily_intelligence()
            
            while True:
                schedule.run_pending()
                time.sleep(60)
            
    except KeyboardInterrupt:
        logging.info("Stopped by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()
