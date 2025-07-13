#!/usr/bin/env python3
"""
RSS Feed Analyzer for A-REIT CEO/COO - Executive Intelligence Platform
Monitors RSS feeds, evaluates content with OpenAI, and sends daily emails
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
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from dotenv import load_dotenv
from collections import defaultdict, Counter

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
        """Fetch and parse RSS feed items"""
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
            
            # Log age distribution
            if items:
                newest = items[0].published
                oldest = items[-1].published
                hours_span = (newest - oldest).total_seconds() / 3600
                logging.info(f"Fetched {len(items)} items from {feed_name} (spanning {hours_span:.1f} hours)")
            else:
                logging.warning(f"No items found in feed: {feed_name}")
            
            return items
            
        except Exception as e:
            logging.error(f"Error fetching feed {feed_name} ({feed_url}): {e}")
            return []
    
    def categorize_sources(self, items: List[FeedItem]) -> Dict[str, str]:
        """Analyze source types and create dynamic categories"""
        source_analysis = {}
        
        for item in items:
            source = item.source_name.lower()
            if 'twitter' in source or 'social' in source:
                source_analysis[item.source_name] = 'Social Media Intelligence'
            elif 'facebook' in source:
                source_analysis[item.source_name] = 'Social Media Intelligence'
            elif 'politics' in source or 'government' in source:
                source_analysis[item.source_name] = 'Government & Policy'
            elif 'property' in source or 'real estate' in source or 'reit' in source:
                source_analysis[item.source_name] = 'Real Estate & Property'
            elif 'financial' in source or 'bloomberg' in source or 'reuters' in source:
                source_analysis[item.source_name] = 'Financial Markets'
            elif 'construction' in source or 'development' in source:
                source_analysis[item.source_name] = 'Construction & Development'
            elif 'tech' in source or 'innovation' in source:
                source_analysis[item.source_name] = 'Technology & Innovation'
            else:
                source_analysis[item.source_name] = 'General Business'
        
        return source_analysis
    
    def evaluate_with_openai(self, item: FeedItem, source_categories: Dict[str, str]) -> Tuple[int, str, str, str, List[str], List[str], List[str]]:
        """Enhanced evaluation with categorization, sentiment, and extraction"""
        try:
            source_category = source_categories.get(item.source_name, 'General Business')
            
            prompt = f"""
You are analyzing news for a CEO/COO of an Australian A-REIT (Real Estate Investment Trust) with global assets. 

ARTICLE DETAILS:
Source: {item.source_name} ({source_category})
Title: {item.title}
Description: {item.description[:1000]}...

ANALYSIS REQUIRED:
1. RELEVANCE SCORE (1-10): Rate importance for A-REIT executives
2. CATEGORY: Choose primary category from:
   - Market Movers (rates, major policy, economic indicators)
   - A-REIT Specific (REIT performance, property transactions, sector news)
   - Global Trends (international markets affecting Australia)
   - Opportunities (M&A, development, partnerships, emerging trends)
   - Risks (economic threats, regulatory changes, market disruption)
   - Government & Policy (legislation, planning, taxation)
   - Social Intelligence (market sentiment, public opinion)
   - Technology Impact (PropTech, digital transformation, AI, automation, smart buildings)

3. SENTIMENT: Positive, Negative, or Neutral for real estate/REIT sector

4. KEY METRICS: Extract numbers, percentages, dollar amounts, dates (if any)

5. GEOGRAPHIC TAGS: Identify locations mentioned (Sydney, Melbourne, Brisbane, Australia, Global, etc.)

6. SECTOR TAGS: Identify property sectors (Office, Retail, Industrial, Residential, Mixed-use, Healthcare, Student, etc.)

7. EXECUTIVE SUMMARY: 2-3 sentences explaining relevance and potential impact

8. AUSTRALIAN COMMERCIAL PROPERTY IMPACT: 2-3 sentences specifically explaining how this relates to or could impact the Australian commercial property market, A-REIT valuations, or investment decisions

IMPORTANT SCORING GUIDELINES:
- **Technology items** that could impact real estate operations, tenant experience, building efficiency, or property management should score 6+ even if not directly property-related
- **PropTech, AI, automation, digital transformation** in real estate context should score 7+
- **Traditional high scores (8-10)**: Direct REIT impact, major market movements, regulatory changes
- **Medium scores (5-7)**: Industry trends, economic indicators, competitive intelligence, relevant technology
- **Lower scores (3-4)**: General business news, emerging technology with potential future impact
- **Very low (1-2)**: Irrelevant content, pure entertainment

Consider these factors for scoring:
- Direct impact on property operations, valuations, or tenant demand
- Technology that could disrupt or enhance real estate industry
- Economic indicators affecting property investment
- Regulatory changes impacting development or operations
- Market trends affecting commercial property performance

RESPONSE FORMAT:
Score: X
Category: [Category Name]
Sentiment: [Positive/Negative/Neutral]
Key_Metrics: [comma-separated list or "None"]
Geographic_Tags: [comma-separated list or "None"]
Sector_Tags: [comma-separated list or "None"]
Summary: [2-3 sentence executive summary]
Australian_Impact: [2-3 sentences specifically about Australian commercial property market relevance]

IMPORTANT: Base your analysis ONLY on the article content provided. If making inferences, clearly state they are AI-generated insights. For the Australian Impact section, be specific about potential effects on commercial property values, tenant demand, investment flows, or sector performance. For technology items, explain how the technology could impact property operations, tenant experience, or investment decisions.
"""
            
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=500,
                temperature=0.3
            )
            
            content = response.choices[0].message.content.strip()
            
            # Parse response
            score = 5
            category = "General Business"
            sentiment = "Neutral" 
            key_metrics = []
            geographic_tags = []
            sector_tags = []
            summary = "Analysis pending"
            australian_impact = "Australian commercial property impact to be assessed"
            
            lines = content.split('\n')
            for line in lines:
                line = line.strip()
                if line.startswith('Score:'):
                    try:
                        score_text = line.split(':', 1)[1].strip()
                        score = int(score_text.split()[0])
                        score = max(1, min(10, score))
                    except (ValueError, IndexError):
                        logging.warning(f"Could not parse score from: {line}")
                elif line.startswith('Category:'):
                    category = line.split(':', 1)[1].strip()
                elif line.startswith('Sentiment:'):
                    sentiment = line.split(':', 1)[1].strip()
                elif line.startswith('Key_Metrics:'):
                    metrics_text = line.split(':', 1)[1].strip()
                    if metrics_text.lower() != "none":
                        key_metrics = [m.strip() for m in metrics_text.split(',')]
                elif line.startswith('Geographic_Tags:'):
                    geo_text = line.split(':', 1)[1].strip()
                    if geo_text.lower() != "none":
                        geographic_tags = [g.strip() for g in geo_text.split(',')]
                elif line.startswith('Sector_Tags:'):
                    sector_text = line.split(':', 1)[1].strip()
                    if sector_text.lower() != "none":
                        sector_tags = [s.strip() for s in sector_text.split(',')]
                elif line.startswith('Summary:'):
                    summary = line.split(':', 1)[1].strip()
                elif line.startswith('Australian_Impact:'):
                    australian_impact = line.split(':', 1)[1].strip()
            
            # Combine summary and Australian impact for storage
            combined_summary = f"{summary}||AUSTRALIAN_IMPACT||{australian_impact}"
            
            logging.info(f"Evaluated '{item.title[:50]}...' - Score: {score}, Category: {category}")
            return score, combined_summary, category, sentiment, key_metrics, geographic_tags, sector_tags
            
        except Exception as e:
            logging.error(f"Error evaluating item with OpenAI: {e}")
            return 5, f"Error in AI evaluation: {str(e)[:100]}||AUSTRALIAN_IMPACT||Unable to assess Australian commercial property impact due to evaluation error.", "General Business", "Neutral", [], [], []
    
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
    
    def process_feeds(self):
        """Main function to process all RSS feeds"""
        logging.info("=" * 60)
        logging.info("Starting feed processing...")
        
        # Only process items from the last 24 hours
        cutoff_time = datetime.now() - timedelta(hours=24)
        
        total_new_items = 0
        total_processed = 0
        total_filtered_old = 0
        total_duplicates = 0
        
        # Get all items first for source categorization
        all_items = []
        for feed_config in self.rss_feeds:
            items = self.fetch_feed_items(feed_config)
            all_items.extend(items)
        
        # Analyze source categories
        source_categories = self.categorize_sources(all_items)
        logging.info(f"Identified source categories: {set(source_categories.values())}")
        
        for feed_config in self.rss_feeds:
            items = self.fetch_feed_items(feed_config)
            feed_new_items = 0
            feed_old_items = 0
            feed_duplicates = 0
            
            for item in items:
                total_processed += 1
                
                # Filter 1: Only process items from the last 24 hours
                if item.published < cutoff_time:
                    feed_old_items += 1
                    total_filtered_old += 1
                    continue
                
                # Filter 2: Check if item already exists (by URL)
                if self.item_exists(item.link):
                    feed_duplicates += 1
                    total_duplicates += 1
                    continue
                
                # Filter 3: Check for duplicate titles
                if self.title_exists(item.title, feed_config['name']):
                    feed_duplicates += 1
                    total_duplicates += 1
                    logging.debug(f"Skipping duplicate title: {item.title[:50]}...")
                    continue
                
                # Process new item
                logging.info(f"Processing: {item.title[:60]}...")
                
                # Enhanced evaluation with OpenAI
                score, summary, category, sentiment, key_metrics, geographic_tags, sector_tags = self.evaluate_with_openai(item, source_categories)
                
                item.interest_score = score
                item.ai_summary = summary
                item.category = category
                item.sentiment = sentiment
                item.key_metrics = key_metrics
                item.geographic_tags = geographic_tags
                item.sector_tags = sector_tags
                
                # Save to database
                self.save_item(item)
                feed_new_items += 1
                total_new_items += 1
                
                # Rate limiting for OpenAI API
                time.sleep(1)
            
            logging.info(f"Feed '{feed_config['name']}': {feed_new_items} new, {feed_old_items} too old, {feed_duplicates} duplicates")
        
        logging.info(f"Feed processing completed:")
        logging.info(f"  NEW: {total_new_items} new items processed")
        logging.info(f"  OLD: {total_filtered_old} items filtered (older than 24h)")
        logging.info(f"  DUP: {total_duplicates} duplicates skipped")
        logging.info(f"  TOT: {total_processed} total items scanned")
        logging.info("=" * 60)
    
    def generate_executive_summary(self, items: List[Tuple]) -> ExecutiveSummary:
        """Generate executive summary from recent items"""
        if not items:
            return ExecutiveSummary(
                market_pulse_score=5.0,
                key_alerts=[],
                major_deals=[],
                regulatory_items=[],
                sentiment_overview="Neutral - No recent items",
                trending_topics=[]
            )
        
        # Calculate market pulse (average of high-priority items)
        high_priority_scores = [item[3] for item in items if item[3] >= 7]
        market_pulse = sum(high_priority_scores) / len(high_priority_scores) if high_priority_scores else 5.0
        
        # Extract key alerts (score 8+)
        key_alerts = [item[0] for item in items if item[3] >= 8][:3]
        
        # Extract major deals (look for dollar amounts)
        major_deals = []
        for item in items:
            if item[10]:  # key_metrics field
                metrics = item[10].split(',')
                for metric in metrics:
                    if '$' in metric and any(word in metric.lower() for word in ['million', 'billion', 'm', 'b']):
                        major_deals.append(f"{item[0][:50]}... ({metric.strip()})")
                        break
        
        # Extract regulatory items
        regulatory_items = [item[0] for item in items if 'Government & Policy' in (item[8] or '') or any(word in item[0].lower() for word in ['regulation', 'policy', 'law', 'government'])][:3]
        
        # Sentiment analysis
        sentiments = [item[9] for item in items if item[9]]
        positive_count = sentiments.count('Positive')
        negative_count = sentiments.count('Negative')
        
        if positive_count > negative_count:
            sentiment_overview = f"Cautiously Optimistic (+{positive_count - negative_count})"
        elif negative_count > positive_count:
            sentiment_overview = f"Cautious ({negative_count - positive_count} negative items)"
        else:
            sentiment_overview = "Mixed signals"
        
        # Trending topics (from categories)
        categories = [item[8] for item in items if item[8]]
        category_counts = Counter(categories)
        trending_topics = category_counts.most_common(5)
        
        return ExecutiveSummary(
            market_pulse_score=market_pulse,
            key_alerts=key_alerts,
            major_deals=major_deals[:3],
            regulatory_items=regulatory_items,
            sentiment_overview=sentiment_overview,
            trending_topics=trending_topics
        )
    
    def generate_executive_actions(self, items: List[Tuple]) -> List[Dict[str, str]]:
        """Generate specific executive actions based on actual news content"""
        try:
            # Get top 5 highest scoring items for action generation
            top_items = sorted(items, key=lambda x: x[3], reverse=True)[:5]
            
            if not top_items:
                return []
            
            # Prepare content summary for AI
            content_summary = []
            for item in top_items:
                title, _, _, score, summary, source, _, category = item[:8]
                
                # Parse the enhanced summary to get both parts
                if "||AUSTRALIAN_IMPACT||" in summary:
                    general_analysis, australian_impact = summary.split("||AUSTRALIAN_IMPACT||", 1)
                else:
                    general_analysis = summary
                    australian_impact = "Impact assessment pending"
                
                content_summary.append({
                    'title': title,
                    'score': score,
                    'category': category or 'General',
                    'source': source,
                    'analysis': general_analysis,
                    'australian_impact': australian_impact
                })
            
            prompt = f"""
You are generating executive action items for an Australian A-REIT CEO/COO based on TODAY'S specific news items. 

TODAY'S TOP NEWS ITEMS:
{self._format_items_for_ai_actions(content_summary)}

TASK: Generate exactly 3 specific, actionable executive items based ONLY on these actual news items. Each action should:
1. Be SPECIFIC to the actual news content (reference specific companies, policies, trends mentioned)
2. Be ACTIONABLE with clear next steps
3. Include WHY this action is important based on the Australian commercial property impact
4. Be executive-level (strategic decisions, briefings, assessments, communications)

RESPONSE FORMAT:
Action 1: [Specific action based on news item]
Why: [2-3 sentences explaining importance based on Australian property market implications from the news]

Action 2: [Specific action based on news item]
Why: [2-3 sentences explaining importance based on Australian property market implications from the news]

Action 3: [Specific action based on news item]
Why: [2-3 sentences explaining importance based on Australian property market implications from the news]

GUIDELINES:
- Reference specific companies, policies, technologies, or events mentioned in the news
- Connect actions directly to Australian commercial property market impact
- Be concrete about timeframes (this week, next quarter, etc.) when relevant
- Focus on strategic decisions that CEOs/COOs would actually make
- Don't create generic actions - they must be tied to the specific news content provided

EXAMPLES OF GOOD ACTIONS:
- "Schedule board discussion on [specific policy/event mentioned] and its impact on [specific portfolio segment]"
- "Assess acquisition opportunities in [specific sector/location] following [specific market development]"
- "Review technology investment strategy in light of [specific PropTech development mentioned]"

Create actions that a real A-REIT executive would put on their calendar this week based on these specific news items.
"""
            
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=800,
                temperature=0.7
            )
            
            content = response.choices[0].message.content.strip()
            
            # Parse the AI response into actions and explanations
            actions = []
            lines = content.split('\n')
            current_action = None
            current_why = None
            
            for line in lines:
                line = line.strip()
                if line.startswith('Action'):
                    if current_action and current_why:
                        actions.append({
                            'action': current_action,
                            'why': current_why
                        })
                    current_action = line.split(':', 1)[1].strip() if ':' in line else line
                    current_why = None
                elif line.startswith('Why:'):
                    current_why = line.split(':', 1)[1].strip() if ':' in line else line
                elif current_why is None and current_action and line and not line.startswith('Action'):  # Multi-line action
                    current_action += " " + line
                elif current_why and line and not line.startswith('Action'):  # Multi-line why
                    current_why += " " + line
            
            # Don't forget the last action
            if current_action and current_why:
                actions.append({
                    'action': current_action,
                    'why': current_why
                })
            
            return actions[:3]  # Ensure exactly 3 actions
            
        except Exception as e:
            logging.error(f"Error generating executive actions: {e}")
            return []
    
    def _format_items_for_ai_actions(self, content_summary: List[Dict]) -> str:
        """Format items for AI action generation prompt"""
        formatted = ""
        for i, item in enumerate(content_summary, 1):
            formatted += f"""
Item {i}: {item['title']} (Score: {item['score']}/10, Category: {item['category']})
Source: {item['source']}
Analysis: {item['analysis']}
Australian Property Impact: {item['australian_impact']}

"""
        return formatted

    def generate_social_content(self, items: List[Tuple]) -> Dict[str, List[str]]:
        """Generate social media content based on top items - Australian English"""
        try:
            # Get top 3 highest scoring items
            top_items = sorted(items, key=lambda x: x[3], reverse=True)[:3]
            
            if not top_items:
                return {"tweets": [], "linkedin": []}
            
            # Extract enhanced analysis for social content
            enhanced_summaries = []
            for item in top_items:
                title, _, _, score, summary, source = item[:6]
                
                # Parse the enhanced summary to get both parts
                if "||AUSTRALIAN_IMPACT||" in summary:
                    general_analysis, australian_impact = summary.split("||AUSTRALIAN_IMPACT||", 1)
                    enhanced_summaries.append({
                        'title': title,
                        'score': score,
                        'source': source,
                        'general': general_analysis,
                        'australian_impact': australian_impact
                    })
                else:
                    enhanced_summaries.append({
                        'title': title,
                        'score': score,
                        'source': source,
                        'general': summary,
                        'australian_impact': 'Australian impact assessment pending'
                    })
            
            prompt = f"""
Create social media content for an Australian A-REIT CEO based on today's top news items with enhanced analysis.

TOP NEWS ITEMS WITH ENHANCED ANALYSIS:

1. TITLE: {enhanced_summaries[0]['title']} (Score: {enhanced_summaries[0]['score']}/10)
   GENERAL ANALYSIS: {enhanced_summaries[0]['general']}
   AUSTRALIAN COMMERCIAL PROPERTY IMPACT: {enhanced_summaries[0]['australian_impact']}
   SOURCE: {enhanced_summaries[0]['source']}

2. TITLE: {enhanced_summaries[1]['title'] if len(enhanced_summaries) > 1 else 'N/A'} (Score: {enhanced_summaries[1]['score'] if len(enhanced_summaries) > 1 else 'N/A'}/10)
   GENERAL ANALYSIS: {enhanced_summaries[1]['general'] if len(enhanced_summaries) > 1 else 'N/A'}
   AUSTRALIAN COMMERCIAL PROPERTY IMPACT: {enhanced_summaries[1]['australian_impact'] if len(enhanced_summaries) > 1 else 'N/A'}
   SOURCE: {enhanced_summaries[1]['source'] if len(enhanced_summaries) > 1 else 'N/A'}

3. TITLE: {enhanced_summaries[2]['title'] if len(enhanced_summaries) > 2 else 'N/A'} (Score: {enhanced_summaries[2]['score'] if len(enhanced_summaries) > 2 else 'N/A'}/10)
   GENERAL ANALYSIS: {enhanced_summaries[2]['general'] if len(enhanced_summaries) > 2 else 'N/A'}
   AUSTRALIAN COMMERCIAL PROPERTY IMPACT: {enhanced_summaries[2]['australian_impact'] if len(enhanced_summaries) > 2 else 'N/A'}
   SOURCE: {enhanced_summaries[2]['source'] if len(enhanced_summaries) > 2 else 'N/A'}

REQUIREMENTS:
- Use AUSTRALIAN ENGLISH spelling (realise not realize, centre not center, analysed not analyzed, etc.)
- NO em-dashes (—) use regular hyphens (-) only
- Professional tone suitable for C-suite executive
- Focus on Australian commercial property market insights from the enhanced analysis
- Include relevant hashtags (#AREIT #CommercialProperty #AustralianProperty #PropertyInvestment)
- Leverage the Australian Commercial Property Impact insights to make content more targeted

Create:
1. THREE TWITTER/X POSTS (max 280 characters each)
2. THREE LINKEDIN POSTS (max 150 words each)

Make the content insights-driven, focusing on how these developments specifically affect Australian commercial property markets, A-REIT performance, or investment decisions.

Format your response exactly as:
TWEETS:
Tweet 1: [content]
Tweet 2: [content]  
Tweet 3: [content]

LINKEDIN:
Post 1: [content]
Post 2: [content]
Post 3: [content]

Keep content professional, insightful, and specifically relevant to Australian A-REIT executives and commercial property decision-makers.
"""
            
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1200,
                temperature=0.7
            )
            
            content = response.choices[0].message.content.strip()
            
            # Parse tweets and LinkedIn posts
            tweets = []
            linkedin_posts = []
            
            lines = content.split('\n')
            current_section = None
            
            for line in lines:
                line = line.strip()
                if line == "TWEETS:":
                    current_section = "tweets"
                elif line == "LINKEDIN:":
                    current_section = "linkedin"
                elif line.startswith("Tweet") and current_section == "tweets":
                    tweet_content = line.split(': ', 1)[1] if ': ' in line else line
                    # Remove any em-dashes that might have slipped through
                    tweet_content = tweet_content.replace('—', '-').replace('–', '-')
                    tweets.append(tweet_content)
                elif line.startswith("Post") and current_section == "linkedin":
                    post_content = line.split(': ', 1)[1] if ': ' in line else line
                    # Remove any em-dashes that might have slipped through
                    post_content = post_content.replace('—', '-').replace('–', '-')
                    linkedin_posts.append(post_content)
            
            return {"tweets": tweets[:3], "linkedin": linkedin_posts[:3]}
            
        except Exception as e:
            logging.error(f"Error generating social content: {e}")
            return {"tweets": [], "linkedin": []}
    
    def get_pulse_class(self, score: float) -> str:
        """Get CSS class for market pulse score"""
        if score >= 7.5:
            return "pulse-negative"  # High alert - red
        elif score >= 6.0:
            return "pulse-warning"  # Caution - orange
        else:
            return "pulse-positive"  # Stable - green
    
    def get_pulse_color(self, score: float) -> str:
        """Get inline color for market pulse score"""
        if score >= 7.5:
            return "#dc2626"  # Red - High alert
        elif score >= 6.0:
            return "#d97706"  # Orange - Caution
        else:
            return "#059669"  # Green - Stable
    
    def format_datetime(self, dt_string: str) -> str:
        """Format datetime string for display"""
        try:
            if isinstance(dt_string, str):
                dt = datetime.fromisoformat(dt_string.replace('Z', '+00:00'))
            else:
                dt = dt_string
            return dt.strftime('%H:%M')
        except:
            return "N/A"
    
    def clean_description(self, description: str, max_length: int = 200) -> str:
        """Clean and truncate description text"""
        if not description:
            return ""
        
        # Remove HTML tags
        clean_desc = re.sub('<[^<]+?>', '', description)
        clean_desc = re.sub(r'\s+', ' ', clean_desc).strip()
        
        if len(clean_desc) > max_length:
            clean_desc = clean_desc[:max_length] + "..."
        
        return clean_desc
    
    def format_beehiiv_article_card(self, item: Tuple) -> str:
        """Format individual article card in beautiful beehiiv style"""
        title, link, description, score, summary, source, processed_at, category, sentiment, key_metrics, geo_tags, sector_tags = item
        
        # Priority styling
        if score >= 8:
            border_color = "#dc2626"
            score_bg = "#fef2f2"
            score_color = "#dc2626"
            priority_badge = "CRITICAL"
        elif score >= 6:
            border_color = "#d97706"
            score_bg = "#fffbeb" 
            score_color = "#d97706"
            priority_badge = "HIGH"
        else:
            border_color = "#059669"
            score_bg = "#f0fdf4"
            score_color = "#059669"
            priority_badge = "MEDIUM"
        
        # Sentiment styling
        if sentiment == "Positive":
            sentiment_bg = "#f0fdf4"
            sentiment_color = "#059669"
        elif sentiment == "Negative":
            sentiment_bg = "#fef2f2"
            sentiment_color = "#dc2626"
        else:
            sentiment_bg = "#f8fafc"
            sentiment_color = "#64748b"
        
        # Parse combined summary
        if "||AUSTRALIAN_IMPACT||" in summary:
            general_analysis, australian_impact = summary.split("||AUSTRALIAN_IMPACT||", 1)
        else:
            general_analysis = summary
            australian_impact = "Australian commercial property impact assessment pending."
        
        # Clean description
        clean_description = self.clean_description(description, 150)
        
        # Format tags
        tags_html = ""
        if (geo_tags and geo_tags.strip()) or (sector_tags and sector_tags.strip()):
            tags_html = '<div style="margin: 16px 0; display: flex; flex-wrap: wrap; gap: 6px;">'
            if geo_tags and geo_tags.strip():
                for tag in geo_tags.split(',')[:3]:  # Limit to 3 tags
                    if tag.strip():
                        tags_html += f'<span style="background: #f0fdf4; border: 1px solid #bbf7d0; color: #059669; padding: 4px 8px; border-radius: 12px; font-size: 10px; font-weight: 500;">📍 {tag.strip()}</span>'
            if sector_tags and sector_tags.strip():
                for tag in sector_tags.split(',')[:2]:  # Limit to 2 tags
                    if tag.strip():
                        tags_html += f'<span style="background: #fffbeb; border: 1px solid #fed7aa; color: #d97706; padding: 4px 8px; border-radius: 12px; font-size: 10px; font-weight: 500;">🏗️ {tag.strip()}</span>'
            tags_html += '</div>'
        
        # Format metrics
        metrics_html = ""
        if key_metrics and key_metrics.strip():
            metrics_html = f'''
                <div style="background: #eff6ff; border: 1px solid #dbeafe; border-radius: 8px; padding: 12px; margin: 16px 0; font-family: monospace; font-size: 12px; color: #1e40af;">
                    <strong>📊 Key Metrics:</strong> {key_metrics}
                </div>
            '''
        
        return f'''
            <div style="background: white; border: 1px solid #f1f5f9; border-left: 4px solid {border_color}; border-radius: 12px; margin-bottom: 24px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.06); transition: all 0.3s ease;">
                
                <!-- Article Header -->
                <div style="padding: 24px 24px 0 24px;">
                    <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 12px;">
                        <h3 style="margin: 0; font-size: 19px; font-weight: 600; line-height: 1.3; flex: 1; color: #1e293b;">
                            <a href="{link}" target="_blank" style="color: #1e293b; text-decoration: none; hover: color: #3b82f6;">{title}</a>
                        </h3>
                        <div style="margin-left: 16px; display: flex; gap: 8px; flex-shrink: 0;">
                            <span style="background: {score_bg}; color: {score_color}; padding: 4px 10px; border-radius: 16px; font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px;">{priority_badge}</span>
                            <span style="background: {score_bg}; color: {score_color}; padding: 4px 10px; border-radius: 16px; font-size: 10px; font-weight: 600;">{score}/10</span>
                        </div>
                    </div>
                    
                    <!-- Source and sentiment -->
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
                        <span style="font-size: 12px; color: #64748b; font-weight: 500;">📰 {source}</span>
                        <span style="background: {sentiment_bg}; color: {sentiment_color}; padding: 3px 8px; border-radius: 12px; font-size: 10px; font-weight: 600;">{sentiment or 'Neutral'}</span>
                    </div>
                    
                    <!-- Description if available -->
                    {f'<p style="margin: 0 0 16px 0; font-size: 14px; line-height: 1.5; color: #475569;">{clean_description}</p>' if clean_description else ''}
                </div>
                
                <!-- AI Analysis -->
                <div style="padding: 0 24px;">
                    <div style="background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%); border: 1px solid #e2e8f0; border-radius: 8px; padding: 16px; margin-bottom: 16px;">
                        <div style="font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; color: #64748b; font-weight: 600; margin-bottom: 8px; display: flex; align-items: center;">
                            <span style="margin-right: 6px;">🤖</span> AI ANALYSIS
                        </div>
                        <div style="color: #374151; font-size: 14px; line-height: 1.5;">{general_analysis}</div>
                    </div>
                    
                    <!-- Australian Impact -->
                    <div style="background: linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%); border: 1px solid #bfdbfe; border-radius: 8px; padding: 16px; margin-bottom: 16px;">
                        <div style="font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; color: #1e40af; font-weight: 600; margin-bottom: 8px; display: flex; align-items: center;">
                            <span style="margin-right: 6px;">🇦🇺</span> AUSTRALIAN PROPERTY IMPACT
                        </div>
                        <div style="color: #1e40af; font-size: 14px; line-height: 1.5; font-weight: 500;">{australian_impact}</div>
                    </div>
                    
                    {metrics_html}
                    {tags_html}
                </div>
                
                <!-- Footer -->
                <div style="padding: 16px 24px; border-top: 1px solid #f1f5f9; background: #fafbfc; display: flex; justify-content: space-between; align-items: center;">
                    <span style="font-size: 11px; color: #64748b; font-weight: 500;">Source: {source}</span>
                    <span style="font-size: 11px; color: #64748b;">{self.format_datetime(processed_at)}</span>
                </div>
            </div>
        '''
    
    def generate_daily_email(self, include_all: bool = False) -> Optional[str]:
        """Generate beautiful, beehiiv-style HTML email content for the last 24 hours"""
        yesterday = datetime.now() - timedelta(days=1)
        
        cursor = self.conn.execute('''
            SELECT title, link, description, interest_score, ai_summary, source_name, 
                   processed_at, category, sentiment, key_metrics, geographic_tags, sector_tags
            FROM items 
            WHERE processed_at >= ? AND email_sent = FALSE
            ORDER BY interest_score DESC, processed_at DESC
        ''', (yesterday,))
        
        all_items = cursor.fetchall()
        
        if include_all:
            # Include everything mode - minimal filtering
            items = []
            for item in all_items:
                title, link, description, score, summary, source_name = item[:6]
                
                # Only exclude obvious RSS errors
                if any(phrase in title.lower() for phrase in [
                    'not found', 'sign up to rss.app', 'error 404', 'access denied'
                ]):
                    logging.info(f"Filtering out RSS error: {title[:50]}...")
                    continue
                
                # Include everything else, even low scores and short content
                items.append(item)
            
            logging.info(f"INCLUDE ALL MODE: Using {len(items)} out of {len(all_items)} total items (minimal filtering)")
        else:
            # Normal filtering mode
            items = []
            for item in all_items:
                title, link, description, score, summary, source_name = item[:6]
                
                # Exclude RSS error messages and placeholder content
                if any(phrase in title.lower() for phrase in [
                    'not found', 'sign up to rss.app', 'error', 'placeholder', 
                    'feed not available', 'access denied'
                ]):
                    logging.info(f"Filtering out RSS error: {title[:50]}...")
                    continue
                
                # Be more inclusive for technology content - lower threshold for tech items
                title_lower = title.lower()
                description_lower = description.lower()
                is_tech_related = any(tech_word in title_lower or tech_word in description_lower for tech_word in [
                    'artificial intelligence', 'ai', 'machine learning', 'blockchain', 'proptech',
                    'technology', 'digital', 'automation', 'software', 'platform', 'data',
                    'analytics', 'cloud', 'iot', 'internet of things', 'smart building', 'fintech',
                    'innovation', 'startup', 'tech', 'app', 'virtual', 'augmented reality',
                    'cybersecurity', 'cryptocurrency', 'bitcoin', 'metaverse', 'robotics'
                ])
                
                # Lower threshold for technology items (score 3+) vs general items (score 5+)
                min_score = 3 if is_tech_related else 5
                
                if score < min_score:
                    if is_tech_related:
                        logging.info(f"Including tech item with lower score ({score}): {title[:50]}...")
                    else:
                        logging.info(f"Filtering out low-interest item (score {score}): {title[:50]}...")
                        continue
                
                # Exclude items with no meaningful content (but be more lenient for tech)
                min_content_length = 30 if is_tech_related else 50
                if len(description.strip()) < min_content_length and len(summary.strip()) < min_content_length:
                    logging.info(f"Filtering out low-content item: {title[:50]}...")
                    continue
                
                items.append(item)
            
            logging.info(f"NORMAL MODE: Filtered {len(all_items)} items down to {len(items)} relevant items for email")
        
        if not items:
            logging.info("No relevant items found for daily email after filtering")
            return None
        
        # Generate executive summary
        exec_summary = self.generate_executive_summary(items)
        
        # Generate executive actions based on actual news content
        executive_actions = self.generate_executive_actions(items)
        
        # Generate social media content
        social_content = self.generate_social_content(items)
        
        # Categorize items
        categories = {
            'Market Movers': [],
            'A-REIT Specific': [],
            'Opportunities': [],
            'Risks': [],
            'Government & Policy': [],
            'Social Intelligence': [],
            'Technology Impact': [],
            'Global Trends': [],
            'Other': []
        }
        
        for item in items:
            category = item[7] or 'Other'
            if category in categories:
                categories[category].append(item)
            else:
                categories['Other'].append(item)
        
        # Remove empty categories
        categories = {k: v for k, v in categories.items() if v}
        
        # Generate beehiiv-style HTML email
        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Matt's Memo • {datetime.now().strftime('%B %d, %Y')}</title>
    <!--[if mso]>
    <noscript>
        <xml>
            <o:OfficeDocumentSettings>
                <o:AllowPNG/>
                <o:PixelsPerInch>96</o:PixelsPerInch>
            </o:OfficeDocumentSettings>
        </xml>
    </noscript>
    <![endif]-->
</head>
<body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #f8fafc; line-height: 1.6;">
    <div style="width: 100%; max-width: 680px; margin: 0 auto; background-color: #ffffff;">
        
        <!-- Header -->
        <div style="background: linear-gradient(135deg, #1e40af 0%, #3b82f6 100%); text-align: center; padding: 48px 32px; color: white;">
            <h1 style="margin: 0 0 8px 0; font-size: 36px; font-weight: 700; letter-spacing: -0.025em;">Matt's Memo</h1>
            <p style="margin: 0 0 16px 0; font-size: 18px; opacity: 0.9; font-weight: 400;">Strategic Intelligence for Real Estate Leaders</p>
            <p style="margin: 0; font-size: 14px; opacity: 0.8;">{datetime.now().strftime('%A, %B %d, %Y')} • {len(items)} insights today</p>
        </div>

        <!-- Executive Dashboard -->
        <div style="background: #f8fafc; padding: 40px 32px; border-bottom: 1px solid #e2e8f0;">
            <h2 style="margin: 0 0 24px 0; font-size: 20px; font-weight: 600; color: #1e293b; text-align: center;">Executive Dashboard</h2>
            
            <table role="presentation" cellspacing="0" cellpadding="0" border="0" style="width: 100%;">
                <tr>
                    <td style="width: 25%; padding: 0 8px;">
                        <div style="background: white; padding: 24px; border-radius: 12px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid #f1f5f9;">
                            <div style="font-size: 12px; font-weight: 500; color: #64748b; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px;">Market Pulse</div>
                            <div style="font-size: 28px; font-weight: 700; margin-bottom: 4px; color: {self.get_pulse_color(exec_summary.market_pulse_score)};">{exec_summary.market_pulse_score:.1f}/10</div>
                            <div style="font-size: 12px; color: #64748b;">{exec_summary.sentiment_overview}</div>
                        </div>
                    </td>
                    <td style="width: 25%; padding: 0 8px;">
                        <div style="background: white; padding: 24px; border-radius: 12px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid #f1f5f9;">
                            <div style="font-size: 12px; font-weight: 500; color: #64748b; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px;">Intelligence</div>
                            <div style="font-size: 28px; font-weight: 700; margin-bottom: 4px; color: #1e293b;">{len(items)}</div>
                            <div style="font-size: 12px; color: #64748b;">items analysed</div>
                        </div>
                    </td>
                    <td style="width: 25%; padding: 0 8px;">
                        <div style="background: white; padding: 24px; border-radius: 12px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid #f1f5f9;">
                            <div style="font-size: 12px; font-weight: 500; color: #64748b; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px;">Priority</div>
                            <div style="font-size: 28px; font-weight: 700; margin-bottom: 4px; color: #dc2626;">{len([i for i in items if i[3] >= 8])}</div>
                            <div style="font-size: 12px; color: #64748b;">high alerts</div>
                        </div>
                    </td>
                    <td style="width: 25%; padding: 0 8px;">
                        <div style="background: white; padding: 24px; border-radius: 12px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid #f1f5f9;">
                            <div style="font-size: 12px; font-weight: 500; color: #64748b; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px;">Trending</div>
                            <div style="font-size: 16px; font-weight: 600; margin-bottom: 4px; color: #1e293b;">{exec_summary.trending_topics[0][0] if exec_summary.trending_topics else 'Mixed'}</div>
                            <div style="font-size: 12px; color: #64748b;">{exec_summary.trending_topics[0][1] if exec_summary.trending_topics else '0'} mentions</div>
                        </div>
                    </td>
                </tr>
            </table>
        </div>
"""

        # Add category sections with consistent styling
        for category_name, category_items in categories.items():
            if not category_items:
                continue
                
            html_content += f"""
        <!-- {category_name} Section -->
        <div style="padding: 40px 32px; border-bottom: 1px solid #f1f5f9;">
            <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 32px;">
                <h2 style="margin: 0; font-size: 24px; font-weight: 600; color: #1e293b; display: flex; align-items: center;">
                    <span style="width: 4px; height: 24px; background: #3b82f6; border-radius: 2px; margin-right: 12px;"></span>
                    {category_name}
                </h2>
                <span style="background: #f1f5f9; color: #64748b; padding: 6px 14px; border-radius: 20px; font-size: 12px; font-weight: 500;">{len(category_items)} items</span>
            </div>
"""
            
            for item in category_items:
                html_content += self.format_beehiiv_article_card(item)
            
            html_content += "</div>"

        # Action Items Section
        html_content += f"""
        <!-- Executive Actions -->
        <div style="background: linear-gradient(135deg, #1e293b 0%, #334155 100%); padding: 48px 32px; color: white;">
            <h2 style="margin: 0 0 8px 0; font-size: 28px; font-weight: 600; text-align: center;">Executive Action Items</h2>
            <p style="margin: 0 0 32px 0; font-size: 16px; text-align: center; opacity: 0.8;">Strategic priorities based on today's intelligence</p>
            
            <div style="background: rgba(255,255,255,0.1); border: 1px solid rgba(255,255,255,0.15); border-radius: 12px; padding: 32px;">
"""
        
        # Add dynamic executive actions or fallback
        if executive_actions:
            for i, action_item in enumerate(executive_actions, 1):
                border_style = "border-bottom: 1px solid rgba(255,255,255,0.1);" if i < len(executive_actions) else ""
                margin_style = "margin-bottom: 20px; padding-bottom: 20px;" if i < len(executive_actions) else "margin-bottom: 0;"
                
                html_content += f"""
                <div style="{margin_style} {border_style}">
                    <div style="display: flex; align-items: flex-start; margin-bottom: 8px;">
                        <span style="background: #3b82f6; color: white; width: 24px; height: 24px; border-radius: 50%; display: inline-flex; align-items: center; justify-content: center; font-weight: 600; font-size: 12px; margin-right: 12px; flex-shrink: 0;">{i}</span>
                        <span style="font-size: 16px; font-weight: 500; line-height: 1.4;">{action_item['action']}</span>
                    </div>
                    <div style="margin-left: 36px; font-size: 14px; opacity: 0.9; line-height: 1.5; font-style: italic;">{action_item['why']}</div>
                </div>
"""
        else:
            # Fallback actions if AI generation fails
            fallback_actions = [
                {
                    'action': 'Review quarterly portfolio performance against current market conditions',
                    'why': 'Current market volatility requires immediate assessment of portfolio resilience and positioning adjustments.'
                },
                {
                    'action': 'Assess technology investment opportunities in PropTech and building automation',
                    'why': 'Emerging technologies are creating competitive advantages in operational efficiency and tenant experience.'
                },
                {
                    'action': 'Monitor competitor announcements and market positioning strategies',
                    'why': 'Competitive intelligence is crucial for maintaining market position and identifying strategic opportunities.'
                }
            ]
            
            for i, action_item in enumerate(fallback_actions, 1):
                border_style = "border-bottom: 1px solid rgba(255,255,255,0.1);" if i < len(fallback_actions) else ""
                margin_style = "margin-bottom: 20px; padding-bottom: 20px;" if i < len(fallback_actions) else "margin-bottom: 0;"
                
                html_content += f"""
                <div style="{margin_style} {border_style}">
                    <div style="display: flex; align-items: flex-start; margin-bottom: 8px;">
                        <span style="background: #3b82f6; color: white; width: 24px; height: 24px; border-radius: 50%; display: inline-flex; align-items: center; justify-content: center; font-weight: 600; font-size: 12px; margin-right: 12px; flex-shrink: 0;">{i}</span>
                        <span style="font-size: 16px; font-weight: 500; line-height: 1.4;">{action_item['action']}</span>
                    </div>
                    <div style="margin-left: 36px; font-size: 14px; opacity: 0.9; line-height: 1.5; font-style: italic;">{action_item['why']}</div>
                </div>
"""
        
        html_content += """
            </div>
        </div>
"""

        # Social Content Section
        if social_content.get('tweets') or social_content.get('linkedin'):
            html_content += f"""
        <!-- Social Content -->
        <div style="background: #374151; padding: 48px 32px; color: white;">
            <h2 style="margin: 0 0 8px 0; font-size: 28px; font-weight: 600; text-align: center;">Ready-to-Share Content</h2>
            <p style="margin: 0 0 32px 0; font-size: 16px; text-align: center; opacity: 0.8;">Professional social media content based on today's insights</p>
            
            <table role="presentation" cellspacing="0" cellpadding="0" border="0" style="width: 100%;">
                <tr>
                    <td style="width: 50%; padding-right: 16px; vertical-align: top;">
                        <div style="background: rgba(255,255,255,0.1); border: 1px solid rgba(255,255,255,0.15); border-radius: 12px; padding: 24px;">
                            <h3 style="margin: 0 0 20px 0; font-size: 18px; font-weight: 600;">Twitter/X Posts</h3>
"""
            
            tweets = social_content.get('tweets', [])
            for i, tweet in enumerate(tweets, 1):
                html_content += f"""
                            <div style="background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1); border-radius: 8px; padding: 16px; margin-bottom: 16px;">
                                <div style="font-size: 11px; font-weight: 600; color: #93c5fd; margin-bottom: 8px; text-transform: uppercase;">Tweet {i}</div>
                                <div style="font-size: 14px; line-height: 1.4;">{tweet}</div>
                            </div>
"""
            
            if not tweets:
                html_content += """
                            <div style="background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1); border-radius: 8px; padding: 16px; font-size: 14px; opacity: 0.7;">No tweets generated - insufficient content</div>
"""
            
            html_content += """
                        </div>
                    </td>
                    <td style="width: 50%; padding-left: 16px; vertical-align: top;">
                        <div style="background: rgba(255,255,255,0.1); border: 1px solid rgba(255,255,255,0.15); border-radius: 12px; padding: 24px;">
                            <h3 style="margin: 0 0 20px 0; font-size: 18px; font-weight: 600;">LinkedIn Posts</h3>
"""
            
            linkedin_posts = social_content.get('linkedin', [])
            for i, post in enumerate(linkedin_posts, 1):
                html_content += f"""
                            <div style="background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1); border-radius: 8px; padding: 16px; margin-bottom: 16px;">
                                <div style="font-size: 11px; font-weight: 600; color: #93c5fd; margin-bottom: 8px; text-transform: uppercase;">Post {i}</div>
                                <div style="font-size: 14px; line-height: 1.4;">{post}</div>
                            </div>
"""
            
            if not linkedin_posts:
                html_content += """
                            <div style="background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1); border-radius: 8px; padding: 16px; font-size: 14px; opacity: 0.7;">No LinkedIn posts generated - insufficient content</div>
"""
            
            html_content += """
                        </div>
                    </td>
                </tr>
            </table>
        </div>
"""

        # Footer
        html_content += f"""
        <!-- Footer -->
        <div style="background: #1e293b; padding: 40px 32px; text-align: center; color: white;">
            <h3 style="margin: 0 0 12px 0; font-size: 20px; font-weight: 600;">Matt's Memo</h3>
            <p style="margin: 0 0 8px 0; font-size: 16px; opacity: 0.8;">Strategic Intelligence for Real Estate Leaders</p>
            <p style="margin: 0 0 16px 0; font-size: 14px; opacity: 0.7;">Powered by AI • Real-time insights • Executive focused</p>
            <a href="https://www.linkedin.com/in/mattwhiteoak" style="color: #60a5fa; text-decoration: none; font-size: 14px; font-weight: 500;">Connect with Matt on LinkedIn →</a>
            
            <div style="margin-top: 32px; padding-top: 24px; border-top: 1px solid rgba(255,255,255,0.1); font-size: 12px; opacity: 0.6; line-height: 1.5;">
                This briefing contains AI-generated analysis based on publicly available information. All AI commentary is clearly labeled and should be considered alongside original source material.
            </div>
        </div>
    </div>
</body>
</html>"""
        
        return html_content
    
    def send_email(self, content: str):
        """Send email via Gmail SMTP with proper HTML formatting"""
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"Matt's Memo • {datetime.now().strftime('%B %d, %Y')}"
            msg['From'] = self.config['gmail_user']
            msg['To'] = self.config['recipient_email']
            
            # Create both plain text and HTML versions
            # Extract plain text from HTML for fallback
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
            
            # Mark items as sent
            yesterday = datetime.now() - timedelta(days=1)
            self.conn.execute('''
                UPDATE items SET email_sent = TRUE 
                WHERE processed_at >= ? AND email_sent = FALSE
            ''', (yesterday,))
            self.conn.commit()
            
            logging.info("Daily email sent successfully")
            
        except Exception as e:
            logging.error(f"Error sending email: {e}")
            raise
    
    def send_daily_brief(self, include_all: bool = False):
        """Generate and send daily email brief"""
        try:
            if include_all:
                logging.info("Generating FULL daily email brief (minimal filtering)...")
            else:
                logging.info("Generating daily email brief...")
            content = self.generate_daily_email(include_all=include_all)
            if content:
                self.send_email(content)
                logging.info("Daily brief sent successfully")
            else:
                logging.info("No new items for daily brief")
        except Exception as e:
            logging.error(f"Error in send_daily_brief: {e}")
    
    def preview_items(self):
        """Preview what items would be included in the email without sending"""
        yesterday = datetime.now() - timedelta(days=1)
        
        cursor = self.conn.execute('''
            SELECT title, link, description, interest_score, ai_summary, source_name, 
                   processed_at, category, sentiment, key_metrics, geographic_tags, sector_tags
            FROM items 
            WHERE processed_at >= ? AND email_sent = FALSE
            ORDER BY interest_score DESC, processed_at DESC
        ''', (yesterday,))
        
        all_items = cursor.fetchall()
        
        print(f"\n📊 FEED PREVIEW - Items from last 24 hours")
        print("=" * 80)
        print(f"Total items in database: {len(all_items)}")
        
        if not all_items:
            print("❌ No items found from the last 24 hours")
            return
        
        # Show filtering breakdown
        rss_errors = 0
        low_score_items = 0
        low_content_items = 0
        included_items = 0
        
        print(f"\n📋 ITEM BREAKDOWN:")
        print("-" * 40)
        
        for i, item in enumerate(all_items, 1):
            title, link, description, score, summary, source_name = item[:6]
            
            # Check RSS errors
            if any(phrase in title.lower() for phrase in [
                'not found', 'sign up to rss.app', 'error', 'placeholder', 
                'feed not available', 'access denied'
            ]):
                rss_errors += 1
                print(f"🚫 RSS ERROR: {title[:60]}... (Score: {score})")
                continue
            
            # Check tech content
            title_lower = title.lower()
            description_lower = description.lower()
            is_tech_related = any(tech_word in title_lower or tech_word in description_lower for tech_word in [
                'artificial intelligence', 'ai', 'machine learning', 'blockchain', 'proptech',
                'technology', 'digital', 'automation', 'software', 'platform', 'data',
                'analytics', 'cloud', 'iot', 'internet of things', 'smart building', 'fintech',
                'innovation', 'startup', 'tech', 'app', 'virtual', 'augmented reality',
                'cybersecurity', 'cryptocurrency', 'bitcoin', 'metaverse', 'robotics'
            ])
            
            min_score = 3 if is_tech_related else 5
            
            # Check score
            if score < min_score:
                low_score_items += 1
                tech_tag = " [TECH]" if is_tech_related else ""
                print(f"📉 LOW SCORE{tech_tag}: {title[:60]}... (Score: {score}, Min: {min_score})")
                continue
            
            # Check content length
            min_content_length = 30 if is_tech_related else 50
            if len(description.strip()) < min_content_length and len(summary.strip()) < min_content_length:
                low_content_items += 1
                print(f"📝 LOW CONTENT: {title[:60]}... (Score: {score})")
                continue
            
            # Would be included
            included_items += 1
            tech_tag = " [TECH]" if is_tech_related else ""
            priority = "🔴" if score >= 8 else "🟡" if score >= 6 else "🟢"
            print(f"{priority} INCLUDED{tech_tag}: {title[:60]}... (Score: {score}, Source: {source_name})")
        
        print(f"\n📈 SUMMARY:")
        print("-" * 40)
        print(f"✅ Would be included in email: {included_items}")
        print(f"🚫 RSS errors filtered out: {rss_errors}")
        print(f"📉 Low score filtered out: {low_score_items}")
        print(f"📝 Low content filtered out: {low_content_items}")
        print(f"📊 Total scanned: {len(all_items)}")
        
        if included_items > 0:
            print(f"\n💡 Run 'python rss_analyzer.py email' to send with these {included_items} items")
            print(f"💡 Run 'python rss_analyzer.py email-full' to include ALL {len(all_items)} items")
        else:
            print(f"\n⚠️  No items would be included with current filtering")
            print(f"💡 Run 'python rss_analyzer.py email-full' to include all {len(all_items)} items anyway")
    
    def run_scheduler(self):
        """Run the scheduled tasks"""
        # Schedule feed processing every 2 hours
        schedule.every(2).hours.do(self.process_feeds)
        
        # Schedule daily email at 6 AM
        schedule.every().day.at("06:00").do(self.send_daily_brief)
        
        # Schedule weekly cleanup at 2 AM on Sundays
        schedule.every().sunday.at("02:00").do(lambda: self.cleanup_old_items(days_to_keep=30))
        
        logging.info("RSS Analyzer started successfully!")
        logging.info("Scheduled tasks:")
        logging.info("   - Feed processing: Every 2 hours (last 24h items only)")
        logging.info("   - Daily email: 6:00 AM")
        logging.info("   - Database cleanup: Sundays 2:00 AM (keep 30 days)")
        logging.info(f"Monitoring {len(self.rss_feeds)} RSS feeds")
        logging.info("Duplicate detection: URL + Title matching enabled")
        
        # Run initial feed processing
        self.process_feeds()
        
        # Main scheduler loop
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute

    def cleanup_old_items(self, days_to_keep=30):
        """Clean up old items from database"""
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        cursor = self.conn.execute('DELETE FROM items WHERE processed_at < ?', (cutoff_date,))
        deleted_count = cursor.rowcount
        self.conn.commit()
        logging.info(f"Cleaned up {deleted_count} items older than {days_to_keep} days")

if __name__ == "__main__":
    import sys
    
    try:
        analyzer = RSSAnalyzer()
        
        # Check for command line arguments
        if len(sys.argv) > 1:
            command = sys.argv[1].lower()
            
            if command == "test":
                # Test mode - process feeds once and exit
                logging.info("Running in TEST mode - processing feeds once...")
                analyzer.process_feeds()
                logging.info("Test completed. Check rss_analyzer.log for details.")
                
            elif command == "email":
                # Send test email with normal filtering
                logging.info("Generating and sending test email (normal filtering)...")
                analyzer.send_daily_brief(include_all=False)
                logging.info("Email test completed.")
                
            elif command == "email-full":
                # Send test email with minimal filtering (include everything)
                logging.info("Generating and sending FULL test email (minimal filtering)...")
                analyzer.send_daily_brief(include_all=True)
                logging.info("Full email test completed.")
                
            elif command == "preview":
                # Preview what would be included in email without sending
                analyzer.preview_items()
                
            elif command == "cleanup":
                # Clean up old items
                days = 30
                if len(sys.argv) > 2:
                    try:
                        days = int(sys.argv[2])
                    except ValueError:
                        pass
                logging.info(f"Cleaning up items older than {days} days...")
                analyzer.cleanup_old_items(days_to_keep=days)
                logging.info("Cleanup completed.")
                
            elif command == "stats":
                # Show database statistics
                cursor = analyzer.conn.execute("SELECT COUNT(*) FROM items")
                total_items = cursor.fetchone()[0]
                
                cursor = analyzer.conn.execute("SELECT COUNT(*) FROM items WHERE processed_at >= ?", 
                                             (datetime.now() - timedelta(days=1),))
                last_24h = cursor.fetchone()[0]
                
                cursor = analyzer.conn.execute("SELECT COUNT(*) FROM items WHERE email_sent = FALSE")
                pending_email = cursor.fetchone()[0]
                
                cursor = analyzer.conn.execute("SELECT AVG(interest_score) FROM items WHERE interest_score IS NOT NULL")
                avg_score = cursor.fetchone()[0] or 0
                
                print(f"Database Statistics:")
                print(f"   Total items: {total_items}")
                print(f"   Last 24 hours: {last_24h}")
                print(f"   Pending email: {pending_email}")
                print(f"   Average interest score: {avg_score:.1f}/10")
                
            else:
                print("Available commands:")
                print("  python3 rss_analyzer.py              - Run normally (scheduled)")
                print("  python3 rss_analyzer.py test         - Test feed processing once")
                print("  python3 rss_analyzer.py preview      - Preview email content without sending")
                print("  python3 rss_analyzer.py email        - Send test email (normal filtering)")
                print("  python3 rss_analyzer.py email-full   - Send test email (include everything)")
                print("  python3 rss_analyzer.py cleanup [days] - Clean old items (default: 30 days)")
                print("  python3 rss_analyzer.py stats        - Show database statistics")
        else:
            # Normal mode - run scheduler
            analyzer.run_scheduler()
            
    except KeyboardInterrupt:
        logging.info("RSS Analyzer stopped by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise