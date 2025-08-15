#!/usr/bin/env python3
"""
Enhanced Beehiiv Feed Writer with 100x More Engaging Content
- Maximum engagement newsletter summary
- Priority-based content organization
- Rich HTML formatting with psychological triggers
- Enhanced visual presentation
"""

import os, sqlite3, html, re, logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple

RSS_DB_PATH = os.getenv("RSS_DB_PATH", "rss_items.db")
OUT_PATH = os.getenv("OUT_PATH", "beehiiv.xml")
NEWSLETTER = os.getenv("NEWSLETTER_NAME", "Morning Briefing")
BASE_URL = os.getenv("BASE_URL", "https://mwhiteoak.github.io/morningBrief")
SCAN_WINDOW_HRS = int(os.getenv("SCAN_WINDOW_HRS", "24"))
TZ = timezone(timedelta(hours=10))

logging.basicConfig(level=logging.INFO)

def esc(t: str) -> str:
    return html.escape(t or "", quote=True)

def cdata(s: str) -> str:
    return "<![CDATA[" + (s or "").replace("]]>", "]]]]><![CDATA[>") + "]]>"

def get_newsletter_metadata(conn: sqlite3.Connection) -> Tuple[str, str, float]:
    """Get latest headline, subhead, and money tracked"""
    try:
        cur = conn.execute("""
            SELECT headline, subhead, total_money_tracked 
            FROM newsletter_metadata 
            ORDER BY created_at DESC LIMIT 1
        """)
        result = cur.fetchone()
        if result:
            return (
                result[0] or "🚨 Property Intelligence Alert",
                result[1] or "Exclusive deals and insider moves",
                result[2] or 0.0
            )
    except sqlite3.OperationalError:
        pass
    
    return "🚨 Property Intelligence Alert", "Exclusive deals and insider moves", 0.0

def calculate_engagement_score(item: Dict[str, Any]) -> int:
    """Calculate engagement potential"""
    title = (item.get('title') or '').lower()
    desc = (item.get('desc') or '').lower()
    
    score = 0
    
    # Money = engagement
    if re.search(r'\$[\d,.]+ ?billion', desc, re.IGNORECASE):
        score += 15
    elif re.search(r'\$[\d,.]+ ?million', desc, re.IGNORECASE):
        score += 8
    elif re.search(r'\$[\d,.]+', desc, re.IGNORECASE):
        score += 3
    
    # Big names = authority
    major_players = [
        'blackstone', 'brookfield', 'charter hall', 'goodman', 'gpt', 
        'mirvac', 'stockland', 'westfield', 'lendlease', 'dexus', 'vicinity'
    ]
    for player in major_players:
        if player in desc:
            score += 5
    
    # Urgent keywords = clicks
    urgent_words = [
        'breaking', 'exclusive', 'secret', 'leaked', 'emergency', 'crisis',
        'surge', 'plunge', 'crash', 'soar', 'alert', 'warning'
    ]
    for word in urgent_words:
        if word in title or word in desc:
            score += 6
    
    # Transaction words = relevance
    transaction_words = [
        'acquires', 'sells', 'buys', 'divests', 'merger', 'takeover', 
        'transaction', 'deal', 'purchase', 'sale'
    ]
    for word in transaction_words:
        if word in desc:
            score += 4
    
    # Urgency score from AI
    score += item.get('urgency_score', 0)
    
    return score

def fetch_prioritized_items(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Fetch and prioritize items by engagement potential"""
    since = datetime.now(tz=TZ) - timedelta(hours=SCAN_WINDOW_HRS)
    
    try:
        # Try with AI columns first
        cur = conn.execute("""
            SELECT source_name, link, link_canonical, ai_title, ai_desc, ai_tags, 
                   published_at, urgency_score
            FROM items 
            WHERE relevant=1 AND ai_desc IS NOT NULL AND length(ai_desc) > 40
            ORDER BY published_at DESC
        """)
        rows = cur.fetchall()
        use_ai_columns = True
        logging.info(f"Found {len(rows)} relevant items with AI processing")
    except sqlite3.OperationalError:
        # Fall back to basic columns
        cur = conn.execute("""
            SELECT source_name, link, link_canonical, title, summary, '', 
                   published_at, 0
            FROM items 
            WHERE length(summary) > 40
            ORDER BY published_at DESC
        """)
        rows = cur.fetchall()
        use_ai_columns = False
        logging.info(f"Using basic columns - found {len(rows)} items")

    items = []
    for r in rows:
        try:
            pub = datetime.fromisoformat(r[6])
        except Exception:
            pub = datetime.now(tz=TZ)
        
        if pub < since:
            continue
        
        # Handle both AI and basic column formats
        if use_ai_columns:
            title = r[3] or r[0]  # ai_title or source fallback
            desc = r[4] or ""     # ai_desc
            tags = (r[5] or "").split(",") if r[5] else []
            urgency_score = r[7] or 0
        else:
            title = r[3] or ""    # basic title
            desc = r[4] or ""     # basic summary
            tags = []
            urgency_score = 0
        
        item = {
            "source": r[0],
            "link": r[1] or "",
            "canon": r[2] or r[1] or "",
            "title": title,
            "desc": desc,
            "tags": tags,
            "pub": pub,
            "urgency_score": urgency_score
        }
        
        # Calculate engagement score
        item['engagement_score'] = calculate_engagement_score(item)
        items.append(item)
    
    # Sort by engagement score (highest first), then by recency
    items.sort(key=lambda x: (x["engagement_score"], x["pub"]), reverse=True)
    
    logging.info(f"Prioritized {len(items)} items by engagement score")
    return items

def enhance_title_for_feed(title: str, engagement_score: int) -> str:
    """Add visual engagement multipliers to titles"""
    if engagement_score >= 15:
        if not title.startswith(('🚨', '🔥', '💥')):
            title = f"🚨 {title}"
    elif engagement_score >= 10:
        if not title.startswith(('📈', '⚡', '💰')):
            title = f"⚡ {title}"
    elif engagement_score >= 5:
        if not title.startswith(('📊', '🎯')):
            title = f"📊 {title}"
    
    return title

def enhance_description_for_feed(desc: str, source: str, engagement_score: int) -> str:
    """Add engagement multipliers to descriptions"""
    if len(desc) < 50:
        return desc
    
    enhanced_desc = desc
    
    # Add strategic context based on engagement level
    if engagement_score >= 15:
        if not desc.endswith('.'):
            enhanced_desc += '.'
        enhanced_desc += " 🎯 <strong>High-impact intelligence for major players.</strong>"
    elif engagement_score >= 10:
        if not desc.endswith('.'):
            enhanced_desc += '.'
        enhanced_desc += " 📈 <strong>Significant market implications ahead.</strong>"
    elif engagement_score >= 5:
        if not desc.endswith('.'):
            enhanced_desc += '.'
        enhanced_desc += " 💡 <strong>Smart money is watching this closely.</strong>"
    
    return enhanced_desc

def create_maximum_engagement_summary(items: List[Dict[str, Any]], headline: str, 
                                   subhead: str, total_money: float) -> str:
    """Create the most engaging newsletter summary possible"""
    now = datetime.now(tz=TZ)
    
    # Calculate key metrics
    high_impact_items = [i for i in items if i['engagement_score'] >= 10]
    total_items = len(items)
    
    # Money calculations
    money_flow_today = 0.0
    for item in items[:10]:
        desc = item.get('desc', '')
        billions = re.findall(r'\$?([\d,]+\.?\d*) ?billion', desc, re.IGNORECASE)
        millions = re.findall(r'\$?([\d,]+\.?\d*) ?million', desc, re.IGNORECASE)
        
        for b in billions:
            try:
                money_flow_today += float(b.replace(',', '')) * 1000
            except:
                pass
        for m in millions:
            try:
                money_flow_today += float(m.replace(',', ''))
            except:
                pass
    
    summary_content = f"""
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 25px; border-radius: 10px; font-family: 'Segoe UI', Arial, sans-serif;">
        <div style="text-align: center; margin-bottom: 20px;">
            <h1 style="color: #FFD700; margin: 0; font-size: 24px; text-shadow: 2px 2px 4px rgba(0,0,0,0.3);">
                {esc(headline)}
            </h1>
            <p style="font-size: 16px; margin: 10px 0; color: #F0F8FF;">
                <em>{esc(subhead)}</em>
            </p>
        </div>
        
        <div style="background: rgba(255,255,255,0.1); padding: 20px; border-radius: 8px; margin: 20px 0; backdrop-filter: blur(10px);">
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; text-align: center;">
                <div>
                    <div style="font-size: 28px; font-weight: bold; color: #FFD700;">⚡ {total_items}</div>
                    <div style="font-size: 12px; color: #F0F8FF;">EXCLUSIVE STORIES</div>
                </div>
                <div>
                    <div style="font-size: 28px; font-weight: bold; color: #FFD700;">🔥 {len(high_impact_items)}</div>
                    <div style="font-size: 12px; color: #F0F8FF;">HIGH-IMPACT ALERTS</div>
                </div>
                <div>
                    <div style="font-size: 28px; font-weight: bold; color: #FFD700;">💰 ${money_flow_today:,.0f}M</div>
                    <div style="font-size: 12px; color: #F0F8FF;">MONEY FLOW TRACKED</div>
                </div>
            </div>
        </div>
        
        <div style="background: rgba(255,215,0,0.1); padding: 15px; border-left: 4px solid #FFD700; margin: 20px 0;">
            <h3 style="color: #FFD700; margin: 0 0 10px 0; font-size: 18px;">🎯 PRIORITY INTELLIGENCE</h3>
            <p style="margin: 5px 0; font-size: 14px;">
                <strong>🕐 Generated:</strong> {now.strftime('%I:%M %p AEST, %d %B %Y')}
            </p>
            <p style="margin: 5px 0; font-size: 14px;">
                <strong>🔐 Classification:</strong> Subscriber-Only Intelligence
            </p>
            <p style="margin: 5px 0; font-size: 14px;">
                <strong>📊 Coverage:</strong> Australian Commercial Property Ecosystem
            </p>
        </div>
        
        <div style="margin: 20px 0;">
            <h3 style="color: #FFD700; margin: 0 0 15px 0; font-size: 18px;">🚨 TODAY'S POWER MOVES</h3>
    """
    
    # Top 5 stories with maximum engagement formatting
    for i, item in enumerate(items[:5], 1):
        title = item.get('title', 'Untitled')
        desc = item.get('desc', '')[:200]
        engagement_score = item.get('engagement_score', 0)
        
        # Dynamic emoji based on engagement
        if engagement_score >= 15:
            priority_emoji = "🔥🚨"
            priority_label = "CRITICAL"
            border_color = "#FF0000"
        elif engagement_score >= 10:
            priority_emoji = "⚡💰"
            priority_label = "HIGH PRIORITY"
            border_color = "#FF6600"
        elif engagement_score >= 5:
            priority_emoji = "📈🎯"
            priority_label = "IMPORTANT"
            border_color = "#FFD700"
        else:
            priority_emoji = "📰"
            priority_label = "STANDARD"
            border_color = "#CCCCCC"
        
        summary_content += f"""
            <div style="background: rgba(255,255,255,0.05); padding: 15px; margin: 10px 0; border-left: 3px solid {border_color}; border-radius: 5px;">
                <div style="display: flex; align-items: center; margin-bottom: 8px;">
                    <span style="font-size: 20px; margin-right: 8px;">{priority_emoji}</span>
                    <strong style="color: #FFD700; font-size: 16px;">{esc(title)}</strong>
                    <span style="background: {border_color}; color: black; padding: 2px 6px; border-radius: 3px; font-size: 10px; margin-left: 10px; font-weight: bold;">
                        {priority_label}
                    </span>
                </div>
                <p style="margin: 0; color: #F0F8FF; font-size: 14px; line-height: 1.4;">
                    {esc(desc)}...
                </p>
            </div>
        """
    
    summary_content += f"""
        </div>
        
        <div style="background: rgba(255,0,0,0.1); padding: 15px; border: 1px solid #FF6B6B; border-radius: 8px; margin: 20px 0;">
            <div style="text-align: center;">
                <h4 style="color: #FF6B6B; margin: 0 0 10px 0; font-size: 16px;">⚠️ SUBSCRIBER EXCLUSIVE</h4>
                <p style="margin: 0; font-size: 13px; color: #F0F8FF;">
                    This intelligence briefing contains market-moving information unavailable in mainstream media.
                    <br><strong>Forward to C-suite decision-makers only.</strong>
                </p>
            </div>
        </div>
        
        <div style="text-align: center; margin-top: 25px; padding-top: 20px; border-top: 1px solid rgba(255,255,255,0.2);">
            <p style="margin: 0; font-size: 14px; color: #B0C4DE; font-weight: bold;">
                🏢 Australian Commercial Property Intelligence Network
            </p>
            <p style="margin: 5px 0 0 0; font-size: 12px; color: #87CEEB; font-style: italic;">
                "Where Smart Money Gets Smarter" ™
            </p>
        </div>
    </div>
    """
    
    return summary_content

def create_enhanced_item_html(item: Dict[str, Any]) -> str:
    """Create enhanced HTML for individual items"""
    link = item["canon"] or item["link"]
    engagement_score = item.get('engagement_score', 0)
    
    # Enhanced styling based on engagement score
    if engagement_score >= 15:
        container_style = "background: linear-gradient(135deg, #ff6b6b, #ee5a52); color: white;"
        badge = "🔥 CRITICAL"
        badge_color = "#FF0000"
    elif engagement_score >= 10:
        container_style = "background: linear-gradient(135deg, #4ecdc4, #44a08d); color: white;"
        badge = "⚡ HIGH PRIORITY"
        badge_color = "#FF6600"
    elif engagement_score >= 5:
        container_style = "background: linear-gradient(135deg, #45b7d1, #96c93d); color: white;"
        badge = "📈 IMPORTANT"
        badge_color = "#FFD700"
    else:
        container_style = "background: #f8f9fa; color: #333;"
        badge = "📰 STANDARD"
        badge_color = "#6c757d"
    
    enhanced_title = enhance_title_for_feed(item['title'], engagement_score)
    enhanced_desc = enhance_description_for_feed(item['desc'], item['source'], engagement_score)
    
    return f"""
    <div style="{container_style} padding: 20px; margin: 15px 0; border-radius: 10px; box-shadow: 0 4px 8px rgba(0,0,0,0.1);">
        <div style="display: flex; justify-content: between; align-items: center; margin-bottom: 10px;">
            <span style="background: {badge_color}; color: white; padding: 4px 8px; border-radius: 15px; font-size: 11px; font-weight: bold;">
                {badge}
            </span>
            <small style="opacity: 0.8; font-size: 12px;">Source: {esc(item['source'])}</small>
        </div>
        
        <h3 style="margin: 10px 0; font-size: 18px; line-height: 1.3;">
            {esc(enhanced_title)}
        </h3>
        
        <div style="margin: 15px 0; font-size: 15px; line-height: 1.5;">
            {enhanced_desc}
        </div>
        
        <div style="margin-top: 15px; text-align: center;">
            <a href="{esc(link)}" target="_blank" 
               style="background: rgba(255,255,255,0.2); color: inherit; padding: 10px 20px; 
                      border-radius: 25px; text-decoration: none; font-weight: bold; 
                      display: inline-block; transition: all 0.3s;">
                📖 Read Full Intelligence →
            </a>
        </div>
    </div>
    """

def write_enhanced_feed(items: List[Dict[str, Any]], headline: str, subhead: str, total_money: float):
    """Write maximum engagement RSS feed"""
    now = datetime.now(tz=TZ)
    
    # Use AI-generated headline as feed title
    feed_title = headline if headline != "🚨 Property Intelligence Alert" else f"{NEWSLETTER} Intelligence"
    
    header = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
<channel>
    <title>{esc(feed_title)}</title>
    <link>{esc(BASE_URL)}</link>
    <description>{esc(subhead)}</description>
    <language>en-au</language>
    <lastBuildDate>{now.strftime('%a, %d %b %Y %H:%M:%S %z')}</lastBuildDate>
    <ttl>30</ttl>
    <category>Commercial Property</category>
    <category>Australian Real Estate</category>
    <category>Investment Intelligence</category>
"""

    items_xml = []
    
    # Add maximum engagement newsletter summary as first item
    summary_content = create_maximum_engagement_summary(items, headline, subhead, total_money)
    
    items_xml.append(f"""<item>
    <title>{esc(headline)}</title>
    <link>{esc(BASE_URL)}</link>
    <guid isPermaLink="false">newsletter-{now.strftime('%Y%m%d-%H%M')}</guid>
    <pubDate>{now.strftime('%a, %d %b %Y %H:%M:%S %z')}</pubDate>
    <description>{cdata(summary_content)}</description>
    <content:encoded>{cdata(summary_content)}</content:encoded>
    <category>newsletter</category>
    <category>intelligence-briefing</category>
    <category>exclusive</category>
</item>

""")
    
    # Add individual enhanced items
    for item in items:
        link = item["canon"] or item["link"]
        pub = item["pub"].strftime('%a, %d %b %Y %H:%M:%S %z')
        
        # Enhanced categories
        cats = ""
        for tag in item["tags"][:5]:
            if tag.strip():
                cats += f"    <category>{esc(tag.strip())}</category>\n"
        
        # Add engagement level as category
        engagement_score = item.get('engagement_score', 0)
        if engagement_score >= 15:
            cats += "    <category>critical-intelligence</category>\n"
        elif engagement_score >= 10:
            cats += "    <category>high-priority</category>\n"
        elif engagement_score >= 5:
            cats += "    <category>important</category>\n"
        
        enhanced_html = create_enhanced_item_html(item)
        enhanced_title = enhance_title_for_feed(item['title'], engagement_score)
        
        items_xml.append(f"""<item>
    <title>{esc(enhanced_title)}</title>
    <link>{esc(link)}</link>
    <guid isPermaLink="false">{esc(link)}-enhanced</guid>
    <pubDate>{pub}</pubDate>
    <description>{cdata(enhanced_html)}</description>
    <content:encoded>{cdata(enhanced_html)}</content:encoded>
{cats}</item>

""")
    
    footer = "</channel>\n</rss>\n"
    
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(header + "".join(items_xml) + footer)

def main():
    conn = sqlite3.connect(RSS_DB_PATH)
    
    # Get AI-generated metadata
    headline, subhead, total_money = get_newsletter_metadata(conn)
    
    # Get prioritized items
    items = fetch_prioritized_items(conn)
    
    if not items:
        logging.warning("No relevant items found for feed generation")
        # Create minimal feed with placeholder
        items = [{
            'source': 'System',
            'link': BASE_URL,
            'canon': BASE_URL,
            'title': 'No Intelligence Available',
            'desc': 'No relevant intelligence available for this period. Check feed sources.',
            'tags': ['system'],
            'pub': datetime.now(tz=TZ),
            'engagement_score': 0,
            'urgency_score': 0
        }]
    
    # Write enhanced feed
    write_enhanced_feed(items, headline, subhead, total_money)
    
    # Statistics
    high_priority = len([i for i in items if i.get('engagement_score', 0) >= 10])
    total_engagement = sum(i.get('engagement_score', 0) for i in items)
    
    print(f"✅ Enhanced RSS feed written to {OUT_PATH}")
    print(f"🎯 Headline: {headline}")
    print(f"📝 Subhead: {subhead}")
    print(f"📊 Items: {len(items)} total, {high_priority} high-priority")
    print(f"🔥 Total engagement score: {total_engagement}")
    print(f"💰 Money tracked: ${total_money:,.0f}M")
    print(f"🌐 Feed URL: {BASE_URL}/beehiiv.xml")

if __name__ == "__main__":
    main()
