#!/usr/bin/env python3
"""
Enhanced RSS Feed Analyzer - Executive Intelligence Platform
With comprehensive summary, social media content, and executive dashboard
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
import logging
import re
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict, Counter

@dataclass
class SocialMediaPost:
    platform: str  # 'twitter' or 'linkedin'
    content: str
    hashtags: List[str]
    source_article: str
    rationale: str

@dataclass
class ExecutiveInsight:
    category: str
    insight: str
    relevance_score: int
    action_required: bool
    timeline: str
    source_articles: List[str]

@dataclass
class PropertyRelevanceAnalysis:
    relevance_score: int  # 1-10
    property_sectors: List[str]  # office, retail, industrial, residential
    market_impact: str  # positive, negative, neutral
    reasoning: str
    key_metrics: List[str]
    geographic_impact: List[str]

class EnhancedEmailGenerator:
    """Generate comprehensive executive briefings with social media content"""
    
    def __init__(self, rss_analyzer):
        self.analyzer = rss_analyzer
        self.openai_client = openai
    
    def analyze_property_relevance(self, items: List[Tuple]) -> Dict[str, PropertyRelevanceAnalysis]:
        """Use AI to analyze why each item is relevant to commercial property"""
        
        if not items:
            return {}
        
        # Process in batches to avoid token limits
        batch_size = 8
        analyses = {}
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            
            prompt = """Analyze these news items for their relevance to commercial property (office, retail, industrial, data centers). 
            For each item, provide:
            1. Relevance score (1-10, where 10 is directly impacts A-REIT operations)
            2. Property sectors affected (office/retail/industrial/residential/mixed)
            3. Market impact (positive/negative/neutral)
            4. Brief reasoning (why it matters to commercial property)
            5. Key metrics mentioned (cap rates, yields, occupancy, etc.)
            6. Geographic regions affected

            Format as JSON for each item:
            {"item_1": {"relevance_score": X, "property_sectors": [...], "market_impact": "...", "reasoning": "...", "key_metrics": [...], "geographic_impact": [...]}}

            Items to analyze:
            """
            
            for idx, item in enumerate(batch, 1):
                title, link, description = item[0], item[1], item[2]
                prompt += f"\nItem {idx}: {title}\nDescription: {description[:300]}...\n"
            
            try:
                response = self.openai_client.ChatCompletion.create(
                    model="gpt-4",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=1500,
                    temperature=0.2
                )
                
                content = response.choices[0].message.content.strip()
                
                # Try to parse JSON response
                try:
                    batch_analyses = json.loads(content)
                    for j, item in enumerate(batch):
                        item_key = f"item_{j+1}"
                        if item_key in batch_analyses:
                            analysis_data = batch_analyses[item_key]
                            analyses[item[1]] = PropertyRelevanceAnalysis(
                                relevance_score=analysis_data.get('relevance_score', 5),
                                property_sectors=analysis_data.get('property_sectors', []),
                                market_impact=analysis_data.get('market_impact', 'neutral'),
                                reasoning=analysis_data.get('reasoning', ''),
                                key_metrics=analysis_data.get('key_metrics', []),
                                geographic_impact=analysis_data.get('geographic_impact', [])
                            )
                except json.JSONDecodeError:
                    # Fallback parsing
                    for item in batch:
                        analyses[item[1]] = PropertyRelevanceAnalysis(
                            relevance_score=6,
                            property_sectors=['office', 'retail'],
                            market_impact='neutral',
                            reasoning='General business relevance to commercial property sector',
                            key_metrics=[],
                            geographic_impact=['Australia']
                        )
                        
            except Exception as e:
                logging.error(f"Property relevance analysis error: {e}")
                # Fallback for batch
                for item in batch:
                    analyses[item[1]] = PropertyRelevanceAnalysis(
                        relevance_score=5,
                        property_sectors=['general'],
                        market_impact='neutral',
                        reasoning='Analysis not available',
                        key_metrics=[],
                        geographic_impact=[]
                    )
        
        return analyses

    def generate_social_media_content(self, top_items: List[Tuple]) -> List[SocialMediaPost]:
        """Generate Twitter and LinkedIn posts from top articles"""
        
        if len(top_items) < 3:
            return []
        
        # Select top 3 most relevant items
        selected_items = top_items[:3]
        
        prompt = f"""Create social media content for a commercial property executive from these news articles.
        Generate exactly 3 posts: 1 Twitter thread starter, 1 LinkedIn thought leadership post, and 1 Twitter quick insight.

        Requirements:
        - Twitter posts: Max 280 characters, professional tone, include relevant hashtags
        - LinkedIn post: 200-300 words, thought leadership angle, business insights
        - Focus on commercial property/A-REIT relevance
        - Include call-to-action or question for engagement
        - Professional but approachable tone

        Articles:
        """
        
        for i, item in enumerate(selected_items, 1):
            title, link, description = item[0], item[1], item[2]
            prompt += f"\n{i}. {title}\n{description[:200]}...\n"
        
        prompt += """\nFormat response as JSON:
        {
            "posts": [
                {
                    "platform": "twitter",
                    "content": "post content",
                    "hashtags": ["hashtag1", "hashtag2"],
                    "source_article": "article title",
                    "rationale": "why this angle"
                },
                ...
            ]
        }"""
        
        try:
            response = self.openai_client.ChatCompletion.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=800,
                temperature=0.3
            )
            
            content = response.choices[0].message.content.strip()
            
            try:
                result = json.loads(content)
                posts = []
                
                for post_data in result.get('posts', []):
                    posts.append(SocialMediaPost(
                        platform=post_data.get('platform', 'twitter'),
                        content=post_data.get('content', ''),
                        hashtags=post_data.get('hashtags', []),
                        source_article=post_data.get('source_article', ''),
                        rationale=post_data.get('rationale', '')
                    ))
                
                return posts
                
            except json.JSONDecodeError:
                # Fallback - create simple posts
                return self._generate_fallback_social_posts(selected_items)
                
        except Exception as e:
            logging.error(f"Social media generation error: {e}")
            return self._generate_fallback_social_posts(selected_items)
    
    def _generate_fallback_social_posts(self, items: List[Tuple]) -> List[SocialMediaPost]:
        """Fallback social media posts if AI generation fails"""
        posts = []
        
        if len(items) >= 1:
            posts.append(SocialMediaPost(
                platform="twitter",
                content=f"Key development in commercial property: {items[0][0][:180]}... Thoughts on the implications? #CommercialProperty #AREIT",
                hashtags=["CommercialProperty", "AREIT"],
                source_article=items[0][0],
                rationale="Market development insight"
            ))
        
        if len(items) >= 2:
            posts.append(SocialMediaPost(
                platform="linkedin",
                content=f"Reflecting on recent market developments...\n\n{items[1][0]}\n\nThis highlights the importance of staying agile in commercial property markets. What trends are you watching?\n\n#RealEstate #CommercialProperty #Leadership",
                hashtags=["RealEstate", "CommercialProperty", "Leadership"],
                source_article=items[1][0],
                rationale="Thought leadership angle"
            ))
        
        if len(items) >= 3:
            posts.append(SocialMediaPost(
                platform="twitter",
                content=f"Quick insight: {items[2][0][:150]}... Worth monitoring for sector impact. #PropertyInvesting",
                hashtags=["PropertyInvesting"],
                source_article=items[2][0],
                rationale="Quick market insight"
            ))
        
        return posts

    def generate_executive_insights(self, items: List[Tuple], property_analyses: Dict) -> List[ExecutiveInsight]:
        """Generate high-level executive insights and action items"""
        
        # Group items by category and priority
        high_priority_items = [item for item in items if item[3] >= 8]  # interest_score >= 8
        
        if not high_priority_items:
            return []
        
        prompt = f"""As a strategic advisor to an A-REIT CEO, analyze these high-priority news items and provide executive insights.

        Generate 3-4 key insights in JSON format:
        {{
            "insights": [
                {{
                    "category": "Market Outlook|Regulatory|Technology|Competition|Operational",
                    "insight": "Key strategic insight in 1-2 sentences",
                    "relevance_score": 1-10,
                    "action_required": true/false,
                    "timeline": "immediate|short-term|medium-term|long-term",
                    "source_articles": ["article title 1", "article title 2"]
                }}
            ]
        }}

        High-priority items:
        """
        
        for item in high_priority_items[:8]:  # Limit to top 8 for token management
            title, link, description, score = item[0], item[1], item[2], item[3]
            prompt += f"\n- {title} (Score: {score})\n  {description[:150]}...\n"
        
        try:
            response = self.openai_client.ChatCompletion.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1000,
                temperature=0.2
            )
            
            content = response.choices[0].message.content.strip()
            
            try:
                result = json.loads(content)
                insights = []
                
                for insight_data in result.get('insights', []):
                    insights.append(ExecutiveInsight(
                        category=insight_data.get('category', 'General'),
                        insight=insight_data.get('insight', ''),
                        relevance_score=insight_data.get('relevance_score', 5),
                        action_required=insight_data.get('action_required', False),
                        timeline=insight_data.get('timeline', 'medium-term'),
                        source_articles=insight_data.get('source_articles', [])
                    ))
                
                return insights
                
            except json.JSONDecodeError:
                return self._generate_fallback_insights(high_priority_items)
                
        except Exception as e:
            logging.error(f"Executive insights generation error: {e}")
            return self._generate_fallback_insights(high_priority_items)
    
    def _generate_fallback_insights(self, items: List[Tuple]) -> List[ExecutiveInsight]:
        """Fallback insights if AI generation fails"""
        insights = []
        
        if len(items) >= 1:
            insights.append(ExecutiveInsight(
                category="Market Outlook",
                insight="Market conditions require close monitoring based on recent developments.",
                relevance_score=7,
                action_required=True,
                timeline="short-term",
                source_articles=[items[0][0]]
            ))
        
        return insights

    def generate_comprehensive_email(self, items: List[Tuple]) -> str:
        """Generate the comprehensive executive briefing email"""
        
        if not items:
            return self._generate_no_content_email()
        
        # Filter and sort items
        high_priority = [item for item in items if item[3] >= 7]
        medium_priority = [item for item in items if 5 <= item[3] < 7]
        
        # Limit items to prevent clipping
        top_items = high_priority[:12] + medium_priority[:8]
        
        # Generate AI analyses
        logging.info("Generating property relevance analysis...")
        property_analyses = self.analyze_property_relevance(top_items)
        
        logging.info("Generating social media content...")
        social_posts = self.generate_social_media_content(high_priority[:5])
        
        logging.info("Generating executive insights...")
        executive_insights = self.generate_executive_insights(top_items, property_analyses)
        
        # Calculate dashboard metrics
        total_items = len(items)
        high_priority_count = len(high_priority)
        market_sentiment = self._calculate_market_sentiment(top_items)
        
        # Generate HTML email
        html = self._build_executive_dashboard_html(
            top_items, property_analyses, social_posts, executive_insights,
            total_items, high_priority_count, market_sentiment
        )
        
        return html
    
    def _calculate_market_sentiment(self, items: List[Tuple]) -> str:
        """Calculate overall market sentiment from news items"""
        if not items:
            return "Neutral"
        
        # Simple sentiment based on keywords in titles/descriptions
        positive_keywords = ['growth', 'increase', 'strong', 'boost', 'positive', 'up', 'gain', 'improvement']
        negative_keywords = ['decline', 'fall', 'drop', 'weak', 'negative', 'down', 'loss', 'concern', 'risk']
        
        positive_count = 0
        negative_count = 0
        
        for item in items:
            title_desc = (item[0] + ' ' + item[2]).lower()
            
            for keyword in positive_keywords:
                positive_count += title_desc.count(keyword)
            
            for keyword in negative_keywords:
                negative_count += title_desc.count(keyword)
        
        if positive_count > negative_count * 1.2:
            return "Positive"
        elif negative_count > positive_count * 1.2:
            return "Cautious"
        else:
            return "Neutral"

    def _build_executive_dashboard_html(self, items, property_analyses, social_posts, 
                                      executive_insights, total_items, high_priority_count, 
                                      market_sentiment) -> str:
        """Build comprehensive HTML email matching the desired layout"""
        
        current_date = datetime.now().strftime('%B %d, %Y')
        current_time = datetime.now().strftime('%I:%M %p')
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Matt's Memo - {current_date}</title>
            <style>
                body {{ 
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
                    line-height: 1.6; 
                    margin: 0; 
                    padding: 20px; 
                    background-color: #f8f9fa;
                    color: #333;
                }}
                .container {{ 
                    max-width: 800px; 
                    margin: 0 auto; 
                    background-color: white;
                    border-radius: 12px;
                    overflow: hidden;
                    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                }}
                .header {{ 
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white; 
                    text-align: center; 
                    padding: 30px 20px;
                }}
                .header h1 {{ 
                    margin: 0; 
                    font-size: 32px; 
                    font-weight: 700;
                }}
                .subtitle {{ 
                    margin: 8px 0 0 0; 
                    opacity: 0.9; 
                    font-size: 16px;
                }}
                .dashboard {{ 
                    display: grid; 
                    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); 
                    gap: 1px; 
                    background-color: #e9ecef;
                    margin: 0;
                }}
                .metric {{ 
                    background: white; 
                    padding: 20px; 
                    text-align: center;
                }}
                .metric-value {{ 
                    font-size: 28px; 
                    font-weight: 700; 
                    color: #667eea;
                    margin: 0;
                }}
                .metric-label {{ 
                    font-size: 12px; 
                    color: #6c757d; 
                    text-transform: uppercase; 
                    letter-spacing: 0.5px;
                    margin: 5px 0 0 0;
                }}
                .content {{ 
                    padding: 30px;
                }}
                .section {{ 
                    margin-bottom: 35px;
                }}
                .section-title {{ 
                    font-size: 20px; 
                    font-weight: 600; 
                    margin-bottom: 20px; 
                    color: #495057;
                    border-bottom: 2px solid #e9ecef;
                    padding-bottom: 8px;
                }}
                .insight-item, .action-item {{ 
                    background: #f8f9fa; 
                    border-left: 4px solid #667eea; 
                    padding: 15px; 
                    margin-bottom: 15px;
                    border-radius: 0 8px 8px 0;
                }}
                .action-item {{ 
                    border-left-color: #28a745;
                }}
                .news-item {{ 
                    border: 1px solid #e9ecef; 
                    border-radius: 8px; 
                    padding: 20px; 
                    margin-bottom: 20px;
                    background: white;
                }}
                .news-title {{ 
                    font-weight: 600; 
                    font-size: 16px; 
                    margin-bottom: 8px;
                }}
                .news-title a {{ 
                    color: #495057; 
                    text-decoration: none;
                }}
                .news-title a:hover {{ 
                    color: #667eea;
                }}
                .news-meta {{ 
                    font-size: 12px; 
                    color: #6c757d; 
                    margin-bottom: 12px;
                }}
                .relevance-analysis {{ 
                    background: #e3f2fd; 
                    border: 1px solid #bbdefb; 
                    border-radius: 6px; 
                    padding: 12px; 
                    margin-top: 12px;
                    font-size: 14px;
                }}
                .social-post {{ 
                    background: #f8f9fa; 
                    border-radius: 8px; 
                    padding: 18px; 
                    margin-bottom: 18px;
                    border-left: 4px solid #1da1f2;
                }}
                .linkedin-post {{ 
                    border-left-color: #0077b5;
                }}
                .social-platform {{ 
                    font-weight: 600; 
                    color: #495057; 
                    font-size: 14px;
                    margin-bottom: 8px;
                }}
                .social-content {{ 
                    font-size: 15px; 
                    line-height: 1.4; 
                    margin-bottom: 10px;
                }}
                .hashtags {{ 
                    color: #1da1f2; 
                    font-size: 13px;
                }}
                .priority-high {{ 
                    border-left-color: #dc3545;
                }}
                .priority-medium {{ 
                    border-left-color: #ffc107;
                }}
                .footer {{ 
                    background: #495057; 
                    color: white; 
                    text-align: center; 
                    padding: 20px; 
                    font-size: 14px;
                }}
                .two-column {{ 
                    display: grid; 
                    grid-template-columns: 1fr 1fr; 
                    gap: 30px;
                }}
                @media (max-width: 600px) {{ 
                    .two-column {{ 
                        grid-template-columns: 1fr;
                    }}
                    .dashboard {{ 
                        grid-template-columns: repeat(2, 1fr);
                    }}
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <!-- Header -->
                <div class="header">
                    <h1>Matt's Memo</h1>
                    <div class="subtitle">Strategic Intelligence for Real Estate Leaders</div>
                    <div class="subtitle">{current_date} • {current_time} • Sydney</div>
                </div>
                
                <!-- Executive Dashboard -->
                <div class="dashboard">
                    <div class="metric">
                        <div class="metric-value">{total_items}</div>
                        <div class="metric-label">Total Items</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value">{high_priority_count}</div>
                        <div class="metric-label">High Priority</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value">{len(executive_insights)}</div>
                        <div class="metric-label">Action Items</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value">{market_sentiment}</div>
                        <div class="metric-label">Market Sentiment</div>
                    </div>
                </div>
                
                <div class="content">
                    <!-- Executive Insights -->
                    {self._build_executive_insights_section(executive_insights)}
                    
                    <!-- Key News with Property Analysis -->
                    {self._build_news_section(items[:8], property_analyses)}
                    
                    <!-- Two Column Layout -->
                    <div class="two-column">
                        <!-- Social Media Content -->
                        {self._build_social_media_section(social_posts)}
                        
                        <!-- Additional News -->
                        {self._build_additional_news_section(items[8:15])}
                    </div>
                </div>
                
                <!-- Footer -->
                <div class="footer">
                    <strong>Matt's Memo</strong><br>
                    Strategic Intelligence for Real Estate Leaders<br>
                    Powered by AI • Real-time Analysis • Executive Focus<br><br>
                    <em>This briefing contains AI-generated insights. Always verify information independently.</em>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html

    def _build_executive_insights_section(self, insights: List[ExecutiveInsight]) -> str:
        """Build executive insights section"""
        if not insights:
            return ""
        
        html = '''
        <div class="section">
            <h2 class="section-title">📊 Executive Insights</h2>
        '''
        
        for insight in insights:
            action_class = "action-item" if insight.action_required else "insight-item"
            action_icon = "🎯" if insight.action_required else "💡"
            
            html += f'''
            <div class="{action_class}">
                <strong>{action_icon} {insight.category}</strong> • {insight.timeline.title()}<br>
                {insight.insight}
            </div>
            '''
        
        html += "</div>"
        return html

    def _build_news_section(self, items: List[Tuple], property_analyses: Dict) -> str:
        """Build main news section with property relevance analysis"""
        if not items:
            return ""
        
        html = '''
        <div class="section">
            <h2 class="section-title">📰 Key Developments</h2>
        '''
        
        for item in items:
            title, link, description, score, summary, source = item[:6]
            
            # Determine priority styling
            priority_class = "priority-high" if score >= 8 else "priority-medium" if score >= 6 else ""
            
            # Get property analysis
            analysis = property_analyses.get(link)
            analysis_html = ""
            
            if analysis and analysis.relevance_score >= 6:
                sectors = ", ".join(analysis.property_sectors) if analysis.property_sectors else "General"
                analysis_html = f'''
                <div class="relevance-analysis">
                    <strong>🏢 Property Relevance (Score: {analysis.relevance_score}/10)</strong><br>
                    <strong>Sectors:</strong> {sectors} • <strong>Impact:</strong> {analysis.market_impact.title()}<br>
                    {analysis.reasoning}
                </div>
                '''
            
            html += f'''
            <div class="news-item {priority_class}">
                <div class="news-title">
                    <a href="{link}" target="_blank">{title}</a>
                </div>
                <div class="news-meta">
                    Score: {score}/10 • Source: {source} • {datetime.now().strftime('%H:%M')}
                </div>
                <div class="news-summary">
                    {summary or description[:200] + "..."}
                </div>
                {analysis_html}
            </div>
            '''
        
        html += "</div>"
        return html

    def _build_social_media_section(self, posts: List[SocialMediaPost]) -> str:
        """Build social media content section"""
        if not posts:
            return '''
            <div>
                <h3 class="section-title">📱 Ready-to-Share Content</h3>
                <div class="social-post">
                    <div class="social-platform">No social content available</div>
                    <div class="social-content">Generate content by processing more relevant news items.</div>
                </div>
            </div>
            '''
        
        html = '''
        <div>
            <h3 class="section-title">📱 Ready-to-Share Content</h3>
        '''
        
        for post in posts:
            platform_class = "linkedin-post" if post.platform == "linkedin" else ""
            platform_icon = "💼" if post.platform == "linkedin" else "🐦"
            hashtag_text = " ".join([f"#{tag}" for tag in post.hashtags])
            
            html += f'''
            <div class="social-post {platform_class}">
                <div class="social-platform">{platform_icon} {post.platform.title()}</div>
                <div class="social-content">{post.content}</div>
                <div class="hashtags">{hashtag_text}</div>
            </div>
            '''
        
        html += "</div>"
        return html

    def _build_additional_news_section(self, items: List[Tuple]) -> str:
        """Build additional news section"""
        if not items:
            return ""
        
        html = '''
        <div>
            <h3 class="section-title">📋 Additional Items</h3>
        '''
        
        for item in items:
            title, link, description, score, summary, source = item[:6]
            
            html += f'''
            <div style="border-bottom: 1px solid #e9ecef; padding-bottom: 10px; margin-bottom: 10px;">
                <div style="font-weight: 500; margin-bottom: 4px;">
                    <a href="{link}" target="_blank" style="color: #495057; text-decoration: none; font-size: 14px;">{title}</a>
                </div>
                <div style="font-size: 12px; color: #6c757d;">
                    {source} • Score: {score}/10
                </div>
            </div>
            '''
        
        html += "</div>"
        return html

    def _generate_no_content_email(self) -> str:
        """Generate email when no content is available"""
        current_date = datetime.now().strftime('%B %d, %Y')
        
        return f'''
        <!DOCTYPE html>
        <html>
        <head><title>Matt's Memo - {current_date}</title></head>
        <body style="font-family: Arial, sans-serif; padding: 20px;">
            <div style="max-width: 600px; margin: 0 auto;">
                <h1 style="color: #667eea;">Matt's Memo - {current_date}</h1>
                <p>No significant news items found for today's briefing.</p>
                <p>This could mean:</p>
                <ul>
                    <li>Quiet news day in commercial property sector</li>
                    <li>All recent items were previously processed</li>
                    <li>RSS feeds may need checking</li>
                </ul>
                <p><em>Next briefing will be sent when new relevant content is available.</em></p>
            </div>
        </body>
        </html>
        '''


# Enhanced RSSAnalyzer class methods to integrate with new email generator
class EnhancedRSSAnalyzer:
    """Enhanced version with comprehensive email generation"""
    
    def __init__(self, base_analyzer):
        self.base = base_analyzer
        self.email_generator = EnhancedEmailGenerator(base_analyzer)
    
    def generate_comprehensive_email_from_items(self, items: List[Tuple]) -> Optional[str]:
        """Generate comprehensive email using enhanced generator"""
        try:
            return self.email_generator.generate_comprehensive_email(items)
        except Exception as e:
            logging.error(f"Enhanced email generation failed: {e}")
            # Fallback to simple email
            return self.base.build_simple_email_html(items)
    
    def send_enhanced_daily_brief(self):
        """Send enhanced daily brief with comprehensive analysis"""
        try:
            items = self.base.get_items_for_email(24)
            if items:
                content = self.generate_comprehensive_email_from_items(items)
                if content:
                    self.base.send_email(content)
                    
                    # Mark items as sent
                    cutoff_time = datetime.now() - timedelta(hours=24)
                    self.base.conn.execute('''
                        UPDATE items SET email_sent = TRUE 
                        WHERE processed_at >= ? AND email_sent = FALSE
                    ''', (cutoff_time,))
                    self.base.conn.commit()
                    
                    logging.info("Enhanced email sent successfully")
                else:
                    logging.info("No content generated for enhanced email")
            else:
                logging.info("No items found for enhanced email")
        except Exception as e:
            logging.error(f"Enhanced email brief error: {e}")
            # Fallback to regular email
            self.base.send_daily_brief()


# Update the main RSSAnalyzer class to use enhanced email generation
def enhance_rss_analyzer(analyzer):
    """Enhance existing RSSAnalyzer with new email capabilities"""
    enhanced = EnhancedRSSAnalyzer(analyzer)
    
    # Replace email generation methods
    analyzer.generate_daily_email_from_items = enhanced.generate_comprehensive_email_from_items
    analyzer.send_daily_brief_enhanced = enhanced.send_enhanced_daily_brief
    
    return analyzer
