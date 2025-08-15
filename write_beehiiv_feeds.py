#!/usr/bin/env python3
import os, sqlite3, json, re
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path

DB_PATH = os.getenv("RSS_DB_PATH", "rss_items.db")
OUT_DIR = Path(os.getenv("OUT_DIR", "."))  # repo root so Pages/Jekyll can see it
NEWSLETTER_NAME = os.getenv("NEWSLETTER_NAME", "Morning Briefing")
BASE_URL = os.getenv("BASE_URL", "https://mwhiteoak.github.io/morningBrief")
TZ_OFFSET = int(os.getenv("TZ_OFFSET_MIN", "600"))  # +10:00 default (600 min) for AEST
WINDOW_HOURS = int(os.getenv("WINDOW_HOURS", "48"))  # how far back to consider

# -------- helpers --------
def now_local():
    return datetime.now(timezone(timedelta(minutes=TZ_OFFSET)))

def rfc822(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return format_datetime(dt)

def cdata(s: str) -> str:
    return f"<![CDATA[ {s} ]]>"

def safe_html(s: str) -> str:
    # we keep simple HTML; trim whitespace
    return re.sub(r"\s+", " ", (s or "").strip())

def get_columns(conn) -> set:
    cur = conn.execute("PRAGMA table_info(items)")
    return {row[1] for row in cur.fetchall()}

def pick(cols: set, *candidates):
    for c in candidates:
        if c and c in cols:
            return c
    return None

def parse_dt(v):
    if not v:
        return None
    try:
        # try ISO-8601 variants
        return datetime.fromisoformat(str(v).replace("Z","+00:00"))
    except Exception:
        return None

def load_rows(conn):
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute("SELECT * FROM items").fetchall()]
    return rows

# -------- feed logic --------
def item_from_row(row, cols, pub_col, link_col, title_col, desc_cols, src_cols, cat_cols, score_cols):
    link = row.get(link_col) or row.get("link") or row.get("url") or ""
    title = (row.get(title_col) or "").strip()
    # choose first non-empty description field
    desc = ""
    for dcol in desc_cols:
        v = row.get(dcol)
        if v and v.strip():
            desc = v.strip()
            break
    # fallbacks: ai_summary if description empty
    if not desc:
        desc = (row.get("ai_summary") or "").strip()

    # content filter: drop empties / “Insufficient data”
    if not desc or desc.lower().startswith("insufficient data"):
        return None

    # published time
    pub_ts = parse_dt(row.get(pub_col)) if pub_col else None
    if not pub_ts:
        pub_ts = parse_dt(row.get("processed_at")) or now_local()

    # source / category
    source = ""
    for s in src_cols:
        v = row.get(s)
        if v and v.strip():
            source = v.strip(); break

    category = ""
    for c in cat_cols:
        v = row.get(c)
        if v and v.strip():
            category = v.strip(); break

    # interest / score
    score = 0
    for sc in score_cols:
        try:
            v = row.get(sc)
            if v is None: continue
            score = float(v); break
        except Exception:
            continue

    # possible ai_package (for future routing)
    pkg = {}
    if "ai_package" in cols and row.get("ai_package"):
        try:
            pkg = json.loads(row["ai_package"])
        except Exception:
            pkg = {}

    # prefer canonical link if present
    link_canon = row.get("link_canonical") or row.get("url_canonical") or link
    guid = link_canon or link

    # ensure HTML in both <description> and <content:encoded>
    html = safe_html(desc)
    if not html.lower().startswith("<"):
        html = f"<p>{html}</p>"

    return {
        "title": title or (pkg.get("title") or "(untitled)"),
        "link": link or link_canon or "",
        "guid": guid,
        "pub_dt": pub_ts,
        "description_html": html,
        "source": source,
        "category": category or pkg.get("category") or "General",
        "score": score,
        "raw": row,
    }

def route_bucket(item):
    s = (item["source"] or "").lower()
    c = (item["category"] or "").lower()
    title = (item["title"] or "").lower()
    # very simple heuristics; adjust to your taste
    if any(k in s for k in ["real commercial", "realestate.com.au", "urban developer", "domain", "pca"]) \
       or any(k in c for k in ["property", "real estate", "residential", "commercial"]):
        return "property"
    if any(k in title for k in ["reit", "a-reit", "asx"]) or "reit" in c:
        return "reits"
    if any(k in s for k in ["afr", "the australian", "wsj", "abc", "rba"]) \
       or any(k in c for k in ["markets","banking","finance","economy"]):
        return "finance"
    if any(k in c for k in ["policy","politics","regulation"]) or "rba" in s:
        return "policy"
    return "top"

def build_rss(items, file_path, channel_title):
    nowdt = now_local()
    with open(file_path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">\n')
        f.write("<channel>\n")
        f.write(f"<title>{channel_title}</title>\n")
        f.write(f"<link>{BASE_URL}</link>\n")
        f.write(f"<description>Daily AU commercial property briefing — asset, fund, deals, development.</description>\n")
        f.write("<language>en-au</language>\n")
        f.write(f"<lastBuildDate>{rfc822(nowdt)}</lastBuildDate>\n")
        f.write("<ttl>60</ttl>\n")

        for it in items:
            f.write("<item>\n")
            f.write(f"<title>{it['title']}</title>\n")
            f.write(f"<link>{it['link']}</link>\n")
            f.write(f'<guid isPermaLink="false">{it["guid"]}</guid>\n')
            f.write(f"<pubDate>{rfc822(it['pub_dt'])}</pubDate>\n")
            f.write("<description>\n")
            f.write(cdata(it["description_html"]) + "\n")
            f.write("</description>\n")
            f.write("<content:encoded>\n")
            f.write(cdata(it["description_html"]) + "\n")
            f.write("</content:encoded>\n")
            # add categories (source + inferred)
            if it["source"]:
                f.write(f"<category>{it['source']}</category>\n")
            if it["category"]:
                f.write(f"<category>{it['category']}</category>\n")
            f.write("</item>\n")

        f.write("</channel>\n</rss>\n")

def main():
    if not Path(DB_PATH).exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    cols = get_columns(conn)

    # Map schema differences
    pub_col = pick(cols, "published", "published_at", "created_at", "processed_at")
    link_col = pick(cols, "link", "url")
    title_col = pick(cols, "title")
    desc_cols = [c for c in ["description","ai_summary","raw_summary"] if c in cols]
    src_cols  = [c for c in ["source_name","source","source_feed"] if c in cols]
    cat_cols  = [c for c in ["category"] if c in cols]
    score_cols= [c for c in ["interest_score","score"] if c in cols]

    rows = load_rows(conn)

    # time window (local)
    since = now_local() - timedelta(hours=WINDOW_HOURS)

    items = []
    for r in rows:
        it = item_from_row(r, cols, pub_col, link_col, title_col, desc_cols, src_cols, cat_cols, score_cols)
        if not it:
            continue
        # window filter
        if it["pub_dt"] and it["pub_dt"] < since:
            continue
        items.append(it)

    # De-dupe by guid
    seen = set()
    unique = []
    for it in sorted(items, key=lambda x: (
        -(x["score"] or 0.0),
        x["pub_dt"] or now_local()
    )):
        gid = it["guid"]
        if gid in seen: 
            continue
        seen.add(gid)
        unique.append(it)

    # Route into buckets
    buckets = {"top":[], "property":[], "finance":[], "reits":[], "policy":[]}
    for it in unique:
        b = route_bucket(it)
        buckets.setdefault(b, []).append(it)
        # also let top get everything, but keep it ranked
        if b != "top":
            buckets["top"].append(it)

    # Final sort in each bucket (score desc, then recency)
    for k in buckets:
        buckets[k].sort(key=lambda x: (-(x["score"] or 0.0), x["pub_dt"] or now_local()))

    # Write feeds (empty feeds still get a valid shell)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    build_rss(buckets["top"],      OUT_DIR / "beehiiv_top.xml",      f"{NEWSLETTER_NAME} · Top")
    build_rss(buckets["property"], OUT_DIR / "beehiiv_property.xml", f"{NEWSLETTER_NAME} · Property")
    build_rss(buckets["finance"],  OUT_DIR / "beehiiv_finance.xml",  f"{NEWSLETTER_NAME} · Finance")
    build_rss(buckets["reits"],    OUT_DIR / "beehiiv_reits.xml",    f"{NEWSLETTER_NAME} · A-REITs")
    build_rss(buckets["policy"],   OUT_DIR / "beehiiv_policy.xml",   f"{NEWSLETTER_NAME} · Policy")

    # Mark exported (if columns exist)
    if "exported_to_rss" in cols or "export_batch" in cols:
        today_batch = now_local().strftime("%Y%m%d")
        ids_to_mark = [it["raw"].get("id") for it in unique if it["raw"].get("id") is not None]
        if ids_to_mark:
            if "exported_to_rss" in cols:
                conn.execute(f"UPDATE items SET exported_to_rss=1 WHERE id IN ({','.join('?'*len(ids_to_mark))})", ids_to_mark)
            if "export_batch" in cols:
                conn.execute(f"UPDATE items SET export_batch=? WHERE id IN ({','.join('?'*len(ids_to_mark))})", [today_batch] + ids_to_mark)
            conn.commit()

    print(f"Wrote feeds: top={len(buckets['top'])} property={len(buckets['property'])} finance={len(buckets['finance'])} reits={len(buckets['reits'])} policy={len(buckets['policy'])}")

if __name__ == "__main__":
    main()
