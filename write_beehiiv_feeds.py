#!/usr/bin/env python3
"""
Write multiple Beehiiv-ready RSS feeds from sqlite DB (schema-agnostic).

- Drops items with no usable content (or "Insufficient data").
- Handles schema differences (published vs published_at, link vs url, etc).
- Normalises all datetimes to a local fixed offset to avoid naive/aware errors.
- Buckets: top, property, finance, reits, policy.
- Sorts by interest/score desc, then recency.
- Respects env knobs (see below).
"""

import os, sqlite3, json, re
from pathlib import Path
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

# ----------------- config via env -----------------
DB_PATH            = os.getenv("RSS_DB_PATH", "rss_items.db")
# Where to write: repo root by default so GitHub Pages/Jekyll can serve it
OUT_DIR            = Path(os.getenv("OUT_DIR", "."))
OUT_PREFIX         = os.getenv("OUT_PREFIX", "beehiiv")  # filenames start with this
NEWSLETTER_NAME    = os.getenv("NEWSLETTER_NAME", "Morning Briefing")
BASE_URL           = os.getenv("BASE_URL", "https://mwhiteoak.github.io/morningBrief")

# Scan window (hours) for items to include
WINDOW_HOURS       = int(os.getenv("SCAN_WINDOW_HRS", os.getenv("WINDOW_HOURS", "36")))

# Cap per feed (Beehiiv ingests fine with 50–100; adjust as you like)
MAX_ITEMS_PER_FEED = int(os.getenv("MAX_ITEMS_PER_FEED", "80"))

# Fixed local offset in minutes (default AEST +10:00). Keep simple: no tz DB needed.
TZ_OFFSET_MIN      = int(os.getenv("TZ_OFFSET_MIN", "600"))
LOCAL_TZ           = timezone(timedelta(minutes=TZ_OFFSET_MIN))
# -------------------------------------------------

# ----------------- tiny helpers ------------------
def now_local() -> datetime:
    """Timezone-aware 'now' in LOCAL_TZ."""
    return datetime.now(LOCAL_TZ)

def rfc822(dt: datetime) -> str:
    """Format datetime for <pubDate> etc."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LOCAL_TZ)
    return format_datetime(dt.astimezone(LOCAL_TZ))

def html_cdata(s: str) -> str:
    return f"<![CDATA[ {s} ]]>"

def safe_html(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def parse_dt(v) -> datetime | None:
    """Best-effort parse from DB string/iso; return tz-aware in LOCAL_TZ."""
    if not v:
        return None
    s = str(v).strip()
    # Fast paths
    try:
        # ISO 8601: add UTC if bare "Z"
        s2 = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
    except Exception:
        return None

    # Make tz-aware
    if dt.tzinfo is None:
        # Assume local when stored without tz (most pipelines do)
        dt = dt.replace(tzinfo=LOCAL_TZ)
    return dt.astimezone(LOCAL_TZ)

def get_columns(conn) -> set[str]:
    cur = conn.execute("PRAGMA table_info(items)")
    return {row[1] for row in cur.fetchall()}

def pick(cols: set[str], *names: str) -> str | None:
    for n in names:
        if n in cols:
            return n
    return None
# -------------------------------------------------

def row_to_item(row: dict, cols: set[str],
                pub_col: str | None,
                link_col: str | None,
                title_col: str | None,
                desc_cols: list[str],
                src_cols: list[str],
                cat_cols: list[str],
                score_cols: list[str]):
    """Map a DB row to a normalised item dict or None (if contentless)."""
    link = (row.get(link_col) or row.get("link") or row.get("url") or "").strip()
    title = (row.get(title_col) or row.get("title") or "").strip()

    # Choose first non-empty description-like field
    desc = ""
    for c in desc_cols:
        v = row.get(c)
        if v and v.strip():
            desc = v.strip()
            break
    if not desc:
        desc = (row.get("ai_summary") or "").strip()

    # Drop contentless
    if not desc or desc.lower().startswith("insufficient data"):
        return None

    # Published date (fall back, then force tz-aware LOCAL_TZ)
    pub_dt = None
    if pub_col:
        pub_dt = parse_dt(row.get(pub_col))
    if not pub_dt:
        pub_dt = parse_dt(row.get("processed_at")) or now_local()

    # Source, category
    source = ""
    for c in src_cols:
        v = row.get(c)
        if v and v.strip():
            source = v.strip(); break

    category = ""
    for c in cat_cols:
        v = row.get(c)
        if v and v.strip():
            category = v.strip(); break

    # Interest / score
    score = 0.0
    for c in score_cols:
        v = row.get(c)
        try:
            if v is not None:
                score = float(v)
                break
        except Exception:
            pass

    # Optional AI package
    pkg = {}
    if "ai_package" in cols and row.get("ai_package"):
        try:
            pkg = json.loads(row["ai_package"])
        except Exception:
            pkg = {}

    # Canonical link & GUID
    link_canon = (row.get("link_canonical") or row.get("url_canonical") or "").strip()
    guid = link_canon or link or (row.get("id") and f"id:{row['id']}") or ""

    # Ensure HTML in both <description> and <content:encoded>
    html = safe_html(desc)
    if not html.lower().startswith("<"):
        html = f"<p>{html}</p>"

    return {
        "title": title or (pkg.get("title") or "(untitled)"),
        "link": link or link_canon,
        "guid": guid,
        "pub_dt": pub_dt.astimezone(LOCAL_TZ),
        "description_html": html,
        "source": source,
        "category": category or pkg.get("category") or "General",
        "score": score,
        "raw": row,
    }

def bucket_for(item: dict) -> str:
    s = (item["source"] or "").lower()
    c = (item["category"] or "").lower()
    t = (item["title"] or "").lower()

    # Property-first bias (target audience)
    if any(k in s for k in ["real commercial", "realestate.com.au", "urban developer", "domain", "pca", "commercial real estate"]) \
       or any(k in c for k in ["property", "real estate", "residential", "commercial", "development", "leasing"]):
        return "property"
    if "reit" in t or "a-reit" in t or "reit" in c:
        return "reits"
    if any(k in s for k in ["afr", "the australian", "wsj", "abc", "rba"]) \
       or any(k in c for k in ["markets","banking","finance","economy","rates"]):
        return "finance"
    if any(k in c for k in ["policy","politics","regulation"]) or "rba" in s:
        return "policy"
    return "top"

def write_rss(file_path: Path, channel_title: str, items: list[dict]):
    nowdt = now_local()
    with open(file_path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">\n')
        f.write("<channel>\n")
        f.write(f"<title>{channel_title}</title>\n")
        f.write(f"<link>{BASE_URL}</link>\n")
        f.write("<description>Daily AU commercial property briefing — assets, funds, deals, development.</description>\n")
        f.write("<language>en-au</language>\n")
        f.write(f"<lastBuildDate>{rfc822(nowdt)}</lastBuildDate>\n")
        f.write("<ttl>60</ttl>\n")

        for it in items[:MAX_ITEMS_PER_FEED]:
            f.write("<item>\n")
            f.write(f"<title>{it['title']}</title>\n")
            f.write(f"<link>{it['link']}</link>\n")
            f.write(f'<guid isPermaLink="false">{it["guid"]}</guid>\n')
            f.write(f"<pubDate>{rfc822(it['pub_dt'])}</pubDate>\n")
            f.write("<description>\n")
            f.write(html_cdata(it["description_html"]) + "\n")
            f.write("</description>\n")
            f.write("<content:encoded>\n")
            f.write(html_cdata(it["description_html"]) + "\n")
            f.write("</content:encoded>\n")
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
    conn.row_factory = sqlite3.Row
    cols = get_columns(conn)

    # Map schema differences
    pub_col   = pick(cols, "published", "published_at", "created_at", "processed_at")
    link_col  = pick(cols, "link", "url")
    title_col = pick(cols, "title")
    desc_cols = [c for c in ["description","ai_summary","raw_summary"] if c in cols]
    src_cols  = [c for c in ["source_name","source","source_feed"] if c in cols]
    cat_cols  = [c for c in ["category"] if c in cols]
    score_cols= [c for c in ["interest_score","score"] if c in cols]

    # Load rows & project to items
    rows = [dict(r) for r in conn.execute("SELECT * FROM items").fetchall()]

    since = (now_local() - timedelta(hours=WINDOW_HOURS)).astimezone(LOCAL_TZ)

    items = []
    for r in rows:
        it = row_to_item(r, cols, pub_col, link_col, title_col, desc_cols, src_cols, cat_cols, score_cols)
        if not it:
            continue
        # ensure tz-aware comparison
        pub = it["pub_dt"].astimezone(LOCAL_TZ) if it["pub_dt"] else None
        if pub and pub < since:
            continue
        items.append(it)

    # De-dupe by guid; rank by score desc, then by recency
    seen = set()
    ranked = sorted(
        items,
        key=lambda x: (-(x.get("score") or 0.0), x.get("pub_dt") or now_local()),
    )
    unique = []
    for it in ranked:
        gid = it["guid"] or (it["link"] + "|" + it["title"])
        if gid in seen:
            continue
        seen.add(gid)
        unique.append(it)

    # Bucket routing
    buckets = {"top": [], "property": [], "finance": [], "reits": [], "policy": []}
    for it in unique:
        b = bucket_for(it)
        buckets.setdefault(b, []).append(it)
        if b != "top":
            buckets["top"].append(it)

    # Final sort inside each bucket
    for k in buckets:
        buckets[k].sort(key=lambda x: (-(x.get("score") or 0.0), x.get("pub_dt") or now_local()))

    # Write feeds in repo root (so Pages can serve them)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_rss(OUT_DIR / f"{OUT_PREFIX}_top.xml",      f"{NEWSLETTER_NAME} · Top",      buckets["top"])
    write_rss(OUT_DIR / f"{OUT_PREFIX}_property.xml", f"{NEWSLETTER_NAME} · Property", buckets["property"])
    write_rss(OUT_DIR / f"{OUT_PREFIX}_finance.xml",  f"{NEWSLETTER_NAME} · Finance",  buckets["finance"])
    write_rss(OUT_DIR / f"{OUT_PREFIX}_reits.xml",    f"{NEWSLETTER_NAME} · A-REITs",  buckets["reits"])
    write_rss(OUT_DIR / f"{OUT_PREFIX}_policy.xml",   f"{NEWSLETTER_NAME} · Policy",   buckets["policy"])

    print(
        f"Wrote feeds ({OUT_PREFIX}_*.xml): "
        f"top={len(buckets['top'])} property={len(buckets['property'])} "
        f"finance={len(buckets['finance'])} reits={len(buckets['reits'])} policy={len(buckets['policy'])} | "
        f"window={WINDOW_HOURS}h tz_offset={TZ_OFFSET_MIN}min"
    )

if __name__ == "__main__":
    main()
