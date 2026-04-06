"""
SherByte Backend — main.py  v3.0  (UPGRADE — Grok AI + Live Markets)
=====================================================================
Install : pip install -r requirements.txt
Run     : python main.py
Deploy  : push to GitHub → Render picks up render.yaml

Environment variables:
  GROK_API_KEY     → https://console.x.ai           (required for AI rewrite)
  JWT_SECRET       → any long random string          (required)
  NEWSAPI_KEY      → https://newsapi.org             (optional)
  OPENWEATHER_KEY  → https://openweathermap.org/api  (optional, for live weather)
"""

import os, json, math, hashlib, asyncio, logging, uuid, sqlite3
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import httpx
import feedparser
from groq import Groq

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from jose import JWTError, jwt
from passlib.context import CryptContext
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────────────────────
# 1. BOOTSTRAP
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("sherbyte")

# ─────────────────────────────────────────────────────────────────────────────
# 2. CONFIG
# ─────────────────────────────────────────────────────────────────────────────
GROK_API_KEY      = os.getenv("GROK_API_KEY", "")
NEWSAPI_KEY       = os.getenv("NEWSAPI_KEY", "")
OPENWEATHER_KEY   = os.getenv("OPENWEATHER_KEY", "")
JWT_SECRET        = os.getenv("JWT_SECRET", "sherbyte-local-dev-secret-CHANGE-ME")
JWT_ALG           = "HS256"
JWT_EXP_DAYS      = 30
DB_PATH           = Path("sherbyte.db")

# ── 7 VIBGYOR Categories with new color codes from spec ──────────────────────
CATEGORIES = ["society", "economy", "tech", "arts", "nature", "selfwell", "philo"]

CATS_META = {
    "society":  {"hex": "#1E88E5", "label": "Society & Governance",    "icon": "🏛"},
    "economy":  {"hex": "#FBC02D", "label": "Business & Economy",       "icon": "💼"},
    "tech":     {"hex": "#3949AB", "label": "Science & Technology",     "icon": "🔬"},
    "arts":     {"hex": "#E53935", "label": "Arts, Culture & Recreation","icon": "🎭"},
    "nature":   {"hex": "#43A047", "label": "The Natural World",         "icon": "🌿"},
    "selfwell": {"hex": "#FB8C00", "label": "The Self & Well-being",     "icon": "🧘"},
    "philo":    {"hex": "#8E24AA", "label": "Philosophy & Belief",       "icon": "🔮"},
}

# ── Expanded keyword classification (all subtopics from spec) ─────────────────
CATEGORY_KEYWORDS = {
    "society": [
        "politics","government","election","law","court","social","education",
        "community","police","justice","protest","vote","parliament","democracy",
        "liberalism","conservatism","socialism","geopolitics","nato","g20","un",
        "human rights","civil rights","feminism","lgbtq","activism","movement",
        "supreme court","intellectual property","cyber law","smart city",
        "public transport","nep","lok sabha","modi","ambedkar","federalism",
        "urban","civic","sociology","anthropology","criminology","gender",
        "diplomat","minister","senator","ias","corruption","caste","census",
    ],
    "economy": [
        "economy","market","stock","finance","business","trade","gdp","inflation",
        "investment","bank","rupee","sensex","nifty","startup","venture capital",
        "merger","acquisition","supply chain","marketing","seo","real estate",
        "fintech","ecommerce","crypto","bitcoin","ethereum","forex","mutual fund",
        "interest rate","recession","unicorn","saas","ipo","budget","rbi",
        "sebi","dalal street","wall street","silicon valley","gift city",
        "accountant","ceo","founder","product manager","insurance","actuarial",
    ],
    "tech": [
        "technology","ai","artificial intelligence","software","startup",
        "digital","cyber","space","science","robot","chip","quantum","llm",
        "neural network","generative ai","computer vision","machine learning",
        "full stack","devops","open source","cloud","cybersecurity","arduino",
        "raspberry pi","3d printing","drone","robotics","internet","dark web",
        "quantum computing","nuclear fusion","crispr","brain computer","isro",
        "nasa","cern","mit","astrophysics","black hole","mathematics","game theory",
        "cryptography","aerospace","nanotechnology","engineer","physicist",
        "hacker","data scientist","astronaut","algorithm","programming",
    ],
    "arts": [
        "art","film","music","culture","festival","theatre","literature",
        "cinema","dance","heritage","bollywood","award","aesthetic","fashion",
        "streetwear","thrift","photography","animation","graphic design",
        "k-pop","lo-fi","vinyl","podcast","gaming","esports","game development",
        "fanfiction","poetry","sci-fi","fantasy","bookTok","novel","subculture",
        "goth","punk","skater","otaku","cosplay","knitting","calligraphy",
        "pottery","origami","influencer","director","designer","writer","artist",
        "taylor swift","shah rukh","nolan","miyazaki","cannes","oscar",
    ],
    "nature": [
        "environment","climate","wildlife","forest","ocean","nature",
        "biodiversity","pollution","renewable","conservation","tiger","river",
        "gardening","plants","hydroponics","permaculture","bonsai","urban farming",
        "animals","zoology","birdwatching","aquarium","entomology","dog",
        "geography","travel","cartography","van life","ecotourism","hiking",
        "biology","genetics","marine biology","evolution","microbiology","botany",
        "aurora","eclipse","volcano","weather","geology","amazon","coral reef",
        "yellowstone","himalayas","darwin","attenborough","greta thunberg",
        "climate change","zero waste","green","solar","wind energy",
    ],
    "selfwell": [
        "health","wellness","mental health","fitness","yoga","medicine",
        "nutrition","psychology","mindfulness","hospital","diet","crossfit",
        "calisthenics","pilates","marathon","zumba","meditation","journaling",
        "dopamine","therapy","veganism","sourdough","coffee","fermentation","keto",
        "minimalism","konmari","feng shui","meal prep","parenting","montessori",
        "eldercare","relationships","attachment","dating","boundary","productivity",
        "pomodoro","notion","bullet journal","life coach","nutritionist","chef",
        "personal trainer","sleep","stress","anxiety","depression","burnout",
        "ayurveda","homeopathy","wim hof","huberman","atomic habits",
    ],
    "philo": [
        "philosophy","ethics","ideas","religion","spirituality","consciousness",
        "meaning","wisdom","thought","debate","moral","stoicism","nihilism",
        "absurdism","utilitarianism","epistemology","hinduism","buddhism",
        "christianity","islam","sikhism","mysticism","tarot","astrology",
        "crystal","meditation","lucid dreaming","witchcraft","numerology",
        "alchemy","paranormal","bioethics","ai ethics","trolley problem",
        "free will","mythology","greek","norse","egyptian","vedic","folklore",
        "urban legend","dalai lama","sadhguru","nietzsche","rumi","osho",
        "varanasi","vatican","mecca","stonehenge","pyramid","monk","guru","priest",
    ],
}

# ── Extended topic list (for FEED Pulse / onboarding) ─────────────────────────
ALL_TOPICS = [
    # Tech & Science
    "AI","Machine Learning","Cybersecurity","Space","Quantum Computing","Robotics",
    "Blockchain","Web3","Programming","Open Source","Data Science","Biotech","CRISPR",
    # Business & Finance
    "Startups","Venture Capital","Stock Market","Crypto","Bitcoin","Real Estate",
    "Finance","Entrepreneurship","E-Commerce","Fintech","IPO","Economy",
    # Society & Politics
    "Politics","Elections","Human Rights","Climate Policy","Education","Law",
    "Urban Planning","Geopolitics","India","International Relations","UN",
    # Arts & Entertainment
    "Bollywood","Hollywood","Cinema","Music","Gaming","Esports","Books","Fashion",
    "Photography","Animation","K-Pop","Podcasts","Streaming",
    # Nature & Environment
    "Climate Change","Wildlife","Conservation","Oceans","Forests","Renewable Energy",
    "Sustainability","Biodiversity","Geology","Astronomy",
    # Health & Lifestyle
    "Mental Health","Fitness","Nutrition","Yoga","Mindfulness","Sleep","Productivity",
    "Relationships","Parenting","Veganism","Travel","Food",
    # Philosophy & Culture
    "Philosophy","Spirituality","Mythology","Religion","Ethics","Psychology",
    "History","Sociology","Anthropology","Linguistics",
    # India-specific
    "Cricket","IPL","Indian Cinema","Indian Politics","ISRO","Startups India",
    "Desi Culture","Indian Economy","Hindi","Sanskrit",
    # Sports
    "Football","Tennis","Basketball","Formula 1","Olympics","Athletics","Kabaddi",
]

# ── RSS Feeds — 15 sources for more articles ──────────────────────────────────
RSS_FEEDS = [
    "https://feeds.feedburner.com/ndtvnews-top-stories",
    "https://timesofindia.indiatimes.com/rssfeedstopstories.cms",
    "https://www.thehindu.com/feeder/default.rss",
    "https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml",
    "https://indianexpress.com/section/india/feed/",
    "https://www.livemint.com/rss/news",
    "http://feeds.bbci.co.uk/news/world/asia/india/rss.xml",
    "https://techcrunch.com/feed/",
    "https://www.theguardian.com/world/rss",
    "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
    "https://feeds.feedburner.com/ndtvnews-science",
    "https://feeds.feedburner.com/ndtvnews-business",
    "https://www.thehindu.com/sport/cricket/feeder/default.rss",
    "https://economictimes.indiatimes.com/rssfeedsdefault.cms",
    "https://www.financialexpress.com/feed/",
    "https://www.moneycontrol.com/rss/latestnews.xml",
    "https://feeds.skynews.com/feeds/rss/world.xml",
    "https://www.aljazeera.com/xml/rss/all.xml",
]

# ─────────────────────────────────────────────────────────────────────────────
# 3. DATABASE
# ─────────────────────────────────────────────────────────────────────────────
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS articles (
            id             TEXT PRIMARY KEY,
            title          TEXT NOT NULL,
            preview        TEXT,
            body_ai        TEXT,
            image_url      TEXT,
            category       TEXT NOT NULL DEFAULT 'society',
            source         TEXT,
            source_url     TEXT,
            published_at   TEXT,
            scope          TEXT DEFAULT 'national',
            view_count     INTEGER DEFAULT 0,
            like_count     INTEGER DEFAULT 0,
            save_count     INTEGER DEFAULT 0,
            trending_score REAL    DEFAULT 0.0,
            quiz           TEXT    DEFAULT '[]',
            word_of_day    TEXT    DEFAULT '{}',
            is_published   INTEGER DEFAULT 1,
            created_at     TEXT    DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS users (
            id            TEXT PRIMARY KEY,
            email         TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            display_name  TEXT,
            avatar_url    TEXT,
            bio           TEXT DEFAULT '',
            links         TEXT DEFAULT '{}',
            interests     TEXT DEFAULT '{"society":0.5,"economy":0.5,"tech":0.5,"arts":0.5,"nature":0.5,"selfwell":0.5,"philo":0.5}',
            topics        TEXT DEFAULT '["AI","Climate","Finance","Cricket","Science"]',
            language      TEXT DEFAULT 'English',
            streak        INTEGER DEFAULT 0,
            score         INTEGER DEFAULT 0,
            articles_read INTEGER DEFAULT 0,
            time_spent    INTEGER DEFAULT 0,
            created_at    TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS interactions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id      TEXT NOT NULL,
            article_id   TEXT NOT NULL,
            action       TEXT NOT NULL,
            duration_sec INTEGER DEFAULT 0,
            created_at   TEXT DEFAULT (datetime('now')),
            UNIQUE(user_id, article_id, action)
        );

        CREATE INDEX IF NOT EXISTS idx_art_cat    ON articles(category);
        CREATE INDEX IF NOT EXISTS idx_art_pub    ON articles(published_at DESC);
        CREATE INDEX IF NOT EXISTS idx_art_trend  ON articles(trending_score DESC);
        CREATE INDEX IF NOT EXISTS idx_art_scope  ON articles(scope);
        CREATE INDEX IF NOT EXISTS idx_int_user   ON interactions(user_id);
    """)
    conn.commit()
    conn.close()
    log.info("DB ready → %s", DB_PATH.resolve())

# ─────────────────────────────────────────────────────────────────────────────
# 4. AUTH
# ─────────────────────────────────────────────────────────────────────────────
pwd_ctx  = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer(auto_error=False)

def hash_pw(pw: str) -> str:         return pwd_ctx.hash(pw)
def verify_pw(pw: str, h: str) -> bool: return pwd_ctx.verify(pw, h)

def make_token(user_id: str) -> str:
    exp = datetime.now(timezone.utc) + timedelta(days=JWT_EXP_DAYS)
    return jwt.encode({"sub": user_id, "exp": exp}, JWT_SECRET, algorithm=JWT_ALG)

def decode_token(token: str) -> Optional[str]:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG]).get("sub")
    except JWTError:
        return None

def get_current_user(creds: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> Optional[str]:
    return decode_token(creds.credentials) if creds else None

def require_user(creds: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> str:
    uid = get_current_user(creds)
    if not uid:
        raise HTTPException(401, "Authentication required")
    return uid

# ─────────────────────────────────────────────────────────────────────────────
# 5. PYDANTIC MODELS
# ─────────────────────────────────────────────────────────────────────────────
class RegisterRequest(BaseModel):
    email: str
    password: str
    display_name: Optional[str] = None

class LoginRequest(BaseModel):
    email: str
    password: str

class InteractRequest(BaseModel):
    article_id: str
    category: str
    action: str
    duration_sec: int = 0

class OnboardRequest(BaseModel):
    interests: dict
    topics: list

class ProfileUpdateRequest(BaseModel):
    display_name: Optional[str] = None
    bio: Optional[str] = None
    avatar_url: Optional[str] = None
    links: Optional[dict] = None
    language: Optional[str] = None

class TopicsUpdateRequest(BaseModel):
    topics: list[str]
    interests: Optional[dict] = None

# ─────────────────────────────────────────────────────────────────────────────
# 6. ARTICLE HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def row_to_article(row) -> dict:
    d = dict(row)
    try:    d["quiz"]        = json.loads(d.get("quiz") or "[]")
    except: d["quiz"]        = []
    try:    d["word_of_day"] = json.loads(d.get("word_of_day") or "{}")
    except: d["word_of_day"] = {}
    return d

def score_article(article: dict, interests: dict) -> float:
    try:
        pub_str   = (article.get("published_at") or "").replace("Z", "+00:00")
        pub       = datetime.fromisoformat(pub_str)
        age_hours = max(1.0, (datetime.now(timezone.utc) - pub).total_seconds() / 3600)
    except Exception:
        age_hours = 24.0
    interest = interests.get(article.get("category", "society"), 0.5)
    recency  = math.exp(-0.15 * age_hours)
    trending = min((article.get("trending_score") or 0.0) / 100.0, 1.0)
    return interest * 0.60 + recency * 0.25 + trending * 0.10

INTEREST_DELTAS = {
    "read": 0.05, "like": 0.10, "save": 0.12,
    "share": 0.08, "skip": -0.03, "quiz_complete": 0.07,
}

def update_interest(conn, user_id: str, category: str, action: str) -> None:
    delta = INTEREST_DELTAS.get(action, 0.0)
    if delta == 0.0: return
    row = conn.execute("SELECT interests FROM users WHERE id=?", (user_id,)).fetchone()
    if not row: return
    try:    interests = json.loads(row["interests"])
    except: interests = {}
    cur = interests.get(category, 0.5)
    interests[category] = round(max(0.05, min(1.0, cur + delta)), 3)
    conn.execute("UPDATE users SET interests=? WHERE id=?", (json.dumps(interests), user_id))

# ─────────────────────────────────────────────────────────────────────────────
# 7. NEWS COLLECTION
# ─────────────────────────────────────────────────────────────────────────────
def classify_category(title: str, body: str) -> str:
    text = (title + " " + (body or "")).lower()
    scores = {cat: sum(1 for kw in kws if kw in text) for cat, kws in CATEGORY_KEYWORDS.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "society"

def classify_scope(title: str, body: str) -> str:
    """Classify article as local/national/global based on content."""
    text = (title + " " + (body or "")).lower()
    global_kws = ["world","global","international","un","nato","g20","europe","america",
                  "china","usa","uk","russia","europe","africa","asia","pacific","climate"]
    local_kws  = ["city","town","village","district","municipal","local","ward",
                  "neighbourhood","street","colony","taluk","tehsil","panchayat"]
    g_score = sum(1 for kw in global_kws if kw in text)
    l_score = sum(1 for kw in local_kws  if kw in text)
    if g_score > l_score and g_score > 0: return "global"
    if l_score > 0: return "local"
    return "national"

def make_article_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:16]

async def fetch_newsapi(client: httpx.AsyncClient) -> list:
    if not NEWSAPI_KEY:
        log.info("NEWSAPI_KEY not set — RSS only")
        return []
    try:
        # Fetch both India top headlines AND everything (more articles)
        results = []
        for params in [
            {"country": "in", "pageSize": 40, "apiKey": NEWSAPI_KEY},
            {"q": "India OR technology OR science OR business", "pageSize": 40,
             "language": "en", "sortBy": "publishedAt", "apiKey": NEWSAPI_KEY},
        ]:
            r = await client.get("https://newsapi.org/v2/top-headlines" if "country" in params
                                 else "https://newsapi.org/v2/everything",
                                 params=params, timeout=10)
            if r.status_code == 200:
                for a in r.json().get("articles", []):
                    if not a.get("url") or not a.get("title") or "[Removed]" in a.get("title",""):
                        continue
                    results.append({
                        "title":        a["title"],
                        "body":         a.get("description") or a.get("content") or "",
                        "image_url":    a.get("urlToImage") or "",
                        "source":       a.get("source", {}).get("name", "NewsAPI"),
                        "source_url":   a["url"],
                        "published_at": a.get("publishedAt", ""),
                    })
        log.info("NewsAPI → %d articles", len(results))
        return results
    except Exception as exc:
        log.warning("NewsAPI error: %s", exc)
        return []

async def fetch_rss(client: httpx.AsyncClient) -> list:
    result = []
    for feed_url in RSS_FEEDS:
        try:
            r    = await client.get(feed_url, timeout=8, follow_redirects=True)
            feed = feedparser.parse(r.text)
            for entry in feed.entries[:8]:   # 8 per feed = more articles
                url = entry.get("link", "")
                if not url: continue
                # Try to get image from media content
                image_url = ""
                if hasattr(entry, "media_content") and entry.media_content:
                    image_url = entry.media_content[0].get("url", "")
                elif hasattr(entry, "media_thumbnail") and entry.media_thumbnail:
                    image_url = entry.media_thumbnail[0].get("url", "")

                result.append({
                    "title":        entry.get("title", ""),
                    "body":         entry.get("summary", "") or entry.get("description", ""),
                    "image_url":    image_url,
                    "source":       feed.feed.get("title", feed_url.split("/")[2]),
                    "source_url":   url,
                    "published_at": entry.get("published", ""),
                })
        except Exception as exc:
            log.debug("RSS skip %s: %s", feed_url, exc)
    log.info("RSS → %d articles", len(result))
    return result

# ─────────────────────────────────────────────────────────────────────────────
# 8. GROK AI REWRITE  ← SWITCHED FROM GEMINI TO GROK
# ─────────────────────────────────────────────────────────────────────────────
async def grok_rewrite(title: str, body: str, category: str):
    """
    Rewrite article with Grok (xAI). Returns (preview, body_ai, quiz, word_of_day).
    Falls back to raw text gracefully if API key missing or call fails.
    """
    preview_default = (body[:200] + "...") if len(body) > 200 else (body or title)
    body_default    = body or title
    quiz_default    = []
    word_default    = {}

    if not GROK_API_KEY:
        log.info("GROK_API_KEY not set — using raw text")
        return preview_default, body_default, quiz_default, word_default

    try:
        client = Groq(api_key=GROK_API_KEY)

        prompt = (
            "You are an AI news editor for SherByte, an Indian news app. "
            f"Rewrite this {category} news article for Indian readers — clear, neutral, engaging.\n\n"
            f"TITLE: {title}\n"
            f"BODY: {body[:1000]}\n\n"
            "Return ONLY a valid JSON object, no markdown, no explanation:\n"
            '{"preview":"A 60-word engaging summary",'
            '"body_ai":"A 160-180 word plain English rewrite of the full article",'
            '"quiz":[{"question":"A factual question from this article",'
            '"options":["Correct answer","Wrong option B","Wrong option C","Wrong option D"],'
            '"answer_index":0}],'
            '"word_of_day":{"word":"a vocabulary word from this article",'
            '"phonetic":"/fəˈnetɪk/","definition":"clear definition in one sentence",'
            '"example":"a natural example sentence using this word"}}'
        )

        # Grok uses OpenAI-compatible chat completions API
        response = await asyncio.to_thread(
            client.chat.completions.create,
            model="llama-3.3-70b-versatile",   # Groq's fastest model
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=800,
        )

        text = response.choices[0].message.content.strip()

        # Strip markdown fences if present
        if text.startswith("```"):
            text = text.strip("`").strip()
            if text.lower().startswith("json"):
                text = text[4:].strip()

        data = json.loads(text)
        return (
            data.get("preview",     preview_default),
            data.get("body_ai",     body_default),
            data.get("quiz",        quiz_default),
            data.get("word_of_day", word_default),
        )

    except Exception as exc:
        log.warning("Grok rewrite failed (%s) — using raw text", exc)
        return preview_default, body_default, quiz_default, word_default

# ─────────────────────────────────────────────────────────────────────────────
# 9. PIPELINE
# ─────────────────────────────────────────────────────────────────────────────
async def collect_news() -> int:
    log.info("[CRON] Starting news collection...")
    async with httpx.AsyncClient() as client:
        news_api_arts, rss_arts = await asyncio.gather(
            fetch_newsapi(client), fetch_rss(client)
        )

    all_raw   = news_api_arts + rss_arts
    conn      = get_conn()
    new_count = 0

    for raw in all_raw:
        if not raw.get("title") or not raw.get("source_url"):
            continue
        art_id = make_article_id(raw["source_url"])
        if conn.execute("SELECT id FROM articles WHERE id=?", (art_id,)).fetchone():
            continue

        category = classify_category(raw["title"], raw.get("body", ""))
        scope    = classify_scope(raw["title"], raw.get("body", ""))
        body     = raw.get("body") or raw["title"]

        preview, body_ai, quiz_json, word_json = await grok_rewrite(
            raw["title"], body, category
        )

        conn.execute(
            """INSERT OR IGNORE INTO articles
               (id,title,preview,body_ai,image_url,category,scope,
                source,source_url,published_at,quiz,word_of_day)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                art_id, raw["title"], preview, body_ai,
                raw.get("image_url", ""), category, scope,
                raw.get("source", ""), raw.get("source_url", ""),
                raw.get("published_at", ""),
                json.dumps(quiz_json), json.dumps(word_json),
            ),
        )
        new_count += 1

    conn.commit()
    conn.close()
    log.info("[CRON] Done — %d new articles saved.", new_count)
    return new_count


def update_trending_scores() -> None:
    conn = get_conn()
    rows = conn.execute(
        "SELECT id,like_count,save_count,view_count,published_at FROM articles"
    ).fetchall()
    for row in rows:
        try:
            pub_str   = (row["published_at"] or "").replace("Z", "+00:00")
            pub       = datetime.fromisoformat(pub_str) if pub_str else datetime.now(timezone.utc)
            age_hours = max(1.0, (datetime.now(timezone.utc) - pub).total_seconds() / 3600)
        except Exception:
            age_hours = 24.0
        score = (row["like_count"]*2 + row["save_count"]*3 + row["view_count"]*0.5) / age_hours
        conn.execute("UPDATE articles SET trending_score=? WHERE id=?", (score, row["id"]))
    conn.commit()
    conn.close()

# ─────────────────────────────────────────────────────────────────────────────
# 10. LIVE MARKET DATA  (free APIs, no key needed for most)
# ─────────────────────────────────────────────────────────────────────────────
def _yahoo_parse(raw: dict) -> dict:
    """Extract price + change from Yahoo Finance v8 chart response."""
    try:
        meta   = raw["chart"]["result"][0]["meta"]
        price  = meta.get("regularMarketPrice", 0)
        prev   = meta.get("chartPreviousClose") or meta.get("previousClose") or price
        change = round(price - prev, 2)
        pct    = round((change / prev * 100) if prev else 0, 2)
        return {"price": round(price, 2), "change": change, "change_pct": pct}
    except Exception:
        return {"price": 0, "change": 0, "change_pct": 0}


async def fetch_live_markets() -> dict:
    """
    Fetch ALL live market data using free APIs (no paid key needed for stocks/gold/oil).
    Sources:
      • Stocks (Nifty/Sensex/Nasdaq)  → Yahoo Finance v8  (free, no key)
      • Gold / Silver / Crude Oil     → Yahoo Finance v8  (free, no key)
      • Crypto                        → CoinGecko         (free, no key)
      • Weather                       → OpenWeatherMap    (free key optional)
      • USD/INR Forex                 → Yahoo Finance v8  (free, no key)
    """
    data: dict = {}
    YAHOO = "https://query1.finance.yahoo.com/v8/finance/chart/"
    HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SherByte/3.0)"}

    async with httpx.AsyncClient(timeout=8, headers=HEADERS) as client:

        # ── Indian & Global Stocks ─────────────────────────────────────────
        stock_symbols = {
            "nifty":   "^NSEI",
            "sensex":  "^BSESN",
            "nasdaq":  "^IXIC",
            "sp500":   "^GSPC",
            "usdinr":  "INR=X",
        }
        stocks = {}
        for name, symbol in stock_symbols.items():
            try:
                r = await client.get(f"{YAHOO}{symbol}", params={"interval":"1d","range":"2d"})
                if r.status_code == 200:
                    stocks[name] = _yahoo_parse(r.json())
            except Exception as e:
                log.debug("Stock %s error: %s", name, e)
                stocks[name] = {"price": 0, "change": 0, "change_pct": 0}
        data["stocks"] = stocks

        # ── Commodities: Gold, Silver, Crude Oil ──────────────────────────
        commodity_symbols = {
            "gold":   "GC=F",    # Gold Futures (USD/oz)
            "silver": "SI=F",    # Silver Futures (USD/oz)
            "oil":    "CL=F",    # Crude Oil Futures (USD/bbl)
        }
        commodities = {}
        for name, symbol in commodity_symbols.items():
            try:
                r = await client.get(f"{YAHOO}{symbol}", params={"interval":"1d","range":"2d"})
                if r.status_code == 200:
                    parsed = _yahoo_parse(r.json())
                    # Convert gold/silver to INR (approximate: multiply by USD/INR rate)
                    usdinr = stocks.get("usdinr", {}).get("price", 84) or 84
                    if name in ("gold", "silver"):
                        parsed["inr"] = round(parsed["price"] * usdinr, 2)
                    commodities[name] = parsed
            except Exception as e:
                log.debug("Commodity %s error: %s", name, e)
                commodities[name] = {"price": 0, "change": 0, "change_pct": 0}
        data["commodities"] = commodities

        # ── Crypto — CoinGecko free API ────────────────────────────────────
        try:
            r = await client.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={
                    "ids": "bitcoin,ethereum,solana,dogecoin,binancecoin,ripple",
                    "vs_currencies": "inr,usd",
                    "include_24hr_change": "true",
                }
            )
            if r.status_code == 200:
                cg = r.json()
                data["crypto"] = {}
                for coin in ("bitcoin", "ethereum", "solana", "dogecoin", "binancecoin", "ripple"):
                    cd = cg.get(coin, {})
                    data["crypto"][coin] = {
                        "inr":        cd.get("inr", 0),
                        "usd":        cd.get("usd", 0),
                        "change_24h": round(cd.get("inr_24h_change") or cd.get("usd_24h_change") or 0, 2),
                    }
            else:
                data["crypto"] = {}
        except Exception as e:
            log.debug("Crypto fetch error: %s", e)
            data["crypto"] = {}

        # ── Weather for major Indian cities ────────────────────────────────
        if OPENWEATHER_KEY:
            cities = ["Mumbai", "Delhi", "Bengaluru", "Kochi", "Chennai", "Hyderabad"]
            weather = {}
            for city in cities[:5]:
                try:
                    r = await client.get(
                        "https://api.openweathermap.org/data/2.5/weather",
                        params={"q": city+",IN", "appid": OPENWEATHER_KEY, "units": "metric"},
                    )
                    if r.status_code == 200:
                        w = r.json()
                        weather[city] = {
                            "temp":        round(w["main"]["temp"]),
                            "feels_like":  round(w["main"]["feels_like"]),
                            "description": w["weather"][0]["description"].title(),
                            "humidity":    w["main"]["humidity"],
                            "icon":        w["weather"][0]["icon"],
                        }
                except Exception as e:
                    log.debug("Weather %s error: %s", city, e)
            data["weather"] = weather
        else:
            data["weather"] = {}

    return data

# ─────────────────────────────────────────────────────────────────────────────
# 11. LIFESPAN
# ─────────────────────────────────────────────────────────────────────────────
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    asyncio.create_task(collect_news())
    scheduler.add_job(collect_news,           "interval", minutes=30, id="collect")
    scheduler.add_job(update_trending_scores, "interval", hours=1,    id="trending")
    scheduler.start()
    log.info("Scheduler started — collect every 30 min | trending every 1 h")
    yield
    scheduler.shutdown()

# ─────────────────────────────────────────────────────────────────────────────
# 12. FASTAPI APP
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="SherByte API",
    version="3.0.0",
    description="AI-powered personalised news — India (Grok powered)",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────────────────────
# 13. ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    conn = get_conn()
    article_count = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    user_count    = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    conn.close()
    return {
        "status":    "ok",
        "version":   "3.0.0",
        "ai_engine": "grok",
        "articles":  article_count,
        "users":     user_count,
        "db":        DB_PATH.exists(),
    }

# ── Auth ──────────────────────────────────────────────────────────────────────
@app.post("/auth/register")
def register(req: RegisterRequest):
    conn = get_conn()
    if conn.execute("SELECT id FROM users WHERE email=?", (req.email.lower().strip(),)).fetchone():
        conn.close()
        raise HTTPException(400, "Email already registered")
    user_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO users (id,email,password_hash,display_name) VALUES (?,?,?,?)",
        (user_id, req.email.lower().strip(), hash_pw(req.password),
         req.display_name or req.email.split("@")[0]),
    )
    conn.commit(); conn.close()
    return {"token": make_token(user_id), "user_id": user_id}

@app.post("/auth/login")
def login(req: LoginRequest):
    conn = get_conn()
    user = conn.execute("SELECT * FROM users WHERE email=?", (req.email.lower().strip(),)).fetchone()
    conn.close()
    if not user or not verify_pw(req.password, user["password_hash"]):
        raise HTTPException(401, "Invalid email or password")
    return {"token": make_token(user["id"]), "user_id": user["id"],
            "display_name": user["display_name"]}

# ── Feed ──────────────────────────────────────────────────────────────────────
@app.get("/feed")
def get_feed(page: int = 1, user_id: Optional[str] = Depends(get_current_user)):
    conn      = get_conn()
    interests = {c: 0.5 for c in CATEGORIES}
    if user_id:
        row = conn.execute("SELECT interests FROM users WHERE id=?", (user_id,)).fetchone()
        if row:
            try: interests = json.loads(row["interests"])
            except: pass

    rows = conn.execute(
        "SELECT * FROM articles WHERE is_published=1 ORDER BY created_at DESC LIMIT 300"
    ).fetchall()
    conn.close()

    articles = [row_to_article(r) for r in rows]
    scored   = sorted(articles, key=lambda a: score_article(a, interests), reverse=True)

    per_page = 20
    start    = (page-1) * per_page
    return {"articles": scored[start:start+per_page], "page": page,
            "has_more": len(scored) > start+per_page, "total": len(scored)}

# ── Explore ───────────────────────────────────────────────────────────────────
@app.get("/explore")
def get_explore(category: Optional[str] = None, scope: Optional[str] = None, page: int = 1):
    conn = get_conn()
    conditions = ["is_published=1"]
    params: list = []

    if category and category in CATEGORIES:
        conditions.append("category=?"); params.append(category)

    # scope: local | national | global
    if scope and scope in ("local", "national", "global"):
        conditions.append("scope=?"); params.append(scope)

    where = " AND ".join(conditions)
    rows = conn.execute(
        f"SELECT * FROM articles WHERE {where} "
        "ORDER BY trending_score DESC, created_at DESC LIMIT 60",
        params,
    ).fetchall()
    conn.close()

    per_page = 20
    start    = (page-1) * per_page
    items    = [row_to_article(r) for r in rows]
    return {"articles": items[start:start+per_page], "page": page,
            "has_more": len(items) > start+per_page}

# ── Top / Breaking news ───────────────────────────────────────────────────────
@app.get("/top-news")
def get_top_news(limit: int = 5):
    """Returns top trending articles for the bulletin strip."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM articles WHERE is_published=1 "
        "ORDER BY trending_score DESC, created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return {"articles": [row_to_article(r) for r in rows]}

# ── Latest headlines ──────────────────────────────────────────────────────────
@app.get("/latest")
def get_latest(limit: int = 10):
    """Returns most recent articles for the 'Latest News' strip."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT id,title,source,category,published_at,image_url FROM articles "
        "WHERE is_published=1 ORDER BY created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return {"headlines": [dict(r) for r in rows]}

# ── Article detail ────────────────────────────────────────────────────────────
@app.get("/article/{article_id}")
def get_article(article_id: str):
    conn = get_conn()
    row  = conn.execute("SELECT * FROM articles WHERE id=?", (article_id,)).fetchone()
    if not row:
        conn.close(); raise HTTPException(404, "Article not found")
    conn.execute("UPDATE articles SET view_count=view_count+1 WHERE id=?", (article_id,))
    conn.commit(); conn.close()
    return row_to_article(row)

# ── Interact ──────────────────────────────────────────────────────────────────
@app.post("/interact")
def interact(req: InteractRequest, user_id: str = Depends(require_user)):
    conn = get_conn()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO interactions (user_id,article_id,action,duration_sec) VALUES (?,?,?,?)",
            (user_id, req.article_id, req.action, req.duration_sec),
        )
        if req.action == "like":
            conn.execute("UPDATE articles SET like_count=like_count+1 WHERE id=?", (req.article_id,))
        elif req.action == "save":
            conn.execute("UPDATE articles SET save_count=save_count+1 WHERE id=?", (req.article_id,))
        elif req.action == "read":
            conn.execute(
                "UPDATE users SET articles_read=articles_read+1, time_spent=time_spent+? WHERE id=?",
                (req.duration_sec, user_id)
            )
        update_interest(conn, user_id, req.category, req.action)
        conn.commit()
    except Exception as exc:
        log.warning("Interact error: %s", exc)
    finally:
        conn.close()
    return {"ok": True}

# ── Me ────────────────────────────────────────────────────────────────────────
@app.get("/me")
def get_me(user_id: str = Depends(require_user)):
    conn = get_conn()
    row  = conn.execute(
        "SELECT id,email,display_name,avatar_url,bio,links,interests,topics,"
        "language,streak,score,articles_read,time_spent FROM users WHERE id=?",
        (user_id,),
    ).fetchone()
    conn.close()
    if not row: raise HTTPException(404, "User not found")
    d = dict(row)
    for field in ("interests", "topics", "links"):
        try:    d[field] = json.loads(d[field] or "{}") if field != "topics" else json.loads(d[field] or "[]")
        except: d[field] = {} if field != "topics" else []
    return d

# ── Profile update ────────────────────────────────────────────────────────────
@app.put("/me")
def update_profile(req: ProfileUpdateRequest, user_id: str = Depends(require_user)):
    conn    = get_conn()
    updates = {}
    if req.display_name is not None: updates["display_name"] = req.display_name
    if req.bio          is not None: updates["bio"]          = req.bio
    if req.avatar_url   is not None: updates["avatar_url"]   = req.avatar_url
    if req.links        is not None: updates["links"]        = json.dumps(req.links)
    if req.language     is not None: updates["language"]     = req.language
    if updates:
        set_clause = ", ".join(f"{k}=?" for k in updates)
        conn.execute(
            f"UPDATE users SET {set_clause} WHERE id=?",
            list(updates.values()) + [user_id]
        )
        conn.commit()
    conn.close()
    return {"ok": True}

# ── Update topics / interests ─────────────────────────────────────────────────
@app.put("/me/topics")
def update_topics(req: TopicsUpdateRequest, user_id: str = Depends(require_user)):
    conn = get_conn()
    conn.execute("UPDATE users SET topics=? WHERE id=?", (json.dumps(req.topics), user_id))
    if req.interests:
        conn.execute("UPDATE users SET interests=? WHERE id=?",
                     (json.dumps(req.interests), user_id))
    conn.commit(); conn.close()
    return {"ok": True}

# ── Onboard ───────────────────────────────────────────────────────────────────
@app.post("/onboard")
def onboard(req: OnboardRequest, user_id: str = Depends(require_user)):
    conn = get_conn()
    conn.execute("UPDATE users SET interests=?, topics=? WHERE id=?",
                 (json.dumps(req.interests), json.dumps(req.topics), user_id))
    conn.commit(); conn.close()
    return {"ok": True}

# ── Bookmarks ─────────────────────────────────────────────────────────────────
@app.get("/bookmarks")
def get_bookmarks(user_id: str = Depends(require_user)):
    conn  = get_conn()
    saved = conn.execute(
        "SELECT article_id FROM interactions WHERE user_id=? AND action='save'", (user_id,)
    ).fetchall()
    articles = []
    for s in saved:
        row = conn.execute("SELECT * FROM articles WHERE id=?", (s["article_id"],)).fetchone()
        if row: articles.append(row_to_article(row))
    conn.close()
    return {"articles": articles}

# ── Search ────────────────────────────────────────────────────────────────────
@app.get("/search")
def search(q: str = ""):
    if not q.strip(): return {"articles": []}
    conn = get_conn()
    like = f"%{q}%"
    rows = conn.execute(
        "SELECT * FROM articles WHERE (title LIKE ? OR preview LIKE ?) AND is_published=1 LIMIT 30",
        (like, like),
    ).fetchall()
    conn.close()
    return {"articles": [row_to_article(r) for r in rows]}

# ── Leaderboard ───────────────────────────────────────────────────────────────
@app.get("/leaderboard")
def leaderboard():
    conn = get_conn()
    rows = conn.execute(
        "SELECT id,display_name,score,streak,articles_read FROM users ORDER BY score DESC LIMIT 20"
    ).fetchall()
    conn.close()
    return {"users": [dict(r) for r in rows]}

# ── Live market data ──────────────────────────────────────────────────────────
@app.get("/markets")
async def get_markets():
    """Live crypto prices + weather. Called by Explore page."""
    data = await fetch_live_markets()
    return data

# ── Categories metadata ───────────────────────────────────────────────────────
@app.get("/categories")
def get_categories():
    return {"categories": CATS_META}

# ── Topics list ───────────────────────────────────────────────────────────────
@app.get("/topics")
def get_topics():
    return {"topics": ALL_TOPICS}

# ── Admin ─────────────────────────────────────────────────────────────────────
@app.post("/admin/collect")
async def admin_collect():
    n = await collect_news()
    return {"collected": n}

@app.post("/admin/trending")
def admin_trending():
    update_trending_scores()
    return {"ok": True}

@app.put("/me/feed")
def update_feed_preferences(req: OnboardRequest, user_id: str = Depends(require_user)):
    """
    FEED Page — Edit button.
    Saves updated 7-category interest weights + topics in one call.
    Frontend sends: { interests: {tech:0.9, arts:0.3, ...}, topics: ["AI","Cricket",...] }
    """
    conn = get_conn()
    conn.execute(
        "UPDATE users SET interests=?, topics=? WHERE id=?",
        (json.dumps(req.interests), json.dumps(req.topics), user_id)
    )
    conn.commit(); conn.close()
    return {"ok": True, "message": "Feed preferences updated"}


# ── Notifications (stored in interactions table, action='notify') ─────────────
@app.get("/notifications")
def get_notifications(user_id: str = Depends(require_user)):
    """Returns notification-style items for the user."""
    conn    = get_conn()
    # Get recent saves + likes by this user as notification context
    recent  = conn.execute(
        """SELECT i.action, i.created_at, a.title, a.category, a.id as article_id
           FROM interactions i
           JOIN articles a ON i.article_id = a.id
           WHERE i.user_id=? AND i.action IN ('like','save','quiz_complete')
           ORDER BY i.created_at DESC LIMIT 20""",
        (user_id,)
    ).fetchall()
    # Also get latest trending articles as "recommendations"
    trending = conn.execute(
        "SELECT id,title,category,image_url,source FROM articles "
        "WHERE is_published=1 ORDER BY trending_score DESC LIMIT 5"
    ).fetchall()
    conn.close()
    return {
        "activity":     [dict(r) for r in recent],
        "trending_now": [dict(r) for r in trending],
    }


# ── Data & Storage stats ──────────────────────────────────────────────────────
@app.get("/me/stats")
def get_user_stats(user_id: str = Depends(require_user)):
    """Profile page — Data & Storage + Your Activity section."""
    conn = get_conn()
    user = conn.execute(
        "SELECT articles_read, time_spent, streak, score FROM users WHERE id=?",
        (user_id,)
    ).fetchone()
    saves = conn.execute(
        "SELECT COUNT(*) FROM interactions WHERE user_id=? AND action='save'", (user_id,)
    ).fetchone()[0]
    likes = conn.execute(
        "SELECT COUNT(*) FROM interactions WHERE user_id=? AND action='like'", (user_id,)
    ).fetchone()[0]
    quizzes = conn.execute(
        "SELECT COUNT(*) FROM interactions WHERE user_id=? AND action='quiz_complete'", (user_id,)
    ).fetchone()[0]
    # Category breakdown
    cats = conn.execute(
        """SELECT a.category, COUNT(*) as cnt
           FROM interactions i JOIN articles a ON i.article_id=a.id
           WHERE i.user_id=? AND i.action='read'
           GROUP BY a.category ORDER BY cnt DESC""",
        (user_id,)
    ).fetchall()
    conn.close()
    return {
        "articles_read":       user["articles_read"] if user else 0,
        "time_spent_minutes":  round((user["time_spent"] if user else 0) / 60, 1),
        "streak_days":         user["streak"] if user else 0,
        "score":               user["score"] if user else 0,
        "saved_count":         saves,
        "liked_count":         likes,
        "quizzes_completed":   quizzes,
        "category_breakdown":  {r["category"]: r["cnt"] for r in cats},
        "storage_mb":          round((saves + likes + quizzes) * 0.02, 1),
    }


@app.get("/admin/stats")
def admin_stats():
    conn = get_conn()
    cats = conn.execute(
        "SELECT category, COUNT(*) as cnt FROM articles GROUP BY category"
    ).fetchall()
    conn.close()
    return {"category_counts": {r["category"]: r["cnt"] for r in cats}}

# ─────────────────────────────────────────────────────────────────────────────
# 14. ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    print(f"""
╔══════════════════════════════════════════════╗
║       ⚡ SherByte Backend v3.0 (Grok)        ║
╠══════════════════════════════════════════════╣
║  Docs    → http://localhost:{port}/docs       ║
║  Health  → http://localhost:{port}/health     ║
║  Markets → http://localhost:{port}/markets    ║
╚══════════════════════════════════════════════╝
  AI Engine : Grok (llama-3.3-70b-versatile)
  Sources   : {len(RSS_FEEDS)} RSS feeds + NewsAPI
  Categories: {len(CATEGORIES)} VIBGYOR categories
""")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
