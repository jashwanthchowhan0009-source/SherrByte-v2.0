"""
SherByte Backend — main.py  v2.1  (PRODUCTION READY)
=====================================================
Install : pip install -r requirements.txt
Run     : python main.py
Deploy  : push to GitHub → Render picks up render.yaml automatically

Environment variables required on Render:
  GEMINI_API_KEY   → https://aistudio.google.com   (required)
  JWT_SECRET       → any long random string         (required)
  NEWSAPI_KEY      → https://newsapi.org            (optional, RSS works without it)
"""

# ─── stdlib ───────────────────────────────────────────────────────────────────
import os
import json
import math
import hashlib
import asyncio
import logging
import uuid
import sqlite3
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

# ─── third-party ──────────────────────────────────────────────────────────────
import httpx
import feedparser
from google import genai                          # pip install google-genai

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from jose import JWTError, jwt                    # pip install python-jose[cryptography]
from passlib.context import CryptContext          # pip install passlib[bcrypt]
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv                    # pip install python-dotenv

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1 — BOOTSTRAP
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("sherbyte")

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2 — CONFIG  (read all env vars in one place)
# ─────────────────────────────────────────────────────────────────────────────
GEMINI_KEY   = os.getenv("GEMINI_API_KEY", "")
NEWSAPI_KEY  = os.getenv("NEWSAPI_KEY", "")
JWT_SECRET   = os.getenv("JWT_SECRET", "sherbyte-local-dev-secret-CHANGE-ME")
JWT_ALG      = "HS256"
JWT_EXP_DAYS = 30
DB_PATH      = Path("sherbyte.db")

CATEGORIES = ["tech", "society", "economy", "nature", "arts", "selfwell", "philo"]

CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "tech":     ["technology", "ai", "artificial intelligence", "software", "startup",
                 "digital", "cyber", "space", "science", "robot", "chip", "quantum"],
    "society":  ["politics", "government", "election", "law", "court", "social",
                 "education", "community", "police", "justice", "protest", "vote"],
    "economy":  ["economy", "market", "stock", "finance", "business", "trade",
                 "gdp", "inflation", "investment", "bank", "rupee", "sensex", "nifty"],
    "nature":   ["environment", "climate", "wildlife", "forest", "ocean", "nature",
                 "biodiversity", "pollution", "renewable", "conservation", "tiger", "river"],
    "arts":     ["art", "film", "music", "culture", "festival", "theatre",
                 "literature", "cinema", "dance", "heritage", "bollywood", "award"],
    "selfwell": ["health", "wellness", "mental health", "fitness", "yoga",
                 "medicine", "nutrition", "psychology", "mindfulness", "hospital", "diet"],
    "philo":    ["philosophy", "ethics", "ideas", "religion", "spirituality",
                 "consciousness", "meaning", "wisdom", "thought", "debate", "moral"],
}

# Plain URLs — NO markdown formatting
RSS_FEEDS: list[str] = [
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
]

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3 — DATABASE
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
            category       TEXT NOT NULL DEFAULT 'tech',
            source         TEXT,
            source_url     TEXT,
            published_at   TEXT,
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
            interests     TEXT DEFAULT '{"tech":0.5,"society":0.5,"economy":0.5,"nature":0.5,"arts":0.5,"selfwell":0.5,"philo":0.5}',
            topics        TEXT DEFAULT '["AI","Climate","Finance","Cricket","Science"]',
            streak        INTEGER DEFAULT 0,
            score         INTEGER DEFAULT 0,
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

        CREATE INDEX IF NOT EXISTS idx_art_cat   ON articles(category);
        CREATE INDEX IF NOT EXISTS idx_art_pub   ON articles(published_at DESC);
        CREATE INDEX IF NOT EXISTS idx_art_trend ON articles(trending_score DESC);
        CREATE INDEX IF NOT EXISTS idx_int_user  ON interactions(user_id);
    """)
    conn.commit()
    conn.close()
    log.info("DB ready → %s", DB_PATH.resolve())

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4 — AUTH HELPERS
# ─────────────────────────────────────────────────────────────────────────────
pwd_ctx  = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer(auto_error=False)


def hash_pw(pw: str) -> str:
    return pwd_ctx.hash(pw)


def verify_pw(pw: str, hashed: str) -> bool:
    return pwd_ctx.verify(pw, hashed)


def make_token(user_id: str) -> str:
    exp = datetime.now(timezone.utc) + timedelta(days=JWT_EXP_DAYS)
    return jwt.encode({"sub": user_id, "exp": exp}, JWT_SECRET, algorithm=JWT_ALG)


def decode_token(token: str) -> Optional[str]:
    try:
        data = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
        return data.get("sub")
    except JWTError:
        return None


def get_current_user(
    creds: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> Optional[str]:
    return decode_token(creds.credentials) if creds else None


def require_user(
    creds: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> str:
    uid = get_current_user(creds)
    if not uid:
        raise HTTPException(status_code=401, detail="Authentication required")
    return uid

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 5 — PYDANTIC MODELS
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
    action: str          # read | like | save | skip | share | quiz_complete
    duration_sec: int = 0


class OnboardRequest(BaseModel):
    interests: dict
    topics: list

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6 — ARTICLE HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def row_to_article(row) -> dict:
    d = dict(row)
    try:
        d["quiz"] = json.loads(d.get("quiz") or "[]")
    except Exception:
        d["quiz"] = []
    try:
        d["word_of_day"] = json.loads(d.get("word_of_day") or "{}")
    except Exception:
        d["word_of_day"] = {}
    return d


def score_article(article: dict, interests: dict) -> float:
    try:
        pub_str = (article.get("published_at") or "").replace("Z", "+00:00")
        pub = datetime.fromisoformat(pub_str)
        age_hours = max(1.0, (datetime.now(timezone.utc) - pub).total_seconds() / 3600)
    except Exception:
        age_hours = 24.0
    interest = interests.get(article.get("category", "tech"), 0.5)
    recency  = math.exp(-0.15 * age_hours)
    trending = min((article.get("trending_score") or 0.0) / 100.0, 1.0)
    return interest * 0.60 + recency * 0.25 + trending * 0.10


INTEREST_DELTAS: dict[str, float] = {
    "read": 0.05, "like": 0.10, "save": 0.12,
    "share": 0.08, "skip": -0.03, "quiz_complete": 0.07,
}


def update_interest(
    conn: sqlite3.Connection, user_id: str, category: str, action: str
) -> None:
    delta = INTEREST_DELTAS.get(action, 0.0)
    if delta == 0.0:
        return
    row = conn.execute("SELECT interests FROM users WHERE id=?", (user_id,)).fetchone()
    if not row:
        return
    try:
        interests = json.loads(row["interests"])
    except Exception:
        interests = {}
    cur = interests.get(category, 0.5)
    interests[category] = round(max(0.05, min(1.0, cur + delta)), 3)
    conn.execute(
        "UPDATE users SET interests=? WHERE id=?",
        (json.dumps(interests), user_id),
    )

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 7 — NEWS COLLECTION  (RSS + NewsAPI)
# ─────────────────────────────────────────────────────────────────────────────
def classify_category(title: str, body: str) -> str:
    text = (title + " " + (body or "")).lower()
    scores = {
        cat: sum(1 for kw in kws if kw in text)
        for cat, kws in CATEGORY_KEYWORDS.items()
    }
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "society"


def make_article_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:16]


async def fetch_newsapi(client: httpx.AsyncClient) -> list:
    if not NEWSAPI_KEY:
        log.info("NEWSAPI_KEY not set — using RSS only")
        return []
    try:
        r = await client.get(
            "https://newsapi.org/v2/top-headlines",
            params={"country": "in", "pageSize": 30, "apiKey": NEWSAPI_KEY},
            timeout=10,
        )
        if r.status_code != 200:
            log.warning("NewsAPI returned %d", r.status_code)
            return []
        result = []
        for a in r.json().get("articles", []):
            if (
                not a.get("url")
                or not a.get("title")
                or "[Removed]" in a.get("title", "")
            ):
                continue
            result.append({
                "title":        a["title"],
                "body":         a.get("description") or a.get("content") or "",
                "image_url":    a.get("urlToImage") or "",
                "source":       a.get("source", {}).get("name", "NewsAPI"),
                "source_url":   a["url"],
                "published_at": a.get("publishedAt", ""),
            })
        log.info("NewsAPI → %d articles", len(result))
        return result
    except Exception as exc:
        log.warning("NewsAPI fetch error: %s", exc)
        return []


async def fetch_rss(client: httpx.AsyncClient) -> list:
    result = []
    for feed_url in RSS_FEEDS:
        try:
            r    = await client.get(feed_url, timeout=8, follow_redirects=True)
            feed = feedparser.parse(r.text)
            for entry in feed.entries[:6]:
                url = entry.get("link", "")
                if not url:
                    continue
                result.append({
                    "title":        entry.get("title", ""),
                    "body":         entry.get("summary", "") or entry.get("description", ""),
                    "image_url":    "",
                    "source":       feed.feed.get("title", feed_url.split("/")[2]),
                    "source_url":   url,
                    "published_at": entry.get("published", ""),
                })
        except Exception as exc:
            log.debug("RSS skip %s: %s", feed_url, exc)
    log.info("RSS → %d articles", len(result))
    return result

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 8 — GEMINI AI REWRITE  (google-genai SDK)
# ─────────────────────────────────────────────────────────────────────────────
async def gemini_rewrite(title: str, body: str, category: str):
    """
    Returns (preview, body_ai, quiz, word_of_day).
    Silently falls back to raw text if Gemini key is missing or API fails.
    """
    preview_default = (body[:200] + "...") if len(body) > 200 else (body or title)
    body_default    = body or title
    quiz_default    = []
    word_default    = {}

    if not GEMINI_KEY:
        return preview_default, body_default, quiz_default, word_default

    try:
        client = genai.Client(api_key=GEMINI_KEY)
        prompt = (
            "You are an AI news editor writing for an Indian audience. "
            "Rewrite the article below clearly and neutrally.\n\n"
            f"Title: {title}\n"
            f"Body: {body[:800]}\n"
            f"Category: {category}\n\n"
            "Reply with ONLY a single valid JSON object — no markdown, no code fences:\n"
            '{"preview":"60-word engaging summary of the article",'
            '"body_ai":"150-180 word plain-English rewrite",'
            '"quiz":[{"question":"A question about this article",'
            '"options":["Option A","Option B","Option C","Option D"],'
            '"answer_index":0}],'
            '"word_of_day":{"word":"relevant vocabulary word",'
            '"phonetic":"/fəˈnetɪk/","definition":"its meaning",'
            '"example":"a sentence using the word"}}'
        )

        response = await asyncio.to_thread(
            client.models.generate_content,
            model="gemini-2.0-flash",
            contents=prompt,
        )

        text = response.text.strip()
        # Strip markdown code fences if Gemini wraps response
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
        log.warning("Gemini rewrite failed (%s) — using raw text", exc)
        return preview_default, body_default, quiz_default, word_default

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 9 — PIPELINE TASKS
# ─────────────────────────────────────────────────────────────────────────────
async def collect_news() -> int:
    """Fetch from all sources, AI-rewrite new articles, save to DB."""
    log.info("[CRON] Starting news collection...")
    async with httpx.AsyncClient() as client:
        news_api_arts, rss_arts = await asyncio.gather(
            fetch_newsapi(client),
            fetch_rss(client),
        )

    all_raw   = news_api_arts + rss_arts
    conn      = get_conn()
    new_count = 0

    for raw in all_raw:
        if not raw.get("title") or not raw.get("source_url"):
            continue

        art_id = make_article_id(raw["source_url"])
        if conn.execute("SELECT id FROM articles WHERE id=?", (art_id,)).fetchone():
            continue  # already in DB

        category = classify_category(raw["title"], raw.get("body", ""))
        body     = raw.get("body") or raw["title"]

        preview, body_ai, quiz_json, word_json = await gemini_rewrite(
            raw["title"], body, category
        )

        conn.execute(
            """INSERT OR IGNORE INTO articles
               (id, title, preview, body_ai, image_url, category,
                source, source_url, published_at, quiz, word_of_day)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                art_id,
                raw["title"],
                preview,
                body_ai,
                raw.get("image_url", ""),
                category,
                raw.get("source", ""),
                raw.get("source_url", ""),
                raw.get("published_at", ""),
                json.dumps(quiz_json),
                json.dumps(word_json),
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
        "SELECT id, like_count, save_count, view_count, published_at FROM articles"
    ).fetchall()
    for row in rows:
        try:
            pub_str   = (row["published_at"] or "").replace("Z", "+00:00")
            pub       = datetime.fromisoformat(pub_str) if pub_str else datetime.now(timezone.utc)
            age_hours = max(1.0, (datetime.now(timezone.utc) - pub).total_seconds() / 3600)
        except Exception:
            age_hours = 24.0
        score = (
            row["like_count"] * 2
            + row["save_count"] * 3
            + row["view_count"] * 0.5
        ) / age_hours
        conn.execute(
            "UPDATE articles SET trending_score=? WHERE id=?", (score, row["id"])
        )
    conn.commit()
    conn.close()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 10 — LIFESPAN  (startup + shutdown hooks)
# ─────────────────────────────────────────────────────────────────────────────
scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Create DB tables if not exist
    init_db()
    # 2. Collect news immediately on startup (background task)
    asyncio.create_task(collect_news())
    # 3. Schedule recurring jobs
    scheduler.add_job(collect_news,           "interval", minutes=30, id="collect")
    scheduler.add_job(update_trending_scores, "interval", hours=2,    id="trending")
    scheduler.start()
    log.info("Scheduler started — collect every 30 min | trending every 2 h")
    yield                          # ← app runs here
    scheduler.shutdown()           # ← called on Render shutdown

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 11 — FASTAPI APP + MIDDLEWARE
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="SherByte API",
    version="2.1.0",
    description="AI-powered personalised news — India",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],           # tighten to your Firebase domain in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 12 — ROUTES
# ─────────────────────────────────────────────────────────────────────────────

# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "version": "2.1.0", "db": DB_PATH.exists()}


# ── Auth ──────────────────────────────────────────────────────────────────────
@app.post("/auth/register")
def register(req: RegisterRequest):
    conn = get_conn()
    if conn.execute(
        "SELECT id FROM users WHERE email=?", (req.email.lower().strip(),)
    ).fetchone():
        conn.close()
        raise HTTPException(400, "Email already registered")
    user_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO users (id, email, password_hash, display_name) VALUES (?,?,?,?)",
        (
            user_id,
            req.email.lower().strip(),
            hash_pw(req.password),
            req.display_name or req.email.split("@")[0],
        ),
    )
    conn.commit()
    conn.close()
    return {"token": make_token(user_id), "user_id": user_id}


@app.post("/auth/login")
def login(req: LoginRequest):
    conn = get_conn()
    user = conn.execute(
        "SELECT * FROM users WHERE email=?", (req.email.lower().strip(),)
    ).fetchone()
    conn.close()
    if not user or not verify_pw(req.password, user["password_hash"]):
        raise HTTPException(401, "Invalid email or password")
    return {
        "token":        make_token(user["id"]),
        "user_id":      user["id"],
        "display_name": user["display_name"],
    }


# ── Feed ──────────────────────────────────────────────────────────────────────
@app.get("/feed")
def get_feed(
    page: int = 1,
    user_id: Optional[str] = Depends(get_current_user),
):
    conn      = get_conn()
    interests = {c: 0.5 for c in CATEGORIES}
    if user_id:
        row = conn.execute(
            "SELECT interests FROM users WHERE id=?", (user_id,)
        ).fetchone()
        if row:
            try:
                interests = json.loads(row["interests"])
            except Exception:
                pass

    rows = conn.execute(
        "SELECT * FROM articles WHERE is_published=1 ORDER BY created_at DESC LIMIT 200"
    ).fetchall()
    conn.close()

    articles = [row_to_article(r) for r in rows]
    scored   = sorted(articles, key=lambda a: score_article(a, interests), reverse=True)

    per_page = 20
    start    = (page - 1) * per_page
    return {
        "articles": scored[start : start + per_page],
        "page":     page,
        "has_more": len(scored) > start + per_page,
    }


# ── Explore ───────────────────────────────────────────────────────────────────
@app.get("/explore")
def get_explore(category: Optional[str] = None, page: int = 1):
    conn = get_conn()
    if category and category in CATEGORIES:
        rows = conn.execute(
            "SELECT * FROM articles WHERE is_published=1 AND category=? "
            "ORDER BY trending_score DESC, created_at DESC LIMIT 40",
            (category,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM articles WHERE is_published=1 "
            "ORDER BY trending_score DESC, created_at DESC LIMIT 40"
        ).fetchall()
    conn.close()

    per_page = 20
    start    = (page - 1) * per_page
    items    = [row_to_article(r) for r in rows]
    return {
        "articles": items[start : start + per_page],
        "page":     page,
        "has_more": len(items) > start + per_page,
    }


# ── Article detail ────────────────────────────────────────────────────────────
@app.get("/article/{article_id}")
def get_article(article_id: str):
    conn = get_conn()
    row  = conn.execute(
        "SELECT * FROM articles WHERE id=?", (article_id,)
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Article not found")
    conn.execute(
        "UPDATE articles SET view_count=view_count+1 WHERE id=?", (article_id,)
    )
    conn.commit()
    conn.close()
    return row_to_article(row)


# ── Interact ──────────────────────────────────────────────────────────────────
@app.post("/interact")
def interact(req: InteractRequest, user_id: str = Depends(require_user)):
    conn = get_conn()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO interactions "
            "(user_id, article_id, action, duration_sec) VALUES (?,?,?,?)",
            (user_id, req.article_id, req.action, req.duration_sec),
        )
        if req.action == "like":
            conn.execute(
                "UPDATE articles SET like_count=like_count+1 WHERE id=?",
                (req.article_id,),
            )
        elif req.action == "save":
            conn.execute(
                "UPDATE articles SET save_count=save_count+1 WHERE id=?",
                (req.article_id,),
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
        "SELECT id, email, display_name, avatar_url, interests, topics, streak, score "
        "FROM users WHERE id=?",
        (user_id,),
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "User not found")
    d = dict(row)
    try:
        d["interests"] = json.loads(d["interests"])
    except Exception:
        d["interests"] = {}
    try:
        d["topics"] = json.loads(d["topics"])
    except Exception:
        d["topics"] = []
    return d


# ── Onboard ───────────────────────────────────────────────────────────────────
@app.post("/onboard")
def onboard(req: OnboardRequest, user_id: str = Depends(require_user)):
    conn = get_conn()
    conn.execute(
        "UPDATE users SET interests=?, topics=? WHERE id=?",
        (json.dumps(req.interests), json.dumps(req.topics), user_id),
    )
    conn.commit()
    conn.close()
    return {"ok": True}


# ── Bookmarks ─────────────────────────────────────────────────────────────────
@app.get("/bookmarks")
def get_bookmarks(user_id: str = Depends(require_user)):
    conn     = get_conn()
    saved    = conn.execute(
        "SELECT article_id FROM interactions WHERE user_id=? AND action='save'",
        (user_id,),
    ).fetchall()
    articles = []
    for s in saved:
        row = conn.execute(
            "SELECT * FROM articles WHERE id=?", (s["article_id"],)
        ).fetchone()
        if row:
            articles.append(row_to_article(row))
    conn.close()
    return {"articles": articles}


# ── Search ────────────────────────────────────────────────────────────────────
@app.get("/search")
def search(q: str = ""):
    if not q.strip():
        return {"articles": []}
    conn = get_conn()
    like = f"%{q}%"
    rows = conn.execute(
        "SELECT * FROM articles "
        "WHERE (title LIKE ? OR preview LIKE ?) AND is_published=1 LIMIT 20",
        (like, like),
    ).fetchall()
    conn.close()
    return {"articles": [row_to_article(r) for r in rows]}


# ── Leaderboard ───────────────────────────────────────────────────────────────
@app.get("/leaderboard")
def leaderboard():
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, display_name, score, streak FROM users ORDER BY score DESC LIMIT 20"
    ).fetchall()
    conn.close()
    return {"users": [dict(r) for r in rows]}


# ── Admin ─────────────────────────────────────────────────────────────────────
@app.post("/admin/collect")
async def admin_collect():
    n = await collect_news()
    return {"collected": n}


@app.post("/admin/trending")
def admin_trending():
    update_trending_scores()
    return {"ok": True}


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 13 — ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    print(f"""
╔══════════════════════════════════════════╗
║        ⚡ SherByte Backend v2.1          ║
╠══════════════════════════════════════════╣
║  Docs   → http://localhost:{port}/docs    ║
║  Health → http://localhost:{port}/health  ║
╚══════════════════════════════════════════╝
    """)
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
