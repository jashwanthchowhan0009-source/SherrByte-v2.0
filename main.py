"""
SherByte Backend — main.py
Run: pip install fastapi uvicorn httpx feedparser apscheduler python-jose[cryptography] passlib[bcrypt] python-dotenv google-generativeai python-multipart
Then: python main.py
"""

import os, json, time, math, hashlib, asyncio, logging
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from typing import Optional

import httpx
import feedparser
import google.generativeai as genai

from fastapi import FastAPI, HTTPException, Depends, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from jose import JWTError, jwt
from passlib.context import CryptContext
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

# ─── SQLite via stdlib (no SQLAlchemy needed) ───────────────────────────────
import sqlite3
from pathlib import Path

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("sherbyte")

# ─── CONFIG ──────────────────────────────────────────────────────────────────
GEMINI_KEY   = os.getenv("GEMINI_API_KEY", "")
NEWSAPI_KEY  = os.getenv("NEWSAPI_KEY", "")
JWT_SECRET   = os.getenv("JWT_SECRET", "sherbyte-local-secret-change-me")
JWT_ALG      = "HS256"
JWT_EXP_DAYS = 30
DB_PATH      = Path("sherbyte.db")

CATEGORIES = ["tech", "society", "economy", "nature", "arts", "selfwell", "philo"]

CATEGORY_KEYWORDS = {
    "tech":     ["technology", "AI", "artificial intelligence", "software", "startup", "digital", "cyber", "space", "science", "robot"],
    "society":  ["politics", "government", "election", "law", "court", "social", "education", "community", "police", "justice"],
    "economy":  ["economy", "market", "stock", "finance", "business", "trade", "GDP", "inflation", "investment", "startup"],
    "nature":   ["environment", "climate", "wildlife", "forest", "ocean", "nature", "biodiversity", "pollution", "renewable", "conservation"],
    "arts":     ["art", "film", "music", "culture", "festival", "theatre", "literature", "cinema", "dance", "heritage"],
    "selfwell": ["health", "wellness", "mental health", "fitness", "yoga", "medicine", "nutrition", "psychology", "mindfulness", "lifestyle"],
    "philo":    ["philosophy", "ethics", "ideas", "religion", "spirituality", "consciousness", "meaning", "wisdom", "thought", "debate"],
}

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
]

# ─── DB SETUP ────────────────────────────────────────────────────────────────
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS articles (
        id          TEXT PRIMARY KEY,
        title       TEXT NOT NULL,
        preview     TEXT,
        body_ai     TEXT,
        image_url   TEXT,
        category    TEXT NOT NULL DEFAULT 'tech',
        source      TEXT,
        source_url  TEXT,
        published_at TEXT,
        view_count  INTEGER DEFAULT 0,
        like_count  INTEGER DEFAULT 0,
        save_count  INTEGER DEFAULT 0,
        trending_score REAL DEFAULT 0.0,
        quiz        TEXT DEFAULT '[]',
        word_of_day TEXT DEFAULT '{}',
        is_published INTEGER DEFAULT 1,
        created_at  TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS users (
        id          TEXT PRIMARY KEY,
        email       TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        display_name TEXT,
        avatar_url  TEXT,
        interests   TEXT DEFAULT '{"tech":0.5,"society":0.5,"economy":0.5,"nature":0.5,"arts":0.5,"selfwell":0.5,"philo":0.5}',
        topics      TEXT DEFAULT '["AI","Climate","Finance","Cricket","Science"]',
        streak      INTEGER DEFAULT 0,
        score       INTEGER DEFAULT 0,
        created_at  TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS interactions (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id     TEXT NOT NULL,
        article_id  TEXT NOT NULL,
        action      TEXT NOT NULL,
        duration_sec INTEGER DEFAULT 0,
        created_at  TEXT DEFAULT (datetime('now')),
        UNIQUE(user_id, article_id, action)
    );

    CREATE INDEX IF NOT EXISTS idx_articles_cat ON articles(category);
    CREATE INDEX IF NOT EXISTS idx_articles_pub ON articles(published_at DESC);
    CREATE INDEX IF NOT EXISTS idx_articles_trend ON articles(trending_score DESC);
    CREATE INDEX IF NOT EXISTS idx_interactions_user ON interactions(user_id);
    """)
    conn.commit()
    conn.close()
    log.info("DB initialized")

# ─── AUTH ─────────────────────────────────────────────────────────────────────
pwd_ctx  = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer(auto_error=False)

def hash_pw(pw: str) -> str: return pwd_ctx.hash(pw)
def verify_pw(pw: str, h: str) -> bool: return pwd_ctx.verify(pw, h)

def make_token(user_id: str) -> str:
    exp = datetime.now(timezone.utc) + timedelta(days=JWT_EXP_DAYS)
    return jwt.encode({"sub": user_id, "exp": exp}, JWT_SECRET, algorithm=JWT_ALG)

def decode_token(token: str) -> Optional[str]:
    try:
        data = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
        return data.get("sub")
    except JWTError:
        return None

def get_current_user(creds: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> Optional[str]:
    if not creds: return None
    return decode_token(creds.credentials)

def require_user(creds: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> str:
    user_id = get_current_user(creds)
    if not user_id:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user_id

# ─── NEWS COLLECTION ──────────────────────────────────────────────────────────
def classify_category(title: str, body: str) -> str:
    text = (title + " " + (body or "")).lower()
    scores = {}
    for cat, keywords in CATEGORY_KEYWORDS.items():
        scores[cat] = sum(1 for kw in keywords if kw.lower() in text)
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "society"

def make_article_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:16]

async def fetch_newsapi(client: httpx.AsyncClient) -> list[dict]:
    if not NEWSAPI_KEY:
        log.warning("NEWSAPI_KEY not set, skipping NewsAPI")
        return []
    try:
        r = await client.get(
            "https://newsapi.org/v2/top-headlines",
            params={"country": "in", "pageSize": 30, "apiKey": NEWSAPI_KEY},
            timeout=10
        )
        if r.status_code != 200:
            return []
        data = r.json()
        articles = []
        for a in data.get("articles", []):
            if not a.get("url") or not a.get("title") or "[Removed]" in a.get("title",""):
                continue
            articles.append({
                "title": a["title"],
                "body": a.get("description") or a.get("content") or "",
                "image_url": a.get("urlToImage") or "",
                "source": a.get("source", {}).get("name", "NewsAPI"),
                "source_url": a["url"],
                "published_at": a.get("publishedAt", ""),
            })
        log.info(f"NewsAPI: {len(articles)} articles")
        return articles
    except Exception as e:
        log.warning(f"NewsAPI error: {e}")
        return []

async def fetch_rss(client: httpx.AsyncClient) -> list[dict]:
    articles = []
    for feed_url in RSS_FEEDS:
        try:
            r = await client.get(feed_url, timeout=8, follow_redirects=True)
            feed = feedparser.parse(r.text)
            for entry in feed.entries[:6]:
                url = entry.get("link", "")
                if not url: continue
                articles.append({
                    "title": entry.get("title", ""),
                    "body": entry.get("summary", "") or entry.get("description", ""),
                    "image_url": "",
                    "source": feed.feed.get("title", feed_url.split("/")[2]),
                    "source_url": url,
                    "published_at": entry.get("published", ""),
                })
        except Exception as e:
            log.debug(f"RSS {feed_url}: {e}")
    log.info(f"RSS: {len(articles)} articles")
    return articles

async def collect_news():
    log.info("[CRON] Starting news collection...")
    async with httpx.AsyncClient() as client:
        news_api, rss = await asyncio.gather(fetch_newsapi(client), fetch_rss(client))

    all_raw = news_api + rss
    conn = get_conn()
    c = conn.cursor()
    new_count = 0

    for raw in all_raw:
        if not raw.get("title") or not raw.get("source_url"):
            continue
        art_id = make_article_id(raw["source_url"])
        exists = c.execute("SELECT id FROM articles WHERE id=?", (art_id,)).fetchone()
        if exists:
            continue

        category = classify_category(raw["title"], raw["body"])
        body = raw["body"] or raw["title"]

        # AI rewrite if Gemini available, else use raw text
        preview, body_ai, quiz_json, word_json = await gemini_rewrite(raw["title"], body, category)

        c.execute("""
            INSERT OR IGNORE INTO articles
            (id, title, preview, body_ai, image_url, category, source, source_url, published_at, quiz, word_of_day)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            art_id, raw["title"], preview, body_ai,
            raw.get("image_url",""), category,
            raw.get("source",""), raw.get("source_url",""),
            raw.get("published_at",""),
            json.dumps(quiz_json), json.dumps(word_json)
        ))
        new_count += 1

    conn.commit()
    conn.close()
    log.info(f"[CRON] Collection done. {new_count} new articles saved.")
    return new_count

# ─── GEMINI REWRITE ───────────────────────────────────────────────────────────
async def gemini_rewrite(title: str, body: str, category: str):
    """Returns (preview, body_ai, quiz, word_of_day). Falls back to raw if Gemini fails."""
    # Default fallback
    preview_default = (body[:200] + "...") if len(body) > 200 else body
    body_default    = body or title
    quiz_default    = []
    word_default    = {}

    if not GEMINI_KEY:
        return preview_default, body_default, quiz_default, word_default

    try:
        genai.configure(api_key=GEMINI_KEY)
        model = genai.GenerativeModel("gemini-1.5-flash")
        prompt = f"""You are an AI news editor. Rewrite this article clearly and neutrally.

# --- Find this around Line 261 ---
app = FastAPI(
    title="SherByte API",
    version="2.0.0",
    description="AI-powered personalised news",
    lifespan=lifespan,
)

# --- PASTE THE CODE HERE ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://sherrbyte.web.app", "https://sherrbyte.firebaseapp.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

{{
  "preview": "60-word engaging summary",
  "body_ai": "150-180 word plain-language rewrite of the article",
  "quiz": [{{"question":"...", "options":["A","B","C","D"], "answer_index": 0}}],
  "word_of_day": {{"word":"...", "phonetic":"...", "definition":"...", "example":"..."}}
}}"""

        response = await asyncio.to_thread(model.generate_content, prompt)
        text = response.text.strip()
        # Strip markdown code fences if present
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        data = json.loads(text)
        return (
            data.get("preview", preview_default),
            data.get("body_ai", body_default),
            data.get("quiz", quiz_default),
            data.get("word_of_day", word_default),
        )
    except Exception as e:
        log.warning(f"Gemini rewrite failed ({e}), using raw text")
        return preview_default, body_default, quiz_default, word_default

# ─── TRENDING SCORE ───────────────────────────────────────────────────────────
def update_trending_scores():
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute("SELECT id, like_count, save_count, view_count, published_at FROM articles").fetchall()
    for row in rows:
        try:
            pub = datetime.fromisoformat(row["published_at"].replace("Z","")) if row["published_at"] else datetime.now()
            age_hours = max(1, (datetime.now() - pub.replace(tzinfo=None)).total_seconds() / 3600)
        except Exception:
            age_hours = 24
        score = (row["like_count"]*2 + row["save_count"]*3 + row["view_count"]*0.5) / age_hours
        c.execute("UPDATE articles SET trending_score=? WHERE id=?", (score, row["id"]))
    conn.commit()
    conn.close()

# ─── APP STARTUP ──────────────────────────────────────────────────────────────
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    # On first start, collect news immediately in background
    asyncio.create_task(collect_news())
    scheduler.add_job(collect_news, "interval", minutes=30, id="collect")
    scheduler.add_job(update_trending_scores, "interval", hours=2, id="trending")
    scheduler.start()
    log.info("Scheduler started: collect=30m, trending=2h")
    yield
    scheduler.shutdown()

app = FastAPI(
    title="SherByte API",
    version="2.0.0",
    description="AI-powered personalised news",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── PYDANTIC MODELS ──────────────────────────────────────────────────────────
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
    action: str  # read|like|save|skip|share|quiz_complete
    duration_sec: int = 0

class OnboardRequest(BaseModel):
    interests: dict  # {"tech": 0.8, ...}
    topics: list[str]

# ─── HELPERS ──────────────────────────────────────────────────────────────────
def row_to_article(row) -> dict:
    d = dict(row)
    try: d["quiz"] = json.loads(d.get("quiz") or "[]")
    except: d["quiz"] = []
    try: d["word_of_day"] = json.loads(d.get("word_of_day") or "{}")
    except: d["word_of_day"] = {}
    return d

def score_article(article: dict, interests: dict) -> float:
    try:
        pub = datetime.fromisoformat(article.get("published_at","").replace("Z",""))
        age_hours = max(1, (datetime.now() - pub.replace(tzinfo=None)).total_seconds() / 3600)
    except Exception:
        age_hours = 24
    interest = interests.get(article.get("category","tech"), 0.5)
    recency  = math.exp(-0.15 * age_hours)
    trending = (article.get("trending_score") or 0) / 100
    return interest*0.60 + recency*0.25 + trending*0.10

INTEREST_DELTAS = {"read":0.05,"like":0.10,"save":0.12,"share":0.08,"skip":-0.03,"quiz_complete":0.07}

def update_interest(conn, user_id: str, category: str, action: str):
    delta = INTEREST_DELTAS.get(action, 0)
    if delta == 0: return
    user = conn.execute("SELECT interests FROM users WHERE id=?", (user_id,)).fetchone()
    if not user: return
    try: interests = json.loads(user["interests"])
    except: interests = {}
    cur = interests.get(category, 0.5)
    interests[category] = round(max(0.05, min(1.0, cur + delta)), 3)
    conn.execute("UPDATE users SET interests=? WHERE id=?", (json.dumps(interests), user_id))

# ─── ROUTES ───────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "version": "2.0.0", "db": DB_PATH.exists()}

# ── AUTH ──
@app.post("/auth/register")
def register(req: RegisterRequest):
    conn = get_conn()
    existing = conn.execute("SELECT id FROM users WHERE email=?", (req.email,)).fetchone()
    if existing:
        conn.close()
        raise HTTPException(400, "Email already registered")
    import uuid
    user_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO users (id, email, password_hash, display_name) VALUES (?,?,?,?)",
        (user_id, req.email.lower().strip(), hash_pw(req.password), req.display_name or req.email.split("@")[0])
    )
    conn.commit()
    conn.close()
    return {"token": make_token(user_id), "user_id": user_id}

@app.post("/auth/login")
def login(req: LoginRequest):
    conn = get_conn()
    user = conn.execute("SELECT * FROM users WHERE email=?", (req.email.lower().strip(),)).fetchone()
    conn.close()
    if not user or not verify_pw(req.password, user["password_hash"]):
        raise HTTPException(401, "Invalid email or password")
    return {"token": make_token(user["id"]), "user_id": user["id"], "display_name": user["display_name"]}

# ── FEED ──
@app.get("/feed")
def get_feed(page: int = 1, user_id: Optional[str] = Depends(get_current_user)):
    conn = get_conn()
    interests = {"tech":0.5,"society":0.5,"economy":0.5,"nature":0.5,"arts":0.5,"selfwell":0.5,"philo":0.5}
    if user_id:
        u = conn.execute("SELECT interests FROM users WHERE id=?", (user_id,)).fetchone()
        if u:
            try: interests = json.loads(u["interests"])
            except: pass

    rows = conn.execute(
        "SELECT * FROM articles WHERE is_published=1 ORDER BY created_at DESC LIMIT 200"
    ).fetchall()
    conn.close()

    articles = [row_to_article(r) for r in rows]
    scored = sorted(articles, key=lambda a: score_article(a, interests), reverse=True)

    per_page = 20
    start = (page-1) * per_page
    page_items = scored[start:start+per_page]
    return {"articles": page_items, "page": page, "has_more": len(scored) > start+per_page}

# ── EXPLORE ──
@app.get("/explore")
def get_explore(category: Optional[str] = None, page: int = 1):
    conn = get_conn()
    if category and category in CATEGORIES:
        rows = conn.execute(
            "SELECT * FROM articles WHERE is_published=1 AND category=? ORDER BY trending_score DESC, created_at DESC LIMIT 40",
            (category,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM articles WHERE is_published=1 ORDER BY trending_score DESC, created_at DESC LIMIT 40"
        ).fetchall()
    conn.close()
    per_page = 20
    start = (page-1)*per_page
    items = [row_to_article(r) for r in rows]
    return {"articles": items[start:start+per_page], "page": page, "has_more": len(items) > start+per_page}

# ── ARTICLE DETAIL ──
@app.get("/article/{article_id}")
def get_article(article_id: str):
    conn = get_conn()
    row = conn.execute("SELECT * FROM articles WHERE id=?", (article_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Article not found")
    conn.execute("UPDATE articles SET view_count=view_count+1 WHERE id=?", (article_id,))
    conn.commit()
    conn.close()
    return row_to_article(row)

# ── INTERACT ──
@app.post("/interact")
def interact(req: InteractRequest, user_id: str = Depends(require_user)):
    conn = get_conn()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO interactions (user_id, article_id, action, duration_sec) VALUES (?,?,?,?)",
            (user_id, req.article_id, req.action, req.duration_sec)
        )
        # Update article counts
        if req.action == "like":
            conn.execute("UPDATE articles SET like_count=like_count+1 WHERE id=?", (req.article_id,))
        elif req.action == "save":
            conn.execute("UPDATE articles SET save_count=save_count+1 WHERE id=?", (req.article_id,))
        update_interest(conn, user_id, req.category, req.action)
        conn.commit()
    except Exception as e:
        log.warning(f"Interact error: {e}")
    finally:
        conn.close()
    return {"ok": True}

# ── ME ──
@app.get("/me")
def get_me(user_id: str = Depends(require_user)):
    conn = get_conn()
    user = conn.execute("SELECT id, email, display_name, avatar_url, interests, topics, streak, score FROM users WHERE id=?", (user_id,)).fetchone()
    conn.close()
    if not user:
        raise HTTPException(404, "User not found")
    d = dict(user)
    try: d["interests"] = json.loads(d["interests"])
    except: d["interests"] = {}
    try: d["topics"] = json.loads(d["topics"])
    except: d["topics"] = []
    return d

# ── ONBOARD ──
@app.post("/onboard")
def onboard(req: OnboardRequest, user_id: str = Depends(require_user)):
    conn = get_conn()
    conn.execute(
        "UPDATE users SET interests=?, topics=? WHERE id=?",
        (json.dumps(req.interests), json.dumps(req.topics), user_id)
    )
    conn.commit()
    conn.close()
    return {"ok": True}

# ── BOOKMARKS ──
@app.get("/bookmarks")
def get_bookmarks(user_id: str = Depends(require_user)):
    conn = get_conn()
    saved_ids = conn.execute(
        "SELECT article_id FROM interactions WHERE user_id=? AND action='save'", (user_id,)
    ).fetchall()
    articles = []
    for row in saved_ids:
        art = conn.execute("SELECT * FROM articles WHERE id=?", (row["article_id"],)).fetchone()
        if art: articles.append(row_to_article(art))
    conn.close()
    return {"articles": articles}

# ── SEARCH ──
@app.get("/search")
def search(q: str = ""):
    if not q.strip():
        return {"articles": []}
    conn = get_conn()
    like = f"%{q}%"
    rows = conn.execute(
        "SELECT * FROM articles WHERE (title LIKE ? OR preview LIKE ?) AND is_published=1 LIMIT 20",
        (like, like)
    ).fetchall()
    conn.close()
    return {"articles": [row_to_article(r) for r in rows]}

# ── LEADERBOARD ──
@app.get("/leaderboard")
def leaderboard():
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, display_name, score, streak FROM users ORDER BY score DESC LIMIT 20"
    ).fetchall()
    conn.close()
    return {"users": [dict(r) for r in rows]}

# ── ADMIN (manual trigger) ──
@app.post("/admin/collect")
async def admin_collect():
    n = await collect_news()
    return {"collected": n}

@app.post("/admin/trending")
def admin_trending():
    update_trending_scores()
    return {"ok": True}

# ─── RUN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    print("""
╔══════════════════════════════════════════╗
║         ⚡ SherByte Backend v2.0         ║
╠══════════════════════════════════════════╣
║  API Docs → http://localhost:8000/docs   ║
║  Health   → http://localhost:8000/health ║
╚══════════════════════════════════════════╝

Required .env keys:
  GEMINI_API_KEY  → from aistudio.google.com
  NEWSAPI_KEY     → from newsapi.org (optional, RSS works without it)
  JWT_SECRET      → any random string (used for tokens)
""")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
