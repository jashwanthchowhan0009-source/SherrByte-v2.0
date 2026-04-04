import os, json, math, hashlib, asyncio, logging, uuid, sqlite3
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional
import httpx
import feedparser
from google import genai
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from jose import JWTError, jwt
from passlib.context import CryptContext
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

load_dotenv()
DB_PATH = Path("sherbyte.db")
GEMINI_KEY = os.getenv("GEMINI_API_KEY", "")

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    conn.executescript("CREATE TABLE IF NOT EXISTS articles (id TEXT PRIMARY KEY, title TEXT, preview TEXT, body_ai TEXT, category TEXT, source_url TEXT);")
    conn.commit()
    conn.close()

async def collect_news():
    feeds = ["https://feeds.feedburner.com/ndtvnews-top-stories"]
    async with httpx.AsyncClient() as client:
        for url in feeds:
            try:
                r = await client.get(url)
                f = feedparser.parse(r.text)
                conn = get_conn()
                for e in f.entries[:3]:
                    art_id = hashlib.md5(e.link.encode()).hexdigest()[:16]
                    conn.execute("INSERT OR IGNORE INTO articles (id, title, preview, source_url) VALUES (?,?,?,?)", (art_id, e.title, e.summary[:100], e.link))
                conn.commit()
                conn.close()
            except: pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    asyncio.create_task(collect_news())
    scheduler = AsyncIOScheduler()
    scheduler.add_job(collect_news, "interval", minutes=30)
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(title="SherByte API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health(): return {"status": "ok"}

@app.get("/feed")
def get_feed():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM articles LIMIT 10").fetchall()
    conn.close()
    return {"articles": [dict(r) for r in rows]}