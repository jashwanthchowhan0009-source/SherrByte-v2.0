"""
SherByte v20 - Python Flask Backend
Deploy in 2-3 hours with: pip install flask flask-cors && python app.py
"""

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import json, os, time
from datetime import datetime, timedelta

app = Flask(__name__, static_folder='static', static_url_path='')
CORS(app)

# ─────────────────────────────────────────
#  IN-MEMORY DATA STORE (swap with DB later)
# ─────────────────────────────────────────
DB = {
    "users": {
        "jashwanth1621": {
            "id": "jashwanth1621",
            "name": "Jashwanth Chowhan",
            "handle": "@jashwanth1621",
            "verified": True,
            "avatar": "https://i.pravatar.cc/200?img=12",
            "bio": "Tech enthusiast & news junkie. Building things. 🇮🇳",
            "followers": 128,
            "following": 45,
            "friends": 18,
            "saved": [],
            "liked": [],
            "settings": {
                "theme": "dark",
                "notifications": True,
                "email_digest": False,
                "privacy": "friends",
                "location_in_posts": False,
            }
        }
    },
    "articles": [
        {"id": 1,  "cat": "tech",    "breaking": True,  "src": "The Wire Science",  "time": "2h",  "title": "India's quantum leap: IIT Delhi achieves 127-qubit milestone, enters global race",  "text": "In a landmark achievement for Indian science, researchers at IIT Delhi have demonstrated a 127-qubit quantum processor capable of running error-corrected algorithms at room temperature.",  "img": "https://picsum.photos/seed/1/400/280"},
        {"id": 2,  "cat": "arts",    "breaking": True,  "src": "The Hindu Arts",    "time": "3h",  "title": "Anurag Kashyap's new film premieres at Cannes to standing ovation",              "text": "The veteran filmmaker returns to his roots with a sweeping three-hour epic set against the backdrop of 1984 Punjab.",                                                                     "img": "https://picsum.photos/seed/2/400/280"},
        {"id": 3,  "cat": "selfwell","breaking": True,  "src": "TOI Wellness",      "time": "4h",  "title": "AIIMS Delhi study: Daily 20-minute walk reduces dementia risk by 40%",             "text": "A landmark five-year study tracking 12,000 Indians aged 50+ has found that moderate daily exercise dramatically reduces the risk of cognitive decline.",                               "img": "https://picsum.photos/seed/3/400/280"},
        {"id": 4,  "cat": "economy", "breaking": True,  "src": "Mint",              "time": "5h",  "title": "India's GDP growth hits 8.4% — fastest among G20 nations for third consecutive quarter","text": "The latest NSO data confirms India's position as the world's fastest-growing major economy, driven by manufacturing and digital services.",                                        "img": "https://picsum.photos/seed/4/400/280"},
        {"id": 5,  "cat": "nature",  "breaking": False, "src": "Down to Earth",     "time": "6h",  "title": "Western Ghats tiger corridor restored after 30 years of fragmentation",           "text": "A decade-long effort by forest departments across Karnataka, Kerala, and Tamil Nadu has successfully reconnected two major tiger habitats.",                                           "img": "https://picsum.photos/seed/5/400/280"},
        {"id": 6,  "cat": "society", "breaking": False, "src": "The Hindu",         "time": "7h",  "title": "Supreme Court mandates free wifi in all government schools by 2026",              "text": "In a historic ruling, the Supreme Court has directed the Union government to ensure high-speed internet connectivity in all 1.4 million government schools.",                         "img": "https://picsum.photos/seed/6/400/280"},
        {"id": 7,  "cat": "philo",   "breaking": False, "src": "Aeon India",        "time": "8h",  "title": "The paradox of choice: Why more options make us less happy",                     "text": "Philosopher Barry Schwartz's ideas about decision paralysis take on new meaning in the age of infinite streaming and infinite scroll.",                                               "img": "https://picsum.photos/seed/7/400/280"},
        {"id": 8,  "cat": "tech",    "breaking": False, "src": "ET Tech",           "time": "9h",  "title": "Jio launches AI-powered vernacular news aggregator — 22 languages",              "text": "Reliance Jio has unveiled a revolutionary news platform that uses AI to translate, summarize, and curate news content across all 22 official Indian languages.",                     "img": "https://picsum.photos/seed/8/400/280"},
        {"id": 9,  "cat": "arts",    "breaking": False, "src": "Firstpost",         "time": "10h", "title": "The resurgence of Indian classical music among Gen Z",                           "text": "Streaming data reveals a surprising trend: classical ragas are among the fastest-growing content categories for listeners aged 18-24.",                                              "img": "https://picsum.photos/seed/9/400/280"},
        {"id": 10, "cat": "selfwell","breaking": False, "src": "NDTV Health",       "time": "11h", "title": "Yoga nidra: The ancient practice being adopted by NASA astronauts",              "text": "The Indian sleep meditation technique is being studied by NASA for its potential to help astronauts maintain mental health during long-duration space missions.",                    "img": "https://picsum.photos/seed/10/400/280"},
        {"id": 11, "cat": "economy", "breaking": False, "src": "Business Today",    "time": "12h", "title": "Electric vehicle sales overtake petrol cars in India for first time",            "text": "March 2026 marked a watershed moment for India's automotive industry: EV sales surpassed internal combustion engine vehicles for the first time.",                                  "img": "https://picsum.photos/seed/11/400/280"},
        {"id": 12, "cat": "nature",  "breaking": False, "src": "Mongabay India",    "time": "14h", "title": "Gangetic dolphin population rebounds by 35% after decade of conservation",      "text": "The national aquatic animal of India, once critically endangered, has shown remarkable recovery following riverside cleanup and fishing regulation.",                                "img": "https://picsum.photos/seed/12/400/280"},
        {"id": 13, "cat": "society", "breaking": False, "src": "Indian Express",    "time": "15h", "title": "India's urban farming revolution: Rooftop gardens feed a million families",    "text": "A comprehensive survey reveals that urban farming initiatives have expanded dramatically across 50 Indian cities.",                                                                   "img": "https://picsum.photos/seed/13/400/280"},
        {"id": 14, "cat": "philo",   "breaking": False, "src": "Scroll.in",         "time": "16h", "title": "Can AI have consciousness? Indian philosophers offer ancient answers",          "text": "The Advaita Vedanta concept of universal consciousness offers a unique lens through which to examine questions about artificial sentience.",                                        "img": "https://picsum.photos/seed/14/400/280"},
        {"id": 15, "cat": "tech",    "breaking": False, "src": "MediaNama",         "time": "18h", "title": "India Digital Personal Data Protection Act: Six months on",                    "text": "An assessment of how India's landmark data privacy legislation is being implemented, with reports of both compliance successes and significant gaps.",                              "img": "https://picsum.photos/seed/15/400/280"},
    ],
    "notifications": [
        {"id": 1, "title": "ISRO launches crew module — India becomes 4th nation to send humans to space", "time": "2 min", "img": "https://picsum.photos/seed/21/400/280"},
        {"id": 2, "title": "India surpasses Germany — officially world's third largest economy",          "time": "15 min","img": "https://picsum.photos/seed/22/400/280"},
        {"id": 3, "title": "Tiger population hits 3,682 — a 60% increase since Project Tiger began",      "time": "1 hr",  "img": "https://picsum.photos/seed/23/400/280"},
        {"id": 4, "title": "IIT Bombay diabetes drug shows 90% success rate in Phase 3 trial",            "time": "2 hr",  "img": "https://picsum.photos/seed/24/400/280"},
    ],
    "market": {
        "NIFTY": {"value": 22580, "change": "+1.1%", "up": True},
        "SENSEX": {"value": 73088, "change": "+0.6%", "up": True},
        "GOLD": {"value": 72450, "change": "+0.4%", "unit": "₹/10g", "up": True},
        "SILVER": {"value": 88200, "change": "-0.2%", "unit": "₹/kg", "up": False},
        "USD_INR": {"value": 83.42, "change": "-0.1%", "up": False},
        "CRUDE": {"value": 82.4, "change": "-1.2%", "unit": "$/bbl", "up": False},
    }
}

# ─────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────
def ok(data=None, msg="success"):
    return jsonify({"status": "ok", "message": msg, "data": data})

def err(msg="error", code=400):
    return jsonify({"status": "error", "message": msg}), code


# ─────────────────────────────────────────
#  ROUTES: STATIC FRONTEND
# ─────────────────────────────────────────
@app.route('/')
def serve_app():
    """Serve the SherByte frontend HTML."""
    static_file = os.path.join(os.path.dirname(__file__), 'sherbyte-v20.html')
    if os.path.exists(static_file):
        with open(static_file, 'r', encoding='utf-8') as f:
            return f.read(), 200, {'Content-Type': 'text/html; charset=utf-8'}
    return '<h1>SherByte v20 — Place sherbyte-v20.html in the same folder as app.py</h1>', 200


# ─────────────────────────────────────────
#  ROUTES: ARTICLES
# ─────────────────────────────────────────
@app.route('/api/articles', methods=['GET'])
def get_articles():
    """
    GET /api/articles?cat=tech&page=1&limit=10
    Returns paginated list of articles, optionally filtered by category.
    """
    cat   = request.args.get('cat')
    page  = int(request.args.get('page', 1))
    limit = min(int(request.args.get('limit', 10)), 50)

    pool = DB['articles']
    if cat and cat != 'all':
        pool = [a for a in pool if a['cat'] == cat]

    start = (page - 1) * limit
    end   = start + limit
    items = pool[start:end]

    return ok({
        "articles": items,
        "total": len(pool),
        "page": page,
        "limit": limit,
        "has_more": end < len(pool)
    })


@app.route('/api/articles/<int:article_id>', methods=['GET'])
def get_article(article_id):
    """GET /api/articles/1 — Get single article by ID."""
    art = next((a for a in DB['articles'] if a['id'] == article_id), None)
    if not art:
        return err("Article not found", 404)
    related = [a for a in DB['articles'] if a['cat'] == art['cat'] and a['id'] != article_id][:3]
    return ok({"article": art, "related": related})


@app.route('/api/articles/breaking', methods=['GET'])
def get_breaking():
    """GET /api/articles/breaking — Get breaking news only."""
    breaking = [a for a in DB['articles'] if a.get('breaking')]
    return ok({"articles": breaking})


# ─────────────────────────────────────────
#  ROUTES: USER
# ─────────────────────────────────────────
@app.route('/api/user/<user_id>', methods=['GET'])
def get_user(user_id):
    """GET /api/user/jashwanth1621 — Get user profile."""
    user = DB['users'].get(user_id)
    if not user:
        return err("User not found", 404)
    # Don't expose password etc (add auth later)
    safe = {k: v for k, v in user.items() if k not in ['password']}
    return ok({"user": safe})


@app.route('/api/user/<user_id>/save', methods=['POST'])
def toggle_save(user_id):
    """
    POST /api/user/<id>/save
    Body: {"article_id": 5}
    Toggles save on the article.
    """
    user = DB['users'].get(user_id)
    if not user:
        return err("User not found", 404)

    data = request.get_json(silent=True) or {}
    art_id = data.get('article_id')
    if not art_id:
        return err("article_id required")

    saved = user['saved']
    if art_id in saved:
        saved.remove(art_id)
        action = "removed"
    else:
        saved.append(art_id)
        action = "saved"

    return ok({"action": action, "saved_count": len(saved), "saved": saved})


@app.route('/api/user/<user_id>/like', methods=['POST'])
def toggle_like(user_id):
    """
    POST /api/user/<id>/like
    Body: {"article_id": 5}
    Toggles like on the article.
    """
    user = DB['users'].get(user_id)
    if not user:
        return err("User not found", 404)

    data = request.get_json(silent=True) or {}
    art_id = data.get('article_id')
    if not art_id:
        return err("article_id required")

    liked = user['liked']
    if art_id in liked:
        liked.remove(art_id)
        action = "unliked"
    else:
        liked.append(art_id)
        action = "liked"

    return ok({"action": action, "liked_count": len(liked), "liked": liked})


@app.route('/api/user/<user_id>/settings', methods=['GET', 'PUT'])
def user_settings(user_id):
    """
    GET  /api/user/<id>/settings — Get user settings
    PUT  /api/user/<id>/settings — Update settings
    Body: {"theme": "light", "notifications": false}
    """
    user = DB['users'].get(user_id)
    if not user:
        return err("User not found", 404)

    if request.method == 'GET':
        return ok({"settings": user['settings']})

    # PUT
    data = request.get_json(silent=True) or {}
    allowed = {'theme', 'notifications', 'email_digest', 'privacy', 'location_in_posts'}
    for key, val in data.items():
        if key in allowed:
            user['settings'][key] = val

    return ok({"settings": user['settings'], "message": "Settings updated"})


# ─────────────────────────────────────────
#  ROUTES: NOTIFICATIONS
# ─────────────────────────────────────────
@app.route('/api/notifications', methods=['GET'])
def get_notifications():
    """GET /api/notifications — Get notification feed."""
    return ok({"notifications": DB['notifications'], "unread": len(DB['notifications'])})


# ─────────────────────────────────────────
#  ROUTES: MARKET DATA
# ─────────────────────────────────────────
@app.route('/api/market', methods=['GET'])
def get_market():
    """GET /api/market — Get market ticker data."""
    return ok({"market": DB['market'], "updated_at": datetime.utcnow().isoformat()})


# ─────────────────────────────────────────
#  ROUTES: SEARCH
# ─────────────────────────────────────────
@app.route('/api/search', methods=['GET'])
def search():
    """
    GET /api/search?q=quantum&cat=tech
    Simple keyword search across article titles and text.
    """
    q   = (request.args.get('q') or '').lower().strip()
    cat = request.args.get('cat')

    if not q:
        return err("Query 'q' is required")

    results = []
    for a in DB['articles']:
        if q in a['title'].lower() or q in a['text'].lower():
            if not cat or cat == a['cat']:
                results.append(a)

    return ok({"results": results, "count": len(results), "query": q})


# ─────────────────────────────────────────
#  ROUTES: HEALTH CHECK
# ─────────────────────────────────────────
@app.route('/api/health', methods=['GET'])
def health():
    """GET /api/health — Liveness check for deployment."""
    return ok({
        "service": "SherByte API",
        "version": "20.0",
        "uptime": "OK",
        "timestamp": datetime.utcnow().isoformat(),
        "articles": len(DB['articles']),
        "users": len(DB['users']),
    })


# ─────────────────────────────────────────
#  ERROR HANDLERS
# ─────────────────────────────────────────
@app.errorhandler(404)
def not_found(e):
    return err("Route not found", 404)

@app.errorhandler(500)
def server_error(e):
    return err("Internal server error", 500)


# ─────────────────────────────────────────
#  RUN
# ─────────────────────────────────────────
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('DEBUG', 'true').lower() == 'true'
    print(f"\n{'='*50}")
    print(f"  SherByte v20 API — Starting on port {port}")
    print(f"  Frontend: http://localhost:{port}/")
    print(f"  API Docs: http://localhost:{port}/api/health")
    print(f"{'='*50}\n")
    app.run(host='0.0.0.0', port=port, debug=debug)
