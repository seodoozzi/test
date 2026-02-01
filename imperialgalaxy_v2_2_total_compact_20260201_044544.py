# ImperialGalaxy v2.2 TOTAL (compact, single-file)
# - Fail-closed publishing (quarantine on any risk)
# - SQLite single source of truth + one-time migration from legacy JSON + optional JSON snapshots
# - Anti-duplicate (product cooldown, max uses/30d, topic window)
# - Robust retries/backoff + stop-aware + atomic lock + log rotation + circuit breaker
# - Optional Pinterest autopin (best-effort)
#
# Absolute dotenv path (as requested):
#   load_dotenv("/Users/seokichan/.env")

import asyncio, os, random, aiohttp, json, re, sys, atexit, tempfile, signal, logging, sqlite3, shutil, argparse
import time, datetime, hashlib, hmac
from html import escape as html_escape
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from logging.handlers import TimedRotatingFileHandler

from dotenv import load_dotenv
from google import genai
from google.genai import types
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

VERSION = "2.2-TOTAL-compact"

# ---------------- Logger ----------------
_logger = logging.getLogger()
_logger.setLevel(logging.INFO)

def _has_timed_rotating_handler(filename: str) -> bool:
    fn = os.path.abspath(filename)
    for h in _logger.handlers:
        if isinstance(h, TimedRotatingFileHandler):
            try:
                if os.path.abspath(getattr(h, "baseFilename", "")) == fn:
                    return True
            except Exception:
                pass
    return False

if not _has_timed_rotating_handler("bot.log"):
    h = TimedRotatingFileHandler("bot.log", when="midnight", interval=1, backupCount=14, encoding="utf-8")
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    _logger.addHandler(h)

# ---------------- Lock / Stop ----------------
LOCK_FILE = os.path.join(tempfile.gettempdir(), "imperial_galaxy.lock")
_SHOULD_STOP = False
_STOP_EVENT = None
_STOP_LOOP = None

def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False

def _read_lock_pid(path: str) -> int:
    try:
        with open(path, "r", encoding="utf-8") as f:
            t = f.read().strip()
        return int(t) if t.isdigit() else 0
    except Exception:
        return 0

def _acquire_lock_or_exit(path: str):
    while True:
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(str(os.getpid()))
            return
        except FileExistsError:
            old = _read_lock_pid(path)
            if old and _pid_alive(old):
                sys.exit(0)
            try:
                os.remove(path)
            except Exception:
                pass
        except Exception:
            sys.exit(0)

def _handle_stop_signal(_s=None, _f=None):
    global _SHOULD_STOP
    _SHOULD_STOP = True
    try:
        if _STOP_LOOP is not None and _STOP_EVENT is not None:
            _STOP_LOOP.call_soon_threadsafe(_STOP_EVENT.set)
    except Exception:
        pass

_acquire_lock_or_exit(LOCK_FILE)
atexit.register(lambda: os.path.exists(LOCK_FILE) and os.remove(LOCK_FILE))
signal.signal(signal.SIGTERM, _handle_stop_signal)
signal.signal(signal.SIGINT, _handle_stop_signal)

def _should_stop() -> bool:
    return bool(_SHOULD_STOP)

# ---------------- Env / Config ----------------
load_dotenv("/Users/seokichan/.env")

def _csv_list(s: str):
    return [x.strip() for x in (s or "").split(",") if x.strip()]

def _int_env(name: str, default: int):
    try:
        v = (os.getenv(name, "") or "").strip()
        return int(v) if v else default
    except Exception:
        return default

def _float_env(name: str, default: float):
    try:
        v = (os.getenv(name, "") or "").strip()
        return float(v) if v else default
    except Exception:
        return default

def _bool_env(name: str, default: bool):
    v = (os.getenv(name, "") or "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")

def now_utc():
    return datetime.datetime.utcnow()

def utc_day_str(ts: float = None) -> str:
    dt = datetime.datetime.utcfromtimestamp(ts if ts is not None else time.time())
    return dt.strftime("%Y-%m-%d")

CONFIG = {
    # Gemini
    "GEMINI_KEY": os.getenv("GEMINI_API_KEY"),
    "GEMINI_MODEL": os.getenv("GEMINI_MODEL", "models/gemini-1.5-flash"),
    "GEMINI_MAX_RETRIES": _int_env("GEMINI_MAX_RETRIES", 5),
    "GEMINI_BACKOFF_BASE_SEC": _float_env("GEMINI_BACKOFF_BASE_SEC", 1.4),
    "GEMINI_BACKOFF_CAP_SEC": _float_env("GEMINI_BACKOFF_CAP_SEC", 30.0),

    # Blogger
    "BLOG_ID": os.getenv("BLOG_ID"),
    "TOKEN_PATH": os.getenv("TOKEN_PATH", "token.json"),
    "BLOGGER_MAX_RETRIES": _int_env("BLOGGER_MAX_RETRIES", 3),
    "BLOGGER_BACKOFF_BASE_SEC": _float_env("BLOGGER_BACKOFF_BASE_SEC", 1.6),
    "BLOGGER_BACKOFF_CAP_SEC": _float_env("BLOGGER_BACKOFF_CAP_SEC", 20.0),

    # Storage
    "DB_PATH": (os.getenv("IG_DB_PATH", "ig.sqlite") or "ig.sqlite").strip(),
    "LEGACY_PRODUCTS_JSON": os.getenv("PRODUCT_DB", "products.json"),
    "LEGACY_HISTORY_JSON": os.getenv("HISTORY_FILE", "history.json"),
    "LEGACY_STATS_JSON": os.getenv("STATS_FILE", "stats.json"),
    "LEGACY_AUTO_BACKUP": _bool_env("IG_LEGACY_AUTO_BACKUP", True),

    # Optional JSON snapshot from SQLite (does not overwrite legacy JSON)
    "SNAPSHOT_ENABLED": _bool_env("IG_JSON_SNAPSHOT_ENABLED", True),
    "SNAPSHOT_SUFFIX": (os.getenv("IG_JSON_SNAPSHOT_SUFFIX", ".sqlite_copy.json") or ".sqlite_copy.json").strip(),

    # Intervals
    "MIN_POST_INTERVAL_SEC": _int_env("MIN_POST_INTERVAL_SEC", 12 * 3600),
    "MIN_COLLECT_INTERVAL_SEC": _int_env("MIN_COLLECT_INTERVAL_SEC", 6 * 3600),
    "EMPTY_COLLECT_RETRY_SEC": _int_env("EMPTY_COLLECT_RETRY_SEC", 1800),

    # Safety/quality
    "POST_FAILURE_COOLDOWN_SEC": _int_env("POST_FAILURE_COOLDOWN_SEC", 30 * 60),
    "MIN_BODY_CHARS": _int_env("MIN_BODY_CHARS", 900),
    "MAX_POSTS_PER_DAY": _int_env("MAX_POSTS_PER_DAY", 2),

    # Anti-duplicate
    "PRODUCT_COOLDOWN_DAYS": _int_env("PRODUCT_COOLDOWN_DAYS", 7),
    "PRODUCT_MAX_USES_30D": _int_env("PRODUCT_MAX_USES_30D", 2),
    "TOPIC_DUPLICATE_WINDOW_DAYS": _int_env("TOPIC_DUPLICATE_WINDOW_DAYS", 14),

    # Retention
    "PRODUCT_RETENTION_DAYS": _int_env("PRODUCT_RETENTION_DAYS", 120),
    "CACHE_TTL_DAYS": _int_env("CACHE_TTL_DAYS", 30),
    "QUARANTINE_RETENTION_DAYS": _int_env("QUARANTINE_RETENTION_DAYS", 120),

    # AliExpress
    "ALI_APP_KEY": os.getenv("ALI_APP_KEY"),
    "ALI_APP_SECRET": os.getenv("ALI_APP_SECRET"),
    "ALI_TRACKING_ID": os.getenv("ALI_TRACKING_ID"),
    "ALI_KEYWORDS": _csv_list(os.getenv("ALI_KEYWORDS", "gan charger,usb c hub,desk gadget,smart light,charging station")),
    "ALI_DEBUG": _bool_env("ALI_DEBUG", False),
    "ALI_TRACKING_MARKERS": _csv_list(os.getenv("ALI_TRACKING_MARKERS", "")),

    # Temu
    "TEMU_ENABLED": (os.getenv("TEMU_ENABLED", "false") or "false").lower() == "true",
    "TEMU_FEED_URL": (os.getenv("TEMU_FEED_URL", "") or "").strip(),

    # Amazon
    "AMAZON_TAG": (os.getenv("AMAZON_ASSOCIATE_TAG", "") or "").strip(),
    "AMAZON_DOMAIN": (os.getenv("AMAZON_DOMAIN", "amazon.com") or "amazon.com").strip() or "amazon.com",
    "AMAZON_DISCLOSURE_HTML": (os.getenv("AMAZON_DISCLOSURE_HTML", "") or "").strip(),
    "AMAZON_QUALIFIED_SALES_ENV": _int_env("AMAZON_QUALIFIED_SALES", -1),

    # Mix
    "MIX_PHASE1": {"AliExpress": 0.50, "Temu": 0.30, "Amazon": 0.20},
    "MIX_PHASE2": {"AliExpress": 0.60, "Temu": 0.30, "Amazon": 0.10},
    "MIX_SWITCH_DAYS": _int_env("MIX_SWITCH_DAYS", 30),
    "LAYOUT_EPSILON": _float_env("LAYOUT_EPSILON", 0.10),

    # HTML safety
    "AFFILIATE_DISCLOSURE_HTML": (os.getenv(
        "AFFILIATE_DISCLOSURE_HTML",
        "<p><small><i>Disclosure: This post may contain affiliate links. If you buy through them, I may earn a commission at no extra cost to you.</i></small></p>"
    ) or "").strip(),
    "SANITIZE_HTML": _bool_env("SANITIZE_HTML", True),
    "ALLOWED_LINK_DOMAINS": _csv_list(os.getenv("ALLOWED_LINK_DOMAINS", "")),
    "CTA_REL": (os.getenv("CTA_REL", "nofollow noopener noreferrer sponsored") or "").strip()
              or "nofollow noopener noreferrer sponsored",

    # Search Console (optional)
    "SEARCH_CONSOLE_ENABLED": (os.getenv("SEARCH_CONSOLE_ENABLED", "false") or "false").lower() == "true",
    "SC_DOMAIN": (os.getenv("SC_DOMAIN", "") or "").strip(),

    # HTTP
    "HTTP_MAX_RETRIES": _int_env("HTTP_MAX_RETRIES", 6),
    "HTTP_BACKOFF_BASE_SEC": _float_env("HTTP_BACKOFF_BASE_SEC", 1.3),
    "HTTP_BACKOFF_CAP_SEC": _float_env("HTTP_BACKOFF_CAP_SEC", 25.0),

    # Loop sleep
    "MAX_LOOP_SLEEP_SEC": _int_env("MAX_LOOP_SLEEP_SEC", 1800),
    "MIN_LOOP_SLEEP_SEC": _int_env("MIN_LOOP_SLEEP_SEC", 45),

    # Circuit breaker
    "ERROR_STREAK_MAX": _int_env("IG_ERROR_STREAK_MAX", 8),
    "ERROR_STREAK_PAUSE_SEC": _int_env("IG_ERROR_STREAK_PAUSE_SEC", 20 * 60),

    # Pinterest optional
    "PINTEREST_ENABLED": (os.getenv("PINTEREST_ENABLED", "false") or "false").lower() == "true",
    "PINTEREST_ACCESS_TOKEN": (os.getenv("PINTEREST_ACCESS_TOKEN", "") or "").strip(),
    "PINTEREST_BOARD_ID": (os.getenv("PINTEREST_BOARD_ID", "") or "").strip(),
    "PINTEREST_MAX_PINS_PER_DAY": _int_env("PINTEREST_MAX_PINS_PER_DAY", 6),
    "PINTEREST_MIN_INTERVAL_SEC": _int_env("PINTEREST_MIN_INTERVAL_SEC", 30 * 60),
    "PINTEREST_COOLDOWN_ON_AUTH_SEC": _int_env("PINTEREST_COOLDOWN_ON_AUTH_SEC", 24 * 3600),
}

GEMINI = genai.Client(api_key=CONFIG["GEMINI_KEY"]) if CONFIG["GEMINI_KEY"] else None

# ---------------- URL / Text ----------------
def is_valid_http_url(u: str) -> bool:
    if not u:
        return False
    try:
        u = u.strip()
        if any(ord(ch) < 32 for ch in u):
            return False
        p = urlparse(u)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False

def safe_url(u: str, max_len: int = 2048) -> str:
    u = (u or "").strip()
    if len(u) > max_len:
        u = u[:max_len]
    if not is_valid_http_url(u):
        return ""
    if '"' in u or "'" in u:
        return ""
    return u

def safe_text(s: str, max_len: int = 240) -> str:
    s = (s or "").strip()
    if len(s) > max_len:
        s = s[:max_len]
    return html_escape(s, quote=True)

def safe_attr(s: str, max_len: int = 2048) -> str:
    s = (s or "").strip()
    if len(s) > max_len:
        s = s[:max_len]
    return html_escape(s, quote=True)

def normalize_url_for_id(u: str) -> str:
    try:
        if not is_valid_http_url(u):
            return ""
        p = urlparse(u.strip())
        return urlunparse((p.scheme.lower(), p.netloc.lower(), p.path or "", "", "", ""))
    except Exception:
        return ""

def url_hash_id(u: str) -> str:
    base = normalize_url_for_id(u) or (u or "")
    if not base:
        return ""
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]

def _host_of(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""

def _domain_matches(host: str, domain: str) -> bool:
    host = (host or "").lower().strip()
    domain = (domain or "").lower().strip()
    if not host or not domain:
        return False
    return host == domain or host.endswith("." + domain)

def amazon_url_from_asin(asin: str) -> str:
    asin = (asin or "").strip()
    if not asin:
        return ""
    base = f"https://{CONFIG['AMAZON_DOMAIN']}/dp/{asin}"
    return f"{base}?tag={CONFIG['AMAZON_TAG']}" if CONFIG["AMAZON_TAG"] else base

def ensure_amazon_tag(url: str) -> str:
    url = (url or "").strip()
    if not url or not is_valid_http_url(url) or not CONFIG["AMAZON_TAG"]:
        return url
    try:
        p = urlparse(url)
        if "amazon." not in (p.netloc or "").lower():
            return url
        qs = dict(parse_qsl(p.query, keep_blank_values=True))
        if qs.get("tag"):
            return url
        qs["tag"] = CONFIG["AMAZON_TAG"]
        return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(list(qs.items())), p.fragment))
    except Exception:
        return url

# ---------------- HTML Sanitizer ----------------
_DISCLOSURE_MARKER = "<!--IG_DISCLOSURE-->"
_AMAZON_MARKER = "<!--IG_AMAZON_DISCLOSURE-->"

_DANGEROUS_TAG_RE = re.compile(r"(?is)<\s*(script|iframe|object|embed|svg)\b[^>]*>.*?<\s*/\s*\1\s*>")
_DANGEROUS_SELF_CLOSING_RE = re.compile(r"(?is)<\s*(script|iframe|object|embed|svg)\b[^>]*/\s*>")
_ON_EVENT_ATTR_RE = re.compile(r"(?is)\s+on[a-z0-9_-]+\s*=\s*(\"[^\"]*\"|'[^']*'|[^\s>]+)")
_BAD_SCHEME_QUOTED_RE = re.compile(r"(?is)\b(href|src)\s*=\s*(\"|')\s*(javascript|data)\s*:[^\"']*(\2)")
_BAD_SCHEME_UNQUOTED_RE = re.compile(r"(?is)\b(href|src)\s*=\s*(javascript|data)\s*:[^\s>]+")
_STYLE_ATTR_RE = re.compile(r'(?is)\s+style\s*=\s*("([^"]*)"|\'([^\']*)\')')
_DANG_STYLE_TOKENS_RE = re.compile(r"(?is)(url\s*\(|expression\s*\(|javascript\s*:|data\s*:)")
_A_HREF_QUOTED_RE = re.compile(r'(?is)<a\b([^>]*?)\bhref\s*=\s*("([^"]*)"|\'([^\']*)\')([^>]*)>')
_A_HREF_UNQUOTED_RE = re.compile(r'(?is)<a\b([^>]*?)\bhref\s*=\s*([^\s"\'=<>`]+)([^>]*)>')
_IMG_SRC_QUOTED_RE = re.compile(r'(?is)<img\b([^>]*?)\bsrc\s*=\s*("([^"]*)"|\'([^\']*)\')([^>]*)/?>')
_IMG_SRC_UNQUOTED_RE = re.compile(r'(?is)<img\b([^>]*?)\bsrc\s*=\s*([^\s"\'=<>`]+)([^>]*)/?>')

def sanitize_html_basic(html: str, allowed_domains: set) -> str:
    if not isinstance(html, str) or not html.strip():
        return ""
    x = html
    x = _DANGEROUS_TAG_RE.sub("", x)
    x = _DANGEROUS_SELF_CLOSING_RE.sub("", x)
    x = _ON_EVENT_ATTR_RE.sub("", x)
    x = _BAD_SCHEME_QUOTED_RE.sub(r'\1=\2#\2', x)
    x = _BAD_SCHEME_UNQUOTED_RE.sub(r'\1=#', x)

    def _style_sub(m):
        full = m.group(0)
        val = m.group(2) if m.group(2) is not None else (m.group(3) or "")
        return "" if _DANG_STYLE_TOKENS_RE.search(val) else full
    x = _STYLE_ATTR_RE.sub(_style_sub, x)

    if allowed_domains:
        def _safe_link(url: str) -> str:
            url = (url or "").strip()
            if not url:
                return "#"
            if url.startswith("#") or url.startswith("/") or url.lower().startswith("mailto:"):
                return url
            if not is_valid_http_url(url):
                return "#"
            h = _host_of(url)
            return url if any(_domain_matches(h, d) for d in allowed_domains) else "#"

        def _href_q_sub(m):
            pre = m.group(1) or ""
            quoted = m.group(2) or ""
            url = (m.group(3) if m.group(3) is not None else (m.group(4) or "")).strip()
            post = m.group(5) or ""
            safe = _safe_link(url)
            q = '"' if quoted.startswith('"') else "'"
            return f"<a{pre}href={q}{html_escape(safe, quote=True)}{q}{post}>"
        x = _A_HREF_QUOTED_RE.sub(_href_q_sub, x)

        def _href_u_sub(m):
            pre = m.group(1) or ""
            url = (m.group(2) or "").strip()
            post = m.group(3) or ""
            safe = _safe_link(url)
            return f"<a{pre}href=\"{html_escape(safe, quote=True)}\"{post}>"
        x = _A_HREF_UNQUOTED_RE.sub(_href_u_sub, x)

        def _safe_img(url: str) -> str:
            url = (url or "").strip()
            if not url:
                return ""
            low = url.lower()
            if low.startswith("data:") or low.startswith("javascript:"):
                return ""
            if not is_valid_http_url(url):
                return ""
            h = _host_of(url)
            return url if any(_domain_matches(h, d) for d in allowed_domains) else ""

        def _img_q_sub(m):
            pre = m.group(1) or ""
            quoted = m.group(2) or ""
            url = (m.group(3) if m.group(3) is not None else (m.group(4) or "")).strip()
            post = m.group(5) or ""
            safe = _safe_img(url)
            if not safe:
                return ""
            q = '"' if quoted.startswith('"') else "'"
            return f"<img{pre}src={q}{html_escape(safe, quote=True)}{q}{post}>"
        x = _IMG_SRC_QUOTED_RE.sub(_img_q_sub, x)

        def _img_u_sub(m):
            pre = m.group(1) or ""
            url = (m.group(2) or "").strip()
            post = m.group(3) or ""
            safe = _safe_img(url)
            if not safe:
                return ""
            return f"<img{pre}src=\"{html_escape(safe, quote=True)}\"{post}>"
        x = _IMG_SRC_UNQUOTED_RE.sub(_img_u_sub, x)

    return x.strip()

def ensure_disclosure_once(html: str) -> str:
    return html if _DISCLOSURE_MARKER in (html or "") else f"{_DISCLOSURE_MARKER}\n{CONFIG['AFFILIATE_DISCLOSURE_HTML']}\n" + (html or "")

def ensure_amazon_disclosure_once(html: str) -> str:
    if not CONFIG["AMAZON_DISCLOSURE_HTML"]:
        return html
    return html if _AMAZON_MARKER in (html or "") else f"{_AMAZON_MARKER}\n{CONFIG['AMAZON_DISCLOSURE_HTML']}\n" + (html or "")

def ensure_faq(html: str) -> str:
    h = html or ""
    if re.search(r"(?is)\bfaq\b", h) is None:
        h += (
            "\n<h3>FAQ</h3>"
            "\n<p><b>Q:</b> How do I choose the right one?<br><b>A:</b> Pick based on your use case and confirm details on the product page.</p>"
            "\n<p><b>Q:</b> Do prices change often?<br><b>A:</b> Yes—check the current listing before buying.</p>"
            "\n<p><b>Q:</b> What should I verify before ordering?<br><b>A:</b> Return policy, shipping time, and recent reviews.</p>"
        )
    return h

# ---------------- Retry helpers ----------------
def _is_retriable_http_status(code: int) -> bool:
    return code in (429, 500, 502, 503, 504)

async def _backoff_sleep(attempt: int, base: float, cap: float):
    delay = min(cap, base * (2 ** attempt))
    delay *= (0.7 + random.random() * 0.6)
    await asyncio.sleep(delay)

def _strip_for_log(s: str, max_len: int = 900) -> str:
    return (s or "").replace("\n", " ").replace("\r", " ").strip()[:max_len]

def safe_json_dumps(obj, max_len: int = 20000):
    try:
        return json.dumps(obj or {}, ensure_ascii=False)[:max_len]
    except Exception:
        return "{}"

# ============================================================
# SQLite store
# ============================================================

class IGStore:
    def __init__(self, db_path: str):
        self.db_path = (db_path or "ig.sqlite").strip() or "ig.sqlite"
        self._initd = False

    def _connect(self):
        c = sqlite3.connect(self.db_path, timeout=12)
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA synchronous=NORMAL;")
        c.execute("PRAGMA temp_store=MEMORY;")
        c.execute("PRAGMA busy_timeout=8000;")
        return c

    def init(self):
        if self._initd:
            return
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)) or ".", exist_ok=True)
        c = self._connect()
        try:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS products(
              uid TEXT PRIMARY KEY,
              platform TEXT NOT NULL,
              stable_id TEXT NOT NULL,
              name TEXT NOT NULL,
              asin TEXT NOT NULL DEFAULT '',
              url TEXT NOT NULL,
              source_url TEXT NOT NULL DEFAULT '',
              image_url TEXT NOT NULL DEFAULT '',
              source_keyword TEXT NOT NULL DEFAULT '',
              added_ts REAL NOT NULL,
              ctr_score REAL NOT NULL DEFAULT 1.0,
              last_used_ts REAL NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_products_platform_score ON products(platform, ctr_score DESC);
            CREATE INDEX IF NOT EXISTS idx_products_added ON products(added_ts);

            CREATE TABLE IF NOT EXISTS posts(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              title TEXT NOT NULL,
              url TEXT NOT NULL,
              ts REAL NOT NULL,
              layout TEXT NOT NULL,
              topic_hash TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_posts_ts ON posts(ts DESC);

            CREATE TABLE IF NOT EXISTS product_uses(uid TEXT NOT NULL, ts REAL NOT NULL);
            CREATE INDEX IF NOT EXISTS idx_uses_uid_ts ON product_uses(uid, ts DESC);

            CREATE TABLE IF NOT EXISTS topic_log(hash TEXT NOT NULL, ts REAL NOT NULL);
            CREATE INDEX IF NOT EXISTS idx_topic_hash_ts ON topic_log(hash, ts DESC);

            CREATE TABLE IF NOT EXISTS cache(bucket TEXT NOT NULL, cache_key TEXT NOT NULL, json TEXT NOT NULL, ts REAL NOT NULL,
              PRIMARY KEY(bucket, cache_key));
            CREATE TABLE IF NOT EXISTS quarantine(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts REAL NOT NULL,
              stage TEXT NOT NULL DEFAULT '',
              reason TEXT NOT NULL,
              detail_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_quarantine_ts ON quarantine(ts DESC);

            CREATE TABLE IF NOT EXISTS layout_scores(layout TEXT PRIMARY KEY, score REAL NOT NULL);
            INSERT OR IGNORE INTO layout_scores(layout, score) VALUES('A', 1.0);
            INSERT OR IGNORE INTO layout_scores(layout, score) VALUES('B', 1.0);

            CREATE TABLE IF NOT EXISTS pinterest_log(ts REAL NOT NULL, pin_id TEXT NOT NULL DEFAULT '', post_url TEXT NOT NULL, image_url TEXT NOT NULL DEFAULT '');
            CREATE INDEX IF NOT EXISTS idx_pinterest_ts ON pinterest_log(ts DESC);
            """)
            c.commit()
        finally:
            c.close()
        self._initd = True

    # ---- meta ----
    def get_meta(self, key: str, default: str = "") -> str:
        self.init()
        c = self._connect()
        try:
            r = c.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return str(r[0]) if r and r[0] is not None else default
        finally:
            c.close()

    def set_meta(self, key: str, value: str):
        self.init()
        c = self._connect()
        try:
            c.execute("BEGIN IMMEDIATE")
            c.execute("REPLACE INTO meta(key,value) VALUES(?,?)", (key, str(value)))
            c.commit()
        finally:
            c.close()

    def get_meta_f(self, key: str, default: float = 0.0) -> float:
        try:
            return float(self.get_meta(key, str(default)) or default)
        except Exception:
            return float(default)

    def set_meta_f(self, key: str, value: float):
        self.set_meta(key, str(float(value)))

    # ---- cache ----
    def cache_get(self, bucket: str, key: str):
        self.init()
        c = self._connect()
        try:
            r = c.execute("SELECT json, ts FROM cache WHERE bucket=? AND cache_key=?", (bucket, key)).fetchone()
            if not r:
                return None
            return {"json": r[0], "ts": float(r[1] or 0)}
        finally:
            c.close()

    def cache_set(self, bucket: str, key: str, json_text: str):
        self.init()
        c = self._connect()
        try:
            c.execute("BEGIN IMMEDIATE")
            c.execute("REPLACE INTO cache(bucket, cache_key, json, ts) VALUES(?,?,?,?)", (bucket, key, json_text, time.time()))
            c.commit()
        except Exception:
            try: c.rollback()
            except Exception: pass
        finally:
            c.close()

    def cache_cleanup(self, ttl_days: int):
        self.init()
        cutoff = time.time() - float(ttl_days) * 86400.0
        c = self._connect()
        try:
            c.execute("BEGIN IMMEDIATE")
            c.execute("DELETE FROM cache WHERE ts < ?", (cutoff,))
            c.commit()
        except Exception:
            try: c.rollback()
            except Exception: pass
        finally:
            c.close()

    # ---- layout ----
    def layout_scores(self) -> dict:
        self.init()
        c = self._connect()
        try:
            rows = c.execute("SELECT layout, score FROM layout_scores").fetchall()
            return {r[0]: float(r[1] or 1.0) for r in rows}
        finally:
            c.close()

    def set_layout_score(self, layout: str, score: float):
        self.init()
        c = self._connect()
        try:
            c.execute("BEGIN IMMEDIATE")
            c.execute("REPLACE INTO layout_scores(layout, score) VALUES(?,?)", (layout, float(score)))
            c.commit()
        except Exception:
            try: c.rollback()
            except Exception: pass
        finally:
            c.close()

    # ---- products ----
    def upsert_products(self, rows: list) -> int:
        if not rows:
            return 0
        self.init()
        c = self._connect()
        n = 0
        try:
            c.execute("BEGIN IMMEDIATE")
            for p in rows:
                if not isinstance(p, dict):
                    continue
                uid = (p.get("uid") or "").strip()
                if not uid:
                    continue
                vals = (
                    uid,
                    (p.get("platform") or "").strip(),
                    (p.get("stable_id") or "").strip(),
                    (p.get("name") or "").strip(),
                    (p.get("asin") or "").strip(),
                    (p.get("url") or "").strip(),
                    (p.get("source_url") or "").strip(),
                    (p.get("image_url") or "").strip(),
                    (p.get("source_keyword") or "").strip(),
                    float(p.get("added_ts") or time.time()),
                    float(p.get("ctr_score") or 1.0),
                    float(p.get("last_used_ts") or 0.0),
                )
                if not (vals[1] and vals[2] and vals[3] and vals[5]):
                    continue
                c.execute("""
                    INSERT INTO products(uid,platform,stable_id,name,asin,url,source_url,image_url,source_keyword,added_ts,ctr_score,last_used_ts)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(uid) DO UPDATE SET
                      name=excluded.name,
                      asin=excluded.asin,
                      url=excluded.url,
                      source_url=excluded.source_url,
                      image_url=excluded.image_url,
                      source_keyword=CASE WHEN excluded.source_keyword<>'' THEN excluded.source_keyword ELSE products.source_keyword END,
                      ctr_score=excluded.ctr_score
                """, vals)
                n += 1
            c.commit()
        except Exception as e:
            try: c.rollback()
            except Exception: pass
            logging.error(f"[DB] upsert_products failed: {e}", exc_info=True)
            return 0
        finally:
            c.close()
        return n

    def products_by_platform(self, platform: str, limit: int = 450) -> list:
        self.init()
        c = self._connect()
        try:
            rows = c.execute("""
                SELECT uid, platform, stable_id, name, asin, url, source_url, image_url, source_keyword, added_ts, ctr_score, last_used_ts
                FROM products WHERE platform=?
                ORDER BY ctr_score DESC LIMIT ?
            """, (platform, int(limit))).fetchall()
            out = []
            for r in rows:
                out.append({
                    "uid": r[0], "platform": r[1], "stable_id": r[2], "name": r[3], "asin": r[4], "url": r[5],
                    "source_url": r[6], "image_url": r[7], "source_keyword": r[8],
                    "added_ts": float(r[9] or 0), "ctr_score": float(r[10] or 1.0), "last_used_ts": float(r[11] or 0),
                })
            return out
        finally:
            c.close()

    def cleanup_products(self, retention_days: int) -> int:
        self.init()
        cutoff = time.time() - float(retention_days) * 86400.0
        c = self._connect()
        try:
            c.execute("BEGIN IMMEDIATE")
            cur = c.execute("DELETE FROM products WHERE added_ts < ?", (cutoff,))
            c.commit()
            return int(cur.rowcount or 0)
        except Exception:
            try: c.rollback()
            except Exception: pass
            return 0
        finally:
            c.close()

    def update_product_scores(self, updates: list):
        if not updates:
            return
        self.init()
        c = self._connect()
        try:
            c.execute("BEGIN IMMEDIATE")
            for u in updates:
                uid = (u.get("uid") or "").strip()
                if not uid:
                    continue
                c.execute("UPDATE products SET ctr_score=?, last_used_ts=? WHERE uid=?",
                          (float(u.get("ctr_score", 1.0)), float(u.get("last_used_ts", time.time())), uid))
            c.commit()
        except Exception:
            try: c.rollback()
            except Exception: pass
        finally:
            c.close()

    # ---- posts / usage / topic ----
    def insert_post(self, title: str, url: str, ts: float, layout: str, topic_hash: str):
        self.init()
        c = self._connect()
        try:
            c.execute("BEGIN IMMEDIATE")
            c.execute("INSERT INTO posts(title,url,ts,layout,topic_hash) VALUES(?,?,?,?,?)",
                      (title, url, float(ts), layout, topic_hash))
            c.commit()
        except Exception:
            try: c.rollback()
            except Exception: pass
        finally:
            c.close()

    def recent_posts(self, k: int = 3) -> list:
        self.init()
        c = self._connect()
        try:
            rows = c.execute("SELECT title,url,ts,layout,topic_hash FROM posts ORDER BY ts DESC LIMIT ?", (int(k),)).fetchall()
            return [{"title": r[0], "url": r[1], "ts": float(r[2] or 0), "layout": r[3], "topic_hash": r[4]} for r in rows]
        finally:
            c.close()

    def count_posts_today_utc(self) -> int:
        self.init()
        dt = now_utc()
        start = datetime.datetime(dt.year, dt.month, dt.day).timestamp()
        c = self._connect()
        try:
            r = c.execute("SELECT COUNT(*) FROM posts WHERE ts >= ?", (start,)).fetchone()
            return int(r[0] or 0) if r else 0
        finally:
            c.close()

    def log_uses(self, uids: list):
        if not uids:
            return
        self.init()
        ts = time.time()
        c = self._connect()
        try:
            c.execute("BEGIN IMMEDIATE")
            c.executemany("INSERT INTO product_uses(uid, ts) VALUES(?,?)", [(u, ts) for u in uids if u])
            c.commit()
        except Exception:
            try: c.rollback()
            except Exception: pass
        finally:
            c.close()

    def usage_counts_last_days(self, days: int = 30) -> dict:
        self.init()
        cutoff = time.time() - float(days) * 86400.0
        c = self._connect()
        try:
            rows = c.execute("SELECT uid, COUNT(*) FROM product_uses WHERE ts >= ? GROUP BY uid", (cutoff,)).fetchall()
            return {r[0]: int(r[1] or 0) for r in rows}
        finally:
            c.close()

    def last_used_map(self) -> dict:
        self.init()
        c = self._connect()
        try:
            rows = c.execute("SELECT uid, MAX(ts) FROM product_uses GROUP BY uid").fetchall()
            return {r[0]: float(r[1] or 0) for r in rows}
        finally:
            c.close()

    def log_topic(self, topic_hash: str):
        if not topic_hash:
            return
        self.init()
        c = self._connect()
        try:
            c.execute("BEGIN IMMEDIATE")
            c.execute("INSERT INTO topic_log(hash, ts) VALUES(?,?)", (topic_hash, time.time()))
            c.commit()
        except Exception:
            try: c.rollback()
            except Exception: pass
        finally:
            c.close()

    def topic_seen_recently(self, topic_hash: str, window_days: int) -> bool:
        if not topic_hash:
            return False
        self.init()
        cutoff = time.time() - float(window_days) * 86400.0
        c = self._connect()
        try:
            r = c.execute("SELECT 1 FROM topic_log WHERE hash=? AND ts>=? LIMIT 1", (topic_hash, cutoff)).fetchone()
            return r is not None
        finally:
            c.close()

    # ---- quarantine ----
    def quarantine(self, stage: str, reason: str, detail: dict):
        self.init()
        c = self._connect()
        try:
            c.execute("BEGIN IMMEDIATE")
            c.execute("INSERT INTO quarantine(ts, stage, reason, detail_json) VALUES(?,?,?,?)",
                      (time.time(), (stage or "")[:30], (reason or "")[:60], safe_json_dumps(detail)))
            c.commit()
        except Exception:
            try: c.rollback()
            except Exception: pass
        finally:
            c.close()
        logging.warning(f"[QUARANTINE] stage={stage} reason={reason} detail={str(detail)[:400]}")

    def prune_quarantine(self, keep_days: int):
        self.init()
        cutoff = time.time() - float(keep_days) * 86400.0
        c = self._connect()
        try:
            c.execute("BEGIN IMMEDIATE")
            c.execute("DELETE FROM quarantine WHERE ts < ?", (cutoff,))
            c.commit()
        except Exception:
            try: c.rollback()
            except Exception: pass
        finally:
            c.close()

    # ---- pinterest log ----
    def pins_today_count_utc(self) -> int:
        self.init()
        dt = now_utc()
        start = datetime.datetime(dt.year, dt.month, dt.day).timestamp()
        c = self._connect()
        try:
            r = c.execute("SELECT COUNT(*) FROM pinterest_log WHERE ts >= ?", (start,)).fetchone()
            return int(r[0] or 0) if r else 0
        finally:
            c.close()

    def pinterest_log(self, pin_id: str, post_url: str, image_url: str):
        self.init()
        c = self._connect()
        try:
            c.execute("BEGIN IMMEDIATE")
            c.execute("INSERT INTO pinterest_log(ts, pin_id, post_url, image_url) VALUES(?,?,?,?)",
                      (time.time(), str(pin_id or ""), str(post_url or ""), str(image_url or "")))
            c.commit()
        except Exception:
            try: c.rollback()
            except Exception: pass
        finally:
            c.close()

    # ---- migration + snapshot ----
    def migrate_from_legacy_json_if_needed(self, products_json: str, history_json: str, stats_json: str, do_backup: bool):
        self.init()
        if self.get_meta("migrated_from_json", "") == "1":
            return
        any_legacy = any(os.path.exists(p) for p in (products_json, history_json, stats_json))
        if not any_legacy:
            self.set_meta("migrated_from_json", "1")
            return

        if do_backup:
            ts = now_utc().strftime("%Y%m%d_%H%M%S")
            for fp in (products_json, history_json, stats_json):
                try:
                    if fp and os.path.exists(fp):
                        shutil.copy2(fp, fp + f".bak_{ts}")
                except Exception:
                    pass

        def _load(path, default):
            try:
                if os.path.exists(path):
                    with open(path, "r", encoding="utf-8") as f:
                        return json.load(f)
            except Exception:
                return default
            return default

        legacy_products = _load(products_json, {"items": {}})
        legacy_history = _load(history_json, {})
        legacy_stats = _load(stats_json, {})

        items = {}
        if isinstance(legacy_products, dict):
            if isinstance(legacy_products.get("items"), dict):
                items = legacy_products["items"]
            elif isinstance(legacy_products.get("tech"), dict):
                items = legacy_products["tech"]

        upserts = []
        for _, p in (items or {}).items():
            if not isinstance(p, dict):
                continue
            name = str(p.get("name") or "").strip()
            if not name:
                continue
            platform = str(p.get("platform") or "AliExpress").strip()
            asin = str(p.get("asin") or "").strip()
            url = str(p.get("url") or "").strip()
            if "amazon" in platform.lower():
                platform = "Amazon"
                if asin and not url:
                    url = amazon_url_from_asin(asin)
                url = ensure_amazon_tag(url)
            url = safe_url(url)
            if not url:
                continue
            source_url = safe_url(str(p.get("source_url") or "").strip()) or ""
            image_url = str(p.get("image_url") or p.get("image") or "").strip()
            image_url = image_url if is_valid_http_url(image_url) else ""
            stable_id = str(p.get("id") or "").strip() or (asin or url_hash_id(url) or url_hash_id(source_url) or "")
            if not stable_id:
                continue
            uid = f"{platform}::{stable_id}"
            upserts.append({
                "uid": uid,
                "platform": platform,
                "stable_id": stable_id,
                "name": name,
                "asin": asin if platform == "Amazon" else "",
                "url": url,
                "source_url": source_url,
                "image_url": image_url,
                "source_keyword": str(p.get("source_keyword") or "").strip(),
                "added_ts": float(p.get("added") or time.time()),
                "ctr_score": float(p.get("ctr_score") or 1.0),
                "last_used_ts": float(p.get("last_used_ts") or 0.0),
            })
        if upserts:
            self.upsert_products(upserts)

        if isinstance(legacy_history, dict):
            for k in ("last_post_ts", "last_collect_ts", "last_collect_try_ts", "last_post_try_ts"):
                try:
                    self.set_meta_f(k, float(legacy_history.get(k, 0) or 0))
                except Exception:
                    pass
            posts = legacy_history.get("posts", [])
            if isinstance(posts, list):
                for it in posts[-200:]:
                    if not isinstance(it, dict):
                        continue
                    title = str(it.get("title") or "").strip()
                    url = safe_url(str(it.get("url") or "").strip())
                    ts0 = float(it.get("ts") or 0)
                    layout = str(it.get("layout") or "A").strip()[:4]
                    th = str(it.get("topic_hash") or "").strip() or hashlib.sha1((layout + "||" + url).encode("utf-8")).hexdigest()[:16]
                    if title and url and ts0:
                        self.insert_post(title, url, ts0, layout, th)

        if isinstance(legacy_stats, dict):
            lay = legacy_stats.get("layout", {})
            if isinstance(lay, dict):
                for k in ("A", "B"):
                    try:
                        self.set_layout_score(k, float(lay.get(k, {}).get("score", 1.0) or 1.0))
                    except Exception:
                        pass

        self.set_meta("migrated_from_json", "1")
        logging.info("[DB] migrated legacy JSON -> sqlite")

    def export_snapshot_json(self, products_json: str, history_json: str, stats_json: str, suffix: str):
        if not CONFIG["SNAPSHOT_ENABLED"]:
            return
        suffix = suffix or ".sqlite_copy.json"
        def _atomic(path: str, data):
            fd, tmp = tempfile.mkstemp(prefix="ig_", suffix=".tmp")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                os.replace(tmp, path)
            finally:
                try:
                    if os.path.exists(tmp):
                        os.remove(tmp)
                except Exception:
                    pass

        # products
        c = self._connect()
        try:
            rows = c.execute("SELECT uid,platform,stable_id,name,asin,url,source_url,image_url,source_keyword,added_ts,ctr_score,last_used_ts FROM products").fetchall()
        finally:
            c.close()
        items = {}
        for r in rows:
            items[r[0]] = {
                "id": r[2], "name": r[3], "platform": r[1], "asin": r[4], "url": r[5],
                "source_url": r[6], "image_url": r[7], "source_keyword": r[8],
                "added": float(r[9] or 0), "ctr_score": float(r[10] or 1.0), "last_used_ts": float(r[11] or 0)
            }
        _atomic(products_json + suffix, {"items": items})

        # history
        _atomic(history_json + suffix, {
            "last_post_ts": self.get_meta_f("last_post_ts", 0.0),
            "last_collect_ts": self.get_meta_f("last_collect_ts", 0.0),
            "last_collect_try_ts": self.get_meta_f("last_collect_try_ts", 0.0),
            "posts": self.recent_posts(200)
        })

        # stats
        lay = self.layout_scores()
        _atomic(stats_json + suffix, {"layout": {k: {"score": float(v)} for k, v in lay.items()}})

# ============================================================
# Bot
# ============================================================

def ali_sign(params: dict) -> str:
    s = "".join(f"{k}{params[k]}" for k in sorted(params))
    return hmac.new((CONFIG["ALI_APP_SECRET"] or "").encode(), s.encode(), hashlib.sha256).hexdigest().upper()

def _normalize_sc_url(u: str) -> str:
    try:
        if not is_valid_http_url(u):
            return ""
        p = urlparse(u)
        path = p.path or "/"
        while "//" in path:
            path = path.replace("//", "/")
        if path != "/" and path.endswith("/"):
            path = path[:-1]
        return urlunparse((p.scheme, p.netloc, path, "", p.query or "", ""))
    except Exception:
        return ""

class ImperialGalaxy:
    def __init__(self, store: IGStore, dry_run: bool):
        self.store = store
        self.dry_run = dry_run
        self.http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25))
        self.blogger_service = None
        self.sc_service = None

    async def close(self):
        await self.http.close()

    async def http_get_json(self, url: str, params=None, headers=None):
        for attempt in range(CONFIG["HTTP_MAX_RETRIES"]):
            if _should_stop():
                return None
            try:
                async with self.http.get(url, params=params, headers=headers) as r:
                    txt = await r.text()
                    if _is_retriable_http_status(r.status):
                        raise RuntimeError(f"HTTP {r.status}: {_strip_for_log(txt)}")
                    if r.status >= 400:
                        logging.warning(f"http_get_json {r.status} url={url} body={_strip_for_log(txt)}")
                        return None
                    try:
                        return json.loads(txt)
                    except Exception:
                        try:
                            return await r.json()
                        except Exception:
                            return None
            except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as e:
                if attempt >= CONFIG["HTTP_MAX_RETRIES"] - 1:
                    logging.warning(f"http_get_json failed url={url} err={e}")
                    return None
                await _backoff_sleep(attempt, CONFIG["HTTP_BACKOFF_BASE_SEC"], CONFIG["HTTP_BACKOFF_CAP_SEC"])
        return None

    # ---- Google auth ----
    def _load_creds(self, scopes):
        creds = Credentials.from_authorized_user_file(CONFIG["TOKEN_PATH"], scopes)
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            try:
                with open(CONFIG["TOKEN_PATH"], "w", encoding="utf-8") as f:
                    f.write(creds.to_json())
            except Exception:
                pass
        return creds

    async def blogger(self):
        if self.blogger_service:
            return self.blogger_service
        def _auth():
            scopes = ["https://www.googleapis.com/auth/blogger", "https://www.googleapis.com/auth/webmasters.readonly"]
            return build("blogger", "v3", credentials=self._load_creds(scopes), cache_discovery=False)
        self.blogger_service = await asyncio.to_thread(_auth)
        return self.blogger_service

    async def blogger_insert_with_retry(self, body: dict):
        last_err = None
        for attempt in range(CONFIG["BLOGGER_MAX_RETRIES"]):
            if _should_stop():
                raise RuntimeError("Stop requested")
            try:
                svc = await self.blogger()
                return await asyncio.to_thread(lambda: svc.posts().insert(blogId=CONFIG["BLOG_ID"], body=body).execute())
            except Exception as e:
                last_err = e
                if attempt >= CONFIG["BLOGGER_MAX_RETRIES"] - 1:
                    raise
                await _backoff_sleep(attempt, CONFIG["BLOGGER_BACKOFF_BASE_SEC"], CONFIG["BLOGGER_BACKOFF_CAP_SEC"])
        raise last_err if last_err else RuntimeError("Unknown blogger error")

    def _auth_sc_sync(self):
        scopes = ["https://www.googleapis.com/auth/webmasters.readonly"]
        return build("searchconsole", "v1", credentials=self._load_creds(scopes), cache_discovery=False)

    async def sc_site_ctr(self):
        if not CONFIG["SEARCH_CONSOLE_ENABLED"] or not CONFIG["SC_DOMAIN"]:
            return None
        try:
            if not self.sc_service:
                self.sc_service = await asyncio.to_thread(self._auth_sc_sync)
            body = {
                "startDate": (now_utc() - datetime.timedelta(days=7)).strftime("%Y-%m-%d"),
                "endDate": now_utc().strftime("%Y-%m-%d"),
                "dimensions": ["page"],
                "rowLimit": 250
            }
            r = await asyncio.to_thread(lambda: self.sc_service.searchanalytics().query(siteUrl="sc-domain:"+CONFIG["SC_DOMAIN"], body=body).execute())
            rows = r.get("rows", [])
            if not rows:
                return None
            clicks = sum(x.get("clicks", 0) for x in rows)
            impr = sum(x.get("impressions", 0) for x in rows)
            return (clicks / impr) if impr else None
        except Exception:
            return None

    async def sc_page_ctr(self, page_url: str):
        if not CONFIG["SEARCH_CONSOLE_ENABLED"] or not CONFIG["SC_DOMAIN"]:
            return None
        page_url = safe_url(page_url)
        if not page_url:
            return None
        variants = [page_url, _normalize_sc_url(page_url)]
        try:
            p = urlparse(variants[1] or variants[0])
            path = p.path or "/"
            if path != "/":
                alt = (path + "/") if not path.endswith("/") else path[:-1]
                variants.append(urlunparse((p.scheme, p.netloc, alt, "", p.query or "", "")))
        except Exception:
            pass
        uniq = []
        seen = set()
        for v in variants:
            if v and v not in seen:
                uniq.append(v); seen.add(v)
        try:
            if not self.sc_service:
                self.sc_service = await asyncio.to_thread(self._auth_sc_sync)
            for u in uniq:
                body = {
                    "startDate": (now_utc() - datetime.timedelta(days=14)).strftime("%Y-%m-%d"),
                    "endDate": now_utc().strftime("%Y-%m-%d"),
                    "dimensions": ["page"],
                    "dimensionFilterGroups": [{"filters": [{"dimension": "page", "operator": "equals", "expression": u}]}],
                    "rowLimit": 25
                }
                r = await asyncio.to_thread(lambda: self.sc_service.searchanalytics().query(siteUrl="sc-domain:"+CONFIG["SC_DOMAIN"], body=body).execute())
                rows = r.get("rows", [])
                if rows:
                    clicks = sum(x.get("clicks", 0) for x in rows)
                    impr = sum(x.get("impressions", 0) for x in rows)
                    return (clicks / impr) if impr else None
            return None
        except Exception:
            return None

    # ---- Gemini JSON ----
    _JSON_BLOCK_RE = re.compile(r"```(?:json)?\s*([\s\S]*?)\s*```", re.IGNORECASE)
    _FIRST_BRACES_RE = re.compile(r"(\{[\s\S]*\})")
    _FIRST_BRACKETS_RE = re.compile(r"(\[[\s\S]*\])")

    def _extract_json(self, text: str):
        if not text:
            return None
        s = text.strip()
        try:
            return json.loads(s)
        except Exception:
            pass
        for rgx in (self._JSON_BLOCK_RE, self._FIRST_BRACES_RE, self._FIRST_BRACKETS_RE):
            m = rgx.search(s)
            if m:
                try:
                    return json.loads(m.group(1).strip())
                except Exception:
                    pass
        return None

    async def gemini_json(self, prompt: str, fallback):
        if GEMINI is None:
            return fallback
        for attempt in range(CONFIG["GEMINI_MAX_RETRIES"]):
            if _should_stop():
                return fallback
            try:
                r = await asyncio.to_thread(
                    GEMINI.models.generate_content,
                    model=CONFIG["GEMINI_MODEL"],
                    contents=prompt,
                    config=types.GenerateContentConfig(response_mime_type="application/json", temperature=0.6),
                )
                data = self._extract_json(getattr(r, "text", "") or "")
                return data if isinstance(data, (dict, list)) else fallback
            except Exception as e:
                msg = str(e).lower()
                retriable = any(x in msg for x in ("429", "quota", "rate", "temporarily"))
                if (not retriable) or attempt >= CONFIG["GEMINI_MAX_RETRIES"] - 1:
                    return fallback
                await _backoff_sleep(attempt, CONFIG["GEMINI_BACKOFF_BASE_SEC"], CONFIG["GEMINI_BACKOFF_CAP_SEC"])
        return fallback

    # ---- Content helpers ----
    def _cache_fresh(self, ts: float) -> bool:
        return (time.time() - float(ts or 0)) <= float(CONFIG["CACHE_TTL_DAYS"]) * 86400.0

    def _title_ok(self, t: str) -> bool:
        t = (t or "").strip()
        if not t or not (45 <= len(t) <= 70):
            return False
        low = t.lower()
        return any(x in low for x in ("review", "worth it", "best", "deal", "price"))

    def _fallback_title(self, product: str) -> str:
        base = (product or "Product").strip()
        suf = " Review: Worth It? Best Price & Deal Tips"
        base = base[:max(10, 70-len(suf))]
        return (base + suf)[:70]

    async def generate_title(self, product: str) -> str:
        key = (product or "")[:160]
        cached = self.store.cache_get("titles", key)
        if cached and self._cache_fresh(cached["ts"]):
            try:
                t = json.loads(cached["json"]).get("title", "")
                if self._title_ok(t):
                    return str(t)[:90]
            except Exception:
                pass
        fb = {"titles":[self._fallback_title(product)], "best": self._fallback_title(product)}
        prompt = f"""Return JSON only.
Schema: {{ "titles": ["..."], "best": "..." }}
Rules: 45-70 chars, US audience, no clickbait, include one of review/worth it/best/deal/price.
Product: {product}
"""
        data = await self.gemini_json(prompt, fb)
        pool = []
        if isinstance(data, dict):
            if isinstance(data.get("best"), str):
                pool.append(data["best"].strip())
            if isinstance(data.get("titles"), list):
                pool += [x.strip() for x in data["titles"] if isinstance(x, str) and x.strip()]
        pick = next((t for t in pool if self._title_ok(t)), self._fallback_title(product))
        self.store.cache_set("titles", key, json.dumps({"title": pick[:90]}, ensure_ascii=False))
        return pick[:90]

    _SPEC_PATTERN = re.compile(r"\b\d{1,4}(\.\d+)?\s?(w|mah|lm|gbps|hz|mm|cm|in|inch|inches|ft|lb|lbs)\b", re.IGNORECASE)
    def soften_specs(self, s: str, max_len: int):
        s = (s or "").strip()
        if not s:
            return ""
        s = self._SPEC_PATTERN.sub("see product page", s)
        return s[:max_len]

    async def blurbs(self, products: list) -> dict:
        out, missing = {}, []
        for p in products:
            nm = (p.get("name") or "").strip()
            if not nm:
                continue
            cached = self.store.cache_get("blurbs", nm[:200])
            if cached and self._cache_fresh(cached["ts"]):
                try:
                    out[nm] = json.loads(cached["json"])
                    continue
                except Exception:
                    pass
            missing.append({"name": nm, "platform": p.get("platform","")})
        if not missing:
            return out
        prompt = f"""Return JSON only.
Schema: {{ "items": [{{"name":"EXACT","one_liner":"<=90","pros":["..",".."],"cons":[".."],"best_for":"<=70"}}] }}
Rules: generic statements only, no invented specs, guide to verify product page.
Products: {json.dumps(missing, ensure_ascii=False)}
"""
        data = await self.gemini_json(prompt, {"items":[]})
        items = data.get("items", []) if isinstance(data, dict) else []
        for it in items if isinstance(items, list) else []:
            if not isinstance(it, dict):
                continue
            nm = (it.get("name") or "").strip()
            if not nm:
                continue
            b = {
                "one_liner": self.soften_specs(it.get("one_liner",""), 90),
                "pros": [self.soften_specs(x, 120) for x in (it.get("pros") or [])[:2] if isinstance(x,str) and x.strip()],
                "cons": [self.soften_specs(x, 120) for x in (it.get("cons") or [])[:1] if isinstance(x,str) and x.strip()],
                "best_for": self.soften_specs(it.get("best_for",""), 70),
            }
            out[nm] = b
            self.store.cache_set("blurbs", nm[:200], json.dumps(b, ensure_ascii=False))
        return out

    async def llm_article(self, products: list, layout: str, internal_links: list) -> str:
        meta = "\n".join(f"- {p['name']} ({p.get('platform','')})" for p in products)
        links = "\n".join(f"- {x.get('title','')} ({x.get('url','')})" for x in internal_links) if internal_links else ""
        structure = "Problem → Checklist → [[PRODUCT_CARDS_HERE]] → Summary" if layout == "A" else "Problem → [[PRODUCT_CARDS_HERE]] → Reason → Summary"
        prompt = f"""You are a US-based affiliate blogger writing for US/UK English readers.
Hard Rules:
- Natural American English
- Honest tone (no exaggerated claims)
- Do NOT invent specs or precise numbers
- Output HTML only (no markdown fences)
- Must include a short FAQ section at the end (3 Q/A)
- Add internal links naturally (if any) as normal <a href="...">Title</a>

Structure:
{structure}

Products:
{meta}

Internal links you may use:
{links}
"""
        if GEMINI is None:
            return "<p>Here’s an honest comparison to help you choose.</p><p>[[PRODUCT_CARDS_HERE]]</p><p><b>Quick summary:</b> Pick the option that matches your needs and budget.</p>"
        for attempt in range(CONFIG["GEMINI_MAX_RETRIES"]):
            if _should_stop():
                break
            try:
                r = await asyncio.to_thread(GEMINI.models.generate_content, model=CONFIG["GEMINI_MODEL"], contents=prompt)
                txt = re.sub(r"```.*?```", "", (getattr(r, "text", "") or ""), flags=re.S).strip()
                return txt
            except Exception as e:
                msg = str(e).lower()
                retriable = any(x in msg for x in ("429", "quota", "rate", "temporarily"))
                if (not retriable) or attempt >= CONFIG["GEMINI_MAX_RETRIES"] - 1:
                    return "<p>Here’s an honest comparison to help you choose.</p><p>[[PRODUCT_CARDS_HERE]]</p><p><b>Quick summary:</b> Pick the option that matches your needs and budget.</p>"
                await _backoff_sleep(attempt, CONFIG["GEMINI_BACKOFF_BASE_SEC"], CONFIG["GEMINI_BACKOFF_CAP_SEC"])
        return "<p>Here’s an honest comparison to help you choose.</p><p>[[PRODUCT_CARDS_HERE]]</p>"

    # ---- selection ----
    def qualified_sales(self) -> int:
        return int(CONFIG["AMAZON_QUALIFIED_SALES_ENV"]) if CONFIG["AMAZON_QUALIFIED_SALES_ENV"] >= 0 else 0

    def mix(self) -> dict:
        if self.qualified_sales() >= 3:
            return CONFIG["MIX_PHASE2"]
        first_seen = self.store.get_meta_f("amazon_first_seen_ts", 0.0)
        if first_seen and (time.time() - first_seen) / 86400.0 >= float(CONFIG["MIX_SWITCH_DAYS"]):
            return CONFIG["MIX_PHASE2"]
        return CONFIG["MIX_PHASE1"]

    def pick_layout(self) -> str:
        scores = self.store.layout_scores()
        keys = list(scores.keys())
        if keys and random.random() < float(CONFIG["LAYOUT_EPSILON"]):
            return random.choice(keys)
        return max(scores, key=lambda k: float(scores.get(k, 1.0)))

    def pick_platform(self, mix: dict, available: list) -> str:
        plats = [k for k in mix.keys() if k in available]
        if not plats:
            return "AliExpress"
        weights = [float(mix[k]) for k in plats]
        s = sum(weights)
        if s <= 0:
            return random.choice(plats)
        r = random.random() * s
        acc = 0.0
        for k, w in zip(plats, weights):
            acc += w
            if r <= acc:
                return k
        return plats[-1]

    def pick_products(self, pool: dict, k: int = 3) -> list:
        mix = self.mix()
        chosen, used = [], set()
        for _ in range(k):
            avail = [plat for plat, arr in pool.items() if any(p["uid"] not in used for p in arr)]
            plat = self.pick_platform(mix, avail)
            arr = pool.get(plat, [])
            pick = None
            for cand in arr[:40]:
                if cand["uid"] not in used:
                    pick = cand; break
            if not pick:
                for fb_plat, fb_arr in pool.items():
                    for cand in fb_arr[:40]:
                        if cand["uid"] not in used:
                            pick = cand; break
                    if pick: break
            if pick:
                used.add(pick["uid"])
                chosen.append(pick)
        return chosen

    def pick_cta(self, rank: int, platform: str) -> str:
        if platform == "Amazon":
            strong = ["👉 Check today’s Amazon price (fast US shipping)", "👉 See the current Amazon deal (price may change)", "👉 Read reviews & check the latest Amazon price"]
            soft = ["👉 View details on Amazon", "👉 Check the latest price on Amazon"]
            return random.choice(strong) if rank == 1 else random.choice(soft)
        if platform == "Temu":
            strong = ["👉 Check today’s price on Temu (limited-time deals)", "👉 See reviews & price on Temu"]
            soft = ["👉 View details on Temu", "👉 Check the latest price on Temu"]
            return random.choice(strong) if rank == 1 else random.choice(soft)
        strong = ["👉 Check today’s price on AliExpress (often discounted)", "👉 See reviews & price on AliExpress"]
        soft = ["👉 View details on AliExpress", "👉 Check the latest price on AliExpress"]
        return random.choice(strong) if rank == 1 else random.choice(soft)

    # ---- publish gates ----
    def build_allowed_domains(self, chosen: list, internal_links: list):
        allowed = set(d.lower() for d in CONFIG["ALLOWED_LINK_DOMAINS"] if d.strip())
        if not allowed:
            allowed.update(["amazon.com","amazon.co.uk","amzn.to","aliexpress.com","a.aliexpress.com","s.click.aliexpress.com","star.aliexpress.com","temu.com","temu.to","share.temu.com"])
        for it in internal_links or []:
            u = safe_url(str(it.get("url") or ""))
            if u: allowed.add(_host_of(u))
        for p in chosen or []:
            u = safe_url(str(p.get("url") or ""))
            if u: allowed.add(_host_of(u))
            img = safe_url(str(p.get("image_url") or ""))
            if img: allowed.add(_host_of(img))
        return {d for d in allowed if d}

    def validate_publish(self, title: str, body: str, chosen: list):
        if not title or not body:
            return False, "empty"
        if len(body) < int(CONFIG["MIN_BODY_CHARS"]):
            return False, "body_too_short"
        if _DISCLOSURE_MARKER not in body:
            return False, "missing_disclosure_marker"
        if "product-card" not in body or body.lower().count('class="cta-button"') < 3:
            return False, "missing_cards_or_cta"
        for p in chosen:
            u = safe_url(p.get("url",""))
            if not u or u == "#":
                return False, "bad_product_url"
        return True, "ok"

    def ensure_min_body(self, body: str):
        if len(body) >= int(CONFIG["MIN_BODY_CHARS"]):
            return body
        filler = """
<h3>Buying guide (quick checklist)</h3>
<ul>
  <li><b>Compatibility:</b> Confirm the details on the product page.</li>
  <li><b>Returns:</b> Check return policy and shipping time before ordering.</li>
  <li><b>Reviews:</b> Skim recent reviews for real-world fit and reliability.</li>
</ul>
"""
        out = body
        for _ in range(5):
            if len(out) >= int(CONFIG["MIN_BODY_CHARS"]):
                break
            out += filler
        return out

    # =====================================================
    # Collect
    # =====================================================
    def _ali_debug(self, msg: str):
        if CONFIG["ALI_DEBUG"]:
            logging.info(f"[ALI_DEBUG] {msg}")

    def _ali_warn_marker(self, promo: str):
        if not CONFIG["ALI_TRACKING_MARKERS"]:
            return
        low = (promo or "").lower()
        if not any(m.lower() in low for m in CONFIG["ALI_TRACKING_MARKERS"]):
            logging.warning(f"[ALI_DEBUG] promo url missing markers: {promo[:200]}")

    def _parse_ali_promo(self, data: dict) -> dict:
        if not isinstance(data, dict):
            return {}
        for ek in ("error_response","errorResponse","error","errors"):
            if ek in data:
                return {}
        root = data.get("result", data)
        cand = []
        if isinstance(root, dict):
            for k in ("promotionUrls","promotion_urls","promotionUrl","promotion_url","promotionURL","promotionURLs"):
                if k in root:
                    cand.append(root.get(k))
        if not cand:
            def walk(o):
                if isinstance(o, dict):
                    for kk, vv in o.items():
                        if "promotion" in str(kk).lower():
                            cand.append(vv)
                        walk(vv)
                elif isinstance(o, list):
                    for it in o:
                        walk(it)
            walk(root)
        promo_list = None
        for x in cand:
            if isinstance(x, list):
                promo_list = x; break
            if isinstance(x, dict):
                for kk, vv in x.items():
                    if isinstance(vv, list) and "url" in kk.lower():
                        promo_list = vv; break
            if promo_list is not None:
                break
        if not isinstance(promo_list, list):
            return {}
        out = {}
        for row in promo_list:
            if isinstance(row, (list, tuple)) and len(row) >= 2:
                orig = str(row[0] or "").strip()
                purl = str(row[1] or "").strip()
                if orig:
                    out[orig] = purl if is_valid_http_url(purl) else orig
            elif isinstance(row, dict):
                orig = str(row.get("url") or row.get("originalUrl") or row.get("originUrl") or row.get("productUrl") or "").strip()
                purl = str(row.get("promotionUrl") or row.get("promoteUrl") or row.get("promotion_url") or row.get("promoUrl") or "").strip()
                if orig:
                    out[orig] = purl if is_valid_http_url(purl) else orig
        return out

    async def _ali_promo_chunk(self, chunk: list):
        endpoint = f"https://gw.api.alibaba.com/openapi/param2/2/portals.open/api.getPromotionLinks/{CONFIG['ALI_APP_KEY']}"
        params = {"fields":"trackingId,publisherId,url,promotionUrl","trackingId":CONFIG["ALI_TRACKING_ID"],"urls":",".join(chunk)}
        d = await self.http_get_json(endpoint, params=params)
        if isinstance(d, dict):
            m = self._parse_ali_promo(d)
            if m: return m
        params2 = {
            "app_key": CONFIG["ALI_APP_KEY"],
            "timestamp": now_utc().strftime("%Y-%m-%d %H:%M:%S"),
            "format": "json",
            "v": "2.0",
            "sign_method": "sha256",
            "fields":"trackingId,publisherId,url,promotionUrl",
            "trackingId": CONFIG["ALI_TRACKING_ID"],
            "urls": ",".join(chunk),
        }
        params2["sign"] = ali_sign(params2)
        d2 = await self.http_get_json(endpoint, params=params2)
        if isinstance(d2, dict):
            m2 = self._parse_ali_promo(d2)
            if m2: return m2
        return {}

    async def ali_get_promotion_links(self, urls: list):
        urls = [u for u in urls if is_valid_http_url(u)]
        if not urls:
            return {}
        if not CONFIG["ALI_APP_KEY"] or not CONFIG["ALI_TRACKING_ID"]:
            return {u: u for u in urls}
        out = {}
        remaining = list(urls)
        for bs in (50,20,10,5,1):
            if not remaining:
                break
            new_rem = []
            for i in range(0, len(remaining), bs):
                chunk = remaining[i:i+bs]
                mapped = {}
                try:
                    mapped = await self._ali_promo_chunk(chunk)
                except Exception as e:
                    self._ali_debug(str(e))
                if not mapped:
                    if bs == 1:
                        out[chunk[0]] = chunk[0]
                    else:
                        new_rem.extend(chunk)
                    continue
                for u in chunk:
                    if u in mapped and is_valid_http_url(mapped[u]):
                        out[u] = mapped[u]
                        self._ali_warn_marker(mapped[u])
                    else:
                        if bs == 1:
                            out[u] = u
                        else:
                            new_rem.append(u)
            remaining = new_rem
        for u in urls:
            out.setdefault(u, u)
        return out

    async def ali_api(self, method: str, extra: dict):
        if not CONFIG["ALI_APP_KEY"] or not CONFIG["ALI_APP_SECRET"]:
            return None
        params = {"app_key":CONFIG["ALI_APP_KEY"],"method":method,"timestamp":now_utc().strftime("%Y-%m-%d %H:%M:%S"),
                  "format":"json","v":"2.0","sign_method":"sha256", **extra}
        params["sign"] = ali_sign(params)
        url = "https://gw.api.alibaba.com/openapi/param2/2/portals.open/api.listPromotionProduct/"
        return await self.http_get_json(url, params=params)

    async def ali_search(self, keyword: str, page_no: int = 1, page_size: int = 20):
        d = await self.ali_api("aliexpress.affiliate.product.query", {"keywords": keyword, "page_no": page_no, "page_size": page_size})
        if d is None:
            return None
        if not isinstance(d, dict):
            return []
        return d.get("result", {}).get("products", []) or []

    async def temu_collect(self, limit: int = 30):
        if not CONFIG["TEMU_ENABLED"] or not CONFIG["TEMU_FEED_URL"]:
            return None
        d = await self.http_get_json(CONFIG["TEMU_FEED_URL"])
        if d is None:
            return None
        items = d.get("items", []) if isinstance(d, dict) else []
        if not isinstance(items, list):
            return []
        random.shuffle(items)
        return items[:limit]

    async def run_collect(self):
        self.store.cache_cleanup(CONFIG["CACHE_TTL_DAYS"])
        self.store.prune_quarantine(CONFIG["QUARANTINE_RETENTION_DAYS"])
        self.store.cleanup_products(CONFIG["PRODUCT_RETENTION_DAYS"])

        last_try = self.store.get_meta_f("last_collect_try_ts", 0.0)
        if time.time() - last_try < float(CONFIG["EMPTY_COLLECT_RETRY_SEC"]):
            return
        last_ok = self.store.get_meta_f("last_collect_ts", 0.0)
        if time.time() - last_ok < float(CONFIG["MIN_COLLECT_INTERVAL_SEC"]):
            return

        self.store.set_meta_f("last_collect_try_ts", time.time())

        upserts = []

        # Ali
        if CONFIG["ALI_APP_KEY"] and CONFIG["ALI_APP_SECRET"] and CONFIG["ALI_TRACKING_ID"]:
            kw_list = CONFIG["ALI_KEYWORDS"] or ["usb c hub", "gan charger"]
            for kw in random.sample(kw_list, min(2, len(kw_list))):
                if _should_stop():
                    break
                page = random.randint(1, 5)
                prods = await self.ali_search(kw, page_no=page, page_size=20)
                if prods is None:
                    continue
                cands = []
                for p in prods:
                    if not isinstance(p, dict):
                        continue
                    name = (p.get("product_title") or p.get("productTitle") or "").strip()
                    url = (p.get("product_detail_url") or p.get("productUrl") or p.get("product_url") or "").strip()
                    img = (p.get("product_main_image_url") or p.get("imageUrl") or p.get("productMainImageUrl") or "").strip()
                    pid = p.get("product_id") or p.get("productId") or ""
                    if not name or not is_valid_http_url(url):
                        continue
                    cands.append({"name": name, "url": url, "image_url": img if is_valid_http_url(img) else "", "product_id": str(pid).strip()})
                if not cands:
                    continue
                promo = await self.ali_get_promotion_links([c["url"] for c in cands])
                for c0 in cands:
                    orig = c0["url"]
                    promo_url = safe_url(promo.get(orig, orig)) or safe_url(orig)
                    if not promo_url:
                        continue
                    stable_id = c0["product_id"] or url_hash_id(orig) or url_hash_id(promo_url)
                    if not stable_id:
                        continue
                    uid = f"AliExpress::{stable_id}"
                    upserts.append({
                        "uid": uid, "platform": "AliExpress", "stable_id": stable_id, "name": c0["name"], "asin": "",
                        "url": promo_url, "source_url": safe_url(orig) or "", "image_url": c0["image_url"], "source_keyword": kw,
                        "added_ts": time.time(), "ctr_score": 1.0, "last_used_ts": 0.0
                    })

        # Temu (optional)
        if CONFIG["TEMU_ENABLED"] and CONFIG["TEMU_FEED_URL"] and not _should_stop():
            titems = await self.temu_collect(limit=30)
            if isinstance(titems, list) and titems:
                for p in titems:
                    if not isinstance(p, dict):
                        continue
                    name = (p.get("title") or p.get("name") or "").strip()
                    url = (p.get("link") or p.get("url") or "").strip()
                    img = (p.get("image") or p.get("image_url") or "").strip()
                    if not name or not is_valid_http_url(url):
                        continue
                    stable_id = str(p.get("id") or p.get("item_id") or p.get("itemId") or p.get("sku_id") or p.get("skuId") or "").strip() or url_hash_id(url)
                    if not stable_id:
                        continue
                    uid = f"Temu::{stable_id}"
                    upserts.append({
                        "uid": uid, "platform": "Temu", "stable_id": stable_id, "name": name, "asin": "",
                        "url": safe_url(url), "source_url": "", "image_url": img if is_valid_http_url(img) else "", "source_keyword": "",
                        "added_ts": time.time(), "ctr_score": 1.0, "last_used_ts": 0.0
                    })

        if upserts:
            n = self.store.upsert_products(upserts)
            logging.info(f"Collect upserted≈{n}")
        self.store.set_meta_f("last_collect_ts", time.time())

    # =====================================================
    # Post
    # =====================================================
    async def run_post(self):
        if self.store.count_posts_today_utc() >= int(CONFIG["MAX_POSTS_PER_DAY"]):
            return

        last_post_ts = self.store.get_meta_f("last_post_ts", 0.0)
        if time.time() - last_post_ts < float(CONFIG["MIN_POST_INTERVAL_SEC"]):
            return

        last_try = self.store.get_meta_f("last_post_try_ts", 0.0)
        if last_try and last_post_ts < last_try and time.time() - last_try < float(CONFIG["POST_FAILURE_COOLDOWN_SEC"]):
            return
        self.store.set_meta_f("last_post_try_ts", time.time())

        uses30 = self.store.usage_counts_last_days(30)
        last_used = self.store.last_used_map()
        cooldown_sec = float(CONFIG["PRODUCT_COOLDOWN_DAYS"]) * 86400.0
        max_uses = int(CONFIG["PRODUCT_MAX_USES_30D"])

        pool = {"AliExpress": [], "Temu": [], "Amazon": []}
        for plat in pool.keys():
            items = self.store.products_by_platform(plat, limit=450)
            for p in items:
                uid = p["uid"]
                lu = float(last_used.get(uid, p.get("last_used_ts", 0)) or 0)
                c30 = int(uses30.get(uid, 0) or 0)
                if lu and (time.time() - lu) < cooldown_sec:
                    continue
                if c30 >= max_uses:
                    continue
                pp = dict(p)
                if plat == "Amazon":
                    pp["url"] = ensure_amazon_tag(pp.get("url",""))
                pp["url"] = safe_url(pp.get("url",""))
                if not pp["url"]:
                    continue
                if not is_valid_http_url(pp.get("image_url","")):
                    pp["image_url"] = ""
                pool[plat].append(pp)
            pool[plat].sort(key=lambda x: float(x.get("ctr_score", 1.0) or 1.0), reverse=True)

        if sum(len(v) for v in pool.values()) < 3:
            self.store.quarantine("post", "not_enough_candidates", {"counts": {k: len(v) for k, v in pool.items()}})
            return

        layout = None
        chosen = None
        topic_hash = None
        for _ in range(6):
            layout = self.pick_layout()
            cand = self.pick_products(pool, k=3)
            if len(cand) < 3:
                continue
            keys = sorted([c["uid"] for c in cand])
            topic_hash = hashlib.sha1((layout + "||" + "||".join(keys)).encode("utf-8")).hexdigest()[:16]
            if not self.store.topic_seen_recently(topic_hash, CONFIG["TOPIC_DUPLICATE_WINDOW_DAYS"]):
                chosen = cand
                break
        if not chosen:
            self.store.quarantine("post", "topic_duplicate_window", {"window_days": CONFIG["TOPIC_DUPLICATE_WINDOW_DAYS"]})
            return

        has_amazon = any(p.get("platform") == "Amazon" for p in chosen)
        if has_amazon and not self.store.get_meta_f("amazon_first_seen_ts", 0.0):
            self.store.set_meta_f("amazon_first_seen_ts", time.time())

        internal_links = self.store.recent_posts(3)
        allowed = self.build_allowed_domains(chosen, internal_links)

        html = await self.llm_article(chosen, layout, internal_links)
        html = ensure_disclosure_once(html)
        if has_amazon and CONFIG["AMAZON_DISCLOSURE_HTML"]:
            html = ensure_amazon_disclosure_once(html)
        html = ensure_faq(html)
        if CONFIG["SANITIZE_HTML"]:
            html = sanitize_html_basic(html, allowed)

        bmap = await self.blurbs(chosen)

        cards = []
        for idx, p in enumerate(chosen, start=1):
            nm_raw = p.get("name","Unknown")
            plat_raw = p.get("platform","AliExpress")
            url = safe_url(p.get("url","")) or "#"
            nm = safe_text(nm_raw, 240)
            plat = safe_text(plat_raw, 60)
            b = bmap.get(nm_raw, {})
            one = safe_text((b.get("one_liner") or "").strip(), 160)
            pros = b.get("pros") if isinstance(b.get("pros"), list) else []
            cons = b.get("cons") if isinstance(b.get("cons"), list) else []
            best_for = safe_text((b.get("best_for") or "").strip(), 120)
            cta = safe_text(self.pick_cta(idx, plat_raw), 140)
            pro_html = "".join(f"<li>{safe_text(x,120)}</li>" for x in pros[:2] if isinstance(x,str) and x.strip())
            con_html = "".join(f"<li>{safe_text(x,120)}</li>" for x in cons[:1] if isinstance(x,str) and x.strip())
            snippet = ""
            if one: snippet += f"<p style='margin:8px 0 10px;opacity:.92;'><i>{one}</i></p>"
            if pro_html: snippet += f"<div style='margin:10px 0;'><b>Pros</b><ul>{pro_html}</ul></div>"
            if con_html: snippet += f"<div style='margin:10px 0;'><b>Cons</b><ul>{con_html}</ul></div>"
            if best_for: snippet += f"<p style='margin:10px 0;'><b>Best for:</b> {best_for}</p>"
            img = (p.get("image_url") or "").strip()
            img_tag = ""
            if is_valid_http_url(img):
                img_tag = f"<img src=\"{safe_attr(img)}\" alt=\"{nm}\" style='width:100%;max-width:720px;border-radius:14px;margin:10px 0;' loading='lazy'/>"
            cards.append(f"""
<div class="product-card" style="border:1px solid #eee;border-radius:16px;padding:18px;margin:18px 0;background:#fff;">
  <h3 style="margin:0 0 8px;">{idx}. {nm} ({plat})</h3>
  {img_tag}
  {snippet}
  <a href="{safe_attr(url)}" target="_blank" rel="{safe_attr(CONFIG['CTA_REL'],200)}" class="cta-button"
     style="display:block;text-align:center;padding:14px 16px;border-radius:12px;text-decoration:none;font-weight:700;">
    {cta}
  </a>
</div>
""")

        marker = "[[PRODUCT_CARDS_HERE]]"
        body = html.replace(marker, "\n".join(cards)) if marker in html else (html + "\n<h3>Top picks</h3>\n" + "\n".join(cards))
        body = self.ensure_min_body(body)
        if CONFIG["SANITIZE_HTML"]:
            body = sanitize_html_basic(body, allowed)

        title = await self.generate_title(chosen[0].get("name","Product"))
        ok, reason = self.validate_publish(title, body, chosen)
        if not ok:
            self.store.quarantine("post", reason, {"title": title[:120], "layout": layout, "topic_hash": topic_hash})
            return

        post_body = {"title": title, "content": body, "labels": ["affiliate","review","comparison"]}

        if self.dry_run:
            post_url = "https://example.invalid/dry-run"
            ctr_signal = 0.03
            logging.info(f"[DRY-RUN] Would post: {title}")
        else:
            if not CONFIG["BLOG_ID"] or not os.path.exists(CONFIG["TOKEN_PATH"]):
                self.store.quarantine("post", "missing_blog_auth", {"BLOG_ID": bool(CONFIG["BLOG_ID"]), "TOKEN_PATH": CONFIG["TOKEN_PATH"]})
                return
            try:
                post = await self.blogger_insert_with_retry(post_body)
            except Exception as e:
                self.store.quarantine("post", "blogger_insert_failed", {"err": str(e)[:300]})
                return
            post_url = safe_url(post.get("url","") or "")
            ctr_page = await self.sc_page_ctr(post_url) if post_url else None
            if ctr_page is None:
                ctr_site = await self.sc_site_ctr()
                ctr_signal = float(ctr_site) if isinstance(ctr_site,(int,float)) and ctr_site is not None else 0.03
            else:
                ctr_signal = float(ctr_page)

        self.store.set_meta_f("last_post_ts", time.time())

        # Update scores (EMA)
        updates = []
        for p in chosen:
            base = float(p.get("ctr_score", 1.0) or 1.0)
            new_score = base * 0.85 + float(ctr_signal) * 10.0
            updates.append({"uid": p["uid"], "ctr_score": new_score, "last_used_ts": time.time()})
        self.store.update_product_scores(updates)

        lay_scores = self.store.layout_scores()
        old = float(lay_scores.get(layout, 1.0) or 1.0)
        self.store.set_layout_score(layout, old * 0.85 + float(ctr_signal) * 10.0)

        self.store.insert_post(title, post_url, time.time(), layout, topic_hash)
        self.store.log_topic(topic_hash)
        self.store.log_uses([p["uid"] for p in chosen])

        logging.info(f"Posted: {title} url={post_url}")

    # =====================================================
    # Pinterest (optional)
    # =====================================================
    async def run_pinterest(self):
        if not CONFIG["PINTEREST_ENABLED"]:
            return
        if not (CONFIG["PINTEREST_ACCESS_TOKEN"] and CONFIG["PINTEREST_BOARD_ID"]):
            return

        disabled_until = self.store.get_meta_f("pinterest_disabled_until", 0.0)
        if disabled_until and time.time() < disabled_until:
            return
        if self.store.pins_today_count_utc() >= int(CONFIG["PINTEREST_MAX_PINS_PER_DAY"]):
            return

        last_pin_ts = self.store.get_meta_f("last_pin_ts", 0.0)
        if time.time() - last_pin_ts < float(CONFIG["PINTEREST_MIN_INTERVAL_SEC"]):
            return

        recent = self.store.recent_posts(1)
        if not recent:
            return
        post_url = safe_url(recent[0]["url"] or "")
        if not post_url:
            return

        picks = (
            self.store.products_by_platform("AliExpress", 80)
            + self.store.products_by_platform("Temu", 80)
            + self.store.products_by_platform("Amazon", 80)
        )
        picks = [p for p in picks if is_valid_http_url(p.get("image_url",""))]
        if not picks:
            return
        img = picks[0]["image_url"]

        endpoint = "https://api.pinterest.com/v5/pins"
        headers = {"Authorization": f"Bearer {CONFIG['PINTEREST_ACCESS_TOKEN']}", "Content-Type": "application/json"}
        payload = {
            "board_id": CONFIG["PINTEREST_BOARD_ID"],
            "title": f"New post: {recent[0]['title']}"[:95],
            "description": "Quick comparison + picks. Check the post for details."[:500],
            "link": post_url,
            "media_source": {"source_type": "image_url", "url": img},
        }

        for attempt in range(3):
            if _should_stop():
                return
            try:
                async with self.http.post(endpoint, headers=headers, data=json.dumps(payload)) as r:
                    txt = await r.text()
                    if r.status in (401, 403):
                        self.store.set_meta_f("pinterest_disabled_until", time.time() + float(CONFIG["PINTEREST_COOLDOWN_ON_AUTH_SEC"]))
                        logging.warning(f"Pinterest auth error disabled. status={r.status} body={_strip_for_log(txt)}")
                        return
                    if _is_retriable_http_status(r.status):
                        raise RuntimeError(f"Pinterest {r.status}: {_strip_for_log(txt)}")
                    if r.status >= 400:
                        logging.warning(f"Pinterest failed {r.status} body={_strip_for_log(txt)}")
                        return
                    data = None
                    try: data = json.loads(txt)
                    except Exception: data = None
                    pin_id = str((data or {}).get("id") or "")
                    self.store.set_meta_f("last_pin_ts", time.time())
                    self.store.pinterest_log(pin_id, post_url, img)
                    logging.info(f"Pinterested pin_id={pin_id} post={post_url}")
                    return
            except Exception as e:
                if attempt >= 2:
                    logging.warning(f"Pinterest failed final: {e}")
                    return
                await _backoff_sleep(attempt, 1.6, 12.0)

# ============================================================
# Main loop
# ============================================================

def preflight_warnings():
    if not CONFIG["GEMINI_KEY"]:
        logging.warning("[PREFLIGHT] GEMINI_API_KEY missing -> LLM uses fallback templates.")
    if not CONFIG["BLOG_ID"]:
        logging.warning("[PREFLIGHT] BLOG_ID missing -> publishing will be quarantined.")
    if not os.path.exists(CONFIG["TOKEN_PATH"]):
        logging.warning(f"[PREFLIGHT] token missing TOKEN_PATH={CONFIG['TOKEN_PATH']} -> publishing quarantined.")
    if CONFIG["SEARCH_CONSOLE_ENABLED"] and not CONFIG["SC_DOMAIN"]:
        logging.warning("[PREFLIGHT] SEARCH_CONSOLE_ENABLED=true but SC_DOMAIN missing -> CTR disabled.")

async def run_loop(once: bool, dry_run: bool):
    global _STOP_EVENT, _STOP_LOOP
    store = IGStore(CONFIG["DB_PATH"])
    store.init()
    store.migrate_from_legacy_json_if_needed(CONFIG["LEGACY_PRODUCTS_JSON"], CONFIG["LEGACY_HISTORY_JSON"], CONFIG["LEGACY_STATS_JSON"], do_backup=CONFIG["LEGACY_AUTO_BACKUP"])
    bot = ImperialGalaxy(store, dry_run=dry_run)
    _STOP_LOOP = asyncio.get_running_loop()
    _STOP_EVENT = asyncio.Event()

    try:
        try:
            _STOP_LOOP.add_signal_handler(signal.SIGTERM, _handle_stop_signal, None, None)
            _STOP_LOOP.add_signal_handler(signal.SIGINT, _handle_stop_signal, None, None)
        except Exception:
            pass

        preflight_warnings()

        error_streak = 0

        async def safe_run(name: str, coro):
            nonlocal error_streak
            try:
                await coro
                error_streak = 0
            except Exception as e:
                error_streak += 1
                logging.error(f"{name} error: {e}", exc_info=True)

        while not _SHOULD_STOP:
            await safe_run("run_collect", bot.run_collect())
            await safe_run("run_post", bot.run_post())
            await safe_run("run_pinterest", bot.run_pinterest())

            # daily snapshot once per UTC day
            last_snap = store.get_meta("last_snapshot_day", "")
            today = utc_day_str()
            if CONFIG["SNAPSHOT_ENABLED"] and last_snap != today:
                try:
                    store.export_snapshot_json(CONFIG["LEGACY_PRODUCTS_JSON"], CONFIG["LEGACY_HISTORY_JSON"], CONFIG["LEGACY_STATS_JSON"], CONFIG["SNAPSHOT_SUFFIX"])
                    store.set_meta("last_snapshot_day", today)
                except Exception:
                    pass

            if once:
                break

            if error_streak >= int(CONFIG["ERROR_STREAK_MAX"]):
                pause = float(CONFIG["ERROR_STREAK_PAUSE_SEC"])
                logging.warning(f"[CIRCUIT] error_streak={error_streak} -> pause {pause:.0f}s")
                try:
                    await asyncio.wait_for(_STOP_EVENT.wait(), timeout=pause)
                except asyncio.TimeoutError:
                    pass
                error_streak = 0

            last_post_ts = store.get_meta_f("last_post_ts", 0.0)
            last_collect_ts = store.get_meta_f("last_collect_ts", 0.0)
            last_collect_try_ts = store.get_meta_f("last_collect_try_ts", 0.0)

            def until_due(last_ts: float, interval: float, if_zero: float):
                if float(last_ts or 0) <= 0:
                    return float(if_zero)
                return max(0.0, float(last_ts) + float(interval) - time.time())

            next_post = until_due(last_post_ts, CONFIG["MIN_POST_INTERVAL_SEC"], min(600.0, float(CONFIG["MIN_POST_INTERVAL_SEC"])))
            next_collect_ok = until_due(last_collect_ts, CONFIG["MIN_COLLECT_INTERVAL_SEC"], 300.0)
            next_collect_retry = until_due(last_collect_try_ts, CONFIG["EMPTY_COLLECT_RETRY_SEC"], 0.0)

            next_due = min(next_post, next_collect_ok, next_collect_retry)
            sleep_s = min(float(CONFIG["MAX_LOOP_SLEEP_SEC"]), max(float(CONFIG["MIN_LOOP_SLEEP_SEC"]), next_due))
            if next_due <= 1.0:
                sleep_s = float(CONFIG["MIN_LOOP_SLEEP_SEC"])

            try:
                await asyncio.wait_for(_STOP_EVENT.wait(), timeout=sleep_s)
            except asyncio.TimeoutError:
                pass
    finally:
        await bot.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    logging.info(f"ImperialGalaxy start v={VERSION} once={args.once} dry_run={args.dry_run}")
    asyncio.run(run_loop(once=args.once, dry_run=args.dry_run))

if __name__ == "__main__":
    main()
