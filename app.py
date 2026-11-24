import os, io, threading, hashlib, secrets, json, re, uuid, hmac, base64, mimetypes, functools, time, requests, smtplib, difflib
from email.message import EmailMessage
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
import psycopg2, psycopg2.extras, psycopg2.pool
from markupsafe import Markup, escape
from flask import Flask, render_template, request, redirect, url_for, session, flash, abort, Response, g, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from dotenv import load_dotenv
from pypdf import PdfReader
load_dotenv()
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "change-me")
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 86400      # browsers keep css/js/video for a day: far fewer requests when many students are online

# ---------------- Supabase (Postgres) ----------------
DBURL = os.getenv("DATABASE_URL", "").strip()
if DBURL.startswith("postgres://"): DBURL = "postgresql://" + DBURL[len("postgres://"):]
if DBURL.startswith("postgresql://") and "sslmode=" not in DBURL: DBURL += ("&" if "?" in DBURL else "?") + "sslmode=require"
DB_POOL = int(os.getenv("DB_POOL", "10"))      # keep this <= your Supabase pooler connection limit
_dbsem = threading.BoundedSemaphore(DB_POOL)
_pool = None
def pool():
    global _pool
    if _pool is None:
        if not DBURL: raise RuntimeError("DATABASE_URL is not set (add your Supabase connection string)")
        if not DBURL.startswith("postgresql://"):
            raise RuntimeError("DATABASE_URL must be the Supabase POSTGRES connection string (starts with postgresql://), not the https://...supabase.co project URL. Supabase > Connect > Session pooler.")
        _pool = psycopg2.pool.ThreadedConnectionPool(1, DB_POOL, DBURL, connect_timeout=15)
    return _pool

def q(*a, **k):
    """Run SQL on Supabase (waits for a free connection when many users are online)."""
    with _dbsem: return _q(*a, **k)

def _q(sql, a=(), one=False, rid=False):
    """Run SQL on Supabase. Use ? placeholders. rid=True -> returns the new row id (inserts)."""
    sql = sql.replace("?", "%s") + (" returning id" if rid else "")
    for attempt in (1, 2):
        p = pool(); c = p.getconn()
        try:
            with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, a)
                if rid: r = cur.fetchone()["id"]
                elif cur.description is not None: r = cur.fetchone() if one else cur.fetchall()
                else: r = None
            c.commit(); p.putconn(c); return r
        except (psycopg2.OperationalError, psycopg2.InterfaceError):   # dropped connection -> retry once
            try: p.putconn(c, close=True)
            except Exception: pass
            if attempt == 2: raise
        except Exception:
            try: c.rollback()
            except Exception: pass
            p.putconn(c); raise

SCHEMA = [
 "create table if not exists users(id serial primary key,name text,email text unique,phone text,pw text,profession text default '',goal text default '',pay_status text default 'none')",
 "create table if not exists payments(id serial primary key,user_id integer,img bytea,mime text default 'image/jpeg',status text default 'pending',created timestamp default current_timestamp)",
 "create table if not exists courses(id serial primary key,user_id integer,track text,title text,exam text,days integer,plan text,cards text,topics text,test text,coach text default '',created timestamp default current_timestamp,src text default '',dtest text default '')",
 "create table if not exists results(id serial primary key,course_id integer,topic text,correct integer,total integer)",
 "create table if not exists checkins(id serial primary key,user_id integer,mood text)",
 "create table if not exists lessons(id serial primary key,course_id integer,day integer,body text,unique(course_id,day))",
 "create table if not exists chats(id serial primary key,course_id integer,day integer,role text,msg text,created timestamp default current_timestamp)",
 "create table if not exists progress(course_id integer,day integer,primary key(course_id,day))",
 "create table if not exists doubts(id serial primary key,user_id integer,question text,answer text,created timestamp default current_timestamp)",
 "alter table courses add column if not exists research text default ''",
 "alter table courses add column if not exists qmode text default 'syllabus'",
 "create table if not exists iq(id serial primary key,course_id integer,n integer,topic text,q text,ans text default '',unique(course_id,n))",
 "alter table users add column if not exists suspended boolean default false",
 "alter table users add column if not exists access text default ''",
 "alter table users add column if not exists last_seen timestamp",
 "alter table users add column if not exists sess_token text default ''",
 "alter table users add column if not exists device_id text default ''",
 "alter table payments add column if not exists decided timestamp",
 "update payments set decided=created where decided is null and status<>'pending'",
 "update users set access='paid' where pay_status='approved' and (access is null or access='')",
 "create table if not exists settings(key text primary key,val text default '',img bytea,mime text default 'image/png')",
 "create table if not exists topicx(course_id integer,key text,body text,primary key(course_id,key))",
 "create index if not exists i_courses_user on courses(user_id)",
 "create index if not exists i_chats_cd on chats(course_id,day)",
 "create index if not exists i_results_c on results(course_id)",
 "create index if not exists i_doubts_u on doubts(user_id)",
 # ---- faculty / student roles, classes, 2-device limit ----
 "alter table users add column if not exists role text default 'student'",
 "alter table users add column if not exists class_id integer",
 "alter table users add column if not exists cur_cid integer",
 "alter table users add column if not exists cur_day integer",
 "create table if not exists classes(id serial primary key,faculty_id integer,course text,subjects text default '[]',code text unique,created timestamp default current_timestamp)",
 "create table if not exists devices(id serial primary key,user_id integer,device_id text,sess_token text default '',label text default '',created timestamp default current_timestamp,last_seen timestamp default current_timestamp,unique(user_id,device_id))",
 # old one-device accounts keep their device: move it into the new devices table once, then clear the old columns
 "insert into devices(user_id,device_id,sess_token,label) select id,device_id,coalesce(sess_token,''),'Earlier device' from users u where coalesce(device_id,'')<>'' and not exists(select 1 from devices d where d.user_id=u.id)",
 "update users set device_id='', sess_token='' where coalesce(device_id,'')<>''",
 "create index if not exists i_users_class on users(class_id)",
 "create index if not exists i_devices_user on devices(user_id)",
 "create index if not exists i_classes_fac on classes(faculty_id)",
 "alter table users add column if not exists verified boolean default true",
 "create table if not exists notes_cache(key text primary key,sources text default '[]',raw text default '',created timestamp default current_timestamp)",
 "create table if not exists jobs(id serial primary key,user_id integer,kind text default 'build',track text default 'college',status text default 'queued',step text default '',msg text default '',warn text default '',first text default '',course_id integer,created timestamp default current_timestamp)",
 "create index if not exists i_jobs_user on jobs(user_id)",
 "update jobs set status='error', msg='The server restarted while building. Please try again.' where status in ('queued','running')",
 # ---- verified professor notes + the student's concern ----
 "alter table courses add column if not exists notes text default ''",
 "alter table courses add column if not exists notes_src text default ''",
 "alter table courses add column if not exists notes_meta text default ''",
 "alter table courses add column if not exists concern text default ''",
 "alter table courses add column if not exists pyq text default ''",
 "alter table courses add column if not exists pyq_meta text default ''",
]
def init_db():
    p = pool(); c = p.getconn()
    try:
        with c.cursor() as cur:
            cur.execute("select pg_advisory_xact_lock(727272)")   # safe when several workers start together
            for s in SCHEMA: cur.execute(s)
        c.commit()
    except Exception:
        c.rollback(); raise
    finally: p.putconn(c)
try: init_db()
except Exception as _e: print("DB INIT ERROR:", _e)

# ---------------- accounts: student / faculty, 2-device limit ----------------
MAX_DEVICES = int(os.getenv("MAX_DEVICES", "2"))
CODE_CHARS = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"      # no 0/O/1/I/L so codes are easy to read out loud

_HASH_SEM = threading.BoundedSemaphore(int(os.getenv("HASH_CONCURRENCY", "2")))
def chk_pw(h, p):
    with _HASH_SEM: return check_password_hash(h, p)
def make_pw(p):
    with _HASH_SEM: return generate_password_hash(p)

def is_faculty(u): return bool(u) and (u.get("role") == "faculty")

def device_label():
    ua = request.headers.get("User-Agent", "")
    os_ = ("iPhone" if "iPhone" in ua else "iPad" if "iPad" in ua else "Android phone" if "Android" in ua else "Windows PC" if "Windows" in ua
           else "Mac" if "Macintosh" in ua else "Linux PC" if "Linux" in ua else "Device")
    br = "Edge" if "Edg/" in ua else "Chrome" if ("Chrome" in ua or "CriOS" in ua) else "Firefox" if "Firefox" in ua else "Safari" if "Safari" in ua else "browser"
    return f"{os_} · {br}"

def client_ip():
    return (request.headers.get("X-Forwarded-For", "").split(",")[0].strip() or request.remote_addr or "?")

_fails = {}
def throttled(key, n=8, win=600):
    """Stop password guessing and mail spam: n attempts per window (default 8 per 10 minutes) for the same key."""
    now = time.time(); _fails[key] = [t for t in _fails.get(key, []) if now - t < win]
    return len(_fails[key]) >= n
def note_fail(key): _fails.setdefault(key, []).append(time.time())

def login_user(u, dest):
    """Log `u` in on THIS device. An account may use at most MAX_DEVICES devices (2). Returns a response, or None if the limit is reached.
    A device is a browser holding the long-life 'gp_dev' cookie; each device has its own session token, so both stay logged in."""
    did = request.cookies.get("gp_dev") or secrets.token_hex(24)
    tok = secrets.token_hex(24); lbl = device_label()
    if q("select 1 from devices where user_id=? and device_id=?", (u["id"], did), one=True):
        q("update devices set sess_token=?, label=?, last_seen=localtimestamp where user_id=? and device_id=?", (tok, lbl, u["id"], did))
    else:      # one atomic insert that only succeeds while the account has fewer than MAX_DEVICES devices
        r = q("insert into devices(user_id,device_id,sess_token,label) select ?,?,?,? where (select count(*) from devices where user_id=?) < ? on conflict do nothing returning id",
              (u["id"], did, tok, lbl, u["id"], MAX_DEVICES), one=True)
        if not r: return None
    q("update users set last_seen=localtimestamp where id=?", (u["id"],))
    session.clear(); session["uid"] = u["id"]; session["tok"] = tok; session["seen"] = time.time()
    resp = redirect(dest)
    resp.set_cookie("gp_dev", did, max_age=10*365*24*3600, httponly=True, samesite="Lax", secure=request.is_secure)
    return resp

LIMIT_MSG = ("This account is already used on %d devices, which is the maximum. Log in on one of those devices and remove the device you no longer use "
             "(Profile → My devices), or ask the admin to reset your devices.") % MAX_DEVICES

def home_for(u): return url_for("faculty_home") if is_faculty(u) else url_for("dashboard")

def me():
    if "uid" not in session: return None
    if "u" not in g: g.u = q("select * from users where id=?", (session["uid"],), one=True)
    return g.u
@app.context_processor
def inj():
    u = me(); cls = None
    if u and not is_faculty(u) and u.get("class_id"): cls = class_of(u)
    return dict(me=u, my_class=cls, faculty=is_faculty(u))

def class_of(u):
    if not u or not u.get("class_id"): return None
    if "cls" not in g: g.cls = q("select c.*, f.name as faculty_name from classes c left join users f on f.id=c.faculty_id where c.id=?", (u["class_id"],), one=True)
    return g.cls
def subjects_of(c):
    try: return [str(s) for s in json.loads(c["subjects"] or "[]")]
    except Exception: return []

def new_class_code():
    for _ in range(20):
        code = "".join(secrets.choice(CODE_CHARS) for _ in range(6))
        if not q("select 1 from classes where code=?", (code,), one=True): return code
    raise RuntimeError("could not make a class code")

def send_mail(to, subject, body):
    """Email through Brevo's HTTPS API (works on Render's free plan) or plain SMTP. Raises if neither is configured."""
    frm = os.getenv("MAIL_FROM", "").strip(); name = os.getenv("MAIL_NAME", "GrowthPulse")
    key = os.getenv("BREVO_API_KEY", "").strip(); host = os.getenv("SMTP_HOST", "").strip()
    if key and frm:
        r = requests.post("https://api.brevo.com/v3/smtp/email", headers={"api-key": key, "accept": "application/json", "content-type": "application/json"},
                          json={"sender": {"name": name, "email": frm}, "to": [{"email": to}], "subject": subject, "textContent": body}, timeout=20)
        print("mail: brevo", r.status_code, r.text[:200])
        if not r.ok: raise RuntimeError(f"email service said {r.status_code}: {r.text[:200]}")
        return r.text[:120]
    if host:
        msg = EmailMessage(); msg["From"] = f"{name} <{frm or os.getenv('SMTP_USER', '')}>"; msg["To"] = to; msg["Subject"] = subject; msg.set_content(body)
        port = int(os.getenv("SMTP_PORT", "587"))
        s = smtplib.SMTP_SSL(host, port, timeout=20) if port == 465 else smtplib.SMTP(host, port, timeout=20)
        try:
            if port != 465: s.starttls()
            if os.getenv("SMTP_USER"): s.login(os.getenv("SMTP_USER"), os.getenv("SMTP_PASS", ""))
            s.send_message(msg); print("mail: smtp sent to", to)
        finally:
            try: s.quit()
            except Exception: pass
        return
    raise RuntimeError("email is not set up on the server (add BREVO_API_KEY + MAIL_FROM, or SMTP_HOST/SMTP_USER/SMTP_PASS)")

def temp_password(): return "".join(secrets.choice("abcdefghjkmnpqrstuvwxyzABCDEFGHJKMNPQRSTUVWXYZ23456789") for _ in range(10))

def paid(f):
    @functools.wraps(f)
    def w(*a, **k):
        u = me()
        if not u: return redirect(url_for("login"))
        if is_faculty(u):
            flash("Faculty accounts use the faculty dashboard.", "err"); return redirect(url_for("faculty_home"))
        if u["pay_status"] != "approved":
            flash("Unlock all features with a one-time payment of ₹50.", "err"); return redirect(url_for("pay"))
        return f(*a, **k)
    return w

def faculty_only(f):
    @functools.wraps(f)
    def w(*a, **k):
        u = me()
        if not u: return redirect(url_for("flogin"))
        if not is_faculty(u): flash("That page is for faculty only.", "err"); return redirect(url_for("dashboard"))
        return f(*a, **k)
    return w

def text_from(fs):
    if not fs or not fs.filename: return ""
    n = fs.filename.lower()
    try:
        if n.endswith(".pdf"): return "\n".join((p.extract_text() or "") for p in PdfReader(fs.stream).pages)
        if n.endswith((".txt", ".md", ".csv")): return fs.read().decode("utf-8", "ignore")
    except Exception: pass
    return ""


# ---------------- make AI text look human-written ----------------
SUP = str.maketrans("0123456789+-=()n", "⁰¹²³⁴⁵⁶⁷⁸⁹⁺⁻⁼⁽⁾ⁿ")
SUB = str.maketrans("0123456789", "₀₁₂₃₄₅₆₇₈₉")
SYM = {"times": "×", "div": "÷", "cdot": "·", "pm": "±", "mp": "∓", "leq": "≤", "le": "≤", "geq": "≥", "ge": "≥", "neq": "≠", "ne": "≠",
       "approx": "≈", "infty": "∞", "to": "→", "rightarrow": "→", "Rightarrow": "⇒", "implies": "⇒", "therefore": "∴", "because": "∵",
       "circ": "°", "degree": "°", "int": "∫", "sum": "Σ", "prod": "∏", "partial": "∂", "Delta": "Δ", "delta": "δ", "pi": "π",
       "theta": "θ", "alpha": "α", "beta": "β", "gamma": "γ", "lambda": "λ", "mu": "μ", "sigma": "σ", "omega": "ω", "phi": "φ",
       "epsilon": "ε", "rho": "ρ", "tau": "τ", "log": "log", "ln": "ln", "sin": "sin", "cos": "cos", "tan": "tan", "lim": "lim",
       "quad": " ", "qquad": "  ", "left": "", "right": "", "ldots": "...", "dots": "...", "cdots": "...", "displaystyle": ""}
def _frac(a, b):
    f = lambda s: s.strip() if re.fullmatch(r"[\w.]+", s.strip()) else "(" + s.strip() + ")"
    return f(a) + "/" + f(b)
def clean(t):
    """Turn markdown / LaTeX leftovers into plain, teacher-style writing."""
    if not isinstance(t, str): return t
    for bad, good in (("\x0crac", "\\frac"), ("\timesx", "\\timesx"), ("\times", "\\times"), ("\text{", "\\text{"), ("\theta", "\\theta"), ("\right", "\\right")):
        t = t.replace(bad, good)
    t = re.sub(r"```[a-zA-Z]*\n?", "", t)
    t = t.replace("$$", "").replace("\\(", "").replace("\\)", "").replace("\\[", "").replace("\\]", "")
    t = re.sub(r"\$([^$\n]{1,200})\$", lambda m: m.group(1) if re.search(r"[\\^_{}=]", m.group(1)) else m.group(0), t)
    for _ in range(4):
        t = re.sub(r"\\sqrt\s*\{([^{}]*)\}", lambda m: "√" + (m.group(1) if re.fullmatch(r"\w+", m.group(1)) else "(" + m.group(1) + ")"), t)
        t = re.sub(r"\\(?:text|mathrm|mathbf|textbf|operatorname|mathit)\s*\{([^{}]*)\}", r"\1", t)
        t = re.sub(r"\\[dt]?frac\s*\{([^{}]*)\}\s*\{([^{}]*)\}", lambda m: _frac(m.group(1), m.group(2)), t)
    t = re.sub(r"\\(?:text|mathrm|mathbf|textbf|operatorname|mathit)\s*\{([^{}]*)\}", r"\1", t)
    t = re.sub(r"\\([a-zA-Z]+)", lambda m: SYM.get(m.group(1), m.group(0)), t)
    t = t.replace("\\%", "%"); t = re.sub(r"\\[,;:! ]", " ", t)
    t = re.sub(r"\^\{?(-?[0-9n]+)\}?", lambda m: m.group(1).translate(SUP), t)
    t = re.sub(r"\^\{([^{}]*)\}", r"^(\1)", t)
    t = re.sub(r"(?<=[A-Za-z])_\{?(\d+)\}?", lambda m: m.group(1).translate(SUB), t)
    t = re.sub(r"\bsqrt\s*\(", "√(", t)
    t = t.replace("**", "").replace("__", "").replace("`", "")
    t = re.sub(r"(?m)^\s*#{1,6}\s*", "", t)
    t = re.sub(r"(?<=[\d\)])\s*\*\s*(?=[\d\(a-zA-Z])", " × ", t)
    t = re.sub(r"(?m)^(\s*)[-*]\s+(?=[A-Za-z])", r"\1• ", t).replace("*", "")
    return re.sub(r"\n{3,}", "\n\n", t).strip()
def deep_clean(x):
    if isinstance(x, str): return clean(x)
    if isinstance(x, list): return [deep_clean(i) for i in x]
    if isinstance(x, dict): return {k: deep_clean(v) for k, v in x.items()}
    return x

STYLE = """WRITING STYLE (very important): write like a kind human teacher writing on paper, not like software. Plain text only: no markdown, no asterisks, no # headings, no tables, no LaTeX, no $ signs. Write maths with ordinary symbols: × ÷ √ ² ³ ± ≤ ≥ ≈ π ° and fractions like 3/4. Short, simple sentences."""
MATH_STYLE = """For any maths or numerical problem write the solution exactly like a teacher writes it in a notebook, every line on its own line:
Given: (what we know)
To find: (what is asked)
Formula: (the rule we will use)
Step 1: (a few words on WHAT we do and WHY)
= (the working)
Step 2: (same way)
= (the working)
Answer: (final answer with units)
Check: (a quick check that the answer makes sense)
Never skip arithmetic: one operation per line. Put a short reason beside every step."""

def ink(text):
    """Notebook-style rendering for maths solutions (handwriting font on ruled paper)."""
    out = []
    for l in clean(text or "").split("\n"):
        s = l.strip()
        if not s: out.append('<div class="ln">&nbsp;</div>'); continue
        cls = "ln"
        if re.match(r"(?i)^(final\s+)?ans(wer)?\s*[:=]", s): cls += " ans"
        elif re.match(r"(?i)^step\s*\d+", s): cls += " stp"
        elif re.match(r"(?i)^link to your topic\s*:", s): cls += " lk"
        elif re.match(r"(?i)^(given|to find|find|formula|solution|check|verify|proof|aim|note|definition|example|diagram|remember|real life|key points|why)\s*:", s): cls += " hd"
        elif s[0] in "=≈": cls += " eq"
        if re.match(r"(?i)^sources used\s*:", s): cls += " hd"
        h = str(escape(l.rstrip()))
        h = re.sub(r"(https?://[^\s<]+?)([.,;)]*)(?=\s|$)", r'<a href="\1" target="_blank" rel="noopener noreferrer" class="lnk">\1</a>\2', h)
        out.append('<div class="%s">%s</div>' % (cls, h))
    return Markup("".join(out))
def ans_html(msg):
    """Every tutor answer is shown as handwritten notes on ruled paper."""
    m = clean(msg or "")
    t = re.match(r"^\[(MATHS?|THEORY)\]\s*", m, re.I)
    if t: m = m[t.end():]
    return Markup('<div class="paper">%s</div>' % ink(m))
def pp(text):
    return Markup('<div class="paper">%s</div>' % ink(text))
app.add_template_filter(ink, "ink"); app.add_template_filter(ans_html, "ans"); app.add_template_filter(pp, "pp")

NOTE_STYLE = """HANDWRITTEN NOTES STYLE (very important): write exactly like a good teacher writing neat class notes by hand for a student. One short idea per line, every line separated by a newline. Begin with the direct answer or definition in one line. Then explain step by step in very simple words. Always include one small everyday EXAMPLE (start that line with 'Example:'). You cannot draw pictures, so never mention or describe diagrams. Finish with one line starting 'Remember:' that holds the key points to memorise. Never write long paragraphs."""


DEPTH_STYLE = """DEPTH RULE (very important): the student must understand the WHOLE concept from the very beginning to full depth, never a thin or partial answer.
- If the student names a whole topic/concept: teach it completely from zero. What it is, why it exists, where it is used, the basic words, how it works step by step, the main parts/types/formulas/rules, several worked examples (easy to hard), common mistakes, exam points, and a short summary.
- If the student asks about ONE specific part, question or doubt: first give a short big-picture of the whole concept it belongs to (what the full concept is and where this part fits), then answer that specific thing in high-level depth (full reasoning, every step, why it is true, a worked example, and a second example from a different angle), then connect it back to the whole concept.
- Never skip the basics, never stop at the definition, never say 'and so on'. Cover every important point; length should match the depth needed."""

def tutor(question, ctx="", doc="", imgs=None, focus=""):
    return ai(f"""A student has a doubt. {('The doubt is about this part of their study material: ' + ctx) if ctx else ''}
{focus}
Student's doubt: {question or 'Please read my handwritten note and clarify my doubt.'}
{'A photo of the handwritten note is attached; say what you understood, and if anything is unclear ask the student to write it more clearly.' if imgs else ''}
Reference material (may be empty):
{doc[:12000]}
Explain like a patient teacher to a beginner, with everyday examples.
{DEPTH_STYLE}
{NOTE_STYLE}
{STYLE}
{MATH_STYLE}
{LINK_RULE if focus else ''}
{SHORT_RULE}""", 5500, imgs or None)

def qctx(c):
    if (c.get("qmode") or "syllabus") != "questions": return ""
    rows = q("select q from iq where course_id=? order by n", (c["id"],))
    return "IMPORTANT QUESTIONS given by the student's teacher (base the test on these, they are likely to come in the exam): " + " | ".join(r["q"] for r in rows)[:5000]

def qplan(items, days):
    """Spread the important questions over the days (max 8 per day)."""
    nd = max(1, min(days, len(items))); nd = max(nd, -(-len(items) // 8)); size = -(-len(items) // nd)
    plan = []
    for i in range(0, len(items), size):
        part = items[i:i + size]
        plan.append({"day": len(plan) + 1, "focus": list(dict.fromkeys(x["topic"] for x in part)), "qs": [x["n"] for x in part],
                     "tip": "Read the model answer, close the book, then write it in your own words."})
    return plan

VISION = os.getenv("VISION_MODEL", "qwen/qwen3.8-27b")
_VCACHE = {}
def vision_models(k):
    """Vision models to try: your VISION_MODEL, known current ones, then anything live on your Groq account that looks vision-capable."""
    if "v" in _VCACHE: return _VCACHE["v"]
    out = []
    for m in (os.getenv("VISION_MODEL"), "qwen/qwen3.8-27b", "qwen/qwen3.6-27b", "meta-llama/llama-4-maverick-17b-128e-instruct"):
        if m and m not in out: out.append(m)
    try:
        r = requests.get("https://api.groq.com/openai/v1/models", headers={"Authorization": "Bearer " + k}, timeout=15)
        live = [x["id"] for x in r.json().get("data", [])] if r.ok else []
        if live:
            out = [m for m in out if m in live] + [m for m in live if re.search(r"qwen3\.\d|llama-4|vision|vl", m) and m not in out and "scout" not in m]
    except Exception as e: print("model list error", str(e)[:100])
    _VCACHE["v"] = out or ["qwen/qwen3.8-27b"]; return _VCACHE["v"]

AI_SEM = threading.BoundedSemaphore(int(os.getenv("AI_CONCURRENCY", "6")))
AI_WAIT = int(os.getenv("AI_WAIT", "100"))
class BusyError(RuntimeError): pass
def groq_post(key, body, timeout=140):
    """Every AI call goes through here: at most AI_CONCURRENCY run at once for the whole server (so 100 students queue politely instead of
    flooding Groq), and a 'too many requests' answer from Groq is waited out and retried instead of failing."""
    if not AI_SEM.acquire(timeout=AI_WAIT): raise BusyError("Many students are using the AI right now. Please try again in a minute.")
    try:
        for i in range(3):
            r = requests.post("https://api.groq.com/openai/v1/chat/completions", headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"}, json=body, timeout=timeout)
            if r.status_code != 429 or i == 2: return r
            try: wait = float(r.headers.get("retry-after", "") or 0)
            except Exception: wait = 0
            if wait > 20: return r
            time.sleep(max(wait, 2 * (i + 1)))
        return r
    finally: AI_SEM.release()

def ai(prompt, tokens=6000, images=None, clean_out=True, effort="low"):
    k = os.getenv("GROQ_API_KEY")
    if not k: raise RuntimeError("GROQ_API_KEY not set")
    if images:   # handwritten photos -> vision model
        models = vision_models(k)
        content = [{"type": "text", "text": prompt}] + [{"type": "image_url", "image_url": {"url": u}} for u in images]
    else:
        models = []; content = prompt
        for m in (os.getenv("AI_MODEL"), "openai/gpt-oss-120b", "openai/gpt-oss-20b"):
            if m and m not in models: models.append(m)
    err = ""
    for m in models:
        body = {"model": m, "max_tokens": tokens + 4000, "temperature": 0.3 if images else 0.4,
                "messages": [{"role": "system", "content": "You are a warm, expert tutor and coach. When JSON is requested reply with valid JSON only, no markdown fences."},
                             {"role": "user", "content": content}]}
        if "gpt-oss" in m: body["reasoning_effort"] = effort
        r = groq_post(k, body, 140)
        if r.ok:
            o = re.sub(r"<think>.*?</think>", "", r.json()["choices"][0]["message"]["content"] or "", flags=re.S).strip()
            return clean(o) if clean_out else o
        err = f"{m}: {r.status_code} {r.text[:200]}"
        if r.status_code not in (400, 404, 429, 503): break
    raise RuntimeError(err)
def aj(prompt, tokens=6000, images=None, effort="low"):
    return deep_clean(json.loads(re.search(r"[\[{].*[\]}]", ai(prompt, tokens, images, clean_out=False, effort=effort), re.S).group(0), strict=False))

def research(subject, exam="", note=""):
    """Search the web (Groq built-in search, same API key) for important topics + frequently asked questions.
    Returns a short plain-text brief. Never raises: if search fails the app simply continues without it."""
    k = os.getenv("GROQ_API_KEY")
    if not k: return ""
    prompt = f"""Use web search. Subject / goal: '{subject}' ({exam or 'general learning'}), for a student in India. {('Syllabus notes: ' + note[:1200]) if note else ''}
Find and write, from what you find online:
1) IMPORTANT TOPICS: the topics that matter most, each with a short reason (how often asked, real use).
2) FREQUENTLY ASKED QUESTIONS: 8 to 12 real questions or problem types that commonly appear in university exams, competitive exams or job interviews on this subject.
3) REAL-WORLD USE: 2 or 3 real examples of how this is used in industry or daily life today.
Plain text only, short lines, no tables, no markdown, under 450 words. End with one line 'Sources:' followed by up to 5 website names."""
    tries = [(os.getenv("SEARCH_MODEL", "groq/compound"), False), ("groq/compound-mini", False), ("openai/gpt-oss-120b", True)]
    for m, browser in tries:
        body = {"model": m, "temperature": 0.3, "messages": [{"role": "user", "content": prompt}]}
        if browser: body.update(tools=[{"type": "browser_search"}], tool_choice="required", reasoning_effort="low", max_tokens=3000)
        try:
            r = groq_post(k, body, 55)
            if r.ok:
                t = clean(r.json()["choices"][0]["message"]["content"] or "")
                if len(t) > 80: return t[:5000]
        except Exception as e:
            print("research error", m, str(e)[:120])
    return ""

# ---------------- professor notes: search the web -> verify with Groq -> final handwritten notes ----------------
import ipaddress, socket
def safe_url(u):
    """Only plain public http(s) pages: the model's links are checked before our server touches them."""
    try:
        p = urlparse(u)
        if p.scheme not in ("http", "https") or not p.hostname or "." not in p.hostname or p.hostname.lower() == "localhost": return False
        for ai_ in socket.getaddrinfo(p.hostname, None):
            ip = ipaddress.ip_address(ai_[4][0])
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast: return False
        return True
    except Exception: return False

def link_alive(url):
    """Drop links the AI made up: a real page answers (403/429 still means the site exists; 404 or no answer means it does not)."""
    if not safe_url(url): return False
    h = {"User-Agent": "Mozilla/5.0 (GrowthPulse notes checker)"}
    try:
        r = requests.head(url, headers=h, timeout=6, allow_redirects=True)
        if r.status_code in (403, 405, 429, 501) or r.status_code >= 500:
            r = requests.get(url, headers=h, timeout=8, stream=True, allow_redirects=True); r.close()
        return r.status_code < 400 or r.status_code in (401, 403, 429)
    except Exception: return False

def trust_of(url):
    h = (urlparse(url).hostname or "").lower()
    if re.search(r"\.(edu|ac\.[a-z]{2}|edu\.[a-z]{2})$", h): return "university"
    if re.search(r"nptel|swayam|ocw\.|opencourseware|openstax|khanacademy", h): return "open course"
    return "web"

def parse_sources(text):
    out = []
    for blk in re.split(r"\n\s*-{3,}\s*(?:\n|$)", text or ""):
        t = re.search(r"SOURCE:\s*(.+)", blk); u = re.search(r"URL:\s*<?(https?://[^\s>]+)", blk)
        if not (t and u): continue
        by = re.search(r"POSTED BY:\s*(.+)", blk); why = re.search(r"WHY TRUSTED:\s*(.+)", blk); n = re.search(r"NOTES:\s*(.*)", blk, re.S)
        out.append(dict(title=clean(t.group(1))[:140], url=u.group(1).rstrip(").,;"), by=clean(by.group(1))[:100] if by else "",
                        why=clean(why.group(1))[:160] if why else "", notes=clean(n.group(1))[:2600] if n else ""))
    return out[:6]

def find_notes(subject, course_name, exam, topic_names, concern="", dbg=None, use_cache=True):
    """Step 1: web search for well-regarded lecture notes that professors / universities posted. Returns (sources, raw_text). Never raises.
    Results are cached 14 days per subject+course, so 100 students of one class cost ONE search. `dbg` (a list) collects what happened."""
    D = dbg if dbg is not None else []
    k = os.getenv("GROQ_API_KEY")
    if not k: D.append("GROQ_API_KEY is not set"); return [], ""
    ckey = hashlib.sha1(f"{subject.lower().strip()}|{(course_name or '').lower().strip()}|{exam}".encode()).hexdigest()
    if use_cache:
        try:
            row = q("select sources,raw from notes_cache where key=? and created > localtimestamp - interval '14 days'", (ckey,), one=True)
            if row: D.append("served from cache (searched earlier for this subject)"); return json.loads(row["sources"]), row["raw"]
        except Exception as e: D.append("cache read failed: " + str(e)[:100])
    prompt = f"""Use web search. Find the best and most trusted LECTURE NOTES / STUDY NOTES for the subject '{subject}' ({course_name or 'college course'}, {exam}) that were written or posted by PROFESSORS or lecturers: university course pages and PDFs (.edu, .ac.in, .ac.uk), NPTEL, MIT OpenCourseWare, IIT / NIT / university departments and similar. Prefer notes that are highly rated, widely recommended or widely used by students and teachers.
Topics the student needs: {', '.join(topic_names)[:1200] or subject}.
{('The student says: ' + concern[:300]) if concern else ''}
Give 4 or 5 different sources (at least 3 if they exist), from different universities or professors so they can be compared. Each exactly in this format:
SOURCE: title of the notes
URL: the full real web address you found
POSTED BY: professor or university
WHY TRUSTED: one short line (official course page, widely recommended, ...)
NOTES: the important definitions, formulas, steps and examples from that source, 10 to 25 short lines
---
Only list pages you really found; never guess a link. If you found no professor notes write only NONE. Plain text, no markdown."""
    tries = [(os.getenv("SEARCH_MODEL", "groq/compound"), False), ("groq/compound-mini", False), ("openai/gpt-oss-120b", True)]
    srcs = []; hits = []
    for m, browser in tries:
        body = {"model": m, "temperature": 0.2, "messages": [{"role": "user", "content": prompt}]}
        if browser: body.update(tools=[{"type": "browser_search"}], tool_choice="required", reasoning_effort="low", max_tokens=4000)
        try:
            r = groq_post(k, body, 60)
            if not r.ok: D.append(f"{m}: HTTP {r.status_code} {r.text[:160]}"); continue
            msg = r.json()["choices"][0]["message"]
            srcs = parse_sources(msg.get("content") or "")
            for ex_ in (msg.get("executed_tools") or []):       # pages the search tool really opened
                for x in ((ex_.get("search_results") or {}).get("results") or []):
                    if isinstance(x, dict) and x.get("url"): hits.append(dict(title=clean(str(x.get("title") or x["url"]))[:140], url=x["url"], by="", why="found by web search", notes=clean(str(x.get("content") or ""))[:2200]))
            D.append(f"{m}: HTTP 200, {len(srcs)} sources in the answer, {len(hits)} pages opened by the search tool")
            if srcs or hits: break
        except Exception as e: D.append(f"{m}: error {str(e)[:140]}")
    known = {s["url"] for s in srcs}
    cand = srcs + [h for h in hits if h["url"] not in known][:4 if srcs else 6]
    if not cand: D.append("no candidate pages at all"); return [], ""
    with ThreadPoolExecutor(6) as ex: alive = list(ex.map(lambda s: link_alive(s["url"]), cand))
    for s_, ok_ in zip(cand, alive): D.append(("LIVE  " if ok_ else "DEAD  (dropped) ") + s_["url"])
    cand = [dict(s, trust=trust_of(s["url"])) for s, ok in zip(cand, alive) if ok]
    order = {"university": 0, "open course": 1, "web": 2}
    cand = sorted(cand, key=lambda s: order[s["trust"]])[:5]     # professor / university pages first
    raw = "\n---\n".join(f"SOURCE {i}: {s['title']} ({s['by'] or s['trust']}) {s['url']}\n{s['notes']}" for i, s in enumerate(cand, 1) if s["notes"])[:11000]
    out = [{k_: s[k_] for k_ in ("title", "url", "by", "why", "trust")} for s in cand]
    if out and use_cache:
        try: q("insert into notes_cache(key,sources,raw) values(?,?,?) on conflict(key) do update set sources=excluded.sources, raw=excluded.raw, created=localtimestamp", (ckey, json.dumps(out), raw))
        except Exception as e: D.append("cache write failed: " + str(e)[:100])
    return out, raw

# ---------------- previous-year questions: search 4+ different websites -> merge -> use in tests ----------------
def parse_pyq_sources(text):
    out = []
    for blk in re.split(r"\n\s*-{3,}\s*(?:\n|$)", text or ""):
        t = re.search(r"SOURCE:\s*(.+)", blk); u = re.search(r"URL:\s*<?(https?://[^\s>]+)", blk)
        if not (t and u): continue
        y = re.search(r"YEAR:\s*(.+)", blk); ty = re.search(r"TYPE:\s*(.+)", blk); qs = re.search(r"QUESTIONS:\s*(.*)", blk, re.S)
        out.append(dict(title=clean(t.group(1))[:140], url=u.group(1).rstrip(").,;"), year=clean(y.group(1))[:40] if y else "", kind=clean(ty.group(1))[:30] if ty else "", notes=clean(qs.group(1))[:3500] if qs else ""))
    return out[:8]

def find_pyq(subject, course_name, exam, dbg=None, use_cache=True):
    """Web search for real previous-year question papers on several DIFFERENT websites. Returns (sources, raw_text). Never raises."""
    D = dbg if dbg is not None else []
    k = os.getenv("GROQ_API_KEY")
    if not k: D.append("GROQ_API_KEY is not set"); return [], ""
    ckey = "pyq|" + hashlib.sha1(f"{subject.lower().strip()}|{(course_name or '').lower().strip()}".encode()).hexdigest()
    if use_cache:
        try:
            row = q("select sources,raw from notes_cache where key=? and created > localtimestamp - interval '14 days'", (ckey,), one=True)
            if row: D.append("served from cache (searched earlier for this subject)"); return json.loads(row["sources"]), row["raw"]
        except Exception as e: D.append("cache read failed: " + str(e)[:100])
    prompt = f"""Use web search. Find PREVIOUS YEAR QUESTION PAPERS (past exam papers) for the subject '{subject}' ({course_name or 'college course'}, {exam}), from the last 10 years.
Search at least 4 DIFFERENT websites of different kinds: the university or college exam page, university or board question-paper archives, question-paper portals that collect old papers, and student study sites. Use a different website for each source.
For each website write exactly:
SOURCE: title of the page
URL: the full real web address you found
YEAR: exam year(s) shown on the page, or unknown
TYPE: university or portal or study site or other
QUESTIONS: the real questions copied from that page, one per line, up to 20
---
Only pages you really opened; never invent a question, year or link. If you found nothing write only NONE. Plain text, no markdown."""
    tries = [(os.getenv("SEARCH_MODEL", "groq/compound"), False), ("groq/compound-mini", False), ("openai/gpt-oss-120b", True)]
    srcs = []; hits = []
    for m, browser in tries:
        body = {"model": m, "temperature": 0.2, "messages": [{"role": "user", "content": prompt}]}
        if browser: body.update(tools=[{"type": "browser_search"}], tool_choice="required", reasoning_effort="low", max_tokens=4500)
        try:
            r = groq_post(k, body, 60)
            if not r.ok: D.append(f"{m}: HTTP {r.status_code} {r.text[:160]}"); continue
            msg = r.json()["choices"][0]["message"]
            srcs = parse_pyq_sources(msg.get("content") or "")
            for ex_ in (msg.get("executed_tools") or []):
                for x in ((ex_.get("search_results") or {}).get("results") or []):
                    if isinstance(x, dict) and x.get("url"): hits.append(dict(title=clean(str(x.get("title") or x["url"]))[:140], url=x["url"], year="", kind="", notes=clean(str(x.get("content") or ""))[:3000]))
            D.append(f"{m}: HTTP 200, {len(srcs)} websites in the answer, {len(hits)} pages opened by the search tool")
            if len(srcs) >= 3 or (srcs or hits) and m != tries[0][0]: break
        except Exception as e: D.append(f"{m}: error {str(e)[:140]}")
    known = {x["url"] for x in srcs}
    cand = srcs + [h for h in hits if h["url"] not in known][:6]
    if not cand: D.append("no candidate pages at all"); return [], ""
    with ThreadPoolExecutor(6) as ex: alive = list(ex.map(lambda x: link_alive(x["url"]), cand))
    for x, ok_ in zip(cand, alive): D.append(("LIVE  " if ok_ else "DEAD  (dropped) ") + x["url"])
    cand = [dict(x, trust=trust_of(x["url"])) for x, ok in zip(cand, alive) if ok]
    seen = set(); uniq = []
    for x in sorted(cand, key=lambda x: {"university": 0, "open course": 1, "web": 2}[x["trust"]]):     # one page per website, so the sources are really different sites
        h = (urlparse(x["url"]).hostname or "").lower().removeprefix("www.")
        if h not in seen: seen.add(h); uniq.append(x)
    cand = uniq[:6]
    raw = "\n---\n".join(f"SOURCE {i}: {x['title']} ({x['kind'] or x['trust']}, {x['year'] or 'year unknown'}) {x['url']}\n{x['notes']}" for i, x in enumerate(cand, 1) if x["notes"])[:14000]
    out = [{k_: x.get(k_, "") for k_ in ("title", "url", "year", "kind", "trust")} for x in cand]
    D.append(f"{len(out)} different websites kept")
    if out and use_cache:
        try: q("insert into notes_cache(key,sources,raw) values(?,?,?) on conflict(key) do update set sources=excluded.sources, raw=excluded.raw, created=localtimestamp", (ckey, json.dumps(out), raw))
        except Exception as e: D.append("cache write failed: " + str(e)[:100])
    return out, raw

def build_pyq(track, title, course_name, exam, topics, papers="", found=None):
    """Search (unless `found`) -> Groq merges the pages into one list of REAL distinct questions, with year and the sites each came from. Returns (pyq_json, meta_json); ('','') on failure."""
    if track != "college": return "", ""
    try:
        sources, raw = found if found is not None else find_pyq(title, course_name, exam)
        up = (papers or "").strip()
        if not raw and not up: return "", json.dumps(dict(status="none", sources=[], count=0, date=time.strftime("%d %b %Y")))
        names = [str(t.get("name", "")) for t in topics if isinstance(t, dict)][:25]
        data = aj(f"""You are collecting PREVIOUS YEAR EXAM QUESTIONS for '{title}' ({course_name or 'college course'}, {exam}).
Below are texts copied from {len(sources)} different web pages (numbered SOURCE 1, 2, ...){' and papers uploaded by the student (call them source 0)' if up else ''}.
TEXT FROM THE PAGES:
{raw[:11000] or '(none)'}
{('UPLOADED PAPERS (source 0):' + chr(10) + up[:5000]) if up else ''}
TASK: list every distinct real exam question that appears in this text. Merge duplicates and near-duplicates into one (keep the clearest wording, fix only typos, never change the meaning). For each give "q", "year" (only if the text states it, else ""), "topic" (the closest of these syllabus topics: {json.dumps(names)}), "marks" (only if stated, else ""), "src" (numbers of the sources it appears in).
NEVER invent a question, year or source that is not in the text. Skip headings, instructions and anything that is not a question.
Return JSON list only, at most 60 items, questions found in more sources first: [{{"q":"","year":"","topic":"","marks":"","src":[1]}}]""", 6500, effort="medium")
        if isinstance(data, dict): data = next((v for v in data.values() if isinstance(v, list)), [])
        items = []
        for x in data:
            if not isinstance(x, dict) or len(str(x.get("q", "")).strip()) < 12: continue
            nums = sorted({int(n) for n in (x.get("src") or []) if str(n).isdigit() and 0 <= int(n) <= len(sources)})
            items.append(dict(id=len(items) + 1, q=str(x["q"]).strip()[:600], year=str(x.get("year") or "").strip()[:12], topic=str(x.get("topic") or "General").strip()[:60] or "General",
                              marks=str(x.get("marks") or "").strip()[:10], src=nums, freq=max(1, len(nums))))
        items = items[:60]
        meta = dict(status="found" if items else "none", sources=sources, count=len(items), date=time.strftime("%d %b %Y"), uploaded=bool(up))
        return (json.dumps(items) if items else ""), json.dumps(meta)
    except Exception as e:
        print("pyq error", repr(e)[:200]); return "", ""

def pyq_list(c):
    try: return [i for i in json.loads(c.get("pyq") or "[]") if isinstance(i, dict)]
    except Exception: return []
def pyq_meta(c):
    try: return json.loads(c.get("pyq_meta") or "{}") or {}
    except Exception: return {}
def pyq_links(c, it):
    srcs = pyq_meta(c).get("sources") or []
    return [{"title": srcs[n - 1]["title"], "url": srcs[n - 1]["url"]} for n in it.get("src", []) if 1 <= n <= len(srcs)]
def pyq_pick(c, n):
    """Real previous-year questions for a test: weak topics first, then the ones asked on most websites, then the newest."""
    items = pyq_list(c); weak = {t for t, p in stats(c["id"]) if p < 70}
    return sorted(items, key=lambda i: (i["topic"] not in weak, -i.get("freq", 1), -(int(i["year"]) if str(i.get("year", "")).isdigit() else 0)))[:n]
def pyq_prompt(items):
    if not items: return "No previous-year questions could be found online, so write the questions yourself in the style of real exams."
    return "REAL PREVIOUS YEAR QUESTIONS found online (id | year | topic | question):\n" + "\n".join(f"{i['id']} | {i.get('year') or '?'} | {i['topic']} | {i['q']}" for i in items)
def pyq_tag(c, qs, items):
    """Attach year + source links to every test question that came from a previous paper."""
    byid = {i["id"]: i for i in items}
    for x in qs if isinstance(qs, list) else []:
        if not isinstance(x, dict): continue
        try: it = byid.get(int(x.get("pyq_id")))
        except (TypeError, ValueError): it = None
        if it: x["pyq"] = True; x["year"] = it.get("year", ""); x["links"] = pyq_links(c, it); x["freq"] = it.get("freq", 1)
    return qs

def verify_group(title, course_name, exam, names, raw, concern, state):
    """Step 2: Groq checks the collected notes, corrects them, adapts them to the student and writes the final handwritten notes."""
    web = raw or "(no professor notes could be fetched from the web this time: write them from your own expert knowledge and say so in 'unsure' where you are not certain)"
    return aj(f"""You are a senior professor checking study notes before they reach a student.
Subject: {title} ({exam}). Student's class/course: {course_name or 'not given'}.
STUDENT'S CONCERN AND STATE: {concern or 'none given'}. {state}
TOPICS TO WRITE (use these exact names): {json.dumps(names)}
NOTES COLLECTED FROM PROFESSORS' WEB PAGES (they can contain errors, typos or off-topic parts):
{web[:9000]}

YOUR JOB
0. COMPARE the numbered sources with each other. Trust what two or more sources agree on. Where sources disagree, decide the correct version with your own expert knowledge and report it in "corrections" (for example: "Source 2 said X, sources 1 and 3 say Y, Y is correct"). For each topic list in "sources" the numbers of the sources you really used.
1. Check every definition, formula, step and example against your own expert knowledge. Fix every mistake. Throw away anything wrong, unclear or not about the topics.
2. If the web notes miss something needed to understand the topic fully and correctly, add it. Never invent facts, numbers, names or links. If you are not sure about a line, leave it out and list it in "unsure".
3. Adapt to this student: beginner-friendly words, and extra care and examples where their concern or weak topics need it.
4. Write the final notes for every topic as neat HUMAN HANDWRITTEN class notes, complete from the basics to exam level.
{NOTE_STYLE}
{STYLE}
{MATH_STYLE}
Return JSON only: {{"topics":[{{"name":"exact topic name","notes":"lines separated by newline characters (14 to 30 lines)","confidence":"high or medium or low","sources":[1,2]}}],"corrections":["short sentence: what was wrong or missing in the web notes and how you fixed it"],"unsure":["point the student should confirm with their teacher"]}}""", 6500, effort="medium")

def make_notes(title, course_name, exam, topics, concern="", state="", found=None):
    """Whole pipeline for a plan: web search (unless `found` is given) -> Groq verification. Returns (notes_json, meta_json, raw_text). Raises if nothing usable came back."""
    ts = sorted([t for t in topics if t.get("name")], key=lambda t: -int(t.get("importance", 3) or 3))[:12]
    names = [str(t["name"]) for t in ts]
    if not names: raise ValueError("no topics")
    sources, raw = found if found is not None else find_notes(title, course_name, exam, names, concern)
    groups = [names[i:i + 6] for i in range(0, len(names), 6)]
    def one(gn):
        try: return verify_group(title, course_name, exam, gn, raw, concern, state)
        except Exception as e: print("verify error", str(e)[:150]); return None
    with ThreadPoolExecutor(2) as ex: outs = list(ex.map(one, groups))
    items = []; corr = []; unsure = []
    for o in outs:
        if not isinstance(o, dict): continue
        for t in (o.get("topics") or []):
            if isinstance(t, dict) and str(t.get("notes", "")).strip() and len(str(t["notes"])) > 40:
                nums = [int(x) for x in (t.get("sources") or []) if str(x).isdigit() and 1 <= int(x) <= len(sources)]
                items.append(dict(name=str(t.get("name") or "").strip()[:120], notes=str(t["notes"]).strip(), confidence=str(t.get("confidence") or "medium").lower()[:8],
                                  links=[{"title": sources[n - 1]["title"], "url": sources[n - 1]["url"]} for n in sorted(set(nums))]))
        corr += [str(x) for x in (o.get("corrections") or []) if str(x).strip()]; unsure += [str(x) for x in (o.get("unsure") or []) if str(x).strip()]
    if not items: raise RuntimeError("AI returned no notes")
    meta = dict(status="verified" if sources else "ai_checked", sources=sources, compared=len(sources), corrections=corr[:12], unsure=unsure[:10], date=time.strftime("%d %b %Y"))
    return json.dumps(items), json.dumps(meta), raw

def notes_list(c):
    try: return [i for i in json.loads(c.get("notes") or "[]") if isinstance(i, dict)]
    except Exception: return []
def notes_meta(c):
    try: return json.loads(c.get("notes_meta") or "{}") or {}
    except Exception: return {}
def notes_for(c, names, limit=4500):
    """Verified notes for the given topic names (exact, contained or close matches)."""
    items = notes_list(c)
    if not items or not names: return ""
    keys = {i["name"].lower(): i for i in items}; out = []
    for n in names:
        key = str(n).lower().strip()
        it = keys.get(key) or next((v for k_, v in keys.items() if k_ and (k_ in key or key in k_)), None)
        if not it:
            m = difflib.get_close_matches(key, list(keys), n=1, cutoff=0.55); it = keys[m[0]] if m else None
        if it and it not in out: out.append(it)
    return "\n\n".join(f"[{i['name']}]\n{i['notes']}" for i in out)[:limit]
def notes_rule(c, names, limit=4500):
    t = notes_for(c, names, limit)
    return ("VERIFIED NOTES (professor notes from the web, checked and corrected; treat them as the correct facts, formulas and definitions and build on them):\n" + t) if t else ""

# ---------------- link every AI answer to what the student is studying right now ----------------
SHORT_RULE = """OVERRIDING RULE (read last, beats every rule above): first look at what the student actually asked. If they want something short (one line, only the name, only the answer, yes/no, one fact), reply with ONLY that, in one single line. In that case write NO definition, NO steps, NO example, NO diagram, NO 'Remember:' line and NO 'Link to your topic:' line. Otherwise answer fully as described above. Never invent websites, URLs, book names, authors or facts. If asked for the source or website, give only what is listed under SOURCES in the study context; if nothing is listed say: No website. These notes were written and checked by AI."""

LINK_RULE = """LINK TO THE STUDENT'S CURRENT TOPIC (very important): use the CURRENT STUDY CONTEXT above. Answer the doubt fully first. Then make it relatable to their own study: show where this doubt fits in the topic they are on now, use an example taken from that current topic, and if it touches one of their weak topics in the test analysis say so and how to improve it. After the 'Remember:' line add one last line that starts with 'Link to your topic:' and ties the answer to their current topic or weak area in one or two sentences. If the doubt is really unrelated to the current topic, answer it completely and say so honestly on that line; never force a false connection and never invent scores."""

def src_footer(u, cid=None):
    """Exact links the answer's study material came from (taken from the saved notes, never written by the AI)."""
    if not u or is_faculty(u): return ""
    cid = cid or u.get("cur_cid")
    c = q("select notes_meta from courses where id=? and user_id=?", (cid, u["id"]), one=True) if cid else None
    if not c: return ""
    srcs = notes_meta(c).get("sources") or []
    if not srcs: return "\n\nSources used: none. These notes were written and checked by AI, no website."
    return "\n\nSources used:\n" + "\n".join(f"{i}. {x.get('title', '')[:80]} - {x.get('url', '')}" for i, x in enumerate(srcs[:5], 1))

def touch(c, n=None):
    """Remember which course/day the student is on, so doubts asked anywhere in the app can be linked to it."""
    u = me()
    if not u or is_faculty(u): return
    day = n if n is not None else (u.get("cur_day") if u.get("cur_cid") == c["id"] else None)
    if u.get("cur_cid") != c["id"] or u.get("cur_day") != day:
        q("update users set cur_cid=?, cur_day=? where id=?", (c["id"], day, u["id"])); g.u = dict(u, cur_cid=c["id"], cur_day=day)

def focus_ctx(u, cid=None, day=None):
    if not u or is_faculty(u): return ""
    cid = cid or u.get("cur_cid")
    if not cid: return ""
    c = q("select * from courses where id=? and user_id=?", (cid, u["id"]), one=True)
    if not c: return ""
    try: plan = json.loads(c["plan"] or "[]")
    except Exception: plan = []
    done = {r["day"] for r in q("select day from progress where course_id=?", (cid,))}
    day = day or (u.get("cur_day") if u.get("cur_cid") == cid else None) or next((int(d["day"]) for d in plan if int(d["day"]) not in done), None)
    d = next((x for x in plan if int(x.get("day", 0)) == day), None) if day else None
    focus = [str(x) for x in (d or {}).get("focus", [])]
    st = stats(cid); weak = [f"{t} ({p}%)" for t, p in st if p < 70][:5]; strong = [f"{t} ({p}%)" for t, p in st[::-1] if p >= 70][:3]
    cls = class_of(u)
    p = [f"CURRENT STUDY CONTEXT: the student is studying '{c['title']}' ({c['exam']})" + (f" in {cls['course']}" if cls else "")
         + (f", now on Day {day} of {len(plan)}, today's topics: {', '.join(focus)}" if focus else "") + f". Days completed: {len(done)} of {len(plan)}."]
    p.append(("Test analysis, topic-wise: weak topics: " + (", ".join(weak) or "none") + ". Strong topics: " + (", ".join(strong) or "none") + ".") if st else "No test taken yet, so there is no test analysis.")
    if (c.get("coach") or "").strip(): p.append("Latest coach analysis of this student: " + c["coach"][:700])
    names = focus or [t.get("name", "") for t in json.loads(c["topics"] or "[]")[:3]]
    nt = notes_rule(c, names, 2500)
    if nt: p.append(nt)
    srcs = notes_meta(c).get("sources") or []
    p.append("SOURCES of the verified notes (the only real ones): " + ("; ".join(f"{x.get('title','')} - {x.get('url','')}" for x in srcs[:5]) if srcs else "none (the notes were written and checked by AI, no website)"))
    return "\n".join(p)

def images_from(files):
    """Handwritten photos -> (list of data-URLs, error). Resized so phone photos stay small."""
    out = []
    for fs in files:
        if not fs or not fs.filename: continue
        if not fs.filename.lower().endswith((".png", ".jpg", ".jpeg", ".webp")):
            return [], "Please upload handwritten work as an image (PNG, JPG or WEBP)."
        raw = fs.read(); mime = "image/jpeg"
        try:
            from PIL import Image, ImageOps
            im = ImageOps.exif_transpose(Image.open(io.BytesIO(raw))).convert("RGB"); im.thumbnail((1800, 1800))
            b = io.BytesIO(); im.save(b, "JPEG", quality=85); raw = b.getvalue()
        except Exception:
            mime = mimetypes.guess_type(fs.filename)[0] or "image/jpeg"
        if len(raw) > 3_500_000: return [], "That photo is too large. Please upload a smaller image."
        out.append(f"data:{mime};base64," + base64.b64encode(raw).decode())
    return out[:3], None

def stats(cid):
    rows = q("select topic,sum(correct) c,sum(total) t from results where course_id=? group by topic", (cid,))
    return sorted([(r["topic"], int(round(100 * float(r["c"]) / float(r["t"])))) for r in rows if r["t"]], key=lambda x: x[1])
def mind(uid, cid=None):
    c = q("select mood from checkins where user_id=? order by id desc limit 1", (uid,), one=True)
    s = "Student's latest mood: %s." % (c["mood"] if c else "unknown")
    if cid:
        w = stats(cid)[:4]
        if w: s += " Weak topics: " + ", ".join(f"{t} ({p}%)" for t, p in w) + "."
    return s

PRICE = int(os.getenv("PRICE", "50"))
OPEN_EP = {"static", "welcome", "demo_done", "adminlogin", "adminlogout", "admin", "admin_act", "admin_user", "admin_settings", "admin_testmail", "admin_testnotes", "admin_testpyq", "admin_purge", "shot", "bot"}
_last_purge = [0.0]

def purge():
    """Keep the database small: doubts live 24h; payment screenshots are deleted 24h after the admin decides (7 days max if never decided)."""
    try:
        q("delete from doubts where created < localtimestamp - interval '24 hours'")
        q("""update payments set img=null where img is not null and
             ((status<>'pending' and decided < localtimestamp - interval '24 hours') or created < localtimestamp - interval '7 days')""")
    except Exception as e: print("purge error", str(e)[:150])

@app.before_request
def gate():
    ep = request.endpoint or ""
    if ep == "static": return
    now = time.time()
    if now - _last_purge[0] > 600: _last_purge[0] = now; purge()
    if ep in OPEN_EP: return
    if not request.cookies.get("gp_demo"): return redirect(url_for("welcome"))   # watch the demo video first
    u = me()
    if u:
        if u.get("suspended"):
            session.clear(); flash("Your account is suspended. Please contact the admin.", "err"); return redirect(url_for("login"))
        if is_faculty(u) and u.get("verified") is False:
            session.clear(); flash("Your faculty account is awaiting admin verification.", "err"); return redirect(url_for("flogin"))
        dv = q("select sess_token from devices where user_id=? and device_id=?", (u["id"], request.cookies.get("gp_dev", "")), one=True)
        if not dv:
            session.clear(); flash("This device is not registered for your account. Please log in again.", "err"); return redirect(url_for("login"))
        if not dv["sess_token"] or session.get("tok") != dv["sess_token"]:
            session.clear(); flash("You were logged out here (you logged out, or changed your password on another device). Please log in again.", "err"); return redirect(url_for("login"))
        if now - session.get("seen", 0) > 240:
            session["seen"] = now; q("update users set last_seen=localtimestamp where id=?", (u["id"],)); q("update devices set last_seen=localtimestamp where user_id=? and device_id=?", (u["id"], request.cookies.get("gp_dev", "")))

@app.route("/welcome")
def welcome():
    return render_template("welcome.html", seen=bool(request.cookies.get("gp_demo")))

@app.route("/demo-done", methods=["POST"])
def demo_done():
    r = jsonify(ok=True); r.set_cookie("gp_demo", "1", max_age=31536000, httponly=True, samesite="Lax", secure=request.is_secure); return r


BOT_SYS = """You are the GrowthPulse assistant inside a study app for students in India. GrowthPulse: upload a syllabus or important questions, pick days, get a day-by-day plan with handwritten-style lessons, ask doubts (type or handwritten photo), take MCQ or descriptive tests, see mind-set analysis and retrain weak topics. Features unlock with a one-time Rs 50 payment approved by the admin. Answer ONLY what the student asked, plain text, no markdown, no tables. If the question is about using the app, answer in a few short lines (max 120 words). If the question is about a STUDY concept, teach it fully: start with a short big-picture of the whole concept from the beginning (what it is, why it matters), then go deep into the exact part asked (step by step, with worked examples and a common mistake), then a short summary. Use simple words, one idea per line, and never stop at a thin answer. Use the page content given to be specific. If you are not sure, say so and suggest the Doubts page. Never invent features, prices or links. If a CURRENT STUDY CONTEXT is given and the question is about a study concept, answer it fully, then end with one line starting 'Link to your topic:' that ties it to what the student is studying now or to a weak topic in their analysis (only if it truly relates; never invent scores)."""
_bot_hits = {}
@app.route("/bot", methods=["POST"])
def bot():
    j = request.get_json(silent=True) or {}
    qn = str(j.get("q", "")).strip()[:500]
    if not qn: return jsonify(answer="Please type a question."), 400
    key = session.get("uid") or request.remote_addr; now = time.time()
    hits = [t for t in _bot_hits.get(key, []) if now - t < 60]
    if len(hits) >= 12: return jsonify(answer="You are asking very fast. Please wait a minute and try again."), 429
    hits.append(now); _bot_hits[key] = hits
    hist = "\n".join(f"{'Student' if h.get('r') == 'user' else 'Assistant'}: {str(h.get('m', ''))[:400]}" for h in (j.get("hist") or [])[-6:] if isinstance(h, dict))
    u = me()
    ok = bool(u) and bool(q("select 1 from devices where user_id=? and device_id=? and sess_token=?", (u["id"], request.cookies.get("gp_dev", ""), session.get("tok", "")), one=True))
    focus = focus_ctx(u) if ok else ""
    prompt = f"""{BOT_SYS}
Student is {'logged in (' + ('fees paid' if u['pay_status'] == 'approved' else 'features locked until payment') + ')' if u else 'not logged in'}.
{focus}
Current page: {str(j.get('title', ''))[:100]} ({str(j.get('path', ''))[:100]})
Page content (may be partial): {str(j.get('page', ''))[:2500]}
Chat so far:
{hist or '(none)'}
Student: {qn}
Assistant:"""
    try: a = ai(prompt, 2800)
    except Exception as e:
        print("bot error", str(e)[:150])
        a = "Sorry, I could not reach the AI just now. Please try again in a moment. For study doubts you can also use the Doubts page."
    return jsonify(answer=a[:7000])

@app.route("/")
def home(): return render_template("home.html")

EMAIL_RE = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]{2,}$")

@app.route("/register", methods=["GET", "POST"])
def register():
    """Student sign-up: name, email, contact, class code, password + confirm. The class is fixed for good once registered."""
    f = request.form
    if request.method == "POST":
        name = f.get("name", "").strip()[:80]; email = f.get("email", "").strip().lower(); phone = re.sub(r"[\s\-()+]", "", f.get("phone", ""))
        code = re.sub(r"\s", "", f.get("class_code", "")).upper(); pw = f.get("password", ""); pw2 = f.get("confirm", "")
        cls = q("select * from classes where code=?", (code,), one=True) if code else None
        err = ("Please fill in every field." if not (name and email and phone and code and pw) else
               "Please enter a valid email." if not EMAIL_RE.match(email) else
               "Please enter a valid contact number (digits only)." if not re.fullmatch(r"\d{7,15}", phone) else
               "That class code was not found. Ask your faculty for the correct class code." if not cls else
               "Password must be at least 6 characters." if len(pw) < 6 else
               "The two passwords do not match." if pw != pw2 else None)
        if not err:
            try:
                uid = q("insert into users(name,email,phone,pw,role,class_id) values(?,?,?,?,'student',?)", (name, email, phone, make_pw(pw), cls["id"]), rid=True)
            except psycopg2.IntegrityError:
                flash("This email is already registered. Please log in.", "err"); return redirect(url_for("login"))
            resp = login_user(q("select * from users where id=?", (uid,), one=True), url_for("dashboard"))
            flash(f"Account created. You are now in {cls['course']}. Welcome aboard!", "ok")
            return resp or redirect(url_for("login"))
        flash(err, "err")
    return render_template("auth.html", mode="register", f=f, code=request.args.get("code", ""))

def do_login(kind):
    mode = "flogin" if kind == "faculty" else "login"
    if request.method == "POST":
        ident = request.form.get("email", "").strip(); pw = request.form.get("password", "")
        key = client_ip() + "|" + ident.lower()
        if throttled(key):
            flash("Too many wrong attempts. Please wait 10 minutes and try again.", "err"); return render_template("auth.html", mode=mode)
        if "@" in ident: row = q("select * from users where email=?", (ident.lower(),), one=True); cands = [row] if row else []
        elif kind == "faculty" and ident: cands = q("select * from users where role='faculty' and lower(name)=?", (ident.lower(),))
        else: cands = []
        u = next((x for x in cands if chk_pw(x["pw"], pw)), None)
        if not u:
            note_fail(key); flash("Wrong " + ("name/email" if kind == "faculty" else "email") + " or password.", "err"); return render_template("auth.html", mode=mode)
        if u.get("suspended"):
            flash("Your account is suspended. Please contact the admin.", "err"); return render_template("auth.html", mode=mode)
        if is_faculty(u) and u.get("verified") is False:
            flash("Your faculty registration is awaiting verification by the admin. You can log in as soon as it is approved.", "err"); return render_template("auth.html", mode=mode)
        if kind == "faculty" and not is_faculty(u):
            flash("That is a student account. Please use Student login.", "err"); return redirect(url_for("login"))
        if kind == "student" and is_faculty(u):
            flash("That is a faculty account. Please use Faculty login.", "err"); return redirect(url_for("flogin"))
        resp = login_user(u, home_for(u))
        if not resp:
            flash(LIMIT_MSG, "err"); return render_template("auth.html", mode=mode)
        _fails.pop(key, None); flash("Logged in successfully.", "ok")
        return resp
    return render_template("auth.html", mode=mode)

@app.route("/login", methods=["GET", "POST"])
def login(): return do_login("student")

@app.route("/faculty/login", methods=["GET", "POST"])
def flogin(): return do_login("faculty")

@app.route("/faculty/register", methods=["GET", "POST"])
def fregister():
    """Faculty give name, email, contact and a password. The account stays 'awaiting verification' until the admin approves it."""
    f = request.form
    if request.method == "POST":
        name = f.get("name", "").strip()[:80]; email = f.get("email", "").strip().lower(); phone = re.sub(r"[\s\-()+]", "", f.get("phone", "")); pw = f.get("password", ""); pw2 = f.get("confirm", "")
        key = "freg|" + client_ip()
        err = ("Please fill in every field." if not (name and email and phone and pw) else
               "Please enter a valid email." if not EMAIL_RE.match(email) else
               "Please enter a valid contact number (digits only)." if not re.fullmatch(r"\d{7,15}", phone) else
               "Password must be at least 6 characters." if len(pw) < 6 else
               "The two passwords do not match." if pw != pw2 else
               "Too many sign-ups from this network. Please try again later." if throttled(key, 5, 3600) else None)
        if not err:
            note_fail(key)
            try:
                q("insert into users(name,email,phone,pw,role,pay_status,access,verified) values(?,?,?,?,'faculty','approved','staff',false)", (name, email, phone, make_pw(pw)))
            except psycopg2.IntegrityError:
                flash("This email is already registered. Please use Faculty login (if it still says awaiting verification, ask the admin).", "err"); return redirect(url_for("flogin"))
            return render_template("fpending.html", name=name, email=email)
        flash(err, "err")
    return render_template("auth.html", mode="fregister", f=f)

@app.route("/forgot", methods=["GET", "POST"])
def forgot():
    """Emails a fresh password to the registered email (nothing is shown on screen)."""
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower(); key = "forgot|" + client_ip()
        if throttled(key, 5, 3600): flash("Too many requests. Please try again later.", "err"); return render_template("auth.html", mode="forgot")
        note_fail(key)
        u = q("select * from users where email=?", (email,), one=True) if EMAIL_RE.match(email) else None
        if u and not u.get("suspended"):
            pw = temp_password()
            try:
                send_mail(email, "Your new GrowthPulse password", f"Hello {u['name']},\n\nYour new GrowthPulse password is: {pw}\n\nLog in with it and change it in your profile/dashboard. If you did not ask for this, please tell the admin.\n\n- GrowthPulse")
                q("update users set pw=? where id=?", (make_pw(pw), u["id"])); q("update devices set sess_token='' where user_id=?", (u["id"],))
            except Exception as e: print("mail error", str(e)[:200])
        flash("If that email is registered, a new password has been sent to it.", "ok"); return redirect(url_for("login"))
    return render_template("auth.html", mode="forgot")

@app.route("/account/password", methods=["POST"])
def change_password():
    u = me()
    if not u: return redirect(url_for("login"))
    f = request.form; back = url_for("faculty_home") + "#password" if is_faculty(u) else url_for("profile") + "#password"; key = "pw|%s" % u["id"]
    if throttled(key): flash("Too many wrong attempts. Please wait 10 minutes.", "err"); return redirect(back)
    if not chk_pw(u["pw"], f.get("current", "")): note_fail(key); flash("Your current password is wrong.", "err"); return redirect(back)
    new = f.get("new", "")
    if len(new) < 6: flash("New password must be at least 6 characters.", "err"); return redirect(back)
    if new != f.get("confirm", ""): flash("The two new passwords do not match.", "err"); return redirect(back)
    q("update users set pw=? where id=?", (make_pw(new), u["id"]))
    q("update devices set sess_token='' where user_id=? and device_id<>?", (u["id"], request.cookies.get("gp_dev", "")))   # other devices must log in again
    flash("Password changed. Other devices were logged out.", "ok"); return redirect(back)

@app.route("/device/<int:did>/remove", methods=["POST"])
def device_remove(did):
    u = me()
    if not u: return redirect(url_for("login"))
    row = q("select device_id from devices where id=? and user_id=?", (did, u["id"]), one=True)
    if row and row["device_id"] == request.cookies.get("gp_dev"): flash("That is the device you are using now. Use Logout instead.", "err")
    elif row: q("delete from devices where id=?", (did,)); flash("Device removed. You can now log in on a new device.", "ok")
    return redirect(url_for("faculty_home") + "#devices" if is_faculty(u) else url_for("profile") + "#devices")

@app.route("/join-class", methods=["POST"])
def join_class():
    """For students who registered before classes existed: choose the class ONCE. It can never be changed afterwards."""
    u = me()
    if not u or is_faculty(u): return redirect(url_for("login"))
    if u.get("class_id"): flash("Your class is already set and cannot be changed.", "err"); return redirect(url_for("profile"))
    cls = q("select * from classes where code=?", (re.sub(r"\s", "", request.form.get("class_code", "")).upper(),), one=True)
    if not cls: flash("That class code was not found. Ask your faculty.", "err"); return redirect(url_for("profile"))
    q("update users set class_id=? where id=? and class_id is null", (cls["id"], u["id"])); flash(f"You joined {cls['course']}. This cannot be changed later.", "ok")
    return redirect(url_for("dashboard"))

@app.route("/logout")
def logout():
    if session.get("uid") and session.get("tok"):
        q("update devices set sess_token='' where user_id=? and device_id=? and sess_token=?", (session["uid"], request.cookies.get("gp_dev", ""), session["tok"]))
    to = url_for("flogin") if session.get("uid") and (me() or {}).get("role") == "faculty" else url_for("home")
    session.clear(); flash("You have been logged out.", "ok"); return redirect(to)

# ---------------- faculty: courses, subjects, class codes ----------------
def class_blocks(f):
    out = []
    for i in sorted({int(m.group(1)) for k in f.keys() for m in [re.fullmatch(r"course_(\d+)", k)] if m}):
        course = f.get(f"course_{i}", "").strip()[:100]; subs = []
        for sname in f.getlist(f"subjects_{i}"):
            sname = sname.strip()[:100]
            if sname and sname.lower() not in [x.lower() for x in subs]: subs.append(sname)
        if course or subs: out.append(dict(course=course, subjects=subs[:30]))
    return out

@app.route("/faculty")
@faculty_only
def faculty_home():
    u = me()
    cls = q("select c.*, (select count(*) from users s where s.class_id=c.id and s.role='student') as n from classes c where c.faculty_id=? order by c.id", (u["id"],))
    if not cls:
        flash("Welcome! First add the course you teach and your subjects. You will get a class code for your students.", "ok"); return redirect(url_for("faculty_class_new"))
    studs = q("select class_id,name,email,phone,last_seen from users where role='student' and class_id in (select id from classes where faculty_id=?) order by name", (u["id"],))
    devs = q("select id,label,last_seen,device_id from devices where user_id=? order by id", (u["id"],))
    return render_template("faculty.html", classes=[dict(c, subs=subjects_of(c), students=[s for s in studs if s["class_id"] == c["id"]]) for c in cls],
                           devs=devs, cur=request.cookies.get("gp_dev"), maxdev=MAX_DEVICES)

@app.route("/faculty/class/new", methods=["GET", "POST"])
@faculty_only
def faculty_class_new():
    u = me(); blocks = [dict(course="", subjects=[""])]
    if request.method == "POST":
        blocks = class_blocks(request.form); bad = next((b for b in blocks if not b["course"] or not b["subjects"]), None)
        if not blocks or bad: flash("For every course, type the course name and at least one subject.", "err")
        else:
            codes = []
            for b in blocks:
                code = new_class_code(); q("insert into classes(faculty_id,course,subjects,code) values(?,?,?,?)", (u["id"], b["course"], json.dumps(b["subjects"]), code)); codes.append(f"{b['course']}: {code}")
            flash("Class code created → " + "; ".join(codes) + ". Share it with your students.", "ok"); return redirect(url_for("faculty_home"))
    return render_template("faculty_class.html", blocks=blocks, edit=None)

@app.route("/faculty/class/<int:cid>", methods=["GET", "POST"])
@faculty_only
def faculty_class_edit(cid):
    u = me(); c = q("select * from classes where id=? and faculty_id=?", (cid, u["id"]), one=True)
    if not c: abort(404)
    blocks = [dict(course=c["course"], subjects=subjects_of(c) or [""])]
    if request.method == "POST":
        blocks = class_blocks(request.form)[:1]
        if not blocks or not blocks[0]["course"] or not blocks[0]["subjects"]: flash("Type the course name and at least one subject.", "err")
        else:
            q("update classes set course=?, subjects=? where id=?", (blocks[0]["course"], json.dumps(blocks[0]["subjects"]), cid)); flash("Class updated. The class code stays the same.", "ok")
            return redirect(url_for("faculty_home"))
    return render_template("faculty_class.html", blocks=blocks, edit=c)

@app.route("/dashboard")
def dashboard():
    u = me()
    if not u: return redirect(url_for("login"))
    if is_faculty(u): return redirect(url_for("faculty_home"))
    cs = q("select * from courses where user_id=? order by id desc", (u["id"],))
    cls = class_of(u)
    return render_template("dashboard.html", cs=cs, subs=subjects_of(cls) if cls else [])

@app.route("/checkin", methods=["POST"])
@paid
def checkin():
    q("insert into checkins(user_id,mood) values(?,?)", (session["uid"], request.form["mood"]))
    flash("Mind-set check-in saved. Your plans will adapt to it.", "ok"); return redirect(url_for("dashboard"))

def shot_bytes(fs):
    """Payment screenshot -> compressed JPEG bytes (kept inside Supabase, so nothing is lost on Render restarts)."""
    raw = fs.read()
    try:
        from PIL import Image, ImageOps
        im = ImageOps.exif_transpose(Image.open(io.BytesIO(raw))).convert("RGB"); im.thumbnail((1600, 1600))
        b = io.BytesIO(); im.save(b, "JPEG", quality=80); return b.getvalue(), "image/jpeg"
    except Exception:
        return raw, (mimetypes.guess_type(fs.filename)[0] or "image/jpeg")

def pay_cfg():
    """Payment details the admin typed in (QR image, PhonePe/UPI number, account name)."""
    try:
        rows = q("select key,val,(img is not null) as has_img from settings")
        d = {r["key"]: r["val"] or "" for r in rows}
        return {"name": d.get("pay_name", ""), "phone": d.get("pay_phone", ""), "upi": d.get("pay_upi", ""),
                "has_qr": any(r["key"] == "qr" and r["has_img"] for r in rows)}
    except Exception: return {"name": "", "phone": "", "upi": "", "has_qr": False}

@app.route("/pay-qr")
def pay_qr():
    r = q("select img,mime from settings where key='qr'", one=True)
    if not r or r["img"] is None: abort(404)
    return Response(bytes(r["img"]), mimetype=r["mime"] or "image/png", headers={"Cache-Control": "no-cache"})

@app.route("/pay", methods=["GET", "POST"])
def pay():
    u = me()
    if not u: return redirect(url_for("login"))
    if is_faculty(u): return redirect(url_for("faculty_home"))
    if request.method == "POST":
        fs = request.files.get("shot")
        if not fs or not fs.filename.lower().endswith((".png", ".jpg", ".jpeg", ".webp")):
            flash("Please upload a payment screenshot (PNG/JPG).", "err"); return redirect(url_for("pay"))
        data, mime = shot_bytes(fs)
        q("insert into payments(user_id,img,mime) values(?,?,?)", (u["id"], psycopg2.Binary(data), mime))
        q("update users set pay_status='pending' where id=?", (u["id"],))
        flash("Screenshot received! Admin will verify and unlock your account soon.", "ok"); return redirect(url_for("pay"))
    return render_template("pay.html", cfg=pay_cfg())

@app.route("/profile", methods=["GET", "POST"])
def profile():
    u = me()
    if not u: return redirect(url_for("login"))
    if is_faculty(u): return redirect(url_for("faculty_home"))
    if request.method == "POST":
        f = request.form      # the class is deliberately NOT editable here: a student stays in the class they registered with
        q("update users set name=?,phone=?,profession=?,goal=? where id=?", (f["name"], f["phone"], f["profession"], f["goal"], u["id"]))
        flash("Profile saved.", "ok"); return redirect(url_for("dashboard"))
    devs = q("select id,label,last_seen,device_id from devices where user_id=? order by id", (u["id"],))
    return render_template("profile.html", devs=devs, cur=request.cookies.get("gp_dev"), maxdev=MAX_DEVICES)

def build_notes(track, title, course_name, exam, topics, concern, state, found=None):
    """Professor notes -> Groq check -> handwritten notes. A problem here never stops the plan from being built."""
    if track != "college": return "", "", ""
    try: return make_notes(title, course_name, exam, topics, concern, state, found)
    except Exception as e:
        print("notes error", str(e)[:200]); return "", "", ""

class BuildError(Exception): pass

def run_build(p, step):
    """The slow part of 'Build my plan' (web search + several AI calls). Runs in a background thread, never inside a web request."""
    uid, track, qmode, exam, days, src, papers, title, concern, course_name = (p[k] for k in ("uid", "track", "qmode", "exam", "days", "src", "papers", "title", "concern", "course_name"))
    state = mind(uid)
    if qmode == "questions":
        step("📄 Reading your important questions…")
        try:
            items = aj(f"""Below is text from an internal-exam IMPORTANT QUESTIONS list for the subject '{title}' (it may be messy text copied from a PDF). Extract every distinct question, fix obvious typos, keep at most 60. Give each a short topic name (2 to 4 words).
Text: {src[:12000]}
Return JSON list: [{{"q":"the question","topic":"topic name"}}]""", 5000)
            if isinstance(items, dict): items = next((v for v in items.values() if isinstance(v, list)), [])
            items = [{"n": i + 1, "q": str(x.get("q", "")).strip(), "topic": (str(x.get("topic") or "General").strip()[:60] or "General")}
                     for i, x in enumerate(items[:60]) if isinstance(x, dict) and str(x.get("q", "")).strip()]
            if not items: raise ValueError("no questions found in the file")
        except Exception as e:
            raise BuildError(f"Could not read the questions ({str(e)[:150]}). Try a clearer PDF or type them one per line.")
        plan = qplan(items, days); cnt = {}
        for x in items: cnt[x["topic"]] = cnt.get(x["topic"], 0) + 1
        topics = [{"name": t, "importance": min(5, 2 + k), "summary": f"{k} important question(s)"} for t, k in cnt.items()]
        step("🔎 Finding professors' notes and previous-year papers online, then ✅ AI is verifying them…")
        with ThreadPoolExecutor(2) as ex:
            f_n = ex.submit(build_notes, track, title, course_name, exam, topics, concern, state); f_p = ex.submit(build_pyq, track, title, course_name, exam, topics, papers)
            nb, nm, nraw = f_n.result(); pq, pm = f_p.result()
        cid = q("insert into courses(user_id,track,title,exam,days,plan,cards,topics,src,research,qmode,notes,notes_meta,notes_src,concern,pyq,pyq_meta) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (uid, track, title, exam, len(plan), json.dumps(plan), "[]", json.dumps(topics), src[:12000], "", "questions", nb, nm, nraw[:12000], concern, pq, pm), rid=True)
        for x in items: q("insert into iq(course_id,n,topic,q) values(?,?,?,?)", (cid, x["n"], x["topic"], x["q"]))
        msg = f"{len(items)} important questions found and planned over {len(plan)} days. Open Day 1 to learn the answers."
    else:
        scope = ("Cover EVERY topic of the syllabus across the days, keeping the last day(s) for revision." if exam == "Semester Exam"
                 else "Use ONLY the topics the student listed." if track == "college" else "Build a skill roadmap from beginner to job-ready.")
        hint = [re.sub(r"\s+", ", ", src.strip()[:700])]
        step("🔎 Step 1: finding professors' notes and previous-year papers on the web…")
        with ThreadPoolExecutor(3) as ex:
            f_brief = ex.submit(research, title, exam, src)
            f_notes = ex.submit(find_notes, title, course_name, exam, hint, concern) if track == "college" else None
            f_pyq = ex.submit(find_pyq, title, course_name, exam) if track == "college" else None
            brief = f_brief.result(); found = f_notes.result() if f_notes else None; found_pyq = f_pyq.result() if f_pyq else None
        profn = "; ".join(s_["title"] + " (" + (s_["by"] or s_["trust"]) + ")" for s_ in (found[0] if found else []))
        step("🧠 Building your day-wise plan…")
        try:
            d = aj(f"""Create a study programme. Exam: {exam}. Subject/goal: {title}. Days available: {days}. {scope}
{state}
The student's own concern: {concern or 'none given'}
Topics that repeat in previous papers (give higher importance): {papers[:5000] or 'none given'}
Real-world research from the web (use it to decide which topics are really important): {brief[:3500] or 'not available'}
Professors' notes found online (what they cover is what the exam usually needs): {profn or 'none found'}
Content: {src[:12000]}
Return JSON: {{"topics":[{{"name":"","importance":1-5,"summary":""}}],"plan":[{{"day":1,"focus":["topic"],"tip":""}}] (exactly {days} days)}}""", 6000)
            d["topics"]; d["plan"]
        except Exception as e:
            raise BuildError(f"AI could not build the plan ({str(e)[:150]}). Please try again in a minute.")
        step("✅ Step 2: AI is checking and correcting the notes, then writing them by hand…")
        with ThreadPoolExecutor(2) as ex:
            f_n = ex.submit(build_notes, track, title, course_name, exam, d["topics"], concern, state, found); f_p = ex.submit(build_pyq, track, title, course_name, exam, d["topics"], papers, found_pyq)
            nb, nm, nraw = f_n.result(); pq, pm = f_p.result()
        cid = q("insert into courses(user_id,track,title,exam,days,plan,cards,topics,src,research,notes,notes_meta,notes_src,concern,pyq,pyq_meta) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (uid, track, title, exam, days, json.dumps(d["plan"]), "[]", json.dumps(d["topics"]), src[:12000], brief, nb, nm, nraw[:12000], concern, pq, pm), rid=True)
        msg = "Your day-wise plan is ready! Open Day 1 to start learning."
    warn = ""
    if track == "college":
        if nb: msg += " 📚 Professor notes checked and verified by AI."
        else: warn = "Plan is ready, but the professor notes could not be verified this time. Tap 'Fetch & verify notes' on the plan page."
        npq = len(json.loads(pq)) if pq else 0
        if npq: msg += f" 📜 {npq} previous-year questions collected from {len((json.loads(pm) or {}).get('sources', []))} websites."
    return cid, msg, warn

BUILDER = ThreadPoolExecutor(int(os.getenv("BUILD_WORKERS", "3")))     # plans being built at the same time; the rest wait in line

def job_runner(jid, p):
    def step(t):
        try: q("update jobs set step=? where id=?", (t, jid))
        except Exception: pass
    try:
        q("update jobs set status='running', step='Starting…' where id=?", (jid,))
        cid, msg, warn = run_build(p, step)
        q("update jobs set status='done', course_id=?, msg=?, warn=?, step='Done' where id=?", (cid, msg, warn, jid))
        q("update users set cur_cid=?, cur_day=null where id=?", (cid, p["uid"]))      # doubts asked right after building already know this course
    except BuildError as e:
        q("update jobs set status='error', msg=? where id=?", (str(e)[:400], jid))
    except Exception as e:
        print("build crashed", repr(e)[:300])
        try: q("update jobs set status='error', msg=? where id=?", ("Something went wrong while building your plan. Please try again.", jid))
        except Exception: pass

@app.route("/new/<track>", methods=["GET", "POST"])
@paid
def new(track):
    if track not in ("college", "personal"): abort(404)
    u = me()
    if track == "personal" and not (u["profession"] and u["goal"]):
        flash("Please complete your profile first so AI can train you accordingly.", "err"); return redirect(url_for("profile"))
    cls = class_of(u); course_name = cls["course"] if cls else ""; subs = subjects_of(cls) if cls else []
    if request.method == "POST":
        f = request.form
        act = q("select id from jobs where user_id=? and kind='build' and status in ('queued','running') and created > localtimestamp - interval '20 minutes' order by id desc limit 1", (u["id"],), one=True)
        if act: flash("Your plan is already being built. Showing its progress.", "ok"); return redirect(url_for("build_wait", jid=act["id"]))
        qmode = "questions" if (track == "college" and f.get("qmode") == "questions") else "syllabus"
        exam = f.get("exam", "Semester Exam") if track == "college" else "Personal Growth"
        days = max(1, min(int(f.get("days") or 7), 120))
        src = "\n".join(text_from(x) for x in request.files.getlist("files")) + "\n" + f.get("topics", "")
        papers = "\n".join(text_from(x) for x in request.files.getlist("papers"))
        title = f.get("title") or u["goal"]
        if track == "personal": src = f"Profession: {u['profession']}. Wants to become: {u['goal']}. Notes: " + src
        if len(src.strip()) < 15:
            flash("Please upload a file or type your " + ("important questions." if qmode == "questions" else "syllabus topics."), "err"); return redirect(request.url)
        params = dict(uid=u["id"], track=track, qmode=qmode, exam=exam, days=days, src=src, papers=papers, title=title, concern=f.get("concern", "").strip()[:300], course_name=course_name)
        jid = q("insert into jobs(user_id,kind,track,status,step,first) values(?,'build',?,'queued','Waiting in line…',?)", (u["id"], track, f.get("firstq", "").strip()[:300]), rid=True)
        BUILDER.submit(job_runner, jid, params)
        return redirect(url_for("build_wait", jid=jid))
    return render_template("new.html", track=track, subs=subs, course_name=course_name, pre=request.args.get("subject", ""))

@app.route("/build/<int:jid>")
@paid
def build_wait(jid):
    u = me(); j = q("select *, (created < localtimestamp - interval '20 minutes') as stale from jobs where id=? and user_id=?", (jid, u["id"]), one=True)
    if not j: abort(404)
    if j["status"] in ("queued", "running") and j["stale"]:
        q("update jobs set status='error', msg='Building took too long. Please try again.' where id=?", (jid,)); j = dict(j, status="error", msg="Building took too long. Please try again.")
    if j["status"] == "done":
        flash(j["msg"], "ok")
        if j["warn"]: flash(j["warn"], "err")
        if j["first"]: return redirect(url_for("explain", cid=j["course_id"], t=j["first"]))
        return redirect(url_for("course", cid=j["course_id"]))
    if j["status"] == "error":
        flash(j["msg"] or "Could not build the plan. Please try again.", "err"); return redirect(url_for("new", track=j["track"]))
    return render_template("build.html", j=j)

@app.route("/build/<int:jid>/status")
@paid
def build_status(jid):
    j = q("select id,status,step from jobs where id=? and user_id=?", (jid, me()["id"]), one=True)
    if not j: abort(404)
    ahead = q("select count(*) n from jobs where status='queued' and id<?", (jid,), one=True)["n"] if j["status"] == "queued" else 0
    return jsonify(status=j["status"], step=j["step"], ahead=ahead)

def get_course(cid):
    c = q("select * from courses where id=? and user_id=?", (cid, session.get("uid")), one=True)
    if not c: abort(404)
    return c

@app.route("/course/<int:cid>")
@paid
def course(cid):
    c = get_course(cid); plan = json.loads(c["plan"])
    done = {r["day"] for r in q("select day from progress where course_id=?", (cid,))}
    opened = {r["day"] for r in q("select day from lessons where course_id=?", (cid,))}
    nxt = next((int(d["day"]) for d in plan if int(d["day"]) not in done), None)
    qmode = c.get("qmode") or "syllabus"
    qs = q("select n,topic,q from iq where course_id=? order by n", (cid,)) if qmode == "questions" else []
    qday = {int(n): int(d["day"]) for d in plan for n in d.get("qs", [])}
    touch(c, nxt)
    return render_template("course.html", c=c, plan=plan, topics=json.loads(c["topics"]), st=stats(cid), done=done, opened=opened, nxt=nxt, qmode=qmode, qs=qs, qday=qday,
                           nmeta=notes_meta(c), ncount=len(notes_list(c)), pcount=len(pyq_list(c)), pmeta=pyq_meta(c))

@app.route("/course/<int:cid>/notes")
@paid
def notes_page(cid):
    c = get_course(cid); touch(c)
    return render_template("notes.html", c=c, items=notes_list(c), meta=notes_meta(c))

@app.route("/course/<int:cid>/notes/refresh", methods=["POST"])
@paid
def notes_refresh(cid):
    """Search the professors' notes again and re-verify them. Lessons are rewritten from the verified notes."""
    c = get_course(cid); u = me(); cls = class_of(u)
    try:
        nb, nm, nraw = make_notes(c["title"], cls["course"] if cls else "", c["exam"], json.loads(c["topics"] or "[]"), c.get("concern") or "", mind(u["id"], cid))
    except Exception as e:
        flash(f"Could not fetch and verify the notes right now ({str(e)[:120]}). Please try again.", "err"); return redirect(url_for("course", cid=cid))
    q("update courses set notes=?, notes_meta=?, notes_src=? where id=?", (nb, nm, nraw[:12000], cid))
    q("delete from lessons where course_id=?", (cid,)); q("update iq set ans='' where course_id=?", (cid,)); q("delete from topicx where course_id=?", (cid,))
    flash("Professor notes fetched and verified. Your lessons will now be written from them.", "ok"); return redirect(url_for("notes_page", cid=cid))

def qlesson(c, d, n):
    nums = [int(x) for x in d.get("qs", [])]
    rows = [dict(r) for r in q("select n,topic,q,ans from iq where course_id=? and n = any(?) order by n", (c["id"], nums))]
    miss = [r for r in rows if not (r["ans"] or "").strip()]
    if miss:
        payload = json.dumps([{"n": r["n"], "q": r["q"], "topic": r["topic"]} for r in miss])
        res = aj(f"""Subject: '{c['title']}' ({c['exam']}). These are IMPORTANT exam questions. Write the full model answer for each one so a student can learn it and write it in the exam.
Questions: {payload}
{notes_rule(c, [r["topic"] for r in miss], 4500)}
Answer length should suit the marks: a short definition question gets 8 to 12 lines, an explain/discuss question gets 20 to 30 lines, a numerical gets the full worked solution. For every answer first give a 2-line big picture of the whole concept the question belongs to, then answer the question in full depth with an example.
{NOTE_STYLE}
{STYLE}
{MATH_STYLE}
Return JSON list: [{{"n":1,"answer":"the handwritten-style notes, lines separated by newline characters"}}]""", 7000)
        if isinstance(res, dict): res = next((v for v in res.values() if isinstance(v, list)), [])
        got = {int(x["n"]): str(x["answer"]) for x in res if isinstance(x, dict) and x.get("n") is not None and x.get("answer")}
        for r in miss:
            if r["n"] in got:
                q("update iq set ans=? where course_id=? and n=?", (got[r["n"]], c["id"], r["n"])); r["ans"] = got[r["n"]]
    if any(not (r["ans"] or "").strip() for r in rows): raise RuntimeError("AI returned incomplete answers")
    return {"title": f"Important questions · Day {n}", "intro": "These questions are likely to come in your exam. Read each model answer, understand the idea, then close the book and write it in your own words. Tap Ask AI under any answer if something is not clear.", "questions": rows}

def lesson_for(c, d, n):
    if (c.get("qmode") or "syllabus") == "questions": return qlesson(c, d, n)
    row = q("select body from lessons where course_id=? and day=?", (c["id"], n), one=True)
    if row: return json.loads(row["body"])
    topics = json.loads(c["topics"]); focus = ", ".join(d.get("focus", []))
    L = aj(f"""Teach Day {n} of a {c['days']}-day programme: '{c['title']}' ({c['exam']}). Today's focus: {focus}. Study tip: {d.get('tip', '')}.
Topic notes: {json.dumps(topics)[:3000]}
{notes_rule(c, d.get('focus', []), 5000)}
The student's own concern: {c.get('concern') or 'none given'}
Source material from the student: {(c['src'] or '')[:6000] or 'none'}
Real-world facts found on the web about this subject (use real uses and examples from here where helpful): {(c['research'] or '')[:2500] or 'none'}
{mind(c['user_id'], c['id'])}

The student may be a COMPLETE BEGINNER who knows nothing about this topic and is studying alone. Teach from zero, like a patient teacher sitting next to them:
- Start with what the topic is in one simple line, why it matters, and where it is used in real life.
- Explain every new word in everyday language the first time it appears. No jargon without its meaning.
- Go from easy to hard, ONE idea at a time, in small steps.
- For every idea give a simple everyday ANALOGY an Indian student relates to (chai, cricket, bus, mobile recharge, kirana shop, cooking, etc).
- For every section give one worked EXAMPLE with small, friendly numbers or a real situation. For maths or numbers set "math":true and write the example in notebook style.
- Tell one COMMON MISTAKE beginners make in that section.
{DEPTH_STYLE}
Cover the WHOLE of today's focus from the beginning to depth: every sub-topic, rule, formula, type and step, with at least two examples per section. Do not leave any part of the focus half-explained.
{STYLE}
{NOTE_STYLE}
{MATH_STYLE}

Return JSON: {{"title":"","math":true or false (true if today's topic needs calculations),"intro":"3-5 simple sentences: what we learn today and why it matters","words":[{{"term":"","meaning":"one simple line"}}] (3 to 6 key words),"sections":[{{"h":"","body":"explanation written as handwritten class notes: one short idea per line, lines separated by newline characters","analogy":"everyday comparison","example":"worked example, one step per line","math":true or false,"mistake":"common beginner mistake and how to avoid it"}}] (4 to 6 sections),"recap":["",""] (4 to 6 simple points),"practice":[{{"q":"a question to try","answer":"full solution written step by step"}}] (3 questions, easy to medium)}}""", 9000)
    q("insert into lessons(course_id,day,body) values(?,?,?) on conflict(course_id,day) do update set body=excluded.body", (c["id"], n, json.dumps(L)))
    return L

def day_plan(c, n):
    d = next((x for x in json.loads(c["plan"]) if int(x.get("day", 0)) == n), None)
    if not d: abort(404)
    return d

@app.route("/course/<int:cid>/day/<int:n>")
@paid
def day(cid, n):
    c = get_course(cid); d = day_plan(c, n)
    try: L = lesson_for(c, d, n)
    except Exception as e:
        flash(f"Could not prepare this lesson ({str(e)[:120]}). Please try again.", "err"); return redirect(url_for("course", cid=cid))
    touch(c, n)
    chats = q("select role,msg from chats where course_id=? and day=? order by id", (cid, n))
    done = bool(q("select 1 from progress where course_id=? and day=?", (cid, n), one=True))
    return render_template("day.html", c=c, d=d, n=n, L=L, chats=chats, done=done, last=len(json.loads(c["plan"])))

@app.route("/course/<int:cid>/day/<int:n>/refresh", methods=["POST"])
@paid
def day_refresh(cid, n):
    c = get_course(cid); d = day_plan(c, n)
    if (c.get("qmode") or "syllabus") == "questions":
        q("update iq set ans='' where course_id=? and n = any(?)", (cid, [int(x) for x in d.get("qs", [])]))
    q("delete from lessons where course_id=? and day=?", (cid, n))
    flash("Writing a fresh, simpler explanation for you…", "ok"); return redirect(url_for("day", cid=cid, n=n))

@app.route("/course/<int:cid>/day/<int:n>/ask", methods=["POST"])
@paid
def ask(cid, n):
    c = get_course(cid); d = day_plan(c, n)
    question = request.form.get("question", "").strip(); ctx = request.form.get("context", "").strip()[:1500]
    back = url_for("day", cid=cid, n=n) + "#chat"
    imgs, err = images_from(request.files.getlist("note"))
    if err: flash(err, "err"); return redirect(back)
    if not question and not imgs:
        flash("Type your doubt or upload a photo of your handwritten note.", "err"); return redirect(back)
    L = lesson_for(c, d, n)
    hist = q("select role,msg from chats where course_id=? and day=? order by id desc limit 6", (cid, n))[::-1]
    convo = "\n".join(f"{'Student' if h['role'] == 'user' else 'Tutor'}: {h['msg']}" for h in hist)
    prompt = f"""You are tutoring a student on Day {n} of '{c['title']}'. Today's lesson:
{json.dumps(L)[:5000]}
Earlier chat:
{convo or '(none)'}
{focus_ctx(me(), cid, n)}
{mind(c['user_id'], cid)}
{('The student is asking about this exact part: ' + ctx) if ctx else ''}
Student's doubt: {question or 'Please read my handwritten note and clarify my doubt.'}
{'The student attached a photo of their handwritten note. Read it carefully, say what you understood, and if a word or line is unclear, ask them to write it more clearly.' if imgs else ''}
Explain like a patient teacher to a beginner: simple words, everyday examples, tied to today's topic.
{DEPTH_STYLE}
{NOTE_STYLE}
{STYLE}
{MATH_STYLE}
{LINK_RULE}
{SHORT_RULE}"""
    try: a = ai(prompt, 5500, imgs or None) + src_footer(me(), cid)
    except Exception as e:
        flash(f"AI error ({str(e)[:120]}).", "err"); return redirect(back)
    tag = ("📌 " + ctx[:70].replace("\n", " ") + "…\n") if ctx else ""
    q("insert into chats(course_id,day,role,msg) values(?,?,?,?)", (cid, n, "user", tag + (question or "Please check my handwritten note.") + (" 📎 [handwritten note attached]" if imgs else "")))
    q("insert into chats(course_id,day,role,msg) values(?,?,?,?)", (cid, n, "ai", a))
    flash("Answer added below in the Doubts section.", "ok")
    return redirect(back)

@app.route("/course/<int:cid>/day/<int:n>/done", methods=["POST"])
@paid
def day_done(cid, n):
    c = get_course(cid); day_plan(c, n)
    if request.form.get("undo"): q("delete from progress where course_id=? and day=?", (cid, n))
    else:
        q("insert into progress(course_id,day) values(?,?) on conflict do nothing", (cid, n)); flash(f"Day {n} completed. Great work!", "ok")
        if n < len(json.loads(c["plan"])): return redirect(url_for("course", cid=cid))
    return redirect(url_for("course", cid=cid))

@app.route("/test/<int:cid>")
@paid
def test(cid):
    c = get_course(cid); touch(c)
    return render_template("test_choose.html", c=c, pcount=len(pyq_list(c)), pmeta=pyq_meta(c))

@app.route("/course/<int:cid>/pyq")
@paid
def pyq_page(cid):
    c = get_course(cid); touch(c); items = pyq_list(c)
    for i in items: i["links"] = pyq_links(c, i)
    years = sorted({i["year"] for i in items if i.get("year")}, reverse=True)
    return render_template("pyq.html", c=c, items=items, meta=pyq_meta(c), years=years)

@app.route("/course/<int:cid>/pyq/refresh", methods=["POST"])
@paid
def pyq_refresh(cid):
    """Search the websites again for previous-year papers (also for plans built before this feature existed)."""
    c = get_course(cid); u = me(); cls = class_of(u)
    pq, pm = build_pyq(c["track"], c["title"], cls["course"] if cls else "", c["exam"], json.loads(c["topics"] or "[]"), "")
    if not pm: flash("Could not search for previous-year papers right now. Please try again in a minute.", "err")
    else:
        q("update courses set pyq=?, pyq_meta=? where id=?", (pq, pm, cid))
        flash(f"📜 {len(json.loads(pq)) if pq else 0} previous-year questions found." if pq else "No previous-year papers could be found online for this subject.", "ok" if pq else "err")
    return redirect(url_for("pyq_page", cid=cid))

@app.route("/test/<int:cid>/mcq")
@paid
def test_mcq(cid):
    c = get_course(cid); picked = pyq_pick(c, 6)
    try:
        t = aj(f"""Write 10 multiple-choice questions for '{c['title']}' ({c['exam']}). Topics: {c['topics'][:5000]}.
{pyq_prompt(picked)}
{'Use ALL of the real previous-year questions above as MCQs (keep their meaning, write 4 good options) and set "pyq_id" to the id of each one. Write the remaining questions yourself with "pyq_id": null.' if picked else ''}
Questions that are frequently asked in real exams (found online, use these as a guide for style and difficulty): {(c['research'] or '')[:2500] or 'none'}
{qctx(c)}
{notes_rule(c, [t.get('name', '') for t in json.loads(c['topics'] or '[]')][:8], 3000)}
{mind(session['uid'], cid)} Ask more questions on weak topics and on high-importance topics.
{STYLE}
Return JSON list: [{{"topic":"","q":"","options":["","","",""],"answer":0-3,"why":"short, simple explanation; for maths show the working line by line","pyq_id":null}}]""")
        t = pyq_tag(c, t, picked)
    except Exception as e:
        flash(f"Could not generate the test ({e}).", "err"); return redirect(url_for("course", cid=cid))
    q("update courses set test=? where id=?", (json.dumps(t), cid))
    return render_template("test.html", c=c, qs=t)

@app.route("/test/<int:cid>/descriptive")
@paid
def test_desc(cid):
    c = get_course(cid); qs = json.loads(c["dtest"] or "[]")
    if request.args.get("fresh") or not qs:
        picked = pyq_pick(c, 5)
        try:
            qs = aj(f"""Write 5 descriptive (written-answer) questions for '{c['title']}' ({c['exam']}). Topics: {c['topics'][:5000]}.
{pyq_prompt(picked)}
{'Use the real previous-year questions above as the questions (keep the wording and meaning exactly) and set "pyq_id" to the id of each. Only if fewer than 5 are given, write the rest yourself with "pyq_id": null.' if picked else ''}
Questions that are frequently asked in real exams (found online, use these as a guide for style and difficulty): {(c['research'] or '')[:2500] or 'none'}
{qctx(c)}
If important questions are given above, use five of them (weak topics first) as the questions.
{mind(session['uid'], cid)} Ask more on weak and high-importance topics.
If the subject is mathematical or numerical, write problems to solve step by step and set "kind":"math"; otherwise "kind":"theory".
Return JSON list: [{{"topic":"","kind":"math or theory","q":"","key_points":["",""],"model_answer":"","pyq_id":null}}] (key_points and model_answer are for grading only)""", 5000)
            qs = pyq_tag(c, qs, picked)
        except Exception as e:
            flash(f"Could not generate the test ({e}).", "err"); return redirect(url_for("test", cid=cid))
        q("update courses set dtest=? where id=?", (json.dumps(qs), cid))
    return render_template("desc.html", c=c, qs=qs)

def grade(x, typed, imgs):
    math = x.get("kind") == "math"
    p = f"""You are a strict but fair examiner. Question (10 marks): {x['q']}
Hidden marking guide - key points: {x.get('key_points')}. Model answer: {x.get('model_answer')}
Student's typed answer: {typed or '(none)'}
{'The student also attached photo(s) of their handwritten answer. Read them carefully.' if imgs else ''}
{'This is a maths problem: check every step and the final answer. Mark it "correct" only if both method and final answer are right.' if math else ''}
If the handwriting is too unclear to read reliably, set "readable":false and do not grade.
{STYLE}
{MATH_STYLE if math else ''}
Return JSON only: {{"readable":true,"verdict":"correct or partially correct or incorrect","score":0-10,"feedback":"2-4 simple sentences to the student: what is right and what is wrong","fix":"{'start with [MATHS] on the first line, then the correct solution in notebook style' if math else 'the correct approach or answer in brief'}"}}"""
    try: d = aj(p, 900, imgs or None)
    except Exception as e:
        return dict(status="error", score=0, verdict="", feedback=f"Could not check this answer right now ({str(e)[:100]}). Please try again.", fix="")
    if d.get("readable") is False:
        return dict(status="unreadable", score=0, verdict="", feedback="I could not read this clearly. Please write clearly in dark ink on plain paper, good light, whole page in the photo, and upload again.", fix="")
    try: sc = max(0, min(10, int(float(d.get("score", 0)))))
    except Exception: sc = 0
    return dict(status="graded", score=sc, verdict=str(d.get("verdict", "")), feedback=d.get("feedback", ""), fix=d.get("fix", ""))

@app.route("/test/<int:cid>/descriptive/submit", methods=["POST"])
@paid
def submit_desc(cid):
    c = get_course(cid); qs = json.loads(c["dtest"] or "[]")
    if not qs: return redirect(url_for("test", cid=cid))
    jobs = []
    for i, x in enumerate(qs):
        imgs, err = images_from(request.files.getlist(f"f{i}"))
        jobs.append((x, request.form.get(f"t{i}", "").strip(), imgs, err))
    def run(j):
        x, typed, imgs, err = j
        if err: return dict(status="error", score=0, verdict="", feedback=err, fix="")
        if not typed and not imgs: return dict(status="skipped", score=0, verdict="", feedback="Not attempted.", fix="")
        return grade(x, typed, imgs)
    with ThreadPoolExecutor(3) as ex: res = list(ex.map(run, jobs))
    agg = {}; review = []
    for (x, *_), r in zip(jobs, res):
        review.append(dict(q=x["q"], topic=x["topic"], kind=x.get("kind"), **r))
        if r["status"] in ("graded", "skipped"):
            t = agg.setdefault(x["topic"], [0, 0]); t[0] += r["score"]; t[1] += 10
    for t, (sc, n) in agg.items(): q("insert into results(course_id,topic,correct,total) values(?,?,?,?)", (cid, t, sc, n))
    score = sum(v[0] for v in agg.values()); out_of = sum(v[1] for v in agg.values())
    coach = ""
    if agg:
        try: coach = ai(f"""Student scored {score}/{out_of} on a written test for '{c['title']}'. Per topic (marks, out of): {agg}. Examiner feedback: {[r['feedback'][:120] for r in review]}.
{mind(session['uid'], cid)} Give a short analysis: strengths, weaknesses, and a clear what-to-do-next list. Plain text, max 150 words.""", 800)
        except Exception: coach = "Review the feedback below and practise your weakest topics again."
        q("update courses set coach=? where id=?", (coach, cid))
    return render_template("desc_result.html", c=c, review=review, score=score, out_of=out_of, coach=coach)

@app.route("/test/<int:cid>/submit", methods=["POST"])
@paid
def submit(cid):
    c = get_course(cid); qs = json.loads(c["test"] or "[]"); agg = {}; review = []
    for i, x in enumerate(qs):
        a = request.form.get(f"a{i}"); ok = a is not None and int(a) == int(x["answer"])
        t = agg.setdefault(x["topic"], [0, 0]); t[0] += ok; t[1] += 1
        review.append(dict(x, picked=a, ok=ok))
    for t, (ok, n) in agg.items(): q("insert into results(course_id,topic,correct,total) values(?,?,?,?)", (cid, t, ok, n))
    score = sum(v[0] for v in agg.values())
    try:
        coach = ai(f"""Student scored {score}/{len(qs)} on '{c['title']}'. Per topic (correct,total): {agg}. Overall weak areas: {stats(cid)[:5]}.
{mind(session['uid'])} Give a short analysis: strengths, weaknesses, mind-set advice, and a clear what-to-do-next list. Plain text, max 180 words.""", 800)
    except Exception: coach = "Review the wrong answers below and retrain on your weakest topics."
    q("update courses set coach=? where id=?", (coach, cid))
    flash(f"Test submitted. You scored {score}/{len(qs)}.", "ok")
    return render_template("analysis.html", c=c, st=stats(cid), review=review, score=score, n=len(qs), coach=coach)

@app.route("/analysis/<int:cid>")
@paid
def analysis(cid):
    c = get_course(cid); touch(c)
    return render_template("analysis.html", c=c, st=stats(cid), review=[], score=None, n=0, coach=c["coach"])

@app.route("/retrain/<int:cid>", methods=["POST"])
@paid
def retrain(cid):
    c = get_course(cid)
    if (c.get("qmode") or "syllabus") == "questions":
        weak = [t for t, p in stats(cid) if p < 70]
        rows = sorted(q("select n,topic from iq where course_id=? order by n", (cid,)), key=lambda r: (r["topic"] not in weak, r["n"]))
        plan = qplan(rows, c["days"])
        q("update courses set plan=?, days=? where id=?", (json.dumps(plan), len(plan), cid))
        flash("Questions re-ordered: your weak topics now come first.", "ok"); return redirect(url_for("course", cid=cid))
    try:
        d = aj(f"""Re-plan the remaining study for '{c['title']}' over {c['days']} days. Topics: {c['topics'][:4000]}.
{mind(session['uid'], cid)} Give weak topics more days and revision. Return JSON: {{"plan":[{{"day":1,"focus":["topic"],"tip":""}}]}}""")
        q("update courses set plan=? where id=?", (json.dumps(d["plan"]), cid)); q("delete from lessons where course_id=?", (cid,)); flash("Plan retrained around your weak topics.", "ok")
    except Exception as e: flash(f"Retraining failed ({e}).", "err")
    return redirect(url_for("course", cid=cid))

@app.route("/doubt", methods=["GET", "POST"])
@paid
def doubt():
    u = me(); question = ""
    if request.method == "POST":
        doc = text_from(request.files.get("file"))
        imgs, err = images_from(request.files.getlist("note"))
        question = request.form.get("question", "").strip()
        if err: flash(err, "err")
        elif not question and not imgs: flash("Type your doubt or upload a photo of your handwritten note.", "err")
        else:
            try:
                ans = tutor(question, doc=doc, imgs=imgs, focus=focus_ctx(u)) + src_footer(u)
                q("insert into doubts(user_id,question,answer) values(?,?,?)",
                  (u["id"], (question or "Handwritten note") + (" 📎" if imgs else ""), ans))
                return redirect(url_for("doubt") + "#answers")
            except Exception as e: flash(f"AI error ({str(e)[:150]}).", "err")
    hist = q("""select question,answer,created,greatest(0, extract(epoch from (created + interval '24 hours' - localtimestamp)))::int as left_s
                from doubts where user_id=? and created > localtimestamp - interval '24 hours' order by id desc limit 15""", (u["id"],))
    return render_template("doubt.html", hist=hist, question=question)

@app.route("/ask-ai", methods=["POST"])
@paid
def ask_ai():
    """'Ask AI' button under any explanation, question or test result."""
    u = me(); ctx = request.form.get("context", "").strip()[:1500]; question = request.form.get("question", "").strip()
    back = request.referrer or url_for("doubt")
    imgs, err = images_from(request.files.getlist("note"))
    if err: flash(err, "err"); return redirect(back)
    if not question and not imgs:
        flash("Type your question or upload a photo of your handwritten note.", "err"); return redirect(back)
    try: ans = tutor(question, ctx=ctx, imgs=imgs, focus=focus_ctx(u)) + src_footer(u)
    except Exception as e:
        flash(f"AI error ({str(e)[:150]}).", "err"); return redirect(back)
    tag = ("📌 " + ctx[:90].replace("\n", " ") + "… → ") if ctx else ""
    q("insert into doubts(user_id,question,answer) values(?,?,?)", (u["id"], tag + (question or "Handwritten note") + (" 📎" if imgs else ""), ans))
    flash("Your answer is ready. It is saved here in Doubts.", "ok")
    return redirect(url_for("doubt") + "#answers")

@app.route("/course/<int:cid>/explain")
@paid
def explain(cid):
    """Explain any specific topic or question in handwritten notes (cached)."""
    c = get_course(cid); t = request.args.get("t", "").strip()[:300]
    if not t: return redirect(url_for("course", cid=cid))
    touch(c)
    key = t.lower(); row = q("select body from topicx where course_id=? and key=?", (cid, key), one=True)
    if row and not request.args.get("fresh"): body = row["body"]
    else:
        try:
            body = ai(f"""Teach this to a student preparing for '{c['title']}' ({c['exam']}): "{t}".
Source material from the student (may be empty): {(c['src'] or '')[:5000]}
{notes_rule(c, [t], 4000)}
{mind(c['user_id'], cid)}
If it is a question, write the complete model answer the student can learn and write in the exam. If it is a topic, teach it from zero to full depth.
{DEPTH_STYLE}
{NOTE_STYLE}
{STYLE}
{MATH_STYLE}""", 5500)
        except Exception as e:
            flash(f"Could not explain this right now ({str(e)[:120]}). Please try again.", "err"); return redirect(url_for("course", cid=cid))
        q("insert into topicx(course_id,key,body) values(?,?,?) on conflict(course_id,key) do update set body=excluded.body", (cid, key, body))
    return render_template("explain.html", c=c, t=t, body=body)

# ---- hidden admin: no link anywhere in the app; open /adminlogin directly ----
@app.route("/adminlogin", methods=["GET", "POST"])
def adminlogin():
    if request.method == "POST":
        ok = hmac.compare_digest(request.form["u"], os.getenv("ADMIN_USER", "")) and hmac.compare_digest(request.form["p"], os.getenv("ADMIN_PASS", ""))
        if ok and os.getenv("ADMIN_USER"): session["admin"] = True; return redirect(url_for("admin"))
        flash("Invalid admin credentials.", "err")
    return render_template("adminlogin.html")

@app.route("/admin")
def admin():
    if not session.get("admin"): return redirect(url_for("adminlogin"))
    paid_n = q("select count(*) n from payments where status='approved'", one=True)["n"]
    pend_n = q("select count(*) n from payments where status='pending'", one=True)["n"]
    st = q("""select count(*) total, count(*) filter (where pay_status='approved') active, count(*) filter (where access='free') free,
              count(*) filter (where suspended) susp, count(*) filter (where last_seen > localtimestamp - interval '10 minutes') online from users""", one=True)
    pending_fac = q("select id,name,email,phone from users where role='faculty' and verified=false order by id")
    users = q("""select id,name,email,phone,pay_status,access,suspended,last_seen,role,verified,(select count(*) from devices d where d.user_id=users.id) as dev_count,
                 (select c.course||' · '||c.code from classes c where c.id=users.class_id) as class_label,
                 coalesce(last_seen > localtimestamp - interval '10 minutes', false) as online from users order by last_seen desc nulls last, id desc""")
    ps = q("""select p.id,p.status,p.created,p.decided,(p.img is not null) as has_img,u.name,u.email,u.phone
              from payments p join users u on u.id=p.user_id order by p.id desc limit 100""")
    classes = q("""select c.id,c.course,c.code,c.subjects,f.name as fname,f.email as femail,(select count(*) from users s where s.class_id=c.id) as n
                   from classes c left join users f on f.id=c.faculty_id order by c.id desc""")
    return render_template("admin.html", pending_fac=pending_fac, classes=classes, cfg=pay_cfg(), ps=ps, users=users, st=st, money=paid_n * PRICE, paid_n=paid_n, pend_n=pend_n, price=PRICE)

@app.route("/admin/<int:pid>/<act>", methods=["POST"])
def admin_act(pid, act):
    if not session.get("admin"): abort(403)
    p = q("select * from payments where id=?", (pid,), one=True)
    if p and act in ("approve", "reject"):
        q("update payments set status=?, decided=localtimestamp where id=?", (act + "d", pid))
        if act == "approve": q("update users set pay_status='approved', access='paid' where id=?", (p["user_id"],))
        else: q("update users set pay_status='rejected' where id=?", (p["user_id"],))
        flash(f"Payment {act}d.", "ok")
    return redirect(url_for("admin"))

@app.route("/admin/user/<int:uid>/<act>", methods=["POST"])
def admin_user(uid, act):
    if not session.get("admin"): abort(403)
    sql = {"suspend": "suspended=true", "unsuspend": "suspended=false",
           "grant": "pay_status='approved', access='free', suspended=false", "revoke": "pay_status='none', access=''",
           "resetdevice": "device_id=''"}.get(act)
    if act == "approvefac":
        q("update users set verified=true where id=? and role='faculty'", (uid,)); flash("Faculty approved. They can log in now.", "ok"); return redirect(url_for("admin") + "#faculty")
    if act == "delete":
        delete_user(uid); flash("User removed with all their data.", "ok"); return redirect(url_for("admin") + "#users")
    if act == "newpass":      # works even when email is broken: the admin sees the password and passes it on
        u = q("select name,email from users where id=?", (uid,), one=True)
        if not u: abort(404)
        pw = temp_password(); q("update users set pw=? where id=?", (make_pw(pw), uid)); q("update devices set sess_token='' where user_id=?", (uid,))
        flash(f"New password for {u['name']} ({u['email']}): {pw}  (give it to them; they should change it after login)", "ok")
        return redirect(url_for("admin") + "#users")
    if sql:
        q("update users set " + sql + " where id=?", (uid,))
        if act == "resetdevice": q("delete from devices where user_id=?", (uid,))
        flash({"suspend": "User suspended.", "unsuspend": "User un-suspended.", "grant": "Free access given.", "revoke": "Access removed.", "resetdevice": "All devices removed. The user can log in again on up to 2 devices."}[act], "ok")
    return redirect(url_for("admin") + "#users")

@app.route("/admin/testmail", methods=["POST"])
def admin_testmail():
    """Sends a test email and shows the real result, so email problems can be found."""
    if not session.get("admin"): abort(403)
    to = request.form.get("to", "").strip().lower()
    if not EMAIL_RE.match(to): flash("Enter a valid email address for the test.", "err")
    else:
        try:
            res = send_mail(to, "GrowthPulse test email", "This is a test email from GrowthPulse. If you can read this, email sending works.")
            flash(f"Email accepted by the mail service for {to} ({res or 'ok'}). If it does not arrive, check Spam and your Brevo sender/logs.", "ok")
        except Exception as e: flash(f"Email FAILED: {str(e)[:300]}", "err")
    return redirect(url_for("admin"))

def delete_user(uid):
    """Remove a user and everything that belongs to them."""
    cids = [r["id"] for r in q("select id from courses where user_id=?", (uid,))]
    if cids:
        for t in ("lessons", "chats", "progress", "results", "iq", "topicx"): q(f"delete from {t} where course_id = any(?)", (cids,))
    for t in ("courses", "doubts", "checkins", "payments", "devices", "jobs"): q(f"delete from {t} where user_id=?", (uid,))
    cl = [r["id"] for r in q("select id from classes where faculty_id=?", (uid,))]
    if cl: q("update users set class_id=null where class_id = any(?)", (cl,)); q("delete from classes where faculty_id=?", (uid,))
    q("delete from users where id=?", (uid,))

# who can be cleaned up: students who never paid and have no free access (a screenshot waiting for approval is NOT removed) + faculty never verified
PURGE_WHERE = """((coalesce(role,'student')<>'faculty' and coalesce(pay_status,'none') in ('none','rejected','') and coalesce(access,'')='')
                 or (role='faculty' and verified=false))"""

@app.route("/admin/purge", methods=["GET", "POST"])
def admin_purge():
    if not session.get("admin"): abort(403)
    if request.method == "POST":
        ids = [int(x) for x in request.form.getlist("ids") if x.isdigit()]
        ok = [r["id"] for r in q(f"select id from users where id = any(?) and {PURGE_WHERE}", (ids,))] if ids else []
        for i in ok: delete_user(i)
        flash(f"Removed {len(ok)} user(s).", "ok"); return redirect(url_for("admin") + "#users")
    rows = q(f"select id,name,email,phone,role,pay_status,verified from users where {PURGE_WHERE} order by role, id")
    return render_template("admin_purge.html", rows=rows)

@app.route("/admin/testnotes", methods=["POST"])
def admin_testnotes():
    """Live check of the professor-notes search: shows every step, so you can see whether it really works."""
    if not session.get("admin"): abort(403)
    subject = request.form.get("subject", "").strip()[:100] or "Data Structures"; course = request.form.get("course", "").strip()[:100]
    D = []; t0 = time.time(); srcs = []; raw = ""; groq_ok = ""
    try: ai("Reply with the single word OK.", 50); groq_ok = "Groq text model answers: OK"
    except Exception as e: groq_ok = "Groq text model FAILED: " + str(e)[:200]
    try: srcs, raw = find_notes(subject, course, "Semester Exam", [subject], "", dbg=D, use_cache=False)
    except Exception as e: D.append("find_notes crashed: " + str(e)[:200])
    return render_template("admin_notes.html", subject=subject, D=D, srcs=srcs, raw=raw, groq_ok=groq_ok, secs=round(time.time() - t0, 1))

@app.route("/admin/testpyq", methods=["POST"])
def admin_testpyq():
    """Live check of the previous-year-papers search."""
    if not session.get("admin"): abort(403)
    subject = request.form.get("subject", "").strip()[:100] or "Data Structures"; course = request.form.get("course", "").strip()[:100]
    D = []; t0 = time.time(); srcs = []; raw = ""
    try: srcs, raw = find_pyq(subject, course, "Semester Exam", dbg=D, use_cache=False)
    except Exception as e: D.append("find_pyq crashed: " + str(e)[:200])
    return render_template("admin_notes.html", subject=subject, D=D, srcs=srcs, raw=raw, groq_ok="Previous-year paper search (needs 4+ different websites to be strong)", secs=round(time.time() - t0, 1), what="previous-year papers")

@app.route("/admin/settings", methods=["POST"])
def admin_settings():
    if not session.get("admin"): abort(403)
    f = request.form
    for k, col in (("pay_name", "name"), ("pay_phone", "phone"), ("pay_upi", "upi")):
        q("insert into settings(key,val) values(?,?) on conflict(key) do update set val=excluded.val", (k, f.get(col, "").strip()[:80]))
    fs = request.files.get("qr")
    if fs and fs.filename:
        try:
            from PIL import Image, ImageOps
            im = ImageOps.exif_transpose(Image.open(fs.stream)).convert("RGB"); im.thumbnail((900, 900))
            b = io.BytesIO(); im.save(b, "PNG", optimize=True)
            q("insert into settings(key,img,mime) values('qr',?,'image/png') on conflict(key) do update set img=excluded.img, mime=excluded.mime", (psycopg2.Binary(b.getvalue()),))
        except Exception:
            flash("Could not read that QR image. Please upload a PNG or JPG.", "err"); return redirect(url_for("admin") + "#settings")
    flash("Payment settings saved. Users will see them on the payment page now.", "ok")
    return redirect(url_for("admin") + "#settings")

@app.route("/adminlogout")
def adminlogout():
    session.pop("admin", None); return redirect(url_for("adminlogin"))

@app.route("/admin/file/<int:pid>")
def shot(pid):
    if not session.get("admin"): abort(403)
    r = q("select img,mime from payments where id=?", (pid,), one=True)
    if not r or r["img"] is None: abort(404)
    return Response(bytes(r["img"]), mimetype=r["mime"] or "image/jpeg", headers={"Cache-Control": "private, max-age=3600"})

if __name__ == "__main__": app.run(debug=True)
