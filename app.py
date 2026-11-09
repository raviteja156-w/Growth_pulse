import os, io, json, re, uuid, hmac, base64, mimetypes, functools, time, requests
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

# ---------------- Supabase (Postgres) ----------------
DBURL = os.getenv("DATABASE_URL", "").strip()
if DBURL.startswith("postgres://"): DBURL = "postgresql://" + DBURL[len("postgres://"):]
if DBURL.startswith("postgresql://") and "sslmode=" not in DBURL: DBURL += ("&" if "?" in DBURL else "?") + "sslmode=require"
_pool = None
def pool():
    global _pool
    if _pool is None:
        if not DBURL: raise RuntimeError("DATABASE_URL is not set (add your Supabase connection string)")
        if not DBURL.startswith("postgresql://"):
            raise RuntimeError("DATABASE_URL must be the Supabase POSTGRES connection string (starts with postgresql://), not the https://...supabase.co project URL. Supabase > Connect > Session pooler.")
        _pool = psycopg2.pool.ThreadedConnectionPool(1, 8, DBURL, connect_timeout=15)
    return _pool

def q(sql, a=(), one=False, rid=False):
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
 "alter table payments add column if not exists decided timestamp",
 "update payments set decided=created where decided is null and status<>'pending'",
 "update users set access='paid' where pay_status='approved' and (access is null or access='')",
 "create table if not exists settings(key text primary key,val text default '',img bytea,mime text default 'image/png')",
 "create table if not exists topicx(course_id integer,key text,body text,primary key(course_id,key))",
 "create index if not exists i_courses_user on courses(user_id)",
 "create index if not exists i_chats_cd on chats(course_id,day)",
 "create index if not exists i_results_c on results(course_id)",
 "create index if not exists i_doubts_u on doubts(user_id)",
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

def me():
    if "uid" not in session: return None
    if "u" not in g: g.u = q("select * from users where id=?", (session["uid"],), one=True)
    return g.u
@app.context_processor
def inj(): return dict(me=me())

def paid(f):
    @functools.wraps(f)
    def w(*a, **k):
        u = me()
        if not u: return redirect(url_for("login"))
        if u["pay_status"] != "approved":
            flash("Unlock all features with a one-time payment of ₹50.", "err"); return redirect(url_for("pay"))
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
        elif re.match(r"(?i)^(given|to find|find|formula|solution|check|verify|proof|aim|note|definition|example|diagram|remember|real life|key points|why)\s*:", s): cls += " hd"
        elif s[0] in "=≈": cls += " eq"
        out.append('<div class="%s">%s</div>' % (cls, escape(l.rstrip())))
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

NOTE_STYLE = """HANDWRITTEN NOTES STYLE (very important): write exactly like a good teacher writing neat class notes by hand for a student. One short idea per line, every line separated by a newline. Begin with the direct answer or definition in one line. Then explain step by step in very simple words. Always include one small everyday EXAMPLE (start that line with 'Example:'). If useful, describe a tiny diagram in words (start that line with 'Diagram:'). Finish with one line starting 'Remember:' that holds the key points to memorise. Never write long paragraphs."""

def tutor(question, ctx="", doc="", imgs=None):
    return ai(f"""A student has a doubt. {('The doubt is about this part of their study material: ' + ctx) if ctx else ''}
Student's doubt: {question or 'Please read my handwritten note and clarify my doubt.'}
{'A photo of the handwritten note is attached; say what you understood, and if anything is unclear ask the student to write it more clearly.' if imgs else ''}
Reference material (may be empty):
{doc[:12000]}
Explain like a patient teacher to a beginner, with a small everyday example.
{NOTE_STYLE}
{STYLE}
{MATH_STYLE}""", 2500, imgs or None)

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

def ai(prompt, tokens=6000, images=None, clean_out=True):
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
        if "gpt-oss" in m: body["reasoning_effort"] = "low"
        r = requests.post("https://api.groq.com/openai/v1/chat/completions",
                          headers={"Authorization": "Bearer " + k, "Content-Type": "application/json"}, json=body, timeout=140)
        if r.ok:
            o = re.sub(r"<think>.*?</think>", "", r.json()["choices"][0]["message"]["content"] or "", flags=re.S).strip()
            return clean(o) if clean_out else o
        err = f"{m}: {r.status_code} {r.text[:200]}"
        if r.status_code not in (400, 404, 429, 503): break
    raise RuntimeError(err)
def aj(prompt, tokens=6000, images=None):
    return deep_clean(json.loads(re.search(r"[\[{].*[\]}]", ai(prompt, tokens, images, clean_out=False), re.S).group(0), strict=False))

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
            r = requests.post("https://api.groq.com/openai/v1/chat/completions",
                              headers={"Authorization": "Bearer " + k, "Content-Type": "application/json"}, json=body, timeout=55)
            if r.ok:
                t = clean(r.json()["choices"][0]["message"]["content"] or "")
                if len(t) > 80: return t[:5000]
        except Exception as e:
            print("research error", m, str(e)[:120])
    return ""

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
OPEN_EP = {"static", "welcome", "demo_done", "adminlogin", "adminlogout", "admin", "admin_act", "admin_user", "admin_settings", "shot", "bot"}
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
        if now - session.get("seen", 0) > 240:
            session["seen"] = now; q("update users set last_seen=localtimestamp where id=?", (u["id"],))

@app.route("/welcome")
def welcome():
    return render_template("welcome.html", seen=bool(request.cookies.get("gp_demo")))

@app.route("/demo-done", methods=["POST"])
def demo_done():
    r = jsonify(ok=True); r.set_cookie("gp_demo", "1", max_age=31536000, httponly=True, samesite="Lax", secure=request.is_secure); return r


BOT_SYS = """You are the GrowthPulse assistant inside a study app for students in India. GrowthPulse: upload a syllabus or important questions, pick days, get a day-by-day plan with handwritten-style lessons, ask doubts (type or handwritten photo), take MCQ or descriptive tests, see mind-set analysis and retrain weak topics. Features unlock with a one-time Rs 50 payment approved by the admin. Answer ONLY what the student asked, in simple short lines (max 120 words), plain text, no markdown, no tables. Use the page content given to be specific. If you are not sure, say so and suggest the Doubts page. Never invent features, prices or links."""
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
    prompt = f"""{BOT_SYS}
Student is {'logged in (' + ('fees paid' if u['pay_status'] == 'approved' else 'features locked until payment') + ')' if u else 'not logged in'}.
Current page: {str(j.get('title', ''))[:100]} ({str(j.get('path', ''))[:100]})
Page content (may be partial): {str(j.get('page', ''))[:1200]}
Chat so far:
{hist or '(none)'}
Student: {qn}
Assistant:"""
    try: a = ai(prompt, 700)
    except Exception as e:
        print("bot error", str(e)[:150])
        a = "Sorry, I could not reach the AI just now. Please try again in a moment. For study doubts you can also use the Doubts page."
    return jsonify(answer=a[:1500])

@app.route("/")
def home(): return render_template("home.html")

@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        f = request.form
        try:
            uid = q("insert into users(name,email,phone,pw) values(?,?,?,?)",
                    (f["name"].strip(), f["email"].strip().lower(), f["phone"].strip(), generate_password_hash(f["password"])), rid=True)
        except psycopg2.IntegrityError:
            flash("This email is already registered. Please log in.", "err"); return redirect(url_for("login"))
        session["uid"] = uid; flash("Account created successfully! Welcome aboard.", "ok"); return redirect(url_for("dashboard"))
    return render_template("auth.html", mode="register")

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        u = q("select * from users where email=?", (request.form["email"].strip().lower(),), one=True)
        if u and check_password_hash(u["pw"], request.form["password"]):
            if u.get("suspended"):
                flash("Your account is suspended. Please contact the admin.", "err"); return render_template("auth.html", mode="login")
            session["uid"] = u["id"]; flash("Logged in successfully.", "ok"); return redirect(url_for("dashboard"))
        flash("Wrong email or password.", "err")
    return render_template("auth.html", mode="login")

@app.route("/logout")
def logout(): session.clear(); flash("You have been logged out.", "ok"); return redirect(url_for("home"))

@app.route("/dashboard")
def dashboard():
    u = me()
    if not u: return redirect(url_for("login"))
    cs = q("select * from courses where user_id=? order by id desc", (u["id"],))
    return render_template("dashboard.html", cs=cs)

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
    if request.method == "POST":
        f = request.form
        q("update users set name=?,phone=?,profession=?,goal=? where id=?", (f["name"], f["phone"], f["profession"], f["goal"], u["id"]))
        flash("Profile saved.", "ok"); return redirect(url_for("dashboard"))
    return render_template("profile.html")

@app.route("/new/<track>", methods=["GET", "POST"])
@paid
def new(track):
    if track not in ("college", "personal"): abort(404)
    u = me()
    if track == "personal" and not (u["profession"] and u["goal"]):
        flash("Please complete your profile first so AI can train you accordingly.", "err"); return redirect(url_for("profile"))
    if request.method == "POST":
        f = request.form
        qmode = "questions" if (track == "college" and f.get("qmode") == "questions") else "syllabus"
        exam = f.get("exam", "Semester Exam") if track == "college" else "Personal Growth"
        days = max(1, min(int(f.get("days") or 7), 120))
        src = "\n".join(text_from(x) for x in request.files.getlist("files")) + "\n" + f.get("topics", "")
        papers = "\n".join(text_from(x) for x in request.files.getlist("papers"))
        title = f.get("title") or u["goal"]
        if track == "personal": src = f"Profession: {u['profession']}. Wants to become: {u['goal']}. Notes: " + src
        if len(src.strip()) < 15:
            flash("Please upload a file or type your " + ("important questions." if qmode == "questions" else "syllabus topics."), "err"); return redirect(request.url)
        if qmode == "questions":
            try:
                items = aj(f"""Below is text from an internal-exam IMPORTANT QUESTIONS list for the subject '{title}' (it may be messy text copied from a PDF). Extract every distinct question, fix obvious typos, keep at most 60. Give each a short topic name (2 to 4 words).
Text: {src[:12000]}
Return JSON list: [{{"q":"the question","topic":"topic name"}}]""", 5000)
                if isinstance(items, dict): items = next((v for v in items.values() if isinstance(v, list)), [])
                items = [{"n": i + 1, "q": str(x.get("q", "")).strip(), "topic": (str(x.get("topic") or "General").strip()[:60] or "General")}
                         for i, x in enumerate(items[:60]) if isinstance(x, dict) and str(x.get("q", "")).strip()]
                if not items: raise ValueError("no questions found in the file")
            except Exception as e:
                flash(f"Could not read the questions ({str(e)[:150]}). Try a clearer PDF or type them one per line.", "err"); return redirect(request.url)
            plan = qplan(items, days); cnt = {}
            for x in items: cnt[x["topic"]] = cnt.get(x["topic"], 0) + 1
            topics = [{"name": t, "importance": min(5, 2 + k), "summary": f"{k} important question(s)"} for t, k in cnt.items()]
            cid = q("insert into courses(user_id,track,title,exam,days,plan,cards,topics,src,research,qmode) values(?,?,?,?,?,?,?,?,?,?,?)",
                    (u["id"], track, title, exam, len(plan), json.dumps(plan), "[]", json.dumps(topics), src[:12000], "", "questions"), rid=True)
            for x in items: q("insert into iq(course_id,n,topic,q) values(?,?,?,?)", (cid, x["n"], x["topic"], x["q"]))
            flash(f"{len(items)} important questions found and planned over {len(plan)} days. Open Day 1 to learn the answers.", "ok")
        else:
            scope = ("Cover EVERY topic of the syllabus across the days, keeping the last day(s) for revision." if exam == "Semester Exam"
                     else "Use ONLY the topics the student listed." if track == "college" else "Build a skill roadmap from beginner to job-ready.")
            brief = research(title, exam, src)
            try:
                d = aj(f"""Create a study programme. Exam: {exam}. Subject/goal: {title}. Days available: {days}. {scope}
{mind(u['id'])}
Topics that repeat in previous papers (give higher importance): {papers[:5000] or 'none given'}
Real-world research from the web (use it to decide which topics are really important): {brief[:3500] or 'not available'}
Content: {src[:12000]}
Return JSON: {{"topics":[{{"name":"","importance":1-5,"summary":""}}],"plan":[{{"day":1,"focus":["topic"],"tip":""}}] (exactly {days} days)}}""", 6000)
            except Exception as e:
                flash(f"AI could not build the plan ({e}). Check the API key and try again.", "err"); return redirect(request.url)
            cid = q("insert into courses(user_id,track,title,exam,days,plan,cards,topics,src,research) values(?,?,?,?,?,?,?,?,?,?)",
                    (u["id"], track, title, exam, days, json.dumps(d["plan"]), "[]", json.dumps(d["topics"]), src[:12000], brief), rid=True)
            flash("Your day-wise plan is ready! Open Day 1 to start learning.", "ok")
        first = f.get("firstq", "").strip()
        if first: return redirect(url_for("explain", cid=cid, t=first))
        return redirect(url_for("course", cid=cid))
    return render_template("new.html", track=track)

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
    return render_template("course.html", c=c, plan=plan, topics=json.loads(c["topics"]), st=stats(cid), done=done, opened=opened, nxt=nxt, qmode=qmode, qs=qs, qday=qday)

def qlesson(c, d, n):
    nums = [int(x) for x in d.get("qs", [])]
    rows = [dict(r) for r in q("select n,topic,q,ans from iq where course_id=? and n = any(?) order by n", (c["id"], nums))]
    miss = [r for r in rows if not (r["ans"] or "").strip()]
    if miss:
        payload = json.dumps([{"n": r["n"], "q": r["q"], "topic": r["topic"]} for r in miss])
        res = aj(f"""Subject: '{c['title']}' ({c['exam']}). These are IMPORTANT exam questions. Write the full model answer for each one so a student can learn it and write it in the exam.
Questions: {payload}
Answer length should suit the marks: a short definition question gets 5 to 7 lines, an explain/discuss question gets 12 to 20 lines, a numerical gets the full worked solution.
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
{STYLE}
{NOTE_STYLE}
{MATH_STYLE}

Return JSON: {{"title":"","math":true or false (true if today's topic needs calculations),"intro":"3-5 simple sentences: what we learn today and why it matters","words":[{{"term":"","meaning":"one simple line"}}] (3 to 6 key words),"sections":[{{"h":"","body":"explanation written as handwritten class notes: one short idea per line, lines separated by newline characters","analogy":"everyday comparison","example":"worked example, one step per line","math":true or false,"mistake":"common beginner mistake and how to avoid it"}}] (4 to 6 sections),"recap":["",""] (4 to 6 simple points),"practice":[{{"q":"a question to try","answer":"full solution written step by step"}}] (3 questions, easy to medium)}}""", 7000)
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
{mind(c['user_id'], cid)}
{('The student is asking about this exact part: ' + ctx) if ctx else ''}
Student's doubt: {question or 'Please read my handwritten note and clarify my doubt.'}
{'The student attached a photo of their handwritten note. Read it carefully, say what you understood, and if a word or line is unclear, ask them to write it more clearly.' if imgs else ''}
Explain like a patient teacher to a beginner: simple words, a small everyday example, tied to today's topic.
{NOTE_STYLE}
{STYLE}
{MATH_STYLE}"""
    try: a = ai(prompt, 2500, imgs or None)
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
    return render_template("test_choose.html", c=get_course(cid))

@app.route("/test/<int:cid>/mcq")
@paid
def test_mcq(cid):
    c = get_course(cid)
    try:
        t = aj(f"""Write 10 multiple-choice questions for '{c['title']}' ({c['exam']}). Topics: {c['topics'][:5000]}.
Questions that are frequently asked in real exams (found online, use these as a guide for style and difficulty): {(c['research'] or '')[:2500] or 'none'}
{qctx(c)}
{mind(session['uid'], cid)} Ask more questions on weak topics and on high-importance topics.
{STYLE}
Return JSON list: [{{"topic":"","q":"","options":["","","",""],"answer":0-3,"why":"short, simple explanation; for maths show the working line by line"}}]""")
    except Exception as e:
        flash(f"Could not generate the test ({e}).", "err"); return redirect(url_for("course", cid=cid))
    q("update courses set test=? where id=?", (json.dumps(t), cid))
    return render_template("test.html", c=c, qs=t)

@app.route("/test/<int:cid>/descriptive")
@paid
def test_desc(cid):
    c = get_course(cid); qs = json.loads(c["dtest"] or "[]")
    if request.args.get("fresh") or not qs:
        try:
            qs = aj(f"""Write 5 descriptive (written-answer) questions for '{c['title']}' ({c['exam']}). Topics: {c['topics'][:5000]}.
Questions that are frequently asked in real exams (found online, use these as a guide for style and difficulty): {(c['research'] or '')[:2500] or 'none'}
{qctx(c)}
If important questions are given above, use five of them (weak topics first) as the questions.
{mind(session['uid'], cid)} Ask more on weak and high-importance topics.
If the subject is mathematical or numerical, write problems to solve step by step and set "kind":"math"; otherwise "kind":"theory".
Return JSON list: [{{"topic":"","kind":"math or theory","q":"","key_points":["",""],"model_answer":""}}] (key_points and model_answer are for grading only)""", 5000)
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
    c = get_course(cid)
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
                ans = tutor(question, doc=doc, imgs=imgs)
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
    try: ans = tutor(question, ctx=ctx, imgs=imgs)
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
    key = t.lower(); row = q("select body from topicx where course_id=? and key=?", (cid, key), one=True)
    if row and not request.args.get("fresh"): body = row["body"]
    else:
        try:
            body = ai(f"""Teach this to a student preparing for '{c['title']}' ({c['exam']}): "{t}".
Source material from the student (may be empty): {(c['src'] or '')[:5000]}
{mind(c['user_id'], cid)}
If it is a question, write the complete model answer the student can learn and write in the exam. If it is a topic, teach it from zero.
{NOTE_STYLE}
{STYLE}
{MATH_STYLE}""", 3000)
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
    users = q("""select id,name,email,phone,pay_status,access,suspended,last_seen,
                 coalesce(last_seen > localtimestamp - interval '10 minutes', false) as online from users order by last_seen desc nulls last, id desc""")
    ps = q("""select p.id,p.status,p.created,p.decided,(p.img is not null) as has_img,u.name,u.email,u.phone
              from payments p join users u on u.id=p.user_id order by p.id desc limit 100""")
    return render_template("admin.html", cfg=pay_cfg(), ps=ps, users=users, st=st, money=paid_n * PRICE, paid_n=paid_n, pend_n=pend_n, price=PRICE)

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
           "grant": "pay_status='approved', access='free', suspended=false", "revoke": "pay_status='none', access=''"}.get(act)
    if sql:
        q("update users set " + sql + " where id=?", (uid,))
        flash({"suspend": "User suspended.", "unsuspend": "User un-suspended.", "grant": "Free access given.", "revoke": "Access removed."}[act], "ok")
    return redirect(url_for("admin") + "#users")

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
