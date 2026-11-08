import os, io, json, re, uuid, hmac, base64, mimetypes, functools, requests
from concurrent.futures import ThreadPoolExecutor
import psycopg2, psycopg2.extras, psycopg2.pool
from markupsafe import Markup, escape
from flask import Flask, render_template, request, redirect, url_for, session, flash, abort, Response
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
    return q("select * from users where id=?", (session["uid"],), one=True) if "uid" in session else None
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
        elif re.match(r"(?i)^(given|to find|find|formula|solution|check|verify|proof|aim|note)\s*:", s): cls += " hd"
        elif s[0] in "=≈": cls += " eq"
        out.append('<div class="%s">%s</div>' % (cls, escape(l.rstrip())))
    return Markup("".join(out))
def ans_html(msg):
    """Render a tutor answer. Maths answers (tagged [MATHS]) go on ruled paper, theory stays plain."""
    m = clean(msg or ""); kind = "theory"
    t = re.match(r"^\[(MATHS?|THEORY)\]\s*", m, re.I)
    if t: kind = "maths" if t.group(1).upper().startswith("MATH") else "theory"; m = m[t.end():]
    if kind == "maths": return Markup('<div class="paper">%s</div>' % ink(m))
    return Markup('<div class="plainans">%s</div>' % escape(m))
app.add_template_filter(ink, "ink"); app.add_template_filter(ans_html, "ans")

VISION = os.getenv("VISION_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct")

def ai(prompt, tokens=6000, images=None, clean_out=True):
    k = os.getenv("GROQ_API_KEY")
    if not k: raise RuntimeError("GROQ_API_KEY not set")
    if images:   # handwritten photos -> vision model
        models = [VISION]
        content = [{"type": "text", "text": prompt}] + [{"type": "image_url", "image_url": {"url": u}} for u in images]
    else:
        models = []; content = prompt
        for m in (os.getenv("AI_MODEL"), "openai/gpt-oss-120b", "openai/gpt-oss-20b"):
            if m and m not in models: models.append(m)
    err = ""
    for m in models:
        body = {"model": m, "max_tokens": tokens if images else tokens + 4000, "temperature": 0.3 if images else 0.4,
                "messages": [{"role": "system", "content": "You are a warm, expert tutor and coach. When JSON is requested reply with valid JSON only, no markdown fences."},
                             {"role": "user", "content": content}]}
        if "gpt-oss" in m: body["reasoning_effort"] = "low"
        r = requests.post("https://api.groq.com/openai/v1/chat/completions",
                          headers={"Authorization": "Bearer " + k, "Content-Type": "application/json"}, json=body, timeout=140)
        if r.ok:
            o = r.json()["choices"][0]["message"]["content"]
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
    return render_template("pay.html")

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
        exam = f.get("exam", "Semester Exam") if track == "college" else "Personal Growth"
        days = max(1, min(int(f.get("days") or 7), 120))
        src = "\n".join(text_from(x) for x in request.files.getlist("files")) + "\n" + f.get("topics", "")
        papers = "\n".join(text_from(x) for x in request.files.getlist("papers"))
        title = f.get("title") or u["goal"]
        if track == "personal": src = f"Profession: {u['profession']}. Wants to become: {u['goal']}. Notes: " + src
        if len(src.strip()) < 15:
            flash("Please upload a syllabus/notes file or type topics.", "err"); return redirect(request.url)
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
        flash("Your day-wise plan is ready! Open Day 1 to start learning.", "ok"); return redirect(url_for("course", cid=cid))
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
    return render_template("course.html", c=c, plan=plan, topics=json.loads(c["topics"]), st=stats(cid), done=done, opened=opened, nxt=nxt)

def lesson_for(c, d, n):
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
{MATH_STYLE}

Return JSON: {{"title":"","math":true or false (true if today's topic needs calculations),"intro":"3-5 simple sentences: what we learn today and why it matters","words":[{{"term":"","meaning":"one simple line"}}] (3 to 6 key words),"sections":[{{"h":"","body":"clear step-by-step explanation in simple language","analogy":"everyday comparison","example":"worked example","math":true or false,"mistake":"common beginner mistake and how to avoid it"}}] (4 to 6 sections),"recap":["",""] (4 to 6 simple points),"practice":[{{"q":"a question to try","answer":"full solution written step by step"}}] (3 questions, easy to medium)}}""", 7000)
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
    c = get_course(cid); day_plan(c, n)
    q("delete from lessons where course_id=? and day=?", (cid, n))
    flash("Writing a fresh, simpler explanation for you…", "ok"); return redirect(url_for("day", cid=cid, n=n))

@app.route("/course/<int:cid>/day/<int:n>/ask", methods=["POST"])
@paid
def ask(cid, n):
    c = get_course(cid); d = day_plan(c, n)
    question = request.form.get("question", "").strip()
    imgs, err = images_from(request.files.getlist("note"))
    if err: flash(err, "err"); return redirect(url_for("day", cid=cid, n=n) + "#chat")
    if not question and not imgs:
        flash("Type your doubt or upload a photo of your handwritten note.", "err"); return redirect(url_for("day", cid=cid, n=n) + "#chat")
    L = lesson_for(c, d, n)
    hist = q("select role,msg from chats where course_id=? and day=? order by id desc limit 6", (cid, n))[::-1]
    convo = "\n".join(f"{'Student' if h['role'] == 'user' else 'Tutor'}: {h['msg']}" for h in hist)
    prompt = f"""You are tutoring a student on Day {n} of '{c['title']}'. Today's lesson:
{json.dumps(L)[:5000]}
Earlier chat:
{convo or '(none)'}
{mind(c['user_id'], cid)}
Student's doubt: {question or 'Please read my handwritten note and clarify my doubt.'}
{'The student attached a photo of their handwritten note. Read it carefully, say what you understood, and if a word or line is unclear, ask them to write it more clearly.' if imgs else ''}
Explain like a patient teacher to a beginner: simple words, a small everyday example, tied to today's topic.
{STYLE}
{MATH_STYLE}
Start your reply with [MATHS] on its own first line if the doubt needs solving or calculating, otherwise start with [THEORY]."""
    try: a = ai(prompt, 2500, imgs or None)
    except Exception as e:
        flash(f"AI error ({str(e)[:120]}).", "err"); return redirect(url_for("day", cid=cid, n=n) + "#chat")
    q("insert into chats(course_id,day,role,msg) values(?,?,?,?)", (cid, n, "user", (question or "Please check my handwritten note.") + (" 📎 [handwritten note attached]" if imgs else "")))
    q("insert into chats(course_id,day,role,msg) values(?,?,?,?)", (cid, n, "ai", a))
    return redirect(url_for("day", cid=cid, n=n) + "#chat")

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
                ans = ai(f"""A student has a doubt. Student's doubt: {question or 'Please read my handwritten note and clarify my doubt.'}
{'A photo of the handwritten note is attached; say what you understood, and if anything is unclear ask the student to write it more clearly.' if imgs else ''}
Reference material (may be empty):
{doc[:12000]}
Explain like a patient teacher to a beginner: simple words and a small everyday example.
{STYLE}
{MATH_STYLE}
Start your reply with [MATHS] on its own first line if the doubt needs solving or calculating, otherwise start with [THEORY].""", 2500, imgs or None)
                q("insert into doubts(user_id,question,answer) values(?,?,?)",
                  (u["id"], (question or "Handwritten note") + (" 📎" if imgs else ""), ans))
                return redirect(url_for("doubt") + "#answers")
            except Exception as e: flash(f"AI error ({str(e)[:150]}).", "err")
    hist = q("select question,answer,created from doubts where user_id=? order by id desc limit 15", (u["id"],))
    return render_template("doubt.html", hist=hist, question=question)

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
    ps = q("select p.id,p.status,p.created,u.name,u.email,u.phone from payments p join users u on u.id=p.user_id order by p.id desc")
    return render_template("admin.html", ps=ps)

@app.route("/admin/<int:pid>/<act>", methods=["POST"])
def admin_act(pid, act):
    if not session.get("admin"): abort(403)
    p = q("select * from payments where id=?", (pid,), one=True)
    if p and act in ("approve", "reject"):
        q("update payments set status=? where id=?", (act + "d", pid))
        q("update users set pay_status=? where id=?", ("approved" if act == "approve" else "rejected", p["user_id"]))
        flash(f"Payment {act}d.", "ok")
    return redirect(url_for("admin"))

@app.route("/admin/file/<int:pid>")
def shot(pid):
    if not session.get("admin"): abort(403)
    r = q("select img,mime from payments where id=?", (pid,), one=True)
    if not r or r["img"] is None: abort(404)
    return Response(bytes(r["img"]), mimetype=r["mime"] or "image/jpeg", headers={"Cache-Control": "private, max-age=3600"})

if __name__ == "__main__": app.run(debug=True)
