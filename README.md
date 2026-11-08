# 💗 GrowthPulse

AI study and growth coach. Upload a syllabus (or tell it your career goal), choose how many days you have, and GrowthPulse builds a day-by-day plan with a beginner-friendly lesson for every day, answers your doubts (even from handwritten photos), and tests you with MCQ or written questions.

Works on mobile and laptop.

---

## ✨ Features

| Feature | What it does |
|---|---|
| 🎓 Exam plans | Internal, lab and semester exams built from your syllabus, notes and previous papers |
| 🚀 Personal growth | Skill roadmap from beginner to job-ready, based on your profession and goal |
| 🌐 Web research | Before building a course, the AI searches the web for important topics and frequently asked questions, then uses them in the plan, lessons and tests |
| 📖 Day-wise lessons | Starts from zero: simple words, everyday analogies, worked examples, common mistakes, recap and practice questions with solutions |
| ✍️ Human-style maths | Maths is solved like a teacher writes in a notebook (Given, To find, Formula, Step 1, Step 2, Answer, Check) on ruled paper in a handwriting font |
| 🤔 Doubts | Ask by typing or by uploading a photo of handwritten notes. Answers are saved |
| 📝 Tests | 10-question MCQ test and 5-question written test. Handwritten answers can be uploaded and are checked by AI |
| 📊 Analysis | Topic-wise strengths and weaknesses, mind-set advice, and "retrain plan" around weak topics |
| 💳 Payment unlock | ₹50 one-time payment. User uploads a screenshot and admin approves it |
| 🛠 Admin | Hidden page at `/adminlogin` to approve or reject payments |

---

## 🧱 Tech stack

- **Backend:** Python, Flask, Gunicorn
- **Database:** Supabase (PostgreSQL)
- **AI:** Groq API (`openai/gpt-oss-120b` for text, `llama-4-scout` for handwriting, `groq/compound` for web search)
- **Frontend:** Jinja templates, Tailwind CSS (CDN), custom CSS
- **Hosting:** Render

---

## 📁 Project structure

```
growthpulse/
├── app.py                 # All routes, database layer, AI calls
├── requirements.txt       # Python packages
├── render.yaml            # Render deployment settings
├── .env.example           # Environment variables template
├── static/
│   ├── app.css            # Styles (mobile + laptop, notebook paper)
│   └── qr.svg / qr.png    # Payment QR code (qr.png is used if present)
└── templates/
    ├── base.html          # Layout and navigation
    ├── home.html, auth.html, dashboard.html, profile.html
    ├── new.html           # Create a plan
    ├── course.html        # Day-by-day plan + web research
    ├── day.html           # Lesson + day doubts
    ├── doubt.html         # Doubts page
    ├── test_choose.html, test.html, desc.html     # Tests
    ├── analysis.html, desc_result.html            # Results
    ├── pay.html           # Payment page
    └── admin.html, adminlogin.html                # Admin
```

---

## 🔑 Environment variables

Copy `.env.example` to `.env` for local use. On Render add the same names under **Environment**.

| Variable | Required | Description |
|---|---|---|
| `DATABASE_URL` | ✅ | Supabase **Postgres** connection string (see below) |
| `SECRET_KEY` | ✅ | Any long random text. Render can generate it |
| `GROQ_API_KEY` | ✅ | Your Groq API key |
| `ADMIN_USER` | ✅ | Admin username |
| `ADMIN_PASS` | ✅ | Admin password |
| `AI_MODEL` | optional | Default `openai/gpt-oss-120b` |
| `VISION_MODEL` | optional | Model that reads handwritten photos |
| `SEARCH_MODEL` | optional | Default `groq/compound` (web search) |

### Getting `DATABASE_URL` from Supabase

1. Open your project in Supabase and click the **Connect** button at the top.
2. Choose **Session pooler** (not "Direct connection", which does not work on Render).
3. Copy the string. It looks like:
   ```
   postgresql://postgres.YOUR_PROJECT_REF:YOUR_DB_PASSWORD@aws-0-REGION.pooler.supabase.com:5432/postgres
   ```
4. Replace `YOUR_DB_PASSWORD` with your **database password** (set when you created the project; reset it under *Project Settings → Database* if forgotten).
5. If the password has special characters, URL-encode them: `@` → `%40`, `#` → `%23`, `/` → `%2F`.

> ⚠️ Use the `postgresql://...` string. The `https://xxxx.supabase.co` project URL will not work here.

Tables are created automatically on first start. Nothing to set up manually.

---

## 💻 Run locally

```bash
pip install -r requirements.txt
cp .env.example .env        # on Windows: copy .env.example .env
# fill in the values in .env
python app.py
```

Open http://127.0.0.1:5000

---

## 🚀 Deploy on Render

1. Push the project to GitHub.
2. On Render choose **New → Web Service** (or **Blueprint** to use `render.yaml`) and connect the repo.
3. Build command: `pip install -r requirements.txt`
4. Start command: `gunicorn app:app --timeout 240 --workers 2 --threads 4`
5. Add the environment variables from the table above.
6. Deploy. Check the logs: if you see `DB INIT ERROR`, the `DATABASE_URL` is wrong.

No disk is needed. All data, including payment screenshots, is stored in Supabase.

---

## 🛠 Admin

- Open `/adminlogin` directly (there is no link to it in the app).
- Log in with `ADMIN_USER` and `ADMIN_PASS`.
- Approve or reject payment screenshots. Approved users get all features unlocked.
- To show your own payment QR, add `static/qr.png` (or replace `static/qr.svg`).

---

## 🗄 What is stored in the database

- **Users:** name, email, phone, profession, goal, payment status. Passwords are stored hashed, never as plain text
- **Payments:** screenshots and their status
- **Courses:** plans, topics and the web research
- **Lessons** generated for each day
- **Progress:** completed days
- **Tests:** questions, scores, topic-wise results and coach analysis
- **Doubts and lesson chats**
- **Mood check-ins**

Not stored: the original uploaded syllabus files (only their text is kept) and handwritten photos (sent to the AI to be read, then discarded).

---

## 🩺 Troubleshooting

| Problem | Fix |
|---|---|
| `invalid dsn` / `DATABASE_URL must be the Supabase POSTGRES connection string` | You pasted the `https://...supabase.co` URL. Use the Session pooler `postgresql://...` string |
| `password authentication failed` | Wrong database password, or special characters not URL-encoded |
| Cannot connect from Render | Use the **Session pooler** string, not Direct connection |
| `AI could not build the plan` | Check `GROQ_API_KEY` and your Groq usage limits |
| Plan takes long to build | Normal. It searches the web first, which can take up to about 2 minutes. Keep the page open |
| Old courses have no web research | Research is added only to courses created after the update. Create a new course |
| Lesson not clear | Open the lesson and tap "Explain this lesson again" |
