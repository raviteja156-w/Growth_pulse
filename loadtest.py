"""Load test for GrowthPulse: simulates many students at once (no AI calls, so it costs nothing on Groq).

Run it from your own computer or phone-hotspot laptop against your live site:
    pip install requests
    python loadtest.py --base https://growth-pulse.onrender.com --code YOURCLASSCODE --users 100 --seconds 60

Each virtual student: opens the site, registers (unique test email), logs in, then keeps opening
dashboard / profile / doubts pages. It prints the number of requests, errors and response times.
The test accounts are unpaid, so afterwards use Admin -> Tools -> "Remove unpaid students" to delete them.
Start small (--users 20) first. Note: the Render free plan sleeps when idle, the first request can be slow.
"""
import argparse, threading, time, random, statistics, requests

ap = argparse.ArgumentParser()
ap.add_argument("--base", required=True); ap.add_argument("--code", required=True, help="a real class code")
ap.add_argument("--users", type=int, default=50); ap.add_argument("--seconds", type=int, default=60)
a = ap.parse_args(); base = a.base.rstrip("/")
lock = threading.Lock(); times = []; errors = []; stop = time.time() + a.seconds

def hit(s, method, path, **kw):
    t = time.time()
    try:
        r = s.request(method, base + path, timeout=60, allow_redirects=True, **kw)
        ok = r.status_code < 400
        with lock:
            times.append(time.time() - t)
            if not ok: errors.append(f"{method} {path} -> {r.status_code}")
        return r
    except Exception as e:
        with lock: errors.append(f"{method} {path} -> {type(e).__name__}")

def student(i):
    s = requests.Session(); s.cookies.set("gp_demo", "1")
    email = f"loadtest{int(time.time())}_{i}@example.com"
    hit(s, "GET", "/register")
    hit(s, "POST", "/register", data=dict(name=f"Load Test {i}", email=email, phone="9000000000", class_code=a.code, password="loadtest1", confirm="loadtest1"))
    while time.time() < stop:
        for path in random.sample(["/dashboard", "/profile", "/doubt", "/dashboard"], 3):
            hit(s, "GET", path); time.sleep(random.uniform(0.5, 2.0))

ts = [threading.Thread(target=student, args=(i,)) for i in range(a.users)]
t0 = time.time()
for t in ts: t.start(); time.sleep(0.05)
for t in ts: t.join()
d = time.time() - t0; times.sort()
pct = lambda p: times[min(len(times) - 1, int(len(times) * p))] if times else 0
print(f"\n{a.users} students for {d:.0f}s: {len(times)} requests ({len(times)/d:.1f}/s), {len(errors)} errors")
if times: print(f"response time: median {statistics.median(times):.2f}s | 95% {pct(.95):.2f}s | slowest {times[-1]:.2f}s")
for e in sorted(set(errors))[:10]: print("  error:", e, "x", errors.count(e))
print("PASS: errors < 1% and 95% under 3s" if times and len(errors) < len(times) * .01 and pct(.95) < 3 else "NEEDS WORK: see numbers above")
