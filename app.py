"""dropfinder web - automatic expired-domain finder.

The home page shows domains found by the scheduled auto-hunt (GitHub Actions writes data/candidates.json;
this app reads it from DATA_URL). "Hunt now" starts an on-demand automatic hunt, no input needed.
"Custom hunt" lets you add your own seed pages.

Run locally:   pip install -r requirements.txt && python app.py      (http://localhost:8000)
Production:    gunicorn app:app --workers 1 --threads 8 --timeout 120   (ONE worker: jobs live in memory)
"""
import io
import os
import threading
import time
import uuid
from urllib.parse import urlparse

import requests
from flask import Flask, Response, abort, jsonify, redirect, render_template_string, request, url_for

import dropfinder as df


def env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


DATA_URL = os.environ.get("DATA_URL",
                          "https://raw.githubusercontent.com/tj5773104-sudo/dropfinder/main/data/candidates.json")
LOCAL_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "candidates.json")
MAX_SEEDS = env_int("MAX_SEEDS", 10)
MAX_PAGES = env_int("MAX_PAGES_PER_SEED", 20)
MAX_DOMAINS = env_int("MAX_DOMAINS", 600)
AUTO_SITES = env_int("AUTO_SITES", 40)
MAX_RUNNING = env_int("MAX_RUNNING_JOBS", 1)
JOBS_PER_HOUR = env_int("JOBS_PER_IP_PER_HOUR", 4)
JOB_TTL = env_int("JOB_TTL_SECONDS", 6 * 3600)

app = Flask(__name__)
JOBS, IP_LOG, LOCK = {}, {}, threading.Lock()
RUNNING = threading.Semaphore(MAX_RUNNING)
FEED = {"at": 0, "data": None}


# --------------------------------------------------------------------------- feed of auto-hunt results
def load_feed():
    with LOCK:
        if FEED["data"] is not None and time.time() - FEED["at"] < 300:
            return FEED["data"]
    data = None
    try:
        r = requests.get(DATA_URL, timeout=20, headers={"User-Agent": df.UA})
        if r.status_code == 200:
            data = r.json()
    except (requests.RequestException, ValueError):
        pass
    if data is None:
        data = df.load_data(LOCAL_DATA)
    rows = [r for r in data.get("domains", {}).values()
            if r.get("status") != "TAKEN" and not df.RESTRICTED_RE.search(r.get("domain", ""))]
    rows.sort(key=lambda r: (-(r.get("quality") or 0), df.PRIORITY.get(r.get("status"), 9), -r.get("score", 0)))
    for r in rows:  # always use this deployment's affiliate code
        r["buy_url"] = df.buy_url(r["domain"])
        srcs = r.get("sources") or []
        for s in srcs:  # rows saved before link sources were tracked: work the source out
            if not s.get("via"):
                s["via"] = df.infer_via(s.get("domain") or df.registrable(urlparse(s.get("url", "")).hostname or ""))
        if not r.get("via"):
            r["via"] = ",".join(sorted({s["via"] for s in srcs if s.get("via")}))
        if r.get("quality") is None:
            r["quality"], r["quality_reasons"] = df.quality_score(r)
        auth = r.get("opr") if r.get("opr") is not None else r.get("cc_authority")
        r["authority"] = auth if auth not in ("", None) else None
    feed = {"updated": data.get("updated", ""), "runs": data.get("runs", [])[:10], "rows": rows}
    with LOCK:
        FEED.update(at=time.time(), data=feed)
    return feed


# --------------------------------------------------------------------------- jobs
def client_ip():
    fwd = request.headers.get("X-Forwarded-For", "")
    return fwd.split(",")[0].strip() if fwd else (request.remote_addr or "?")


def rate_ok(ip):
    now = time.time()
    with LOCK:
        hits = [t for t in IP_LOG.get(ip, []) if now - t < 3600]
        ok = len(hits) < JOBS_PER_HOUR
        if ok:
            hits.append(now)
        IP_LOG[ip] = hits
        return ok


def cleanup():
    now = time.time()
    with LOCK:
        for jid in [j for j, v in JOBS.items() if v["state"] in ("done", "error") and now - v["ended"] > JOB_TTL]:
            del JOBS[jid]


def run_job(jid):
    job = JOBS[jid]
    o = job["opts"]

    def log(msg):
        job["log"].append(f"{time.strftime('%H:%M:%S')}  {msg}")
        del job["log"][:-200]

    log("Waiting for a free slot..." if RUNNING._value == 0 else "Starting...")
    with RUNNING:
        job["state"] = "running"
        try:
            df.AUTH.load(log)
            ranked = df.sample_ranked_sites(o["auto_sites"]) if o["auto_sites"] and df.AUTH.rank else []
            cats = (df.random_dead_link_categories(o["wiki_months"]) if o["wiki_months"] else []) + o["wiki"]
            records = df.harvest(o["seeds"], o["depth"], o["max_pages"], delay=1.0, workers=12,
                                 wiki_categories=cats, wiki_limit=80, github_query=o["github"], github_limit=15,
                                 public_only=True, sitemaps=o["sitemaps"], ranked_sites=ranked,
                                 deadline=time.time() + 6 * 60, log=log)
            job["links"] = len(records)
            job["dofollow_links"] = sum(r["follow"] == "dofollow" for r in records)
            job["unique_domains"] = len({r["target_domain"] for r in records})
            log(f"{job['links']} links ({job['dofollow_links']} dofollow) to {job['unique_domains']} domains.")
            job["rows"] = df.check(records, dofollow_only=o["dofollow_only"], min_sources=o["min_sources"],
                                   tlds=o["tlds"], expiring_days=45, workers=24, wayback=True, wayback_top=60,
                                   max_domains=MAX_DOMAINS, site_check_top=200, verify=True,
                                   opr_key=os.environ.get("OPR_API_KEY"), public_only=True, log=log)
            log(f"Done: {len(job['rows'])} candidate domains.")
            job["state"] = "done"
        except Exception as e:  # noqa: BLE001 - show any failure to the user
            log(f"Error: {type(e).__name__}: {e}")
            job["state"] = "error"
        job["ended"] = time.time()


def start_job(opts):
    jid = uuid.uuid4().hex[:12]
    JOBS[jid] = dict(id=jid, state="queued", opts=opts, log=[], rows=[], created=time.time(), ended=0,
                     links=0, dofollow_links=0, unique_domains=0)
    threading.Thread(target=run_job, args=(jid,), daemon=True).start()
    return jid


def base_opts():
    return dict(seeds=[], wiki=[], github=None, depth=1, max_pages=10, sitemaps=True, auto_sites=0, wiki_months=0,
                min_sources=1, tlds=None, dofollow_only=False)


def parse_form(f):
    errors, seeds = [], []
    for line in f.get("seeds", "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if not line.startswith(("http://", "https://")):
            line = "https://" + line
        u = urlparse(line)
        if not u.hostname or "." not in u.hostname:
            errors.append(f"Not a valid URL: {line}")
            continue
        seeds.append(line)
    if len(seeds) > MAX_SEEDS:
        errors.append(f"At most {MAX_SEEDS} seed pages per run (you gave {len(seeds)}).")
    wiki = [c.strip() for c in f.get("wiki", "").splitlines() if c.strip()][:3]
    wiki = [c if c.lower().startswith("category:") else "Category:" + c for c in wiki]

    def num(name, lo, hi, default):
        try:
            return max(lo, min(hi, int(f.get(name, default))))
        except ValueError:
            return default

    opts = base_opts()
    opts.update(seeds=seeds, wiki=wiki, github=f.get("github", "").strip()[:100] or None,
                max_pages=num("max_pages", 1, MAX_PAGES, 10), sitemaps=bool(f.get("sitemaps")),
                auto_sites=num("auto_sites", 0, AUTO_SITES, 0), wiki_months=num("wiki_months", 0, 2, 0),
                min_sources=num("min_sources", 1, 10, 1),
                tlds=[t for t in f.get("tlds", "").replace(" ", "").split(",") if t] or None,
                dofollow_only=bool(f.get("dofollow_only")))
    if not (opts["seeds"] or opts["wiki"] or opts["github"] or opts["auto_sites"] or opts["wiki_months"]):
        errors.append("Add at least one source, or use the one-click automatic hunt.")
    return opts, errors


# --------------------------------------------------------------------------- routes
@app.get("/")
def index():
    return render_template_string(PAGE, view="feed", limits=limits())


@app.get("/feed.json")
def feed_json():
    f = load_feed()
    return jsonify(updated=f["updated"], runs=f["runs"],
                   rows=[{k: v for k, v in r.items() if k != "more_sources"} for r in f["rows"]])


@app.get("/feed.csv")
def feed_csv():
    buf = io.StringIO()
    df.write_csv(load_feed()["rows"], buf)
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": 'attachment; filename="dropfinder-latest.csv"'})


@app.post("/hunt/auto")
def hunt_auto():
    cleanup()
    if not rate_ok(client_ip()):
        return render_template_string(PAGE, view="custom", errors=[f"Limit reached: {JOBS_PER_HOUR} hunts per hour."],
                                      form={}, limits=limits()), 429
    opts = base_opts()
    opts.update(auto_sites=AUTO_SITES, wiki_months=1)
    return redirect(url_for("job_page", jid=start_job(opts)))


@app.get("/custom")
def custom():
    return render_template_string(PAGE, view="custom", errors=[], form={}, limits=limits())


@app.post("/run")
def run():
    cleanup()
    opts, errors = parse_form(request.form)
    if not errors and not rate_ok(client_ip()):
        errors.append(f"Limit reached: {JOBS_PER_HOUR} hunts per hour. Try again later.")
    if errors:
        return render_template_string(PAGE, view="custom", errors=errors, form=request.form, limits=limits()), 400
    return redirect(url_for("job_page", jid=start_job(opts)))


def get_job(jid):
    job = JOBS.get(jid)
    if not job:
        abort(404)
    return job


@app.get("/job/<jid>")
def job_page(jid):
    return render_template_string(PAGE, view="job", job=get_job(jid), limits=limits())


@app.get("/job/<jid>.json")
def job_json(jid):
    j = get_job(jid)
    return jsonify(state=j["state"], log=j["log"][-40:], links=j["links"], dofollow_links=j["dofollow_links"],
                   unique_domains=j["unique_domains"],
                   rows=[{k: v for k, v in r.items() if k != "more_sources"} for r in j["rows"]]
                   if j["state"] == "done" else [])


@app.get("/job/<jid>.csv")
def job_csv(jid):
    buf = io.StringIO()
    df.write_csv(get_job(jid)["rows"], buf)
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f'attachment; filename="dropfinder-{jid}.csv"'})


@app.get("/healthz")
def healthz():
    return "ok"


def limits():
    return dict(seeds=MAX_SEEDS, pages=MAX_PAGES, domains=MAX_DOMAINS, per_hour=JOBS_PER_HOUR, auto=AUTO_SITES)


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>dropfinder - expired domains with authority backlinks</title>
<link href="https://fonts.googleapis.com/css2?family=Libre+Franklin:wght@400;600;800&display=swap" rel="stylesheet">
<style>
:root{color-scheme:light dark;--bg:light-dark(#fff8f5,#1c1512);--card:light-dark(#fff,#261d19);--ink:light-dark(#1a1817,#f3ebe6);
--mute:light-dark(#6b625d,#b3a69e);--line:light-dark(#ecd9d1,#3a2d27);--teal:light-dark(#0d7680,#4fb3bc);--claret:light-dark(#990f3d,#ff8fb1)}
*{box-sizing:border-box;min-width:0}
body{margin:0;background:var(--bg);color:var(--ink);font:16px/1.55 "Libre Franklin",system-ui,sans-serif}
.wrap{max-width:1280px;margin:auto;padding:0 max(4vw,16px)}
nav{display:flex;gap:18px;align-items:center;flex-wrap:wrap;padding:16px 0;border-bottom:1px solid var(--line)}
nav b{font-weight:800;font-size:20px;margin-right:auto}nav a{color:var(--ink);text-decoration:none;font-weight:600}
nav a.on{color:var(--teal)}
h1{font-weight:800;font-size:clamp(28px,4.5vw,44px);line-height:1.05;margin:24px 0 8px;text-wrap:balance}
h1 span{color:var(--teal)}
.lede{color:var(--mute);max-width:70ch;margin:0 0 18px}
.stats{display:flex;flex-wrap:wrap;gap:12px;margin:8px 0 18px}
.stat{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 16px;min-width:140px}
.stat b{display:block;font-size:28px;font-weight:800;line-height:1.1}.stat span{font-size:13px;color:var(--mute)}
.bar{display:flex;flex-wrap:wrap;gap:10px;align-items:center;background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px;margin-bottom:12px}
.bar input[type=text],.bar input[type=number]{font:16px inherit;padding:8px 10px;border:1px solid var(--line);border-radius:8px;background:var(--bg);color:var(--ink);min-height:42px}
.bar select{font:16px inherit;padding:8px 10px;border:1px solid var(--line);border-radius:8px;background:var(--bg);color:var(--ink);min-height:42px}
.bar input[type=number]{width:80px}
.via{display:inline-block;font-size:11px;font-weight:600;padding:1px 7px;border-radius:99px;border:1px solid var(--teal);color:var(--teal);margin:0 4px 4px 0;white-space:nowrap}.bar label{display:flex;gap:6px;align-items:center;font-size:14px}
.chips{display:flex;flex-wrap:wrap;gap:6px}
.chip{border:1px solid var(--line);background:var(--bg);color:var(--ink);border-radius:99px;padding:6px 12px;font:600 13px inherit;cursor:pointer;min-height:36px}
.chip[aria-pressed=true]{background:var(--teal);border-color:var(--teal);color:#fff}
button,.btn{display:inline-flex;align-items:center;gap:6px;background:var(--teal);color:#fff;border:0;border-radius:8px;padding:11px 20px;font:600 16px inherit;cursor:pointer;text-decoration:none;min-height:44px;white-space:nowrap}
.btn.small{padding:6px 12px;font-size:13px;min-height:32px;border-radius:6px}
.btn.ghost{background:transparent;color:var(--teal);border:1px solid var(--teal)}
:focus-visible{outline:3px solid var(--claret);outline-offset:2px}
.hero{display:grid;grid-template-columns:minmax(0,1.6fr) minmax(0,1fr);gap:20px;align-items:end}
@media(max-width:760px){.hero{grid-template-columns:minmax(0,1fr)}}
.huntbox{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:16px}
.huntbox p{margin:6px 0 0;font-size:13px;color:var(--mute)}
.scroll{overflow-x:auto;border:1px solid var(--line);border-radius:10px;background:var(--card)}
table{border-collapse:collapse;width:100%;font-size:14px}
th,td{padding:9px 10px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}
th{font-weight:600;white-space:nowrap;background:var(--card);cursor:pointer;user-select:none}
td.n{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}
td a{color:var(--teal);overflow-wrap:anywhere}
.tag{display:inline-block;font-size:12px;font-weight:600;padding:2px 8px;border-radius:99px;white-space:nowrap;background:var(--line)}
.AVAILABLE,.LIKELY_AVAILABLE{background:#0d768026;color:var(--teal)}
.PENDING_DELETE,.REDEMPTION,.EXPIRED_GRACE{background:#990f3d1f;color:var(--claret)}
.flag{font-size:12px;color:var(--claret)}
.count{margin:0 0 8px;font-size:14px;color:var(--mute)}.count b{color:var(--ink)}
.q{display:inline-block;min-width:38px;text-align:center;font-weight:800;border-radius:6px;padding:2px 6px}
.qhi{background:#0d768026;color:var(--teal)}.qmid{background:var(--line)}.qlo{color:var(--mute)}
details summary{cursor:pointer;color:var(--mute);font-size:13px}
details ul{margin:6px 0 0;padding-left:18px;font-size:13px}
pre.log{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px;max-height:220px;overflow:auto;font-size:13px;white-space:pre-wrap}
form.custom{display:grid;gap:18px;grid-template-columns:minmax(0,1.3fr) minmax(0,1fr);background:var(--card);border:1px solid var(--line);border-radius:12px;padding:22px}
@media(max-width:760px){form.custom{grid-template-columns:minmax(0,1fr)}}
form.custom label.t{display:block;font-weight:600;margin:12px 0 4px}
form.custom textarea,form.custom input[type=text],form.custom input[type=number]{width:100%;font:16px/1.4 inherit;padding:10px 12px;border:1px solid var(--line);border-radius:8px;background:var(--bg);color:var(--ink)}
form.custom textarea{min-height:140px;font-family:ui-monospace,monospace;font-size:14px}
.check{display:flex;gap:10px;align-items:flex-start;margin:10px 0}.check input{width:20px;height:20px;accent-color:var(--teal)}
.hint{font-size:13px;color:var(--mute);margin:4px 0 0}
.errors{background:#990f3d14;border-left:4px solid var(--claret);padding:10px 14px;margin:14px 0;border-radius:6px}
.spin{display:inline-block;width:14px;height:14px;border:2px solid var(--line);border-top-color:var(--teal);border-radius:50%;animation:s 1s linear infinite;vertical-align:-2px;margin-right:6px}
@keyframes s{to{transform:rotate(360deg)}}@media(prefers-reduced-motion:reduce){.spin{animation:none}}
.empty{padding:28px;text-align:center;color:var(--mute)}
footer{color:var(--mute);font-size:13px;padding:24px 0 40px}
</style></head><body><div class="wrap">
<nav><b>dropfinder</b><a href="{{ url_for('index') }}" class="{{ 'on' if view=='feed' }}">Latest finds</a>
<a href="{{ url_for('custom') }}" class="{{ 'on' if view=='custom' }}">Custom hunt</a></nav>

{% if view == "feed" %}
<div class="hero">
  <div><h1>Expired domains that still have <span>authority backlinks</span></h1>
  <p class="lede">Found automatically, authority first. Every few hours dropfinder takes domains that already have strong
  backlink profiles - the StableDomains quality drop feed, the Majestic Million and the top of Common Crawl's web graph -
  finds the ones that are free or dropping, checks the Wayback Machine for a real site history, and scores each one 0-100.
  Link discovery on Hacker News, Debian, EU research projects and ranked sites adds more.</p></div>
  <form class="huntbox" method="post" action="{{ url_for('hunt_auto') }}">
    <button type="submit">Hunt now</button>
    <p>Runs a fresh automatic hunt on {{ limits.auto }} random ranked sites (about 5-8 minutes). No input needed.</p>
  </form>
</div>
<div class="stats">
  <div class="stat"><b id="s-avail">-</b><span>available now</span></div>
  <div class="stat"><b id="s-drop">-</b><span>dropping / expired</span></div>
  <div class="stat"><b id="s-q60">-</b><span>quality 60+</span></div>
  <div class="stat"><b id="s-total">-</b><span>tracked domains</span></div>
  <div class="stat"><b id="s-upd">-</b><span>last auto-hunt</span></div>
</div>
<div class="bar">
  <div class="chips" id="chips"></div>
  <input type="text" id="q" placeholder="Search domain" aria-label="Search domain">
  <input type="text" id="tld" placeholder="TLDs e.g. com,org" aria-label="TLDs">
  <select id="via" aria-label="Link source"><option value="">All sources</option><option value="stabledomains">Drop feed</option>
    <option value="majestic">Majestic Million</option><option value="commoncrawl">Top web graph</option><option value="hackernews">Hacker News</option>
    <option value="debian">Debian packages</option><option value="cordis">EU research projects</option>
    <option value="crawl">Ranked sites</option><option value="wikipedia">Wikipedia</option></select>
  <label>Min quality <input type="number" id="minscore" min="0" max="100" step="5" value="0"></label>
  <label><input type="checkbox" id="dfonly"> Dofollow only</label>
  <label><input type="checkbox" id="nospam" checked> Hide spammy</label>
  <a class="btn small ghost" href="{{ url_for('feed_csv') }}">CSV</a>
</div>
<p class="count" id="count" aria-live="polite"></p>
<div class="scroll"><table><thead><tr id="thead"></tr></thead><tbody id="rows"><tr><td class="empty" colspan="10"><span class="spin"></span>Loading finds...</td></tr></tbody></table></div>
<p id="more" hidden><button class="btn ghost" id="morebtn" type="button">Show more</button></p>

{% elif view == "custom" %}
<h1>Custom hunt</h1>
<p class="lede">Optional extras. Everything here is automatic too: leave the seed box empty and dropfinder finds its own pages.</p>
{% if errors %}<div class="errors">{% for e in errors %}<div>{{ e }}</div>{% endfor %}</div>{% endif %}
<form class="custom" method="post" action="{{ url_for('run') }}">
  <div>
    <label class="t" for="auto_sites">Random ranked sites to scan</label>
    <input id="auto_sites" name="auto_sites" type="number" min="0" max="{{ limits.auto }}" value="{{ form.get('auto_sites', limits.auto) }}">
    <p class="hint">Picked from the Tranco top-sites list; their oldest pages are read via sitemaps.</p>
    <label class="t" for="wiki_months">Random Wikipedia dead-link months</label>
    <input id="wiki_months" name="wiki_months" type="number" min="0" max="2" value="{{ form.get('wiki_months', 1) }}">
    <label class="t" for="seeds">Your own seed pages (optional, one per line)</label>
    <textarea id="seeds" name="seeds" placeholder="https://example.edu/resources/links">{{ form.get('seeds','') }}</textarea>
    <p class="hint">Up to {{ limits.seeds }}. robots.txt is obeyed, one request per second per site.</p>
    <label class="check"><input type="checkbox" name="sitemaps" {{ 'checked' if form.get('sitemaps', 'on') }}> Read each seed site's oldest sitemap pages (finds far more dead links)</label>
  </div>
  <div>
    <label class="t" for="wiki">Specific Wikipedia category (optional)</label>
    <input id="wiki" name="wiki" type="text" value="{{ form.get('wiki','') }}" placeholder="Articles with dead external links from June 2014">
    <label class="t" for="github">GitHub repo search (optional)</label>
    <input id="github" name="github" type="text" value="{{ form.get('github','') }}" placeholder="awesome list">
    <label class="t" for="tlds">Only these TLDs</label>
    <input id="tlds" name="tlds" type="text" value="{{ form.get('tlds','') }}" placeholder="com,net,org,io">
    <label class="t" for="min_sources">Min. linking sites</label>
    <input id="min_sources" name="min_sources" type="number" min="1" max="10" value="{{ form.get('min_sources', 1) }}">
    <label class="check"><input type="checkbox" name="dofollow_only" {{ 'checked' if form.get('dofollow_only') }}> Dofollow links only</label>
    <p style="margin-top:18px"><button type="submit">Start hunt</button></p>
    <p class="hint">{{ limits.per_hour }} hunts per hour per visitor. Up to {{ limits.domains }} domains checked per hunt.</p>
  </div>
</form>

{% else %}
<h1>Hunting...</h1>
<p id="status"><span class="spin"></span>Working. This usually takes 3-8 minutes; keep the page open or bookmark it.</p>
<div class="stats">
  <div class="stat"><b id="links">-</b><span>outbound links</span></div>
  <div class="stat"><b id="dofollow">-</b><span>dofollow links</span></div>
  <div class="stat"><b id="domains">-</b><span>linked domains</span></div>
  <div class="stat"><b id="found">-</b><span>candidates</span></div>
</div>
<pre class="log" id="log"></pre>
<p id="actions" hidden><a class="btn ghost" href="{{ url_for('job_csv', jid=job.id) }}">Download CSV</a></p>
<div class="scroll" id="tablewrap" hidden><table><thead><tr id="thead"></tr></thead><tbody id="rows"></tbody></table></div>
{% endif %}
<footer>Status comes from public DNS, RDAP, the Wayback Machine and StableDomains and can change quickly; confirm at checkout.
Authority = Tranco top-sites rank of the linking site. "Dofollow" is read from page HTML: a strong filter, not a guarantee.
Register links go to StableDomains (affiliate).</footer>
</div>
<script>
{
  const esc = s => String(s ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
  const $ = x => document.getElementById(x);
  const safeUrl = u => /^https?:\/\//i.test(String(u || ""));
  const link = (u, t) => safeUrl(u) ? `<a href="${esc(u)}" rel="nofollow noopener" target="_blank">${esc(t ?? u)}</a>` : esc(t ?? u);
  const NICE = {AVAILABLE:"available",LIKELY_AVAILABLE:"likely available",PENDING_DELETE:"pending delete",REDEMPTION:"redemption",
    EXPIRED_GRACE:"expired (grace)",EXPIRING_SOON:"expiring soon",PARKED:"parked",DEAD_SITE:"dead site",REGISTERED:"registered"};
  const ORDER = {AVAILABLE:0,PENDING_DELETE:1,REDEMPTION:2,EXPIRED_GRACE:3,LIKELY_AVAILABLE:4,EXPIRING_SOON:5,PARKED:6,DEAD_SITE:7};
  const BUY = new Set(["AVAILABLE","LIKELY_AVAILABLE"]);
  const VIA = {hackernews:"Hacker News", debian:"Debian", cordis:"EU project", crawl:"ranked site", wikipedia:"Wikipedia",
    stabledomains:"Drop feed", majestic:"Majestic Million", commoncrawl:"Top web graph"};
  const qClass = q => q >= 60 ? "qhi" : q >= 35 ? "qmid" : "qlo";
  const viaTags = v => (v || "").split(",").filter(Boolean).map(x => `<span class="via">${esc(VIA[x] || x)}</span>`).join("");
  const COLS = [["domain","Domain"],["status","Status"],["quality","Quality"],["ref_subnets","Ref. networks"],
    ["authority","Authority"],["real_years","Real site (yrs)"],["dofollow_sources","Dofollow sites"],["expiry","Drops / expiry"],["sources","Source"],["buy",""]];
  function rowHtml(r) {
    const src = r.sources || [];
    const items = src.map(s => `<li>${link(s.url)} <span class="tag">${esc(s.follow)}</span>${s.rank ? ` <span class="tag">#${esc(Number(s.rank).toLocaleString())}</span>` : ""}${s.anchor ? ` "${esc(s.anchor)}"` : ""}</li>`).join("");
    const first = src[0] ? src[0].url : r.best_source_url;
    const flags = r.flags ? `<div class="flag">${esc(r.flags)}</div>` : "";
    const price = r.sd_price ? ` $${esc(r.sd_price)}` : "";
    const buy = safeUrl(r.buy_url) ? esc(r.buy_url) : "#";
    const act = BUY.has(r.status) ? `<a class="btn small" href="${buy}" target="_blank" rel="noopener">Register${price}</a>`
      : `<a class="btn small ghost" href="${buy}" target="_blank" rel="noopener">Check</a>`;
    const why = (r.quality_reasons || []).map(x => `<li>${esc(x)}</li>`).join("");
    const realY = r.real_years != null && r.real_years !== "" ? r.real_years : "";
    return `<tr><td><b>${esc(r.domain)}</b>${flags}${why ? `<details><summary>why</summary><ul>${why}</ul></details>` : ""}</td>
      <td><span class="tag ${esc(r.status)}">${esc(NICE[r.status] || r.status)}</span></td>
      <td class="n"><span class="q ${qClass(r.quality || 0)}">${r.quality != null ? esc(r.quality) : ""}</span></td>
      <td class="n">${r.ref_subnets ? esc(Number(r.ref_subnets).toLocaleString()) : ""}</td>
      <td class="n">${r.authority != null ? esc(r.authority) : ""}</td>
      <td class="n">${esc(realY)}</td>
      <td class="n">${r.dofollow_sources ? esc(r.dofollow_sources) : ""}</td>
      <td>${esc((r.expiry || "").slice(0, 10))}</td>
      <td>${viaTags(r.via)}${link(first)}${src.length > 1 ? `<details><summary>${src.length} linking pages</summary><ul>${items}</ul></details>` : ""}</td>
      <td>${act}</td></tr>`;
  }
  function head() { $("thead").innerHTML = COLS.map(([k, t]) => `<th data-k="${k}">${t}</th>`).join(""); }

  {% if view == "feed" %}
  let all = [], shown = 200, sortKey = null, sortDir = 1;
  const status = new Set();
  const GROUPS = {"Available":["AVAILABLE","LIKELY_AVAILABLE"],"Dropping":["PENDING_DELETE","REDEMPTION","EXPIRED_GRACE"],
    "Expiring":["EXPIRING_SOON"],"Parked / dead":["PARKED","DEAD_SITE"]};
  const DEFAULT_ON = ["Available", "Dropping", "Expiring"];
  $("chips").innerHTML = Object.keys(GROUPS).map(g => `<button type="button" class="chip" aria-pressed="${DEFAULT_ON.includes(g)}" data-g="${g}">${g}</button>`).join("");
  DEFAULT_ON.forEach(g => GROUPS[g].forEach(s => status.add(s)));
  function render() {
    const q = $("q").value.trim().toLowerCase();
    const tl = $("tld").value.toLowerCase().split(",").map(s => s.trim().replace(/^\./, "")).filter(Boolean);
    const ms = parseFloat($("minscore").value) || 0;
    let rows = all.filter(r => (!status.size || status.has(r.status)) && (!q || r.domain.includes(q))
      && (!tl.length || tl.includes(r.domain.split(".").slice(1).join(".")) || tl.includes(r.domain.split(".").pop()))
      && (r.quality || 0) >= ms && (!$("dfonly").checked || r.dofollow_sources > 0)
      && (!$("nospam").checked || !(r.flags || "").includes("spammy"))
      && (!$("via").value || (r.via || "").split(",").includes($("via").value)));
    if (sortKey) rows = rows.slice().sort((a, b) => {
      let x = a[sortKey] ?? "", y = b[sortKey] ?? "";
      if (sortKey === "status") { x = ORDER[x] ?? 9; y = ORDER[y] ?? 9; }
      if (x === "" && y !== "") return 1;
      if (y === "" && x !== "") return -1;
      return (x > y ? 1 : x < y ? -1 : 0) * sortDir; });
    $("rows").innerHTML = rows.length ? rows.slice(0, shown).map(rowHtml).join("")
      : `<tr><td class="empty" colspan="10">${all.length ? "Nothing matches these filters." : "No finds yet - the first automatic hunt runs within a few hours. Press Hunt now to run one."}</td></tr>`;
    $("more").hidden = rows.length <= shown;
    $("count").innerHTML = `Showing <b>${Math.min(rows.length, shown)}</b> of <b>${rows.length}</b> matching finds (${all.length} tracked)`;
  }
  $("chips").addEventListener("click", e => { const b = e.target.closest(".chip"); if (!b) return;
    const on = b.getAttribute("aria-pressed") !== "true"; b.setAttribute("aria-pressed", String(on));
    GROUPS[b.dataset.g].forEach(s => on ? status.add(s) : status.delete(s)); shown = 200; render(); });
  ["q", "tld", "minscore", "dfonly", "nospam", "via"].forEach(id => ["input", "change"].forEach(ev =>
    $(id).addEventListener(ev, () => { shown = 200; render(); })));
  head();
  $("thead").addEventListener("click", e => { const k = e.target.dataset.k; if (!k || k === "sources" || k === "buy") return;
    sortDir = sortKey === k ? -sortDir : (["domain", "status", "expiry", "best_source_rank", "cc_rank"].includes(k) ? 1 : -1); sortKey = k; render(); });
  $("morebtn").addEventListener("click", () => { shown += 200; render(); });
  fetch("{{ url_for('feed_json') }}").then(r => r.json()).then(d => {
    all = d.rows || [];
    $("s-avail").textContent = all.filter(r => BUY.has(r.status)).length;
    $("s-drop").textContent = all.filter(r => ["PENDING_DELETE", "REDEMPTION", "EXPIRED_GRACE", "EXPIRING_SOON"].includes(r.status)).length;
    $("s-total").textContent = all.length;
    $("s-q60").textContent = all.filter(r => (r.quality || 0) >= 60).length;
    $("s-upd").textContent = d.updated ? new Date(d.updated).toLocaleString([], {dateStyle: "short", timeStyle: "short"}) : "pending";
    render();
  }).catch(() => { $("rows").innerHTML = `<tr><td class="empty" colspan="10">Couldn't load finds. Refresh to try again.</td></tr>`; });
  {% elif view == "job" %}
  const id = {{ job.id|tojson }};
  head();
  async function poll() {
    let j;
    try { j = await (await fetch(`/job/${id}.json`)).json(); } catch (e) { return setTimeout(poll, 4000); }
    $("log").textContent = j.log.join("\n"); $("log").scrollTop = $("log").scrollHeight;
    if (j.links) { $("links").textContent = j.links; $("dofollow").textContent = j.dofollow_links; $("domains").textContent = j.unique_domains; }
    if (j.state === "done") {
      $("status").textContent = j.rows.length ? "Done. Best candidates first." : "Done. Nothing free or dropping this time - press Hunt now again for a new random sample.";
      $("found").textContent = j.rows.length; $("actions").hidden = false;
      if (j.rows.length) { $("tablewrap").hidden = false; $("rows").innerHTML = j.rows.map(rowHtml).join(""); }
    } else if (j.state === "error") { $("status").textContent = "The hunt failed - see the log."; $("actions").hidden = false; }
    else setTimeout(poll, 2500);
  }
  poll();
  {% endif %}
}
</script>
</body></html>"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), threaded=True)
