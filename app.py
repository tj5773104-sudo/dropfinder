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
from datetime import datetime
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
            if r.get("status") != "TAKEN" and not df.RESTRICTED_RE.search(r.get("domain", ""))
            and df.registrable(r.get("domain", "")) == r.get("domain")]  # never list subdomains
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
        if r.get("est_value") is None:
            r["est_value"] = df.estimate_value(r)
            r["reg_price"] = df.reg_price(r)
        r["value_ratio"] = df.value_ratio(r)
        r["wiki_only"] = df.wiki_only(r)
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
    return Response(TABLE_PAGE, mimetype="text/html")


@app.get("/cards")
def cards():
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
                   rows=[dict({k: v for k, v in r.items() if k != "more_sources"},
                              quality=r.get("quality", df.quality_score(r)[0])) for r in j["rows"]]
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



# --------------------------------------------------------------------------- simple table (home page)
GRAPH_VIA = {"stabledomains", "majestic", "commoncrawl"}


def real_sources(r):
    """Only real web pages that link to the domain (not feeds / rank lists / archive placeholders)."""
    return [s for s in (r.get("sources") or [])
            if s.get("url") and s.get("via") not in GRAPH_VIA and "web.archive.org/web/2*" not in s["url"]]


def table_rows():
    f = load_feed()
    with LOCK:
        cached = FEED.get("table")
        if cached is not None and FEED.get("table_at") == FEED["at"]:
            return cached
    out = []
    for r in f["rows"]:
        src = real_sources(r)
        rank = str(r.get("cc_rank") or "")
        au = df.cc_authority(int(rank)) if rank.isdigit() else 0
        if not src:
            if "commoncrawl" not in (r.get("via") or "") or au < 1:
                continue
            # found by the web-graph sweep: many sites link to it, no single crawled page to show
            src = [{"url": "https://ahrefs.com/backlink-checker/?input=" + r["domain"] + "&mode=subdomains",
                    "anchor": "", "follow": "", "via": "commoncrawl", "graph": True}]
        src.sort(key=lambda s: (s.get("follow") != "dofollow", not s.get("anchor")))
        d = r["domain"]
        out.append({
            "d": d, "tld": d.split(".", 1)[1] if "." in d else "", "st": r.get("status", ""),
            "q": r.get("quality") or 0, "v": r.get("est_value") or 0,
            "y": str(r.get("wb_first") or "")[:4], "n": 0 if src[0].get("graph") else len(src), "au": au or 0,
            "df": sum(1 for s in src if s.get("follow") == "dofollow"),
            "via": src[0].get("via") or "", "found": (r.get("first_seen") or "")[:10],
            "spam": bool(df.SPAM_RE.search(d.split(".")[0])),
            "drop": (r.get("dropped_at") or "")[:10], "was": r.get("prev_status") or "",
            "sp": r.get("spam_hist") or "", "spc": 1 if r.get("spam_checked") else 0,
            "s": [{"u": s["url"], "a": (s.get("anchor") or "")[:120], "f": s.get("follow") or "",
                   "via": s.get("via") or "", **({"g": 1} if s.get("graph") else {})} for s in src[:5]],
        })
    with LOCK:
        FEED.update(table=out, table_at=FEED["at"])
    return out


@app.get("/table.json")
def table_json():
    f = load_feed()
    resp = jsonify(updated=f["updated"], buy=df.buy_url("DOMAIN"), rows=table_rows())
    resp.headers["Cache-Control"] = "public, max-age=120"
    return resp


@app.get("/drops.rss")
def drops_rss():
    """Watched domains that just dropped / became registrable - subscribe in any RSS reader for alerts."""
    from xml.sax.saxutils import escape as x
    rows = sorted((r for r in table_rows() if r["drop"]), key=lambda r: r["drop"], reverse=True)[:100]
    items = "".join(
        f"<item><title>{x(r['d'])} - {x(r['st'].replace('_', ' ').lower())}</title>"
        f"<link>{x(df.buy_url(r['d']))}</link><guid isPermaLink=\"false\">{x(r['d'] + r['drop'])}</guid>"
        f"<description>{x('Was ' + r['was'].replace('_', ' ').lower() + '. Linked from ' + r['s'][0]['u'])}</description>"
        f"<pubDate>{datetime.strptime(r['drop'], '%Y-%m-%d').strftime('%a, %d %b %Y 06:00:00 +0000')}</pubDate></item>"
        for r in rows)
    body = ('<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>DropFinder - just dropped</title>'
            f"<link>{x(request.url_root)}</link><description>Backlinked domains that just dropped</description>{items}"
            "</channel></rss>")
    return Response(body, mimetype="application/rss+xml")


TABLE_PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>DropFinder - expired domains with real backlinks</title>
<style>
:root{--b:#d9dee5;--t:#1b2430;--m:#667085;--a:#1f5fd1;--bg:#fff;--alt:#f6f8fb;--g:#137333;--o:#b35c00}
*{box-sizing:border-box}body{margin:0;font:14px/1.4 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;color:var(--t);background:var(--bg)}
header{display:flex;align-items:baseline;gap:10px;flex-wrap:wrap;padding:10px 12px 4px}
h1{font-size:18px;margin:0}h1 span{color:var(--a)}.sub{color:var(--m);font-size:13px}
.bar{display:flex;flex-wrap:wrap;gap:6px;padding:6px 12px 8px;border-bottom:1px solid var(--b);position:sticky;top:0;background:var(--bg);z-index:3}
.bar input,.bar select{font:inherit;padding:6px 8px;border:1px solid var(--b);border-radius:6px;background:#fff;min-height:34px}
.bar input[type=search]{flex:1 1 180px}.bar label{display:flex;align-items:center;gap:4px;font-size:13px;white-space:nowrap}
.info{display:flex;justify-content:space-between;gap:8px;padding:6px 12px;color:var(--m);font-size:13px;flex-wrap:wrap}
.info a{color:var(--a)}
.wrap{overflow-x:auto;-webkit-overflow-scrolling:touch}
table{border-collapse:collapse;width:100%;min-width:700px}
td.src{max-width:330px}.stc{white-space:nowrap}.stc a.buy{display:block;margin-top:4px;text-align:center}
@media(max-width:900px){.opt{display:none}th,td{padding:6px 6px}td.src{max-width:250px}td.dom{max-width:150px;overflow:hidden;text-overflow:ellipsis}}
th,td{padding:6px 8px;border-bottom:1px solid var(--b);text-align:left;vertical-align:top}
th{background:#eef2f7;font-size:12px;text-transform:uppercase;letter-spacing:.02em;color:#3b4656;cursor:pointer;user-select:none;white-space:nowrap;position:sticky;top:0}
th.ns{cursor:default}th[data-k].on::after{content:" \25BE"}th[data-k].on.asc::after{content:" \25B4"}
tbody tr:nth-child(even){background:var(--alt)}
td.dom{font-weight:600;white-space:nowrap;position:sticky;left:0;background:inherit;z-index:1}
tbody tr td.dom{background:var(--bg)}tbody tr:nth-child(even) td.dom{background:var(--alt)}
td.num{text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums}
.src a{color:var(--a);text-decoration:none;word-break:break-all}.src a:hover{text-decoration:underline}
.anc{color:var(--m);font-size:12px;display:block}.more{border:0;background:none;color:var(--a);padding:0;font:inherit;font-size:12px;cursor:pointer}
.tag{display:inline-block;font-size:11px;padding:1px 6px;border-radius:9px;background:#eef2f7;color:#3b4656;white-space:nowrap}
.df{background:#e6f4ea;color:var(--g)}.warn{display:block;width:max-content;margin-top:3px;background:#fdecea;color:#b42318}
#pop .spw{background:#fdecea;color:#b42318;border-radius:7px;padding:6px 8px;font-size:12px;margin:-4px 0 10px}
#pop .spok{color:var(--g);font-size:12px;margin:-6px 0 10px}.new{display:block;width:max-content;margin-top:3px;background:#fff4e5;color:var(--o)}.nf{background:#f3f4f6;color:var(--m)}
.s-AVAILABLE,.s-LIKELY_AVAILABLE{color:var(--g);font-weight:600}.s-PENDING_DELETE,.s-REDEMPTION,.s-EXPIRED_GRACE,.s-EXPIRING_SOON{color:var(--o);font-weight:600}
.s-DEAD_SITE,.s-PARKED{color:var(--m)}
a.buy{display:inline-block;padding:4px 10px;border-radius:6px;background:var(--a);color:#fff;text-decoration:none;font-size:13px;white-space:nowrap}
.pg{display:flex;gap:6px;justify-content:center;align-items:center;padding:12px}
.pg button{font:inherit;padding:6px 12px;border:1px solid var(--b);border-radius:6px;background:#fff}
.empty{padding:30px;text-align:center;color:var(--m)}
footer{color:var(--m);font-size:12px;padding:8px 12px 20px}
td.dom{max-width:230px;overflow:hidden;text-overflow:ellipsis}
.dn{all:unset;cursor:pointer;display:block;max-width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;
  font-weight:600;color:var(--t);border-bottom:1px dashed #9aa6b6}.dn:focus-visible{outline:2px solid var(--a)}
.mk{font-size:12px;margin-right:3px}tr.m-c td{opacity:.55}tr.m-c td.dom{opacity:1}tr.m-c .dn{color:var(--m)}
#pop{position:fixed;z-index:20;width:360px;max-width:calc(100vw - 16px);max-height:min(560px,calc(100vh - 16px));overflow:auto;
  background:#fff;border:1px solid var(--b);border-radius:10px;box-shadow:0 12px 32px rgba(16,24,40,.18);padding:12px 14px}
#pop[hidden]{display:none}
#pop .ph{display:flex;align-items:flex-start;gap:8px}#pop .pd{font-weight:700;font-size:16px;word-break:break-all;flex:1}
#pop .x{border:0;background:none;font-size:20px;line-height:1;color:var(--m);cursor:pointer;padding:0 2px}
#pop .facts{color:var(--m);font-size:12px;margin:4px 0 10px}#pop .facts b{color:var(--t);font-weight:600}
#pop h4{margin:10px 0 6px;font-size:11px;text-transform:uppercase;letter-spacing:.04em;color:var(--m)}
#pop .qc{display:grid;grid-template-columns:1fr 1fr;gap:6px}
#pop .qc a,#pop .qc button,#pop .marks button{font:inherit;font-size:13px;text-align:center;padding:7px 6px;border:1px solid var(--b);
  border-radius:7px;background:#fff;color:var(--t);text-decoration:none;cursor:pointer}
#pop .qc a:hover,#pop .marks button:hover{background:#f2f5fa}
#pop .marks{display:flex;gap:6px}#pop .marks button{flex:1}#pop .marks button.on{background:#e8effc;border-color:var(--a);color:var(--a);font-weight:600}
#pop .src div{margin-bottom:6px}#pop .src a{word-break:break-all}#pop a.buy{display:block;text-align:center;margin-top:10px;padding:8px}
@media(max-width:900px){td.dom{max-width:135px}td.src{max-width:220px}th{letter-spacing:0}}
@media(max-width:700px){#pop{left:0!important;right:0;bottom:0;top:auto!important;width:auto;max-width:none;border-radius:14px 14px 0 0;max-height:75vh}}
</style></head><body>
<header><h1>Drop<span>Finder</span></h1><span class="sub">Expired &amp; available domains that real web pages still link to. Updated automatically.</span></header>
<div class="bar">
  <input type="search" id="q" placeholder="Search domain, linking page or anchor">
  <select id="st">
    <option value="buy">Can register now</option><option value="new">Just dropped (14 days)</option><option value="drop">Dropping soon</option>
    <option value="all">All statuses</option><option value="dead">Dead site (still registered)</option>
  </select>
  <select id="via"><option value="">All sources</option></select>
  <select id="tld"><option value="">All TLDs</option></select>
  <select id="au"><option value="1">Authority 1+</option><option value="3">Strong only (3+)</option><option value="0">Any authority</option></select>
  <select id="mk"><option value="">All (except hidden)</option><option value="s">&#9733; My shortlist</option><option value="u">Not checked yet</option><option value="c">&#10003; Checked</option><option value="h">Hidden</option></select>
  <label><input type="checkbox" id="dfo"> Dofollow only</label>
  <label><input type="checkbox" id="nospam" checked> Hide spam names</label>
  <label title="Hide domains whose archived pages show casino, pharma, adult or other spam"><input type="checkbox" id="nosph"> Hide spam history</label>
</div>
<div class="info"><span><span id="count">Loading...</span> &middot; tap a domain for quick checks</span><span><span id="upd"></span> &middot; <a href="/feed.csv">CSV</a> &middot; <a href="/drops.rss">Drop alerts (RSS)</a></span></div>
<div class="wrap"><table>
<thead><tr>
  <th data-k="d">Domain</th><th class="ns">Linked from</th><th data-k="n">Links</th>
  <th data-k="au" title="How strongly the whole web links to this domain (Common Crawl web graph), 0-10">Authority</th>
  <th data-k="y">Since</th><th data-k="q">Quality</th><th data-k="v">Value</th>
  <th data-k="st">Status</th><th data-k="found" class="opt">Found</th>
</tr></thead><tbody id="rows"></tbody></table></div>
<div id="pop" hidden role="dialog" aria-label="Domain details"></div>
<div class="pg"><button id="prev">&lsaquo; Prev</button><span id="pno"></span><button id="next">Next &rsaquo;</button></div>
<footer>Est. value is a rough guide from link and history signals, not an appraisal. Authority (0-10) is how strongly the whole web links to the domain, from the Common Crawl web graph. Quality (0-100) weighs real-site history, dofollow links and authority. Always check a domain's history before buying.</footer>
<script>
const VIA={wikipedia:"Wikipedia",cordis:"EU research projects",wikidata:"Closed organisations",crawl:"Top websites",
  hackernews:"Hacker News",debian:"Debian packages",publishers:"News & gov archives",github:"GitHub",seed:"Your seeds",
  commoncrawl:"Web-graph sweep",wikicount:"Wikipedia link counts",wordpress:"WordPress authors",crypto:"Dead crypto projects",slashdot:"Slashdot stories",packages:"Package homepages",wikidata_old:"Former official sites",dmoz:"DMOZ directory",resources:"Gov/edu links pages",press:"Press releases"};
const ST={AVAILABLE:"Available",LIKELY_AVAILABLE:"Likely free",PENDING_DELETE:"Pending delete",REDEMPTION:"Redemption",
  EXPIRED_GRACE:"Expired (grace)",EXPIRING_SOON:"Expiring",DEAD_SITE:"Dead site",PARKED:"Parked"};
const GROUP={buy:["AVAILABLE","LIKELY_AVAILABLE"],drop:["PENDING_DELETE","REDEMPTION","EXPIRED_GRACE","EXPIRING_SOON"],
  dead:["DEAD_SITE","PARKED"]};
const SO={AVAILABLE:0,LIKELY_AVAILABLE:1,PENDING_DELETE:2,REDEMPTION:3,EXPIRED_GRACE:4,EXPIRING_SOON:5,DEAD_SITE:6,PARKED:7};
let MARKS={};try{MARKS=JSON.parse(localStorage.getItem("df-marks")||"{}")}catch(e){}
const mk=d=>MARKS[d]||{};
function setMark(d,k){const m={...mk(d)};m[k]=!m[k];if(!m[k])delete m[k];if(Object.keys(m).length)MARKS[d]=m;else delete MARKS[d];
  try{localStorage.setItem("df-marks",JSON.stringify(MARKS))}catch(e){}}
const PER=100; let BUY="", ALL=[], VIEW=[], page=0, sortK="au", asc=false;
const $=id=>document.getElementById(id);
const esc=s=>String(s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const safe=u=>/^https?:\/\//i.test(u||"");
const host=u=>{try{return new URL(u).hostname.replace(/^www\./,"")}catch(e){return u}};
const frag=t=>encodeURIComponent(t).replace(/-/g,"%2D").replace(/,/g,"%2C");
function jump(s){const b=s.u.split("#")[0];let t=(s.a||"").replace(/\s+/g," ").trim();
  if(t.length>70)t=t.slice(0,70).replace(/\s+\S*$/,"");
  return t.length>=3&&!/^https?:/i.test(t)&&s.via!=="wikipedia"?b+"#:~:text="+frag(t):b;}
function srcHtml(s){if(!safe(s.u))return"";
  if(s.g)return `<div>Linked from many sites across the web<span class="anc"><a href="${esc(s.u)}" target="_blank" rel="noopener nofollow">See its backlinks &rsaquo;</a></span></div>`;const p=s.u.replace(/^https?:\/\/(www\.)?/,"");
  return `<div><a href="${esc(jump(s))}" target="_blank" rel="noopener nofollow">${esc(p.length>70?p.slice(0,70)+"…":p)}</a>
  <span class="tag ${s.f==="dofollow"?"df":"nf"}">${s.f==="dofollow"?"dofollow":s.f||"link"}</span>
  ${s.a?`<span class="anc">“${esc(s.a)}”</span>`:""}</div>`;}
function row(r,i){const extra=r.s.length-1,m=mk(r.d);
  return `<tr class="${m.c?"m-c":""}"><td class="dom"><button class="dn" data-i="${i}" title="${esc(r.d)}">${m.s?'<span class="mk">&#9733;</span>':""}${m.c?'<span class="mk">&#10003;</span>':""}${esc(r.d)}</button></td>
  <td class="src">${srcHtml(r.s[0])}${extra>0?`<div class="x" hidden>${r.s.slice(1).map(srcHtml).join("")}</div>
   <button class="more" data-i="${i}">+${extra} more page${extra>1?"s":""}</button>`:""}
   <span class="anc">${esc(VIA[r.via]||r.via)}</span></td>
  <td class="num">${r.n||"web"}</td>
  <td class="num">${r.au?r.au.toFixed(1):"-"}</td><td class="num">${r.y||"-"}</td><td class="num">${r.q}</td>
  <td class="num">${r.v?"$"+Number(r.v).toLocaleString():"-"}</td>
  <td class="stc"><span class="s-${r.st}">${ST[r.st]||r.st}</span>${r.drop?`<span class="tag new" title="Was: ${esc(ST[r.was]||r.was)}">Dropped ${r.drop.slice(5)}</span>`:""}${r.sp?`<span class="tag warn" title="Archive shows spam: ${esc(r.sp)}">&#9888; spam history</span>`:""}${["AVAILABLE","LIKELY_AVAILABLE"].includes(r.st)&&BUY?`<a class="buy" href="${esc(BUY.replace("DOMAIN",encodeURIComponent(r.d)))}" target="_blank" rel="noopener">Register</a>`:""}</td>
  <td class="num opt">${r.found}</td></tr>`;}
function apply(){closePop();const q=$("q").value.trim().toLowerCase(),g=$("st").value,via=$("via").value,tld=$("tld").value,
  dfo=$("dfo").checked,ns=$("nospam").checked,amin=+$("au").value,mv=$("mk").value,nsh=$("nosph").checked;
  const cut=new Date(Date.now()-14*864e5).toISOString().slice(0,10);
  VIEW=ALL.filter(r=>(g==="all"||(g==="new"?r.drop>=cut:GROUP[g].includes(r.st)))&&(!via||r.s.some(s=>s.via===via))&&(!tld||r.tld===tld)
    &&(!dfo||r.df>0||r.n===0)&&(!ns||!r.spam)&&(!nsh||!r.sp)&&r.au>=amin
    &&(()=>{const m=mk(r.d);return mv===""?!m.h:mv==="u"?!m.c&&!m.h:!!m[mv]})()
    &&(!q||r.d.includes(q)||r.s.some(s=>s.u.toLowerCase().includes(q)||s.a.toLowerCase().includes(q))));
  const k=sortK,m=asc?1:-1;
  VIEW.sort((a,b)=>{let x=a[k],y=b[k];if(k==="y"){const z=asc?"9999":"0000";x=x||z;y=y||z}
    if(k==="st"){x=SO[x]??9;y=SO[y]??9}
    return (x<y?-1:x>y?1:0)*m||b.q-a.q});
  page=0;render();save();}
function render(){const n=VIEW.length,pages=Math.max(1,Math.ceil(n/PER));page=Math.min(page,pages-1);
  const slice=VIEW.slice(page*PER,page*PER+PER);
  $("rows").innerHTML=slice.length?slice.map((r,i)=>row(r,page*PER+i)).join(""):
    `<tr><td colspan="9" class="empty">No domains match these filters.</td></tr>`;
  $("count").textContent=`${n.toLocaleString()} domains (of ${ALL.length.toLocaleString()})`;
  $("pno").textContent=`Page ${page+1} of ${pages}`;$("prev").disabled=page===0;$("next").disabled=page>=pages-1;
  document.querySelectorAll("th[data-k]").forEach(th=>{th.classList.toggle("on",th.dataset.k===sortK);th.classList.toggle("asc",th.dataset.k===sortK&&asc)});}
function save(){try{localStorage.setItem("df-t2",JSON.stringify({mk:$("mk").value,st:$("st").value,via:$("via").value,tld:$("tld").value,au:$("au").value,
  dfo:$("dfo").checked,ns:$("nospam").checked,nsh:$("nosph").checked,k:sortK,asc}))}catch(e){}}
function restore(){try{const o=JSON.parse(localStorage.getItem("df-t2")||"{}");if(o.st)$("st").value=o.st;if(o.mk)$("mk").value=o.mk;if(o.au)$("au").value=o.au;
  if(o.via)$("via").value=o.via;if(o.tld)$("tld").value=o.tld;$("dfo").checked=!!o.dfo;if(o.ns===false)$("nospam").checked=false;$("nosph").checked=!!o.nsh;
  if(o.k){sortK=o.k;asc=!!o.asc}}catch(e){}}
function fill(sel,counts,label){Object.entries(counts).sort((a,b)=>b[1]-a[1]).forEach(([k,c])=>{
  const o=document.createElement("option");o.value=k;o.textContent=`${label(k)} (${c})`;$(sel).appendChild(o)});}
document.querySelector("thead").addEventListener("click",e=>{const th=e.target.closest("th[data-k]");if(!th)return;
  const k=th.dataset.k;if(sortK===k)asc=!asc;else{sortK=k;asc=(k==="d"||k==="y")}apply();});
$("rows").addEventListener("click",e=>{const b=e.target.closest(".more");if(!b)return;const x=b.previousElementSibling;
  x.hidden=!x.hidden;b.textContent=x.hidden?b.dataset.t:"Show less";});
document.addEventListener("click",e=>{const b=e.target.closest(".more");if(b&&!b.dataset.t)b.dataset.t=b.textContent},true);
["q","st","via","tld","au","mk","dfo","nospam","nosph"].forEach(id=>$(id).addEventListener(id==="q"?"input":"change",apply));
let popI=-1,popPinned=false,hoverT=0;
function qlinks(d){const e=encodeURIComponent;return [
  ["Google mentions","https://www.google.com/search?q="+e('"'+d+'"')],["Google site:","https://www.google.com/search?q="+e("site:"+d)],
  ["Wayback history","https://web.archive.org/web/*/"+d+"*"],["Backlinks (Ahrefs)","https://ahrefs.com/backlink-checker/?input="+e(d)+"&mode=subdomains"],
  ["Backlinks (Majestic)","https://majestic.com/reports/site-explorer?q="+e(d)+"&IndexDataSource=F"],["Owner / expiry","https://lookup.icann.org/en/lookup?name="+e(d)]];}
function openPop(i,cell,pin){const r=VIEW[i];if(!r)return;popI=i;popPinned=!!pin;const m=mk(r.d),P=$("pop");
  const buy=["AVAILABLE","LIKELY_AVAILABLE"].includes(r.st)&&BUY;
  P.innerHTML=`<div class="ph"><div class="pd">${esc(r.d)}</div><button class="x" data-act="close" aria-label="Close">&times;</button></div>
  <div class="facts"><b class="s-${r.st}">${ST[r.st]||r.st}</b>${r.drop?` &middot; dropped ${r.drop}`:""} &middot; Authority <b>${r.au?r.au.toFixed(1):"-"}</b>
   &middot; Quality <b>${r.q}</b> &middot; Since <b>${r.y||"-"}</b> &middot; Value <b>${r.v?"$"+Number(r.v).toLocaleString():"-"}</b></div>
  ${r.sp?`<div class="spw">&#9888; Archived pages show spam (${esc(r.sp)}). Check the Wayback history before buying.</div>`:r.spc?`<div class="spok">&#10003; No spam found in archived pages</div>`:""}
  <div class="marks"><button data-act="s" class="${m.s?"on":""}">&#9733; Shortlist</button><button data-act="c" class="${m.c?"on":""}">&#10003; Checked</button><button data-act="h" class="${m.h?"on":""}">&#10005; Hide</button></div>
  <h4>Check it</h4><div class="qc">${qlinks(r.d).map(([t,u])=>`<a href="${esc(u)}" target="_blank" rel="noopener nofollow">${t}</a>`).join("")}
   <button data-act="copy">Copy domain</button></div>
  <h4>Linked from</h4><div class="src">${r.s.map(srcHtml).join("")}</div>
  ${buy?`<a class="buy" href="${esc(BUY.replace("DOMAIN",encodeURIComponent(r.d)))}" target="_blank" rel="noopener">Register ${esc(r.d)}</a>`:""}`;
  P.hidden=false;
  if(innerWidth>700&&cell){const b=cell.getBoundingClientRect(),w=P.offsetWidth,h=P.offsetHeight;
    let x=b.left,y=b.bottom+6;if(x+w>innerWidth-8)x=innerWidth-8-w;if(y+h>innerHeight-8)y=Math.max(8,b.top-h-6);
    P.style.left=Math.max(8,x)+"px";P.style.top=y+"px";}}
function closePop(){$("pop").hidden=true;popI=-1;popPinned=false;}
$("rows").addEventListener("click",e=>{const b=e.target.closest(".dn");if(!b)return;e.stopPropagation();
  const i=+b.dataset.i;if(popI===i&&popPinned){closePop();return}openPop(i,b.closest("td"),true);});
const canHover=matchMedia("(hover:hover) and (pointer:fine)").matches;
if(canHover){$("rows").addEventListener("mouseover",e=>{const b=e.target.closest(".dn");if(!b||popPinned)return;
    clearTimeout(hoverT);hoverT=setTimeout(()=>openPop(+b.dataset.i,b.closest("td"),false),350);});
  $("rows").addEventListener("mouseout",e=>{if(e.target.closest(".dn")&&!popPinned){clearTimeout(hoverT);hoverT=setTimeout(()=>{if(!popPinned&&!$("pop").matches(":hover"))closePop()},300)}});
  $("pop").addEventListener("mouseleave",()=>{if(!popPinned){hoverT=setTimeout(closePop,300)}});
  $("pop").addEventListener("mouseenter",()=>clearTimeout(hoverT));}
$("pop").addEventListener("click",e=>{const b=e.target.closest("[data-act]");if(!b){popPinned=true;return}const r=VIEW[popI],a=b.dataset.act;
  if(a==="close"){closePop();return}
  if(a==="copy"){const done=()=>{b.textContent="Copied";setTimeout(()=>b.textContent="Copy domain",1200)};
    (navigator.clipboard?navigator.clipboard.writeText(r.d):Promise.reject()).then(done).catch(()=>{const t=document.createElement("textarea");t.value=r.d;
    document.body.appendChild(t);t.select();try{document.execCommand("copy");done()}catch(_){}t.remove()});return}
  setMark(r.d,a);b.classList.toggle("on",!!mk(r.d)[a]);popPinned=true;
  const keep=popI;const tr=document.querySelector(`.dn[data-i="${keep}"]`);if(tr){const m=mk(r.d);
    tr.innerHTML=(m.s?'<span class="mk">&#9733;</span>':"")+(m.c?'<span class="mk">&#10003;</span>':"")+esc(r.d);tr.closest("tr").classList.toggle("m-c",!!m.c);
    if(a==="h"&&m.h)tr.closest("tr").style.display="none";}});
document.addEventListener("click",e=>{if(!$("pop").hidden&&!e.target.closest("#pop")&&!e.target.closest(".dn"))closePop()});
document.addEventListener("keydown",e=>{if(e.key==="Escape")closePop()});
addEventListener("scroll",()=>{if(!$("pop").hidden&&innerWidth>700&&!popPinned)closePop()},{passive:true});
$("prev").onclick=()=>{closePop();page--;render();scrollTo(0,0)};$("next").onclick=()=>{closePop();page++;render();scrollTo(0,0)};
fetch("/table.json").then(r=>r.json()).then(j=>{ALL=j.rows||[];BUY=j.buy||"";
  const v={},t={};ALL.forEach(r=>{new Set(r.s.map(s=>s.via)).forEach(x=>{if(x)v[x]=(v[x]||0)+1});t[r.tld]=(t[r.tld]||0)+1});
  fill("via",v,k=>VIA[k]||k);fill("tld",t,k=>"."+k);restore();
  if(j.updated)$("upd").textContent="Updated "+new Date(j.updated).toLocaleString([], {dateStyle:"medium",timeStyle:"short"});
  apply();if(location.hash==="#demo-pop"){const b=document.querySelector(".dn");if(b)openPop(+b.dataset.i,b.closest("td"),true)}
  }).catch(()=>{$("count").textContent="Couldn't load the data - please refresh."});
</script></body></html>"""


def limits():
    return dict(seeds=MAX_SEEDS, pages=MAX_PAGES, domains=MAX_DOMAINS, per_hour=JOBS_PER_HOUR, auto=AUTO_SITES)


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>dropfinder - expired domains with authority backlinks</title>
<link href="https://fonts.googleapis.com/css2?family=Libre+Franklin:wght@400;600;800&display=swap" rel="stylesheet">
<style>
/* VOICE: a trader's watchlist - dense, calm, scannable on a phone.
   IDEA: each domain is a ticket card; the quality badge is the price tag.
   PICTURE: the card grid itself is the picture.
   COLOUR/TYPE: warm paper ground, teal for good, claret for urgent; Libre Franklin.
   LAYOUT: slim header, one control row, cards 1/2/3 columns. */
:root{color-scheme:light dark;--bg:light-dark(#fff8f5,#1c1512);--card:light-dark(#fff,#261d19);--ink:light-dark(#1a1817,#f3ebe6);
--mute:light-dark(#6b625d,#b3a69e);--line:light-dark(#ecd9d1,#3a2d27);--teal:light-dark(#0d7680,#4fb3bc);--claret:light-dark(#990f3d,#ff8fb1);
--tealbg:light-dark(#0d76801f,#4fb3bc26);--claretbg:light-dark(#990f3d17,#ff8fb126)}
:root[data-theme=light]{color-scheme:light}:root[data-theme=dark]{color-scheme:dark}
*{box-sizing:border-box;min-width:0}
body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 "Libre Franklin",system-ui,sans-serif;overflow-x:clip}
a{color:var(--teal)}
.wrap{max-width:1280px;margin:auto;padding:0 max(3vw,12px)}
header{display:flex;align-items:center;gap:12px;flex-wrap:wrap;padding:12px 0;border-bottom:1px solid var(--line)}
header .logo{font-weight:800;font-size:19px;color:var(--ink);text-decoration:none;margin-right:auto}
header nav{display:flex;gap:14px}header nav a{color:var(--mute);text-decoration:none;font-weight:600;font-size:14px}
header nav a.on{color:var(--ink)}
button,.btn{display:inline-flex;align-items:center;justify-content:center;gap:6px;background:var(--teal);color:#fff;border:0;border-radius:8px;
  padding:9px 16px;font:600 15px "Libre Franklin",system-ui,sans-serif;cursor:pointer;text-decoration:none;min-height:40px;white-space:nowrap}
.btn.ghost,button.ghost{background:transparent;color:var(--teal);border:1px solid var(--teal)}
:focus-visible{outline:3px solid var(--claret);outline-offset:2px}
.intro{display:flex;flex-wrap:wrap;align-items:baseline;gap:6px 16px;margin:14px 0 10px}
h1{font-size:clamp(20px,2.6vw,26px);line-height:1.15;margin:0;font-weight:800;text-wrap:balance}
h1 span{color:var(--teal)}
.intro p{margin:0;color:var(--mute);font-size:13px;max-width:70ch}
.stats{display:flex;gap:8px;overflow-x:auto;scrollbar-width:none;margin:0 0 12px;padding-bottom:2px}
.stat{flex:none;background:var(--card);border:1px solid var(--line);border-radius:10px;padding:6px 12px;font-size:12px;color:var(--mute)}
.stat b{font-size:17px;color:var(--ink);margin-right:4px;font-weight:800}
.controls{position:sticky;top:0;z-index:5;background:var(--bg);padding:8px 0;border-bottom:1px solid var(--line);margin-bottom:12px}
.row1{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.chips{display:flex;gap:6px;overflow-x:auto;scrollbar-width:none;flex:1 1 auto}
.chip{flex:none;border:1px solid var(--line);background:var(--card);color:var(--ink);border-radius:99px;padding:6px 12px;font:600 13px inherit;min-height:34px;cursor:pointer}
.chip[aria-pressed=true]{background:var(--teal);border-color:var(--teal);color:#fff}
select,input[type=text],input[type=number]{font:16px "Libre Franklin",system-ui,sans-serif;padding:7px 10px;border:1px solid var(--line);border-radius:8px;
  background:var(--card);color:var(--ink);min-height:38px}
#sort{flex:0 1 190px}
.more{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:8px;margin-top:8px}
.more[hidden]{display:none}
.more label{display:flex;gap:6px;align-items:center;font-size:14px}
.more input[type=number]{width:72px}
.count{margin:0 0 10px;font-size:13px;color:var(--mute)}.count b{color:var(--ink)}
#rows{display:grid;gap:12px;grid-template-columns:repeat(auto-fill,minmax(min(100%,340px),1fr));grid-auto-rows:1fr;align-items:stretch}
.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:14px;display:flex;flex-direction:column;gap:10px;height:100%}
.card .acts{margin-top:auto}
.card .src a.bk{display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.card details[open]{position:relative}
.viewtog{display:inline-flex;border:1px solid var(--line);border-radius:8px;overflow:hidden;flex:none}
.viewtog button{background:var(--card);color:var(--ink);border:0;border-radius:0;min-height:38px;padding:6px 12px;font-size:13px}
.viewtog button[aria-pressed=true]{background:var(--teal);color:#fff}
/* list view: one find per full-width row */
#rows.list{grid-template-columns:1fr;grid-auto-rows:auto}
@media(min-width:700px){
  #rows.list .card{display:grid;grid-template-columns:minmax(230px,1.3fr) minmax(0,1.5fr) minmax(0,1.6fr) auto;
    grid-template-areas:"top meta src acts" "top why src acts";align-items:start;gap:6px 18px}
  #rows.list .top{grid-area:top}#rows.list .meta{grid-area:meta}#rows.list .why{grid-area:why}
  #rows.list .src{grid-area:src;border-top:0;padding-top:0}
  #rows.list .acts{grid-area:acts;margin:0;grid-template-columns:1fr;align-self:center;min-width:170px}
  #rows.list .top .q{margin-left:12px}
  #rows.list .dom{overflow-wrap:normal;word-break:normal}
  #rows.list .acts{min-width:150px}
}
.top{display:flex;gap:10px;align-items:flex-start}
.dom{font-weight:800;font-size:17px;line-height:1.25;overflow-wrap:anywhere;margin:0}
.q{flex:none;margin-left:auto;text-align:center;border-radius:10px;padding:4px 8px;min-width:52px;line-height:1.1}
.q b{display:block;font-size:20px}.q small{font-size:10px;font-weight:600;text-transform:none}
.qhi{background:var(--tealbg);color:var(--teal)}.qmid{background:var(--line)}.qlo{background:var(--line);color:var(--mute)}
.tag{display:inline-block;font-size:12px;font-weight:600;padding:2px 8px;border-radius:99px;background:var(--line);white-space:nowrap}
.AVAILABLE,.LIKELY_AVAILABLE,.df{background:var(--tealbg);color:var(--teal)}
.PENDING_DELETE,.REDEMPTION,.EXPIRED_GRACE{background:var(--claretbg);color:var(--claret)}
.meta{display:flex;flex-wrap:wrap;gap:4px 12px;font-size:13px;color:var(--mute)}
.meta b{color:var(--ink);font-weight:600}
.val.good b{color:var(--teal)}
.src{font-size:13px;border-top:1px solid var(--line);padding-top:8px;display:grid;gap:4px}
.src .lbl{color:var(--mute);font-size:11px;font-weight:600}
.src a{overflow-wrap:anywhere}
.anchor{font-style:italic}
.flag{font-size:12px;color:var(--claret)}
details summary{cursor:pointer;color:var(--mute);font-size:12px}
details ul{margin:4px 0 0;padding-left:18px;font-size:13px}
.acts{display:grid;grid-template-columns:1fr auto;gap:8px}
.acts .btn{min-height:42px}
.empty{padding:28px;text-align:center;color:var(--mute);grid-column:1/-1}
pre.log{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px;max-height:200px;overflow:auto;font-size:13px;white-space:pre-wrap}
form.custom{display:grid;gap:18px;grid-template-columns:repeat(auto-fit,minmax(min(100%,320px),1fr));background:var(--card);border:1px solid var(--line);border-radius:12px;padding:18px}
form.custom label.t{display:block;font-weight:600;margin:10px 0 4px}
form.custom textarea,form.custom input[type=text],form.custom input[type=number]{width:100%}
form.custom textarea{min-height:120px;font:14px ui-monospace,monospace;padding:10px;border:1px solid var(--line);border-radius:8px;background:var(--bg);color:var(--ink)}
.check{display:flex;gap:10px;align-items:flex-start;margin:10px 0}
.hint{font-size:13px;color:var(--mute);margin:4px 0 0}
.errors{background:var(--claretbg);border-left:4px solid var(--claret);padding:10px 14px;margin:14px 0;border-radius:6px}
.spin{display:inline-block;width:14px;height:14px;border:2px solid var(--line);border-top-color:var(--teal);border-radius:50%;animation:s 1s linear infinite;vertical-align:-2px;margin-right:6px}
@keyframes s{to{transform:rotate(360deg)}}@media(prefers-reduced-motion:reduce){.spin{animation:none}}
footer{color:var(--mute);font-size:12px;padding:20px 0 32px}
dialog#buy{width:min(980px,96vw);max-height:92vh;border:1px solid var(--line);border-radius:16px;padding:16px;background:var(--bg);color:var(--ink)}
dialog#buy::backdrop{background:rgb(0 0 0 / .45)}
.buyhead{display:flex;align-items:flex-start;justify-content:space-between;gap:12px}
.buyhead h2{margin:2px 0 0;font-size:22px;overflow-wrap:anywhere}.buyhead .lbl{margin:0;color:var(--mute);font-size:12px;font-weight:600}
.buyhint{margin:8px 0;color:var(--teal);font-size:14px}
.buyrow{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px}
#buyFrame iframe{display:block;width:100%;min-height:420px;border:0;background:transparent;color-scheme:normal}
@media(max-width:600px){.intro p{display:none}#sort{flex:1 1 100%}}
</style></head><body><div class="wrap">
<header><a class="logo" href="{{ url_for('index') }}">dropfinder</a>
<nav><a href="{{ url_for('index') }}" class="{{ 'on' if view=='feed' }}">Finds</a><a href="{{ url_for('custom') }}" class="{{ 'on' if view=='custom' }}">Custom hunt</a></nav>
{% if view == "feed" %}<form method="post" action="{{ url_for('hunt_auto') }}"><button type="submit" title="Fresh automatic hunt, about 5 minutes">Hunt now</button></form>{% endif %}
</header>

{% if view == "feed" %}
<div class="intro"><h1>Expired domains with <span>authority backlinks</span></h1>
<p>Found automatically every few hours from strong backlink profiles, closed organisations and old articles on top publishers; checked against registries and the Wayback Machine; scored for quality and value for money.</p></div>
<div class="stats">
  <div class="stat"><b id="s-avail">-</b>available</div>
  <div class="stat"><b id="s-drop">-</b>dropping</div>
  <div class="stat"><b id="s-q60">-</b>quality 60+</div>
  <div class="stat"><b id="s-total">-</b>tracked</div>
  <div class="stat"><b id="s-upd">-</b>updated</div>
</div>
<div class="controls">
  <div class="row1">
    <div class="chips" id="chips"></div>
    <select id="sort" aria-label="Sort by">
      <option value="quality">Best quality</option><option value="value">Best value for money</option><option value="drops">Dropping soonest</option>
      <option value="ref_subnets">Most referring networks</option><option value="authority">Highest authority</option>
      <option value="real_years">Longest real history</option><option value="price">Cheapest</option><option value="domain">A-Z</option></select>
    <div class="viewtog" role="group" aria-label="Layout"><button type="button" id="vGrid" aria-pressed="true">Grid</button><button type="button" id="vList" aria-pressed="false">List</button></div>
    <button type="button" class="ghost" id="moreBtn" aria-expanded="false" aria-controls="more">Filters</button>
  </div>
  <div class="more" id="more" hidden>
    <input type="text" id="q" placeholder="Search domain" aria-label="Search domain">
    <input type="text" id="tld" placeholder="TLDs e.g. com,org" aria-label="TLDs">
    <select id="via" aria-label="Link source"><option value="">All sources</option><option value="stabledomains">Drop feed</option>
      <option value="majestic">Majestic Million</option><option value="commoncrawl">Top web graph</option><option value="hackernews">Hacker News</option>
      <option value="debian">Debian packages</option><option value="cordis">EU research projects</option>
      <option value="wikidata">Closed organisations</option><option value="publishers">Top publishers</option>
      <option value="crawl">Ranked sites</option><option value="wikipedia">Wikipedia</option></select>
    <label>Min quality <input type="number" id="minscore" min="0" max="100" step="5" value="0"></label>
    <label title="Estimated value divided by registration price">Worth at least <input type="number" id="minratio" min="0" max="100" step="1" value="3">× price</label>
    <label><input type="checkbox" id="wikionly"> Include Wikipedia-only</label>
    <label><input type="checkbox" id="dfonly"> Dofollow only</label>
    <label><input type="checkbox" id="nospam" checked> Hide spammy</label>
    <a class="btn ghost" href="{{ url_for('feed_csv') }}">Download CSV</a>
  </div>
</div>
<p class="count" id="count" aria-live="polite"></p>
<div id="rows"><div class="empty"><span class="spin"></span>Loading finds...</div></div>
<p id="more2" hidden style="text-align:center"><button class="ghost" id="morebtn" type="button">Show more</button></p>

{% elif view == "custom" %}
<div class="intro"><h1>Custom hunt</h1><p>Optional extras. Leave the seed box empty and dropfinder finds its own pages.</p></div>
{% if errors %}<div class="errors">{% for e in errors %}<div>{{ e }}</div>{% endfor %}</div>{% endif %}
<form class="custom" method="post" action="{{ url_for('run') }}">
  <div>
    <label class="t" for="auto_sites">Random ranked sites to scan</label>
    <input id="auto_sites" name="auto_sites" type="number" min="0" max="{{ limits.auto }}" value="{{ form.get('auto_sites', limits.auto) }}">
    <label class="t" for="wiki_months">Random Wikipedia dead-link months</label>
    <input id="wiki_months" name="wiki_months" type="number" min="0" max="2" value="{{ form.get('wiki_months', 1) }}">
    <label class="t" for="seeds">Your own seed pages (optional, one per line)</label>
    <textarea id="seeds" name="seeds" placeholder="https://example.edu/resources/links">{{ form.get('seeds','') }}</textarea>
    <p class="hint">Up to {{ limits.seeds }}. robots.txt is obeyed, one request per second per site.</p>
    <label class="check"><input type="checkbox" name="sitemaps" {{ 'checked' if form.get('sitemaps', 'on') }}> Read each seed site's oldest sitemap pages</label>
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
    <p style="margin-top:14px"><button type="submit">Start hunt</button></p>
    <p class="hint">{{ limits.per_hour }} hunts per hour per visitor. Up to {{ limits.domains }} domains checked per hunt.</p>
  </div>
</form>

{% else %}
<div class="intro"><h1>Hunting...</h1><p id="status"><span class="spin"></span>Usually 3-8 minutes. Keep this page open or bookmark it.</p></div>
<div class="stats">
  <div class="stat"><b id="links">-</b>links</div><div class="stat"><b id="dofollow">-</b>dofollow</div>
  <div class="stat"><b id="domains">-</b>domains</div><div class="stat"><b id="found">-</b>candidates</div>
</div>
<pre class="log" id="log"></pre>
<p id="actions" hidden><a class="btn ghost" href="{{ url_for('job_csv', jid=job.id) }}">Download CSV</a></p>
<div id="rows"></div>
{% endif %}
<dialog id="buy" aria-labelledby="buyTitle">
  <div class="buyhead">
    <div><p class="lbl">Register with StableDomains</p><h2 id="buyTitle">domain</h2></div>
    <button type="button" class="ghost" id="buyClose" aria-label="Close">Close</button>
  </div>
  <p class="buyhint" id="buyHint">Domain copied - paste it into the search below.</p>
  <div class="buyrow"><button type="button" id="buyCopy">Copy domain</button>
    <a class="btn ghost" id="buyOut" href="#" target="_blank" rel="noopener">Open on stabledomains.net ↗</a></div>
  <div id="buyFrame"></div>
</dialog>
<footer>Status from public DNS, RDAP, the Wayback Machine and StableDomains; it can change quickly, so confirm at checkout.
Tap a source to open the linking page with the backlink highlighted. Register links go to StableDomains (affiliate).</footer>
</div>
<script>
{
  const esc = s => String(s ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
  const $ = x => document.getElementById(x);
  const safeUrl = u => /^https?:\/\//i.test(String(u || ""));
  const NICE = {AVAILABLE:"available",LIKELY_AVAILABLE:"likely available",PENDING_DELETE:"pending delete",REDEMPTION:"redemption",
    EXPIRED_GRACE:"expired (grace)",EXPIRING_SOON:"expiring soon",PARKED:"parked",DEAD_SITE:"dead site",REGISTERED:"registered"};
  const ORDER = {AVAILABLE:0,PENDING_DELETE:1,REDEMPTION:2,EXPIRED_GRACE:3,LIKELY_AVAILABLE:4,EXPIRING_SOON:5,PARKED:6,DEAD_SITE:7};
  const BUY = new Set(["AVAILABLE","LIKELY_AVAILABLE"]);
  const VIA = {hackernews:"Hacker News", debian:"Debian", cordis:"EU project", crawl:"ranked site", wikipedia:"Wikipedia",
    stabledomains:"Drop feed", majestic:"Majestic Million", commoncrawl:"Top web graph",
    wikidata:"Closed organisation", publishers:"Top publisher"};
  const qClass = q => q >= 60 ? "qhi" : q >= 35 ? "qmid" : "qlo";
  const num = v => (v === null || v === undefined || v === "") ? null : Number(v);
  // open the linking page scrolled to the backlink, highlighted (#:~:text=)
  const frag = t => encodeURIComponent(t).replace(/-/g, "%2D").replace(/,/g, "%2C");
  function jumpUrl(s) {
    if (!s || !safeUrl(s.url)) return "";
    const base = s.url.split("#")[0];
    let t = (s.anchor || "").replace(/\s+/g, " ").trim();
    if (t.length > 70) t = t.slice(0, 70).replace(/\s+\S*$/, "");
    return t.length >= 3 && t !== "Wayback history" && s.via !== "wikipedia" ? `${base}#:~:text=${frag(t)}` : base;
  }
  const hostOf = u => { try { return new URL(u).hostname.replace(/^www\./, ""); } catch (e) { return u || ""; } };
  function card(r) {
    const src = (r.sources || []).filter(s => s.url);
    const anchored = src.filter(s => s.anchor && s.anchor !== "Wayback history");
    const best = anchored.find(s => s.follow === "dofollow") || anchored[0] || src[0];
    const q = r.quality != null ? r.quality : null;
    const meta = [];
    if (num(r.est_value)) meta.push(`<span class="${(r.value_ratio || 0) >= 3 ? "val good" : "val"}">est. <b>$${esc(Number(r.est_value).toLocaleString())}</b>${r.value_ratio ? ` (${esc(r.value_ratio)}× price)` : ""}</span>`);
    if (num(r.ref_subnets)) meta.push(`<span><b>${esc(Number(r.ref_subnets).toLocaleString())}</b> ref. networks</span>`);
    if (num(r.authority) !== null) meta.push(`<span>authority <b>${esc(r.authority)}</b>/10</span>`);
    if (num(r.real_years)) meta.push(`<span><b>${esc(r.real_years)}</b>y real site</span>`);
    else if (num(r.wb_years)) meta.push(`<span><b>${esc(r.wb_years)}</b>y archived</span>`);
    if (num(r.dofollow_sources)) meta.push(`<span><b>${esc(r.dofollow_sources)}</b> dofollow</span>`);
    if (r.expiry) meta.push(`<span>${r.status === "PENDING_DELETE" ? "drops" : "expiry"} <b>${esc(String(r.expiry).slice(0, 10))}</b></span>`);
    const vias = (r.via || "").split(",").filter(Boolean).map(v => VIA[v] || v).join(", ");
    let srcHtml = "";
    if (best && best.anchor && best.anchor !== "Wayback history") {
      srcHtml = `<span class="lbl">Backlink${vias ? " - " + esc(vias) : ""}</span>
        <a class="bk" href="${esc(jumpUrl(best))}" target="_blank" rel="noopener nofollow"><span class="anchor">"${esc(best.anchor)}"</span> on ${esc(hostOf(best.url))} ↗</a>
        <span><span class="tag ${best.follow === "dofollow" ? "df" : ""}">${esc(best.follow || "link")}</span>${best.rank ? ` <span class="tag">site #${esc(Number(best.rank).toLocaleString())}</span>` : ""}</span>`;
    } else if (best) {
      srcHtml = `<span class="lbl">Found via ${esc(vias || "link")}</span><a href="${esc(jumpUrl(best))}" target="_blank" rel="noopener nofollow">${esc(hostOf(best.url))} ↗</a>`;
    }
    const others = anchored.filter(s => s !== best);
    if (others.length) srcHtml += `<details><summary>${others.length} more backlink${others.length > 1 ? "s" : ""}</summary><ul>${others.map(s =>
      `<li><a href="${esc(jumpUrl(s))}" target="_blank" rel="noopener nofollow">"${esc(s.anchor)}"</a> on ${esc(hostOf(s.url))} <span class="tag">${esc(s.follow || "")}</span></li>`).join("")}</ul></details>`;
    const why = (r.quality_reasons || []).map(x => `<li>${esc(x)}</li>`).join("");
    const buy = safeUrl(r.buy_url) ? esc(r.buy_url) : "#";
    const price = r.sd_price ? ` $${esc(r.sd_price)}` : "";
    const main = BUY.has(r.status) ? `<a class="btn reg" data-domain="${esc(r.domain)}" href="${buy}" target="_blank" rel="noopener">Register${price}</a>`
      : `<a class="btn ghost" href="${buy}" target="_blank" rel="noopener">${r.status === "PENDING_DELETE" || r.status === "REDEMPTION" ? "Watch / backorder" : "Check"}</a>`;
    return `<article class="card">
      <div class="top"><div><p class="dom">${esc(r.domain)}</p>
        <span class="tag ${esc(r.status)}">${esc(NICE[r.status] || r.status)}</span>${r.flags ? ` <span class="flag">${esc(r.flags)}</span>` : ""}</div>
        ${q !== null ? `<div class="q ${qClass(q)}"><b>${esc(q)}</b><small>quality</small></div>` : ""}</div>
      ${meta.length ? `<div class="meta">${meta.join("")}</div>` : ""}
      ${why ? `<details class="why"><summary>Why this score</summary><ul>${why}</ul></details>` : ""}
      ${srcHtml ? `<div class="src">${srcHtml}</div>` : ""}
      <div class="acts">${main}<a class="btn ghost" href="https://majestic.com/reports/site-explorer?q=${encodeURIComponent(r.domain)}" target="_blank" rel="noopener nofollow">Backlinks</a></div>
    </article>`;
  }

  // Register opens the StableDomains widget (affiliate) in a pop-up; the widget has no prefill, so copy the domain.
  const WIDGET = "https://stabledomains.net/embed/domains/RvcumQY-UxdjY6Uagk4v0Lzb?utm_campaign=dropfinder";
  let widgetLoaded = false;
  async function copyDomain(d) {
    try { await navigator.clipboard.writeText(d); return true; } catch (e) { return false; }
  }
  function openBuy(domain, href) {
    const dlg = $("buy");
    if (!dlg || typeof dlg.showModal !== "function") return false;
    $("buyTitle").textContent = domain;
    $("buyOut").href = href;
    if (!widgetLoaded) {
      $("buyFrame").innerHTML = `<iframe id="sd-domain-widget" src="${WIDGET}" title="Domain search" allowtransparency="true" allow="clipboard-write" referrerpolicy="origin"></iframe>`;
      const s = document.createElement("script");
      s.src = "https://stabledomains.net/widget-host.js"; s.async = true; s.dataset.frame = "sd-domain-widget";
      document.body.appendChild(s);
      widgetLoaded = true;
    }
    copyDomain(domain).then(ok => { $("buyHint").textContent = ok ? "Domain copied - paste it into the search below." : `Type ${domain} into the search below.`; });
    dlg.showModal();
    return true;
  }
  if ($("buy")) {
    document.addEventListener("click", e => {
      const a = e.target.closest("a.reg");
      if (!a) return;
      if (openBuy(a.dataset.domain, a.href)) e.preventDefault();
    });
    $("buyClose").addEventListener("click", () => $("buy").close());
    $("buy").addEventListener("click", e => { if (e.target === $("buy")) $("buy").close(); });
    $("buyCopy").addEventListener("click", () => copyDomain($("buyTitle").textContent).then(ok => {
      $("buyCopy").textContent = ok ? "Copied ✓" : "Copy failed - select the name above";
      setTimeout(() => { $("buyCopy").textContent = "Copy domain"; }, 1800);
    }));
  }

  {% if view == "feed" %}
  let all = [], shown = 60;
  const status = new Set();
  const GROUPS = {"Available":["AVAILABLE","LIKELY_AVAILABLE"],"Dropping":["PENDING_DELETE","REDEMPTION","EXPIRED_GRACE"],
    "Expiring":["EXPIRING_SOON"],"Parked / dead":["PARKED","DEAD_SITE"]};
  const DEFAULT_ON = ["Available", "Dropping"];
  $("chips").innerHTML = Object.keys(GROUPS).map(g => `<button type="button" class="chip" aria-pressed="${DEFAULT_ON.includes(g)}" data-g="${g}">${g}</button>`).join("");
  DEFAULT_ON.forEach(g => GROUPS[g].forEach(s => status.add(s)));
  const SORTS = {
    value: (a, b) => (b.value_ratio || 0) * (0.3 + (b.quality || 0) / 100) - (a.value_ratio || 0) * (0.3 + (a.quality || 0) / 100),
    quality: (a, b) => (b.quality || 0) - (a.quality || 0),
    drops: (a, b) => (a.expiry || "9999").localeCompare(b.expiry || "9999"),
    ref_subnets: (a, b) => (num(b.ref_subnets) || 0) - (num(a.ref_subnets) || 0),
    authority: (a, b) => (num(b.authority) || 0) - (num(a.authority) || 0),
    real_years: (a, b) => (num(b.real_years) || num(b.wb_years) || 0) - (num(a.real_years) || num(a.wb_years) || 0),
    price: (a, b) => (parseFloat(a.sd_price) || 1e9) - (parseFloat(b.sd_price) || 1e9),
    domain: (a, b) => a.domain.localeCompare(b.domain),
  };
  function render() {
    const q = $("q").value.trim().toLowerCase();
    const tl = $("tld").value.toLowerCase().split(",").map(s => s.trim().replace(/^\./, "")).filter(Boolean);
    const ms = parseFloat($("minscore").value) || 0;
    const rows = all.filter(r => (!status.size || status.has(r.status)) && (!q || r.domain.includes(q))
      && (!tl.length || tl.includes(r.domain.split(".").slice(1).join(".")) || tl.includes(r.domain.split(".").pop()))
      && (r.quality || 0) >= ms && (!$("dfonly").checked || r.dofollow_sources > 0)
      && (!$("nospam").checked || !(r.flags || "").includes("spam"))
      && (!$("via").value || (r.via || "").split(",").includes($("via").value))
      && (r.value_ratio || 0) >= (parseFloat($("minratio").value) || 0)
      && ($("wikionly").checked || !r.wiki_only))
      .sort((a, b) => SORTS[$("sort").value](a, b) || (ORDER[a.status] ?? 9) - (ORDER[b.status] ?? 9));
    $("rows").innerHTML = rows.length ? rows.slice(0, shown).map(card).join("")
      : `<div class="empty">${all.length ? "Nothing matches these filters. Try lowering \"Worth at least\" under Filters." : "No finds yet - press Hunt now or wait for the next automatic hunt."}</div>`;
    $("more2").hidden = rows.length <= shown;
    $("count").innerHTML = `Showing <b>${Math.min(rows.length, shown)}</b> of <b>${rows.length}</b> matching finds`;
  }
  $("chips").addEventListener("click", e => { const b = e.target.closest(".chip"); if (!b) return;
    const on = b.getAttribute("aria-pressed") !== "true"; b.setAttribute("aria-pressed", String(on));
    GROUPS[b.dataset.g].forEach(s => on ? status.add(s) : status.delete(s)); shown = 60; render(); });
  ["q", "tld", "minscore", "minratio", "wikionly", "dfonly", "nospam", "via", "sort"].forEach(id => ["input", "change"].forEach(ev =>
    $(id).addEventListener(ev, () => { shown = 60; render(); })));
  $("moreBtn").addEventListener("click", () => { const open = $("more").hidden; $("more").hidden = !open;
    $("moreBtn").setAttribute("aria-expanded", String(open)); $("moreBtn").textContent = open ? "Hide filters" : "Filters"; });
  $("morebtn").addEventListener("click", () => { shown += 60; render(); });
  function setView(v) {
    $("rows").classList.toggle("list", v === "list");
    $("vGrid").setAttribute("aria-pressed", String(v !== "list"));
    $("vList").setAttribute("aria-pressed", String(v === "list"));
    try { localStorage.setItem("df-view", v); } catch (e) {}
  }
  $("vGrid").addEventListener("click", () => setView("grid"));
  $("vList").addEventListener("click", () => setView("list"));
  let saved = "grid";
  try { saved = localStorage.getItem("df-view") || "grid"; } catch (e) {}
  setView(saved);
  fetch("{{ url_for('feed_json') }}").then(r => r.json()).then(d => {
    all = d.rows || [];
    $("s-avail").textContent = all.filter(r => BUY.has(r.status)).length;
    $("s-drop").textContent = all.filter(r => ["PENDING_DELETE", "REDEMPTION", "EXPIRED_GRACE"].includes(r.status)).length;
    $("s-total").textContent = all.length;
    $("s-q60").textContent = all.filter(r => (r.quality || 0) >= 60).length;
    $("s-upd").textContent = d.updated ? new Date(d.updated).toLocaleString([], {dateStyle: "short", timeStyle: "short"}) : "pending";
    const vc = {};
    all.forEach(r => (r.via || "").split(",").filter(Boolean).forEach(v => vc[v] = (vc[v] || 0) + 1));
    [...$("via").options].forEach(o => {
      if (!o.dataset.name) o.dataset.name = o.textContent;
      if (!o.value) { o.textContent = `All sources (${all.length})`; return; }
      const n = vc[o.value] || 0;
      o.textContent = n ? `${o.dataset.name} (${n})` : `${o.dataset.name} (0 - next hunt)`;
      o.disabled = !n;
    });
    render();
  }).catch(() => { $("rows").innerHTML = `<div class="empty">Couldn't load finds. Refresh to try again.</div>`; });
  {% elif view == "job" %}
  const id = {{ job.id|tojson }};
  async function poll() {
    let j;
    try { j = await (await fetch(`/job/${id}.json`)).json(); } catch (e) { return setTimeout(poll, 4000); }
    $("log").textContent = j.log.join("\n"); $("log").scrollTop = $("log").scrollHeight;
    if (j.links) { $("links").textContent = j.links; $("dofollow").textContent = j.dofollow_links; $("domains").textContent = j.unique_domains; }
    if (j.state === "done") {
      $("status").textContent = j.rows.length ? "Done. Best candidates first." : "Done. Nothing free or dropping this time - press Hunt now again for a new random sample.";
      $("found").textContent = j.rows.length; $("actions").hidden = false;
      $("rows").innerHTML = j.rows.map(card).join("");
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
