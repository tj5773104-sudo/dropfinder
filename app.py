"""dropfinder web - a small Flask front end for dropfinder.py.

Run locally:   pip install -r requirements.txt && python app.py      (http://localhost:8000)
Production:    gunicorn app:app --workers 1 --threads 8 --timeout 120

Jobs run in background threads and live in memory, so keep ONE gunicorn worker
(use threads for concurrency). Limits are set with environment variables below.
"""
import io
import os
import threading
import time
import uuid
from collections import deque
from urllib.parse import urlparse

from flask import Flask, Response, abort, jsonify, redirect, render_template_string, request, url_for

import dropfinder as df


def env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


MAX_SEEDS = env_int("MAX_SEEDS", 10)
MAX_PAGES = env_int("MAX_PAGES_PER_SEED", 20)
MAX_DEPTH = env_int("MAX_DEPTH", 1)
MAX_DOMAINS = env_int("MAX_DOMAINS", 400)
MAX_WIKI = env_int("MAX_WIKI_ARTICLES", 200)
MAX_GITHUB = env_int("MAX_GITHUB_REPOS", 20)
MAX_RUNNING = env_int("MAX_RUNNING_JOBS", 2)
JOBS_PER_HOUR = env_int("JOBS_PER_IP_PER_HOUR", 5)
JOB_TTL = env_int("JOB_TTL_SECONDS", 6 * 3600)

app = Flask(__name__)
JOBS = {}
QUEUE = deque()
LOCK = threading.Lock()
RUNNING = threading.Semaphore(MAX_RUNNING)
IP_LOG = {}


def client_ip():
    fwd = request.headers.get("X-Forwarded-For", "")
    return fwd.split(",")[0].strip() if fwd else (request.remote_addr or "?")


def rate_ok(ip):
    now = time.time()
    with LOCK:
        hits = [t for t in IP_LOG.get(ip, []) if now - t < 3600]
        if len(hits) >= JOBS_PER_HOUR:
            IP_LOG[ip] = hits
            return False
        hits.append(now)
        IP_LOG[ip] = hits
        return True


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

    with RUNNING:
        job["state"] = "running"
        job["started"] = time.time()
        try:
            records = df.harvest(o["seeds"], o["depth"], o["max_pages"], delay=1.0, workers=4,
                                 wiki_categories=o["wiki"], wiki_limit=o["wiki_limit"],
                                 github_query=o["github"], github_limit=o["github_limit"],
                                 public_only=True, log=log)
            job["links"] = len(records)
            job["dofollow_links"] = sum(r["follow"] == "dofollow" for r in records)
            job["unique_domains"] = len({r["target_domain"] for r in records})
            log(f"{job['links']} links ({job['dofollow_links']} dofollow) to {job['unique_domains']} domains.")
            job["rows"] = df.check(records, dofollow_only=o["dofollow_only"], min_sources=o["min_sources"],
                                   tlds=o["tlds"], rdap_all=o["rdap_all"], expiring_days=o["expiring_days"],
                                   workers=20, wayback=o["wayback"], wayback_top=50,
                                   max_domains=MAX_DOMAINS, log=log)
            log(f"Done: {len(job['rows'])} candidate domains.")
            job["state"] = "done"
        except Exception as e:  # noqa: BLE001 - show any failure to the user
            log(f"Error: {type(e).__name__}: {e}")
            job["state"] = "error"
        job["ended"] = time.time()


def parse_form(f):
    errors = []
    seeds = []
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
    github = f.get("github", "").strip()[:100] or None
    if not (seeds or wiki or github):
        errors.append("Give at least one seed page, Wikipedia category or GitHub search.")

    def num(name, lo, hi, default):
        try:
            return max(lo, min(hi, int(f.get(name, default))))
        except ValueError:
            return default

    opts = dict(seeds=seeds, wiki=wiki, github=github,
                depth=num("depth", 0, MAX_DEPTH, min(1, MAX_DEPTH)),
                max_pages=num("max_pages", 1, MAX_PAGES, min(10, MAX_PAGES)),
                wiki_limit=MAX_WIKI, github_limit=MAX_GITHUB,
                min_sources=num("min_sources", 1, 10, 1),
                expiring_days=num("expiring_days", 1, 365, 45),
                tlds=[t for t in f.get("tlds", "").replace(" ", "").split(",") if t] or None,
                dofollow_only=bool(f.get("dofollow_only")), rdap_all=bool(f.get("rdap_all")),
                wayback=bool(f.get("wayback")))
    return opts, errors


@app.get("/")
def index():
    return render_template_string(PAGE, view="form", errors=[], form={}, limits=limits())


@app.post("/run")
def run():
    cleanup()
    opts, errors = parse_form(request.form)
    if not errors and not rate_ok(client_ip()):
        errors.append(f"Limit reached: {JOBS_PER_HOUR} runs per hour. Try again later.")
    if errors:
        return render_template_string(PAGE, view="form", errors=errors, form=request.form, limits=limits()), 400
    jid = uuid.uuid4().hex[:12]
    JOBS[jid] = dict(id=jid, state="queued", opts=opts, log=[], rows=[], created=time.time(), started=0, ended=0,
                     links=0, dofollow_links=0, unique_domains=0)
    threading.Thread(target=run_job, args=(jid,), daemon=True).start()
    return redirect(url_for("job_page", jid=jid))


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
                   unique_domains=j["unique_domains"], rows=j["rows"] if j["state"] == "done" else [])


@app.get("/job/<jid>.csv")
def job_csv(jid):
    j = get_job(jid)
    buf = io.StringIO()
    df.write_csv(j["rows"], buf)
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f'attachment; filename="dropfinder-{jid}.csv"'})


@app.get("/healthz")
def healthz():
    return "ok"


def limits():
    return dict(seeds=MAX_SEEDS, pages=MAX_PAGES, depth=MAX_DEPTH, domains=MAX_DOMAINS, per_hour=JOBS_PER_HOUR)


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>dropfinder - expired domains with backlinks</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Libre+Franklin:wght@400;600;800&display=swap" rel="stylesheet">
<style>
:root{color-scheme:light dark;--bg:light-dark(#fff8f5,#1c1512);--card:light-dark(#fff,#261d19);--ink:light-dark(#1a1817,#f3ebe6);
--mute:light-dark(#6b625d,#b3a69e);--line:light-dark(#ecd9d1,#3a2d27);--teal:light-dark(#0d7680,#4fb3bc);--claret:#990f3d}
*{box-sizing:border-box;min-width:0}
body{margin:0;background:var(--bg);color:var(--ink);font:16px/1.55 "Libre Franklin",system-ui,sans-serif}
header{padding:28px max(5vw,16px) 8px;max-width:1240px;margin:auto}
h1{font-weight:800;font-size:clamp(30px,5vw,46px);line-height:1.05;margin:0 0 8px;text-wrap:balance}
h1 span{color:var(--teal)}
.lede{color:var(--mute);max-width:66ch;margin:0}
main{padding:16px max(5vw,16px) 48px;max-width:1240px;margin:auto}
form{display:grid;gap:18px;grid-template-columns:minmax(0,1.3fr) minmax(0,1fr);background:var(--card);border:1px solid var(--line);border-radius:12px;padding:22px}
@media(max-width:760px){form{grid-template-columns:minmax(0,1fr)}}
label{display:block;font-weight:600;margin-bottom:4px}
.hint{font-size:13px;color:var(--mute);margin:4px 0 0}
textarea,input[type=text],input[type=number]{width:100%;font:16px/1.4 inherit;padding:10px 12px;border:1px solid var(--line);border-radius:8px;background:var(--bg);color:var(--ink)}
textarea{min-height:150px;font-family:ui-monospace,monospace;font-size:14px}
.row{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:12px}
.check{display:flex;gap:10px;align-items:flex-start;font-weight:400;margin:8px 0}
.check input{width:20px;height:20px;margin-top:2px;accent-color:var(--teal)}
button,.btn{display:inline-block;background:var(--teal);color:#fff;border:0;border-radius:8px;padding:12px 22px;font:600 16px inherit;cursor:pointer;text-decoration:none;min-height:44px}
button:focus-visible,.btn:focus-visible,input:focus-visible,textarea:focus-visible{outline:3px solid var(--claret);outline-offset:2px}
.errors{background:#990f3d14;border-left:4px solid var(--claret);padding:10px 14px;margin-bottom:14px;border-radius:6px}
.stats{display:flex;flex-wrap:wrap;gap:12px;margin:8px 0 18px}
.stat{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 16px;min-width:150px}
.stat b{display:block;font-size:28px;font-weight:800;line-height:1.1}
.stat span{font-size:13px;color:var(--mute)}
pre.log{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px;max-height:220px;overflow:auto;font-size:13px;white-space:pre-wrap}
.scroll{overflow-x:auto;border:1px solid var(--line);border-radius:10px;background:var(--card)}
table{border-collapse:collapse;width:100%;font-size:14px}
th,td{padding:9px 12px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}
th{font-weight:600;white-space:nowrap;position:sticky;top:0;background:var(--card)}
td.n{text-align:right;font-variant-numeric:tabular-nums}
td a{color:var(--teal);overflow-wrap:anywhere}
.tag{display:inline-block;font-size:12px;font-weight:600;padding:2px 8px;border-radius:99px;white-space:nowrap;background:var(--line)}
.AVAILABLE,.LIKELY_AVAILABLE{background:#0d768022;color:var(--teal)}
.PENDING_DELETE,.REDEMPTION{background:#990f3d1c;color:light-dark(#990f3d,#ff8fb1)}
.spin{display:inline-block;width:14px;height:14px;border:2px solid var(--line);border-top-color:var(--teal);border-radius:50%;animation:s 1s linear infinite;vertical-align:-2px;margin-right:6px}
@keyframes s{to{transform:rotate(360deg)}}@media(prefers-reduced-motion:reduce){.spin{animation:none}}
footer{color:var(--mute);font-size:13px;padding:0 max(5vw,16px) 32px;max-width:1240px;margin:auto}
footer a{color:inherit}
</style></head><body>
<header>
  <h1>Find <span>expired domains</span> that still have backlinks</h1>
  <p class="lede">Paste pages that link out (resource pages, blogrolls, .edu link lists). dropfinder reads every outbound link,
  marks it dofollow or nofollow, then checks DNS and RDAP to see which linked-to domains are free, dropping or about to expire.</p>
</header>
<main>
{% if view == "form" %}
  {% if errors %}<div class="errors">{% for e in errors %}<div>{{ e }}</div>{% endfor %}</div>{% endif %}
  <form method="post" action="{{ url_for('run') }}">
    <div>
      <label for="seeds">Seed pages, one per line</label>
      <textarea id="seeds" name="seeds" placeholder="https://example.edu/resources/links&#10;https://someblog.com/blogroll">{{ form.get('seeds','') }}</textarea>
      <p class="hint">Up to {{ limits.seeds }} pages. robots.txt is obeyed and each site is fetched at most once a second.</p>
      <div class="row" style="margin-top:14px">
        <div><label for="depth">Link hops</label><input id="depth" name="depth" type="number" min="0" max="{{ limits.depth }}" value="{{ form.get('depth', [1, limits.depth]|min) }}">
          <p class="hint">0 = only the page itself</p></div>
        <div><label for="max_pages">Pages per site</label><input id="max_pages" name="max_pages" type="number" min="1" max="{{ limits.pages }}" value="{{ form.get('max_pages', [10, limits.pages]|min) }}"></div>
      </div>
      <div style="margin-top:14px"><label for="wiki">Wikipedia categories (optional)</label>
        <input id="wiki" name="wiki" type="text" placeholder="Articles with dead external links from June 2024" value="{{ form.get('wiki','') }}">
        <p class="hint">Wikipedia links are always nofollow, but they're a good source of dead sites.</p></div>
      <div style="margin-top:14px"><label for="github">GitHub repo search (optional)</label>
        <input id="github" name="github" type="text" placeholder="awesome list" value="{{ form.get('github','') }}"></div>
    </div>
    <div>
      <div class="row">
        <div><label for="tlds">Only these TLDs</label><input id="tlds" name="tlds" type="text" placeholder="com,net,org,io" value="{{ form.get('tlds','') }}"></div>
        <div><label for="min_sources">Min. linking sites</label><input id="min_sources" name="min_sources" type="number" min="1" max="10" value="{{ form.get('min_sources', 1) }}"></div>
        <div><label for="expiring_days">"Expiring" window (days)</label><input id="expiring_days" name="expiring_days" type="number" min="1" max="365" value="{{ form.get('expiring_days', 45) }}"></div>
      </div>
      <label class="check"><input type="checkbox" name="dofollow_only" {{ 'checked' if form.get('dofollow_only') }}> Dofollow links only</label>
      <label class="check"><input type="checkbox" name="wayback" {{ 'checked' if form.get('wayback', 'on') }}> Add Wayback Machine history</label>
      <label class="check"><input type="checkbox" name="rdap_all" {{ 'checked' if form.get('rdap_all') }}> Also find registered domains expiring soon (slower)</label>
      <p class="hint">Each run checks up to {{ limits.domains }} domains. {{ limits.per_hour }} runs per hour per visitor.</p>
      <p style="margin-top:18px"><button type="submit">Find domains</button></p>
    </div>
  </form>
{% else %}
  <div id="status"><span class="spin"></span>Working... this usually takes 1-5 minutes. You can leave this page open or bookmark it.</div>
  <div class="stats">
    <div class="stat"><b id="links">-</b><span>outbound links</span></div>
    <div class="stat"><b id="dofollow">-</b><span>dofollow links</span></div>
    <div class="stat"><b id="domains">-</b><span>linked domains</span></div>
    <div class="stat"><b id="found">-</b><span>candidates found</span></div>
  </div>
  <pre class="log" id="log"></pre>
  <p id="actions" hidden><a class="btn" href="{{ url_for('job_csv', jid=job.id) }}">Download CSV</a>
     &nbsp; <a href="{{ url_for('index') }}">New search</a></p>
  <div class="scroll" id="tablewrap" hidden><table><thead><tr>
    <th>Domain</th><th>Status</th><th>Score</th><th>Dofollow sites</th><th>.edu/.gov</th><th>Expiry</th><th>Wayback</th><th>Best linking page</th><th>Anchor</th>
  </tr></thead><tbody id="rows"></tbody></table></div>
{% endif %}
</main>
<footer>Results come from public DNS, RDAP and the Wayback Machine and can be out of date; check a domain at your registrar before relying on it.
"Dofollow" is read from the page's HTML and is a strong filter, not a guarantee.</footer>
{% if view == "job" %}
<script>
{
  const id = {{ job.id|tojson }};
  const esc = s => String(s ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
  const $ = x => document.getElementById(x);
  const link = u => /^https?:\/\//.test(u) ? `<a href="${esc(u)}" rel="nofollow noopener" target="_blank">${esc(u)}</a>` : esc(u);
  async function poll() {
    let j;
    try { j = await (await fetch(`/job/${id}.json`)).json(); } catch (e) { return setTimeout(poll, 4000); }
    $("log").textContent = j.log.join("\n");
    $("log").scrollTop = $("log").scrollHeight;
    if (j.links) { $("links").textContent = j.links; $("dofollow").textContent = j.dofollow_links; $("domains").textContent = j.unique_domains; }
    if (j.state === "done") {
      $("status").textContent = j.rows.length ? "Done. Best candidates first." : "Done. No available or dropping domains were found this time - try more or older seed pages.";
      $("found").textContent = j.rows.length;
      $("actions").hidden = false;
      if (j.rows.length) {
        $("tablewrap").hidden = false;
        $("rows").innerHTML = j.rows.map(r => `<tr>
          <td><b>${esc(r.domain)}</b></td><td><span class="tag ${esc(r.status)}">${esc(r.status.replaceAll("_"," ").toLowerCase())}</span></td>
          <td class="n">${esc(r.score)}</td><td class="n">${esc(r.dofollow_sources)}</td><td class="n">${esc(r.edu_gov_sources)}</td>
          <td>${esc((r.expiry||"").slice(0,10))}</td><td>${r.wb_months ? esc(r.wb_first+"-"+r.wb_last+" ("+r.wb_months+" mo)") : ""}</td>
          <td>${link(r.best_source_url)}</td><td>${esc(r.best_anchor)}</td></tr>`).join("");
      }
    } else if (j.state === "error") {
      $("status").textContent = "The run failed - see the log below.";
      $("actions").hidden = false;
    } else setTimeout(poll, 2500);
  }
  poll();
}
</script>
{% endif %}
</body></html>"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), threaded=True)
