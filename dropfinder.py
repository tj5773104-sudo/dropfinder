#!/usr/bin/env python3
"""
dropfinder - find expired / available domains that are linked from pages you choose,
and tell you whether each link is dofollow.

Pipeline
  1. harvest : collect outbound links from seed pages (blogs, resource pages, .edu
               pages, awesome-lists), Wikipedia categories and GitHub READMEs.
               Each link is classified dofollow / nofollow from the rendered HTML.
  2. check   : for every linked-to domain, check DNS and RDAP to see whether it is
               unregistered, in redemption, pending delete, or about to expire.
               Optionally pulls Wayback Machine history.
  3. output  : a CSV ranked by availability and by a simple link-quality score.

Quick start
  pip install requests tldextract dnspython
  python dropfinder.py harvest --seeds seeds.txt --depth 1 --out links.jsonl
  python dropfinder.py check --links links.jsonl --out candidates.csv --wayback

  python dropfinder.py selftest      # offline tests, no network needed

"dofollow" here means: no rel=nofollow/ugc/sponsored on the link, no robots
nofollow on the page, and the source is not a platform known to nofollow
everything. It is read from static HTML, so links injected by JavaScript are
not seen. Treat it as a strong filter, not a guarantee.
"""
import argparse
import csv
import json
import os
import re
import sys
import threading
import time
import urllib.robotparser
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import requests
import tldextract

UA = "dropfinder/0.1 (personal research crawler; polite, obeys robots.txt)"
_TLD = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)

# Platforms that nofollow user-placed links. Links from these are useful for
# discovering candidates (and for referral traffic) but carry little direct SEO value.
NOFOLLOW_PLATFORMS = {"wikipedia.org", "wikimedia.org", "youtube.com", "reddit.com", "github.com"}
EDU_GOV_SUFFIXES = (".edu", ".gov", ".ac.uk", ".gov.uk", ".edu.au", ".gov.au", ".ac.nz", ".ac.jp", ".ac.in", ".edu.cn")
SKIP_EXT = re.compile(r"\.(pdf|jpe?g|png|gif|svg|zip|gz|mp4|mp3|css|js|xml|docx?|xlsx?|pptx?)(\?|$)", re.I)
NOFOLLOW_RELS = {"nofollow", "ugc", "sponsored"}


# --------------------------------------------------------------------------- helpers
def registrable(host):
    e = _TLD(host or "")
    return f"{e.domain}.{e.suffix}".lower() if e.domain and e.suffix else ""


class LinkParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links, self.meta_robots, self.base, self._cur = [], "", None, None

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "meta" and (a.get("name") or "").lower() in ("robots", "googlebot"):
            self.meta_robots += " " + (a.get("content") or "").lower()
        elif tag == "base" and a.get("href") and not self.base:
            self.base = a["href"]
        elif tag == "a" and a.get("href"):
            self._cur = {"href": a["href"], "rel": (a.get("rel") or "").lower().split(), "text": ""}

    def handle_data(self, data):
        if self._cur is not None and len(self._cur["text"]) < 200:
            self._cur["text"] += data

    def handle_endtag(self, tag):
        if tag == "a" and self._cur is not None:
            self._cur["text"] = " ".join(self._cur["text"].split())[:100]
            self.links.append(self._cur)
            self._cur = None


def extract_links(html, page_url, headers=None):
    """Return (external_link_records, internal_urls) for one page."""
    p = LinkParser()
    p.feed(html)
    base = urljoin(page_url, p.base) if p.base else page_url
    robots = p.meta_robots + " " + ((headers or {}).get("X-Robots-Tag", "") or "").lower()
    robot_tokens = set(re.split(r"[\s,]+", robots))
    page_nofollow = bool(robot_tokens & {"nofollow", "none"})
    src_dom = registrable(urlparse(page_url).hostname or "")
    platform_nofollow = src_dom in NOFOLLOW_PLATFORMS

    external, internal = [], []
    for l in p.links:
        href = l["href"].strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        url = urljoin(base, href)
        u = urlparse(url)
        if u.scheme not in ("http", "https") or not u.hostname:
            continue
        tgt = registrable(u.hostname)
        if not tgt:
            continue
        if tgt == src_dom:
            internal.append(url.split("#")[0])
            continue
        rel_hit = sorted(set(l["rel"]) & NOFOLLOW_RELS)
        if rel_hit:
            follow, reason = "nofollow", "rel=" + ",".join(rel_hit)
        elif page_nofollow:
            follow, reason = "nofollow", "page robots nofollow"
        elif platform_nofollow:
            follow, reason = "nofollow", "platform policy"
        else:
            follow, reason = "dofollow", "no nofollow signal found"
        external.append(dict(source_url=page_url, source_domain=src_dom, target_url=url,
                             target_domain=tgt, anchor=l["text"], follow=follow, reason=reason))
    return external, internal


def fetch_html(url, session, timeout=15, max_bytes=2_000_000, url_ok=None):
    """Fetch a page. If url_ok is given, redirects are followed by hand and every hop must pass it."""
    for _ in range(6):
        r = session.get(url, timeout=timeout, stream=True, allow_redirects=url_ok is None)
        if url_ok is not None and r.is_redirect:
            nxt = urljoin(url, r.headers.get("location", ""))
            r.close()
            if urlparse(nxt).scheme not in ("http", "https") or not url_ok(nxt):
                return None
            url = nxt
            continue
        break
    else:
        return None
    try:
        if "html" not in r.headers.get("content-type", "").lower() or r.status_code >= 400:
            return None
        raw = r.raw.read(max_bytes, decode_content=True)
        return raw.decode(r.encoding or "utf-8", errors="replace"), r.headers, r.url
    finally:
        r.close()


def is_public_host(host):
    """True if every address the host resolves to is a public internet address (SSRF guard)."""
    import ipaddress
    import socket
    try:
        infos = socket.getaddrinfo(host, None)
    except (socket.gaierror, UnicodeError):
        return False
    for info in infos:
        ip = ipaddress.ip_address(info[4][0].split("%")[0])
        if not ip.is_global:
            return False
    return bool(infos)


class Crawler:
    """Polite crawler: obeys robots.txt, rate-limits per host."""

    def __init__(self, delay=1.0, public_only=False):
        self.s = requests.Session()
        self.s.headers["User-Agent"] = UA
        self.s.max_redirects = 5
        self.delay, self.robots, self.last, self.lock = delay, {}, {}, threading.Lock()
        self.public_only, self.host_ok = public_only, {}

    def allowed(self, url):
        u = urlparse(url)
        if self.public_only:
            ok = self.host_ok.get(u.hostname)
            if ok is None:
                ok = self.host_ok[u.hostname] = is_public_host(u.hostname)
            if not ok:
                return False
        rp = self.robots.get(u.netloc)
        if rp is None:
            rp = urllib.robotparser.RobotFileParser()
            try:
                r = self.s.get(f"{u.scheme}://{u.netloc}/robots.txt", timeout=10,
                               allow_redirects=not self.public_only)
                rp.parse(r.text.splitlines() if r.status_code == 200 else [])
            except requests.RequestException:
                rp.parse([])
            self.robots[u.netloc] = rp
        return rp.can_fetch(UA, url)

    def get(self, url):
        if not self.allowed(url):
            return None
        host = urlparse(url).netloc
        with self.lock:
            slot = max(time.time(), self.last.get(host, 0) + self.delay)
            self.last[host] = slot
        if slot > time.time():
            time.sleep(slot - time.time())
        try:
            return fetch_html(url, self.s, url_ok=self.allowed if self.public_only else None)
        except requests.RequestException:
            return None

    def crawl(self, seed, depth, max_pages):
        host = urlparse(seed).netloc
        seen, queue, out, n = {seed}, [(seed, 0)], [], 0
        while queue and n < max_pages:
            url, d = queue.pop(0)
            res = self.get(url)
            if not res:
                continue
            n += 1
            text, headers, final = res
            ext, internal = extract_links(text, final, headers)
            out.extend(ext)
            if d < depth:
                for u in internal:
                    if u not in seen and urlparse(u).netloc == host and not SKIP_EXT.search(u):
                        seen.add(u)
                        queue.append((u, d + 1))
        return out


# --------------------------------------------------------------------------- harvesters
WIKI_API = "https://en.wikipedia.org/w/api.php"


def wikipedia_links(category, limit):
    """External links from articles in a Wikipedia category. All are nofollow by policy."""
    s = requests.Session()
    s.headers["User-Agent"] = UA
    titles, cont = [], {}
    while len(titles) < limit:
        r = s.get(WIKI_API, params={"action": "query", "list": "categorymembers", "cmtitle": category,
                                    "cmlimit": "500", "cmnamespace": "0", "format": "json", **cont}, timeout=30).json()
        titles += [m["title"] for m in r.get("query", {}).get("categorymembers", [])]
        if "continue" not in r:
            break
        cont = r["continue"]
    titles = titles[:limit]
    out = []
    for i in range(0, len(titles), 50):
        cont = {}
        while True:
            r = s.get(WIKI_API, params={"action": "query", "prop": "extlinks", "titles": "|".join(titles[i:i + 50]),
                                        "ellimit": "max", "format": "json", "formatversion": "2", **cont},
                      timeout=30).json()
            for p in r.get("query", {}).get("pages", []):
                src = "https://en.wikipedia.org/wiki/" + p["title"].replace(" ", "_")
                for el in p.get("extlinks", []):
                    url = el["url"]
                    url = "https:" + url if url.startswith("//") else url
                    dom = registrable(urlparse(url).hostname)
                    if dom and dom != "wikipedia.org":
                        out.append(dict(source_url=src, source_domain="wikipedia.org", target_url=url,
                                        target_domain=dom, anchor="", follow="nofollow", reason="platform policy"))
            if "continue" not in r:
                break
            cont = r["continue"]
    return out


def github_links(query, limit):
    """External links from READMEs of repos matching a GitHub search (set GITHUB_TOKEN for rate limits)."""
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/vnd.github+json"})
    if os.environ.get("GITHUB_TOKEN"):
        s.headers["Authorization"] = "Bearer " + os.environ["GITHUB_TOKEN"]
    r = s.get("https://api.github.com/search/repositories",
              params={"q": query, "sort": "stars", "per_page": min(limit, 100)}, timeout=30).json()
    out = []
    for repo in r.get("items", [])[:limit]:
        full = repo["full_name"]
        rr = s.get(f"https://api.github.com/repos/{full}/readme",
                   headers={"Accept": "application/vnd.github.html+json"}, timeout=30)
        if rr.status_code == 200:
            out.extend(extract_links(rr.text, f"https://github.com/{full}")[0])
    return out


# --------------------------------------------------------------------------- availability
class RateLimiter:
    def __init__(self, interval):
        self.interval, self.next, self.lock = interval, 0.0, threading.Lock()

    def wait(self):
        with self.lock:
            now = time.time()
            slot = max(now, self.next)
            self.next = slot + self.interval
        if slot > now:
            time.sleep(slot - now)


def load_rdap_bootstrap(session):
    data = session.get("https://data.iana.org/rdap/dns.json", timeout=30).json()
    m = {}
    for tlds, urls in data["services"]:
        base = next((u for u in urls if u.startswith("https")), urls[0])
        for t in tlds:
            m[t.lower()] = base
    return m


def parse_rdap(j):
    statuses = [s.lower() for s in j.get("status", [])]
    expiry = next((e.get("eventDate") for e in j.get("events", []) if e.get("eventAction") == "expiration"), "")
    state = "registered"
    if any("pending delete" in s for s in statuses):
        state = "pending_delete"
    elif any("redemption" in s for s in statuses):
        state = "redemption"
    return {"registration": state, "expiry": expiry or "", "statuses": ";".join(statuses)}


def rdap_lookup(domain, base, session, limiter, retries=3):
    url = base.rstrip("/") + "/domain/" + domain
    for i in range(retries):
        limiter.wait()
        try:
            r = session.get(url, timeout=20, headers={"Accept": "application/rdap+json"})
        except requests.RequestException:
            time.sleep(1 + i)
            continue
        if r.status_code == 404:
            return {"registration": "available"}
        if r.status_code == 429:
            time.sleep(5 * (i + 1))
            continue
        if r.status_code == 200:
            try:
                return parse_rdap(r.json())
            except ValueError:
                return {"registration": "unknown", "note": "bad rdap json"}
        return {"registration": "unknown", "note": f"rdap http {r.status_code}"}
    return {"registration": "unknown", "note": "rdap unreachable"}


def dns_status(domain, resolver):
    import dns.exception
    import dns.resolver
    try:
        resolver.resolve(domain, "NS")
        return "has_ns"
    except dns.resolver.NXDOMAIN:
        return "nxdomain"
    except dns.resolver.NoAnswer:
        return "no_ns_answer"
    except (dns.resolver.NoNameservers, dns.exception.Timeout):
        return "dns_error"


def wayback_summary(domain, session):
    try:
        r = session.get("https://web.archive.org/cdx/search/cdx",
                        params={"url": domain, "output": "json", "fl": "timestamp", "filter": "statuscode:200",
                                "collapse": "timestamp:6", "limit": "1000"}, timeout=30)
        rows = r.json()[1:]
    except (requests.RequestException, ValueError):
        return {}
    if not rows:
        return {"wb_first": "", "wb_last": "", "wb_months": 0}
    return {"wb_first": rows[0][0][:4], "wb_last": rows[-1][0][:4], "wb_months": len(rows)}


def label(rec, expiring_days):
    reg = rec.get("registration")
    if reg == "available":
        return "AVAILABLE"
    if reg == "pending_delete":
        return "PENDING_DELETE"
    if reg == "redemption":
        return "REDEMPTION"
    if reg == "no_rdap" and rec["dns"] == "nxdomain":
        return "LIKELY_AVAILABLE"
    if reg == "registered" and rec.get("expiry"):
        try:
            exp = datetime.fromisoformat(rec["expiry"].replace("Z", "+00:00"))
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            if exp < datetime.now(timezone.utc) + timedelta(days=expiring_days):
                return "EXPIRING_SOON"
        except ValueError:
            pass
    return "REGISTERED"


# --------------------------------------------------------------------------- scoring
def source_weight(domain):
    return 3.0 if domain.endswith(EDU_GOV_SUFFIXES) else 1.0


def score_domain(records):
    """Best link per source domain: dofollow = 2 pts, nofollow = 0.25, x3 for .edu/.gov-type sources."""
    best = {}
    for r in records:
        pts = source_weight(r["source_domain"]) * (2.0 if r["follow"] == "dofollow" else 0.25)
        best[r["source_domain"]] = max(best.get(r["source_domain"], 0), pts)
    return round(sum(best.values()), 2)


PRIORITY = {"AVAILABLE": 0, "PENDING_DELETE": 1, "REDEMPTION": 2, "LIKELY_AVAILABLE": 3, "EXPIRING_SOON": 4, "REGISTERED": 9}


# --------------------------------------------------------------------------- library api
CSV_COLS = ["domain", "status", "score", "dofollow_sources", "nofollow_only_sources", "edu_gov_sources", "expiry",
            "dns", "wb_first", "wb_last", "wb_months", "best_source_url", "best_anchor", "link_target_url",
            "more_sources"]


def harvest(seeds=(), depth=1, max_pages=30, delay=1.0, workers=8, wiki_categories=(), wiki_limit=300,
            github_query=None, github_limit=30, public_only=False, log=lambda msg: None):
    """Collect outbound link records from seed pages, Wikipedia categories and GitHub READMEs."""
    crawler = Crawler(delay=delay, public_only=public_only)
    records = []
    seeds = list(seeds)
    if seeds:
        log(f"Crawling {len(seeds)} seed page(s)...")
        with ThreadPoolExecutor(max(1, workers)) as ex:
            for seed, recs in zip(seeds, ex.map(lambda s: crawler.crawl(s, depth, max_pages), seeds)):
                log(f"  {seed}: {len(recs)} external links")
                records.extend(recs)
    for cat in wiki_categories or []:
        log(f"Reading Wikipedia {cat}...")
        try:
            recs = wikipedia_links(cat, wiki_limit)
        except (requests.RequestException, ValueError) as e:
            log(f"  Wikipedia failed: {e}")
            recs = []
        log(f"  {len(recs)} external links")
        records.extend(recs)
    if github_query:
        log(f"Searching GitHub for '{github_query}'...")
        try:
            recs = github_links(github_query, github_limit)
        except (requests.RequestException, ValueError) as e:
            log(f"  GitHub failed: {e}")
            recs = []
        log(f"  {len(recs)} external links")
        records.extend(recs)
    return records


def check(records, dofollow_only=False, min_sources=1, tlds=None, resolver_ip=None, rdap_all=False,
          expiring_days=45, rdap_interval=0.3, workers=40, wayback=False, wayback_top=100, include_all=False,
          max_domains=None, log=lambda msg: None):
    """Check availability of every linked-to domain. Returns ranked result rows."""
    by = defaultdict(list)
    for r in records:
        by[r["target_domain"]].append(r)
    if dofollow_only:
        by = {d: [r for r in rs if r["follow"] == "dofollow"] for d, rs in by.items()}
        by = {d: rs for d, rs in by.items() if rs}
    domains = [d for d, rs in by.items() if len({r["source_domain"] for r in rs}) >= min_sources]
    if tlds:
        keep = {t.strip().lstrip(".").lower() for t in tlds if t.strip()}
        domains = [d for d in domains if d.rsplit(".", 1)[-1] in keep]
    if max_domains and len(domains) > max_domains:
        domains.sort(key=lambda d: -score_domain(by[d]))
        log(f"Limiting to the {max_domains} best-linked of {len(domains)} domains.")
        domains = domains[:max_domains]
    log(f"Checking {len(domains)} domains...")

    import dns.resolver
    resolver = dns.resolver.Resolver()
    if resolver_ip:
        resolver.nameservers = [resolver_ip]
    resolver.lifetime = 6
    session = requests.Session()
    session.headers["User-Agent"] = UA
    bootstrap = load_rdap_bootstrap(session)
    limiter = RateLimiter(rdap_interval)
    done = [0]
    lock = threading.Lock()

    def check_one(domain):
        rec = {"domain": domain, "dns": dns_status(domain, resolver)}
        if rec["dns"] in ("nxdomain", "dns_error") or rdap_all:
            base = bootstrap.get(domain.rsplit(".", 1)[-1])
            rec.update(rdap_lookup(domain, base, session, limiter) if base else {"registration": "no_rdap"})
        else:
            rec["registration"] = "registered"
        rec["status"] = label(rec, expiring_days)
        with lock:
            done[0] += 1
            if done[0] % 25 == 0 or done[0] == len(domains):
                log(f"  checked {done[0]}/{len(domains)}")
        return rec

    with ThreadPoolExecutor(max(1, workers)) as ex:
        results = list(ex.map(check_one, domains))

    rows = []
    for rec in results:
        if rec["status"] == "REGISTERED" and not include_all:
            continue
        rs = by[rec["domain"]]
        df = {r["source_domain"] for r in rs if r["follow"] == "dofollow"}
        nf = {r["source_domain"] for r in rs} - df
        best = sorted(rs, key=lambda r: (r["follow"] != "dofollow", -source_weight(r["source_domain"])))[0]
        rows.append({"domain": rec["domain"], "status": rec["status"], "score": score_domain(rs),
                     "dofollow_sources": len(df), "nofollow_only_sources": len(nf),
                     "edu_gov_sources": sum(source_weight(s) > 1 for s in df | nf),
                     "expiry": rec.get("expiry", ""), "dns": rec["dns"],
                     "best_source_url": best["source_url"], "best_anchor": best["anchor"],
                     "link_target_url": best["target_url"],
                     "more_sources": " | ".join(sorted({r["source_url"] for r in rs})[:5])})
    rows.sort(key=lambda r: (PRIORITY.get(r["status"], 9), -r["score"]))

    if wayback and rows:
        log(f"Pulling Wayback history for top {min(len(rows), wayback_top)}...")
        for row in rows[:wayback_top]:
            row.update(wayback_summary(row["domain"], session))
    return rows


def write_csv(rows, f):
    w = csv.DictWriter(f, fieldnames=CSV_COLS, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)


# --------------------------------------------------------------------------- commands
def cmd_harvest(a):
    seeds = []
    if a.seeds:
        seeds = [l.strip() for l in open(a.seeds) if l.strip() and not l.startswith("#")]
    records = harvest(seeds, a.depth, a.max_pages, a.delay, a.workers, a.wikipedia_category, a.wiki_limit,
                      a.github_query, a.github_limit, log=lambda m: print(m, file=sys.stderr))
    with open(a.out, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    follow = sum(r["follow"] == "dofollow" for r in records)
    print(f"{len(records)} external links ({follow} dofollow) -> {len({r['target_domain'] for r in records})} "
          f"unique domains. Saved to {a.out}")


def cmd_check(a):
    recs = [json.loads(l) for l in open(a.links)]
    rows = check(recs, a.dofollow_only, a.min_sources, a.tlds.split(",") if a.tlds else None, a.resolver,
                 a.rdap_all, a.expiring_days, a.rdap_interval, a.workers, a.wayback, a.wayback_top, a.all,
                 log=lambda m: print(m, file=sys.stderr))
    with open(a.out, "w", newline="") as f:
        write_csv(rows, f)
    print(f"{len(rows)} candidates -> {a.out}")
    for r in rows[:15]:
        print(f"  {r['status']:<16} {r['domain']:<32} score={r['score']:<6} dofollow={r['dofollow_sources']}")



def selftest():
    html = """<html><head><meta name="robots" content="index,follow"></head><body>
    <a href="https://dead-example.com/page" rel="nofollow">x</a>
    <a href="https://gone-startup.io/">Gone <b>Startup</b></a>
    <a href="/internal">in</a><a href="mailto:a@b.c">m</a>
    <a href="https://sponsor.com" rel="sponsored noopener">s</a>
    <a href="https://old-lab.example.co.uk/x">lab</a></body></html>"""
    ext, internal = extract_links(html, "https://blog.mysite.com/post")
    by = {e["target_domain"]: e for e in ext}
    assert by["dead-example.com"]["follow"] == "nofollow"
    assert by["sponsor.com"]["follow"] == "nofollow"
    assert by["gone-startup.io"]["follow"] == "dofollow" and by["gone-startup.io"]["anchor"] == "Gone Startup"
    assert by["example.co.uk"]["follow"] == "dofollow"
    assert internal == ["https://blog.mysite.com/internal"]
    ext, _ = extract_links(html.replace("index,follow", "noindex,nofollow"), "https://blog.mysite.com/post")
    assert all(e["follow"] == "nofollow" for e in ext)
    ext, _ = extract_links(html, "https://en.wikipedia.org/wiki/X")
    assert all(e["follow"] == "nofollow" for e in ext)
    assert {e["target_domain"]: e["reason"] for e in ext}["gone-startup.io"] == "platform policy"
    ext, _ = extract_links(html, "https://x.org/", headers={"X-Robots-Tag": "noindex, nofollow"})
    assert all(e["follow"] == "nofollow" for e in ext)
    assert registrable("www.cs.mit.edu") == "mit.edu" and registrable("192.168.0.1") == ""
    p = parse_rdap({"status": ["client hold", "pending delete"],
                    "events": [{"eventAction": "expiration", "eventDate": "2026-12-01T00:00:00Z"}]})
    assert p["registration"] == "pending_delete" and p["expiry"].startswith("2026-12-01")
    assert label({"registration": "available", "dns": "nxdomain"}, 45) == "AVAILABLE"
    assert label({"registration": "no_rdap", "dns": "nxdomain"}, 45) == "LIKELY_AVAILABLE"
    soon = (datetime.now(timezone.utc) + timedelta(days=10)).isoformat()
    assert label({"registration": "registered", "dns": "has_ns", "expiry": soon}, 45) == "EXPIRING_SOON"
    recs = [dict(source_domain="mit.edu", follow="dofollow"), dict(source_domain="mit.edu", follow="nofollow"),
            dict(source_domain="blog.com", follow="nofollow")]
    assert score_domain(recs) == 6.25
    print("selftest: all checks passed")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    h = sub.add_parser("harvest", help="collect outbound links and classify dofollow/nofollow")
    h.add_argument("--seeds", help="text file, one page URL per line")
    h.add_argument("--depth", type=int, default=1, help="how many link-hops to follow within each seed's site")
    h.add_argument("--max-pages", type=int, default=30, help="max pages fetched per seed")
    h.add_argument("--delay", type=float, default=1.0, help="seconds between requests to the same host")
    h.add_argument("--workers", type=int, default=8)
    h.add_argument("--wikipedia-category", action="append", help='e.g. "Category:All articles with dead external links"')
    h.add_argument("--wiki-limit", type=int, default=300, help="max articles per category")
    h.add_argument("--github-query", help='GitHub repo search, e.g. "awesome list"')
    h.add_argument("--github-limit", type=int, default=30)
    h.add_argument("--out", default="links.jsonl")
    h.set_defaults(fn=cmd_harvest)

    c = sub.add_parser("check", help="find which linked-to domains are available or expiring")
    c.add_argument("--links", default="links.jsonl")
    c.add_argument("--out", default="candidates.csv")
    c.add_argument("--dofollow-only", action="store_true", help="ignore nofollow links entirely")
    c.add_argument("--min-sources", type=int, default=1, help="min distinct linking domains")
    c.add_argument("--tlds", help="comma list of TLD labels to keep, e.g. com,net,org,io")
    c.add_argument("--resolver", help="DNS server to use, e.g. 1.1.1.1")
    c.add_argument("--rdap-all", action="store_true", help="RDAP-check every domain (finds EXPIRING_SOON; slow)")
    c.add_argument("--expiring-days", type=int, default=45)
    c.add_argument("--rdap-interval", type=float, default=0.3, help="seconds between RDAP requests")
    c.add_argument("--workers", type=int, default=40)
    c.add_argument("--wayback", action="store_true", help="add Wayback Machine history to top results")
    c.add_argument("--wayback-top", type=int, default=100)
    c.add_argument("--all", action="store_true", help="include registered domains in the output")
    c.set_defaults(fn=cmd_check)

    sub.add_parser("selftest", help="offline tests").set_defaults(fn=lambda a: selftest())
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
