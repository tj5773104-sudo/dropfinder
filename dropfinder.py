#!/usr/bin/env python3
"""
dropfinder - find expired / dropping domains that still have links from high-authority pages,
and tell you whether each link is dofollow.

Pipeline
  1. harvest : collect outbound links from
                 - seed pages you choose (resource pages, blogrolls, .edu link lists)
                 - the OLDEST pages of ranked sites, found through their sitemaps (old pages = most dead links)
                 - Wikipedia "dead external links" categories
                 - GitHub READMEs
               Each link is classified dofollow / nofollow from the HTML, and each linking
               site gets an authority weight from its Tranco top-1M rank.
  2. check   : for every linked-to domain: DNS, then RDAP for unregistered / redemption /
               pending delete / expired-in-grace. Live-looking domains with strong links are
               also visited to spot dead or parked sites. Optional Wayback history,
               Open PageRank, StableDomains availability + price.
  3. hunt    : fully automatic: samples ranked sites + Wikipedia months, checks everything and
               merges results into data/candidates.json (run on a schedule by GitHub Actions).

Quick start
  pip install -r requirements.txt
  python dropfinder.py selftest
  python dropfinder.py hunt --budget-minutes 20 --sites 100
  python dropfinder.py harvest --seeds seeds.txt --sitemaps --out links.jsonl
  python dropfinder.py check --links links.jsonl --out candidates.csv --wayback --verify

"dofollow" = no rel=nofollow/ugc/sponsored on the link, no robots nofollow on the page, and the
source is not a platform that nofollows everything. Read from static HTML: a strong filter, not a guarantee.
"""
import argparse
import csv
import gzip
import html as htmllib
import io
import json
import os
import random
import re
import sys
import threading
import time
import urllib.robotparser
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from urllib.parse import quote, urljoin, urlparse

import requests
import tldextract

UA = "dropfinder/0.2 (+https://github.com/tj5773104-sudo/dropfinder; polite research crawler, obeys robots.txt)"
_TLD = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)
SD_REF = os.environ.get("SD_REF", "wfmff7tz")

NOFOLLOW_PLATFORMS = {"wikipedia.org", "wikimedia.org", "youtube.com", "reddit.com", "github.com", "medium.com",
                      "quora.com", "twitter.com", "x.com", "facebook.com", "linkedin.com", "pinterest.com"}
EDU_GOV_SUFFIXES = (".edu", ".gov", ".ac.uk", ".gov.uk", ".edu.au", ".gov.au", ".ac.nz", ".ac.jp", ".ac.in", ".edu.cn",
                    ".mil", ".int", ".nhs.uk", ".gc.ca", ".europa.eu")
SKIP_EXT = re.compile(r"\.(pdf|jpe?g|png|gif|svg|webp|zip|gz|mp4|mp3|css|js|xml|json|docx?|xlsx?|pptx?)(\?|$)", re.I)
NOFOLLOW_RELS = {"nofollow", "ugc", "sponsored"}
# Domains that never make sense as targets (infrastructure, shorteners, giants).
IGNORE_TARGETS = {"google.com", "googleapis.com", "gstatic.com", "googleusercontent.com", "doubleclick.net",
                  "cloudflare.com", "cloudfront.net", "amazonaws.com", "akamaihd.net", "akamaized.net", "fbcdn.net",
                  "bit.ly", "t.co", "goo.gl", "tinyurl.com", "ow.ly", "doi.org", "archive.org", "wikipedia.org",
                  "wikimedia.org", "wikidata.org", "w3.org", "schema.org", "apple.com", "microsoft.com",
                  "amazon.com", "facebook.com", "twitter.com", "x.com", "instagram.com", "youtube.com",
                  "linkedin.com", "github.com", "jsdelivr.net", "unpkg.com", "gravatar.com", "wordpress.org",
                  "wp.com", "addtoany.com", "sharethis.com", "feedburner.com", "creativecommons.org"}
# Zones the public can't register in (government, education, military...).
RESTRICTED_RE = re.compile(r"\.(gov|edu|mil|ac|nhs|police|sch|k12|gob|gouv|govt|int|gc|fed|nic)(\.[a-z]{2,3})?$"
                           r"|\.(mod|parliament)\.uk$|\.(state|k12|ci|co|cc|lib|tec|dst)\.[a-z]{2}\.us$|^(gc|fed)\.(ca|us)$",
                           re.I)
RESOURCE_RE = re.compile(r"(link|resource|blogroll|partner|directory|friends|useful|recommended|sites|websites|"
                         r"tools|references|bookmarks|further-reading|webliography|sponsor)", re.I)
SPAM_RE = re.compile(r"(casino|poker|slot|bet(ting)?|gambl|viagra|cialis|pharma|pills|porn|sex|xxx|escort|adult|"
                     r"payday|loan|replica|cbd|vape|crypto|forex|dating|hookup|weight-?loss|keto)", re.I)
PARK_RE = re.compile(r"(domain (name )?(is|may be) for sale|buy this domain|this domain (name )?(has expired|is parked|"
                     r"may be for sale)|domain has expired|parked (free|domain|by)|sedoparking|parkingcrew|bodis|"
                     r"afternic|dan\.com|hugedomains|domainmarket|undeveloped|godaddy\.com/forsale|"
                     r"expired domain|renew (it )?now|account suspended|suspendedpage)", re.I)


# --------------------------------------------------------------------------- helpers
def registrable(host):
    e = _TLD(host or "")
    return f"{e.domain}.{e.suffix}".lower() if e.domain and e.suffix else ""


def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def buy_url(domain):
    return (f"https://stabledomains.net/domain-search?q={quote(domain)}&ref={SD_REF}"
            "&utm_source=affiliate&utm_medium=referral&utm_campaign=dropfinder")


def name_flags(domain):
    label = domain.split(".")[0]
    flags = []
    if SPAM_RE.search(label):
        flags.append("spammy-name")
    if len(label) > 20:
        flags.append("long")
    if re.search(r"\d", label) and "-" in label:
        flags.append("digits+hyphens")
    return flags


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
    try:
        p.feed(html)
    except Exception:  # noqa: BLE001 - broken markup: keep what we parsed
        pass
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
        try:
            url = urljoin(base, href)
            u = urlparse(url)
            host = u.hostname
        except ValueError:
            continue
        if u.scheme not in ("http", "https") or not host:
            continue
        tgt = registrable(host)
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
                             target_domain=tgt, anchor=l["text"], follow=follow, reason=reason, via="crawl"))
    return external, internal


def safe_decode(raw, encoding):
    """Decode bytes with the server's charset if it is a real codec, else UTF-8."""
    import codecs
    try:
        codecs.lookup(encoding or "utf-8")
        enc = encoding or "utf-8"
    except (LookupError, TypeError):
        enc = "utf-8"
    return raw.decode(enc, errors="replace")


def read_capped(r, max_bytes, max_seconds=20):
    """Read a streamed response, stopping at max_bytes or after max_seconds of wall-clock time
    (defeats servers that drip data slowly to keep a connection open)."""
    buf, t_end = bytearray(), time.time() + max_seconds
    while len(buf) < max_bytes and time.time() < t_end:
        chunk = r.raw.read(min(65536, max_bytes - len(buf)), decode_content=True)
        if not chunk:
            break
        buf.extend(chunk)
    return bytes(buf)


def fetch(url, session, timeout=15, max_bytes=2_000_000, url_ok=None, kinds=("html",)):
    """GET a URL. Returns (text, headers, final_url) or None. Content-type must contain one of `kinds`
    (None = anything). If url_ok is given, redirects are followed by hand and every hop must pass it."""
    for _ in range(6):
        r = session.get(url, timeout=(6, timeout), stream=True, allow_redirects=url_ok is None)
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
        ctype = r.headers.get("content-type", "").lower()
        if r.status_code >= 400 or (kinds and not any(k in ctype for k in kinds)):
            return None
        raw = read_capped(r, max_bytes)
        if raw[:2] == b"\x1f\x8b":
            try:
                raw = gzip.decompress(raw)[:max_bytes * 5]
            except OSError:
                return None
        return safe_decode(raw, r.encoding), r.headers, r.url
    finally:
        r.close()


def fetch_html(url, session, timeout=15, max_bytes=2_000_000, url_ok=None):
    return fetch(url, session, timeout, max_bytes, url_ok, ("html",))


_PUBLIC_CACHE = {}


def is_public_host(host):
    """True if every address the host resolves to is a public internet address (SSRF guard)."""
    import ipaddress
    import socket
    if host in _PUBLIC_CACHE:
        return _PUBLIC_CACHE[host]
    try:
        infos = socket.getaddrinfo(host, None)
        ok = bool(infos) and all(ipaddress.ip_address(i[4][0].split("%")[0]).is_global for i in infos)
    except (socket.gaierror, UnicodeError, ValueError):
        ok = False
    _PUBLIC_CACHE[host] = ok
    return ok


def public_url(url):
    return is_public_host(urlparse(url).hostname or "")


# --------------------------------------------------------------------------- authority
class Authority:
    """Tranco top-1M rank as a site-authority proxy (https://tranco-list.eu)."""
    URL = "https://tranco-list.eu/top-1m.csv.zip"

    def __init__(self, limit=None):
        self.limit = limit or int(os.environ.get("TRANCO_LIMIT", 300_000))
        self.rank, self.loaded, self.error, self.lock = {}, False, "", threading.Lock()

    def load(self, log=lambda m: None):
        with self.lock:
            if self.loaded:
                return self
            self.loaded = True
            try:
                log("Loading Tranco top-sites ranking...")
                r = requests.get(self.URL, timeout=90, headers={"User-Agent": UA})
                r.raise_for_status()
                z = zipfile.ZipFile(io.BytesIO(r.content))
                with z.open(z.namelist()[0]) as f:
                    for line in io.TextIOWrapper(f, "utf-8", errors="replace"):
                        rk, _, dom = line.strip().partition(",")
                        if not rk.isdigit():
                            continue
                        if int(rk) > self.limit:
                            break
                        reg = registrable(dom)
                        if reg and reg not in self.rank:
                            self.rank[reg] = int(rk)
                log(f"  {len(self.rank):,} ranked sites loaded.")
            except Exception as e:  # noqa: BLE001
                self.error = f"{type(e).__name__}: {e}"
                log(f"  Tranco unavailable ({self.error}); authority falls back to .edu/.gov only.")
        return self

    def get(self, domain):
        return self.rank.get(domain)


AUTH = Authority()


def source_weight(domain):
    """Authority multiplier for a linking site: Tranco tier, boosted for .edu/.gov-type sites."""
    rk = AUTH.get(domain)
    if rk is None:
        w = 1.0
    elif rk <= 1_000:
        w = 6.0
    elif rk <= 10_000:
        w = 4.5
    elif rk <= 100_000:
        w = 3.0
    else:
        w = 2.0
    if domain.endswith(EDU_GOV_SUFFIXES):
        w = max(w, 3.0) * 1.5
    return w


def score_domain(records):
    """Best link per linking site: authority weight x (1.0 dofollow, 0.15 nofollow), summed over sites."""
    best = {}
    for r in records:
        pts = source_weight(r["source_domain"]) * (1.0 if r["follow"] == "dofollow" else 0.15)
        best[r["source_domain"]] = max(best.get(r["source_domain"], 0), pts)
    return round(sum(best.values()), 2)


def open_pagerank(domains, key, session):
    """Open PageRank (0-10) + referring domains from Keywords Everywhere (free key: 30k domains/month).
    Returns {domain: {"opr": float, "ref_domains": int}}."""
    out = {}
    if not key:
        return out
    api = os.environ.get("OPR_API_URL", "https://openpagerank.keywordseverywhere.com") + "/v1/domains/bulk"
    for i in range(0, len(domains), 100):
        chunk = domains[i:i + 100]
        try:
            r = session.post(api, json={"domains": chunk, "include_history": False}, timeout=60,
                             headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"})
            if r.status_code != 200:
                continue
            for it in r.json().get("results", []):
                d = str(it.get("domain", "")).lower()
                if d and it.get("open_page_rank") is not None:
                    out[d] = {"opr": round(float(it["open_page_rank"]), 2),
                              "ref_domains": int(it.get("referring_domains") or 0)}
        except (requests.RequestException, ValueError, TypeError):
            pass
    return out


# --------------------------------------------------------------------------- crawling
LOC_BLOCK = re.compile(r"<(?:\w+:)?(url|sitemap)\b[^>]*>(.*?)</(?:\w+:)?\1>", re.S | re.I)
LOC_TAG = re.compile(r"<(?:\w+:)?loc>\s*(.*?)\s*</(?:\w+:)?loc>", re.S | re.I)
LASTMOD_TAG = re.compile(r"<(?:\w+:)?lastmod>\s*(.*?)\s*</(?:\w+:)?lastmod>", re.S | re.I)


def parse_sitemap(xml):
    """Return [(kind, loc, lastmod)] where kind is 'url' or 'sitemap'. Regex based: no entity expansion."""
    out = []
    for kind, body in LOC_BLOCK.findall(xml):
        loc = LOC_TAG.search(body)
        if loc:
            lm = LASTMOD_TAG.search(body)
            out.append((kind.lower(), htmllib.unescape(loc.group(1).strip()), lm.group(1).strip() if lm else ""))
    return out


class Crawler:
    """Polite crawler: obeys robots.txt, rate-limits per host, optional SSRF guard and deadline."""

    def __init__(self, delay=1.0, public_only=False, deadline=None):
        self.s = requests.Session()
        self.s.headers["User-Agent"] = UA
        self.s.max_redirects = 5
        self.delay, self.robots, self.last, self.lock = delay, {}, {}, threading.Lock()
        self.public_only, self.deadline = public_only, deadline
        self._local = threading.local()

    def expired(self):
        now = time.time()
        site_dl = getattr(self._local, "site_deadline", None)
        return (self.deadline is not None and now > self.deadline) or (site_dl is not None and now > site_dl)

    def robots_for(self, url):
        u = urlparse(url)
        rp = self.robots.get(u.netloc)
        if rp is None:
            rp = urllib.robotparser.RobotFileParser()
            try:
                r = self.s.get(f"{u.scheme}://{u.netloc}/robots.txt", timeout=10, allow_redirects=not self.public_only)
                rp.parse(r.text.splitlines() if r.status_code == 200 else [])
            except requests.RequestException:
                rp.parse([])
            self.robots[u.netloc] = rp
        return rp

    def allowed(self, url):
        u = urlparse(url)
        if not u.hostname or (self.public_only and not is_public_host(u.hostname)):
            return False
        return self.robots_for(url).can_fetch(UA, url)

    def _wait_turn(self, host):
        with self.lock:
            slot = max(time.time(), self.last.get(host, 0) + self.delay)
            self.last[host] = slot
        if slot > time.time():
            time.sleep(slot - time.time())

    def get(self, url, kinds=("html",), max_bytes=2_000_000):
        if self.expired() or not self.allowed(url):
            return None
        self._wait_turn(urlparse(url).netloc)
        try:
            return fetch(url, self.s, url_ok=self.allowed if self.public_only else None, kinds=kinds,
                         max_bytes=max_bytes)
        except requests.RequestException:
            return None

    def crawl(self, seed, depth, max_pages):
        """Breadth-first crawl inside one site, starting at seed."""
        host = urlparse(seed).netloc
        seen, queue, out, n = {seed}, [(seed, 0)], [], 0
        while queue and n < max_pages and not self.expired():
            url, d = queue.pop(0)
            res = self.get(url)
            if not res:
                continue
            n += 1
            text, headers, final = res
            ext, internal = extract_links(text, final, headers)
            out.extend(ext)
            if d < depth:
                internal.sort(key=lambda u: not RESOURCE_RE.search(urlparse(u).path))
                for u in internal:
                    if u not in seen and urlparse(u).netloc == host and not SKIP_EXT.search(u):
                        seen.add(u)
                        queue.append((u, d + 1))
        return out

    def old_pages(self, root, k=8, max_child=3):
        """The k oldest page URLs from a site's sitemap(s) (by <lastmod>; undated entries count as old)."""
        u = urlparse(root)
        base = f"{u.scheme}://{u.netloc}"
        maps = list(self.robots_for(base + "/").site_maps() or []) or [base + "/sitemap.xml"]
        site = registrable(u.hostname or "")
        urls, todo, seen = [], maps[:3], set()
        while todo and len(seen) < max_child + 3 and not self.expired():
            sm = todo.pop(0)
            if sm in seen:
                continue
            seen.add(sm)
            res = self.get(sm, kinds=None, max_bytes=5_000_000)
            if not res:
                continue
            entries = parse_sitemap(res[0])
            children = sorted([e for e in entries if e[0] == "sitemap"], key=lambda e: (e[2] or "0000", e[1]))
            # prefer post/article sitemaps, oldest first
            children.sort(key=lambda e: (not re.search(r"post|article|blog|news|page", e[1], re.I),))
            todo.extend(c[1] for c in children[:max_child])
            urls.extend(e for e in entries if e[0] == "url")
        urls = [e for e in urls if registrable(urlparse(e[1]).hostname or "") == site and not SKIP_EXT.search(e[1])]
        urls.sort(key=lambda e: e[2] or "0000")
        # resource / links pages first (most outbound links), then the oldest pages
        urls.sort(key=lambda e: not RESOURCE_RE.search(urlparse(e[1]).path))
        picked, seen_u = [], set()
        for _, loc, _ in urls:
            if loc not in seen_u:
                seen_u.add(loc)
                picked.append(loc)
            if len(picked) >= k:
                break
        return picked

    def crawl_site(self, root, old_k=12, fallback_pages=8, site_budget=90):
        """Old pages from the sitemap (best for dead links) plus the homepage; falls back to a shallow crawl.
        Gives up on a site after site_budget seconds so slow sites can't stall a run."""
        self._local.site_deadline = time.time() + site_budget
        try:
            return self._crawl_site(root, old_k, fallback_pages)
        finally:
            self._local.site_deadline = None

    def _crawl_site(self, root, old_k, fallback_pages):
        if not root.startswith(("http://", "https://")):
            root = "https://" + root.strip("/") + "/"
        out = []
        pages = self.old_pages(root, k=old_k) if old_k else []
        if pages:
            for p in pages + [root]:
                if self.expired():
                    break
                res = self.get(p)
                if res:
                    out.extend(extract_links(res[0], res[2], res[1])[0])
        else:
            out = self.crawl(root, 1, fallback_pages)
        return out


# --------------------------------------------------------------------------- harvesters
WIKI_API = "https://en.wikipedia.org/w/api.php"
MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October",
          "November", "December"]


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
                    url = el.get("url", "")
                    url = "https:" + url if url.startswith("//") else url
                    try:
                        dom = registrable(urlparse(url).hostname)
                    except ValueError:
                        continue
                    if dom and dom != "wikipedia.org":
                        out.append(dict(source_url=src, source_domain="wikipedia.org", target_url=url,
                                        target_domain=dom, anchor="", follow="nofollow", reason="platform policy",
                                        via="wikipedia"))
            if "continue" not in r:
                break
            cont = r["continue"]
    return out


def random_dead_link_categories(n, rng=random):
    """Wikipedia maintenance categories 'Articles with dead external links from <Month> <Year>'."""
    year_now = datetime.now(timezone.utc).year
    cats = {f"Category:Articles with dead external links from {rng.choice(MONTHS)} {rng.randint(2010, year_now - 1)}"
            for _ in range(n * 3)}
    return sorted(cats)[:n]


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


# ---- Common Crawl web-graph authority (free, no key) ------------------------------------------
CC_INDEX = "https://index.commoncrawl.org/graphinfo.json"
CC_BASE = "https://data.commoncrawl.org/projects/hyperlinkgraph"


def cc_authority(pos, total):
    """0-10 score from a harmonic-centrality position among `total` domains (log scale, like PageRank)."""
    import math
    if not pos or pos < 1:
        return None
    return round(max(0.0, 10 * (1 - math.log10(pos) / math.log10(max(total, 10)))), 1)


def cc_web_ranks(domains, session=None, log=lambda m: None, max_seconds=1200, url=None, window=None, window_out=None):
    """Stream Common Crawl's latest domain-level ranks file (several GB, gzip) once and return
    {domain: {"cc_rank": harmonic-centrality position, "cc_pr_rank": pagerank position, "cc_authority": 0-10}}.
    Dead domains keep a rank while other sites still link to them - that is the point."""
    import zlib
    want = {".".join(reversed(d.split("."))).encode(): d for d in domains}
    if not want and not window:
        return {}
    s = session or requests.Session()
    s.headers["User-Agent"] = UA
    if url is None:
        try:
            rel = s.get(CC_INDEX, timeout=30).json()[0]["id"]
        except (requests.RequestException, ValueError, KeyError, IndexError) as e:
            log(f"  Common Crawl index unavailable: {e}")
            return {}
        url = f"{CC_BASE}/{rel}/domain/{rel}-domain-ranks.txt.gz"
        log(f"  streaming {rel} domain ranks...")
    found, total, buf, t_end = {}, 0, b"", time.time() + max_seconds
    dec = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        r = s.get(url, stream=True, timeout=(15, 120))
        for chunk in r.iter_content(1 << 20):
            data = buf + dec.decompress(chunk)
            lines = data.split(b"\n")
            buf = lines.pop()
            for ln in lines:
                parts = ln.split(b"\t", 5)
                if len(parts) < 5 or not parts[0][:1].isdigit():
                    continue
                idx = total  # 0-based position in the file (the file is sorted by harmonic centrality)
                total += 1
                key = parts[4].strip()
                if key in want:
                    found[want[key]] = (int(parts[0]), int(parts[2]))
                if window and window_out is not None and window[0] <= idx < window[1]:
                    window_out.append((".".join(reversed(key.decode("utf-8", "replace").split("."))),
                                       int(parts[0]), int(parts[2])))
            if time.time() > t_end:
                log("  time limit reached while streaming ranks; using what was read")
                break
            if not want and window and total >= window[1]:
                break
        r.close()
    except (requests.RequestException, zlib.error) as e:
        log(f"  Common Crawl ranks stream failed: {e}")
    total = max(total, 1)
    out = {d: {"cc_rank": hc, "cc_pr_rank": pr, "cc_authority": cc_authority(hc, total)} for d, (hc, pr) in found.items()}
    log(f"  {total:,} domains scanned, {len(out)} of {len(want)} candidates ranked")
    return out


# ---- authority-first sources (v3) ----------------------------------------------------------------
# Start from domains that already have strong backlink profiles, then find the dead ones.
SD_STATUS = {"available": "AVAILABLE", "pending_delete": "PENDING_DELETE", "redemption": "REDEMPTION",
             "expired_grace": "EXPIRED_GRACE"}


def stabledomains_feed(max_pages=40, session=None, log=lambda m: None, **filters):
    """StableDomains quality drop feed (free, 25 rows a page, paged by cursor). Each row already has a
    0-100 quality score built from Wayback real-site history, Majestic referring subnets and spam checks."""
    s = session or requests.Session()
    s.headers["User-Agent"] = UA
    out, cursor = [], None
    for _ in range(max_pages):
        params = dict(filters, **({"cursor": cursor} if cursor else {}))
        try:
            j = s.get("https://stabledomains.net/api/v1/radar", params=params, timeout=30).json()
            if not isinstance(j, dict):
                raise ValueError("unexpected response")
        except (requests.RequestException, ValueError, KeyError, TypeError) as e:
            log(f"  StableDomains feed stopped: {e}")
            break
        out.extend(j.get("items", []))
        cursor = j.get("next_cursor")
        if not cursor or not j.get("items"):
            break
        time.sleep(0.5)
    return out


def sd_item_to_row(it):
    d = str(it.get("domain", "")).lower()
    subnets = None
    for reason in it.get("quality_reasons") or []:
        m = re.search(r"([\d,]+) referring subnets", reason)
        if m:
            subnets = int(m.group(1).replace(",", ""))
    first = (it.get("archived_since") or "")[:4]
    return {"domain": d, "status": SD_STATUS.get(it.get("status"), "REGISTERED"), "score": 0.0,
            "quality": it.get("quality"), "quality_reasons": it.get("quality_reasons") or [],
            "dofollow_sources": 0, "nofollow_only_sources": 0, "edu_gov_sources": 0, "best_source_rank": "",
            "expiry": it.get("drops_at") or "", "dns": "", "site": "", "ref_subnets": subnets,
            "wb_first": first, "wb_years": (datetime.now(timezone.utc).year - int(first) + 1) if first else "",
            "wb_months": it.get("wayback_months") or "", "sd_price": str(it.get("price_usd") or ""),
            "sd_available": "yes" if it.get("status") == "available" else "",
            "value_usd": it.get("value_usd"), "flags": ",".join(name_flags(d)) or ("spam" if it.get("spam") else ""),
            "via": "stabledomains", "buy_url": buy_url(d),
            "sources": [{"url": it.get("wayback_url") or f"https://web.archive.org/web/2*/{d}", "domain": "archive.org",
                         "follow": "", "via": "stabledomains", "rank": None, "anchor": "Wayback history"}],
            "best_source_url": "", "best_anchor": "", "link_target_url": "", "more_sources": ""}


def majestic_million(session=None, log=lambda m: None):
    """[(rank, domain, ref_subnets)] from the free Majestic Million list (top 1M sites by referring subnets).
    It keeps listing domains for a while after they die - that is what we are fishing for."""
    s = session or requests.Session()
    s.headers["User-Agent"] = UA
    try:
        r = s.get("https://downloads.majestic.com/majestic_million.csv", timeout=300)
        text = r.content.decode("utf-8", "replace")
    except requests.RequestException as e:
        log(f"  Majestic Million download failed: {e}")
        return []
    out = []
    for row in csv.DictReader(io.StringIO(text)):
        try:
            out.append((int(row["GlobalRank"]), row["Domain"].strip().lower(), int(row.get("RefSubNets") or 0)))
        except (KeyError, ValueError):
            continue
    log(f"  Majestic Million: {len(out):,} domains")
    return out


def dns_sweep(domains, workers=150, log=lambda m: None):
    """Fast parallel NS lookup; returns the domains that do NOT resolve (nxdomain / no NS / errors)."""
    resolvers = [make_resolver(ns) for ns in ("1.1.1.1", "8.8.8.8", "9.9.9.9")]
    done, lock = [0], threading.Lock()

    def one(i_d):
        i, d = i_d
        st = dns_status(d, resolvers[i % 3])
        if st == "dns_error":  # retry once on another resolver before trusting it
            st = dns_status(d, resolvers[(i + 1) % 3])
        with lock:
            done[0] += 1
            if done[0] % 10000 == 0:
                log(f"    DNS {done[0]:,}/{len(domains):,}")
        return d, st

    with ThreadPoolExecutor(workers) as ex:
        res = list(ex.map(one, enumerate(domains)))
    return {d: st for d, st in res if st != "has_ns"}


WB_REAL_MIN_WORDS = 120


def wayback_real_history(domain, session, samples=4):
    """Sample archived homepages across the years and classify each as real content vs parked/for-sale/empty.
    Returns {"real_ratio", "real_years", "wb_first", "wb_last", "wb_years", "parked_only"}."""
    try:
        r = session.get("https://web.archive.org/cdx/search/cdx", timeout=30, params={
            "url": domain, "output": "json", "fl": "timestamp,original", "filter": "statuscode:200",
            "collapse": "timestamp:4", "limit": "60"})
        rows = [(str(x[0]), str(x[1]) if len(x) > 1 else f"http://{domain}/") for x in r.json()[1:]
                if x and str(x[0])[:4].isdigit()]
    except (requests.RequestException, ValueError, TypeError, IndexError):
        return {}
    if not rows:
        return {"real_ratio": 0, "real_years": 0, "wb_years": 0, "parked_only": False, "never_archived": True}
    years = sorted({int(t[:4]) for t, _ in rows})
    step = max(1, len(rows) // samples)
    picks = rows[::step][:samples]
    real = 0
    for ts, orig in picks:
        try:
            pr = session.get(f"https://web.archive.org/web/{ts}id_/{orig}", timeout=25, stream=True)
            body = safe_decode(read_capped(pr, 400_000, 15), pr.encoding)
            pr.close()
        except requests.RequestException:
            continue
        text = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", body)
        text = re.sub(r"<[^>]+>", " ", text)
        words = len(re.findall(r"[A-Za-zÀ-ɏ]{3,}", text))
        if words >= WB_REAL_MIN_WORDS and not PARK_RE.search(text[:20000]):
            real += 1
        time.sleep(0.7)
    ratio = real / max(1, len(picks))
    span = years[-1] - years[0] + 1
    return {"real_ratio": round(ratio, 2), "real_years": round(span * ratio, 1), "wb_first": str(years[0]),
            "wb_last": str(years[-1]), "wb_years": span, "parked_only": real == 0, "never_archived": False}


def quality_score(r):
    """0-100: real site history, referring networks, web-graph authority, discovered dofollow links, minus spam."""
    import math
    reasons = []
    q = 0.0
    ry = r.get("real_years")
    if ry not in (None, ""):
        pts = min(float(ry), 15) / 15 * 35
        q += pts
        reasons.append(f"~{ry}y of real site history in the Wayback Machine")
    elif r.get("wb_years"):
        q += min(float(r["wb_years"]), 15) / 15 * 20
        reasons.append(f"archived {r['wb_years']}y (content not verified)")
    subnets = r.get("ref_subnets")
    if subnets:
        q += min(1.0, math.log10(subnets + 1) / 3) * 25
        reasons.append(f"{subnets} referring subnets (Majestic)")
    auth = r.get("opr") if r.get("opr") not in (None, "") else r.get("cc_authority")
    if auth not in (None, ""):
        q += float(auth) * 2
        reasons.append(f"web-graph authority {auth}/10 (Common Crawl)")
    if r.get("score"):
        q += min(float(r["score"]), 10)
        if r.get("dofollow_sources"):
            reasons.append(f"{r['dofollow_sources']} dofollow linking site(s) found")
    if "wikidata" in (r.get("via") or ""):
        q += 8
        reasons.append("official site of a closed organisation (Wikidata)")
    flags = r.get("flags") or ""
    if "spam" in flags:
        q -= 30
        reasons.append("spammy name")
    if r.get("parked_only"):
        q -= 30
        reasons.append("archive shows only parked / for-sale pages")
    if r.get("never_archived"):
        q -= 15
        reasons.append("never archived")
    if "digits+hyphens" in flags:
        q -= 10
    return max(0, min(100, round(q))), reasons


def graph_rows(candidates, via, info, stamp):
    """Rows for domains found by an authority-first scan (no crawled linking page)."""
    rows = []
    for d, rec in candidates.items():
        meta = info.get(d, {})
        rows.append({"domain": d, "status": rec["status"], "score": 0.0, "dofollow_sources": 0,
                     "nofollow_only_sources": 0, "edu_gov_sources": 0, "best_source_rank": "",
                     "expiry": rec.get("expiry", ""), "dns": rec.get("dns", ""), "site": "",
                     "flags": ",".join(name_flags(d)), "via": via, "buy_url": buy_url(d), **meta,
                     "sources": [{"url": f"https://web.archive.org/web/2*/{d}", "domain": "archive.org", "follow": "",
                                  "via": via, "rank": None, "anchor": "Wayback history"}],
                     "best_source_url": "", "best_anchor": "", "link_target_url": "", "more_sources": "",
                     "first_seen": stamp, "last_checked": stamp})
    return rows


# ---- value: is it worth the registration fee? -------------------------------------------------------
TLD_PRICE = {"com": 16.99, "net": 20.99, "org": 22.99, "io": 49.99, "co": 29.99, "info": 19.99, "uk": 9.99, "co.uk": 9.99}
TLD_VALUE = {"com": 1.0, "net": 0.55, "org": 0.6, "io": 0.7, "co": 0.5, "co.uk": 0.6, "uk": 0.5, "de": 0.5, "ai": 0.9}


def estimate_value(r):
    """Rough resale/SEO value in USD from referring networks, real history, authority and the name itself.
    StableDomains' own estimate wins when we have it."""
    import math
    if r.get("value_usd"):
        return int(r["value_usd"])
    d = r["domain"]
    sld, _, tld = d.partition(".")
    tl = TLD_VALUE.get(tld, 0.35)
    n = len(sld)
    ln = 1.8 if n <= 5 else 1.5 if n <= 7 else 1.2 if n <= 10 else 0.9 if n <= 14 else 0.6
    subnets = float(r.get("ref_subnets") or 0)
    real = float(r.get("real_years") or 0) or float(r.get("wb_years") or 0) * 0.5
    auth = float(r.get("cc_authority") or r.get("opr") or 0)
    links = float(r.get("dofollow_sources") or 0)
    core = 8 + 1.1 * subnets + 5 * min(real, 20) + 35 * auth + 12 * min(links, 10)
    flags = r.get("flags") or ""
    m = 1.0
    if "spam" in flags:
        m *= 0.1
    if r.get("parked_only"):
        m *= 0.3
    if re.search(r"\d", sld):
        m *= 0.7
    if "-" in sld:
        m *= 0.6
    return int(round(core * tl * ln * m / 5) * 5)


def reg_price(r):
    try:
        return float(r.get("sd_price") or 0) or TLD_PRICE.get(r["domain"].partition(".")[2], 25.0)
    except ValueError:
        return TLD_PRICE.get(r["domain"].partition(".")[2], 25.0)


def wiki_only(r):
    """Only evidence is a Wikipedia citation (nofollow) - weak unless other signals back it up."""
    vias = {v for v in (r.get("via") or "").split(",") if v}
    return vias == {"wikipedia"} and not r.get("ref_subnets") and float(r.get("cc_authority") or 0) < 2


# ---- defunct organisations (Wikidata) -------------------------------------------------------------
WD_TYPES = {"Q11032": "newspaper", "Q41298": "magazine", "Q4830453": "business", "Q6881511": "enterprise",
            "Q35127": "website", "Q163740": "nonprofit", "Q43229": "organization", "Q132241": "festival",
            "Q2001305": "TV channel", "Q14350": "radio station", "Q18127": "record label", "Q1137109": "video game developer",
            "Q215380": "band", "Q7275": "state agency", "Q327333": "government agency", "Q2085381": "publisher",
            "Q1331793": "media company", "Q15265344": "broadcaster", "Q2385804": "educational institution",
            "Q5633421": "scientific journal", "Q1002697": "periodical"}


def wikidata_defunct(per_type=3000, types=None, session=None, rng=random, log=lambda m: None):
    """Official websites of organisations Wikidata records as dissolved/closed (P576 + P856).
    Their domains often lapse while news, .edu and .gov pages keep linking to them."""
    s = session or requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/sparql-results+json"})
    out, seen = [], set()
    for qid in (types or list(WD_TYPES)):
        q = ("SELECT ?item ?itemLabel ?site ?end WHERE { ?item wdt:P31 wd:%s . ?item wdt:P856 ?site . "
             "?item wdt:P576 ?end . SERVICE wikibase:label { bd:serviceParam wikibase:language \"en\" } } LIMIT %d"
             % (qid, per_type))
        try:
            r = s.get("https://query.wikidata.org/sparql", params={"query": q, "format": "json"}, timeout=90)
            rows = r.json()["results"]["bindings"]
        except (requests.RequestException, ValueError, KeyError) as e:
            log(f"  Wikidata {WD_TYPES.get(qid, qid)}: failed ({type(e).__name__})")
            continue
        n0 = len(out)
        for b in rows:
            site = b.get("site", {}).get("value", "")
            label = b.get("itemLabel", {}).get("value", "")
            end = b.get("end", {}).get("value", "")[:4]
            item = b.get("item", {}).get("value", "")
            rec = _rec(item, "wikidata.org", site, f"{label} (closed {end})" if end else label, "wikidata",
                       follow="", reason=f"defunct {WD_TYPES.get(qid, '')}")
            if rec and rec["target_domain"] not in seen:
                seen.add(rec["target_domain"])
                rec["defunct"] = WD_TYPES.get(qid, "")
                out.append(rec)
        log(f"  Wikidata defunct {WD_TYPES.get(qid, qid)}: {len(out) - n0} domains")
        time.sleep(1.5)  # be gentle with the public endpoint
    return out


# ---- old articles on top publishers / government sites (via Wayback's URL index) ------------------
AUTHORITY_SITES = [
    # (url prefix whose OLD pages are still live, years to sample)
    ("news.bbc.co.uk/2/hi/", (2002, 2011)), ("www.bbc.co.uk/news/", (2010, 2014)),
    ("www.theguardian.com/technology/", (2009, 2014)), ("www.guardian.co.uk/technology/", (2005, 2010)),
    ("www.nytimes.com/20", (2005, 2012)), ("www.forbes.com/sites/", (2011, 2015)),
    ("www.wired.com/20", (2008, 2014)), ("techcrunch.com/20", (2007, 2013)), ("www.theverge.com/20", (2012, 2015)),
    ("www.telegraph.co.uk/technology/", (2008, 2013)), ("www.independent.co.uk/life-style/gadgets-and-tech/", (2008, 2014)),
    ("www.npr.org/templates/story/", (2004, 2012)), ("www.smithsonianmag.com/", (2008, 2014)),
    ("www.nature.com/news/", (2008, 2014)), ("www.scientificamerican.com/article/", (2008, 2014)),
    ("www.nasa.gov/", (2006, 2012)), ("www.loc.gov/", (2005, 2012)), ("www.nih.gov/news/", (2005, 2012)),
    ("www.gov.uk/government/news/", (2012, 2015)), ("www.ed.gov/", (2005, 2012)), ("www.epa.gov/", (2005, 2012)),
    ("www.mit.edu/", (2005, 2012)), ("news.mit.edu/20", (2010, 2014)), ("www.stanford.edu/", (2005, 2012)),
    ("www.harvard.edu/", (2005, 2012)), ("www.ox.ac.uk/", (2005, 2012)), ("www.ucl.ac.uk/", (2005, 2012)),
]


def wayback_old_urls(prefix, years, limit=40, session=None, rng=random):
    """Old article URLs under a prefix, from the Wayback Machine index (the live pages usually still exist)."""
    s = session or requests.Session()
    s.headers["User-Agent"] = UA
    y = rng.randint(years[0], years[1])
    try:
        r = s.get("https://web.archive.org/cdx/search/cdx", timeout=60, params={
            "url": prefix, "matchType": "prefix", "from": str(y), "to": str(y), "output": "json",
            "fl": "original", "filter": ["statuscode:200", "mimetype:text/html"], "collapse": "urlkey",
            "limit": "2000"})
        rows = [x[0] for x in r.json()[1:]]
    except (requests.RequestException, ValueError, IndexError):
        return []
    rows = [u for u in rows if not SKIP_EXT.search(u) and "?" not in u]
    rng.shuffle(rows)
    return rows[:limit]


def authority_old_pages(sites=8, per_site=40, crawler=None, rng=random, log=lambda m: None):
    """Dofollow outbound links on the OLD pages of top publishers and government/university sites."""
    crawler = crawler or Crawler(delay=1.0)
    picks = rng.sample(AUTHORITY_SITES, min(sites, len(AUTHORITY_SITES)))
    out = []
    sess = requests.Session()
    for prefix, years in picks:
        urls = wayback_old_urls(prefix, years, per_site, sess, rng)
        n0 = len(out)
        for u in urls:
            if not u.startswith("http"):
                u = "https://" + u.lstrip("/")
            res = crawler.get(u)
            if res:
                for rec in extract_links(res[0], res[2], res[1])[0]:
                    rec["via"] = "publishers"
                    out.append(rec)
        log(f"  {prefix}: {len(urls)} old pages, {len(out) - n0} outbound links")
    return out


# ---- verified dofollow niches -------------------------------------------------------------
# Each of these was checked by hand: the outbound link carries no rel=nofollow/ugc/sponsored.
CODE_HOSTS = {"sourceforge.net", "gitlab.com", "launchpad.net", "gnu.org", "nongnu.org", "pypi.org", "python.org",
              "cpan.org", "metacpan.org", "rubygems.org", "npmjs.com", "haskell.org", "bitbucket.org", "debian.org",
              "freedesktop.org", "kde.org", "gnome.org", "perl.org", "codeberg.org", "packagist.org", "crates.io",
              "golang.org", "go.dev", "readthedocs.io", "readthedocs.org", "github.io", "googlecode.com",
              "mozilla.org", "apache.org", "r-project.org", "ctan.org", "ubuntu.com", "fedoraproject.org",
              "europa.eu", "ec.europa.eu", "medium.com", "blogspot.com", "wordpress.com", "tumblr.com",
              "techcrunch.com", "nytimes.com", "bbc.co.uk", "theguardian.com", "arxiv.org", "nature.com"}


VIA_BY_SOURCE = {"wikipedia.org": "wikipedia", "ycombinator.com": "hackernews", "debian.org": "debian",
                 "europa.eu": "cordis", "github.com": "github", "wikidata.org": "wikidata"}


def infer_via(source_domain):
    """Which harvester a link came from, worked out from the linking site (for rows saved before 'via')."""
    return VIA_BY_SOURCE.get(source_domain or "", "crawl")


def _rec(source_url, source_domain, target_url, anchor, via, follow="dofollow", reason="verified source"):
    try:
        dom = registrable(urlparse(target_url).hostname or "")
    except ValueError:
        return None
    if not dom or dom == source_domain or dom in CODE_HOSTS or dom in IGNORE_TARGETS or RESTRICTED_RE.search(dom):
        return None
    return dict(source_url=source_url, source_domain=source_domain, target_url=target_url, target_domain=dom,
                anchor=(anchor or "")[:100], follow=follow, reason=reason, via=via)


def hn_links(months=3, min_points=100, rng=random, session=None):
    """Hacker News stories with >= min_points from random months 2008-2018 (title links are dofollow)."""
    s = session or requests.Session()
    s.headers["User-Agent"] = UA
    out = []
    for _ in range(months):
        y, m = rng.randint(2008, 2018), rng.randint(1, 12)
        start = int(datetime(y, m, 1, tzinfo=timezone.utc).timestamp())
        end = int(datetime(y + (m == 12), m % 12 + 1, 1, tzinfo=timezone.utc).timestamp())
        for page in range(3):
            try:
                j = s.get("https://hn.algolia.com/api/v1/search_by_date", timeout=30, params={
                    "tags": "story", "hitsPerPage": 1000, "page": page,
                    "numericFilters": f"points>={min_points},created_at_i>={start},created_at_i<{end}"}).json()
            except (requests.RequestException, ValueError):
                break
            for h in j.get("hits", []):
                if h.get("url"):
                    r = _rec(f"https://news.ycombinator.com/item?id={h.get('objectID')}", "ycombinator.com",
                             h["url"], h.get("title"), "hackernews")
                    if r:
                        out.append(r)
            if page + 1 >= j.get("nbPages", 0):
                break
    return out


def debian_links(sample=2500, rng=random, session=None):
    """Upstream homepages of Debian source packages (packages.debian.org 'Homepage' links are dofollow)."""
    import lzma
    s = session or requests.Session()
    s.headers["User-Agent"] = UA
    pkgs = {}
    for suite in ("stable", "unstable"):
        try:
            r = s.get(f"https://deb.debian.org/debian/dists/{suite}/main/source/Sources.xz", timeout=120)
            text = lzma.decompress(r.content).decode("utf-8", "replace")
        except (requests.RequestException, lzma.LZMAError):
            continue
        for block in text.split("\n\n"):
            pk = re.search(r"^Package: (.+)$", block, re.M)
            hp = re.search(r"^Homepage: (\S+)", block, re.M)
            if pk and hp:
                pkgs.setdefault(pk.group(1).strip(), (hp.group(1).strip(), suite))
    items = list(pkgs.items())
    rng.shuffle(items)
    out = []
    for name, (hp, suite) in items[:sample]:
        r = _rec(f"https://packages.debian.org/source/{suite}/{name}", "debian.org", hp, "Homepage", "debian")
        if r:
            out.append(r)
    return out


def cordis_links(sample=2500, rng=random, session=None, log=lambda m: None):
    """Project websites of EU-funded FP7 / Horizon 2020 research projects (CORDIS links are dofollow).
    Current CORDIS dumps keep websites in webLink.csv (one row per link, tied to a projectID)."""
    s = session or requests.Session()
    s.headers["User-Agent"] = UA
    rows = []

    def read_csv(z, name):
        raw = z.read(name).decode("utf-8-sig", "replace")
        delim = ";" if raw[:2000].count(";") > raw[:2000].count(",") else ","
        return csv.DictReader(io.StringIO(raw), delimiter=delim)

    for prog in ("fp7", "h2020"):
        try:
            r = s.get(f"https://cordis.europa.eu/data/cordis-{prog}projects-csv.zip", timeout=300)
            z = zipfile.ZipFile(io.BytesIO(r.content))
        except (requests.RequestException, zipfile.BadZipFile) as e:
            log(f"  CORDIS {prog}: download failed ({type(e).__name__}: {e})")
            continue
        names = z.namelist()
        acronyms = {}
        proj = next((n for n in names if n.lower().endswith("project.csv")), None)
        if proj:
            for row in read_csv(z, proj):
                low = {k.lower().strip(): (v or "").strip() for k, v in row.items() if k}
                if low.get("id"):
                    acronyms[low["id"]] = low.get("acronym", "")
                url = low.get("projecturl") or low.get("projectwebsite") or ""
                if url and low.get("id"):  # older dumps kept the website here
                    rows.append((low["id"], url, low.get("acronym", "")))
        web = next((n for n in names if "weblink" in n.lower()), None)
        if web:
            reader = read_csv(z, web)
            cols = [c.lower().strip() for c in (reader.fieldnames or [])]
            log(f"  CORDIS {prog}/{web}: columns {', '.join(cols[:15])}")
            types, before = defaultdict(int), len(rows)
            for row in reader:
                low = {k.lower().strip(): (v or "").strip() for k, v in row.items() if k}
                pid = low.get("projectid") or low.get("project_id") or low.get("projectrcn") or ""
                url = low.get("physurl") or low.get("url") or low.get("link") or ""
                typ = (low.get("type") or low.get("weblinktype") or low.get("represents") or "").lower()
                types[typ] += 1
                if low.get("organisationid") or low.get("organizationid"):
                    continue  # a participant's own website, not the project's
                if pid and url and ("cordis" not in url and "europa.eu" not in url):
                    rows.append((pid, url, acronyms.get(pid, "")))
            log(f"  CORDIS {prog}: {len(rows) - before} project websites; link types {dict(list(types.items())[:6])}")
    rows = [(p, u if u.startswith(("http://", "https://")) else "http://" + u, a) for p, u, a in rows]
    rows = list(dict.fromkeys(rows))
    rng.shuffle(rows)
    out = []
    for pid, url, acr in rows[:sample]:
        rec = _rec(f"https://cordis.europa.eu/project/id/{pid}", "europa.eu", url, acr, "cordis")
        if rec:
            out.append(rec)
    return out


LINKY_SUFFIX_RE = re.compile(r"\.(edu|ac\.[a-z]{2}|edu\.[a-z]{2}|gov|gov\.[a-z]{2}|org|org\.[a-z]{2}|museum|int)$")


def sample_ranked_sites(n, lo=2_000, hi=300_000, rng=random, linky_share=0.5):
    """n random sites from the Tranco ranking between ranks lo and hi; about half from .edu/.ac/.org/.gov-type
    sites, which keep old resource pages full of outbound links."""
    pool = [d for d, rk in AUTH.rank.items() if lo <= rk <= hi and d not in IGNORE_TARGETS]
    linky = [d for d in pool if LINKY_SUFFIX_RE.search(d)]
    k = min(int(n * linky_share), len(linky))
    picked = set(rng.sample(linky, k))
    rest = [d for d in pool if d not in picked]
    picked.update(rng.sample(rest, min(n - k, len(rest))))
    return list(picked)


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
    elif any("renew period" in s for s in statuses):
        state = "grace"
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


def make_resolver(nameserver=None):
    import dns.resolver
    res = dns.resolver.Resolver(configure=False)
    res.nameservers = [nameserver] if nameserver else ["1.1.1.1", "8.8.8.8", "9.9.9.9"]
    res.lifetime = 6
    return res


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
    except Exception:  # noqa: BLE001 - e.g. malformed names
        return "dns_error"


def site_state(domain, session, public_only=False):
    """Visit the homepage: 'live', 'parked' (for-sale / expired / parking page), 'moved' or 'dead'.
    Any HTTP answer (even 403/503 from bot protection) counts as alive; only no answer at all is 'dead'.
    Sites in the Tranco ranking are treated as live without a visit."""
    if AUTH.get(domain):
        return "live"
    for scheme in ("https", "http"):
        url = f"{scheme}://{domain}/"
        try:
            for _ in range(6):
                if public_only and not public_url(url):
                    return "live"  # cannot inspect safely; never claim dead
                r = session.get(url, timeout=(6, 8), stream=True, allow_redirects=False)
                if r.is_redirect:
                    nxt = urljoin(url, r.headers.get("location", ""))
                    r.close()
                    if urlparse(nxt).scheme not in ("http", "https"):
                        return "live"
                    url = nxt
                    continue
                break
            else:
                return "live"
            try:
                body = safe_decode(read_capped(r, 300_000, 10), r.encoding)
            except Exception:  # noqa: BLE001
                body = ""
            finally:
                r.close()
        except requests.RequestException:
            continue
        final_dom = registrable(urlparse(url).hostname or "")
        if (r.status_code < 400 and PARK_RE.search(body)) or (final_dom and final_dom != domain
                                                              and PARK_RE.search(final_dom)):
            return "parked"
        if final_dom and final_dom != domain:
            return "moved"
        return "live"
    return "dead"


def wayback_summary(domain, session):
    try:
        r = session.get("https://web.archive.org/cdx/search/cdx",
                        params={"url": domain, "output": "json", "fl": "timestamp", "filter": "statuscode:200",
                                "collapse": "timestamp:6", "limit": "1000"}, timeout=30)
        rows = r.json()[1:]
    except (requests.RequestException, ValueError):
        return {}
    if not rows:
        return {"wb_first": "", "wb_last": "", "wb_months": 0, "wb_years": 0}
    first, last = rows[0][0][:4], rows[-1][0][:4]
    return {"wb_first": first, "wb_last": last, "wb_months": len(rows), "wb_years": int(last) - int(first) + 1}


def stabledomains_check(domain, session):
    """Confirm availability + price with StableDomains (affiliate checkout)."""
    try:
        r = session.get("https://stabledomains.net/api/v1/availability", params={"domain": domain}, timeout=20)
        j = r.json()
    except (requests.RequestException, ValueError):
        return {}

    def walk(o):
        yield o
        if isinstance(o, dict):
            for v in o.values():
                yield from walk(v)
        elif isinstance(o, list):
            for v in o:
                yield from walk(v)

    for o in walk(j):
        if isinstance(o, dict) and str(o.get("domain", "")).lower() == domain:
            av = o.get("availability", o.get("available", o.get("status")))
            if av is True or str(av).lower() in ("available", "true", "free"):
                avs = "yes"
            elif av is False or str(av).lower() in ("taken", "registered", "unavailable", "false"):
                avs = "no"
            else:
                avs = str(av or "")
            price = o.get("total_usd") or o.get("first_year_usd") or o.get("price_usd") or o.get("price") or ""
            if isinstance(price, dict):
                price = price.get("usd") or price.get("total") or ""
            return {"sd_available": avs, "sd_price": str(price), "premium": bool(o.get("premium"))}
    return {}


def label(rec, expiring_days):
    reg = rec.get("registration")
    if reg == "available":
        return "AVAILABLE"
    if reg == "pending_delete":
        return "PENDING_DELETE"
    if reg == "redemption":
        return "REDEMPTION"
    if reg == "grace":
        return "EXPIRED_GRACE"
    if reg == "no_rdap" and rec.get("dns") == "nxdomain":
        return "LIKELY_AVAILABLE"
    # An expiry date only matters when the site is gone too: live sites (brands etc.) auto-renew.
    site_gone = rec.get("site") in ("dead", "parked") or rec.get("dns") in ("nxdomain", "no_ns_answer", "dns_error")
    if reg in ("registered", "unknown") and rec.get("expiry") and site_gone:
        try:
            exp = datetime.fromisoformat(rec["expiry"].replace("Z", "+00:00"))
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            if exp < now:
                return "EXPIRED_GRACE"
            if exp < now + timedelta(days=expiring_days):
                return "EXPIRING_SOON"
        except ValueError:
            pass
    if rec.get("site") == "parked":
        return "PARKED"
    if rec.get("site") == "dead":
        return "DEAD_SITE"
    return "REGISTERED"


PRIORITY = {"AVAILABLE": 0, "PENDING_DELETE": 1, "REDEMPTION": 2, "EXPIRED_GRACE": 3, "LIKELY_AVAILABLE": 4,
            "EXPIRING_SOON": 5, "PARKED": 6, "DEAD_SITE": 7, "REGISTERED": 9}
BUYABLE = {"AVAILABLE", "LIKELY_AVAILABLE"}


# --------------------------------------------------------------------------- library api
CSV_COLS = ["domain", "status", "quality", "score", "dofollow_sources", "nofollow_only_sources", "edu_gov_sources",
            "best_source_rank", "est_value", "value_ratio", "ref_subnets", "real_years", "cc_authority", "cc_rank", "opr", "ref_domains", "via", "expiry", "dns", "site", "wb_first", "wb_last", "wb_years", "wb_months",
            "sd_available", "sd_price", "flags", "best_source_url", "best_anchor", "link_target_url", "buy_url",
            "more_sources", "first_seen", "last_checked"]


def harvest(seeds=(), depth=1, max_pages=30, delay=1.0, workers=8, wiki_categories=(), wiki_limit=300,
            github_query=None, github_limit=30, public_only=False, sitemaps=False, ranked_sites=(),
            deadline=None, hn_months=0, debian_sample=0, cordis_sample=0, rng=random, wikidata_types=0,
            publisher_sites=0, log=lambda msg: None):
    """Collect outbound link records from seeds, ranked sites' old pages, Wikipedia and GitHub."""
    crawler = Crawler(delay=delay, public_only=public_only, deadline=deadline)
    records = []
    seeds, ranked_sites = list(seeds), list(ranked_sites)
    lock = threading.Lock()
    done, nlinks = [0], [0]

    def one_seed(s):
        try:
            recs = crawler.crawl_site(s, old_k=8) if sitemaps else crawler.crawl(s, depth, max_pages)
        except Exception:  # noqa: BLE001
            recs = []
        return s, recs

    def one_ranked(d):
        try:
            recs = crawler.crawl_site(d, old_k=12)
        except Exception:  # noqa: BLE001 - one bad site must not kill the run
            recs = []
        with lock:
            done[0] += 1
            nlinks[0] += len(recs)
            if done[0] % 20 == 0:
                log(f"  {done[0]}/{len(ranked_sites)} ranked sites crawled, {nlinks[0]} links so far")
        return recs

    if seeds:
        log(f"Crawling {len(seeds)} seed page(s){' (oldest sitemap pages)' if sitemaps else ''}...")
        with ThreadPoolExecutor(max(1, workers)) as ex:
            for seed, recs in ex.map(one_seed, seeds):
                log(f"  {seed}: {len(recs)} external links")
                records.extend(recs)
    if ranked_sites:
        log(f"Crawling the oldest pages of {len(ranked_sites)} ranked sites...")
        with ThreadPoolExecutor(max(1, workers)) as ex:
            for recs in ex.map(one_ranked, ranked_sites):
                records.extend(recs)
        log(f"  ranked sites gave {len(records)} links")
    for cat in wiki_categories or []:
        if crawler.expired():
            break
        log(f"Reading Wikipedia {cat}...")
        try:
            recs = wikipedia_links(cat, wiki_limit)
        except (requests.RequestException, ValueError, KeyError) as e:
            log(f"  Wikipedia failed: {e}")
            recs = []
        log(f"  {len(recs)} external links")
        records.extend(recs)
    for label_, fn, arg in (("Hacker News front-page stories", hn_links, hn_months),
                            ("Debian package homepages", debian_links, debian_sample),
                            ("EU research project websites", cordis_links, cordis_sample)):
        if arg and not crawler.expired():
            log(f"Reading {label_}...")
            try:
                recs = fn(arg, rng=rng, log=log) if fn is cordis_links else fn(arg, rng=rng)
            except Exception as e:  # noqa: BLE001
                log(f"  failed: {type(e).__name__}: {e}")
                recs = []
            log(f"  {len(recs)} dofollow links")
            records.extend(recs)
    if publisher_sites and not crawler.expired():
        log(f"Reading old articles on {publisher_sites} top publisher / government sites...")
        try:
            recs = authority_old_pages(publisher_sites, crawler=crawler, rng=rng, log=log)
        except Exception as e:  # noqa: BLE001
            log(f"  failed: {type(e).__name__}: {e}")
            recs = []
        log(f"  {len(recs)} links ({sum(r['follow'] == 'dofollow' for r in recs)} dofollow)")
        records.extend(recs)
    if wikidata_types:
        log("Reading defunct organisations from Wikidata...")
        types = rng.sample(list(WD_TYPES), min(wikidata_types, len(WD_TYPES)))
        try:
            recs = wikidata_defunct(types=types, rng=rng, log=log)
        except Exception as e:  # noqa: BLE001
            log(f"  failed: {type(e).__name__}: {e}")
            recs = []
        log(f"  {len(recs)} official websites of closed organisations")
        records.extend(recs)
    if github_query:
        log(f"Searching GitHub for '{github_query}'...")
        try:
            recs = github_links(github_query, github_limit)
        except (requests.RequestException, ValueError, KeyError) as e:
            log(f"  GitHub failed: {e}")
            recs = []
        log(f"  {len(recs)} external links")
        records.extend(recs)
    return [r for r in records if r["target_domain"] not in IGNORE_TARGETS
            and not RESTRICTED_RE.search(r["target_domain"])]


def status_check(domains, resolver_ip=None, rdap_all=False, expiring_days=45, rdap_interval=0.3, workers=40,
                 site_check=(), public_only=False, log=lambda msg: None):
    """DNS + RDAP (+ homepage visit for domains in site_check) -> {domain: status record}."""
    resolver = make_resolver(resolver_ip)
    session = requests.Session()
    session.headers["User-Agent"] = UA
    bootstrap = load_rdap_bootstrap(session)
    limiters = {}
    site_check = set(site_check)
    done, lock = [0], threading.Lock()

    def rdap(domain):
        base = bootstrap.get(domain.rsplit(".", 1)[-1])
        if not base:
            return {"registration": "no_rdap"}
        with lock:  # one rate limit per registry RDAP server
            limiter = limiters.setdefault(base, RateLimiter(rdap_interval))
        return rdap_lookup(domain, base, session, limiter)

    def check_one(domain):
        try:
            return _check_one(domain)
        except Exception as e:  # noqa: BLE001 - one odd domain must not kill the run
            return {"domain": domain, "dns": "", "registration": "unknown", "status": "REGISTERED",
                    "note": f"{type(e).__name__}"}

    def _check_one(domain):
        rec = {"domain": domain, "dns": dns_status(domain, resolver)}
        if rec["dns"] in ("nxdomain", "dns_error", "no_ns_answer") or rdap_all:
            rec.update(rdap(domain))
        else:
            rec["registration"] = "registered"
            if domain in site_check:
                rec["site"] = site_state(domain, session, public_only)
                if rec["site"] in ("dead", "parked"):
                    rec.update(rdap(domain))
        rec["status"] = label(rec, expiring_days)
        with lock:
            done[0] += 1
            if done[0] % 100 == 0 or done[0] == len(domains):
                log(f"  checked {done[0]}/{len(domains)}")
        return rec

    with ThreadPoolExecutor(max(1, workers)) as ex:
        return {r["domain"]: r for r in ex.map(check_one, domains)}


def check(records, dofollow_only=False, min_sources=1, tlds=None, resolver_ip=None, rdap_all=False,
          expiring_days=45, rdap_interval=0.3, workers=40, wayback=False, wayback_top=100, include_all=False,
          max_domains=None, site_check_top=0, verify=False, opr_key=None, public_only=False,
          log=lambda msg: None):
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
    scores = {d: score_domain(by[d]) for d in domains}
    domains.sort(key=lambda d: -scores[d])
    if max_domains and len(domains) > max_domains:
        log(f"Limiting to the {max_domains} best-linked of {len(domains)} domains.")
        domains = domains[:max_domains]
    log(f"Checking {len(domains)} domains (DNS/RDAP{', dead-site scan of top ' + str(site_check_top) if site_check_top else ''})...")
    results = status_check(domains, resolver_ip, rdap_all, expiring_days, rdap_interval, workers,
                           site_check=domains[:site_check_top], public_only=public_only, log=log)
    rows = [build_row(rec, by[rec["domain"]], scores[rec["domain"]]) for rec in results.values()
            if include_all or rec["status"] != "REGISTERED"]
    rows.sort(key=lambda r: (PRIORITY.get(r["status"], 9), -r["score"]))
    enrich(rows, wayback=wayback, wayback_top=wayback_top, verify=verify, opr_key=opr_key, log=log,
           include_registered=include_all)
    return rows


def build_row(rec, rs, score):
    df = {r["source_domain"] for r in rs if r["follow"] == "dofollow"}
    nf = {r["source_domain"] for r in rs} - df
    ordered = sorted(rs, key=lambda r: (r["follow"] != "dofollow", -source_weight(r["source_domain"])))
    best = ordered[0]
    ranks = [AUTH.get(s) for s in df | nf if AUTH.get(s)]
    sources, seen = [], set()
    for r in ordered:
        if r["source_url"] in seen:
            continue
        seen.add(r["source_url"])
        sources.append({"url": r["source_url"], "domain": r["source_domain"], "follow": r["follow"],
                        "via": r.get("via", ""),
                        "rank": AUTH.get(r["source_domain"]), "anchor": r["anchor"], "target": r["target_url"]})
        if len(sources) >= 6:
            break
    return {"domain": rec["domain"], "status": rec["status"], "score": score,
            "dofollow_sources": len(df), "nofollow_only_sources": len(nf),
            "edu_gov_sources": sum(s.endswith(EDU_GOV_SUFFIXES) for s in df | nf),
            "best_source_rank": min(ranks) if ranks else "",
            "expiry": rec.get("expiry", ""), "dns": rec.get("dns", ""), "site": rec.get("site", ""),
            "flags": ",".join(name_flags(rec["domain"])),
            "via": ",".join(sorted({r.get("via", "") for r in rs if r.get("via")})),
            "best_source_url": best["source_url"], "best_anchor": best["anchor"], "link_target_url": best["target_url"],
            "buy_url": buy_url(rec["domain"]),
            "more_sources": " | ".join(s["url"] for s in sources[:5]), "sources": sources}


def enrich(rows, wayback=False, wayback_top=100, verify=False, opr_key=None, log=lambda m: None,
           include_registered=False):
    session = requests.Session()
    session.headers["User-Agent"] = UA
    if wayback and rows:
        top = rows[:wayback_top]
        log(f"Pulling Wayback history for {len(top)} domains...")
        with ThreadPoolExecutor(4) as ex:
            def wb(r):
                try:
                    return wayback_summary(r["domain"], session)
                except Exception:  # noqa: BLE001
                    return {}
            for row, wb in zip(top, ex.map(wb, top)):
                row.update(wb)
    if verify:
        avail = [r for r in rows if r["status"] in BUYABLE][:150]
        if avail:
            log(f"Confirming {len(avail)} available domains with StableDomains...")
            for r in avail:
                info = stabledomains_check(r["domain"], session)
                r.update(info)
                if info.get("sd_available") == "yes" and r["status"] == "LIKELY_AVAILABLE":
                    r["status"] = "AVAILABLE"
                elif info.get("sd_available") == "no" and info.get("sd_price"):
                    r["status"] = "REGISTERED"  # registry says taken
                time.sleep(0.5)
        rows[:] = [r for r in rows if r["status"] != "REGISTERED" or include_registered]
    if opr_key and rows:
        need = [r["domain"] for r in rows if r["status"] != "REGISTERED" and "opr" not in r][:1500]
        log(f"Fetching Open PageRank for {len(need)} domains...")
        pr = open_pagerank(need, opr_key, session)
        for r in rows:
            if r["domain"] in pr:
                r.update(pr[r["domain"]])
        log(f"  got scores for {len(pr)}")
    rows.sort(key=lambda r: (PRIORITY.get(r["status"], 9), -r["score"]))


def write_csv(rows, f):
    w = csv.DictWriter(f, fieldnames=CSV_COLS, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)


# --------------------------------------------------------------------------- automatic hunt
def load_data(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {"domains": {}, "runs": []}


def _merge(old, r, stamp):
    prev = old.get(r["domain"])
    r["first_seen"] = prev.get("first_seen", stamp) if prev else r.get("first_seen", stamp)
    r["last_checked"] = stamp
    if prev:  # keep the best evidence we have seen
        if prev.get("score", 0) > r.get("score", 0):
            r["score"], r["sources"] = prev["score"], prev.get("sources", r.get("sources"))
            r["dofollow_sources"] = max(r.get("dofollow_sources", 0), prev.get("dofollow_sources", 0))
        vias = {v for v in (prev.get("via", "") + "," + r.get("via", "")).split(",") if v}
        r["via"] = ",".join(sorted(vias))
        for k in ("wb_first", "wb_last", "wb_years", "wb_months", "opr", "ref_domains", "cc_rank", "cc_pr_rank",
                  "cc_authority", "ref_subnets", "real_years", "real_ratio", "parked_only", "never_archived",
                  "mm_rank", "value_usd"):
            if r.get(k) in (None, "") and prev.get(k) not in (None, ""):
                r[k] = prev[k]
    old[r["domain"]] = r


def graph_scan(pairs, info, log, label):
    """pairs: domains in authority order. DNS sweep -> registry check -> candidate rows."""
    pairs = [d for d in pairs if d and not RESTRICTED_RE.search(d) and d not in IGNORE_TARGETS]
    log(f"{label}: DNS sweep of {len(pairs):,} high-authority domains...")
    dead = dns_sweep(pairs, log=log)
    log(f"  {len(dead):,} do not resolve; asking the registries...")
    recs = status_check(list(dead), rdap_all=True, workers=40, log=log) if dead else {}
    cands = {d: r for d, r in recs.items() if r["status"] != "REGISTERED"}
    log(f"  {len(cands)} are available / dropping / expired")
    return cands


def hunt(data_path="data/candidates.json", csv_path="data/latest.csv", budget_minutes=40, sites=100,
         wiki_months=2, wiki_limit=150, seeds_file="seeds.txt", opr_key=None, workers=24, max_keep=6000,
         seed=None, hn_months=4, debian_sample=1500, cordis_sample=1500, cc_ranks=True,
         sd_pages=40, majestic_batch=40000, cc_batch=40000, wayback_checks=150, wikidata_types=8,
         publisher_sites=8, log=print):
    """Fully automatic run.
    Authority-first: StableDomains quality drop feed, a rotating batch of the Majestic Million and of the
    Common Crawl ranking (DNS + registry checks), then link discovery (ranked sites, Hacker News, Debian,
    EU projects, Wikipedia). Every candidate gets a Wayback real-history check and a 0-100 Quality score."""
    t0 = time.time()
    rng = random.Random(seed)
    data = load_data(data_path)
    cursors = data.get("cursors", {"majestic": 0, "cc": 0})
    old = {d: r for d, r in data.get("domains", {}).items() if not RESTRICTED_RE.search(d)}
    stamp = now_iso()
    by_source = defaultdict(int)

    # 1. StableDomains quality feed
    if sd_pages:
        log("Reading the StableDomains quality drop feed...")
        items = stabledomains_feed(sd_pages, sort="quality", log=log)
        sd_rows = [sd_item_to_row(it) for it in items if it.get("status") in SD_STATUS and not it.get("spam")]
        sd_rows = [r for r in sd_rows if r["domain"] and not RESTRICTED_RE.search(r["domain"])]
        log(f"  {len(sd_rows)} quality-scored drops")
        for r in sd_rows:
            _merge(old, r, stamp)
        by_source["stabledomains"] = len(sd_rows)

    # 2. Majestic Million batch
    if majestic_batch:
        mm = majestic_million(log=log)
        if mm:
            start = cursors.get("majestic", 0) % len(mm)
            batch = mm[start:start + majestic_batch]
            cursors["majestic"] = (start + majestic_batch) % len(mm)
            info = {d: {"ref_subnets": sub, "mm_rank": rk} for rk, d, sub in batch}
            cands = graph_scan([d for _, d, _ in batch], info, log, f"Majestic Million ranks {start + 1:,}+")
            for r in graph_rows(cands, "majestic", info, stamp):
                _merge(old, r, stamp)
            by_source["majestic"] = len(cands)

    # 3. link discovery
    AUTH.load(log)
    ranked = sample_ranked_sites(sites, rng=rng) if AUTH.rank and sites else []
    my_seeds = []
    if seeds_file and os.path.exists(seeds_file):
        my_seeds = [l.strip() for l in open(seeds_file) if l.strip() and not l.lstrip().startswith("#")]
    cats = random_dead_link_categories(wiki_months, rng) if wiki_months else []
    deadline = time.time() + budget_minutes * 60 * 0.3
    records = harvest(my_seeds, sitemaps=True, ranked_sites=ranked, wiki_categories=cats, wiki_limit=wiki_limit,
                      workers=workers, deadline=deadline, hn_months=hn_months, debian_sample=debian_sample,
                      cordis_sample=cordis_sample, rng=rng, wikidata_types=wikidata_types,
                      publisher_sites=publisher_sites, log=log)
    for v in ("crawl", "wikipedia", "hackernews", "debian", "cordis", "wikidata", "publishers"):
        by_source[v + "_links"] = sum(1 for r in records if r.get("via") == v)
    log(f"Harvested {len(records)} links ({sum(r['follow'] == 'dofollow' for r in records)} dofollow) "
        f"to {len({r['target_domain'] for r in records})} domains.")
    rows = check(records, workers=40, wayback=True, wayback_top=300, verify=True, opr_key=opr_key,
                 site_check_top=1000, max_domains=15000, log=log)
    for r in rows:
        _merge(old, r, stamp)

    # 4. re-check earlier finds that were not refreshed this run
    fresh_stamp = {d for d, r in old.items() if r.get("last_checked") == stamp}
    stale = [d for d, r in old.items() if d not in fresh_stamp and r.get("status") != "TAKEN"]
    if stale:
        log(f"Re-checking {len(stale)} domains from earlier runs...")
        recs = status_check(stale, rdap_all=True,
                            site_check=[d for d in stale if old[d].get("status") in ("PARKED", "DEAD_SITE")], log=log)
        for d, rec in recs.items():
            r = old[d]
            if rec["status"] == "REGISTERED" and rec.get("registration") == "registered":
                r["status"], r["taken_at"] = "TAKEN", stamp
            elif rec["status"] == "REGISTERED":
                r["last_checked"] = stamp
            else:
                r.update(status=rec["status"], expiry=rec.get("expiry", r.get("expiry", "")), dns=rec.get("dns", ""),
                         site=rec.get("site", r.get("site", "")), last_checked=stamp)

    # 5. Common Crawl: authority for every candidate + a rotating window of top-ranked domains to scan
    if cc_ranks:
        need = [d for d, r in old.items() if r.get("status") != "TAKEN" and r.get("cc_rank") in (None,)]
        start = cursors.get("cc", 0)
        window = (start, start + cc_batch) if cc_batch else None
        win = []
        log(f"Common Crawl web graph: authority for {len(need)} domains"
            + (f", scanning ranks {start + 1:,}-{start + cc_batch:,}" if window else "") + "...")
        found = cc_web_ranks(need, log=log, window=window, window_out=win)
        for d, inf in found.items():
            old[d].update(inf)
        for d in need:
            old[d].setdefault("cc_rank", "")
        if window:
            cursors["cc"] = start + cc_batch if win else 0
            info = {}
            total_est = 1.2e8
            for d, hc, pr in win:
                info[d] = {"cc_rank": hc, "cc_pr_rank": pr, "cc_authority": cc_authority(hc, total_est)}
            cands = graph_scan([d for d, _, _ in win], info, log, "Common Crawl top ranks")
            for r in graph_rows(cands, "commoncrawl", info, stamp):
                _merge(old, r, stamp)
            by_source["commoncrawl"] = len(cands)

    # 6. Wayback real-history check for unverified candidates (best first), then Quality for everything
    live = [r for r in old.values() if r.get("status") not in ("TAKEN", "REGISTERED")]
    todo = [r for r in live if r.get("via") != "stabledomains" and r.get("real_years") in (None,)
            and r.get("status") in ("AVAILABLE", "LIKELY_AVAILABLE", "PENDING_DELETE", "REDEMPTION", "EXPIRED_GRACE")]
    todo.sort(key=lambda r: -((r.get("ref_subnets") or 0) + (r.get("cc_authority") or 0) * 50 + r.get("score", 0) * 20))
    todo = todo[:wayback_checks]
    if todo:
        log(f"Checking Wayback real-site history for {len(todo)} candidates...")
        sess = requests.Session()
        sess.headers["User-Agent"] = UA
        with ThreadPoolExecutor(3) as ex:
            def wb_safe(r):
                try:
                    return wayback_real_history(r["domain"], sess)
                except Exception:  # noqa: BLE001
                    return {}
            for r, h in zip(todo, ex.map(wb_safe, todo)):
                if h:
                    r.update({k: v for k, v in h.items() if v not in (None, "") or k in ("real_years",)})
    for r in old.values():
        r["est_value"] = estimate_value(r)
        r["reg_price"] = reg_price(r)
        r["value_ratio"] = round(r["est_value"] / max(r["reg_price"], 1), 1)
        r["wiki_only"] = wiki_only(r)
        if r.get("via") == "stabledomains" and r.get("quality") is not None and "real_years" not in r:
            continue  # StableDomains already scored it
        q, why = quality_score(r)
        r["quality"], r["quality_reasons"] = q, why

    cutoff = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    old = {d: r for d, r in old.items() if not (r.get("status") == "TAKEN" and r.get("taken_at", "") < cutoff)}
    keep = sorted(old.values(), key=lambda r: (r.get("status") in ("TAKEN", "REGISTERED"), -(r.get("quality") or 0),
                                               PRIORITY.get(r.get("status"), 9)))[:max_keep]
    counts = defaultdict(int)
    for r in keep:
        counts[r["status"]] += 1
    run = {"at": stamp, "seconds": int(time.time() - t0), "ranked_sites": len(ranked), "wiki_categories": cats,
           "by_source": dict(by_source), "links": len(records),
           "new_candidates": len([r for r in keep if r.get("first_seen") == stamp]),
           "quality_60_plus": len([r for r in keep if (r.get("quality") or 0) >= 60 and r["status"] != "TAKEN"]),
           "counts": dict(counts)}
    data = {"updated": stamp, "tranco_loaded": bool(AUTH.rank), "cursors": cursors,
            "runs": ([run] + data.get("runs", []))[:30], "domains": {r["domain"]: r for r in keep}}
    os.makedirs(os.path.dirname(data_path) or ".", exist_ok=True)
    with open(data_path, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    if csv_path:
        with open(csv_path, "w", newline="") as f:
            write_csv([r for r in keep if r["status"] != "TAKEN"], f)
    log(f"Done in {int(time.time() - t0)}s: {run['new_candidates']} new, {len(keep)} tracked, "
        f"{run['quality_60_plus']} with quality 60+. {dict(counts)}")
    return data


# --------------------------------------------------------------------------- commands
def cmd_harvest(a):
    seeds = []
    if a.seeds:
        seeds = [l.strip() for l in open(a.seeds) if l.strip() and not l.startswith("#")]
    ranked = []
    if a.ranked_sites:
        AUTH.load(lambda m: print(m, file=sys.stderr))
        ranked = sample_ranked_sites(a.ranked_sites)
    records = harvest(seeds, a.depth, a.max_pages, a.delay, a.workers, a.wikipedia_category, a.wiki_limit,
                      a.github_query, a.github_limit, sitemaps=a.sitemaps, ranked_sites=ranked,
                      log=lambda m: print(m, file=sys.stderr))
    with open(a.out, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    follow = sum(r["follow"] == "dofollow" for r in records)
    print(f"{len(records)} external links ({follow} dofollow) -> {len({r['target_domain'] for r in records})} "
          f"unique domains. Saved to {a.out}")


def cmd_check(a):
    recs = [json.loads(l) for l in open(a.links)]
    if not a.no_authority:
        AUTH.load(lambda m: print(m, file=sys.stderr))
    rows = check(recs, a.dofollow_only, a.min_sources, a.tlds.split(",") if a.tlds else None, a.resolver,
                 a.rdap_all, a.expiring_days, a.rdap_interval, a.workers, a.wayback, a.wayback_top, a.all,
                 site_check_top=a.site_check, verify=a.verify, opr_key=os.environ.get("OPR_API_KEY"),
                 log=lambda m: print(m, file=sys.stderr))
    with open(a.out, "w", newline="") as f:
        write_csv(rows, f)
    print(f"{len(rows)} candidates -> {a.out}")
    for r in rows[:15]:
        print(f"  {r['status']:<16} {r['domain']:<32} score={r['score']:<6} dofollow={r['dofollow_sources']}")


def cmd_hunt(a):
    hunt(a.data, a.csv, a.budget_minutes, a.sites, a.wiki_months, a.wiki_limit, a.seeds,
         os.environ.get("OPR_API_KEY"), a.workers, seed=a.random_seed, hn_months=a.hn_months,
         debian_sample=a.debian, cordis_sample=a.cordis, cc_ranks=not a.no_cc_ranks, sd_pages=a.sd_pages,
         majestic_batch=a.majestic, cc_batch=a.cc_batch, wayback_checks=a.wayback_checks,
         wikidata_types=a.wikidata_types, publisher_sites=a.publishers,
         log=lambda m: print(m, flush=True))


def selftest():
    html = """<html><head><meta name="robots" content="index,follow"></head><body>
    <a href="https://dead-example.com/page" rel="nofollow">x</a>
    <a href="https://gone-startup.io/">Gone <b>Startup</b></a>
    <a href="/internal">in</a><a href="mailto:a@b.c">m</a>
    <a href="https://sponsor.com" rel="sponsored noopener">s</a>
    <a href="http://[bad">broken</a>
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
    ext, _ = extract_links(html, "https://x.org/", headers={"X-Robots-Tag": "noindex, nofollow"})
    assert all(e["follow"] == "nofollow" for e in ext)
    assert registrable("www.cs.mit.edu") == "mit.edu" and registrable("192.168.0.1") == ""
    p = parse_rdap({"status": ["client hold", "pending delete"],
                    "events": [{"eventAction": "expiration", "eventDate": "2026-12-01T00:00:00Z"}]})
    assert p["registration"] == "pending_delete" and p["expiry"].startswith("2026-12-01")
    assert parse_rdap({"status": ["auto renew period"]})["registration"] == "grace"
    assert label({"registration": "available", "dns": "nxdomain"}, 45) == "AVAILABLE"
    assert label({"registration": "no_rdap", "dns": "nxdomain"}, 45) == "LIKELY_AVAILABLE"
    soon = (datetime.now(timezone.utc) + timedelta(days=10)).isoformat()
    past = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    assert label({"registration": "registered", "dns": "has_ns", "site": "dead", "expiry": soon}, 45) == "EXPIRING_SOON"
    assert label({"registration": "registered", "dns": "has_ns", "site": "parked", "expiry": past}, 45) == "EXPIRED_GRACE"
    assert label({"registration": "registered", "dns": "has_ns", "expiry": soon}, 45) == "REGISTERED"  # live brand
    assert label({"registration": "registered", "dns": "nxdomain", "expiry": past}, 45) == "EXPIRED_GRACE"
    for d in ("international.gc.ca", "fed.us", "dot.state.ny.us", "x.gouv.fr"):
        assert RESTRICTED_RE.search(d), d
    assert label({"registration": "registered", "dns": "has_ns", "site": "parked"}, 45) == "PARKED"
    AUTH.rank = {"bigsite.com": 500, "midsite.com": 50_000}
    recs = [dict(source_domain="mit.edu", follow="dofollow"), dict(source_domain="mit.edu", follow="nofollow"),
            dict(source_domain="blog.com", follow="nofollow"), dict(source_domain="bigsite.com", follow="dofollow")]
    assert score_domain(recs) == round(4.5 + 0.15 + 6.0, 2), score_domain(recs)
    sm = """<?xml version="1.0"?><urlset><url><loc>https://a.com/new</loc><lastmod>2024-01-01</lastmod></url>
    <url><loc>https://a.com/old?x=1&amp;y=2</loc><lastmod>2009-05-01</lastmod></url></urlset>"""
    ps = parse_sitemap(sm)
    assert ps[1] == ("url", "https://a.com/old?x=1&y=2", "2009-05-01"), ps
    assert "spammy-name" in name_flags("best-casino-bonus.com") and not name_flags("oldlab.org")
    assert "ref=" + SD_REF in buy_url("x.com")
    assert safe_decode(b"ok", "None") == "ok" and safe_decode(b"ok", None) == "ok"
    assert RESTRICTED_RE.search("recsport.sa.gov.au") and RESTRICTED_RE.search("metrokc.gov") and not RESTRICTED_RE.search("government.com")
    assert RESOURCE_RE.search("/useful-links/") and not RESOURCE_RE.search("/2009/05/post")
    AUTH.rank = {f"s{i}.org": 3000 + i for i in range(10)} | {f"c{i}.com": 3000 + i for i in range(10)}
    sm = sample_ranked_sites(6, rng=random.Random(1))
    assert len(sm) == 6 and sum(d.endswith(".org") for d in sm) >= 3, sm
    assert cc_authority(1, 10**8) == 10.0 and cc_authority(10**4, 10**8) == 5.0 and cc_authority(0, 5) is None
    AUTH.rank = {}
    print("selftest: all checks passed")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    h = sub.add_parser("harvest", help="collect outbound links and classify dofollow/nofollow")
    h.add_argument("--seeds", help="text file, one page URL per line")
    h.add_argument("--sitemaps", action="store_true", help="crawl each seed site's OLDEST sitemap pages")
    h.add_argument("--ranked-sites", type=int, default=0, help="also crawl N random Tranco-ranked sites")
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
    c.add_argument("--site-check", type=int, default=500, help="visit the top N live domains to find dead/parked sites")
    c.add_argument("--expiring-days", type=int, default=45)
    c.add_argument("--rdap-interval", type=float, default=0.3, help="seconds between RDAP requests")
    c.add_argument("--workers", type=int, default=40)
    c.add_argument("--wayback", action="store_true", help="add Wayback Machine history to top results")
    c.add_argument("--wayback-top", type=int, default=100)
    c.add_argument("--verify", action="store_true", help="confirm availability + price with StableDomains")
    c.add_argument("--no-authority", action="store_true", help="skip loading the Tranco ranking")
    c.add_argument("--all", action="store_true", help="include registered domains in the output")
    c.set_defaults(fn=cmd_check)

    k = sub.add_parser("hunt", help="fully automatic run, merges into a data file")
    k.add_argument("--data", default="data/candidates.json")
    k.add_argument("--csv", default="data/latest.csv")
    k.add_argument("--budget-minutes", type=int, default=40)
    k.add_argument("--sites", type=int, default=100, help="random Tranco-ranked sites to sample")
    k.add_argument("--wiki-months", type=int, default=2, help="random Wikipedia dead-link months")
    k.add_argument("--wiki-limit", type=int, default=150, help="articles per Wikipedia month")
    k.add_argument("--seeds", default="seeds.txt", help="your own seed pages (optional)")
    k.add_argument("--hn-months", type=int, default=4, help="random Hacker News months (100+ point stories)")
    k.add_argument("--debian", type=int, default=1500, help="random Debian package homepages")
    k.add_argument("--cordis", type=int, default=1500, help="random EU research project websites")
    k.add_argument("--no-cc-ranks", action="store_true", help="skip the Common Crawl authority lookup")
    k.add_argument("--sd-pages", type=int, default=40, help="pages (25 rows) of the StableDomains quality feed")
    k.add_argument("--majestic", type=int, default=40000, help="Majestic Million domains to DNS-scan per run")
    k.add_argument("--cc-batch", type=int, default=40000, help="top Common Crawl domains to DNS-scan per run")
    k.add_argument("--wayback-checks", type=int, default=150, help="candidates to verify in the Wayback Machine")
    k.add_argument("--wikidata-types", type=int, default=8, help="random kinds of defunct organisations to read")
    k.add_argument("--publishers", type=int, default=8, help="top publisher / gov sites whose old pages to read")
    k.add_argument("--workers", type=int, default=24)
    k.add_argument("--random-seed", type=int)
    k.set_defaults(fn=cmd_hunt)

    sub.add_parser("selftest", help="offline tests").set_defaults(fn=lambda a: selftest())
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
