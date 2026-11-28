"""One-off probe: status, size and a peek at candidate source URLs."""
import re, sys, bz2, json
import requests
UA = "Mozilla/5.0 (compatible; dropfinder/1.0; +https://dropfinder.onrender.com)"
s = requests.Session(); s.headers["User-Agent"] = UA
def peek(name, url, n=600, binary=False):
    try:
        r = s.get(url, timeout=60)
        body = r.content
        print(f"== {name} {r.status_code} {len(body):,} bytes ct={r.headers.get('content-type')}")
        if binary:
            return body
        t = body.decode("utf-8", "replace")
        ext = re.findall(r'<a [^>]*href="(https?://[^"]+)"[^>]*>', t)
        own = [u for u in ext if "slashdot" not in u]
        print(f"   external links: {len(own)}; sample: {own[:8]}")
        rels = re.findall(r'<a [^>]*href="https?://(?![^"]*slashdot)[^"]+"[^>]*>', t)[:4]
        print("   tags:", rels)
        print("   head:", re.sub(r"\s+", " ", t[:n]))
        return body
    except Exception as e:
        print(f"== {name} ERROR {e}")
peek("slashdot robots", "https://slashdot.org/robots.txt", 1500)
for u in ["https://slashdot.org/?issue=20080512", "https://slashdot.org/index2.pl?issue=20080512",
          "https://slashdot.org/story/08/05/12/1438206/", "https://slashdot.org/archive.pl?op=bytime&keyword=&year=2008",
          "https://tech.slashdot.org/story/08/05/12/1438206"]:
    peek("slashdot " + u.split("slashdot.org")[1][:40], u, 300)
b = peek("arch json", "https://archlinux.org/packages/search/json/?page=1&limit=250", 400)
try:
    j = json.loads(b); print("   arch keys:", list(j.keys()), "num_pages", j.get("num_pages"), "first:", {k: j["results"][0].get(k) for k in ("pkgname","url","repo","arch")})
except Exception as e: print("   arch parse", e)
b = peek("freebsd INDEX", "https://download.freebsd.org/ports/index/INDEX-14.bz2", binary=True)
try:
    lines = bz2.decompress(b).decode("utf-8","replace").splitlines(); print("   lines", len(lines)); print("   ex:", lines[0][:400])
except Exception as e: print("   fb parse", e)
b = peek("fdroid index", "https://f-droid.org/repo/index-v1.json", binary=True)
try:
    j = json.loads(b); a = j["apps"][0]; print("   apps", len(j["apps"]), {k: a.get(k) for k in ("packageName","webSite","sourceCode","name")})
except Exception as e: print("   fd parse", e)
peek("gentoo", "https://packages.gentoo.org/packages/net-misc/wget.json", 400)
