"""One-off: fetch sample pages from candidate platforms and report the rel attribute of their outbound links."""
import re
import sys
from collections import Counter
from html.parser import HTMLParser
from urllib.parse import urlparse

import requests

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
PAGES = {
    "npm": "https://www.npmjs.com/package/express",
    "PyPI": "https://pypi.org/project/requests/",
    "RubyGems": "https://rubygems.org/gems/rails",
    "Packagist": "https://packagist.org/packages/laravel/framework",
    "WordPress plugin": "https://wordpress.org/plugins/akismet/",
    "WordPress theme": "https://wordpress.org/themes/astra/",
    "Gravatar": "https://gravatar.com/matt",
    "GitHub profile": "https://github.com/sindresorhus",
    "GitHub repo": "https://github.com/expressjs/express",
    "GitLab project": "https://gitlab.com/gitlab-org/gitlab",
    "Firefox add-ons": "https://addons.mozilla.org/en-GB/firefox/addon/ublock-origin/",
    "Stack Overflow user": "https://stackoverflow.com/users/22656/jon-skeet",
    "dev.to profile": "https://dev.to/ben",
    "HN user": "https://news.ycombinator.com/user?id=pg",
    "SourceForge": "https://sourceforge.net/projects/keepass/",
    "Open Hub": "https://openhub.net/p/firefox",
    "AlternativeTo": "https://alternativeto.net/software/keepass/about/",
    "Libraries.io": "https://libraries.io/npm/express",
    "Launchpad": "https://launchpad.net/inkscape",
    "F-Droid": "https://f-droid.org/en/packages/org.fdroid.fdroid/",
    "Homebrew": "https://formulae.brew.sh/formula/wget",
    "Chocolatey": "https://community.chocolatey.org/packages/git",
    "Arch packages": "https://archlinux.org/packages/extra/x86_64/wget/",
    "Gentoo packages": "https://packages.gentoo.org/packages/net-misc/wget",
    "FreshPorts": "https://www.freshports.org/ftp/wget/",
    "Softpedia": "https://www.softpedia.com/get/Security/Password-Managers-Generators/KeePass.shtml",
    "itch.io": "https://itch.io/profile/leafo",
    "Behance": "https://www.behance.net/adobe",
    "Dribbble": "https://dribbble.com/dribbble/about",
    "Medium profile": "https://medium.com/@ev",
    "Reddit (old) post": "https://old.reddit.com/r/programming/top/?t=all",
    "Product Hunt": "https://www.producthunt.com/products/notion",
    "Crunchbase": "https://www.crunchbase.com/organization/airbnb",
    "Wellfound": "https://wellfound.com/company/airbnb",
    "Kickstarter": "https://www.kickstarter.com/projects/elanlee/exploding-kittens",
    "Indiegogo": "https://www.indiegogo.com/projects/the-ultimate-hackers-keyboard",
    "Open Collective": "https://opencollective.com/webpack",
    "Docker Hub": "https://hub.docker.com/_/nginx",
    "Wikidata": "https://www.wikidata.org/wiki/Q95",
    "Wikipedia": "https://en.wikipedia.org/wiki/KeePass",
    "Debian": "https://packages.debian.org/bookworm/wget",
    "Ubuntu Launchpad PPA": "https://launchpad.net/~ubuntu-mozilla-security",
    "IMDb": "https://www.imdb.com/title/tt0111161/externalsites/",
    "Goodreads author": "https://www.goodreads.com/author/show/1077326.J_K_Rowling",
    "Discogs label": "https://www.discogs.com/label/1-Warp",
    "MusicBrainz": "https://musicbrainz.org/artist/a74b1b7f-71a5-4011-9441-d0b5e4122711",
    "Last.fm": "https://www.last.fm/music/Radiohead/+wiki",
    "Bandcamp": "https://radiohead.bandcamp.com/",
    "OpenStreetMap wiki": "https://wiki.openstreetmap.org/wiki/Main_Page",
    "Fandom wiki": "https://minecraft.fandom.com/wiki/Minecraft_Wiki",
    "Mastodon profile": "https://mastodon.social/@Gargron",
    "Bluesky profile": "https://bsky.app/profile/jay.bsky.team",
    "Linktree": "https://linktr.ee/selenagomez",
    "about.me": "https://about.me/johnsmith",
    "Archive.org item": "https://archive.org/details/texts",
    "Scribd": "https://www.scribd.com/",
    "Issuu": "https://issuu.com/vogue",
    "Trustpilot": "https://www.trustpilot.com/review/www.amazon.com",
    "Yelp": "https://www.yelp.com/biz/the-french-laundry-yountville",
    "Clutch": "https://clutch.co/profile/toptal",
    "G2": "https://www.g2.com/products/notion/reviews",
    "Capterra": "https://www.capterra.com/p/186596/Notion/",
    "Slashdot": "https://slashdot.org/",
    "Lobsters": "https://lobste.rs/",
    "Quora": "https://www.quora.com/profile/Adam-DAngelo",
}


class A(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            d = dict(attrs)
            if d.get("href", "").startswith("http"):
                self.links.append((d["href"], (d.get("rel") or "").lower()))


def host(u):
    h = (urlparse(u).hostname or "").lower()
    return h[4:] if h.startswith("www.") else h


def base(h):
    p = h.split(".")
    return ".".join(p[-3:] if len(p) > 2 and len(p[-1]) == 2 and p[-2] in ("co", "com", "org", "ac", "gov") else p[-2:])


s = requests.Session()
s.headers.update({"User-Agent": UA, "Accept-Language": "en-GB,en;q=0.9"})
for name, url in PAGES.items():
    try:
        r = s.get(url, timeout=25)
        html = r.text
    except Exception as e:  # noqa: BLE001
        print(f"{name:22} ERROR {type(e).__name__}")
        continue
    p = A()
    try:
        p.feed(html)
    except Exception:  # noqa: BLE001
        pass
    own = base(host(url))
    ext = [(u, rel) for u, rel in p.links if base(host(u)) != own and not re.search(r"(google|facebook|twitter|x\.com|apple|microsoft|cloudflare|gstatic)", host(u))]
    c = Counter("nofollow" if ("nofollow" in rel or "ugc" in rel or "sponsored" in rel) else "FOLLOW" for _, rel in ext)
    ex = [f"{host(u)}[{rel or '-'}]" for u, rel in ext[:6]]
    print(f"{name:22} HTTP {r.status_code} ext={len(ext):3} follow={c['FOLLOW']:3} nofollow={c['nofollow']:3}  e.g. {' '.join(ex)}")
    sys.stdout.flush()
