# dropfinder

Finds expired, dropping and available domains that still have backlinks from high-authority pages. Fully automatic.

Live site: https://dropfinder.onrender.com

## How it finds domains (no input needed)
Authority first - start from domains that already have strong backlink profiles, then find the dead ones:
1. **StableDomains quality drop feed** - pending-delete / redemption / available domains already scored 0-100 from
   Wayback real-site history, Majestic referring subnets and spam checks (free, paged).
2. **Majestic Million** - the top 1M sites by referring subnets; a rotating batch is DNS + registry checked each run.
3. **Common Crawl web graph** - a rotating window of the top-ranked domains (harmonic centrality) is checked the same way.

Link discovery adds more (each link classified dofollow / nofollow):
4. **Hacker News** front-page stories (100+ points, dofollow), **Debian** package homepages, **EU research project**
   sites (CORDIS), the oldest/resource pages of Tranco-ranked sites, Wikipedia dead-link months, and `seeds.txt`.

Every candidate gets: registry status (RDAP), a **Wayback real-site check** (sampled snapshots: real content vs
parked/for-sale), Common Crawl **authority 0-10**, StableDomains price + Register link (affiliate), and a
**Quality score 0-100** (real history, referring networks, authority, dofollow links, minus spam/parked).

## Automation
`.github/workflows/hunt.yml` runs every 6 hours on GitHub Actions (free for public repos), merges results into
`data/candidates.json` and `data/latest.csv`, re-checks earlier finds and drops ones that got registered.
Run it by hand from the repo's Actions tab: "auto-hunt" > Run workflow.

The web app reads `data/candidates.json` from GitHub (`DATA_URL`), so new finds appear without redeploying.

## Optional settings
- `OPR_API_KEY` (GitHub secret and/or Render env var): free Open PageRank key from keywordseverywhere.com/openpagerank,
  adds PageRank (0-10) and referring-domain columns (30,000 domains/month free).
- `SD_REF`: StableDomains affiliate code for Register links (default wfmff7tz).
- Web limits: `AUTO_SITES=40`, `MAX_DOMAINS=600`, `JOBS_PER_IP_PER_HOUR=4`, `MAX_RUNNING_JOBS=1`.

## Command line
    pip install -r requirements.txt
    python dropfinder.py selftest
    python dropfinder.py hunt --sites 100 --budget-minutes 20
    python dropfinder.py harvest --seeds seeds.txt --sitemaps --ranked-sites 50 --out links.jsonl
    python dropfinder.py check --links links.jsonl --out candidates.csv --wayback --verify

## Statuses
AVAILABLE, PENDING_DELETE (drops within ~5 days), REDEMPTION (~30 days then pending delete),
EXPIRED_GRACE (past expiry, owner can still renew), LIKELY_AVAILABLE (no RDAP but no DNS),
EXPIRING_SOON, PARKED (parking / for-sale page), DEAD_SITE (registered, site down).

## Polite and safe
robots.txt obeyed, 1 request/second per site, private/internal addresses refused (including via redirects),
per-visitor rate limits, all page output escaped.
