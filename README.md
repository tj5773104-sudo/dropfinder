# dropfinder

Finds expired, dropping and available domains that still have backlinks from high-authority pages. Fully automatic.

Live site: https://dropfinder.onrender.com

## How it finds domains (no input needed)
1. **Ranked sites:** picks random sites from the Tranco top-1M list and reads their *oldest* pages via sitemaps
   (old pages hold the most dead links).
2. **Wikipedia:** mines random "Articles with dead external links from <month>" categories.
3. **Your seeds (optional):** anything listed in `seeds.txt`.
4. Every link is classified dofollow / nofollow, and each linking site gets an authority weight from its Tranco rank
   (.edu/.gov boosted).
5. Every linked-to domain is checked: DNS, then RDAP (available, pending delete, redemption, expired-in-grace).
   Strongly linked domains that still resolve are visited to catch dead and parked sites.
6. Available domains are confirmed with StableDomains (price + Register link with affiliate code), and Wayback
   history gives domain age. Spammy names are flagged.

## Automation
`.github/workflows/hunt.yml` runs every 6 hours on GitHub Actions (free for public repos), merges results into
`data/candidates.json` and `data/latest.csv`, re-checks earlier finds and drops ones that got registered.
Run it by hand from the repo's Actions tab: "auto-hunt" > Run workflow.

The web app reads `data/candidates.json` from GitHub (`DATA_URL`), so new finds appear without redeploying.

## Optional settings
- `OPR_API_KEY` (GitHub secret and/or Render env var): free Open PageRank key from domcop.com/openpagerank,
  adds a PageRank column.
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
