# dropfinder web

Finds expired, dropping and soon-to-expire domains that are still linked from pages you choose,
and marks each link dofollow or nofollow.

## Deploy on Render (free, about 10 minutes)
1. Create a new GitHub repository (github.com/new), e.g. `dropfinder`.
2. On the repo page click "uploading an existing file" and drag in every file from this folder. Commit.
3. Go to dashboard.render.com > New > Blueprint, connect GitHub, choose the repo. Render reads
   `render.yaml` and builds it. Your site appears at `https://dropfinder-XXXX.onrender.com`.
4. Optional: Settings > Custom Domain to put it on your own domain.

Free Render services sleep after 15 minutes idle (first visit then takes ~30 s) and results are kept in memory,
so they vanish on restart. The $7/month Starter plan removes the sleep.

## Other hosts
Any Docker host works (Railway, Fly.io, a VPS): `docker build -t dropfinder . && docker run -p 8000:8000 dropfinder`.
Keep gunicorn at ONE worker; use threads for concurrency.

## Limits (environment variables)
MAX_SEEDS=10, MAX_PAGES_PER_SEED=20, MAX_DEPTH=1, MAX_DOMAINS=400, MAX_RUNNING_JOBS=2,
JOBS_PER_IP_PER_HOUR=5, MAX_WIKI_ARTICLES=200, MAX_GITHUB_REPOS=20. Set GITHUB_TOKEN to raise GitHub API limits.

## Command line still works
    python dropfinder.py selftest
    python dropfinder.py harvest --seeds seeds.txt --out links.jsonl
    python dropfinder.py check --links links.jsonl --out candidates.csv --wayback

## Safety built in
- robots.txt obeyed, 1 request/second per site.
- Seed URLs that resolve to private or internal addresses are refused, including via redirects.
- Rate limit per visitor, cap on domains checked per run, all page output escaped.
