import requests, json, time
s = requests.Session(); s.headers["User-Agent"] = "Mozilla/5.0 (compatible; dropfinder/1.0)"
coins = s.get("https://api.coinpaprika.com/v1/coins", timeout=60).json()
ina = [c for c in coins if not c.get("is_active", True)]
print("inactive", len(ina), "sample", ina[:3])
ranked = [c for c in ina if c.get("rank")]
print("inactive with rank>0:", len(ranked))
for c in ina[:5] + ranked[:5]:
    r = s.get(f"https://api.coinpaprika.com/v1/coins/{c['id']}", timeout=30)
    try: j = r.json()
    except Exception: j = {}
    print(c["id"], r.status_code, r.headers.get("x-ratelimit-remaining"), json.dumps(j.get("links", j))[:200], j.get("started_at"))
    time.sleep(1)
