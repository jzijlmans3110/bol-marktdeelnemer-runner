"""Mini-test: kan een GitHub-runner (Azure datacenter-IP) Amazon.de scrapen
zonder geblokkeerd te worden? Self-contained (alleen requests).
Elke shard pakt een stride uit de ASIN-lijst. Print runner-IP + per ASIN
ok/blocked/other + een samenvatting.
"""
import os, time, random, sys
import requests

ASINS = [
    "B07DN5S61Q","B0CL6NNWQR","B0CKZ2S13R","B07BXW8PPY","B0DQQDVHPJ","B0BVRFSY79",
    "B01MUB52OA","B01LZK7APG","B0767ML2TX","B06X6F7C3S","B07581CN4M","B0H2JNMWFY",
    "B0H39BGZBG","B0H378SS6T","B0768L84P7","B01LWYZ2IM","B084VTG34J","B0767GFZVC",
    "B0H2J9HX28","B01BYTG2HA","B084VT5ZHM","B01BYTG47I","B01M1SC4F4","B07583ZQF6",
    "B01BYTG17G","B077G8HGCS","B078BJFBN7","B0767KDN7N","B06X6FMB8R","B077VRCJ4R",
    "B0H373XZ64","B0H393QR73","B0H39FD3HH","B07582JN5C","B077VTHTNY","B01BYTG0WM",
    "B07D3V6TBL","B084VTGH96","B0H373HT6Q","B0H2JHGZ92",
]

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]
BLOCK_MARKERS = ("captcha","api-services-support@amazon","automated access","robot check",
                 "enter the characters you see below","tut uns leid","to discuss automated access")

def headers():
    return {
        "User-Agent": random.choice(UA_POOL),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-Fetch-Dest": "document", "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none", "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }

def classify(status, body):
    low = body.lower()
    if status in (403, 503) or any(m in low[:8000] for m in BLOCK_MARKERS):
        return "BLOCKED"
    if status == 200 and ('id="productTitle"' in body or 'id="title"' in body):
        return "OK"
    return f"OTHER({status})"

def main():
    shard = int(os.getenv("SHARD", "0"))
    count = int(os.getenv("SHARD_COUNT", "4"))
    mine = ASINS[shard::count]
    s = requests.Session()
    try:
        ip = s.get("https://api.ipify.org", timeout=10).text
    except Exception:
        ip = "?"
    print(f"=== shard {shard}/{count} | runner-IP {ip} | {len(mine)} ASINs ===", flush=True)
    tally = {"OK":0,"BLOCKED":0}
    for a in mine:
        url = f"https://www.amazon.de/dp/{a}?language=de_DE"
        try:
            r = s.get(url, headers=headers(), timeout=30)
            res = classify(r.status_code, r.text or "")
        except Exception as e:
            res = f"ERR({type(e).__name__})"
        tally[res] = tally.get(res, 0) + 1
        print(f"  {a}: {res}", flush=True)
        time.sleep(random.uniform(1.0, 2.0))
    print(f"=== SHARD {shard} SAMENVATTING | IP {ip} | "
          + " ".join(f"{k}={v}" for k,v in sorted(tally.items())), flush=True)

if __name__ == "__main__":
    main()
