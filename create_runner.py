"""Cloud combined-create runner. Maakt offers aan uit een CSV, elke create-call
zet meteen economicOperatorId (md) + volumestaffel (vlm).

Env:
  BOL_CLIENT_ID / BOL_CLIENT_SECRET  - app-creds (per shard eigen app)
  BOL_SHARD        - "n/m": verwerkt alleen rij-index i waar i % m == n
  BOL_LIST_FILE    - pad naar CSV(.gz) met kolommen: ean,ref,price,stock,dcode,title
  BOL_WORKERS      - threads (default 15)
  BOL_429_BACKOFF  - sec (default 5)
  BOL_LIMIT        - test: alleen eerste N van de shard (0 = alles)
  BOL_NAME         - label voor logs
"""
import os, sys, csv, json, time, threading, base64, gzip, urllib.request
from collections import deque
import requests

API = "https://api.bol.com"
ACC = "application/vnd.retailer.v10+json"
ACC_OP = "application/vnd.economic-operator.v1+json"
TIERS = [(2, 0.03), (3, 0.05), (5, 0.10)]

CID = os.environ["BOL_CLIENT_ID"]
CSEC = os.environ["BOL_CLIENT_SECRET"]
SHARD = os.environ.get("BOL_SHARD", "")
WORKERS = int(os.environ.get("BOL_WORKERS", "15"))
BACKOFF = float(os.environ.get("BOL_429_BACKOFF", "5"))
LIST_FILE = os.environ.get("BOL_LIST_FILE", "lists/create.csv.gz")
LIMIT = int(os.environ.get("BOL_LIMIT", "0"))
NAME = os.environ.get("BOL_NAME", "create")
MAX_ATT = int(os.environ.get("BOL_MAX_ATTEMPTS", "8"))

shard_n = shard_m = None
if "/" in SHARD:
    a, b = SHARD.split("/"); shard_n, shard_m = int(a), int(b)


def _token():
    b = base64.b64encode(f"{CID}:{CSEC}".encode()).decode()
    req = urllib.request.Request(
        "https://login.bol.com/token?grant_type=client_credentials",
        method="POST", headers={"Authorization": f"Basic {b}", "Accept": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=30).read())["access_token"]


class TM:
    def __init__(self): self.t = None; self.ts = 0.0; self.lock = threading.Lock()
    def get(self):
        with self.lock:
            if self.t is None or time.time() - self.ts > 240:
                self.t = _token(); self.ts = time.time()
            return self.t
    def refresh(self):
        with self.lock:
            self.t = _token(); self.ts = time.time(); return self.t


tm = TM()

req = urllib.request.Request(f"{API}/retailer/economic-operators",
      headers={"Authorization": f"Bearer {tm.get()}", "Accept": ACC_OP})
ops = json.loads(urllib.request.urlopen(req, timeout=30).read()).get("operators", [])
OP = next((o["id"] for o in ops if o.get("status") == "VALID"), None)
print(f"[{NAME}] operator={OP}", flush=True)
if not OP:
    sys.exit("geen VALID operator")


def bundle(base):
    base = round(float(base), 2); out = [{"quantity": 1, "unitPrice": base}]; prev = base
    for q, d in TIERS:
        up = round(base * (1 - d), 2)
        if up >= prev: up = round(prev - 0.01, 2)
        if up <= 0: break
        out.append({"quantity": q, "unitPrice": up}); prev = up
    return out


op_open = gzip.open if LIST_FILE.endswith(".gz") else open
work = deque()
with op_open(LIST_FILE, "rt", encoding="utf-8", newline="") as fh:
    for i, r in enumerate(csv.DictReader(fh)):
        if shard_m is not None and i % shard_m != shard_n:
            continue
        work.append(r)
        if LIMIT and len(work) >= LIMIT:
            break
print(f"[{NAME}] shard={SHARD} rijen={len(work)}", flush=True)

ok = fail = 0
lock = threading.Lock()
t0 = time.time()


def worker():
    global ok, fail
    sess = requests.Session()
    while True:
        with lock:
            if not work: break
            r = work.popleft()
        ean = (r.get("ean") or "").strip()
        price = (r.get("price") or "").strip()
        if not ean or not price:
            with lock: fail += 1
            continue
        body = {"ean": ean, "condition": {"name": "NEW"}, "reference": (r.get("ref") or "")[:20],
                "onHoldByRetailer": False, "unknownProductTitle": (r.get("title") or "product")[:500],
                "pricing": {"bundlePrices": bundle(price)},
                "stock": {"amount": int(float(r.get("stock") or 50)), "managedByRetailer": False},
                "fulfilment": {"method": "FBR", "deliveryCode": (r.get("dcode") or "3-5d")},
                "economicOperatorId": OP}
        for att in range(MAX_ATT):
            try:
                h = {"Authorization": f"Bearer {tm.get()}", "Accept": ACC, "Content-Type": ACC}
                resp = sess.post(f"{API}/retailer/offers", headers=h, json=body, timeout=30)
                if resp.status_code == 202:
                    with lock: ok += 1
                    break
                if resp.status_code == 429 or resp.status_code >= 500:
                    time.sleep(BACKOFF); continue
                if resp.status_code == 401:
                    tm.refresh(); continue
                with lock: fail += 1
                break
            except Exception:
                time.sleep(BACKOFF)
        else:
            with lock: fail += 1


def reporter():
    while True:
        time.sleep(30)
        with lock:
            d = ok + fail; rem = len(work)
        el = time.time() - t0
        print(f"[{NAME}] ok={ok} fail={fail} rest={rem} rate={d/el:.1f}/s", flush=True)
        if rem == 0: break


ts = [threading.Thread(target=worker, daemon=True) for _ in range(WORKERS)]
rp = threading.Thread(target=reporter, daemon=True)
for t in ts: t.start()
rp.start()
for t in ts: t.join()
print(f"[{NAME}] === KLAAR === ok={ok} fail={fail} in {(time.time()-t0)/60:.1f}min", flush=True)
