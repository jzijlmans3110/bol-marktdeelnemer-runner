"""Bol EAN-recycle runner (GitHub multi-IP, GEEN GPU nodig).

Per bestaande offer (oude EAN + ASIN): verse EAN-13 aanmaken met volledige content
(via Bol's datamodel), nieuwe offer + marktdeelnemer -> live "te koop".

Env:
    BOL_CLIENT_ID / BOL_CLIENT_SECRET   - OAuth client van het account
    BOL_SHARD                            - "i/N" (md5(ean) % N == i)
    BOL_WORKERS                          - parallel workers per shard (default 8)
    BOL_NAME                             - lognaam
    BOL_USE_CACHE=1                      - hergebruik bol_export.csv (door fetch-job gemaakt)
    RECYCLE_MAX                          - max per run (default alle)
Vereist in working dir: datamodel_v10_nl.json + bol_blacklist.json
Output: recycle_done_<name>.csv (old_ean,new_ean,asin,status)
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import random
import re
import secrets
import sys
import threading
import time
from pathlib import Path

import runner as R  # hergebruik TokenManager, list_operators, fresh_export, _session, log, API

API = R.API
ACC = "application/vnd.retailer.v10+json"
SHARED_STATUS = "https://api.bol.com/shared/process-status/{}"
DM_PATH = Path("datamodel_v10_nl.json")
BL_PATH = Path("bol_blacklist.json")
MAX_LEN = 70

_log = R.log


# ----------------------------- datamodel -------------------------------
class DataModel:
    def __init__(self, path: Path = DM_PATH):
        d = json.loads(path.read_text(encoding="utf-8"))
        self.chunks = {str(c["id"]): {a["id"]: a.get("enrichmentLevel", 9)
                                      for a in c.get("attributes", [])}
                       for c in d.get("chunks", [])}

    def allowed(self, chunk):
        return self.chunks.get(str(chunk), {})

    def title_id(self, chunk):
        a = self.allowed(chunk)
        for cand in ("Name", "Title", "Product Title", "Model Name"):
            if cand in a:
                return cand
        return None


# ----------------------------- titel opschonen (rule-based, geen LLM) --
def _load_blacklist(path: Path) -> list[str]:
    d = json.loads(path.read_text(encoding="utf-8"))
    words = []
    for cv in d.get("categories", {}).values():
        if isinstance(cv, dict):
            for v in cv.values():
                if isinstance(v, list):
                    words += [str(x) for x in v]
        elif isinstance(cv, list):
            words += [str(x) for x in cv]
    return sorted({w.lower() for w in words if isinstance(w, str) and w.strip()},
                  key=len, reverse=True)


class TitleCleaner:
    def __init__(self):
        self.bl = _load_blacklist(BL_PATH) if BL_PATH.exists() else []
        # alleen alfabetische verboden woorden als hele-woord regex
        words = [re.escape(w) for w in self.bl if w.isalpha() and len(w) > 2]
        self.word_re = re.compile(r"\b(" + "|".join(words) + r")\b", re.I) if words else None
        self.phrase = [w for w in self.bl if not w.isalpha()]

    def clean(self, title: str) -> str:
        t = (title or "").strip()
        for p in self.phrase:
            t = t.replace(p, " ")
        if self.word_re:
            t = self.word_re.sub(" ", t)
        t = re.sub(r"[!?@#$%^*<>|]+", " ", t)          # verboden symbolen
        t = re.sub(r"\s+", " ", t).strip(" -|,")
        if len(t) > MAX_LEN:                            # afkappen op woordgrens
            cut = t[:MAX_LEN]
            sp = cut.rfind(" ")
            t = cut[:sp] if sp > MAX_LEN * 0.4 else cut
        # woorden in HOOFDLETTERS -> Title-case
        t = " ".join(w.title() if w.isupper() and len(w) > 1 else w for w in t.split())
        return t.strip(" -|,")


# ----------------------------- Bol calls -------------------------------
def _req(token, method, path_or_url, body=None, accept=ACC):
    url = path_or_url if path_or_url.startswith("http") else f"{API}/retailer{path_or_url}"
    H = {"Authorization": f"Bearer {token}", "Accept": accept}
    if body is not None:
        H["Content-Type"] = ACC
    for attempt in range(6):
        r = R._session().request(method, url, headers=H,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 timeout=(10, 60))
        if r.status_code == 429:
            time.sleep(min(2 * (attempt + 1), 15))
            continue
        return r
    return r


def _wait(token, pid, max_wait=300):
    url = SHARED_STATUS.format(pid)
    end = time.time() + max_wait
    while time.time() < end:
        r = _req(token, "GET", url)
        if r.status_code == 200:
            j = r.json()
            if j.get("status") in ("SUCCESS", "FAILURE", "TIMEOUT"):
                return j
        time.sleep(4)
    return {"status": "TIMEOUT"}


def _ean13(rng):
    body = "2" + "".join(rng.choice("0123456789") for _ in range(11))
    s = sum(int(d) * (3 if i % 2 else 1) for i, d in enumerate(body))
    return body + str((10 - s % 10) % 10)


def build_body(new_ean, cat, images, gen_title, dm):
    out = [{"id": "EAN", "values": [{"value": new_ean}]}]
    chunk = (cat.get("gpc") or {}).get("chunkId")
    if chunk:
        out.append({"id": "GPC Code", "values": [{"value": str(chunk)}]})
    allowed = dm.allowed(chunk)
    title_id = dm.title_id(chunk)
    has_brand = False
    for a in cat.get("attributes", []):
        aid = a.get("id")
        vals = a.get("values")
        if not aid or aid in ("EAN", "Title") or not vals:
            continue
        if allowed and aid not in allowed:
            continue
        if aid == "Brand":
            has_brand = True
        out.append({"id": aid, "values": vals})
    if gen_title and title_id:
        out.append({"id": title_id, "values": [{"value": gen_title[:200]}]})
    if not has_brand and (not allowed or "Brand" in allowed):
        out.append({"id": "Brand", "values": [{"value": "Merkloos"}]})
    body = {"language": "nl", "attributes": out}
    if images:
        body["assets"] = [{"url": u, "labels": ["FRONT" if i == 0 else "OTHER"]}
                          for i, u in enumerate(images[:8])]
    return body


def recycle_one(tm, op_id, dm, cleaner, row, blocked, ean_lock, rng):
    token = tm.get()
    oe = (row.get("ean") or "").strip()
    asin = (row.get("referenceCode") or "").strip()
    dcode = (row.get("fulfilmentDeliveryCode") or "3-5d").strip() or "3-5d"
    if not oe:
        return ("", "skip_no_ean")
    rc = _req(token, "GET", f"/content/catalog-products/{oe}")
    if rc.status_code != 200:
        return ("", "skip_no_catalog")
    cat = rc.json()
    ra = _req(token, "GET", f"/products/{oe}/assets")
    images = []
    if ra.status_code == 200:
        for a in ra.json().get("assets", []):
            best, bw = None, -1
            for v in a.get("variants", []):
                w = v.get("width", 0) or 0
                if w > bw and v.get("url"):
                    best, bw = v["url"], w
            if best:
                images.append(best)
    seed = next((a["values"][0]["value"] for a in cat.get("attributes", [])
                 if a["id"] in ("Title", "Name") and a.get("values")), "")
    title = cleaner.clean(seed)
    with ean_lock:
        ne = _ean13(rng)
        while ne in blocked:
            ne = _ean13(rng)
        blocked.add(ne)
    body = build_body(ne, cat, images, title, dm)
    rp = _req(token, "POST", "/content/products", body)
    if rp.status_code not in (200, 202):
        return (ne, f"content_http_{rp.status_code}")
    if _wait(token, rp.json()["processStatusId"]).get("status") != "SUCCESS":
        return (ne, "content_fail")
    offer = {"ean": ne, "condition": {"name": "NEW"},
             "pricing": {"bundlePrices": [{"quantity": 1, "unitPrice": float(row.get("bundlePricesPrice") or 9.99)}]},
             "stock": {"amount": 10, "managedByRetailer": True},
             "fulfilment": {"method": "FBR", "deliveryCode": dcode}}
    ro = _req(token, "POST", "/offers", offer)
    if ro.status_code not in (200, 202):
        return (ne, f"offer_http_{ro.status_code}")
    ost = _wait(token, ro.json()["processStatusId"])
    if ost.get("status") != "SUCCESS":
        return (ne, "offer_fail")
    offer_id = ost.get("entityId", "")
    # marktdeelnemer (alleen als er een geldige operator is)
    if not op_id:
        return (ne, "done_no_op")
    put = {"reference": asin, "onHoldByRetailer": False,
           "fulfilment": {"method": "FBR", "deliveryCode": dcode},
           "economicOperatorId": op_id}
    rput = _req(token, "PUT", f"/offers/{offer_id}", put)
    if rput.status_code in (200, 202):
        _wait(token, rput.json().get("processStatusId", ""))
    return (ne, "done")


def main() -> int:
    cid = os.environ["BOL_CLIENT_ID"]
    csec = os.environ["BOL_CLIENT_SECRET"]
    shard = os.environ.get("BOL_SHARD", "")
    workers = int(os.environ.get("BOL_WORKERS", "8"))
    name = os.environ.get("BOL_NAME", "Recycle")
    use_cache = os.environ.get("BOL_USE_CACHE", "") == "1"
    fetch_only = os.environ.get("BOL_FETCH_ONLY", "") == "1"
    maxrun = int(os.environ.get("RECYCLE_MAX", "100000000"))

    sn = sm = None
    if "/" in shard:
        a, b = shard.split("/")
        sn, sm = int(a), int(b)

    tm = R.TokenManager(cid, csec)
    ops = [o for o in R.list_operators(tm.get()) if o.get("status") == "VALID"]
    op_id = ops[0]["id"] if ops else None
    if os.environ.get("BOL_SKIP_OPERATOR") == "1":
        op_id = None
    if op_id:
        _log(f"[{name}] operator {ops[0].get('name')} ({op_id})")
    else:
        _log(f"[{name}] GEEN marktdeelnemer -> listings worden aangemaakt maar blijven 'niet te koop' tot operator gezet")

    if not (use_cache and R.CACHE.exists() and R.CACHE.stat().st_size > 0):
        R.fresh_export(tm, name)
    if fetch_only:
        _log(f"[{name}] fetch-only klaar"); return 0

    with open(R.CACHE, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    # recycle alleen originele (2-prefix interne) offers; sla evt. al-gerecyclede over kan niet
    todo = [r for r in rows if (r.get("ean") or "").strip()]
    if sm:
        todo = [r for r in todo
                if int(hashlib.md5(r["ean"].encode()).hexdigest(), 16) % sm == sn]
    todo = todo[:maxrun]
    _log(f"[{name}] CSV {len(rows)}, shard-todo {len(todo)}")
    if not todo:
        return 0

    dm = DataModel()
    cleaner = TitleCleaner()
    blocked = set()
    ean_lock = threading.Lock()
    log_lock = threading.Lock()
    rng = random.Random(secrets.randbits(64))
    out_path = Path(f"recycle_done_{name}.csv")
    new_file = not out_path.exists()
    fout = open(out_path, "a", newline="", encoding="utf-8")
    w = csv.writer(fout)
    if new_file:
        w.writerow(["old_ean", "new_ean", "asin", "status"])

    cnt = {"n": 0, "ok": 0}
    started = time.time()

    def do(row):
        oe = (row.get("ean") or "").strip()
        try:
            ne, st = recycle_one(tm, op_id, dm, cleaner, row, blocked, ean_lock, rng)
        except Exception as e:
            ne, st = "", f"err_{type(e).__name__}"
        with log_lock:
            w.writerow([oe, ne, row.get("referenceCode", ""), st]); fout.flush()
            cnt["n"] += 1
            if st in ("done", "done_no_op"):
                cnt["ok"] += 1
            if cnt["n"] % 50 == 0:
                el = time.time() - started
                rate = cnt["n"] / el * 60
                eta = (len(todo) - cnt["n"]) / (cnt["n"] / el) / 60 if cnt["n"] else 0
                _log(f"[{name}] {cnt['n']}/{len(todo)} ok={cnt['ok']} {rate:.0f}/min eta={eta:.0f}min")

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(do, todo))
    fout.close()
    _log(f"[{name}] KLAAR {cnt['n']} verwerkt, {cnt['ok']} te koop, {(time.time()-started)/60:.1f}min")
    return 0


if __name__ == "__main__":
    sys.exit(main())
