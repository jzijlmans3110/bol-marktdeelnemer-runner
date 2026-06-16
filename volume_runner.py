"""Standalone Bol volumekorting (bundlePrices) apply runner.

Pas een vaste staffel toe op ALLE live offers van een winkel:
    2 stuks -3% , 3 stuks -5% , 5 stuks -10%  (t.o.v. huidige stukprijs)

Werkt identiek aan runner.py (fetch/shard/token), maar doet per offer een
PUT /retailer/offers/{offerId}/price i.p.v. de marktdeelnemer-PUT.
Idempotent: opnieuw draaien zet dezelfde staffel weer -> geen kwaad.

Env vars:
    BOL_CLIENT_ID / BOL_CLIENT_SECRET   - OAuth app
    BOL_SHARD       - bv "1/16" (alleen offers waar md5(id) % 16 == 1)
    BOL_WORKERS     - parallel workers (default 20)
    BOL_MAX         - max offers per run (default 1000000)
    BOL_NAME        - logging-naam
    BOL_FETCH_ONLY  - '1' = alleen CSV-export trekken en stoppen
    BOL_USE_CACHE   - '1' = bestaande bol_export.csv hergebruiken
    BOL_429_BACKOFF - seconden backoff bij 429 (default 5)
"""
from __future__ import annotations

import base64
import csv
import hashlib
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

API = "https://api.bol.com"
ACCEPT_RETAILER = "application/vnd.retailer.v10+json"
ACCEPT_CSV = "application/vnd.retailer.v10+csv"

CACHE = Path(os.environ.get("BOL_CACHE_PATH", "./bol_export.csv"))
TOKEN_TTL_SEC = 540
PROGRESS_EVERY = 200

# Staffel: quantity -> kortingsfactor t.o.v. stukprijs
TIERS = [(2, 0.03), (3, 0.05), (5, 0.10)]

_log_lock = threading.Lock()


def log(msg: str) -> None:
    line = f"[{datetime.utcnow().isoformat(timespec='seconds')}Z] {msg}"
    with _log_lock:
        print(line, flush=True)


def bundle_prices(base) -> list[dict]:
    """Bouw bundlePrices-array van een stukprijs. Strikt dalend afgedwongen."""
    base = round(float(base), 2)
    out = [{"quantity": 1, "unitPrice": base}]
    prev = base
    for qty, disc in TIERS:
        up = round(base * (1 - disc), 2)
        if up >= prev:                 # voorkom gelijke/oplopende stukprijs door afronding
            up = round(prev - 0.01, 2)
        if up <= 0:                    # te lage basisprijs -> geen verdere staffel
            break
        out.append({"quantity": qty, "unitPrice": up})
        prev = up
    return out


def _get_token_raw(cid: str, csec: str) -> str:
    creds = base64.b64encode(f"{cid}:{csec}".encode()).decode()
    req = urllib.request.Request(
        "https://login.bol.com/token?grant_type=client_credentials",
        method="POST",
        headers={"Authorization": f"Basic {creds}", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())["access_token"]


class TokenManager:
    def __init__(self, cid: str, csec: str) -> None:
        self.cid, self.csec = cid, csec
        self.token: str | None = None
        self.t = 0.0
        self.lock = threading.Lock()

    def get(self) -> str:
        with self.lock:
            if self.token is None or (time.time() - self.t) > TOKEN_TTL_SEC:
                self.token = _get_token_raw(self.cid, self.csec)
                self.t = time.time()
        return self.token

    def force_refresh(self) -> str:
        with self.lock:
            self.token = _get_token_raw(self.cid, self.csec)
            self.t = time.time()
        return self.token


class ExportBusy(Exception):
    pass


def _request_export(token: str, name: str) -> str:
    H = {
        "Authorization": f"Bearer {token}",
        "Accept": ACCEPT_RETAILER,
        "Content-Type": ACCEPT_RETAILER,
    }
    req = urllib.request.Request(
        f"{API}/retailer/offers/export", method="POST", headers=H,
        data=json.dumps({"format": "CSV"}).encode(),
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        pid = json.loads(r.read())["processStatusId"]
    H2 = {"Authorization": f"Bearer {token}", "Accept": ACCEPT_RETAILER}
    for i in range(600):
        time.sleep(3)
        req = urllib.request.Request(f"{API}/shared/process-status/{pid}", headers=H2)
        with urllib.request.urlopen(req, timeout=30) as r:
            st = json.loads(r.read())
        if st.get("status") == "SUCCESS":
            return st["entityId"]
        if st.get("status") == "FAILURE":
            msg = st.get("errorMessage", "")
            if "already active" in msg.lower() or "similar specification" in msg.lower():
                raise ExportBusy(msg)
            raise RuntimeError(f"export FAIL: {msg}")
        if i and i % 60 == 0:
            log(f"[{name}] export bezig na {i*3}s")
    raise RuntimeError("export timeout")


def _download_csv(token: str, rid: str, path: Path) -> int:
    import requests
    H = {"Authorization": f"Bearer {token}", "Accept": ACCEPT_CSV}
    total = 0
    with requests.get(
        f"{API}/retailer/offers/export/{rid}", headers=H, stream=True, timeout=(30, 600)
    ) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=131072):
                if chunk:
                    f.write(chunk)
                    total += len(chunk)
    return total


def fresh_export(tm: TokenManager, name: str) -> Path:
    busy_wait = 60
    for outer in range(1, 21):
        log(f"[{name}] verse offer-export trekken (ronde {outer})")
        try:
            rid = _request_export(tm.get(), name)
            log(f"[{name}] export rid={rid}, CSV downloaden")
        except ExportBusy as e:
            log(f"[{name}] export busy ('{str(e)[:80]}'), wacht {busy_wait}s")
            time.sleep(busy_wait)
            busy_wait = min(busy_wait + 30, 300)
            continue
        except urllib.error.HTTPError as e:
            log(f"[{name}] export-request {e.code}, wacht 90s")
            time.sleep(90)
            continue
        for attempt in range(4):
            try:
                total = _download_csv(tm.get(), rid, CACHE)
                log(f"[{name}] CSV: {total/1_000_000:.1f} MB")
                return CACHE
            except Exception as e:
                log(f"[{name}] download faalde poging {attempt+1}: {type(e).__name__}: {str(e)[:120]}")
                time.sleep(10 * (attempt + 1))
        time.sleep(60)
    raise RuntimeError("export failed na 20 rondes")


_session_local = threading.local()


def _session():
    import requests
    s = getattr(_session_local, "s", None)
    if s is None:
        s = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=0)
        s.mount("https://", adapter)
        _session_local.s = s
    return s


def put_once(token: str, row: dict) -> tuple[int, str | None]:
    H = {
        "Authorization": f"Bearer {token}",
        "Accept": ACCEPT_RETAILER,
        "Content-Type": ACCEPT_RETAILER,
    }
    try:
        body = {"pricing": {"bundlePrices": bundle_prices(row["_price"])}}
    except Exception as e:
        return -1, f"BODY: {str(e)[:120]}"
    try:
        r = _session().put(
            f"{API}/retailer/offers/{row['offerId']}/price",
            headers=H, json=body, timeout=(10, 45),
        )
        if 200 <= r.status_code < 300:
            return 200, None
        return r.status_code, r.text[:200]
    except Exception as e:
        return 0, str(e)


_429_BACKOFF = float(os.environ.get("BOL_429_BACKOFF", "5"))


def worker(row: dict, tm: TokenManager) -> tuple[dict, int, str | None]:
    status, err = put_once(tm.get(), row)
    if status == -1:                       # onbruikbare prijs -> niet retryen
        return row, status, err
    if status == 401:
        token = tm.force_refresh()
        status, err = put_once(token, row)
    if status == 429:
        time.sleep(_429_BACKOFF)
        status, err = put_once(tm.get(), row)
    if status == 0 and err:
        time.sleep(2)
        status, err = put_once(tm.get(), row)
    return row, status, err


def _price_of(row: dict) -> str:
    # Bol offer-export kolom met de huidige stukprijs
    return (row.get("bundlePricesPrice") or row.get("price") or "").strip()


def main() -> int:
    cid = os.environ["BOL_CLIENT_ID"]
    csec = os.environ["BOL_CLIENT_SECRET"]
    shard = os.environ.get("BOL_SHARD", "")
    n_workers = int(os.environ.get("BOL_WORKERS", "20"))
    max_run = int(os.environ.get("BOL_MAX", "1000000"))
    name = os.environ.get("BOL_NAME", "Runner")

    shard_n = shard_m = None
    if shard and "/" in shard:
        a, b = shard.split("/")
        shard_n, shard_m = int(a), int(b)

    fetch_only = os.environ.get("BOL_FETCH_ONLY", "") == "1"
    use_cache = os.environ.get("BOL_USE_CACHE", "") == "1"

    log(f"=== START VOLUME {name} workers={n_workers} shard={shard or 'all'} fetch_only={fetch_only} use_cache={use_cache} ===")
    tm = TokenManager(cid, csec)

    if use_cache and CACHE.exists() and CACHE.stat().st_size > 0:
        log(f"[{name}] CSV cache aanwezig: {CACHE.stat().st_size/1_000_000:.1f} MB, skip export")
    else:
        fresh_export(tm, name)
    if fetch_only:
        log(f"[{name}] fetch-only mode, klaar")
        return 0

    with open(CACHE, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    todo = []
    for r in rows:
        p = _price_of(r)
        if not r.get("offerId") or not p:
            continue
        r["_price"] = p
        todo.append(r)
    if shard_m:
        todo = [
            r for r in todo
            if (int(hashlib.md5(r["offerId"].encode()).hexdigest(), 16) % shard_m) == shard_n
        ]
    log(f"[{name}] totaal CSV: {len(rows)}, todo (shard): {len(todo)}")

    todo = todo[:max_run]
    if not todo:
        log("Niets te doen.")
        return 0

    n_ok = n_err = rate_429 = completed = 0
    started = time.time()
    with ThreadPoolExecutor(max_workers=n_workers) as ex:
        futures = [ex.submit(worker, r, tm) for r in todo]
        for fut in as_completed(futures):
            try:
                row, status, err = fut.result()
            except Exception as e:
                completed += 1
                n_err += 1
                log(f"future EXC: {e}")
                continue
            if status == 200:
                n_ok += 1
            else:
                n_err += 1
                if status == 429:
                    rate_429 += 1
            completed += 1
            if completed % PROGRESS_EVERY == 0:
                elapsed = time.time() - started
                rate = completed / elapsed
                eta = (len(todo) - completed) / rate / 60 if rate else 0
                log(
                    f"[{name}] {completed}/{len(todo)} ok={n_ok} err={n_err} "
                    f"rps={rate:.2f} eta={eta:.0f}min (429s: {rate_429})"
                )

    elapsed = time.time() - started
    log(f"[{name}] KLAAR ok={n_ok} err={n_err} 429s={rate_429} duur={elapsed/60:.1f}min")
    return 0


if __name__ == "__main__":
    sys.exit(main())
