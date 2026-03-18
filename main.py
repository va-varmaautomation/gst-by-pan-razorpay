import asyncio
import csv
import io
import random
import time
import re
from typing import Optional

import httpx
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse
import logging

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

RAZORPAY_API = "https://razorpay.com/api/gstin/pan/{pan}"
CONCURRENCY = 2
DELAY = 1.5
JITTER = 0.6
MIN_INTERVAL = 0.9
MAX_ADAPTIVE_MULTIPLIER = 3.5
ADAPTIVE_DECAY = 0.92
TIMEOUT = 15
MAX_RETRIES = 7
BASE_BACKOFF = 3.0

PAN_REGEX = re.compile(r'^[A-Z]{5}[0-9]{4}[A-Z]$')

logger = logging.getLogger("gst_lookup")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

_rate_lock = asyncio.Lock()
_last_request_time = 0.0
_adaptive_lock = asyncio.Lock()
_adaptive_multiplier = 1.0

async def _throttle_requests():
    global _last_request_time
    async with _rate_lock:
        now = time.monotonic()
        wait_for = MIN_INTERVAL - (now - _last_request_time)
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        _last_request_time = time.monotonic()

async def _get_adaptive_multiplier() -> float:
    async with _adaptive_lock:
        return _adaptive_multiplier

async def _record_upstream_signal(signal: str):
    global _adaptive_multiplier
    async with _adaptive_lock:
        if signal in ("blocked", "rate_limited"):
            _adaptive_multiplier = min(
                MAX_ADAPTIVE_MULTIPLIER,
                _adaptive_multiplier * 1.25 + 0.1
            )
        elif signal == "success":
            _adaptive_multiplier = max(1.0, _adaptive_multiplier * ADAPTIVE_DECAY)

@app.on_event("startup")
async def startup_event():
    logger.info("Starting up GST Lookup Service v2.0")
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            content = f.read()
            if "Varma & Varma" in content:
                logger.info("Valid index.html detected with correct branding.")
            else:
                logger.warning("index.html might be missing branding!")
    except Exception as e:
        logger.error(f"Failed to read index.html on startup: {e}")


def empty_row(pan: str, error: str) -> dict:
    return {
        "pan": pan,
        "gstin_count": "",
        "gstins": "",
        "active_gstins": "",
        "inactive_gstins": "",
        "error": error
    }


async def fetch_gstin(client: httpx.AsyncClient, pan: str, semaphore: asyncio.Semaphore) -> dict:
    async with semaphore:
        for attempt in range(MAX_RETRIES + 1):
            try:
                await _throttle_requests()
                adaptive = await _get_adaptive_multiplier()
                await asyncio.sleep((DELAY * adaptive) + random.uniform(0, JITTER * adaptive))
                ua = random.choice([
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                ])
                response = await client.get(
                    RAZORPAY_API.format(pan=pan.upper()),
                    timeout=TIMEOUT,
                    headers={
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "User-Agent": ua,
                        "Accept-Language": "en-US,en;q=0.9",
                        "Accept-Encoding": "gzip, deflate, br",
                        "Connection": "keep-alive",
                        "Referer": "https://razorpay.com/",
                    }
                )
                logger.debug(f"PAN={pan} attempt={attempt} status={response.status_code}")

                if response.status_code == 429:
                    await _record_upstream_signal("rate_limited")
                    retry_after = response.headers.get("Retry-After")
                    logger.warning(f"Rate limited from upstream for PAN={pan} attempt={attempt} retry_after={retry_after}")
                    if attempt < MAX_RETRIES:
                        if retry_after is not None:
                            try:
                                delay = float(retry_after)
                            except ValueError:
                                delay = BASE_BACKOFF * (2 ** attempt)
                        else:
                            delay = BASE_BACKOFF * (2 ** attempt)
                        delay += random.uniform(0, 1.0)
                        await asyncio.sleep(delay)
                        continue
                    return empty_row(pan, "Rate limited (HTTP 429)")

                if response.status_code in (403, 406):
                    await _record_upstream_signal("blocked")
                    logger.warning(f"Forbidden from upstream for PAN={pan} attempt={attempt} status={response.status_code}")
                    if attempt < MAX_RETRIES:
                        delay = BASE_BACKOFF * (2 ** attempt) + random.uniform(0, 1.0)
                        await asyncio.sleep(delay)
                        continue
                    return empty_row(pan, f"Access denied (HTTP {response.status_code})")

                if response.status_code == 200:
                    await _record_upstream_signal("success")
                    try:
                        data = response.json()
                    except Exception:
                        logger.error(f"Invalid JSON from upstream for PAN={pan} attempt={attempt}")
                        return empty_row(pan, "Invalid JSON from upstream")
                    count = data.get("count", 0)
                    items = data.get("items", [])

                    if not items:
                        logger.info(f"PAN={pan} found 0 GSTINs")
                        return empty_row(pan, "No GSTINs found")

                    all_gstins = [i.get("gstin", "") for i in items]
                    active = [i.get("gstin", "") for i in items if i.get("auth_status", "").lower() == "active"]
                    inactive = [i.get("gstin", "") for i in items if i.get("auth_status", "").lower() != "active"]

                    logger.info(f"PAN={pan} count={count} active={len(active)} inactive={len(inactive)}")
                    return {
                        "pan": pan,
                        "gstin_count": count,
                        "gstins": ", ".join(all_gstins),
                        "active_gstins": ", ".join(active),
                        "inactive_gstins": ", ".join(inactive),
                        "error": ""
                    }

                if response.status_code == 404:
                    logger.info(f"PAN not found upstream PAN={pan}")
                    return empty_row(pan, "PAN not found")

                if 500 <= response.status_code < 600:
                    snippet = ""
                    try:
                        snippet = response.text[:200]
                    except Exception:
                        pass
                    logger.error(f"Upstream 5xx for PAN={pan} status={response.status_code} attempt={attempt} body_snippet={snippet!r}")
                    if attempt < MAX_RETRIES:
                        delay = BASE_BACKOFF * (2 ** attempt) + random.uniform(0, 0.5)
                        await asyncio.sleep(delay)
                        continue
                    return empty_row(pan, f"Server error (HTTP {response.status_code})")

                logger.warning(f"Unexpected upstream status for PAN={pan} status={response.status_code}")
                return empty_row(pan, f"HTTP {response.status_code}")

            except httpx.TimeoutException:
                logger.warning(f"Timeout contacting upstream for PAN={pan} attempt={attempt}")
                if attempt < MAX_RETRIES:
                    delay = BASE_BACKOFF * (2 ** attempt) + random.uniform(0, 0.5)
                    await asyncio.sleep(delay)
                    continue
                return empty_row(pan, "Timeout")
            except Exception as e:
                logger.exception(f"Error fetching PAN={pan} attempt={attempt}: {e}")
                return empty_row(pan, str(e))

        return empty_row(pan, "Failed after retries")


async def process_pans(pan_list: list[str]) -> list[dict]:
    semaphore = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient() as client:
        tasks = [fetch_gstin(client, pan.strip(), semaphore) for pan in pan_list if pan.strip()]
        results = await asyncio.gather(*tasks)
    return list(results)


def validate_pans(raw: list[str]) -> tuple[list[str], list[str]]:
    valid, invalid = [], []
    for p in raw:
        p = p.strip().upper()
        if not p:
            continue
        if PAN_REGEX.match(p):
            valid.append(p)
        else:
            invalid.append(p)
    return valid, invalid


def to_csv(rows: list[dict]) -> str:
    if not rows:
        return ""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


# --- API Endpoints ---

@app.post("/lookup")
async def lookup(
    pans_text: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None)
):
    raw_pans = []

    if file:
        content = await file.read()
        text = content.decode("utf-8", errors="ignore")
        # Support CSV (first column) or plain text (one PAN per line)
        if file.filename.endswith(".csv"):
            reader = csv.reader(io.StringIO(text))
            for row in reader:
                if row:
                    raw_pans.append(row[0])
        else:
            raw_pans.extend(text.splitlines())

    if pans_text:
        raw_pans.extend(pans_text.splitlines())

    # Deduplicate
    raw_pans = list(dict.fromkeys(raw_pans))

    valid, invalid = validate_pans(raw_pans)

    if not valid:
        return {"error": "No valid PANs found.", "invalid": invalid}

    results = await process_pans(valid)

    # Append invalid PANs as error rows
    for p in invalid:
        results.append(empty_row(p, "Invalid PAN format"))

    csv_data = to_csv(results)

    return StreamingResponse(
        io.StringIO(csv_data),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=gst_results.csv"}
    )


# --- Serve frontend ---

@app.get("/", response_class=HTMLResponse)
async def root():
    with open("index.html", "r", encoding="utf-8") as f:
        content = f.read()
    return HTMLResponse(
        content=content,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )
