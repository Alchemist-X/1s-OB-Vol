"""Resolve a Polymarket event URL into market identifiers via the Gamma API."""

import json
import logging
import re
import sys

import requests

GAMMA_API_URL = "https://gamma-api.polymarket.com"

log = logging.getLogger(__name__)


def parse_slug(url: str) -> str:
    url = url.strip().rstrip("/")
    m = re.search(r"polymarket\.com/event/([^/?#]+)", url)
    if m:
        return m.group(1)
    return url


def resolve_market(url: str) -> dict:
    slug = parse_slug(url)
    log.info("Resolving slug: %s", slug)

    market = _try_events_api(slug) or _try_markets_api(slug)
    if not market:
        print(f"ERROR: Could not resolve market for '{slug}'")
        sys.exit(1)

    condition_id = market.get("conditionId", "")
    token_ids = _parse_json_field(market.get("clobTokenIds", "[]"))
    if len(token_ids) < 2:
        print(f"ERROR: Market has fewer than 2 token IDs: {token_ids}")
        sys.exit(1)

    result = {
        "slug": slug,
        "condition_id": condition_id,
        "market_id": condition_id,
        "token_id_yes": token_ids[0],
        "token_id_no": token_ids[1],
        "meta": market,
    }

    question = market.get("question", slug)
    log.info("Resolved: %s", question)
    log.info("  condition_id: %s", condition_id[:20] + "...")
    log.info("  token_yes:    %s...", token_ids[0][:20])
    log.info("  token_no:     %s...", token_ids[1][:20])

    return result


def _try_events_api(slug: str):
    try:
        resp = requests.get(f"{GAMMA_API_URL}/events", params={"slug": slug}, timeout=10)
        resp.raise_for_status()
        events = resp.json()
        if events and len(events) > 0:
            markets = events[0].get("markets", [])
            if markets and len(markets) > 0:
                log.info("Found via /events API")
                return markets[0]
    except Exception as e:
        log.debug("Events API failed: %s", e)
    return None


def _try_markets_api(slug: str):
    try:
        resp = requests.get(f"{GAMMA_API_URL}/markets", params={"slug": slug}, timeout=10)
        resp.raise_for_status()
        markets = resp.json()
        if markets and len(markets) > 0:
            log.info("Found via /markets API")
            return markets[0]
    except Exception as e:
        log.debug("Markets API failed: %s", e)
    return None


def _parse_json_field(raw):
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return []
    return []
