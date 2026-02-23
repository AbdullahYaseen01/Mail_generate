"""
Google Places API utilities for compliant lead dataset building.
Uses official Google Places API - no scraping.
"""

import logging
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import googlemaps

logger = logging.getLogger(__name__)


@dataclass
class PlaceResult:
    """A validated place result with required fields."""

    place_id: str
    name: str
    formatted_address: str
    latitude: float
    longitude: float
    website: str
    phone: str
    google_maps_url: str
    rating: float | None
    user_ratings_total: int | None
    types: list[str]


def _normalize_website(url: str) -> str:
    """Normalize website URL: strip tracking params, enforce https."""
    if not url or not isinstance(url, str):
        return ""
    url = url.strip()
    # Remove query string (tracking params like utm_, fbclid, gclid)
    if "?" in url:
        url = url.split("?")[0].rstrip("/")
    if url and not url.lower().startswith(("http://", "https://")):
        url = f"https://{url}"
    return url


def _extract_domain(url: str) -> str:
    """Extract domain from URL for deduplication."""
    try:
        parsed = urlparse(url)
        host = parsed.netloc or parsed.path
        return host.lower().replace("www.", "")
    except Exception:
        return url


def geocode_city(
    client: googlemaps.Client,
    city: str,
    country: str = "Germany",
    sleep_seconds: float = 0.5,
) -> tuple[float, float] | None:
    """
    Get city center coordinates using Geocoding API.
    Returns (lat, lng) or None if failed.
    """
    query = f"{city}, {country}"

    def _geocode() -> list:
        return client.geocode(query)

    try:
        results = _call_with_retry(_geocode)
        if results and len(results) > 0:
            location = results[0].get("geometry", {}).get("location")
            if location:
                lat = float(location.get("lat", 0))
                lng = float(location.get("lng", 0))
                logger.info("Geocoded %s -> (%.4f, %.4f)", city, lat, lng)
                time.sleep(sleep_seconds)
                return (lat, lng)
    except Exception as e:
        logger.error("Geocoding failed for %s: %s", city, e)
    return None


def _parse_place_details(
    details: dict[str, Any],
) -> PlaceResult | None:
    """Parse Place Details response into PlaceResult. Returns None if invalid."""
    place_id = details.get("place_id")
    if not place_id:
        return None

    geometry = details.get("geometry", {})
    location = geometry.get("location")
    if not location:
        return None

    lat = location.get("lat")
    lng = location.get("lng")
    if lat is None or lng is None:
        return None

    address = details.get("formatted_address")
    if not address or not isinstance(address, str):
        return None

    website = details.get("website")
    if not website or not isinstance(website, str):
        return None

    website = _normalize_website(website)
    if not website:
        return None

    name = details.get("name") or ""
    phone = details.get("formatted_phone_number") or ""
    url = details.get("url") or ""
    rating = details.get("rating")
    user_ratings_total = details.get("user_ratings_total")
    types = details.get("types") or []

    return PlaceResult(
        place_id=place_id,
        name=name,
        formatted_address=address,
        latitude=float(lat),
        longitude=float(lng),
        website=website,
        phone=phone,
        google_maps_url=url,
        rating=float(rating) if rating is not None else None,
        user_ratings_total=int(user_ratings_total) if user_ratings_total is not None else None,
        types=types,
    )


def fetch_place_details(
    client: googlemaps.Client,
    place_id: str,
    fields: list[str],
    sleep_seconds: float = 0.3,
) -> PlaceResult | None:
    """
    Fetch Place Details for a place_id.
    Returns PlaceResult if valid (has website, location, address), else None.
    """
    try:
        details = client.place(
            place_id,
            fields=fields,
        )
        result = details.get("result")
        if not result:
            return None

        parsed = _parse_place_details(result)
        time.sleep(sleep_seconds)
        return parsed
    except Exception as e:
        logger.warning("Place Details failed for %s: %s", place_id, e)
        return None


def _call_with_retry(
    fn,
    *args,
    max_retries: int = 5,
    base_delay: float = 2.0,
    **kwargs,
) -> Any:
    """Execute function with exponential backoff on OVER_QUERY_LIMIT."""
    last_error = None
    for attempt in range(max_retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_error = e
            err_str = str(e).lower()
            if "over_query_limit" in err_str or "rate" in err_str or "429" in err_str:
                delay = base_delay * (2**attempt)
                logger.warning(
                    "Rate limit hit, backing off for %.1fs (attempt %d)",
                    delay,
                    attempt + 1,
                )
                time.sleep(delay)
            else:
                raise
    raise last_error


def text_search_places(
    client: googlemaps.Client,
    query: str,
    sleep_seconds: float = 0.5,
    token_sleep: float = 2.0,
) -> list[dict[str, Any]]:
    """
    Text Search with pagination. Returns list of place results (raw).
    """
    all_results: list[dict[str, Any]] = []
    next_page_token: str | None = None

    def _do_search(page_token: str | None = None) -> dict:
        if page_token:
            return client.places(query=query, page_token=page_token)
        return client.places(query=query)

    while True:
        try:
            if next_page_token:
                time.sleep(token_sleep)

            response = _call_with_retry(
                _do_search,
                page_token=next_page_token,
            )
            time.sleep(sleep_seconds)

            results = response.get("results", [])
            all_results.extend(results)

            next_page_token = response.get("next_page_token")
            if not next_page_token:
                break

        except Exception as e:
            logger.error("Text search failed: %s", e)
            break

    return all_results


def nearby_search_places(
    client: googlemaps.Client,
    lat: float,
    lng: float,
    keyword: str,
    radius: int = 15000,
    sleep_seconds: float = 0.5,
    token_sleep: float = 2.0,
) -> list[dict[str, Any]]:
    """
    Nearby Search with pagination. Returns list of place results (raw).
    """
    all_results: list[dict[str, Any]] = []
    next_page_token: str | None = None

    def _do_nearby(page_token: str | None = None) -> dict:
        if page_token:
            return client.places_nearby(page_token=page_token)
        return client.places_nearby(
            location=(lat, lng),
            keyword=keyword,
            radius=radius,
        )

    while True:
        try:
            if next_page_token:
                time.sleep(token_sleep)

            response = _call_with_retry(
                _do_nearby,
                page_token=next_page_token,
            )
            time.sleep(sleep_seconds)

            results = response.get("results", [])
            all_results.extend(results)

            next_page_token = response.get("next_page_token")
            if not next_page_token:
                break

        except Exception as e:
            logger.error("Nearby search failed: %s", e)
            break

    return all_results


def collect_places_for_niche_city(
    client: googlemaps.Client,
    niche: str,
    city: str,
    fields: list[str],
    sleep_api: float = 0.5,
    token_sleep: float = 2.0,
    use_nearby_fallback: bool = True,
    min_text_results: int = 5,
) -> list[PlaceResult]:
    """
    Get places for a niche in a city.
    Uses Text Search first; if results are low, falls back to Nearby Search.
    """
    seen_place_ids: set[str] = set()
    seen_domains: set[str] = set()
    valid_places: list[PlaceResult] = []

    # 1. Text Search
    query = f"{niche} in {city}, Germany"
    logger.info("Text search: %s", query)
    raw_results = text_search_places(
        client,
        query,
        sleep_seconds=sleep_api,
        token_sleep=token_sleep,
    )

    place_ids = [r.get("place_id") for r in raw_results if r.get("place_id")]
    logger.info("Text search returned %d place IDs", len(place_ids))

    for pid in place_ids:
        if pid in seen_place_ids:
            continue
        details = fetch_place_details(client, pid, fields, sleep_seconds=sleep_api)
        if details:
            domain = _extract_domain(details.website)
            if domain not in seen_domains:
                seen_domains.add(domain)
                seen_place_ids.add(pid)
                valid_places.append(details)

    # 2. Nearby Search fallback if text search yielded few results
    if (
        use_nearby_fallback
        and len(valid_places) < min_text_results
        and len(raw_results) < min_text_results
    ):
        coords = geocode_city(client, city, sleep_seconds=sleep_api)
        if coords:
            lat, lng = coords
            logger.info("Nearby search fallback for %s in %s", niche, city)
            nearby_raw = nearby_search_places(
                client,
                lat,
                lng,
                keyword=niche,
                radius=15000,
                sleep_seconds=sleep_api,
                token_sleep=token_sleep,
            )
            for r in nearby_raw:
                pid = r.get("place_id")
                if not pid or pid in seen_place_ids:
                    continue
                details = fetch_place_details(client, pid, fields, sleep_seconds=sleep_api)
                if details:
                    domain = _extract_domain(details.website)
                    if domain not in seen_domains:
                        seen_domains.add(domain)
                        seen_place_ids.add(pid)
                        valid_places.append(details)

    return valid_places


def get_domain_for_dedup(url: str) -> str:
    """Get domain for deduplication."""
    return _extract_domain(url)
