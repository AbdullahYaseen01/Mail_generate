"""
OpenStreetMap (Overpass API) as a free alternative to Google Places.
No API key or billing required. Uses Nominatim for city bbox and Overpass for POIs.
"""

import logging
import time
from dataclasses import dataclass
import requests

logger = logging.getLogger(__name__)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# Map our niches to OSM Overpass tag filters (amenity, shop, office, etc.)
# Each niche can have multiple tag queries to maximize results.
NICHE_TO_OVERPASS_TAGS = [
    # (niche_key, list of (tag_key, tag_value) for Overpass)
    ("Physical therapists", [("amenity", "physiotherapist"), ("healthcare", "physiotherapist")]),
    ("Dentists", [("amenity", "dentist")]),
    ("Auto repair shops", [("shop", "car_repair"), ("amenity", "car_repair")]),
    ("Moving companies", [("office", "moving_company"), ("shop", "storage")]),
    ("Cleaning companies", [("office", "company")]),  # broad; filter by name later if needed
    ("Beauty & wellness (premium)", [("shop", "beauty"), ("shop", "cosmetics"), ("amenity", "spa")]),
    ("Real estate agents", [("office", "estate_agent")]),
    ("Lawyers / tax advisors", [("office", "lawyer"), ("office", "accountant"), ("office", "tax_advisor")]),
    ("Pet services", [("shop", "pet"), ("amenity", "animal_boarding"), ("amenity", "veterinary")]),
    ("Plumbing & heating", [("craft", "plumber"), ("shop", "plumber")]),
    ("Gardening & landscaping", [("shop", "garden_centre"), ("craft", "gardener")]),
]


@dataclass
class OSMPlace:
    """A place from OSM with fields aligned to our lead CSV."""

    place_id: str  # osm type:id
    name: str
    formatted_address: str
    latitude: float
    longitude: float
    website: str
    phone: str
    google_maps_url: str  # we'll use OSM link
    rating: None
    user_ratings_total: None
    types: list[str]


def _normalize_website(url: str) -> str:
    if not url or not isinstance(url, str):
        return ""
    url = url.strip()
    if "?" in url:
        url = url.split("?")[0].rstrip("/")
    if url and not url.lower().startswith(("http://", "https://")):
        url = f"https://{url}"
    return url


# Cache bbox by (city, country) to avoid repeated Nominatim calls
_bbox_cache: dict[tuple[str, str], tuple[float, float, float, float]] = {}


def get_city_bbox(city: str, country: str = "Germany", sleep_seconds: float = 0.2) -> tuple[float, float, float, float] | None:
    """
    Get bounding box (south, west, north, east) for a city using Nominatim.
    Returns None on failure. Results are cached per (city, country).
    """
    cache_key = (city.strip(), country.strip())
    if cache_key in _bbox_cache:
        return _bbox_cache[cache_key]
    try:
        q = f"{city}, {country}"
        params = {"q": q, "format": "json", "limit": 1}
        headers = {"User-Agent": "LeadDatasetBuilder/1.0 (OSM lead collection)"}
        r = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=8)
        r.raise_for_status()
        data = r.json()
        time.sleep(sleep_seconds)
        if not data:
            return None
        b = data[0].get("boundingbox")
        if not b or len(b) != 4:
            lat = data[0].get("lat")
            lon = data[0].get("lon")
            if lat and lon:
                delta = 0.05
                return (float(lat) - delta, float(lon) - delta, float(lat) + delta, float(lon) + delta)
            return None
        south, north, west, east = float(b[0]), float(b[1]), float(b[2]), float(b[3])
        result = (south, west, north, east)
        _bbox_cache[cache_key] = result
        return result
    except Exception as e:
        logger.warning("Nominatim bbox failed for %s: %s", city, e)
        return None


def _build_overpass_query(bbox: tuple[float, float, float, float], tag_key: str, tag_value: str) -> str:
    south, west, north, east = bbox
    return f"""
    [out:json][timeout:25];
    (
      node["{tag_key}"="{tag_value}"]({south},{west},{north},{east});
      way["{tag_key}"="{tag_value}"]({south},{west},{north},{east});
    );
    out center body;
    """


def _element_to_place(elem: dict, tag_key: str, tag_value: str) -> OSMPlace | None:
    """Convert Overpass node/way to OSMPlace. Requires name and (website or addr:street/addr:housenumber for address)."""
    tags = elem.get("tags") or {}
    name = tags.get("name") or tags.get("brand") or ""
    if not name:
        return None

    lat, lon = None, None
    if elem.get("type") == "node":
        lat, lon = elem.get("lat"), elem.get("lon")
    else:
        center = elem.get("center") or {}
        lat, lon = center.get("lat"), center.get("lon")
    if lat is None or lon is None:
        return None

    # Build address from OSM tags
    addr_parts = [
        tags.get("addr:street"),
        tags.get("addr:housenumber"),
        tags.get("addr:postcode"),
        tags.get("addr:city"),
    ]
    addr = " ".join(str(p) for p in addr_parts if p) or f"{lat:.4f}, {lon:.4f}"

    website = tags.get("website") or tags.get("contact:website") or ""
    website = _normalize_website(website)
    # Require website for consistency with Google flow (leads with website)
    if not website:
        return None

    phone = tags.get("phone") or tags.get("contact:phone") or ""
    osm_type = elem.get("type", "node")
    osm_id = elem.get("id", 0)
    place_id = f"osm:{osm_type}:{osm_id}"
    osm_link = f"https://www.openstreetmap.org/{osm_type}/{osm_id}"
    return OSMPlace(
        place_id=place_id,
        name=name,
        formatted_address=addr,
        latitude=float(lat),
        longitude=float(lon),
        website=website,
        phone=phone,
        google_maps_url=osm_link,
        rating=None,
        user_ratings_total=None,
        types=[f"{tag_key}={tag_value}"],
    )


def fetch_places_overpass(
    bbox: tuple[float, float, float, float],
    tag_key: str,
    tag_value: str,
    sleep_seconds: float = 1.0,
) -> list[OSMPlace]:
    """Query Overpass for POIs in bbox with given tag. Returns list of OSMPlace (with website)."""
    query = _build_overpass_query(bbox, tag_key, tag_value)
    try:
        r = requests.post(
            OVERPASS_URL,
            data={"data": query},
            headers={"User-Agent": "LeadDatasetBuilder/1.0"},
            timeout=18,
        )
        r.raise_for_status()
        data = r.json()
        time.sleep(max(0.1, sleep_seconds))
        elements = data.get("elements") or []
        results = []
        for elem in elements:
            if not isinstance(elem, dict):
                continue
            place = _element_to_place(elem, tag_key, tag_value)
            if place:
                results.append(place)
        return results
    except Exception as e:
        logger.warning("Overpass query failed %s=%s: %s", tag_key, tag_value, e)
        return []


def collect_osm_places_for_niche_city(
    niche: str,
    city: str,
    country: str = "Germany",
    sleep_seconds: float = 1.0,
) -> list[OSMPlace]:
    """
    Get OSM places for a niche in a city.
    Uses first matching tag set for the niche; deduplication by place_id is done in main.
    """
    bbox = get_city_bbox(city, country, sleep_seconds=sleep_seconds)
    if not bbox:
        logger.warning("No bbox for %s", city)
        return []

    # Find tag sets for this niche
    tag_sets = []
    for n, tag_list in NICHE_TO_OVERPASS_TAGS:
        if n == niche:
            tag_sets = [(k, v) for k, v in tag_list]
            break
    if not tag_sets:
        return []

    all_places: list[OSMPlace] = []
    seen_ids: set[str] = set()
    for tag_key, tag_value in tag_sets:
        places = fetch_places_overpass(bbox, tag_key, tag_value, sleep_seconds=sleep_seconds)
        for p in places:
            if p.place_id not in seen_ids:
                seen_ids.add(p.place_id)
                all_places.append(p)
    return all_places


def get_domain_for_dedup(url: str) -> str:
    """Extract domain for deduplication."""
    from urllib.parse import urlparse
    try:
        parsed = urlparse(url)
        host = (parsed.netloc or parsed.path).lower().replace("www.", "")
        return host
    except Exception:
        return url
