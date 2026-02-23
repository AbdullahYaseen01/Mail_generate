"""
Lead Dataset Builder - Compliant business lead collection.
Supports Google Places API or OpenStreetMap (Overpass) - no scraping.
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import googlemaps

from utils.email_extractor import extract_emails_from_website
from utils.osm_places import (
    OSMPlace,
    collect_osm_places_for_niche_city,
    get_domain_for_dedup as osm_get_domain,
)
from utils.places import (
    PlaceResult,
    collect_places_for_niche_city,
    get_domain_for_dedup,
)

# Constants
CITIES = [
    "Mannheim",
    "Heidelberg",
    "Heilbronn",
    "Pforzheim",
    "Ulm",
    "Reutlingen",
    "Tübingen",
    "Esslingen am Neckar",
]

NICHES = [
    "Physical therapists",
    "Dentists",
    "Auto repair shops",
    "Moving companies",
    "Cleaning companies",
    "Beauty & wellness (premium)",
    "Real estate agents",
    "Lawyers / tax advisors",
    "Pet services",
    "Plumbing & heating",
    "Gardening & landscaping",
]

OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
CHECKPOINT_FILE = OUTPUT_DIR / "checkpoint.json"
OUTPUT_CSV = OUTPUT_DIR / "leads_de_bw.csv"
CHECKPOINT_INTERVAL = 100
# Parallel email extraction (number of concurrent website fetches)
CONCURRENT_EMAIL_WORKERS = 14

# Gmail domain variants for the gmail_addresses column
GMAIL_DOMAINS = ("gmail.com", "googlemail.com")


def _gmail_addresses_from_emails(emails: list[str]) -> str:
    """Return semicolon-separated list of emails that are @gmail.com or @googlemail.com."""
    gmails = [e for e in emails if e.strip().lower().split("@")[-1] in GMAIL_DOMAINS]
    return "; ".join(gmails) if gmails else ""


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_config(config_path: Path) -> dict:
    """Load configuration from JSON file."""
    if not config_path.exists():
        example = Path("config.example.json")
        logging.error(
            "Config file not found: %s. Copy %s to config.json and add your API key.",
            config_path,
            example if example.exists() else "config.example.json",
        )
        sys.exit(1)

    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)

    api_key = (
        config.get("google_api_key")
        or config.get("api_key")
        or os.environ.get("GOOGLE_API_KEY")
    )
    if not api_key or api_key == "YOUR_GOOGLE_API_KEY_HERE":
        logging.error(
            "Please set your Google API key in %s or set GOOGLE_API_KEY env var. "
            "Copy config.example.json to config.json.",
            config_path,
        )
        sys.exit(1)
    config["google_api_key"] = api_key

    return config


def _writable_checkpoint_path(requested: Path | None) -> Path:
    """Use requested path, or /tmp on Vercel (read-only fs), else default."""
    if requested is not None:
        return requested
    if os.environ.get("VERCEL"):
        return Path("/tmp/checkpoint.json")
    return CHECKPOINT_FILE


def load_checkpoint(checkpoint_file: Path | None = None) -> tuple[list[dict], set[str], set[str]]:
    """Load checkpoint if exists. Returns (leads, seen_place_ids, seen_domains)."""
    cf = _writable_checkpoint_path(checkpoint_file)
    if not cf.exists():
        return [], set(), set()

    try:
        with open(cf, encoding="utf-8") as f:
            data = json.load(f)
        leads = data.get("leads", [])
        seen_place_ids = set(data.get("seen_place_ids", []))
        seen_domains = set(data.get("seen_domains", []))
        logging.info("Loaded checkpoint: %d leads, %d place_ids, %d domains", len(leads), len(seen_place_ids), len(seen_domains))
        return leads, seen_place_ids, seen_domains
    except Exception as e:
        logging.warning("Failed to load checkpoint: %s", e)
        return [], set(), set()


def save_checkpoint(
    leads: list[dict],
    seen_place_ids: set[str],
    seen_domains: set[str],
    checkpoint_file: Path | None = None,
) -> None:
    """Save checkpoint to file."""
    cf = _writable_checkpoint_path(checkpoint_file)
    cf.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "leads": leads,
        "seen_place_ids": list(seen_place_ids),
        "seen_domains": list(seen_domains),
    }
    with open(cf, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logging.info("Checkpoint saved: %d leads", len(leads))


def place_to_lead(place: PlaceResult, niche: str, city: str) -> dict:
    """Convert PlaceResult to lead dict for CSV."""
    reviews = place.user_ratings_total if place.user_ratings_total is not None else ""
    owner_full = (getattr(place, "owner", "") or "").strip() or "Nill"
    return {
        "niche": niche,
        "city": city,
        "business_name": place.name,
        "first_name": owner_full,
        "reviews_count": reviews,
        "formatted_address": place.formatted_address,
        "latitude": place.latitude,
        "longitude": place.longitude,
        "phone": place.phone or "Nill",
        "google_maps_url": place.google_maps_url,
        "website_url": place.website or "Nill",
        "rating": place.rating if place.rating is not None else "",
        "ratings_count": reviews,
        "place_id": place.place_id,
        "emails_found": "Nill",
        "email_source_page": "",
        "gmail_addresses": "",
    }


def osm_place_to_lead(place: OSMPlace, niche: str, city: str) -> dict:
    """Convert OSMPlace to lead dict for CSV (same columns)."""
    owner_full = (place.owner or "").strip() or "Nill"
    return {
        "niche": niche,
        "city": city,
        "business_name": place.name,
        "first_name": owner_full,
        "reviews_count": "",
        "formatted_address": place.formatted_address,
        "latitude": place.latitude,
        "longitude": place.longitude,
        "phone": place.phone or "Nill",
        "google_maps_url": place.google_maps_url,
        "website_url": place.website or "Nill",
        "rating": "",
        "ratings_count": "",
        "place_id": place.place_id,
        "emails_found": "Nill",
        "email_source_page": "",
        "gmail_addresses": "",
    }


def export_csv(leads: list[dict], output_path: Path, require_email_and_website: bool = True) -> None:
    """Export leads to CSV. If require_email_and_website, only export leads with both email and website."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "niche", "city", "business_name",
        "phone", "google_maps_url", "website_url",
        "emails_found",
    ]
    if require_email_and_website:
        rows = [
            lead for lead in leads
            if lead.get("emails_found") and str(lead.get("emails_found", "")).strip() != "Nill"
            and lead.get("website_url") and str(lead.get("website_url", "")).strip() != "Nill"
        ]
    else:
        rows = leads
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logging.info("Exported %d leads to %s", len(rows), output_path)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Lead Dataset Builder - Google Places API or OpenStreetMap (free)",
    )
    parser.add_argument(
        "--source",
        choices=["google", "osm"],
        default="google",
        help="Data source: google (Places API) or osm (OpenStreetMap, free, no key)",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config.json"),
        help="Path to config JSON (default: config.json)",
    )
    parser.add_argument(
        "--max-leads",
        type=int,
        default=1000,
        help="Maximum number of leads to collect (default: 1000)",
    )
    parser.add_argument(
        "--extract-emails",
        type=str,
        choices=["true", "false"],
        default="false",
        help="Whether to extract emails from business websites (default: false)",
    )
    parser.add_argument(
        "--sleep-api",
        type=float,
        default=0.5,
        help="Sleep seconds between API calls (default: 0.5)",
    )
    parser.add_argument(
        "--sleep-web",
        type=float,
        default=1.0,
        help="Sleep seconds between website fetches for email extraction (default: 1.0)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_CSV,
        help=f"Output CSV path (default: {OUTPUT_CSV})",
    )
    parser.add_argument(
        "--clear-checkpoint",
        action="store_true",
        help="Clear checkpoint and start fresh",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    return parser.parse_args()


def _run_google(
    args,
    leads: list,
    seen_place_ids: set,
    seen_domains: set,
    max_leads: int,
    extract_emails: bool,
    niches: list[str] | None = None,
    cities: list[str] | None = None,
) -> None:
    """Collect leads using Google Places API."""
    niches = niches or NICHES
    cities = cities or CITIES
    config = load_config(args.config)
    api_key = config.get("google_api_key") or config.get("api_key")
    fields = config.get("places_api", {}).get("fields", [
        "name", "formatted_address", "geometry", "website",
        "formatted_phone_number", "url", "rating", "user_ratings_total", "types",
    ])
    client = googlemaps.Client(key=api_key)
    for niche in niches:
        if len(leads) >= max_leads:
            break
        for city in cities:
            if len(leads) >= max_leads:
                break
            logging.info("Collecting: %s in %s", niche, city)
            try:
                places = collect_places_for_niche_city(
                    client, niche=niche, city=city, fields=fields,
                    sleep_api=args.sleep_api, token_sleep=2.0,
                    use_nearby_fallback=True, min_text_results=5,
                )
                for place in places:
                    if place.place_id in seen_place_ids:
                        continue
                    domain = get_domain_for_dedup(place.website)
                    if domain in seen_domains:
                        continue
                    seen_place_ids.add(place.place_id)
                    seen_domains.add(domain)
                    lead = place_to_lead(place, niche, city)
                    if extract_emails and place.website:
                        try:
                            emails, source = extract_emails_from_website(place.website, sleep_seconds=args.sleep_web)
                            lead["emails_found"] = "; ".join(emails) if emails else "Nill"
                            lead["email_source_page"] = source
                            lead["gmail_addresses"] = _gmail_addresses_from_emails(emails)
                        except Exception as e:
                            logging.debug("Email extraction failed for %s: %s", place.website, e)
                    leads.append(lead)
                    if len(leads) % CHECKPOINT_INTERVAL == 0:
                        save_checkpoint(leads, seen_place_ids, seen_domains, getattr(args, "checkpoint_file", None))
                    if len(leads) >= max_leads:
                        break
            except Exception as e:
                logging.error("Error collecting %s in %s: %s", niche, city, e)


def _process_place_emails(
    place: OSMPlace, niche: str, city: str, sleep_web: float
) -> tuple[dict, bool]:
    """Build lead and extract emails; returns (lead_dict, has_email). Used for parallel execution."""
    lead = osm_place_to_lead(place, niche, city)
    if not place.website:
        return (lead, False)
    try:
        emails, source = extract_emails_from_website(place.website, sleep_seconds=sleep_web)
        lead["emails_found"] = "; ".join(emails) if emails else "Nill"
        lead["email_source_page"] = source
        lead["gmail_addresses"] = _gmail_addresses_from_emails(emails)
        return (lead, bool(emails))
    except Exception as e:
        logging.debug("Email extraction failed for %s: %s", place.website, e)
        return (lead, False)


def _run_osm(
    args,
    leads: list,
    seen_place_ids: set,
    seen_domains: set,
    max_leads: int,
    extract_emails: bool,
    niches: list[str] | None = None,
    cities: list[str] | None = None,
) -> None:
    """Collect leads using OpenStreetMap (Overpass). No API key required."""
    niches = niches or NICHES
    cities = cities or CITIES
    workers = getattr(args, "email_workers", CONCURRENT_EMAIL_WORKERS)

    for niche in niches:
        if len(leads) >= max_leads:
            break
        for city in cities:
            if len(leads) >= max_leads:
                break
            logging.info("Collecting: %s in %s (OSM)", niche, city)
            try:
                places = collect_osm_places_for_niche_city(
                    niche=niche, city=city, country="Germany", sleep_seconds=args.sleep_api,
                )
                logging.info("OSM returned %d places for %s in %s", len(places), niche, city)
                candidates: list[tuple[OSMPlace, str, str]] = []
                for place in places:
                    if place.place_id in seen_place_ids:
                        continue
                    domain = osm_get_domain(place.website) if place.website else ""
                    if domain and domain in seen_domains:
                        continue
                    seen_place_ids.add(place.place_id)
                    if domain:
                        seen_domains.add(domain)
                    if extract_emails and place.website:
                        candidates.append((place, niche, city))
                    else:
                        lead = osm_place_to_lead(place, niche, city)
                        leads.append(lead)
                    if len(leads) >= max_leads:
                        break

                if not candidates:
                    if len(leads) % CHECKPOINT_INTERVAL == 0:
                        save_checkpoint(leads, seen_place_ids, seen_domains, getattr(args, "checkpoint_file", None))
                    continue

                # Parallel email extraction
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = {
                        executor.submit(_process_place_emails, p, n, c, args.sleep_web): (p, n, c)
                        for p, n, c in candidates
                    }
                    for future in as_completed(futures):
                        if len(leads) >= max_leads:
                            break
                        try:
                            lead, _has_email = future.result()
                            leads.append(lead)
                            if len(leads) % CHECKPOINT_INTERVAL == 0:
                                save_checkpoint(leads, seen_place_ids, seen_domains, getattr(args, "checkpoint_file", None))
                        except Exception as e:
                            logging.debug("Worker error: %s", e)

                if len(leads) >= max_leads:
                    break
            except Exception as e:
                logging.error("Error collecting %s in %s: %s", niche, city, e)


def main() -> None:
    """Main entry point."""
    args = parse_args()
    setup_logging(args.verbose)

    extract_emails = args.extract_emails.lower() == "true"
    max_leads = args.max_leads

    if args.clear_checkpoint and CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        logging.info("Checkpoint cleared.")

    leads, seen_place_ids, seen_domains = load_checkpoint()

    if len(leads) >= max_leads:
        logging.info("Already have %d leads (>= max %d). Exporting and exiting.", len(leads), max_leads)
        export_csv(leads, args.output)
        return

    logging.info(
        "Source=%s, max_leads=%d, extract_emails=%s, sleep_api=%.2f, sleep_web=%.2f",
        args.source, max_leads, extract_emails, args.sleep_api, args.sleep_web,
    )

    if args.source == "google":
        _run_google(args, leads, seen_place_ids, seen_domains, max_leads, extract_emails)
    else:
        _run_osm(args, leads, seen_place_ids, seen_domains, max_leads, extract_emails)

    save_checkpoint(leads, seen_place_ids, seen_domains, getattr(args, "checkpoint_file", None))
    export_csv(leads, args.output)
    logging.info("Done. Total leads: %d", len(leads))


def run_collection(
    niches: list[str],
    cities: list[str],
    max_leads: int = 50,
    extract_emails: bool = True,
    source: str = "osm",
    output_path: Path | None = None,
    clear_checkpoint: bool = True,
    sleep_api: float = 0.25,
    sleep_web: float = 0.15,
    config_path: Path | None = None,
    work_dir: Path | None = None,
) -> Path:
    """
    Programmatic entry point for lead collection (e.g. from web app).
    Returns path to the generated CSV.
    When work_dir is set (e.g. /tmp on Vercel), checkpoint and output use it.
    """
    if not niches or not cities:
        raise ValueError("niches and cities must be non-empty lists")
    # Serverless (e.g. Vercel) has read-only fs; use /tmp for all writes
    if work_dir is None and os.environ.get("VERCEL"):
        work_dir = Path("/tmp")
    out_dir = work_dir or OUTPUT_DIR
    output_path = output_path or (out_dir / "leads_export.csv")
    # On Vercel, never write CSV to /var/task; use /tmp even if caller passed another path
    if os.environ.get("VERCEL") and output_path and "/tmp" not in str(output_path):
        output_path = out_dir / output_path.name
    checkpoint_file = out_dir / "checkpoint.json" if work_dir else None
    config_path = config_path or Path("config.json")
    # Build a minimal args object
    class Args:
        pass

    args = Args()
    args.source = source
    args.config = config_path
    args.max_leads = max_leads
    args.extract_emails = "true" if extract_emails else "false"
    args.sleep_api = sleep_api
    args.sleep_web = sleep_web
    args.output = output_path
    args.clear_checkpoint = clear_checkpoint
    args.verbose = False
    args.checkpoint_file = checkpoint_file

    setup_logging(verbose=False)
    cf = _writable_checkpoint_path(checkpoint_file)
    if clear_checkpoint and cf.exists():
        cf.unlink()
    leads, seen_place_ids, seen_domains = load_checkpoint(checkpoint_file)
    # When using custom niches/cities we ignore checkpoint and start fresh for this run
    if len(leads) >= max_leads and not (niches and cities):
        export_csv(leads, output_path)
        return output_path
    if args.source == "google":
        try:
            _run_google(args, leads, seen_place_ids, seen_domains, max_leads, extract_emails, niches=niches, cities=cities)
        except Exception as e:
            logging.error("Google run failed: %s", e)
            raise
    else:
        _run_osm(args, leads, seen_place_ids, seen_domains, max_leads, extract_emails, niches=niches, cities=cities)
    save_checkpoint(leads, seen_place_ids, seen_domains, args.checkpoint_file)
    export_csv(leads, output_path)
    return output_path


if __name__ == "__main__":
    main()
