"""
Email extraction from business websites.
Only visits URLs returned from Place Details - no Google scraping.
Respects robots.txt.
"""

import logging
import re
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests

logger = logging.getLogger(__name__)

# RFC 5322 compliant email regex (simplified for common cases)
EMAIL_PATTERN = re.compile(
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

# Placeholder/invalid email domains to skip
INVALID_EMAIL_DOMAINS = {
    "example.com",
    "example.org",
    "example.net",
    "test.com",
    "placeholder.com",
    "email.com",
    "domain.com",
    "sentry.io",
    "wixpress.com",
    "sentry.wixpress.com",
    "sentry-next.wixpress.com",
    "yoursite.com",
    "youremail.com",
}

# Domain suffixes to skip (e.g. all *.wixpress.com)
INVALID_EMAIL_DOMAIN_SUFFIXES = (
    ".wixpress.com",
    ".sentry.io",
)

# Local part that looks like a tracking/internal ID (long hex string)
HEX_ID_PATTERN = re.compile(r"^[a-f0-9]{20,}$", re.IGNORECASE)


def _is_valid_email(email: str) -> bool:
    """Validate email - skip placeholders, Wix/Sentry internal addresses, and hex IDs."""
    if not email or len(email) > 254:
        return False
    email_lower = email.lower().strip()
    if "@" not in email_lower:
        return False
    local, domain = email_lower.rsplit("@", 1)
    if domain in INVALID_EMAIL_DOMAINS:
        return False
    for suffix in INVALID_EMAIL_DOMAIN_SUFFIXES:
        if domain.endswith(suffix) or domain == suffix.strip("."):
            return False
    # Skip local parts that are long hex strings (Wix/Sentry internal IDs)
    if HEX_ID_PATTERN.match(local):
        return False
    # Skip image/data URIs that might match
    if email.startswith("data:") or ".png" in email or ".jpg" in email:
        return False
    return True


def _check_robots_allowed(url: str, user_agent: str = "LeadDatasetBuilder/1.0") -> bool:
    """Check if robots.txt allows fetching the given URL."""
    try:
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        robots_url = urljoin(base, "/robots.txt")

        parser = RobotFileParser()
        parser.set_url(robots_url)
        parser.read()

        return parser.can_fetch(user_agent, url)
    except Exception as e:
        logger.debug("robots.txt check failed for %s: %s", url, e)
        # On error, be conservative - allow (many sites have no robots.txt)
        return True


def _fetch_page(
    url: str,
    timeout: int = 6,
    user_agent: str = "LeadDatasetBuilder/1.0",
    session: requests.Session | None = None,
) -> str | None:
    """Fetch page HTML. Returns None on failure. Uses session for connection reuse."""
    try:
        headers = {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        }
        req = session.get if session else requests.get
        resp = req(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logger.debug("Failed to fetch %s: %s", url, e)
        return None


def _get_candidate_urls(website: str, max_pages: int = 2) -> list[str]:
    """Get homepage plus contact/impressum (max_pages total) for faster extraction."""
    parsed = urlparse(website)
    base = f"{parsed.scheme}://{parsed.netloc}"
    base_path = parsed.path.rstrip("/") or "/"

    candidates = [
        urljoin(base, base_path),
        urljoin(base, "/kontakt"),
        urljoin(base, "/contact"),
        urljoin(base, "/impressum"),
    ]

    seen = set()
    unique: list[str] = []
    for u in candidates:
        norm = u.rstrip("/") or u
        if norm not in seen:
            seen.add(norm)
            unique.append(u)

    return unique[:max_pages]


def extract_emails_from_html(html: str) -> list[tuple[str, str]]:
    """
    Extract valid emails from HTML.
    Returns list of (email, source_hint) - source_hint is the page type for logging.
    """
    if not html:
        return []
    found: set[str] = set()
    results: list[tuple[str, str]] = []
    for m in EMAIL_PATTERN.finditer(html):
        email = m.group(0).strip()
        # Filter out false positives
        if " " in email or "\n" in email:
            continue
        if email in found:
            continue
        if _is_valid_email(email):
            found.add(email)
            results.append((email, "page"))
    return results


def extract_emails_from_website(
    website_url: str,
    sleep_seconds: float = 0.0,
    max_pages: int = 2,
) -> tuple[list[str], str]:
    """
    Visit business website and extract public emails.
    Respects robots.txt. Uses Session for connection reuse and limits pages for speed.
    Returns (list of unique emails, comma-separated source page hints).
    """
    if not website_url or not website_url.startswith(("http://", "https://")):
        return [], ""

    if not _check_robots_allowed(website_url):
        logger.info("robots.txt blocks %s - skipping email extraction", website_url)
        return [], "robots_blocked"

    all_emails: list[tuple[str, str]] = []
    source_pages: list[str] = []
    import time

    with requests.Session() as session:
        for i, url in enumerate(_get_candidate_urls(website_url, max_pages=max_pages)):
            if i > 0:
                time.sleep(sleep_seconds)

            html = _fetch_page(url, session=session)
            if html:
                page_type = "homepage" if i == 0 else f"page_{i}"
                emails = extract_emails_from_html(html)
                for email, _ in emails:
                    all_emails.append((email, page_type))

    # Deduplicate by email
    seen_emails: set[str] = set()
    unique_emails: list[str] = []
    for email, src in all_emails:
        if email not in seen_emails:
            seen_emails.add(email)
            unique_emails.append(email)
            source_pages.append(src)

    source_str = ",".join(source_pages) if source_pages else ""
    return unique_emails, source_str
