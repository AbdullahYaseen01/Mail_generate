"""
Quick integration test - verifies modules load and non-API logic works.
Run: python test_integration.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def test_places_utils():
    """Test places module (no API calls)."""
    from utils.places import _normalize_website, _extract_domain

    assert _normalize_website("http://example.com?utm_source=fb") == "http://example.com"
    assert _normalize_website("example.com") == "https://example.com"
    assert _extract_domain("https://www.foo.de/kontakt") == "foo.de"
    print("  places: OK")


def test_email_extractor():
    """Test email extraction from HTML."""
    from utils.email_extractor import extract_emails_from_html

    html = "Contact: info@business.de or support@example.com"
    emails = extract_emails_from_html(html)
    found = [e for e, _ in emails]
    assert "info@business.de" in found
    assert "support@example.com" not in found  # example.com is placeholder
    print("  email_extractor: OK")


def test_csv_export():
    """Test CSV export logic."""
    import csv

    from main import OUTPUT_DIR, export_csv

    test_leads = [
        {"niche": "Dentists", "city": "Heidelberg", "business_name": "Test", "formatted_address": "Test St 1"},
        {"niche": "Dentists", "city": "Heidelberg", "business_name": "Test2", "formatted_address": "Test St 2"},
    ]
    out = OUTPUT_DIR / "test_output.csv"
    export_csv(test_leads, out)
    assert out.exists()
    with open(out, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 2
    assert rows[0]["business_name"] == "Test"
    out.unlink()
    print("  CSV export: OK")


def main():
    print("Running integration tests...")
    test_places_utils()
    test_email_extractor()
    test_csv_export()
    print("All tests passed.")


if __name__ == "__main__":
    main()
