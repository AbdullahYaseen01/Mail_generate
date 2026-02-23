"""
Flask backend for Lead Dataset Builder frontend.
Serves the UI and /api/generate to run collection and return CSV.
"""

import logging
import time
from pathlib import Path

from flask import Flask, jsonify, request, send_file

# Import after we're in the project directory
from main import (
    NICHES,
    CITIES,
    OUTPUT_DIR,
    run_collection,
)

app = Flask(__name__, static_folder="static", static_url_path="")
app.config["MAX_CONTENT_LENGTH"] = 64 * 1024  # 64KB max request

# Ensure output dir exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(level=logging.INFO)


@app.route("/")
def index():
    """Serve the frontend."""
    return send_file(Path(__file__).parent / "static" / "index.html")


@app.route("/api/options")
def api_options():
    """Return default niches and cities for the form."""
    return jsonify({"niches": NICHES, "cities": CITIES})


@app.route("/api/generate", methods=["POST"])
def api_generate():
    """
    Run lead collection and return the CSV file.
    JSON body: { "niches": ["Dentists"], "cities": ["Mannheim"], "max_leads": 10, "extract_emails": true }
    """
    try:
        data = request.get_json(force=True, silent=True) or {}
        niches = data.get("niches") or []
        cities = data.get("cities") or []
        max_leads = int(data.get("max_leads", 20))
        extract_emails = bool(data.get("extract_emails", True))

        if not niches or not cities:
            return jsonify({"error": "Please provide at least one niche and one city."}), 400
        if not isinstance(niches, list):
            niches = [str(niches)]
        else:
            niches = [str(n).strip() for n in niches if str(n).strip()]
        if not isinstance(cities, list):
            cities = [str(cities)]
        else:
            cities = [str(c).strip() for c in cities if str(c).strip()]
        if not niches or not cities:
            return jsonify({"error": "Please provide at least one niche and one city."}), 400

        max_leads = max(1, min(500, max_leads))
        timestamp = int(time.time())
        out_path = OUTPUT_DIR / f"leads_web_{timestamp}.csv"

        run_collection(
            niches=niches,
            cities=cities,
            max_leads=max_leads,
            extract_emails=extract_emails,
            source="osm",
            output_path=out_path,
            clear_checkpoint=True,
            sleep_api=0.25,
            sleep_web=0.15,
        )

        if not out_path.exists():
            return jsonify({"error": "CSV was not generated."}), 500

        return send_file(
            out_path,
            as_attachment=True,
            download_name=f"leads_{timestamp}.csv",
            mimetype="text/csv",
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logging.exception("Generate failed")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
