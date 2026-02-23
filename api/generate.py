"""
Vercel serverless: POST /api/generate - runs lead collection and returns CSV.
Uses /tmp for output (Vercel serverless writable dir).
"""
import json
import sys
import time
from pathlib import Path

# Ensure project root is on path when running on Vercel (api/generate.py -> parent.parent)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from main import run_collection
from http.server import BaseHTTPRequestHandler


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            content_len = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_len).decode("utf-8") if content_len else "{}"
            data = json.loads(body) if body.strip() else {}
        except (json.JSONDecodeError, ValueError):
            self._send_json(400, {"error": "Invalid JSON body."})
            return

        niches = data.get("niches") or []
        cities = data.get("cities") or []
        max_leads = min(500, max(1, int(data.get("max_leads", 15))))
        extract_emails = bool(data.get("extract_emails", True))

        if not niches or not cities:
            self._send_json(400, {"error": "Please provide at least one niche and one city."})
            return
        if not isinstance(niches, list):
            niches = [str(niches)]
        else:
            niches = [str(n).strip() for n in niches if str(n).strip()]
        if not isinstance(cities, list):
            cities = [str(cities)]
        else:
            cities = [str(c).strip() for c in cities if str(c).strip()]
        if not niches or not cities:
            self._send_json(400, {"error": "Please provide at least one niche and one city."})
            return

        work_dir = Path("/tmp")
        timestamp = int(time.time())
        out_path = work_dir / f"leads_vercel_{timestamp}.csv"

        try:
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
                work_dir=work_dir,
            )
        except ValueError as e:
            self._send_json(400, {"error": str(e)})
            return
        except Exception as e:
            self._send_json(500, {"error": str(e)})
            return

        if not out_path.exists():
            self._send_json(500, {"error": "CSV was not generated."})
            return

        try:
            csv_bytes = out_path.read_bytes()
            out_path.unlink(missing_ok=True)
        except Exception as e:
            self._send_json(500, {"error": f"Could not read CSV: {e}"})
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", f'attachment; filename="leads_{timestamp}.csv"')
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(csv_bytes)

    def _send_json(self, status: int, obj: dict):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(obj).encode("utf-8"))

    def log_message(self, format, *args):
        pass
