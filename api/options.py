"""
Vercel serverless: GET /api/options - returns niches and cities for the form.
"""
import json
import sys
from pathlib import Path

# Ensure project root is on path when running on Vercel
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from main import NICHES, CITIES
from http.server import BaseHTTPRequestHandler


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        body = json.dumps({"niches": NICHES, "cities": CITIES})
        self.wfile.write(body.encode("utf-8"))

    def log_message(self, format, *args):
        pass
