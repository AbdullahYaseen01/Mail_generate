# Lead Dataset Builder

A **compliant** lead dataset builder for businesses in Germany (Baden-Württemberg). Supports **Google Places API** or **OpenStreetMap (Overpass)** as data sources. No scraping. Optional email extraction from business websites.

## Features

- **Two data sources:**
  - **Google** – Places API (Text Search, Place Details, Nearby Search, Geocoding). Requires API key and billing enabled.
  - **OSM** – OpenStreetMap via Overpass API. **Free, no API key, no billing.**
- **Data quality** – Deduplication by place_id and website domain; normalized URLs; validated emails
- **Checkpointing** – Saves progress every 100 leads for resume after interruption
- **Optional email extraction** – Only visits business websites; respects `robots.txt`
- **Rate limiting** – Sleep between calls; exponential backoff (Google)

## Requirements

- Python 3.10+
- For **Google**: Google Cloud project, API key, billing enabled, Places + Geocoding APIs
- For **OSM**: Nothing else (uses public Overpass and Nominatim)

## Setup

### 1. Create a Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the following APIs:
   - **Places API** (includes Text Search, Place Details, Nearby Search)
   - **Geocoding API**

### 2. Create an API Key

1. Navigate to **APIs & Services** → **Credentials**
2. Click **Create Credentials** → **API Key**
3. Copy the API key
4. (Recommended) Restrict the key to the above APIs and set usage quotas

### 3. Configure the Project

```bash
# Copy the example config
cp config.example.json config.json

# Edit config.json and add your API key
# Replace YOUR_GOOGLE_API_KEY_HERE with your actual key
```

### 4. Install Dependencies

```bash
pip install -r requirements.txt
```

## Usage

### Option 1: OpenStreetMap (no API key, free)

```bash
python main.py --source osm --max-leads 1000
```

No config or API key needed. Uses Overpass + Nominatim. Same CSV format and cities/niches.

### Option 2: Google Places API

Requires `config.json` with a valid API key and billing enabled.

```bash
python main.py --source google --max-leads 1000 --extract-emails false
```

### Example Commands

```bash
# OSM: 500 leads, no key (recommended if you don't have Google billing)
python main.py --source osm --max-leads 500

# OSM with email extraction
python main.py --source osm --max-leads 200 --extract-emails true --sleep-web 1.5

# Google (default source): needs config.json + billing
python main.py --max-leads 1000

# Start fresh (clear checkpoint)
python main.py --source osm --clear-checkpoint --max-leads 200

# Custom output
python main.py --source osm --output outputs/leads_osm.csv --max-leads 100
```

### CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--source` | `google` | `google` (Places API) or `osm` (OpenStreetMap, free) |
| `--config` | `config.json` | Path to config (only for `--source google`) |
| `--max-leads` | `1000` | Maximum number of leads to collect |
| `--extract-emails` | `false` | Whether to extract emails from business websites |
| `--sleep-api` | `0.5` | Seconds to sleep between API/Overpass calls |
| `--sleep-web` | `1.0` | Seconds between website fetches (email extraction) |
| `--output` | `outputs/leads_de_bw.csv` | Output CSV path |
| `--clear-checkpoint` | - | Clear checkpoint and start fresh |
| `--verbose`, `-v` | - | Enable verbose logging |

## Output

- **CSV file** (`outputs/leads_de_bw.csv` by default) with columns:
  - `niche`, `city`, `business_name`, `formatted_address`, `latitude`, `longitude`
  - `phone`, `google_maps_url`, `website_url`, `rating`, `ratings_count`, `place_id`
  - `emails_found`, `email_source_page` (when `--extract-emails true`)

- **Checkpoint** (`outputs/checkpoint.json`) – saved every 100 leads for resume

With **OSM**, `google_maps_url` is an OpenStreetMap link; `rating` and `ratings_count` are empty.

## Cities & Niches

**Cities (Germany):** Mannheim, Heidelberg, Heilbronn, Pforzheim, Ulm, Reutlingen, Tübingen, Esslingen am Neckar

**Niches:** Physical therapists, Dentists, Auto repair shops, Moving companies, Cleaning companies, Beauty & wellness (premium), Real estate agents, Lawyers / tax advisors, Pet services, Plumbing & heating, Gardening & landscaping

## Compliance & Email Rules

- **No scraping** – Google: official Places/Geocoding APIs only. OSM: public Overpass/Nominatim APIs.
- **Email extraction** – Only visits business websites (from API/OSM); respects `robots.txt`
- **Spam** – Do not use collected data for unsolicited emails. Follow GDPR, CAN-SPAM and local laws. Obtain consent where required.

## Web frontend

A simple web UI lets you pick niches and cities and download a CSV.

1. Install dependencies (including Flask): `pip install -r requirements.txt`
2. From the project folder run: `python app.py`
3. Open in the browser: **http://127.0.0.1:5000**
4. Select one or more **niches** and **cities**, set max leads, then click **Generate CSV**. The file is generated (using OSM, 1–3 minutes for ~20 leads with emails) and downloaded automatically.

## Deploy on Vercel

The project is set up for Vercel (serverless + static).

1. **Install Vercel CLI** (optional): `npm i -g vercel`
2. **Deploy**: From the project root run `vercel` and follow the prompts, or connect the repo in [Vercel Dashboard](https://vercel.com/new).
3. **Requirements**: Root `requirements.txt` is used for the Python runtime. No env vars needed (OSM is used, no API key).
4. **Limits**:
   - **Hobby**: Serverless timeout is 10s — use **max leads 5–8** so the run finishes in time.
   - **Pro**: `api/generate.py` has `maxDuration: 60` in `vercel.json` — you can use up to ~15–25 leads per run.
5. **Routes**: `public/index.html` is served at `/`. `api/options` (GET) and `api/generate` (POST) are the serverless handlers.

## Project Structure

```
├── main.py              # Entry point, CLI, orchestration
├── app.py               # Flask web app (local)
├── api/                 # Vercel serverless
│   ├── options.py       # GET /api/options
│   └── generate.py      # POST /api/generate
├── public/
│   └── index.html       # Static frontend (Vercel: /)
├── static/
│   └── index.html       # Same UI for Flask (local)
├── vercel.json          # Vercel config (maxDuration 60s for generate)
├── config.json          # API key for Google (optional for OSM)
├── config.example.json  # Template
├── requirements.txt
├── README.md
├── utils/
│   ├── places.py        # Google Places API
│   ├── osm_places.py    # OpenStreetMap (Overpass) – no key
│   └── email_extractor.py  # Website email extraction
└── outputs/
    ├── leads_de_bw.csv      # Final CSV
    └── checkpoint.json      # Save checkpoint
```

## License

Use responsibly. Respect API terms of service and data protection regulations.
