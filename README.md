Playwright scraper for iden challenge

Files:

- `scrape_products.py` - main async script that reuses storage_state.json if present, logs in, navigates the 4-step wizard, and extracts product rows from a scrollable table area and writes `products.json`.
- `.env.example` - example environment variables
- `requirements.txt` - Python deps

Quick start (Windows cmd.exe):

1. Copy `.env.example` to `.env` and fill in credentials.
2. Create a virtual env and install deps:

   python -m venv .venv
   .\.venv\Scripts\pip.exe install -r requirements.txt
   .\.venv\Scripts\pip.exe install playwright
   .\.venv\Scripts\python.exe -m playwright install

3. Run the script:

   .\.venv\Scripts\python.exe scrape_products.py

Notes:

- The script tries to reuse `storage_state.json` to preserve sessions.
- The table extraction uses heuristics; you may need to tune selectors for the exact page structure.
