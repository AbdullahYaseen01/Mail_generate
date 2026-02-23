@echo off
REM Lead Dataset Builder - Run script
REM 1. Edit config.json and add your Google API key, OR
REM 2. Set GOOGLE_API_KEY env var: set GOOGLE_API_KEY=your_key_here

python main.py --max-leads 1000 --extract-emails false %*
