# main.py

import config # Import config directly
import fetch_api
import parse_wiki
import sys
from datetime import datetime

# --- IMPORTANT ---
# Before running, please install the required libraries:
# pip install requests beautifulsoup4 pandas pycountry

def log(message):
    """Prints a message with a timestamp."""
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}")

def main():
    """
    Main function to run the full data fetching and enrichment process.
    """
    try:
        log("=== Starting the Formula 1 data setup process ===")
        
        # Step 1: Create database schema
        config.create_database()
        
        # Step 2: Populate database with data from the OpenF1 API
        fetch_api.populate_database()
        
        # Step 3: Enrich the database with data from Wikipedia
        parse_wiki.run_wiki_parsers()
        
        log("=== Process finished successfully. ===")
        
    except Exception as e:
        log(f"FATAL ERROR: An unexpected error occurred during the process: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
