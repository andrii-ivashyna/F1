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
    Usage:
    python main.py                    - Run full process (create DB, fetch API, parse Wikipedia)
    python main.py api                - Only fetch API data (creates DB first)
    python main.py wiki               - Only parse Wikipedia data (all parsers)
    python main.py circuits           - Only parse Wikipedia circuit data
    python main.py constructors       - Only parse Wikipedia constructor data
    """
    try:
        # Check command line arguments
        if len(sys.argv) > 1:
            command = sys.argv[1].lower()
            
            if command == 'api':
                log("=== Running API data fetching only ===")
                config.create_database()
                fetch_api.populate_database()
                
            elif command == 'wiki':
                log("=== Running Wikipedia parsing only ===")
                parse_wiki.run_wiki_parsers()
                
            elif command == 'circuits':
                log("=== Running Wikipedia circuit parsing only ===")
                parse_wiki.parse_circuits()
                
            elif command == 'constructors':
                log("=== Running Wikipedia constructor parsing only ===")
                parse_wiki.parse_constructors()
                
            else:
                log(f"Unknown command: {command}")
                log("Available commands: api, wiki, circuits, constructors")
                sys.exit(1)
        else:
            # Default: run full process
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