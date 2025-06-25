# main.py

import config # Import config directly
import fetch_api
import parse_circuit
import parse_team
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
    python main.py                    - Run full process (create DB, fetch API, parse all sources)
    python main.py api                - Only fetch API data (creates DB first)
    python main.py wiki               - Only parse Wikipedia data (circuit and team)
    python main.py f1                 - Only parse F1 official site data (circuit and team)
    python main.py circuit           - Only parse circuit data (both Wikipedia and F1)
    python main.py team              - Only parse team data (both Wikipedia and F1)
    python main.py circuit-wiki      - Only parse Wikipedia circuit data
    python main.py circuit-f1        - Only parse F1 official site circuit data
    python main.py team-wiki         - Only parse Wikipedia team data
    python main.py team-f1           - Only parse F1 official site team data
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
                parse_circuit.parse_circuit_wiki()
                parse_team.parse_team_wiki()
                
            elif command == 'f1':
                log("=== Running F1 official site parsing only ===")
                parse_circuit.parse_circuit_f1()
                parse_team.parse_team_f1()
                
            elif command == 'circuit':
                log("=== Running circuit parsing only ===")
                parse_circuit.run_circuit_parsers()
                
            elif command == 'team':
                log("=== Running team parsing only ===")
                parse_team.run_team_parsers()
                
            elif command == 'circuit-wiki':
                log("=== Running Wikipedia circuit parsing only ===")
                parse_circuit.parse_circuit_wiki()
                
            elif command == 'circuit-f1':
                log("=== Running F1 official site circuit parsing only ===")
                parse_circuit.parse_circuit_f1()
                
            elif command == 'team-wiki':
                log("=== Running Wikipedia team parsing only ===")
                parse_team.parse_team_wiki()
                
            elif command == 'team-f1':
                log("=== Running F1 official site team parsing only ===")
                parse_team.parse_team_f1()
                
            else:
                log(f"Unknown command: {command}")
                log("Available commands: api, wiki, f1, circuit, team, circuit-wiki, circuit-f1, team-wiki, team-f1")
                sys.exit(1)
        else:
            # Default: run full process
            log("=== Starting the Formula 1 data setup process ===")
            
            # Step 1: Create database schema
            config.create_database()
            
            # Step 2: Populate database with data from the OpenF1 API
            fetch_api.populate_database()
            
            # Step 3: Enrich the database with data from Wikipedia and F1 official site
            parse_circuit.run_circuit_parsers()
            parse_team.run_team_parsers()
        
        log("=== Process finished successfully. ===")
        
    except Exception as e:
        log(f"FATAL ERROR: An unexpected error occurred during the process: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
