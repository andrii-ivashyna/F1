# main.py

import sys
import config
import fetch_api
import parse_circuit
import parse_team
import parse_driver
from config import log, end_log

# --- IMPORTANT ---
# Before running, please install the required libraries:
# pip install requests beautifulsoup4 pandas pycountry lxml

def main():
    """
    Main function to run the full data fetching and enrichment process.
    Provides command-line arguments to run specific parts of the process.
    """
    try:
        if len(sys.argv) > 1:
            command = sys.argv[1].lower()
            log(f"Running command: {command}", 'HEADING')
            
            if command == 'api':
                config.create_database()
                fetch_api.populate_database()
            elif command == 'wiki':
                parse_circuit.parse_circuit_wiki()
                parse_team.parse_team_wiki()
            elif command == 'f1':
                parse_circuit.parse_circuit_f1()
                parse_team.parse_team_f1()
                parse_driver.parse_driver_f1()
            elif command == 'circuit':
                parse_circuit.run_circuit_parsers()
            elif command == 'team':
                parse_team.run_team_parsers()
            elif command == 'driver':
                parse_driver.run_driver_parsers()
            elif command == 'circuit-wiki':
                parse_circuit.parse_circuit_wiki()
            elif command == 'circuit-f1':
                parse_circuit.parse_circuit_f1()
            elif command == 'team-wiki':
                parse_team.parse_team_wiki()
            elif command == 'team-f1':
                parse_team.parse_team_f1()
            elif command == 'driver-f1':
                parse_driver.parse_driver_f1()
            else:
                log(f"Unknown command: {command}", 'ERROR')
                log("See --help for available commands.", 'INFO')
                sys.exit(1)
        else:
            # Default: run full process
            log("Starting the Full Formula 1 Data Setup Process", 'HEADING')
            
            config.create_database()
            fetch_api.populate_database()
            parse_circuit.run_circuit_parsers()
            parse_team.run_team_parsers()
            parse_driver.run_driver_parsers()
        
        log("Process finished successfully.", 'SUCCESS')
        end_log()
        
    except Exception as e:
        log(f"An unexpected error occurred during the process", 'ERROR', data={'error': str(e)})
        import traceback
        traceback.print_exc()
        end_log()
        sys.exit(1)

if __name__ == '__main__':
    main()
