# main.py

import fetch_api
import sys
from datetime import datetime

def log(message):
    """Prints a message with a timestamp."""
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}")

def main():
    """
    Main function to run the database setup and data fetching process.
    """
    try:
        log("Starting the Formula 1 data setup process...")
        fetch_api.create_database()
        fetch_api.populate_database()
        log("Process finished successfully.")
    except Exception as e:
        log(f"An error occurred during the process: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
