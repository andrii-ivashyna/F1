#!/usr/bin/env python3
"""
Enhanced OpenF1 API Data Fetcher - Main Script
Fetches F1 data from OpenF1 API with flexible year/meeting selection and robust error handling.
"""

import os
import logging
from fetcher.fetcher import OpenF1Fetcher
from fetcher.config import DATA_FOLDER, LOG_FILENAME

def setup_logging():
    """Setup logging configuration."""
    log_path = os.path.join(DATA_FOLDER, LOG_FILENAME)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    return logger

def main():
    """Main function to run the data fetcher."""
    logger = setup_logging()
    
    logger.info("Starting Enhanced OpenF1 Data Fetcher")
    logger.info(f"Data will be saved to: {DATA_FOLDER}/")
    
    # Examples of usage:
    # Single year, all meetings: OpenF1Fetcher(2024)
    # Multiple years: OpenF1Fetcher([2023, 2024])
    # Single year, specific meeting: OpenF1Fetcher(2024, 0)  # First meeting
    # Single year, multiple meetings: OpenF1Fetcher(2024, [0, 1, 3])
    # Single year, slice of meetings: OpenF1Fetcher(2024, slice(6, None))  # From 6th meeting onwards
    
    fetcher = OpenF1Fetcher(
        years=2024,           # Can be int or list of ints
        meetings=None  # Can be int, list of ints, or slice (None for all)
    )
    
    try:
        fetcher.fetch_all_data()
        logger.info(f"All data successfully fetched and stored in {DATA_FOLDER}/")
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        logger.info("Progress has been saved. You can resume by running the script again.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

if __name__ == "__main__":
    main()
