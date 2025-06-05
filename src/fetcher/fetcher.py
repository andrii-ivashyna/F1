"""
Enhanced OpenF1 API Data Fetcher class - Meeting-by-meeting processing
"""

import logging
from typing import List, Union, Dict
from .config import DB_FILENAME
from .database import DatabaseManager
from .progress import ProgressManager
from .api_client import OpenF1APIClient

logger = logging.getLogger(__name__)

class OpenF1Fetcher:
    def __init__(self, years: Union[int, List[int]] = 2024, meetings: Union[int, List[int], slice] = None, db_filename: str = DB_FILENAME):
        # Process years parameter
        if isinstance(years, int):
            self.years = [years]
        else:
            self.years = years
        
        self.meetings_filter = meetings
        
        # Initialize managers with current configuration
        self.db_manager = DatabaseManager(db_filename)
        self.progress_manager = ProgressManager(self.years, self.meetings_filter)
        self.api_client = OpenF1APIClient(self.db_manager)
    
    def fetch_meetings_for_years(self) -> List[Dict]:
        """Fetch all meetings for the specified years."""
        all_meetings = []
        
        for year in self.years:
            logger.info(f"Fetching meetings for year {year}...")
            meetings = self.api_client.make_request_with_retry("meetings", {"year": year})
            
            if meetings:
                all_meetings.extend(meetings)
                logger.info(f"Found {len(meetings)} meetings for {year}")
            else:
                logger.warning(f"No meetings found for year {year}")
        
        if all_meetings:
            logger.info(f"Total meetings found: {len(all_meetings)}")
        
        return all_meetings
    
    def process_meeting(self, meeting: Dict):
        """Process a single meeting: fetch meeting data, sessions, and all related data."""
        meeting_key = meeting['meeting_key']
        meeting_name = meeting.get('meeting_name', f'Meeting {meeting_key}')
        
        logger.info(f"Processing meeting {meeting_key}: {meeting_name}")
        
        # Step 1: Store meeting data if not already done
        if not self.progress_manager.is_meeting_fetched(meeting_key):
            logger.info(f"Storing meeting {meeting_key} data...")
            self.db_manager.insert_data("meetings", [meeting])
            self.progress_manager.mark_meeting_fetched(meeting_key)
            self.progress_manager.save_progress()
            logger.info(f"Meeting {meeting_key} data stored")
        else:
            logger.info(f"Meeting {meeting_key} data already stored")
        
        # Step 2: Fetch and store sessions for this meeting
        if not self.progress_manager.is_sessions_fetched(meeting_key):
            logger.info(f"Fetching sessions for meeting {meeting_key}...")
            sessions = self.api_client.make_request_with_retry("sessions", {"meeting_key": meeting_key})
            
            if sessions:
                self.db_manager.insert_data("sessions", sessions)
                self.progress_manager.mark_sessions_fetched(meeting_key)
                self.progress_manager.save_progress()
                logger.info(f"Stored {len(sessions)} sessions for meeting {meeting_key}")
            else:
                logger.warning(f"No sessions found for meeting {meeting_key}")
                # Mark as fetched even if no sessions to avoid retrying
                self.progress_manager.mark_sessions_fetched(meeting_key)
                self.progress_manager.save_progress()
        else:
            logger.info(f"Sessions for meeting {meeting_key} already fetched")
        
        # Step 3: Fetch meeting-level data
        if not self.progress_manager.is_meeting_data_fetched(meeting_key):
            self.fetch_meeting_data(meeting_key)
        else:
            logger.info(f"Meeting data for {meeting_key} already fetched")
        
        # Step 4: Fetch session-driver data for all sessions in this meeting
        self.fetch_session_driver_data_for_meeting(meeting_key)
        
        logger.info(f"Completed processing meeting {meeting_key}: {meeting_name}")
    
    def fetch_meeting_data(self, meeting_key: int):
        """Fetch data organized by meeting_key."""
        logger.info(f"Fetching meeting-level data for meeting {meeting_key}...")
        
        # Meeting-based endpoints
        meeting_endpoints = [
            'drivers', 'intervals', 'laps', 'pit', 'position', 
            'race_control', 'stints', 'team_radio', 'weather'
        ]
        
        for endpoint in meeting_endpoints:
            logger.info(f"Fetching {endpoint} data for meeting {meeting_key}...")
            
            data = self.api_client.make_request_with_retry(endpoint, {"meeting_key": meeting_key})
            if data:
                self.db_manager.insert_data(endpoint, data)
                logger.info(f"Successfully fetched {len(data)} records for {endpoint}")
            else:
                logger.warning(f"No data found for {endpoint} in meeting {meeting_key}")
        
        # Mark meeting data as fetched
        self.progress_manager.mark_meeting_data_fetched(meeting_key)
        self.progress_manager.save_progress()
        logger.info(f"Completed fetching meeting-level data for meeting {meeting_key}")
    
    def fetch_session_driver_data_for_meeting(self, meeting_key: int):
        """Fetch session-driver data for all sessions in a specific meeting."""
        # Get all sessions for this meeting directly
        meeting_sessions = self.db_manager.get_sessions_for_meeting(meeting_key)
        
        if not meeting_sessions:
            logger.warning(f"No sessions found for meeting {meeting_key}")
            return
        
        logger.info(f"Processing session-driver data for {len(meeting_sessions)} sessions in meeting {meeting_key}")
        
        # Session-driver endpoints
        session_driver_endpoints = ['car_data', 'location']
        
        for session_key in meeting_sessions:
            if self.progress_manager.is_session_data_fetched(session_key):
                logger.info(f"Session {session_key} data already fetched, skipping...")
                continue
            
            # Get drivers for this specific session
            driver_numbers = self.db_manager.get_drivers_for_session(session_key)
            
            if not driver_numbers:
                logger.warning(f"No drivers found for session {session_key}, skipping...")
                self.progress_manager.mark_session_data_fetched(session_key, meeting_key)
                self.progress_manager.save_progress()
                continue
            
            logger.info(f"Processing session {session_key} with {len(driver_numbers)} drivers")
            
            for endpoint in session_driver_endpoints:
                logger.info(f"Fetching {endpoint} data for session {session_key}...")
                
                # Use chunked request to handle large data sets
                all_data = self.api_client.make_chunked_request(
                    endpoint, 
                    {"session_key": session_key}, 
                    "driver_number", 
                    driver_numbers
                )
                
                if all_data:
                    self.db_manager.insert_data(endpoint, all_data)
                    logger.info(f"Successfully fetched {len(all_data)} records for {endpoint}")
                else:
                    logger.warning(f"No data found for {endpoint} in session {session_key}")
            
            # Mark this session as completed
            self.progress_manager.mark_session_data_fetched(session_key, meeting_key)
            self.progress_manager.save_progress()
            
            logger.info(f"Completed processing session {session_key}")
    
    def fetch_all_data(self):
        """Main method to fetch all F1 data with meeting-by-meeting processing."""
        logger.info("Starting Enhanced F1 data fetch...")
        logger.info(f"Years: {self.years}")
        logger.info(f"Meetings filter: {self.meetings_filter}")
        logger.info(self.progress_manager.get_progress_summary())
        
        try:
            # Get all meetings for the specified years
            all_meetings = self.fetch_meetings_for_years()
            
            if not all_meetings:
                logger.error("No meetings found for the specified years")
                return
            
            # Apply meetings filter
            meetings_to_process = self.progress_manager.get_meetings_to_process(all_meetings)
            
            if not meetings_to_process:
                logger.error("No meetings to process after applying filter")
                return
            
            logger.info(f"Processing {len(meetings_to_process)} meetings out of {len(all_meetings)} total")
            
            # Process each meeting individually
            for i, meeting in enumerate(meetings_to_process, 1):
                meeting_key = meeting['meeting_key']
                meeting_name = meeting.get('meeting_name', f'Meeting {meeting_key}')
                
                logger.info(f"=== Processing meeting {i}/{len(meetings_to_process)}: {meeting_name} (Key: {meeting_key}) ===")
                
                # Check if this meeting is already fully completed
                if self.progress_manager.is_meeting_completed(meeting_key):
                    logger.info(f"Meeting {meeting_key} already fully completed, skipping...")
                    continue
                
                # Process this meeting
                self.process_meeting(meeting)
                
                logger.info(f"=== Completed meeting {i}/{len(meetings_to_process)}: {meeting_name} ===")
                logger.info(self.progress_manager.get_progress_summary())
            
            logger.info("All meetings processed successfully!")
            self.db_manager.print_summary()
            
        except Exception as e:
            logger.error(f"Error during data fetch: {e}")
            logger.info("Progress has been saved. You can resume by running the script again.")
            raise
