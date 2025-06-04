"""
Main OpenF1 API Data Fetcher class
"""

import logging
from typing import List, Union
from .config import DB_FILENAME
from .database import DatabaseManager
from .progress import ProgressManager
from .api_client import OpenF1APIClient

logger = logging.getLogger(__name__)

class OpenF1Fetcher:
    def __init__(self, years: Union[int, List[int]] = 2024, meetings: Union[int, List[int], slice] = None, db_filename: str = DB_FILENAME):
        self.db_manager = DatabaseManager(db_filename)
        self.progress_manager = ProgressManager()
        self.api_client = OpenF1APIClient(self.db_manager)
        
        # Process years parameter
        if isinstance(years, int):
            self.years = [years]
        else:
            self.years = years
        
        # Process meetings parameter (will be applied after fetching meetings)
        self.meetings_filter = meetings
    
    def apply_meetings_filter(self, meetings: List[dict]) -> List[dict]:
        """Apply meetings filter based on the meetings parameter."""
        if self.meetings_filter is None:
            return meetings
        
        if isinstance(self.meetings_filter, int):
            # Single meeting by index
            if 0 <= self.meetings_filter < len(meetings):
                return [meetings[self.meetings_filter]]
            else:
                logger.warning(f"Meeting index {self.meetings_filter} is out of range (0-{len(meetings)-1})")
                return []
        
        elif isinstance(self.meetings_filter, list):
            # Multiple meetings by indices
            filtered_meetings = []
            for idx in self.meetings_filter:
                if 0 <= idx < len(meetings):
                    filtered_meetings.append(meetings[idx])
                else:
                    logger.warning(f"Meeting index {idx} is out of range (0-{len(meetings)-1})")
            return filtered_meetings
        
        elif isinstance(self.meetings_filter, slice):
            # Slice notation
            return meetings[self.meetings_filter]
        
        else:
            logger.warning(f"Invalid meetings filter type: {type(self.meetings_filter)}")
            return meetings
    
    def fetch_meetings(self):
        """Fetch meetings data for specified years."""
        if self.progress_manager.get('meetings_completed', False):
            logger.info("Meetings already fetched, skipping...")
            return
        
        all_meetings = []
        for year in self.years:
            logger.info(f"Fetching {year} meetings...")
            meetings = self.api_client.make_request_with_retry("meetings", {"year": year})
            
            if meetings:
                all_meetings.extend(meetings)
                logger.info(f"Successfully fetched {len(meetings)} meetings for {year}")
            else:
                logger.warning(f"No meetings found for year {year}")
        
        if all_meetings:
            # Apply meetings filter
            filtered_meetings = self.apply_meetings_filter(all_meetings)
            
            if filtered_meetings:
                logger.info(f"Filtered to {len(filtered_meetings)} meetings from {len(all_meetings)} total")
                self.db_manager.insert_data("meetings", filtered_meetings)
                self.progress_manager.set('meetings_completed', True)
                self.progress_manager.set('current_step', 'sessions')
                self.progress_manager.save_progress()
                logger.info(f"Successfully stored {len(filtered_meetings)} meetings")
            else:
                logger.error("No meetings remaining after filtering")
        else:
            logger.error("Failed to fetch any meetings")
    
    def fetch_sessions(self):
        """Fetch sessions data for stored meetings."""
        if self.progress_manager.get('sessions_completed', False):
            logger.info("Sessions already fetched, skipping...")
            return
        
        # Get meeting keys from stored meetings
        meeting_keys = self.db_manager.get_existing_keys("meetings", "meeting_key")
        
        if not meeting_keys:
            logger.error("No meetings found in database. Cannot fetch sessions.")
            return
        
        all_sessions = []
        for meeting_key in meeting_keys:
            logger.info(f"Fetching sessions for meeting {meeting_key}...")
            sessions = self.api_client.make_request_with_retry("sessions", {"meeting_key": meeting_key})
            
            if sessions:
                all_sessions.extend(sessions)
                logger.info(f"Successfully fetched {len(sessions)} sessions for meeting {meeting_key}")
            else:
                logger.warning(f"No sessions found for meeting {meeting_key}")
        
        if all_sessions:
            self.db_manager.insert_data("sessions", all_sessions)
            self.progress_manager.set('sessions_completed', True)
            self.progress_manager.set('current_step', 'meeting_data')
            self.progress_manager.save_progress()
            logger.info(f"Successfully stored {len(all_sessions)} sessions")
        else:
            logger.error("Failed to fetch any sessions")
    
    def fetch_meeting_data(self):
        """Fetch data organized by meeting_key."""
        meeting_keys = self.db_manager.get_existing_keys("meetings", "meeting_key")
        completed_meetings = self.progress_manager.get('completed_meetings', [])
        
        # Meeting-based endpoints
        meeting_endpoints = [
            'drivers', 'intervals', 'laps', 'pit', 'position', 
            'race_control', 'stints', 'team_radio', 'weather'
        ]
        
        for meeting_key in meeting_keys:
            if meeting_key in completed_meetings:
                logger.info(f"Meeting {meeting_key} already processed, skipping...")
                continue
            
            logger.info(f"Processing meeting {meeting_key}...")
            
            for endpoint in meeting_endpoints:
                logger.info(f"Fetching {endpoint} data for meeting {meeting_key}...")
                
                data = self.api_client.make_request_with_retry(endpoint, {"meeting_key": meeting_key})
                if data:
                    self.db_manager.insert_data(endpoint, data)
                    logger.info(f"Successfully fetched {len(data)} records for {endpoint}")
                else:
                    logger.warning(f"No data found for {endpoint} in meeting {meeting_key}")
            
            # Mark this meeting as completed
            self.progress_manager.add_completed_meeting(meeting_key)
            self.progress_manager.save_progress()
            
            logger.info(f"Completed processing meeting {meeting_key}")
        
        # Update progress
        self.progress_manager.set('current_step', 'session_driver_data')
        self.progress_manager.save_progress()
    
    def fetch_session_driver_data(self):
        """Fetch data that requires both session_key and driver_number."""
        session_keys = self.db_manager.get_existing_keys("sessions", "session_key")
        completed_sessions = self.progress_manager.get('completed_sessions', [])
        
        # Session-driver endpoints
        session_driver_endpoints = ['car_data', 'location']
        
        for session_key in session_keys:
            if session_key in completed_sessions:
                logger.info(f"Session {session_key} already processed, skipping...")
                continue
            
            # Get drivers for this specific session instead of all drivers
            driver_numbers = self.db_manager.get_drivers_for_session(session_key)
            
            if not driver_numbers:
                logger.warning(f"No drivers found for session {session_key}, skipping...")
                self.progress_manager.add_completed_session(session_key)
                self.progress_manager.save_progress()
                continue
            
            logger.info(f"Processing session {session_key} with {len(driver_numbers)} drivers: {driver_numbers}")
            
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
            self.progress_manager.add_completed_session(session_key)
            self.progress_manager.save_progress()
            
            logger.info(f"Completed processing session {session_key}")
    
    def fetch_all_data(self):
        """Main method to fetch all F1 data with resumable functionality."""
        logger.info("Starting F1 data fetch...")
        logger.info(f"Years: {self.years}")
        logger.info(f"Meetings filter: {self.meetings_filter}")
        
        try:
            # Step 1: Fetch meetings
            if self.progress_manager.get('current_step') in ['meetings']:
                self.fetch_meetings()
            
            # Step 2: Fetch sessions
            if self.progress_manager.get('current_step') in ['meetings', 'sessions']:
                self.fetch_sessions()
            
            # Step 3: Fetch meeting-based data
            if self.progress_manager.get('current_step') in ['meetings', 'sessions', 'meeting_data']:
                self.fetch_meeting_data()
            
            # Step 4: Fetch session-driver data
            if self.progress_manager.get('current_step') in ['meetings', 'sessions', 'meeting_data', 'session_driver_data']:
                self.fetch_session_driver_data()
            
            # Mark as completed
            self.progress_manager.set('current_step', 'completed')
            self.progress_manager.save_progress()
            
            logger.info("Data fetch completed successfully!")
            self.db_manager.print_summary()
            
        except Exception as e:
            logger.error(f"Error during data fetch: {e}")
            raise
