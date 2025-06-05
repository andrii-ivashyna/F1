"""
Enhanced Progress tracking for OpenF1 API Data Fetcher
"""

import json
import os
import logging
import hashlib
from typing import Dict, List, Union
from .config import DATA_FOLDER, PROGRESS_FILENAME

logger = logging.getLogger(__name__)

class ProgressManager:
    def __init__(self, years: Union[int, List[int]], meetings_filter: Union[int, List[int], slice] = None):
        self.progress_path = os.path.join(DATA_FOLDER, PROGRESS_FILENAME)
        self.years = years if isinstance(years, list) else [years]
        self.meetings_filter = meetings_filter
        
        # Create a hash of the current configuration to detect changes
        self.config_hash = self._create_config_hash()
        self.progress = self.load_progress()
    
    def _create_config_hash(self) -> str:
        """Create a hash of the current configuration to detect parameter changes."""
        config_str = f"years:{sorted(self.years)},meetings_filter:{self.meetings_filter}"
        return hashlib.md5(config_str.encode()).hexdigest()
    
    def load_progress(self) -> Dict:
        """Load fetch progress from file."""
        default_progress = {
            'config_hash': self.config_hash,
            'years': self.years,
            'meetings_filter': self.meetings_filter,
            'completed_meetings': {},  # {meeting_key: {'meeting_fetched': bool, 'sessions_fetched': bool, 'data_fetched': bool}}
            'completed_sessions': {},  # {session_key: {'meeting_key': int, 'data_fetched': bool}}
            'current_meeting_index': 0,
            'total_meetings_found': 0
        }
        
        if os.path.exists(self.progress_path):
            try:
                with open(self.progress_path, 'r') as f:
                    saved_progress = json.load(f)
                    
                    # Check if configuration has changed
                    if saved_progress.get('config_hash') != self.config_hash:
                        logger.info("Configuration changed, resetting progress")
                        logger.info(f"Old config: years={saved_progress.get('years')}, meetings_filter={saved_progress.get('meetings_filter')}")
                        logger.info(f"New config: years={self.years}, meetings_filter={self.meetings_filter}")
                        return default_progress
                    
                    # Merge with defaults for any missing keys
                    for key, value in default_progress.items():
                        if key not in saved_progress:
                            saved_progress[key] = value
                    
                    logger.info(f"Loaded progress: {len(saved_progress.get('completed_meetings', {}))} meetings processed")
                    return saved_progress
                    
            except Exception as e:
                logger.warning(f"Could not load progress file: {e}")
        
        logger.info("Starting with fresh progress")
        return default_progress
    
    def save_progress(self):
        """Save current progress to file."""
        try:
            with open(self.progress_path, 'w') as f:
                json.dump(self.progress, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save progress: {e}")
    
    def get(self, key: str, default=None):
        """Get progress value."""
        return self.progress.get(key, default)
    
    def set(self, key: str, value):
        """Set progress value."""
        self.progress[key] = value
    
    def is_meeting_completed(self, meeting_key: int) -> bool:
        """Check if a meeting is fully completed (meeting + sessions + data)."""
        meeting_progress = self.progress['completed_meetings'].get(str(meeting_key), {})
        return (meeting_progress.get('meeting_fetched', False) and 
                meeting_progress.get('sessions_fetched', False) and 
                meeting_progress.get('data_fetched', False))
    
    def is_meeting_fetched(self, meeting_key: int) -> bool:
        """Check if meeting data is fetched."""
        meeting_progress = self.progress['completed_meetings'].get(str(meeting_key), {})
        return meeting_progress.get('meeting_fetched', False)
    
    def is_sessions_fetched(self, meeting_key: int) -> bool:
        """Check if sessions for meeting are fetched."""
        meeting_progress = self.progress['completed_meetings'].get(str(meeting_key), {})
        return meeting_progress.get('sessions_fetched', False)
    
    def is_meeting_data_fetched(self, meeting_key: int) -> bool:
        """Check if meeting data (intervals, laps, etc.) is fetched."""
        meeting_progress = self.progress['completed_meetings'].get(str(meeting_key), {})
        return meeting_progress.get('data_fetched', False)
    
    def mark_meeting_fetched(self, meeting_key: int):
        """Mark meeting as fetched."""
        meeting_key_str = str(meeting_key)
        if meeting_key_str not in self.progress['completed_meetings']:
            self.progress['completed_meetings'][meeting_key_str] = {}
        self.progress['completed_meetings'][meeting_key_str]['meeting_fetched'] = True
    
    def mark_sessions_fetched(self, meeting_key: int):
        """Mark sessions for meeting as fetched."""
        meeting_key_str = str(meeting_key)
        if meeting_key_str not in self.progress['completed_meetings']:
            self.progress['completed_meetings'][meeting_key_str] = {}
        self.progress['completed_meetings'][meeting_key_str]['sessions_fetched'] = True
    
    def mark_meeting_data_fetched(self, meeting_key: int):
        """Mark meeting data as fetched."""
        meeting_key_str = str(meeting_key)
        if meeting_key_str not in self.progress['completed_meetings']:
            self.progress['completed_meetings'][meeting_key_str] = {}
        self.progress['completed_meetings'][meeting_key_str]['data_fetched'] = True
    
    def is_session_data_fetched(self, session_key: int) -> bool:
        """Check if session-specific data (car_data, location) is fetched."""
        session_progress = self.progress['completed_sessions'].get(str(session_key), {})
        return session_progress.get('data_fetched', False)
    
    def mark_session_data_fetched(self, session_key: int, meeting_key: int):
        """Mark session data as fetched."""
        session_key_str = str(session_key)
        self.progress['completed_sessions'][session_key_str] = {
            'meeting_key': meeting_key,
            'data_fetched': True
        }
    
    def get_meetings_to_process(self, all_meetings: List[Dict]) -> List[Dict]:
        """Get list of meetings that need processing based on current filter."""
        # Apply meetings filter first
        if self.meetings_filter is None:
            filtered_meetings = all_meetings
        elif isinstance(self.meetings_filter, int):
            if 0 <= self.meetings_filter < len(all_meetings):
                filtered_meetings = [all_meetings[self.meetings_filter]]
            else:
                logger.warning(f"Meeting index {self.meetings_filter} is out of range (0-{len(all_meetings)-1})")
                filtered_meetings = []
        elif isinstance(self.meetings_filter, list):
            filtered_meetings = []
            for idx in self.meetings_filter:
                if 0 <= idx < len(all_meetings):
                    filtered_meetings.append(all_meetings[idx])
                else:
                    logger.warning(f"Meeting index {idx} is out of range (0-{len(all_meetings)-1})")
        elif isinstance(self.meetings_filter, slice):
            filtered_meetings = all_meetings[self.meetings_filter]
        else:
            logger.warning(f"Invalid meetings filter type: {type(self.meetings_filter)}")
            filtered_meetings = all_meetings
        
        return filtered_meetings
    
    def get_progress_summary(self) -> str:
        """Get a summary of current progress."""
        completed_meetings = len([m for m in self.progress['completed_meetings'].values() 
                                if m.get('meeting_fetched', False) and m.get('sessions_fetched', False) and m.get('data_fetched', False)])
        total_sessions = len(self.progress['completed_sessions'])
        
        return f"Progress: {completed_meetings} meetings fully completed, {total_sessions} sessions processed"
