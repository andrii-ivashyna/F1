"""
Progress tracking for OpenF1 API Data Fetcher
"""

import json
import os
import logging
from typing import Dict
from .config import DATA_FOLDER, PROGRESS_FILENAME

logger = logging.getLogger(__name__)

class ProgressManager:
    def __init__(self):
        self.progress_path = os.path.join(DATA_FOLDER, PROGRESS_FILENAME)
        self.progress = self.load_progress()
    
    def load_progress(self) -> Dict:
        """Load fetch progress from file."""
        if os.path.exists(self.progress_path):
            try:
                with open(self.progress_path, 'r') as f:
                    progress = json.load(f)
                    logger.info(f"Loaded progress: {progress}")
                    return progress
            except Exception as e:
                logger.warning(f"Could not load progress file: {e}")
        
        return {
            'meetings_completed': False,
            'sessions_completed': False,
            'completed_meetings': [],
            'completed_sessions': [],
            'current_step': 'meetings'
        }
    
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
    
    def add_completed_meeting(self, meeting_key: int):
        """Add meeting to completed list."""
        if 'completed_meetings' not in self.progress:
            self.progress['completed_meetings'] = []
        if meeting_key not in self.progress['completed_meetings']:
            self.progress['completed_meetings'].append(meeting_key)
    
    def add_completed_session(self, session_key: int):
        """Add session to completed list."""
        if 'completed_sessions' not in self.progress:
            self.progress['completed_sessions'] = []
        if session_key not in self.progress['completed_sessions']:
            self.progress['completed_sessions'].append(session_key)
