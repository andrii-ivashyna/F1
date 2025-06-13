"""Data loading and caching functionality"""

import json
import os
from typing import Dict, List, Any

class DataLoader:
    """Handles data loading and caching for the dashboard"""
    
    def __init__(self, analysis_path: str):
        self.analysis_path = analysis_path
        self.cache = {}
        self.summary = self._load_summary()
    
    def _load_summary(self) -> Dict[str, Any]:
        """Load analysis summary with error handling"""
        summary_path = os.path.join(self.analysis_path, "analysis_summary.json")
        try:
            with open(summary_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {"data": {"files_generated": []}}
    
    def load_data(self, filename: str) -> Dict[str, Any]:
        """Load and cache JSON data"""
        if filename in self.cache:
            return self.cache[filename]
        
        file_path = os.path.join(self.analysis_path, filename)
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                self.cache[filename] = data
                return data
        except FileNotFoundError:
            return {"error": f"File not found: {filename}"}
    
    def get_file_list(self) -> List[str]:
        """Get list of generated files"""
        return self.summary.get("data", {}).get("files_generated", [])
