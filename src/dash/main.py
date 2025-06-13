"""
F1 Data Dashboard - Main Application Entry Point
"""

import sys
from pathlib import Path

# Add the 'src' directory to the Python path
file_path = Path(__file__).resolve()
src_path = file_path.parent.parent
sys.path.append(str(src_path))

from dashboard import F1Dashboard

def main():
    """Main function to run the F1 Dashboard."""
    dashboard = F1Dashboard("f1db_YR=2024")
    dashboard.run(debug=True, port=8050)

if __name__ == "__main__":
    main()
