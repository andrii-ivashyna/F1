"""
F1 Data Dashboard - Main Application Entry Point
"""

import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / 'src'))

from dashboard.dashboard import F1Dashboard

def main():
    """Main function to run the F1 Dashboard"""
    dashboard = F1Dashboard("f1db_YR=2024")
    dashboard.run(debug=True, port=8050)

if __name__ == "__main__":
    main()
