#!/usr/bin/env python3
"""
F1 2024 Race Results Dashboard
=============================
A streamlined F1 data visualization tool for race results analysis.
This script serves as the main entry point to generate various plots.
"""

import os
import sys
from pathlib import Path
import traceback

# Add the current directory to Python path for local imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from race_result_plot import F1RaceResultPlotter

def main():
    """Main application entry point."""
    
    # --- Configuration ---
    db_name = 'f1db_YR=2024'
    data_dir = 'data'
    verbose = True
    db_path = Path(data_dir) / db_name / 'database.db'
    
    print("🏎️  F1 2024 Race Results Dashboard")
    print("=" * 50)
    print(f"Database: {db_path}")
    
    if not db_path.exists():
        print(f"\n❌ Database not found at: {db_path}")
        return False
    
    try:
        # --- Plot Generation ---
        # This section can be expanded to call different plot generators.
        # For now, we generate the Position vs. Grand Prix plot.
        
        print(f"\n📊 Generating 'Position vs Grand Prix' Plot...")
        plotter = F1RaceResultPlotter()
        saved_plot_path = plotter.generate_and_save_plot(str(db_path), db_name)
        
        # --- Future Plotting Calls Would Go Here ---
        # For example:
        # print("\n📊 Generating 'Team Performance' Plot...")
        # team_plotter = F1TeamPlotter()
        # team_plotter.generate_plot(...)
        
        if saved_plot_path:
            print(f"\n✅ Dashboard generation completed successfully!")
            return True
        else:
            print(f"\n❌ Dashboard generation failed.")
            return False
        
    except Exception as e:
        print(f"❌ A critical error occurred in main: {e}")
        if verbose:
            print("Full traceback:")
            traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
