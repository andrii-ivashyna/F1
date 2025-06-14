#!/usr/bin/env python3
"""
F1 2024 Race Results Dashboard
=============================
This script serves as the main entry point to generate various plots.
"""

import os
import sys
from pathlib import Path
import traceback

# Add the current directory to Python path for local imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dashboard.position_plot import F1RaceResultPlotter
from lap_pit_plot import F1PitStopPlotter

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
        success_count = 0
        total_plots = 2
        
        # Generate Position vs Grand Prix plot
        print(f"\n📊 Generating 'Position vs Grand Prix' Plot...")
        plotter = F1RaceResultPlotter()
        saved_plot_path = plotter.generate_and_save_plot(str(db_path), db_name)
        if saved_plot_path:
            success_count += 1
        
        # Generate Pit Stop Analysis plot
        print(f"\n📊 Generating 'Laps vs Pit Stops' Plot...")
        pit_plotter = F1PitStopPlotter()
        pit_plot_path = pit_plotter.generate_and_save_plot(str(db_path), db_name)
        if pit_plot_path:
            success_count += 1
        
        # --- Future Plotting Calls Would Go Here ---
        # For example:
        # print("\n📊 Generating 'Team Performance' Plot...")
        # team_plotter = F1TeamPlotter()
        # team_plotter.generate_plot(...)
        
        if success_count == total_plots:
            print(f"\n✅ Dashboard generation completed successfully! ({success_count}/{total_plots} plots generated)")
            return True
        elif success_count > 0:
            print(f"\n⚠️  Dashboard generation partially completed. ({success_count}/{total_plots} plots generated)")
            return True
        else:
            print(f"\n❌ Dashboard generation failed. No plots were generated.")
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
