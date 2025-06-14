#!/usr/bin/env python3
"""
F1 2024 Race Results Dashboard
=============================
A streamlined F1 data visualization tool for race results analysis.
"""

import os
from pathlib import Path

# Add the current directory to Python path for imports
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_loader import F1DataLoader
from race_result_plot import F1Plotter


def main():
    """Main application entry point."""
    
    # Configuration - hardcoded values
    db_name = 'f1db_YR=2024'
    data_dir = 'data'
    verbose = True
    
    # Build database path
    db_path = Path(data_dir) / db_name / 'database.db'
    
    print("🏎️  F1 2024 Race Results Dashboard")
    print("=" * 50)
    print(f"Database: {db_path}")
    print(f"Plot Type: Position vs Grand Prix")
    print(f"Season: 2024")
    print(f"Session Filter: Race only")
    print(f"Save Location: {data_dir}/{db_name}/dashboard/")
    
    # Check if database exists
    if not db_path.exists():
        print(f"\n❌ Database not found at: {db_path}")
        print("Please ensure the database file exists in the correct location.")
        return False
    
    try:
        # Load data using optimized loader
        print(f"\n🚀 Loading F1 race data...")
        
        with F1DataLoader(str(db_path)) as loader:
            race_data = loader.load_race_results_data()
        
        if race_data.empty:
            print("❌ No race data found! Please check your database.")
            return False
        
        # Create plotter and generate visualization
        plotter = F1Plotter()
        
        # Show data summary if verbose
        if verbose:
            plotter.get_plot_summary(race_data)
        
        # Generate and save the plot
        print(f"\n🎨 Generating F1-style visualization...")
        saved_plot_path = plotter.plot_position_vs_grandprix(race_data, db_name)
        
        print(f"\n✅ Dashboard completed successfully!")
        
        # Show final statistics
        print(f"\n📈 Final Statistics:")
        print(f"   • {len(race_data)} race results processed")
        print(f"   • {race_data['meeting_name'].nunique()} races analyzed")
        print(f"   • {race_data['name_acronym'].nunique()} drivers tracked")
        print(f"   • {race_data['team_name'].nunique()} teams covered")
        print(f"   • Plot saved to: {saved_plot_path}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error occurred: {e}")
        if verbose:
            import traceback
            print("Full traceback:")
            traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
