#!/usr/bin/env python3
"""
F1 2024 Lap Time Analysis
==========================================
A visualization tool for F1 lap time data showing driver lap time distributions
with tyre compound markers and team colors for each Grand Prix.
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import re
import matplotlib.patches as patches
import shutil

class F1LapTimePlotter:
    """
    A class to load, process, and plot F1 lap time data with tyre compounds,
    showing lap time distributions using markers styled like F1 visualization.
    """

    def __init__(self):
        """Initializes the plotter with F1-style theme and data loading components."""
        self.f1_colors = {
            'background': "#F1F1F1", 'grid': '#E0E0E0', 'text': "#101010",
            'accent': '#FF1801', 'secondary': '#F5F5F5', 'blue': "#0055AA"
        }
        
        # Tyre compound colors
        self.compound_colors = {
            'SOFT': '#FF1801',      # Red
            'MEDIUM': '#FFD700',    # Gold/Yellow
            'HARD': '#F5F5F5',     # White
            'INTERMEDIATE': '#00AA00', # Green
            'WET': '#0055AA',       # Blue
            'TEST_UNKNOWN': '#333333', # Dark Grey
            'UNKNOWN': '#808080'    # Grey
        }
        
        self._setup_f1_style()
        
        self.db_path = None
        self.connection_pool = {}
        self.lock = threading.Lock()

    def _setup_f1_style(self):
        """Configure matplotlib for a consistent F1-style plot theme."""
        plt.style.use('default')
        sns.set_context("notebook", font_scale=1.5)
        plt.rcParams.update({
            'figure.facecolor': self.f1_colors['background'], 
            'axes.facecolor': self.f1_colors['background'],
            'axes.edgecolor': self.f1_colors['text'],
            'axes.labelcolor': self.f1_colors['text'],
            'axes.spines.left': True, 'axes.spines.bottom': True, 
            'axes.spines.top': False, 'axes.spines.right': True,
            'axes.grid': False,
            'text.color': self.f1_colors['text'], 
            'xtick.color': self.f1_colors['text'], 
            'ytick.color': self.f1_colors['text'],
            'legend.facecolor': self.f1_colors['background'], 
            'legend.edgecolor': 'none',
            'font.family': 'monospace', 'font.weight': 'bold', 'font.size': 20
        })

    def _get_connection(self, thread_id: int) -> sqlite3.Connection:
        """Get or create a database connection for the current thread."""
        if thread_id not in self.connection_pool:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            self.connection_pool[thread_id] = conn
        return self.connection_pool[thread_id]

    def _execute_query(self, query: str, thread_id: int) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame."""
        return pd.read_sql_query(query, self._get_connection(thread_id))

    def _format_lap_time(self, seconds: float) -> str:
        """Format lap time in seconds to 'm:ss.sss' format."""
        if pd.isna(seconds) or seconds <= 0:
            return "N/A"
        
        minutes = int(seconds // 60)
        remaining_seconds = seconds % 60
        return f"{minutes}:{remaining_seconds:06.3f}"

    def _parse_lap_time_to_seconds(self, time_str: str) -> float:
        """Parse lap time string to seconds."""
        if pd.isna(time_str) or time_str in ['', 'N/A']:
            return np.nan
        
        try:
            # Handle different time formats
            time_str = str(time_str).strip()
            
            # If already in seconds format
            if ':' not in time_str:
                return float(time_str)
            
            # If in m:ss.sss format
            if ':' in time_str:
                parts = time_str.split(':')
                minutes = int(parts[0])
                seconds = float(parts[1])
                return minutes * 60 + seconds
                
        except (ValueError, IndexError):
            return np.nan
        
        return np.nan

    def _load_lap_time_data(self, session_key: int) -> Dict:
        """Load all lap time data for a specific session."""
        print(f"🏎️  Loading lap time data for session {session_key}...")
        
        thread_id = threading.get_ident()
        
        # Load drivers data
        drivers_query = f"""
            SELECT DISTINCT driver_number, name_acronym, full_name, team_name, team_colour
            FROM drivers 
            WHERE session_key = {session_key}
            ORDER BY driver_number
        """
        drivers = self._execute_query(drivers_query, thread_id)
        
        # Load lap times
        laps_query = f"""
            SELECT l.driver_number, l.lap_number, l.lap_duration, l.is_pit_out_lap
            FROM laps l
            WHERE l.session_key = {session_key}
            AND l.lap_duration IS NOT NULL
            AND l.lap_duration > 0
            ORDER BY l.driver_number, l.lap_number
        """
        laps = self._execute_query(laps_query, thread_id)
        
        # Load tyre stint data
        stints_query = f"""
            SELECT driver_number, compound, lap_start, lap_end, stint_number
            FROM stints 
            WHERE session_key = {session_key}
            ORDER BY driver_number, stint_number
        """
        stints = self._execute_query(stints_query, thread_id)
        
        # Load pit stop data
        pit_query = f"""
            SELECT driver_number, lap_number, pit_duration
            FROM pit 
            WHERE session_key = {session_key}
            ORDER BY driver_number, lap_number
        """
        pit_stops = self._execute_query(pit_query, thread_id)
        
        return {
            'drivers': drivers,
            'laps': laps,
            'stints': stints,
            'pit_stops': pit_stops
        }

    def _get_driver_info(self, drivers: pd.DataFrame) -> Dict:
        """Create driver information dictionary."""
        driver_info = {}
        
        for _, row in drivers.iterrows():
            driver_num = row['driver_number']
            
            # Determine line style based on driver number within team
            team_drivers = drivers[drivers['team_name'] == row['team_name']]['driver_number'].tolist()
            
            # Team color
            team_color = str(row['team_colour']).strip()
            if not team_color.startswith('#'):
                team_color = f"#{team_color}"
            if len(team_color) != 7:
                team_color = '#000000'
            
            driver_info[driver_num] = {
                'acronym': row['name_acronym'],
                'full_name': row['full_name'],
                'team_name': row['team_name'],
                'team_color': team_color
            }
        
        return driver_info

    def _get_lap_compound(self, lap_number: int, driver_stints: pd.DataFrame) -> str:
        """Get the tyre compound for a specific lap."""
        # Process stint data to handle overlapping laps
        # Sort by stint number to ensure priority for first stint
        driver_stints = driver_stints.sort_values('stint_number')
        
        for _, stint in driver_stints.iterrows():
            lap_start = stint['lap_start']
            lap_end = stint['lap_end']
            
            # Handle NaN values - if lap_start or lap_end is NaN, skip
            if pd.isna(lap_start) or pd.isna(lap_end):
                continue
            
            if int(lap_start) <= lap_number <= int(lap_end):
                compound = stint['compound'].upper() if pd.notna(stint['compound']) else 'UNKNOWN'
                return compound
        
        return 'UNKNOWN'

    def _analyze_pit_stops(self, pit_stops: pd.DataFrame, driver_info: Dict) -> Dict:
        """Analyze pit stop statistics."""
        pit_analysis = {
            'total_pit_stops': len(pit_stops),
            'average_pit_stop': 0,
            'fastest_pitstop_driver': None,
            'fastest_pitstop_time': float('inf')
        }
        
        if pit_stops.empty:
            return pit_analysis
        
        # Calculate average pit stop time
        pit_analysis['average_pit_stop'] = pit_stops['pit_duration'].mean()
        
        # Find fastest pit stop
        fastest_pit = pit_stops.loc[pit_stops['pit_duration'].idxmin()]
        pit_analysis['fastest_pitstop_driver'] = driver_info[fastest_pit['driver_number']]['acronym']
        pit_analysis['fastest_pitstop_time'] = fastest_pit['pit_duration']
        
        return pit_analysis

    def _plot_lap_time_distribution(self, ax, driver_info: Dict, laps: pd.DataFrame, stints: pd.DataFrame, pit_stops: pd.DataFrame):
        """Plot lap time distributions for each driver using markers."""
        
        # Calculate positions for each driver
        driver_positions = {}
        x_positions = []
        x_labels = []
        
        # Process each driver's lap times to calculate averages
        all_lap_times = []
        driver_averages = {}
        
        for driver_num in driver_info.keys():
            driver_laps = laps[laps['driver_number'] == driver_num].copy()
            driver_stints = stints[stints['driver_number'] == driver_num].copy()
            
            if driver_laps.empty:
                continue
            
            # Filter out outliers (laps > 2 minutes or < 1 minute typically indicate issues)
            driver_laps = driver_laps[
                (driver_laps['lap_duration'] >= 60) & 
                (driver_laps['lap_duration'] <= 120) &
                (driver_laps['is_pit_out_lap'] != True)  # Exclude pit out laps
            ]
            
            if driver_laps.empty:
                continue
            
            lap_times = driver_laps['lap_duration'].values
            all_lap_times.extend(lap_times)
            driver_averages[driver_num] = np.mean(lap_times)
        
        # Calculate overall average
        overall_average = np.mean(all_lap_times) if all_lap_times else 0
        
        # Calculate differences and sort drivers by fastest (lowest difference)
        driver_differences = {}
        for driver_num in driver_averages:
            driver_differences[driver_num] = driver_averages[driver_num] - overall_average
        
        # Sort drivers by difference (fastest first - lowest difference)
        sorted_drivers = sorted(driver_differences.keys(), key=lambda x: driver_differences[x])
        
        # Find fastest and slowest lap times across all drivers
        fastest_lap_time = min(all_lap_times) if all_lap_times else 0
        slowest_lap_time = max(all_lap_times) if all_lap_times else 0
        fastest_driver = None
        slowest_driver = None
        
        # Find which drivers had fastest and slowest laps
        for driver_num in sorted_drivers:
            driver_laps = laps[laps['driver_number'] == driver_num].copy()
            driver_laps = driver_laps[
                (driver_laps['lap_duration'] >= 60) & 
                (driver_laps['lap_duration'] <= 120) &
                (driver_laps['is_pit_out_lap'] != True)
            ]
            
            if not driver_laps.empty:
                min_lap = driver_laps['lap_duration'].min()
                max_lap = driver_laps['lap_duration'].max()
                
                if min_lap == fastest_lap_time:
                    fastest_driver = driver_info[driver_num]['acronym']
                if max_lap == slowest_lap_time:
                    slowest_driver = driver_info[driver_num]['acronym']
        
        for i, driver_num in enumerate(sorted_drivers):
            driver_positions[driver_num] = i
            x_positions.append(i)
            x_labels.append(driver_info[driver_num]['acronym'])
        
        # Plot overall average line (behind everything else)
        if all_lap_times:
            ax.axhline(y=overall_average, color='#ffbe0b', 
                      linestyle='--', linewidth=3, alpha=0.75, zorder=0)
        
        # Plot vertical lines for each team (only vertical edges)
        team_positions = {}
        for driver_num in sorted_drivers:
            team_name = driver_info[driver_num]['team_name']
            team_color = driver_info[driver_num]['team_color']
            x_pos = driver_positions[driver_num]
            
            if team_name not in team_positions:
                team_positions[team_name] = []
            team_positions[team_name].append(x_pos)
            
            # Get driver's lap times for vertical line range
            driver_laps = laps[laps['driver_number'] == driver_num].copy()
            driver_laps = driver_laps[
                (driver_laps['lap_duration'] >= 60) & 
                (driver_laps['lap_duration'] <= 120) &
                (driver_laps['is_pit_out_lap'] != True)
            ]
            
            if not driver_laps.empty:
                lap_times = driver_laps['lap_duration'].values
                q25 = np.percentile(lap_times, 25)
                q75 = np.percentile(lap_times, 75)
                
                # Draw left vertical line
                ax.plot([x_pos - 0.25, x_pos - 0.25], [q25, q75], 
                       color=team_color, linewidth=4, alpha=0.5, zorder=1)
                
                # Draw right vertical line
                ax.plot([x_pos + 0.25, x_pos + 0.25], [q25, q75], 
                       color=team_color, linewidth=4, alpha=0.5, zorder=1)
        
        # Plot each lap time with appropriate tyre compound color
        for driver_num in sorted_drivers:
            driver_laps = laps[laps['driver_number'] == driver_num].copy()
            driver_stints = stints[stints['driver_number'] == driver_num].copy()
            
            if driver_laps.empty:
                continue
            
            x_pos = driver_positions[driver_num]
            team_color = driver_info[driver_num]['team_color']
            
            # Add some jitter to x position for better visibility
            jitter_range = 0.15
            
            for _, lap_row in driver_laps.iterrows():
                lap_time = lap_row['lap_duration']
                lap_number = lap_row['lap_number']
                
                # Get compound for this lap
                compound = self._get_lap_compound(lap_number, driver_stints)
                compound_color = self.compound_colors.get(compound, self.compound_colors['UNKNOWN'])
                
                # Add jitter to x position
                x_jitter = x_pos + np.random.uniform(-jitter_range, jitter_range)
                
                ax.scatter(x_jitter, lap_time, 
                          color=compound_color, s=140, alpha=0.8, 
                          edgecolors=team_color, linewidth=1.5, zorder=3)
        
        # Add difference text at each driver's average lap time with transparent grey background
        if all_lap_times:
            for driver_num in sorted_drivers:
                if driver_num in driver_averages:
                    x_pos = driver_positions[driver_num]
                    driver_avg = driver_averages[driver_num]
                    difference = driver_differences[driver_num]
                    
                    # Format difference
                    diff_text = f"{difference:+.3f}s"
                    color = self.f1_colors['accent'] if difference > 0 else '#00AA00'
                    
                    # Add text with transparent grey background
                    text = ax.text(x_pos, driver_avg, diff_text, 
                                 ha='center', va='center', 
                                 color=color, fontweight='bold', fontsize=16,
                                 zorder=4)
                    
                    # Add tight transparent grey background
                    text.set_bbox(dict(boxstyle="square,pad=0.1", 
                                     facecolor='white', alpha=0.7, 
                                     edgecolor='none'))
        
        # Configure x-axis
        ax.set_xticks(x_positions)
        ax.set_xticklabels(x_labels, fontsize=20)
        ax.set_xlabel('Driver', fontsize=20, fontweight='bold', labelpad=15)
        
        # Configure y-axis with 1-second intervals
        if all_lap_times:
            y_min = min(all_lap_times)
            y_max = max(all_lap_times)
            
            # Round to nearest second for cleaner ticks
            y_min_rounded = int(np.floor(y_min))
            y_max_rounded = int(np.ceil(y_max))
            
            # Create ticks for every second
            y_ticks = list(range(y_min_rounded, y_max_rounded + 1))
            y_labels = [self._format_lap_time(float(t)) for t in y_ticks]
            
            ax.set_ylim(y_min_rounded - 0.5, y_max_rounded + 0.5)
            ax.set_yticks(y_ticks)
            ax.set_yticklabels(y_labels, fontsize=20)
        
        ax.set_ylabel('Lap Time', fontsize=20, fontweight='bold', labelpad=15)
        
        # Analyze pit stops
        pit_analysis = self._analyze_pit_stops(pit_stops, driver_info)
        
        # Create legends
        # Left legend - Lap time statistics
        legend1_elements = [
            plt.Line2D([0], [0], color='#ffbe0b', linestyle='--', linewidth=3,
                      label=f'Average Lap Time: {self._format_lap_time(overall_average)}'),
            plt.Line2D([0], [0], color='#00AA00', marker='o', linestyle='None', markersize=8,
                      label=f'Fastest Lap Time ({fastest_driver}): {self._format_lap_time(fastest_lap_time)}'),
            plt.Line2D([0], [0], color=self.f1_colors['accent'], marker='o', linestyle='None', markersize=8,
                      label=f'Slowest Lap Time ({slowest_driver}): {self._format_lap_time(slowest_lap_time)}')
        ]
        
        legend1 = ax.legend(handles=legend1_elements, loc='upper left', fontsize=20, title='Lap Time Stats')
        legend1.get_title().set_fontsize(20)
        legend1.get_title().set_fontweight('bold')
        
        # Right legend - Pit stop statistics
        legend2_elements = [
            plt.Line2D([0], [0], color='black', marker='s', linestyle='None', markersize=8,
                      label=f'Total Pit Stops: {pit_analysis["total_pit_stops"]}'),
            plt.Line2D([0], [0], color='#FFD700', marker='o', linestyle='None', markersize=8,
                      label=f'Average Pit Stop: {pit_analysis["average_pit_stop"]:.3f}s'),
            plt.Line2D([0], [0], color='#0055AA', marker='o', linestyle='None', markersize=8,
                      label=f'Fastest Pit Stop ({pit_analysis["fastest_pitstop_driver"]}): {pit_analysis["fastest_pitstop_time"]:.3f}s')
        ]
        
        legend2 = ax.legend(handles=legend2_elements, loc='upper right', fontsize=20, title='Pit Stop Stats')
        legend2.get_title().set_fontsize(20)
        legend2.get_title().set_fontweight('bold')
        
        # Add the first legend back (matplotlib removes previous legend when creating new one)
        ax.add_artist(legend1)

    def generate_lap_time_plot(self, db_path: str, db_name: str, session_key: int, 
                              meeting_name: str, circuit_name: str) -> str:
        """Generate and save a lap time plot for a specific Grand Prix."""
        self.db_path = db_path
        
        try:
            # Load lap time data
            lap_data = self._load_lap_time_data(session_key)
            
            if lap_data['drivers'].empty:
                print(f"❌ No driver data found for session {session_key}")
                return None
            
            if lap_data['laps'].empty:
                print(f"❌ No lap data found for session {session_key}")
                return None
            
            print(f"🎨 Generating lap time plot for {meeting_name}...")
            
            # Process data
            driver_info = self._get_driver_info(lap_data['drivers'])
            
            # Create plot
            fig, ax = plt.subplots(figsize=(24, 16))
            
            # Plot lap time distributions
            self._plot_lap_time_distribution(ax, driver_info, lap_data['laps'], lap_data['stints'], lap_data['pit_stops'])
            
            # Configure plot
            year = 2024  # Assuming 2024 data
            
            # Title
            ax.set_title(f'F1 {year} {meeting_name} - Lap Time Distribution', 
                        fontsize=24, fontweight='bold', pad=20)
            
            # Layout and save
            plt.tight_layout()
            
            # Create save directory
            lap_time_dir = Path("data") / db_name / "dashboard" / "race_lap_time"
            lap_time_dir.mkdir(parents=True, exist_ok=True)
            
            # Clean meeting name for filename
            clean_name = re.sub(r'[^\w\s-]', '', meeting_name).strip()
            clean_name = re.sub(r'[-\s]+', '_', clean_name)
            
            save_path = lap_time_dir / f"F1_{year}_{clean_name}_Lap_Times.png"
            
            plt.savefig(save_path, dpi=300, bbox_inches='tight', 
                       facecolor=self.f1_colors['background'])
            plt.close(fig)
            
            print(f"✅ Plot saved to: {save_path}")
            return str(save_path)
            
        except Exception as e:
            print(f"❌ Error generating lap time plot: {e}")
            import traceback
            traceback.print_exc()
            return None
        finally:
            self._close_connections()

    def generate_all_lap_time_plots(self, db_path: str, db_name: str):
        """Generate lap time plots for all Grand Prix in the season."""
        self.db_path = db_path
        
        try:
            # Delete everything in output folder before starting
            lap_time_dir = Path("data") / db_name / "dashboard" / "race_lap_time"
            if lap_time_dir.exists():
                shutil.rmtree(lap_time_dir)
                print(f"🗑️  Cleared output directory: {lap_time_dir}")
            
            print("🏁 Loading race sessions...")
            
            # Get all race sessions
            q_sessions = """
                SELECT s.session_key, s.circuit_short_name, s.date_start, m.meeting_name
                FROM sessions s 
                JOIN meetings m ON s.meeting_key = m.meeting_key
                WHERE s.session_name = 'Race' 
                ORDER BY s.date_start
            """
            race_sessions = self._execute_query(q_sessions, threading.get_ident())
            
            if race_sessions.empty:
                print("❌ No race sessions found!")
                return []
            
            print(f"📊 Found {len(race_sessions)} race sessions")
            
            saved_plots = []
            
            # Generate plot for each race
            for _, session in race_sessions.iterrows():
                plot_path = self.generate_lap_time_plot(
                    db_path, db_name, 
                    session['session_key'],
                    session['meeting_name'],
                    session['circuit_short_name']
                )
                if plot_path:
                    saved_plots.append(plot_path)
            
            print(f"\n✅ Generated {len(saved_plots)} lap time plots successfully!")
            return saved_plots
            
        except Exception as e:
            print(f"❌ Error generating lap time plots: {e}")
            import traceback
            traceback.print_exc()
            return []
        finally:
            self._close_connections()

    def _close_connections(self):
        """Close all database connections."""
        for conn in self.connection_pool.values():
            conn.close()
        self.connection_pool.clear()


def main():
    """Main function to run the lap time plotter."""
    # Configuration
    db_name = 'f1db_YR=2024'
    data_dir = 'data'
    db_path = Path(data_dir) / db_name / 'database.db'
    
    print("🏎️  F1 2024 Lap Time Analysis")
    print("=" * 60)
    print(f"Database: {db_path}")
    
    if not db_path.exists():
        print(f"\n❌ Database not found at: {db_path}")
        return False
    
    try:
        plotter = F1LapTimePlotter()
        saved_plots = plotter.generate_all_lap_time_plots(str(db_path), db_name)
        
        if saved_plots:
            print(f"\n🎉 Successfully generated {len(saved_plots)} lap time plots!")
            return True
        else:
            print(f"\n❌ Failed to generate lap time plots")
            return False
            
    except Exception as e:
        print(f"❌ Critical error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
