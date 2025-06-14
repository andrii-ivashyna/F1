#!/usr/bin/env python3
"""
F1 2024 Race Analysis
==========================================
A visualization tool for F1 race data showing driver positions, tyre compounds, 
and race control events (DRS, SC, VSC, RF, SS, CF) for each Grand Prix.
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

class F1RacePlotter:
    """
    A class to load, process, and plot F1 race data with tyre compounds,
    race control events, and driver positions using F1-style visualization.
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
        
        # Race control event colors
        self.event_colors = {
            'DRS_ENABLED': '#00AA00',   # Green
            'DRS_DISABLED': '#FF1801',  # Red
            'SAFETY_CAR': '#FFD700',    # Yellow
            'VIRTUAL_SAFETY_CAR': '#FFD700', # Yellow
            'RED_FLAG': '#FF1801',      # Red
            'START': '#00AA00', # Green
            'CHEQUERED_FLAG': '#000000'  # Black
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

    def _fetch_data_concurrently(self, task_function, items, desc):
        """Helper to fetch data in parallel using multithreading."""
        dfs = []
        batch_size = max(1, len(items) // 4)
        batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]
        with ThreadPoolExecutor(max_workers=8) as executor:
            with tqdm(desc=desc, total=len(batches), unit="batch") as pbar:
                futures = {executor.submit(task_function, batch, i): batch for i, batch in enumerate(batches)}
                for future in as_completed(futures):
                    dfs.append(future.result())
                    pbar.update(1)
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def _load_race_data(self, session_key: int) -> Dict:
        """Load all race data for a specific session."""
        print(f"🏎️  Loading race data for session {session_key}...")
        
        thread_id = threading.get_ident()
        
        # Load drivers data
        drivers_query = f"""
            SELECT DISTINCT driver_number, name_acronym, full_name, team_name, team_colour
            FROM drivers 
            WHERE session_key = {session_key}
            ORDER BY driver_number
        """
        drivers = self._execute_query(drivers_query, thread_id)
        
        # Load final positions
        positions_query = f"""
            SELECT p.driver_number, p.position, p.date
            FROM position p
            WHERE p.session_key = {session_key}
            AND p.position IS NOT NULL
            AND p.position <= 20
            ORDER BY p.driver_number, p.date DESC
        """
        positions = self._execute_query(positions_query, thread_id)
        
        # Get final positions (latest timestamp for each driver)
        final_positions = positions.loc[positions.groupby('driver_number')['date'].idxmax()]
        
        # Load tyre stint data
        stints_query = f"""
            SELECT driver_number, compound, lap_start, lap_end, stint_number
            FROM stints 
            WHERE session_key = {session_key}
            ORDER BY driver_number, stint_number
        """
        stints = self._execute_query(stints_query, thread_id)
        
        # Load race control events
        race_control_query = f"""
            SELECT lap_number, message, date, driver_number, category, flag
            FROM race_control 
            WHERE session_key = {session_key}
            ORDER BY date
        """
        race_control = self._execute_query(race_control_query, thread_id)
        
        # Load lap data to get total laps
        laps_query = f"""
            SELECT MAX(lap_number) as total_laps
            FROM laps 
            WHERE session_key = {session_key}
        """
        laps_info = self._execute_query(laps_query, thread_id)
        total_laps = laps_info['total_laps'].iloc[0] if not laps_info.empty else 50
        
        return {
            'drivers': drivers,
            'final_positions': final_positions,
            'stints': stints,
            'race_control': race_control,
            'total_laps': total_laps
        }

    def _process_race_control_events(self, race_control: pd.DataFrame, total_laps: int) -> Dict:
        """Process race control events and return structured event data."""
        events = {
            'drs': [],
            'safety_car': [],
            'virtual_safety_car': [],
            'red_flag': [],
            'start': [],
            'chequered_flag': []
        }
        
        # Track active states
        sc_active = False
        vsc_active = False
        sc_start_lap = None
        vsc_start_lap = None
        
        for _, row in race_control.iterrows():
            message = str(row['message']).strip()
            lap_num = row['lap_number'] if pd.notna(row['lap_number']) else 1
            
            # DRS Events - exact match
            if message == 'DRS ENABLED':
                events['drs'].append({'lap': lap_num, 'type': 'ENABLED'})
            elif message == 'DRS DISABLED':
                events['drs'].append({'lap': lap_num, 'type': 'DISABLED'})
            
            # Safety Car Events - exact match
            elif message == 'SAFETY CAR DEPLOYED':
                events['safety_car'].append({'lap': lap_num, 'type': 'DEPLOYED'})
                sc_active = True
                sc_start_lap = lap_num
                vsc_active = False  # SC overrides VSC
            elif message == 'SAFETY CAR IN THIS LAP':
                events['safety_car'].append({'lap': lap_num, 'type': 'ENDING'})
                sc_active = False
                sc_start_lap = None
            
            # Virtual Safety Car Events - exact match
            elif message == 'VIRTUAL SAFETY CAR DEPLOYED' and not sc_active:
                events['virtual_safety_car'].append({'lap': lap_num, 'type': 'DEPLOYED'})
                vsc_active = True
                vsc_start_lap = lap_num
            elif message == 'VIRTUAL SAFETY CAR ENDING':
                events['virtual_safety_car'].append({'lap': lap_num, 'type': 'ENDING'})
                vsc_active = False
                vsc_start_lap = None
            
            # Red Flag Events - exact match
            elif message == 'RED FLAG':
                # If SC or VSC is active, they end at the RF lap
                if sc_active:
                    events['safety_car'].append({'lap': lap_num, 'type': 'ENDING'})
                    sc_active = False
                if vsc_active:
                    events['virtual_safety_car'].append({'lap': lap_num, 'type': 'ENDING'})
                    vsc_active = False
                
                events['red_flag'].append({'lap': lap_num, 'type': 'DEPLOYED'})
            
            # Start Events - exact match
            elif message == 'STANDING START' or message == 'STANDING START PROCEDURE' or message == 'ROLLING START':
                # SS starts on the lap it's announced
                events['start'].append({'lap': lap_num, 'type': 'START'})
            
            # Chequered Flag Events - exact match
            elif message == 'CHEQUERED FLAG':
                events['chequered_flag'].append({'lap': lap_num, 'type': 'FINISH'})
        
        return events

    def _get_driver_grid_info(self, drivers: pd.DataFrame, final_positions: pd.DataFrame) -> Dict:
        """Create driver grid information with starting and finishing positions."""
        # Merge drivers with final positions
        driver_info = pd.merge(drivers, final_positions, on='driver_number', how='left')
        
        # Sort by final position (or driver number if no position)
        driver_info = driver_info.sort_values(['position', 'driver_number'])
        
        grid_info = {}
        for idx, row in driver_info.iterrows():
            driver_num = row['driver_number']
            pos = row['position'] if pd.notna(row['position']) else 20
            
            # Determine line style based on driver number within team
            team_drivers = driver_info[driver_info['team_name'] == row['team_name']]['driver_number'].tolist()
            line_style = '-' if driver_num == min(team_drivers) else '--'
            
            # Team color
            team_color = str(row['team_colour']).strip()
            if not team_color.startswith('#'):
                team_color = f"#{team_color}"
            if len(team_color) != 7:
                team_color = '#000000'
            
            grid_info[driver_num] = {
                'acronym': row['name_acronym'],
                'start_pos': pos,  # For now, same as finish position
                'finish_pos': pos,
                'team_color': team_color,
                'line_style': line_style
            }
        
        return grid_info

    def _create_checkered_pattern(self, ax, start_lap, end_lap, y_min, y_max):
        """Create a checkered pattern background for chequered flag periods."""
        # Create checkered pattern
        square_size = 0.2  # Size of each square
        
        # Calculate number of squares needed
        lap_width = end_lap - start_lap
        height = y_max - y_min
        
        num_x_squares = max(1, int(lap_width / square_size))
        num_y_squares = max(1, int(height / square_size))
        
        for i in range(num_x_squares):
            for j in range(num_y_squares):
                x_start = start_lap + i * square_size
                x_end = min(start_lap + (i + 1) * square_size, end_lap)
                y_start = y_min + j * square_size
                y_end = min(y_min + (j + 1) * square_size, y_max)
                
                # Alternate colors based on position
                color = 'black' if (i + j) % 2 == 0 else 'white'
                
                rect = patches.Rectangle((x_start, y_start), x_end - x_start, y_end - y_start,
                                       facecolor=color, alpha=0.5, zorder=1)
                ax.add_patch(rect)

    def _plot_race_events(self, ax, events: Dict, total_laps: int, grid_info: Dict):
        """Plot race control events on the chart."""
        # Get the first position for DRS placement
        first_position = min(info['start_pos'] for info in grid_info.values())
        y_top = first_position - 0.3  # Position DRS text above the first line
        
        # Track active background states and their ranges
        sc_ranges = []
        vsc_ranges = []
        rf_ranges = []
        ss_ranges = []
        cf_ranges = []
        
        # Process Safety Car background
        sc_start = None
        for event in events['safety_car']:
            if event['type'] == 'DEPLOYED':
                sc_start = event['lap']
            elif event['type'] == 'ENDING' and sc_start is not None:
                sc_ranges.append((sc_start, event['lap']))
                sc_start = None
        
        # Process Virtual Safety Car background
        vsc_start = None
        for event in events['virtual_safety_car']:
            if event['type'] == 'DEPLOYED':
                vsc_start = event['lap']
            elif event['type'] == 'ENDING' and vsc_start is not None:
                vsc_ranges.append((vsc_start, event['lap']))
                vsc_start = None
        
        # Process Red Flag background - from lap announced to lap+1
        for event in events['red_flag']:
            if event['type'] == 'DEPLOYED':
                rf_ranges.append((event['lap'], event['lap'] + 1))
        
        # Process Standing Start background - from lap announced to lap+1
        for event in events['start']:
            if event['type'] == 'START':
                ss_ranges.append((event['lap'], event['lap'] + 1))
        
        # Process Chequered Flag background - from lap announced to lap+1
        for event in events['chequered_flag']:
            if event['type'] == 'FINISH':
                cf_ranges.append((event['lap'], event['lap'] + 1))
        
        # Draw background ranges (layer 1)
        # Safety Car ranges
        for start_lap, end_lap in sc_ranges:
            # Check if this SC range is interrupted by RF
            interrupted_by_rf = False
            for rf_start, rf_end in rf_ranges:
                if rf_start <= end_lap and rf_start >= start_lap:
                    # SC ends at RF start, then RF takes over
                    if rf_start > start_lap:
                        ax.axvspan(start_lap, rf_start, alpha=0.75, color=self.event_colors['SAFETY_CAR'], zorder=1)
                    interrupted_by_rf = True
                    break
            
            if not interrupted_by_rf:
                ax.axvspan(start_lap, end_lap, alpha=0.75, color=self.event_colors['SAFETY_CAR'], zorder=1)
        
        # Virtual Safety Car ranges
        for start_lap, end_lap in vsc_ranges:
            # Check if this VSC range overlaps with SC or RF
            overlap_with_sc = any(start_lap < sc_end and end_lap > sc_start for sc_start, sc_end in sc_ranges)
            overlap_with_rf = any(start_lap < rf_end and end_lap > rf_start for rf_start, rf_end in rf_ranges)
            
            if not overlap_with_sc and not overlap_with_rf:
                ax.axvspan(start_lap, end_lap, alpha=0.25, color=self.event_colors['VIRTUAL_SAFETY_CAR'], zorder=1)
        
        # Red Flag ranges
        for start_lap, end_lap in rf_ranges:
            ax.axvspan(start_lap, end_lap, alpha=0.75, color=self.event_colors['RED_FLAG'], zorder=1)
        
        # Start ranges
        for start_lap, end_lap in ss_ranges:
            ax.axvspan(start_lap, end_lap, alpha=0.5, color=self.event_colors['START'], zorder=1)
        
        # Chequered Flag ranges - checkered pattern
        for start_lap, end_lap in cf_ranges:
            self._create_checkered_pattern(ax, start_lap, end_lap, 0.5, 20.5)
        
        # Plot DRS events (only text, moved to top)
        for event in events['drs']:
            color = self.event_colors['DRS_ENABLED'] if event['type'] == 'ENABLED' else self.event_colors['DRS_DISABLED']
            ax.text(event['lap'], y_top, 'DRS', ha='center', va='bottom', 
                   color=color, fontweight='bold', fontsize=20)

    def _plot_driver_lines(self, ax, grid_info: Dict, stints: pd.DataFrame, total_laps: int):
        """Plot driver position lines with tyre compound markers."""
        
        for driver_num, info in grid_info.items():
            # Create straight line from start to finish position
            x_data = list(range(1, total_laps + 2))  # +1 for the extra lap
            y_data = [info['start_pos']] * len(x_data)
            
            # Get driver's stint data
            driver_stints = stints[stints['driver_number'] == driver_num].copy()
            
            # Plot base line (layer 2 - driver lines)
            ax.plot(x_data, y_data, color=info['team_color'], 
                   linestyle=info['line_style'], linewidth=30, alpha=0.7, zorder=2)
            
            # Check if driver has stint data
            if driver_stints.empty:
                continue
            
            # Process stint data to handle overlapping laps
            # Sort by stint number to ensure priority for first stint
            driver_stints = driver_stints.sort_values('stint_number')
            
            # Create a lap-to-compound mapping with priority for earlier stints
            lap_compounds = {}
            
            for _, stint in driver_stints.iterrows():
                compound = stint['compound'].upper() if pd.notna(stint['compound']) else 'UNKNOWN'
                lap_start = stint['lap_start']
                lap_end = stint['lap_end']
                
                # Handle NaN values - if lap_start or lap_end is NaN, set to 1
                if pd.isna(lap_start):
                    lap_start = 1
                if pd.isna(lap_end):
                    lap_end = 1
                
                # Assign compound to laps, but don't overwrite if already assigned (priority to first stint)
                for lap in range(int(lap_start), int(lap_end) + 1):
                    if lap not in lap_compounds:
                        lap_compounds[lap] = compound
            
            # Plot markers based on lap-to-compound mapping
            for lap, compound in lap_compounds.items():
                if lap <= total_laps:
                    compound_color = self.compound_colors.get(compound, self.compound_colors['UNKNOWN'])
                    ax.scatter(lap, info['start_pos'], 
                             color=compound_color, s=100, alpha=0.9, 
                             edgecolors=info['team_color'], linewidth=1, zorder=4)

    def generate_race_plot(self, db_path: str, db_name: str, session_key: int, 
                          meeting_name: str, circuit_name: str) -> str:
        """Generate and save a race plot for a specific Grand Prix."""
        self.db_path = db_path
        
        try:
            # Load race data
            race_data = self._load_race_data(session_key)
            
            if race_data['drivers'].empty:
                print(f"❌ No driver data found for session {session_key}")
                return None
            
            print(f"🎨 Generating race plot for {meeting_name}...")
            
            # Process data
            events = self._process_race_control_events(race_data['race_control'], race_data['total_laps'])
            grid_info = self._get_driver_grid_info(race_data['drivers'], race_data['final_positions'])
            
            # Create plot
            fig, ax = plt.subplots(figsize=(24, 16))
            
            # Plot race control events
            self._plot_race_events(ax, events, race_data['total_laps'], grid_info)
            
            # Plot driver lines with tyre compounds
            self._plot_driver_lines(ax, grid_info, race_data['stints'], race_data['total_laps'])
            
            # Configure axes
            year = 2024  # Assuming 2024 data
            
            # Y-axis configuration (positions)
            positions = sorted([info['start_pos'] for info in grid_info.values()])
            y_labels = [f"P{int(pos)} - {next(info['acronym'] for info in grid_info.values() if info['start_pos'] == pos)}" 
                       for pos in positions]
            
            ax.set_ylim(max(positions) + 0.5, 0.5)
            ax.set_yticks(positions)
            ax.set_yticklabels(y_labels, fontsize=20)
            
            # Add mirrored y-axis on the right with black edge
            ax2 = ax.twinx()
            ax2.set_ylim(max(positions) + 0.5, 0.5)
            ax2.set_yticks(positions)
            ax2.spines['right'].set_color(self.f1_colors['text'])
            ax2.spines['right'].set_linewidth(1)
            # Create right side labels with final position format
            right_labels = [f"{next(info['acronym'] for info in grid_info.values() if info['start_pos'] == pos)} - P{int(pos)}" 
                           for pos in positions]
            ax2.set_yticklabels(right_labels, fontsize=20)
            
            # X-axis configuration (laps) - add extra tick but don't show it
            ax.set_xlim(0.5, race_data['total_laps'] + 1.5)
            
            # Create tick positions including the extra one
            all_ticks = list(range(1, race_data['total_laps'] + 2))
            visible_ticks = list(range(1, race_data['total_laps'] + 1))
            
            ax.set_xticks(all_ticks)
            # Show labels only for visible ticks, empty string for the extra tick
            tick_labels = [str(i) for i in visible_ticks] + ['']
            ax.set_xticklabels(tick_labels, rotation=90, fontsize=20)
            ax.set_xlabel('Lap Number', fontsize=20, fontweight='bold', labelpad=15)
            
            # Title
            ax.set_title(f'F1 {year} {meeting_name} - Race Overview', 
                        fontsize=24, fontweight='bold', pad=20)
            
            # Layout and save
            plt.tight_layout()
            
            # Create save directory
            race_dir = Path("data") / db_name / "dashboard" / "race"
            race_dir.mkdir(parents=True, exist_ok=True)
            
            # Clean meeting name for filename
            clean_name = re.sub(r'[^\w\s-]', '', meeting_name).strip()
            clean_name = re.sub(r'[-\s]+', '_', clean_name)
            
            save_path = race_dir / f"F1_{year}_{clean_name}_Race.png"
            
            plt.savefig(save_path, dpi=300, bbox_inches='tight', 
                       facecolor=self.f1_colors['background'])
            plt.close(fig)
            
            print(f"✅ Plot saved to: {save_path}")
            return str(save_path)
            
        except Exception as e:
            print(f"❌ Error generating race plot: {e}")
            import traceback
            traceback.print_exc()
            return None
        finally:
            self._close_connections()

    def generate_all_race_plots(self, db_path: str, db_name: str):
        """Generate race plots for all Grand Prix in the season."""
        self.db_path = db_path
        
        try:
            # Delete everything in output folder before starting
            race_dir = Path("data") / db_name / "dashboard" / "race_overview"
            if race_dir.exists():
                shutil.rmtree(race_dir)
                print(f"🗑️  Cleared output directory: {race_dir}")
            
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
                plot_path = self.generate_race_plot(
                    db_path, db_name, 
                    session['session_key'],
                    session['meeting_name'],
                    session['circuit_short_name']
                )
                if plot_path:
                    saved_plots.append(plot_path)
            
            print(f"\n✅ Generated {len(saved_plots)} race plots successfully!")
            return saved_plots
            
        except Exception as e:
            print(f"❌ Error generating race plots: {e}")
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
    """Main function to run the race plotter."""
    # Configuration
    db_name = 'f1db_YR=2024'
    data_dir = 'data'
    db_path = Path(data_dir) / db_name / 'database.db'
    
    print("🏎️  F1 2024 Race Analysis")
    print("=" * 60)
    print(f"Database: {db_path}")
    
    if not db_path.exists():
        print(f"\n❌ Database not found at: {db_path}")
        return False
    
    try:
        plotter = F1RacePlotter()
        saved_plots = plotter.generate_all_race_plots(str(db_path), db_name)
        
        if saved_plots:
            print(f"\n🎉 Successfully generated {len(saved_plots)} race plots!")
            return True
        else:
            print(f"\n❌ Failed to generate race plots")
            return False
            
    except Exception as e:
        print(f"❌ Critical error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
