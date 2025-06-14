#!/usr/bin/env python3
"""
F1 2024 Pit Stop Analysis Dashboard
==================================
A visualization tool for F1 pit stop data analysis showing total laps and average pit stops per race.
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
from typing import Dict
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

class F1PitStopPlotter:
    """
    A class to load, process, and plot F1 pit stop data with race lap information,
    using the same F1-style visualization aesthetics as the race result plotter.
    """

    def __init__(self):
        """Initializes the plotter with F1-style theme and data loading components."""
        self.f1_colors = {
            'background': "#F1F1F1", 'grid': '#E0E0E0', 'text': "#101010",
            'accent': '#FF1801', 'secondary': '#F5F5F5', 'blue': "#0055AA"
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
            'axes.edgecolor': self.f1_colors['text'], # Changed to black
            'axes.labelcolor': self.f1_colors['text'],
            'axes.spines.left': True, 'axes.spines.bottom': True, 
            'axes.spines.top': False, 'axes.spines.right': True,
            'axes.grid': False,  # Disabled grid
            'text.color': self.f1_colors['text'], 
            'xtick.color': self.f1_colors['text'], 
            'ytick.color': self.f1_colors['text'],
            'legend.facecolor': self.f1_colors['background'], 
            'legend.edgecolor': 'none',
            'font.family': 'monospace', 'font.weight': 'bold', 'font.size': 14
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

    def _load_and_process_pit_data(self) -> pd.DataFrame:
        """Load and process pit stop data with race session information using multithreading."""
        print("🏁 Loading and Processing F1 Pit Stop Data...") # Aligned print format
        
        # 1. Get race sessions
        q_sessions = """
            SELECT s.session_key, s.circuit_short_name, s.date_start, m.meeting_name
            FROM sessions s 
            JOIN meetings m ON s.meeting_key = m.meeting_key
            WHERE s.session_name = 'Race' 
            ORDER BY s.date_start
        """
        race_sessions = self._execute_query(q_sessions, threading.get_ident())
        
        if race_sessions.empty:
            print("❌ No race sessions found!") # Aligned print format
            return pd.DataFrame()
        
        session_keys = race_sessions['session_key'].tolist()
        # Removed the print statement for number of sessions to align with race_result_plot

        # 2. Load data in parallel using multithreading
        # Load pit stop data
        pit_data = self._fetch_data_concurrently(
            lambda b, tid: self._execute_query(f"""
                SELECT session_key, driver_number, lap_number, pit_duration, date
                FROM pit 
                WHERE session_key IN ({','.join(map(str, b))})
                ORDER BY session_key, driver_number, lap_number
            """, tid),
            session_keys, "Loading pit stop data"
        )
        
        # Load lap data
        lap_data = self._fetch_data_concurrently(
            lambda b, tid: self._execute_query(f"""
                SELECT session_key, MAX(lap_number) as total_laps
                FROM laps 
                WHERE session_key IN ({','.join(map(str, b))})
                GROUP BY session_key
            """, tid),
            session_keys, "Loading lap data"
        )
        
        # Load position data
        position_data = self._fetch_data_concurrently(
            lambda b, tid: self._execute_query(f"""
                SELECT DISTINCT session_key, driver_number, position
                FROM position 
                WHERE session_key IN ({','.join(map(str, b))})
                AND position IS NOT NULL
                AND position <= 20
            """, tid),
            session_keys, "Loading position data"
        )
        
        if pit_data.empty or lap_data.empty:
            print("❌ No pit or lap data found!") # Aligned print format
            return pd.DataFrame()

        # 3. Process pit stops - count pit stops per driver per race
        pit_counts = pit_data.groupby(['session_key', 'driver_number']).size().reset_index(name='pit_count')
        
        # 4. Merge with race sessions and lap data
        results = pd.merge(race_sessions, lap_data, on='session_key')
        
        # 5. Calculate average pit stops per race (only for race finishers)
        finisher_pit_counts = pd.merge(pit_counts, position_data, on=['session_key', 'driver_number'], how='inner')
        avg_pit_stops = finisher_pit_counts.groupby('session_key')['pit_count'].mean().reset_index(name='avg_pit_stops')
        
        # 6. Merge everything together
        results = pd.merge(results, avg_pit_stops, on='session_key', how='left')
        results['avg_pit_stops'] = results['avg_pit_stops'].fillna(0)
        
        # 7. Sort by date
        results['date_start'] = pd.to_datetime(results['date_start'], format='ISO8601')
        results = results.sort_values('date_start')
        
        print(f"✅ Data processed successfully: {len(results)} records") # Aligned print format
        return results

    def generate_and_save_plot(self, db_path: str, db_name: str):
        """Generate and save the F1 pit stop analysis plot."""
        self.db_path = db_path
        
        try:
            plot_data = self._load_and_process_pit_data()
            if plot_data.empty:
                print("❌ No pit stop data to plot!") # Aligned print format
                return None
            
            print("\n🎨 Generating Laps vs Pit Stops plot...") # Aligned print format and new name
            
            # --- Plotting Logic ---
            meeting_order = plot_data['meeting_name'].tolist()
            formatted_meetings = [name.replace(" Grand Prix", " GP") for name in meeting_order]
            
            # Create figure with dual y-axes
            fig, ax1 = plt.subplots(figsize=(30, 20))
            ax2 = ax1.twinx()
            
            # Prepare data
            x_positions = range(len(meeting_order))
            total_laps = plot_data['total_laps'].values
            avg_pit_stops = plot_data['avg_pit_stops'].values
            
            # Create gradient colors for bars (fb5607 to ffbe0b)
            min_laps, max_laps = total_laps.min(), total_laps.max()
            colors = []
            for laps in total_laps:
                # Normalize lap count to 0-1 range
                norm_value = (laps - min_laps) / (max_laps - min_laps) if max_laps > min_laps else 0
                # Interpolate between colors
                r = int(0xfb + (0xff - 0xfb) * norm_value)
                g = int(0x56 + (0xbe - 0x56) * norm_value)
                b = int(0x07 + (0x0b - 0x07) * norm_value)
                colors.append(f'#{r:02x}{g:02x}{b:02x}')
            
            # Plot bars for total laps (left y-axis)
            bars = ax1.bar(x_positions, total_laps, color=colors, alpha=0.7, 
                          width=0.6, edgecolor='black', linewidth=1.5, zorder=1)
            
            # Plot line for average pit stops (right y-axis) - Changed to blue, markersize 14
            line = ax2.plot(x_positions, avg_pit_stops, color=self.f1_colors['blue'], 
                           marker='o', markersize=14, linewidth=4, markerfacecolor=self.f1_colors['blue'], # Changed markersize and markerfacecolor
                           markeredgecolor=self.f1_colors['blue'], markeredgewidth=3, zorder=2)
            
            # --- Aesthetics and Configuration ---
            year = plot_data['date_start'].min().year
            
            # Main title
            fig.suptitle(f'F1 {year} - Race Laps & Average Pit Stops by Grand Prix', 
                        fontsize=30, fontweight='bold', color=self.f1_colors['text'], y=0.95)
            
            # Left y-axis (Total Laps)
            ax1.set_xlabel('Grand Prix', fontsize=24, fontweight='bold', 
                          color=self.f1_colors['text'], labelpad=20)
            ax1.set_ylabel('Total Laps', fontsize=24, fontweight='bold', 
                          color=self.f1_colors['text'], labelpad=20)
            ax1.set_ylim(0, max(total_laps) * 1.1)
            ax1.tick_params(axis='y', labelcolor=self.f1_colors['text'], labelsize=20)
            ax1.tick_params(axis='x', labelcolor=self.f1_colors['text'], labelsize=20)
            
            # Right y-axis (Average Pit Stops) - Proportionate scaling and black text
            max_laps_val = max(total_laps) * 1.1
            max_pits_val = max_laps_val / 20  # 10 laps = 0.5 pits, so 20 laps = 1 pit
            ax2.set_ylabel('Average Pit Stops', fontsize=24, fontweight='bold', 
                          color=self.f1_colors['text'], labelpad=20)
            ax2.set_ylim(0, max_pits_val)
            ax2.tick_params(axis='y', labelcolor=self.f1_colors['text'], labelsize=20)
            ax2.spines['right'].set_color(self.f1_colors['text'])
            ax2.spines['right'].set_linewidth(2)
            
            # X-axis configuration
            ax1.set_xlim(-0.5, len(meeting_order) - 0.5)
            ax1.set_xticks(x_positions)
            ax1.set_xticklabels(formatted_meetings, rotation=90, ha='center', fontsize=20)
            
            # Remove grid for both axes
            ax1.grid(False)
            ax2.grid(False)
            
            # Add value labels on bars
            for i, (bar, laps, pits) in enumerate(zip(bars, total_laps, avg_pit_stops)):
                # Lap count on top of bars
                ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(total_laps) * 0.01,
                        f'{int(laps)}', ha='center', va='bottom', fontsize=16, fontweight='bold',
                        color=self.f1_colors['text'])
                
                # Pit stop average closer to the line points - moved closer
                ax2.text(i, pits + max_pits_val * 0.02, f'{pits:.1f}', 
                        ha='center', va='bottom', fontsize=16, fontweight='bold',
                        color=self.f1_colors['text'])
            
            # Legend
            legend_elements = [
                plt.Rectangle((0, 0), 1, 1, facecolor='#fb5607', alpha=0.7, 
                             edgecolor='black', linewidth=1.5, label='Total Laps (Gradient: Low→High)'),
                plt.Line2D([0], [0], color=self.f1_colors['blue'], marker='o', 
                          markersize=14, linewidth=4, markerfacecolor=self.f1_colors['blue'],
                          markeredgecolor=self.f1_colors['blue'], markeredgewidth=3,
                          label='Average Pit Stops')
            ]
            
            legend = ax1.legend(handles=legend_elements, loc='upper left', frameon=False, 
                               fontsize=20)
            for text in legend.get_texts():
                text.set_color(self.f1_colors['text'])
                text.set_fontweight('bold')
            
            # Layout and save
            plt.tight_layout(rect=[0, 0, 1, 0.95])
            
            dashboard_dir = Path("data") / db_name / "dashboard"
            dashboard_dir.mkdir(parents=True, exist_ok=True)
            save_path = dashboard_dir / f"F1_{year}_Laps_&_Pit_Stops_vs_Grand_Prix.png"
            
            plt.savefig(save_path, dpi=300, bbox_inches='tight', 
                       facecolor=self.f1_colors['background'])
            plt.close(fig)
            
            print(f"✅ Plot saved to: {save_path}") # Aligned print format
            return str(save_path)
            
        except Exception as e:
            print(f"❌ Error generating pit stop plot: {e}") # Aligned print format
            import traceback
            traceback.print_exc()
            return None
        finally:
            self._close_connections()

    def _close_connections(self):
        """Close all database connections."""
        for conn in self.connection_pool.values():
            conn.close()
        self.connection_pool.clear()
