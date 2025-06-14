import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import List, Dict
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

class F1RaceResultPlotter:
    """
    An integrated class to load, process, and plot F1 race result data,
    preserving the original F1-style visualization aesthetics.
    """

    def __init__(self):
        """Initializes the plotter with a predefined style and data loading components."""
        self.f1_colors = {
            'background': "#F1F1F1", 'grid': '#E0E0E0', 'text': "#101010",
            'accent': '#FF1801', 'secondary': '#F5F5F5'
        }
        self.driver_markers = {1: 'o', 2: 's', 3: '^', 4: '*'}
        self._setup_f1_style()

        self.db_path = None
        self.connection_pool = {}
        self.lock = threading.Lock()

    def _setup_f1_style(self):
        """Configure matplotlib for a consistent F1-style plot theme."""
        plt.style.use('default')
        sns.set_context("notebook", font_scale=1.5)
        plt.rcParams.update({
            'figure.facecolor': self.f1_colors['background'], 'axes.facecolor': self.f1_colors['background'],
            'axes.edgecolor': self.f1_colors['text'], # Changed to black
            'axes.labelcolor': self.f1_colors['text'],
            'axes.spines.left': True, 'axes.spines.bottom': True, 'axes.spines.top': False, 'axes.spines.right': False,
            'axes.grid': False, # Changed to False to remove gridlines
            'axes.grid.axis': 'y', 'grid.color': self.f1_colors['grid'], 'grid.alpha': 0.7, # These lines become less relevant with grid=False
            'text.color': self.f1_colors['text'], 'xtick.color': self.f1_colors['text'], 'ytick.color': self.f1_colors['text'],
            'legend.facecolor': self.f1_colors['background'], 'legend.edgecolor': 'none',
            'font.family': 'monospace', 'font.weight': 'bold', 'font.size': 14
        })

    # --- Data Loading and Processing Methods (from original F1DataLoader) ---

    def _get_connection(self, thread_id: int) -> sqlite3.Connection:
        if thread_id not in self.connection_pool:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            self.connection_pool[thread_id] = conn
        return self.connection_pool[thread_id]

    def _execute_query(self, query: str, thread_id: int) -> pd.DataFrame:
        return pd.read_sql_query(query, self._get_connection(thread_id))

    def _load_and_process_data(self) -> pd.DataFrame:
        print("🏎️  Loading and Processing F1 Race Data...") # Aligned print format
        
        # 1. Get race sessions
        q_sessions = """
            SELECT s.session_key, s.circuit_short_name, s.date_start, m.meeting_name
            FROM sessions s JOIN meetings m ON s.meeting_key = m.meeting_key
            WHERE s.session_name = 'Race' ORDER BY s.date_start
        """
        race_sessions = self._execute_query(q_sessions, threading.get_ident())
        if race_sessions.empty: 
            print("❌ No race sessions found!") # Aligned print format
            return pd.DataFrame()
        session_keys = race_sessions['session_key'].tolist()

        # 2. Load position and driver data in parallel
        def fetch_data_concurrently(task_function, items, desc):
            dfs = []
            batch_size = max(1, len(items) // 4)
            batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]
            with ThreadPoolExecutor(max_workers=8) as ex:
                with tqdm(desc=desc, total=len(batches), unit="batch") as pbar:
                    futures = {ex.submit(task_function, b, i): b for i, b in enumerate(batches)}
                    for future in as_completed(futures):
                        dfs.append(future.result()); pbar.update(1)
            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

        positions = fetch_data_concurrently(
            lambda b, tid: self._execute_query(f"SELECT session_key, driver_number, position, date FROM position WHERE session_key IN ({','.join(map(str, b))})", tid),
            session_keys, "Loading position data"
        )
        drivers = fetch_data_concurrently(
            lambda b, tid: self._execute_query(f"SELECT DISTINCT driver_number, name_acronym, full_name, team_name, team_colour, session_key FROM drivers WHERE session_key IN ({','.join(map(str, b))})", tid),
            session_keys, "Loading driver data"
        )
        
        if positions.empty or drivers.empty: 
            print("❌ No position or driver data found!") # Aligned print format
            return pd.DataFrame()

        # 3. Process and Merge
        positions['date'] = pd.to_datetime(positions['date'], format='ISO8601')
        final_pos = positions.loc[positions.groupby(['session_key', 'driver_number'])['date'].idxmax()]
        
        results = pd.merge(final_pos, race_sessions, on='session_key')
        results = pd.merge(results, drivers, on=['session_key', 'driver_number'])
        results = results.dropna(subset=['position', 'name_acronym', 'team_colour'])
        results['date_start'] = pd.to_datetime(results['date_start'], format='ISO8601')
        
        print(f"✅ Data processed successfully: {len(results)} records") # Aligned print format
        return results.sort_values(['date_start', 'position'])

    # --- Plotting Methods (from original F1Plotter with restored visuals) ---

    def _get_driver_legend_info(self, data: pd.DataFrame) -> Dict[str, Dict]:
        data_sorted = data.sort_values('date_start').reset_index()
        driver_info, team_prio = {}, {}
        first_race = data_sorted[data_sorted['meeting_name'] == data_sorted['meeting_name'].iloc[0]]
        for team in first_race['team_name'].unique():
            drivers = first_race[first_race['team_name'] == team].sort_values('driver_number')
            team_prio[team] = {row['name_acronym']: i for i, (_, row) in enumerate(drivers.iterrows(), 1)}
        for _, row in data_sorted.drop_duplicates(subset=['name_acronym'], keep='first').iterrows():
            prio = team_prio.get(row['team_name'], {}).get(row['name_acronym'], 3)
            color = str(row['team_colour']).strip()
            color = f"#{color}" if not color.startswith('#') else color
            color = '#000000' if len(color) != 7 else color
            driver_info[row['name_acronym']] = {'color': color, 'priority': prio, 'marker': self.driver_markers.get(prio, '^'), 'number': row['driver_number']}
        return driver_info

    def generate_and_save_plot(self, db_path: str, db_name: str):
        """Main method to generate and save the F1 position plot with original aesthetics."""
        self.db_path = db_path
        try:
            plot_data = self._load_and_process_data()
            if plot_data.empty: 
                print("❌ No race data to plot!") # Aligned print format
                return None
            
            print("\n🎨 Generating Position vs Grand Prix plot...") # Aligned print format
            
            # --- Plotting Logic with Restored Visuals ---
            meeting_order = plot_data.groupby('meeting_name')['date_start'].first().sort_values().index
            formatted_meetings = [name.replace(" Grand Prix", " GP") for name in meeting_order]
            fig, ax = plt.subplots(figsize=(30, 20))
            driver_info = self._get_driver_legend_info(plot_data)
            drivers_sorted = sorted(driver_info.keys(), key=lambda d: driver_info[d]['number'])

            for acronym in drivers_sorted:
                driver_season_data = plot_data[plot_data['name_acronym'] == acronym]
                x, y, colors = [], [], []
                for i, meeting in enumerate(meeting_order):
                    race = driver_season_data[driver_season_data['meeting_name'] == meeting]
                    if not race.empty:
                        x.append(i); y.append(race['position'].iloc[0])
                        color = str(race['team_colour'].iloc[0]).strip()
                        color = f"#{color}" if not color.startswith('#') else color
                        colors.append('#000000' if len(color) != 7 else color)
                if x:
                    for i in range(len(x) - 1): # Line color is that of the DESTINATION race
                        ax.plot([x[i], x[i+1]], [y[i], y[i+1]], color=colors[i+1], linewidth=3.5, alpha=0.7, zorder=1)
                    # Marker color is that of the SPECIFIC race
                    ax.scatter(x, y, c=colors, marker=driver_info[acronym]['marker'], s=140, alpha=0.9, zorder=2)

            # --- Aesthetics and Configuration (Restored from Original) ---
            year = plot_data['date_start'].min().year
            ax.set_title(f'F1 {year} - Driver Positions by Grand Prix', fontsize=30, fontweight='bold', color=self.f1_colors['text'], pad=20)
            ax.set_xlabel('Grand Prix', fontsize=24, fontweight='bold', color=self.f1_colors['text'], labelpad=20)
            ax.set_ylabel('Position', fontsize=24, fontweight='bold', color=self.f1_colors['text'], labelpad=20)
            ax.set_ylim(20.5, 0.5); ax.set_yticks(range(1, 21)); ax.set_yticklabels([f'P{i}' for i in range(1, 21)], fontsize=20)
            ax.set_xlim(-0.5, len(meeting_order) - 0.5); ax.set_xticks(range(len(meeting_order))); ax.set_xticklabels(formatted_meetings, rotation=90, ha='center', fontsize=20)
            ax.grid(False) # Removed gridlines explicitly
            
            legend_elements = [plt.Line2D([0], [0], marker=info['marker'], color=info['color'], label=acronym, markersize=14, linewidth=4, linestyle='-')
                               for acronym, info in sorted(driver_info.items(), key=lambda item: item[1]['number'])]
            legend = ax.legend(handles=legend_elements, loc='center left', bbox_to_anchor=(1.01, 0.5), frameon=False, fontsize=20, title='Drivers', title_fontsize=24)
            legend.get_title().set_color(self.f1_colors['text']); legend.get_title().set_fontweight('bold')
            for text in legend.get_texts(): text.set_color(self.f1_colors['text']); text.set_fontweight('bold')
            
            plt.tight_layout(rect=[0, 0, 0.9, 1]) # Keep original margin for legend
            
            dashboard_dir = Path("data") / db_name / "dashboard"; dashboard_dir.mkdir(parents=True, exist_ok=True)
            save_path = dashboard_dir / f"F1_{year}_Driver_Positions_vs_Grand_Prix.png"
            plt.savefig(save_path, dpi=300, bbox_inches='tight', facecolor=self.f1_colors['background'])
            plt.close(fig)
            
            print(f"✅ Plot saved to: {save_path}") # Aligned print format
            return str(save_path)
        finally:
            self._close_connections()

    def _close_connections(self):
        for conn in self.connection_pool.values(): conn.close()
        self.connection_pool.clear()
