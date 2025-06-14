import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from matplotlib.patches import Rectangle
import matplotlib.patches as mpatches
from pathlib import Path
import os


class F1Plotter:
    """
    F1-style plotting class for creating beautiful race visualization plots.
    """
    
    def __init__(self):
        """Initializes the plotter with a predefined style."""
        # Light theme color palette
        self.f1_colors = {
            'background': "#F1F1F1",
            'grid': '#E0E0E0',
            'text': '#000000',
            'accent': '#FF1801',
            'secondary': '#F5F5F5'
        }
        
        # Marker styles for different driver priorities within teams
        self.driver_markers = {
            1: 'o',  # Circle - first driver
            2: 's',  # Square - second driver
            3: '^',  # Triangle - third driver
            4: '*'   # Star - fourth driver
        }
        
        # Set up the base F1 style for all plots
        self._setup_f1_style()
    
    def _setup_f1_style(self):
        """Configure matplotlib and seaborn for a consistent F1-style plot theme."""
        plt.style.use('default')
        sns.set_palette("husl")
        sns.set_context("notebook", font_scale=1.5)
        
        plt.rcParams.update({
            'figure.facecolor': self.f1_colors['background'],
            'axes.facecolor': self.f1_colors['background'],
            'axes.edgecolor': self.f1_colors['grid'],
            'axes.labelcolor': self.f1_colors['text'],
            'axes.spines.left': True,
            'axes.spines.bottom': True,
            'axes.spines.top': False,
            'axes.spines.right': False,
            'axes.grid': True,
            'axes.grid.axis': 'y',
            'grid.color': self.f1_colors['grid'],
            'grid.alpha': 0.7,
            'text.color': self.f1_colors['text'],
            'xtick.color': self.f1_colors['text'],
            'ytick.color': self.f1_colors['text'],
            'legend.facecolor': self.f1_colors['background'],
            'legend.edgecolor': 'none',
            'font.family': 'monospace',
            'font.weight': 'bold',
            'font.size': 14
        })

    def _get_driver_legend_and_priority_info(self, data: pd.DataFrame) -> Dict[str, Dict]:
        """
        Determines driver priorities and legend attributes based on the first GP and first appearance.
        This function is the new core logic to fix the priority and legend issues.
        
        Args:
            data: DataFrame with all season driver data.
            
        Returns:
            A dictionary where keys are driver acronyms and values are dicts with their
            legend color, priority, marker, and initial driver number.
            e.g., {"VER": {"color": "#...", "priority": 1, "marker": "o", "number": 1}}
        """
        if 'date_start' not in data.columns:
            # Fallback if date is not available, though sorting by date is crucial
            data_sorted = data.copy()
        else:
            data_sorted = data.sort_values('date_start').reset_index(drop=True)

        # This dictionary will store the definitive legend info for each driver
        driver_info = {}

        # Dictionary to store team priorities established in the first race
        team_priorities_first_race = {}
        
        # 1. Determine priorities from the first race of the season
        first_race_meeting = data_sorted['meeting_name'].iloc[0]
        first_race_data = data_sorted[data_sorted['meeting_name'] == first_race_meeting]
        
        teams_in_first_race = first_race_data['team_name'].unique()
        
        for team in teams_in_first_race:
            if pd.isna(team):
                continue
            
            # Get drivers for the team in the first race, sorted by their number
            team_drivers = first_race_data[first_race_data['team_name'] == team]
            team_drivers = team_drivers.sort_values('driver_number')
            
            # Assign priority 1 (first) and 2 (second)
            priorities = {}
            for i, (_, driver_row) in enumerate(team_drivers.iterrows(), 1):
                priorities[driver_row['name_acronym']] = i
            team_priorities_first_race[team] = priorities

        # 2. Build the definitive legend info for EVERY driver based on their first appearance
        # Use drop_duplicates to ensure we only process each driver once
        unique_drivers_first_appearance = data_sorted.drop_duplicates(subset=['name_acronym'], keep='first')
        
        for _, row in unique_drivers_first_appearance.iterrows():
            driver_acronym = row['name_acronym']
            team_name = row['team_name']
            
            priority = 3 # Default to 3 for reserve/replacement drivers
            
            # Check if the driver was in the first race for their team to get P1/P2
            if team_name in team_priorities_first_race:
                if driver_acronym in team_priorities_first_race[team_name]:
                    priority = team_priorities_first_race[team_name][driver_acronym]
            
            # Clean up team color
            team_color = row['team_colour']
            if pd.isna(team_color): team_color = '000000'
            team_color = str(team_color).strip()
            if not team_color.startswith('#'): team_color = f'#{team_color}'
            if len(team_color) != 7: team_color = '#000000'

            # Store the definitive info. This ensures one legend entry per driver.
            driver_info[driver_acronym] = {
                'color': team_color,
                'priority': priority,
                'marker': self.driver_markers.get(priority, '^'),
                'number': row['driver_number'] # Store their first number for sorting
            }

        return driver_info

    def _create_save_directory(self, db_name: str) -> Path:
        """Creates the directory for saving plot images."""
        dashboard_dir = Path("data") / db_name / "dashboard"
        dashboard_dir.mkdir(parents=True, exist_ok=True)
        return dashboard_dir
    
    def _format_meeting_name(self, meeting_name: str) -> str:
        """Converts 'Italian Grand Prix' to 'Italian GP'."""
        if pd.isna(meeting_name):
            return meeting_name
        return str(meeting_name).replace(" Grand Prix", " GP")
    
    def plot_position_vs_grandprix(self, data: pd.DataFrame, db_name: str = "f1db_YR=2024"):
        """
        Creates and saves the F1 Position vs. Grand Prix plot.
        
        Args:
            data: DataFrame with race result data.
            db_name: Database name used for the save directory.
        """
        if data.empty:
            print("❌ No data to plot!")
            return
        
        print("🎨 Creating Position vs Grand Prix plot with corrected logic...")
        
        dashboard_dir = self._create_save_directory(db_name)
        plot_data = data.copy()
        
        # Establish a consistent race order based on date
        if 'date_start' in plot_data.columns:
            meeting_order = plot_data.groupby('meeting_name')['date_start'].first().sort_values().index.tolist()
        else:
            meeting_order = sorted(plot_data['meeting_name'].unique())
        
        formatted_meeting_order = [self._format_meeting_name(name) for name in meeting_order]
        
        fig, ax = plt.subplots(figsize=(30, 20))
        fig.patch.set_facecolor(self.f1_colors['background'])
        
        # Get the definitive driver info for legend and priorities
        driver_info = self._get_driver_legend_and_priority_info(plot_data)
        
        # Get unique drivers and sort them by their initial driver number for a consistent legend order
        drivers_sorted_by_number = sorted(driver_info.keys(), key=lambda d: driver_info[d]['number'])
        
        # Plot data for each driver
        for driver_acronym in drivers_sorted_by_number:
            driver_season_data = plot_data[plot_data['name_acronym'] == driver_acronym]
            
            x_data, y_data, colors_data = [], [], []
            
            for i, meeting in enumerate(meeting_order):
                race_data = driver_season_data[driver_season_data['meeting_name'] == meeting]
                if not race_data.empty:
                    x_data.append(i)
                    y_data.append(race_data['position'].iloc[0])
                    
                    team_color = race_data['team_colour'].iloc[0]
                    if pd.isna(team_color): team_color = '000000'
                    team_color = str(team_color).strip()
                    if not team_color.startswith('#'): team_color = f'#{team_color}'
                    if len(team_color) != 7: team_color = "#000000"
                    colors_data.append(team_color)
            
            if x_data and y_data:
                # Plot line segments with color of the destination point
                for i in range(len(x_data) - 1):
                    ax.plot([x_data[i], x_data[i+1]], 
                           [y_data[i], y_data[i+1]],
                           color=colors_data[i+1], # Line takes color of the upcoming race
                           linewidth=3.5, alpha=0.7, zorder=1)
                
                # Plot markers for each race result
                marker_for_driver = driver_info[driver_acronym]['marker']
                for x, y, color in zip(x_data, y_data, colors_data):
                    ax.scatter(x, y, color=color, marker=marker_for_driver,
                               s=140, alpha=0.9, linewidth=1.5, zorder=2)
        
        # Customize the plot aesthetics
        ax.set_xlabel('Grand Prix', fontsize=24, fontweight='bold', color=self.f1_colors['text'], labelpad=20)
        ax.set_ylabel('Position', fontsize=24, fontweight='bold', color=self.f1_colors['text'], labelpad=20)
        
        year = pd.to_datetime(data['date_start'].min()).year if 'date_start' in data.columns else "Season"
        title = f'F1 {year} - Driver Positions by Grand Prix'
        ax.set_title(title, fontsize=30, fontweight='bold', color=self.f1_colors['text'], pad=20)
        
        ax.invert_yaxis()
        ax.set_ylim(20.5, 0.5)
        ax.set_yticks(range(1, 21))
        ax.set_yticklabels([f'P{i}' for i in range(1, 21)], fontsize=20)
        
        ax.set_xlim(-0.5, len(meeting_order) - 0.5)
        ax.set_xticks(range(len(meeting_order)))
        ax.set_xticklabels(formatted_meeting_order, rotation=90, ha='center', fontsize=20)
        
        ax.grid(True, alpha=0.7, color=self.f1_colors['grid'], linewidth=1.2)
        ax.set_axisbelow(True)
        
        # Create a clean legend using the definitive driver_info
        legend_elements = []
        for driver_acronym in drivers_sorted_by_number:
            info = driver_info[driver_acronym]
            legend_elements.append(
                plt.Line2D([0], [0], marker=info['marker'], color=info['color'], 
                          label=driver_acronym, markersize=14, linewidth=4, linestyle='-')
            )
        
        legend = ax.legend(handles=legend_elements, loc='center left', bbox_to_anchor=(1.02, 0.5),
                          frameon=False, fontsize=20, title='Drivers', title_fontsize=24,
                          handletextpad=1.2, columnspacing=2)
        
        legend.get_title().set_color(self.f1_colors['text'])
        legend.get_title().set_fontweight('bold')
        for text in legend.get_texts():
            text.set_color(self.f1_colors['text'])
            text.set_fontweight('bold')
        
        plt.tight_layout(rect=[0, 0, 0.9, 1]) # Adjust layout to make space for legend
        
        save_path = dashboard_dir / f"{title.replace(' ', '_')}.png"
        plt.savefig(save_path, dpi=300, bbox_inches='tight', facecolor=self.f1_colors['background'])
        
        print(f"💾 Plot saved to: {save_path}")
        plt.close(fig)
        
        print("✅ Position vs Grand Prix plot created successfully!")
        return str(save_path)
    
    def get_plot_summary(self, data: pd.DataFrame):
        """Prints a summary of the data used for the plot."""
        if data.empty:
            print("No data available for summary.")
            return
        
        print("\n📊 Plot Data Summary:")
        print("=" * 40)
        print(f"🏁 Total Races: {data['meeting_name'].nunique()}")
        print(f"🏎️  Total Drivers: {data['name_acronym'].nunique()}")
        print(f"🏆 Total Teams: {data['team_name'].nunique()}")
        
        if 'date_start' in data.columns:
            driver_info = self._get_driver_legend_and_priority_info(data)
            drivers_sorted = sorted(driver_info.keys(), key=lambda d: driver_info[d]['number'])
            print(f"\n🏎️  Drivers (by number): {', '.join(drivers_sorted)}")
        
        print("=" * 40)
