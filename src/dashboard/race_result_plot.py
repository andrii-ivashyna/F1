import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from matplotlib.patches import Rectangle
import matplotlib.patches as mpatches
from pathlib import Path
import os


class F1Plotter:
    """
    F1-style plotting class for creating beautiful race visualization plots.
    """
    
    def __init__(self):
        # F1 Official Color Palette
        self.f1_colors = {
            'background': '#15151E',
            'grid': '#2D2D2D',
            'text': '#FFFFFF',
            'accent': '#FF1801',
            'secondary': '#38383F'
        }
        
        # Set up F1 style
        self._setup_f1_style()
    
    def _setup_f1_style(self):
        """Configure matplotlib and seaborn for F1-style plots."""
        # Set dark theme
        plt.style.use('dark_background')
        
        # Configure seaborn
        sns.set_palette("husl")
        sns.set_context("notebook", font_scale=1.2)
        
        # Configure matplotlib
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
            'grid.alpha': 0.3,
            'text.color': self.f1_colors['text'],
            'xtick.color': self.f1_colors['text'],
            'ytick.color': self.f1_colors['text'],
            'legend.facecolor': self.f1_colors['background'],
            'legend.edgecolor': 'none',
            'font.family': 'monospace',
            'font.weight': 'bold'
        })
    
    def _parse_team_colors(self, color_str: str) -> str:
        """Parse team color string and return valid hex color."""
        if pd.isna(color_str):
            return '#888888'
        
        # Clean the color string
        color = str(color_str).strip()
        
        # If it's already a hex color, validate it
        if color.startswith('#'):
            if len(color) == 7:
                return color
            elif len(color) == 4:  # Short hex like #F00
                return f"#{color[1]*2}{color[2]*2}{color[3]*2}"
        
        # Common F1 team colors mapping
        team_colors = {
            'red': '#FF1801',
            'blue': '#0033CC',
            'green': '#00CC00',
            'orange': '#FF8000',
            'pink': '#FF69B4',
            'purple': '#8A2BE2',
            'yellow': '#FFD700',
            'silver': '#C0C0C0',
            'black': '#000000',
            'white': '#FFFFFF'
        }
        
        # Try to match common color names
        color_lower = color.lower()
        for name, hex_color in team_colors.items():
            if name in color_lower:
                return hex_color
        
        # Default fallback
        return '#888888'
    
    def _create_save_directory(self, db_name: str) -> Path:
        """Create the dashboard directory for saving plots."""
        dashboard_dir = Path("data") / db_name / "dashboard"
        dashboard_dir.mkdir(parents=True, exist_ok=True)
        return dashboard_dir
    
    def plot_position_vs_grandprix(self, data: pd.DataFrame, db_name: str = "f1db_YR=2024"):
        """
        Create F1-style Position vs Grand Prix plot and save it.
        
        Args:
            data: DataFrame with columns [circuit_short_name, name_acronym, position, team_colour, full_name]
            db_name: Database name for creating save directory
        """
        
        if data.empty:
            print("❌ No data to plot!")
            return
        
        print("🎨 Creating Position vs Grand Prix plot...")
        
        # Create save directory
        dashboard_dir = self._create_save_directory(db_name)
        
        # Prepare data
        plot_data = data.copy()
        
        # Sort circuits by date to maintain race order
        if 'date_start' in plot_data.columns:
            circuit_order = plot_data.sort_values('date_start')['circuit_short_name'].unique()
        else:
            circuit_order = sorted(plot_data['circuit_short_name'].unique())
        
        # Create figure with F1 proportions
        fig, ax = plt.subplots(figsize=(20, 12))
        fig.patch.set_facecolor(self.f1_colors['background'])
        
        # Get unique drivers and assign colors
        drivers = plot_data['name_acronym'].unique()
        driver_colors = {}
        
        # Group by driver and get their team color
        for driver in drivers:
            driver_data = plot_data[plot_data['name_acronym'] == driver]
            team_color = driver_data['team_colour'].iloc[0]
            driver_colors[driver] = self._parse_team_colors(team_color)
        
        # Plot lines for each driver
        for driver in drivers:
            driver_data = plot_data[plot_data['name_acronym'] == driver]
            
            # Prepare x and y data
            x_data = []
            y_data = []
            
            for circuit in circuit_order:
                circuit_data = driver_data[driver_data['circuit_short_name'] == circuit]
                if not circuit_data.empty:
                    x_data.append(circuit)
                    y_data.append(circuit_data['position'].iloc[0])
            
            if x_data and y_data:
                # Plot line with square markers
                ax.plot(x_data, y_data, 
                       color=driver_colors[driver], 
                       marker='s',  # Square marker
                       markersize=8,
                       linewidth=2.5,
                       alpha=0.8,
                       label=driver)
        
        # Customize the plot
        ax.set_xlabel('Grand Prix', fontsize=16, fontweight='bold', color=self.f1_colors['text'])
        ax.set_ylabel('Position', fontsize=16, fontweight='bold', color=self.f1_colors['text'])
        
        # Generate title from data
        year = "2024"
        if 'date_start' in plot_data.columns and not plot_data['date_start'].empty:
            first_date = pd.to_datetime(plot_data['date_start'].iloc[0])
            year = str(first_date.year)
        
        title = f'F1 {year} Season - Driver Positions by Grand Prix'
        ax.set_title(title, fontsize=24, fontweight='bold', color=self.f1_colors['text'], pad=20)
        
        # Invert y-axis (position 1 at top)
        ax.invert_yaxis()
        
        # Set y-axis limits and ticks
        ax.set_ylim(20.5, 0.5)
        ax.set_yticks(range(1, 21))
        ax.set_yticklabels([f'P{i}' for i in range(1, 21)], fontsize=12)
        
        # Rotate x-axis labels
        ax.set_xticks(range(len(circuit_order)))
        ax.set_xticklabels(circuit_order, rotation=90, ha='center', fontsize=11)
        
        # Add grid
        ax.grid(True, alpha=0.3, color=self.f1_colors['grid'])
        ax.set_axisbelow(True)
        
        # Create legend with driver acronyms
        legend_elements = [
            plt.Line2D([0], [0], marker='s', color=driver_colors[driver], 
                      label=f'{driver}', markersize=10, linewidth=3, linestyle='-')
            for driver in sorted(drivers)
        ]
        
        # Position legend outside the plot
        legend = ax.legend(handles=legend_elements, 
                          loc='center left', 
                          bbox_to_anchor=(1.02, 0.5),
                          frameon=False,
                          fontsize=14,
                          title='Drivers',
                          title_fontsize=16)
        
        # Style legend
        legend.get_title().set_color(self.f1_colors['text'])
        legend.get_title().set_fontweight('bold')
        
        for text in legend.get_texts():
            text.set_color(self.f1_colors['text'])
            text.set_fontweight('bold')
        
        # Add F1 branding
        ax.text(0.02, 0.98, 'FORMULA 1®', transform=ax.transAxes, 
                fontsize=10, fontweight='bold', color=self.f1_colors['accent'],
                verticalalignment='top')
        
        # Add race count info
        race_count = len(circuit_order)
        driver_count = len(drivers)
        ax.text(0.02, 0.02, f'{race_count} Races • {driver_count} Drivers', 
                transform=ax.transAxes, fontsize=10, 
                color=self.f1_colors['text'], alpha=0.7)
        
        # Adjust layout
        plt.tight_layout()
        
        # Save plot with title as filename
        safe_title = title.replace(' ', '_').replace('-', '_').replace('/', '_')
        save_path = dashboard_dir / f"{safe_title}.png"
        
        plt.savefig(save_path, dpi=300, bbox_inches='tight', 
                   facecolor=self.f1_colors['background'])
        
        print(f"💾 Plot saved to: {save_path}")
        
        # Close the figure to free memory
        plt.close(fig)
        
        print("✅ Position vs Grand Prix plot created successfully!")
        
        return str(save_path)
    
    def get_plot_summary(self, data: pd.DataFrame):
        """Print summary statistics for the plot data."""
        if data.empty:
            print("No data available for summary.")
            return
        
        print("\n📊 Plot Data Summary:")
        print("=" * 40)
        print(f"🏁 Total Races: {data['circuit_short_name'].nunique()}")
        print(f"🏎️  Total Drivers: {data['name_acronym'].nunique()}")
        print(f"🏆 Total Teams: {data['team_name'].nunique()}")
        print(f"📈 Total Results: {len(data)}")
        
        # Show circuits
        circuits = sorted(data['circuit_short_name'].unique())
        print(f"\n🏁 Circuits: {', '.join(circuits)}")
        
        # Show drivers
        drivers = sorted(data['name_acronym'].unique())
        print(f"\n🏎️  Drivers: {', '.join(drivers)}")
        
        print("=" * 40)
