import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

def analyze_lap_times_saudi_gp(db_name="f1db_YR=2024", driver_number=1):
    # Connect to the database
    db_path = f"data/{db_name}/database.db"
    conn = sqlite3.connect(db_path)
    
    try:
        print(f"🏁 F1 2024 Lap Time Analysis - Saudi Arabian GP - Driver #{driver_number}")
        print("="*80)
        
        # Get lap time data for driver #1 in Saudi Arabian GP
        query = """
        SELECT 
            l.session_key,
            m.meeting_name,
            s.session_name,
            s.date_start,
            d.driver_number,
            d.broadcast_name,
            d.team_name,
            l.lap_number,
            l.lap_duration,
            l.is_pit_out_lap,
            p.pit_duration,
            st.compound,
            st.tyre_age_at_start,
            l.lap_number - st.lap_start + st.tyre_age_at_start as tyre_age
        FROM laps l
        JOIN sessions s ON l.session_key = s.session_key
        JOIN meetings m ON s.meeting_key = m.meeting_key
        JOIN drivers d ON l.driver_number = d.driver_number AND l.session_key = d.session_key
        LEFT JOIN stints st ON l.session_key = st.session_key 
            AND l.driver_number = st.driver_number 
            AND l.lap_number BETWEEN st.lap_start AND st.lap_end
        LEFT JOIN pit p ON l.session_key = p.session_key 
            AND l.driver_number = p.driver_number 
            AND l.lap_number = p.lap_number
        WHERE m.meeting_name = 'Saudi Arabian Grand Prix'
            AND s.session_name = 'Race'
            AND l.driver_number = ?
            AND l.lap_duration IS NOT NULL
        ORDER BY l.lap_number
        """
        
        data = pd.read_sql_query(query, conn, params=[driver_number])
        
        if data.empty:
            print(f"No lap data found for driver #{driver_number} in Saudi Arabian Grand Prix")
            return
        
        # Driver info
        driver_name = data.iloc[0]['broadcast_name']
        team_name = data.iloc[0]['team_name']
        race_date = data.iloc[0]['date_start']
        
        print(f"Driver: #{driver_number} {driver_name} ({team_name})")
        print(f"Race Date: {race_date}")
        print(f"Total laps completed: {len(data)}")
        print()
        
        # lap_duration is already in seconds (REAL type), so we can use it directly
        data['lap_duration_seconds'] = data['lap_duration']
        
        # Filter out obvious outliers (pit stops, safety cars, etc.)
        # We'll identify these as laps significantly slower than the median
        median_time = data['lap_duration_seconds'].median()
        std_time = data['lap_duration_seconds'].std()
        normal_laps = data[data['lap_duration_seconds'] <= median_time + 2 * std_time]
        
        print("📊 Lap Time Statistics:")
        print("="*30)
        print(f"Fastest lap: {data['lap_duration_seconds'].min():.3f}s (Lap {data.loc[data['lap_duration_seconds'].idxmin(), 'lap_number']})")
        print(f"Slowest lap: {data['lap_duration_seconds'].max():.3f}s (Lap {data.loc[data['lap_duration_seconds'].idxmax(), 'lap_number']})")
        print(f"Average lap time: {normal_laps['lap_duration_seconds'].mean():.3f}s")
        print(f"Median lap time: {median_time:.3f}s")
        print(f"Standard deviation: {std_time:.3f}s")
        print()
        
        # Lap-by-lap breakdown
        print("🔄 Lap-by-Lap Breakdown:")
        print("="*40)
        print(f"{'Lap':>3} {'Time':>8} {'Gap':>6} {'Compound':>8} {'Tyre Age':>8} {'Notes':>10}")
        print("-" * 55)
        
        for _, lap in data.iterrows():
            lap_time_str = f"{lap['lap_duration_seconds']:.3f}s"
            
            # Calculate gap to fastest lap
            fastest_time = data['lap_duration_seconds'].min()
            gap = lap['lap_duration_seconds'] - fastest_time
            gap_str = f"+{gap:.3f}s" if gap > 0 else "BEST"
            
            # Determine tyre compound emoji
            compound_emoji = {
                'SOFT': '🔴',
                'MEDIUM': '🟡', 
                'HARD': '⚪',
                'INTERMEDIATE': '🟢',
                'WET': '🔵'
            }.get(lap['compound'], '❓')
            
            compound_str = f"{compound_emoji}{lap['compound'][:3]}" if pd.notna(lap['compound']) else "---"
            tyre_age_str = f"{int(lap['tyre_age'])}" if pd.notna(lap['tyre_age']) else "---"
            
            # Notes for special laps
            notes = ""
            if lap['is_pit_out_lap']:
                notes = "PIT OUT"
            elif pd.notna(lap['pit_duration']):
                notes = "PIT STOP"
            elif lap['lap_duration_seconds'] > median_time + 2 * std_time:
                notes = "SLOW LAP"
            
            print(f"{int(lap['lap_number']):>3} {lap_time_str:>8} {gap_str:>6} {compound_str:>8} {tyre_age_str:>8} {notes:>10}")
        
        print()
        
        # Stint analysis
        stint_data = data.groupby('compound').agg({
            'lap_duration_seconds': ['count', 'mean', 'min', 'max'],
            'lap_number': ['min', 'max']
        }).round(3)
        
        if not stint_data.empty:
            print("🏎️ Performance by Tyre Compound:")
            print("="*40)
            for compound in stint_data.index:
                if pd.notna(compound):
                    compound_emoji = {
                        'SOFT': '🔴',
                        'MEDIUM': '🟡', 
                        'HARD': '⚪',
                        'INTERMEDIATE': '🟢',
                        'WET': '🔵'
                    }.get(compound, '❓')
                    
                    laps_count = stint_data.loc[compound, ('lap_duration_seconds', 'count')]
                    avg_time = stint_data.loc[compound, ('lap_duration_seconds', 'mean')]
                    best_time = stint_data.loc[compound, ('lap_duration_seconds', 'min')]
                    
                    print(f"{compound_emoji} {compound}:")
                    print(f"  Laps: {int(laps_count)}")
                    print(f"  Average: {avg_time:.3f}s")
                    print(f"  Best: {best_time:.3f}s")
                    print()
        
        # Performance trends
        print("📈 Performance Analysis:")
        print("="*25)
        
        # First vs last 5 laps comparison (excluding outliers)
        first_5_laps = normal_laps.head(5)['lap_duration_seconds'].mean()
        last_5_laps = normal_laps.tail(5)['lap_duration_seconds'].mean()
        
        print(f"First 5 laps average: {first_5_laps:.3f}s")
        print(f"Last 5 laps average: {last_5_laps:.3f}s")
        print(f"Difference: {last_5_laps - first_5_laps:+.3f}s")
        
        if last_5_laps > first_5_laps:
            print("📉 Pace dropped towards the end of the race")
        else:
            print("📈 Pace improved or maintained towards the end")
        
        print()
        print("="*80)
        print("Analysis complete! 🏁")
    
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    analyze_lap_times_saudi_gp(driver_number=1)
