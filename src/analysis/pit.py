import sqlite3
import pandas as pd
from collections import defaultdict

def analyze_pitstops():
    # Connect to the database
    db_path = "data/f1db_YR=2024/database.db"
    conn = sqlite3.connect(db_path)
    
    try:
        # Get all race sessions with GP names
        race_sessions_query = """
        SELECT DISTINCT 
            s.session_key,
            s.meeting_key,
            m.meeting_name,
            s.date_start
        FROM sessions s
        JOIN meetings m ON s.meeting_key = m.meeting_key
        WHERE s.session_name = 'Race'
        ORDER BY s.date_start
        LIMIT 3
        """
        
        race_sessions = pd.read_sql_query(race_sessions_query, conn)
        print("First 3 GP Races found:")
        for _, session in race_sessions.iterrows():
            print(f"- {session['meeting_name']} (Session Key: {session['session_key']})")
        print("\n" + "="*80 + "\n")
        
        # Get pit stop data for these race sessions
        session_keys = tuple(race_sessions['session_key'].tolist())
        
        pit_data_query = f"""
        SELECT 
            p.session_key,
            p.driver_number,
            p.lap_number,
            p.pit_duration,
            p.date,
            d.full_name,
            d.team_name,
            m.meeting_name
        FROM pit p
        JOIN drivers d ON p.driver_number = d.driver_number AND p.session_key = d.session_key
        JOIN sessions s ON p.session_key = s.session_key
        JOIN meetings m ON s.meeting_key = m.meeting_key
        WHERE p.session_key IN {session_keys}
        ORDER BY m.date_start, p.driver_number, p.lap_number
        """
        
        pit_data = pd.read_sql_query(pit_data_query, conn)
        
        # Group data by GP and analyze
        for _, race_session in race_sessions.iterrows():
            session_key = race_session['session_key']
            gp_name = race_session['meeting_name']
            
            print(f"🏁 {gp_name}")
            print("=" * len(gp_name) + "==")
            
            # Filter pit data for this GP
            gp_pit_data = pit_data[pit_data['session_key'] == session_key]
            
            if gp_pit_data.empty:
                print("No pit stop data found for this GP\n")
                continue
            
            # Group by driver
            driver_stats = defaultdict(list)
            
            for _, pit_stop in gp_pit_data.iterrows():
                driver_stats[pit_stop['driver_number']].append({
                    'full_name': pit_stop['full_name'],
                    'team_name': pit_stop['team_name'],
                    'lap_number': pit_stop['lap_number'],
                    'pit_duration': pit_stop['pit_duration']
                })
            
            # Print driver statistics
            for driver_number in sorted(driver_stats.keys()):
                stops = driver_stats[driver_number]
                driver_name = stops[0]['full_name']
                team_name = stops[0]['team_name']
                
                print(f"\n🏎️  {driver_name} (#{driver_number}) - {team_name}")
                print(f"    Number of pit stops: {len(stops)}")
                
                total_pit_time = 0
                for i, stop in enumerate(stops, 1):
                    pit_time = stop['pit_duration']
                    total_pit_time += pit_time
                    print(f"    Stop {i}: Lap {stop['lap_number']}, Duration: {pit_time:.3f}s")
                
                if len(stops) > 0:
                    avg_pit_time = total_pit_time / len(stops)
                    print(f"    Total pit time: {total_pit_time:.3f}s")
                    print(f"    Average pit time: {avg_pit_time:.3f}s")
                
                # Find fastest and slowest pit stops
                if len(stops) > 1:
                    fastest_stop = min(stops, key=lambda x: x['pit_duration'])
                    slowest_stop = max(stops, key=lambda x: x['pit_duration'])
                    print(f"    Fastest stop: {fastest_stop['pit_duration']:.3f}s (Lap {fastest_stop['lap_number']})")
                    print(f"    Slowest stop: {slowest_stop['pit_duration']:.3f}s (Lap {slowest_stop['lap_number']})")
            
            # GP Summary Statistics
            print(f"\n📊 {gp_name} - Summary Statistics:")
            print("-" * 40)
            
            if not gp_pit_data.empty:
                total_stops = len(gp_pit_data)
                avg_pit_duration = gp_pit_data['pit_duration'].mean()
                fastest_overall = gp_pit_data['pit_duration'].min()
                slowest_overall = gp_pit_data['pit_duration'].max()
                
                print(f"Total pit stops: {total_stops}")
                print(f"Average pit duration: {avg_pit_duration:.3f}s")
                print(f"Fastest pit stop: {fastest_overall:.3f}s")
                print(f"Slowest pit stop: {slowest_overall:.3f}s")
                
                # Find who had the fastest and slowest stops
                fastest_driver = gp_pit_data[gp_pit_data['pit_duration'] == fastest_overall].iloc[0]
                slowest_driver = gp_pit_data[gp_pit_data['pit_duration'] == slowest_overall].iloc[0]
                
                print(f"Fastest stop by: {fastest_driver['full_name']} (Lap {fastest_driver['lap_number']})")
                print(f"Slowest stop by: {slowest_driver['full_name']} (Lap {slowest_driver['lap_number']})")
            
            print("\n" + "="*80 + "\n")
    
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    print("🏁 F1 2024 Pit Stop Analysis - First 3 GP Races")
    print("="*80)
    analyze_pitstops()
