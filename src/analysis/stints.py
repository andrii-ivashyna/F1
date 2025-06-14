import sqlite3
import pandas as pd

def analyze_stints_three_gps(db_name="f1db_YR=2024"):
    # Connect to the database
    db_path = f"data/{db_name}/database.db"
    conn = sqlite3.connect(db_path)
    
    try:
        print("🏁 F1 2024 Stint Analysis - British GP, Monaco GP & Canadian GP")
        print("="*80)
        
        # Target races
        target_races = ['British Grand Prix', 'Monaco Grand Prix', 'Canadian Grand Prix']
        
        # Get stint data for these three races
        query = """
        SELECT 
            st.session_key,
            m.meeting_name,
            s.session_name,
            s.date_start,
            d.driver_number,
            d.broadcast_name,
            d.team_name,
            st.stint_number,
            st.lap_start,
            st.lap_end,
            st.compound,
            st.tyre_age_at_start,
            (st.lap_end - st.lap_start + 1) as stint_length
        FROM stints st
        JOIN sessions s ON st.session_key = s.session_key
        JOIN meetings m ON s.meeting_key = m.meeting_key
        JOIN drivers d ON st.driver_number = d.driver_number AND st.session_key = d.session_key
        WHERE m.meeting_name IN ('British Grand Prix', 'Monaco Grand Prix', 'Canadian Grand Prix')
            AND s.session_name = 'Race'
        ORDER BY m.date_start, d.broadcast_name, st.stint_number
        """
        
        data = pd.read_sql_query(query, conn)
        
        if data.empty:
            print("No stint data found for the specified races")
            return
        
        print(f"Found {len(data)} stints across the three races\n")
        
        # Analyze each race
        for gp_name in target_races:
            gp_data = data[data['meeting_name'] == gp_name]
            
            if gp_data.empty:
                print(f"🏆 {gp_name} - No data found\n")
                continue
                
            print(f"🏆 {gp_name}")
            print("=" * (len(gp_name) + 2))
            print(f"Date: {gp_data.iloc[0]['date_start']}")
            print(f"Total stints: {len(gp_data)}")
            print()
            
            # Group by driver and show their stint strategy
            for driver_name, driver_data in gp_data.groupby('broadcast_name'):
                team = driver_data.iloc[0]['team_name']
                driver_num = driver_data.iloc[0]['driver_number']
                
                print(f"  #{driver_num:2} {driver_name:15} ({team})")
                
                for _, stint in driver_data.iterrows():
                    compound_emoji = {
                        'SOFT': '🔴',
                        'MEDIUM': '🟡', 
                        'HARD': '⚪',
                        'INTERMEDIATE': '🟢',
                        'WET': '🔵'
                    }.get(stint['compound'], '❓')
                    
                    print(f"      Stint {stint['stint_number']}: Laps {stint['lap_start']:2}-{stint['lap_end']:2} "
                          f"({stint['stint_length']:2} laps) {compound_emoji} {stint['compound']} "
                          f"(Age: {stint['tyre_age_at_start']} laps)")
                
                print()  # Empty line between drivers
            
            # Show stint statistics for this race
            print(f"  📊 Stint Statistics for {gp_name}:")
            print(f"      Average stint length: {gp_data['stint_length'].mean():.1f} laps")
            print(f"      Longest stint: {gp_data['stint_length'].max()} laps")
            print(f"      Shortest stint: {gp_data['stint_length'].min()} laps")
            print(f"      Most common compound: {gp_data['compound'].mode().iloc[0] if not gp_data['compound'].mode().empty else 'N/A'}")
            
            # Compound usage breakdown
            compound_counts = gp_data['compound'].value_counts()
            print(f"      Compound usage:")
            for compound, count in compound_counts.items():
                compound_emoji = {
                    'SOFT': '🔴',
                    'MEDIUM': '🟡', 
                    'HARD': '⚪',
                    'INTERMEDIATE': '🟢',
                    'WET': '🔵'
                }.get(compound, '❓')
                print(f"        {compound_emoji} {compound}: {count} stints")
            
            print("\n" + "="*60 + "\n")
    
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    analyze_stints_three_gps()
