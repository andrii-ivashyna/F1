import sqlite3
import pandas as pd
from datetime import datetime

def track_race_positions(db_name="f1db_YR=2024", grand_prix="Saudi Arabian Grand Prix"):
    # Connect to the database
    db_path = f"data/{db_name}/database.db"
    conn = sqlite3.connect(db_path)
    
    try:
        print(f"🏁 F1 2024 Race Position Tracker - {grand_prix}")
        print("="*80)
        
        # Get all lap data for the specified Grand Prix race
        query = """
        SELECT 
            l.session_key,
            m.meeting_name,
            s.session_name,
            s.date_start as session_start,
            l.date_start as lap_completion_time,
            d.driver_number,
            d.broadcast_name,
            d.team_name,
            l.lap_number,
            l.lap_duration
        FROM laps l
        JOIN sessions s ON l.session_key = s.session_key
        JOIN meetings m ON s.meeting_key = m.meeting_key
        JOIN drivers d ON l.driver_number = d.driver_number AND l.session_key = d.session_key
        WHERE m.meeting_name = ?
            AND s.session_name = 'Race'
            AND l.lap_duration IS NOT NULL
            AND l.date_start IS NOT NULL
        ORDER BY l.lap_number, l.date_start
        """
        
        data = pd.read_sql_query(query, conn, params=[grand_prix])
        
        if data.empty:
            print(f"No race data found for {grand_prix}")
            return
        
        # Convert date strings to datetime objects for proper sorting
        # Handle mixed datetime formats (some with microseconds, some without)
        data['lap_completion_time'] = pd.to_datetime(data['lap_completion_time'], format='mixed')
        data['session_start'] = pd.to_datetime(data['session_start'], format='mixed')
        
        # Get race info
        race_date = data.iloc[0]['session_start'].strftime('%Y-%m-%d %H:%M:%S')
        total_drivers = data['driver_number'].nunique()
        max_laps = data['lap_number'].max()
        
        print(f"Race Date: {race_date}")
        print(f"Drivers: {total_drivers}")
        print(f"Maximum laps completed: {max_laps}")
        print()
        
        # Create position tracking for each lap
        position_data = []
        
        for lap_num in range(1, max_laps + 1):
            lap_data = data[data['lap_number'] == lap_num].copy()
            
            if lap_data.empty:
                continue
            
            # Sort by lap completion time to determine positions
            lap_data = lap_data.sort_values('lap_completion_time')
            
            # Assign positions
            for pos, (_, row) in enumerate(lap_data.iterrows(), 1):
                position_data.append({
                    'lap_number': lap_num,
                    'position': pos,
                    'driver_number': row['driver_number'],
                    'driver_name': row['broadcast_name'],
                    'team_name': row['team_name'],
                    'lap_completion_time': row['lap_completion_time'],
                    'lap_duration': row['lap_duration']
                })
        
        # Convert to DataFrame
        positions_df = pd.DataFrame(position_data)
        
        # Display position changes throughout the race
        print("🏆 Final Race Classification:")
        print("="*50)
        
        # Get final positions (last lap each driver completed)
        final_positions = []
        for driver_num in positions_df['driver_number'].unique():
            driver_data = positions_df[positions_df['driver_number'] == driver_num]
            last_lap_data = driver_data.loc[driver_data['lap_number'].idxmax()]
            
            final_positions.append({
                'final_position': last_lap_data['position'],
                'driver_number': driver_num,
                'driver_name': last_lap_data['driver_name'],
                'team_name': last_lap_data['team_name'],
                'laps_completed': last_lap_data['lap_number']
            })
        
        final_df = pd.DataFrame(final_positions)
        final_df = final_df.sort_values('final_position')
        
        print(f"{'Pos':>3} {'Driver':>3} {'Name':<20} {'Team':<25} {'Laps':>4}")
        print("-" * 65)
        
        for _, row in final_df.iterrows():
            print(f"{row['final_position']:>3} #{row['driver_number']:>2} {row['driver_name']:<20} {row['team_name']:<25} {row['laps_completed']:>4}")
        
        print()
        
        # Show position changes for top 10 drivers at key intervals
        print("📊 Position Changes During Race (Top 10):")
        print("="*60)
        
        # Select key laps to show (start, 25%, 50%, 75%, finish)
        key_laps = [1, max_laps // 4, max_laps // 2, (3 * max_laps) // 4, max_laps]
        key_laps = [lap for lap in key_laps if lap > 0]  # Remove any zero values
        
        # Get top 10 drivers by final position
        top_10_drivers = final_df.head(10)['driver_number'].tolist()
        
        print(f"{'Driver':<15}", end="")
        for lap in key_laps:
            print(f"{'L' + str(lap):>6}", end="")
        print()
        print("-" * (15 + 6 * len(key_laps)))
        
        for driver_num in top_10_drivers:
            driver_name = final_df[final_df['driver_number'] == driver_num]['driver_name'].iloc[0][:14]
            print(f"{driver_name:<15}", end="")
            
            for lap in key_laps:
                # Find position at this lap
                lap_pos_data = positions_df[
                    (positions_df['driver_number'] == driver_num) & 
                    (positions_df['lap_number'] == lap)
                ]
                
                if not lap_pos_data.empty:
                    pos = lap_pos_data['position'].iloc[0]
                    print(f"P{pos:>2}   ", end="")
                else:
                    # Driver might not have completed this lap - find their last position
                    last_pos_data = positions_df[
                        (positions_df['driver_number'] == driver_num) & 
                        (positions_df['lap_number'] <= lap)
                    ]
                    if not last_pos_data.empty:
                        last_pos = last_pos_data.iloc[-1]['position']
                        print(f"P{last_pos:>2}*  ", end="")
                    else:
                        print("  ---  ", end="")
            print()
        
        print("\n* = Last known position (driver didn't complete this lap)")
        
        # Biggest position changes
        print("\n🔄 Biggest Position Changes:")
        print("="*40)
        
        position_changes = []
        for driver_num in positions_df['driver_number'].unique():
            driver_data = positions_df[positions_df['driver_number'] == driver_num]
            if len(driver_data) > 1:
                start_pos = driver_data.iloc[0]['position']
                end_pos = driver_data.iloc[-1]['position']
                change = start_pos - end_pos  # Positive = gained positions
                
                position_changes.append({
                    'driver_number': driver_num,
                    'driver_name': driver_data.iloc[0]['driver_name'],
                    'start_position': start_pos,
                    'end_position': end_pos,
                    'change': change
                })
        
        changes_df = pd.DataFrame(position_changes)
        
        # Show biggest gainers
        gainers = changes_df[changes_df['change'] > 0].sort_values('change', ascending=False).head(5)
        if not gainers.empty:
            print("📈 Biggest Gainers:")
            for _, row in gainers.iterrows():
                print(f"  #{row['driver_number']} {row['driver_name']}: P{row['start_position']} → P{row['end_position']} (+{row['change']} positions)")
        
        # Show biggest losers
        losers = changes_df[changes_df['change'] < 0].sort_values('change').head(5)
        if not losers.empty:
            print("\n📉 Biggest Losers:")
            for _, row in losers.iterrows():
                print(f"  #{row['driver_number']} {row['driver_name']}: P{row['start_position']} → P{row['end_position']} ({row['change']} positions)")
        
        print()
        print("="*80)
        print("Position tracking complete! 🏁")
        
        # Return the positions DataFrame for further analysis if needed
        return positions_df
    
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

def get_position_at_lap(positions_df, driver_number, lap_number):
    """Helper function to get a driver's position at a specific lap"""
    result = positions_df[
        (positions_df['driver_number'] == driver_number) & 
        (positions_df['lap_number'] == lap_number)
    ]
    return result['position'].iloc[0] if not result.empty else None

def get_drivers_at_position(positions_df, position, lap_number):
    """Helper function to get which driver was at a specific position during a lap"""
    result = positions_df[
        (positions_df['position'] == position) & 
        (positions_df['lap_number'] == lap_number)
    ]
    return result['driver_name'].iloc[0] if not result.empty else None

if __name__ == "__main__":
    # Run the position tracker
    positions_data = track_race_positions(grand_prix="Saudi Arabian Grand Prix")
    
    # Example usage of helper functions
    if positions_data is not None and not positions_data.empty:
        print("\n🔍 Example Queries:")
        print("="*30)
        
        # Who was leading at lap 10?
        leader_lap_10 = get_drivers_at_position(positions_data, 1, 10)
        if leader_lap_10:
            print(f"Leader at lap 10: {leader_lap_10}")
        
        # What position was driver #1 at lap 20?
        driver_1_pos = get_position_at_lap(positions_data, 1, 20)
        if driver_1_pos:
            print(f"Driver #1 position at lap 20: P{driver_1_pos}")
