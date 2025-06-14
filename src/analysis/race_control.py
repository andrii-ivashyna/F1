import sqlite3
import pandas as pd

def analyze_drs_safety_car_messages(db_name="f1db_YR=2024"):
    # Connect to the database
    db_path = f"data/{db_name}/database.db"
    conn = sqlite3.connect(db_path)
    
    try:
        print("🏁 F1 2024 DRS & Safety Car Messages by Race")
        print("="*80)
        
        # Get race control messages containing DRS or Safety Car
        query = """
        SELECT 
            rc.session_key,
            m.meeting_name,
            s.date_start,
            rc.date,
            rc.lap_number,
            rc.message
        FROM race_control rc
        JOIN sessions s ON rc.session_key = s.session_key
        JOIN meetings m ON s.meeting_key = m.meeting_key
        WHERE s.session_name = 'Race'
            AND (
                LOWER(rc.message) LIKE '%drs%' OR 
                LOWER(rc.message) LIKE '%safety car%' OR 
                LOWER(rc.message) LIKE '%red%' OR
                LOWER(rc.message) LIKE '%standing start%'
            )
        ORDER BY s.date_start, rc.date
        """
        
        data = pd.read_sql_query(query, conn)
        
        if data.empty:
            print("No messages found containing 'DRS' or 'Safety Car'")
            return
        
        print(f"Found {len(data)} messages containing 'DRS' or 'Safety Car'\n")
        
        # Group by race and show chronologically
        for gp_name, gp_data in data.groupby('meeting_name', sort=False):
            print(f"🏆 {gp_name}")
            print("=" * (len(gp_name) + 2))
            
            for _, row in gp_data.iterrows():
                lap_info = f"Lap {int(row['lap_number'])}" if pd.notna(row['lap_number']) else "Pre-race"
                print(f"  {lap_info:>10}: {row['message']}")
            
            print()  # Empty line between races
    
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    analyze_drs_safety_car_messages()
