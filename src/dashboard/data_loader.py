import sqlite3
import pandas as pd
from typing import List, Dict, Optional, Tuple
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time


class F1DataLoader:
    """
    Optimized F1 data loader that fetches only necessary columns with progress tracking
    and multithreading support for efficient data retrieval.
    """
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection_pool = {}
        self.lock = threading.Lock()
    
    def _get_connection(self, thread_id: int) -> sqlite3.Connection:
        """Get a database connection for the current thread."""
        if thread_id not in self.connection_pool:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")  # Enable WAL mode for better concurrency
            self.connection_pool[thread_id] = conn
        return self.connection_pool[thread_id]
    
    def _execute_query(self, query: str, thread_id: int) -> pd.DataFrame:
        """Execute a SQL query and return DataFrame."""
        conn = self._get_connection(thread_id)
        return pd.read_sql_query(query, conn)
    
    def get_race_sessions(self) -> pd.DataFrame:
        """Get all race sessions with circuit information."""
        query = """
        SELECT DISTINCT 
            s.session_key,
            s.meeting_key,
            s.circuit_short_name,
            s.location,
            s.date_start,
            m.meeting_name
        FROM sessions s
        JOIN meetings m ON s.meeting_key = m.meeting_key
        WHERE s.session_name = 'Race'
        ORDER BY s.date_start
        """
        
        with tqdm(desc="Loading race sessions", unit="query") as pbar:
            df = self._execute_query(query, threading.get_ident())
            pbar.update(1)
        
        print(f"✓ Found {len(df)} race sessions")
        return df
    
    def load_position_data(self, session_keys: List[int]) -> pd.DataFrame:
        """Load position data for specified sessions with multithreading."""
        
        def fetch_positions_batch(session_batch: List[int], thread_id: int) -> pd.DataFrame:
            """Fetch positions for a batch of sessions."""
            session_keys_str = ','.join(map(str, session_batch))
            query = f"""
            SELECT 
                p.session_key,
                p.driver_number,
                p.position,
                p.date
            FROM position p
            WHERE p.session_key IN ({session_keys_str})
            ORDER BY p.date DESC
            """
            return self._execute_query(query, thread_id)
        
        # Split session keys into batches for parallel processing
        batch_size = max(1, len(session_keys) // 4)  # 4 threads
        batches = [session_keys[i:i + batch_size] for i in range(0, len(session_keys), batch_size)]
        
        position_dfs = []
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            with tqdm(desc="Loading position data", total=len(batches), unit="batch") as pbar:
                future_to_batch = {
                    executor.submit(fetch_positions_batch, batch, i): batch 
                    for i, batch in enumerate(batches)
                }
                
                for future in as_completed(future_to_batch):
                    try:
                        df = future.result()
                        position_dfs.append(df)
                        pbar.update(1)
                    except Exception as e:
                        print(f"Error loading position batch: {e}")
        
        # Combine all DataFrames
        combined_df = pd.concat(position_dfs, ignore_index=True) if position_dfs else pd.DataFrame()
        
        # Get final positions (latest timestamp per session/driver)
        if not combined_df.empty:
            # Fix: Use format='ISO8601' for better datetime parsing
            combined_df['date'] = pd.to_datetime(combined_df['date'], format='ISO8601')
            final_positions = combined_df.loc[
                combined_df.groupby(['session_key', 'driver_number'])['date'].idxmax()
            ]
            print(f"✓ Loaded {len(final_positions)} final position records")
            return final_positions[['session_key', 'driver_number', 'position']]
        
        return combined_df
    
    def load_driver_data(self, session_keys: List[int]) -> pd.DataFrame:
        """Load driver information for specified sessions."""
        
        def fetch_drivers_batch(session_batch: List[int], thread_id: int) -> pd.DataFrame:
            """Fetch driver data for a batch of sessions."""
            session_keys_str = ','.join(map(str, session_batch))
            query = f"""
            SELECT DISTINCT
                d.driver_number,
                d.name_acronym,
                d.full_name,
                d.team_name,
                d.team_colour,
                d.session_key
            FROM drivers d
            WHERE d.session_key IN ({session_keys_str})
            """
            return self._execute_query(query, thread_id)
        
        # Split into batches for parallel processing
        batch_size = max(1, len(session_keys) // 4)
        batches = [session_keys[i:i + batch_size] for i in range(0, len(session_keys), batch_size)]
        
        driver_dfs = []
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            with tqdm(desc="Loading driver data", total=len(batches), unit="batch") as pbar:
                future_to_batch = {
                    executor.submit(fetch_drivers_batch, batch, i): batch 
                    for i, batch in enumerate(batches)
                }
                
                for future in as_completed(future_to_batch):
                    try:
                        df = future.result()
                        driver_dfs.append(df)
                        pbar.update(1)
                    except Exception as e:
                        print(f"Error loading driver batch: {e}")
        
        # Combine and deduplicate
        combined_df = pd.concat(driver_dfs, ignore_index=True) if driver_dfs else pd.DataFrame()
        
        if not combined_df.empty:
            # Remove duplicates, keeping the most recent entry per driver
            drivers_unique = combined_df.drop_duplicates(
                subset=['driver_number'], 
                keep='last'
            )
            print(f"✓ Loaded {len(drivers_unique)} unique drivers")
            return drivers_unique
        
        return combined_df
    
    def load_race_results_data(self) -> pd.DataFrame:
        """
        Load complete race results data optimized for Position vs Grand Prix plot.
        Returns a DataFrame with columns: circuit_short_name, driver_number, name_acronym, 
        full_name, team_name, team_colour, position, date_start
        """
        
        print("🏎️  Loading F1 2024 Race Results Data...")
        print("=" * 50)
        
        # Step 1: Get race sessions
        race_sessions = self.get_race_sessions()
        
        if race_sessions.empty:
            print("❌ No race sessions found!")
            return pd.DataFrame()
        
        session_keys = race_sessions['session_key'].tolist()
        
        # Step 2: Load position and driver data in parallel
        print("\n📊 Loading race data with multithreading...")
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            position_future = executor.submit(self.load_position_data, session_keys)
            driver_future = executor.submit(self.load_driver_data, session_keys)
            
            positions_df = position_future.result()
            drivers_df = driver_future.result()
        
        if positions_df.empty or drivers_df.empty:
            print("❌ Failed to load position or driver data!")
            return pd.DataFrame()
        
        # Step 3: Merge all data
        print("\n🔄 Combining race results...")
        with tqdm(desc="Merging data", unit="step") as pbar:
            # Merge positions with race sessions
            results = positions_df.merge(
                race_sessions[['session_key', 'circuit_short_name', 'date_start', 'meeting_name']], 
                on='session_key', 
                how='left'
            )
            pbar.update(1)
            
            # Merge with driver information
            results = results.merge(
                drivers_df[['driver_number', 'name_acronym', 'full_name', 'team_name', 'team_colour']], 
                on='driver_number', 
                how='left'
            )
            pbar.update(1)
        
        # Step 4: Clean and sort data
        results = results.dropna(subset=['position', 'name_acronym', 'team_colour'])
        
        # Parse date_start with ISO8601 format
        if 'date_start' in results.columns:
            results['date_start'] = pd.to_datetime(results['date_start'], format='ISO8601')
        
        results = results.sort_values(['date_start', 'position'])
        
        print(f"\n✅ Successfully loaded race results!")
        print(f"   📈 {len(results)} race results")
        print(f"   🏁 {results['circuit_short_name'].nunique()} circuits")
        print(f"   🏎️  {results['name_acronym'].nunique()} drivers")
        print(f"   🏆 {results['team_name'].nunique()} teams")
        print("=" * 50)
        
        return results
    
    def close_connections(self):
        """Close all database connections."""
        for conn in self.connection_pool.values():
            conn.close()
        self.connection_pool.clear()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connections()
