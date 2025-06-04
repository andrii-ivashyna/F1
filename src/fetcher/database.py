"""
Database operations for OpenF1 API Data Fetcher
"""

import sqlite3
import json
import os
import logging
from typing import List, Dict, Any
from .config import TABLE_SCHEMAS, DATA_FOLDER

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_filename: str):
        self.db_path = os.path.join(DATA_FOLDER, db_filename)
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite database with all required tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create progress tracking table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fetch_progress (
                table_name TEXT PRIMARY KEY,
                last_updated TEXT,
                status TEXT
            )
        ''')
        
        # Create all tables
        for table_name, schema in TABLE_SCHEMAS.items():
            cursor.execute(schema)
            logger.info(f"Created/verified table: {table_name}")
        
        conn.commit()
        conn.close()
        logger.info("Database initialization completed")
    
    def insert_data(self, table_name: str, data: List[Dict]):
        """Insert data into specified table with dynamic column handling."""
        if not data:
            return
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get existing table columns
            cursor.execute(f"PRAGMA table_info({table_name})")
            existing_columns = {row[1] for row in cursor.fetchall()}
            
            # Handle JSON arrays in lap segments and other list fields
            processed_data = []
            for record in data:
                processed_record = {}
                for key, value in record.items():
                    if isinstance(value, list):
                        processed_record[key] = json.dumps(value)
                    else:
                        processed_record[key] = value
                processed_data.append(processed_record)
            
            if processed_data:
                # Only use columns that exist in the table
                first_record = processed_data[0]
                valid_columns = [col for col in first_record.keys() if col in existing_columns]
                
                if valid_columns:
                    placeholders = ', '.join(['?' for _ in valid_columns])
                    insert_sql = f"INSERT OR REPLACE INTO {table_name} ({', '.join(valid_columns)}) VALUES ({placeholders})"
                    
                    # Prepare data for insertion
                    rows = []
                    for record in processed_data:
                        row = [record.get(col) for col in valid_columns]
                        rows.append(row)
                    
                    cursor.executemany(insert_sql, rows)
                    conn.commit()
                    logger.info(f"Inserted {len(rows)} records into {table_name}")
                else:
                    logger.warning(f"No valid columns found for {table_name}")
        
        except Exception as e:
            logger.error(f"Error inserting data into {table_name}: {e}")
        finally:
            conn.close()
    
    def is_data_exists(self, table_name: str, key_column: str, key_value: Any) -> bool:
        """Check if data already exists in the table for given key."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {key_column} = ?", (key_value,))
            count = cursor.fetchone()[0]
            return count > 0
        except Exception as e:
            logger.error(f"Error checking if data exists in {table_name}: {e}")
            return False
        finally:
            conn.close()
    
    def get_existing_keys(self, table_name: str, key_column: str) -> List[Any]:
        """Get all existing keys from a table."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute(f"SELECT DISTINCT {key_column} FROM {table_name}")
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting existing keys from {table_name}: {e}")
            return []
        finally:
            conn.close()
    
    def get_session_dates(self, session_key: int) -> tuple:
        """Get date_start and date_end for a specific session."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT date_start, date_end FROM sessions WHERE session_key = ?", (session_key,))
            result = cursor.fetchone()
            return result if result else (None, None)
        except Exception as e:
            logger.error(f"Error getting session dates for session {session_key}: {e}")
            return (None, None)
        finally:
            conn.close()
    
    def get_drivers_for_session(self, session_key: int) -> List[int]:
        """Get all driver numbers that participated in a specific session."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT DISTINCT driver_number FROM drivers WHERE session_key = ? AND driver_number IS NOT NULL", (session_key,))
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting drivers for session {session_key}: {e}")
            return []
        finally:
            conn.close()
    
    def print_summary(self):
        """Print summary of data in database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        tables = [
            'meetings', 'sessions', 'drivers', 'intervals', 'laps',
            'pit', 'position', 'race_control', 'stints', 'team_radio',
            'car_data', 'location', 'weather'
        ]
        
        logger.info("\n" + "="*60)
        logger.info("DATABASE SUMMARY")
        logger.info("="*60)
        
        total_records = 0
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                total_records += count
                logger.info(f"{table:<20}: {count:>15,} records")
            except Exception as e:
                logger.error(f"Error counting {table}: {e}")
        
        logger.info("="*60)
        logger.info(f"{'TOTAL':<20}: {total_records:>15,} records")
        logger.info(f"Database saved to: {self.db_path}")
        logger.info("="*60)
        
        conn.close()
