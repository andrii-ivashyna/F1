#!/usr/bin/env python3
"""
Table Analyzer Module
Analyzes table structures, column types, and basic statistics
"""

import sqlite3
from typing import Dict, Any, List
from collections import defaultdict
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

class TableAnalyzer:
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def get_connection(self):
        """Get database connection"""
        return sqlite3.connect(self.db_path)
    
    def get_table_list(self) -> List[str]:
        """Get list of all tables in the database"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            return [row[0] for row in cursor.fetchall()]
    
    def analyze_table_structure(self, table_name: str) -> Dict[str, Any]:
        """Analyze structure of a specific table"""
        # This method creates its own connection, making it suitable for threading.
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get table info
            cursor.execute(f"PRAGMA table_info(`{table_name}`)") # Quoted table_name
            columns_info = cursor.fetchall()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`") # Quoted table_name
            row_count = cursor.fetchone()[0]
            
            # Analyze each column
            columns_analysis = {}
            for col_info in columns_info:
                col_name = col_info[1]
                col_type = col_info[2]
                not_null = bool(col_info[3])
                default_val = col_info[4]
                is_pk = bool(col_info[5])
                
                columns_analysis[col_name] = {
                    "type": col_type,
                    "not_null": not_null,
                    "default_value": default_val,
                    "is_primary_key": is_pk,
                    "statistics": self._analyze_column_stats(table_name, col_name, col_type, conn) # Pass conn
                }
            
            return {
                "table_name": table_name,
                "row_count": row_count,
                "column_count": len(columns_info),
                "columns": columns_analysis,
                "indexes": self._get_table_indexes(table_name, conn), # Pass conn
                "foreign_keys": self._get_foreign_keys(table_name, conn) # Pass conn
            }
    
    def _analyze_column_stats(self, table_name: str, col_name: str, col_type: str, conn) -> Dict[str, Any]:
        """Analyze statistics for a specific column using provided connection"""
        cursor = conn.cursor() # Use passed connection's cursor
        
        stats = {
            "distinct_count": 0,
            "null_count": 0,
            "min_value": None,
            "max_value": None,
            "avg_value": None,
            "sample_values": []
        }
        
        try:
            # Distinct count
            cursor.execute(f"SELECT COUNT(DISTINCT `{col_name}`) FROM `{table_name}`")
            stats["distinct_count"] = cursor.fetchone()[0]
            
            # Null count
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}` WHERE `{col_name}` IS NULL")
            stats["null_count"] = cursor.fetchone()[0]
            
            # Min/Max for numeric and text columns
            if col_type.upper() in ['INTEGER', 'REAL', 'NUMERIC']:
                cursor.execute(f"SELECT MIN(`{col_name}`), MAX(`{col_name}`), AVG(`{col_name}`) FROM `{table_name}` WHERE `{col_name}` IS NOT NULL")
                result = cursor.fetchone()
                if result and result[0] is not None: # Added check for result
                    stats["min_value"] = result[0]
                    stats["max_value"] = result[1]
                    stats["avg_value"] = result[2]
            elif col_type.upper() == 'TEXT':
                cursor.execute(f"SELECT MIN(LENGTH(`{col_name}`)), MAX(LENGTH(`{col_name}`)), AVG(LENGTH(`{col_name}`)) FROM `{table_name}` WHERE `{col_name}` IS NOT NULL")
                result = cursor.fetchone()
                if result and result[0] is not None: # Added check for result
                    stats["min_length"] = result[0]
                    stats["max_length"] = result[1]
                    stats["avg_length"] = result[2]
            
            # Sample values (up to 10)
            cursor.execute(f"SELECT DISTINCT `{col_name}` FROM `{table_name}` WHERE `{col_name}` IS NOT NULL LIMIT 10")
            stats["sample_values"] = [row[0] for row in cursor.fetchall()]
            
        except Exception as e:
            stats["error"] = str(e)
        
        return stats
    
    def _get_table_indexes(self, table_name: str, conn) -> List[Dict[str, Any]]:
        """Get indexes for a table using provided connection"""
        cursor = conn.cursor() # Use passed connection's cursor
        cursor.execute(f"PRAGMA index_list(`{table_name}`)") # Quoted table_name
        indexes = []
        
        # PRAGMA index_list can return an empty list if table doesn't exist or has no indexes.
        # Need to handle fetchall() on empty cursor result for index_info.
        index_list_result = cursor.fetchall()

        for index_info_row in index_list_result:
            index_name = index_info_row[1]
            # It's possible index_info is not a cursor but the result of fetchall
            # So, we need to execute PRAGMA index_info for each index name
            cursor.execute(f"PRAGMA index_info(`{index_name}`)") # Quoted index_name
            columns = [col_info[2] for col_info in cursor.fetchall()]
            
            indexes.append({
                "name": index_name,
                "unique": bool(index_info_row[2]),
                "columns": columns
            })
        
        return indexes
    
    def _get_foreign_keys(self, table_name: str, conn) -> List[Dict[str, Any]]:
        """Get foreign keys for a table using provided connection"""
        cursor = conn.cursor() # Use passed connection's cursor
        cursor.execute(f"PRAGMA foreign_key_list(`{table_name}`)") # Quoted table_name
        
        foreign_keys = []
        for fk_info in cursor.fetchall():
            foreign_keys.append({
                "column": fk_info[3],
                "references_table": fk_info[2],
                "references_column": fk_info[4]
            })
        
        return foreign_keys
    
    def analyze_all_tables(self) -> Dict[str, Dict[str, Any]]:
        """Analyze all tables in the database using multithreading"""
        tables = self.get_table_list()
        results = {}
        max_workers = os.cpu_count() or 4 # Sensible default for I/O bound tasks

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_table = {executor.submit(self.analyze_table_structure, table_name): table_name for table_name in tables}
            
            for future in tqdm(as_completed(future_to_table), total=len(tables), desc="Analyzing table structures"):
                table_name = future_to_table[future]
                try:
                    results[table_name] = future.result()
                except Exception as e:
                    results[table_name] = {"error": f"Error analyzing table {table_name}: {str(e)}"}
                    print(f"Error analyzing table {table_name}: {e}") # Log error to console
        
        return results
    
    def get_database_schema_summary(self) -> Dict[str, Any]:
        """Get high-level database schema summary"""
        tables = self.get_table_list()
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            total_records = 0
            table_sizes = {}
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM `{table}`") # Quoted table
                count = cursor.fetchone()[0]
                total_records += count
                table_sizes[table] = count
            
            return {
                "total_tables": len(tables),
                "total_records": total_records,
                "table_sizes": table_sizes,
                "largest_table": max(table_sizes.items(), key=lambda x: x[1]) if table_sizes else None,
                "smallest_table": min(table_sizes.items(), key=lambda x: x[1]) if table_sizes else None
            }
