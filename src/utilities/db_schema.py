#!/usr/bin/env python3
"""
Database Schema Analyzer
Analyzes SQLite database structure and generates JSON schema with multithreading support.
"""

import sqlite3
import json
import os
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm
import time
from typing import Dict, List, Tuple, Any


class DatabaseSchemaAnalyzer:
    def __init__(self, max_workers: int = 8):
        # Set database name as a variable inside the code
        self.db_name = "f1db_YR=2024"
        self.max_workers = max_workers
        self.db_path = Path(f"data/{self.db_name}/database.db")
        self.output_path = Path(f"data/{self.db_name}/schema.json")
        self.lock = Lock()
        self.schema = {}
        
    def validate_database(self) -> bool:
        """Check if database file exists and is accessible."""
        if not self.db_path.exists():
            print(f"❌ Database file not found: {self.db_path}")
            return False
            
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute("SELECT 1")
            return True
        except sqlite3.Error as e:
            print(f"❌ Database error: {e}")
            return False
    
    def get_table_names(self) -> List[str]:
        """Get all table names from the database."""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            return [row[0] for row in cursor.fetchall()]
    
    def analyze_table(self, table_name: str) -> Dict[str, Any]:
        """Analyze a single table structure and row count."""
        with sqlite3.connect(str(self.db_path)) as conn:
            # Get column information (excluding not_null and primary_key)
            cursor = conn.execute(f"PRAGMA table_info({table_name})")
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "name": row[1],
                    "type": row[2] if row[2] else "NULL"
                })
            
            # Get row count
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            return {
                "name": table_name,
                "row_count": row_count,
                "columns": columns
            }
    
    def process_tables_batch(self, table_names: List[str]) -> List[Dict[str, Any]]:
        """Process a batch of tables in parallel."""
        results = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all table analysis tasks
            future_to_table = {
                executor.submit(self.analyze_table, table): table 
                for table in table_names
            }
            
            # Create progress bar
            with tqdm(total=len(table_names), desc="Analyzing tables", 
                     unit="table", ncols=80, colour="green") as pbar:
                
                for future in as_completed(future_to_table):
                    table_name = future_to_table[future]
                    try:
                        result = future.result()
                        results.append(result)
                        pbar.set_postfix(current=table_name[:20])
                        pbar.update(1)
                    except Exception as e:
                        print(f"\n⚠️  Error analyzing table '{table_name}': {e}")
                        pbar.update(1)
        
        return results
    
    def generate_schema(self) -> Dict[str, Any]:
        """Generate complete database schema."""
        print(f"🔍 Analyzing database: {self.db_path}")
        
        # Get all table names
        table_names = self.get_table_names()
        total_tables = len(table_names)
        
        if not table_names:
            print("⚠️  No tables found in database")
            return {"tables": [], "metadata": {"total_tables": 0}}
        
        print(f"📊 Found {total_tables} tables")
        
        # Process tables in parallel
        start_time = time.time()
        table_results = self.process_tables_batch(table_names)
        end_time = time.time()
        
        # Sort results by table name for consistency
        table_results.sort(key=lambda x: x["name"])
        
        # Calculate totals
        total_rows = sum(table["row_count"] for table in table_results)
        total_columns = sum(len(table["columns"]) for table in table_results)
        
        schema = {
            "database_name": self.db_name,
            "tables": table_results,
            "metadata": {
                "total_tables": total_tables,
                "total_rows": total_rows,
                "total_columns": total_columns,
                "analysis_time_seconds": round(end_time - start_time, 2),
                "generated_at": time.strftime("%Y-%m-%d %H:%M:%S")
            }
        }
        
        print(f"✅ Analysis complete in {schema['metadata']['analysis_time_seconds']}s")
        print(f"📈 Summary: {total_tables} tables, {total_rows:,} rows, {total_columns} columns")
        
        return schema
    
    def save_schema(self, schema: Dict[str, Any]) -> bool:
        """Save schema to JSON file."""
        try:
            # Ensure output directory exists
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.output_path, 'w', encoding='utf-8') as f:
                json.dump(schema, f, indent=2, ensure_ascii=False)
            
            print(f"💾 Schema saved to: {self.output_path}")
            print(f"📄 File size: {self.output_path.stat().st_size:,} bytes")
            return True
            
        except Exception as e:
            print(f"❌ Error saving schema: {e}")
            return False
    
    def run(self) -> bool:
        """Main execution method."""
        print("🚀 Database Schema Analyzer")
        print("=" * 50)
        
        # Validate database
        if not self.validate_database():
            return False
        
        try:
            # Generate schema
            schema = self.generate_schema()
            
            # Save to file
            if self.save_schema(schema):
                print("\n🎉 Schema analysis completed successfully!")
                return True
            else:
                return False
                
        except KeyboardInterrupt:
            print("\n⏹️  Analysis interrupted by user")
            return False
        except Exception as e:
            print(f"\n❌ Unexpected error: {e}")
            return False


def main():
    """Main entry point."""
    # Create and run analyzer (no command line arguments needed)
    analyzer = DatabaseSchemaAnalyzer(max_workers=8)
    success = analyzer.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
