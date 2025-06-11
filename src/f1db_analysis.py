"""
Optimized F1 Database Statistical Analysis Script
Performs comprehensive statistical analysis with a unified approach and structured output
"""

import sqlite3
import json
import shutil
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from typing import Dict, List, Any, Optional
import threading
from dataclasses import dataclass, asdict

# --- Configuration ---
DB_NAME = "f1db_YR=2024"
BASE_PATH = Path("data") / DB_NAME
DB_PATH = BASE_PATH / "database.db"
ANALYSIS_PATH = BASE_PATH / "analysis/"
MAX_WORKERS = 8
SAMPLE_SIZE = 50000  # For large tables

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class AnalysisResult:
    """Data class for holding analysis results."""
    name: str
    data: Any
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()

class SQLiteManager:
    """Optimized SQLite connection and query manager."""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        
    def get_connection(self) -> sqlite3.Connection:
        """Get an optimized database connection that prevents temporary file creation."""
        conn = sqlite3.connect(
            f"file:{self.db_path}?mode=ro&immutable=1", 
            uri=True, 
            check_same_thread=False
        )
        
        # Configure to prevent temporary files and optimize performance
        optimizations = [
            "PRAGMA journal_mode=OFF",
            "PRAGMA synchronous=OFF",
            "PRAGMA temp_store=MEMORY",
            "PRAGMA cache_size=-10000",
            "PRAGMA mmap_size=0"
        ]
        
        for pragma in optimizations:
            conn.execute(pragma)
        
        return conn
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """Execute a query and return results as DataFrame."""
        with self.get_connection() as conn:
            return pd.read_sql(query, conn, params=params)
    
    def get_table_names(self, schema_filter: Dict[str, Any]) -> List[str]:
        """Get available table names that match the schema filter."""
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        tables_df = self.execute_query(query)
        return [name for name in tables_df['name'].tolist() if name in schema_filter]
    
    def get_table_count(self, table_name: str) -> int:
        """Get total row count for a table."""
        try:
            result = self.execute_query(f"SELECT COUNT(*) as total_count FROM {table_name}")
            return int(result.iloc[0]['total_count'])
        except Exception as e:
            logger.warning(f"Could not get row count for {table_name}: {e}")
            return 0
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """Get column names for a table."""
        columns_info = self.execute_query(f"PRAGMA table_info({table_name})")
        return columns_info['name'].tolist()
    
    def sample_table(self, table_name: str, sample_size: int) -> pd.DataFrame:
        """Get a sample from a table using simple random sampling."""
        query = f"SELECT * FROM {table_name} ORDER BY RANDOM() LIMIT {sample_size}"
        return self.execute_query(query)

class F1DatabaseAnalyzer:
    """Optimized F1 Database Statistical Analysis Engine."""
    
    # Define column types for each table to guide the analysis
    TABLE_SCHEMA = {
        'meetings': {'categorical': ['circuit_short_name', 'country_code', 'country_name', 'location', 'meeting_name', 'meeting_official_name', 'gmt_offset'], 'numeric': ['circuit_key', 'country_key', 'year'], 'date': ['date_start'], 'foreign_keys': ['meeting_key']},
        'sessions': {'categorical': ['circuit_short_name', 'country_code', 'country_name', 'location', 'session_name', 'session_type', 'gmt_offset'], 'numeric': ['circuit_key', 'country_key', 'year'], 'date': ['date_start', 'date_end'], 'foreign_keys': ['session_key', 'meeting_key']},
        'drivers': {'categorical': ['broadcast_name', 'country_code', 'first_name', 'last_name', 'full_name', 'name_acronym', 'team_name', 'team_colour', 'headshot_url'], 'numeric': [], 'foreign_keys': ['driver_number', 'meeting_key', 'session_key']},
        'intervals': {'categorical': [], 'numeric': ['gap_to_leader', 'interval'], 'date': ['date'], 'foreign_keys': ['driver_number', 'meeting_key', 'session_key']},
        'laps': {'categorical': ['segments_sector_1', 'segments_sector_2', 'segments_sector_3'], 'numeric': ['lap_duration', 'duration_sector_1', 'duration_sector_2', 'duration_sector_3', 'i1_speed', 'i2_speed', 'st_speed', 'lap_number'], 'boolean': ['is_pit_out_lap'], 'date': ['date_start'], 'foreign_keys': ['driver_number', 'meeting_key', 'session_key']},
        'position': {'categorical': [], 'numeric': ['position'], 'date': ['date'], 'foreign_keys': ['driver_number', 'meeting_key', 'session_key']},
        'pit': {'categorical': [], 'numeric': ['pit_duration', 'lap_number'], 'date': ['date'], 'foreign_keys': ['driver_number', 'meeting_key', 'session_key']},
        'stints': {'categorical': ['compound'], 'numeric': ['lap_start', 'lap_end', 'stint_number', 'tyre_age_at_start'], 'foreign_keys': ['driver_number', 'meeting_key', 'session_key']},
        'team_radio': {'categorical': [], 'numeric': [], 'date': ['date'], 'foreign_keys': ['driver_number', 'meeting_key', 'session_key']},
        'location': {'categorical': [], 'numeric': ['x', 'y', 'z'], 'date': ['date'], 'foreign_keys': ['driver_number', 'meeting_key', 'session_key']},
        'car_data': {'categorical': [], 'numeric': ['speed', 'rpm', 'throttle', 'brake', 'drs'], 'date': ['date'], 'foreign_keys': ['driver_number', 'meeting_key', 'session_key']},
        'weather': {'categorical': [], 'numeric': ['air_temperature', 'track_temperature', 'humidity', 'wind_speed', 'wind_direction', 'pressure', 'rainfall'], 'date': ['date'], 'foreign_keys': ['meeting_key', 'session_key']},
        'race_control': {'categorical': ['category', 'flag', 'scope', 'sector', 'message'], 'numeric': ['lap_number'], 'date': ['date'], 'foreign_keys': ['driver_number', 'meeting_key', 'session_key']}
    }
    
    def __init__(self, db_path: Path, analysis_path: Path):
        self.db_path = db_path
        self.analysis_path = analysis_path
        self.lock = threading.Lock()
        self.sql_manager = SQLiteManager(db_path)
        
        # Clean and recreate analysis directory
        if self.analysis_path.exists():
            shutil.rmtree(self.analysis_path)
        self.analysis_path.mkdir(parents=True, exist_ok=True)
        
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found at {self.db_path}")
        
        self.available_tables = self.sql_manager.get_table_names(self.TABLE_SCHEMA)
        self.table_row_counts = self._get_table_row_counts()
        logger.info(f"Found {len(self.available_tables)} tables in database.")
    
    def _get_table_row_counts(self) -> Dict[str, int]:
        """Get row counts for all tables."""
        return {table: self.sql_manager.get_table_count(table) for table in self.available_tables}
    
    def _get_table_sample(self, table_name: str) -> tuple[pd.DataFrame, bool]:
        """Get a sample from a table. Returns (dataframe, was_sampled)."""
        total_rows = self.table_row_counts.get(table_name, 0)
        
        if total_rows == 0:
            return pd.DataFrame(), False
        
        # Simple sampling logic - if larger than SAMPLE_SIZE, sample exactly SAMPLE_SIZE
        if total_rows <= SAMPLE_SIZE:
            df = self.sql_manager.execute_query(f"SELECT * FROM {table_name}")
            return df, False
        else:
            df = self.sql_manager.sample_table(table_name, SAMPLE_SIZE)
            return df, True
    
    def _format_number(self, num: int) -> str:
        """Format number with space separators for thousands."""
        return f"{num:,}".replace(',', ' ')
    
    def _calculate_date_duration(self, series: pd.Series) -> Optional[str]:
        """Calculate duration between min and max date values in ISO 8601 format."""
        try:
            valid_dates = pd.to_datetime(series.dropna(), errors='coerce').dropna()
            if len(valid_dates) < 2:
                return None
            
            min_date = valid_dates.min()
            max_date = valid_dates.max()
            duration = max_date - min_date
            
            # Convert to ISO 8601 duration format
            days = duration.days
            seconds = duration.seconds
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            seconds = seconds % 60
            
            return f"P{days}DT{hours}H{minutes}M{seconds}S"
        except Exception:
            return None
    
    def _calculate_stats(self, series: pd.Series, column_name: str, data_type: str) -> Dict[str, Any]:
        """Unified statistics calculation for a given pandas Series."""
        total_count = len(series)
        null_count = series.isna().sum()
        stats = {
            f"{column_name}_total_count": total_count,
            f"{column_name}_null_count": int(null_count)
        }
        
        valid_series = series.dropna()
        if valid_series.empty:
            return stats

        if data_type == 'numeric':
            numeric_series = pd.to_numeric(valid_series, errors='coerce').dropna()
            if not numeric_series.empty:
                stats.update({
                    f"{column_name}_mean": round(float(numeric_series.mean()), 4),
                    f"{column_name}_median": round(float(numeric_series.median()), 4),
                    f"{column_name}_std": round(float(numeric_series.std()), 4),
                    f"{column_name}_min": float(numeric_series.min()),
                    f"{column_name}_max": float(numeric_series.max()),
                    f"{column_name}_q25": round(float(numeric_series.quantile(0.25)), 4),
                    f"{column_name}_q75": round(float(numeric_series.quantile(0.75)), 4),
                })
        elif data_type == 'categorical':
            stats[f"{column_name}_unique_count"] = valid_series.nunique()
            if column_name not in ['date', 'recording_url']:
                value_counts = valid_series.value_counts().head(3)  # Changed from 10 to 3
                stats[f"{column_name}_top_values"] = {str(k): int(v) for k, v in value_counts.items()}
        elif data_type == 'boolean':
            bool_series = valid_series.astype(bool)
            true_count = bool_series.sum()
            stats[f"{column_name}_true_count"] = int(true_count)
        elif data_type == 'date':
            duration = self._calculate_date_duration(valid_series)
            if duration:
                stats[f"{column_name}_duration"] = duration
        elif data_type == 'foreign_key':
            # For foreign keys, only count and nulls (already handled above)
            pass

        return stats
    
    def _get_table_specific_metrics(self, table_name: str, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate table-specific performance metrics for deeper insights."""
        metrics = {}
        try:
            if table_name == 'laps' and 'lap_duration' in df.columns:
                laps = pd.to_numeric(df['lap_duration'], errors='coerce').dropna()
                if not laps.empty and laps.mean() > 0:
                    metrics['lap_consistency_cv'] = round((laps.std() / laps.mean()), 4)
        except Exception as e:
            logger.warning(f"Could not calculate specific metrics for {table_name}: {e}")
        return metrics

    def analyze_table(self, table_name: str) -> AnalysisResult:
        """Analyzes a single database table and returns structured results."""
        try:
            total_records = self.table_row_counts.get(table_name, 0)
            df, was_sampled = self._get_table_sample(table_name)
            
            if df.empty:
                return AnalysisResult(
                    name=f"{table_name}_statistics", 
                    data={"error": f"No data in {table_name}"}
                )

            schema = self.TABLE_SCHEMA.get(table_name, {})
            analysis = {
                'table_total_records': len(df) if was_sampled else total_records
            }
            
            # Calculate table-level nulls
            if len(df) > 0:  
                null_rows_count = df.isnull().all(axis=1).sum()
                analysis['table_null_records'] = int(null_rows_count)
            else:
                analysis['table_null_records'] = 0

            # Process columns based on schema
            for col_type, columns in schema.items():
                if col_type in ['numeric', 'categorical', 'boolean', 'date']:
                    for col in columns:
                        if col in df.columns:
                            analysis.update(self._calculate_stats(df[col], col, col_type))
                elif col_type == 'foreign_keys':
                    # Process foreign keys
                    for col in columns:
                        if col in df.columns:
                            fk_stats = self._calculate_stats(df[col], f"FK_{col}", 'foreign_key')
                            analysis.update(fk_stats)
            
            analysis.update(self._get_table_specific_metrics(table_name, df))
            
            return AnalysisResult(
                name=f"{table_name}_statistics", 
                data=analysis
            )
        except Exception as e:
            logger.error(f"Error analyzing {table_name}: {e}")
            return AnalysisResult(
                name=f"{table_name}_statistics", 
                data={"error": str(e)}
            )

    def _convert_for_json(self, data):
        """Recursively converts numpy types to native Python types for JSON serialization."""
        if isinstance(data, dict):
            return {str(k): self._convert_for_json(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._convert_for_json(item) for item in data]
        elif isinstance(data, (np.integer, np.int64)):
            return int(data)
        elif isinstance(data, (np.floating, np.float64)):
            return float(data)
        elif pd.isna(data):
            return None
        return data
    
    def save_analysis(self, result: AnalysisResult):
        """Saves an analysis result to a JSON file, ensuring thread safety."""
        with self.lock:
            try:
                filepath = self.analysis_path / f"{result.name}.json"
                result_dict = asdict(result)
                # Ensure all data is JSON serializable
                result_dict['data'] = self._convert_for_json(result_dict['data'])
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(result_dict, f, indent=2, ensure_ascii=False)
                logger.info(f"✅ Saved: {result.name}")
            except Exception as e:
                logger.error(f"❌ Error saving {result.name}: {e}")
    
    def run_analysis(self):
        """Main function to run the entire analysis pipeline."""
        print(f"🚀 Starting F1 Database Analysis on {self.db_path.name}")
        print(f"📊 Analyzing {len(self.available_tables)} tables into {self.analysis_path}")
        print(f"📈 Total records across all tables: {self._format_number(sum(self.table_row_counts.values()))}")
        print("-" * 80)
        
        # Concurrently analyze all tables
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_table = {executor.submit(self.analyze_table, table): table for table in self.available_tables}
            table_results = {}
            
            for i, future in enumerate(as_completed(future_to_table), 1):
                table = future_to_table[future]
                try:
                    result = future.result()
                    if result and 'error' not in result.data:
                        self.save_analysis(result)
                        table_results[table] = result.data
                        
                        table_total_records = self.table_row_counts.get(table, 0)
                        analyzed_records = result.data.get('table_total_records', 0)
                        was_sampled = analyzed_records < table_total_records
                        
                        if was_sampled:
                            sample_info = f"(sampled: {self._format_number(analyzed_records)} / total: {self._format_number(table_total_records)})"
                        else:
                            sample_info = f"(total: {self._format_number(table_total_records)})"
                        
                        print(f"✅ [{i:2d}/{len(self.available_tables)}] {table:<15} | {sample_info}")
                    else:
                        error_msg = result.data.get('error', 'Unknown error') if result else 'No result'
                        print(f"❌ [{i:2d}/{len(self.available_tables)}] {table:<15} | FAILED: {error_msg}")
                except Exception as e:
                    logger.error(f"Table {table} analysis threw an exception: {e}")
                    print(f"💥 [{i:2d}/{len(self.available_tables)}] {table:<15} | EXCEPTION: {str(e)[:50]}...")
        
        # --- Generate Final Summary Report ---
        try:
            # Build tables_overview
            tables_overview = {}
            for table in self.available_tables:
                table_count = self.table_row_counts.get(table, 0)
                tables_overview[f"{table}_total_records"] = table_count
                
                res = table_results.get(table, {})
                tables_overview[f"{table}_null_records"] = res.get('table_null_records', 0)

            # Build general section
            general = {
                'database_path': str(self.db_path),
                'total_records': sum(self.table_row_counts.values()),
                'sample_size': SAMPLE_SIZE,
                'tables_available': len(self.available_tables),
                'tables_completed': len(table_results)
            }
            
            # Swapped order: tables_overview first, then files_generated
            summary = AnalysisResult(
                name="analysis_summary",
                data={
                    'general': general,
                    'tables_overview': tables_overview,
                    'files_generated': [f"{t}_statistics.json" for t in table_results]
                }
            )
            self.save_analysis(summary)
        except Exception as e:
            logger.error(f"Could not generate final summary: {e}")
        
        print("\n" + "=" * 80)
        print("🎯 ANALYSIS COMPLETE!")
        print(f"📁 Reports generated in: {self.analysis_path}")
        print(f"📊 Successfully analyzed {len(table_results)}/{len(self.available_tables)} tables")
        print(f"📈 Total records processed: {self._format_number(sum(self.table_row_counts.values()))}")
        print("=" * 80)

def main():
    """Main execution function."""
    try:
        if not DB_PATH.exists():
            print(f"❌ Database not found at expected path: {DB_PATH}")
            return
        analyzer = F1DatabaseAnalyzer(DB_PATH, ANALYSIS_PATH)
        analyzer.run_analysis()
    except Exception as e:
        logger.critical(f"A fatal error occurred: {e}", exc_info=True)
        print(f"❌ Fatal error during execution: {e}")

if __name__ == "__main__":
    main()
