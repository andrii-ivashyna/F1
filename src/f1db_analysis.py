#!/usr/bin/env python3
"""
Optimized F1 Database Statistical Analysis Script
Performs comprehensive statistical analysis with a unified approach and structured output.
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
from typing import Dict, List, Any
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
    metadata: Dict[str, Any]
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()

class F1DatabaseAnalyzer:
    """Optimized F1 Database Statistical Analysis Engine."""
    
    # Define column types for each table to guide the analysis
    TABLE_SCHEMA = {
        'meetings': {'categorical': ['circuit_short_name', 'country_code', 'country_name', 'location', 'meeting_name', 'meeting_official_name', 'gmt_offset'], 'numeric': ['circuit_key', 'country_key'], 'skip_detailed': ['meeting_key', 'year']},
        'sessions': {'categorical': ['circuit_short_name', 'country_code', 'country_name', 'location', 'session_name', 'session_type', 'gmt_offset'], 'numeric': ['circuit_key', 'country_key'], 'skip_detailed': ['session_key', 'meeting_key', 'year']},
        'drivers': {'categorical': ['broadcast_name', 'country_code', 'first_name', 'last_name', 'full_name', 'name_acronym', 'team_name', 'team_colour', 'headshot_url'], 'numeric': [], 'skip_detailed': ['driver_number', 'meeting_key', 'session_key', 'year']},
        'intervals': {'categorical': [], 'numeric': ['gap_to_leader', 'interval'], 'skip_detailed': ['driver_number', 'meeting_key', 'session_key']},
        'laps': {'categorical': ['segments_sector_1', 'segments_sector_2', 'segments_sector_3'], 'numeric': ['lap_duration', 'duration_sector_1', 'duration_sector_2', 'duration_sector_3', 'i1_speed', 'i2_speed', 'st_speed', 'lap_number'], 'boolean': ['is_pit_out_lap'], 'skip_detailed': ['driver_number', 'meeting_key', 'session_key']},
        'position': {'categorical': [], 'numeric': ['position'], 'skip_detailed': ['driver_number', 'meeting_key', 'session_key']},
        'pit': {'categorical': [], 'numeric': ['pit_duration', 'lap_number'], 'skip_detailed': ['driver_number', 'meeting_key', 'session_key']},
        'stints': {'categorical': ['compound'], 'numeric': ['lap_start', 'lap_end', 'stint_number', 'tyre_age_at_start'], 'skip_detailed': ['driver_number', 'meeting_key', 'session_key']},
        'team_radio': {'categorical': [], 'numeric': [], 'skip_detailed': ['driver_number', 'meeting_key', 'session_key']},
        'location': {'categorical': [], 'numeric': ['x', 'y', 'z'], 'skip_detailed': ['driver_number', 'meeting_key', 'session_key']},
        'car_data': {'categorical': [], 'numeric': ['speed', 'rpm', 'throttle', 'brake', 'drs'], 'skip_detailed': ['driver_number', 'meeting_key', 'session_key']},
        'weather': {'categorical': [], 'numeric': ['air_temperature', 'track_temperature', 'humidity', 'wind_speed', 'wind_direction', 'pressure', 'rainfall'], 'skip_detailed': ['meeting_key', 'session_key']},
        'race_control': {'categorical': ['category', 'flag', 'scope', 'sector', 'message'], 'numeric': ['lap_number'], 'skip_detailed': ['driver_number', 'meeting_key', 'session_key']}
    }
    
    def __init__(self, db_path: Path, analysis_path: Path):
        self.db_path = db_path
        self.analysis_path = analysis_path
        self.lock = threading.Lock()
        
        # Clean and recreate analysis directory
        if self.analysis_path.exists():
            shutil.rmtree(self.analysis_path)
        self.analysis_path.mkdir(parents=True, exist_ok=True)
        
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found at {self.db_path}")
        
        self.available_tables = self._get_available_tables()
        logger.info(f"Found {len(self.available_tables)} tables in database.")
    
    def _get_available_tables(self) -> List[str]:
        """Get the list of available tables in the database that match our schema."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            return [row[0] for row in cursor.fetchall() if row[0] in self.TABLE_SCHEMA]
    
    def get_connection(self) -> sqlite3.Connection:
        """Get an optimized database connection."""
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-10000") # 10MB cache
        return conn
    
    def _get_table_sample(self, table_name: str) -> pd.DataFrame:
        """Get a representative sample from a table, fetching all data for small tables."""
        with self.get_connection() as conn:
            total_rows = pd.read_sql(f"SELECT COUNT(*) FROM {table_name}", conn).iloc[0, 0]
            if total_rows <= SAMPLE_SIZE:
                return pd.read_sql(f"SELECT * FROM {table_name}", conn)
            
            # For large tables, attempt to sample across sessions, otherwise do a random sample
            if 'session_key' in pd.read_sql(f"PRAGMA table_info({table_name})", conn)['name'].values:
                query = f"SELECT * FROM {table_name} WHERE session_key IN (SELECT DISTINCT session_key FROM {table_name} ORDER BY RANDOM() LIMIT 10) ORDER BY RANDOM() LIMIT {SAMPLE_SIZE}"
            else:
                query = f"SELECT * FROM {table_name} ORDER BY RANDOM() LIMIT {SAMPLE_SIZE}"
            return pd.read_sql(query, conn)
            
    def _calculate_stats(self, series: pd.Series, column_name: str, data_type: str) -> Dict[str, Any]:
        """Unified statistics calculation for a given pandas Series."""
        total_count = len(series)
        null_count = series.isna().sum()
        stats = {
            f"{column_name}_count": total_count,
            f"{column_name}_valid_count": total_count - null_count,
            f"{column_name}_null_count": int(null_count),
            f"{column_name}_null_percentage": round(null_count / total_count, 4) if total_count > 0 else 0.0
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
                value_counts = valid_series.value_counts().head(10)
                stats[f"{column_name}_top_values"] = {str(k): int(v) for k, v in value_counts.items()}
        elif data_type == 'boolean':
            bool_series = valid_series.astype(bool)
            true_count = bool_series.sum()
            stats[f"{column_name}_true_count"] = int(true_count)
            stats[f"{column_name}_true_percentage"] = round(true_count / len(bool_series), 4)

        return stats
    
    def _get_table_specific_metrics(self, table_name: str, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate table-specific performance metrics for deeper insights."""
        metrics = {}
        try:
            if table_name == 'laps' and 'lap_duration' in df.columns:
                laps = pd.to_numeric(df['lap_duration'], errors='coerce').dropna()
                if not laps.empty and laps.mean() > 0:
                    metrics['lap_consistency_cv'] = round((laps.std() / laps.mean()), 4)
            elif table_name == 'pit' and 'pit_duration' in df.columns:
                pits = pd.to_numeric(df['pit_duration'], errors='coerce').dropna()
                if not pits.empty:
                    metrics['pit_stop_efficiency_pct'] = {'under_3_sec': round((pits < 3.0).mean(), 4)}
        except Exception as e:
            logger.warning(f"Could not calculate specific metrics for {table_name}: {e}")
        return metrics

    def analyze_table(self, table_name: str) -> AnalysisResult:
        """Analyzes a single database table and returns structured results."""
        try:
            df = self._get_table_sample(table_name)
            if df.empty:
                return AnalysisResult(name=f"{table_name}_statistics", data={"error": f"No data in {table_name}"}, metadata={})

            schema = self.TABLE_SCHEMA.get(table_name, {})
            analysis = {'dataset_size': len(df)}
            
            # Calculate table-level nulls (based on sample, where a row is null if ALL elements are null)
            total_rows = len(df)
            if total_rows > 0:
                null_rows_count = df.isnull().all(axis=1).sum()
                analysis['table_null_records'] = int(null_rows_count)
                analysis['table_null_percentage'] = round(null_rows_count / total_rows, 4)
            else:
                analysis['table_null_records'] = 0
                analysis['table_null_percentage'] = 0.0

            # Process columns based on schema
            for col_type, columns in schema.items():
                if col_type in ['numeric', 'categorical', 'boolean']:
                    for col in columns:
                        if col in df.columns:
                            analysis.update(self._calculate_stats(df[col], col, col_type))
            
            analysis.update(self._get_table_specific_metrics(table_name, df))
            
            return AnalysisResult(name=f"{table_name}_statistics", data=analysis, metadata={"description": f"Statistical analysis of F1 {table_name} data"})
        except Exception as e:
            logger.error(f"Error analyzing {table_name}: {e}")
            return AnalysisResult(name=f"{table_name}_statistics", data={"error": str(e)}, metadata={})
    
    def _get_temporal_coverage(self) -> Dict[str, Any]:
        """Fetches temporal coverage from the meetings table."""
        if 'meetings' not in self.available_tables:
            return {}
        try:
            with self.get_connection() as conn:
                query = "SELECT MIN(date_start) as min, MAX(date_start) as max, COUNT(DISTINCT year) as years FROM meetings"
                res = pd.read_sql(query, conn).iloc[0]
                return {
                    'earliest_date': str(res['min']) if pd.notna(res['min']) else None,
                    'latest_date': str(res['max']) if pd.notna(res['max']) else None,
                    'years_covered': int(res['years']) if pd.notna(res['years']) else 0
                }
        except Exception as e:
            logger.warning(f"Could not analyze temporal coverage: {e}")
            return {}

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
        print("-" * 60)
        
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
                        print(f"✅ {i}/{len(self.available_tables)} - Analyzed {table}")
                    else:
                        print(f"❌ {i}/{len(self.available_tables)} - Failed {table}: {result.data.get('error', 'Unknown')}")
                except Exception as e:
                    logger.error(f"Table {table} analysis threw an exception: {e}")
                    print(f"❌ {i}/{len(self.available_tables)} - Errored on {table}")
        
        # --- Generate Final Summary Report ---
        try:
            database_overview = {}
            with self.get_connection() as conn:
                table_counts = {tbl: pd.read_sql(f"SELECT COUNT(*) as c FROM {tbl}", conn).iloc[0]['c'] for tbl in self.available_tables}

            # Populate the overview dict, ensuring the desired order
            for table in self.available_tables:
                database_overview[f"{table}_records"] = int(table_counts.get(table, 0))
                res = table_results.get(table, {})
                database_overview[f"{table}_null_records"] = res.get('table_null_records')
                database_overview[f"{table}_null_percentage"] = res.get('table_null_percentage')

            # Add aggregate stats
            database_overview['total_records'] = sum(table_counts.values())
            database_overview['tables_with_data'] = sum(1 for v in table_counts.values() if v > 0)
            database_overview['completeness_pct'] = round((database_overview['tables_with_data'] / len(self.available_tables)) * 100, 2) if self.available_tables else 0.0
            database_overview['temporal_coverage'] = self._get_temporal_coverage()
            
            summary = AnalysisResult(
                name="analysis_summary",
                data={
                    'analysis_metadata': {
                        'timestamp': datetime.now().isoformat(),
                        'database_path': str(self.db_path),
                        'sample_size_used': SAMPLE_SIZE,
                        'tables_analyzed': len(table_results),
                        'total_tables_available': len(self.available_tables)
                    },
                    'database_overview': database_overview,
                    'files_generated': [f"{t}_statistics.json" for t in table_results],
                    'analysis_approach': 'Unified statistical analysis with optimized sampling'
                },
                metadata={"description": "Complete analysis summary with database overview"}
            )
            self.save_analysis(summary)
        except Exception as e:
            logger.error(f"Could not generate final summary: {e}")
        
        print("\n" + "=" * 60)
        print("🎯 ANALYSIS COMPLETE!")
        print(f"📁 Reports generated in: {self.analysis_path}")
        print("=" * 60)

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
