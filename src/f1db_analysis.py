#!/usr/bin/env python3
"""
Optimized F1 Database Statistical Analysis Script
Performs comprehensive statistical analysis with unified approach.
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

# Configuration
DB_NAME = "f1db_YR=2024"
BASE_PATH = Path("data") / DB_NAME
DB_PATH = BASE_PATH / "database.db"
ANALYSIS_PATH = BASE_PATH / "analysis"
MAX_WORKERS = 8
SAMPLE_SIZE = 50000  # For large tables

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class AnalysisResult:
    """Data class for analysis results"""
    name: str
    data: Any
    metadata: Dict[str, Any]
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()

class F1DatabaseAnalyzer:
    """Optimized F1 Database Statistical Analysis Engine"""
    
    # Define column types for each table (removed ID columns and filtered out unwanted sections)
    TABLE_SCHEMA = {
        'meetings': {
            'categorical': ['circuit_short_name', 'country_code', 'country_name', 'location', 
                          'meeting_name', 'meeting_official_name', 'gmt_offset'],
            'numeric': ['circuit_key', 'country_key'],
            'skip_detailed': ['meeting_key', 'year']
        },
        'sessions': {
            'categorical': ['circuit_short_name', 'country_code', 'country_name', 'location',
                          'session_name', 'session_type', 'gmt_offset'],
            'numeric': ['circuit_key', 'country_key'],
            'skip_detailed': ['session_key', 'meeting_key', 'year']
        },
        'drivers': {
            'categorical': ['broadcast_name', 'country_code', 'first_name', 'last_name',
                          'full_name', 'name_acronym', 'team_name', 'team_colour', 'headshot_url'],
            'numeric': [],
            'skip_detailed': ['driver_number', 'meeting_key', 'session_key', 'year']
        },
        'intervals': {
            'categorical': [],
            'numeric': ['gap_to_leader', 'interval'],
            'skip_detailed': ['driver_number', 'meeting_key', 'session_key']
        },
        'laps': {
            'categorical': ['segments_sector_1', 'segments_sector_2', 'segments_sector_3'],
            'numeric': ['lap_duration', 'duration_sector_1', 'duration_sector_2', 'duration_sector_3',
                       'i1_speed', 'i2_speed', 'st_speed', 'lap_number'],
            'boolean': ['is_pit_out_lap'],
            'skip_detailed': ['driver_number', 'meeting_key', 'session_key']
        },
        'position': {
            'categorical': [],
            'numeric': ['position'],
            'skip_detailed': ['driver_number', 'meeting_key', 'session_key']
        },
        'pit': {
            'categorical': [],
            'numeric': ['pit_duration', 'lap_number'],
            'skip_detailed': ['driver_number', 'meeting_key', 'session_key']
        },
        'stints': {
            'categorical': ['compound'],
            'numeric': ['lap_start', 'lap_end', 'stint_number', 'tyre_age_at_start'],
            'skip_detailed': ['driver_number', 'meeting_key', 'session_key']
        },
        'team_radio': {
            'categorical': [],
            'numeric': [],
            'skip_detailed': ['driver_number', 'meeting_key', 'session_key']
        },
        'location': {
            'categorical': [],
            'numeric': ['x', 'y', 'z'],
            'skip_detailed': ['driver_number', 'meeting_key', 'session_key']
        },
        'car_data': {
            'categorical': [],
            'numeric': ['speed', 'rpm', 'throttle', 'brake', 'drs'],
            'skip_detailed': ['driver_number', 'meeting_key', 'session_key']
        },
        'weather': {
            'categorical': [],
            'numeric': ['air_temperature', 'track_temperature', 'humidity', 'wind_speed', 
                       'wind_direction', 'pressure', 'rainfall'],
            'skip_detailed': ['meeting_key', 'session_key']
        },
        'race_control': {
            'categorical': ['category', 'flag', 'scope', 'sector', 'message'],
            'numeric': ['lap_number'],
            'skip_detailed': ['driver_number', 'meeting_key', 'session_key']
        }
    }
    
    def __init__(self, db_path: Path, analysis_path: Path):
        self.db_path = db_path
        self.analysis_path = analysis_path
        self.lock = threading.Lock()
        
        # Clean and recreate analysis directory
        if self.analysis_path.exists():
            shutil.rmtree(self.analysis_path)
        self.analysis_path.mkdir(parents=True, exist_ok=True)
        
        # Verify database exists
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found at {self.db_path}")
        
        # Get available tables
        self.available_tables = self._get_available_tables()
        logger.info(f"Found {len(self.available_tables)} tables in database")
    
    def _get_available_tables(self) -> List[str]:
        """Get list of available tables in database"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall() 
                     if row[0] != 'sqlite_sequence' and row[0] in self.TABLE_SCHEMA]
            return tables
        finally:
            conn.close()
    
    def get_connection(self) -> sqlite3.Connection:
        """Get optimized database connection"""
        conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")
        return conn
    
    def _get_table_sample(self, table_name: str) -> pd.DataFrame:
        """Get sample data from table with session distribution for large tables"""
        conn = self.get_connection()
        try:
            # First, get row count
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            total_rows = pd.read_sql(count_query, conn).iloc[0, 0]
            
            if total_rows <= SAMPLE_SIZE:
                # Small table, get all data
                return pd.read_sql(f"SELECT * FROM {table_name}", conn)
            
            # Large table, sample across sessions if possible
            if 'session_key' in [col for schema in self.TABLE_SCHEMA.values() for col in schema.get('skip_detailed', [])]:
                # Sample across different sessions
                sessions_query = f"SELECT DISTINCT session_key FROM {table_name} ORDER BY RANDOM() LIMIT 10"
                try:
                    sessions_df = pd.read_sql(sessions_query, conn)
                    if not sessions_df.empty:
                        session_list = sessions_df['session_key'].tolist()
                        session_str = ','.join(map(str, session_list))
                        sample_query = f"""
                            SELECT * FROM {table_name} 
                            WHERE session_key IN ({session_str})
                            ORDER BY RANDOM() LIMIT {SAMPLE_SIZE}
                        """
                        return pd.read_sql(sample_query, conn)
                except:
                    pass
            
            # Fallback to random sampling
            return pd.read_sql(f"SELECT * FROM {table_name} ORDER BY RANDOM() LIMIT {SAMPLE_SIZE}", conn)
            
        finally:
            conn.close()
    
    def _calculate_basic_counts(self, series: pd.Series, column_name: str) -> Dict[str, Any]:
        """Calculate basic count statistics for any column"""
        total_count = len(series)
        null_count = series.isna().sum()
        valid_count = total_count - null_count
        
        return {
            f"{column_name}_count": total_count,
            f"{column_name}_valid_count": valid_count,
            f"{column_name}_null_count": int(null_count),
            f"{column_name}_null_percentage": round((null_count / total_count) * 100, 2) if total_count > 0 else 0
        }
    
    def _calculate_numeric_stats(self, series: pd.Series, column_name: str) -> Dict[str, Any]:
        """Calculate detailed numeric statistics with proper data type handling"""
        stats = self._calculate_basic_counts(series, column_name)
        
        # Convert to numeric, coercing any non-numeric values to NaN
        try:
            numeric_series = pd.to_numeric(series, errors='coerce')
            clean_series = numeric_series.dropna()
        except Exception as e:
            logger.warning(f"Could not convert {column_name} to numeric: {e}")
            clean_series = pd.Series(dtype=float)
        
        if clean_series.empty:
            stats.update({
                f"{column_name}_mean": None,
                f"{column_name}_median": None,
                f"{column_name}_std": None,
                f"{column_name}_min": None,
                f"{column_name}_max": None,
                f"{column_name}_q25": None,
                f"{column_name}_q75": None
            })
        else:
            try:
                stats.update({
                    f"{column_name}_mean": round(float(clean_series.mean()), 4),
                    f"{column_name}_median": round(float(clean_series.median()), 4),
                    f"{column_name}_std": round(float(clean_series.std()), 4),
                    f"{column_name}_min": float(clean_series.min()),
                    f"{column_name}_max": float(clean_series.max()),
                    f"{column_name}_q25": round(float(clean_series.quantile(0.25)), 4),
                    f"{column_name}_q75": round(float(clean_series.quantile(0.75)), 4)
                })
            except Exception as e:
                logger.warning(f"Error calculating stats for {column_name}: {e}")
                stats.update({
                    f"{column_name}_mean": None,
                    f"{column_name}_median": None,
                    f"{column_name}_std": None,
                    f"{column_name}_min": None,
                    f"{column_name}_max": None,
                    f"{column_name}_q25": None,
                    f"{column_name}_q75": None
                })
        
        return stats
    
    def _calculate_categorical_stats(self, series: pd.Series, column_name: str, top_n: int = 10) -> Dict[str, Any]:
        """Calculate categorical statistics with filtering for unwanted sections"""
        stats = self._calculate_basic_counts(series, column_name)
        
        clean_series = series.dropna()
        unique_count = len(clean_series.unique()) if not clean_series.empty else 0
        
        # Get top values - but skip for date and recording_url columns
        top_values = {}
        most_common = None
        
        # Skip top_values and most_common for date and recording_url columns
        if column_name not in ['date', 'recording_url'] and not clean_series.empty:
            try:
                value_counts = clean_series.value_counts().head(top_n)
                top_values = {str(k): int(v) for k, v in value_counts.items()}
                most_common = str(value_counts.index[0]) if len(value_counts) > 0 else None
            except Exception as e:
                logger.warning(f"Error calculating top values for {column_name}: {e}")
        
        stats.update({
            f"{column_name}_unique_count": unique_count
        })
        
        # Only add top_values and most_common if they exist (not for filtered columns)
        if top_values:
            stats[f"{column_name}_top_values"] = top_values
        if most_common:
            stats[f"{column_name}_most_common"] = most_common
        
        return stats
    
    def _calculate_boolean_stats(self, series: pd.Series, column_name: str) -> Dict[str, Any]:
        """Calculate boolean statistics"""
        stats = self._calculate_basic_counts(series, column_name)
        
        clean_series = series.dropna()
        if not clean_series.empty:
            try:
                # Convert to boolean if needed
                if clean_series.dtype != bool:
                    clean_series = clean_series.astype(bool)
                
                true_count = clean_series.sum()
                false_count = len(clean_series) - true_count
                stats.update({
                    f"{column_name}_true_count": int(true_count),
                    f"{column_name}_false_count": int(false_count),
                    f"{column_name}_true_percentage": round((true_count / len(clean_series)) * 100, 2)
                })
            except Exception as e:
                logger.warning(f"Error calculating boolean stats for {column_name}: {e}")
                stats.update({
                    f"{column_name}_true_count": 0,
                    f"{column_name}_false_count": 0,
                    f"{column_name}_true_percentage": 0
                })
        else:
            stats.update({
                f"{column_name}_true_count": 0,
                f"{column_name}_false_count": 0,
                f"{column_name}_true_percentage": 0
            })
        
        return stats
    
    def analyze_table(self, table_name: str) -> AnalysisResult:
        """Unified table analysis method"""
        try:
            df = self._get_table_sample(table_name)
            
            if df.empty:
                return AnalysisResult(
                    name=f"{table_name}_statistics",
                    data={"error": f"No data available in {table_name}"},
                    metadata={"description": f"Statistical analysis of {table_name} table"}
                )
            
            schema = self.TABLE_SCHEMA.get(table_name, {})
            analysis = {
                'dataset_size': len(df),
                'is_sampled': len(df) == SAMPLE_SIZE,
            }
            
            # Add table-specific insights
            if table_name in ['car_data', 'location'] and len(df) == SAMPLE_SIZE:
                analysis['sample_info'] = f"Analyzed {SAMPLE_SIZE} records sampled across multiple sessions"
            
            # Process each column type
            for col_type, columns in schema.items():
                if col_type == 'skip_detailed':
                    # Only basic counts for these columns, and skip ID columns
                    for col in columns:
                        if col in df.columns and not col.lower().endswith('_id') and col != 'id':
                            analysis.update(self._calculate_basic_counts(df[col], col))
                elif col_type == 'numeric':
                    for col in columns:
                        if col in df.columns:
                            analysis.update(self._calculate_numeric_stats(df[col], col))
                elif col_type == 'categorical':
                    for col in columns:
                        if col in df.columns:
                            analysis.update(self._calculate_categorical_stats(df[col], col))
                elif col_type == 'boolean':
                    for col in columns:
                        if col in df.columns:
                            analysis.update(self._calculate_boolean_stats(df[col], col))
            
            # Add table-specific performance metrics
            analysis.update(self._calculate_table_specific_metrics(table_name, df))
            
            return AnalysisResult(
                name=f"{table_name}_statistics",
                data=analysis,
                metadata={"description": f"Statistical analysis of F1 {table_name} data"}
            )
            
        except Exception as e:
            logger.error(f"Error analyzing {table_name}: {str(e)}")
            return AnalysisResult(
                name=f"{table_name}_statistics",
                data={"error": f"Analysis failed: {str(e)}"},
                metadata={"description": f"Failed analysis of {table_name} table"}
            )
    
    def _calculate_table_specific_metrics(self, table_name: str, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate table-specific performance metrics"""
        metrics = {}
        
        try:
            if table_name == 'laps' and 'lap_duration' in df.columns:
                # Convert to numeric first
                lap_durations = pd.to_numeric(df['lap_duration'], errors='coerce').dropna()
                if not lap_durations.empty and lap_durations.mean() > 0:
                    metrics['lap_performance'] = {
                        'coefficient_of_variation': round((lap_durations.std() / lap_durations.mean()) * 100, 2),
                        'consistency_score': round(100 - ((lap_durations.std() / lap_durations.mean()) * 100), 2)
                    }
            
            elif table_name == 'pit' and 'pit_duration' in df.columns:
                pit_durations = pd.to_numeric(df['pit_duration'], errors='coerce').dropna()
                if not pit_durations.empty:
                    metrics['pit_performance'] = {
                        'under_3_seconds_pct': round((pit_durations < 3.0).mean() * 100, 2),
                        'over_5_seconds_pct': round((pit_durations > 5.0).mean() * 100, 2)
                    }
            
            elif table_name == 'weather' and 'rainfall' in df.columns:
                rainfall = pd.to_numeric(df['rainfall'], errors='coerce').dropna()
                if not rainfall.empty:
                    metrics['weather_conditions'] = {
                        'wet_sessions_pct': round((rainfall > 0).mean() * 100, 2),
                        'heavy_rain_pct': round((rainfall > 10).mean() * 100, 2)
                    }
            
            elif table_name == 'car_data':
                if 'speed' in df.columns:
                    speed = pd.to_numeric(df['speed'], errors='coerce').dropna()
                    if not speed.empty:
                        metrics['speed_insights'] = {
                            'high_speed_pct': round((speed > 300).mean() * 100, 2),
                            'low_speed_pct': round((speed < 100).mean() * 100, 2)
                        }
                
                if 'throttle' in df.columns:
                    throttle = pd.to_numeric(df['throttle'], errors='coerce').dropna()
                    if not throttle.empty:
                        metrics['throttle_insights'] = {
                            'full_throttle_pct': round((throttle >= 99).mean() * 100, 2)
                        }
        
        except Exception as e:
            logger.warning(f"Could not calculate specific metrics for {table_name}: {e}")
        
        return metrics
    
    def get_database_overview(self) -> AnalysisResult:
        """Get basic database overview"""
        conn = self.get_connection()
        try:
            stats = {}
            
            # Table row counts
            for table in self.available_tables:
                try:
                    count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table}", conn).iloc[0]['count']
                    stats[f"{table}_records"] = int(count)
                except:
                    stats[f"{table}_records"] = 0
            
            stats['total_records'] = sum(stats.values())
            stats['tables_with_data'] = sum(1 for v in stats.values() if v > 0)
            stats['completeness_pct'] = round((stats['tables_with_data'] / len(self.available_tables)) * 100, 2)
            
            # Date range from meetings
            try:
                if 'meetings' in self.available_tables:
                    date_query = pd.read_sql("""
                        SELECT MIN(date_start) as min_date, MAX(date_start) as max_date,
                               COUNT(DISTINCT year) as unique_years
                        FROM meetings
                    """, conn).iloc[0]
                    
                    stats['temporal_coverage'] = {
                        'earliest_date': str(date_query['min_date']) if pd.notna(date_query['min_date']) else None,
                        'latest_date': str(date_query['max_date']) if pd.notna(date_query['max_date']) else None,
                        'years_covered': int(date_query['unique_years']) if pd.notna(date_query['unique_years']) else 0
                    }
            except Exception as e:
                logger.warning(f"Could not analyze temporal coverage: {e}")
            
            return AnalysisResult(
                name="database_overview",
                data=stats,
                metadata={"description": "Database structure and basic statistics"}
            )
        finally:
            conn.close()
    
    def save_analysis(self, result: AnalysisResult) -> None:
        """Save analysis result to JSON file"""
        filename = f"{result.name}.json"
        filepath = self.analysis_path / filename
        
        with self.lock:
            try:
                # Convert data for JSON serialization
                def convert_for_json(obj):
                    if isinstance(obj, (np.integer, np.int64)):
                        return int(obj)
                    elif isinstance(obj, (np.floating, np.float64)):
                        return float(obj)
                    elif isinstance(obj, np.ndarray):
                        return obj.tolist()
                    elif pd.isna(obj):
                        return None
                    return obj
                
                def deep_convert(data):
                    if isinstance(data, dict):
                        return {str(k): deep_convert(v) for k, v in data.items()}
                    elif isinstance(data, list):
                        return [deep_convert(item) for item in data]
                    else:
                        return convert_for_json(data)
                
                result_dict = asdict(result)
                result_dict['data'] = deep_convert(result_dict['data'])
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(result_dict, f, indent=2, ensure_ascii=False, default=str)
                
                logger.info(f"✅ Saved: {result.name}")
                
            except Exception as e:
                logger.error(f"❌ Error saving {result.name}: {str(e)}")
    
    def run_analysis(self):
        """Run comprehensive analysis on all available tables"""
        print(f"🚀 Starting F1 Database Analysis")
        print(f"📍 Database: {self.db_path}")
        print(f"📁 Output: {self.analysis_path}")
        print(f"📊 Tables found: {len(self.available_tables)}")
        print(f"🔄 Sample size for large tables: {SAMPLE_SIZE:,}")
        print("-" * 60)
        
        # Prepare analysis tasks
        tasks = [('database_overview', self.get_database_overview, [])]
        tasks.extend([(table, self.analyze_table, [table]) for table in self.available_tables])
        
        completed = 0
        total = len(tasks)
        
        # Execute analyses
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_task = {}
            for task_name, task_func, task_args in tasks:
                future = executor.submit(self._execute_task, task_func, task_args)
                future_to_task[future] = task_name
            
            for future in as_completed(future_to_task):
                task_name = future_to_task[future]
                try:
                    result = future.result()
                    if result:
                        self.save_analysis(result)
                        completed += 1
                        print(f"✅ {completed}/{total} - {task_name}")
                    else:
                        print(f"❌ {completed}/{total} - {task_name} (failed)")
                except Exception as e:
                    logger.error(f"Task {task_name} failed: {str(e)}")
                    print(f"❌ {completed}/{total} - {task_name} (error)")
        
        # Generate final summary
        self._generate_summary()
        
        print("\n" + "=" * 60)
        print("🎯 ANALYSIS COMPLETE!")
        print(f"✅ Completed: {completed}/{total} analyses")
        print(f"📁 Files generated: {len(list(self.analysis_path.glob('*.json')))}")
        print(f"📍 Location: {self.analysis_path}")
        print("=" * 60)
    
    def _execute_task(self, task_func, task_args):
        """Execute a single analysis task"""
        try:
            return task_func(*task_args) if task_args else task_func()
        except Exception as e:
            logger.error(f"Task execution failed: {str(e)}")
            return None
    
    def _generate_summary(self):
        """Generate analysis summary"""
        try:
            files = list(self.analysis_path.glob("*.json"))
            
            summary = {
                'analysis_metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'total_files': len(files),
                    'database_path': str(self.db_path),
                    'sample_size_used': SAMPLE_SIZE
                },
                'files_generated': [f.name for f in files],
                'analysis_approach': 'Unified statistical analysis with optimized sampling'
            }
            
            # Try to extract key metrics
            try:
                overview_file = self.analysis_path / "database_overview.json"
                if overview_file.exists():
                    with open(overview_file, 'r') as f:
                        overview_data = json.load(f)
                        if 'data' in overview_data:
                            summary['key_metrics'] = {
                                'total_records': overview_data['data'].get('total_records', 0),
                                'tables_analyzed': overview_data['data'].get('tables_with_data', 0),
                                'completeness_percentage': overview_data['data'].get('completeness_pct', 0)
                            }
            except:
                pass
            
            summary_result = AnalysisResult(
                name="analysis_summary",
                data=summary,
                metadata={"description": "Complete analysis summary and metadata"}
            )
            self.save_analysis(summary_result)
            
        except Exception as e:
            logger.warning(f"Could not generate summary: {e}")


def main():
    """Main execution function"""
    try:
        if not DB_PATH.exists():
            print(f"❌ Database not found: {DB_PATH}")
            return
        
        analyzer = F1DatabaseAnalyzer(DB_PATH, ANALYSIS_PATH)
        analyzer.run_analysis()
        
    except Exception as e:
        logger.error(f"Main execution failed: {str(e)}")
        print(f"❌ Fatal error: {str(e)}")
        raise


if __name__ == "__main__":
    main()
