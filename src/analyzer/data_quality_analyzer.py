#!/usr/bin/env python3
"""
Data Quality Analyzer Module
Analyzes data quality issues, null values, duplicates, and data patterns
"""

import sqlite3
import re
from typing import Dict, Any, List, Tuple
from collections import Counter
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

class DataQualityAnalyzer:
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
    
    def analyze_table_quality(self, table_name: str) -> Dict[str, Any]:
        """Analyze data quality for a specific table"""
        # This method creates its own connection, making it suitable for threading.
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute(f"PRAGMA table_info(`{table_name}`)") # Quoted table name
            columns_info = cursor.fetchall()
            
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`") # Quoted table name
            total_rows = cursor.fetchone()[0]
            
            if total_rows == 0:
                return {
                    "table_name": table_name,
                    "total_rows": 0,
                    "columns": {},
                    "duplicate_analysis": {"duplicate_rows": 0, "unique_rows": 0, "duplicate_percentage": 0.0},
                    "consistency_analysis": {},
                    "quality_score": 100.0 # Perfect score for an empty table
                }
            
            columns_analysis = {}
            for col_info in columns_info:
                col_name = col_info[1]
                col_type = col_info[2]
                columns_analysis[col_name] = self._analyze_column_quality(
                    table_name, col_name, col_type, total_rows, conn # Pass conn
                )
            
            duplicate_analysis = self._analyze_duplicates(table_name, conn) # Pass conn
            consistency_analysis = self._analyze_data_consistency(table_name, columns_info, conn) # Pass conn
            
            return {
                "table_name": table_name,
                "total_rows": total_rows,
                "columns": columns_analysis,
                "duplicate_analysis": duplicate_analysis,
                "consistency_analysis": consistency_analysis,
                "quality_score": self._calculate_table_quality_score(columns_analysis, duplicate_analysis)
            }
    
    def _analyze_column_quality(self, table_name: str, col_name: str, col_type: str, total_rows: int, conn) -> Dict[str, Any]:
        """Analyze quality metrics for a specific column using provided connection"""
        cursor = conn.cursor() # Use passed connection's cursor
        
        quality_metrics = {
            "column_type": col_type,
            "null_count": 0,
            "null_percentage": 0.0,
            "distinct_count": 0,
            "distinct_percentage": 0.0,
            "empty_string_count": 0,
            "whitespace_only_count": 0,
            "data_type_consistency": True, # Assuming true unless proven otherwise
            "pattern_analysis": {},
            "outlier_analysis": {}
        }
        
        try:
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}` WHERE `{col_name}` IS NULL")
            null_count = cursor.fetchone()[0]
            quality_metrics["null_count"] = null_count
            quality_metrics["null_percentage"] = round((null_count / total_rows) * 100, 2) if total_rows > 0 else 0
            
            cursor.execute(f"SELECT COUNT(DISTINCT `{col_name}`) FROM `{table_name}`")
            distinct_count = cursor.fetchone()[0]
            quality_metrics["distinct_count"] = distinct_count
            quality_metrics["distinct_percentage"] = round((distinct_count / total_rows) * 100, 2) if total_rows > 0 else 0
            
            if col_type.upper() == 'TEXT':
                cursor.execute(f"SELECT COUNT(*) FROM `{table_name}` WHERE `{col_name}` = ''")
                quality_metrics["empty_string_count"] = cursor.fetchone()[0]
                
                cursor.execute(f"SELECT COUNT(*) FROM `{table_name}` WHERE TRIM(`{col_name}`) = '' AND `{col_name}` != ''")
                quality_metrics["whitespace_only_count"] = cursor.fetchone()[0]
                
                quality_metrics["pattern_analysis"] = self._analyze_text_patterns(table_name, col_name, conn)
            
            elif col_type.upper() in ['INTEGER', 'REAL', 'NUMERIC']:
                quality_metrics["outlier_analysis"] = self._analyze_numeric_outliers(table_name, col_name, conn)
            
            elif 'date' in col_name.lower() or 'time' in col_name.lower() or col_type.upper() == 'DATE' or col_type.upper() == 'DATETIME':
                quality_metrics["date_analysis"] = self._analyze_date_patterns(table_name, col_name, conn)
                
        except Exception as e:
            quality_metrics["analysis_error"] = str(e)
        
        return quality_metrics
    
    def _analyze_text_patterns(self, table_name: str, col_name: str, conn) -> Dict[str, Any]:
        """Analyze patterns in text columns using provided connection"""
        cursor = conn.cursor()
        patterns = {
            "avg_length": 0, "min_length": 0, "max_length": 0,
            "common_patterns": {}, "special_characters": False,
            "numeric_strings": 0, "email_like": 0, "url_like": 0
        }
        try:
            cursor.execute(f"SELECT AVG(LENGTH(`{col_name}`)), MIN(LENGTH(`{col_name}`)), MAX(LENGTH(`{col_name}`)) FROM `{table_name}` WHERE `{col_name}` IS NOT NULL")
            result = cursor.fetchone()
            if result and result[0] is not None:
                patterns["avg_length"] = round(result[0], 2)
                patterns["min_length"] = result[1]
                patterns["max_length"] = result[2]
            
            cursor.execute(f"SELECT `{col_name}` FROM `{table_name}` WHERE `{col_name}` IS NOT NULL LIMIT 1000")
            sample_values = [row[0] for row in cursor.fetchall() if row[0] is not None] # Ensure row[0] is not None
            
            if sample_values:
                patterns["numeric_strings"] = sum(1 for val in sample_values if str(val).isdigit())
                patterns["email_like"] = sum(1 for val in sample_values if isinstance(val, str) and '@' in val and '.' in val)
                patterns["url_like"] = sum(1 for val in sample_values if isinstance(val, str) and (val.startswith('http://') or val.startswith('https://')))
                patterns["special_characters"] = any(bool(re.search(r'[!@#$%^&*(),.?":{}|<>]', str(val))) for val in sample_values)
        except Exception as e:
            patterns["error"] = str(e)
        return patterns

    def _analyze_numeric_outliers(self, table_name: str, col_name: str, conn) -> Dict[str, Any]:
        """Analyze numeric outliers using provided connection"""
        cursor = conn.cursor()
        outliers = {
            "min_value": None, "max_value": None, "mean": None, "median": None,
            "q1": None, "q3": None, "outlier_count": 0,
            "negative_count": 0, "zero_count": 0
        }
        try:
            cursor.execute(f"SELECT MIN(`{col_name}`), MAX(`{col_name}`), AVG(`{col_name}`) FROM `{table_name}` WHERE `{col_name}` IS NOT NULL")
            result = cursor.fetchone()
            if result and result[0] is not None:
                outliers["min_value"] = result[0]
                outliers["max_value"] = result[1]
                outliers["mean"] = round(result[2], 2) if result[2] is not None else None
            
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}` WHERE `{col_name}` < 0")
            outliers["negative_count"] = cursor.fetchone()[0]
            
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}` WHERE `{col_name}` = 0")
            outliers["zero_count"] = cursor.fetchone()[0]
        except Exception as e:
            outliers["error"] = str(e)
        return outliers

    def _analyze_date_patterns(self, table_name: str, col_name: str, conn) -> Dict[str, Any]:
        """Analyze date/time patterns using provided connection"""
        cursor = conn.cursor()
        date_analysis = {
            "sample_values": [], "unique_formats": [], "null_dates": 0,
            "future_dates": 0, "very_old_dates": 0
        }
        try:
            cursor.execute(f"SELECT DISTINCT `{col_name}` FROM `{table_name}` WHERE `{col_name}` IS NOT NULL LIMIT 20")
            date_analysis["sample_values"] = [row[0] for row in cursor.fetchall()]
            
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}` WHERE `{col_name}` IS NULL")
            date_analysis["null_dates"] = cursor.fetchone()[0]
        except Exception as e:
            date_analysis["error"] = str(e)
        return date_analysis

    def _analyze_duplicates(self, table_name: str, conn) -> Dict[str, Any]:
        """Analyze duplicate rows in table using provided connection"""
        cursor = conn.cursor()
        try:
            cursor.execute(f"PRAGMA table_info(`{table_name}`)")
            columns = [col[1] for col in cursor.fetchall()]
            if not columns: return {"error": "No columns found", "duplicate_percentage": 0.0} # Added default for percentage
            
            columns_str = ", ".join(f"`{col}`" for col in columns)
            # SQLite does not directly support COUNT(DISTINCT col1, col2, ...).
            # A common workaround is to concatenate columns or use a subquery.
            # For simplicity, using string concatenation (might be slow or problematic with NULLs/types).
            # A more robust way involves `GROUP BY all columns` then `COUNT(*)`.
            
            # Using GROUP BY approach for unique row count
            cursor.execute(f"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM `{table_name}`)")
            unique_rows = cursor.fetchone()[0]

            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            total_rows = cursor.fetchone()[0]

            duplicate_rows = total_rows - unique_rows
            
            return {
                "total_rows": total_rows, "unique_rows": unique_rows,
                "duplicate_rows": duplicate_rows,
                "duplicate_percentage": round((duplicate_rows / total_rows) * 100, 2) if total_rows > 0 else 0
            }
        except Exception as e:
            return {"error": str(e), "duplicate_percentage": 0.0} # Ensure default for percentage

    def _analyze_data_consistency(self, table_name: str, columns_info: List, conn) -> Dict[str, Any]:
        """Analyze data consistency issues using provided connection"""
        cursor = conn.cursor()
        consistency = {"case_inconsistency": {}, "format_inconsistency": {}, "referential_integrity": {}}
        try:
            for col_info in columns_info:
                col_name, col_type = col_info[1], col_info[2]
                if col_type.upper() == 'TEXT':
                    cursor.execute(f"SELECT COUNT(DISTINCT `{col_name}`), COUNT(DISTINCT UPPER(`{col_name}`)) FROM `{table_name}` WHERE `{col_name}` IS NOT NULL")
                    result = cursor.fetchone()
                    if result and result[0] != result[1]: # Check result is not None
                        consistency["case_inconsistency"][col_name] = {
                            "original_distinct": result[0], "upper_distinct": result[1],
                            "potential_duplicates": result[0] - result[1]
                        }
        except Exception as e:
            consistency["error"] = str(e)
        return consistency

    def _calculate_table_quality_score(self, columns_analysis: Dict, duplicate_analysis: Dict) -> float:
        if not columns_analysis: return 0.0
        total_score, factors = 0.0, 0 # Ensure float for score
        
        avg_null_percentage = sum(col.get("null_percentage", 0) for col in columns_analysis.values() if isinstance(col, dict)) / len(columns_analysis) if columns_analysis else 0
        total_score += max(0, 100 - avg_null_percentage)
        factors += 1
        
        duplicate_percentage = duplicate_analysis.get("duplicate_percentage", 0)
        total_score += max(0, 100 - duplicate_percentage)
        factors += 1
        
        # Filter out non-dict items from columns_analysis values if any error occurred
        valid_cols_for_distinct = [col for col in columns_analysis.values() if isinstance(col, dict)]
        avg_distinct_percentage = sum(col.get("distinct_percentage", 0) for col in valid_cols_for_distinct) / len(valid_cols_for_distinct) if valid_cols_for_distinct else 0
        total_score += min(100, avg_distinct_percentage)
        factors += 1
        
        return round(total_score / factors, 2) if factors > 0 else 0.0
    
    def analyze_data_quality(self) -> Dict[str, Dict[str, Any]]:
        """Analyze data quality for all tables using multithreading"""
        tables = self.get_table_list()
        results = {}
        max_workers = os.cpu_count() or 4

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_table = {executor.submit(self.analyze_table_quality, table_name): table_name for table_name in tables}
            
            for future in tqdm(as_completed(future_to_table), total=len(tables), desc="Analyzing data quality per table"):
                table_name = future_to_table[future]
                try:
                    results[table_name] = future.result()
                except Exception as e:
                    results[table_name] = {"error": f"Error analyzing data quality for table {table_name}: {str(e)}"}
                    print(f"Error analyzing data quality for table {table_name}: {e}")
        return results
    
    def get_overall_quality_summary(self) -> Dict[str, Any]:
        quality_results = self.analyze_data_quality()
        if not quality_results: return {"error": "No quality results available"}
        
        total_rows = sum(table.get("total_rows", 0) for table in quality_results.values() if isinstance(table, dict))
        quality_scores = [table.get("quality_score", 0) for table in quality_results.values() if isinstance(table, dict) and "quality_score" in table]
        avg_quality_score = sum(quality_scores) / len(quality_scores) if quality_scores else 0.0 # Ensure float
        
        tables_with_issues = sum(1 for table in quality_results.values() if isinstance(table, dict) and table.get("quality_score", 100) < 80)
        
        return {
            "total_tables_analyzed": len(quality_results),
            "total_rows_analyzed": total_rows,
            "average_quality_score": round(avg_quality_score, 2),
            "tables_with_quality_issues": tables_with_issues,
            "quality_grade": self._get_quality_grade(avg_quality_score)
        }
    
    def _get_quality_grade(self, score: float) -> str:
        if score >= 90: return "A (Excellent)"
        elif score >= 80: return "B (Good)"
        elif score >= 70: return "C (Fair)"
        elif score >= 60: return "D (Poor)"
        else: return "F (Very Poor)"
