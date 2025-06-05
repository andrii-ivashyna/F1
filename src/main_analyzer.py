#!/usr/bin/env python3
"""
F1 Database Metadata Analyzer - Main Script
Analyzes the F1 database for metadata, null values, data types, unique values, and more.
"""

import sqlite3
import json
import os
from datetime import datetime
from pathlib import Path

# Import analyzer modules
from analyzer.table_analyzer import TableAnalyzer
from analyzer.data_quality_analyzer import DataQualityAnalyzer
from analyzer.relationship_analyzer import RelationshipAnalyzer

class F1DatabaseAnalyzer:
    def __init__(self, db_path: str, output_dir: str):
        self.db_path = db_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize analyzers
        self.table_analyzer = TableAnalyzer(db_path)
        self.quality_analyzer = DataQualityAnalyzer(db_path)
        self.relationship_analyzer = RelationshipAnalyzer(db_path)
        
    def run_complete_analysis(self):
        """Run complete database analysis and save results"""
        print("Starting F1 Database Analysis...")
        
        analysis_results = {
            "analysis_metadata": {
                "timestamp": datetime.now().isoformat(),
                "database_path": self.db_path,
                "database_size_mb": os.path.getsize(self.db_path) / (1024 * 1024)
            }
        }
        
        # 1. Basic Table Analysis
        print("Analyzing table structures...")
        analysis_results["table_analysis"] = self.table_analyzer.analyze_all_tables()
        
        # 2. Data Quality Analysis
        print("Analyzing data quality...")
        analysis_results["data_quality"] = self.quality_analyzer.analyze_data_quality()
        
        # 3. Relationship Analysis
        print("Analyzing table relationships...")
        analysis_results["relationships"] = self.relationship_analyzer.analyze_relationships()
        
        # 4. Generate Summary Report
        print("Generating summary report...")
        analysis_results["summary"] = self._generate_summary(analysis_results)
        
        # Save results
        db_filename = Path(self.db_path).stem
        output_file = self.output_dir / f"{db_filename}_analysis.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(analysis_results, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"Analysis complete! Results saved to: {output_file}")
        return analysis_results
    
    def _generate_summary(self, results):
        """Generate a summary of the analysis"""
        summary = {
            "total_tables": len(results["table_analysis"]),
            "total_records": sum(table["row_count"] for table in results["table_analysis"].values() if "row_count" in table), # Added check for row_count
            "tables_with_nulls": len([t for t in results["data_quality"].values() if isinstance(t, dict) and any(col.get("null_percentage", 0) > 0 for col in t.get("columns", {}).values())]),
            "most_populated_table": max(results["table_analysis"].items(), key=lambda x: x[1].get("row_count", -1)) if results["table_analysis"] else None,
            "least_populated_table": min(results["table_analysis"].items(), key=lambda x: x[1].get("row_count", float('inf'))) if results["table_analysis"] else None,
            "data_quality_score": self._calculate_overall_quality_score(results["data_quality"])
        }
        # Handle cases where tables might have errors and no row_count
        if summary["most_populated_table"] and summary["most_populated_table"][1].get("row_count", -1) == -1:
            summary["most_populated_table"] = ("N/A (error in analysis)", {"row_count": "N/A"})
        if summary["least_populated_table"] and summary["least_populated_table"][1].get("row_count", float('inf')) == float('inf'):
            summary["least_populated_table"] = ("N/A (error in analysis)", {"row_count": "N/A"})

        return summary
    
    def _calculate_overall_quality_score(self, quality_data):
        """Calculate overall data quality score (0-100)"""
        total_fields = 0
        null_fields = 0
        
        for table_data in quality_data.values():
            if isinstance(table_data, dict) and "columns" in table_data: # Check if table_data is a dict and has columns
                for col_data in table_data["columns"].values():
                    if isinstance(col_data, dict): # Check if col_data is a dict
                        total_fields += 1
                        if col_data.get("null_percentage", 0) > 50:  # Consider >50% nulls as poor quality
                            null_fields += 1
        
        if total_fields == 0:
            return 100.0 # Return float
        
        return round((1 - null_fields / total_fields) * 100, 2)

def main():
    # Path to database (relative to src/utilities/)
    base_dir = os.path.dirname(__file__)
    data_folder_path = os.path.join(base_dir, '..', 'data')
    db_path = os.path.join(data_folder_path, 'f1_meeting_1229.db')
    
    if not os.path.exists(db_path):
        print(f"Database not found at: {db_path}")
        return
    
    analyzer = F1DatabaseAnalyzer(db_path, data_folder_path)
    results = analyzer.run_complete_analysis()
    
    # Print quick summary
    print("\n" + "="*50)
    print("ANALYSIS SUMMARY")
    print("="*50)
    if results and results.get('summary'):
        summary = results['summary']
        print(f"Total Tables: {summary.get('total_tables', 'N/A')}")
        print(f"Total Records: {summary.get('total_records', 'N/A'):,}" if isinstance(summary.get('total_records'), int) else f"Total Records: {summary.get('total_records', 'N/A')}")
        print(f"Data Quality Score: {summary.get('data_quality_score', 'N/A')}%")
        
        most_pop_table_data = summary.get('most_populated_table')
        if most_pop_table_data and isinstance(most_pop_table_data[1], dict):
            print(f"Most Populated Table: {most_pop_table_data[0]} ({most_pop_table_data[1].get('row_count', 'N/A'):,} rows)" if isinstance(most_pop_table_data[1].get('row_count'), int) else f"Most Populated Table: {most_pop_table_data[0]} ({most_pop_table_data[1].get('row_count', 'N/A')} rows)")
        else:
            print(f"Most Populated Table: N/A")

        least_pop_table_data = summary.get('least_populated_table')
        if least_pop_table_data and isinstance(least_pop_table_data[1], dict):
            print(f"Least Populated Table: {least_pop_table_data[0]} ({least_pop_table_data[1].get('row_count', 'N/A'):,} rows)" if isinstance(least_pop_table_data[1].get('row_count'), int) else f"Least Populated Table: {least_pop_table_data[0]} ({least_pop_table_data[1].get('row_count', 'N/A')} rows)")
        else:
            print(f"Least Populated Table: N/A")
    else:
        print("Summary could not be generated.")

if __name__ == "__main__":
    main()
