import sqlite3
import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import seaborn as sns
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
import os

warnings.filterwarnings('ignore')

class F1BrakeThrottleAnalyzer:
    def __init__(self, db_path="data/f1db_YR=2024/database.db"):
        self.db_path = db_path
        # Extract db_name for output folder
        self.db_name = os.path.basename(os.path.dirname(db_path))
        self.output_dir = os.path.join("data", self.db_name)
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.conn = sqlite3.connect(db_path)
        print(f"Connected to database: {db_path}")
        
    def get_basic_stats(self):
        """Get basic statistics about brake and throttle columns"""
        print("\n" + "="*80)
        print("BASIC STATISTICS")
        print("="*80)
        
        query = """
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT brake) as unique_brake_values,
            COUNT(DISTINCT throttle) as unique_throttle_values,
            MIN(brake) as min_brake, MAX(brake) as max_brake,
            MIN(throttle) as min_throttle, MAX(throttle) as max_throttle
        FROM car_data
        """
        
        stats = pd.read_sql_query(query, self.conn)
        print(f"Total rows: {stats['total_rows'].iloc[0]:,}")
        print(f"Unique brake values: {stats['unique_brake_values'].iloc[0]}")
        print(f"Unique throttle values: {stats['unique_throttle_values'].iloc[0]}")
        print(f"Brake range: {stats['min_brake'].iloc[0]} - {stats['max_brake'].iloc[0]}")
        print(f"Throttle range: {stats['min_throttle'].iloc[0]} - {stats['max_throttle'].iloc[0]}")
        
        return stats
    
    def analyze_brake_distribution(self):
        """Analyze brake value distributions"""
        print("\n" + "="*80)
        print("BRAKE VALUE DISTRIBUTION")
        print("="*80)
        
        query = """
        SELECT 
            brake,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM car_data), 4) as percentage
        FROM car_data
        GROUP BY brake
        ORDER BY brake
        """
        
        brake_dist = pd.read_sql_query(query, self.conn)
        
        for _, row in brake_dist.iterrows():
            print(f"Brake {int(row['brake']):3d}: {row['count']:>10,} records ({row['percentage']:>7.4f}%)")
        
        return brake_dist
    
    def analyze_throttle_distribution(self):
        """Analyze throttle value distributions"""
        print("\n" + "="*80)
        print("THROTTLE VALUE DISTRIBUTION")
        print("="*80)
        
        # Get throttle statistics
        query = """
        SELECT 
            AVG(throttle) as avg_throttle,
            MIN(throttle) as min_throttle,
            MAX(throttle) as max_throttle,
            COUNT(CASE WHEN throttle = 0 THEN 1 END) as zero_throttle,
            COUNT(CASE WHEN throttle = 100 THEN 1 END) as full_throttle,
            COUNT(CASE WHEN throttle BETWEEN 1 AND 99 THEN 1 END) as partial_throttle
        FROM car_data
        """
        
        throttle_stats = pd.read_sql_query(query, self.conn)
        total_rows = throttle_stats['zero_throttle'].iloc[0] + throttle_stats['full_throttle'].iloc[0] + throttle_stats['partial_throttle'].iloc[0]
        
        print(f"Average throttle: {throttle_stats['avg_throttle'].iloc[0]:.2f}%")
        print(f"Zero throttle (0%): {throttle_stats['zero_throttle'].iloc[0]:,} records ({throttle_stats['zero_throttle'].iloc[0]/total_rows*100:.2f}%)")
        print(f"Full throttle (100%): {throttle_stats['full_throttle'].iloc[0]:,} records ({throttle_stats['full_throttle'].iloc[0]/total_rows*100:.2f}%)")
        print(f"Partial throttle (1-99%): {throttle_stats['partial_throttle'].iloc[0]:,} records ({throttle_stats['partial_throttle'].iloc[0]/total_rows*100:.2f}%)")
        
        return throttle_stats
    
    def analyze_brake_throttle_combinations(self):
        """Analyze brake and throttle combinations"""
        print("\n" + "="*80)
        print("BRAKE-THROTTLE COMBINATIONS")
        print("="*80)
        
        query = """
        SELECT 
            brake,
            CASE 
                WHEN throttle = 0 THEN 'Zero (0%)'
                WHEN throttle BETWEEN 1 AND 25 THEN 'Low (1-25%)'
                WHEN throttle BETWEEN 26 AND 50 THEN 'Medium (26-50%)'
                WHEN throttle BETWEEN 51 AND 75 THEN 'High (51-75%)'
                WHEN throttle BETWEEN 76 AND 99 THEN 'Very High (76-99%)'
                WHEN throttle = 100 THEN 'Full (100%)'
                ELSE 'Other'
            END as throttle_category,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM car_data), 4) as percentage
        FROM car_data
        GROUP BY brake, throttle_category
        ORDER BY brake, 
                 CASE throttle_category
                    WHEN 'Zero (0%)' THEN 1
                    WHEN 'Low (1-25%)' THEN 2
                    WHEN 'Medium (26-50%)' THEN 3
                    WHEN 'High (51-75%)' THEN 4
                    WHEN 'Very High (76-99%)' THEN 5
                    WHEN 'Full (100%)' THEN 6
                    ELSE 7
                 END
        """
        
        combinations = pd.read_sql_query(query, self.conn)
        
        current_brake = None
        for _, row in combinations.iterrows():
            if int(row['brake']) != current_brake:
                current_brake = int(row['brake'])
                print(f"\nBrake {current_brake}:")
            print(f"  {row['throttle_category']:>18}: {row['count']:>10,} records ({row['percentage']:>7.4f}%)")
        
        return combinations

    def analyze_outlier_timing(self):
        """
        Analyze when brake > 100 and throttle > 100 values appear,
        checking if they are within the official session times.
        The result is saved to a text file.
        """
        print("\n" + "="*80)
        print("ANALYZING OUTLIER BRAKE/THROTTLE VALUES (> 100)")
        print("="*80)

        query = """
        SELECT
            cd.date,
            cd.brake,
            cd.throttle,
            cd.session_key,
            s.date_start,
            s.date_end,
            s.session_name
        FROM car_data AS cd
        JOIN sessions AS s ON cd.session_key = s.session_key
        WHERE
            cd.brake > 100 OR cd.throttle > 100
        """
        
        try:
            outliers_df = pd.read_sql_query(query, self.conn)
            print(f"Found {len(outliers_df)} data points with brake or throttle > 100.")

            if outliers_df.empty:
                print("No outlier data points found.")
                return

            # Convert date columns to datetime objects, coercing errors
            outliers_df['date'] = pd.to_datetime(outliers_df['date'], errors='coerce')
            outliers_df['date_start'] = pd.to_datetime(outliers_df['date_start'], errors='coerce')
            outliers_df['date_end'] = pd.to_datetime(outliers_df['date_end'], errors='coerce')

            # Drop rows where date conversion failed
            outliers_df.dropna(subset=['date', 'date_start', 'date_end'], inplace=True)

            # Determine if the data point is within the session time window
            outliers_df['within_session'] = (outliers_df['date'] >= outliers_df['date_start']) & \
                                            (outliers_df['date'] <= outliers_df['date_end'])

            # Generate report
            report_path = os.path.join(self.output_dir, "outlier_timing_analysis.txt")
            
            with open(report_path, "w") as f:
                f.write("Analysis of Anomalous Brake (>100) and Throttle (>100) Values\n")
                f.write("="*70 + "\n\n")
                
                total_anomalies = len(outliers_df)
                within_session_count = outliers_df['within_session'].sum()
                outside_session_count = total_anomalies - within_session_count
                
                f.write(f"Total anomalous data points found: {total_anomalies}\n")
                f.write(f" - Within official session time: {within_session_count} ({within_session_count/total_anomalies:.2%})\n")
                f.write(f" - Outside official session time: {outside_session_count} ({outside_session_count/total_anomalies:.2%})\n\n")
                
                f.write("Summary by Session:\n")
                f.write("-" * 20 + "\n")
                
                summary_by_session = outliers_df.groupby('session_name')['within_session'].agg(['count', lambda x: x.sum()]).rename(
                    columns={'count': 'total_outliers', '<lambda_0>': 'within_session'}
                )
                summary_by_session['outside_session'] = summary_by_session['total_outliers'] - summary_by_session['within_session']
                
                for session_name, data in summary_by_session.iterrows():
                    f.write(f"Session: {session_name}\n")
                    f.write(f"  - Total Outliers: {int(data['total_outliers'])}\n")
                    f.write(f"  - Within Session: {int(data['within_session'])}\n")
                    f.write(f"  - Outside Session: {int(data['outside_session'])}\n\n")
                    
                f.write("\nInterpretation:\n")
                f.write("-" * 20 + "\n")
                f.write("Data points 'Within Session Time' suggest potential sensor errors, data transmission glitches, or specific but undocumented vehicle states during live running.\n")
                f.write("Data points 'Outside Session Time' are likely telemetry noise or system checks occurring before the session officially begins or after it has concluded.\n")

            print(f"Analysis complete. Report saved to: {report_path}")

        except Exception as e:
            print(f"An error occurred during outlier analysis: {e}")
    
    def run_complete_analysis(self):
        """Run the complete analysis pipeline"""
        print("F1 BRAKE AND THROTTLE DATA ANALYSIS")
        print("=" * 80)
        print("Analyzing 57+ million data points...")
        
        try:
            # Run all analyses
            self.get_basic_stats()
            self.analyze_brake_distribution()
            self.analyze_throttle_distribution()
            # self.analyze_brake_throttle_combinations() # This can be verbose, uncomment if needed
            
            # Run the new targeted analysis
            self.analyze_outlier_timing()
            
            print("\n" + "="*80)
            print("ANALYSIS COMPLETE")
            print("="*80)
            
        except Exception as e:
            print(f"Error during analysis: {e}")
        finally:
            self.conn.close()
            print("Database connection closed.")

def main():
    # Initialize analyzer
    # The database path should point to your actual database file
    analyzer = F1BrakeThrottleAnalyzer(db_path="data/f1db_YR=2024/database.db")
    
    # Run complete analysis
    analyzer.run_complete_analysis()

if __name__ == "__main__":
    main()
