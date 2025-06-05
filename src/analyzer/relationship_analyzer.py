#!/usr/bin/env python3
"""
Relationship Analyzer Module
Analyzes relationships between tables, foreign keys, and data connections
"""

import sqlite3
from typing import Dict, Any, List, Tuple, Set
from collections import defaultdict, Counter
import re
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

class RelationshipAnalyzer:
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def get_connection(self):
        """Get database connection"""
        # Ensure PRAGMA foreign_keys=ON if needed for checks, though PRAGMA foreign_key_list works regardless.
        conn = sqlite3.connect(self.db_path)
        # conn.execute("PRAGMA foreign_keys = ON") # Optional: if subsequent operations in same conn need it.
        return conn
    
    def get_table_list(self) -> List[str]:
        """Get list of all tables in the database"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            return [row[0] for row in cursor.fetchall()]

    def _fetch_foreign_keys_for_table(self, table_name: str) -> Tuple[str, List[Dict[str, Any]]]:
        """Helper function to fetch foreign keys for a single table, for threading."""
        fks_for_table = []
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA foreign_key_list(`{table_name}`)") # Quoted table_name
            fk_list = cursor.fetchall()
            if fk_list:
                for fk_row in fk_list: # Renamed fk to fk_row to avoid conflict
                    fks_for_table.append({
                        "id": fk_row[0], "seq": fk_row[1], "table": fk_row[2],
                        "from_column": fk_row[3], "to_column": fk_row[4],
                        "on_update": fk_row[5], "on_delete": fk_row[6], "match": fk_row[7]
                    })
        return table_name, fks_for_table

    def _analyze_foreign_keys(self) -> Dict[str, List[Dict[str, Any]]]:
        """Analyze explicit foreign key relationships using multithreading."""
        tables = self.get_table_list()
        foreign_keys_results = {}
        max_workers = os.cpu_count() or 4

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self._fetch_foreign_keys_for_table, table_name) for table_name in tables]
            for future in tqdm(as_completed(futures), total=len(tables), desc="Analyzing foreign keys"):
                try:
                    table_name, fks_for_table = future.result()
                    if fks_for_table:
                        foreign_keys_results[table_name] = fks_for_table
                except Exception as e:
                    print(f"Error fetching foreign keys for a table during threaded execution: {e}")
        return foreign_keys_results

    def _process_table_pair_for_potential_relationships(self, table1_name: str, all_tables: List[str], table_columns_map: Dict[str, List[Tuple[str, str]]]) -> Tuple[str, List[Dict[str, Any]]]:
        """Helper to find potential relationships for table1 against all other tables using a dedicated connection."""
        table1_relationships = []
        cols1 = table_columns_map.get(table1_name, []) # Get cols1 safely
        with self.get_connection() as conn: # Connection for this thread
            cursor = conn.cursor()
            for table2_name in all_tables:
                if table1_name != table2_name:
                    cols2 = table_columns_map.get(table2_name, []) # Get cols2 safely
                    if cols1 and cols2: # Proceed only if columns exist for both tables
                        relationships = self._find_column_matches(
                            table1_name, cols1,
                            table2_name, cols2,
                            cursor # Pass the thread-local cursor
                        )
                        if relationships:
                            table1_relationships.extend(relationships)
        return table1_name, table1_relationships

    def _find_potential_relationships(self) -> Dict[str, List[Dict[str, Any]]]:
        """Find potential relationships based on column names and data patterns using multithreading."""
        tables = self.get_table_list()
        potential_relationships_map = {}
        max_workers = os.cpu_count() or 4
        
        table_columns_info = {}
        with self.get_connection() as conn:
            cursor = conn.cursor()
            for table_name in tables:
                try: # Add try-except for PRAGMA failing on non-existent/virtual tables
                    cursor.execute(f"PRAGMA table_info(`{table_name}`)") # Quoted table_name
                    columns = cursor.fetchall()
                    table_columns_info[table_name] = [(col[1], col[2]) for col in columns]
                except sqlite3.OperationalError as e:
                    print(f"Could not get table_info for {table_name}: {e}")
                    table_columns_info[table_name] = []


        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self._process_table_pair_for_potential_relationships,
                    table1, tables, table_columns_info
                ): table1 for table1 in tables
            }
            for future in tqdm(as_completed(futures), total=len(tables), desc="Finding potential relationships"):
                try:
                    origin_table_name, relationships_found = future.result()
                    if relationships_found:
                        potential_relationships_map[origin_table_name] = relationships_found
                except Exception as e:
                    table_name_in_error = futures[future]
                    print(f"Error finding potential relationships for table {table_name_in_error}: {e}")
        return potential_relationships_map
    
    def _find_column_matches(self, table1: str, cols1: List[Tuple], table2: str, cols2: List[Tuple], cursor) -> List[Dict[str, Any]]:
        """Find potential column matches between two tables using provided cursor."""
        matches = []
        for col1_name, col1_type in cols1:
            for col2_name, col2_type in cols2:
                if self._columns_might_be_related(col1_name, col2_name, table1, table2):
                    match_info = self._verify_potential_match(table1, col1_name, table2, col2_name, cursor)
                    if match_info["confidence"] > 0.3:
                        matches.append({
                            "from_table": table1, "from_column": col1_name,
                            "to_table": table2, "to_column": col2_name,
                            "confidence": match_info["confidence"],
                            "match_type": match_info["match_type"],
                            "sample_matches": match_info["sample_matches"]
                        })
        return matches

    def _columns_might_be_related(self, col1: str, col2: str, table1: str, table2: str) -> bool:
        col1_lower, col2_lower = col1.lower(), col2.lower()
        table1_singular = table1.lower().rstrip('s') # Basic singularization
        table2_singular = table2.lower().rstrip('s')

        if col1_lower == col2_lower and "id" not in col1_lower : # Avoid self-referencing ID columns as table_id=table_id
             if table1_singular != table2_singular : # If same name but not IDs and from different conceptual entities
                return True

        if 'id' in col1_lower or 'id' in col2_lower:
            # e.g. table1.column_id == table2.id where column is table2_singular
            if col1_lower == f"{table2_singular}_id" and col2_lower == "id": return True
            if col2_lower == f"{table1_singular}_id" and col1_lower == "id": return True
            # e.g. table1.id == table2.table1_id
            if col1_lower == "id" and col2_lower == f"{table1_singular}_id": return True
            if col2_lower == "id" and col1_lower == f"{table2_singular}_id": return True

        common_suffixes = ['id', 'key', 'code', 'num', 'no']
        c1_base = re.sub(r'_(' + '|'.join(common_suffixes) + r')$', '', col1_lower)
        c2_base = re.sub(r'_(' + '|'.join(common_suffixes) + r')$', '', col2_lower)

        if c1_base == c2_base and c1_base != '': # Matched base names e.g. user_id and user_code
            return True
        
        # Check for table_name_id pattern more generally
        if col1_lower == f"{table2_singular}_id" or col1_lower == f"{table2.lower()}_id": return True
        if col2_lower == f"{table1_singular}_id" or col2_lower == f"{table1.lower()}_id": return True

        return False

    def _verify_potential_match(self, table1: str, col1: str, table2: str, col2: str, cursor) -> Dict[str, Any]:
        """Verify potential relationship by sampling data using provided cursor."""
        try:
            cursor.execute(f"SELECT DISTINCT `{col1}` FROM `{table1}` WHERE `{col1}` IS NOT NULL LIMIT 100")
            sample1 = {row[0] for row in cursor.fetchall()}
            cursor.execute(f"SELECT DISTINCT `{col2}` FROM `{table2}` WHERE `{col2}` IS NOT NULL LIMIT 100")
            sample2 = {row[0] for row in cursor.fetchall()}
            
            if not sample1 or not sample2: return {"confidence": 0, "match_type": "no_data", "sample_matches": []}
            
            intersection, union = sample1.intersection(sample2), sample1.union(sample2)
            confidence = len(intersection) / len(union) if union else 0
            
            match_type = "weak_match" # Default to weak if some confidence
            if confidence > 0.8: match_type = "strong_match"
            elif confidence > 0.5: match_type = "moderate_match"
            
            return {"confidence": round(confidence, 3), "match_type": match_type, "sample_matches": list(intersection)[:10]}
        except Exception as e:
            return {"confidence": 0, "match_type": "error", "sample_matches": [], "error": str(e)}

    def _analyze_table_connections_from_data(self, fk_analysis: Dict, potential_analysis: Dict) -> Dict[str, Any]:
        """Analyze how tables are connected using pre-fetched data."""
        tables = self.get_table_list()
        connections = defaultdict(set)
        for table, fks in fk_analysis.items():
            for fk in fks:
                connections[table].add(fk["table"])
                connections[fk["table"]].add(table)
        for table, relationships in potential_analysis.items():
            for rel in relationships:
                if rel["confidence"] > 0.5:
                    connections[table].add(rel["to_table"])
                    connections[rel["to_table"]].add(table)
        
        connection_dict = {table: list(connected_tables) for table, connected_tables in connections.items()}
        connection_counts = {table: len(connected) for table, connected in connection_dict.items()}
        
        most_connected = None
        if connection_counts:
             # Ensure there's something to get max from, and handle ties if necessary (not done here)
            most_connected_item = max(connection_counts.items(), key=lambda x: x[1], default=None)
            if most_connected_item: # Ensure max returned something
                 most_connected = most_connected_item

        return {
            "table_connections": connection_dict,
            "connection_counts": connection_counts,
            "most_connected_table": most_connected,
            "isolated_tables": [table for table in tables if not connections.get(table)] # Simpler check
        }

    def _check_integrity_for_table_fks(self, table_item: Tuple[str, List[Dict[str, Any]]]) -> Tuple[str, List[Dict[str, Any]]]:
        """Helper to check referential integrity for a single table's foreign keys using a dedicated connection."""
        table_name, foreign_keys = table_item
        table_issues = []
        with self.get_connection() as conn:
            cursor = conn.cursor()
            for fk in foreign_keys:
                from_column, to_table, to_column = fk["from_column"], fk["table"], fk["to_column"]
                try:
                    query = f"""
                        SELECT COUNT(*) 
                        FROM `{table_name}` a 
                        LEFT JOIN `{to_table}` b ON a.`{from_column}` = b.`{to_column}`
                        WHERE a.`{from_column}` IS NOT NULL AND b.`{to_column}` IS NULL
                    """
                    cursor.execute(query)
                    orphaned_count = cursor.fetchone()[0]
                    if orphaned_count > 0:
                        table_issues.append({"foreign_key": fk, "issue_type": "orphaned_records", "orphaned_count": orphaned_count})
                except Exception as e:
                    table_issues.append({"foreign_key": fk, "issue_type": "check_error", "error": str(e)})
        return table_name, table_issues

    def _check_referential_integrity_from_data(self, fk_analysis: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Check referential integrity issues using pre-fetched FK data and multithreading."""
        integrity_issues_results = {}
        if not fk_analysis: return {}
        max_workers = os.cpu_count() or 4
        items_to_process = list(fk_analysis.items())

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self._check_integrity_for_table_fks, item) for item in items_to_process]
            for future in tqdm(as_completed(futures), total=len(items_to_process), desc="Checking referential integrity"):
                try:
                    table_name, issues_found = future.result()
                    if issues_found:
                        integrity_issues_results[table_name] = issues_found
                except Exception as e:
                    print(f"Error checking referential integrity (from data) during threaded execution: {e}")
        return integrity_issues_results
    
    def analyze_relationships(self) -> Dict[str, Any]:
        """Analyze all relationships in the database using parallel processing for major steps."""
        foreign_keys_data = self._analyze_foreign_keys()
        potential_relationships_data = self._find_potential_relationships()
        referential_integrity_data = self._check_referential_integrity_from_data(foreign_keys_data)
        table_connections_data = self._analyze_table_connections_from_data(foreign_keys_data, potential_relationships_data)
        
        results = {
            "foreign_keys": foreign_keys_data,
            "potential_relationships": potential_relationships_data,
            "table_connections": table_connections_data,
            "referential_integrity": referential_integrity_data,
        }
        results["relationship_summary"] = self._generate_relationship_summary(results)
        return results

    def _generate_relationship_summary(self, analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        fk_count = sum(len(fks) for fks in analysis_results.get("foreign_keys", {}).values())
        
        potential_strong, potential_moderate, potential_weak = 0, 0, 0
        for table_rels in analysis_results.get("potential_relationships", {}).values():
            for rel in table_rels:
                if rel["confidence"] > 0.7: potential_strong += 1
                elif rel["confidence"] > 0.5: potential_moderate += 1
                elif rel.get("confidence", 0) > 0.3 : potential_weak += 1 # Check if confidence exists
        
        integrity_issues_count = sum(len(issues) for issues in analysis_results.get("referential_integrity", {}).values())
        connections_data = analysis_results.get("table_connections", {})
        
        avg_conn = 0
        connection_counts = connections_data.get("connection_counts", {})
        if connection_counts: # Check if connection_counts is not empty
            avg_conn = round(sum(connection_counts.values()) / len(connection_counts), 2)

        return {
            "total_explicit_foreign_keys": fk_count,
            "potential_relationships": {"strong": potential_strong, "moderate": potential_moderate, "weak": potential_weak, "total": potential_strong + potential_moderate + potential_weak},
            "referential_integrity_issues": integrity_issues_count,
            "most_connected_table": connections_data.get("most_connected_table"),
            "isolated_tables_count": len(connections_data.get("isolated_tables", [])),
            "average_connections_per_table": avg_conn
        }

    # analyze_table_relationship and _calculate_relationship_strength are unchanged as they are for specific pairs, not batch.
    # If they were to be used in batch, they'd also need similar threading considerations.
    def analyze_table_relationship(self, table1: str, table2: str) -> Dict[str, Any]:
        """Analyze relationship between two specific tables"""
        with self.get_connection() as conn: # Uses a single connection for this specific task
            cursor = conn.cursor()
            
            cursor.execute(f"PRAGMA table_info(`{table1}`)")
            table1_cols = [(col[1], col[2]) for col in cursor.fetchall()]
            
            cursor.execute(f"PRAGMA table_info(`{table2}`)")
            table2_cols = [(col[1], col[2]) for col in cursor.fetchall()]
            
            potential_matches = self._find_column_matches(table1, table1_cols, table2, table2_cols, cursor)
            
            cursor.execute(f"PRAGMA foreign_key_list(`{table1}`)")
            fk_to_table2 = [fk_row for fk_row in cursor.fetchall() if fk_row[2] == table2]
            
            cursor.execute(f"PRAGMA foreign_key_list(`{table2}`)")
            fk_to_table1 = [fk_row for fk_row in cursor.fetchall() if fk_row[2] == table1]
            
            return {
                "table1": table1, "table2": table2,
                "explicit_foreign_keys": {"table1_to_table2": fk_to_table2, "table2_to_table1": fk_to_table1},
                "potential_relationships": potential_matches,
                "relationship_strength": self._calculate_relationship_strength(fk_to_table2, fk_to_table1, potential_matches)
            }
    
    def _calculate_relationship_strength(self, fk1: List, fk2: List, potential: List[Dict]) -> str:
        if fk1 or fk2: return "strong"
        if potential:
            max_confidence = max(rel.get("confidence", 0) for rel in potential) if potential else 0 # Safe max
            if max_confidence > 0.7: return "strong"
            if max_confidence > 0.5: return "moderate"
            if max_confidence > 0.3: return "weak"
        return "none"
