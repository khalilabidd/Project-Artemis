import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from typing import Dict, List, Tuple
import json

class ParquetComparator:
    def __init__(self, current_file: str, previous_file: str):
        self.current = pd.read_parquet(current_file)
        self.previous = pd.read_parquet(previous_file)
        self.current_schema = pq.read_schema(current_file)
        self.previous_schema = pq.read_schema(previous_file)
        
    def compare_schema(self) -> Dict:
        """Compare column names and types"""
        current_cols = set(self.current.columns)
        previous_cols = set(self.previous.columns)
        
        added_cols = current_cols - previous_cols
        removed_cols = previous_cols - current_cols
        common_cols = current_cols & previous_cols
        
        type_changes = {}
        for col in common_cols:
            curr_type = str(self.current[col].dtype)
            prev_type = str(self.previous[col].dtype)
            if curr_type != prev_type:
                type_changes[col] = {"previous": prev_type, "current": curr_type}
        
        return {
            "added_columns": list(added_cols),
            "removed_columns": list(removed_cols),
            "type_changes": type_changes,
            "total_columns_current": len(self.current.columns),
            "total_columns_previous": len(self.previous.columns)
        }
    
    def compare_values(self) -> Dict:
        """Compare values in common columns"""
        common_cols = set(self.current.columns) & set(self.previous.columns)
        
        if len(self.current) != len(self.previous):
            return {"row_count_change": {
                "previous": len(self.previous),
                "current": len(self.current),
                "difference": len(self.current) - len(self.previous)
            }}
        
        value_changes = {}
        for col in common_cols:
            if self.current[col].dtype == self.previous[col].dtype:
                differences = (self.current[col] != self.previous[col]).sum()
                if differences > 0:
                    value_changes[col] = {
                        "changed_rows": int(differences),
                        "percentage": round((differences / len(self.current)) * 100, 2)
                    }
        
        return {
            "row_count_change": {
                "previous": len(self.previous),
                "current": len(self.current)
            },
            "columns_with_value_changes": value_changes
        }
    
    def generate_statistics(self) -> Dict:
        """Generate comprehensive comparison statistics"""
        schema_diff = self.compare_schema()
        value_diff = self.compare_values()
        
        has_regression = bool(
            schema_diff["removed_columns"] or 
            schema_diff["type_changes"] or 
            (value_diff.get("row_count_change", {}).get("difference", 0) < 0) or
            value_diff.get("columns_with_value_changes")
        )
        
        return {
            "timestamp": pd.Timestamp.now().isoformat(),
            "schema_changes": schema_diff,
            "value_changes": value_diff,
            "potential_regression": has_regression,
            "summary": {
                "total_schema_issues": len(schema_diff["removed_columns"]) + len(schema_diff["type_changes"]),
                "columns_with_value_changes": len(value_diff.get("columns_with_value_changes", {}))
            }
        }
    
    def print_report(self):
        """Print formatted comparison report"""
        stats = self.generate_statistics()
        
        print("\n" + "="*60)
        print("PARQUET FILE COMPARISON REPORT")
        print("="*60)
        
        print("\n📊 SCHEMA CHANGES:")
        print(f"  Added columns: {stats['schema_changes']['added_columns'] or 'None'}")
        print(f"  Removed columns: {stats['schema_changes']['removed_columns'] or 'None'}")
        print(f"  Type changes: {stats['schema_changes']['type_changes'] or 'None'}")
        
        print("\n📈 VALUE CHANGES:")
        row_change = stats['value_changes']['row_count_change']
        print(f"  Previous rows: {row_change['previous']}")
        print(f"  Current rows: {row_change['current']}")
        print(f"  Row difference: {row_change.get('difference', 0)}")
        
        if stats['value_changes'].get('columns_with_value_changes'):
            print("\n  Columns with value changes:")
            for col, info in stats['value_changes']['columns_with_value_changes'].items():
                print(f"    - {col}: {info['changed_rows']} rows ({info['percentage']}%)")
        
        print("\n⚠️  REGRESSION ALERT:" if stats['potential_regression'] else "\n✅ NO REGRESSION DETECTED")
        print(f"  Potential regression: {stats['potential_regression']}")
        print("="*60 + "\n")
        
        return stats


# Usage example
if __name__ == "__main__":
    current_parquet = r"c:\Users\khali\projects\project\data_current.parquet"
    previous_parquet = r"c:\Users\khali\projects\project\data_previous.parquet"
    
    comparator = ParquetComparator(current_parquet, previous_parquet)
    stats = comparator.print_report()
    
    # Save report to JSON
    with open(r"c:\Users\khali\projects\project\comparison_report.json", "w") as f:
        json.dump(stats, f, indent=2)
    print("Report saved to comparison_report.json")