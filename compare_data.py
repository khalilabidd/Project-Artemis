import pandas as pd
from typing import Dict, List

class DFComparator:
    def __init__(self, df_current: pd.DataFrame, df_previous: pd.DataFrame, primary_keys: List[str] = None):
        self.current = df_current
        self.previous = df_previous
        self.primary_keys = primary_keys or []

    def _are_types_compatible(self, dtype1, dtype2) -> bool:
        """Check if two dtypes are comparable (e.g., int and float are compatible, but string and int are not)"""
        dtype1_str = str(dtype1).lower()
        dtype2_str = str(dtype2).lower()
        
        # Numeric types (int, uint, float, bool) are compatible with each other
        numeric_types = {'int', 'uint', 'float', 'bool'}
        is_numeric1 = any(t in dtype1_str for t in numeric_types)
        is_numeric2 = any(t in dtype2_str for t in numeric_types)
        
        if is_numeric1 and is_numeric2:
            return True
        
        # String types are compatible with each other
        string_types = {'object', 'string', 'str'}
        is_string1 = any(t in dtype1_str for t in string_types)
        is_string2 = any(t in dtype2_str for t in string_types)
        
        if is_string1 and is_string2:
            return True
        
        # Datetime types are compatible with each other
        datetime_types = {'datetime', 'datetime64', 'timestamp'}
        is_datetime1 = any(t in dtype1_str for t in datetime_types)
        is_datetime2 = any(t in dtype2_str for t in datetime_types)
        
        if is_datetime1 and is_datetime2:
            return True
        
        # Category/period types are compatible with themselves
        if dtype1_str == dtype2_str:
            return True
        
        return False
        
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
    
    def compare_values(self, schema_diff) -> Dict:
        """Compare values in common columns"""
        
        if len(self.current) != len(self.previous):
            return {"row_count_change": {
                "previous": len(self.previous),
                "current": len(self.current),
                "difference": len(self.current) - len(self.previous)
            }}
        
        current_len = len(self.current)
        previous_len = len(self.previous)

        # If both are empty, return safe empty comparison
        if current_len == 0 and previous_len == 0:
            return {
                "row_count_change": {"previous": 0, "current": 0, "difference": 0},
                "percentage_row_change": 0.0,
                "columns_with_value_changes": {}
            }

        # Identify columns with incompatible type changes (only exclude these)
        incompatible_type_cols = set()
        for col, change_info in schema_diff.get('type_changes', {}).items():
            prev_type = change_info['previous']
            curr_type = change_info['current']
            if not self._are_types_compatible(prev_type, curr_type):
                incompatible_type_cols.add(col)
        
        common_cols = list(set(self.current.columns) & set(self.previous.columns) - incompatible_type_cols)
        self.current['df_origin'] = 'current'
        self.previous['df_origin'] = 'previous'
        result = pd.concat([self.current[common_cols+ ['df_origin']], self.previous[common_cols+ ['df_origin']]], ignore_index=True).drop_duplicates(subset=common_cols, keep=False)
        self.current = result[result['df_origin'] == 'current']
        self.previous = result[result['df_origin'] == 'previous']
        del result
        self.current.drop(columns=['df_origin'], inplace=True)
        self.previous.drop(columns=['df_origin'], inplace=True)
        # prepare results container
        value_changes = {}

        if self.primary_keys:
            # Ensure primary keys exist
            for pk in self.primary_keys:
                if pk not in self.current.columns:
                    raise KeyError(f"Primary key column '{pk}' not found in both dataframe")

            try:
                self.current.set_index(self.primary_keys, inplace=True)
                self.previous.set_index(self.primary_keys, inplace=True)
                for col in self.current.columns:
                    # skip columns that are part of the index
                    if col in self.primary_keys:
                        continue
                    try:
                        s1 = self.current[col]
                        s2 = self.previous[col]
                        neq = (~((s1 == s2) | (s1.isna() & s2.isna()))).sum()
                    except ValueError:
                        # alignment issue (e.g., duplicate index labels) -> trigger fallback
                        raise

                    if neq > 0:
                        value_changes[col] = {
                            "changed_rows": int(neq),
                            "percentage": round((neq / current_len) * 100, 2) if current_len else 0.0
                        }
            except Exception:
                # Fallback: merge on primary keys to support duplicate PKs and misaligned labels
                self.previous.reset_index(inplace=True)
                self.current.reset_index(inplace=True)
                result = self.previous.merge(self.current, on=self.primary_keys, how='outer', suffixes=('_prev', '_curr'), indicator=True)
                # columns to check are the intersection of both sides excluding PKs
                both_mask = result['_merge'] == 'both'
                cols_to_check = [c for c in self.current.columns if c not in self.primary_keys]
                for col in cols_to_check:
                    lcol = f"{col}_prev"
                    rcol = f"{col}_curr"
                    if lcol not in result.columns or rcol not in result.columns:
                        continue
                    s1 = result.loc[both_mask, lcol]
                    s2 = result.loc[both_mask, rcol]
                    neq = (~((s1 == s2) | (s1.isna() & s2.isna()))).sum()
                    if neq > 0:
                        value_changes[col] = {
                            "changed_rows": int(neq),
                            "percentage": round((neq / current_len) * 100, 2) if current_len else 0.0
                        }

        pct_change = round((len(self.current) / current_len) * 100, 2) if current_len else 0.0
        return {
            "row_count_change": {
                "previous": previous_len,
                "current": current_len,
                "difference": current_len - previous_len
            },
            "percentage_row_change": pct_change,
            "columns_with_value_changes": value_changes
        }
    
    def generate_statistics(self) -> Dict:
        """Generate comprehensive comparison statistics"""
        schema_diff = self.compare_schema()
        value_diff = self.compare_values(schema_diff)
        
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
        print(f"  Row difference: {stats['value_changes'].get('percentage_row_change', 0)}%")
        
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
    df_previous = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'user_id': ['U001', 'U002', 'U003', 'U004', 'U005'],
        'product_name': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones'],
        'price': [999.99, 25.50, 89.99, 299.99, 149.99],
        'quantity': [10, 50, 30, 15, 25],
    })
    
    # Second dataframe (current version) - same primary keys, different values
    df_current = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'user_id': ['U001', 'U002', 'U003', 'U004', 'U005'],
        'product_name': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones'],
        'price': [1199.99, 35.50, 129.99, 299.99, 179.99],  # price changes
        'quantity': ['8', '42', '30', '20', '25'],  # quantity changes
        'status': ['active', 'active', 'inactive', 'active', 'inactive']  # status changes
    })
    
    comparator = DFComparator(df_current, df_previous, primary_keys=['id', 'user_id'])
    stats = comparator.print_report()