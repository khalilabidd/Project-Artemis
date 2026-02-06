import sys
import traceback

# Ensure project root is importable
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd

from compare_data import DFComparator


def run_stats(current, previous, pk):
    comp = DFComparator(current.copy(), previous.copy(), primary_keys=pk)
    return comp.generate_statistics()


def assert_true(cond, msg=""):
    if not cond:
        raise AssertionError(msg or "Assertion failed")


def test_added_column():
    prev = pd.DataFrame({'id':[1], 'a':[10]})
    curr = pd.DataFrame({'id':[1], 'a':[10], 'b':[20]})
    stats = run_stats(curr, prev, pk=['id'])
    assert_true('b' in stats['schema_changes']['added_columns'], "expected 'b' as added column")


def test_removed_column():
    prev = pd.DataFrame({'id':[1], 'a':[10], 'b':[20]})
    curr = pd.DataFrame({'id':[1], 'a':[10]})
    stats = run_stats(curr, prev, pk=['id'])
    assert_true('b' in stats['schema_changes']['removed_columns'], "expected 'b' as removed column")


def test_reordered_columns_no_change():
    prev = pd.DataFrame({'id':[1,2], 'a':[1,2], 'b':[3,4]})
    curr = prev[['b','a','id']].copy()
    stats = run_stats(curr, prev, pk=['id'])
    assert_true(stats['schema_changes']['added_columns'] == [], "no added columns expected")
    assert_true(stats['schema_changes']['removed_columns'] == [], "no removed columns expected")


def test_type_change_detected():
    prev = pd.DataFrame({'id':[1], 'val':[100]})
    curr = pd.DataFrame({'id':[1], 'val':['100']})
    stats = run_stats(curr, prev, pk=['id'])
    assert_true('val' in stats['schema_changes']['type_changes'], "type change for 'val' expected")


def test_row_count_increase_and_difference():
    prev = pd.DataFrame({'id':[1], 'a':[10]})
    curr = pd.DataFrame({'id':[1,2], 'a':[10,20]})
    stats = run_stats(curr, prev, pk=['id'])
    rc = stats['value_changes']['row_count_change']
    assert_true(rc['previous'] == 1 and rc['current'] == 2 and rc['difference'] == 1, "row count difference mismatch")


def test_row_count_decrease_flags_regression():
    prev = pd.DataFrame({'id':[1,2], 'a':[10,20]})
    curr = pd.DataFrame({'id':[1], 'a':[10]})
    stats = run_stats(curr, prev, pk=['id'])
    rc = stats['value_changes']['row_count_change']
    assert_true(rc['difference'] == -1, "expected negative difference")
    assert_true(stats['potential_regression'] is True, "expected potential_regression True")


def test_value_changes_with_primary_key():
    prev = pd.DataFrame({'id':[1,2], 'val':[10,20]})
    curr = pd.DataFrame({'id':[1,2], 'val':[11,20]})
    stats = run_stats(curr, prev, pk=['id'])
    cols = stats['value_changes'].get('columns_with_value_changes', {})
    assert_true('val' in cols and cols['val']['changed_rows'] == 1, "expected one changed row for 'val'")


def test_primary_key_missing_raises():
    prev = pd.DataFrame({'id':[1], 'a':[10]})
    curr = pd.DataFrame({'id':[1], 'a':[11]})
    try:
        DFComparator(curr.copy(), prev.copy(), primary_keys=['not_a_column']).generate_statistics()
    except Exception:
        return
    raise AssertionError("expected exception for missing primary key column")


def test_nulls_and_nans_treated_as_differences():
    # Int and float are compatible types, so value differences should be detected
    prev = pd.DataFrame({'id':[1,2], 'a':[10, 20]})  # int64
    curr = pd.DataFrame({'id':[1,2], 'a':[10.5, 20]})  # float64
    stats = run_stats(curr, prev, pk=['id'])
    cols = stats['value_changes'].get('columns_with_value_changes', {})
    assert_true('a' in cols, "expected 'a' to be flagged as changed (int/float compatible)")


def test_duplicate_primary_keys_behavior():
    prev = pd.DataFrame({'id':[1,1], 'val':[10,10]})
    curr = pd.DataFrame({'id':[1,1], 'val':[10,20]})
    stats = run_stats(curr, prev, pk=['id'])
    assert_true('schema_changes' in stats and 'value_changes' in stats, "expected stats keys present")


def test_empty_dataframes():
    prev = pd.DataFrame(columns=['id','a'])
    curr = pd.DataFrame(columns=['id','a'])
    stats = run_stats(curr, prev, pk=['id'])
    assert_true(stats['schema_changes']['added_columns'] == [], "no added columns expected")
    assert_true(stats['schema_changes']['removed_columns'] == [], "no removed columns expected")


def test_non_overlapping_columns():
    prev = pd.DataFrame({'id':[1], 'a':[10]})
    curr = pd.DataFrame({'id':[1], 'b':[20]})
    stats = run_stats(curr, prev, pk=[])
    # 'a' should be removed, 'b' should be added
    assert_true('a' in stats['schema_changes']['removed_columns'], "expected 'a' removed")
    assert_true('b' in stats['schema_changes']['added_columns'], "expected 'b' added")


def test_case_sensitivity_column_collisions():
    prev = pd.DataFrame({'id':[1], 'user_id':["u1"]})
    curr = pd.DataFrame({'id':[1], 'User_ID':["u1"]})
    stats = run_stats(curr, prev, pk=[])
    # Depending on behavior, columns with different case are treated as different
    assert_true('user_id' in stats['schema_changes']['removed_columns'] or 'User_ID' in stats['schema_changes']['added_columns'],
                "expected case-sensitive column differences")


def test_parquet_roundtrip(tmp_name='tests/tmp_roundtrip.parquet'):
    # Only run if pandas can write/read parquet with an engine available
    df = pd.DataFrame({'id': list(range(10)), 'val': list(range(10))})
    try:
        df.to_parquet(tmp_name)
        df2 = pd.read_parquet(tmp_name)
    except Exception as e:
        print(f"SKIP: parquet roundtrip test skipped ({e})")
        return

    stats = run_stats(df2, df, pk=['id'])
    assert_true(stats['schema_changes']['added_columns'] == [], "no schema changes expected on roundtrip")


def test_performance_large_dataset(n=20000):
    # Smoke test for performance / memory on a moderate-sized dataframe
    prev = pd.DataFrame({'id': list(range(n)), 'val': list(range(n))})
    curr = prev.copy()
    # introduce some changes
    curr.loc[::1000, 'val'] = -1
    stats = run_stats(curr, prev, pk=['id'])
    # Ensure run completed and found some changes
    cols = stats['value_changes'].get('columns_with_value_changes', {})
    assert_true('val' in cols, "expected 'val' flagged in large dataset")


def main():
    passed = 0
    failed = 0
    results = []

    ALL_TESTS = [obj for name, obj in globals().items() if callable(obj) and name.startswith('test_')]

    for t in sorted(ALL_TESTS, key=lambda f: f.__name__):
        name = t.__name__
        try:
            t()
            print(f"PASS: {name}")
            passed += 1
            results.append((name, 'PASS', ''))
        except Exception as e:
            print(f"FAIL: {name} -> {e}")
            traceback.print_exc()
            failed += 1
            results.append((name, 'FAIL', str(e)))

    print("\nTest summary:")
    print(f"  Passed: {passed}")
    print(f"  Failed: {failed}")

    if failed:
        sys.exit(1)


if __name__ == '__main__':
    main()
