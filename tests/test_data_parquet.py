import pandas as pd

from compare_data import DFComparator


def run_stats(current, previous, pk):
    comp = DFComparator(current.copy(), previous.copy(), primary_keys=pk)
    return comp.generate_statistics()


def test_added_column():
    prev = pd.DataFrame({'id':[1], 'a':[10]})
    curr = pd.DataFrame({'id':[1], 'a':[10], 'b':[20]})
    stats = run_stats(curr, prev, pk=['id'])
    assert 'b' in stats['schema_changes']['added_columns']


def test_removed_column():
    prev = pd.DataFrame({'id':[1], 'a':[10], 'b':[20]})
    curr = pd.DataFrame({'id':[1], 'a':[10]})
    stats = run_stats(curr, prev, pk=['id'])
    assert 'b' in stats['schema_changes']['removed_columns']


def test_reordered_columns_no_change():
    prev = pd.DataFrame({'id':[1,2], 'a':[1,2], 'b':[3,4]})
    curr = prev[['b','a','id']].copy()
    stats = run_stats(curr, prev, pk=['id'])
    assert stats['schema_changes']['added_columns'] == []
    assert stats['schema_changes']['removed_columns'] == []


def test_type_change_detected():
    prev = pd.DataFrame({'id':[1], 'val':[100]})
    curr = pd.DataFrame({'id':[1], 'val':['100']})
    stats = run_stats(curr, prev, pk=['id'])
    assert 'val' in stats['schema_changes']['type_changes']


def test_row_count_increase_and_difference():
    prev = pd.DataFrame({'id':[1], 'a':[10]})
    curr = pd.DataFrame({'id':[1,2], 'a':[10,20]})
    stats = run_stats(curr, prev, pk=['id'])
    rc = stats['value_changes']['row_count_change']
    assert rc['previous'] == 1 and rc['current'] == 2 and rc['difference'] == 1


def test_row_count_decrease_flags_regression():
    prev = pd.DataFrame({'id':[1,2], 'a':[10,20]})
    curr = pd.DataFrame({'id':[1], 'a':[10]})
    stats = run_stats(curr, prev, pk=['id'])
    rc = stats['value_changes']['row_count_change']
    assert rc['difference'] == -1
    assert stats['potential_regression'] is True


def test_value_changes_with_primary_key():
    prev = pd.DataFrame({'id':[1,2], 'val':[10,20]})
    curr = pd.DataFrame({'id':[1,2], 'val':[11,20]})
    stats = run_stats(curr, prev, pk=['id'])
    cols = stats['value_changes'].get('columns_with_value_changes', {})
    assert 'val' in cols and cols['val']['changed_rows'] == 1


def test_primary_key_missing_raises():
    prev = pd.DataFrame({'id':[1], 'a':[10]})
    curr = pd.DataFrame({'id':[1], 'a':[11]})
    # provide an invalid PK to simulate missing PK column
    with pytest.raises(Exception):
        DFComparator(curr.copy(), prev.copy(), primary_keys=['not_a_column']).generate_statistics()


def test_nulls_and_nans_treated_as_differences():
    prev = pd.DataFrame({'id':[1,2], 'a':[None, 2]})
    curr = pd.DataFrame({'id':[1,2], 'a':[0, 2]})
    stats = run_stats(curr, prev, pk=['id'])
    cols = stats['value_changes'].get('columns_with_value_changes', {})
    assert 'a' in cols


def test_duplicate_primary_keys_behavior():
    prev = pd.DataFrame({'id':[1,1], 'val':[10,10]})
    curr = pd.DataFrame({'id':[1,1], 'val':[10,20]})
    # Ensure code runs and returns stats (behavior with duplicates is implementation-specific)
    stats = run_stats(curr, prev, pk=['id'])
    assert 'schema_changes' in stats and 'value_changes' in stats


def test_empty_dataframes():
    prev = pd.DataFrame(columns=['id','a'])
    curr = pd.DataFrame(columns=['id','a'])
    stats = run_stats(curr, prev, pk=['id'])
    assert stats['schema_changes']['added_columns'] == []
    assert stats['schema_changes']['removed_columns'] == []
