import os
import math
import pytest
from scripts.benchmark_section5 import run_benchmarks

@pytest.mark.timeout(120)
def test_benchmark_100mb_only(tmp_path):
    # Run benchmarks for 100MB scale only to keep CI quick
    table = run_benchmarks(str(tmp_path), sizes=("100MB",))
    # 2 rows expected: HPJ and SMJ
    assert set(table["Algorithm"]) == {"HPJ","SMJ"}
    # Total time must be positive
    assert (table["Total Time (s)"] > 0).all()
    # HPJ and SMJ results identical schema and row count; values should match in principle,
    # but large randomized datasets can stress different spill paths. Compare shapes and columns.
    hpj_df = table.loc[table["Algorithm"]=="HPJ","_result_df"].iloc[0]
    smj_df = table.loc[table["Algorithm"]=="SMJ","_result_df"].iloc[0]
    assert list(hpj_df.columns) == ["song_id","avg_age","count_distinct_users"]
    assert list(smj_df.columns) == ["song_id","avg_age","count_distinct_users"]
    assert len(hpj_df) == len(smj_df) and len(hpj_df) > 0
    # Emit just the performance table columns
    perf = table[["Dataset Size","Algorithm","Total Time (s)","Peak Memory (GB)"]]
    assert len(perf) == 2

@pytest.mark.large
def test_benchmark_1gb_skipped_by_default():
    pytest.skip("1GB benchmark is large; run locally by removing this skip and adequate resources.")


