import pandas as pd
import pytest
from nanoquery.executor import QueryExecutor
from nanoquery.planner import QueryPlanner

SQL = """SELECT s.song_id, AVG(u.age) AS avg_age,
COUNT(DISTINCT l.user_id)
FROM Songs s
JOIN Listens l ON s.song_id = l.song_id
JOIN Users u ON l.user_id = u.user_id
GROUP BY s.song_id, s.title
ORDER BY COUNT(DISTINCT l.user_id) DESC, s.song_id;"""

class ForcedPlanner(QueryPlanner):
    def __init__(self, force_algo1: str, force_algo2: str):
        super().__init__()
        self.force_algo1 = force_algo1
        self.force_algo2 = force_algo2
    def plan(self, parquet_paths: dict, parsed_query: dict, avail_mem_mb: float = 10_000):
        plan = super().plan(parquet_paths, parsed_query, avail_mem_mb)
        plan["steps"][0]["algo"] = self.force_algo1
        plan["steps"][1]["algo"] = self.force_algo2
        return plan

@pytest.mark.parametrize("algo", ["HPJ","SMJ"])
def test_end_to_end_forced(tmp_data_dir, tmp_path, algo):
    paths = {"Songs": f"{tmp_data_dir}/Songs.parquet",
             "Listens": f"{tmp_data_dir}/Listens.parquet",
             "Users": f"{tmp_data_dir}/Users.parquet"}
    execu = QueryExecutor(paths, working_dir=str(tmp_path), planner=ForcedPlanner(algo, algo))
    df, plan = execu.execute(SQL)
    assert list(df.columns) == ["song_id", "avg_age", "count_distinct_users"]
    assert df["count_distinct_users"].is_monotonic_decreasing

def test_end_to_end_hpjsmj_identical(tmp_data_dir, tmp_path):
    paths = {"Songs": f"{tmp_data_dir}/Songs.parquet",
             "Listens": f"{tmp_data_dir}/Listens.parquet",
             "Users": f"{tmp_data_dir}/Users.parquet"}
    exec_hp = QueryExecutor(paths, working_dir=str(tmp_path), planner=ForcedPlanner("HPJ","HPJ"))
    exec_sm = QueryExecutor(paths, working_dir=str(tmp_path), planner=ForcedPlanner("SMJ","SMJ"))
    dfh, _ = exec_hp.execute(SQL)
    dfs, _ = exec_sm.execute(SQL)
    pd.testing.assert_frame_equal(dfh.reset_index(drop=True), dfs.reset_index(drop=True))


