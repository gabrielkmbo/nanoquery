import pandas as pd
from nanoquery.executor import QueryExecutor

SQL = """SELECT s.song_id, AVG(u.age) AS avg_age,
COUNT(DISTINCT l.user_id)
FROM Songs s
JOIN Listens l ON s.song_id = l.song_id
JOIN Users u ON l.user_id = u.user_id
GROUP BY s.song_id, s.title
ORDER BY COUNT(DISTINCT l.user_id) DESC, s.song_id;"""

def test_end_to_end(tmp_data_dir, tmp_path):
    paths = {
        "Songs": f"{tmp_data_dir}/Songs.parquet",
        "Listens": f"{tmp_data_dir}/Listens.parquet",
        "Users": f"{tmp_data_dir}/Users.parquet",
    }
    execu = QueryExecutor(paths, working_dir=str(tmp_path))
    df, plan = execu.execute(SQL)
    # Columns and sort order
    assert list(df.columns) == ["song_id", "avg_age", "count_distinct_users"]
    assert df["count_distinct_users"].is_monotonic_decreasing
