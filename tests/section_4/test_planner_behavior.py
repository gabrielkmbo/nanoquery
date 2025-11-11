import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from nanoquery.planner import QueryPlanner, choose_algo
from nanoquery.parser import parse_sql_hardcoded
from nanoquery.executor import QueryExecutor
import pytest

def _write_parquet(df, path):
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)

def test_planner_metadata_only(tmp_path):
    d = tmp_path / "meta"; d.mkdir()
    _write_parquet(pd.DataFrame({"song_id":[1], "title":["A"]}), d / "Songs.parquet")
    _write_parquet(pd.DataFrame({"user_id":[1], "age":[20]}), d / "Users.parquet")
    _write_parquet(pd.DataFrame({"listen_id":[1], "song_id":[1], "user_id":[1]}), d / "Listens.parquet")
    qp = QueryPlanner()
    plan = qp.plan({"Songs": str(d / "Songs.parquet"),
                    "Users": str(d / "Users.parquet"),
                    "Listens": str(d / "Listens.parquet")},
                   parse_sql_hardcoded("ignored"))
    assert "metadata" in plan and all(k in plan["metadata"] for k in ["Songs","Users","Listens"])
    assert plan["metadata"]["Songs"]["rows"] == 1
    assert "columns" in plan and set(plan["columns"]["Songs"]) == {"song_id","title"}

def test_column_pruning_executor(tmp_path, monkeypatch):
    d = tmp_path / "prune"; d.mkdir()
    _write_parquet(pd.DataFrame({"song_id":[1], "title":["A"], "extra":[0]}), d / "Songs.parquet")
    _write_parquet(pd.DataFrame({"user_id":[1], "age":[20], "e":[0]}), d / "Users.parquet")
    _write_parquet(pd.DataFrame({"listen_id":[1], "song_id":[1], "user_id":[1], "e":[0]}), d / "Listens.parquet")
    paths = {"Songs": str(d / "Songs.parquet"),
             "Listens": str(d / "Listens.parquet"),
             "Users": str(d / "Users.parquet")}
    calls = []
    import pandas as _pd
    real_read = _pd.read_parquet
    def spy_read(path, columns=None, **kw):
        calls.append((os.path.basename(str(path)), tuple(columns) if columns else None))
        return real_read(path, columns=columns, **kw)
    monkeypatch.setattr(_pd, "read_parquet", spy_read)
    from nanoquery.executor import QueryExecutor
    ex = QueryExecutor(paths, working_dir=str(tmp_path))
    SQL = "SELECT s.song_id, AVG(u.age) AS avg_age, COUNT(DISTINCT l.user_id) FROM Songs s JOIN Listens l ON s.song_id = l.song_id JOIN Users u ON l.user_id = u.user_id GROUP BY s.song_id, s.title ORDER BY COUNT(DISTINCT l.user_id) DESC, s.song_id;"
    ex.execute(SQL)
    # Ensure pruning happened
    assert ("Songs.parquet", ("song_id","title")) in calls
    assert ("Users.parquet", ("user_id","age")) in calls
    assert ("Listens.parquet", ("song_id","user_id")) in calls

def test_choose_algo_thresholds():
    # size * overhead < avail_mem_mb -> HPJ, else SMJ
    assert choose_algo(100, avail_mem_mb=2000, overhead=5.0) == "HPJ"
    assert choose_algo(100, avail_mem_mb=400, overhead=5.0) == "SMJ"

def test_join_order_enforced(tmp_path):
    d = tmp_path / "ord"; d.mkdir()
    _write_parquet(pd.DataFrame({"song_id":[1], "title":["A"]}), d / "Songs.parquet")
    _write_parquet(pd.DataFrame({"user_id":[1], "age":[20]}), d / "Users.parquet")
    _write_parquet(pd.DataFrame({"listen_id":[1], "song_id":[1], "user_id":[1]}), d / "Listens.parquet")
    qp = QueryPlanner()
    plan = qp.plan({"Songs": str(d / "Songs.parquet"),
                    "Users": str(d / "Users.parquet"),
                    "Listens": str(d / "Listens.parquet")},
                   parse_sql_hardcoded("ignored"))
    assert plan["steps"][0]["left"] == "Songs" and plan["steps"][0]["right"] == "Listens"
    assert plan["steps"][1]["right"] == "Users"


