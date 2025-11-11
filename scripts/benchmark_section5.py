import os, time
import psutil
import pandas as pd
from typing import Tuple
from nanoquery.section4_planner import QueryExecutor, QueryPlanner
from scripts.generate_data import generate_songs, generate_users, generate_listens, write_parquet

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

def _make_dataset(dir_path: str, size_label: str):
    os.makedirs(dir_path, exist_ok=True)
    if size_label == "100MB":
        n_songs, n_users, n_listens = 10_000, 50_000, 1_000_000
    elif size_label == "1GB":
        n_songs, n_users, n_listens = 100_000, 500_000, 10_000_000
    else:
        raise ValueError("size_label must be '100MB' or '1GB'")
    write_parquet(generate_songs(n_songs), os.path.join(dir_path, "Songs.parquet"))
    write_parquet(generate_users(n_users), os.path.join(dir_path, "Users.parquet"))
    write_parquet(generate_listens(n_listens, n_users, n_songs), os.path.join(dir_path, "Listens.parquet"))
    return {
        "Songs": os.path.join(dir_path, "Songs.parquet"),
        "Users": os.path.join(dir_path, "Users.parquet"),
        "Listens": os.path.join(dir_path, "Listens.parquet"),
    }

def _run_once(paths: dict, algo: str, workdir: str) -> Tuple[pd.DataFrame, float, float, float]:
    proc = psutil.Process(os.getpid())
    start_mem = proc.memory_info().rss
    t0 = time.time()
    execu = QueryExecutor(paths, working_dir=workdir, planner=ForcedPlanner(algo, algo))
    df, plan = execu.execute(SQL)
    t1 = time.time()
    # Rough memory captured post-exec; peak would need a profiler, we approximate
    end_mem = proc.memory_info().rss
    total_s = t1 - t0
    peak_mem_gb = max(end_mem, start_mem) / (1024**3)
    # We don't separate I/O vs join precisely; leave as total and None fields
    return df, total_s, None, peak_mem_gb

def run_benchmarks(base_dir: str, sizes=("100MB","1GB")) -> pd.DataFrame:
    rows = []
    for sz in sizes:
        ds_dir = os.path.join(base_dir, f"data_{sz}")
        paths = _make_dataset(ds_dir, sz)
        for algo in ("HPJ","SMJ"):
            df, total_s, io_s, peak_gb = _run_once(paths, algo, os.path.join(base_dir, f"work_{sz}_{algo}"))
            rows.append({
                "Dataset Size": sz,
                "Algorithm": algo,
                "Total Time (s)": total_s,
                "I/O Time (s)": io_s if io_s is not None else float("nan"),
                "Join Time (s)": float("nan"),
                "Peak Memory (GB)": peak_gb,
                "_result_df": df,  # for validation outside
            })
    return pd.DataFrame(rows)

if __name__ == "__main__":
    df = run_benchmarks("bench_out", sizes=("100MB",))
    print(df[["Dataset Size","Algorithm","Total Time (s)","I/O Time (s)","Join Time (s)","Peak Memory (GB)"]])


