import os
from .utils import parquet_metadata

def choose_algo(size_smaller_mb: float, avail_mem_mb: float = 10_000, overhead: float = 5.0) -> str:
    return "HPJ" if size_smaller_mb * overhead < avail_mem_mb else "SMJ"

class QueryPlanner:
    """
    Uses Parquet metadata + hardcoded query semantics to:
    - choose join order (Songs ⨝ Listens) → ⨝ Users
    - pick HPJ vs SMJ per step
    - determine exact columns to read
    """
    def plan(self, parquet_paths: dict, parsed_query: dict, avail_mem_mb: float = 10_000):
        meta = {t: parquet_metadata(p) for t, p in parquet_paths.items()}
        cols = parsed_query["needed_columns"]

        # naive in-memory size est: each int64 ≈ 8B/val; MB = rows*8/1e6 * num_needed_cols
        est_mb = {t: (meta[t]["rows"] * 8 * len(cols[t])) / (1024*1024) for t in cols}
        step1 = {"left": "Songs", "right": "Listens", "left_key": "song_id", "right_key": "song_id"}
        step2 = {"left": "__prev__", "right": "Users",  "left_key": "user_id", "right_key": "user_id"}

        algo1 = choose_algo(min(est_mb["Songs"], est_mb["Listens"]), avail_mem_mb)
        algo2 = choose_algo(min(est_mb["Users"], est_mb["Listens"]), avail_mem_mb)
        step1["algo"] = algo1
        step2["algo"] = algo2

        return {"columns": cols, "steps": [step1, step2], "metadata": meta}
