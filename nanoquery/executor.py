import os
import pandas as pd
from .storage import ColumnarDbFile
from .parser import parse_sql_hardcoded
from .planner import QueryPlanner
from .hash_join import HashPartitionJoin
from .sort_merge_join import SortMergeJoin
from .aggregation import aggregate_final

class QueryExecutor:
    def __init__(self, parquet_paths: dict, working_dir: str = "temp", planner: QueryPlanner | None = None):
        """
        parquet_paths: {"Songs": ".../songs.parquet", "Listens": "...", "Users": "..."}
        """
        self.paths = parquet_paths
        self.working_dir = working_dir
        os.makedirs(self.working_dir, exist_ok=True)
        self.planner = planner or QueryPlanner()

    def _load_cols_to_temp(self, table_name: str, cols: list[str]) -> ColumnarDbFile:
        cdf_src = ColumnarDbFile(table_name, file_dir=os.path.dirname(self.paths[table_name]))
        df = pd.read_parquet(self.paths[table_name], columns=cols)
        cdf_dst = ColumnarDbFile(f"{table_name}_loaded", file_dir=self.working_dir)
        cdf_dst.build_table(df)
        return cdf_dst

    def execute(self, sql: str):
        parsed = parse_sql_hardcoded(sql)
        plan = self.planner.plan(self.paths, parsed)

        # Column pruning loads
        songs = self._load_cols_to_temp("Songs", plan["columns"]["Songs"])
        listens = self._load_cols_to_temp("Listens", plan["columns"]["Listens"])
        users = self._load_cols_to_temp("Users", plan["columns"]["Users"])

        # Step 1: Songs ⨝ Listens on song_id
        if plan["steps"][0]["algo"] == "HPJ":
            step1_out = HashPartitionJoin(8).join(songs, listens, "song_id", "song_id",
                                                  temp_dir=self.working_dir,
                                                  cols_left=plan["columns"]["Songs"],
                                                  cols_right=plan["columns"]["Listens"])
        else:
            step1_out = SortMergeJoin().join(songs, listens, "song_id", "song_id",
                                             temp_dir=self.working_dir,
                                             cols_left=plan["columns"]["Songs"],
                                             cols_right=plan["columns"]["Listens"])

        # Step 2: (step1) ⨝ Users on user_id
        if plan["steps"][1]["algo"] == "HPJ":
            step2_out = HashPartitionJoin(8).join(step1_out, users, "user_id", "user_id",
                                                  temp_dir=self.working_dir,
                                                  cols_left=None, cols_right=plan["columns"]["Users"])
        else:
            step2_out = SortMergeJoin().join(step1_out, users, "user_id", "user_id",
                                             temp_dir=self.working_dir,
                                             cols_left=None, cols_right=plan["columns"]["Users"])

        # Final aggregation
        result = aggregate_final(step2_out.path)
        return result, plan
