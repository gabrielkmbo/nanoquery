import pandas as pd

def aggregate_final(joined_parquet_path: str) -> pd.DataFrame:
    # Expect columns: song_id, title, age, user_id (after both joins)
    df = pd.read_parquet(joined_parquet_path, columns=["song_id", "title", "age", "user_id"])
    res = (df.groupby(["song_id", "title"])
             .agg(avg_age=("age", "mean"),
                  count_distinct_users=("user_id", pd.Series.nunique))
             .reset_index())
    res = res.sort_values(["count_distinct_users", "song_id"], ascending=[False, True])
    return res[["song_id", "avg_age", "count_distinct_users"]]
