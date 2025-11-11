import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from nanoquery.aggregation import aggregate_final

def test_aggregation_matches_pandas(tmp_path):
    d = tmp_path / "agg"; d.mkdir()
    # Construct a joined-like dataframe with needed columns
    df = pd.DataFrame({
        "song_id":[1,1,2,2,3],
        "title":["A","A","B","B","C"],
        "user_id":[10,11,11,12,10],
        "age":[20,30,40,50,20],
    })
    path = d / "joined.parquet"
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)
    got = aggregate_final(str(path))
    exp = (df.groupby(["song_id","title"])
             .agg(avg_age=("age","mean"),
                  count_distinct_users=("user_id", pd.Series.nunique))
             .reset_index()
             .sort_values(["count_distinct_users","song_id"], ascending=[False, True]))
    got = got.sort_values(["count_distinct_users","song_id"], ascending=[False, True]).reset_index(drop=True)
    exp = exp[["song_id","avg_age","count_distinct_users"]].reset_index(drop=True)
    assert list(got.columns) == ["song_id","avg_age","count_distinct_users"]
    pd.testing.assert_frame_equal(got, exp)


