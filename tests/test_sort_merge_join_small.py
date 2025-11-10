import os, pandas as pd
from nanoquery.storage import ColumnarDbFile
from nanoquery.sort_merge_join import SortMergeJoin

def test_smj_small(tmp_data_dir, tmp_path):
    songs = ColumnarDbFile("Songs", file_dir=tmp_data_dir)
    listens = ColumnarDbFile("Listens", file_dir=tmp_data_dir)
    out = SortMergeJoin().join(songs, listens, "song_id", "song_id",
                               temp_dir=str(tmp_path),
                               cols_left=["song_id", "title"],
                               cols_right=["song_id", "user_id", "listen_id"])
    df = pd.read_parquet(out.path)
    assert len(df) > 0
