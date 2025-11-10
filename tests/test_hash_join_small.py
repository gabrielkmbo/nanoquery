import os, pandas as pd
from nanoquery.storage import ColumnarDbFile
from nanoquery.hash_join import HashPartitionJoin

def test_hpj_small(tmp_data_dir, tmp_path):
    songs = ColumnarDbFile("Songs", file_dir=tmp_data_dir)
    listens = ColumnarDbFile("Listens", file_dir=tmp_data_dir)
    out = HashPartitionJoin(4).join(songs, listens, "song_id", "song_id",
                                    temp_dir=str(tmp_path),
                                    cols_left=["song_id", "title"],
                                    cols_right=["song_id", "user_id", "listen_id"])
    df = pd.read_parquet(out.path)
    assert {"song_id","title","user_id"}.issubset(df.columns)
    # Simple correctness sanity: song_id values in output must exist in both inputs
    assert set(df["song_id"]).issubset({1,2,3})
