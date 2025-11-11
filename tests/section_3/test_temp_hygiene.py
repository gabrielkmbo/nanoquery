import os
from nanoquery.storage import ColumnarDbFile
from nanoquery.hash_join import HashPartitionJoin
from nanoquery.sort_merge_join import SortMergeJoin
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def _write_parquet(df, path):
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)

def test_temp_dirs_unique(tmp_path):
    d = tmp_path / "hyg"; d.mkdir()
    songs = pd.DataFrame({"song_id":[1,1], "title":["A","A"]})
    listens = pd.DataFrame({"listen_id":[1,2], "song_id":[1,1], "user_id":[5,6]})
    _write_parquet(songs, d / "Songs.parquet")
    _write_parquet(listens, d / "Listens.parquet")
    s = ColumnarDbFile("Songs", file_dir=str(d))
    l = ColumnarDbFile("Listens", file_dir=str(d))
    out1 = HashPartitionJoin(2).join(s, l, "song_id", "song_id", temp_dir=str(tmp_path),
                                     cols_left=["song_id","title"], cols_right=["song_id","user_id","listen_id"])
    out2 = HashPartitionJoin(2).join(s, l, "song_id", "song_id", temp_dir=str(tmp_path),
                                     cols_left=["song_id","title"], cols_right=["song_id","user_id","listen_id"])
    assert os.path.dirname(out1.path) != os.path.dirname(out2.path)
    out3 = SortMergeJoin().join(s, l, "song_id", "song_id", temp_dir=str(tmp_path),
                                cols_left=["song_id","title"], cols_right=["song_id","user_id","listen_id"])
    out4 = SortMergeJoin().join(s, l, "song_id", "song_id", temp_dir=str(tmp_path),
                                cols_left=["song_id","title"], cols_right=["song_id","user_id","listen_id"])
    assert os.path.dirname(out3.path) != os.path.dirname(out4.path)


