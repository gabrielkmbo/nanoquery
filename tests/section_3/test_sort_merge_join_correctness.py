import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nanoquery.storage import ColumnarDbFile
from nanoquery.sort_merge_join import SortMergeJoin

def _write_parquet(df, path):
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)

@pytest.fixture
def tiny_data(tmp_path):
    d = tmp_path / "tiny"
    d.mkdir()
    songs = pd.DataFrame({"song_id":[1,1,2,3], "title":["A","A","B","C"]})
    listens = pd.DataFrame({
        "listen_id":[100,101,102,103,104],
        "song_id":[1,1,2,2,3],
        "user_id":[10,11,11,12,10],
    })
    _write_parquet(songs, d / "Songs.parquet")
    _write_parquet(listens, d / "Listens.parquet")
    return str(d)

def _smj_join_dir(in_dir, tmp_dir, left_cols, right_cols):
    songs = ColumnarDbFile("Songs", file_dir=in_dir)
    listens = ColumnarDbFile("Listens", file_dir=in_dir)
    out = SortMergeJoin().join(songs, listens, "song_id", "song_id",
                               temp_dir=tmp_dir, cols_left=left_cols, cols_right=right_cols)
    return pd.read_parquet(out.path)

def test_smj_matches_pandas_unsorted(tiny_data, tmp_path):
    tmp_dir = str(tmp_path)
    df_out = _smj_join_dir(tiny_data, tmp_dir, ["song_id","title"], ["song_id","user_id","listen_id"])
    l = pd.read_parquet(os.path.join(tiny_data, "Songs.parquet"))[["song_id","title"]]
    r = pd.read_parquet(os.path.join(tiny_data, "Listens.parquet"))[["song_id","user_id","listen_id"]]
    baseline = l.merge(r, on="song_id", how="inner")
    # Align columns order before equality
    common = list(baseline.columns)
    assert df_out[common].sort_values(common).reset_index(drop=True).equals(
        baseline[common].sort_values(common).reset_index(drop=True)
    )

def test_smj_matches_pandas_already_sorted(tmp_path):
    d = tmp_path / "sorted"; d.mkdir()
    songs = pd.DataFrame({"song_id":[1,1,2,3], "title":["A","A","B","C"]}).sort_values("song_id")
    listens = pd.DataFrame({
        "listen_id":[100,101,102,103,104],
        "song_id":[1,1,2,2,3],
        "user_id":[10,11,11,12,10],
    }).sort_values("song_id")
    _write_parquet(songs, d / "Songs.parquet")
    _write_parquet(listens, d / "Listens.parquet")
    df_out = _smj_join_dir(str(d), str(tmp_path), ["song_id","title"], ["song_id","user_id","listen_id"])
    baseline = songs.merge(listens, on="song_id", how="inner")
    common = list(baseline.columns)
    assert df_out[common].sort_values(common).reset_index(drop=True).equals(
        baseline[common].sort_values(common).reset_index(drop=True)
    )


