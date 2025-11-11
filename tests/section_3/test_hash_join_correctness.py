import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nanoquery.storage import ColumnarDbFile
from nanoquery.hash_join import HashPartitionJoin

def _write_parquet(df, path):
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)

@pytest.fixture
def tiny_data(tmp_path):
    d = tmp_path / "tiny"
    d.mkdir()
    # Many-to-many duplicates
    songs = pd.DataFrame({"song_id":[1,1,2,3], "title":["A","A","B","C"]})
    listens = pd.DataFrame({
        "listen_id":[100,101,102,103,104],
        "song_id":[1,1,2,2,3],
        "user_id":[10,11,11,12,10],
    })
    _write_parquet(songs, d / "Songs.parquet")
    _write_parquet(listens, d / "Listens.parquet")
    return str(d)

def _hpj_join_dir(in_dir, tmp_dir, left_cols, right_cols):
    songs = ColumnarDbFile("Songs", file_dir=in_dir)
    listens = ColumnarDbFile("Listens", file_dir=in_dir)
    out = HashPartitionJoin(4).join(songs, listens, "song_id", "song_id",
                                    temp_dir=tmp_dir, cols_left=left_cols, cols_right=right_cols)
    return pd.read_parquet(out.path)

def test_hpj_matches_pandas_many_to_many(tiny_data, tmp_path):
    tmp_dir = str(tmp_path)
    df_out = _hpj_join_dir(tiny_data, tmp_dir, ["song_id","title"], ["song_id","user_id","listen_id"])
    # pandas baseline
    l = pd.read_parquet(os.path.join(tiny_data, "Songs.parquet"))[["song_id","title"]]
    r = pd.read_parquet(os.path.join(tiny_data, "Listens.parquet"))[["song_id","user_id","listen_id"]]
    baseline = l.merge(r, on="song_id", how="inner")
    assert set(df_out.columns) == set(baseline.columns)
    assert df_out.sort_values(list(df_out.columns)).reset_index(drop=True).equals(
        baseline.sort_values(list(baseline.columns)).reset_index(drop=True)
    )

def test_hpj_no_matches(tmp_path):
    d = tmp_path / "nomatch"; d.mkdir()
    songs = pd.DataFrame({"song_id":[10], "title":["X"]})
    listens = pd.DataFrame({"listen_id":[1], "song_id":[99], "user_id":[5]})
    _write_parquet(songs, d / "Songs.parquet")
    _write_parquet(listens, d / "Listens.parquet")
    df_out = _hpj_join_dir(str(d), str(tmp_path), ["song_id","title"], ["song_id","user_id","listen_id"])
    assert len(df_out) == 0

def test_hpj_empty_side(tmp_path):
    d = tmp_path / "empty"; d.mkdir()
    songs = pd.DataFrame({"song_id":[1,2], "title":["A","B"]})
    listens = pd.DataFrame({"listen_id":[], "song_id":[], "user_id":[]})
    _write_parquet(songs, d / "Songs.parquet")
    _write_parquet(listens, d / "Listens.parquet")
    df_out = _hpj_join_dir(str(d), str(tmp_path), ["song_id","title"], ["song_id","user_id","listen_id"])
    assert df_out.empty


