import os, pandas as pd, numpy as np, pyarrow as pa, pyarrow.parquet as pq
import pytest

@pytest.fixture(scope="session")
def tmp_data_dir(tmp_path_factory):
    d = tmp_path_factory.mktemp("data")
    # tiny synthetic data with duplicates
    songs = pd.DataFrame({
        "song_id": [1,1,2,3],
        "title":   ["A","A","B","C"],
    })
    users = pd.DataFrame({
        "user_id": [10,11,11,12],
        "age":     [20,30,40,50],
    })
    listens = pd.DataFrame({
        "listen_id": [100,101,102,103,104],
        "song_id":   [1,1,2,2,3],
        "user_id":   [10,11,11,12,10],
    })
    pq.write_table(pa.Table.from_pandas(songs, preserve_index=False), os.path.join(d, "Songs.parquet"))
    pq.write_table(pa.Table.from_pandas(users, preserve_index=False), os.path.join(d, "Users.parquet"))
    pq.write_table(pa.Table.from_pandas(listens, preserve_index=False), os.path.join(d, "Listens.parquet"))
    return str(d)
