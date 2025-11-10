import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os, gc

def generate_base_strings(n, k):
    chars = np.array(list("ab"))
    rnd = np.random.randint(0, len(chars), size=(n, k))
    return np.array(list(map("".join, chars[rnd])))

def generate_songs(n, string_length=100):
    df = pd.DataFrame({
        "song_id": range(n),
        "title": [f"Song_{i}" for i in range(n)],
    })
    base = generate_base_strings(n, string_length)
    for i in range(1, 11):
        df[f"extra_col_{i}"] = np.roll(base, i)
    return df

def generate_users(n, string_length=100):
    df = pd.DataFrame({
        "user_id": range(n),
        "age": [18 + (i % 60) for i in range(n)],
    })
    base = generate_base_strings(n, string_length)
    for i in range(1, 11):
        df[f"extra_col_{i}"] = np.roll(base, i)
    return df

def generate_listens(n, num_users, num_songs, string_length=16):
    df = pd.DataFrame({
        "listen_id": range(n),
        "user_id": np.random.randint(0, num_users, size=n),
        "song_id": np.random.randint(0, num_songs, size=n),
    })
    base = generate_base_strings(n, string_length)
    for i in range(1, 11):
        df[f"extra_col_{i}"] = np.roll(base, i)
    return df

def write_parquet(df, path, compression="snappy", row_group_size=50_000):
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path,
                   compression=compression, row_group_size=row_group_size)

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    # ~100MB compressed (ballpark)
    songs = generate_songs(10_000)
    users = generate_users(50_000)
    listens = generate_listens(1_000_000, num_users=len(users), num_songs=len(songs))
    write_parquet(songs, "data/Songs.parquet")
    write_parquet(users, "data/Users.parquet")
    write_parquet(listens, "data/Listens.parquet")
    print("Wrote data/*.parquet")
