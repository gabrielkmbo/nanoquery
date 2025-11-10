import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

class ColumnarDbFile:
    """
    Thin wrapper: one Parquet file per logical table, with column pruning reads.
    """
    def __init__(self, table_name: str, file_dir: str = "data", file_pfx: str = ""):
        self.table_name = table_name
        self.file_dir = file_dir
        os.makedirs(self.file_dir, exist_ok=True)
        self.path = os.path.join(self.file_dir, f"{file_pfx}{table_name}.parquet")

    def build_table(self, df: pd.DataFrame, compression="snappy", row_group_size=50_000) -> None:
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, self.path, compression=compression, row_group_size=row_group_size)

    def retrieve_data(self, columns=None) -> pd.DataFrame:
        return pd.read_parquet(self.path, columns=columns)

    def append_data(self, df: pd.DataFrame, compression="snappy") -> None:
        table = pa.Table.from_pandas(df, preserve_index=False)
        if not os.path.exists(self.path):
            pq.write_table(table, self.path, compression=compression)
        else:
            with pq.ParquetWriter(self.path, table.schema, compression=compression, use_dictionary=True) as w:
                w.write_table(table)
