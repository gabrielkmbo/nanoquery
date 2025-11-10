import os
import hashlib
import numpy as np
import pyarrow.parquet as pq

def file_size_mb(path: str) -> float:
    return os.path.getsize(path) / (1024*1024)

def parquet_metadata(path: str):
    pf = pq.ParquetFile(path)
    rows = sum(pf.metadata.row_group(i).num_rows for i in range(pf.metadata.num_row_groups))
    cols = [pf.schema.names[i] for i in range(pf.schema.num_columns)]
    return {"rows": rows, "columns": cols, "row_groups": pf.metadata.num_row_groups, "disk_mb": file_size_mb(path)}

def hash_value(v, B: int) -> int:
    if isinstance(v, (int, np.integer)): 
        return int(v) % B
    h = hashlib.sha256(str(v).encode()).hexdigest()
    return int(h, 16) % B
