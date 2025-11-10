import os, uuid
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from .storage import ColumnarDbFile
from .utils import hash_value

class HashPartitionJoin:
    """
    Hash-partition both sides; per-partition build+probe; emit partition results.
    Uses pandas.merge *inside a single partition only* after real partitioning.
    """
    def __init__(self, num_partitions: int = 8, batch_rows: int = 100_000):
        self.B = num_partitions
        self.batch_rows = batch_rows

    def _partition_to_disk(self, cdf: ColumnarDbFile, key: str, out_dir: str, tag: str, columns=None):
        os.makedirs(out_dir, exist_ok=True)
        pf = pq.ParquetFile(cdf.path)
        part_paths = [os.path.join(out_dir, f"{tag}_part{b}.parquet") for b in range(self.B)]
        writers = [None]*self.B
        try:
            for batch in pf.iter_batches(batch_size=self.batch_rows, columns=columns):
                df = batch.to_pandas()
                buckets = df[key].apply(lambda x: hash_value(x, self.B)).to_numpy()
                for b in range(self.B):
                    chunk = df[buckets == b]
                    if chunk.empty: continue
                    tbl = pa.Table.from_pandas(chunk, preserve_index=False)
                    if writers[b] is None:
                        writers[b] = pq.ParquetWriter(part_paths[b], tbl.schema, compression="snappy")
                    writers[b].write_table(tbl)
        finally:
            for w in writers:
                if w: w.close()
        return part_paths

    def join(self, left: ColumnarDbFile, right: ColumnarDbFile, left_key: str, right_key: str,
             temp_dir="temp", cols_left=None, cols_right=None) -> ColumnarDbFile:
        work = os.path.join(temp_dir, f"hpj_{uuid.uuid4().hex}")
        os.makedirs(work, exist_ok=True)
        Lparts = self._partition_to_disk(left, left_key, work, "L", columns=cols_left)
        Rparts = self._partition_to_disk(right, right_key, work, "R", columns=cols_right)

        out = ColumnarDbFile("HPJ_out", file_dir=work)
        writer = None

        for b in range(self.B):
            if not (os.path.exists(Lparts[b]) and os.path.exists(Rparts[b])): 
                continue
            lpf, rpf = pq.ParquetFile(Lparts[b]), pq.ParquetFile(Rparts[b])

            # Estimate rows and choose smaller to build hash dict in memory
            l_rows = sum(lpf.metadata.row_group(i).num_rows for i in range(lpf.metadata.num_row_groups))
            r_rows = sum(rpf.metadata.row_group(i).num_rows for i in range(rpf.metadata.num_row_groups))
            build_pf, build_key = (lpf, left_key) if l_rows <= r_rows else (rpf, right_key)
            probe_pf, probe_key = (rpf, right_key) if l_rows <= r_rows else (lpf, left_key)

            # Build dict: key -> DataFrame (concatenated batches)
            build_map = {}
            for batch in build_pf.iter_batches(batch_size=self.batch_rows):
                dfb = batch.to_pandas()
                for k, g in dfb.groupby(build_key):
                    if k in build_map:
                        build_map[k] = pd.concat([build_map[k], g], ignore_index=True)
                    else:
                        build_map[k] = g

            # Probe and emit matches
            for batch in probe_pf.iter_batches(batch_size=self.batch_rows):
                dfo = batch.to_pandas()
                out_buf = []
                for k, probe_group in dfo.groupby(probe_key):
                    if k in build_map:
                        if build_pf is lpf:
                            joined = build_map[k].merge(probe_group, left_on=left_key, right_on=right_key, how="inner")
                        else:
                            joined = probe_group.merge(build_map[k], left_on=left_key, right_on=right_key, how="inner")
                        if not joined.empty:
                            out_buf.append(joined)
                if out_buf:
                    tbl = pa.Table.from_pandas(pd.concat(out_buf, ignore_index=True), preserve_index=False)
                    if writer is None:
                        writer = pq.ParquetWriter(out.path, tbl.schema, compression="snappy")
                    writer.write_table(tbl)

        if writer: writer.close()
        return out
