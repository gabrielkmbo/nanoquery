import os, uuid, heapq
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from .storage import ColumnarDbFile

class SortMergeJoin:
    """
    External sort both sides by key; streaming merge that handles duplicates (many-to-many).
    """
    def __init__(self, run_rows: int = 250_000, merge_batch_rows: int = 200_000):
        self.run_rows = run_rows
        self.merge_batch_rows = merge_batch_rows

    def _external_sort(self, cdf: ColumnarDbFile, key: str, out_dir: str, tag: str, columns=None) -> str:
        os.makedirs(out_dir, exist_ok=True)
        pf = pq.ParquetFile(cdf.path)
        run_paths = []
        for batch in pf.iter_batches(batch_size=self.run_rows, columns=columns):
            df = batch.to_pandas().sort_values(key, kind="mergesort")
            p = os.path.join(out_dir, f"{tag}_run_{uuid.uuid4().hex}.parquet")
            pq.write_table(pa.Table.from_pandas(df, preserve_index=False), p, compression="snappy")
            run_paths.append(p)
        out_path = os.path.join(out_dir, f"{tag}_sorted.parquet")
        self._kway_merge(run_paths, out_path, key)
        for p in run_paths: os.remove(p)
        return out_path

    def _kway_merge(self, paths, out_path, key):
        readers = [pq.ParquetFile(p) for p in paths]
        iters = []
        for r in readers:
            gen = r.iter_batches(batch_size=self.merge_batch_rows)
            first = next(gen, None)
            iters.append({"gen": gen, "df": first.to_pandas() if first is not None else None, "i": 0})
        heap = []
        def push(idx):
            row = iters[idx]["df"].iloc[iters[idx]["i"]]
            heapq.heappush(heap, (row[key], idx, row))
        for i, st in enumerate(iters):
            if st["df"] is not None and len(st["df"]) > 0:
                push(i)
        writer, out_buf = None, []
        while heap:
            _, i, row = heapq.heappop(heap)
            out_buf.append(row)
            iters[i]["i"] += 1
            if iters[i]["i"] >= len(iters[i]["df"]):
                nxt = next(iters[i]["gen"], None)
                if nxt is None:
                    iters[i]["df"] = None
                else:
                    iters[i]["df"] = nxt.to_pandas()
                iters[i]["i"] = 0
            if iters[i]["df"] is not None and len(iters[i]["df"]) > 0:
                push(i)
            if len(out_buf) >= self.merge_batch_rows:
                tbl = pa.Table.from_pandas(pd.DataFrame(out_buf), preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(out_path, tbl.schema, compression="snappy")
                writer.write_table(tbl); out_buf.clear()
        if out_buf:
            tbl = pa.Table.from_pandas(pd.DataFrame(out_buf), preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(out_path, tbl.schema, compression="snappy")
            writer.write_table(tbl)
        if writer: writer.close()

    def join(self, left: ColumnarDbFile, right: ColumnarDbFile, left_key: str, right_key: str,
             temp_dir="temp", cols_left=None, cols_right=None) -> ColumnarDbFile:
        work = os.path.join(temp_dir, f"smj_{uuid.uuid4().hex}")
        os.makedirs(work, exist_ok=True)
        L_sorted = self._external_sort(left, left_key, work, "L", columns=cols_left)
        R_sorted = self._external_sort(right, right_key, work, "R", columns=cols_right)

        out = ColumnarDbFile("SMJ_out", file_dir=work)
        writer = None
        lpf, rpf = pq.ParquetFile(L_sorted), pq.ParquetFile(R_sorted)
        li, ri = lpf.iter_batches(batch_size=self.merge_batch_rows), rpf.iter_batches(batch_size=self.merge_batch_rows)
        ldf = next(li, None); ldf = ldf.to_pandas() if ldf is not None else None
        rdf = next(ri, None); rdf = rdf.to_pandas() if rdf is not None else None
        lp = rp = 0
        out_buf = []

        def refill_left():
            nonlocal ldf, lp
            batch = next(li, None)
            ldf = batch.to_pandas() if batch is not None else None; lp = 0

        def refill_right():
            nonlocal rdf, rp
            batch = next(ri, None)
            rdf = batch.to_pandas() if batch is not None else None; rp = 0

        while ldf is not None and rdf is not None and lp < len(ldf) and rp < len(rdf):
            lv, rv = ldf.iloc[lp][left_key], rdf.iloc[rp][right_key]
            if lv < rv:
                lp += 1
                if lp >= len(ldf): refill_left()
            elif lv > rv:
                rp += 1
                if rp >= len(rdf): refill_right()
            else:
                lstart = lp
                while lp < len(ldf) and ldf.iloc[lp][left_key] == lv: lp += 1
                rstart = rp
                while rp < len(rdf) and rdf.iloc[rp][right_key] == rv: rp += 1
                Lgrp = ldf.iloc[lstart:lp]
                Rgrp = rdf.iloc[rstart:rp]
                out_buf.append(Lgrp.merge(Rgrp, left_on=left_key, right_on=right_key, how="inner"))
                if len(out_buf) >= 16:
                    chunk = pd.concat(out_buf, ignore_index=True)
                    tbl = pa.Table.from_pandas(chunk, preserve_index=False)
                    if writer is None:
                        writer = pq.ParquetWriter(out.path, tbl.schema, compression="snappy")
                    writer.write_table(tbl); out_buf.clear()
                if lp >= len(ldf): refill_left()
                if rp >= len(rdf): refill_right()

        if out_buf:
            chunk = pd.concat(out_buf, ignore_index=True)
            tbl = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(out.path, tbl.schema, compression="snappy")
            writer.write_table(tbl)
        if writer: writer.close()
        return out
