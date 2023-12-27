!/usr/bin/env python3
import argparse
import pyarrow
import pyarrow.compute
import pyarrow.dataset
from pyarrow import ipc
import struct


# Slow, consider using ogr2ogr instead

def main():
    parser = argparse.ArgumentParser(
        description="Filters an Apache Arrow file on the NewID column"
    )
    parser.add_argument("input", help="The input Arrow file")
    parser.add_argument("filter", help="The input filter CSV values")
    parser.add_argument("output", help="The output Arrow file")

    args = parser.parse_args()

    filter_schema = pyarrow.schema([("NewID", pyarrow.int32())])
    ds = pyarrow.dataset.dataset(args.input, format="ipc")
    ds_filter = pyarrow.dataset.dataset(args.filter, format="csv", schema=filter_schema)

    ds_table = ds_filter.to_table()
    filter_col = ds_table.column("NewID")

    # filter_expr = pyarrow.compute.is_in(pyarrow.compute.field("NewID"), filter_col)
    filter_expr = pyarrow.compute.is_in(
        pyarrow.compute.field("NewID"), pyarrow.array(filter_col.to_pylist())
    )

    # subset = ds.join(ds_filter, keys="NewID", join_type="inner")
    subset = ds.filter(filter_expr)
    writer = pyarrow.ipc.new_file(args.output, schema=subset.schema)
    writer.write_table(subset.to_table())
    writer.close()

    reader = ipc.open_file(args.output)
    with open("{}.idx".format(args.output), "wb") as idxFile:
        for i in range(0, reader.num_record_batches):
            b = reader.get_batch(i)

            idCol = b.column(0)
            idxFile.write(struct.pack(">i", i))
            idxFile.write(struct.pack(">i", idCol[0].as_py()))
            idxFile.write(struct.pack(">i", idCol[len(idCol) - 1].as_py()))


if __name__ == "__main__":
    main()
