import pyarrow
from pyarrow import csv, ipc
import argparse
import os
import struct
 
def main():
    parser = argparse.ArgumentParser(description="Create a new column with aggredated values from other columns.")
    parser.add_argument('-i', '--input', help="The input CSV file")
    parser.add_argument('-o', '--output', help="The output IPC file")

    args = parser.parse_args()
 
    reader = csv.open_csv(
        args.input, 
    )
    column_types_map={}
    schema = reader.schema
    for i in range(1, len(schema.names)):
        field = schema.field(i)
        if field.name.startswith("XX_"):
            field = field.with_name(field.name[3:])
        field = field.with_type(pyarrow.float32())
        schema = schema.set(i, field)
        if field.name != "NewID":
            column_types_map[field.name] = pyarrow.float32()
     
    # Re-open the csv with the new convert options for columns 
    # we try to avoid automatic detection of column types as if a column starts with null we will later get the error
    # pyarrow.lib.ArrowInvalid: In CSV column #105: CSV conversion error to null: invalid value <our good float value>
    reader = csv.open_csv(
        args.input, 
        convert_options=csv.ConvertOptions(
            column_types=column_types_map
        )
    )
     
    if not os.path.exists(os.path.dirname(args.output)):
        try:
            os.makedirs(os.path.dirname(args.output))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise    
    writer = ipc.new_file(args.output, schema)
    idxFile = open("{}.idx".format(args.output), "wb")
    currentBatch = 0
    for b in reader:
        pd = b.to_pandas()
        pd.columns = schema.names
        pd = pd.astype("float32")
        pd["NewID"] = pd["NewID"].astype("int64")
        b = pyarrow.RecordBatch.from_pandas(pd)
        # print(b.schema)
        writer.write_batch(b)
        
        idCol = b.column(0)
        idxFile.write(struct.pack('i', currentBatch))
        idxFile.write(struct.pack('i', idCol[0].as_py()))
        idxFile.write(struct.pack('i', idCol[len(idCol) - 1].as_py()))
        currentBatch = currentBatch + 1
    writer.close()
    idxFile.close()
    
if __name__ == '__main__':
    main()
    