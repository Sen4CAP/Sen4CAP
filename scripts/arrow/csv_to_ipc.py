import pyarrow
from pyarrow import csv, ipc
import argparse
import os
import struct
import re
 
def check_regex(regex, field_name) :
    if regex == "":
        return False
    print("Checking regex {}".format(regex))
    z = re.match(regex, field_name)
    if z :
        return True
    return False
 
def get_type_for_column(args, field_name) :
    if field_name == "NewID" :
        return pyarrow.int64()
        
    pd_type = pyarrow.float32() # TODO: what should be the default ?
    if check_regex(args.int8_columns, field_name) == True :
        pd_type = pyarrow.int8()
    elif check_regex(args.int16_columns, field_name) == True :
        pd_type = pyarrow.int16()
    elif check_regex(args.int32_columns, field_name) == True :
        pd_type = pyarrow.int32()
    elif check_regex(args.float_columns, field_name) == True :
        pd_type = pyarrow.float32()
    elif check_regex(args.bool_columns, field_name) == True :
        pd_type = pyarrow.bool_()
    elif check_regex(args.text_columns, field_name) == True :
        pd_type = pyarrow.string()
        
    return pd_type

def get_panda_type_for_column(args, field_name) :
    if field_name == "NewID" :
        return "int64"
    
    pd_type = "float32"     # TODO: what should be the default ?
    if check_regex(args.int8_columns, field_name) == True :
        pd_type = "int8"
    elif check_regex(args.int16_columns, field_name) == True :
        pd_type = "int16"
    elif check_regex(args.int32_columns, field_name) == True :
        pd_type = "int32"
    elif check_regex(args.float_columns, field_name) == True :
        pd_type = "float32"
    elif check_regex(args.bool_columns, field_name) == True :
        pd_type = "bool"
    elif check_regex(args.text_columns, field_name) == True :
        pd_type = "str"
        
    return pd_type

def main():
    parser = argparse.ArgumentParser(description="Create a new column with aggredated values from other columns.")
    parser.add_argument('-i', '--input', help="The input CSV file")
    parser.add_argument('-o', '--output', help="The output IPC file")
    parser.add_argument('-c', '--int8-columns', default="", help="Int8 columns")
    parser.add_argument('-s', '--int16-columns', default="", help="Int16 columns")
    parser.add_argument('-g', '--int32-columns', default="", help="Int32 columns")
    parser.add_argument('-f', '--float-columns', default="", help="Float columns")
    parser.add_argument('-b', '--bool-columns', default="", help="Boolean columns")
    parser.add_argument('-n', '--nullable-columns', default="", help="Nullable columns")
    parser.add_argument('-t', '--text-columns', default="", help="Text columns")

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

        pd_type = get_type_for_column(args, field.name)
        field = field.with_type(pd_type)
        schema = schema.set(i, field)
        
        column_types_map[field.name] = pd_type
        print ("Setting field {} with type : {}".format(field.name, pd_type))
            
     
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
        # pd = pd.astype("float32")
        for column in pd.columns : 
            pd_type = get_panda_type_for_column(args, column)
            pd[column] = pd[column].astype(pd_type)
            print ("Setting panda column {} with type : {}".format(column, pd_type))
                
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
    