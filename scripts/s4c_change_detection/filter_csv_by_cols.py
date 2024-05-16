#!/usr/bin/env python
import argparse
import pandas as pd
import time

def get_filtering_ids(csv_file, col_name):
    df = pd.read_csv(csv_file)
    return set(df[col_name])

def main():
    parser = argparse.ArgumentParser(
        description="Filters columns in CSV file"
    )
    parser.add_argument("-i", "--input", help="Input csv", required=True)
    parser.add_argument("-f", "--columns-to-keep", help="Columns to keep ", required=True, nargs="*")
    parser.add_argument("-m", "--mapping-columns", help="Mapping columns to the output", required=False, nargs="*")
    parser.add_argument("-o", "--output", help="Output file containing S2 results", required=True)

    parser.add_argument("-g", "--filtering-ids-file", help="Sites ids mapping file", required=False)
    parser.add_argument("-c", "--filtering-ids-col-name", help="The NewID column name for ids filtering", required=False, default="NewID")
    parser.add_argument("-t", "--target-ids-col-name", help="The NewID column name to be filtered", required=False, default="NewID")

    CHUNKSIZE = 100000

    args = parser.parse_args()

    time1 = time.time()

    mapping_cols = dict()
    if args.mapping_columns is not None:
        for element in args.mapping_columns:
            elems = element.split('=')
            if len(elems) == 2:
                mapping_cols[elems[0]] = elems[1]

    # get the IDs to be filtered
    filtering_ids = get_filtering_ids(args.filtering_ids_file, args.filtering_ids_col_name)

    columns_to_keep = list(args.columns_to_keep)
    reader = pd.read_csv(args.input, chunksize=CHUNKSIZE)

    # replace the columns to keep elements to be at the same case sensitivity as the 
    # values in the cvs colums

    first_df = True
    for df in reader:
        if first_df:        
            columns_to_keep_idx = 0
            for col in columns_to_keep:
                for column in df.columns:
                    if column.lower() == col.lower() :
                        columns_to_keep[columns_to_keep_idx] = column
                columns_to_keep_idx = columns_to_keep_idx + 1

        # rename columns
        df_to_write = df[columns_to_keep]
        for key in mapping_cols.keys():
            if key in df_to_write.columns:
                df_to_write.rename(columns = {key:mapping_cols[key]}, inplace = True) 

        # filter the ids
        df_to_write = df_to_write[df_to_write[args.target_ids_col_name].isin(filtering_ids)]
        if first_df:
            first_df = False
            df_to_write.to_csv(args.output, index=False)
        else:
            df_to_write.to_csv(args.output, mode='a', index=False, header=False)

    time2 = time.time()
    print("ALL Execution took: {} s" .format(time2 - time1))

if __name__ == "__main__":
    main()



