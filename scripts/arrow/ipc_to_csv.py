#!/usr/bin/env python3
import argparse
import csv
from pyarrow import ipc


def main():
    parser = argparse.ArgumentParser(description="Converts an Apache Arrow file to CSV")
    parser.add_argument('input', help="The input Arrow file")
    parser.add_argument('output', help="The output CSV file")

    args = parser.parse_args()

    reader = ipc.open_file(args.input)
    with open(args.output, "w") as file:
        writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(reader.schema.names)
        for i in range(0, reader.num_record_batches):
            b = reader.get_batch(i)
            writer.writerows(map(list, zip(*b)))


if __name__ == '__main__':
    main()
