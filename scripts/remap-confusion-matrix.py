#!/usr/bin/env python3
# Python 3.8 compat
from __future__ import annotations

import argparse

REFERENCE_LABELS = "#Reference labels (rows):"
PRODUCED_LABELS = "#Produced labels (columns):"


def read_confusion_matrix(
    file_path: str,
) -> tuple[list[int], list[int], list[list[int]]]:
    matrix = []
    with open(file_path, "r") as file:
        line = file.readline()
        reference_labels = list(map(int, line.strip().split(":")[1].split(",")))
        line = file.readline()
        produced_labels = list(map(int, line.strip().split(":")[1].split(",")))
        for line in file:
            matrix.append(list(map(int, line.strip().split(","))))

    return reference_labels, produced_labels, matrix


def read_remapping_table(file_path: str) -> dict[int, int]:
    remapping = {}
    with open(file_path, "r") as file:
        for line in file:
            original, new = map(int, line.strip().split(","))
            remapping[original] = new
    return remapping


def remap_labels(old_labels: list[int], remapping: dict[int, int]) -> dict[int, int]:
    new_labels = list(set(map(lambda label: remapping.get(label, label), old_labels)))
    new_labels.sort(key=str)
    mapping = {}
    for idx, label in enumerate(new_labels):
        mapping[label] = idx
    return mapping


def create_new_confusion_matrix(
    old_matrix: list[list[int]],
    old_ref_labels: list[int],
    old_prod_labels: list[int],
    ref_mapping: dict[int, int],
    prod_mapping: dict[int, int],
    remapping: dict[int, int],
) -> list[list[int]]:
    new_matrix = [
        [0 for _ in range(len(prod_mapping))] for _ in range(len(ref_mapping))
    ]

    for i, row in enumerate(old_matrix):
        for j, value in enumerate(row):
            old_ref_label = old_ref_labels[i]
            old_prod_label = old_prod_labels[j]
            new_ref_label = remapping.get(old_ref_label, old_ref_label)
            new_prod_label = remapping.get(old_prod_label, old_prod_label)
            new_i = ref_mapping[new_ref_label]
            new_j = prod_mapping[new_prod_label]
            new_matrix[new_i][new_j] += value

    return new_matrix


parser = argparse.ArgumentParser(description="Remap a confusion matrix")
parser.add_argument(
    "--confusion-matrix",
    required=True,
    help="original confusion matrix",
)
parser.add_argument(
    "--remapping-table",
    required=True,
    help="remapping table",
)
parser.add_argument(
    "--remapped-confusion-matrix",
    help="remapped confusion matrix",
)
args = parser.parse_args()

old_ref_labels, old_prod_labels, old_matrix = read_confusion_matrix(
    args.confusion_matrix
)
remapping = read_remapping_table(args.remapping_table)

ref_mapping = remap_labels(old_ref_labels, remapping)
prod_mapping = remap_labels(old_prod_labels, remapping)

new_matrix = create_new_confusion_matrix(
    old_matrix,
    old_ref_labels,
    old_prod_labels,
    ref_mapping,
    prod_mapping,
    remapping,
)

with open(args.remapped_confusion_matrix, "w") as file:
    file.write(REFERENCE_LABELS)
    file.write(",".join(map(str, ref_mapping.keys())))
    file.write("\n")
    file.write(PRODUCED_LABELS)
    file.write(",".join(map(str, prod_mapping.keys())))
    file.write("\n")
    for row in new_matrix:
        file.write(",".join(map(str, row)))
        file.write("\n")
