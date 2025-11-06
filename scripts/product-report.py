#!/usr/bin/env python3

import json
import csv
import os
import xlsxwriter

with open("polygon-statistics.json", "r") as f:
    polygon_stats = json.load(f)

strata = list(polygon_stats.keys())
multi_strata = len(strata) > 1

remapping_exists = os.path.exists("remapping-table.csv")
remapping_dict = {}

if remapping_exists:
    with open("remapping-table.csv", "r") as f:
        csv_reader = csv.reader(f)
        for row in csv_reader:
            remapping_dict[int(row[0])] = int(row[1])

output_file = "classification_report.xlsx"
try:
    os.remove(output_file)
except FileNotFoundError:
    pass

workbook = xlsxwriter.Workbook(output_file)

percent_format = workbook.add_format({"num_format": "0.00%", "font_color": "automatic"})
header_format = workbook.add_format({"bold": True, "font_color": "automatic"})
standard_format = workbook.add_format({"font_color": "automatic"})

ws_poly_stats = workbook.add_worksheet("Polygon Statistics")

headers = []
if multi_strata:
    headers.append("Stratum")
headers.append("Original Class")
if remapping_exists:
    headers.append("Remapped Class")

headers.extend(
    [
        "Training polygons",
        "Validation polygons",
        "Training samples",
        "Validation samples",
        "SMOTE samples",
    ]
)

for col, header in enumerate(headers):
    ws_poly_stats.write(0, col, header, header_format)

row_idx = 1
for stratum, classes in polygon_stats.items():
    stratum = int(stratum)
    for class_id, stats in classes.items():
        col_idx = 0
        if multi_strata:
            ws_poly_stats.write(row_idx, col_idx, stratum, standard_format)
            col_idx += 1

        ws_poly_stats.write(row_idx, col_idx, class_id, standard_format)
        col_idx += 1

        if remapping_exists:
            remapped_class = remapping_dict.get(int(class_id), class_id)
            ws_poly_stats.write(row_idx, col_idx, remapped_class, standard_format)
            col_idx += 1

        ws_poly_stats.write(
            row_idx, col_idx, stats["training_polygons"], standard_format
        )
        col_idx += 1

        ws_poly_stats.write(
            row_idx, col_idx, stats["validation_polygons"], standard_format
        )
        col_idx += 1

        ws_poly_stats.write(
            row_idx, col_idx, stats["training_samples"], standard_format
        )
        col_idx += 1

        ws_poly_stats.write(
            row_idx, col_idx, stats["validation_samples"], standard_format
        )
        col_idx += 1

        ws_poly_stats.write(row_idx, col_idx, stats["smote_samples"], standard_format)

        row_idx += 1

summary_data = []

for stratum in strata:
    if multi_strata:
        pre_metrics_file = f"confusion_matrix_pre_{stratum}.json"
        post_metrics_file = f"confusion_matrix_{stratum}.json"
    else:
        pre_metrics_file = "confusion_matrix_pre.json"
        post_metrics_file = "confusion_matrix.json"

    pre_metrics_exists = os.path.exists(pre_metrics_file)
    post_metrics_exists = os.path.exists(post_metrics_file)

    if not post_metrics_exists:
        print(f"Warning: No metrics file found for stratum {stratum}")
        continue

    with open(post_metrics_file, "r") as f:
        confusion_post = json.load(f)

    if pre_metrics_exists and remapping_exists:
        with open(pre_metrics_file, "r") as f:
            confusion_pre = json.load(f)

        sheet_name = (
            f"Pre-Remapping Metrics {stratum}"
            if multi_strata
            else "Pre-Remapping Metrics"
        )
        ws_pre_metrics = workbook.add_worksheet(sheet_name)

        ws_pre_metrics.write(0, 0, "Original Class", header_format)
        ws_pre_metrics.write(0, 1, "Recall", header_format)
        ws_pre_metrics.write(0, 2, "Precision", header_format)
        ws_pre_metrics.write(0, 3, "F1-Score", header_format)

        for i, label in enumerate(confusion_pre["labels"]):
            ws_pre_metrics.write(i + 1, 0, label, header_format)
            ws_pre_metrics.write(
                i + 1, 1, confusion_pre["class_recall"][i], percent_format
            )
            ws_pre_metrics.write(
                i + 1, 2, confusion_pre["class_precision"][i], percent_format
            )
            ws_pre_metrics.write(
                i + 1, 3, confusion_pre["class_f1_score"][i], percent_format
            )

        sheet_name = (
            f"Pre-Remapping Matrix {stratum}"
            if multi_strata
            else "Pre-Remapping Confusion Matrix"
        )
        ws_pre_confusion = workbook.add_worksheet(sheet_name)

        ws_pre_confusion.write(0, 0, "Actual / Predicted", header_format)

        for i, label in enumerate(confusion_pre["labels"]):
            ws_pre_confusion.write(0, i + 1, label, header_format)
            ws_pre_confusion.write(i + 1, 0, label, header_format)

        for i, row_data in enumerate(confusion_pre["confusion_matrix"]):
            for j, val in enumerate(row_data):
                ws_pre_confusion.write(i + 1, j + 1, val, standard_format)

    sheet_prefix = "Post-Remapping" if remapping_exists else "Metrics"
    sheet_name = (
        f"{sheet_prefix} Metrics {stratum}"
        if multi_strata
        else f"{sheet_prefix} Metrics"
    )
    ws_post_metrics = workbook.add_worksheet(sheet_name)

    class_label = "Remapped Class" if remapping_exists else "Class"
    ws_post_metrics.write(0, 0, class_label, header_format)
    ws_post_metrics.write(0, 1, "Recall", header_format)
    ws_post_metrics.write(0, 2, "Precision", header_format)
    ws_post_metrics.write(0, 3, "F1-Score", header_format)

    for i, label in enumerate(confusion_post["labels"]):
        ws_post_metrics.write(i + 1, 0, label, header_format)
        ws_post_metrics.write(
            i + 1, 1, confusion_post["class_recall"][i], percent_format
        )
        ws_post_metrics.write(
            i + 1, 2, confusion_post["class_precision"][i], percent_format
        )
        ws_post_metrics.write(
            i + 1, 3, confusion_post["class_f1_score"][i], percent_format
        )

    sheet_prefix = "Post-Remapping" if remapping_exists else "Confusion"
    sheet_name = (
        f"{sheet_prefix} Matrix {stratum}" if multi_strata else f"{sheet_prefix} Matrix"
    )
    ws_post_confusion = workbook.add_worksheet(sheet_name)

    ws_post_confusion.write(0, 0, "Actual / Predicted", header_format)

    for i, label in enumerate(confusion_post["labels"]):
        ws_post_confusion.write(0, i + 1, label, header_format)
        ws_post_confusion.write(i + 1, 0, label, header_format)

    for i, row_data in enumerate(confusion_post["confusion_matrix"]):
        for j, val in enumerate(row_data):
            ws_post_confusion.write(i + 1, j + 1, val, standard_format)

    metric_map = [
        ("Overall Accuracy", "overall_accuracy"),
        ("Balanced Accuracy", "balanced_accuracy"),
        ("Weighted Precision", "weighted_precision"),
        ("Weighted Recall", "weighted_recall"),
        ("Weighted F1-Score", "weighted_f1_score"),
    ]

    summary_entry = {
        "stratum": stratum,
    }

    for _, json_key in metric_map:
        if json_key in confusion_post:
            summary_entry[json_key] = confusion_post[json_key]

    summary_data.append(summary_entry)

    chart = workbook.add_chart({"type": "column"})
    if chart:
        chart.add_series(
            {
                "name": "F1-Score",
                "categories": [
                    ws_post_metrics.name,
                    1,
                    0,
                    len(confusion_post["labels"]),
                    0,
                ],
                "values": [
                    ws_post_metrics.name,
                    1,
                    3,
                    len(confusion_post["labels"]),
                    3,
                ],
                "data_labels": {"value": True, "num_format": "0.00%"},
            }
        )

        chart.set_title({"name": "Class F1-Scores"})
        chart.set_x_axis({"name": "Class"})
        chart.set_y_axis({"name": "F1-Score", "min": 0, "max": 1})
        chart.set_style(11)

        ws_post_metrics.insert_chart("F2", chart, {"x_scale": 1.5, "y_scale": 1.5})

if remapping_exists:
    ws_remapping = workbook.add_worksheet("Remapping Table")
    ws_remapping.write(0, 0, "Original Class", header_format)
    ws_remapping.write(0, 1, "Remapped Class", header_format)

    row_idx = 1
    for original, remapped in remapping_dict.items():
        ws_remapping.write(row_idx, 0, original, standard_format)
        ws_remapping.write(row_idx, 1, remapped, standard_format)
        row_idx += 1

ws_summary = workbook.add_worksheet("Summary")
col_idx = 0
if multi_strata:
    ws_summary.write(0, 0, "Stratum", header_format)
    col_idx += 1

metric_display_names = [
    "Overall Accuracy",
    "Balanced Accuracy",
    "Weighted Precision",
    "Weighted Recall",
    "Weighted F1-Score",
]
metric_keys = [
    "overall_accuracy",
    "balanced_accuracy",
    "weighted_precision",
    "weighted_recall",
    "weighted_f1_score",
]

for display_name in metric_display_names:
    ws_summary.write(0, col_idx, display_name, header_format)
    col_idx += 1

for i, data in enumerate(summary_data):
    col_idx = 0
    if multi_strata:
        ws_summary.write(i + 1, 0, data["stratum"], header_format)
        col_idx += 1

    for key in metric_keys:
        if key in data:
            ws_summary.write(i + 1, col_idx, data[key], percent_format)
        col_idx += 1

for worksheet in workbook.worksheets():
    worksheet.set_column("A:Z", 15)

ws_poly_stats.set_column(first_col=0, last_col=7, width=20)
if multi_strata:
    ws_summary.set_column(first_col=0, last_col=0, width=10)
    first_col = 1
else:
    first_col = 0
last_col = len(metric_keys)
if multi_strata:
    last_col += 1

ws_summary.set_column(first_col=first_col, last_col=last_col, width=30)

workbook.close()
