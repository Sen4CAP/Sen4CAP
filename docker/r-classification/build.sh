#!/bin/bash
tar -czh . | docker build -t sen4cap/r-classification:0.1.0 -
