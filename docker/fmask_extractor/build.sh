#!/bin/bash
tar -czh . | docker build -t sen4x/fmask_extractor:0.1.1 -
