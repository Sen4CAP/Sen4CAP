#!/bin/bash
tar -czh . | docker build -t lnicola/fmask_extractor -
