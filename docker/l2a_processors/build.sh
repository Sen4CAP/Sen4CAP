#!/bin/bash
tar -czh . | docker build -t sen4x/l2a-processors:0.1 -
