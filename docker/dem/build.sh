#!/bin/bash
tar -czh . | docker build -t sen4x/l2a-dem:0.1.3 -
