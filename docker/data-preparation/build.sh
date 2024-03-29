#!/bin/bash
tar -czh . | docker build -t sen4cap/data-preparation:0.3 -
docker build -f Dockerfile.isect -t sen4cap/data-preparation:0.3-isect .
