#!/bin/bash
tar -czh . | docker build -t sen4cap/grassland_mowing -
