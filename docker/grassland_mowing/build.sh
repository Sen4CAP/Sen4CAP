#!/bin/bash
tar -czh . | docker build -t sen4cap/grassland_mowing:3.0.0 -
