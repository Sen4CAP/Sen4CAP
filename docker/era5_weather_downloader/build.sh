#!/bin/bash
tar -czh . | docker build -t sen4x/era5-weather:0.0.1 -
