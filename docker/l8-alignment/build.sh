#!/bin/bash
tar -czh . | docker build -t sen4x/l2a-l8-alignment:0.1.2 -
