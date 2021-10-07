#!/bin/bash
tar -czh . | docker build -t sen4stat/processors:1.0.0 -
