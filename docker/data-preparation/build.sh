#!/bin/bash
tar -czh . | docker build -t sen4cap/data-preparation:0.1 -
