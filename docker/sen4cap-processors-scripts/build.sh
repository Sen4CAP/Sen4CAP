#!/bin/bash
tar -czh . | docker build --progress=plain -t sen4cap/processors-scripts:3.3.0 -
