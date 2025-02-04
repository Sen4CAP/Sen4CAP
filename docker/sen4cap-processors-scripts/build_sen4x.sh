#!/bin/bash
tar -czh . | docker build --progress=plain -t sen4x/sen4cap-processors-scripts:5.0.0 -
