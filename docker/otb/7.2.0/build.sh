#!/bin/bash
tar -czh . | docker build -t sen4x/otb:7.2.0 -
