#!/bin/bash
tar -czh . | docker build -t lnicola/sen2agri-l8-alignment -
