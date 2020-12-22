#!/bin/bash
tar -czh . | docker build -t lnicola/sen2agri-l2a-processors -
