#!/bin/bash

docker run -it --rm \
       -v $(realpath ../..):/sen2agri \
       -u $(id -u):$(id -g) \
       sen2agri-build /bin/bash entry.sh
