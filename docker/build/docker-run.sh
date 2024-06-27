#!/bin/sh
docker build \
    -t sen2agri-build \
    .

docker run -it --rm \
       -v $(realpath ../..):/sen2agri \
       sen2agri-build /bin/bash entry.sh
