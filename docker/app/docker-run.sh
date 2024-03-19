#!/bin/sh
docker build \
    -t sen2agri-app-build \
    .

docker run -it --rm \
       -v $(realpath ../..):/sen2agri \
       sen2agri-app-build /bin/bash entry.sh
