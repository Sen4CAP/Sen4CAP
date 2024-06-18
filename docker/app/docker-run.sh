#!/bin/sh
docker build \
    -t sen2agri-app-build \
    .

docker run -it --rm \
       -v $(realpath ../..):/sen2agri:z \
       sen2agri-app-build /bin/bash entry.sh

sudo chown $USER:$USER ../packaging/Sen2AgriApp/rpm_binaries/*.rpm
