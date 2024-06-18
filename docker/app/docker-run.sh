#!/bin/sh
docker build \
    -t sen2agri-app-build \
    .

docker run -it --rm \
       -v $PWD/../..:/sen2agri:z \
       -v $PWD/entry.sh:/entry.sh:z \
       sen2agri-app-build /entry.sh

sudo chown $USER:$USER ../../packaging/Sen2AgriRPM/sen2agri-app-*.rpm
