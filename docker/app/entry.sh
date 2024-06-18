#!/bin/bash
set -e

cd sen2agri/packaging
rm -rf Sen2AgriPlatform \
       Sen2AgriApp
rm -rf Sen2AgriRPM/sen2agri-app-*.rpm \
       Sen2AgriRPM/sen2agri-downloaders-demmaccs-*.rpm

mkdir -p Sen2AgriRPM

./Sen2AgriAppBuild.sh

mv Sen2AgriApp/rpm_binaries/*.rpm \
   Sen2AgriRPM
