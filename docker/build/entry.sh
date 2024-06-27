#!/bin/bash
set -e

cd sen2agri/packaging
rm -rf Sen2AgriPlatform \
       Sen2AgriProcessors
rm -rf Sen2AgriRPM/sen2agri-downloaders-demmaccs-*.rpm \
       Sen2AgriRPM/sen2agri-processors-*.rpm

mkdir -p Sen2AgriRPM
if test -z "$(shopt -s nullglob; echo Sen2AgriRPM/otb-*.rpm)"
then
    if test -z "$(shopt -s nullglob; echo Sen2AgriPlatform/rpm_binaries/otb-*.rpm)"
    then
        ./Sen2AgriPlatformBuild.sh
    fi
    cp Sen2AgriPlatform/rpm_binaries/otb-*.rpm Sen2AgriRPM
fi

yum -y install Sen2AgriRPM/otb-*.rpm

./Sen2AgriProcessorsBuild.sh

mv Sen2AgriProcessors/rpm_binaries/*.rpm \
   Sen2AgriRPM

rm -rf Sen2AgriProcessors
