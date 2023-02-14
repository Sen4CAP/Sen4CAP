#!/bin/bash

DOCKER_IMG_NAME="sen4cap/processors:3.2.0"

if [ $# -eq 1 ]; then
	echo "Provided docker image $1"
	DOCKER_IMG_NAME="$1"
fi

echo "Removing sen2agri-processors RPM ..."
rm -f ./sen2agri-processors-*.rpm
echo "Copying latest sen2agri-processors RPM ..."
cp -fR ../../packaging/Sen2AgriRPM/sen2agri-processors-*.rpm .

echo "Starting building docker image for $DOCKER_IMG_NAME ..."
# sudo docker build -t "$DOCKER_IMG_NAME" .

tar -czh . | docker build -t "$DOCKER_IMG_NAME" -

