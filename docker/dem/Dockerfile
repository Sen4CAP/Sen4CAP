FROM osgeo/gdal:ubuntu-full-3.3.1

LABEL maintainer="lnicola@c-s.ro" \
    description="DEM script for MAJA"

RUN apt-get update && \
    apt-get install -y python3-lxml && \
    rm -rf /var/lib/apt/lists/*

COPY dem.py /usr/bin
