FROM osgeo/gdal:ubuntu-full-3.3.1

LABEL maintainer="lnicola@c-s.ro" \
    description="L8 alignment script for MAJA"

RUN apt-get update && \
    apt-get install -y python3-lxml && \
    rm -rf /var/lib/apt/lists/*

COPY wrs2_descending /usr/share/sen2agri/wrs2_descending
COPY l8_alignment /usr/share/sen2agri/l8_alignment
