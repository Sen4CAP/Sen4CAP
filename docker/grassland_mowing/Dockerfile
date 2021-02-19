FROM osgeo/gdal:ubuntu-full-3.2.0

LABEL maintainer="cosmin@c-s.ro" \
    description="Grassland mowing scripts"

RUN apt-get update && \
    apt-get install -y python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install pandas~=1.1.5 numpy~=1.19.5 scipy~=1.5.3 lxml~=4.6.2

COPY GrasslandMowing /usr/share/sen2agri/S4C_L4B_GrasslandMowing/Bin/


